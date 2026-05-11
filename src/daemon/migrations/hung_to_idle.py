"""Migrate legacy Redis states after Phase 2 removals.

PR-276/PR-277 removed ``PipelineState.HUNG`` from the runtime model. This
startup migration rewrites any already-persisted Redis payloads before the
daemon validates them through the current enum. PR-280 also removed
``TaskStatus.CANCELED``; persisted queue/task statuses are rewritten to
``ERROR`` during the same startup scan.
"""

from __future__ import annotations

import inspect
import json
from collections.abc import AsyncIterator
from typing import Any

from src.cancellation import CancellationCause, record_cancellation_cause

MIGRATION_NOTE = "deploy_2026_hung_to_idle_phase_2"
MIGRATION_REASON = "Migrated from PipelineState.HUNG by Phase 2 deploy"


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _iter_pipeline_keys(redis_client: Any) -> AsyncIterator[str | bytes]:
    keys = redis_client.scan_iter(match="pipeline:*")
    if hasattr(keys, "__aiter__"):
        async for key in keys:
            yield key
        return
    for key in keys:
        yield key


def _key_to_repo_slug(key: str | bytes) -> str:
    if isinstance(key, bytes):
        key = key.decode("utf-8")
    return key.removeprefix("pipeline:")


def _extract_task_id(current_task: Any) -> str | None:
    if current_task is None:
        return None
    if isinstance(current_task, dict):
        pr_id = current_task.get("pr_id")
        return str(pr_id) if pr_id else None
    return str(current_task)


def _warn(log: Any, message: str) -> None:
    if hasattr(log, "warning"):
        log.warning(message)
    else:
        log(message)


def _info(log: Any, message: str) -> None:
    if hasattr(log, "info"):
        log.info(message)
    else:
        log(message)


def _rewrite_legacy_task_statuses(value: Any) -> bool:
    if isinstance(value, dict):
        changed = False
        if value.get("status") == "CANCELED":
            value["status"] = "ERROR"
            changed = True
        nested_changed = False
        for item in value.values():
            nested_changed = _rewrite_legacy_task_statuses(item) or nested_changed
        return nested_changed or changed
    if isinstance(value, list):
        changed = False
        for item in value:
            changed = _rewrite_legacy_task_statuses(item) or changed
        return changed
    return False


async def migrate_hung_to_idle_on_startup(redis_client: Any, log: Any) -> int:
    """Rewrite legacy Redis ``state=HUNG`` payloads to ``state=IDLE``.

    The migration is idempotent: after the first successful rewrite, future
    startup scans observe no ``HUNG`` states and perform no writes. Malformed
    ``pipeline:*`` values and cancellation-cause storage failures are logged
    and skipped without aborting daemon startup.
    """
    migrated = 0
    async for key in _iter_pipeline_keys(redis_client):
        raw = await _maybe_await(redis_client.get(key))
        try:
            payload = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            _warn(log, f"[MIGRATION] Skipping malformed pipeline:* key {key}")
            continue

        if not isinstance(payload, dict):
            continue

        repo_slug = _key_to_repo_slug(key)
        status_migrated = _rewrite_legacy_task_statuses(payload)
        if payload.get("state") != "HUNG":
            if status_migrated:
                await _maybe_await(redis_client.set(key, json.dumps(payload)))
                migrated += 1
                _info(
                    log,
                    f"[MIGRATION] CANCELED→ERROR task statuses for {repo_slug}",
                )
            continue

        task_id = _extract_task_id(payload.get("current_task"))
        payload["state"] = "IDLE"
        await _maybe_await(redis_client.set(key, json.dumps(payload)))
        migrated += 1

        cause_recorded = False
        if task_id is not None:
            cause = CancellationCause(
                category="ERROR",
                payload={
                    "subsource": "daemon",
                    "reason_text": MIGRATION_REASON,
                    "migration_note": MIGRATION_NOTE,
                    "legacy_category": "ESCALATE",
                },
            )
            try:
                await record_cancellation_cause(redis_client, repo_slug, task_id, cause)
                cause_recorded = True
            except Exception as exc:
                _warn(
                    log,
                    "[MIGRATION] Failed to record cancellation cause for "
                    f"{repo_slug}, task {task_id}: {exc}",
                )

        suffix = (
            "cancellation cause recorded"
            if cause_recorded
            else "no cancellation cause recorded"
        )
        _info(
            log,
            f"[MIGRATION] HUNG\u2192IDLE for {repo_slug}, task {task_id}, {suffix}",
        )

    return migrated
