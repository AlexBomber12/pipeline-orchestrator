"""Backfill legacy run metrics with outcome/cause fields.

PR-286 added normalized run-record fields for new writes. This startup
migration fills the fields that can be derived from legacy ``exit_reason``
values and rebuilds the per-task run index for existing Redis hashes.
"""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator
from typing import Any

RUN_RECORD_TTL_SECONDS = 365 * 86400
NULL_CAUSE_VALUE = ""

_EXIT_REASON_TO_OUTCOME_CAUSE: dict[str, tuple[str, str]] = {
    "success_merged": ("merged", NULL_CAUSE_VALUE),
    "coding_complete": ("superseded", NULL_CAUSE_VALUE),
    "closed_unmerged": ("superseded", NULL_CAUSE_VALUE),
    "rate_limit": ("paused", NULL_CAUSE_VALUE),
    "crash": ("failed", "CRASH"),
    "timeout": ("failed", "TIMEOUT"),
    "error": ("failed", "INFRA"),
    "escalated": ("failed", "ESCALATE"),
    "paused": ("paused", NULL_CAUSE_VALUE),
    "stopped": ("paused", NULL_CAUSE_VALUE),
    "cancelled": ("failed", "ESCALATE"),
}


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _iter_run_record_keys(redis_client: Any) -> AsyncIterator[str | bytes]:
    keys = redis_client.scan_iter(match="metrics:run:*")
    if hasattr(keys, "__aiter__"):
        async for key in keys:
            yield key
        return
    for key in keys:
        yield key


def _decode(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _normalize_hash(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    return {str(_decode(key)): _decode(value) for key, value in raw.items()}


async def _key_type(redis_client: Any, key: str | bytes) -> str | None:
    type_method = getattr(redis_client, "type", None)
    if type_method is None:
        return None
    raw_type = await _maybe_await(type_method(key))
    decoded = _decode(raw_type)
    if decoded is None:
        return None
    return str(decoded)


def _extract_repo_and_record_id(key: str | bytes) -> tuple[str | None, str] | None:
    normalized = _decode(key)
    if not isinstance(normalized, str):
        return None
    parts = normalized.split(":", 3)
    if len(parts) not in {3, 4} or parts[0] != "metrics" or parts[1] != "run":
        return None
    if len(parts) == 3:
        record_id = parts[2]
        if not record_id:
            return None
        return None, record_id

    repo, record_id = parts[2], parts[3]
    if not repo or not record_id:
        return None
    return repo, record_id


def _warn(log: Any, message: str) -> None:
    if hasattr(log, "warning"):
        log.warning(message)
    else:
        log(message)


async def migrate_run_records_to_outcome_cause(
    redis_client: Any,
    log: Any,
) -> dict[str, int]:
    """Populate derivable PR-286 fields on legacy ``metrics:run:*`` hashes."""
    counts = {
        "records_scanned": 0,
        "records_migrated": 0,
        "records_skipped_already_migrated": 0,
        "records_skipped_malformed": 0,
    }

    async for key in _iter_run_record_keys(redis_client):
        counts["records_scanned"] += 1
        parsed = _extract_repo_and_record_id(key)
        if parsed is None:
            counts["records_skipped_malformed"] += 1
            _warn(log, f"[MIGRATION] Skipping malformed run-record key {key}")
            continue
        repo, record_id = parsed

        redis_type = await _key_type(redis_client, key)
        if redis_type not in {None, "hash"}:
            counts["records_skipped_malformed"] += 1
            _warn(log, f"[MIGRATION] Skipping non-hash run-record key {key}")
            continue

        raw_record = await _maybe_await(redis_client.hgetall(key))
        record = _normalize_hash(raw_record)
        if not record:
            counts["records_skipped_malformed"] += 1
            _warn(log, f"[MIGRATION] Skipping malformed run-record hash {key}")
            continue
        if "outcome" in record:
            counts["records_skipped_already_migrated"] += 1
            continue

        task_id = record.get("task_id")
        if not task_id:
            counts["records_skipped_malformed"] += 1
            _warn(log, f"[MIGRATION] Skipping run record {key}: missing task_id")
            continue
        repo_scope = repo or str(record.get("repo_name") or "global")

        exit_reason = str(record.get("exit_reason") or "")
        mapped = _EXIT_REASON_TO_OUTCOME_CAUSE.get(exit_reason)
        if mapped is None:
            mapped = ("failed", NULL_CAUSE_VALUE)
            _warn(
                log,
                "[MIGRATION] Unknown run-record exit_reason "
                f"{exit_reason!r} for {key}; defaulting to failed",
            )
        outcome, cause = mapped

        await _maybe_await(
            redis_client.hset(key, mapping={"outcome": outcome, "cause": cause})
        )
        task_runs_key = f"metrics:task_runs:{repo_scope}:{task_id}"
        await _maybe_await(redis_client.sadd(task_runs_key, record_id))
        await _maybe_await(
            redis_client.expire(task_runs_key, RUN_RECORD_TTL_SECONDS)
        )
        await _maybe_await(redis_client.expire(key, RUN_RECORD_TTL_SECONDS))
        counts["records_migrated"] += 1

    return counts
