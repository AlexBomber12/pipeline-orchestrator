"""Rewrite legacy ``CancellationCause`` records to the unified ``ERROR`` category.

PR-315 collapsed the five legacy ``category`` values (``CRASH``,
``ESCALATE``, ``TIMEOUT``, ``INFRA``, ``NO_PUSH_DEADLOCK``) into a
single canonical ``ERROR``. Forensic detail moved to
``payload.subsource`` (see ``src/cancellation/storage.py`` for the
documented vocabulary).

This startup migration rewrites any persisted ``cancellation:*`` Redis
records so the dashboard and recovery layers can dispatch on the new
unified category without special-casing pre-PR-315 history. The
migration is idempotent: rerunning it on records that already carry
``category == "ERROR"`` performs no writes. The original detector
value is preserved as ``payload.legacy_category`` for forensic recall.
"""

from __future__ import annotations

import inspect
import json
from collections.abc import AsyncIterator
from typing import Any

UNIFIED_CATEGORY = "ERROR"


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _iter_cancellation_keys(redis_client: Any) -> AsyncIterator[str | bytes]:
    keys = redis_client.scan_iter(match="cancellation:*")
    if hasattr(keys, "__aiter__"):
        async for key in keys:
            yield key
        return
    for key in keys:
        yield key


def _decode_key(key: str | bytes) -> str:
    return key.decode("utf-8") if isinstance(key, bytes) else key


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


async def migrate_escalate_to_error_on_startup(
    redis_client: Any, log: Any
) -> int:
    """Rewrite legacy ``cancellation:*`` records to ``category=ERROR``.

    Returns the number of records rewritten. Records that already carry
    ``category == "ERROR"`` are left untouched so subsequent startups
    perform no writes. Malformed payloads and read/write failures are
    logged and skipped without aborting daemon startup; index entries
    (``cancellation_index:*``) carry no category data and are not
    touched here.
    """
    migrated = 0
    async for raw_key in _iter_cancellation_keys(redis_client):
        key_str = _decode_key(raw_key)
        raw = await _maybe_await(redis_client.get(raw_key))
        if raw is None:
            continue
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            _warn(log, f"[MIGRATION] Skipping malformed cancellation:* key {key_str}")
            continue

        if not isinstance(payload, dict):
            continue

        category = payload.get("category")
        if category == UNIFIED_CATEGORY:
            continue

        cause_payload = payload.get("payload")
        if not isinstance(cause_payload, dict):
            cause_payload = {}
            payload["payload"] = cause_payload
        cause_payload.setdefault("legacy_category", category)
        payload["category"] = UNIFIED_CATEGORY

        try:
            await _maybe_await(
                redis_client.set(raw_key, json.dumps(payload, separators=(",", ":")))
            )
        except Exception as exc:
            _warn(
                log,
                f"[MIGRATION] Failed to rewrite {key_str} "
                f"(legacy_category={category}): {exc}",
            )
            continue

        migrated += 1
        _info(
            log,
            f"[MIGRATION] cancellation cause {key_str} {category}→{UNIFIED_CATEGORY}",
        )

    return migrated
