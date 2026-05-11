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

# Explicit allow-list of legacy ``category`` values PR-315 intends to
# collapse. Records carrying any other historical category (for example
# ``OPERATOR_RECOVERY``, which predates the PR-281 cleanup and is still
# treated as a legacy value by the dashboard's cancellation card) are
# preserved verbatim — silently rewriting them to ``ERROR`` would
# corrupt their original semantics in Redis history.
LEGACY_CATEGORIES = frozenset(
    {"CRASH", "ESCALATE", "TIMEOUT", "INFRA", "NO_PUSH_DEADLOCK"}
)


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
        if category not in LEGACY_CATEGORIES:
            # Unknown / out-of-scope historical category — leave the
            # record untouched rather than silently relabeling its
            # forensic semantics to ERROR.
            continue

        cause_payload = payload.get("payload")
        if not isinstance(cause_payload, dict):
            cause_payload = {}
            payload["payload"] = cause_payload
        cause_payload.setdefault("legacy_category", category)
        payload["category"] = UNIFIED_CATEGORY

        serialized = json.dumps(payload, separators=(",", ":"))
        try:
            # keepttl preserves the prior expiry so migrated records still
            # roll off after the original cancellation:* TTL window.
            await _maybe_await(
                redis_client.set(raw_key, serialized, keepttl=True)
            )
        except TypeError:
            # Older Redis client shims may not accept keepttl=; fall back to
            # reapplying the remaining TTL fetched via ttl().
            try:
                ttl_remaining = await _maybe_await(redis_client.ttl(raw_key))
            except Exception as exc:
                _warn(
                    log,
                    f"[MIGRATION] Failed to read TTL for {key_str} "
                    f"(legacy_category={category}): {exc}",
                )
                continue
            if ttl_remaining == -2:
                # Redis returns -2 when the key no longer exists, i.e.
                # it expired between the get() above and this ttl()
                # probe. Writing it back would resurrect stale
                # cancellation data as a persistent record; skip the
                # rewrite entirely.
                continue
            try:
                if isinstance(ttl_remaining, int) and ttl_remaining >= 0:
                    # Redis TTL is second-granularity, so a reported 0
                    # means the key is within its final second of life.
                    # Reapply ex=max(ttl, 1) instead of falling through
                    # to the no-expiry set() path, which would convert a
                    # near-expiry cancellation record into a persistent
                    # one. ex=0 is rejected by Redis, hence the floor.
                    await _maybe_await(
                        redis_client.set(
                            raw_key, serialized, ex=max(ttl_remaining, 1)
                        )
                    )
                else:
                    # -1 means the key exists with no expiry; rewrite
                    # it without an ex= argument to preserve persistence.
                    await _maybe_await(redis_client.set(raw_key, serialized))
            except Exception as exc:
                _warn(
                    log,
                    f"[MIGRATION] Failed to rewrite {key_str} "
                    f"(legacy_category={category}): {exc}",
                )
                continue
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
