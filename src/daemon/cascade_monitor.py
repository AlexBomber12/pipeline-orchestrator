"""Daemon-wide ESCALATE cascade detector — Redis state + threshold logic.

When several managed repos accumulate ESCALATE-class cancellation causes
within a short sliding window, the failure is almost always systemic
(rate-limit exhaustion, GitHub API outage, network partition, daemon
bug). Continuing to dispatch new tasks across the rest of the fleet
just burns operator budget on work that is likely to fail in the same
way. ``check_cascade_escalate_state`` is the boolean gate the main
daemon loop consults before dispatching: when ``True`` the loop skips
the per-repo cycle for that tick.

The detector reads ``cancellation_index:*`` zsets that record per-repo
cause timestamps (written by ``src.cancellation.storage``) and counts
distinct repos whose most recent indexed entry within
``cascade_escalate_window_min`` carries an ESCALATE-class cause. When
that count crosses ``cascade_escalate_threshold``, panic state is
persisted to the ``daemon:panic_state`` key. Auto-resume after
``cascade_escalate_auto_resume_min`` clears the key so the daemon
recovers without operator intervention once the spike subsides.

Operator-visible surfacing (dashboard banner, Resume button, webhook)
ships in PR-308b — this module is the detector core only.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from src.cancellation.storage import (
    CancellationCause,
    cause_key,
    index_key,
)
from src.config import AppConfig
from src.keyspace import daemon_panic_state

_INDEX_PREFIX = index_key("")


def _is_escalate_cause(cause: CancellationCause) -> bool:
    """Return whether ``cause`` falls in the legacy ESCALATE class.

    Matches the PR-315 vocabulary preserved on migrated records: an
    ESCALATE-class cause has either ``payload.legacy_category ==
    "ESCALATE"`` (from the startup migration) or
    ``payload.subsource`` in the canonical set
    (``coder_escalate``, ``guardrail``) that mapped to legacy
    ``ESCALATE`` before the category collapse.
    """
    payload = cause.payload if isinstance(cause.payload, dict) else None
    if payload is None:
        return False
    if payload.get("legacy_category") == "ESCALATE":
        return True
    return payload.get("subsource") in ("coder_escalate", "guardrail")


def _decode(value: Any) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else value


async def _read_panic_state(redis_client: Any) -> dict[str, Any] | None:
    raw = await redis_client.get(daemon_panic_state())
    if raw is None:
        return None
    try:
        parsed = json.loads(_decode(raw))
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


async def _count_affected_repos(
    redis_client: Any, cutoff_ts: float
) -> list[str]:
    affected: list[str] = []
    keys = redis_client.scan_iter(match=f"{_INDEX_PREFIX}*")
    async for raw_key in keys:
        index_full_key = _decode(raw_key)
        repo_slug = index_full_key[len(_INDEX_PREFIX):]
        if not repo_slug:
            continue
        task_ids = await redis_client.zrangebyscore(
            index_full_key, cutoff_ts, "+inf"
        )
        for tid in task_ids or []:
            tid = _decode(tid)
            raw_cause = await redis_client.get(cause_key(repo_slug, tid))
            if raw_cause is None:
                continue
            try:
                cause = CancellationCause.from_redis(raw_cause)
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if _is_escalate_cause(cause):
                affected.append(repo_slug)
                break
    return affected


async def check_cascade_escalate_state(
    redis_client: Any,
    app_config: AppConfig,
    log: Any,
) -> bool:
    """Return whether daemon-wide PANIC is active for this poll tick.

    Implements PR-308a:
    * Count distinct repos whose ``cancellation_index:{repo}`` zset
      contains at least one ESCALATE-class cause indexed within the
      configured window.
    * If the count meets or exceeds ``cascade_escalate_threshold``,
      persist (or refresh) the JSON panic record at ``daemon:panic_state``
      and return ``True``.
    * If the count is below threshold but an existing panic record has
      aged past ``cascade_escalate_auto_resume_min``, delete the record
      and return ``False`` (auto-resume).
    * Otherwise, honor the existing panic record's ``enabled`` flag so
      a brief sub-threshold dip during a real outage does not flap.
    """
    cfg = app_config.daemon
    threshold = cfg.cascade_escalate_threshold
    if threshold <= 0:
        return False

    now = datetime.now(timezone.utc)
    cutoff_ts = now.timestamp() - cfg.cascade_escalate_window_min * 60
    affected = await _count_affected_repos(redis_client, cutoff_ts)
    unique_affected = sorted(set(affected))

    existing = await _read_panic_state(redis_client)
    if len(unique_affected) >= threshold:
        triggered_at = now.isoformat()
        if existing is not None and existing.get("enabled"):
            prior = existing.get("triggered_at")
            if isinstance(prior, str):
                triggered_at = prior
        payload = {
            "enabled": True,
            "reason": "cascade_escalate_threshold_exceeded",
            "triggered_at": triggered_at,
            "affected_repos": unique_affected,
            "threshold_at_trigger": threshold,
        }
        await redis_client.set(daemon_panic_state(), json.dumps(payload))
        log.warning(
            "[PANIC] cascade ESCALATE threshold hit: %d repos affected",
            len(unique_affected),
        )
        return True

    if existing is None or not existing.get("enabled"):
        return False

    auto_resume_min = cfg.cascade_escalate_auto_resume_min
    if auto_resume_min <= 0:
        return True
    triggered_raw = existing.get("triggered_at")
    try:
        triggered_at_dt = datetime.fromisoformat(str(triggered_raw))
    except (TypeError, ValueError):
        return True
    if triggered_at_dt.tzinfo is None:
        triggered_at_dt = triggered_at_dt.replace(tzinfo=timezone.utc)
    elapsed_sec = (now - triggered_at_dt).total_seconds()
    if elapsed_sec >= auto_resume_min * 60:
        await redis_client.delete(daemon_panic_state())
        log.info("[PANIC] auto-resume after %.0fs cooldown", elapsed_sec)
        return False
    return True
