"""Redis-backed ERROR-rate window tracking for repo auto-pause policy."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

STALE_AFTER = timedelta(hours=24)


def key(repo_slug: str) -> str:
    return f"error_rate:{repo_slug}"


def last_auto_pause_key(repo_slug: str) -> str:
    return f"error_rate_last_auto_pause:{repo_slug}"


def _timestamp(ts: datetime | float | int | None = None) -> float:
    if ts is None:
        return datetime.now(timezone.utc).timestamp()
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.timestamp()
    return float(ts)


async def record(
    redis_client: Any,
    repo_slug: str,
    ts: datetime | float | int | None = None,
) -> None:
    """Record one task cancellation/error event for ``repo_slug``."""
    score = _timestamp(ts)
    member = f"{score:.6f}:{uuid.uuid4().hex}"
    await redis_client.zadd(key(repo_slug), {member: score})


async def prune(
    redis_client: Any,
    repo_slug: str,
    *,
    retain_for: timedelta = STALE_AFTER,
    now: datetime | float | int | None = None,
) -> int:
    """Remove entries outside the retained history window."""
    cutoff = _timestamp(now) - retain_for.total_seconds()
    return int(await redis_client.zremrangebyscore(key(repo_slug), "-inf", cutoff))


async def count_recent(
    redis_client: Any,
    repo_slug: str,
    window_min: int,
    *,
    now: datetime | float | int | None = None,
) -> int:
    """Return ERROR records inside the trailing ``window_min`` minutes."""
    now_ts = _timestamp(now)
    await prune(
        redis_client,
        repo_slug,
        retain_for=timedelta(minutes=window_min),
        now=now_ts,
    )
    since = now_ts - (window_min * 60)
    return int(await redis_client.zcount(key(repo_slug), since, "+inf"))


async def mark_auto_pause(
    redis_client: Any,
    repo_slug: str,
    *,
    now: datetime | float | int | None = None,
) -> None:
    """Remember when ERROR-rate auto-pause last stopped this repo."""
    await redis_client.set(last_auto_pause_key(repo_slug), str(_timestamp(now)))


async def has_records_after_last_auto_pause(
    redis_client: Any,
    repo_slug: str,
) -> bool:
    """Return True when any ERROR event is newer than the last auto-pause."""
    raw = await redis_client.get(last_auto_pause_key(repo_slug))
    if raw is None:
        return True
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        last_pause = float(raw)
    except (TypeError, ValueError):
        return True
    return int(
        await redis_client.zcount(key(repo_slug), f"({last_pause}", "+inf")
    ) > 0
