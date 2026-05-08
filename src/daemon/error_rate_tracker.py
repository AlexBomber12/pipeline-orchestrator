"""Redis-backed ERROR-rate window tracking for repo auto-pause policy."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

STALE_AFTER = timedelta(hours=24)


def key(repo_slug: str) -> str:
    return f"error_rate:{repo_slug}"


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
    await prune(redis_client, repo_slug)


async def prune(
    redis_client: Any,
    repo_slug: str,
    *,
    now: datetime | float | int | None = None,
) -> int:
    """Remove entries older than 24 hours to keep the sorted set bounded."""
    cutoff = _timestamp(now) - STALE_AFTER.total_seconds()
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
    await prune(redis_client, repo_slug, now=now_ts)
    since = now_ts - (window_min * 60)
    return int(await redis_client.zcount(key(repo_slug), since, "+inf"))
