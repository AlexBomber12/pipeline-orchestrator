"""Cancellation cause storage - Redis schema for OBS-BE expanded.

The cancellation policy substrate. Detection paths (PR-253) write
through these helpers; UI (PR-254) reads through them. Centralizing
the schema here makes the contract bisectable from its consumers.
PR-252.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

CATEGORIES = (
    "CRASH",
    "ESCALATE",
    "TIMEOUT",
    "INFRA",
    "OPERATOR_RECOVERY",
    "NO_PUSH_DEADLOCK",
)

TTL_SECONDS = 30 * 24 * 3600


@dataclass
class CancellationCause:
    """Structured cancellation reason persisted per task."""

    category: str
    payload: dict[str, Any] = field(default_factory=dict)
    created_at: str = ""
    task_id: str = ""
    repo_slug: str = ""

    def to_redis(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))

    @classmethod
    def from_redis(cls, raw: str | bytes) -> "CancellationCause":
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return cls(**json.loads(raw))


def cause_key(repo_slug: str, task_id: str) -> str:
    return f"cancellation:{repo_slug}:{task_id}"


def index_key(repo_slug: str) -> str:
    return f"cancellation_index:{repo_slug}"


async def record_cancellation_cause(
    redis_client: Any,
    repo_slug: str,
    task_id: str,
    cause: CancellationCause,
) -> None:
    """Persist a cancellation cause for later UI surfacing."""
    if not cause.created_at:
        cause.created_at = datetime.now(timezone.utc).isoformat()
    cause.task_id = task_id
    cause.repo_slug = repo_slug
    serialized = cause.to_redis()
    score = datetime.fromisoformat(cause.created_at).timestamp()
    expiry_cutoff = datetime.now(timezone.utc).timestamp() - TTL_SECONDS
    pipe = redis_client.pipeline()
    pipe.set(cause_key(repo_slug, task_id), serialized, ex=TTL_SECONDS)
    pipe.zadd(index_key(repo_slug), {task_id: score})
    pipe.zremrangebyscore(index_key(repo_slug), "-inf", f"({expiry_cutoff}")
    pipe.expire(index_key(repo_slug), TTL_SECONDS)
    await pipe.execute()


async def get_cancellation_cause(
    redis_client: Any,
    repo_slug: str,
    task_id: str,
) -> CancellationCause | None:
    raw = await redis_client.get(cause_key(repo_slug, task_id))
    if raw is None:
        return None
    return CancellationCause.from_redis(raw)


async def list_recent_cancellations(
    redis_client: Any,
    repo_slug: str,
    since: datetime,
) -> list[CancellationCause]:
    """Return causes recorded for repo at or after ``since``, newest first."""
    since_ts = since.timestamp()
    task_ids = await redis_client.zrangebyscore(
        index_key(repo_slug), since_ts, "+inf"
    )
    causes: list[CancellationCause] = []
    stale: list[str] = []
    for tid in task_ids or []:
        if isinstance(tid, bytes):
            tid = tid.decode("utf-8")
        cause = await get_cancellation_cause(redis_client, repo_slug, tid)
        if cause is None:
            stale.append(tid)
        else:
            causes.append(cause)
    if stale:
        await redis_client.zrem(index_key(repo_slug), *stale)
    causes.sort(key=lambda c: datetime.fromisoformat(c.created_at), reverse=True)
    return causes
