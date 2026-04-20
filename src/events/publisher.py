"""Publish repo-scoped dashboard events through Redis."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as aioredis

DEFAULT_REDIS_URL = "redis://localhost:6379/0"
EVENT_HISTORY_LIMIT = 50


def _channel_name(repo_name: str) -> str:
    return f"repo-events:{repo_name}"


def _history_name(repo_name: str) -> str:
    return f"repo-events-history:{repo_name}"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def build_repo_event(
    repo_name: str,
    event_type: str,
    payload: dict[str, Any],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return the canonical repo event payload."""
    return {
        "type": event_type,
        "repo": repo_name,
        "data": payload,
        "timestamp": _isoformat_z(now or _utc_now()),
    }


async def publish_repo_event(
    repo_name: str,
    event_type: str,
    payload: dict[str, Any],
    redis_client: Any | None = None,
) -> None:
    """Publish a repo event and retain recent history for reconnects."""
    owns_client = redis_client is None
    client = redis_client or aioredis.from_url(
        os.environ.get("REDIS_URL", DEFAULT_REDIS_URL),
        decode_responses=True,
    )
    message = json.dumps(build_repo_event(repo_name, event_type, payload))
    try:
        await client.lpush(_history_name(repo_name), message)
        await client.ltrim(_history_name(repo_name), 0, EVENT_HISTORY_LIMIT - 1)
        await client.publish(_channel_name(repo_name), message)
    finally:
        if owns_client:
            await client.aclose()
