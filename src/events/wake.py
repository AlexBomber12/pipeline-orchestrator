"""Redis pub/sub helpers for waking the daemon out of its IDLE sleep.

The dashboard publishes on a per-repo wake channel after a user-visible
mutation (e.g. uploading a tasks zip) so the daemon can react within
1-2 seconds instead of waiting for ``poll_interval_sec`` to elapse.
The channel naming convention lives here so both publisher and subscriber
agree on it without importing each other.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Iterable

import redis.asyncio as aioredis

DEFAULT_REDIS_URL = "redis://localhost:6379/0"


def wake_channel(repo_name: str) -> str:
    """Return the Redis pub/sub channel for ``repo_name`` wake events."""
    return f"orchestrator:wake:{repo_name}"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


async def publish_wake(
    repo_name: str,
    event_type: str = "upload",
    redis_client: Any | None = None,
) -> None:
    """Publish a wake message for ``repo_name`` on the orchestrator channel.

    ``event_type`` is included in the payload so future subscribers can
    differentiate signal sources without a channel-name change.
    """
    owns_client = redis_client is None
    client = redis_client or aioredis.from_url(
        os.environ.get("REDIS_URL", DEFAULT_REDIS_URL),
        decode_responses=True,
    )
    message = json.dumps(
        {
            "event_type": event_type,
            "repo": repo_name,
            "timestamp": _utc_now_iso(),
        }
    )
    try:
        await client.publish(wake_channel(repo_name), message)
    finally:
        if owns_client:
            await client.aclose()


async def subscribe_wake(
    repo_names: Iterable[str],
    redis_client: Any | None = None,
) -> Any:
    """Return a Redis pubsub object subscribed to wake channels for ``repo_names``.

    The caller is responsible for closing both the pubsub object and (when
    ``redis_client`` was created here) the underlying client.
    """
    client = redis_client or aioredis.from_url(
        os.environ.get("REDIS_URL", DEFAULT_REDIS_URL),
        decode_responses=True,
    )
    pubsub = client.pubsub()
    channels = [wake_channel(name) for name in repo_names]
    if channels:
        await pubsub.subscribe(*channels)
    return pubsub
