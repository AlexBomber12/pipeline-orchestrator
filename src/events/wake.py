"""Daemon wake-up triggers via Redis Pub/Sub.

The web layer publishes a small message on ``orchestrator:wake:{repo}``
after a successful task upload. The daemon main loop subscribes on every
configured repo and uses the message as a signal to short-circuit its
sleep so that the next ``run_cycle`` runs immediately. The payload is
deliberately tiny — the daemon only needs to learn *which* repo to wake;
the rest of the cycle work is unchanged.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterable

CHANNEL_PREFIX = "orchestrator:wake"


def _channel_name(repo_name: str) -> str:
    return f"{CHANNEL_PREFIX}:{repo_name}"


def _isoformat_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def build_wake_message(
    repo_name: str,
    event_type: str,
    *,
    now: datetime | None = None,
) -> str:
    """Return the canonical wake-trigger JSON payload."""
    moment = now or datetime.now(timezone.utc)
    return json.dumps(
        {
            "event_type": event_type,
            "repo": repo_name,
            "timestamp": _isoformat_z(moment),
        }
    )


async def publish_wake(
    redis_client: Any,
    repo_name: str,
    event_type: str,
) -> None:
    """Publish a wake-up trigger for ``repo_name`` on the wake channel."""
    message = build_wake_message(repo_name, event_type)
    await redis_client.publish(_channel_name(repo_name), message)


async def subscribe_wake(redis_client: Any, repo_names: Iterable[str]) -> Any:
    """Subscribe to wake channels for ``repo_names`` and return the pubsub.

    Caller is responsible for calling ``aclose()``/``unsubscribe()`` on the
    returned pubsub object. Returns ``None`` if subscription fails so the
    daemon can fall back to a pure sleep without crashing.
    """
    channels = [_channel_name(name) for name in repo_names]
    if not channels:
        return None
    try:
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(*channels)
    except Exception:
        return None
    return pubsub


def repo_from_channel(channel: str) -> str | None:
    """Return the repo slug encoded in a wake channel name, if any."""
    prefix = f"{CHANNEL_PREFIX}:"
    if not channel.startswith(prefix):
        return None
    suffix = channel[len(prefix):]
    return suffix or None
