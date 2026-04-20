"""Server-Sent Events helpers backed by Redis Pub/Sub."""

from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncIterator, Awaitable, Callable

KEEPALIVE_INTERVAL_SECONDS = 15.0
HISTORY_REPLAY_LIMIT = 20
POLL_INTERVAL_SECONDS = 0.1


def _channel_name(repo_name: str) -> str:
    return f"repo-events:{repo_name}"


def _history_name(repo_name: str) -> str:
    return f"repo-events-history:{repo_name}"


def format_sse_event(message: str) -> bytes:
    """Serialize a JSON event message into SSE wire format."""
    event = json.loads(message)
    event_type = event.get("type", "message")
    return f"event: {event_type}\ndata: {message}\n\n".encode("utf-8")


def format_sse_comment(comment: str) -> bytes:
    """Serialize an SSE comment frame."""
    return f":{comment}\n\n".encode("utf-8")


async def _is_disconnected(request: Any) -> bool:
    checker = getattr(request, "is_disconnected", None)
    if checker is None:
        return False
    result = checker()
    if asyncio.iscoroutine(result):
        return bool(await result)
    return bool(result)


async def stream_repo_events(
    redis_client: Any,
    repo_name: str,
    request: Any,
    *,
    history_limit: int = HISTORY_REPLAY_LIMIT,
    keepalive_interval: float = KEEPALIVE_INTERVAL_SECONDS,
    poll_interval: float = POLL_INTERVAL_SECONDS,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> AsyncIterator[bytes]:
    """Yield SSE frames for repo history replay and live Redis Pub/Sub."""
    history = await redis_client.lrange(_history_name(repo_name), 0, history_limit - 1)
    for message in reversed(history):
        if await _is_disconnected(request):
            return
        yield format_sse_event(message)

    pubsub = redis_client.pubsub()
    await pubsub.subscribe(_channel_name(repo_name))
    last_keepalive = asyncio.get_running_loop().time()

    try:
        while True:
            if await _is_disconnected(request):
                return

            message = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=poll_interval,
            )
            if message is not None:
                data = message.get("data")
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                if isinstance(data, str):
                    yield format_sse_event(data)
                    last_keepalive = asyncio.get_running_loop().time()
                    continue

            now = asyncio.get_running_loop().time()
            if now - last_keepalive >= keepalive_interval:
                yield format_sse_comment("keepalive")
                last_keepalive = now
                continue

            await sleep(poll_interval)
    finally:
        await pubsub.unsubscribe(_channel_name(repo_name))
        await pubsub.aclose()
