from __future__ import annotations

import asyncio
import json
from typing import Any

from fastapi.testclient import TestClient
from src.events import publisher
from src.events.sse import (
    _is_disconnected,
    format_sse_comment,
    format_sse_event,
    stream_repo_events,
)
from src.web import app as web_app
from src.web.app import app


class _FakePubSub:
    def __init__(self) -> None:
        self.channels: set[str] = set()
        self.messages: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.subscribed = asyncio.Event()
        self.unsubscribed: list[str] = []
        self.closed = False

    async def subscribe(self, channel: str) -> None:
        self.channels.add(channel)
        self.subscribed.set()

    async def unsubscribe(self, channel: str) -> None:
        self.unsubscribed.append(channel)
        self.channels.discard(channel)

    async def get_message(
        self,
        *,
        ignore_subscribe_messages: bool = True,
        timeout: float = 0.0,
    ) -> dict[str, Any] | None:
        try:
            return await asyncio.wait_for(self.messages.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    async def aclose(self) -> None:
        self.closed = True


class _FakeRedis:
    def __init__(self) -> None:
        self.lists: dict[str, list[str]] = {}
        self.pubsubs: list[_FakePubSub] = []

    async def lrange(self, key: str, start: int, stop: int) -> list[str]:
        values = self.lists.get(key, [])
        return values[start : stop + 1]

    def pubsub(self) -> _FakePubSub:
        pubsub = _FakePubSub()
        self.pubsubs.append(pubsub)
        return pubsub

    async def publish(self, channel: str, message: str) -> int:
        for pubsub in self.pubsubs:
            if channel in pubsub.channels:
                await pubsub.messages.put({"data": message})
        return 1


class _Request:
    def __init__(self, *, disconnected: bool = False) -> None:
        self.disconnected = disconnected

    async def is_disconnected(self) -> bool:
        return self.disconnected


async def _no_sleep(_delay: float) -> None:
    return None


async def _wait_for_pubsub(redis: _FakeRedis) -> _FakePubSub:
    while not redis.pubsubs:
        await asyncio.sleep(0)
    await redis.pubsubs[0].subscribed.wait()
    return redis.pubsubs[0]


def test_format_sse_helpers() -> None:
    event = format_sse_event('{"type":"progress_updated","repo":"alpha","data":{}}')
    default_event = format_sse_event('{"repo":"alpha","data":{}}')

    assert event == b'event: progress_updated\ndata: {"type":"progress_updated","repo":"alpha","data":{}}\n\n'
    assert default_event == b'event: message\ndata: {"repo":"alpha","data":{}}\n\n'
    assert format_sse_comment("keepalive") == b":keepalive\n\n"


async def test_is_disconnected_defaults_to_false_and_supports_sync_checker() -> None:
    class _NoChecker:
        pass

    class _SyncChecker:
        @staticmethod
        def is_disconnected() -> bool:
            return True

    assert await _is_disconnected(_NoChecker()) is False
    assert await _is_disconnected(_SyncChecker()) is True


async def test_stream_repo_events_replays_history_then_forwards_live_messages() -> None:
    redis = _FakeRedis()
    first = json.dumps(
        publisher.build_repo_event("example__repo", "state_changed", {"state": "IDLE"})
    )
    second = json.dumps(
        publisher.build_repo_event("example__repo", "progress_updated", {"percent": 50})
    )
    redis.lists["repo-events-history:example__repo"] = [second, first]
    request = _Request()

    stream = stream_repo_events(
        redis,
        "example__repo",
        request,
        keepalive_interval=60.0,
        poll_interval=0.01,
        sleep=_no_sleep,
    )

    assert await anext(stream) == format_sse_event(first)
    assert await anext(stream) == format_sse_event(second)

    next_message = asyncio.create_task(anext(stream))
    pubsub = await _wait_for_pubsub(redis)
    await pubsub.messages.put({"data": second.encode("utf-8")})

    assert await next_message == format_sse_event(second)

    request.disconnected = True
    try:
        await anext(stream)
    except StopAsyncIteration:
        pass
    else:
        raise AssertionError("stream should stop after disconnect")

    assert pubsub.unsubscribed == ["repo-events:example__repo"]
    assert pubsub.closed is True


async def test_stream_repo_events_emits_keepalive_after_ignored_message() -> None:
    redis = _FakeRedis()
    request = _Request()
    stream = stream_repo_events(
        redis,
        "example__repo",
        request,
        keepalive_interval=0.0,
        poll_interval=0.01,
        sleep=_no_sleep,
    )

    next_message = asyncio.create_task(anext(stream))
    pubsub = await _wait_for_pubsub(redis)
    await pubsub.messages.put({"data": 123})

    assert await next_message == b":keepalive\n\n"

    request.disconnected = True
    try:
        await anext(stream)
    except StopAsyncIteration:
        pass
    else:
        raise AssertionError("stream should stop after disconnect")


async def test_stream_repo_events_stops_before_subscribing_when_already_disconnected() -> None:
    redis = _FakeRedis()
    message = json.dumps(
        publisher.build_repo_event("example__repo", "event_log_appended", {"line": "x"})
    )
    redis.lists["repo-events-history:example__repo"] = [message]
    request = _Request(disconnected=True)

    frames = [frame async for frame in stream_repo_events(redis, "example__repo", request)]

    assert frames == []
    assert redis.pubsubs == []


async def test_stream_repo_events_uses_sleep_when_idle() -> None:
    redis = _FakeRedis()
    request = _Request()
    sleep_calls: list[float] = []

    async def _record_sleep(delay: float) -> None:
        sleep_calls.append(delay)
        request.disconnected = True

    stream = stream_repo_events(
        redis,
        "example__repo",
        request,
        keepalive_interval=60.0,
        poll_interval=0.01,
        sleep=_record_sleep,
    )

    try:
        await anext(stream)
    except StopAsyncIteration:
        pass
    else:
        raise AssertionError("stream should stop after disconnect")

    assert sleep_calls == [0.01]
    pubsub = redis.pubsubs[0]
    assert pubsub.unsubscribed == ["repo-events:example__repo"]
    assert pubsub.closed is True


def test_api_repo_events_route_returns_sse_response(monkeypatch) -> None:
    redis = object()
    seen: dict[str, object] = {}

    class _StubAioredis:
        @staticmethod
        def from_url(url: str, decode_responses: bool = True) -> object:
            return redis

    async def _fake_stream(redis_client: object, repo_name: str, request: object):
        seen["redis_client"] = redis_client
        seen["repo_name"] = repo_name
        seen["request"] = request
        yield b":keepalive\n\n"

    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())
    monkeypatch.setattr(web_app, "stream_repo_events", _fake_stream)

    with TestClient(app) as client:
        response = client.get("/api/repos/example__repo/events")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.headers["cache-control"] == "no-cache"
    assert response.headers["x-accel-buffering"] == "no"
    assert response.content == b":keepalive\n\n"
    assert seen["redis_client"] is redis
    assert seen["repo_name"] == "example__repo"


def test_api_repo_events_route_returns_503_without_redis(monkeypatch) -> None:
    class _StubAioredis:
        @staticmethod
        def from_url(url: str, decode_responses: bool = True) -> None:
            return None

    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/api/repos/example__repo/events")

    assert response.status_code == 503
    assert response.text == "Redis unavailable"
