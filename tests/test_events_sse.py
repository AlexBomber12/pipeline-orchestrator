from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError
from src.events import publisher
from src.events.sse import (
    INITIAL_BUFFER_DRAIN_LIMIT,
    RepoEventsUnavailableError,
    _is_disconnected,
    _message_timestamp,
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
        self.subscribe_error: Exception | None = None
        self.unsubscribe_error: Exception | None = None
        self.close_error: Exception | None = None

    async def subscribe(self, channel: str) -> None:
        if self.subscribe_error is not None:
            raise self.subscribe_error
        self.channels.add(channel)
        self.subscribed.set()

    async def unsubscribe(self, channel: str) -> None:
        if self.unsubscribe_error is not None:
            raise self.unsubscribe_error
        self.unsubscribed.append(channel)
        self.channels.discard(channel)

    async def get_message(
        self,
        *,
        ignore_subscribe_messages: bool = True,
        timeout: float = 0.0,
    ) -> dict[str, Any] | None:
        if timeout <= 0:
            try:
                return self.messages.get_nowait()
            except asyncio.QueueEmpty:
                return None
        try:
            return await asyncio.wait_for(self.messages.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    async def aclose(self) -> None:
        if self.close_error is not None:
            self.closed = True
            raise self.close_error
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


async def _wait_for_pubsub(redis: _FakeRedis) -> _FakePubSub:
    while not redis.pubsubs:
        await asyncio.sleep(0)
    await redis.pubsubs[0].subscribed.wait()
    return redis.pubsubs[0]


def test_format_sse_helpers() -> None:
    event = format_sse_event('{"type":"progress_updated","repo":"alpha","data":{}}')
    default_event = format_sse_event('{"repo":"alpha","data":{}}')
    invalid_event = format_sse_event("not-json")
    scalar_event = format_sse_event('"not-an-object"')

    assert event == b'event: progress_updated\ndata: {"type":"progress_updated","repo":"alpha","data":{}}\n\n'
    assert default_event == b'event: message\ndata: {"repo":"alpha","data":{}}\n\n'
    assert invalid_event == b":invalid event payload\n\n"
    assert scalar_event == b":invalid event payload\n\n"
    assert format_sse_comment("keepalive") == b":keepalive\n\n"


def test_message_timestamp_rejects_malformed_payloads() -> None:
    assert _message_timestamp('"not-an-object"') is None
    assert _message_timestamp('{"type":"progress_updated"}') is None
    assert _message_timestamp('{"timestamp":123}') is None
    assert _message_timestamp('{"timestamp":"not-a-timestamp"}') is None


def test_message_timestamp_normalizes_naive_values_to_utc() -> None:
    assert _message_timestamp('{"timestamp":"2026-04-20T15:00:00"}') == datetime(
        2026,
        4,
        20,
        15,
        0,
        0,
        tzinfo=timezone.utc,
    )


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
    )
    stream = await stream

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
    )
    stream = await stream

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


async def test_stream_repo_events_sanitizes_invalid_history_and_live_messages() -> None:
    redis = _FakeRedis()
    request = _Request()
    valid_message = json.dumps(
        publisher.build_repo_event("example__repo", "progress_updated", {"percent": 50})
    )
    redis.lists["repo-events-history:example__repo"] = ["not-json"]

    stream = await stream_repo_events(
        redis,
        "example__repo",
        request,
        keepalive_interval=60.0,
        poll_interval=0.01,
    )

    assert await anext(stream) == b":invalid event payload\n\n"

    next_message = asyncio.create_task(anext(stream))
    pubsub = await _wait_for_pubsub(redis)
    await pubsub.messages.put({"data": b"not-json"})
    assert await next_message == b":invalid event payload\n\n"

    next_message = asyncio.create_task(anext(stream))
    await pubsub.messages.put({"data": valid_message.encode("utf-8")})
    assert await next_message == format_sse_event(valid_message)

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

    frames = [frame async for frame in await stream_repo_events(redis, "example__repo", request)]

    assert frames == []
    assert len(redis.pubsubs) == 1
    pubsub = redis.pubsubs[0]
    assert pubsub.unsubscribed == ["repo-events:example__repo"]
    assert pubsub.closed is True


async def test_stream_repo_events_returns_keepalive_without_extra_idle_sleep() -> None:
    redis = _FakeRedis()
    request = _Request()

    stream = stream_repo_events(
        redis,
        "example__repo",
        request,
        keepalive_interval=0.0,
        poll_interval=0.01,
    )
    stream = await stream

    assert await anext(stream) == b":keepalive\n\n"

    request.disconnected = True
    try:
        await anext(stream)
    except StopAsyncIteration:
        pass
    else:
        raise AssertionError("stream should stop after disconnect")

    pubsub = redis.pubsubs[0]
    assert pubsub.unsubscribed == ["repo-events:example__repo"]
    assert pubsub.closed is True


async def test_stream_repo_events_forwards_buffered_live_message_after_history_replay() -> None:
    redis = _FakeRedis()
    request = _Request()
    buffered_message = json.dumps(
        publisher.build_repo_event("example__repo", "progress_updated", {"percent": 10})
    )

    stream = await stream_repo_events(
        redis,
        "example__repo",
        request,
        keepalive_interval=0.0,
        poll_interval=0.01,
    )
    pubsub = await _wait_for_pubsub(redis)
    await pubsub.messages.put({"data": buffered_message.encode("utf-8")})

    assert await anext(stream) == format_sse_event(buffered_message)
    assert await anext(stream) == b":keepalive\n\n"

    request.disconnected = True
    try:
        await anext(stream)
    except StopAsyncIteration:
        pass
    else:
        raise AssertionError("stream should stop after disconnect")


async def test_stream_repo_events_subscribes_before_history_without_replaying_duplicates() -> None:
    redis = _FakeRedis()
    request = _Request()
    old_message = json.dumps(
        publisher.build_repo_event("example__repo", "state_changed", {"state": "IDLE"})
    )
    raced_message = json.dumps(
        publisher.build_repo_event("example__repo", "progress_updated", {"percent": 10})
    )
    redis.lists["repo-events-history:example__repo"] = [old_message]

    async def _racing_lrange(key: str, start: int, stop: int) -> list[str]:
        redis.lists[key].insert(0, raced_message)
        await redis.publish("repo-events:example__repo", raced_message)
        values = redis.lists.get(key, [])
        return values[start : stop + 1]

    redis.lrange = _racing_lrange  # type: ignore[method-assign]

    stream = await stream_repo_events(
        redis,
        "example__repo",
        request,
        keepalive_interval=0.0,
        poll_interval=0.01,
    )

    assert await anext(stream) == format_sse_event(old_message)
    assert await anext(stream) == format_sse_event(raced_message)
    assert await anext(stream) == b":keepalive\n\n"

    request.disconnected = True
    try:
        await anext(stream)
    except StopAsyncIteration:
        pass
    else:
        raise AssertionError("stream should stop after disconnect")


async def test_stream_repo_events_orders_buffered_and_history_messages_by_timestamp() -> None:
    redis = _FakeRedis()
    request = _Request()
    base = datetime(2026, 4, 20, 15, 0, tzinfo=timezone.utc)
    buffered_older = json.dumps(
        publisher.build_repo_event(
            "example__repo",
            "progress_updated",
            {"percent": 10},
            now=base,
        )
    )
    newer_history = json.dumps(
        publisher.build_repo_event(
            "example__repo",
            "progress_updated",
            {"percent": 20},
            now=base + timedelta(seconds=1),
        )
    )
    newest_history = json.dumps(
        publisher.build_repo_event(
            "example__repo",
            "progress_updated",
            {"percent": 30},
            now=base + timedelta(seconds=2),
        )
    )

    async def _racing_lrange(key: str, start: int, stop: int) -> list[str]:
        await redis.publish("repo-events:example__repo", buffered_older)
        return [newest_history, newer_history]

    redis.lrange = _racing_lrange  # type: ignore[method-assign]

    stream = await stream_repo_events(
        redis,
        "example__repo",
        request,
        history_limit=2,
        keepalive_interval=0.0,
        poll_interval=0.01,
    )

    assert await anext(stream) == format_sse_event(buffered_older)
    assert await anext(stream) == format_sse_event(newer_history)
    assert await anext(stream) == format_sse_event(newest_history)
    assert await anext(stream) == b":keepalive\n\n"

    request.disconnected = True
    try:
        await anext(stream)
    except StopAsyncIteration:
        pass
    else:
        raise AssertionError("stream should stop after disconnect")


async def test_stream_repo_events_orders_mixed_naive_and_aware_replay_timestamps() -> None:
    redis = _FakeRedis()
    request = _Request()
    naive_history = json.dumps(
        {
            "type": "progress_updated",
            "repo": "example__repo",
            "data": {"percent": 10},
            "timestamp": "2026-04-20T15:00:00",
        }
    )
    aware_history = json.dumps(
        {
            "type": "progress_updated",
            "repo": "example__repo",
            "data": {"percent": 30},
            "timestamp": "2026-04-20T15:00:02Z",
        }
    )
    buffered_aware = json.dumps(
        {
            "type": "progress_updated",
            "repo": "example__repo",
            "data": {"percent": 20},
            "timestamp": "2026-04-20T15:00:01Z",
        }
    )

    async def _racing_lrange(key: str, start: int, stop: int) -> list[str]:
        await redis.publish("repo-events:example__repo", buffered_aware)
        return [aware_history, naive_history]

    redis.lrange = _racing_lrange  # type: ignore[method-assign]

    stream = await stream_repo_events(
        redis,
        "example__repo",
        request,
        history_limit=2,
        keepalive_interval=0.0,
        poll_interval=0.01,
    )

    assert await anext(stream) == format_sse_event(naive_history)
    assert await anext(stream) == format_sse_event(buffered_aware)
    assert await anext(stream) == format_sse_event(aware_history)
    assert await anext(stream) == b":keepalive\n\n"

    request.disconnected = True
    try:
        await anext(stream)
    except StopAsyncIteration:
        pass
    else:
        raise AssertionError("stream should stop after disconnect")


async def test_stream_repo_events_stops_before_buffered_messages_when_disconnected() -> None:
    redis = _FakeRedis()
    request = _Request()
    buffered_message = json.dumps(
        publisher.build_repo_event("example__repo", "progress_updated", {"percent": 10})
    )

    stream = await stream_repo_events(
        redis,
        "example__repo",
        request,
        keepalive_interval=0.0,
        poll_interval=0.01,
    )
    pubsub = await _wait_for_pubsub(redis)
    await pubsub.messages.put({"data": buffered_message.encode("utf-8")})
    request.disconnected = True

    frames = [frame async for frame in stream]

    assert frames == []
    assert pubsub.unsubscribed == ["repo-events:example__repo"]
    assert pubsub.closed is True


async def test_stream_repo_events_bounds_initial_buffer_drain_under_load() -> None:
    request = _Request()

    class _BusyPubSub(_FakePubSub):
        def __init__(self) -> None:
            super().__init__()
            self.index = 0

        async def get_message(
            self,
            *,
            ignore_subscribe_messages: bool = True,
            timeout: float = 0.0,
        ) -> dict[str, Any] | None:
            if timeout <= 0:
                payload = json.dumps(
                    publisher.build_repo_event(
                        "example__repo",
                        "progress_updated",
                        {"percent": self.index},
                    )
                )
                self.index += 1
                return {"data": payload}
            return None

    class _BusyRedis(_FakeRedis):
        def __init__(self) -> None:
            super().__init__()
            self.busy_pubsub = _BusyPubSub()

        def pubsub(self) -> _BusyPubSub:
            self.pubsubs.append(self.busy_pubsub)
            return self.busy_pubsub

    redis = _BusyRedis()
    stream = await stream_repo_events(
        redis,
        "example__repo",
        request,
        keepalive_interval=0.0,
        poll_interval=0.01,
    )

    first_frame = await asyncio.wait_for(anext(stream), timeout=0.1)

    assert b'"percent": 0' in first_frame
    assert redis.busy_pubsub.index == INITIAL_BUFFER_DRAIN_LIMIT

    request.disconnected = True
    try:
        await anext(stream)
    except StopAsyncIteration:
        pass
    else:
        raise AssertionError("stream should stop after disconnect")


async def test_stream_repo_events_raises_unavailable_when_history_fetch_fails() -> None:
    redis = _FakeRedis()
    request = _Request()

    async def _failing_lrange(key: str, start: int, stop: int) -> list[str]:
        raise ConnectionError("redis down")

    redis.lrange = _failing_lrange  # type: ignore[method-assign]

    try:
        await stream_repo_events(redis, "example__repo", request)
    except RepoEventsUnavailableError as exc:
        assert str(exc) == "Redis unavailable"
    else:
        raise AssertionError("stream setup should fail when Redis history fetch fails")


async def test_stream_repo_events_closes_pubsub_when_unsubscribe_fails() -> None:
    redis = _FakeRedis()
    request = _Request()
    stream = await stream_repo_events(
        redis,
        "example__repo",
        request,
        keepalive_interval=0.0,
        poll_interval=0.01,
    )
    pubsub = await _wait_for_pubsub(redis)
    pubsub.unsubscribe_error = ConnectionError("redis down")
    request.disconnected = True

    try:
        await anext(stream)
    except StopAsyncIteration:
        pass
    else:
        raise AssertionError("stream should stop after disconnect")

    assert pubsub.closed is True


async def test_stream_repo_events_ignores_close_errors_during_disconnect_teardown() -> None:
    redis = _FakeRedis()
    request = _Request()
    stream = await stream_repo_events(
        redis,
        "example__repo",
        request,
        keepalive_interval=0.0,
        poll_interval=0.01,
    )
    pubsub = await _wait_for_pubsub(redis)
    pubsub.close_error = ConnectionError("close failed")
    request.disconnected = True

    try:
        await anext(stream)
    except StopAsyncIteration:
        pass
    else:
        raise AssertionError("stream should stop after disconnect")

    assert pubsub.closed is True


async def test_stream_repo_events_stops_cleanly_when_live_reads_fail() -> None:
    redis = _FakeRedis()
    request = _Request()
    stream = await stream_repo_events(
        redis,
        "example__repo",
        request,
        keepalive_interval=60.0,
        poll_interval=0.01,
    )
    pubsub = await _wait_for_pubsub(redis)

    async def _failing_get_message(
        *,
        ignore_subscribe_messages: bool = True,
        timeout: float = 0.0,
    ) -> dict[str, Any] | None:
        if timeout > 0:
            raise ConnectionError("redis down")
        return None

    pubsub.get_message = _failing_get_message  # type: ignore[method-assign]

    try:
        await anext(stream)
    except StopAsyncIteration:
        pass
    else:
        raise AssertionError("stream should stop when live Redis reads fail")

    assert pubsub.unsubscribed == ["repo-events:example__repo"]
    assert pubsub.closed is True


async def test_stream_repo_events_closes_pubsub_when_subscribe_fails() -> None:
    redis = _FakeRedis()
    request = _Request()
    pubsub = _FakePubSub()
    pubsub.subscribe_error = ConnectionError("redis down")

    def _pubsub() -> _FakePubSub:
        return pubsub

    redis.pubsub = _pubsub  # type: ignore[method-assign]

    try:
        await stream_repo_events(redis, "example__repo", request)
    except RepoEventsUnavailableError as exc:
        assert str(exc) == "Redis unavailable"
    else:
        raise AssertionError("stream setup should fail when Redis subscribe fails")

    assert pubsub.closed is True


async def test_stream_repo_events_ignores_close_errors_after_subscribe_failure() -> None:
    redis = _FakeRedis()
    request = _Request()
    pubsub = _FakePubSub()
    pubsub.subscribe_error = ConnectionError("redis down")
    pubsub.close_error = ConnectionError("close failed")

    def _pubsub() -> _FakePubSub:
        return pubsub

    redis.pubsub = _pubsub  # type: ignore[method-assign]

    try:
        await stream_repo_events(redis, "example__repo", request)
    except RepoEventsUnavailableError as exc:
        assert str(exc) == "Redis unavailable"
    else:
        raise AssertionError("stream setup should fail when Redis subscribe fails")

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
        async def _stream():
            yield b":keepalive\n\n"

        return _stream()

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


def test_api_repo_events_route_returns_503_when_stream_setup_fails(monkeypatch) -> None:
    redis = object()

    class _StubAioredis:
        @staticmethod
        def from_url(url: str, decode_responses: bool = True) -> object:
            return redis

    async def _failing_stream(
        redis_client: object,
        repo_name: str,
        request: object,
    ) -> Any:
        raise RepoEventsUnavailableError("Redis unavailable")

    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())
    monkeypatch.setattr(web_app, "stream_repo_events", _failing_stream)

    with TestClient(app) as client:
        response = client.get("/api/repos/example__repo/events")

    assert response.status_code == 503
    assert response.text == "Redis unavailable"
