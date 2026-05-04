"""Tests for the Human Availability chip endpoints introduced in PR-256.

Three surfaces are exercised here:

* ``GET /api/availability`` — composed verdict + raw manual override
  the chip uses to drive its dot color and label independently.
* ``POST /api/availability/{state}`` — operator-driven manual override
  set/clear plus the wake publish that syncs other dashboard tabs.
* ``GET /api/availability/events`` — SSE wake stream listening on the
  ``orchestrator:availability:changed`` channel.

All tests bypass the lifespan-attached Redis client by injecting a fake
on ``app.state.redis`` (or by deliberately omitting it) so the routes can
be exercised without a real Redis or a live daemon.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from src.cancellation.availability import AvailabilityState
from src.web import app as web_app
from src.web.routes import dashboard as dashboard_routes


class _FakeRedis:
    """Minimal Redis double with the surface the availability routes use.

    ``get/set/delete`` back ``operator_override`` so :class:`ManualOverrideSource`
    composes the verdict the same way the daemon does. ``exists`` covers
    :class:`HeartbeatSource`. ``publish`` records the channel/message
    pair so tests can assert wake fan-out.
    """

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.heartbeat: bool = False
        self.published: list[tuple[str, str]] = []

    async def get(self, key: str):
        return self.store.get(key)

    async def set(self, key: str, value: str):
        self.store[key] = value

    async def delete(self, key: str):
        self.store.pop(key, None)

    async def exists(self, key: str) -> int:
        if key == "operator_heartbeat":
            return 1 if self.heartbeat else 0
        return 1 if key in self.store else 0

    async def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, message))
        return 1


@pytest.fixture
def availability_client(monkeypatch: pytest.MonkeyPatch):
    """TestClient with a fake Redis pinned to ``app.state.redis``."""
    redis = _FakeRedis()
    with TestClient(web_app.app) as client:
        monkeypatch.setattr(web_app.app.state, "redis", redis)
        yield client, redis


def test_get_returns_composed_state_and_manual_override(
    availability_client,
) -> None:
    client, redis = availability_client
    redis.store["operator_override"] = "AVAILABLE"

    resp = client.get("/api/availability")

    assert resp.status_code == 200
    body = resp.json()
    # Manual override pinned explicitly, so composed must match the override
    # and the raw label exposes "AVAILABLE" so the chip cycles correctly.
    assert body == {"composed_state": "AVAILABLE", "manual_override": "AVAILABLE"}


def test_get_reports_auto_when_no_override(
    availability_client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without ``operator_override``, the chip label collapses to AUTO and
    the composed state derives from the remaining sources."""
    client, _redis = availability_client

    async def fake_compose(sources):
        return AvailabilityState.AWAY

    monkeypatch.setattr(dashboard_routes, "is_operator_available", fake_compose)

    resp = client.get("/api/availability")

    assert resp.status_code == 200
    assert resp.json() == {"composed_state": "AWAY", "manual_override": "AUTO"}


def test_get_decodes_bytes_override(availability_client) -> None:
    """Redis clients with ``decode_responses=False`` return bytes; the
    helper must decode them so the label renders correctly."""
    client, redis = availability_client
    redis.store["operator_override"] = b"AWAY"  # type: ignore[assignment]

    resp = client.get("/api/availability")

    assert resp.status_code == 200
    assert resp.json()["manual_override"] == "AWAY"


def test_get_collapses_to_auto_on_redis_error(
    availability_client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A Redis read error during the override probe must not 5xx the
    chip — it falls back to AUTO so the composed state is still served."""
    client, redis = availability_client

    async def boom(key: str):
        raise RuntimeError("redis unreachable")

    monkeypatch.setattr(redis, "get", boom)

    async def fake_compose(sources):
        return AvailabilityState.AVAILABLE

    monkeypatch.setattr(dashboard_routes, "is_operator_available", fake_compose)

    resp = client.get("/api/availability")

    assert resp.status_code == 200
    assert resp.json()["manual_override"] == "AUTO"


def test_get_handles_missing_redis(monkeypatch: pytest.MonkeyPatch) -> None:
    """No Redis attached ⇒ chip endpoint still serves a composed verdict
    derived from the remaining sources without crashing on ``None`` reads."""

    async def fake_compose(sources):
        return AvailabilityState.AVAILABLE

    monkeypatch.setattr(dashboard_routes, "is_operator_available", fake_compose)

    if hasattr(web_app.app.state, "redis"):
        monkeypatch.delattr(web_app.app.state, "redis", raising=False)

    client = TestClient(web_app.app)
    resp = client.get("/api/availability")

    assert resp.status_code == 200
    assert resp.json() == {
        "composed_state": "AVAILABLE",
        "manual_override": "AUTO",
    }


def test_post_invalid_state_returns_400(availability_client) -> None:
    client, redis = availability_client

    resp = client.post("/api/availability/MAYBE")

    assert resp.status_code == 400
    assert resp.json() == {"error": "invalid_state"}
    # Invalid state must not have touched Redis at all.
    assert "operator_override" not in redis.store
    assert redis.published == []


def test_post_auto_clears_redis_key(availability_client) -> None:
    client, redis = availability_client
    redis.store["operator_override"] = "AVAILABLE"

    resp = client.post("/api/availability/AUTO")

    assert resp.status_code == 200
    assert resp.json() == {"manual_override": "AUTO"}
    assert "operator_override" not in redis.store


@pytest.mark.parametrize("state", ["AVAILABLE", "AWAY"])
def test_post_pins_explicit_override(availability_client, state: str) -> None:
    client, redis = availability_client

    resp = client.post(f"/api/availability/{state}")

    assert resp.status_code == 200
    assert resp.json() == {"manual_override": state}
    assert redis.store["operator_override"] == state


def test_post_publishes_wake(availability_client) -> None:
    client, redis = availability_client

    resp = client.post("/api/availability/AWAY")

    assert resp.status_code == 200
    # Wake message goes out on the dashboard-wide availability channel.
    assert ("orchestrator:availability:changed", "AWAY") in redis.published


def test_post_returns_503_when_redis_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without Redis the override cannot be persisted; the chip surfaces
    503 so the operator sees the failure rather than a silent no-op."""
    if hasattr(web_app.app.state, "redis"):
        monkeypatch.delattr(web_app.app.state, "redis", raising=False)

    client = TestClient(web_app.app)
    resp = client.post("/api/availability/AVAILABLE")

    assert resp.status_code == 503


@pytest.mark.parametrize("failing_op", ["set", "delete", "publish"])
def test_post_returns_503_when_redis_write_fails(
    availability_client, failing_op: str
) -> None:
    """A runtime Redis failure (timeout, connection drop, pool exhaustion)
    during the override write or wake publish must collapse to 503 — the
    same status code the redis-missing branch returns — instead of
    bubbling a 500 that would break click handling."""
    client, redis = availability_client

    async def boom(*_args, **_kwargs):
        raise RuntimeError("redis connection lost")

    setattr(redis, failing_op, boom)
    state = "AUTO" if failing_op == "delete" else "AVAILABLE"

    resp = client.post(f"/api/availability/{state}")

    assert resp.status_code == 503
    assert resp.json() == {"error": "redis_unavailable"}


# ---------------------------------------------------------------------------
# SSE stream
# ---------------------------------------------------------------------------


class _FakePubSub:
    """Pub/Sub double whose ``get_message`` walks a scripted queue.

    Tests append entries to ``messages`` (each either a dict, ``None`` for
    "no message yet", or an Exception to raise) so the stream loop can be
    exercised without a real Redis.
    """

    def __init__(self) -> None:
        self.subscribed: list[str] = []
        self.unsubscribed: list[str] = []
        self.closed = False
        self.messages: list = []
        self.subscribe_should_fail = False

    async def subscribe(self, *channels: str) -> None:
        if self.subscribe_should_fail:
            raise RuntimeError("subscribe failed")
        self.subscribed.extend(channels)

    async def unsubscribe(self, *channels: str) -> None:
        self.unsubscribed.extend(channels)

    async def aclose(self) -> None:
        self.closed = True

    async def get_message(self, ignore_subscribe_messages: bool = False, timeout: float = 0.0):
        if not self.messages:
            return None
        item = self.messages.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


class _FakeRedisWithPubSub(_FakeRedis):
    def __init__(self, pubsub: _FakePubSub) -> None:
        super().__init__()
        self._pubsub = pubsub

    def pubsub(self) -> _FakePubSub:
        return self._pubsub


async def test_sse_stream_yields_availability_changed_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A published value on the channel surfaces as an
    ``availability_changed`` SSE frame the chip JS listens for."""
    pubsub = _FakePubSub()
    pubsub.messages = [
        {"data": "AWAY"},  # first poll: forwarded as event
    ]
    redis = _FakeRedisWithPubSub(pubsub)
    web_app.app.state.redis = redis

    class _DisconnectAfterFirst:
        app = web_app.app

        def __init__(self) -> None:
            self._calls = 0

        async def is_disconnected(self) -> bool:
            self._calls += 1
            # Stay connected for the first iteration so the message is
            # yielded, then disconnect to terminate the generator.
            return self._calls > 1

    request = _DisconnectAfterFirst()
    resp = await dashboard_routes.api_availability_events(request)  # type: ignore[arg-type]
    chunks = []
    async for chunk in resp.body_iterator:
        chunks.append(chunk)
    body = b"".join(chunks).decode("utf-8")
    assert "event: availability_changed" in body
    assert '"manual_override": "AWAY"' in body
    assert pubsub.unsubscribed == ["orchestrator:availability:changed"]
    assert pubsub.closed is True


async def test_sse_stream_emits_keepalive_when_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An idle stream emits ``:keepalive`` comments so intermediaries do
    not idle the connection out before the operator clicks the chip."""
    pubsub = _FakePubSub()
    redis = _FakeRedisWithPubSub(pubsub)
    web_app.app.state.redis = redis

    monkeypatch.setattr(
        dashboard_routes, "AVAILABILITY_SSE_KEEPALIVE_SECONDS", 0.0
    )
    monkeypatch.setattr(
        dashboard_routes, "AVAILABILITY_SSE_POLL_INTERVAL_SECONDS", 0.0
    )

    class _DisconnectAfterFirst:
        app = web_app.app

        def __init__(self) -> None:
            self._calls = 0

        async def is_disconnected(self) -> bool:
            self._calls += 1
            return self._calls > 1

    request = _DisconnectAfterFirst()
    resp = await dashboard_routes.api_availability_events(request)  # type: ignore[arg-type]
    chunks = []
    async for chunk in resp.body_iterator:
        chunks.append(chunk)
    assert b":keepalive" in b"".join(chunks)


async def test_sse_stream_terminates_on_get_message_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Redis error mid-stream must close the generator cleanly without
    leaking an unsubscribe path."""
    pubsub = _FakePubSub()
    pubsub.messages = [RuntimeError("boom")]
    redis = _FakeRedisWithPubSub(pubsub)
    web_app.app.state.redis = redis

    class _Connected:
        app = web_app.app

        async def is_disconnected(self) -> bool:
            return False

    resp = await dashboard_routes.api_availability_events(_Connected())  # type: ignore[arg-type]
    chunks = []
    async for chunk in resp.body_iterator:
        chunks.append(chunk)
    # Generator returned cleanly; cleanup ran.
    assert pubsub.closed is True


async def test_sse_stream_503_when_redis_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if hasattr(web_app.app.state, "redis"):
        monkeypatch.delattr(web_app.app.state, "redis", raising=False)

    class _Req:
        app = web_app.app

    resp = await dashboard_routes.api_availability_events(_Req())  # type: ignore[arg-type]
    assert resp.status_code == 503


async def test_sse_stream_503_when_subscribe_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pubsub = _FakePubSub()
    pubsub.subscribe_should_fail = True
    redis = _FakeRedisWithPubSub(pubsub)
    web_app.app.state.redis = redis

    class _Req:
        app = web_app.app

    resp = await dashboard_routes.api_availability_events(_Req())  # type: ignore[arg-type]
    assert resp.status_code == 503
    assert pubsub.closed is True


async def test_sse_stream_503_when_subscribe_fails_and_aclose_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cleanup of a partial subscribe must swallow ``aclose`` errors so
    the operator still receives a 503 response rather than a 500."""
    pubsub = _FakePubSub()
    pubsub.subscribe_should_fail = True

    async def boom_close() -> None:
        raise RuntimeError("close failed")

    pubsub.aclose = boom_close  # type: ignore[assignment]
    redis = _FakeRedisWithPubSub(pubsub)
    web_app.app.state.redis = redis

    class _Req:
        app = web_app.app

    resp = await dashboard_routes.api_availability_events(_Req())  # type: ignore[arg-type]
    assert resp.status_code == 503


async def test_sse_stream_cleanup_swallows_unsubscribe_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unsubscribe and ``aclose`` errors during teardown must not bubble
    out of the stream — the operator already disconnected at that point."""
    pubsub = _FakePubSub()

    async def boom_unsubscribe(*channels: str) -> None:
        raise RuntimeError("unsubscribe failed")

    async def boom_close() -> None:
        raise RuntimeError("close failed")

    pubsub.unsubscribe = boom_unsubscribe  # type: ignore[assignment]
    pubsub.aclose = boom_close  # type: ignore[assignment]
    redis = _FakeRedisWithPubSub(pubsub)
    web_app.app.state.redis = redis

    class _Disconnected:
        app = web_app.app

        async def is_disconnected(self) -> bool:
            return True

    resp = await dashboard_routes.api_availability_events(_Disconnected())  # type: ignore[arg-type]
    chunks = []
    async for _chunk in resp.body_iterator:
        chunks.append(_chunk)
    # No exception bubbled; teardown ran to completion despite the errors.


async def test_is_request_disconnected_handles_sync_and_missing_checker() -> None:
    """The helper supports both ASGI's coroutine ``is_disconnected`` and
    objects that lack the attribute entirely (legacy callers / tests)."""

    class _Coro:
        async def is_disconnected(self) -> bool:
            return True

    class _Sync:
        def is_disconnected(self) -> bool:
            return False

    class _Missing:
        pass

    assert await dashboard_routes._is_request_disconnected(_Coro()) is True  # type: ignore[arg-type]
    assert await dashboard_routes._is_request_disconnected(_Sync()) is False  # type: ignore[arg-type]
    assert await dashboard_routes._is_request_disconnected(_Missing()) is False  # type: ignore[arg-type]


async def test_availability_sources_compose_three_signal_sources() -> None:
    """Sanity check that ``_availability_sources`` returns the three
    canonical sources in order so the dashboard verdict tracks the
    daemon-side composition rather than diverging."""
    from src.cancellation.availability import (
        ActiveHoursSource,
        HeartbeatSource,
        ManualOverrideSource,
    )
    from src.config import load_config

    cfg = load_config(web_app.CONFIG_PATH)
    sources = await dashboard_routes._availability_sources(_FakeRedis(), cfg)

    assert [type(s) for s in sources] == [
        ManualOverrideSource,
        HeartbeatSource,
        ActiveHoursSource,
    ]


async def test_availability_sources_skip_redis_backed_when_redis_missing() -> None:
    """When ``redis_client`` is None, the Redis-backed sources must be
    omitted entirely. Including them would make every query() raise inside
    ``is_operator_available``, flip ``any_failed`` true, and bias the
    verdict to AVAILABLE — misreporting the operator as available during
    off-hours whenever Redis is down. ``ActiveHoursSource`` should compose
    the verdict alone instead."""
    from src.cancellation.availability import ActiveHoursSource
    from src.config import load_config

    cfg = load_config(web_app.CONFIG_PATH)
    sources = await dashboard_routes._availability_sources(None, cfg)

    assert [type(s) for s in sources] == [ActiveHoursSource]


async def test_get_reports_active_hours_when_redis_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end version of the P2 fix: with Redis absent and the
    ``ActiveHoursSource`` reporting AWAY, the composed verdict must be
    AWAY rather than the failure-safe AVAILABLE bias that would fire if
    Redis-backed sources were left in the source list and raised."""
    if hasattr(web_app.app.state, "redis"):
        monkeypatch.delattr(web_app.app.state, "redis", raising=False)

    class _AwayActiveHours:
        name = "active_hours"

        async def query(self):
            return AvailabilityState.AWAY

    async def fake_sources(redis_client, cfg):
        # Mirror the real selector's redis-missing branch so the test
        # fails if a future change re-introduces redis-backed sources here.
        assert redis_client is None
        return [_AwayActiveHours()]

    monkeypatch.setattr(dashboard_routes, "_availability_sources", fake_sources)

    client = TestClient(web_app.app)
    resp = client.get("/api/availability")

    assert resp.status_code == 200
    assert resp.json() == {
        "composed_state": "AWAY",
        "manual_override": "AUTO",
    }


async def test_sse_stream_decodes_bytes_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Redis client without ``decode_responses`` returns bytes; the
    SSE generator must decode so the payload renders as a UTF-8 string."""
    pubsub = _FakePubSub()
    pubsub.messages = [{"data": b"AVAILABLE"}]
    redis = _FakeRedisWithPubSub(pubsub)
    web_app.app.state.redis = redis

    class _DisconnectAfterFirst:
        app = web_app.app

        def __init__(self) -> None:
            self._calls = 0

        async def is_disconnected(self) -> bool:
            self._calls += 1
            return self._calls > 1

    request = _DisconnectAfterFirst()
    resp = await dashboard_routes.api_availability_events(request)  # type: ignore[arg-type]
    body = b""
    async for chunk in resp.body_iterator:
        body += chunk
    assert b'"manual_override": "AVAILABLE"' in body
