"""Tests for the orchestrator wake pub/sub helpers."""

from __future__ import annotations

import json
from typing import Any

from src.events import wake


class _FakeRedis:
    def __init__(self) -> None:
        self.published: list[tuple[str, str]] = []
        self.closed = False
        self.subscribed: list[str] = []

    async def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, message))
        return 1

    async def aclose(self) -> None:
        self.closed = True

    def pubsub(self) -> _FakePubsub:
        ps = _FakePubsub()
        self._pubsub = ps
        return ps


class _FakePubsub:
    def __init__(self) -> None:
        self.subscribed: list[str] = []

    async def subscribe(self, *channels: str) -> None:
        self.subscribed.extend(channels)


def test_wake_channel_uses_orchestrator_namespace() -> None:
    assert wake.wake_channel("example__alpha") == "orchestrator:wake:example__alpha"


async def test_publish_wake_emits_canonical_message() -> None:
    redis = _FakeRedis()

    await wake.publish_wake("example__alpha", "upload", redis_client=redis)

    assert len(redis.published) == 1
    channel, raw = redis.published[0]
    assert channel == "orchestrator:wake:example__alpha"
    payload = json.loads(raw)
    assert payload["event_type"] == "upload"
    assert payload["repo"] == "example__alpha"
    assert payload["timestamp"].endswith("Z")
    assert redis.closed is False  # caller-owned client must not be closed.


async def test_publish_wake_closes_owned_client(monkeypatch: Any) -> None:
    redis = _FakeRedis()

    class _StubAioredis:
        @staticmethod
        def from_url(url: str, decode_responses: bool = True) -> _FakeRedis:
            return redis

    monkeypatch.setattr(wake, "aioredis", _StubAioredis())

    await wake.publish_wake("example__alpha")

    assert redis.closed is True
    assert redis.published[0][0] == "orchestrator:wake:example__alpha"


async def test_subscribe_wake_subscribes_to_per_repo_channels() -> None:
    redis = _FakeRedis()

    pubsub = await wake.subscribe_wake(
        ["example__alpha", "octo__beta"], redis_client=redis
    )

    assert pubsub.subscribed == [
        "orchestrator:wake:example__alpha",
        "orchestrator:wake:octo__beta",
    ]


async def test_subscribe_wake_with_empty_repos_skips_subscribe() -> None:
    redis = _FakeRedis()

    pubsub = await wake.subscribe_wake([], redis_client=redis)

    assert pubsub.subscribed == []


async def test_subscribe_wake_uses_default_url_when_no_client(monkeypatch: Any) -> None:
    redis = _FakeRedis()

    class _StubAioredis:
        @staticmethod
        def from_url(url: str, decode_responses: bool = True) -> _FakeRedis:
            assert decode_responses is True
            return redis

    monkeypatch.setattr(wake, "aioredis", _StubAioredis())

    pubsub = await wake.subscribe_wake(["example__alpha"])

    assert pubsub.subscribed == ["orchestrator:wake:example__alpha"]
