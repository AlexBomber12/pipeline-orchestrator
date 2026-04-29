"""Tests for src/events/wake.py."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import pytest
from src.events import wake


class _FakePubSub:
    def __init__(self, *, fail_subscribe: bool = False) -> None:
        self.subscribed: list[str] = []
        self._fail_subscribe = fail_subscribe
        self.closed = False

    async def subscribe(self, *channels: str) -> None:
        if self._fail_subscribe:
            raise RuntimeError("subscribe failed")
        self.subscribed.extend(channels)

    async def aclose(self) -> None:
        self.closed = True


class _FakeRedis:
    def __init__(self, *, fail_pubsub: bool = False, pubsub_obj: Any | None = None) -> None:
        self.published: list[tuple[str, str]] = []
        self._fail_pubsub = fail_pubsub
        self._pubsub_obj = pubsub_obj or _FakePubSub()

    async def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, message))
        return 1

    def pubsub(self) -> Any:
        if self._fail_pubsub:
            raise RuntimeError("pubsub() unavailable")
        return self._pubsub_obj


def test_build_wake_message_uses_expected_shape() -> None:
    payload = wake.build_wake_message(
        "owner__repo",
        "upload",
        now=datetime(2026, 4, 29, 14, 30, tzinfo=timezone.utc),
    )

    parsed = json.loads(payload)
    assert parsed == {
        "event_type": "upload",
        "repo": "owner__repo",
        "timestamp": "2026-04-29T14:30:00Z",
    }


def test_build_wake_message_defaults_now_to_utc(monkeypatch: pytest.MonkeyPatch) -> None:
    parsed = json.loads(wake.build_wake_message("owner__repo", "upload"))
    parsed_dt = datetime.fromisoformat(parsed["timestamp"].replace("Z", "+00:00"))
    assert parsed_dt.tzinfo == timezone.utc


async def test_publish_wake_uses_repo_channel() -> None:
    redis = _FakeRedis()

    await wake.publish_wake(redis, "owner__repo", "upload")

    assert len(redis.published) == 1
    channel, message = redis.published[0]
    assert channel == "orchestrator:wake:owner__repo"
    parsed = json.loads(message)
    assert parsed["event_type"] == "upload"
    assert parsed["repo"] == "owner__repo"


async def test_subscribe_wake_returns_none_when_no_repos() -> None:
    redis = _FakeRedis()
    pubsub = await wake.subscribe_wake(redis, ())
    assert pubsub is None


async def test_subscribe_wake_subscribes_each_repo() -> None:
    redis = _FakeRedis()
    pubsub = await wake.subscribe_wake(redis, ("a", "b"))
    assert pubsub is redis._pubsub_obj
    assert pubsub.subscribed == [
        "orchestrator:wake:a",
        "orchestrator:wake:b",
    ]


async def test_subscribe_wake_returns_none_when_pubsub_constructor_fails() -> None:
    redis = _FakeRedis(fail_pubsub=True)
    pubsub = await wake.subscribe_wake(redis, ("a",))
    assert pubsub is None


async def test_subscribe_wake_returns_none_when_subscribe_fails() -> None:
    failing = _FakePubSub(fail_subscribe=True)
    redis = _FakeRedis(pubsub_obj=failing)
    pubsub = await wake.subscribe_wake(redis, ("a",))
    assert pubsub is None


def test_repo_from_channel_extracts_slug() -> None:
    assert wake.repo_from_channel("orchestrator:wake:owner__repo") == "owner__repo"


def test_repo_from_channel_returns_none_for_unrelated_channel() -> None:
    assert wake.repo_from_channel("repo-events:owner__repo") is None


def test_repo_from_channel_returns_none_for_empty_slug() -> None:
    assert wake.repo_from_channel("orchestrator:wake:") is None
