from __future__ import annotations

import json
from datetime import datetime, timezone

from src.events import publisher


class _FakeRedis:
    def __init__(self) -> None:
        self.lists: dict[str, list[str]] = {}
        self.published: list[tuple[str, str]] = []
        self.closed = False

    async def lpush(self, key: str, value: str) -> int:
        bucket = self.lists.setdefault(key, [])
        bucket.insert(0, value)
        return len(bucket)

    async def ltrim(self, key: str, start: int, stop: int) -> None:
        values = self.lists.get(key, [])
        self.lists[key] = values[start : stop + 1]

    async def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, message))
        return 1

    async def aclose(self) -> None:
        self.closed = True


def test_build_repo_event_uses_expected_shape() -> None:
    event = publisher.build_repo_event(
        "example__repo",
        "progress_updated",
        {"percent": 80},
        now=datetime(2026, 4, 20, 14, 30, tzinfo=timezone.utc),
    )

    assert event == {
        "type": "progress_updated",
        "repo": "example__repo",
        "data": {"percent": 80},
        "timestamp": "2026-04-20T14:30:00Z",
    }


async def test_publish_repo_event_appends_history_trims_and_publishes() -> None:
    redis = _FakeRedis()
    history_key = "repo-events-history:example__repo"
    redis.lists[history_key] = [f"old-{index}" for index in range(60)]

    await publisher.publish_repo_event(
        "example__repo",
        "state_changed",
        {"state": "WATCH"},
        redis_client=redis,
    )

    assert len(redis.lists[history_key]) == publisher.EVENT_HISTORY_LIMIT
    channel, message = redis.published[-1]
    assert channel == "repo-events:example__repo"
    assert redis.lists[history_key][0] == message
    payload = json.loads(message)
    assert payload["type"] == "state_changed"
    assert payload["repo"] == "example__repo"
    assert payload["data"] == {"state": "WATCH"}
    assert payload["timestamp"].endswith("Z")


async def test_publish_repo_event_closes_owned_client(
    monkeypatch,
) -> None:
    redis = _FakeRedis()

    class _StubAioredis:
        @staticmethod
        def from_url(url: str, decode_responses: bool = True) -> _FakeRedis:
            return redis

    monkeypatch.setattr(publisher, "aioredis", _StubAioredis())

    await publisher.publish_repo_event("example__repo", "test_event", {"foo": "bar"})

    assert redis.closed is True
    assert redis.published
