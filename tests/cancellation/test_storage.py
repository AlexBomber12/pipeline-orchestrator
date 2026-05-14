from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from src.cancellation.storage import (
    CancellationCause,
    index_key,
    list_pending_guardrail_decisions,
    record_cancellation_cause,
)


class _StorageRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.zsets: dict[str, dict[str, float]] = {}

    def pipeline(self) -> "_StoragePipe":
        return _StoragePipe(self)

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def zrange(self, key: str, start: int, end: int) -> list[str]:
        items = sorted(self.zsets.get(key, {}).items(), key=lambda item: item[1])
        if end == -1:
            return [member for member, _score in items[start:]]
        return [member for member, _score in items[start : end + 1]]

    async def zrem(self, key: str, *members: str) -> int:
        zset = self.zsets.setdefault(key, {})
        removed = 0
        for member in members:
            removed += int(member in zset)
            zset.pop(member, None)
        return removed

    async def scan_iter(self, match: str) -> Any:
        prefix = match.removesuffix("*")
        for key in sorted(self.zsets):
            if key.startswith(prefix):
                yield key.encode("utf-8")


class _ScanFallbackRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.zsets: dict[str, dict[str, float]] = {}

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def zrange(self, key: str, start: int, end: int) -> list[bytes]:
        items = sorted(self.zsets.get(key, {}).items(), key=lambda item: item[1])
        return [member.encode("utf-8") for member, _score in items[start : end + 1]]

    async def zrem(self, key: str, *members: str) -> int:
        for member in members:
            self.zsets.setdefault(key, {}).pop(member, None)
        return len(members)

    async def scan(
        self,
        cursor: int,
        *,
        match: str,
        count: int,
    ) -> tuple[int, list[bytes]]:
        prefix = match.removesuffix("*")
        return (
            0,
            [
                key.encode("utf-8")
                for key in sorted(self.zsets)
                if key.startswith(prefix)
            ],
        )


class _StoragePipe:
    def __init__(self, redis: _StorageRedis) -> None:
        self.redis = redis

    def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.redis.store[key] = value

    def zadd(self, key: str, mapping: dict[str, float]) -> None:
        self.redis.zsets.setdefault(key, {}).update(mapping)

    def zremrangebyscore(self, key: str, minimum: str, maximum: str) -> None:
        return None

    def expire(self, key: str, seconds: int) -> None:
        return None

    async def execute(self) -> None:
        return None


def _cause(subsource: str, when: int, **payload: object) -> CancellationCause:
    return CancellationCause(
        category="ERROR",
        payload={"subsource": subsource, **payload},
        created_at=datetime.fromtimestamp(when, tz=timezone.utc).isoformat(),
    )


@pytest.mark.asyncio
async def test_list_pending_guardrail_decisions_filters_by_subsource() -> None:
    redis = _StorageRedis()
    await record_cancellation_cause(
        redis, "repo_a", "PR-1", _cause("guardrail", 10, rule="large", excerpt="+1")
    )
    await record_cancellation_cause(
        redis, "repo_a", "PR-2", _cause("coder", 11, rule="coder", excerpt="bad")
    )
    await record_cancellation_cause(
        redis, "repo_a", "PR-3", _cause("guardrail", 12, rule="secret", excerpt="x")
    )

    pending = await list_pending_guardrail_decisions(redis, repo="repo_a")

    assert [item.pr_id for item in pending] == ["PR-1", "PR-3"]
    assert [item.rule for item in pending] == ["large", "secret"]


@pytest.mark.asyncio
async def test_list_pending_guardrail_decisions_sorted_by_recorded_at() -> None:
    redis = _StorageRedis()
    await record_cancellation_cause(redis, "repo_a", "PR-2", _cause("guardrail", 20))
    await record_cancellation_cause(redis, "repo_a", "PR-1", _cause("guardrail", 10))
    await record_cancellation_cause(redis, "repo_a", "PR-3", _cause("guardrail", 30))

    pending = await list_pending_guardrail_decisions(redis, repo="repo_a")

    assert [item.pr_id for item in pending] == ["PR-1", "PR-2", "PR-3"]


@pytest.mark.asyncio
async def test_list_pending_guardrail_decisions_repo_filter() -> None:
    redis = _StorageRedis()
    await record_cancellation_cause(redis, "repo_a", "PR-1", _cause("guardrail", 10))
    await record_cancellation_cause(redis, "repo_b", "PR-2", _cause("guardrail", 20))

    pending = await list_pending_guardrail_decisions(redis, repo="repo_a")

    assert [(item.repo, item.pr_id) for item in pending] == [("repo_a", "PR-1")]


@pytest.mark.asyncio
async def test_list_pending_guardrail_decisions_bounded_scan_100_entries() -> None:
    redis = _StorageRedis()
    for index in range(150):
        await record_cancellation_cause(
            redis,
            "repo_a",
            f"PR-{index}",
            _cause("guardrail", index),
        )

    pending = await list_pending_guardrail_decisions(redis, repo="repo_a")

    assert len(pending) <= 100
    assert len(redis.zsets[index_key("repo_a")]) == 150


@pytest.mark.asyncio
async def test_list_pending_guardrail_decisions_repo_none_uses_index_scan() -> None:
    redis = _StorageRedis()
    await record_cancellation_cause(redis, "repo_a", "PR-1", _cause("guardrail", 10))
    await record_cancellation_cause(redis, "repo_b", "PR-2", _cause("guardrail", 20))

    pending = await list_pending_guardrail_decisions(redis)

    assert [(item.repo, item.pr_id) for item in pending] == [
        ("repo_a", "PR-1"),
        ("repo_b", "PR-2"),
    ]


@pytest.mark.asyncio
async def test_list_pending_guardrail_decisions_scan_fallback_and_stale_cleanup() -> None:
    redis = _ScanFallbackRedis()
    redis.zsets[index_key("repo_a")] = {"PR-1": 1.0, "PR-2": 2.0}
    redis.store["cancellation:repo_a:PR-2"] = CancellationCause(
        category="ERROR",
        payload={"subsource": "guardrail"},
        created_at="not-a-date",
    ).to_redis()

    pending = await list_pending_guardrail_decisions(redis)

    assert [(item.pr_id, item.recorded_at) for item in pending] == [("PR-2", 0)]
    assert "PR-1" not in redis.zsets[index_key("repo_a")]
