"""Tests for src.cancellation.storage (PR-252 substrate)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.cancellation import storage
from src.cancellation.storage import (
    CATEGORIES,
    TTL_SECONDS,
    CancellationCause,
    cause_key,
    get_cancellation_cause,
    index_key,
    list_recent_cancellations,
    record_cancellation_cause,
)


class _FakePipeline:
    def __init__(self, store: "_FakeRedis") -> None:
        self._store = store
        self._ops: list[tuple] = []

    def set(self, key: str, value: str, ex: int | None = None) -> "_FakePipeline":
        self._ops.append(("set", key, value, ex))
        return self

    def zadd(self, key: str, mapping: dict[str, float]) -> "_FakePipeline":
        self._ops.append(("zadd", key, dict(mapping)))
        return self

    def zremrangebyscore(self, key: str, min_score, max_score) -> "_FakePipeline":
        self._ops.append(("zremrangebyscore", key, min_score, max_score))
        return self

    async def execute(self) -> list:
        results = []
        for op in self._ops:
            if op[0] == "set":
                _, key, value, ex = op
                self._store.values[key] = value
                if ex is not None:
                    self._store.ttls[key] = ex
                results.append(True)
            elif op[0] == "zadd":
                _, key, mapping = op
                bucket = self._store.zsets.setdefault(key, {})
                bucket.update(mapping)
                results.append(len(mapping))
            elif op[0] == "zremrangebyscore":
                _, key, min_score, max_score = op
                results.append(self._store._zremrangebyscore(key, min_score, max_score))
        self._ops.clear()
        return results


class _FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.ttls: dict[str, int] = {}
        self.zsets: dict[str, dict[str, float]] = {}

    def pipeline(self) -> _FakePipeline:
        return _FakePipeline(self)

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def zrangebyscore(self, key: str, min_score, max_score) -> list[str]:
        bucket = self.zsets.get(key, {})
        if max_score == "+inf":
            upper = float("inf")
        else:
            upper = float(max_score)
        lower = float(min_score)
        items = [tid for tid, score in bucket.items() if lower <= score <= upper]
        items.sort(key=lambda tid: bucket[tid])
        return items

    async def zrem(self, key: str, *members: str) -> int:
        bucket = self.zsets.get(key, {})
        removed = 0
        for member in members:
            if member in bucket:
                del bucket[member]
                removed += 1
        return removed

    def _zremrangebyscore(self, key: str, min_score, max_score) -> int:
        bucket = self.zsets.get(key)
        if not bucket:
            return 0
        lower, lower_excl = self._parse_bound(min_score, default=float("-inf"))
        upper, upper_excl = self._parse_bound(max_score, default=float("inf"))
        to_remove = [
            tid
            for tid, score in bucket.items()
            if (score > lower if lower_excl else score >= lower)
            and (score < upper if upper_excl else score <= upper)
        ]
        for tid in to_remove:
            del bucket[tid]
        return len(to_remove)

    @staticmethod
    def _parse_bound(value, default: float) -> tuple[float, bool]:
        if value in ("-inf", "+inf"):
            return float(value), False
        if isinstance(value, str) and value.startswith("("):
            return float(value[1:]), True
        return float(value), False


def test_categories_cover_expected_set() -> None:
    assert set(CATEGORIES) == {
        "CRASH",
        "ESCALATE",
        "TIMEOUT",
        "INFRA",
        "OPERATOR_RECOVERY",
        "NO_PUSH_DEADLOCK",
    }


def test_cause_and_index_key_format() -> None:
    assert cause_key("alpha", "PR-101") == "cancellation:alpha:PR-101"
    assert index_key("alpha") == "cancellation_index:alpha"


def test_cancellation_cause_round_trip_serialization() -> None:
    cause = CancellationCause(
        category="CRASH",
        payload={"exit_code": 137, "stderr": "killed"},
        created_at="2026-05-04T12:00:00+00:00",
        task_id="PR-200",
        repo_slug="example__repo",
    )
    raw = cause.to_redis()
    restored = CancellationCause.from_redis(raw)
    assert restored == cause
    restored_bytes = CancellationCause.from_redis(raw.encode("utf-8"))
    assert restored_bytes == cause


async def test_round_trip_write_read() -> None:
    redis = _FakeRedis()
    cause = CancellationCause(
        category="ESCALATE",
        payload={"reason": "manual ESCALATE"},
        created_at="2026-05-04T10:00:00+00:00",
    )
    await record_cancellation_cause(redis, "alpha", "PR-300", cause)

    fetched = await get_cancellation_cause(redis, "alpha", "PR-300")
    assert fetched is not None
    assert fetched.category == "ESCALATE"
    assert fetched.payload == {"reason": "manual ESCALATE"}
    assert fetched.task_id == "PR-300"
    assert fetched.repo_slug == "alpha"
    assert fetched.created_at == "2026-05-04T10:00:00+00:00"


async def test_record_fills_missing_metadata() -> None:
    redis = _FakeRedis()
    cause = CancellationCause(category="INFRA")
    await record_cancellation_cause(redis, "beta", "PR-301", cause)

    stored = await get_cancellation_cause(redis, "beta", "PR-301")
    assert stored is not None
    assert stored.task_id == "PR-301"
    assert stored.repo_slug == "beta"
    assert stored.created_at
    datetime.fromisoformat(stored.created_at)


async def test_record_overrides_stale_identifiers_in_payload() -> None:
    redis = _FakeRedis()
    cause = CancellationCause(
        category="CRASH",
        created_at="2026-05-04T08:00:00+00:00",
        task_id="PR-OLD",
        repo_slug="old-repo",
    )
    await record_cancellation_cause(redis, "new-repo", "PR-NEW", cause)

    stored = await get_cancellation_cause(redis, "new-repo", "PR-NEW")
    assert stored is not None
    assert stored.task_id == "PR-NEW"
    assert stored.repo_slug == "new-repo"
    # Caller's object is also coerced to match the key arguments.
    assert cause.task_id == "PR-NEW"
    assert cause.repo_slug == "new-repo"


async def test_missing_key_returns_none() -> None:
    redis = _FakeRedis()
    assert await get_cancellation_cause(redis, "alpha", "PR-404") is None


async def test_multi_repo_isolation() -> None:
    redis = _FakeRedis()
    await record_cancellation_cause(
        redis,
        "alpha",
        "PR-500",
        CancellationCause(
            category="TIMEOUT",
            payload={"limit": "wallclock"},
            created_at="2026-05-04T09:00:00+00:00",
        ),
    )
    await record_cancellation_cause(
        redis,
        "beta",
        "PR-500",
        CancellationCause(
            category="OPERATOR_RECOVERY",
            payload={"actor": "alice"},
            created_at="2026-05-04T09:05:00+00:00",
        ),
    )

    alpha = await get_cancellation_cause(redis, "alpha", "PR-500")
    beta = await get_cancellation_cause(redis, "beta", "PR-500")
    assert alpha is not None and alpha.category == "TIMEOUT"
    assert beta is not None and beta.category == "OPERATOR_RECOVERY"
    assert alpha.repo_slug == "alpha"
    assert beta.repo_slug == "beta"


async def test_list_recent_filters_by_timestamp_and_orders_newest_first() -> None:
    redis = _FakeRedis()
    base = datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc)
    timestamps = [base - timedelta(hours=2), base - timedelta(minutes=15), base]
    for idx, ts in enumerate(timestamps):
        await record_cancellation_cause(
            redis,
            "alpha",
            f"PR-{600 + idx}",
            CancellationCause(category="CRASH", created_at=ts.isoformat()),
        )

    since = base - timedelta(minutes=30)
    recent = await list_recent_cancellations(redis, "alpha", since)
    assert [c.task_id for c in recent] == ["PR-602", "PR-601"]


async def test_list_recent_returns_empty_when_no_index() -> None:
    redis = _FakeRedis()
    since = datetime(2026, 5, 4, tzinfo=timezone.utc)
    assert await list_recent_cancellations(redis, "ghost", since) == []


async def test_list_recent_skips_expired_payloads_and_prunes_index() -> None:
    redis = _FakeRedis()
    created = datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc)
    await record_cancellation_cause(
        redis,
        "alpha",
        "PR-700",
        CancellationCause(category="CRASH", created_at=created.isoformat()),
    )
    redis.values.pop(cause_key("alpha", "PR-700"))

    recent = await list_recent_cancellations(
        redis, "alpha", created - timedelta(hours=1)
    )
    assert recent == []
    # Stale member must be removed from the index so list work stays bounded.
    assert "PR-700" not in redis.zsets.get(index_key("alpha"), {})


async def test_record_prunes_index_entries_older_than_ttl() -> None:
    redis = _FakeRedis()
    now = datetime.now(timezone.utc)
    ancient_ts = (now - timedelta(seconds=TTL_SECONDS + 3600)).timestamp()
    redis.zsets[index_key("alpha")] = {"PR-EXPIRED": ancient_ts}

    await record_cancellation_cause(
        redis,
        "alpha",
        "PR-FRESH",
        CancellationCause(category="INFRA", created_at=now.isoformat()),
    )

    bucket = redis.zsets[index_key("alpha")]
    assert "PR-EXPIRED" not in bucket
    assert "PR-FRESH" in bucket


async def test_list_recent_decodes_bytes_task_ids() -> None:
    redis = _FakeRedis()
    created = datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc)
    await record_cancellation_cause(
        redis,
        "alpha",
        "PR-800",
        CancellationCause(category="ESCALATE", created_at=created.isoformat()),
    )

    async def _bytes_zrange(key: str, min_score, max_score) -> list[bytes]:
        return [b"PR-800"]

    redis.zrangebyscore = _bytes_zrange  # type: ignore[assignment]

    recent = await list_recent_cancellations(
        redis, "alpha", created - timedelta(hours=1)
    )
    assert [c.task_id for c in recent] == ["PR-800"]


async def test_ttl_set_to_thirty_days() -> None:
    redis = _FakeRedis()
    await record_cancellation_cause(
        redis,
        "alpha",
        "PR-900",
        CancellationCause(category="INFRA"),
    )
    assert TTL_SECONDS == 30 * 24 * 3600
    assert redis.ttls[cause_key("alpha", "PR-900")] == TTL_SECONDS


def test_module_reexports_match_storage() -> None:
    from src import cancellation

    assert cancellation.CancellationCause is storage.CancellationCause
    assert cancellation.record_cancellation_cause is storage.record_cancellation_cause
    assert cancellation.get_cancellation_cause is storage.get_cancellation_cause
    assert cancellation.list_recent_cancellations is storage.list_recent_cancellations


@pytest.mark.parametrize(
    "raw",
    [
        b'{"category":"CRASH","payload":{},"created_at":"","task_id":"","repo_slug":""}',
        '{"category":"CRASH","payload":{},"created_at":"","task_id":"","repo_slug":""}',
    ],
)
def test_from_redis_accepts_bytes_or_str(raw) -> None:
    cause = CancellationCause.from_redis(raw)
    assert cause.category == "CRASH"
