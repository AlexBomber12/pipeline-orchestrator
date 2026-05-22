"""Tests for the Redis-backed suppression store port."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from src.cancellation.storage import (
    CancellationCause,
    cause_key,
    get_cancellation_cause,
    index_key,
    record_cancellation_cause,
)
from src.subsource_registry import SuppressionReason
from src.suppression.redis_store import RedisSuppressionStore
from tests.test_cancellation_storage import _FakeRedis


async def test_suppress_then_is_suppressed() -> None:
    redis = _FakeRedis()
    store = RedisSuppressionStore(redis)
    detail = {"rule": "no-default-branch-push", "excerpt": "git push origin main"}

    await store.suppress("alpha", "PR-377", SuppressionReason.GUARDRAIL, detail)

    record = await store.is_suppressed("alpha", "PR-377")
    assert record is not None
    assert record.task_id == "PR-377"
    assert record.reason == SuppressionReason.GUARDRAIL
    assert record.detail == detail
    assert record.created_at is not None
    assert record.approved_once is False


async def test_clear_removes_suppression() -> None:
    redis = _FakeRedis()
    store = RedisSuppressionStore(redis)
    await store.suppress("alpha", "PR-377", SuppressionReason.GUARDRAIL, {})

    await store.clear("alpha", "PR-377")

    assert await store.is_suppressed("alpha", "PR-377") is None


async def test_is_suppressed_none_when_absent() -> None:
    redis = _FakeRedis()
    store = RedisSuppressionStore(redis)

    assert await store.is_suppressed("alpha", "PR-404") is None


async def test_store_matches_direct_cancellation_read() -> None:
    redis = _FakeRedis()
    store = RedisSuppressionStore(redis)
    detail = {"rule": "forbidden-workflow-edit", "path": ".github/workflows/ci.yml"}

    await store.suppress("alpha", "PR-377", SuppressionReason.GUARDRAIL, detail)

    cause = await get_cancellation_cause(redis, "alpha", "PR-377")
    assert cause is not None
    assert cause.category == "ERROR"
    assert cause.payload == {"subsource": "guardrail", **detail}
    assert cause.task_id == "PR-377"
    assert cause.repo_slug == "alpha"


async def test_list_suppressed_returns_records() -> None:
    redis = _FakeRedis()
    store = RedisSuppressionStore(redis)
    base = datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc)
    for offset, task_id in enumerate(["PR-1", "PR-2", "PR-3"]):
        await record_cancellation_cause(
            redis,
            "alpha",
            task_id,
            CancellationCause(
                category="ERROR",
                payload={"subsource": "guardrail", "offset": offset},
                created_at=(base + timedelta(minutes=offset)).isoformat(),
            ),
        )

    records = await store.list_suppressed("alpha", since=base - timedelta(minutes=1))

    assert [record.task_id for record in records] == ["PR-3", "PR-2", "PR-1"]
    assert [record.reason for record in records] == [
        SuppressionReason.GUARDRAIL,
        SuppressionReason.GUARDRAIL,
        SuppressionReason.GUARDRAIL,
    ]
    assert [record.detail["offset"] for record in records] == [2, 1, 0]


async def test_list_suppressed_honors_limit() -> None:
    redis = _FakeRedis()
    store = RedisSuppressionStore(redis)
    await store.suppress("alpha", "PR-1", SuppressionReason.GUARDRAIL, {})

    assert await store.list_suppressed("alpha", limit=0) == []


async def test_unknown_subsource_defensive_crash(caplog) -> None:
    redis = _FakeRedis()
    store = RedisSuppressionStore(redis)
    await record_cancellation_cause(
        redis,
        "alpha",
        "PR-UNKNOWN",
        CancellationCause(
            category="ERROR",
            payload={"subsource": "not_in_registry", "raw": "kept"},
            created_at="2026-05-04T12:00:00+00:00",
        ),
    )

    with caplog.at_level(logging.WARNING, logger="src.suppression.redis_store"):
        first = await store.is_suppressed("alpha", "PR-UNKNOWN")
        second = await store.is_suppressed("alpha", "PR-UNKNOWN")

    assert first is not None
    assert second is not None
    assert first.reason == SuppressionReason.CRASH
    assert first.detail == {"raw": "kept"}
    warnings = [
        record
        for record in caplog.records
        if "Unknown suppression subsource" in record.getMessage()
    ]
    assert len(warnings) == 1


async def test_legacy_category_without_subsource_derives_reason() -> None:
    redis = _FakeRedis()
    store = RedisSuppressionStore(redis)
    await record_cancellation_cause(
        redis,
        "alpha",
        "PR-LEGACY-TIMEOUT",
        CancellationCause(
            category="TIMEOUT",
            payload={"note": "pre-migration"},
            created_at="2026-05-04T12:00:00+00:00",
        ),
    )

    record = await store.is_suppressed("alpha", "PR-LEGACY-TIMEOUT")

    assert record is not None
    assert record.reason == SuppressionReason.REVIEW_TIMEOUT
    assert record.detail == {"note": "pre-migration"}


async def test_payload_legacy_category_without_subsource_derives_reason() -> None:
    redis = _FakeRedis()
    store = RedisSuppressionStore(redis)
    await record_cancellation_cause(
        redis,
        "alpha",
        "PR-MIGRATED-LEGACY",
        CancellationCause(
            category="ERROR",
            payload={"legacy_category": "TIMEOUT"},
            created_at="2026-05-04T12:00:00+00:00",
        ),
    )

    record = await store.is_suppressed("alpha", "PR-MIGRATED-LEGACY")

    assert record is not None
    assert record.reason == SuppressionReason.REVIEW_TIMEOUT


async def test_created_at_parsing_handles_invalid_and_naive_values() -> None:
    redis = _FakeRedis()
    store = RedisSuppressionStore(redis)
    bad_cause = CancellationCause(
        category="ERROR",
        payload={"subsource": "guardrail"},
        created_at="not-a-timestamp",
        task_id="PR-BAD-TIME",
        repo_slug="alpha",
    )
    redis.values[cause_key("alpha", "PR-BAD-TIME")] = bad_cause.to_redis()
    await record_cancellation_cause(
        redis,
        "alpha",
        "PR-NAIVE-TIME",
        CancellationCause(
            category="ERROR",
            payload={"subsource": "guardrail"},
            created_at="2026-05-04T12:00:00",
        ),
    )

    bad = await store.is_suppressed("alpha", "PR-BAD-TIME")
    naive = await store.is_suppressed("alpha", "PR-NAIVE-TIME")

    assert bad is not None
    assert bad.created_at is None
    assert naive is not None
    assert naive.created_at == datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc)


async def test_list_suppressed_tolerates_malformed_created_at() -> None:
    redis = _FakeRedis()
    store = RedisSuppressionStore(redis)
    base = datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc)
    malformed = CancellationCause(
        category="ERROR",
        payload={"subsource": "guardrail", "kind": "malformed"},
        created_at="not-a-timestamp",
        task_id="PR-BAD-TIME",
        repo_slug="alpha",
    )
    redis.values[cause_key("alpha", "PR-BAD-TIME")] = malformed.to_redis()
    redis.zsets[index_key("alpha")] = {"PR-BAD-TIME": base.timestamp()}
    await record_cancellation_cause(
        redis,
        "alpha",
        "PR-GOOD-TIME",
        CancellationCause(
            category="ERROR",
            payload={"subsource": "guardrail", "kind": "valid"},
            created_at=(base + timedelta(minutes=1)).isoformat(),
        ),
    )

    records = await store.list_suppressed(
        "alpha",
        since=base - timedelta(minutes=1),
    )

    assert [record.task_id for record in records] == [
        "PR-GOOD-TIME",
        "PR-BAD-TIME",
    ]
    assert [record.detail["kind"] for record in records] == ["valid", "malformed"]
    assert records[1].created_at is None


async def test_expired_cause_not_active() -> None:
    redis = _FakeRedis()
    store = RedisSuppressionStore(redis)
    score = datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc).timestamp()
    redis.zsets[index_key("alpha")] = {"PR-EXPIRED": score}
    redis.values.pop(cause_key("alpha", "PR-EXPIRED"), None)

    assert await store.is_suppressed("alpha", "PR-EXPIRED") is None

    records = await store.list_suppressed(
        "alpha",
        since=datetime(2026, 5, 4, 11, 0, tzinfo=timezone.utc),
    )
    assert records == []
    assert "PR-EXPIRED" not in redis.zsets[index_key("alpha")]


async def test_approved_once_round_trips() -> None:
    redis = _FakeRedis()
    store = RedisSuppressionStore(redis)

    await store.suppress(
        "alpha",
        "PR-APPROVED",
        SuppressionReason.GUARDRAIL,
        {"approved_once": True, "rule": "tier-1"},
    )

    record = await store.is_suppressed("alpha", "PR-APPROVED")
    assert record is not None
    assert record.approved_once is True
    assert record.detail == {"approved_once": True, "rule": "tier-1"}
