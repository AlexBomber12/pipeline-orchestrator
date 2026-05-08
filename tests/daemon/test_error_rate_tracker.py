from datetime import datetime, timedelta, timezone

import pytest

from src.daemon import error_rate_tracker
from tests.runner._helpers import _FakeRedis


@pytest.mark.asyncio
async def test_record_and_count_within_window() -> None:
    redis = _FakeRedis()
    now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)

    await error_rate_tracker.record(redis, "octo__demo", now - timedelta(minutes=5))
    await error_rate_tracker.record(redis, "octo__demo", now - timedelta(minutes=15))
    await error_rate_tracker.record(redis, "octo__demo", now - timedelta(minutes=30))

    assert (
        await error_rate_tracker.count_recent(
            redis, "octo__demo", 60, now=now
        )
        == 3
    )


@pytest.mark.asyncio
async def test_count_excludes_outside_window() -> None:
    redis = _FakeRedis()
    now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)

    await error_rate_tracker.record(redis, "octo__demo", now - timedelta(minutes=90))

    assert (
        await error_rate_tracker.count_recent(
            redis, "octo__demo", 60, now=now
        )
        == 0
    )


@pytest.mark.asyncio
async def test_count_keeps_event_exactly_at_window_boundary() -> None:
    redis = _FakeRedis()
    now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
    boundary = now - timedelta(minutes=60)

    await error_rate_tracker.record(redis, "octo__demo", boundary)

    assert (
        await error_rate_tracker.count_recent(
            redis, "octo__demo", 60, now=now
        )
        == 1
    )
    assert len(redis.zsets[error_rate_tracker.key("octo__demo")]) == 1


@pytest.mark.asyncio
async def test_count_honors_windows_longer_than_24_hours() -> None:
    redis = _FakeRedis()
    now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)

    await error_rate_tracker.record(redis, "octo__demo", now - timedelta(hours=36))

    assert (
        await error_rate_tracker.count_recent(
            redis, "octo__demo", 48 * 60, now=now
        )
        == 1
    )


@pytest.mark.asyncio
async def test_prune_removes_24h_old_entries() -> None:
    redis = _FakeRedis()
    now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)

    old_ts = (now - timedelta(hours=25)).timestamp()
    await redis.zadd(error_rate_tracker.key("octo__demo"), {"old": old_ts})

    assert await error_rate_tracker.prune(redis, "octo__demo", now=now) == 1
    assert redis.zsets[error_rate_tracker.key("octo__demo")] == {}


@pytest.mark.asyncio
async def test_record_accepts_default_and_naive_timestamps() -> None:
    redis = _FakeRedis()
    naive = datetime(2026, 5, 8, 12, 0)

    await error_rate_tracker.record(redis, "octo__demo")
    await error_rate_tracker.record(redis, "octo__demo", naive)

    assert len(redis.zsets[error_rate_tracker.key("octo__demo")]) == 2


@pytest.mark.asyncio
async def test_record_with_member_id_counts_each_event() -> None:
    redis = _FakeRedis()
    now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)

    await error_rate_tracker.record(
        redis, "octo__demo", now, member_id="PR-123"
    )
    await error_rate_tracker.record(
        redis, "octo__demo", now + timedelta(seconds=1), member_id="PR-123"
    )

    members = redis.zsets[error_rate_tracker.key("octo__demo")]
    assert len(members) == 2
    assert all(member.startswith("PR-123:") for member in members)


@pytest.mark.asyncio
async def test_discard_removes_exact_member_id() -> None:
    redis = _FakeRedis()
    await redis.zadd(error_rate_tracker.key("octo__demo"), {"event-1": 1.0})

    assert await error_rate_tracker.discard(redis, "octo__demo", "event-1") == 1
    assert redis.zsets[error_rate_tracker.key("octo__demo")] == {}


@pytest.mark.asyncio
async def test_last_auto_pause_marker_requires_newer_records() -> None:
    redis = _FakeRedis()
    now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)

    await error_rate_tracker.record(redis, "octo__demo", now)
    await error_rate_tracker.mark_auto_pause(redis, "octo__demo", now=now)

    assert (
        await error_rate_tracker.has_records_after_last_auto_pause(
            redis, "octo__demo"
        )
        is False
    )

    await error_rate_tracker.record(
        redis,
        "octo__demo",
        now + timedelta(seconds=1),
    )

    assert (
        await error_rate_tracker.has_records_after_last_auto_pause(
            redis, "octo__demo"
        )
        is True
    )


@pytest.mark.asyncio
async def test_last_auto_pause_marker_accepts_bytes() -> None:
    redis = _FakeRedis()
    now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
    redis.store[error_rate_tracker.last_auto_pause_key("octo__demo")] = (
        str(now.timestamp()).encode("utf-8")
    )

    await error_rate_tracker.record(
        redis,
        "octo__demo",
        now + timedelta(seconds=1),
    )

    assert (
        await error_rate_tracker.has_records_after_last_auto_pause(
            redis, "octo__demo"
        )
        is True
    )


@pytest.mark.asyncio
async def test_last_auto_pause_marker_fails_open_when_invalid() -> None:
    redis = _FakeRedis()
    redis.store[error_rate_tracker.last_auto_pause_key("octo__demo")] = "invalid"

    assert (
        await error_rate_tracker.has_records_after_last_auto_pause(
            redis, "octo__demo"
        )
        is True
    )
