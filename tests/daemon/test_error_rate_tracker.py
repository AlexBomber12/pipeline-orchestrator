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
