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


@pytest.fixture
async def event_redis():
    """Use an isolated Redis namespace when a component-test URL is supplied."""
    import os
    import uuid

    from redis.asyncio import Redis
    from tests.test_cancellation_detection import _FakePipeline

    url = os.environ.get("ERROR_EVENT_TEST_REDIS_URL")
    if url:
        redis = Redis.from_url(url, decode_responses=True)
    else:
        redis = _FakeRedis()
        redis.values = redis.store
        redis.pipeline = lambda: _FakePipeline(redis)
    repo = f"error-events-{uuid.uuid4().hex}"
    try:
        yield redis, repo
    finally:
        if url:
            # Only this fixture's namespace; never flush a shared database.
            keys = [key async for key in redis.scan_iter(match=f"*{repo}*")]
            if keys:
                await redis.delete(*keys)
            await redis.aclose()


async def test_replayed_failures_survive_restart_resume_and_window_expiry(event_redis, monkeypatch):
    from src.cancellation import (
        CancellationCause,
        cause_key,
        get_cancellation_cause,
        record_current_run_started_at,
        retry_count_key,
        safe_record_cancellation_cause,
    )
    from src.cancellation.availability import AvailabilityState
    from src.daemon import runner as runner_module
    from src.models import PipelineState
    from src.subsource_registry import SuppressionReason
    from src.suppression.redis_store import RedisSuppressionStore
    from tests.runner.test_auto_pause import _runner

    redis, repo = event_redis
    runner = _runner(error_rate_threshold=5, error_rate_window_min=60)
    runner.redis, runner.name = redis, repo
    availability = AvailabilityState.AVAILABLE

    async def available(_sources):
        return availability

    monkeypatch.setattr(runner_module, "is_operator_available", available)
    now = datetime.now(timezone.utc)
    retry_key = retry_count_key(repo, "PR-1")
    await redis.set(retry_key, "3")
    await record_current_run_started_at(redis, repo, "PR-1", now - timedelta(seconds=1))
    original = CancellationCause(category="ERROR", created_at=now.isoformat())
    await safe_record_cancellation_cause(redis, repo, "PR-1", original)
    for _ in range(8):
        # Recreate the service and cause just as after daemon recovery.
        restored = await get_cancellation_cause(redis, repo, "PR-1", refresh_ttl=False)
        await safe_record_cancellation_cause(redis, repo, "PR-1", restored)
        await RedisSuppressionStore(redis).suppress(
            repo, "PR-1", SuppressionReason.CRASH, {"error_message": "updated reason"}
        )
    assert await error_rate_tracker.count_recent(redis, repo, 60, now=now) == 1
    assert await redis.get(retry_key) == "3"
    assert not await runner._maybe_auto_pause_for_error_rate()

    # Four more failures of the SAME task must still reach the real threshold.
    for index in range(1, 5):
        started = now + timedelta(seconds=index * 2)
        await record_current_run_started_at(redis, repo, "PR-1", started)
        cause = CancellationCause(category="ERROR", created_at=(started + timedelta(seconds=1)).isoformat())
        await safe_record_cancellation_cause(redis, repo, "PR-1", cause)
        await RedisSuppressionStore(redis).suppress(repo, "PR-1", SuppressionReason.CRASH, {})
    later = now + timedelta(seconds=20)
    assert await error_rate_tracker.count_recent(redis, repo, 60, now=later) == 5
    timestamp = error_rate_tracker._timestamp
    monkeypatch.setattr(error_rate_tracker, "_timestamp", lambda ts=None: timestamp(later if ts is None else ts))
    availability = AvailabilityState.AWAY
    assert not await runner._maybe_auto_pause_for_error_rate()
    availability = AvailabilityState.AVAILABLE
    assert await runner._maybe_auto_pause_for_error_rate()
    assert runner.state.state is PipelineState.PAUSED
    assert runner.state.user_paused
    runner.state.state = PipelineState.IDLE
    runner.state.user_paused = False
    # Resume preserves history, but old cause replays cannot repause the repo.
    latest = await get_cancellation_cause(redis, repo, "PR-1", refresh_ttl=False)
    await safe_record_cancellation_cause(redis, repo, "PR-1", latest)
    assert not await error_rate_tracker.has_records_after_last_auto_pause(redis, repo)
    assert not await runner._maybe_auto_pause_for_error_rate()
    assert await error_rate_tracker.count_recent(redis, repo, 60, now=later) == 5

    # Simulate cause-key expiry while replaying a serialized original event.
    await redis.delete(cause_key(repo, "PR-1"))
    await safe_record_cancellation_cause(redis, repo, "PR-1", original)
    assert await error_rate_tracker.count_recent(redis, repo, 60, now=later) == 5
    expired = now + timedelta(hours=2)
    assert await error_rate_tracker.count_recent(redis, repo, 60, now=expired) == 0
    await safe_record_cancellation_cause(redis, repo, "PR-1", original)
    assert await error_rate_tracker.count_recent(redis, repo, 60, now=expired) == 0
    assert not await error_rate_tracker.has_records_after_last_auto_pause(redis, repo)
    assert await redis.get(retry_key) == "3"


@pytest.mark.parametrize("dispatch", [None, -1, 1])
async def test_fresh_cause_uses_existing_failure_until_next_dispatch(event_redis, dispatch):
    from src.cancellation import (
        CancellationCause,
        get_cancellation_cause,
        record_cancellation_cause,
        record_current_run_started_at,
        safe_delete_cancellation_cause,
        safe_record_cancellation_cause,
    )

    redis, repo = event_redis
    # Include legacy naive timestamps in the update path.
    old = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=1)
    await record_cancellation_cause(
        redis, repo, "PR-1", CancellationCause(category="ERROR", created_at=old.isoformat())
    )
    if dispatch is not None:
        await record_current_run_started_at(redis, repo, "PR-1", old + timedelta(seconds=dispatch))
    fresh = CancellationCause(category="ERROR", payload={"subsource": "crash"})
    await safe_record_cancellation_cause(redis, repo, "PR-1", fresh)
    assert (fresh.created_at == old.isoformat()) is (dispatch != 1)
    stored = await get_cancellation_cause(redis, repo, "PR-1", refresh_ttl=False)
    assert stored.payload == fresh.payload
    await safe_delete_cancellation_cause(redis, repo, "PR-1")
    retry = CancellationCause(category="ERROR")
    await safe_record_cancellation_cause(redis, repo, "PR-1", retry)
    assert retry.created_at != fresh.created_at
    assert await error_rate_tracker.count_recent(redis, repo, 60) == 2
