from datetime import datetime, timedelta, timezone

import pytest

from src.daemon import error_rate_tracker
from src.daemon import runner as runner_module
from src.daemon.runner import PipelineRunner
from src.keyspace import pipeline_state
from src.models import PipelineState, RepoState
from tests.runner._helpers import (
    _FakeRedis,
    _app_cfg,
    _repo_cfg,
    _usage_providers,
)


def _runner(**daemon_overrides: object) -> PipelineRunner:
    claude_provider, codex_provider = _usage_providers()
    redis = _FakeRedis()
    return PipelineRunner(
        _repo_cfg(),
        _app_cfg(**daemon_overrides),
        redis,
        claude_provider,
        codex_provider,
    )


async def _record_errors(
    runner: PipelineRunner,
    count: int,
    *,
    now: datetime,
) -> None:
    for index in range(count):
        await error_rate_tracker.record(
            runner.redis,
            runner.name,
            now - timedelta(minutes=index),
        )


@pytest.mark.asyncio
async def test_auto_pause_triggers_in_available_mode_at_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(error_rate_threshold=5, error_rate_window_min=60)
    runner.redis.store["operator_override"] = "AVAILABLE"
    await runner.publish_state()
    now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
    await _record_errors(runner, 5, now=now)
    monkeypatch.setattr(error_rate_tracker, "_timestamp", lambda ts=None: now.timestamp())

    assert await runner._maybe_auto_pause_for_error_rate() is True
    assert runner.state.state is PipelineState.PAUSED
    assert runner.state.user_paused is True
    persisted = RepoState.model_validate_json(
        runner.redis.store[pipeline_state(runner.name)]
    )
    assert persisted.user_paused is True
    assert any("[AUTO-PAUSE]" in entry["event"] for entry in runner.state.history)


@pytest.mark.asyncio
async def test_auto_pause_does_not_retrigger_after_resume_without_new_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(error_rate_threshold=5, error_rate_window_min=60)
    runner.redis.store["operator_override"] = "AVAILABLE"
    now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
    current = now

    await _record_errors(runner, 5, now=now)
    monkeypatch.setattr(
        error_rate_tracker,
        "_timestamp",
        lambda ts=None: current.timestamp()
        if ts is None
        else ts.timestamp()
        if isinstance(ts, datetime)
        else float(ts),
    )

    assert await runner._maybe_auto_pause_for_error_rate() is True

    runner.state.state = PipelineState.IDLE
    runner.state.user_paused = False
    assert await runner._maybe_auto_pause_for_error_rate() is False
    assert runner.state.state is PipelineState.IDLE

    current = now + timedelta(seconds=1)
    await error_rate_tracker.record(runner.redis, runner.name, current)

    assert await runner._maybe_auto_pause_for_error_rate() is True


@pytest.mark.asyncio
async def test_no_auto_pause_in_away_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(error_rate_threshold=5, error_rate_window_min=60)
    runner.redis.store["operator_override"] = "AWAY"
    now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
    await _record_errors(runner, 5, now=now)
    monkeypatch.setattr(error_rate_tracker, "_timestamp", lambda ts=None: now.timestamp())

    assert await runner._maybe_auto_pause_for_error_rate() is False
    assert runner.state.state is PipelineState.IDLE


@pytest.mark.asyncio
async def test_no_auto_pause_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(
        error_rate_threshold=5,
        error_rate_window_min=60,
        error_rate_auto_pause_enabled=False,
    )
    runner.redis.store["operator_override"] = "AVAILABLE"
    now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
    await _record_errors(runner, 5, now=now)
    monkeypatch.setattr(error_rate_tracker, "_timestamp", lambda ts=None: now.timestamp())

    assert await runner._maybe_auto_pause_for_error_rate() is False
    assert runner.state.state is PipelineState.IDLE


@pytest.mark.asyncio
async def test_auto_pause_threshold_not_reached_below_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(error_rate_threshold=5, error_rate_window_min=60)
    runner.redis.store["operator_override"] = "AVAILABLE"
    now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
    await _record_errors(runner, 4, now=now)
    monkeypatch.setattr(error_rate_tracker, "_timestamp", lambda ts=None: now.timestamp())

    assert await runner._maybe_auto_pause_for_error_rate() is False
    assert runner.state.state is PipelineState.IDLE


@pytest.mark.asyncio
async def test_auto_pause_treats_availability_source_failure_as_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(error_rate_threshold=5, error_rate_window_min=60)
    now = datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc)
    await _record_errors(runner, 5, now=now)
    monkeypatch.setattr(error_rate_tracker, "_timestamp", lambda ts=None: now.timestamp())

    async def fail_availability(_sources: object) -> object:
        raise RuntimeError("availability down")

    monkeypatch.setattr(runner_module, "is_operator_available", fail_availability)

    assert await runner._maybe_auto_pause_for_error_rate() is True
    assert runner.state.state is PipelineState.PAUSED


@pytest.mark.asyncio
async def test_auto_pause_tracker_read_failure_does_not_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(error_rate_threshold=5, error_rate_window_min=60)
    runner.redis.store["operator_override"] = "AVAILABLE"

    async def fail_count(*_args: object, **_kwargs: object) -> int:
        raise RuntimeError("tracker down")

    monkeypatch.setattr(error_rate_tracker, "count_recent", fail_count)

    assert await runner._maybe_auto_pause_for_error_rate() is False
    assert runner.state.state is PipelineState.IDLE


@pytest.mark.asyncio
async def test_run_cycle_returns_after_error_rate_auto_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(error_rate_threshold=5, error_rate_window_min=60)
    runner._recovered = True

    async def noop() -> None:
        return None

    async def yes() -> bool:
        return True

    async def auto_pause() -> bool:
        runner.state.state = PipelineState.PAUSED
        return True

    async def fail_handle_idle() -> None:
        raise AssertionError("handle_idle should not run after auto-pause")

    monkeypatch.setattr(runner, "ensure_repo_cloned", noop)
    monkeypatch.setattr(runner, "_check_github_api_budget", yes)
    monkeypatch.setattr(runner, "preflight", yes)
    monkeypatch.setattr(runner, "reload_repo_config_if_dirty", noop)
    monkeypatch.setattr(runner, "_maybe_auto_pause_for_error_rate", auto_pause)
    monkeypatch.setattr(runner, "handle_idle", fail_handle_idle)

    await runner._run_cycle_body()

    assert runner.state.state is PipelineState.PAUSED
