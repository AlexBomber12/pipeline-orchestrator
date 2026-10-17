from __future__ import annotations

import asyncio
import types
from collections.abc import Iterator
from typing import Any

import pytest
from src.daemon import fix_supervision


class _Target:
    def __init__(self) -> None:
        self.cancelled = False

    def cancel(self) -> None:
        self.cancelled = True


class _Runner:
    owner_repo = "owner/repo"

    def __init__(self) -> None:
        self.events: list[str] = []
        self.cleanup_calls = 0
        self.state = types.SimpleNamespace(coder="claude")

    def log_event(self, message: str) -> None:
        self.events.append(message)

    async def _terminate_current_coder(self) -> bool:
        self.cleanup_calls += 1
        return True


class _StopMonitor(Exception):
    pass


class _ObservedNow(float):
    def __new__(cls, value: float, subtractions: list[float]) -> "_ObservedNow":
        obj = float.__new__(cls, value)
        obj.subtractions = subtractions
        return obj

    def __sub__(self, other: object) -> float:
        self.subtractions.append(float(other))
        return float(self) - float(other)


async def _direct_to_thread(func: Any, *args: object, **kwargs: object) -> Any:
    return func(*args, **kwargs)


def _patch_idle_monitor(
    monkeypatch: pytest.MonkeyPatch,
    *,
    head_age: float | None,
    monotonic_values: Iterator[float],
    sleep,
) -> None:
    monkeypatch.setattr(
        fix_supervision.gh_prs,
        "get_branch_last_push_time",
        lambda repo, pr_number: None,
    )
    monkeypatch.setattr(
        fix_supervision.gh_prs,
        "get_last_push_age_seconds",
        lambda repo, pr_number: (_ for _ in ()).throw(
            AssertionError(f"push age {head_age} must not affect the FIX deadline")
        ),
    )
    monkeypatch.setattr(fix_supervision.asyncio, "to_thread", _direct_to_thread)
    monkeypatch.setattr(fix_supervision.asyncio, "sleep", sleep)
    monkeypatch.setattr(
        fix_supervision,
        "time",
        types.SimpleNamespace(monotonic=lambda: next(monotonic_values)),
    )


async def _run_monitor(idle_limit: int) -> tuple[_Runner, _Target, dict[str, bool]]:
    runner = _Runner()
    target = _Target()
    idle_flag = {"timed_out": False}
    await fix_supervision.monitor_fix_idle(
        runner,
        pr_number=5,
        idle_limit=idle_limit,
        target=target,  # type: ignore[arg-type]
        idle_flag=idle_flag,
    )
    return runner, target, idle_flag


def test_idle_baseline_previous_push_22_minutes_ago_gets_full_hour(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    subtractions: list[float] = []
    monotonic_values = iter([_ObservedNow(100.0, subtractions), 3700.0])

    async def sleep(_delay: float) -> None:
        return None

    _patch_idle_monitor(
        monkeypatch,
        head_age=1320.0,
        monotonic_values=monotonic_values,
        sleep=sleep,
    )

    runner, target, idle_flag = asyncio.run(_run_monitor(idle_limit=3600))

    assert subtractions == []
    assert idle_flag["timed_out"] is True
    assert target.cancelled is True
    assert runner.cleanup_calls == 1
    assert runner.events == [
        "[FIX] idle timeout (3600s since invocation start or last observed push), killing."
    ]


def test_idle_baseline_recent_push_does_not_backdate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    subtractions: list[float] = []
    monotonic_values = iter([_ObservedNow(1000.0, subtractions), 1060.0])
    sleep_calls = 0

    async def sleep(_delay: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls > 1:
            raise _StopMonitor

    _patch_idle_monitor(
        monkeypatch,
        head_age=300.0,
        monotonic_values=monotonic_values,
        sleep=sleep,
    )

    with pytest.raises(_StopMonitor):
        asyncio.run(_run_monitor(idle_limit=1800))

    assert subtractions == []
    assert sleep_calls == 2


def test_idle_baseline_large_head_age_starts_fresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    subtractions: list[float] = []
    monotonic_values = iter([_ObservedNow(100.0, subtractions), 160.0, 220.0])
    sleep_calls = 0

    async def sleep(_delay: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1

    _patch_idle_monitor(
        monkeypatch,
        head_age=28800.0,
        monotonic_values=monotonic_values,
        sleep=sleep,
    )

    runner, target, idle_flag = asyncio.run(_run_monitor(idle_limit=120))

    assert subtractions == []
    assert sleep_calls == 2
    assert idle_flag["timed_out"] is True
    assert target.cancelled is True
    assert runner.cleanup_calls == 1
    assert runner.events == [
        "[FIX] idle timeout (120s since invocation start or last observed push), killing."
    ]


def test_idle_baseline_none_head_age_starts_fresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    subtractions: list[float] = []
    monotonic_values = iter([_ObservedNow(100.0, subtractions), 220.0])

    async def sleep(_delay: float) -> None:
        return None

    _patch_idle_monitor(
        monkeypatch,
        head_age=None,
        monotonic_values=monotonic_values,
        sleep=sleep,
    )

    runner, target, idle_flag = asyncio.run(_run_monitor(idle_limit=120))

    assert subtractions == []
    assert idle_flag["timed_out"] is True
    assert target.cancelled is True
    assert runner.cleanup_calls == 1
    assert runner.events == [
        "[FIX] idle timeout (120s since invocation start or last observed push), killing."
    ]


def test_pr_321_regression_does_not_kill_within_minute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reproduces 2026-05-15 production incident where post-PR-321 head_age
    values 6+ hours large made FIX iterations get killed within 60 seconds of
    spawn instead of getting a fresh 30-minute window.
    """

    subtractions: list[float] = []
    monotonic_values = iter([_ObservedNow(100.0, subtractions), 160.0])
    runner = _Runner()
    target = _Target()
    idle_flag = {"timed_out": False}
    sleep_calls = 0

    async def sleep(_delay: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls > 1:
            raise _StopMonitor

    _patch_idle_monitor(
        monkeypatch,
        head_age=28000.0,
        monotonic_values=monotonic_values,
        sleep=sleep,
    )

    with pytest.raises(_StopMonitor):
        asyncio.run(
            fix_supervision.monitor_fix_idle(
                runner,
                pr_number=5,
                idle_limit=1800,
                target=target,  # type: ignore[arg-type]
                idle_flag=idle_flag,
            )
        )

    assert subtractions == []
    assert sleep_calls == 2
    assert idle_flag["timed_out"] is False
    assert target.cancelled is False
    assert runner.events == []


def test_recreated_monitor_keeps_same_invocation_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monotonic_values = iter([100.0, 160.0, 170.0, 220.0])
    sleep_calls = 0

    async def sleep(_delay: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 2:
            raise _StopMonitor

    _patch_idle_monitor(
        monkeypatch,
        head_age=None,
        monotonic_values=monotonic_values,
        sleep=sleep,
    )
    runner, target = _Runner(), _Target()
    idle_flag: dict[str, bool | float] = {"timed_out": False}

    async def monitor() -> None:
        await fix_supervision.monitor_fix_idle(
            runner, 5, 120, target, idle_flag,  # type: ignore[arg-type]
        )

    with pytest.raises(_StopMonitor):
        asyncio.run(monitor())
    asyncio.run(monitor())

    assert idle_flag["started_at"] == 100.0
    assert idle_flag["deadline"] == 220.0
    assert idle_flag["timed_out"] is True
    assert runner.cleanup_calls == 1


def test_new_push_resets_deadline_and_unchanged_head_does_not(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monotonic_values = iter([100.0, 160.0, 220.0, 280.0, 340.0])

    async def sleep(_delay: float) -> None:
        return None

    _patch_idle_monitor(
        monkeypatch,
        head_age=None,
        monotonic_values=monotonic_values,
        sleep=sleep,
    )
    observations = iter([None, None, 50.0, None, None])
    monkeypatch.setattr(
        fix_supervision.gh_prs,
        "get_branch_last_push_time",
        lambda repo, pr_number: next(observations),
    )

    runner, target, idle_flag = asyncio.run(_run_monitor(idle_limit=120))

    assert idle_flag["deadline"] == 340.0
    assert runner.events.count("[FIX] [claude] pushed, resetting idle timer.") == 1
    assert target.cancelled is True
    assert runner.cleanup_calls == 1
