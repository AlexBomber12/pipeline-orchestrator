"""PR-311b: handle_idle git-bundle backup scheduling tests.

The IDLE handler tracks the last successful backup on a monotonic clock
and fires the next one when ``git_bundle_backup_interval_hours`` worth of
elapsed seconds have passed. The first IDLE cycle after runner start
anchors the clock without firing so the first backup waits a full
interval. Time-based scheduling makes the cadence robust against the
runner's adaptive IDLE slowdown (extended-idle / rate-limit) that swings
``effective_idle_poll_interval`` mid-run. Failures during creation or
pruning are logged but never crash the daemon, and a failed creation
leaves the timestamp untouched so the next IDLE cycle retries.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from src.daemon.handlers import idle as idle_module

from tests.runner import _helpers as h


@pytest.fixture(autouse=True)
def _default_no_merged_branch_api(monkeypatch: pytest.MonkeyPatch) -> None:
    """Match the autouse stub in test_handle_idle.py for consistency."""
    monkeypatch.setattr(
        "src.task_status.gh_pr_get_merged_branches",
        lambda repo, branches: set(),
    )


class _FakeClock:
    """Monotonic clock controllable by tests.

    Each ``advance`` call moves the simulated time forward; the
    ``monotonic`` callable returns the current simulated time. This
    lets tests step ``handle_idle`` through any number of cycles while
    deterministically controlling how much wall time the backup
    scheduler perceives between cycles.
    """

    def __init__(self) -> None:
        self.now = 0.0

    def advance(self, seconds: float) -> None:
        self.now += seconds

    def monotonic(self) -> float:
        return self.now


def _install_fake_clock(monkeypatch: pytest.MonkeyPatch) -> _FakeClock:
    clock = _FakeClock()
    monkeypatch.setattr(idle_module.time, "monotonic", clock.monotonic)
    return clock


def _wire_stable_idle(monkeypatch: pytest.MonkeyPatch) -> None:
    """Wire the GitHub layer so handle_idle reaches the backup block."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )


def _make_backup_runner(
    *,
    enabled: bool = True,
    backup_dir: str | None = "/tmp/test-backup",
    interval_hours: int = 1,
    poll_interval_sec: int = 60,
    daily_retention: int = 7,
    weekly_retention: int = 4,
) -> Any:
    runner = h._make_runner(poll_interval_sec=poll_interval_sec)
    runner.app_config.daemon.git_bundle_backup_enabled = enabled
    runner.app_config.daemon.git_bundle_backup_dir = backup_dir
    runner.app_config.daemon.git_bundle_backup_interval_hours = interval_hours
    runner.app_config.daemon.git_bundle_backup_daily_retention = daily_retention
    runner.app_config.daemon.git_bundle_backup_weekly_retention = weekly_retention
    return runner


def _drive_idle(
    runner: Any,
    times: int,
    clock: _FakeClock | None = None,
    *,
    seconds_per_cycle: float = 0.0,
) -> None:
    for _ in range(times):
        if clock is not None:
            clock.advance(seconds_per_cycle)
        asyncio.run(runner.handle_idle())


def test_handle_idle_backup_disabled_skips_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _wire_stable_idle(monkeypatch)
    clock = _install_fake_clock(monkeypatch)
    create_calls: list[dict[str, Any]] = []

    async def fake_create(**kwargs: Any) -> Path | None:
        create_calls.append(kwargs)
        return Path("/tmp/test-backup/octo__demo/x.bundle")

    monkeypatch.setattr(idle_module, "create_repo_bundle", fake_create)

    runner = _make_backup_runner(enabled=False)
    _drive_idle(runner, 100, clock, seconds_per_cycle=60.0)

    assert create_calls == []


def test_handle_idle_backup_dir_none_skips_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _wire_stable_idle(monkeypatch)
    clock = _install_fake_clock(monkeypatch)
    create_calls: list[dict[str, Any]] = []

    async def fake_create(**kwargs: Any) -> Path | None:
        create_calls.append(kwargs)
        return Path("/tmp/test-backup/octo__demo/x.bundle")

    monkeypatch.setattr(idle_module, "create_repo_bundle", fake_create)

    runner = _make_backup_runner(enabled=True, backup_dir=None)
    _drive_idle(runner, 100, clock, seconds_per_cycle=60.0)

    assert create_calls == []


def test_handle_idle_backup_fires_after_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _wire_stable_idle(monkeypatch)
    clock = _install_fake_clock(monkeypatch)
    create_calls: list[dict[str, Any]] = []
    prune_calls: list[dict[str, Any]] = []
    fake_bundle = Path("/tmp/test-backup/octo__demo/octo__demo-X.bundle")

    async def fake_create(**kwargs: Any) -> Path | None:
        create_calls.append(kwargs)
        return fake_bundle

    async def fake_prune(**kwargs: Any) -> int:
        prune_calls.append(kwargs)
        return 0

    monkeypatch.setattr(idle_module, "create_repo_bundle", fake_create)
    monkeypatch.setattr(idle_module, "prune_old_bundles", fake_prune)

    runner = _make_backup_runner(interval_hours=1, poll_interval_sec=60)

    # First cycle anchors the clock; subsequent cycles advance 60s each
    # and must not fire until elapsed >= 3600s.
    _drive_idle(runner, 1, clock, seconds_per_cycle=0.0)
    assert runner._git_bundle_backup_last_run_at == 0.0

    _drive_idle(runner, 59, clock, seconds_per_cycle=60.0)
    assert create_calls == []

    _drive_idle(runner, 1, clock, seconds_per_cycle=60.0)
    assert len(create_calls) == 1
    assert create_calls[0] == {
        "repo_path": runner.repo_path,
        "repo_name": runner.name,
        "backup_dir": "/tmp/test-backup",
    }
    assert runner._git_bundle_backup_last_run_at == 3600.0

    _drive_idle(runner, 1, clock, seconds_per_cycle=60.0)
    assert len(create_calls) == 1


def test_handle_idle_backup_create_failure_logs_and_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _wire_stable_idle(monkeypatch)
    clock = _install_fake_clock(monkeypatch)
    prune_calls: list[dict[str, Any]] = []

    async def fake_create(**kwargs: Any) -> Path | None:
        return None

    async def fake_prune(**kwargs: Any) -> int:
        prune_calls.append(kwargs)
        return 0

    monkeypatch.setattr(idle_module, "create_repo_bundle", fake_create)
    monkeypatch.setattr(idle_module, "prune_old_bundles", fake_prune)

    runner = _make_backup_runner(interval_hours=1, poll_interval_sec=60)
    _drive_idle(runner, 1, clock, seconds_per_cycle=0.0)
    _drive_idle(runner, 60, clock, seconds_per_cycle=60.0)

    events = [entry["event"] for entry in runner.state.history]
    assert any(
        "[BACKUP] git bundle failed; will retry next cycle" == event
        for event in events
    )
    assert prune_calls == []
    # Timestamp stays anchored at the original value so the next IDLE
    # cycle retries instead of waiting another full interval.
    assert runner._git_bundle_backup_last_run_at == 0.0


def test_handle_idle_backup_create_exception_logs_and_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _wire_stable_idle(monkeypatch)
    clock = _install_fake_clock(monkeypatch)
    prune_calls: list[dict[str, Any]] = []

    async def fake_create(**kwargs: Any) -> Path | None:
        raise RuntimeError("disk gone")

    async def fake_prune(**kwargs: Any) -> int:
        prune_calls.append(kwargs)
        return 0

    monkeypatch.setattr(idle_module, "create_repo_bundle", fake_create)
    monkeypatch.setattr(idle_module, "prune_old_bundles", fake_prune)

    runner = _make_backup_runner(interval_hours=1, poll_interval_sec=60)
    _drive_idle(runner, 1, clock, seconds_per_cycle=0.0)
    _drive_idle(runner, 60, clock, seconds_per_cycle=60.0)

    events = [entry["event"] for entry in runner.state.history]
    assert any(
        "[BACKUP] bundle creation crashed: disk gone" == event
        for event in events
    )
    assert prune_calls == []
    assert runner._git_bundle_backup_last_run_at == 0.0


def test_handle_idle_backup_failure_retries_on_next_cycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: a failed bundle creation must retry on the next IDLE
    cycle instead of waiting another full interval."""
    _wire_stable_idle(monkeypatch)
    clock = _install_fake_clock(monkeypatch)
    create_calls: list[None] = []

    async def fake_create(**kwargs: Any) -> Path | None:
        create_calls.append(None)
        return None

    async def fake_prune(**kwargs: Any) -> int:
        return 0

    monkeypatch.setattr(idle_module, "create_repo_bundle", fake_create)
    monkeypatch.setattr(idle_module, "prune_old_bundles", fake_prune)

    runner = _make_backup_runner(interval_hours=1, poll_interval_sec=60)
    _drive_idle(runner, 1, clock, seconds_per_cycle=0.0)
    _drive_idle(runner, 60, clock, seconds_per_cycle=60.0)
    assert len(create_calls) == 1

    _drive_idle(runner, 1, clock, seconds_per_cycle=60.0)
    assert len(create_calls) == 2


def test_handle_idle_backup_prune_logs_removed_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _wire_stable_idle(monkeypatch)
    clock = _install_fake_clock(monkeypatch)
    fake_bundle = Path("/tmp/test-backup/octo__demo/octo__demo-X.bundle")

    async def fake_create(**kwargs: Any) -> Path | None:
        return fake_bundle

    async def fake_prune(**kwargs: Any) -> int:
        return 3

    monkeypatch.setattr(idle_module, "create_repo_bundle", fake_create)
    monkeypatch.setattr(idle_module, "prune_old_bundles", fake_prune)

    runner = _make_backup_runner(interval_hours=1, poll_interval_sec=60)
    _drive_idle(runner, 1, clock, seconds_per_cycle=0.0)
    _drive_idle(runner, 60, clock, seconds_per_cycle=60.0)

    events = [entry["event"] for entry in runner.state.history]
    assert any(event == "[BACKUP] pruned 3 old bundles" for event in events)


def test_handle_idle_backup_prune_exception_logs_and_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _wire_stable_idle(monkeypatch)
    clock = _install_fake_clock(monkeypatch)
    fake_bundle = Path("/tmp/test-backup/octo__demo/octo__demo-X.bundle")

    async def fake_create(**kwargs: Any) -> Path | None:
        return fake_bundle

    async def fake_prune(**kwargs: Any) -> int:
        raise OSError("permission denied")

    monkeypatch.setattr(idle_module, "create_repo_bundle", fake_create)
    monkeypatch.setattr(idle_module, "prune_old_bundles", fake_prune)

    runner = _make_backup_runner(interval_hours=1, poll_interval_sec=60)
    _drive_idle(runner, 1, clock, seconds_per_cycle=0.0)
    _drive_idle(runner, 60, clock, seconds_per_cycle=60.0)

    events = [entry["event"] for entry in runner.state.history]
    assert any(event.startswith("[BACKUP] prune failed:") for event in events)


def test_handle_idle_backup_long_cadence_honors_elapsed_seconds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 24h backup interval with a 7200s effective cadence must fire
    after ~24h of elapsed wall-clock seconds, regardless of how cycles
    map to seconds.
    """
    _wire_stable_idle(monkeypatch)
    clock = _install_fake_clock(monkeypatch)
    create_calls: list[dict[str, Any]] = []
    fake_bundle = Path("/tmp/test-backup/octo__demo/octo__demo-X.bundle")

    async def fake_create(**kwargs: Any) -> Path | None:
        create_calls.append(kwargs)
        return fake_bundle

    async def fake_prune(**kwargs: Any) -> int:
        return 0

    monkeypatch.setattr(idle_module, "create_repo_bundle", fake_create)
    monkeypatch.setattr(idle_module, "prune_old_bundles", fake_prune)

    runner = _make_backup_runner(interval_hours=24, poll_interval_sec=7200)
    _drive_idle(runner, 1, clock, seconds_per_cycle=0.0)
    # 11 cycles of 7200s = 79200s elapsed, still below the 86400s target.
    _drive_idle(runner, 11, clock, seconds_per_cycle=7200.0)
    assert create_calls == []

    _drive_idle(runner, 1, clock, seconds_per_cycle=7200.0)
    assert len(create_calls) == 1


def test_handle_idle_backup_robust_to_cadence_change_mid_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression for the cycle-counter bug: when the runner switches
    from the base IDLE cadence to the extended-idle cadence partway
    through an hour, the backup must still wait a full configured
    interval of wall time before firing. The previous cycle-counter
    implementation fired early on cycle 12 because early fast cycles
    counted toward a threshold recomputed against the slower cadence.
    """
    _wire_stable_idle(monkeypatch)
    clock = _install_fake_clock(monkeypatch)
    create_calls: list[dict[str, Any]] = []
    fake_bundle = Path("/tmp/test-backup/octo__demo/octo__demo-X.bundle")

    async def fake_create(**kwargs: Any) -> Path | None:
        create_calls.append(kwargs)
        return fake_bundle

    async def fake_prune(**kwargs: Any) -> int:
        return 0

    monkeypatch.setattr(idle_module, "create_repo_bundle", fake_create)
    monkeypatch.setattr(idle_module, "prune_old_bundles", fake_prune)

    runner = _make_backup_runner(interval_hours=1, poll_interval_sec=60)
    # Anchor cycle.
    _drive_idle(runner, 1, clock, seconds_per_cycle=0.0)
    # 3 fast cycles at 60s = 180s of wall time.
    _drive_idle(runner, 3, clock, seconds_per_cycle=60.0)
    # 8 slow cycles at 300s = 2400s; total elapsed = 2580s, below 3600s.
    _drive_idle(runner, 8, clock, seconds_per_cycle=300.0)
    assert create_calls == []

    # 4 more slow cycles at 300s = 1200s; total elapsed = 3780s.
    _drive_idle(runner, 4, clock, seconds_per_cycle=300.0)
    assert len(create_calls) == 1


def test_handle_idle_backup_anchors_on_first_cycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The first IDLE cycle after runner start must anchor the
    monotonic clock and not fire a backup immediately.
    """
    _wire_stable_idle(monkeypatch)
    clock = _install_fake_clock(monkeypatch)
    clock.now = 12345.0
    create_calls: list[None] = []

    async def fake_create(**kwargs: Any) -> Path | None:
        create_calls.append(None)
        return Path("/tmp/test-backup/octo__demo/x.bundle")

    monkeypatch.setattr(idle_module, "create_repo_bundle", fake_create)

    runner = _make_backup_runner(interval_hours=1, poll_interval_sec=60)
    _drive_idle(runner, 1, clock, seconds_per_cycle=0.0)

    assert create_calls == []
    assert runner._git_bundle_backup_last_run_at == 12345.0
