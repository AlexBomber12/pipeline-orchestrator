"""PR-311b: handle_idle git-bundle backup scheduling tests.

The IDLE handler increments ``_git_bundle_backup_counter`` once per cycle.
When the config gate opens (``git_bundle_backup_enabled`` and
``git_bundle_backup_dir`` set) and the counter reaches ``interval_cycles``,
the runner awaits ``create_repo_bundle`` and then ``prune_old_bundles``.
Failures in either step are logged but never crash the daemon.
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
    # poll_interval_sec drives the repo_config field because the IDLE
    # cadence comes from ``effective_idle_poll_interval``, which reads
    # ``repo_config.poll_interval_sec`` (not ``daemon.poll_interval_sec``).
    runner = h._make_runner(poll_interval_sec=poll_interval_sec)
    runner.app_config.daemon.git_bundle_backup_enabled = enabled
    runner.app_config.daemon.git_bundle_backup_dir = backup_dir
    runner.app_config.daemon.git_bundle_backup_interval_hours = interval_hours
    runner.app_config.daemon.git_bundle_backup_daily_retention = daily_retention
    runner.app_config.daemon.git_bundle_backup_weekly_retention = weekly_retention
    return runner


def _drive_idle(runner: Any, times: int) -> None:
    for _ in range(times):
        asyncio.run(runner.handle_idle())


def test_handle_idle_backup_disabled_skips_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _wire_stable_idle(monkeypatch)
    create_calls: list[dict[str, Any]] = []

    async def fake_create(**kwargs: Any) -> Path | None:
        create_calls.append(kwargs)
        return Path("/tmp/test-backup/octo__demo/x.bundle")

    monkeypatch.setattr(idle_module, "create_repo_bundle", fake_create)

    runner = _make_backup_runner(enabled=False)
    _drive_idle(runner, 100)

    assert create_calls == []


def test_handle_idle_backup_dir_none_skips_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _wire_stable_idle(monkeypatch)
    create_calls: list[dict[str, Any]] = []

    async def fake_create(**kwargs: Any) -> Path | None:
        create_calls.append(kwargs)
        return Path("/tmp/test-backup/octo__demo/x.bundle")

    monkeypatch.setattr(idle_module, "create_repo_bundle", fake_create)

    runner = _make_backup_runner(enabled=True, backup_dir=None)
    _drive_idle(runner, 100)

    assert create_calls == []


def test_handle_idle_backup_fires_at_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _wire_stable_idle(monkeypatch)
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

    runner = _make_backup_runner(
        interval_hours=1, poll_interval_sec=60
    )
    # cycles_per_hour = 60; interval_cycles = 60.
    _drive_idle(runner, 59)
    assert create_calls == []

    _drive_idle(runner, 1)
    assert len(create_calls) == 1
    assert create_calls[0] == {
        "repo_path": runner.repo_path,
        "repo_name": runner.name,
        "backup_dir": "/tmp/test-backup",
    }
    assert runner._git_bundle_backup_counter == 0

    _drive_idle(runner, 1)
    assert len(create_calls) == 1


def test_handle_idle_backup_create_failure_logs_and_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _wire_stable_idle(monkeypatch)
    prune_calls: list[dict[str, Any]] = []

    async def fake_create(**kwargs: Any) -> Path | None:
        return None

    async def fake_prune(**kwargs: Any) -> int:
        prune_calls.append(kwargs)
        return 0

    monkeypatch.setattr(idle_module, "create_repo_bundle", fake_create)
    monkeypatch.setattr(idle_module, "prune_old_bundles", fake_prune)

    runner = _make_backup_runner(interval_hours=1, poll_interval_sec=60)
    _drive_idle(runner, 60)

    events = [entry["event"] for entry in runner.state.history]
    assert any(
        "[BACKUP] git bundle failed; will retry next cycle" == event
        for event in events
    )
    assert prune_calls == []
    # Counter must stay at threshold so the next IDLE cycle retries.
    assert runner._git_bundle_backup_counter == 60


def test_handle_idle_backup_create_exception_logs_and_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _wire_stable_idle(monkeypatch)
    prune_calls: list[dict[str, Any]] = []

    async def fake_create(**kwargs: Any) -> Path | None:
        raise RuntimeError("disk gone")

    async def fake_prune(**kwargs: Any) -> int:
        prune_calls.append(kwargs)
        return 0

    monkeypatch.setattr(idle_module, "create_repo_bundle", fake_create)
    monkeypatch.setattr(idle_module, "prune_old_bundles", fake_prune)

    runner = _make_backup_runner(interval_hours=1, poll_interval_sec=60)
    _drive_idle(runner, 60)

    events = [entry["event"] for entry in runner.state.history]
    assert any(
        "[BACKUP] bundle creation crashed: disk gone" == event
        for event in events
    )
    assert prune_calls == []
    # Counter must stay at threshold so the next IDLE cycle retries.
    assert runner._git_bundle_backup_counter == 60


def test_handle_idle_backup_failure_retries_on_next_cycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: a failed bundle creation must retry on the next IDLE
    cycle instead of waiting another full ``interval_cycles``."""
    _wire_stable_idle(monkeypatch)
    create_calls: list[None] = []

    async def fake_create(**kwargs: Any) -> Path | None:
        create_calls.append(None)
        return None

    async def fake_prune(**kwargs: Any) -> int:
        return 0

    monkeypatch.setattr(idle_module, "create_repo_bundle", fake_create)
    monkeypatch.setattr(idle_module, "prune_old_bundles", fake_prune)

    runner = _make_backup_runner(interval_hours=1, poll_interval_sec=60)
    # cycles_per_hour = 60; interval_cycles = 60.
    _drive_idle(runner, 60)
    assert len(create_calls) == 1
    assert runner._git_bundle_backup_counter == 60

    _drive_idle(runner, 1)
    assert len(create_calls) == 2
    assert runner._git_bundle_backup_counter == 61


def test_handle_idle_backup_prune_logs_removed_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _wire_stable_idle(monkeypatch)
    fake_bundle = Path("/tmp/test-backup/octo__demo/octo__demo-X.bundle")

    async def fake_create(**kwargs: Any) -> Path | None:
        return fake_bundle

    async def fake_prune(**kwargs: Any) -> int:
        return 3

    monkeypatch.setattr(idle_module, "create_repo_bundle", fake_create)
    monkeypatch.setattr(idle_module, "prune_old_bundles", fake_prune)

    runner = _make_backup_runner(interval_hours=1, poll_interval_sec=60)
    _drive_idle(runner, 60)

    events = [entry["event"] for entry in runner.state.history]
    assert any(event == "[BACKUP] pruned 3 old bundles" for event in events)


def test_handle_idle_backup_prune_exception_logs_and_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _wire_stable_idle(monkeypatch)
    fake_bundle = Path("/tmp/test-backup/octo__demo/octo__demo-X.bundle")

    async def fake_create(**kwargs: Any) -> Path | None:
        return fake_bundle

    async def fake_prune(**kwargs: Any) -> int:
        raise OSError("permission denied")

    monkeypatch.setattr(idle_module, "create_repo_bundle", fake_create)
    monkeypatch.setattr(idle_module, "prune_old_bundles", fake_prune)

    runner = _make_backup_runner(interval_hours=1, poll_interval_sec=60)
    _drive_idle(runner, 60)

    events = [entry["event"] for entry in runner.state.history]
    assert any(event.startswith("[BACKUP] prune failed:") for event in events)


def test_handle_idle_backup_uses_effective_idle_poll_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cadence must scale with the runner's IDLE interval, not the
    daemon-level ``poll_interval_sec``. With a 300s effective IDLE cadence
    and a 1h interval, the backup must fire after 12 cycles — not 60.
    """
    _wire_stable_idle(monkeypatch)
    create_calls: list[dict[str, Any]] = []
    fake_bundle = Path("/tmp/test-backup/octo__demo/octo__demo-X.bundle")

    async def fake_create(**kwargs: Any) -> Path | None:
        create_calls.append(kwargs)
        return fake_bundle

    async def fake_prune(**kwargs: Any) -> int:
        return 0

    monkeypatch.setattr(idle_module, "create_repo_bundle", fake_create)
    monkeypatch.setattr(idle_module, "prune_old_bundles", fake_prune)

    runner = _make_backup_runner(interval_hours=1, poll_interval_sec=300)
    # cycles_per_hour = 3600 // 300 = 12; interval_cycles = 12.
    # If the implementation used daemon.poll_interval_sec (60s default
    # from DaemonConfig), it would expect 60 cycles instead.
    _drive_idle(runner, 11)
    assert create_calls == []

    _drive_idle(runner, 1)
    assert len(create_calls) == 1


def test_handle_idle_backup_long_cadence_scales_by_elapsed_seconds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: when the effective IDLE cadence exceeds 3600s, the
    schedule must derive cycles from elapsed seconds rather than a
    floor-based cycles-per-hour. A 24h interval with a 7200s cadence must
    fire every 12 cycles (12 * 7200s = 24h), not every 24 cycles (which
    would stretch the cadence to 48h).
    """
    _wire_stable_idle(monkeypatch)
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
    # ceil(24 * 3600 / 7200) = 12 cycles.
    _drive_idle(runner, 11)
    assert create_calls == []

    _drive_idle(runner, 1)
    assert len(create_calls) == 1


def test_handle_idle_backup_counter_persists_across_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _wire_stable_idle(monkeypatch)

    async def fake_create(**kwargs: Any) -> Path | None:
        return None

    async def fake_prune(**kwargs: Any) -> int:
        return 0

    monkeypatch.setattr(idle_module, "create_repo_bundle", fake_create)
    monkeypatch.setattr(idle_module, "prune_old_bundles", fake_prune)

    runner = _make_backup_runner(enabled=False)
    _drive_idle(runner, 30)

    assert runner._git_bundle_backup_counter == 30
