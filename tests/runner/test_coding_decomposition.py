"""PR-229b: tests for handle_coding decomposition helpers.

Verifies the three async helpers extracted from ``handle_coding``:

- ``_prepare_coder_invocation``: breach env allocation and kwargs
  build. (Auth refresh, run-record start, rate-limit gate, and branch
  guard all happen in ``handle_coding`` before this helper.)
- ``_run_coder_with_supervision``: subprocess plus stop and breach
  monitors; resolves user-stop and breach pauses.
- ``_post_coder_resolution``: CLI log save, exit classification, PR
  lookup or daemon-side PR creation, run record save.
"""

from __future__ import annotations

import asyncio
import threading
import types
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from src import codex_cli
from src.coders.codex import CodexPlugin
from src.daemon.handlers import CoderUnavailable
from src.daemon.handlers import coding as coding_module
from src.github.prs import BranchPublication
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus
from src.process_supervisor import (
    CleanupResult,
    CleanupStatus,
    ProcessRunResult,
    ProcessSupervisionError,
    SupervisedProcess,
)

from tests.runner import _helpers as h


def _runner_with_task(monkeypatch: pytest.MonkeyPatch):
    """Return a runner with a current task and a no-op subprocess fake."""
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="Sample task",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )
    runner._post_codex_review = lambda pr_number: True  # type: ignore[method-assign]
    return runner


def _publication(
    number: int,
    *,
    sha: str = "a" * 40,
    state: str = "OPEN",
    is_draft: bool = False,
    created_at: datetime | None = None,
) -> BranchPublication:
    return BranchPublication(
        number=number,
        title="PR-001: Sample task",
        base_branch="main",
        head_branch="pr-001",
        head_sha=sha,
        state=state,
        is_draft=is_draft,
        is_cross_repository=False,
        created_at=created_at
        or datetime.now(timezone.utc) + timedelta(seconds=1),
        url=f"https://github.com/octo/demo/pull/{number}",
    )


def _publication_snapshots(
    monkeypatch: pytest.MonkeyPatch,
    *snapshots: list[BranchPublication] | Exception,
) -> list[int]:
    calls: list[int] = []
    local_head = {"value": "a" * 40}

    def fetch(*args: object, **kwargs: object) -> list[BranchPublication]:
        calls.append(len(calls) + 1)
        snapshot = snapshots[min(len(calls) - 1, len(snapshots) - 1)]
        if isinstance(snapshot, Exception):
            raise snapshot
        if snapshot:
            local_head["value"] = snapshot[-1].head_sha
        return snapshot

    monkeypatch.setattr(
        coding_module.gh_prs,
        "get_branch_publications",
        fetch,
    )
    monkeypatch.setattr(
        coding_module,
        "_local_branch_head_sha",
        lambda *args, **kwargs: local_head["value"],
    )
    return calls


class _BoundaryProcess:
    returncode: int | None = None


class _BoundaryManaged:
    def __init__(self, *, quiescent: bool = True) -> None:
        self.process = _BoundaryProcess()
        self.quiescent = quiescent
        self.run_calls = 0
        self.cleanup_calls: list[tuple[float, float]] = []

    async def cleanup(
        self, *, term_grace: float, kill_grace: float
    ) -> object:
        self.cleanup_calls.append((term_grace, kill_grace))
        if self.quiescent:
            self.process.returncode = -15
        return types.SimpleNamespace(
            quiescent=self.quiescent,
            detail=None if self.quiescent else "owned publication child still live",
        )


class _AdapterReader:
    def __init__(self, *reads: bytes | BaseException) -> None:
        self.reads = iter(reads)

    async def read(self, _size: int) -> bytes:
        value = next(self.reads, b"")
        if isinstance(value, BaseException):
            raise value
        return value


class _AdapterProcess:
    def __init__(self, *, fail_output: bool) -> None:
        self.returncode: int | None = 0
        stdout_reads: tuple[bytes | BaseException, ...] = (
            (b"partial output", OSError("reader broke"))
            if fail_output
            else (b"partial output",)
        )
        self.stdout = _AdapterReader(*stdout_reads)
        self.stderr = _AdapterReader(b"provider diagnostic")


class _AdapterManaged(SupervisedProcess):
    def __init__(self, *, fail_output: bool, clear_returncode: bool) -> None:
        self._process = _AdapterProcess(fail_output=fail_output)
        self._supervision_failure = None
        self.clear_returncode = clear_returncode

    async def cleanup(
        self, *, term_grace: float, kill_grace: float
    ) -> CleanupResult:
        if self.clear_returncode:
            self.process.returncode = None
        return CleanupResult(
            CleanupStatus.QUIESCENT,
            None if self.clear_returncode else 0,
            False,
            False,
        )


def _install_waiting_cli(
    monkeypatch: pytest.MonkeyPatch,
    plugin: object,
    managed: _BoundaryManaged,
    *,
    stdout: str = "captured stdout",
    stderr: str = "captured stderr",
    propagate_cancel: bool = False,
) -> None:
    async def cli_waits(
        *args: object, **kwargs: Any
    ) -> tuple[int, str, str]:
        managed.run_calls += 1
        kwargs["on_process_start"](managed.process)
        kwargs["on_supervised_process_start"](managed)
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            if propagate_cancel:
                raise
            return (-15, stdout, stderr)

    monkeypatch.setattr(plugin, "run_auto_pr", cli_waits)


# ---------- _prepare_coder_invocation ----------


def test_prepare_coder_invocation_returns_kwargs_with_breach_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The helper allocates the breach env, stores it on ``self``, and
    returns a kwargs dict carrying ``timeout`` plus ``on_process_start``."""
    runner = _runner_with_task(monkeypatch)
    coder_name, plugin = runner._get_coder()

    monkeypatch.setattr(
        runner, "_breach_env", lambda: ("/tmp/breach", "run-abc")
    )

    kwargs = asyncio.run(runner._prepare_coder_invocation(coder_name, plugin))

    assert runner._current_breach_dir == "/tmp/breach"
    assert runner._current_breach_run_id == "run-abc"
    assert "timeout" in kwargs
    assert kwargs["on_process_start"] == runner._track_current_coder_process
    assert kwargs["on_supervised_process_start"] == (
        runner._track_current_coder_supervised_process
    )


# ---------- _run_coder_with_supervision ----------


def test_run_coder_with_supervision_defers_for_device_login(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_task(monkeypatch)
    coder_name, plugin = runner._get_coder()
    calls: list[str] = []

    async def must_not_run(*_args: Any, **_kwargs: Any) -> tuple[int, str, str]:
        calls.append("run")
        return (0, "", "")

    monkeypatch.setattr(plugin, "run_auto_pr", must_not_run)
    monkeypatch.setattr(
        coding_module.gh_prs, "get_branch_publications", lambda *_args: []
    )
    monkeypatch.setattr(runner, "_reserve_coder_credentials", lambda _name: False)
    runner._current_breach_dir = "/tmp/breach-login"
    runner._current_breach_run_id = "run-login"

    result = asyncio.run(
        runner._run_coder_with_supervision(
            coder_name,
            plugin,
            {},
            target_branch="pr-001",
            current_pr_id="PR-001",
            pr_id="PR-001",
            task_file="tasks/PR-001.md",
            task_body="# PR-001\n",
        )
    )

    assert result is None
    assert calls == []
    assert any("device login" in event["event"] for event in runner.state.history)


def test_run_coder_with_supervision_releases_reservation_on_schedule_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_task(monkeypatch)
    coder_name, plugin = runner._get_coder()
    released: list[bool] = []
    monkeypatch.setattr(plugin, "run_auto_pr", lambda *_args, **_kwargs: (0, "", ""))
    monkeypatch.setattr(
        coding_module.gh_prs, "get_branch_publications", lambda *_args: []
    )
    monkeypatch.setattr(runner, "_reserve_coder_credentials", lambda _name: True)
    monkeypatch.setattr(
        runner, "_release_coder_credentials", lambda: released.append(True)
    )
    runner._current_breach_dir = "/tmp/breach-schedule"
    runner._current_breach_run_id = "run-schedule"

    with pytest.raises(TypeError, match="a coroutine was expected"):
        asyncio.run(
            runner._run_coder_with_supervision(
                coder_name,
                plugin,
                {},
                target_branch="pr-001",
                current_pr_id="PR-001",
                pr_id="PR-001",
                task_file="tasks/PR-001.md",
                task_body="# PR-001\n",
            )
        )

    assert released == [True]
    assert runner._coder_invocation_active is False


def test_run_coder_with_supervision_returns_none_on_stop_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A user stop pressed while the coder runs must short-circuit the
    supervised invocation, return ``None`` to handle_coding, and leave the
    runner in PAUSED state. The PR id is recorded in
    ``_user_stopped_task_pr_ids`` so a later cycle does not auto-resume."""
    runner = _runner_with_task(monkeypatch)
    coder_name, plugin = runner._get_coder()

    async def cli_blocks_until_cancelled(*args: Any, **kwargs: Any) -> tuple[int, str, str]:
        runner._stop_requested = True
        raise asyncio.CancelledError

    monkeypatch.setattr(plugin, "run_auto_pr", cli_blocks_until_cancelled)
    runner._current_breach_dir = "/tmp/breach-stop"
    runner._current_breach_run_id = "run-stop"

    # Avoid filesystem side effects from breach lifecycle.
    monkeypatch.setattr(runner, "_check_late_breach", lambda *a, **kw: None)
    monkeypatch.setattr(runner, "_cleanup_breach_marker", lambda *a, **kw: None)

    result = asyncio.run(
        runner._run_coder_with_supervision(
            coder_name,
            plugin,
            {},
            target_branch="pr-001",
            current_pr_id="PR-001",
            pr_id="PR-001",
            task_file="tasks/PR-001.md",
            task_body="# PR-001\n",
        )
    )

    assert result is None
    assert runner.state.state == PipelineState.PAUSED
    assert "PR-001" in runner._user_stopped_task_pr_ids


def test_run_coder_with_supervision_returns_none_on_breach(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An in-flight rate-limit breach must short-circuit the supervised
    invocation, return ``None``, transition to PAUSED, and tag the run
    record as ``"rate_limit"``."""
    runner = _runner_with_task(monkeypatch)
    coder_name, plugin = runner._get_coder()

    async def cli_breaches(*args: Any, **kwargs: Any) -> tuple[int, str, str]:
        # Simulate the breach monitor flipping the flag and cancelling.
        await asyncio.sleep(0)
        raise asyncio.CancelledError

    async def fake_breach_monitor(
        self, breach_dir, run_id, coder_name, task, flag
    ):
        assert coder_name == "claude"
        flag["breached"] = True
        task.cancel()

    saved: list[str] = []

    async def fake_save(reason: str) -> None:
        saved.append(reason)

    monkeypatch.setattr(plugin, "run_auto_pr", cli_breaches)
    monkeypatch.setattr(
        type(runner), "_monitor_inflight_breach", fake_breach_monitor
    )
    monkeypatch.setattr(runner, "_save_current_run_record", fake_save)
    monkeypatch.setattr(runner, "_check_late_breach", lambda *a, **kw: None)
    monkeypatch.setattr(runner, "_cleanup_breach_marker", lambda *a, **kw: None)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs", lambda *a, **kw: []
    )

    async def _no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("src.daemon.handlers.coding.asyncio.sleep", _no_sleep)

    runner._current_breach_dir = "/tmp/breach"
    runner._current_breach_run_id = "run-breach"

    result = asyncio.run(
        runner._run_coder_with_supervision(
            coder_name,
            plugin,
            {},
            target_branch="pr-001",
            current_pr_id="PR-001",
            pr_id="PR-001",
            task_file="tasks/PR-001.md",
            task_body="# PR-001\n",
        )
    )

    assert result is None
    assert runner.state.state == PipelineState.PAUSED
    assert saved == ["rate_limit"]


def test_run_coder_with_supervision_returns_completion_on_normal_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the coder exits normally the supervised invocation returns the
    ``(code, stdout, stderr)`` tuple unchanged for downstream resolution."""
    runner = _runner_with_task(monkeypatch)
    coder_name, plugin = runner._get_coder()

    async def cli_ok(*args: Any, **kwargs: Any) -> tuple[int, str, str]:
        return (0, "out", "")

    monkeypatch.setattr(plugin, "run_auto_pr", cli_ok)
    monkeypatch.setattr(runner, "_check_late_breach", lambda *a, **kw: None)
    monkeypatch.setattr(runner, "_cleanup_breach_marker", lambda *a, **kw: None)
    runner._current_breach_dir = "/tmp/breach-ok"
    runner._current_breach_run_id = "run-ok"

    result = asyncio.run(
        runner._run_coder_with_supervision(
            coder_name,
            plugin,
            {},
            target_branch="pr-001",
            current_pr_id="PR-001",
            pr_id="PR-001",
            task_file="tasks/PR-001.md",
            task_body="# PR-001\n",
        )
    )

    assert result == (0, "out", "", None)


def test_publication_terminates_group_preserves_output_and_refreshes_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_task(monkeypatch)
    runner.app_config.daemon.coder_terminate_grace_sec = 0
    _coder_name, plugin = runner._get_coder()
    observed = _publication(41, sha="a" * 40)
    final = _publication(41, sha="b" * 40)
    calls = _publication_snapshots(monkeypatch, [], [observed], [final])
    monkeypatch.setattr(
        coding_module.gh_prs,
        "get_open_prs",
        lambda *args, **kwargs: pytest.fail("full WATCH lookup is premature"),
    )
    managed = _BoundaryManaged()
    _install_waiting_cli(
        monkeypatch, plugin, managed, propagate_cancel=True
    )
    monkeypatch.setattr(
        coding_module,
        "cancelled_process_result",
        lambda exc: ProcessRunResult(
            returncode=-15,
            stdout=b"captured stdout",
            stderr=b"captured stderr",
        ),
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.head_sha == "b" * 40
    assert managed.cleanup_calls == [(0, 0)]
    assert calls == [1, 2, 3, 4]
    stored = runner.redis.store[f"cli_log:{runner.name}:latest"]
    assert "captured stdout" in stored and "captured stderr" in stored


def test_publication_allows_normal_exit_during_grace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_task(monkeypatch)
    _coder_name, plugin = runner._get_coder()
    publication = _publication(42)
    _publication_snapshots(monkeypatch, [], [publication], [publication])
    publication_logged = asyncio.Event()
    original_log_event = runner.log_event

    def capture_log(message: str, **kwargs: object) -> None:
        original_log_event(message, **kwargs)
        if "Verified publication" in message:
            publication_logged.set()

    async def cli_finishes(*args: object, **kwargs: object) -> tuple[int, str, str]:
        await publication_logged.wait()
        return (0, "finished naturally", "")

    runner.log_event = capture_log  # type: ignore[method-assign]
    monkeypatch.setattr(plugin, "run_auto_pr", cli_finishes)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 42
    assert runner._current_coder_process is None


@pytest.mark.parametrize("recheck", ["error", "mismatch"])
def test_unverified_pre_cleanup_recheck_leaves_coder_running(
    monkeypatch: pytest.MonkeyPatch,
    recheck: str,
) -> None:
    runner = _runner_with_task(monkeypatch)
    runner.app_config.daemon.coder_terminate_grace_sec = 0
    _coder_name, plugin = runner._get_coder()
    publication = _publication(43)
    third_read: list[BranchPublication] | Exception = (
        RuntimeError("recheck unavailable")
        if recheck == "error"
        else [publication]
    )
    _publication_snapshots(monkeypatch, [], [publication], third_read)
    if recheck == "mismatch":
        local_heads = iter(["a" * 40, "c" * 40, "c" * 40])
        monkeypatch.setattr(
            coding_module,
            "_local_branch_head_sha",
            lambda *args, **kwargs: next(local_heads),
        )
    continue_coder = asyncio.Event()
    original_log_event = runner.log_event

    def capture_log(message: str, **kwargs: object) -> None:
        original_log_event(message, **kwargs)
        if "coder will continue" in message:
            continue_coder.set()

    async def cli_finishes(*args: object, **kwargs: object) -> tuple[int, str, str]:
        await continue_coder.wait()
        return (0, "finished", "")

    runner.log_event = capture_log  # type: ignore[method-assign]
    monkeypatch.setattr(plugin, "run_auto_pr", cli_finishes)
    monkeypatch.setattr(
        coding_module.gh_prs,
        "get_open_prs",
        lambda *args, **kwargs: [PRInfo(number=43, branch="pr-001")],
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert runner._current_coder_process is None


def test_publication_monitor_rearms_after_sha_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_task(monkeypatch)
    runner.app_config.daemon.coder_terminate_grace_sec = 0
    runner.app_config.daemon.fix_poll_interval_sec = 0
    _coder_name, plugin = runner._get_coder()
    observed = _publication(47, sha="a" * 40)
    updated = _publication(47, sha="b" * 40)
    publication_reads: list[list[BranchPublication] | Exception] = [
        [],
        [observed],
        [updated],
        [updated],
    ]

    def fetch(*args: object, **kwargs: object) -> list[BranchPublication]:
        snapshot = publication_reads.pop(0)
        if isinstance(snapshot, Exception):
            raise snapshot
        return snapshot

    monitor_inputs: list[tuple[set[int] | None, datetime]] = []

    async def monitor(
        cli_task: asyncio.Task[tuple[int, str, str]],
        *,
        base_branch: str,
        target_branch: str,
        baseline_numbers: set[int] | None,
        not_before: datetime,
    ) -> BranchPublication:
        monitor_inputs.append((baseline_numbers, not_before))
        return observed if len(monitor_inputs) == 1 else updated

    local_heads = iter(["b" * 40, "b" * 40, "b" * 40])
    monkeypatch.setattr(coding_module.gh_prs, "get_branch_publications", fetch)
    monkeypatch.setattr(runner, "_monitor_coding_publication", monitor)
    monkeypatch.setattr(
        coding_module,
        "_local_branch_head_sha",
        lambda *args, **kwargs: next(local_heads),
    )
    managed = _BoundaryManaged()
    _install_waiting_cli(monkeypatch, plugin, managed, propagate_cancel=True)
    monkeypatch.setattr(
        coding_module,
        "cancelled_process_result",
        lambda exc: ProcessRunResult(returncode=-15, stdout=b"out", stderr=b""),
    )

    asyncio.run(asyncio.wait_for(runner.handle_coding(), timeout=1))

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.head_sha == "b" * 40
    assert len(monitor_inputs) == 2
    assert monitor_inputs[0][0] == monitor_inputs[1][0] == set()
    assert monitor_inputs[0][1] == monitor_inputs[1][1]
    assert publication_reads == []
    assert managed.run_calls == 1
    assert managed.cleanup_calls == [(0, 0)]


def test_publication_monitor_rearms_after_transient_recheck_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_task(monkeypatch)
    runner.app_config.daemon.coder_terminate_grace_sec = 0
    runner.app_config.daemon.fix_poll_interval_sec = 0
    _coder_name, plugin = runner._get_coder()
    publication = _publication(48)
    publication_reads: list[list[BranchPublication] | Exception] = [
        [],
        RuntimeError("recheck unavailable"),
        [publication],
        [publication],
    ]

    def fetch(*args: object, **kwargs: object) -> list[BranchPublication]:
        snapshot = publication_reads.pop(0)
        if isinstance(snapshot, Exception):
            raise snapshot
        return snapshot

    monitor_calls = 0

    async def monitor(*args: object, **kwargs: object) -> BranchPublication:
        nonlocal monitor_calls
        monitor_calls += 1
        return publication

    monkeypatch.setattr(coding_module.gh_prs, "get_branch_publications", fetch)
    monkeypatch.setattr(runner, "_monitor_coding_publication", monitor)
    monkeypatch.setattr(
        coding_module,
        "_local_branch_head_sha",
        lambda *args, **kwargs: publication.head_sha,
    )
    managed = _BoundaryManaged()
    _install_waiting_cli(monkeypatch, plugin, managed, propagate_cancel=True)
    monkeypatch.setattr(
        coding_module,
        "cancelled_process_result",
        lambda exc: ProcessRunResult(returncode=-15, stdout=b"out", stderr=b""),
    )

    asyncio.run(asyncio.wait_for(runner.handle_coding(), timeout=1))

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 48
    assert monitor_calls == 2
    assert publication_reads == []
    assert managed.run_calls == 1
    assert managed.cleanup_calls == [(0, 0)]


def test_cli_exit_during_rearm_delay_rechecks_published_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_task(monkeypatch)
    runner.app_config.daemon.coder_terminate_grace_sec = 0
    _coder_name, plugin = runner._get_coder()
    observed = _publication(50, sha="a" * 40)
    updated = _publication(50, sha="b" * 40)
    publication_reads = iter([[], [observed], [updated], [updated]])
    local_heads = iter(["b" * 40, "b" * 40, "b" * 40])
    allow_cli_exit = asyncio.Event()
    monitor_calls = 0

    async def monitor(*args: object, **kwargs: object) -> BranchPublication:
        nonlocal monitor_calls
        monitor_calls += 1
        return observed

    async def cli_exits(*args: object, **kwargs: object) -> tuple[int, str, str]:
        await allow_cli_exit.wait()
        return (1, "published corrected head", "coder exited nonzero")

    original_log_event = runner.log_event

    def capture_log(message: str, **kwargs: object) -> None:
        original_log_event(message, **kwargs)
        if "renewed publication observation" in message:
            allow_cli_exit.set()

    monkeypatch.setattr(
        coding_module.gh_prs,
        "get_branch_publications",
        lambda *args, **kwargs: next(publication_reads),
    )
    monkeypatch.setattr(
        coding_module,
        "_local_branch_head_sha",
        lambda *args, **kwargs: next(local_heads),
    )
    monkeypatch.setattr(runner, "_monitor_coding_publication", monitor)
    monkeypatch.setattr(plugin, "run_auto_pr", cli_exits)
    runner.log_event = capture_log  # type: ignore[method-assign]

    asyncio.run(asyncio.wait_for(runner.handle_coding(), timeout=1))

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.head_sha == "b" * 40
    assert monitor_calls == 1


def test_simultaneous_cli_and_publication_completion_rechecks_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_task(monkeypatch)
    _coder_name, plugin = runner._get_coder()
    publication = _publication(54, sha="b" * 40)
    publication_reads = iter([[], [publication], [publication]])
    real_wait = asyncio.wait
    force_initial_pair = True

    async def monitor(*args: object, **kwargs: object) -> BranchPublication:
        return publication

    async def cli_exits(*args: object, **kwargs: object) -> tuple[int, str, str]:
        return (1, "published", "coder exited nonzero")

    async def wait_for_initial_pair(*args: Any, **kwargs: Any):
        nonlocal force_initial_pair
        tasks = args[0]
        if force_initial_pair and len(tasks) == 2:
            force_initial_pair = False
            await asyncio.gather(*tasks, return_exceptions=True)
            return set(tasks), set()
        return await real_wait(*args, **kwargs)

    monkeypatch.setattr(
        coding_module.gh_prs,
        "get_branch_publications",
        lambda *args, **kwargs: next(publication_reads),
    )
    monkeypatch.setattr(
        coding_module,
        "_local_branch_head_sha",
        lambda *args, **kwargs: publication.head_sha,
    )
    monkeypatch.setattr(runner, "_monitor_coding_publication", monitor)
    monkeypatch.setattr(plugin, "run_auto_pr", cli_exits)
    monkeypatch.setattr(coding_module.asyncio, "wait", wait_for_initial_pair)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 54
    assert publication_reads.__length_hint__() == 0


def test_stop_during_renewed_publication_observation_settles_monitor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_task(monkeypatch)
    runner.app_config.daemon.coder_terminate_grace_sec = 0
    runner.app_config.daemon.fix_poll_interval_sec = 0
    _coder_name, plugin = runner._get_coder()
    publication = _publication(49)
    publication_reads = iter([[], [publication]])
    renewed_started = asyncio.Event()
    renewed_settled = asyncio.Event()
    monitor_calls = 0

    async def monitor(*args: object, **kwargs: object) -> BranchPublication:
        nonlocal monitor_calls
        monitor_calls += 1
        if monitor_calls == 1:
            return publication
        renewed_started.set()
        try:
            await asyncio.Future()
        finally:
            renewed_settled.set()
        raise AssertionError("renewed monitor unexpectedly returned")

    async def cli_waits(*args: object, **kwargs: object) -> tuple[int, str, str]:
        await asyncio.Future()
        raise AssertionError("coder unexpectedly returned")

    async def stop_monitor(
        cli_task: asyncio.Task[tuple[int, str, str]],
    ) -> None:
        await renewed_started.wait()
        runner._stop_requested = True
        runner.state.user_paused = True
        cli_task.cancel()

    monkeypatch.setattr(
        coding_module.gh_prs,
        "get_branch_publications",
        lambda *args, **kwargs: next(publication_reads),
    )
    monkeypatch.setattr(
        coding_module,
        "_local_branch_head_sha",
        lambda *args, **kwargs: "b" * 40,
    )
    monkeypatch.setattr(runner, "_monitor_coding_publication", monitor)
    monkeypatch.setattr(runner, "_monitor_stop_request", stop_monitor)
    monkeypatch.setattr(plugin, "run_auto_pr", cli_waits)
    monkeypatch.setattr(runner, "_check_late_breach", lambda *a, **kw: None)
    monkeypatch.setattr(runner, "_cleanup_breach_marker", lambda *a, **kw: None)

    asyncio.run(asyncio.wait_for(runner.handle_coding(), timeout=1))

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.current_pr is None
    assert monitor_calls == 2
    assert renewed_settled.is_set()


def test_ordinary_completion_settles_publication_monitor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_task(monkeypatch)
    _coder_name, plugin = runner._get_coder()
    monitor_started = asyncio.Event()
    monitor_settled = asyncio.Event()

    async def monitor(*args: object, **kwargs: object) -> None:
        monitor_started.set()
        try:
            await asyncio.Future()
        finally:
            monitor_settled.set()

    async def cli_finishes(*args: object, **kwargs: object) -> tuple[int, str, str]:
        await monitor_started.wait()
        return (0, "done", "")

    monkeypatch.setattr(runner, "_monitor_coding_publication", monitor)
    monkeypatch.setattr(plugin, "run_auto_pr", cli_finishes)
    monkeypatch.setattr(
        coding_module.gh_prs,
        "get_open_prs",
        lambda *args, **kwargs: [PRInfo(number=43, branch="pr-001")],
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert monitor_settled.is_set()


def test_empty_publication_monitor_result_resumes_cli_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_task(monkeypatch)
    _coder_name, plugin = runner._get_coder()
    monitor_done = asyncio.Event()
    allow_cli_exit = asyncio.Event()
    real_wait = asyncio.wait

    async def monitor(*args: object, **kwargs: object) -> None:
        monitor_done.set()

    async def cli_finishes(*args: object, **kwargs: object) -> tuple[int, str, str]:
        await monitor_done.wait()
        await allow_cli_exit.wait()
        return (0, "done", "")

    async def wait_then_release_cli(*args: Any, **kwargs: Any):
        completed, pending = await real_wait(*args, **kwargs)
        if monitor_done.is_set() and len(args[0]) == 2:
            allow_cli_exit.set()
        return completed, pending

    monkeypatch.setattr(runner, "_monitor_coding_publication", monitor)
    monkeypatch.setattr(plugin, "run_auto_pr", cli_finishes)
    monkeypatch.setattr(coding_module.asyncio, "wait", wait_then_release_cli)
    monkeypatch.setattr(
        coding_module.gh_prs,
        "get_open_prs",
        lambda *args, **kwargs: [PRInfo(number=44, branch="pr-001")],
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH


def test_failed_publication_baseline_keeps_ordinary_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_task(monkeypatch)
    _coder_name, plugin = runner._get_coder()
    monkeypatch.setattr(
        coding_module.gh_prs,
        "get_branch_publications",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("offline")),
    )
    async def cli_finishes(*args: object, **kwargs: object) -> tuple[int, str, str]:
        await asyncio.sleep(0)
        return (0, "done", "")

    monkeypatch.setattr(plugin, "run_auto_pr", cli_finishes)
    monkeypatch.setattr(
        coding_module.gh_prs,
        "get_open_prs",
        lambda *args, **kwargs: [PRInfo(number=45, branch="pr-001")],
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert any(
        "Publication baseline unavailable" in entry["event"]
        for entry in runner.state.history
    )


def test_publication_cleanup_failure_blocks_handoff_and_keeps_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_task(monkeypatch)
    runner.app_config.daemon.coder_terminate_grace_sec = 0
    _coder_name, plugin = runner._get_coder()
    publication = _publication(44)
    calls = _publication_snapshots(monkeypatch, [], [publication])
    branch_cleanup_calls: list[str] = []
    managed = _BoundaryManaged(quiescent=False)
    _install_waiting_cli(monkeypatch, plugin, managed)
    monkeypatch.setattr(
        runner,
        "_cleanup_expected_branch",
        lambda: branch_cleanup_calls.append("removed"),
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    assert "owned publication child still live" in (runner.state.error_message or "")
    assert runner._current_coder_supervised_process is managed
    assert branch_cleanup_calls == []
    assert calls == [1, 2, 3]


@pytest.mark.parametrize(
    ("fail_output", "clear_returncode", "expected_failure"),
    [
        (True, False, "stdout reader failed: OSError: reader broke"),
        (
            False,
            True,
            "cleanup confirmed quiescence without a leader return code",
        ),
    ],
)
def test_adapter_supervision_failure_blocks_publication_handoff(
    monkeypatch: pytest.MonkeyPatch,
    fail_output: bool,
    clear_returncode: bool,
    expected_failure: str,
) -> None:
    runner = _runner_with_task(monkeypatch)
    plugin = CodexPlugin()
    publication = _publication(56)
    _publication_snapshots(monkeypatch, [], [publication])
    managed = _AdapterManaged(
        fail_output=fail_output,
        clear_returncode=clear_returncode,
    )

    async def launch(*args: object, **kwargs: object) -> _AdapterManaged:
        return managed

    async def monitor(*args: object, **kwargs: object) -> None:
        await asyncio.Future()

    monkeypatch.setattr(runner, "_get_coder", lambda: ("codex", plugin))
    monkeypatch.setattr(runner, "_monitor_coding_publication", monitor)
    monkeypatch.setattr(codex_cli, "launch_process", launch)
    monkeypatch.setattr(codex_cli, "_maybe_wrap_sandbox", lambda cmd, cwd: cmd)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.current_pr is None
    assert expected_failure in (runner.state.error_message or "")
    assert isinstance(managed.supervision_failure, ProcessSupervisionError)
    assert runner._current_coder_supervised_process is None
    stored = runner.redis.store[f"cli_log:{runner.name}:latest"]
    assert "partial output" in stored
    assert "provider diagnostic" in stored
    assert expected_failure in stored


def test_publication_cleanup_keeps_invocation_supervision_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_task(monkeypatch)
    runner.app_config.daemon.coder_terminate_grace_sec = 0
    plugin = CodexPlugin()
    publication = _publication(57)
    release_failure = asyncio.Event()
    adapter_finished = threading.Event()
    loop: asyncio.AbstractEventLoop | None = None
    publication_reads = 0

    class DelayedFailingReader:
        def __init__(self) -> None:
            self.calls = 0

        async def read(self, _size: int) -> bytes:
            self.calls += 1
            if self.calls == 1:
                return b"partial output"
            await release_failure.wait()
            raise OSError("reader broke during publication recheck")

    managed = _AdapterManaged(fail_output=False, clear_returncode=False)
    managed.process.stdout = DelayedFailingReader()

    async def launch(*args: object, **kwargs: object) -> _AdapterManaged:
        return managed

    async def monitor(*args: object, **kwargs: object) -> BranchPublication:
        return publication

    def fetch(*args: object, **kwargs: object) -> list[BranchPublication]:
        nonlocal publication_reads
        publication_reads += 1
        if publication_reads == 1:
            return []
        if publication_reads == 2:
            assert loop is not None
            loop.call_soon_threadsafe(release_failure.set)
            if not adapter_finished.wait(timeout=2):
                raise RuntimeError("adapter did not finish during recheck")
        return [publication]

    original_run_auto_pr = plugin.run_auto_pr

    async def run_auto_pr(*args: Any, **kwargs: Any) -> tuple[int, str, str]:
        try:
            return await original_run_auto_pr(*args, **kwargs)
        finally:
            adapter_finished.set()

    monkeypatch.setattr(runner, "_get_coder", lambda: ("codex", plugin))
    monkeypatch.setattr(runner, "_monitor_coding_publication", monitor)
    monkeypatch.setattr(plugin, "run_auto_pr", run_auto_pr)
    monkeypatch.setattr(codex_cli, "launch_process", launch)
    monkeypatch.setattr(codex_cli, "_maybe_wrap_sandbox", lambda cmd, cwd: cmd)
    monkeypatch.setattr(coding_module.gh_prs, "get_branch_publications", fetch)
    monkeypatch.setattr(
        coding_module,
        "_local_branch_head_sha",
        lambda *args, **kwargs: publication.head_sha,
    )

    async def scenario() -> None:
        nonlocal loop
        loop = asyncio.get_running_loop()
        await runner.handle_coding()

    asyncio.run(asyncio.wait_for(scenario(), timeout=3))

    assert publication_reads == 2
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.current_pr is None
    assert "reader broke during publication recheck" in (
        runner.state.error_message or ""
    )
    assert runner._current_coder_supervised_process is None
    stored = runner.redis.store[f"cli_log:{runner.name}:latest"]
    assert "partial output" in stored
    assert "provider diagnostic" in stored
    assert "reader broke during publication recheck" in stored


@pytest.mark.parametrize("captured", [None, "timed_out"])
def test_publication_cancellation_without_clean_process_result_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    captured: str | None,
) -> None:
    runner = _runner_with_task(monkeypatch)
    runner.app_config.daemon.coder_terminate_grace_sec = 0
    _coder_name, plugin = runner._get_coder()
    publication = _publication(46)
    _publication_snapshots(monkeypatch, [], [publication])
    managed = _BoundaryManaged()
    _install_waiting_cli(
        monkeypatch, plugin, managed, propagate_cancel=True
    )
    result = (
        None
        if captured is None
        else ProcessRunResult(
            returncode=-15,
            stdout=b"partial output",
            stderr=b"",
            timed_out=True,
        )
    )
    monkeypatch.setattr(
        coding_module, "cancelled_process_result", lambda exc: result
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    expected = "without a captured" if captured is None else "timeout overlapped"
    assert expected in (runner.state.error_message or "")


@pytest.mark.parametrize(
    ("final_kind", "expected_state", "expected_number"),
    [
        ("changed_identity", PipelineState.WATCH, 52),
        ("draft", PipelineState.ERROR, None),
        ("closed", PipelineState.ERROR, None),
        ("sha_mismatch", PipelineState.ERROR, None),
        ("read_error", PipelineState.ERROR, None),
    ],
)
def test_publication_refresh_rejects_stale_final_evidence(
    monkeypatch: pytest.MonkeyPatch,
    final_kind: str,
    expected_state: PipelineState,
    expected_number: int | None,
) -> None:
    runner = _runner_with_task(monkeypatch)
    runner.app_config.daemon.coder_terminate_grace_sec = 0
    _coder_name, plugin = runner._get_coder()
    observed = _publication(51)
    final: BranchPublication | Exception = {
        "changed_identity": _publication(52, sha="c" * 40),
        "draft": _publication(51, is_draft=True),
        "closed": _publication(51, state="CLOSED"),
        "sha_mismatch": _publication(51, sha="b" * 40),
        "read_error": RuntimeError("refresh unavailable"),
    }[final_kind]
    final_snapshot = final if isinstance(final, Exception) else [final]
    _publication_snapshots(
        monkeypatch, [], [observed], [observed], final_snapshot
    )
    if final_kind == "sha_mismatch":
        local_heads = iter(["a" * 40, "a" * 40, "c" * 40])
        monkeypatch.setattr(
            coding_module,
            "_local_branch_head_sha",
            lambda *args, **kwargs: next(local_heads),
        )
    managed = _BoundaryManaged()
    _install_waiting_cli(monkeypatch, plugin, managed, stderr="")

    asyncio.run(runner.handle_coding())

    assert runner.state.state == expected_state
    if expected_number is None:
        assert runner.state.current_pr is None
        if final_kind == "read_error":
            assert "refresh unavailable" in (runner.state.error_message or "")
        else:
            assert "changed or disappeared" in (runner.state.error_message or "")
    else:
        assert runner.state.current_pr is not None
        assert runner.state.current_pr.number == expected_number


def test_fresh_selector_rejects_stale_draft_and_invalid_evidence() -> None:
    boundary = datetime.now(timezone.utc)
    publications = [
        _publication(1, created_at=boundary + timedelta(seconds=1)),
        _publication(2, created_at=boundary - timedelta(seconds=1)),
        _publication(3, is_draft=True, created_at=boundary + timedelta(seconds=1)),
        _publication(4, sha="invalid", created_at=boundary + timedelta(seconds=1)),
        _publication(5, sha="e" * 40, created_at=boundary + timedelta(seconds=1)),
    ]

    assert coding_module._fresh_ready_publication(
        publications,
        baseline_numbers={1},
        not_before=boundary,
        expected_head_sha="f" * 40,
    ) is None


def test_publication_monitor_retries_transient_read_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_task(monkeypatch)
    publication = _publication(60)
    _publication_snapshots(
        monkeypatch,
        RuntimeError("poll failed"),
        [publication],
    )
    real_sleep = asyncio.sleep

    async def yield_once(_seconds: float) -> None:
        await real_sleep(0)

    monkeypatch.setattr(coding_module.asyncio, "sleep", yield_once)

    async def scenario() -> None:
        cli_task = asyncio.create_task(asyncio.Event().wait())
        try:
            found = await runner._monitor_coding_publication(
                cli_task,
                base_branch="main",
                target_branch="pr-001",
                baseline_numbers=set(),
                not_before=datetime.now(timezone.utc) - timedelta(seconds=1),
            )
            assert found == publication
        finally:
            cli_task.cancel()

    asyncio.run(scenario())


def test_unarmed_publication_monitor_waits_for_cli_and_returns_none() -> None:
    runner = h._make_runner()

    async def scenario() -> None:
        release = asyncio.Event()
        cli_task = asyncio.create_task(release.wait())
        monitor = asyncio.create_task(
            runner._monitor_coding_publication(
                cli_task,  # type: ignore[arg-type]
                base_branch="main",
                target_branch="pr-001",
                baseline_numbers=None,
                not_before=datetime.now(timezone.utc),
            )
        )
        await asyncio.sleep(0)
        release.set()
        await cli_task
        assert await monitor is None

    asyncio.run(scenario())


@pytest.mark.parametrize("race", ["stop", "breach"])
def test_stop_and_breach_win_publication_race(
    monkeypatch: pytest.MonkeyPatch,
    race: str,
) -> None:
    runner = _runner_with_task(monkeypatch)
    _coder_name, plugin = runner._get_coder()
    publication_ready = asyncio.Event()

    async def publication_monitor(*args: object, **kwargs: object) -> BranchPublication:
        publication_ready.set()
        return _publication(61)

    async def cli_waits(*args: object, **kwargs: object) -> tuple[int, str, str]:
        await asyncio.Future()
        return (0, "", "")

    monkeypatch.setattr(runner, "_monitor_coding_publication", publication_monitor)
    monkeypatch.setattr(plugin, "run_auto_pr", cli_waits)
    monkeypatch.setattr(runner, "_check_late_breach", lambda *a, **kw: None)
    monkeypatch.setattr(runner, "_cleanup_breach_marker", lambda *a, **kw: None)
    if race == "stop":
        async def stop_monitor(cli_task: asyncio.Task[tuple[int, str, str]]) -> None:
            await publication_ready.wait()
            runner._stop_requested = True
            runner.state.user_paused = True
            cli_task.cancel()

        monkeypatch.setattr(runner, "_monitor_stop_request", stop_monitor)
    else:
        async def breach_monitor(
            breach_dir: str,
            run_id: str,
            coder_name: str,
            cli_task: asyncio.Task[tuple[int, str, str]],
            breach_flag: dict[str, bool],
        ) -> None:
            assert coder_name == "claude"
            await publication_ready.wait()
            breach_flag["breached"] = True
            cli_task.cancel()

        monkeypatch.setattr(runner, "_monitor_inflight_breach", breach_monitor)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.current_pr is None


def test_run_coder_cleanup_failure_precedes_normal_result_processing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_task(monkeypatch)
    coder_name, plugin = runner._get_coder()

    class _Process:
        returncode = 0

    class _Managed:
        process = _Process()

        async def cleanup(self, **kwargs: object) -> object:
            return types.SimpleNamespace(
                quiescent=False,
                detail="owned descendant still live",
            )

    managed = _Managed()

    async def cli_returns_after_failed_cleanup(
        *args: Any, **kwargs: Any
    ) -> tuple[int, str, str]:
        kwargs["on_process_start"](managed.process)
        kwargs["on_supervised_process_start"](managed)
        return (0, "out", "")

    monkeypatch.setattr(plugin, "run_auto_pr", cli_returns_after_failed_cleanup)
    monkeypatch.setattr(runner, "_check_late_breach", lambda *a, **kw: None)
    monkeypatch.setattr(runner, "_cleanup_breach_marker", lambda *a, **kw: None)
    runner._current_breach_dir = "/tmp/breach-cleanup-failure"
    runner._current_breach_run_id = "run-cleanup-failure"

    result = asyncio.run(
        runner._run_coder_with_supervision(
            coder_name,
            plugin,
            {
                "on_process_start": runner._track_current_coder_process,
                "on_supervised_process_start": (
                    runner._track_current_coder_supervised_process
                ),
            },
            target_branch="pr-001",
            current_pr_id="PR-001",
            pr_id="PR-001",
            task_file="tasks/PR-001.md",
            task_body="# PR-001\n",
        )
    )

    assert result is None
    assert runner.state.state == PipelineState.ERROR
    assert "owned descendant still live" in (runner.state.error_message or "")
    assert runner._current_coder_supervised_process is managed
    assert runner._current_coder_process is managed.process


def test_handle_coding_preserves_branch_marker_on_cleanup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_task(monkeypatch)
    _coder_name, plugin = runner._get_coder()
    branch_cleanup_calls: list[str] = []

    async def fake_run(*args: Any, **kwargs: Any) -> tuple[int, str, str]:
        return (
            -1,
            "",
            "Process supervision launch failed: RuntimeError: cleanup timed "
            "out without a structured outcome",
        )

    monkeypatch.setattr(plugin, "run_auto_pr", fake_run)
    monkeypatch.setattr(runner, "_check_late_breach", lambda *a, **kw: None)
    monkeypatch.setattr(runner, "_cleanup_breach_marker", lambda *a, **kw: None)
    monkeypatch.setattr(
        runner,
        "_cleanup_expected_branch",
        lambda: branch_cleanup_calls.append("removed"),
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    assert branch_cleanup_calls == []
    assert any(
        "Preserving expected-branch marker" in entry["event"]
        for entry in runner.state.history
    )


def test_run_coder_cancellation_confirms_cleanup_before_propagating(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_task(monkeypatch)
    coder_name, plugin = runner._get_coder()
    runner._current_breach_dir = "/tmp/breach-cancel"
    runner._current_breach_run_id = "run-cancel"
    monkeypatch.setattr(runner, "_check_late_breach", lambda *a, **kw: None)
    monkeypatch.setattr(runner, "_cleanup_breach_marker", lambda *a, **kw: None)

    async def scenario() -> None:
        started = asyncio.Event()
        cleaned = asyncio.Event()

        class _Process:
            returncode = None

        class _Managed:
            process = _Process()

            async def cleanup(self, **kwargs: object) -> object:
                cleaned.set()
                return types.SimpleNamespace(quiescent=True, detail=None)

        managed = _Managed()

        async def cli_waits(*args: Any, **kwargs: Any) -> tuple[int, str, str]:
            kwargs["on_process_start"](managed.process)
            kwargs["on_supervised_process_start"](managed)
            started.set()
            await asyncio.Future()
            return (0, "", "")

        monkeypatch.setattr(plugin, "run_auto_pr", cli_waits)
        task = asyncio.create_task(
            runner._run_coder_with_supervision(
                coder_name,
                plugin,
                {
                    "on_process_start": runner._track_current_coder_process,
                    "on_supervised_process_start": (
                        runner._track_current_coder_supervised_process
                    ),
                },
                target_branch="pr-001",
                current_pr_id="PR-001",
                pr_id="PR-001",
                task_file="tasks/PR-001.md",
                task_body="# PR-001\n",
            )
        )
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert cleaned.is_set()

    asyncio.run(scenario())

    assert runner._current_coder_supervised_process is None
    assert runner._current_coder_process is None


def test_run_coder_stop_during_launch_settles_before_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_task(monkeypatch)
    coder_name, plugin = runner._get_coder()
    runner._current_breach_dir = "/tmp/breach-launch-stop"
    runner._current_breach_run_id = "run-launch-stop"
    runner.redis.store[f"control:{runner.name}:stop"] = "1"
    monkeypatch.setattr(runner, "_check_late_breach", lambda *a, **kw: None)
    monkeypatch.setattr(runner, "_cleanup_breach_marker", lambda *a, **kw: None)

    async def cli_launches_slowly(*args: Any, **kwargs: Any) -> tuple[int, str, str]:
        await asyncio.Future()
        return (0, "", "")

    monkeypatch.setattr(plugin, "run_auto_pr", cli_launches_slowly)

    result = asyncio.run(
        runner._run_coder_with_supervision(
            coder_name,
            plugin,
            {
                "on_process_start": runner._track_current_coder_process,
                "on_supervised_process_start": (
                    runner._track_current_coder_supervised_process
                ),
            },
            target_branch="pr-001",
            current_pr_id="PR-001",
            pr_id="PR-001",
            task_file="tasks/PR-001.md",
            task_body="# PR-001\n",
        )
    )

    assert result is None
    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.user_paused is True
    assert runner._coder_invocation_active is False
    assert runner._current_coder_process is None


def test_run_coder_failed_launch_cleanup_without_handle_parks_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_task(monkeypatch)
    coder_name, plugin = runner._get_coder()
    runner._current_breach_dir = "/tmp/breach-launch-cleanup"
    runner._current_breach_run_id = "run-launch-cleanup"
    monkeypatch.setattr(runner, "_check_late_breach", lambda *a, **kw: None)
    monkeypatch.setattr(runner, "_cleanup_breach_marker", lambda *a, **kw: None)

    async def failed_launch(*args: Any, **kwargs: Any) -> tuple[int, str, str]:
        return (
            -1,
            "",
            "Process supervision launch failed: RuntimeError: cleanup exploded",
        )

    monkeypatch.setattr(plugin, "run_auto_pr", failed_launch)

    result = asyncio.run(
        runner._run_coder_with_supervision(
            coder_name,
            plugin,
            {
                "on_process_start": runner._track_current_coder_process,
                "on_supervised_process_start": (
                    runner._track_current_coder_supervised_process
                ),
            },
            target_branch="pr-001",
            current_pr_id="PR-001",
            pr_id="PR-001",
            task_file="tasks/PR-001.md",
            task_body="# PR-001\n",
        )
    )

    assert result is None
    assert runner.state.state == PipelineState.ERROR
    assert "cleanup exploded" in (runner.state.error_message or "")
    assert runner._current_coder_supervised_process is None
    assert runner._coder_cleanup_failure_detail is not None
    assert asyncio.run(runner._hold_for_coder_cleanup()) is True


def test_run_coder_breach_cleanup_failure_wins_over_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_task(monkeypatch)
    coder_name, plugin = runner._get_coder()
    runner._current_breach_dir = "/tmp/breach-cleanup"
    runner._current_breach_run_id = "run-breach-cleanup"

    class _Process:
        returncode = None

    class _Managed:
        process = _Process()

        async def cleanup(self, **kwargs: object) -> object:
            return types.SimpleNamespace(
                quiescent=False,
                detail="breach cleanup unconfirmed",
            )

    managed = _Managed()

    async def cli_waits(*args: Any, **kwargs: Any) -> tuple[int, str, str]:
        kwargs["on_process_start"](managed.process)
        kwargs["on_supervised_process_start"](managed)
        await asyncio.Future()
        return (0, "", "")

    async def breach_monitor(
        self: object,
        breach_dir: str,
        run_id: str,
        coder_name: str,
        task: asyncio.Task,  # type: ignore[type-arg]
        flag: dict[str, bool],
    ) -> None:
        assert coder_name == "claude"
        await asyncio.sleep(0)
        flag["breached"] = True
        task.cancel()

    monkeypatch.setattr(plugin, "run_auto_pr", cli_waits)
    monkeypatch.setattr(
        type(runner), "_monitor_inflight_breach", breach_monitor
    )
    monkeypatch.setattr(runner, "_check_late_breach", lambda *a, **kw: None)
    monkeypatch.setattr(runner, "_cleanup_breach_marker", lambda *a, **kw: None)

    result = asyncio.run(
        runner._run_coder_with_supervision(
            coder_name,
            plugin,
            {
                "on_process_start": runner._track_current_coder_process,
                "on_supervised_process_start": (
                    runner._track_current_coder_supervised_process
                ),
            },
            target_branch="pr-001",
            current_pr_id="PR-001",
            pr_id="PR-001",
            task_file="tasks/PR-001.md",
            task_body="# PR-001\n",
        )
    )

    assert result is None
    assert runner.state.state == PipelineState.ERROR
    assert "breach cleanup unconfirmed" in (runner.state.error_message or "")
    assert runner._current_coder_supervised_process is managed


# ---------- _post_coder_resolution ----------


def test_post_coder_resolution_transitions_to_watch_when_pr_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ``get_open_prs`` returns a PR matching ``target_branch`` after
    a clean coder exit, the helper transitions to WATCH and posts the
    Codex review trigger."""
    runner = _runner_with_task(monkeypatch)
    coder_name, _plugin = runner._get_coder()
    candidate = PRInfo(number=42, branch="pr-001")

    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda *a, **kw: [candidate],
    )

    async def _no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(h.runner_module.asyncio, "sleep", _no_sleep)
    posted: list[int] = []
    runner._post_codex_review = lambda pr_number: (  # type: ignore[method-assign]
        posted.append(pr_number) or True
    )

    asyncio.run(
        runner._post_coder_resolution(
            coder_name,
            0,
            "ok",
            "",
            target_branch="pr-001",
            current_pr_id="PR-001",
        )
    )

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 42
    assert posted == [42]


def test_post_coder_resolution_routes_to_diagnose_on_no_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the coder exits 0 but no PR matches ``target_branch``, the
    helper hands off to ``_diagnose_exit_zero_no_pr`` for the A/B/C
    decision tree (HUNG vs daemon recovery vs branch mismatch)."""
    runner = _runner_with_task(monkeypatch)
    coder_name, _plugin = runner._get_coder()

    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda *a, **kw: [],
    )

    async def _no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(h.runner_module.asyncio, "sleep", _no_sleep)

    diagnose_calls: list[tuple[str, str]] = []

    async def fake_diagnose(
        target_branch: str,
        coder_name_arg: str,
        pause_for_stop_if_requested,
    ) -> None:
        diagnose_calls.append((target_branch, coder_name_arg))

    monkeypatch.setattr(runner, "_diagnose_exit_zero_no_pr", fake_diagnose)

    asyncio.run(
        runner._post_coder_resolution(
            coder_name,
            0,
            "ok",
            "",
            target_branch="pr-001",
            current_pr_id="PR-001",
        )
    )

    assert diagnose_calls == [("pr-001", coder_name)]


# ---------- handle_coding ordering ----------


def test_handle_coding_refreshes_auth_before_selecting_coder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``handle_coding`` must refresh the auth-status cache before calling
    ``_get_coder``. ``_select_coder`` reads ``self._auth_status_cache``, so
    selecting first against an empty/stale cache could pick an ineligible
    coder that no later refresh can undo within this run."""
    runner = _runner_with_task(monkeypatch)
    runner._auth_status_cache = {}
    runner._auth_status_cache_expires_at = None

    order: list[str] = []

    async def fake_refresh() -> None:
        order.append("refresh")

    original_get_coder = runner._get_coder

    def spy_get_coder(*args: Any, **kwargs: Any):
        order.append("select")
        return original_get_coder(*args, **kwargs)

    async def stop_after_prepare(coder_name: str, plugin) -> dict[str, Any]:
        order.append("prepare")
        raise CoderUnavailable("test-stop")

    monkeypatch.setattr(runner, "_refresh_auth_status_cache", fake_refresh)
    monkeypatch.setattr(runner, "_get_coder", spy_get_coder)
    monkeypatch.setattr(
        runner, "_prepare_coder_invocation", stop_after_prepare
    )

    asyncio.run(runner.handle_coding())

    assert order == ["refresh", "select", "prepare"]


def test_handle_coding_rate_limit_runs_before_branch_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_check_rate_limit`` must run before the missing-branch guard.

    Pre-decomposition the gate ran ahead of the branch guard so an active
    or renewed pause window kept the runner PAUSED with a ``"rate_limit"``
    run record. The decomposition tucked the gate inside
    ``_prepare_coder_invocation`` (called after the guard), so a malformed
    task hit ``_transition_to_error`` first and the cooldown was bypassed.
    Verify the rate-limit short-circuit fires even when ``branch=`` is
    missing, recording ``"rate_limit"`` rather than ``"error"``.
    """
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    # No ``branch=`` so the branch guard would fire if reached.
    runner.state.current_task = QueueTask(
        pr_id="PR-701",
        title="missing branch with rate limit",
        status=TaskStatus.TODO,
    )

    rate_limit_calls: list[str | None] = []

    async def fake_rate_limit(proactive_coder: str | None = None) -> bool:
        rate_limit_calls.append(proactive_coder)
        return False

    transition_calls: list[str] = []

    async def fake_transition(message: str, **kwargs: Any) -> None:
        transition_calls.append(message)

    monkeypatch.setattr(runner, "_check_rate_limit", fake_rate_limit)
    monkeypatch.setattr(runner, "_transition_to_error", fake_transition)

    saved: list[tuple[str, Any]] = []

    async def fake_metrics_save(record: Any) -> None:
        saved.append((record.exit_reason, record))

    monkeypatch.setattr(runner._metrics_store, "save", fake_metrics_save)

    asyncio.run(runner.handle_coding())

    assert rate_limit_calls, "rate-limit gate must fire before the branch guard"
    assert transition_calls == [], (
        "missing-branch ERROR transition must not fire when rate-limited"
    )
    assert [reason for reason, _ in saved] == ["rate_limit"]


def test_handle_coding_records_run_for_missing_branch_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The missing-branch ERROR transition must persist a run record.

    ``_transition_to_error`` calls ``_save_current_run_record("error")``,
    which is a no-op when no record is active. Pre-decomposition the
    branch guard ran AFTER ``_start_current_run_record`` so this path
    produced telemetry; the helper extraction moved the guard ahead of
    the start call and silently dropped the record. Verify that
    ``_start_current_run_record`` runs before the guard so the malformed-
    task path still saves with ``exit_reason="error"``.
    """
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    # No ``branch=`` to trigger the missing-branch guard.
    runner.state.current_task = QueueTask(
        pr_id="PR-700",
        title="missing branch",
        status=TaskStatus.TODO,
    )

    saved: list[tuple[str, Any]] = []

    async def fake_metrics_save(record: Any) -> None:
        saved.append((record.exit_reason, record))

    monkeypatch.setattr(runner._metrics_store, "save", fake_metrics_save)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    assert runner._current_run_record is not None
    assert runner._current_run_record.exit_reason == "error"
    assert [reason for reason, _ in saved] == ["error"]
    assert saved[0][1].task_id == "PR-700"
