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
import types
from typing import Any

import pytest
from src.daemon.handlers import CoderUnavailable
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus

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

    async def fake_breach_monitor(self, breach_dir, run_id, task, flag):
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

    assert result == (0, "out", "")


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
            "Process supervision launch failed: RuntimeError: failed launch "
            "cleanup could not confirm quiescence: witness still live",
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
    assert "witness still live" in (runner.state.error_message or "")
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
        task: asyncio.Task,  # type: ignore[type-arg]
        flag: dict[str, bool],
    ) -> None:
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
