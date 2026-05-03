"""PR-229b: tests for handle_coding decomposition helpers.

Verifies the three async helpers extracted from ``handle_coding``:

- ``_prepare_coder_invocation``: auth refresh, rate-limit check, run
  record start, breach env allocation, kwargs build.
- ``_run_coder_with_supervision``: subprocess plus stop and breach
  monitors; resolves user-stop and breach pauses.
- ``_post_coder_resolution``: CLI log save, exit classification, PR
  lookup or daemon-side PR creation, run record save.
"""

from __future__ import annotations

import asyncio
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


def test_prepare_coder_invocation_raises_when_rate_limit_blocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_check_rate_limit`` returning False raises ``CoderUnavailable``.

    The helper must save the current run record as ``"rate_limit"`` before
    raising so the metrics store reflects the proactive pause.
    """
    runner = _runner_with_task(monkeypatch)
    coder_name, plugin = runner._get_coder()

    async def fake_rate_limit(*args: Any, **kwargs: Any) -> bool:
        return False

    saved: list[str] = []

    async def fake_save(reason: str) -> None:
        saved.append(reason)

    monkeypatch.setattr(runner, "_check_rate_limit", fake_rate_limit)
    monkeypatch.setattr(runner, "_save_current_run_record", fake_save)

    with pytest.raises(CoderUnavailable):
        asyncio.run(runner._prepare_coder_invocation(coder_name, plugin))

    assert saved == ["rate_limit"]


def test_prepare_coder_invocation_returns_kwargs_with_breach_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The helper allocates the breach env, stores it on ``self``, and
    returns a kwargs dict carrying ``timeout`` plus ``on_process_start``."""
    runner = _runner_with_task(monkeypatch)
    coder_name, plugin = runner._get_coder()

    async def fake_rate_limit(*args: Any, **kwargs: Any) -> bool:
        return True

    monkeypatch.setattr(runner, "_check_rate_limit", fake_rate_limit)
    monkeypatch.setattr(
        runner, "_breach_env", lambda: ("/tmp/breach", "run-abc")
    )

    kwargs = asyncio.run(runner._prepare_coder_invocation(coder_name, plugin))

    assert runner._current_breach_dir == "/tmp/breach"
    assert runner._current_breach_run_id == "run-abc"
    assert "timeout" in kwargs
    assert kwargs["on_process_start"] == runner._track_current_coder_process


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

    monkeypatch.setattr(plugin, "run_planned_pr", cli_blocks_until_cancelled)
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

    monkeypatch.setattr(plugin, "run_planned_pr", cli_breaches)
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

    monkeypatch.setattr(plugin, "run_planned_pr", cli_ok)
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
        )
    )

    assert result == (0, "out", "")


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
