from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from src.config import FeatureFlags
from src.daemon.runner import PipelineRunner
from src.models import PipelineState
from src.subsource_registry import SuppressionReason, is_operator_clearable

from tests.runner._helpers import _make_runner


def _task(pr_id: str = "PR-380") -> SimpleNamespace:
    return SimpleNamespace(
        pr_id=pr_id,
        task_file=f"tasks/{pr_id}.md",
        branch=f"branch-{pr_id.lower()}",
    )


def _write_task(
    repo: Path,
    task: SimpleNamespace,
    reason: SuppressionReason | None,
    *,
    status: str = "ERROR",
) -> None:
    path = repo / task.task_file
    path.parent.mkdir(parents=True, exist_ok=True)
    blocked = f"blocked_reason: {reason.value}\n" if reason is not None else ""
    path.write_text(
        f"---\nstatus: {status}\n{blocked}---\n\n"
        f"# {task.pr_id}: Test task\n\n"
        f"Branch: {task.branch}\n"
        "- Type: refactor\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )


def _runner(tmp_path: Path) -> PipelineRunner:
    runner = _make_runner(
        feature_flags=FeatureFlags(
            use_unified_inhibitor_check=False,
            use_single_error_exit=True,
        )
    )
    runner.repo_path = str(tmp_path)
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.ERROR
    runner.state.current_task = _task()
    runner.state.error_message = "ERROR: synthetic failure"
    _write_task(tmp_path, runner.state.current_task, SuppressionReason.GUARDRAIL)
    return runner


def _events(runner: PipelineRunner) -> list[str]:
    return [str(entry["event"]) for entry in runner.state.history]


def _stub_cycle(monkeypatch: pytest.MonkeyPatch, runner: PipelineRunner) -> None:
    async def ok() -> bool:
        return True

    async def none() -> None:
        return None

    monkeypatch.setattr(runner, "ensure_repo_cloned", none)
    monkeypatch.setattr(runner, "_check_github_api_budget", ok)
    monkeypatch.setattr(runner, "_refresh_user_paused_from_redis", none)
    monkeypatch.setattr(runner, "preflight", ok)
    monkeypatch.setattr(runner, "publish_state", none)


def _suppress(
    runner: PipelineRunner,
    reason: SuppressionReason,
    *,
    task: SimpleNamespace | None = None,
) -> None:
    task = task or runner.state.current_task
    runner.state.current_task = task
    _write_task(Path(runner.repo_path), task, reason)


def _clear_task_frontmatter(runner: PipelineRunner) -> None:
    _write_task(
        Path(runner.repo_path),
        runner.state.current_task,
        None,
        status="TODO",
    )


def test_guardrail_stays_parked_until_cleared(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)
    _stub_cycle(monkeypatch, runner)
    diagnose_calls = 0

    async def fake_handle_error() -> None:
        nonlocal diagnose_calls
        diagnose_calls += 1

    monkeypatch.setattr(runner, "handle_error", fake_handle_error)
    _suppress(runner, SuppressionReason.GUARDRAIL)

    asyncio.run(runner._run_cycle_body())

    assert runner.state.state == PipelineState.ERROR
    assert diagnose_calls == 0
    assert any(
        "guardrail operator-clearable ERROR park active" in event
        for event in _events(runner)
    )

    _clear_task_frontmatter(runner)
    asyncio.run(runner._run_cycle_body())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None
    assert diagnose_calls == 0
    assert any(
        "operator-cleared ERROR task frontmatter -> IDLE" in event
        for event in _events(runner)
    )


def test_guardrail_does_not_loop_diagnosis(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)
    _stub_cycle(monkeypatch, runner)
    calls = 0

    async def fake_handle_error() -> None:
        nonlocal calls
        calls += 1

    monkeypatch.setattr(runner, "handle_error", fake_handle_error)
    _suppress(runner, SuppressionReason.GUARDRAIL)

    for _ in range(3):
        runner.state.state = PipelineState.ERROR
        asyncio.run(runner._run_cycle_body())

    assert calls == 0
    assert runner.state.state == PipelineState.ERROR


def test_silent_hole_removed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)
    _stub_cycle(monkeypatch, runner)
    runner.app_config.daemon.error_handler_use_ai = False
    _suppress(runner, SuppressionReason.INFRA_FAILURE)

    asyncio.run(runner._run_cycle_body())

    assert runner.state.state == PipelineState.IDLE
    assert any(
        "AI diagnosis disabled for self-healing ERROR (infra_failure) -> IDLE"
        in event
        for event in _events(runner)
    )


def test_no_error_cycle_without_log(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)
    _stub_cycle(monkeypatch, runner)
    runner.app_config.daemon.error_handler_use_ai = False

    for reason in SuppressionReason:
        task = _task(f"PR-{reason.value.upper()}")
        runner.state.state = PipelineState.ERROR
        runner.state.current_task = task
        runner.state.error_message = f"{reason.value} failure"
        before_state = runner.state.state
        before_logs = len(runner.state.history)
        _suppress(runner, reason, task=task)

        asyncio.run(runner._run_cycle_body())

        assert (
            runner.state.state != before_state
            or len(runner.state.history) > before_logs
        ), reason.value


def test_self_healing_unchanged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    infra = _runner(tmp_path / "infra")
    _stub_cycle(monkeypatch, infra)
    infra.state.error_message = "git fetch origin failed"
    _suppress(infra, SuppressionReason.INFRA_FAILURE)

    asyncio.run(infra._run_cycle_body())

    assert infra.state.state == PipelineState.IDLE
    assert any("Infra error detected" in event for event in _events(infra))

    limited = _runner(tmp_path / "limited")
    _stub_cycle(monkeypatch, limited)
    limited.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(
        minutes=5
    )

    asyncio.run(limited._run_cycle_body())

    assert limited.state.state == PipelineState.PAUSED


def test_reupload_releases_park_from_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)
    _stub_cycle(monkeypatch, runner)
    _suppress(runner, SuppressionReason.GUARDRAIL)
    asyncio.run(runner._run_cycle_body())
    assert runner.state.state == PipelineState.ERROR

    _clear_task_frontmatter(runner)
    asyncio.run(runner._run_cycle_body())

    assert runner.state.state == PipelineState.IDLE


def test_frontmatter_fallback_after_redis_loss(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)
    _stub_cycle(monkeypatch, runner)
    asyncio.run(runner._run_cycle_body())

    assert runner.state.state == PipelineState.ERROR
    assert any(
        "guardrail operator-clearable ERROR park active" in event
        for event in _events(runner)
    )


def test_flag_off_uses_legacy_branch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)
    runner.repo_config.feature_flags = FeatureFlags(
        use_unified_inhibitor_check=False,
        use_single_error_exit=False,
    )
    _stub_cycle(monkeypatch, runner)
    calls: list[str] = []

    async def fake_handle_error() -> None:
        calls.append("handle_error")

    monkeypatch.setattr(runner, "handle_error", fake_handle_error)
    _suppress(runner, SuppressionReason.GUARDRAIL)

    asyncio.run(runner._run_cycle_body())

    assert calls == ["handle_error"]
    assert runner.state.state == PipelineState.ERROR


def test_park_log_deduped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)
    _stub_cycle(monkeypatch, runner)
    _suppress(runner, SuppressionReason.GUARDRAIL)

    asyncio.run(runner._run_cycle_body())
    asyncio.run(runner._run_cycle_body())

    events = [
        event
        for event in _events(runner)
        if "guardrail operator-clearable ERROR park active" in event
    ]
    assert len(events) == 1


def test_operator_clearable_classification_documents_reason_table() -> None:
    operator_clearable = {
        SuppressionReason.CODER_ESCALATE,
        SuppressionReason.GUARDRAIL,
        SuppressionReason.FIX_ITERATION_CAP,
        SuppressionReason.NO_PUSH_DEADLOCK,
        SuppressionReason.OPERATOR_REJECT,
        SuppressionReason.DIAGNOSE_EXHAUSTED,
        SuppressionReason.OPERATOR_STOPPED,
    }

    for reason in SuppressionReason:
        assert is_operator_clearable(reason) is (reason in operator_clearable)


def test_operator_clearable_unknown_reason_is_false() -> None:
    assert is_operator_clearable("not_a_real_reason") is False


def test_frontmatter_helper_handles_missing_and_unsafe_tasks(
    tmp_path: Path,
) -> None:
    runner = _runner(tmp_path)

    runner.state.current_task = None
    assert runner._frontmatter_error_status_for_current_task() == (None, None)

    runner.state.current_task = SimpleNamespace(
        pr_id="PR-ABS",
        task_file=str((tmp_path / "tasks" / "PR-ABS.md").resolve()),
        branch="abs",
    )
    assert runner._frontmatter_error_status_for_current_task() == (None, None)

    runner.state.current_task = SimpleNamespace(
        pr_id="PR-PARENT",
        task_file="../tasks/PR-PARENT.md",
        branch="parent",
    )
    assert runner._frontmatter_error_status_for_current_task() == (None, None)

    runner.state.current_task = _task("PR-MISSING")
    assert runner._frontmatter_error_status_for_current_task() == (None, None)


def test_error_suppression_reason_handles_absent_task_and_store_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)
    runner.state.current_task = None
    assert asyncio.run(runner._error_suppression_reason()) == (None, False)

    runner.state.current_task = SimpleNamespace(pr_id="", task_file=None, branch=None)
    assert asyncio.run(runner._error_suppression_reason()) == (None, False)

    runner.state.current_task = _task("PR-STORE")

    async def fake_record(_task_id: str) -> SimpleNamespace:
        return SimpleNamespace(reason=SuppressionReason.GUARDRAIL)

    monkeypatch.setattr(runner, "_suppression_record_for_task", fake_record)

    assert asyncio.run(runner._error_suppression_reason()) == (
        SuppressionReason.GUARDRAIL,
        False,
    )


def test_suppression_cleared_returns_false_on_store_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)

    async def boom(_task_id: str) -> None:
        raise RuntimeError("store down")

    monkeypatch.setattr(runner, "_suppression_record_for_task", boom)

    assert asyncio.run(runner._suppression_cleared("PR-380")) is False


def test_suppression_cleared_returns_true_when_store_has_no_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)

    async def no_record(_task_id: str) -> None:
        return None

    monkeypatch.setattr(runner, "_suppression_record_for_task", no_record)

    assert asyncio.run(runner._suppression_cleared("PR-380")) is True


def test_error_suppression_reason_falls_back_after_store_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)

    async def boom(_task_id: str) -> None:
        raise RuntimeError("store down")

    monkeypatch.setattr(runner, "_suppression_record_for_task", boom)

    assert asyncio.run(runner._error_suppression_reason()) == (
        SuppressionReason.GUARDRAIL,
        True,
    )


def test_store_record_clear_path_releases_to_idle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)
    _stub_cycle(monkeypatch, runner)

    async def fake_reason() -> tuple[SuppressionReason, bool]:
        return SuppressionReason.GUARDRAIL, False

    async def cleared(_task_id: str) -> bool:
        return True

    monkeypatch.setattr(runner, "_error_suppression_reason", fake_reason)
    monkeypatch.setattr(runner, "_suppression_cleared", cleared)

    asyncio.run(runner._run_cycle_body())

    assert runner.state.state == PipelineState.IDLE
    assert any(
        "guardrail park cleared by operator -> IDLE" in event
        for event in _events(runner)
    )
