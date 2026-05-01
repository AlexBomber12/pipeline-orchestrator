"""PR-219a: ``_transition_to_error`` primitive tests.

These tests pin the contract for the unified ERROR transition primitive
that PR-219a introduces. The PR-213 baseline tests in
``test_state_transitions.py`` cover the per-callsite migrations that the
primitive replaces; these tests assert the primitive itself in isolation
so a future regression that re-fragments the ERROR transition surface
surfaces here, not at a specific handler site.

After PR-219b ships, all transitions to ``PipelineState.ERROR`` must use
this primitive — direct ``state.state = PipelineState.ERROR`` writes are
forbidden by the grep success criterion in the task spec.
"""

from __future__ import annotations

import asyncio

from src.metrics import RunRecord
from src.models import PipelineState, QueueTask, TaskStatus

from tests import test_runner as h


def _install_publish_state_spy(runner) -> list[None]:
    """Replace ``publish_state`` with an awaitable spy and return the call log."""
    calls: list[None] = []

    async def fake_publish() -> None:
        calls.append(None)

    runner.publish_state = fake_publish  # type: ignore[method-assign]
    return calls


def _install_save_run_record_spy(runner) -> list[str]:
    """Replace ``_save_current_run_record`` with a spy that records exit_reason."""
    calls: list[str] = []

    async def fake_save(exit_reason: str, **_: object) -> None:
        calls.append(exit_reason)

    runner._save_current_run_record = fake_save  # type: ignore[method-assign]
    return calls


def test_transition_to_error_sets_state_message_and_logs() -> None:
    """The primitive sets ERROR state, error_message, and logs an [ERROR] event."""
    runner = h._make_runner()
    publish_calls = _install_publish_state_spy(runner)
    save_calls = _install_save_run_record_spy(runner)

    asyncio.run(runner._transition_to_error("boom"))

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "boom"
    assert any(
        e["event"] == "[ERROR] boom." for e in runner.state.history
    )
    assert save_calls == ["error"]
    assert publish_calls == [None]


def test_transition_to_error_skips_run_record_when_disabled() -> None:
    """``save_run_record_as=None`` short-circuits the run-record save."""
    runner = h._make_runner()
    _install_publish_state_spy(runner)
    save_calls = _install_save_run_record_spy(runner)

    asyncio.run(
        runner._transition_to_error("boom", save_run_record_as=None)
    )

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "boom"
    assert save_calls == []


def test_transition_to_error_skips_publish_when_disabled() -> None:
    """``publish=False`` defers the dashboard publish to the caller."""
    runner = h._make_runner()
    publish_calls = _install_publish_state_spy(runner)
    _install_save_run_record_spy(runner)

    asyncio.run(runner._transition_to_error("boom", publish=False))

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "boom"
    assert publish_calls == []


def test_transition_to_error_uses_custom_log_prefix() -> None:
    """``log_prefix`` overrides the ``[ERROR]`` default at the log line."""
    runner = h._make_runner()
    _install_publish_state_spy(runner)
    _install_save_run_record_spy(runner)

    asyncio.run(
        runner._transition_to_error("iteration cap", log_prefix="[FIX]")
    )

    assert any(
        e["event"] == "[FIX] iteration cap." for e in runner.state.history
    )


def test_transition_to_error_preserves_current_task() -> None:
    """ERROR transition keeps the active task pinned so diagnose can resume it.

    The primitive only writes state.state and error_message; current_task
    must be untouched so the ErrorMixin's diagnose-or-skip flow has the
    PR/branch/task context it needs to recover the cycle.
    """
    runner = h._make_runner()
    _install_publish_state_spy(runner)
    _install_save_run_record_spy(runner)
    task = QueueTask(
        pr_id="PR-042", title="active", status=TaskStatus.DOING,
    )
    runner.state.current_task = task

    asyncio.run(runner._transition_to_error("boom"))

    assert runner.state.current_task == task


def test_transition_to_error_passes_exit_reason_to_run_record() -> None:
    """``save_run_record_as`` is forwarded verbatim to the metrics store."""
    runner = h._make_runner()
    _install_publish_state_spy(runner)
    save_calls = _install_save_run_record_spy(runner)

    asyncio.run(
        runner._transition_to_error("boom", save_run_record_as="rate_limit")
    )

    assert save_calls == ["rate_limit"]


def test_transition_to_error_finalizes_active_run_record() -> None:
    """When a run record is active, the primitive finalises it via the store."""
    runner = h._make_runner()
    _install_publish_state_spy(runner)
    runner._current_run_record = RunRecord(
        run_id="run-1",
        task_id="PR-042",
        profile_id="claude:opus:container",
        task_type="refactor",
        complexity="medium",
        started_at="2026-05-01T00:00:00+00:00",
        ended_at=None,
        duration_ms=None,
        fix_iterations=0,
        tokens_in=0,
        tokens_out=0,
        exit_reason="",
        operator_intervention=False,
        repo_name=runner.name,
        stage="coder",
    )
    saved: list[RunRecord] = []

    async def fake_metrics_save(record: RunRecord) -> None:
        saved.append(record)

    runner._metrics_store.save = fake_metrics_save  # type: ignore[method-assign]

    asyncio.run(runner._transition_to_error("boom"))

    assert len(saved) == 1
    assert saved[0].exit_reason == "error"
