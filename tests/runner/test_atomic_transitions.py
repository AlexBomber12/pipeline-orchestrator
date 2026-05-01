"""PR-218: Atomic task state transition primitive tests.

These tests pin the contract for the unified task-clear primitive that
PR-218 introduced: ``state.current_task = None`` plus
``runner._reset_runner_local_task_counters()`` together produce the
canonical "drop the active task handle" reset that the recovery.py
no-recoverable path documented as the superset before this PR.

The PR-213 baseline tests in ``test_state_transitions.py`` cover the
per-callsite migrations; these tests assert the primitive itself in
isolation so a future regression that sneaks one of the runner-private
counters back into a hand-rolled clear pattern surfaces here, not at a
specific handler site.
"""

from __future__ import annotations

from src.models import PipelineState, PRInfo, QueueTask, TaskStatus

from tests import test_runner as h


def test_reset_runner_local_task_counters_zeroes_all_fields() -> None:
    """The helper must reset every runner-private field tied to the active task.

    The fields are documented at the helper docstring: the SKIP retry
    triple (``_error_skip_active``, ``_error_skip_count``,
    ``_error_skip_context``), the diagnose retry counter
    (``_error_diagnose_count``), and the IDLE soft-defer flag
    (``_idle_dispatch_deferred``). Drift here would re-fragment the
    reset surface that PR-218 just unified.
    """
    runner = h._make_runner()
    runner._error_skip_active = True
    runner._error_skip_count = 7
    runner._error_skip_context = "stale-context"
    runner._error_diagnose_count = 3
    runner._idle_dispatch_deferred = True

    runner._reset_runner_local_task_counters()

    assert runner._error_skip_active is False
    assert runner._error_skip_count == 0
    assert runner._error_skip_context is None
    assert runner._error_diagnose_count == 0
    assert runner._idle_dispatch_deferred is False


def test_atomic_clear_matches_recovery_superset() -> None:
    """Combined primitive matches the recovery.py:371-375 superset pattern.

    Before PR-218, the recovery no-recoverable branch was the only site
    that wrote the complete reset (``current_task=None``,
    ``current_pr=None``, ``error_message=None``,
    ``_error_diagnose_count=0``). Calling ``state.current_task = None``
    plus the helper must produce the same observable shape on the
    runner so any callsite migrated onto the primitive inherits the
    superset semantics for free.
    """
    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING,
    )
    runner.state.current_pr = PRInfo(number=1, branch="pr-001")
    runner.state.error_message = "stale"
    runner._error_skip_active = True
    runner._error_skip_count = 4
    runner._error_skip_context = "ctx"
    runner._error_diagnose_count = 2
    runner._idle_dispatch_deferred = True

    runner.state.current_task = None
    runner._reset_runner_local_task_counters()

    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert runner.state.error_message is None
    assert runner._error_skip_active is False
    assert runner._error_skip_count == 0
    assert runner._error_skip_context is None
    assert runner._error_diagnose_count == 0
    assert runner._idle_dispatch_deferred is False


def test_atomic_clear_leaves_pipeline_state_untouched() -> None:
    """The primitive only releases task handles; it does not move ``state.state``.

    Each callsite owns its own terminal state transition (typically to
    ``IDLE``) — the primitive does not implicitly drive that move. This
    keeps callsites that still need to publish or run additional logic
    after the clear (FIX, MERGE, recovery) free to sequence their work
    without fighting an implicit state write.
    """
    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING,
    )

    runner.state.current_task = None
    runner._reset_runner_local_task_counters()

    assert runner.state.state == PipelineState.WATCH
