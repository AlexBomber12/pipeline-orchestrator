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

from tests.runner import _helpers as h


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


def test_no_inline_error_writes_outside_primitive():
    """Enforce the PR-219b global invariant repaired by PR-237.

    After PR-237 ships, no module under src/daemon/ should write
    ``state.state = PipelineState.ERROR`` directly. All ERROR
    transitions go through ``_transition_to_error``.

    The primitive's own body contains the single legitimate inline
    write; the whitelist is scoped to that body's line range, not the
    whole file, so future regressions in ``runner.py`` outside the
    primitive still surface here.
    """
    import ast
    import pathlib
    import re

    daemon_root = pathlib.Path("src/daemon")
    pattern = re.compile(r"self\.state\.state\s*=\s*PipelineState\.ERROR")
    primitive_file = daemon_root / "runner.py"
    primitive_name = "_transition_to_error"

    primitive_text = primitive_file.read_text()
    primitive_tree = ast.parse(primitive_text)
    primitive_ranges: list[tuple[int, int]] = [
        (node.lineno, node.end_lineno or node.lineno)
        for node in ast.walk(primitive_tree)
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
        and node.name == primitive_name
    ]
    assert len(primitive_ranges) == 1, (
        f"Expected exactly one definition of {primitive_name} in "
        f"{primitive_file}, found {len(primitive_ranges)}"
    )
    primitive_start, primitive_end = primitive_ranges[0]

    offenders: list[str] = []
    for path in daemon_root.rglob("*.py"):
        text = path.read_text()
        for lineno, line in enumerate(text.splitlines(), start=1):
            if not pattern.search(line):
                continue
            if (
                path == primitive_file
                and primitive_start <= lineno <= primitive_end
            ):
                continue
            offenders.append(f"{path}:{lineno}: {line.strip()}")

    assert not offenders, (
        "Direct ERROR writes found outside _transition_to_error:\n"
        + "\n".join(offenders)
    )


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
