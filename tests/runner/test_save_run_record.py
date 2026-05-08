from __future__ import annotations

import asyncio
from typing import Any

import pytest

from src.models import PipelineState, PRInfo, QueueTask, TaskStatus

from tests.runner import _helpers as h


def _install_task(runner: Any, pr_id: str = "PR-286") -> None:
    runner.state.current_task = QueueTask(
        pr_id=pr_id,
        title="Run record schema",
        status=TaskStatus.DOING,
    )


@pytest.mark.parametrize(
    ("state", "exit_reason", "outcome", "cause", "phase"),
    [
        (PipelineState.MERGE, "success_merged", "merged", None, "merge"),
        (PipelineState.WATCH, "coding_complete", "superseded", None, "coding"),
        (PipelineState.WATCH, "closed_unmerged", "superseded", None, "fix"),
        (PipelineState.CODING, "error", "failed", "CRASH", "coding"),
        (PipelineState.CODING, "timeout", "failed", "TIMEOUT", "coding"),
        (PipelineState.FIX, "escalated", "failed", "ESCALATE", "fix"),
        (PipelineState.FIX, "rate_limit", "paused", None, "fix"),
    ],
)
def test_save_current_run_record_populates_schema_fields(
    monkeypatch: pytest.MonkeyPatch,
    state: PipelineState,
    exit_reason: str,
    outcome: str,
    cause: str | None,
    phase: str,
) -> None:
    runner = h._make_runner()
    h._patch_subprocess(monkeypatch)
    _install_task(runner)
    runner.state.state = state
    if state == PipelineState.WATCH:
        runner.state.current_pr = PRInfo(number=286, branch="pr-286")
    runner._start_current_run_record("claude", "opus")
    assert runner._current_run_record is not None
    runner._current_run_record.attempt_index = 4
    runner._current_run_record.task_spec_hash = "hash-1"
    runner._current_run_record.base_sha = "base-sha"

    asyncio.run(runner._save_current_run_record(exit_reason))

    record = runner._current_run_record
    assert record is not None
    assert record.outcome == outcome
    assert record.cause == cause
    assert record.run_phase == phase
    assert record.attempt_index == 4
    assert record.task_spec_hash == "hash-1"
    assert record.base_sha == "base-sha"
    assert record.head_sha == "head-before-abc"


def test_transition_to_error_uses_caller_phase_and_cause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    h._patch_subprocess(monkeypatch)
    _install_task(runner)
    runner.state.state = PipelineState.MERGE
    runner._start_current_run_record("claude", "opus")

    asyncio.run(runner._transition_to_error("boom", publish=False))

    record = runner._current_run_record
    assert record is not None
    assert record.outcome == "failed"
    assert record.cause == "CRASH"
    assert record.run_phase == "merge"


def test_run_phase_from_state_covers_fix_and_recovery() -> None:
    runner = h._make_runner()

    assert runner._run_phase_from_state(PipelineState.FIX) == "fix"
    assert runner._run_phase_from_state(PipelineState.ERROR) == "recovery"


def test_prepare_dispatch_metadata_noops_without_active_task() -> None:
    runner = h._make_runner()

    asyncio.run(
        runner._prepare_current_run_record_dispatch_metadata(
            task_hash="hash-a",
            previous_task_hash=None,
        )
    )

    assert runner._current_run_record is None


def test_attempt_index_increments_on_redispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    h._patch_subprocess(monkeypatch)
    _install_task(runner)

    runner._start_current_run_record("claude", "opus")
    asyncio.run(
        runner._prepare_current_run_record_dispatch_metadata(
            task_hash="hash-a",
            previous_task_hash=None,
        )
    )
    assert runner._current_run_record is not None
    assert runner._current_run_record.attempt_index == 1

    runner._start_current_run_record("claude", "opus")
    asyncio.run(
        runner._prepare_current_run_record_dispatch_metadata(
            task_hash="hash-a",
            previous_task_hash="hash-a",
        )
    )

    assert runner._current_run_record is not None
    assert runner._current_run_record.attempt_index == 2
    key = runner._attempt_count_key(runner.name, "PR-286")
    assert runner.redis.ttls[key] == 30 * 24 * 3600


def test_attempt_index_resets_on_file_content_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    h._patch_subprocess(monkeypatch)
    _install_task(runner)

    runner._start_current_run_record("claude", "opus")
    asyncio.run(
        runner._prepare_current_run_record_dispatch_metadata(
            task_hash="hash-a",
            previous_task_hash=None,
        )
    )
    runner._start_current_run_record("claude", "opus")
    asyncio.run(
        runner._prepare_current_run_record_dispatch_metadata(
            task_hash="hash-b",
            previous_task_hash="hash-a",
        )
    )

    assert runner._current_run_record is not None
    assert runner._current_run_record.attempt_index == 1
    assert runner._current_run_record.task_spec_hash == "hash-b"


def test_attempt_metadata_tolerates_redis_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    h._patch_subprocess(monkeypatch)
    _install_task(runner)
    runner._start_current_run_record("claude", "opus")

    async def fail_delete(key: str) -> int:
        raise RuntimeError("delete failed")

    async def fail_get(key: str) -> str | None:
        raise RuntimeError("get failed")

    async def fail_set(*args: object, **kwargs: object) -> bool:
        raise RuntimeError("set failed")

    runner.redis.delete = fail_delete  # type: ignore[method-assign]
    runner.redis.get = fail_get  # type: ignore[method-assign]
    runner.redis.set = fail_set  # type: ignore[method-assign]

    asyncio.run(
        runner._prepare_current_run_record_dispatch_metadata(
            task_hash="hash-b",
            previous_task_hash="hash-a",
        )
    )

    assert runner._current_run_record is not None
    assert runner._current_run_record.attempt_index == 1
