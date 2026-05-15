from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from src.cancellation import CancellationCause
from src.daemon.runner import _legacy_run_cause_from_cancellation
from src.metrics import RunRecord
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


@pytest.mark.parametrize(
    ("subsource", "expected_cause"),
    [
        ("crash", "CRASH"),
        ("coder_escalate", "ESCALATE"),
        ("guardrail", "ESCALATE"),
        ("review_timeout", "TIMEOUT"),
        ("fix_idle_timeout", "TIMEOUT"),
        ("fix_iteration_cap", "TIMEOUT"),
        ("no_push_deadlock", "NO_PUSH_DEADLOCK"),
        ("infra_failure", "INFRA"),
    ],
)
def test_transition_to_error_maps_pr315_subsource_to_legacy_run_cause(
    monkeypatch: pytest.MonkeyPatch,
    subsource: str,
    expected_cause: str,
) -> None:
    """PR-315 ``category="ERROR"`` causes must persist a legacy ``RunCause``.

    ``RunRecord.cause`` only accepts the legacy enum, so forwarding the raw
    new-style ``"ERROR"`` would break ``MetricsStore.get/recent``
    deserialization. The transition must translate ``payload.subsource``
    back to the legacy vocabulary before saving.
    """
    runner = h._make_runner()
    h._patch_subprocess(monkeypatch)
    _install_task(runner)
    runner.state.state = PipelineState.FIX
    runner._start_current_run_record("claude", "opus")

    cause = CancellationCause(
        category="ERROR",
        payload={"subsource": subsource, "reason_text": "boom"},
    )
    asyncio.run(
        runner._transition_to_error(
            "boom",
            publish=False,
            cancellation_cause=cause,
        )
    )

    record = runner._current_run_record
    assert record is not None
    assert record.outcome == "failed"
    assert record.cause == expected_cause
    # Round-trip through the persisted payload to confirm
    # ``RunRecord.__post_init__`` accepts the value (the bug surfaced as a
    # ``ValueError("invalid run record cause: ERROR")`` on read-back).
    rehydrated = RunRecord(
        **json.loads(json.dumps(record.__dict__))
    )
    assert rehydrated.cause == expected_cause


def test_save_run_record_propagates_subsource_from_cancellation_cause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-310: payload.subsource flows verbatim onto RunRecord.cause_subsource
    when the cancellation cause maps to an ESCALATE legacy cause.

    Without this plumbing, OBS-BE has no way to split ``cause=ESCALATE``
    rows into guardrail vs coder_escalate without re-reading the
    cancellation cause record per row — the cause subsource exists
    precisely to keep that join out of the analytics hot path.
    """
    runner = h._make_runner()
    h._patch_subprocess(monkeypatch)
    _install_task(runner)
    runner.state.state = PipelineState.FIX
    runner._start_current_run_record("claude", "opus")

    cause = CancellationCause(
        category="ERROR",
        payload={
            "subsource": "guardrail",
            "rule": "large_diff",
            "excerpt": "+12000 LOC",
        },
    )
    asyncio.run(
        runner._transition_to_error(
            "guardrail violation",
            publish=False,
            cancellation_cause=cause,
        )
    )

    record = runner._current_run_record
    assert record is not None
    assert record.outcome == "failed"
    assert record.cause == "ESCALATE"
    assert record.cause_subsource == "guardrail"


def test_save_run_record_drops_subsource_for_non_escalate_cause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-310: a non-ESCALATE legacy cause must persist ``cause_subsource=None``
    even when the cancellation cause payload carries a subsource value.

    The dataclass invariant rejects ``cause_subsource`` outside ESCALATE,
    so the runner's save path must drop the subsource rather than rely
    on the callsite to remember the constraint.
    """
    runner = h._make_runner()
    h._patch_subprocess(monkeypatch)
    _install_task(runner)
    runner.state.state = PipelineState.FIX
    runner._start_current_run_record("claude", "opus")

    cause = CancellationCause(
        category="ERROR",
        payload={
            "subsource": "review_timeout",
            "elapsed_min": 90,
        },
    )
    asyncio.run(
        runner._transition_to_error(
            "review timeout",
            publish=False,
            cancellation_cause=cause,
        )
    )

    record = runner._current_run_record
    assert record is not None
    assert record.cause == "TIMEOUT"
    assert record.cause_subsource is None


def test_legacy_run_cause_helper_prefers_legacy_category_payload() -> None:
    """When the migration preserved ``legacy_category``, surface that value."""
    cause = CancellationCause(
        category="ERROR",
        payload={
            "subsource": "guardrail",
            "legacy_category": "INFRA",
        },
    )

    assert _legacy_run_cause_from_cancellation(cause) == "INFRA"


def test_legacy_run_cause_helper_returns_none_for_unknown_subsource() -> None:
    """Unknown subsources fall back to the caller's exit-reason default."""
    cause = CancellationCause(
        category="ERROR",
        payload={"subsource": "mystery"},
    )

    assert _legacy_run_cause_from_cancellation(cause) is None
    assert _legacy_run_cause_from_cancellation(None) is None


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
