"""Tests for src/models.py."""

from __future__ import annotations

from datetime import datetime, timezone

from src.inhibitor import InhibitorType, WorkInhibitor
from src.models import (
    CIStatus,
    PipelineState,
    PRInfo,
    QueueTask,
    RepoState,
    ReviewStatus,
    TaskStatus,
)


def test_pipeline_state_values() -> None:
    assert PipelineState.PREFLIGHT.value == "PREFLIGHT"
    assert PipelineState.IDLE.value == "IDLE"
    assert PipelineState.CODING.value == "CODING"
    assert PipelineState.WATCH.value == "WATCH"
    assert PipelineState.FIX.value == "FIX"
    assert PipelineState.MERGE.value == "MERGE"
    assert PipelineState.ERROR.value == "ERROR"


def test_pipelinestate_enum_no_hung() -> None:
    assert not hasattr(PipelineState, "HUNG")


def test_repo_state_migrates_legacy_hung_payload_to_error() -> None:
    payload = (
        '{"url":"https://github.com/example/repo.git",'
        '"name":"repo","state":"HUNG"}'
    )

    state = RepoState.model_validate_json(payload)

    assert state.state == PipelineState.ERROR


def test_task_status_values() -> None:
    assert TaskStatus.TODO.value == "TODO"
    assert TaskStatus.DOING.value == "DOING"
    assert TaskStatus.DONE.value == "DONE"
    assert TaskStatus.ERROR.value == "ERROR"


def test_taskstatus_error_value() -> None:
    assert TaskStatus.ERROR.value == "ERROR"


def test_taskstatus_no_canceled() -> None:
    assert not hasattr(TaskStatus, "CANCELED")


def test_queue_task_defaults() -> None:
    task = QueueTask(pr_id="PR-002", title="Config loader and data models", status=TaskStatus.TODO)

    assert task.pr_id == "PR-002"
    assert task.title == "Config loader and data models"
    assert task.status == TaskStatus.TODO
    assert task.task_file is None
    assert task.depends_on == []
    assert task.branch is None


def test_repo_state_json_round_trip() -> None:
    now = datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc)
    state = RepoState(
        url="https://github.com/example/repo.git",
        name="repo",
        state=PipelineState.CODING,
        current_task=QueueTask(
            pr_id="PR-002",
            title="Config loader and data models",
            status=TaskStatus.DOING,
            task_file="tasks/PR-002.md",
            depends_on=["PR-001"],
            branch="pr-002-models",
        ),
        current_pr=PRInfo(
            number=2,
            branch="pr-002-models",
            ci_status=CIStatus.SUCCESS,
            review_status=ReviewStatus.EYES,
            push_count=1,
            fix_iteration_count=2,
            url="https://github.com/example/repo/pull/2",
            last_activity=now,
        ),
        error_message=None,
        last_updated=now,
        history=[{"event": "started", "at": now.isoformat()}],
        rate_limited_coders={"claude", "codex"},
        last_stale_retrigger_at=now,
    )

    payload = state.model_dump_json()
    restored = RepoState.model_validate_json(payload)

    assert restored == state


def test_repostate_current_queue_default_none() -> None:
    state = RepoState(
        url="https://github.com/example/repo.git",
        name="repo",
    )

    assert state.current_queue is None


def test_repostate_current_queue_round_trip() -> None:
    queue = [
        QueueTask(
            pr_id="PR-001",
            title="First",
            status=TaskStatus.DONE,
            task_file="tasks/PR-001.md",
            depends_on=[],
            branch="pr-001-first",
        ),
        QueueTask(
            pr_id="PR-002",
            title="Second",
            status=TaskStatus.DOING,
            task_file="tasks/PR-002.md",
            depends_on=["PR-001"],
            branch="pr-002-second",
        ),
        QueueTask(
            pr_id="PR-003",
            title="Third",
            status=TaskStatus.TODO,
            task_file="tasks/PR-003.md",
            depends_on=["PR-001"],
            branch="pr-003-third",
        ),
    ]
    state = RepoState(
        url="https://github.com/example/repo.git",
        name="repo",
        current_queue=queue,
    )

    restored = RepoState.model_validate(state.model_dump())

    assert restored.current_queue == queue


def test_pr_info_fix_iteration_count_defaults_to_zero() -> None:
    pr = PRInfo(number=7, branch="pr-007")

    assert pr.fix_iteration_count == 0


def test_repo_state_resets_stale_retrigger_on_new_pr_transition() -> None:
    state = RepoState(
        url="https://github.com/example/repo.git",
        name="repo",
        current_pr=PRInfo(number=1, branch="pr-001"),
    )
    state.last_stale_retrigger_at = datetime.now(timezone.utc)

    state.current_pr = PRInfo(number=2, branch="pr-002")

    assert state.last_stale_retrigger_at is None


def test_repo_state_keeps_stale_retrigger_when_refreshing_same_pr() -> None:
    now = datetime.now(timezone.utc)
    state = RepoState(
        url="https://github.com/example/repo.git",
        name="repo",
        current_pr=PRInfo(number=1, branch="pr-001"),
        last_stale_retrigger_at=now,
    )

    state.current_pr = PRInfo(number=1, branch="pr-001", title="refreshed")

    assert state.last_stale_retrigger_at == now


def test_repo_state_transition_helper_handles_non_prinfo_values() -> None:
    assert RepoState._is_new_pr_transition("old", "new") is True


def test_repo_state_clearing_current_task_clears_current_pr() -> None:
    state = RepoState(
        url="https://github.com/example/repo.git",
        name="repo",
        current_task=QueueTask(
            pr_id="PR-001", title="t", status=TaskStatus.DOING,
        ),
        current_pr=PRInfo(number=1, branch="pr-001"),
    )

    state.current_task = None

    assert state.current_task is None
    assert state.current_pr is None


def test_repo_state_clearing_current_task_clears_error_message() -> None:
    state = RepoState(
        url="https://github.com/example/repo.git",
        name="repo",
        current_task=QueueTask(
            pr_id="PR-001", title="t", status=TaskStatus.DOING,
        ),
        error_message="boom",
    )

    state.current_task = None

    assert state.current_task is None
    assert state.error_message is None


def test_repo_state_assigning_current_task_does_not_clear_current_pr() -> None:
    state = RepoState(
        url="https://github.com/example/repo.git",
        name="repo",
        current_pr=PRInfo(number=1, branch="pr-001"),
    )

    state.current_task = QueueTask(
        pr_id="PR-002", title="t", status=TaskStatus.DOING,
    )

    assert state.current_task is not None
    assert state.current_pr is not None
    assert state.current_pr.number == 1


def test_repo_state_assigning_current_queue_stamps_snapshot_at() -> None:
    state = RepoState(
        url="https://github.com/example/repo.git",
        name="repo",
    )
    assert state.current_queue_snapshot_at is None

    before = datetime.now(timezone.utc)
    state.current_queue = [
        QueueTask(pr_id="PR-001", title="t", status=TaskStatus.TODO),
    ]
    after = datetime.now(timezone.utc)

    stamped = state.current_queue_snapshot_at
    assert stamped is not None
    assert before <= stamped <= after


def test_repo_state_clearing_current_queue_clears_snapshot_at() -> None:
    state = RepoState(
        url="https://github.com/example/repo.git",
        name="repo",
    )
    state.current_queue = [
        QueueTask(pr_id="PR-001", title="t", status=TaskStatus.TODO),
    ]
    assert state.current_queue_snapshot_at is not None

    state.current_queue = None

    assert state.current_queue_snapshot_at is None


def test_repostate_default_inhibitors_empty_list() -> None:
    state = RepoState(
        url="https://github.com/example/repo.git",
        name="repo",
    )

    assert state.active_inhibitors == []


def test_repostate_json_round_trip_preserves_inhibitors() -> None:
    expires = datetime(2030, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    inhibitors = [
        WorkInhibitor(
            inhibitor_type=InhibitorType.USER_PAUSE,
            reason_text="Operator paused",
            source_key="state:repo.user_paused",
        ),
        WorkInhibitor(
            inhibitor_type=InhibitorType.RATE_LIMIT,
            coder_affected="claude",
            expires_at=expires,
            reason_text="claude rate-limited",
            source_key="state:repo.rate_limited_coder_until.claude",
        ),
        WorkInhibitor(
            inhibitor_type=InhibitorType.CASCADE_PANIC,
            reason_text="Cascade panic mode auto-stop",
            source_key="daemon:panic_state",
        ),
    ]
    state = RepoState(
        url="https://github.com/example/repo.git",
        name="repo",
        active_inhibitors=inhibitors,
    )

    restored = RepoState.model_validate_json(state.model_dump_json())

    assert restored.active_inhibitors == inhibitors
    assert [i.inhibitor_type for i in restored.active_inhibitors] == [
        InhibitorType.USER_PAUSE,
        InhibitorType.RATE_LIMIT,
        InhibitorType.CASCADE_PANIC,
    ]
    assert restored.active_inhibitors[1].expires_at == expires


def test_repo_state_clearing_current_pr_does_not_clear_current_task() -> None:
    task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)
    state = RepoState(
        url="https://github.com/example/repo.git",
        name="repo",
        current_task=task,
        current_pr=PRInfo(number=1, branch="pr-001"),
        error_message="active failure",
    )

    state.current_pr = None

    assert state.current_task == task
    assert state.error_message == "active failure"
