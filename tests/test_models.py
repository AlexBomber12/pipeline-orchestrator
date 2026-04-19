"""Tests for src/models.py."""

from __future__ import annotations

from datetime import datetime, timezone

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
    assert PipelineState.HUNG.value == "HUNG"
    assert PipelineState.ERROR.value == "ERROR"


def test_task_status_values() -> None:
    assert TaskStatus.TODO.value == "TODO"
    assert TaskStatus.DOING.value == "DOING"
    assert TaskStatus.DONE.value == "DONE"


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
            url="https://github.com/example/repo/pull/2",
            last_activity=now,
        ),
        error_message=None,
        last_updated=now,
        history=[{"event": "started", "at": now.isoformat()}],
        rate_limited_coders={"claude", "codex"},
    )

    payload = state.model_dump_json()
    restored = RepoState.model_validate_json(payload)

    assert restored == state
