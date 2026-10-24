"""Bounded review reminders and late review observation in WATCH."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from src.cancellation.storage import CancellationCause
from src.config import DaemonConfig
from src.daemon.handlers import watch as watch_module
from src.models import (
    CIStatus,
    FeedbackCheckResult,
    PipelineState,
    PRInfo,
    QueueTask,
    ReviewStatus,
    TaskStatus,
)
from tests.runner import _helpers as h

HEAD = "a" * 40
NOW = datetime(2026, 10, 24, 12, tzinfo=timezone.utc)


class FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz: timezone | None = None) -> datetime:
        return NOW if tz is None else NOW.astimezone(tz)


def _runner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, interval: int | None = 10):
    runner = h._make_runner(review_timeout_min=interval)
    runner.repo_path = str(tmp_path)
    task_file = tmp_path / "tasks" / "PR-005.md"
    task_file.parent.mkdir()
    task_file.write_text("# PR-005\nBranch: pr-005\n", encoding="utf-8")
    runner.state.current_task = QueueTask(
        pr_id="PR-005", title="review", status=TaskStatus.DOING,
        branch="pr-005", task_file="tasks/PR-005.md",
    )
    runner.state.current_pr = PRInfo(number=5, branch="pr-005", head_sha=HEAD)
    runner.state.state = PipelineState.WATCH
    monkeypatch.setattr(watch_module, "datetime", FrozenDateTime)

    async def publish() -> None:
        return None

    runner.publish_state = publish
    return runner


def _pr(*, review: ReviewStatus = ReviewStatus.PENDING, ci: CIStatus = CIStatus.SUCCESS):
    return PRInfo(
        number=5, branch="pr-005", head_sha=HEAD,
        review_status=review, ci_status=ci,
        publication_at=NOW - timedelta(minutes=30),
        publication_observed=True,
    )


def test_default_interval_and_explicit_override() -> None:
    assert DaemonConfig().review_timeout_min == 10
    assert DaemonConfig(review_timeout_min=17).review_timeout_min == 17


@pytest.mark.parametrize("review", [ReviewStatus.PENDING, ReviewStatus.EYES])
def test_one_reminder_then_shared_cooldown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, review: ReviewStatus
) -> None:
    runner = _runner(tmp_path, monkeypatch)
    monkeypatch.setattr(
        "src.github.comments.latest_codex_review_request_time",
        lambda *_args, **_kwargs: NOW - timedelta(minutes=11),
    )
    posts: list[int] = []
    runner._post_codex_review_result = lambda number, **_kwargs: (
        posts.append(number) is None, True, None
    )
    pr = _pr(review=review)
    asyncio.run(runner._maybe_remind_review(pr))
    asyncio.run(runner._maybe_remind_review(pr))
    assert posts == [5]
    assert runner.state.review_reminder_count == 1
    assert runner.state.state == PipelineState.WATCH


def test_override_waits_for_its_interval(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runner = _runner(tmp_path, monkeypatch, interval=20)
    monkeypatch.setattr(
        "src.github.comments.latest_codex_review_request_time",
        lambda *_args, **_kwargs: NOW - timedelta(minutes=11),
    )
    runner._post_codex_review_result = lambda *_args, **_kwargs: pytest.fail("early post")
    asyncio.run(runner._maybe_remind_review(_pr()))
    assert runner.state.review_reminder_count == 0


def test_restart_preserves_count_and_budget(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runner = _runner(tmp_path, monkeypatch)
    monkeypatch.setattr(
        "src.github.comments.latest_codex_review_request_time",
        lambda *_args, **_kwargs: NOW - timedelta(minutes=11),
    )
    runner.state.review_request_pr = 5
    runner.state.review_request_head = HEAD
    runner.state.review_request_task = "PR-005"
    from src.cancellation.storage import task_spec_content_hash

    runner.state.review_request_generation = task_spec_content_hash(
        (tmp_path / "tasks/PR-005.md").read_text(encoding="utf-8")
    )
    runner.state.review_requested_at = NOW - timedelta(minutes=11)
    runner.state.review_reminder_count = 3
    runner.state = type(runner.state).model_validate_json(runner.state.model_dump_json())
    runner._post_codex_review_result = lambda *_args, **_kwargs: pytest.fail("cap bypass")
    asyncio.run(runner._maybe_remind_review(_pr()))
    assert runner.state.review_reminder_count == 3
    assert runner.state.state == PipelineState.WATCH


def test_failed_post_has_cooldown_and_no_false_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _runner(tmp_path, monkeypatch)
    monkeypatch.setattr(
        "src.github.comments.latest_codex_review_request_time",
        lambda *_args, **_kwargs: NOW - timedelta(minutes=11),
    )
    posts: list[int] = []
    runner._post_codex_review_result = lambda number, **_kwargs: (
        posts.append(number) is not None, False, None
    )
    asyncio.run(runner._maybe_remind_review(_pr()))
    asyncio.run(runner._maybe_remind_review(_pr()))
    assert posts == [5]
    assert runner.state.review_reminder_count == 1
    assert runner.state.review_requested_at == NOW - timedelta(minutes=11)
    assert runner.state.review_post_attempted_at == NOW


def test_failed_history_read_does_not_post(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _runner(tmp_path, monkeypatch)

    def unavailable(*_args, **_kwargs):
        raise RuntimeError("GitHub unavailable")

    monkeypatch.setattr(
        "src.github.comments.latest_codex_review_request_time", unavailable
    )
    runner._post_codex_review_result = lambda *_args, **_kwargs: pytest.fail("post")
    asyncio.run(runner._maybe_remind_review(_pr()))
    assert runner.state.review_reminder_count == 0


def test_approved_review_waits_for_ci_without_reminder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _runner(tmp_path, monkeypatch)
    pr = _pr(review=ReviewStatus.APPROVED, ci=CIStatus.PENDING)
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda *_args, **_kw: [pr])
    monkeypatch.setattr("src.github.prs.get_pr_state", lambda *_args: "OPEN")

    async def quiet(*_args, **_kwargs):
        return False

    runner._scan_pr_diff_once = quiet
    runner._maybe_reclassify_stuck_pending = quiet
    runner._validate_publication_observation = lambda *_args, **_kwargs: (
        pr.publication_at, None
    )
    runner._maybe_remind_review = lambda *_args: pytest.fail("approved review reminded")
    asyncio.run(runner.handle_watch())
    assert runner.state.state == PipelineState.WATCH


def test_new_head_resets_review_budget(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    runner = _runner(tmp_path, monkeypatch)
    runner.state.review_request_pr = 5
    runner.state.review_request_head = HEAD
    runner.state.review_reminder_count = 3
    runner.state.current_pr = _pr().model_copy(update={"head_sha": "b" * 40})
    assert runner.state.review_reminder_count == 0
    assert runner.state.review_request_head == ""


@pytest.mark.parametrize("single_error_exit", [False, True])
def test_legacy_timeout_resumes_for_late_approval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, single_error_exit: bool,
) -> None:
    runner = _runner(tmp_path, monkeypatch)
    runner.repo_config.feature_flags.use_single_error_exit = single_error_exit
    runner.state.state = PipelineState.ERROR
    runner.state.skip_ai_error_diagnose = True
    runner.state.error_message = "review timeout"
    runner.state.current_queue = [runner.state.current_task]
    runner._current_run_record = None
    cause = CancellationCause(category="ERROR", payload={"subsource": "review_timeout"})

    async def read_cause(*_args, **_kwargs):
        return cause

    async def no_suppression(*_args, **_kwargs):
        return None

    async def commit(*_args, **_kwargs):
        return True

    cleared: list[str] = []

    async def clear(_redis, _name, task_id, **_kwargs):
        cleared.append(task_id)

    monkeypatch.setattr("src.daemon.runner.get_cancellation_cause", read_cause)
    monkeypatch.setattr("src.daemon.runner.safe_delete_cancellation_cause", clear)
    runner._suppression_record_for_task = no_suppression
    runner._remote_frontmatter_error_status_for_current_task = lambda: (
        "error", watch_module.SuppressionReason.REVIEW_TIMEOUT, "revision"
    )
    runner._commit_task_status_change = commit
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda *_args, **_kwargs: [
        _pr(review=ReviewStatus.APPROVED, ci=CIStatus.PENDING)
    ])

    assert asyncio.run(runner._maybe_resume_review_timeout()) is True
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_task.status == TaskStatus.DOING
    assert runner._current_run_record is None
    assert cleared == ["PR-005"]


def test_timeout_recovery_rejects_changed_head_and_other_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = _runner(tmp_path, monkeypatch)
    runner.state.state = PipelineState.ERROR

    async def read_cause(*_args, **_kwargs):
        return CancellationCause(category="ERROR", payload={"subsource": "review_timeout"})

    async def no_suppression(*_args, **_kwargs):
        return None

    monkeypatch.setattr("src.daemon.runner.get_cancellation_cause", read_cause)
    runner._suppression_record_for_task = no_suppression
    runner._remote_frontmatter_error_status_for_current_task = lambda: (
        "error", watch_module.SuppressionReason.REVIEW_TIMEOUT, "revision"
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda *_args, **_kwargs: [
        _pr(review=ReviewStatus.APPROVED).model_copy(update={"head_sha": "b" * 40})
    ])
    assert asyncio.run(runner._maybe_resume_review_timeout()) is False
    assert runner.state.state == PipelineState.ERROR
    runner.state.user_paused = True
    assert asyncio.run(runner._maybe_resume_review_timeout()) is False


@pytest.mark.parametrize("feedback", [FeedbackCheckResult.NEW, FeedbackCheckResult.UNKNOWN])
def test_timeout_recovery_requires_fresh_actionable_findings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, feedback: FeedbackCheckResult,
) -> None:
    runner = _runner(tmp_path, monkeypatch)
    runner.state.state = PipelineState.ERROR

    async def read_cause(*_args, **_kwargs):
        return CancellationCause(category="ERROR", payload={"subsource": "review_timeout"})

    async def no_suppression(*_args, **_kwargs):
        return None

    async def commit(*_args, **_kwargs):
        return True

    async def clear(*_args, **_kwargs):
        return None

    monkeypatch.setattr("src.daemon.runner.get_cancellation_cause", read_cause)
    monkeypatch.setattr("src.daemon.runner.safe_delete_cancellation_cause", clear)
    runner._suppression_record_for_task = no_suppression
    runner._remote_frontmatter_error_status_for_current_task = lambda: (
        "error", watch_module.SuppressionReason.REVIEW_TIMEOUT, "revision"
    )
    runner._commit_task_status_change = commit
    runner._rehydrate_last_push_at = lambda *_args: None
    runner._has_new_codex_feedback_since_last_push = lambda *_args: feedback
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda *_args, **_kwargs: [
        _pr(review=ReviewStatus.CHANGES_REQUESTED)
    ])
    resumed = asyncio.run(runner._maybe_resume_review_timeout())
    assert resumed is (feedback == FeedbackCheckResult.NEW)
    assert runner.state.state == (PipelineState.WATCH if resumed else PipelineState.ERROR)
