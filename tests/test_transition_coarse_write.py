from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from src.cancellation import CancellationCause
from src.daemon import runner as runner_module
from src.daemon.handlers.error import ErrorCategory
from src.models import PipelineState, QueueTask, TaskStatus
from src.queue_parser import parse_task_header
from src.subsource_registry import SuppressionReason, error_category_to_reason
from src.web import app as web_app

from tests.runner import _helpers as h


def _task(
    pr_id: str = "PR-379",
    status: TaskStatus = TaskStatus.DOING,
) -> QueueTask:
    return QueueTask(
        pr_id=pr_id,
        title="coarse write",
        status=status,
        branch="pr-379-transition-coarse-write",
        task_file=f"tasks/{pr_id}.md",
    )


def _stub_publish_and_save(runner: Any) -> None:
    async def _noop(*_args: Any, **_kwargs: Any) -> None:
        return None

    runner.publish_state = _noop  # type: ignore[method-assign]
    runner._save_current_run_record = _noop  # type: ignore[method-assign]


def _stub_git(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    calls: list[list[str]] = []

    def fake_git(repo_path: str, *args: str, **_kwargs: Any) -> Any:
        calls.append(list(args))
        return SimpleNamespace(returncode=1, stdout="", stderr="")

    monkeypatch.setattr(runner_module.git_ops, "_git", fake_git)
    return calls


def _install_tmp_repo(
    runner: Any,
    tmp_path: Path,
    task: QueueTask,
) -> Path:
    runner.repo_path = str(tmp_path)
    task_path = tmp_path / task.task_file
    task_path.parent.mkdir(parents=True)
    task_path.write_text(
        (
            "---\nstatus: DOING\n---\n\n"
            "# PR-379: Coarse write\n"
            "Branch: pr-379-transition-coarse-write\n"
            "- Type: refactor\n"
            "- Complexity: high\n"
            "- Depends on: none\n"
        ),
        encoding="utf-8",
    )
    return task_path


def _capture_rich_write(
    monkeypatch: pytest.MonkeyPatch,
    *,
    fail: bool = False,
) -> list[str]:
    events: list[str] = []

    async def fake_record(*_args: Any, **_kwargs: Any) -> None:
        events.append("rich")
        if fail:
            raise RuntimeError("redis down")

    monkeypatch.setattr(
        runner_module,
        "safe_record_cancellation_cause",
        fake_record,
    )
    return events


def test_transition_writes_status_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_git(monkeypatch)
    _capture_rich_write(monkeypatch)
    runner = h._make_runner()
    _stub_publish_and_save(runner)
    task = _task()
    task_path = _install_tmp_repo(runner, tmp_path, task)
    runner.state.current_task = task

    asyncio.run(
        runner._transition_to_error(
            "subprocess crashed",
            commit_task_status=True,
        )
    )

    header = parse_task_header(task_path)
    assert header.frontmatter_status == "error"
    assert header.blocked_reason == SuppressionReason.CRASH.value


def test_guardrail_transition_writes_guardrail_reason(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_git(monkeypatch)
    _capture_rich_write(monkeypatch)
    runner = h._make_runner()
    _stub_publish_and_save(runner)
    task = _task()
    task_path = _install_tmp_repo(runner, tmp_path, task)
    runner.state.current_task = task

    asyncio.run(
        runner._transition_to_error(
            "GUARDRAIL: secret: token",
            cancellation_cause=CancellationCause(
                category="ERROR",
                payload={"subsource": "guardrail"},
            ),
            commit_task_status=True,
        )
    )

    assert parse_task_header(task_path).blocked_reason == "guardrail"


def test_retry_button_visible_after_transition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_git(monkeypatch)
    _capture_rich_write(monkeypatch)
    runner = h._make_runner()
    _stub_publish_and_save(runner)
    task = _task()
    task_path = _install_tmp_repo(runner, tmp_path, task)
    runner.state.current_task = task

    asyncio.run(
        runner._transition_to_error(
            "subprocess crashed",
            commit_task_status=True,
        )
    )
    header = parse_task_header(task_path)
    template = web_app.templates.get_template("components/tasks_panel.html")
    rendered = template.render(
        repo_name="octo__demo",
        tasks_total=1,
        retry_cap=3,
        tasks_by_status={
            "doing": [],
            "todo": [],
            "done": [],
            "error": [
                {
                    "pr_id": task.pr_id,
                    "title": task.title,
                    "branch": task.branch,
                    "retry_count": 0,
                    "cancellation_subsource": header.blocked_reason,
                }
            ],
        },
    )

    assert ">Error<" in rendered
    assert "hx-post=\"/repos/octo__demo/tasks/PR-379/retry\"" in rendered
    assert "Retry" in rendered


def test_pre_writing_callsites_no_double_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    _stub_publish_and_save(runner)
    task = _task()
    runner.state.current_task = task
    writes: list[tuple[str, str]] = []

    async def fake_commit(
        current_task: QueueTask,
        status: str,
        _reason: str,
        blocked_reason: SuppressionReason | str | None = None,
    ) -> bool:
        writes.append((current_task.pr_id, str(blocked_reason)))
        return True

    monkeypatch.setattr(runner, "_commit_task_status_change", fake_commit)
    _capture_rich_write(monkeypatch)

    asyncio.run(
        runner._commit_task_status_change(
            task,
            "ERROR",
            "hung",
            blocked_reason=SuppressionReason.REVIEW_TIMEOUT,
        )
    )
    asyncio.run(
        runner._transition_to_error(
            "hung",
            cancellation_cause=CancellationCause(
                category="ERROR",
                payload={"subsource": "review_timeout"},
            ),
            commit_task_status=False,
        )
    )

    assert writes == [("PR-379", "review_timeout")]


def test_coarse_before_rich(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = h._make_runner()
    _stub_publish_and_save(runner)
    runner.state.current_task = _task()
    events: list[str] = []

    async def fake_commit(*_args: Any, **_kwargs: Any) -> bool:
        events.append("coarse")
        return True

    async def fake_record(*_args: Any, **_kwargs: Any) -> None:
        events.append("rich")
        raise RuntimeError("redis down")

    monkeypatch.setattr(runner, "_commit_task_status_change", fake_commit)
    monkeypatch.setattr(
        runner_module,
        "safe_record_cancellation_cause",
        fake_record,
    )

    asyncio.run(
        runner._transition_to_error(
            "subprocess crashed",
            commit_task_status=True,
        )
    )

    assert events == ["coarse", "rich"]
    assert runner.state.state == PipelineState.ERROR


def test_status_write_failed_marker_creates_suppression_when_needed() -> None:
    runner = h._make_runner()
    task = _task()

    asyncio.run(
        runner._mark_status_write_failed_task(
            task,
            blocked_reason=SuppressionReason.CRASH,
            detail={"subsource": "crash"},
        )
    )

    record = asyncio.run(runner._suppression_record_for_task("PR-379"))
    assert record is not None
    assert record.reason == SuppressionReason.CRASH


def test_status_write_failed_fallback_suppresses_when_cause_record_missing() -> None:
    runner = h._make_runner()
    task = _task()

    asyncio.run(
        runner._mark_status_write_failed_task(
            task,
            blocked_reason=SuppressionReason.NO_PUSH_DEADLOCK,
            ensure_suppression=False,
        )
    )

    record = asyncio.run(runner._suppression_record_for_task("PR-379"))
    assert record is not None
    assert record.reason == SuppressionReason.NO_PUSH_DEADLOCK
    assert runner._task_suppression_blocks_selection(record.reason) is True


def test_status_write_failed_fallback_preserves_existing_suppression() -> None:
    runner = h._make_runner()
    task = _task()
    asyncio.run(
        runner._suppress_task(
            "PR-379",
            SuppressionReason.REVIEW_TIMEOUT,
            {"subsource": "review_timeout"},
        )
    )

    asyncio.run(
        runner._mark_status_write_failed_task(
            task,
            blocked_reason=SuppressionReason.NO_PUSH_DEADLOCK,
            ensure_suppression=False,
        )
    )

    record = asyncio.run(runner._suppression_record_for_task("PR-379"))
    assert record is not None
    assert record.reason == SuppressionReason.REVIEW_TIMEOUT


def test_status_write_failed_fallback_replaces_nonblocking_suppression() -> None:
    runner = h._make_runner()
    task = _task()
    asyncio.run(
        runner._suppress_task(
            "PR-379",
            SuppressionReason.INFRA_FAILURE,
            {"subsource": "infra_failure"},
        )
    )

    asyncio.run(
        runner._mark_status_write_failed_task(
            task,
            blocked_reason=SuppressionReason.NO_PUSH_DEADLOCK,
            ensure_suppression=False,
        )
    )

    record = asyncio.run(runner._suppression_record_for_task("PR-379"))
    assert record is not None
    assert record.reason == SuppressionReason.NO_PUSH_DEADLOCK
    assert runner._task_suppression_blocks_selection(record.reason) is True


def test_status_write_failed_fallback_tolerates_suppression_store_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    task = _task()
    runner.state.current_queue = [task]
    published: list[bool] = []

    async def fail_record(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("redis unavailable")

    async def fail_suppress(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("redis still unavailable")

    async def publish() -> None:
        published.append(True)

    monkeypatch.setattr(runner, "_suppression_record_for_task", fail_record)
    monkeypatch.setattr(runner, "_suppress_task", fail_suppress)
    monkeypatch.setattr(runner, "publish_state", publish)

    asyncio.run(
        runner._mark_status_write_failed_task(
            task,
            blocked_reason=SuppressionReason.NO_PUSH_DEADLOCK,
        )
    )

    assert runner.state.current_queue[0].status == TaskStatus.ERROR
    assert runner._status_write_failed_task_pr_ids == {"PR-379"}
    assert published == [True]
    assert any(
        "failed to read suppression for status-write fallback PR-379"
        in event["event"]
        for event in runner.state.history
    )
    assert any(
        "failed to record status-write fallback suppression for PR-379"
        in event["event"]
        for event in runner.state.history
    )


def test_transition_backfills_suppression_when_safe_record_is_best_effort(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner(
        feature_flags=h.FeatureFlags(use_single_error_exit=True)
    )
    _stub_publish_and_save(runner)
    task = _task()
    runner.state.current_task = task

    async def fake_commit(*_args: Any, **_kwargs: Any) -> bool:
        return False

    async def fake_safe_record(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(runner, "_commit_task_status_change", fake_commit)
    monkeypatch.setattr(
        runner_module,
        "safe_record_cancellation_cause",
        fake_safe_record,
    )

    asyncio.run(
        runner._transition_to_error(
            "subprocess crashed",
            commit_task_status=True,
        )
    )

    record = asyncio.run(runner._suppression_record_for_task("PR-379"))
    assert record is not None
    assert record.reason == SuppressionReason.CRASH


def test_transition_does_not_write_task_status_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    _stub_publish_and_save(runner)
    runner.state.current_task = _task()
    writes: list[str] = []

    async def fake_commit(*_args: Any, **_kwargs: Any) -> bool:
        writes.append("commit")
        return True

    monkeypatch.setattr(runner, "_commit_task_status_change", fake_commit)
    _capture_rich_write(monkeypatch)

    asyncio.run(runner._transition_to_error("git fetch origin failed"))

    assert writes == []
    assert runner.state.state == PipelineState.ERROR


@pytest.mark.parametrize(
    "cause",
    [None]
    + [
        CancellationCause(
            category="ERROR",
            payload={"subsource": "not_a_known_reason"},
        )
    ]
    + [
        CancellationCause(category="ERROR", payload={"subsource": reason.value})
        for reason in SuppressionReason
    ]
    + [
        CancellationCause(category=category.value, payload={})
        for category in ErrorCategory
    ],
)
def test_every_callsite_reason_nonnull(
    cause: CancellationCause | None,
) -> None:
    runner = h._make_runner()

    assert runner._suppression_reason_from_cancellation(cause) is not None


@pytest.mark.parametrize(
    ("cause", "expected"),
    [
        (
            CancellationCause(
                category="ERROR",
                payload={"subsource": "guardrail"},
            ),
            SuppressionReason.GUARDRAIL,
        ),
        (
            CancellationCause(
                category="ERROR",
                payload={"subsource": "infra_failure"},
            ),
            SuppressionReason.INFRA_FAILURE,
        ),
        (
            CancellationCause(
                category="ERROR",
                payload={"subsource": "review_timeout"},
            ),
            SuppressionReason.REVIEW_TIMEOUT,
        ),
        (None, SuppressionReason.CRASH),
        (
            CancellationCause(category=ErrorCategory.GIT_ERROR.value, payload={}),
            error_category_to_reason(ErrorCategory.GIT_ERROR),
        ),
    ],
)
def test_reason_matches_cause(
    cause: CancellationCause | None,
    expected: SuppressionReason,
) -> None:
    runner = h._make_runner()

    assert runner._suppression_reason_from_cancellation(cause) == expected
