from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from src.cancellation import CancellationCause
from src.daemon import runner as runner_module
from src.daemon.handlers import error as error_module
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


def test_coarse_write_failure_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = h._make_runner()
    _stub_publish_and_save(runner)
    task = _task()
    runner.state.current_task = task
    runner.state.current_queue = [task]
    attempts: list[str] = []

    async def fake_commit(*_args: Any, **_kwargs: Any) -> bool:
        attempts.append("commit")
        return len(attempts) > 2

    monkeypatch.setattr(runner, "_commit_task_status_change", fake_commit)
    _capture_rich_write(monkeypatch)

    asyncio.run(
        runner._transition_to_error(
            "git fetch origin failed",
            commit_task_status=True,
        )
    )

    assert runner.state.state == PipelineState.ERROR
    assert runner._status_write_failed_task_pr_ids == {"PR-379"}

    asyncio.run(runner.handle_error("git fetch origin failed"))

    assert attempts == ["commit", "commit"]
    assert runner.state.state == PipelineState.ERROR
    assert runner._status_write_failed_task_pr_ids == {"PR-379"}

    asyncio.run(runner.handle_error("git fetch origin failed"))

    assert attempts == ["commit", "commit", "commit"]
    assert runner._status_write_failed_task_pr_ids == set()


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
    assert runner._status_write_failed_task_pr_ids == set()
    assert runner.state.state == PipelineState.ERROR


def test_coarse_write_retry_uses_crash_when_cause_read_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    _stub_publish_and_save(runner)
    task = _task()
    runner.state.current_task = task
    runner._status_write_failed_task_pr_ids.add(task.pr_id)
    blocked_reasons: list[SuppressionReason | str | None] = []

    async def fake_get_cause(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("redis down")

    async def fake_commit(
        *_args: Any,
        blocked_reason: SuppressionReason | str | None = None,
        **_kwargs: Any,
    ) -> bool:
        blocked_reasons.append(blocked_reason)
        return False

    monkeypatch.setattr(error_module, "get_cancellation_cause", fake_get_cause)
    monkeypatch.setattr(runner, "_commit_task_status_change", fake_commit)

    asyncio.run(runner.handle_error("still failing"))

    assert blocked_reasons == [SuppressionReason.CRASH]


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
