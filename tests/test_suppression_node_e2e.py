from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from src.cancellation import CancellationCause
from src.cancellation.storage import cause_key, index_key
from src.daemon import runner as runner_module
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus
from src.queue_parser import TaskHeader, parse_task_header, write_frontmatter_status
from src.subsource_registry import SuppressionReason
from src.task_status import MergedState, derive_task_status
from src.web import app as web_app

from tests.runner import _helpers as h


def _task(
    pr_id: str = "PR-385",
    *,
    status: TaskStatus = TaskStatus.DOING,
) -> QueueTask:
    return QueueTask(
        pr_id=pr_id,
        title="Suppression node invariant",
        status=status,
        branch="pr-385-node-invariant-test",
        task_file=f"tasks/{pr_id}.md",
    )


def _write_task(repo_path: Path, task: QueueTask, status: str = "DOING") -> Path:
    task_path = repo_path / str(task.task_file)
    task_path.parent.mkdir(parents=True, exist_ok=True)
    task_path.write_text(
        f"---\nstatus: {status}\n---\n\n"
        f"# {task.pr_id}: Suppression node invariant\n\n"
        "Branch: pr-385-node-invariant-test\n"
        "- Type: refactor\n"
        "- Complexity: medium\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )
    return task_path


def _runner(tmp_path: Path, task: QueueTask | None = None) -> Any:
    runner = h._make_runner(
        feature_flags=h.FeatureFlags(use_single_error_exit=True)
    )
    runner.app_config.daemon.error_handler_use_ai = False
    runner.repo_path = str(tmp_path)
    current = task or _task()
    runner.state.current_task = current
    runner.state.current_queue = [current]

    async def _noop(*_args: Any, **_kwargs: Any) -> None:
        return None

    runner.publish_state = _noop  # type: ignore[method-assign]
    runner._save_current_run_record = _noop  # type: ignore[method-assign]
    return runner


def _cause(reason: SuppressionReason) -> CancellationCause:
    return CancellationCause(
        category="ERROR",
        payload={
            "subsource": reason.value,
            "reason_text": f"{reason.value} invariant trip",
        },
    )


def _message(reason: SuppressionReason) -> str:
    if reason == SuppressionReason.GUARDRAIL:
        return "GUARDRAIL: secret: token"
    if reason == SuppressionReason.INFRA_FAILURE:
        return "git fetch origin failed"
    return f"{reason.value} invariant trip"


def _install_coarse_writer(runner: Any, events: list[str] | None = None) -> None:
    async def fake_commit(
        current_task: QueueTask,
        status: str,
        _reason: str,
        blocked_reason: SuppressionReason | str | None = None,
    ) -> bool:
        if events is not None:
            events.append("coarse")
        write_frontmatter_status(
            Path(runner.repo_path) / str(current_task.task_file),
            status,
            blocked_reason,
        )
        return True

    runner._commit_task_status_change = fake_commit  # type: ignore[method-assign]


async def _transition(
    runner: Any,
    reason: SuppressionReason,
) -> None:
    await runner._transition_to_error(
        _message(reason),
        cancellation_cause=_cause(reason),
        commit_task_status=True,
    )


def _render_error_group(task: QueueTask, reason: SuppressionReason) -> str:
    template = web_app.templates.get_template("components/tasks_panel.html")
    return template.render(
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
                    "cancellation_subsource": reason.value,
                }
            ],
        },
    )


def _assert_no_legacy_storage(runner: Any, task_id: str) -> None:
    legacy_key_prefixes = (
        "status_write_failed_tasks:",
        "recovered_tasks:",
        "diagnose_exhausted:",
    )
    assert not getattr(runner.state, "quarantined_prs", set())
    assert not getattr(runner, "_stopped_task_pr_ids", set())
    assert not getattr(runner, "_crashed_task_pr_ids", set())
    assert runner.state.skip_ai_error_diagnose is False
    assert runner._error_diagnose_count == 0
    assert runner._error_skip_count == 0
    assert all(
        not key.startswith(legacy_key_prefixes)
        for key in runner.redis.store
    )
    assert all(
        "diagnose_attempts" not in raw
        for raw in runner.redis.store.values()
        if isinstance(raw, str)
    )
    assert cause_key(runner.name, task_id) in runner.redis.store


def test_coarse_before_rich_on_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = _task()
    task_path = _write_task(tmp_path, task)
    runner = _runner(tmp_path, task)
    events: list[str] = []
    _install_coarse_writer(runner, events)
    original_record = runner_module.safe_record_cancellation_cause

    async def record_spy(*args: Any, **kwargs: Any) -> None:
        events.append("rich")
        header = parse_task_header(task_path)
        assert header.frontmatter_status == "error"
        assert header.blocked_reason == SuppressionReason.GUARDRAIL.value
        await original_record(*args, **kwargs)

    monkeypatch.setattr(runner_module, "safe_record_cancellation_cause", record_spy)

    asyncio.run(_transition(runner, SuppressionReason.GUARDRAIL))

    assert events == ["coarse", "rich"]


def test_single_owner_no_legacy_storage(tmp_path: Path) -> None:
    task = _task()
    _write_task(tmp_path, task)
    runner = _runner(tmp_path, task)
    _install_coarse_writer(runner)

    asyncio.run(_transition(runner, SuppressionReason.GUARDRAIL))

    record = asyncio.run(runner._suppression_record_for_task(task.pr_id))
    assert record is not None
    assert record.reason == SuppressionReason.GUARDRAIL
    _assert_no_legacy_storage(runner, task.pr_id)


@pytest.mark.parametrize(
    "reason",
    [
        SuppressionReason.GUARDRAIL,
        SuppressionReason.NO_PUSH_DEADLOCK,
        SuppressionReason.FIX_ITERATION_CAP,
        SuppressionReason.INFRA_FAILURE,
    ],
)
def test_visible_exit_always(
    tmp_path: Path,
    reason: SuppressionReason,
) -> None:
    task = _task(pr_id=f"PR-385-{reason.value.replace('_', '-')}")
    task_path = _write_task(tmp_path, task)
    runner = _runner(tmp_path, task)
    _install_coarse_writer(runner)

    asyncio.run(_transition(runner, reason))

    header = parse_task_header(task_path)
    rendered = _render_error_group(task, reason)
    assert runner.state.state == PipelineState.ERROR
    assert header.frontmatter_status == "error"
    assert header.blocked_reason == reason.value
    assert ">Error<" in rendered
    assert f'hx-post="/repos/octo__demo/tasks/{task.pr_id}/retry"' in rendered
    assert "Retry" in rendered


@pytest.mark.parametrize("clear_path", ["retry", "reupload", "repo-reset"])
def test_operator_clear_all_paths(
    tmp_path: Path,
    clear_path: str,
) -> None:
    task = _task(pr_id=f"PR-385-{clear_path}")
    task_path = _write_task(tmp_path, task)
    runner = _runner(tmp_path, task)
    _install_coarse_writer(runner)
    asyncio.run(_transition(runner, SuppressionReason.GUARDRAIL))

    asyncio.run(runner._clear_task_suppression(task.pr_id))
    write_frontmatter_status(task_path, "TODO")
    asyncio.run(runner._handle_single_error_exit())

    assert asyncio.run(runner._suppression_record_for_task(task.pr_id)) is None
    assert parse_task_header(task_path).frontmatter_status == "todo"
    assert runner.state.state == PipelineState.IDLE
    assert any("operator-cleared ERROR task frontmatter" in e["event"] for e in runner.state.history)


def test_self_healing_auto_exits(tmp_path: Path) -> None:
    task = _task()
    _write_task(tmp_path, task)
    runner = _runner(tmp_path, task)
    _install_coarse_writer(runner)
    asyncio.run(_transition(runner, SuppressionReason.INFRA_FAILURE))

    asyncio.run(runner._handle_single_error_exit())

    assert runner.state.state == PipelineState.IDLE
    assert asyncio.run(runner._suppression_record_for_task(task.pr_id)) is None
    assert any("AI diagnosis disabled for self-healing ERROR" in e["event"] for e in runner.state.history)


def test_operator_clearable_stays_until_cleared(tmp_path: Path) -> None:
    task = _task()
    task_path = _write_task(tmp_path, task)
    runner = _runner(tmp_path, task)
    _install_coarse_writer(runner)
    diagnose_calls: list[str] = []

    async def diagnose_spy(*_args: Any, **_kwargs: Any) -> None:
        diagnose_calls.append("diagnose")

    runner.handle_error = diagnose_spy  # type: ignore[method-assign]
    asyncio.run(_transition(runner, SuppressionReason.GUARDRAIL))
    asyncio.run(runner._handle_single_error_exit())

    assert runner.state.state == PipelineState.ERROR
    assert diagnose_calls == []
    assert any("guardrail operator-clearable ERROR park active" in e["event"] for e in runner.state.history)

    asyncio.run(runner._clear_task_suppression(task.pr_id))
    write_frontmatter_status(task_path, "TODO")
    asyncio.run(runner._handle_single_error_exit())

    assert runner.state.state == PipelineState.IDLE
    assert diagnose_calls == []


def test_redis_flush_keeps_park(tmp_path: Path) -> None:
    task = _task()
    task_path = _write_task(tmp_path, task)
    runner = _runner(tmp_path, task)
    _install_coarse_writer(runner)
    diagnose_calls: list[str] = []
    runner.handle_error = (  # type: ignore[method-assign]
        lambda *_args, **_kwargs: diagnose_calls.append("diagnose")
    )
    asyncio.run(_transition(runner, SuppressionReason.GUARDRAIL))
    runner.redis.store.clear()
    runner.redis.zsets.clear()

    asyncio.run(runner._handle_single_error_exit())

    assert parse_task_header(task_path).blocked_reason == "guardrail"
    assert asyncio.run(runner._suppression_record_for_task(task.pr_id)) is None
    assert runner.state.state == PipelineState.ERROR
    assert diagnose_calls == []


def test_variant2_zombie_prevented() -> None:
    status = derive_task_status(
        TaskHeader(
            pr_id="PR-385",
            title="Suppression node invariant",
            branch="pr-385-node-invariant-test",
            task_type="refactor",
            complexity="medium",
            depends_on=[],
            priority=1,
            coder="any",
            frontmatter_status="done",
        ),
        MergedState(merged_pr_ids=set(), merged_branches=set(), api_available=True),
        [PRInfo(number=385, branch="pr-385-node-invariant-test")],
    )

    assert status == TaskStatus.DOING


def test_guardrail_full_lifecycle(tmp_path: Path) -> None:
    task = _task()
    task_path = _write_task(tmp_path, task)
    runner = _runner(tmp_path, task)
    _install_coarse_writer(runner)
    diagnose_calls: list[str] = []

    async def diagnose_spy(*_args: Any, **_kwargs: Any) -> None:
        diagnose_calls.append("diagnose")

    runner.handle_error = diagnose_spy  # type: ignore[method-assign]

    asyncio.run(_transition(runner, SuppressionReason.GUARDRAIL))
    header = parse_task_header(task_path)
    assert header.frontmatter_status == "error"
    assert header.blocked_reason == "guardrail"
    assert "Retry" in _render_error_group(task, SuppressionReason.GUARDRAIL)
    assert asyncio.run(runner._suppression_record_for_task(task.pr_id)) is not None

    asyncio.run(runner._handle_single_error_exit())
    assert runner.state.state == PipelineState.ERROR
    assert diagnose_calls == []

    asyncio.run(runner._clear_task_suppression(task.pr_id))
    write_frontmatter_status(task_path, "TODO")
    asyncio.run(runner._handle_single_error_exit())

    assert asyncio.run(runner._suppression_record_for_task(task.pr_id)) is None
    assert parse_task_header(task_path).frontmatter_status == "todo"
    assert runner.state.state == PipelineState.IDLE
    assert diagnose_calls == []


@pytest.mark.parametrize(
    "reason",
    [
        SuppressionReason.CODER_ESCALATE,
        SuppressionReason.GUARDRAIL,
        SuppressionReason.FIX_ITERATION_CAP,
        SuppressionReason.NO_PUSH_DEADLOCK,
        SuppressionReason.DIAGNOSE_EXHAUSTED,
        SuppressionReason.OPERATOR_STOPPED,
    ],
)
def test_eight_mechanisms_collapsed(
    tmp_path: Path,
    reason: SuppressionReason,
) -> None:
    task = _task(pr_id=f"PR-385-{reason.value.replace('_', '-')}")
    _write_task(tmp_path, task)
    runner = _runner(tmp_path, task)
    _install_coarse_writer(runner)

    asyncio.run(_transition(runner, reason))

    _assert_no_legacy_storage(runner, task.pr_id)
    assert index_key(runner.name) in runner.redis.zsets
