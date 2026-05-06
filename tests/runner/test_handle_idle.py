"""PR-224a: handle_idle handler tests for src/daemon/runner.py

Mechanical move from tests/test_runner.py. Helpers and fixtures still live in
tests/test_runner.py and are referenced via the ``h`` alias.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from src.coders import claude as claude_plugin_module
from src.daemon import git_ops as git_ops_module
from src.daemon import runner as runner_module
from src.daemon.handlers import idle as idle_module
from src.models import (
    CIStatus,
    PipelineState,
    PRInfo,
    QueueTask,
    ReviewStatus,
    TaskStatus,
)
from src.queue_parser import QueueValidationError, TaskHeader
from src.task_status import MergedState

from tests.runner import _helpers as h

claude_cli = claude_plugin_module.claude_cli


def _merged_state(
    pr_ids: set[str] | None = None,
    branches: set[str] | None = None,
    *,
    api_available: bool = True,
) -> MergedState:
    return MergedState(set(pr_ids or ()), set(branches or ()), api_available)


@pytest.fixture(autouse=True)
def _default_no_merged_branch_api(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.task_status.gh_pr_get_merged_branches",
        lambda repo, branches: set(),
    )


def test_handle_idle_no_tasks_leaves_state_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr("src.github.prs.get_merged_prs", lambda repo, branch, refresh=False: [])

    runner = h._make_runner()
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 0
    assert any("No tasks" in e["event"] for e in runner.state.history)
    # sync_to_main must run fetch -> checkout -> reset --hard in order so
    # that parse_queue reads QUEUE.md from the tip of origin/{branch}, not
    # whatever branch/commit the repo was left on by a prior cycle.
    commands = [cmd[:4] for cmd in calls]
    fetch_idx = commands.index(["git", "fetch", "--prune", "origin"])
    checkout_idx = next(i for i, cmd in enumerate(commands) if cmd[:2] == ["git", "checkout"])
    reset_idx = next(i for i, cmd in enumerate(commands) if cmd[:2] == ["git", "reset"])
    assert fetch_idx < checkout_idx < reset_idx
    # No git pull anywhere: sync_to_main replaced it with reset --hard.
    assert not any(cmd[:2] == ["git", "pull"] for cmd in calls)
    # ``git reset --hard`` only removes tracked-file edits; untracked
    # files left by a crashed prior cycle would otherwise survive into
    # the next preflight as a dirty tree. ``git clean -fd`` after the
    # reset guarantees the working copy matches origin/{branch}.
    clean_idx = next(i for i, cmd in enumerate(commands) if cmd[:2] == ["git", "clean"])
    assert reset_idx < clean_idx
    assert ["git", "clean", "-fd"] in calls


def test_handle_idle_picks_task_and_drives_coding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-042",
        title="Sample",
        status=TaskStatus.TODO,
        branch="pr-042-sample",
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: task)

    claude_calls: list[str] = []

    async def fake_run_planned_pr(
        path: str, *_args: object, **kwargs: object
    ) -> tuple[int, str, str]:
        claude_calls.append(path)
        return (0, "ok", "")

    monkeypatch.setattr(claude_cli, "run_auto_pr_async", fake_run_planned_pr)

    opened_pr = PRInfo(
        number=17,
        branch="pr-042-sample",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )
    # First call (guard in handle_idle) returns no matching PR;
    # subsequent calls (handle_coding) return the opened PR.
    call_count = {"n": 0}

    def _get_open_prs(repo: str, **kw: Any) -> list[PRInfo]:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return []  # guard: no existing PR
        return [opened_pr]

    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        _get_open_prs,
    )
    monkeypatch.setattr("src.github.prs.get_merged_prs", lambda repo, branch, refresh=False: [])
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )

    runner = h._make_runner()
    asyncio.run(runner.handle_idle())

    assert claude_calls == [runner.repo_path]
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 17
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-042"
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 1


def test_handle_idle_sets_queue_counters_with_mixed_statuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    tasks = [
        QueueTask(pr_id="PR-001", title="Done1", status=TaskStatus.DONE, branch="pr-001"),
        QueueTask(pr_id="PR-002", title="Done2", status=TaskStatus.DONE, branch="pr-002"),
        QueueTask(pr_id="PR-003", title="Todo", status=TaskStatus.TODO, branch="pr-003"),
    ]
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: tasks)
    monkeypatch.setattr(idle_module, "get_next_task", lambda t: tasks[2])
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, open_pr_branches: tasks,
    )
    monkeypatch.setattr(
        claude_cli,
        "run_auto_pr_async",
        h._async_cli_result(0, "ok", ""),
    )
    # First call (guard) returns no matching PR; subsequent calls return the PR.
    call_count = {"n": 0}

    def _get_open_prs(repo: str, **kw: Any) -> list[PRInfo]:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return []
        return [PRInfo(number=1, branch="pr-003")]

    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        _get_open_prs,
    )
    monkeypatch.setattr("src.github.prs.get_merged_prs", lambda repo, branch, refresh=False: [])
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )

    runner = h._make_runner()
    asyncio.run(runner.handle_idle())

    assert runner.state.queue_done == 2
    assert runner.state.queue_total == 3


def test_handle_idle_publishes_progress_update_only_for_new_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    tasks = [
        QueueTask(pr_id="PR-001", title="Done", status=TaskStatus.DONE, branch="pr-001"),
        QueueTask(pr_id="PR-002", title="Todo", status=TaskStatus.TODO, branch="pr-002"),
    ]
    published: list[tuple[str, str, dict[str, int], object | None]] = []

    async def _fake_publish_repo_event(
        repo_name: str,
        event_type: str,
        payload: dict[str, int],
        redis_client: object | None = None,
    ) -> None:
        published.append((repo_name, event_type, payload, redis_client))

    async def _fake_handle_coding() -> None:
        return None

    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: tasks)
    monkeypatch.setattr(idle_module, "get_next_task", lambda t: tasks[1])
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, open_pr_branches: tasks,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr("src.github.prs.get_merged_prs", lambda repo, branch, refresh=False: [])
    monkeypatch.setattr(runner_module, "publish_repo_event", _fake_publish_repo_event)

    runner = h._make_runner()
    runner.handle_coding = _fake_handle_coding  # type: ignore[method-assign]

    asyncio.run(runner.handle_idle())
    asyncio.run(runner.publish_state())
    runner._set_queue_progress(1, 2)
    asyncio.run(runner.publish_state())

    progress_events = [event for event in published if event[1] == "progress_updated"]
    assert progress_events == [
        (
            runner.name,
            "progress_updated",
            {"queue_done": 1, "queue_total": 2},
            runner.redis,
        )
    ]


def test_handle_idle_uses_dag_when_headers_present(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: Bootstrap\n\n"
        "Branch: pr-001-bootstrap\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-002.md").write_text(
        "# PR-002: Next task\n\n"
        "Branch: pr-002-next-task\n"
        "- Type: feature\n"
        "- Complexity: medium\n"
        "- Depends on: PR-001\n"
        "- Priority: 2\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )
    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state({"PR-001"}))
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert coding_called["v"] is True
    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-002"
    assert runner.state.current_task.task_file == "tasks/PR-002.md"
    assert runner.state.queue_done == 1
    assert runner.state.queue_total == 2


def test_handle_idle_falls_back_to_queue_md(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text("No structured header here\n", encoding="utf-8")
    (tasks_dir / "PR-099.md").write_text("Legacy fallback task body\n", encoding="utf-8")

    fallback_task = QueueTask(
        pr_id="PR-099",
        title="Fallback queue task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-099.md",
        branch="pr-099-fallback",
    )
    parse_calls: list[str] = []
    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: (parse_calls.append(path) or [fallback_task]),
    )
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: fallback_task)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert parse_calls == [str(tasks_dir / "QUEUE.md")]
    assert coding_called["v"] is True
    assert runner.state.current_task == fallback_task
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 1


def test_handle_idle_dag_skips_files_without_headers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: Structured\n\n"
        "Branch: pr-001-structured\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-002.md").write_text(
        "Missing structured metadata\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )
    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state())
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert coding_called["v"] is True
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-001"
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 1


def test_handle_idle_dag_surfaces_malformed_task_headers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: Broken structured task\n\n"
        "Branch: pr-001-broken\n"
        "- Type: definitely-not-valid\n"
        "- Complexity: low\n"
        "- Depends on: none\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message is not None
    assert "invalid Type" in runner.state.error_message


def test_handle_idle_dag_falls_back_for_legacy_task_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: Legacy task\n\nBranch: pr-001-legacy\n",
        encoding="utf-8",
    )

    fallback_task = QueueTask(
        pr_id="PR-001",
        title="Legacy task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-001.md",
        branch="pr-001-legacy",
    )
    parse_calls: list[str] = []

    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )
    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: parse_calls.append(path) or [fallback_task],
    )
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda *args, **kwargs: [fallback_task],
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: fallback_task)

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert parse_calls == [str(tasks_dir / "QUEUE.md")]
    assert coding_called["v"] is True
    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task == fallback_task
    assert runner.state.error_message is None


def test_handle_idle_dag_falls_back_when_structured_task_depends_on_legacy_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: Legacy task\n\nBranch: pr-001-legacy\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-002.md").write_text(
        "# PR-002: Structured task\n\n"
        "Branch: pr-002-structured\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: PR-001\n",
        encoding="utf-8",
    )

    fallback_task = QueueTask(
        pr_id="PR-001",
        title="Legacy task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-001.md",
        branch="pr-001-legacy",
    )
    parse_calls: list[str] = []

    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state())
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )
    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: parse_calls.append(path) or [fallback_task],
    )
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda *args, **kwargs: [fallback_task],
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: fallback_task)

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert parse_calls == [str(tasks_dir / "QUEUE.md")]
    assert coding_called["v"] is True
    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task == fallback_task
    assert runner.state.error_message is None


def test_handle_idle_dag_falls_back_when_structured_task_depends_on_missing_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "QUEUE.md").write_text(
        "## PR-001: Queue-only dependency\n"
        "- Status: TODO\n"
        "- Tasks file: tasks/PR-001.md\n"
        "- Branch: pr-001-queue-only\n\n"
        "## PR-002: Structured task\n"
        "- Status: TODO\n"
        "- Tasks file: tasks/PR-002.md\n"
        "- Branch: pr-002-structured\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-001.md").write_text(
        "Queue-only dependency body without structured header\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-002.md").write_text(
        "# PR-002: Structured task\n\n"
        "Branch: pr-002-structured\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: PR-001\n",
        encoding="utf-8",
    )

    fallback_task = QueueTask(
        pr_id="PR-001",
        title="Queue-only dependency",
        status=TaskStatus.TODO,
        task_file="tasks/PR-001.md",
        branch="pr-001-queue-only",
    )
    parse_calls: list[str] = []

    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state())
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )
    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: parse_calls.append(path) or [fallback_task],
    )
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda *args, **kwargs: [fallback_task],
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: fallback_task)

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert parse_calls == [str(tasks_dir / "QUEUE.md")]
    assert coding_called["v"] is True
    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task == fallback_task
    assert runner.state.error_message is None


def test_handle_idle_keeps_independent_dag_task_when_other_dependency_file_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-002.md").write_text(
        "# PR-002: Blocked structured task\n\n"
        "Branch: pr-002-blocked\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: PR-001\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-003.md").write_text(
        "# PR-003: Independent structured task\n\n"
        "Branch: pr-003-independent\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state())
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert coding_called["v"] is True
    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-003"
    assert runner.state.current_task.branch == "pr-003-independent"
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 1
    assert runner.state.error_message is None


def test_handle_idle_keeps_structured_task_when_legacy_dependency_is_already_done(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-002.md").write_text(
        "# PR-002: Structured task\n\n"
        "Branch: pr-002-structured\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: PR-001\n",
        encoding="utf-8",
    )

    def fake_resolve_merged_state(
        repo_path: str,
        base_branch: str,
        owner_repo: str,
        candidate_pr_ids,
        headers,
        *,
        log_event,
    ) -> MergedState:
        assert repo_path == str(tmp_path)
        assert base_branch == "main"
        assert owner_repo == "octo/demo"
        assert set(candidate_pr_ids or ()) == {"PR-001", "PR-002"}
        assert {header.pr_id for header in headers} == {"PR-002"}
        return _merged_state({"PR-001"})

    monkeypatch.setattr(
        idle_module,
        "_resolve_merged_state",
        fake_resolve_merged_state,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert coding_called["v"] is True
    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-002"
    assert runner.state.current_task.branch == "pr-002-structured"
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 1
    assert runner.state.error_message is None
    assert runner._idle_dag_headers[0].depends_on == []
    assert runner._idle_dag_tasks[0].depends_on == []


def test_handle_idle_prefers_legacy_queue_task_over_dag_task(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "QUEUE.md").write_text(
        "## PR-001: Legacy task\n"
        "- Status: TODO\n"
        "- Tasks file: tasks/PR-001.md\n"
        "- Branch: pr-001-legacy\n\n"
        "## PR-002: Structured task\n"
        "- Status: TODO\n"
        "- Tasks file: tasks/PR-002.md\n"
        "- Branch: pr-002-structured\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: Legacy task\n\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-002.md").write_text(
        "# PR-002: Structured task\n\n"
        "Branch: pr-002-structured\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state())
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert coding_called["v"] is True
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-001"
    assert runner.state.current_task.branch == "pr-001-legacy"
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 2


def test_handle_idle_ignores_ghost_legacy_queue_task_without_task_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """PR-181 follow-up: a stale ``tasks/QUEUE.md`` snapshot can name a
    DOING task whose ``tasks/PR-*.md`` file no longer exists in the
    working tree (e.g. when the base branch was wiped between cycles).
    Such a "ghost" entry must not override the structured DAG selection
    — otherwise the daemon resurrects a stale task over a fresh upload.
    """
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    # QUEUE.md mentions PR-001 as DOING but PR-001.md is absent from
    # disk — that is the "ghost" case the dispatch must reject.
    (tasks_dir / "QUEUE.md").write_text(
        "## PR-001: Ghost legacy task\n"
        "- Status: DOING\n"
        "- Tasks file: tasks/PR-001.md\n"
        "- Branch: pr-001-ghost\n\n"
        "## PR-002: Structured task\n"
        "- Status: TODO\n"
        "- Tasks file: tasks/PR-002.md\n"
        "- Branch: pr-002-structured\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-002.md").write_text(
        "# PR-002: Structured task\n\n"
        "Branch: pr-002-structured\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state())
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert coding_called["v"] is True
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-002"
    assert any(
        "Ignoring ghost legacy QUEUE.md entry PR-001" in entry.get("event", "") for entry in runner.state.history
    )
    # The ghost entry must not block QUEUE.md regeneration: leaving the
    # stale ``PR-001: DOING`` block on disk would let the shim's
    # ``parse_doing_task`` (tests/e2e/lib/coder_shim.sh) latch onto it
    # and create a PR for the wrong branch (Codex P1 from CI run on
    # PR-181 branch).
    rewritten = (tasks_dir / "QUEUE.md").read_text(encoding="utf-8")
    assert "PR-001" not in rewritten
    assert "## PR-002: Structured task" in rewritten
    assert "- Status: DOING" in rewritten


def test_handle_idle_advances_to_real_legacy_after_skipping_ghost(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """When ``get_next_task`` would return a ghost legacy DOING entry,
    dispatch must re-select from the queue with ghosts removed and
    advance to a real legacy task that follows it. Otherwise, queues
    with a stale ghost ahead of a runnable legacy entry stall on "no
    tasks available" until someone manually edits QUEUE.md (PR-181
    follow-up).
    """
    h._patch_subprocess(monkeypatch)

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    # PR-001 is a ghost: declared task file is missing.
    # PR-002 is a real legacy entry: its task file exists on disk and
    # is unstructured (no Type/Complexity/Depends on header lines), so
    # ``parse_task_header`` rejects it via the legacy-unstructured path
    # and it is NOT considered structured — but it is still runnable.
    (tasks_dir / "QUEUE.md").write_text(
        "## PR-001: Ghost legacy\n"
        "- Status: DOING\n"
        "- Tasks file: tasks/PR-001.md\n"
        "- Branch: pr-001-ghost\n\n"
        "## PR-002: Real legacy\n"
        "- Status: TODO\n"
        "- Tasks file: tasks/PR-002.md\n"
        "- Branch: pr-002-real-legacy\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-002.md").write_text(
        "# PR-002: Real legacy\n\nSome legacy body without structured headers.\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state())
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert coding_called["v"] is True
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-002"
    assert any(
        "Ignoring ghost legacy QUEUE.md entry PR-001" in entry.get("event", "") for entry in runner.state.history
    )


def test_handle_idle_attaches_to_existing_pr_instead_of_coding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When a TODO task already has an open PR on its branch, handle_idle
    should attach to that PR and go to WATCH instead of running CODING."""
    h._patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-042",
        title="Sample",
        status=TaskStatus.TODO,
        branch="pr-042-sample",
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: task)

    existing_pr = PRInfo(
        number=99,
        branch="pr-042-sample",
        title="PR-042: Sample",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [existing_pr],
    )
    monkeypatch.setattr("src.github.prs.get_merged_prs", lambda repo, branch, refresh=False: [])
    monkeypatch.setattr(
        "src.github.prs.get_pr_metadata",
        lambda repo, number: {"head_commit_date": "2026-04-14T12:00:00Z"},
    )

    coding_called = {"v": False}

    async def spy_handle_coding(self):
        coding_called["v"] = True

    monkeypatch.setattr(runner_module.PipelineRunner, "handle_coding", spy_handle_coding)

    runner = h._make_runner()
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 99
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-042"
    assert not coding_called["v"], "handle_coding should NOT be called"


def test_handle_idle_proceeds_to_coding_when_no_matching_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When no open PR matches the task branch, handle_idle proceeds to CODING."""
    h._patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-042",
        title="Sample",
        status=TaskStatus.TODO,
        branch="pr-042-sample",
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: task)

    # Guard returns no matching PR; handle_coding's call returns the PR.
    call_count = {"n": 0}

    def _get_open_prs(repo: str, **kw: Any) -> list[PRInfo]:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return []
        return [PRInfo(number=17, branch="pr-042-sample")]

    monkeypatch.setattr("src.github.prs.get_open_prs", _get_open_prs)
    monkeypatch.setattr("src.github.prs.get_merged_prs", lambda repo, branch, refresh=False: [])
    monkeypatch.setattr(
        claude_cli,
        "run_auto_pr_async",
        h._async_cli_result(0, "ok", ""),
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )

    runner = h._make_runner()
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 17


def test_handle_idle_transitions_to_hung_when_pinned_coder_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A task pinned to ``codex`` whose coder is unavailable must transition
    to HUNG with a clear message instead of silently falling back."""
    h._patch_subprocess(monkeypatch)
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-200.md").write_text(
        "# PR-200: Pinned to codex\n\n"
        "Branch: pr-200-pinned\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: codex\n",
        encoding="utf-8",
    )
    task = QueueTask(
        pr_id="PR-200",
        title="Pinned to codex",
        status=TaskStatus.TODO,
        task_file="tasks/PR-200.md",
        branch="pr-200-pinned",
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: task)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    coding_called = {"v": False}

    async def spy_handle_coding(self):
        coding_called["v"] = True

    monkeypatch.setattr(runner_module.PipelineRunner, "handle_coding", spy_handle_coding)

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._auth_status_cache = {
        "claude": {"status": "ok"},
        "codex": {"status": "failed"},
    }
    runner._auth_status_cache_expires_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    runner.state.current_pr = PRInfo(
        number=1234,
        branch="some-other-branch",
        title="Unrelated manual PR from prior cycle",
    )
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.HUNG
    assert runner.state.error_message == ("Task PR-200 pinned to codex but coder unavailable")
    assert runner.state.current_pr is None
    assert not coding_called["v"]


def test_handle_idle_defers_on_gh_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When get_open_prs raises during the guard check, handle_idle defers
    without entering CODING."""
    h._patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-042",
        title="Sample",
        status=TaskStatus.TODO,
        branch="pr-042-sample",
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: task)

    def _exploding_get_open_prs(repo: str, **kw: Any) -> list[PRInfo]:
        raise RuntimeError("GitHub API unavailable")

    monkeypatch.setattr("src.github.prs.get_open_prs", _exploding_get_open_prs)

    coding_called = {"v": False}

    async def spy_handle_coding(self):
        coding_called["v"] = True

    monkeypatch.setattr(runner_module.PipelineRunner, "handle_coding", spy_handle_coding)

    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-999",
        title="Stale task",
        status=TaskStatus.DOING,
        branch="pr-999-stale-task",
    )
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert not coding_called["v"], "handle_coding should NOT be called on GH failure"


def test_handle_idle_sets_error_when_task_status_derivation_times_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-042",
        title="Sample",
        status=TaskStatus.TODO,
        branch="pr-042-sample",
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr("src.github.prs.get_merged_prs", lambda repo, branch, refresh=False: [])

    def _timed_out(*args: Any, **kwargs: Any) -> list[QueueTask]:
        raise subprocess.TimeoutExpired(cmd=["git", "branch", "--merged"], timeout=10)

    monkeypatch.setattr(idle_module, "derive_queue_task_statuses", _timed_out)

    coding_called = {"v": False}

    async def spy_handle_coding(self):
        coding_called["v"] = True

    monkeypatch.setattr(runner_module.PipelineRunner, "handle_coding", spy_handle_coding)

    runner = h._make_runner()
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.ERROR
    assert "Task status derivation failed" in (runner.state.error_message or "")
    assert not coding_called["v"], "handle_coding should NOT be called on timeout"


def test_handle_idle_waits_for_pending_queue_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    sync_calls: list[str] = []

    async def fake_resolve() -> bool:
        sync_calls.append("pending")
        return False

    runner = h._make_runner()
    runner.state.pending_queue_sync_branch = "queue-sync/pr-120"
    runner._resolve_pending_queue_sync = fake_resolve  # type: ignore[method-assign]

    asyncio.run(runner.handle_idle())

    assert sync_calls == ["pending"]
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None


def test_handle_idle_sets_error_when_initial_sync_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    runner.sync_to_main = lambda: (_ for _ in ()).throw(RuntimeError("sync broke"))  # type: ignore[method-assign]

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "sync_to_main failed: sync broke"
    assert any("sync_to_main failed: sync broke" in e["event"] for e in runner.state.history)


def test_handle_idle_stops_when_pending_upload_processing_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()

    async def fake_uploads() -> None:
        return None

    runner.process_pending_uploads = fake_uploads  # type: ignore[method-assign]

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert any(
        "Pending upload failed; skipping task dispatch to retry next cycle" in e["event"] for e in runner.state.history
    )


def test_handle_idle_sets_error_when_sync_after_upload_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    sync_calls = {"count": 0}

    def fake_sync() -> None:
        sync_calls["count"] += 1
        if sync_calls["count"] == 2:
            raise RuntimeError("resync broke")

    async def fake_uploads() -> bool:
        return True

    runner.sync_to_main = fake_sync  # type: ignore[method-assign]
    runner.process_pending_uploads = fake_uploads  # type: ignore[method-assign]

    asyncio.run(runner.handle_idle())

    assert sync_calls["count"] == 2
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "sync_to_main after upload failed: resync broke"
    assert any("sync_to_main after upload failed: resync broke" in e["event"] for e in runner.state.history)


def test_handle_idle_sets_error_when_queue_validation_fails_without_dag_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )
    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: (_ for _ in ()).throw(QueueValidationError(["tasks/QUEUE.md: malformed status"])),
    )

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == ("Queue validation failed:\n  - tasks/QUEUE.md: malformed status")
    assert any("Queue validation failed:" in e["event"] for e in runner.state.history)
    assert any("tasks/QUEUE.md: malformed status" in e["event"] for e in runner.state.history)


def test_handle_idle_preserves_fix_iteration_count_when_reattaching_same_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-145",
        title="Fix iteration cap",
        status=TaskStatus.TODO,
        branch="pr-145-fix-iteration-cap",
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: task)

    reattached_pr = PRInfo(
        number=145,
        branch="pr-145-fix-iteration-cap",
        title="PR-145: Fix iteration cap",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.CHANGES_REQUESTED,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [reattached_pr],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_metadata",
        lambda repo, number: {"head_commit_date": "2026-04-21T00:48:54Z"},
    )

    runner = h._make_runner()
    runner.state.current_task = task
    runner.state.current_pr = PRInfo(
        number=145,
        branch="pr-145-fix-iteration-cap",
        fix_iteration_count=15,
        no_push_fix_count=2,
    )

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 145
    assert runner.state.current_pr.fix_iteration_count == 15
    assert runner.state.current_pr.no_push_fix_count == 2


def test_handle_idle_no_tasks_but_open_pr_sets_current_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    done_task = QueueTask(
        pr_id="PR-001",
        title="Done",
        status=TaskStatus.DONE,
        branch="pr-001-done",
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [done_task])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)

    open_pr = PRInfo(
        number=42,
        branch="pr-001-done",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.PENDING,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [open_pr])
    monkeypatch.setattr("src.github.prs.get_merged_prs", lambda repo, branch, refresh=False: [])

    runner = h._make_runner()
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 42
    assert any("open PR(s) detected" in e["event"] for e in runner.state.history)


def test_handle_idle_no_tasks_no_open_prs_clears_current_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])
    monkeypatch.setattr("src.github.prs.get_merged_prs", lambda repo, branch, refresh=False: [])

    runner = h._make_runner()
    # Set a stale current_pr to verify it gets cleared.
    runner.state.current_pr = PRInfo(number=99, branch="old")
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None


def test_handle_idle_new_open_pr_resets_fix_iteration_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [PRInfo(number=42, branch="fresh-branch")],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = h._make_runner()
    runner.state.current_pr = PRInfo(
        number=99,
        branch="old-branch",
        fix_iteration_count=7,
    )

    asyncio.run(runner.handle_idle())

    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 42
    assert runner.state.current_pr.fix_iteration_count == 0


def test_handle_idle_no_tasks_does_not_change_state_from_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)

    open_pr = PRInfo(number=7, branch="feature-x")
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [open_pr])

    runner = h._make_runner()
    assert runner.state.state == PipelineState.IDLE
    asyncio.run(runner.handle_idle())

    # State must remain IDLE — observation only.
    assert runner.state.state == PipelineState.IDLE


def test_handle_idle_open_pr_check_survives_github_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: (_ for _ in ()).throw(RuntimeError("API down")),
    )

    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=5, branch="stale")
    asyncio.run(runner.handle_idle())

    # Must not crash, state stays IDLE, and stale current_pr is cleared.
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert any("open PR check failed" in e["event"] for e in runner.state.history)


def test_handle_idle_falls_back_when_merged_pr_check_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    # task_file omitted: this test exercises the merged_prs API failure
    # path, not the ghost-task guard that PR-181 added — leaving
    # task_file unset keeps the legacy fallback in scope.
    task = QueueTask(
        pr_id="PR-123",
        title="Keep dispatching",
        branch="pr-123-keep-dispatching",
        status=TaskStatus.TODO,
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: (_ for _ in ()).throw(RuntimeError("API down")),
    )
    derived_calls: list[list[PRInfo]] = []
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): (derived_calls.append(list(merged_prs)) or tasks),
    )
    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True
        return None

    runner = h._make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    runner.state.current_pr = PRInfo(number=5, branch="stale")
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task == task
    assert runner.state.current_pr.number == 5
    assert coding_called["v"] is True
    assert derived_calls == [[]]
    assert any("merged PR check failed" in e["event"] for e in runner.state.history)


def test_handle_idle_silences_merged_pr_http_304_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-236: a single transient HTTP 304 surfacing through
    ``get_merged_prs`` must not log ``[INFRA] IDLE: merged PR check failed``
    (operator-visible spam) nor the persistent-degradation event."""
    h._patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-123",
        title="Keep dispatching",
        branch="pr-123-keep-dispatching",
        status=TaskStatus.TODO,
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: (_ for _ in ()).throw(
            RuntimeError("gh api repos/owner/name/pulls failed (exit 1): HTTP 304")
        ),
    )
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )

    async def fake_handle_coding() -> None:
        return None

    runner = h._make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert not any("merged PR check failed" in e["event"] for e in runner.state.history)
    assert not any("merged-PR detection degraded" in e["event"] for e in runner.state.history)
    assert runner._idle_merged_pr_304_streak == 1


def test_handle_idle_warns_when_merged_pr_http_304_streak_persists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-236 follow-up: once the HTTP 304 streak crosses the warn threshold,
    a degraded-detection INFRA event must surface so a stuck merged-PR
    visibility outage is operator-visible."""
    h._patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-123",
        title="Keep dispatching",
        branch="pr-123-keep-dispatching",
        status=TaskStatus.TODO,
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: (_ for _ in ()).throw(
            RuntimeError("gh api repos/owner/name/pulls failed (exit 1): HTTP 304")
        ),
    )
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )

    async def fake_handle_coding() -> None:
        return None

    runner = h._make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    runner._idle_merged_pr_304_streak = idle_module._IDLE_MERGED_PR_304_WARN_AT - 1
    asyncio.run(runner.handle_idle())

    assert any("merged-PR detection degraded" in e["event"] for e in runner.state.history)
    assert not any("merged PR check failed" in e["event"] for e in runner.state.history)
    assert runner._idle_merged_pr_304_streak == (idle_module._IDLE_MERGED_PR_304_WARN_AT)


def test_handle_idle_does_not_rewarn_before_full_cadence_after_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-236 follow-up: the re-emit cadence is measured from the threshold
    crossing, not from streak=0. With WARN_AT=10 and WARN_EVERY=50 the next
    warning lands at streak=60, not streak=50."""
    h._patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-123",
        title="Keep dispatching",
        branch="pr-123-keep-dispatching",
        status=TaskStatus.TODO,
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: (_ for _ in ()).throw(
            RuntimeError("gh api repos/owner/name/pulls failed (exit 1): HTTP 304")
        ),
    )
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )

    async def fake_handle_coding() -> None:
        return None

    runner = h._make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    # Streak about to land on a multiple of WARN_EVERY (e.g. 50) but still
    # short of WARN_AT + WARN_EVERY (e.g. 60). Pre-fix this would re-warn.
    runner._idle_merged_pr_304_streak = idle_module._IDLE_MERGED_PR_304_WARN_EVERY - 1
    asyncio.run(runner.handle_idle())

    assert runner._idle_merged_pr_304_streak == (idle_module._IDLE_MERGED_PR_304_WARN_EVERY)
    assert not any("merged-PR detection degraded" in e["event"] for e in runner.state.history)


def test_handle_idle_rewarns_after_full_cadence_past_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-236 follow-up: once a full WARN_EVERY cycles have elapsed past the
    initial WARN_AT crossing, the degraded-detection event must re-emit so
    operators see the persistent outage at the configured spacing."""
    h._patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-123",
        title="Keep dispatching",
        branch="pr-123-keep-dispatching",
        status=TaskStatus.TODO,
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: (_ for _ in ()).throw(
            RuntimeError("gh api repos/owner/name/pulls failed (exit 1): HTTP 304")
        ),
    )
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )

    async def fake_handle_coding() -> None:
        return None

    runner = h._make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    runner._idle_merged_pr_304_streak = (
        idle_module._IDLE_MERGED_PR_304_WARN_AT + idle_module._IDLE_MERGED_PR_304_WARN_EVERY - 1
    )
    asyncio.run(runner.handle_idle())

    assert runner._idle_merged_pr_304_streak == (
        idle_module._IDLE_MERGED_PR_304_WARN_AT + idle_module._IDLE_MERGED_PR_304_WARN_EVERY
    )
    assert any("merged-PR detection degraded" in e["event"] for e in runner.state.history)


def test_handle_idle_resets_merged_pr_http_304_streak_on_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-236 follow-up: a successful merged-PR fetch resets the 304 streak,
    so a recovered upstream cache stops the degraded-detection alarm."""
    h._patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-123",
        title="Keep dispatching",
        branch="pr-123-keep-dispatching",
        status=TaskStatus.TODO,
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )

    async def fake_handle_coding() -> None:
        return None

    runner = h._make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    runner._idle_merged_pr_304_streak = 42
    asyncio.run(runner.handle_idle())

    assert runner._idle_merged_pr_304_streak == 0


def test_handle_idle_resets_merged_pr_http_304_streak_on_non_304_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-236 follow-up: a non-304 merged-PR failure resets the 304 streak
    and continues to log the regular ``merged PR check failed`` INFRA event,
    so distinct failure modes don't accumulate into a misleading streak."""
    h._patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-123",
        title="Keep dispatching",
        branch="pr-123-keep-dispatching",
        status=TaskStatus.TODO,
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: (_ for _ in ()).throw(RuntimeError("API down")),
    )
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )

    async def fake_handle_coding() -> None:
        return None

    runner = h._make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    runner._idle_merged_pr_304_streak = 7
    asyncio.run(runner.handle_idle())

    assert runner._idle_merged_pr_304_streak == 0
    assert any("merged PR check failed" in e["event"] for e in runner.state.history)


def test_handle_idle_resets_merged_pr_http_304_streak_when_check_skipped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-236 follow-up: the 304 streak must reset whenever the merged-PR
    check is skipped this cycle (e.g. ``get_open_prs`` failed earlier and
    triggered an early return). Otherwise non-consecutive 304s across
    skipped cycles would accumulate and emit a spurious degraded warning.
    """
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: (_ for _ in ()).throw(RuntimeError("API down")),
    )
    merged_calls = {"n": 0}

    def fail_get_merged_prs(repo, branch, refresh=False):
        merged_calls["n"] += 1
        raise AssertionError("get_merged_prs must not run when get_open_prs failed")

    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        fail_get_merged_prs,
    )

    runner = h._make_runner()
    runner._idle_merged_pr_304_streak = 9
    asyncio.run(runner.handle_idle())

    assert merged_calls["n"] == 0
    assert runner._idle_merged_pr_304_streak == 0
    assert not any("merged-PR detection degraded" in e["event"] for e in runner.state.history)


def test_handle_idle_resets_merged_pr_http_304_streak_on_pending_queue_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-236 follow-up: a cycle that exits via the pending-queue-sync
    early return must reset the 304 streak too, so non-consecutive 304s
    don't accumulate across unrelated skipped cycles and trigger a
    spurious degraded-detection warning later."""
    h._patch_subprocess(monkeypatch)

    async def fake_resolve() -> bool:
        return False

    def fail_get_merged_prs(repo, branch, refresh=False):
        raise AssertionError("get_merged_prs must not run when pending queue sync is unresolved")

    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        fail_get_merged_prs,
    )

    runner = h._make_runner()
    runner.state.pending_queue_sync_branch = "queue-sync/pr-120"
    runner._resolve_pending_queue_sync = fake_resolve  # type: ignore[method-assign]
    runner._idle_merged_pr_304_streak = 9

    asyncio.run(runner.handle_idle())

    assert runner._idle_merged_pr_304_streak == 0
    assert not any("merged-PR detection degraded" in e["event"] for e in runner.state.history)


def test_handle_idle_uses_fallback_queue_counters_when_dag_picks_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    dag_tasks = [
        QueueTask(
            pr_id="PR-001",
            title="Done DAG task",
            status=TaskStatus.DONE,
            task_file="tasks/PR-001.md",
            branch="pr-001-done",
        ),
        QueueTask(
            pr_id="PR-002",
            title="Blocked DAG task",
            status=TaskStatus.TODO,
            task_file="tasks/PR-002.md",
            branch="pr-002-blocked",
        ),
    ]
    # task_file omitted: this test verifies counter handling, not the
    # ghost-task guard PR-181 added — leaving task_file unset keeps the
    # legacy fallback in scope.
    fallback_task = QueueTask(
        pr_id="PR-099",
        title="Fallback queue task",
        status=TaskStatus.TODO,
        branch="pr-099-fallback",
    )
    fallback_tasks = [fallback_task]

    async def fake_select(self):
        self._idle_dag_tasks = dag_tasks
        return None

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: fallback_tasks)
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: fallback_task)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    async def fake_handle_coding() -> None:
        return None

    runner = h._make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert runner.state.current_task == fallback_task
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 1


def test_idle_populates_current_queue_after_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    dag_tasks = [
        QueueTask(
            pr_id="PR-001",
            title="Done task",
            status=TaskStatus.DONE,
            task_file="tasks/PR-001.md",
            branch="pr-001-done",
        ),
        QueueTask(
            pr_id="PR-002",
            title="Selected task",
            status=TaskStatus.TODO,
            task_file="tasks/PR-002.md",
            branch="pr-002-selected",
        ),
        QueueTask(
            pr_id="PR-003",
            title="Waiting task",
            status=TaskStatus.TODO,
            task_file="tasks/PR-003.md",
            branch="pr-003-waiting",
            depends_on=["PR-002"],
        ),
    ]

    async def fake_select(self):
        self._idle_dag_tasks = list(dag_tasks)
        self._idle_dag_statuses = {
            task.pr_id: task.status for task in dag_tasks
        }
        return dag_tasks[1]

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: list(dag_tasks))
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    async def fake_handle_coding() -> None:
        return None

    runner = h._make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert runner.state.current_queue == [
        dag_tasks[0],
        dag_tasks[1].model_copy(update={"status": TaskStatus.DOING}),
        dag_tasks[2],
    ]


def test_idle_populates_current_queue_no_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    dag_tasks = [
        QueueTask(
            pr_id="PR-001",
            title="Done task",
            status=TaskStatus.DONE,
            task_file="tasks/PR-001.md",
            branch="pr-001-done",
        ),
        QueueTask(
            pr_id="PR-002",
            title="Also done",
            status=TaskStatus.DONE,
            task_file="tasks/PR-002.md",
            branch="pr-002-done",
        ),
    ]

    async def fake_select(self):
        self._idle_dag_tasks = list(dag_tasks)
        return None

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: list(dag_tasks))
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = h._make_runner()
    asyncio.run(runner.handle_idle())

    assert runner.state.current_task is None
    assert runner.state.current_queue == dag_tasks


def test_idle_skips_population_on_validation_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    prior_queue = [
        QueueTask(
            pr_id="PR-001",
            title="Prior",
            status=TaskStatus.DONE,
            task_file="tasks/PR-001.md",
            branch="pr-001-prior",
        )
    ]

    async def fake_select(self):
        raise QueueValidationError(["tasks/PR-002.md: invalid header"])

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = h._make_runner()
    runner.state.current_queue = list(prior_queue)
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.current_queue == prior_queue


def test_handle_idle_keeps_dag_task_when_queue_validation_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    dag_task = QueueTask(
        pr_id="PR-123",
        title="Structured task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-123.md",
        branch="pr-123-structured",
    )
    dag_tasks = [dag_task]

    async def fake_select(self):
        self._idle_dag_tasks = dag_tasks
        return dag_task

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: (_ for _ in ()).throw(
            idle_module.QueueValidationError(["Queue validation failed:\n- malformed queue"])
        ),
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True
        return None

    runner = h._make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert coding_called["v"] is True
    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task == dag_task
    assert runner.state.error_message is None
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 1
    assert any("Queue validation failed after DAG selection" in entry["event"] for entry in runner.state.history)


def test_handle_idle_keeps_dag_state_when_queue_status_derivation_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    dag_task = QueueTask(
        pr_id="PR-123",
        title="Structured task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-123.md",
        branch="pr-123-structured",
    )
    dag_tasks = [dag_task]
    queue_tasks = [
        QueueTask(
            pr_id="PR-123",
            title="Structured task",
            status=TaskStatus.TODO,
            task_file="tasks/PR-123.md",
            branch="pr-123-structured",
        )
    ]

    async def fake_select(self):
        self._idle_dag_tasks = dag_tasks
        return dag_task

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: queue_tasks)
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            idle_module.QueueValidationError(["tasks/QUEUE.md: PR-123 does not match task file"])
        ),
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True
        return None

    runner = h._make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert coding_called["v"] is True
    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task == dag_task
    assert runner.state.error_message is None
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 1
    assert any("Task status derivation failed after DAG selection" in entry["event"] for entry in runner.state.history)


def test_handle_idle_keeps_dag_metrics_when_derivation_fails_with_visible_legacy_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    dag_task = QueueTask(
        pr_id="PR-123",
        title="Structured task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-123.md",
        branch="pr-123-structured",
    )
    dag_tasks = [dag_task]
    queue_tasks = [
        QueueTask(
            pr_id="PR-123",
            title="Structured task",
            status=TaskStatus.TODO,
            task_file="tasks/PR-123.md",
            branch="pr-123-structured",
        )
    ]

    async def fake_select(self):
        self._idle_dag_tasks = dag_tasks
        return dag_task

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: queue_tasks)
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            idle_module.QueueValidationError(["tasks/QUEUE.md: PR-123 does not match task file"])
        ),
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    async def fake_handle_coding() -> None:
        return None

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    (queue_dir / "QUEUE.md").write_text(
        "# Task Queue\n\n## PR-001: Legacy queue task\n- Status: TODO\n",
        encoding="utf-8",
    )
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task == dag_task
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 1


def test_handle_idle_keeps_dag_state_when_queue_validation_fails_without_dag_pick(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    dag_tasks = [
        QueueTask(
            pr_id="PR-123",
            title="Structured task",
            status=TaskStatus.DONE,
            task_file="tasks/PR-123.md",
            branch="pr-123-structured",
        )
    ]

    async def fake_select(self):
        self._idle_dag_tasks = dag_tasks
        return None

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: (_ for _ in ()).throw(
            idle_module.QueueValidationError(["Queue validation failed:\n- malformed queue"])
        ),
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = h._make_runner()
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None
    assert runner.state.current_task is None
    assert runner.state.queue_done == 1
    assert runner.state.queue_total == 1
    assert any("Queue validation failed after DAG selection" in entry["event"] for entry in runner.state.history)


def test_handle_idle_keeps_doing_dag_task_over_legacy_queue_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    dag_task = QueueTask(
        pr_id="PR-123",
        title="Structured in-flight task",
        status=TaskStatus.DOING,
        task_file="tasks/PR-123.md",
        branch="pr-123-structured",
    )
    dag_tasks = [dag_task]
    legacy_queue_task = QueueTask(
        pr_id="PR-001",
        title="Legacy queue task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-001.md",
        branch="pr-001-legacy",
    )

    async def fake_select(self):
        self._idle_dag_tasks = dag_tasks
        return dag_task

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [legacy_queue_task])
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: legacy_queue_task)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True
        return None

    runner = h._make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert coding_called["v"] is True
    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task == dag_task
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 1


def test_handle_idle_marks_freshly_picked_dag_task_as_doing_in_regenerated_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A freshly picked structured TODO task must be regenerated as DOING in
    QUEUE.md before handle_coding runs, so the shim sees a DOING entry."""
    h._patch_subprocess(monkeypatch)
    dag_task = QueueTask(
        pr_id="PR-123",
        title="Fresh structured task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-123.md",
        branch="pr-123-fresh",
    )
    header = TaskHeader(
        pr_id=dag_task.pr_id,
        title=dag_task.title,
        branch=dag_task.branch or "",
        task_type="feature",
        complexity="low",
        depends_on=[],
        priority=1,
        coder="any",
    )

    async def fake_select(self):
        self._idle_dag_tasks = [dag_task]
        self._idle_dag_headers = [header]
        self._idle_dag_statuses = {dag_task.pr_id: TaskStatus.TODO}
        return dag_task

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [dag_task])
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(), **kwargs: tasks,
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: dag_task)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    write_calls: list[tuple[list[TaskHeader], dict[str, TaskStatus]]] = []

    def fake_write_generated_queue_md(
        self,
        headers: list[TaskHeader],
        statuses: dict[str, TaskStatus],
    ) -> bool:
        write_calls.append((list(headers), dict(statuses)))
        return True

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_write_generated_queue_md",
        fake_write_generated_queue_md,
    )

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True

    runner = h._make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert coding_called["v"] is True
    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == dag_task.pr_id
    assert runner.state.current_task.status == TaskStatus.DOING
    assert len(write_calls) == 1
    written_headers, written_statuses = write_calls[0]
    assert written_headers == [header]
    assert written_statuses == {dag_task.pr_id: TaskStatus.DOING}


def test_handle_idle_skips_queue_regeneration_when_legacy_tasks_exist(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    dag_task = QueueTask(
        pr_id="PR-123",
        title="Structured in-flight task",
        status=TaskStatus.DOING,
        task_file="tasks/PR-123.md",
        branch="pr-123-structured",
    )
    legacy_queue_task = QueueTask(
        pr_id="PR-001",
        title="Legacy queue task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-001.md",
        branch="pr-001-legacy",
    )

    async def fake_select(self):
        self._idle_dag_tasks = [dag_task]
        self._idle_dag_headers = [
            TaskHeader(
                pr_id=dag_task.pr_id,
                title=dag_task.title,
                branch=dag_task.branch or "",
                task_type="feature",
                complexity="low",
                depends_on=[],
                priority=1,
                coder="any",
            )
        ]
        self._idle_dag_statuses = {dag_task.pr_id: dag_task.status}
        return dag_task

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [legacy_queue_task])
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: legacy_queue_task)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    write_calls: list[tuple[list[TaskHeader], dict[str, TaskStatus]]] = []

    def fake_write_generated_queue_md(
        self,
        headers: list[TaskHeader],
        statuses: dict[str, TaskStatus],
    ) -> None:
        write_calls.append((headers, statuses))

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_write_generated_queue_md",
        fake_write_generated_queue_md,
    )

    async def fake_handle_coding() -> None:
        return None

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    # PR-001 is a "real" legacy entry: its task file is present on
    # disk, so it represents a hand-managed migration task that the
    # daemon must not blow away. Without the file the entry would be
    # classified as a ghost (PR-181 follow-up) and regeneration would
    # legitimately overwrite it.
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text("# PR-001: Legacy queue task\n", encoding="utf-8")
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert write_calls == []


def test_handle_idle_skips_queue_regeneration_when_legacy_check_fails_with_visible_legacy_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    dag_task = QueueTask(
        pr_id="PR-123",
        title="Structured in-flight task",
        status=TaskStatus.DOING,
        task_file="tasks/PR-123.md",
        branch="pr-123-structured",
    )

    async def fake_select(self):
        self._idle_dag_tasks = [dag_task]
        self._idle_dag_headers = [
            TaskHeader(
                pr_id=dag_task.pr_id,
                title=dag_task.title,
                branch=dag_task.branch or "",
                task_type="feature",
                complexity="low",
                depends_on=[],
                priority=1,
                coder="any",
            )
        ]
        self._idle_dag_statuses = {dag_task.pr_id: dag_task.status}
        return dag_task

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: (_ for _ in ()).throw(idle_module.QueueValidationError(["Queue validation failed"])),
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    write_calls: list[tuple[list[TaskHeader], dict[str, TaskStatus]]] = []

    def fake_write_generated_queue_md(
        self,
        headers: list[TaskHeader],
        statuses: dict[str, TaskStatus],
    ) -> None:
        write_calls.append((headers, statuses))

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_write_generated_queue_md",
        fake_write_generated_queue_md,
    )

    async def fake_handle_coding() -> None:
        return None

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    (queue_dir / "QUEUE.md").write_text(
        "# Task Queue\n\n## PR-001: Legacy queue task\n- Status: TODO\n",
        encoding="utf-8",
    )
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert write_calls == []


def test_handle_idle_skips_queue_regeneration_when_visible_legacy_row_is_malformed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    dag_task = QueueTask(
        pr_id="PR-123",
        title="Structured in-flight task",
        status=TaskStatus.DOING,
        task_file="tasks/PR-123.md",
        branch="pr-123-structured",
    )

    async def fake_select(self):
        self._idle_dag_tasks = [dag_task]
        self._idle_dag_headers = [
            TaskHeader(
                pr_id=dag_task.pr_id,
                title=dag_task.title,
                branch=dag_task.branch or "",
                task_type="feature",
                complexity="low",
                depends_on=[],
                priority=1,
                coder="any",
            )
        ]
        self._idle_dag_statuses = {dag_task.pr_id: dag_task.status}
        return dag_task

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: (_ for _ in ()).throw(idle_module.QueueValidationError(["Queue validation failed"])),
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    write_calls: list[tuple[list[TaskHeader], dict[str, TaskStatus]]] = []

    def fake_write_generated_queue_md(
        self,
        headers: list[TaskHeader],
        statuses: dict[str, TaskStatus],
    ) -> bool:
        write_calls.append((headers, statuses))
        return True

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_write_generated_queue_md",
        fake_write_generated_queue_md,
    )

    async def fake_handle_coding() -> None:
        return None

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    (queue_dir / "QUEUE.md").write_text(
        "# Task Queue\n\n## PR-001 Legacy queue task\n- Status: TODO\n",
        encoding="utf-8",
    )
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert write_calls == []


def test_handle_idle_skips_queue_regeneration_when_successful_parse_still_has_visible_legacy_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    dag_task = QueueTask(
        pr_id="PR-123",
        title="Structured in-flight task",
        status=TaskStatus.DOING,
        task_file="tasks/PR-123.md",
        branch="pr-123-structured",
    )

    async def fake_select(self):
        self._idle_dag_tasks = [dag_task]
        self._idle_dag_headers = [
            TaskHeader(
                pr_id=dag_task.pr_id,
                title=dag_task.title,
                branch=dag_task.branch or "",
                task_type="feature",
                complexity="low",
                depends_on=[],
                priority=1,
                coder="any",
            )
        ]
        self._idle_dag_statuses = {dag_task.pr_id: dag_task.status}
        return dag_task

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: [dag_task],
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda *args, **kwargs: [dag_task],
    )
    monkeypatch.setattr(
        idle_module,
        "get_next_task",
        lambda tasks: dag_task,
    )

    write_calls: list[tuple[list[TaskHeader], dict[str, TaskStatus]]] = []

    def fake_write_generated_queue_md(
        self,
        headers: list[TaskHeader],
        statuses: dict[str, TaskStatus],
    ) -> bool:
        write_calls.append((headers, statuses))
        return True

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_write_generated_queue_md",
        fake_write_generated_queue_md,
    )

    async def fake_handle_coding() -> None:
        return None

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    (queue_dir / "QUEUE.md").write_text(
        "# Task Queue\n\n"
        "## PR-123: Structured in-flight task\n"
        "- Status: DOING\n"
        "- Tasks file: tasks/PR-123.md\n"
        "- Branch: pr-123-structured\n\n"
        "## PR-001 Legacy queue task\n"
        "- Status: TODO\n",
        encoding="utf-8",
    )
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert write_calls == []


def test_handle_idle_regenerates_queue_when_validation_fails_without_visible_legacy_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    dag_task = QueueTask(
        pr_id="PR-123",
        title="Structured in-flight task",
        status=TaskStatus.DOING,
        task_file="tasks/PR-123.md",
        branch="pr-123-structured",
    )

    async def fake_select(self):
        self._idle_dag_tasks = [dag_task]
        self._idle_dag_headers = [
            TaskHeader(
                pr_id=dag_task.pr_id,
                title=dag_task.title,
                branch=dag_task.branch or "",
                task_type="feature",
                complexity="low",
                depends_on=[],
                priority=1,
                coder="any",
            )
        ]
        self._idle_dag_statuses = {dag_task.pr_id: dag_task.status}
        return dag_task

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: (_ for _ in ()).throw(idle_module.QueueValidationError(["Queue validation failed"])),
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    write_calls: list[tuple[list[TaskHeader], dict[str, TaskStatus]]] = []

    def fake_write_generated_queue_md(
        self,
        headers: list[TaskHeader],
        statuses: dict[str, TaskStatus],
    ) -> bool:
        write_calls.append((headers, statuses))
        return True

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_write_generated_queue_md",
        fake_write_generated_queue_md,
    )

    async def fake_handle_coding() -> None:
        return None

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    (queue_dir / "QUEUE.md").write_text(
        "# Task Queue\n\n## PR-123: Structured in-flight task\n- Status: TODO,\n",
        encoding="utf-8",
    )
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert write_calls == [([runner._idle_dag_headers[0]], {dag_task.pr_id: dag_task.status})]


def test_handle_idle_stops_when_queue_regeneration_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    dag_task = QueueTask(
        pr_id="PR-123",
        title="Structured in-flight task",
        status=TaskStatus.DOING,
        task_file="tasks/PR-123.md",
        branch="pr-123-structured",
    )

    async def fake_select(self):
        self._idle_dag_tasks = [dag_task]
        self._idle_dag_headers = [
            TaskHeader(
                pr_id=dag_task.pr_id,
                title=dag_task.title,
                branch=dag_task.branch or "",
                task_type="feature",
                complexity="low",
                depends_on=[],
                priority=1,
                coder="any",
            )
        ]
        self._idle_dag_statuses = {dag_task.pr_id: dag_task.status}
        return dag_task

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True
        return None

    def fake_write_generated_queue_md(
        self,
        headers: list[TaskHeader],
        statuses: dict[str, TaskStatus],
    ) -> bool:
        raise OSError("disk write failed")

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_write_generated_queue_md",
        fake_write_generated_queue_md,
    )

    runner = h._make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert coding_called["v"] is False
    assert runner.state.state == PipelineState.ERROR
    assert "QUEUE.md auto-generation failed" in (runner.state.error_message or "")


def test_handle_idle_does_not_promote_structured_queue_task_when_dag_blocks_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    dag_tasks = [
        QueueTask(
            pr_id="PR-123",
            title="Blocked structured task",
            status=TaskStatus.TODO,
            task_file="tasks/PR-123.md",
            branch="pr-123-structured",
            depends_on=["PR-001"],
        )
    ]
    queue_task = QueueTask(
        pr_id="PR-123",
        title="Blocked structured task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-123.md",
        branch="pr-123-structured",
        depends_on=["PR-001"],
    )

    async def fake_select(self):
        self._idle_dag_tasks = dag_tasks
        return None

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [queue_task])
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: queue_task)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = h._make_runner()
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.error_message is None
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 1
    assert any("No tasks available" in entry["event"] for entry in runner.state.history)


def test_handle_idle_uses_cached_merged_prs_for_status_derivation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-123",
        title="Cached merged PRs",
        branch="pr-123-cached-merged-prs",
        status=TaskStatus.TODO,
        task_file="tasks/PR-123.md",
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    refresh_calls: list[bool] = []

    def fake_get_merged_prs(
        repo: str,
        branch: str,
        refresh: bool = False,
    ) -> list[PRInfo]:
        refresh_calls.append(refresh)
        return []

    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        fake_get_merged_prs,
    )
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )

    async def fake_handle_coding() -> None:
        return None

    runner = h._make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert refresh_calls == [False]


def test_handle_idle_marks_upload_deferred_when_processing_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``handle_idle`` flags the cycle so the streak skips this IDLE tick."""
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()

    async def fake_uploads() -> None:
        return None

    runner.process_pending_uploads = fake_uploads  # type: ignore[method-assign]

    asyncio.run(runner.handle_idle())

    assert runner._idle_dispatch_deferred is True


def test_handle_idle_marks_dispatch_deferred_on_open_prs_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``get_open_prs`` exception leaves queue/PR status unknown: skip the streak."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: (_ for _ in ()).throw(RuntimeError("API down")),
    )

    runner = h._make_runner()
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner._idle_dispatch_deferred is True


# ---------------------------------------------------------------------------
# PR-224b moved from tests/test_runner.py — handle_idle group
# ---------------------------------------------------------------------------


def test_idle_uses_cached_merged_prs(monkeypatch: pytest.MonkeyPatch) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )

    merged_pr_calls: list[dict[str, object]] = []

    def fake_get_merged_prs(*args: object, **kwargs: object) -> list[PRInfo]:
        merged_pr_calls.append(dict(kwargs))
        return []

    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        fake_get_merged_prs,
    )

    runner = h._make_runner()
    started_at = time.monotonic()
    asyncio.run(runner.handle_idle())
    asyncio.run(runner.handle_idle())

    assert time.monotonic() - started_at < 60
    assert len(merged_pr_calls) == 2
    assert all(call.get("refresh", False) is False for call in merged_pr_calls)


def test_idle_refreshes_merged_prs_when_open_pr_snapshot_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    open_pr_cycles = [[PRInfo(number=42, branch="pr-042-sample")], []]

    def fake_get_open_prs(repo: str, **kwargs: object) -> list[PRInfo]:
        del repo, kwargs
        return open_pr_cycles.pop(0)

    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        fake_get_open_prs,
    )
    refresh_calls: list[bool] = []

    def fake_get_merged_prs(
        repo: str,
        branch: str,
        refresh: bool = False,
    ) -> list[PRInfo]:
        del repo, branch
        refresh_calls.append(refresh)
        return []

    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        fake_get_merged_prs,
    )

    runner = h._make_runner()
    asyncio.run(runner.handle_idle())
    asyncio.run(runner.handle_idle())

    assert refresh_calls == [False, True]


def test_select_next_task_from_dag_prefers_doing_task(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: In flight task\n\n"
        "Branch: pr-001-in-flight\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-002.md").write_text(
        "# PR-002: Fresh task\n\nBranch: pr-002-fresh\n- Type: feature\n- Complexity: low\n- Depends on: none\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state())
    monkeypatch.setattr(
        idle_module,
        "derive_task_status",
        lambda header, merged_pr_ids, open_prs, merged_prs, **kwargs: (
            TaskStatus.DOING if header.pr_id == "PR-001" else TaskStatus.TODO
        ),
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = []
    runner._idle_merged_prs = []

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is not None
    assert task.pr_id == "PR-001"
    assert task.status == TaskStatus.DOING


def test_select_next_task_from_dag_marks_current_task_doing_without_open_pr(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: Active task\n\n"
        "Branch: pr-001-active\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state())

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = []
    runner._idle_merged_prs = []
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="Active task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-001.md",
        branch="pr-001-active",
    )

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is not None
    assert task.pr_id == "PR-001"
    assert task.status == TaskStatus.DOING
    assert runner._idle_dag_statuses == {"PR-001": TaskStatus.DOING}
    assert all(t.status == TaskStatus.DOING for t in runner._idle_dag_tasks)
    queue_md = runner._generate_queue_md(
        runner._idle_dag_headers,
        runner._idle_dag_statuses,
    )
    assert "## PR-001" in queue_md
    assert "- Status: DOING" in queue_md


def test_select_next_task_from_dag_skips_user_stopped_current_task(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: Stopped task\n\n"
        "Branch: pr-001-stopped\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-002.md").write_text(
        "# PR-002: Follow-up task\n\n"
        "Branch: pr-002-follow-up\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state())

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = []
    runner._idle_merged_prs = []
    runner._user_stopped_task_pr_ids.add("PR-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="Stopped task",
        status=TaskStatus.DOING,
        task_file="tasks/PR-001.md",
        branch="pr-001-stopped",
    )

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is not None
    assert task.pr_id == "PR-002"
    assert task.status == TaskStatus.TODO
    assert runner._idle_dag_statuses == {
        "PR-001": TaskStatus.TODO,
        "PR-002": TaskStatus.TODO,
    }
    assert runner._user_stopped_task_pr_ids == set()


def test_select_next_task_from_dag_retries_user_stopped_task_when_only_choice(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: Stopped task\n\n"
        "Branch: pr-001-stopped\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state())

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = []
    runner._idle_merged_prs = []
    runner._user_stopped_task_pr_ids.add("PR-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="Stopped task",
        status=TaskStatus.DOING,
        task_file="tasks/PR-001.md",
        branch="pr-001-stopped",
    )

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is not None
    assert task.pr_id == "PR-001"
    assert task.status == TaskStatus.TODO
    assert runner._user_stopped_task_pr_ids == set()


def test_select_next_task_from_dag_watches_user_stopped_task_with_open_pr(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: Open PR task\n\n"
        "Branch: pr-001-open\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state())

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = [PRInfo(number=11, branch="pr-001-open", pr_id="PR-001")]
    runner._idle_merged_prs = []
    runner._user_stopped_task_pr_ids.add("PR-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="Open PR task",
        status=TaskStatus.DOING,
        task_file="tasks/PR-001.md",
        branch="pr-001-open",
    )

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is not None
    assert task.pr_id == "PR-001"
    assert task.status == TaskStatus.DOING
    assert runner._user_stopped_task_pr_ids == set()


def test_select_next_task_from_dag_rejects_header_filename_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-999: Wrong task\n\nBranch: pr-999-wrong-task\n- Type: feature\n- Complexity: low\n- Depends on: none\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state())

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    with pytest.raises(QueueValidationError) as excinfo:
        asyncio.run(runner._select_next_task_from_dag())

    assert excinfo.value.issues == [
        f"{tasks_dir / 'PR-001.md'}: header PR ID 'PR-999' does not match task file 'PR-001'"
    ]


def test_init_migrates_legacy_clone_when_origin_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_name = f"runner-init-migrate-{time.time_ns()}"
    old_path = Path("/data/repos") / repo_name
    new_path = Path("/data/repos") / f"octo__{repo_name}"
    old_path.mkdir(parents=True)
    (old_path / ".git").mkdir()
    info_logs: list[tuple[object, ...]] = []
    run_calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        run_calls.append(cmd)
        assert cmd in (
            ["git", "-C", str(old_path), "remote", "get-url", "origin"],
            ["git", "-C", str(new_path), "remote", "get-url", "origin"],
        )
        return h._FakeCompletedProcess(args=cmd, stdout=f"https://github.com/octo/{repo_name}.git\n")

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        runner_module.logger,
        "info",
        lambda *args: info_logs.append(args),
    )

    try:
        runner = h._make_runner(url=f"https://github.com/octo/{repo_name}.git")
        assert runner.repo_path == str(new_path)
        assert new_path.exists()
        assert not old_path.exists()
        assert run_calls == [
            ["git", "-C", str(old_path), "remote", "get-url", "origin"],
            ["git", "-C", str(new_path), "remote", "get-url", "origin"],
        ]
        assert info_logs
    finally:
        with contextlib.suppress(FileNotFoundError):
            import shutil

            shutil.rmtree(new_path)
        with contextlib.suppress(FileNotFoundError):
            import shutil

            shutil.rmtree(old_path)


def test_init_skips_legacy_clone_migration_when_origin_mismatches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_name = f"runner-init-skip-{time.time_ns()}"
    old_path = Path("/data/repos") / repo_name
    new_path = Path("/data/repos") / f"octo__{repo_name}"
    old_path.mkdir(parents=True)
    warnings: list[tuple[object, ...]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        return h._FakeCompletedProcess(args=cmd, stdout="https://github.com/octo/other.git\n")

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        runner_module.logger,
        "warning",
        lambda *args: warnings.append(args),
    )

    try:
        h._make_runner(url=f"https://github.com/octo/{repo_name}.git")
        assert old_path.exists()
        assert not new_path.exists()
        assert any("Legacy clone" in str(args[0]) for args in warnings)
    finally:
        with contextlib.suppress(FileNotFoundError):
            import shutil

            shutil.rmtree(new_path)
        with contextlib.suppress(FileNotFoundError):
            import shutil

            shutil.rmtree(old_path)


def test_init_skips_legacy_clone_migration_when_origin_probe_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_name = f"runner-init-error-{time.time_ns()}"
    old_path = Path("/data/repos") / repo_name
    new_path = Path("/data/repos") / f"octo__{repo_name}"
    old_path.mkdir(parents=True)
    warnings: list[tuple[object, ...]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        raise RuntimeError("git unavailable")

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        runner_module.logger,
        "warning",
        lambda *args: warnings.append(args),
    )

    try:
        h._make_runner(url=f"https://github.com/octo/{repo_name}.git")
        assert old_path.exists()
        assert not new_path.exists()
        assert any("Could not verify origin for %s — skipping migration" in str(args[0]) for args in warnings)
    finally:
        with contextlib.suppress(FileNotFoundError):
            import shutil

            shutil.rmtree(new_path)
        with contextlib.suppress(FileNotFoundError):
            import shutil

            shutil.rmtree(old_path)


def test_init_removes_non_git_directory_at_new_clone_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_name = f"runner-init-nongit-{time.time_ns()}"
    new_path = Path("/data/repos") / f"octo__{repo_name}"
    new_path.mkdir(parents=True)
    warnings: list[tuple[object, ...]] = []

    monkeypatch.setattr(
        runner_module.logger,
        "warning",
        lambda *args: warnings.append(args),
    )

    try:
        h._make_runner(url=f"https://github.com/octo/{repo_name}.git")
        assert not new_path.exists()
        assert any("Removing non-git directory %s" in str(args[0]) for args in warnings)
    finally:
        with contextlib.suppress(FileNotFoundError):
            import shutil

            shutil.rmtree(new_path)


def test_init_removes_stale_clone_when_origin_mismatches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_name = f"runner-init-stale-{time.time_ns()}"
    new_path = Path("/data/repos") / f"octo__{repo_name}"
    (new_path / ".git").mkdir(parents=True)
    warnings: list[tuple[object, ...]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        return h._FakeCompletedProcess(args=cmd, stdout="https://github.com/octo/other.git\n")

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        runner_module.logger,
        "warning",
        lambda *args: warnings.append(args),
    )

    try:
        h._make_runner(url=f"https://github.com/octo/{repo_name}.git")
        assert not new_path.exists()
        assert any("removing stale clone" in str(args[0]).lower() for args in warnings)
    finally:
        with contextlib.suppress(FileNotFoundError):
            import shutil

            shutil.rmtree(new_path)


def test_init_logs_when_new_clone_origin_probe_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_name = f"runner-init-new-error-{time.time_ns()}"
    new_path = Path("/data/repos") / f"octo__{repo_name}"
    (new_path / ".git").mkdir(parents=True)
    warnings: list[tuple[object, ...]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        raise RuntimeError("git unavailable")

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        runner_module.logger,
        "warning",
        lambda *args: warnings.append(args),
    )

    try:
        h._make_runner(url=f"https://github.com/octo/{repo_name}.git")
        assert new_path.exists()
        assert any("Could not verify origin for %s" == str(args[0]) for args in warnings)
    finally:
        with contextlib.suppress(FileNotFoundError):
            import shutil

            shutil.rmtree(new_path)


def test_mark_queue_done_writes_updated_queue_to_disk(tmp_path: Path) -> None:
    """PR-181: ``_mark_queue_done`` updates the local QUEUE.md only —
    no commit, no push, no remediation PR. The next IDLE cycle
    regenerates the file deterministically from task headers anyway,
    so the disk write is just a best-effort tweak for read consumers
    between merge and the next IDLE tick."""
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    queue_path.write_text("## PR-001: first\n- Status: DOING\n\n## PR-002: second\n- Status: TODO\n")

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.current_task = QueueTask(pr_id="PR-001", title="first", status=TaskStatus.DOING)

    runner._mark_queue_done()

    updated = queue_path.read_text()
    assert "## PR-001: first\n- Status: DONE" in updated
    assert "## PR-002: second\n- Status: TODO" in updated
    # The pending queue-sync infrastructure is no longer engaged.
    assert runner.state.pending_queue_sync_branch is None
    assert runner.state.pending_queue_sync_started_at is None


def test_mark_queue_done_skips_when_origin_queue_md_tracked(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Legacy repos that still track ``tasks/QUEUE.md`` on origin must
    not have the local file rewritten — an unstaged rewrite would
    dirty the working tree, push the next-cycle preflight to ERROR,
    and block normal IDLE dispatch. Mirrors the
    ``_write_generated_queue_md`` skip in IDLE."""
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    original = "## PR-001: first\n- Status: DOING\n\n## PR-002: second\n- Status: TODO\n"
    queue_path.write_text(original)

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.current_task = QueueTask(pr_id="PR-001", title="first", status=TaskStatus.DOING)
    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_origin_queue_md_tracked",
        lambda self: True,
    )

    runner._mark_queue_done()

    assert queue_path.read_text() == original


def test_mark_queue_done_skips_when_tracking_probe_indeterminate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """If the tracked-QUEUE probe itself failed (``None``), ``_mark_queue_done``
    must skip the in-place rewrite. Conservatively treating ``None`` as
    "tracked" protects legacy repos with a transiently flaky probe from
    a dirtied working tree on every merge; post-PR-181 repos lose only
    one in-place tweak and the next IDLE cycle regenerates QUEUE.md."""
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    original = "## PR-001: first\n- Status: DOING\n\n## PR-002: second\n- Status: TODO\n"
    queue_path.write_text(original)

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.current_task = QueueTask(pr_id="PR-001", title="first", status=TaskStatus.DOING)
    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_origin_queue_md_tracked",
        lambda self: None,
    )

    runner._mark_queue_done()

    assert queue_path.read_text() == original


def test_mark_queue_done_returns_without_current_task() -> None:
    runner = h._make_runner()
    runner._mark_queue_done()

    assert runner.state.pending_queue_sync_branch is None
    assert runner.state.pending_queue_sync_started_at is None


def test_mark_queue_done_no_op_when_queue_missing(tmp_path: Path) -> None:
    """A missing local QUEUE.md is a no-op — nothing to update."""
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.current_task = QueueTask(pr_id="PR-001", title="first", status=TaskStatus.DOING)

    runner._mark_queue_done()

    assert runner.state.pending_queue_sync_branch is None


def test_mark_queue_done_no_op_when_pr_id_not_in_queue(tmp_path: Path) -> None:
    """If the merged ``pr_id`` is absent from the local QUEUE.md the
    file is left untouched — ``mark_task_done`` returns ``None`` and
    the helper exits without writing."""
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    original = "## PR-999: other\n- Status: TODO\n"
    queue_path.write_text(original)

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.current_task = QueueTask(pr_id="PR-001", title="first", status=TaskStatus.DOING)

    runner._mark_queue_done()

    assert queue_path.read_text() == original


def test_mark_queue_done_logs_warning_on_read_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    queue_path.write_text("## PR-001: first\n- Status: DOING\n")

    def boom(self: Path, *args: Any, **kwargs: Any) -> str:
        raise OSError("read denied")

    monkeypatch.setattr(Path, "read_text", boom)

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.current_task = QueueTask(pr_id="PR-001", title="first", status=TaskStatus.DOING)
    events: list[str] = []
    monkeypatch.setattr(runner, "log_event", events.append)

    runner._mark_queue_done()

    assert any("read QUEUE.md to mark PR-001 DONE failed" in e for e in events)


def test_mark_queue_done_logs_warning_on_write_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    queue_path.write_text("## PR-001: first\n- Status: DOING\n")

    def boom(self: Path, *args: Any, **kwargs: Any) -> int:
        raise OSError("write denied")

    monkeypatch.setattr(Path, "write_text", boom)

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.current_task = QueueTask(pr_id="PR-001", title="first", status=TaskStatus.DOING)
    events: list[str] = []
    monkeypatch.setattr(runner, "log_event", events.append)

    runner._mark_queue_done()

    assert any("write QUEUE.md to mark PR-001 DONE failed" in e for e in events)


def test_resolve_pending_queue_sync_returns_true_without_branch() -> None:
    runner = h._make_runner()

    assert asyncio.run(runner._resolve_pending_queue_sync()) is True


def test_resolve_pending_queue_sync_continues_when_pr_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    escalations: list[str] = []
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda cmd, **kwargs: {"state": "open", "mergedAt": None},
    )

    runner = h._make_runner()
    runner.state.pending_queue_sync_branch = "queue-done-pr-001"
    runner.state.pending_queue_sync_started_at = datetime.now(timezone.utc)

    async def fake_escalate(branch: str) -> None:
        escalations.append(branch)

    monkeypatch.setattr(
        runner,
        "_escalate_queue_sync_if_expired",
        fake_escalate,
    )

    assert asyncio.run(runner._resolve_pending_queue_sync()) is False
    assert runner.state.pending_queue_sync_branch == "queue-done-pr-001"
    assert escalations == ["queue-done-pr-001"]


def test_resolve_pending_queue_sync_clears_state_when_pr_merged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda cmd, **kwargs: {
            "state": "merged",
            "mergedAt": "2026-04-19T18:00:00Z",
        },
    )

    runner = h._make_runner()
    runner.state.pending_queue_sync_branch = "queue-done-pr-001"
    runner.state.pending_queue_sync_started_at = datetime.now(timezone.utc)
    events: list[str] = []
    monkeypatch.setattr(runner, "log_event", events.append)

    assert asyncio.run(runner._resolve_pending_queue_sync()) is True
    assert runner.state.pending_queue_sync_branch is None
    assert runner.state.pending_queue_sync_started_at is None
    assert events == ["[MERGE] Queue-sync PR merged (queue-done-pr-001)."]


def test_resolve_pending_queue_sync_clears_state_when_pr_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda cmd, **kwargs: {"state": "closed", "mergedAt": None},
    )

    runner = h._make_runner()
    runner.state.pending_queue_sync_branch = "queue-done-pr-001"
    runner.state.pending_queue_sync_started_at = datetime.now(timezone.utc)
    events: list[str] = []
    monkeypatch.setattr(runner, "log_event", events.append)

    assert asyncio.run(runner._resolve_pending_queue_sync()) is False
    assert runner.state.pending_queue_sync_branch is None
    assert runner.state.pending_queue_sync_started_at is None
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == ("queue-sync PR queue-done-pr-001 closed without merging")
    assert events == [f"[MERGE] {runner.state.error_message}."]


def test_resolve_pending_queue_sync_handles_missing_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    escalations: list[str] = []
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda cmd, **kwargs: None,
    )

    runner = h._make_runner()
    runner.state.pending_queue_sync_branch = "queue-done-pr-001"

    async def fake_escalate(branch: str) -> None:
        escalations.append(branch)

    monkeypatch.setattr(
        runner,
        "_escalate_queue_sync_if_expired",
        fake_escalate,
    )

    assert asyncio.run(runner._resolve_pending_queue_sync()) is False
    assert runner.state.pending_queue_sync_branch == "queue-done-pr-001"
    assert escalations == ["queue-done-pr-001"]


def test_resolve_pending_queue_sync_logs_and_escalates_on_view_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    escalations: list[str] = []

    def fail_run_gh(cmd: list[str], **kwargs: Any) -> None:
        raise RuntimeError("gh unavailable")

    monkeypatch.setattr("src.github.gh_runner.run_gh", fail_run_gh)

    runner = h._make_runner()
    runner.state.pending_queue_sync_branch = "queue-done-pr-001"
    events: list[str] = []
    monkeypatch.setattr(runner, "log_event", events.append)

    async def fake_escalate(branch: str) -> None:
        escalations.append(branch)

    monkeypatch.setattr(
        runner,
        "_escalate_queue_sync_if_expired",
        fake_escalate,
    )

    assert asyncio.run(runner._resolve_pending_queue_sync()) is False
    assert events == ["[MERGE] queue-sync PR queue-done-pr-001 view failed: gh unavailable."]
    assert escalations == ["queue-done-pr-001"]


def test_escalate_queue_sync_no_op_when_started_at_missing() -> None:
    runner = h._make_runner()
    runner.state.pending_queue_sync_branch = "queue-done-pr-001"

    asyncio.run(runner._escalate_queue_sync_if_expired("queue-done-pr-001"))

    assert runner.state.pending_queue_sync_branch == "queue-done-pr-001"
    assert runner.state.pending_queue_sync_started_at is None
    assert runner.state.state == PipelineState.IDLE


def test_escalate_queue_sync_no_op_when_not_expired() -> None:
    runner = h._make_runner()
    runner.state.pending_queue_sync_branch = "queue-done-pr-001"
    runner.state.pending_queue_sync_started_at = datetime.now(timezone.utc) - timedelta(minutes=5)

    asyncio.run(runner._escalate_queue_sync_if_expired("queue-done-pr-001"))

    assert runner.state.pending_queue_sync_branch == "queue-done-pr-001"
    assert runner.state.pending_queue_sync_started_at is not None
    assert runner.state.state == PipelineState.IDLE


def test_ensure_repo_cloned_retries_scaffold_after_transient_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """A transient scaffold failure (e.g. initial push timeout) must
    not be swallowed and must leave ``_scaffolded`` unset so the next
    cycle retries. Once scaffold_repo finally succeeds,
    ``_scaffolded`` flips to True and scaffold_repo is never called
    again. Without this loop, the first-clone push failure strands
    ``origin/{branch}`` without ``tasks/QUEUE.md`` and the runner sits
    in ERROR forever because ``_parse_base_queue`` keeps reading a
    missing file.
    """
    h._patch_subprocess(monkeypatch)

    scaffold_calls: list[str] = []
    attempts = {"n": 0}

    def fake_scaffold(path: str, branch: str) -> list[str]:
        attempts["n"] += 1
        scaffold_calls.append(branch)
        if attempts["n"] == 1:
            raise RuntimeError("simulated push timeout")
        return ["AGENTS.md", "tasks/QUEUE.md"]

    monkeypatch.setattr(runner_module.scaffolder, "scaffold_repo", fake_scaffold)

    runner = h._make_runner()
    # Point repo_path at a non-existent directory so ensure_repo_cloned
    # takes the clone branch on every call (clone is mocked to a no-op
    # via h._patch_subprocess).
    runner.repo_path = str(tmp_path / "clone-target")

    # Cycle 1: scaffold raises -> RuntimeError out of
    # ensure_repo_cloned (no longer silently swallowed).
    with pytest.raises(RuntimeError, match="scaffold_repo failed"):
        asyncio.run(runner.ensure_repo_cloned())
    assert runner._scaffolded is False
    assert scaffold_calls == ["main"]

    # Cycle 2: scaffold succeeds -> _scaffolded flips True and the
    # created files are logged.
    asyncio.run(runner.ensure_repo_cloned())
    assert runner._scaffolded is True
    assert scaffold_calls == ["main", "main"]
    assert any("scaffold_repo created" in e["event"] for e in runner.state.history)

    # Cycle 3: scaffold_repo is NOT called again — _scaffolded gates
    # the entire retry loop.
    asyncio.run(runner.ensure_repo_cloned())
    assert scaffold_calls == ["main", "main"]


def test_ensure_repo_cloned_tolerates_fetch_failure_before_first_scaffold(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """On a previously-cloned but never-successfully-scaffolded repo,
    ``git fetch origin {branch}`` can fail with "couldn't find remote
    ref" because the prior cycle's scaffolding push never landed.
    ``ensure_repo_cloned`` must tolerate that failure and still call
    scaffold_repo, which is idempotent at the remote level and will
    re-push the stranded commit.
    """
    # Make the path exist so ensure_repo_cloned takes the fetch branch.
    existing = tmp_path / "clone-target"
    existing.mkdir()

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:2] == ["git", "fetch"]:
            raise subprocess.CalledProcessError(
                128,
                cmd,
                stderr="fatal: couldn't find remote ref main",
            )
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    scaffold_calls: list[str] = []

    def fake_scaffold(path: str, branch: str) -> list[str]:
        scaffold_calls.append(branch)
        return ["AGENTS.md"]

    monkeypatch.setattr(runner_module.scaffolder, "scaffold_repo", fake_scaffold)

    runner = h._make_runner()
    runner.repo_path = str(existing)
    # Simulate the pre-scaffold state explicitly — h._make_runner's
    # default repo_path doesn't exist so __init__ already seeded
    # _scaffolded=False, but we re-assert here for clarity.
    runner._scaffolded = False

    # fetch failure before first scaffold: must NOT raise, must still
    # call scaffold_repo, and must set _scaffolded True on success.
    asyncio.run(runner.ensure_repo_cloned())

    assert scaffold_calls == ["main"]
    assert runner._scaffolded is True
    # The tolerated fetch failure leaves a breadcrumb in history so
    # the operator can see what happened.
    assert any("will retry scaffold" in e["event"] for e in runner.state.history)


def test_ensure_repo_cloned_raises_non_missing_ref_fetch_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """``git fetch`` failures that are NOT the missing-remote-ref
    case must raise immediately, regardless of ``_scaffolded`` state.
    The earlier tolerance was too broad: an auth/network blip before
    the first scaffold would silently let ``recover_state`` proceed
    with stale local ``origin/{branch}`` data, even though we have
    no way to refresh it on this cycle.
    """
    existing = tmp_path / "clone-target"
    existing.mkdir()

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:2] == ["git", "fetch"]:
            raise subprocess.CalledProcessError(
                128,
                cmd,
                stderr=("fatal: Authentication failed for 'https://github.com/octo/demo.git'"),
            )
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    # Assert both code paths raise on non-missing-ref fetch failures:
    # the pre-scaffold state (was previously tolerated too broadly)
    # AND the post-scaffold state.
    for scaffolded in (False, True):
        runner = h._make_runner()
        runner.repo_path = str(existing)
        runner._scaffolded = scaffolded
        with pytest.raises(RuntimeError, match="git fetch failed"):
            asyncio.run(runner.ensure_repo_cloned())


def test_ensure_repo_cloned_skips_scaffold_when_repo_already_looks_scaffolded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """On a daemon restart with an existing clone that already has
    the scaffolding files on disk, ``scaffold_repo`` must NOT run.
    Its upfront ``git checkout {branch}`` would clobber a dirty
    working tree left by an interrupted coding cycle, masking the
    real crash-recovery path handled by ``recover_state``. The
    ``_scaffolded`` gate is seeded from ``_repo_looks_scaffolded``
    at ``__init__`` time so it survives process restarts (the
    in-memory flag itself does not).
    """
    existing = tmp_path / "clone-target"
    existing.mkdir()
    h._populate_fully_scaffolded_repo(existing)

    # The helper should recognise this directory as already scaffolded.
    assert runner_module._repo_looks_scaffolded(str(existing)) is True

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        # Both local refs/heads/main and refs/remotes/origin/main
        # exist, and rev-list --count reports 0 commits ahead — the
        # repo is fully in sync, so _base_branch_ahead_of_origin
        # returns False and no scaffold retry is triggered.
        if cmd[:2] == ["git", "rev-list"]:
            return h._FakeCompletedProcess(args=cmd, returncode=0, stdout="0\n")
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    scaffold_calls: list[str] = []

    def fake_scaffold(path: str, branch: str) -> list[str]:
        scaffold_calls.append(branch)
        return []

    monkeypatch.setattr(runner_module.scaffolder, "scaffold_repo", fake_scaffold)

    runner = h._make_runner()
    runner.repo_path = str(existing)
    # Re-seed the gate using the helper, mirroring what __init__ would
    # have done if ``/data/repos/demo`` were this test-local path.
    runner._scaffolded = runner_module._repo_looks_scaffolded(str(existing))
    assert runner._scaffolded is True

    asyncio.run(runner.ensure_repo_cloned())

    # scaffold_repo must not have run: the repo already looks
    # scaffolded, so no git checkout runs against the working tree.
    assert scaffold_calls == []
    assert runner._scaffolded is True


def test_ensure_repo_cloned_defers_scaffold_when_working_tree_dirty(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """A restart on a partially-scaffolded repo (``_repo_looks_
    scaffolded`` returns False) that also has a dirty working tree
    from an interrupted coding cycle must NOT call scaffold_repo:
    scaffold_repo starts with ``git checkout {branch}`` which would
    hit "Your local changes would be overwritten" and raise every
    cycle, masking the real crash-recovery path. ``ensure_repo_
    cloned`` must instead defer scaffolding so ``recover_state`` /
    ``preflight`` can run and either clean up the tree or surface
    the real error; a later cycle with a clean tree will retry.
    """
    existing = tmp_path / "clone-target"
    existing.mkdir()
    # Partial scaffolding: only AGENTS.md. Missing tasks/QUEUE.md,
    # scripts/ci.sh, scripts/make-review-artifacts.sh, and the
    # .gitignore entry — so _repo_looks_scaffolded returns False.
    (existing / "AGENTS.md").write_text("# AGENTS\n")
    assert runner_module._repo_looks_scaffolded(str(existing)) is False

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:2] == ["git", "status"] and "--porcelain" in cmd:
            # Dirty working tree: interrupted coding left a modified
            # file and an untracked file.
            return h._FakeCompletedProcess(
                args=cmd,
                stdout=" M src/foo.py\n?? src/bar.py\n",
                returncode=0,
            )
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    scaffold_calls: list[str] = []

    def fake_scaffold(path: str, branch: str) -> list[str]:
        scaffold_calls.append(branch)
        return ["tasks/QUEUE.md"]

    monkeypatch.setattr(runner_module.scaffolder, "scaffold_repo", fake_scaffold)

    runner = h._make_runner()
    runner.repo_path = str(existing)
    runner._scaffolded = False  # partial fs → __init__ would also set False

    # Must NOT raise: the scaffold is deferred, not executed.
    asyncio.run(runner.ensure_repo_cloned())

    # scaffold_repo must not have run — its git checkout would have
    # clobbered the dirty tree.
    assert scaffold_calls == []
    # _scaffolded stays False so the next cycle (with a clean tree)
    # will retry.
    assert runner._scaffolded is False
    # A defer breadcrumb is logged so the operator can see why
    # scaffold_repo did not run.
    assert any("scaffold_repo deferred" in e["event"] for e in runner.state.history)


def test_repo_looks_scaffolded_rejects_partial_provisioning(
    tmp_path: Any,
) -> None:
    """The fs probe must require **every** asset scaffold_repo would
    commit — not just the three most visible files. A repo that
    pre-existed with ``AGENTS.md`` + ``tasks/QUEUE.md`` +
    ``scripts/ci.sh`` but no ``scripts/make-review-artifacts.sh``
    (or no ``artifacts/`` entry in ``.gitignore``) must NOT be
    classified as scaffolded: the daemon would otherwise skip
    scaffold_repo permanently, leaving those files uncreated, and
    the first ``make-review-artifacts.sh`` run would dirty the
    working tree until ``preflight`` forces ERROR.
    """
    base = tmp_path / "partial"
    base.mkdir()
    (base / "AGENTS.md").write_text("# AGENTS\n")
    (base / "tasks").mkdir()
    (base / "tasks" / "QUEUE.md").write_text("# Task Queue\n")
    (base / "scripts").mkdir()
    (base / "scripts" / "ci.sh").write_text("#!/usr/bin/env bash\n")
    # Missing: scripts/make-review-artifacts.sh and .gitignore.
    assert runner_module._repo_looks_scaffolded(str(base)) is False

    # Add the missing review-artifacts script — still missing .gitignore.
    (base / "scripts" / "make-review-artifacts.sh").write_text("#!/usr/bin/env bash\n")
    assert runner_module._repo_looks_scaffolded(str(base)) is False

    # Add a .gitignore that does NOT mention artifacts/.
    (base / ".gitignore").write_text("node_modules/\n*.pyc\n")
    assert runner_module._repo_looks_scaffolded(str(base)) is False

    # Finally append artifacts/ — now fully scaffolded.
    (base / ".gitignore").write_text("node_modules/\n*.pyc\nartifacts/\n")
    assert runner_module._repo_looks_scaffolded(str(base)) is True


def test_ensure_repo_cloned_resets_scaffolded_when_base_branch_ahead(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """Daemon restart on a repo whose local base branch has commits
    not yet on ``origin/{branch}``: the prior cycle committed
    scaffolding locally but the push failed while ``origin/{branch}``
    still existed (so the missing-ref tolerance did NOT trigger).
    The fs check at ``__init__`` seeds ``_scaffolded=True`` but the
    base-branch-ahead probe must reset it so scaffold_repo runs and
    re-pushes the stranded commit. Without this, ``recover_state``
    keeps reading stale data from ``origin/{branch}:tasks/QUEUE.md``
    with no retry path.
    """
    existing = tmp_path / "clone-target"
    existing.mkdir()
    h._populate_fully_scaffolded_repo(existing)

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:3] == ["git", "rev-parse", "--verify"]:
            # Both refs/heads/main and refs/remotes/origin/main exist.
            return h._FakeCompletedProcess(args=cmd, returncode=0)
        if cmd[:2] == ["git", "rev-list"]:
            # Local base is 1 commit ahead of origin — the stranded
            # scaffolding commit.
            return h._FakeCompletedProcess(args=cmd, returncode=0, stdout="1\n")
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    scaffold_calls: list[str] = []

    def fake_scaffold(path: str, branch: str) -> list[str]:
        scaffold_calls.append(branch)
        return []

    monkeypatch.setattr(runner_module.scaffolder, "scaffold_repo", fake_scaffold)

    runner = h._make_runner()
    runner.repo_path = str(existing)
    runner._scaffolded = runner_module._repo_looks_scaffolded(str(existing))
    assert runner._scaffolded is True

    asyncio.run(runner.ensure_repo_cloned())

    # Despite the fs check seeding True, the base-branch-ahead probe
    # reset the gate and the retry block ran scaffold_repo.
    assert scaffold_calls == ["main"]
    assert runner._scaffolded is True  # set back to True after retry
    # A breadcrumb records why the retry happened.
    assert any("ahead of origin" in e["event"] for e in runner.state.history)


def test_ensure_repo_cloned_resets_scaffolded_on_probe_timeout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """A ``TimeoutExpired`` on any of the three
    ``_base_branch_ahead_of_origin`` probes must fall back to
    "ahead" so the scaffold retry still runs. Without this, the
    helper would raise a non-``RuntimeError`` out of
    ``ensure_repo_cloned`` and ``run_cycle`` would skip its normal
    ERROR-state/publish path — most visible during transient git
    stalls (lock contention, slow storage).
    """
    existing = tmp_path / "clone-target"
    existing.mkdir()
    h._populate_fully_scaffolded_repo(existing)

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:2] == ["git", "rev-list"]:
            raise subprocess.TimeoutExpired(cmd, kwargs.get("timeout", 0))
        if cmd[:3] == ["git", "rev-parse", "--verify"]:
            return h._FakeCompletedProcess(args=cmd, returncode=0)
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    scaffold_calls: list[str] = []

    def fake_scaffold(path: str, branch: str) -> list[str]:
        scaffold_calls.append(branch)
        return []

    monkeypatch.setattr(runner_module.scaffolder, "scaffold_repo", fake_scaffold)

    runner = h._make_runner()
    runner.repo_path = str(existing)
    runner._scaffolded = runner_module._repo_looks_scaffolded(str(existing))
    assert runner._scaffolded is True

    # Must NOT raise TimeoutExpired out of ensure_repo_cloned.
    asyncio.run(runner.ensure_repo_cloned())

    # The timeout was interpreted as "ahead" → scaffold retry ran.
    assert scaffold_calls == ["main"]
    assert runner._scaffolded is True


def test_ensure_repo_cloned_preserves_scaffolded_when_base_branch_synced(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """Daemon restart on a fully-synced, fully-scaffolded repo must
    NOT reset ``_scaffolded`` — doing so would re-run scaffold_repo
    on every normal restart and defeat the round-5 P2 fix that
    protected the crash-recovery path. The base-branch-ahead probe
    should report False (synced), and the retry block should be
    skipped.
    """
    existing = tmp_path / "clone-target"
    existing.mkdir()
    h._populate_fully_scaffolded_repo(existing)

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:3] == ["git", "rev-parse", "--verify"]:
            return h._FakeCompletedProcess(args=cmd, returncode=0)
        if cmd[:2] == ["git", "rev-list"]:
            # 0 commits ahead — fully synced with origin.
            return h._FakeCompletedProcess(args=cmd, returncode=0, stdout="0\n")
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    scaffold_calls: list[str] = []

    def fake_scaffold(path: str, branch: str) -> list[str]:
        scaffold_calls.append(branch)
        return []

    monkeypatch.setattr(runner_module.scaffolder, "scaffold_repo", fake_scaffold)

    runner = h._make_runner()
    runner.repo_path = str(existing)
    runner._scaffolded = runner_module._repo_looks_scaffolded(str(existing))
    assert runner._scaffolded is True

    asyncio.run(runner.ensure_repo_cloned())

    # scaffold_repo not called, gate preserved.
    assert scaffold_calls == []
    assert runner._scaffolded is True


def test_ensure_repo_cloned_retries_scaffold_on_missing_ref_after_restart(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """Even on a restart where the local fs looks scaffolded, if
    ``git fetch`` reports the missing-remote-ref condition the
    scaffold retry must still run so the stranded commit from a
    prior cycle is re-pushed. Without this, a crashed daemon after
    a transient first-push failure would sit in ERROR forever
    because ``_scaffolded`` seeded True at ``__init__`` would
    otherwise skip the retry.
    """
    existing = tmp_path / "clone-target"
    existing.mkdir()
    # Scaffolding files are on disk (prior cycle committed them)...
    h._populate_fully_scaffolded_repo(existing)

    # ...but fetch reports the branch is missing upstream (the prior
    # cycle's initial push failed transiently).
    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:2] == ["git", "fetch"]:
            raise subprocess.CalledProcessError(
                128,
                cmd,
                stderr="fatal: couldn't find remote ref main",
            )
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    scaffold_calls: list[str] = []

    def fake_scaffold(path: str, branch: str) -> list[str]:
        scaffold_calls.append(branch)
        return []

    monkeypatch.setattr(runner_module.scaffolder, "scaffold_repo", fake_scaffold)

    runner = h._make_runner()
    runner.repo_path = str(existing)
    # Simulate the post-__init__ state: fs check passed so
    # _scaffolded is seeded True, but fetch will report missing ref
    # and force the retry.
    runner._scaffolded = runner_module._repo_looks_scaffolded(str(existing))
    assert runner._scaffolded is True

    asyncio.run(runner.ensure_repo_cloned())

    # The missing-ref fetch reset the gate and ran scaffold_repo so
    # the stranded commit gets re-pushed.
    assert scaffold_calls == ["main"]
    assert runner._scaffolded is True


def test_generate_queue_md_format() -> None:
    runner = h._make_runner()
    headers = [
        TaskHeader(
            pr_id="PR-001",
            title="Project bootstrap",
            branch="pr-001-bootstrap",
            task_type="feature",
            complexity="low",
            depends_on=[],
            priority=1,
            coder="any",
        ),
        TaskHeader(
            pr_id="PR-002",
            title="Config loader",
            branch="pr-002-models",
            task_type="feature",
            complexity="low",
            depends_on=["PR-001"],
            priority=2,
            coder="any",
        ),
    ]

    rendered = runner._generate_queue_md(
        headers,
        {
            "PR-001": TaskStatus.DONE,
            "PR-002": TaskStatus.TODO,
        },
    )

    assert rendered == (
        "# Task Queue\n\n"
        "## PR-001: Project bootstrap\n"
        "- Status: DONE\n"
        "- Tasks file: tasks/PR-001.md\n"
        "- Branch: pr-001-bootstrap\n\n"
        "## PR-002: Config loader\n"
        "- Status: TODO\n"
        "- Tasks file: tasks/PR-002.md\n"
        "- Branch: pr-002-models\n"
        "- Depends on: PR-001\n"
    )


def test_queue_md_not_committed_when_unchanged(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    headers = [
        TaskHeader(
            pr_id="PR-001",
            title="Project bootstrap",
            branch="pr-001-bootstrap",
            task_type="feature",
            complexity="low",
            depends_on=[],
            priority=1,
            coder="any",
        )
    ]
    statuses = {"PR-001": TaskStatus.DONE}
    queue_path.write_text(
        h._make_runner()._generate_queue_md(headers, statuses),
        encoding="utf-8",
    )

    git_calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        git_calls.append(cmd)
        # Simulate the post-PR-181 untracked state: ``git cat-file -e
        # origin/main:tasks/QUEUE.md`` reports the file is missing from
        # origin so the helper takes the local-write branch.
        if cmd[1:3] == ["cat-file", "-e"]:
            return h._FakeCompletedProcess(args=cmd, returncode=1)
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(git_ops_module.subprocess, "run", fake_run)

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._write_generated_queue_md(headers, statuses)

    assert all(call[1] == "cat-file" for call in git_calls), git_calls
    assert queue_path.read_text(encoding="utf-8") == runner._generate_queue_md(
        headers,
        statuses,
    )


def test_write_generated_queue_md_writes_disk_only(tmp_path: Path) -> None:
    """PR-181: ``_write_generated_queue_md`` writes the regenerated
    QUEUE.md to disk for read-side consumers and never commits or
    pushes it (the file is gitignored)."""
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    queue_path.write_text("# Task Queue\n\n## PR-000: Existing\n", encoding="utf-8")
    headers = [
        TaskHeader(
            pr_id="PR-001",
            title="Project bootstrap",
            branch="pr-001-bootstrap",
            task_type="feature",
            complexity="low",
            depends_on=[],
            priority=1,
            coder="any",
        )
    ]
    statuses = {"PR-001": TaskStatus.DONE}

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    published = runner._write_generated_queue_md(headers, statuses)

    assert published is True
    expected = runner._generate_queue_md(headers, statuses)
    assert queue_path.read_text(encoding="utf-8") == expected
    assert runner._idle_generated_queue_needs_resync is False


def test_write_generated_queue_md_no_op_when_content_unchanged(
    tmp_path: Path,
) -> None:
    """If the regenerated queue matches the on-disk content, the
    helper short-circuits without rewriting the file."""
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    headers = [
        TaskHeader(
            pr_id="PR-001",
            title="Project bootstrap",
            branch="pr-001-bootstrap",
            task_type="feature",
            complexity="low",
            depends_on=[],
            priority=1,
            coder="any",
        )
    ]
    statuses = {"PR-001": TaskStatus.DONE}

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    rendered = runner._generate_queue_md(headers, statuses)
    queue_path.write_text(rendered, encoding="utf-8")
    mtime_before = queue_path.stat().st_mtime_ns

    published = runner._write_generated_queue_md(headers, statuses)

    assert published is True
    assert queue_path.stat().st_mtime_ns == mtime_before


def test_write_generated_queue_md_creates_tasks_dir_if_missing(
    tmp_path: Path,
) -> None:
    """Fresh repos may not have ``tasks/`` yet — the helper must create
    the parent directory before writing the queue file."""
    headers = [
        TaskHeader(
            pr_id="PR-001",
            title="Project bootstrap",
            branch="pr-001-bootstrap",
            task_type="feature",
            complexity="low",
            depends_on=[],
            priority=1,
            coder="any",
        )
    ]
    statuses = {"PR-001": TaskStatus.DONE}

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    published = runner._write_generated_queue_md(headers, statuses)

    assert published is True
    assert (tmp_path / "tasks" / "QUEUE.md").exists()


def test_write_generated_queue_md_skips_when_tracked_on_origin(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Legacy repos (pre-PR-181) keep ``tasks/QUEUE.md`` tracked on
    ``origin/{branch}``. ``.gitignore`` does not retroactively untrack
    files, so a write here would dirty the working tree on every IDLE
    cycle, push preflight into ERROR, and block dispatch. The helper
    must detect the tracked snapshot and skip the write entirely.
    """
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    legacy_text = "# legacy on disk\n"
    queue_path.write_text(legacy_text, encoding="utf-8")
    headers = [
        TaskHeader(
            pr_id="PR-001",
            title="Project bootstrap",
            branch="pr-001-bootstrap",
            task_type="feature",
            complexity="low",
            depends_on=[],
            priority=1,
            coder="any",
        )
    ]
    statuses = {"PR-001": TaskStatus.DONE}

    git_calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        git_calls.append(cmd)
        if cmd[1:3] == ["cat-file", "-e"]:
            return h._FakeCompletedProcess(args=cmd, returncode=0)
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(git_ops_module.subprocess, "run", fake_run)

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    published = runner._write_generated_queue_md(headers, statuses)

    assert published is True
    # The tracked file on disk is left alone — no dirty modification.
    assert queue_path.read_text(encoding="utf-8") == legacy_text
    # Only the cat-file probe runs; no further git plumbing is invoked.
    assert git_calls == [["git", "cat-file", "-e", "origin/main:tasks/QUEUE.md"]]
    assert any(
        "Skipping QUEUE.md regeneration" in entry["event"] and "tracked on origin/main" in entry["event"]
        for entry in runner.state.history
    )

    # Re-running on the same legacy repo must not log the warning a
    # second time (one-shot guard via ``_legacy_tracked_queue_md_logged``).
    history_count_before = len(runner.state.history)
    runner._write_generated_queue_md(headers, statuses)
    new_logs = [
        entry
        for entry in runner.state.history[history_count_before:]
        if "Skipping QUEUE.md regeneration" in entry["event"]
    ]
    assert new_logs == []


def test_write_generated_queue_md_skips_when_probe_indeterminate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """When the tracked-QUEUE probe itself is indeterminate (timeout /
    OSError reports ``None``), ``_write_generated_queue_md`` skips the
    write conservatively. Treating ``None`` as "not tracked" would
    let a legacy repo's working tree be dirtied on every IDLE tick
    while the probe was flaky; treating it as "tracked" only loses one
    cycle of regeneration on post-PR-181 repos and self-heals next
    tick. The legacy-tracked log line must NOT fire — it would mislead
    operators into untracking a file that's actually fine."""
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    existing = "# existing on disk\n"
    queue_path.write_text(existing, encoding="utf-8")
    headers = [
        TaskHeader(
            pr_id="PR-001",
            title="Project bootstrap",
            branch="pr-001-bootstrap",
            task_type="feature",
            complexity="low",
            depends_on=[],
            priority=1,
            coder="any",
        )
    ]
    statuses = {"PR-001": TaskStatus.DONE}

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_origin_queue_md_tracked",
        lambda self: None,
    )

    published = runner._write_generated_queue_md(headers, statuses)

    assert published is True
    # The on-disk file is left alone — no rewrite, no dirty tree.
    assert queue_path.read_text(encoding="utf-8") == existing
    # The "tracked on origin" log line must NOT fire under indeterminate
    # probe results — that message tells operators to untrack the file.
    assert not any("Skipping QUEUE.md regeneration" in entry["event"] for entry in runner.state.history)


def test_select_next_task_from_dag_returns_none_when_tasks_dir_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is None


def test_filter_dag_headers_blocks_tasks_with_transitively_blocked_dependencies(
    tmp_path: Path,
) -> None:
    headers = [
        TaskHeader(
            pr_id="PR-001",
            title="Base",
            branch="pr-001-base",
            task_type="feature",
            complexity="low",
            depends_on=["PR-LEGACY"],
            priority=1,
            coder="any",
        ),
        TaskHeader(
            pr_id="PR-002",
            title="Blocked by blocked task",
            branch="pr-002-blocked",
            task_type="feature",
            complexity="low",
            depends_on=["PR-001"],
            priority=2,
            coder="any",
        ),
    ]

    filtered = idle_module.IdleMixin._filter_dag_headers_with_available_dependencies(
        headers,
        {"PR-LEGACY"},
        tmp_path,
        set(),
    )

    assert filtered == []


def test_select_next_task_from_dag_wraps_dag_cycle_errors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: Cyclic\n\n"
        "Branch: pr-001-cyclic\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = []
    runner._idle_merged_prs = []

    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state())
    monkeypatch.setattr(idle_module, "get_eligible_tasks", h._raise_cycle_detected)

    with pytest.raises(QueueValidationError, match="cycle detected"):
        asyncio.run(runner._select_next_task_from_dag())


def test_select_next_task_from_dag_returns_none_when_nothing_is_eligible(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: Waiting\n\n"
        "Branch: pr-001-waiting\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = []
    runner._idle_merged_prs = []

    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state())
    monkeypatch.setattr(
        idle_module,
        "derive_task_status",
        lambda header, merged_pr_ids, open_prs, merged_prs, **kwargs: TaskStatus.DONE,
    )
    monkeypatch.setattr(idle_module, "get_eligible_tasks", lambda headers, statuses: [])

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is None


def test_select_next_task_from_dag_skips_merged_probe_without_structured_headers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# Legacy task without structured metadata\n\nSome older task body.\n",
        encoding="utf-8",
    )

    def fail_resolve_merged_state(*args, **kwargs):
        raise AssertionError("_resolve_merged_state should not be called")

    monkeypatch.setattr(
        idle_module,
        "_resolve_merged_state",
        fail_resolve_merged_state,
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is None
    assert runner._idle_dag_tasks is None


def test_process_pending_uploads_preserves_upload_on_git_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """On transient git failure, Redis key and staging dir must survive for retry."""

    def failing_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:2] == ["git", "rev-list"]:
            return h._FakeCompletedProcess(args=cmd, stdout="0\n", returncode=0)
        if cmd[:2] == ["git", "add"]:
            raise subprocess.CalledProcessError(1, cmd, stderr="git error")
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", failing_run)

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    staging = tmp_path.parent / "uploads" / runner.name / "abc123"
    staging.mkdir(parents=True)
    (staging / "PR-001.md").write_text("- PR-001")

    manifest = json.dumps({"files": ["PR-001.md"], "staging_dir": str(staging)})
    key = f"upload:{runner.name}:pending"
    asyncio.run(runner.redis.set(key, manifest))

    result = asyncio.run(runner.process_pending_uploads())
    assert result is None
    assert asyncio.run(runner.redis.get(key)) == manifest
    assert staging.is_dir()


def test_process_pending_uploads_cas_delete_skips_newer_manifest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """After a successful push, a newer manifest must not be deleted."""
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    staging = tmp_path.parent / "uploads" / runner.name / "old123"
    staging.mkdir(parents=True, exist_ok=True)
    (staging / "PR-001.md").write_text("- PR-001")
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir(exist_ok=True)

    old_manifest = json.dumps({"files": ["PR-001.md"], "staging_dir": str(staging)})
    new_manifest = json.dumps({"files": ["PR-099.md"]})
    key = f"upload:{runner.name}:pending"
    asyncio.run(runner.redis.set(key, old_manifest))

    # Simulate a new upload arriving after the daemon read the old manifest
    original_eval = runner.redis.eval

    async def inject_new_manifest(script: str, numkeys: int, *args: Any) -> int:
        runner.redis.store[key] = new_manifest
        return await original_eval(script, numkeys, *args)

    runner.redis.eval = inject_new_manifest  # type: ignore[assignment]

    result = asyncio.run(runner.process_pending_uploads())
    assert result is None, "newer upload pending must block dispatch"
    assert asyncio.run(runner.redis.get(key)) == new_manifest
    assert staging.is_dir(), "staging dir must survive when CAS delete skips newer manifest"


def test_process_pending_uploads_routes_root_instruction_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    staging = tmp_path.parent / "uploads" / runner.name / "rootfiles"
    staging.mkdir(parents=True, exist_ok=True)
    (staging / "QUEUE.md").write_text("# Task Queue\n", encoding="utf-8")
    (staging / "AGENTS.md").write_text("# AGENTS\n", encoding="utf-8")
    (staging / "CLAUDE.md").write_text("Read AGENTS.md\n", encoding="utf-8")
    (tmp_path / "tasks").mkdir(exist_ok=True)

    manifest = json.dumps(
        {
            "files": ["QUEUE.md", "AGENTS.md", "CLAUDE.md"],
            "staging_dir": str(staging),
        }
    )
    key = f"upload:{runner.name}:pending"
    asyncio.run(runner.redis.set(key, manifest))

    result = asyncio.run(runner.process_pending_uploads())

    assert result is True
    # QUEUE.md is gitignored (PR-181) and must NOT be staged or copied
    # to the working tree from an upload, otherwise ``git add`` would
    # abort the whole batch and block subsequent dispatches.
    assert not (tmp_path / "tasks" / "QUEUE.md").exists()
    assert (tmp_path / "AGENTS.md").read_text(encoding="utf-8") == "# AGENTS\n"
    assert (tmp_path / "CLAUDE.md").read_text(encoding="utf-8") == "Read AGENTS.md\n"
    assert not (tmp_path / "tasks" / "AGENTS.md").exists()
    assert not (tmp_path / "tasks" / "CLAUDE.md").exists()


def test_process_pending_uploads_redis_error_blocks_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Redis read error must return None so handle_idle skips task dispatch."""
    runner = h._make_runner()

    async def broken_get(key: str) -> bytes:
        raise ConnectionError("redis gone")

    runner.redis.get = broken_get  # type: ignore[assignment]

    result = asyncio.run(runner.process_pending_uploads())
    assert result is None


def test_run_cycle_handles_ensure_repo_cloned_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publishes: list[str] = []
    runner = h._make_runner()

    async def fake_ensure_repo_cloned() -> None:
        raise RuntimeError("clone failed")

    async def fake_publish_state() -> None:
        publishes.append("published")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "clone failed"
    assert publishes == ["published"]
    assert any("ensure_repo_cloned failed: clone failed" in entry["event"] for entry in runner.state.history)


@pytest.mark.parametrize(
    ("head_ref", "expect_checkout"),
    [
        ("main", False),
        ("feature/work", True),
    ],
)
def test_run_cycle_processes_pending_uploads_from_recovery_when_on_or_off_base(
    monkeypatch: pytest.MonkeyPatch,
    head_ref: str,
    expect_checkout: bool,
) -> None:
    publishes: list[str] = []
    upload_calls: list[bool] = []
    git_calls: list[tuple[str, ...]] = []
    runner = h._make_runner()

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_recover_state() -> bool:
        return False

    async def fake_publish_state() -> None:
        publishes.append("published")

    async def fake_process_pending_uploads(*, _safe: bool) -> None:
        upload_calls.append(_safe)

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        git_calls.append(args)
        if args == ("rev-parse", "--abbrev-ref", "HEAD"):
            return h._FakeCompletedProcess(stdout=f"{head_ref}\n")
        if args == ("checkout", runner.repo_config.branch):
            return h._FakeCompletedProcess(stdout="")
        raise AssertionError(f"unexpected git call: {args}")

    runner.redis.store[f"upload:{runner.name}:pending"] = "pending"
    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "recover_state", fake_recover_state)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)
    monkeypatch.setattr(runner, "process_pending_uploads", fake_process_pending_uploads)
    monkeypatch.setattr(git_ops_module, "_git", fake_git)

    asyncio.run(runner.run_cycle())

    assert publishes == ["published"]
    assert upload_calls == [True]
    if expect_checkout:
        assert ("checkout", runner.repo_config.branch) in git_calls
    else:
        assert ("checkout", runner.repo_config.branch) not in git_calls


def test_run_cycle_skips_pending_uploads_when_git_probe_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publishes: list[str] = []
    upload_calls: list[bool] = []
    runner = h._make_runner()

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_recover_state() -> bool:
        return False

    async def fake_publish_state() -> None:
        publishes.append("published")

    async def fake_process_pending_uploads(*, _safe: bool) -> None:
        upload_calls.append(_safe)

    runner.redis.store[f"upload:{runner.name}:pending"] = "pending"
    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "recover_state", fake_recover_state)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)
    monkeypatch.setattr(runner, "process_pending_uploads", fake_process_pending_uploads)
    monkeypatch.setattr(
        git_ops_module,
        "_git",
        lambda repo_path, *args, **kwargs: (_ for _ in ()).throw(RuntimeError("rev-parse failed")),
    )

    asyncio.run(runner.run_cycle())

    assert publishes == ["published"]
    assert upload_calls == []


def test_run_cycle_ignores_pending_upload_probe_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publishes: list[str] = []
    runner = h._make_runner()

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_recover_state() -> bool:
        return False

    async def fake_publish_state() -> None:
        publishes.append("published")

    async def fake_get(key: str) -> str | None:
        raise RuntimeError("redis unavailable")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "recover_state", fake_recover_state)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)
    monkeypatch.setattr(runner.redis, "get", fake_get)

    asyncio.run(runner.run_cycle())

    assert publishes == ["published"]


def test_effective_idle_poll_interval_uses_base_below_threshold() -> None:
    """First two consecutive IDLE cycles still poll at the base interval."""
    runner = h._make_runner(poll_interval_sec=60)
    runner.app_config.daemon.idle_extended_after_cycles = 3
    runner.app_config.daemon.idle_extended_poll_interval_sec = 300

    runner._idle_streak = 1
    assert runner.effective_idle_poll_interval == 60
    runner._idle_streak = 2
    assert runner.effective_idle_poll_interval == 60


def test_effective_idle_poll_interval_uses_extended_at_threshold() -> None:
    """Third+ consecutive IDLE cycle drops to the extended interval."""
    runner = h._make_runner(poll_interval_sec=60)
    runner.app_config.daemon.idle_extended_after_cycles = 3
    runner.app_config.daemon.idle_extended_poll_interval_sec = 300

    runner._idle_streak = 3
    assert runner.effective_idle_poll_interval == 300
    runner._idle_streak = 50
    assert runner.effective_idle_poll_interval == 300


def test_effective_idle_poll_interval_takes_larger_of_two_slowdowns() -> None:
    """Rate-limit slowdown is folded in as ``max(extended, base*multiplier)``.

    Below the IDLE-streak threshold the slowdown does not affect the
    interval — the budget-check skip logic still throttles work to one
    in ``multiplier`` cycles. At/above the threshold the property
    returns the larger of the extended interval and ``base*multiplier``
    so the two slowdowns do not compound (the budget check then
    proceeds every cycle on the extended cadence, see
    ``test_check_budget_no_skip_when_extended_idle_active``).
    """
    runner = h._make_runner(poll_interval_sec=60)
    runner.app_config.daemon.idle_extended_after_cycles = 3
    runner.app_config.daemon.idle_extended_poll_interval_sec = 200
    runner.app_config.daemon.github_api_slowdown_multiplier = 5

    runner._idle_streak = 0
    runner._github_api_slowdown_attempts = 2
    assert runner.effective_idle_poll_interval == 60

    # extended=200 < base*multiplier=300 → take the larger (300).
    runner._idle_streak = 3
    runner._github_api_slowdown_attempts = 2
    assert runner.effective_idle_poll_interval == 300

    # extended=400 > base*multiplier=300 → stay at extended.
    runner.app_config.daemon.idle_extended_poll_interval_sec = 400
    assert runner.effective_idle_poll_interval == 400

    # No slowdown active: extended interval applies as-is.
    runner._github_api_slowdown_attempts = 0
    runner.app_config.daemon.idle_extended_poll_interval_sec = 200
    assert runner.effective_idle_poll_interval == 200


def test_update_idle_streak_increments_on_idle_with_no_pr() -> None:
    """The streak grows by one each cycle that ends in IDLE with no PR."""
    runner = h._make_runner()
    runner.state.state = PipelineState.IDLE
    runner.state.current_pr = None

    for expected in range(1, 6):
        runner._update_idle_streak_after_cycle(PipelineState.IDLE)
        assert runner._idle_streak == expected


def test_update_idle_streak_resets_when_state_leaves_idle() -> None:
    """Transitioning out of IDLE clears the streak so polling stays fast."""
    runner = h._make_runner()
    runner._idle_streak = 5
    runner.state.state = PipelineState.WATCH

    runner._update_idle_streak_after_cycle(PipelineState.IDLE)
    assert runner._idle_streak == 0


def test_update_idle_streak_resets_when_cycle_started_outside_idle() -> None:
    """A cycle that began in an active state and ended in IDLE resets the streak."""
    runner = h._make_runner()
    runner._idle_streak = 5
    runner.state.state = PipelineState.IDLE
    runner.state.current_pr = None

    for active in (
        PipelineState.WATCH,
        PipelineState.FIX,
        PipelineState.MERGE,
        PipelineState.CODING,
    ):
        runner._idle_streak = 5
        runner._update_idle_streak_after_cycle(active)
        assert runner._idle_streak == 0


def test_update_idle_streak_resets_when_idle_attaches_open_pr() -> None:
    """An IDLE-with-open-PR cycle is real work: reset the streak too."""
    runner = h._make_runner()
    runner._idle_streak = 4
    runner.state.state = PipelineState.IDLE
    runner.state.current_pr = PRInfo(number=42, branch="pr-042")

    runner._update_idle_streak_after_cycle(PipelineState.IDLE)
    assert runner._idle_streak == 0


def test_update_idle_streak_caps_at_sane_ceiling() -> None:
    """``_idle_streak`` does not grow without bound across long uptimes."""
    runner = h._make_runner()
    runner.state.state = PipelineState.IDLE
    runner.state.current_pr = None

    runner._idle_streak = runner_module._IDLE_STREAK_CAP
    runner._update_idle_streak_after_cycle(PipelineState.IDLE)
    assert runner._idle_streak == runner_module._IDLE_STREAK_CAP


def test_update_idle_streak_cap_respects_high_configured_threshold() -> None:
    """A configured threshold above the static cap must still be reachable."""
    runner = h._make_runner()
    runner.app_config.daemon.idle_extended_after_cycles = 250
    runner.state.state = PipelineState.IDLE
    runner.state.current_pr = None

    runner._idle_streak = runner_module._IDLE_STREAK_CAP
    runner._update_idle_streak_after_cycle(PipelineState.IDLE)
    assert runner._idle_streak == runner_module._IDLE_STREAK_CAP + 1

    runner._idle_streak = 250
    runner._update_idle_streak_after_cycle(PipelineState.IDLE)
    assert runner._idle_streak == 250


def test_reset_idle_streak_clears_counter() -> None:
    """``reset_idle_streak`` is the wake-event entry point."""
    runner = h._make_runner()
    runner._idle_streak = 7
    runner.reset_idle_streak()
    assert runner._idle_streak == 0


def test_run_cycle_grows_idle_streak_across_consecutive_idle_cycles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end: three IDLE-ending cycles flip to extended polling."""
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner(poll_interval_sec=60)
    runner.app_config.daemon.idle_extended_after_cycles = 3
    runner.app_config.daemon.idle_extended_poll_interval_sec = 300
    runner._recovered = True
    runner._scaffolded = True

    async def fake_handle_idle() -> None:
        runner.state.state = PipelineState.IDLE
        runner.state.current_pr = None

    async def fake_ensure_repo_cloned() -> None:
        return None

    monkeypatch.setattr(runner, "handle_idle", fake_handle_idle)
    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "preflight", h._preflight_true_stub)

    runner.state.state = PipelineState.IDLE
    intervals: list[int] = []
    for _ in range(4):
        asyncio.run(runner.run_cycle())
        intervals.append(runner.effective_idle_poll_interval)

    assert intervals == [60, 60, 300, 300]
    assert runner._idle_streak == 4


def test_run_cycle_resets_idle_streak_on_transition_into_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A WATCH→IDLE cycle must reset the streak, not increment it."""
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner(poll_interval_sec=60)
    runner.app_config.daemon.idle_extended_after_cycles = 3
    runner.app_config.daemon.idle_extended_poll_interval_sec = 300
    runner._recovered = True
    runner._scaffolded = True
    runner._idle_streak = 2

    async def fake_handle_watch() -> None:
        runner.state.state = PipelineState.IDLE
        runner.state.current_pr = None

    async def fake_ensure_repo_cloned() -> None:
        return None

    monkeypatch.setattr(runner, "handle_watch", fake_handle_watch)
    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "preflight", h._preflight_true_stub)

    runner.state.state = PipelineState.WATCH
    asyncio.run(runner.run_cycle())

    assert runner.state.state == PipelineState.IDLE
    assert runner._idle_streak == 0


def test_update_idle_streak_resets_when_pending_upload_deferred() -> None:
    """A cycle that deferred a pending upload must not grow the streak."""
    runner = h._make_runner()
    runner.state.state = PipelineState.IDLE
    runner.state.current_pr = None
    runner._idle_streak = 2
    runner._idle_dispatch_deferred = True

    runner._update_idle_streak_after_cycle(PipelineState.IDLE)

    assert runner._idle_streak == 0
    assert runner._idle_dispatch_deferred is False


def test_update_idle_streak_clears_deferred_flag_after_consuming() -> None:
    """The deferred flag is one-shot: cleared regardless of streak path."""
    runner = h._make_runner()
    runner.state.state = PipelineState.IDLE
    runner.state.current_pr = None
    runner._idle_dispatch_deferred = True
    runner._update_idle_streak_after_cycle(PipelineState.IDLE)
    assert runner._idle_dispatch_deferred is False

    runner._update_idle_streak_after_cycle(PipelineState.IDLE)
    assert runner._idle_streak == 1


def test_run_cycle_open_prs_failures_keep_polling_fast(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated GitHub read failures must not flip the runner to extended IDLE."""
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner(poll_interval_sec=60)
    runner.app_config.daemon.idle_extended_after_cycles = 3
    runner.app_config.daemon.idle_extended_poll_interval_sec = 300
    runner._recovered = True
    runner._scaffolded = True

    async def fake_handle_idle() -> None:
        runner.state.state = PipelineState.IDLE
        runner.state.current_pr = None
        runner._idle_dispatch_deferred = True

    async def fake_ensure_repo_cloned() -> None:
        return None

    monkeypatch.setattr(runner, "handle_idle", fake_handle_idle)
    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "preflight", h._preflight_true_stub)

    runner.state.state = PipelineState.IDLE
    intervals: list[int] = []
    for _ in range(5):
        asyncio.run(runner.run_cycle())
        intervals.append(runner.effective_idle_poll_interval)

    assert intervals == [60, 60, 60, 60, 60]
    assert runner._idle_streak == 0


def test_run_cycle_pending_upload_retries_keep_polling_fast(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pending-upload retries do not slow the runner into extended IDLE."""
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner(poll_interval_sec=60)
    runner.app_config.daemon.idle_extended_after_cycles = 3
    runner.app_config.daemon.idle_extended_poll_interval_sec = 300
    runner._recovered = True
    runner._scaffolded = True

    async def fake_handle_idle() -> None:
        runner.state.state = PipelineState.IDLE
        runner.state.current_pr = None
        runner._idle_dispatch_deferred = True

    async def fake_ensure_repo_cloned() -> None:
        return None

    monkeypatch.setattr(runner, "handle_idle", fake_handle_idle)
    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "preflight", h._preflight_true_stub)

    runner.state.state = PipelineState.IDLE
    intervals: list[int] = []
    for _ in range(5):
        asyncio.run(runner.run_cycle())
        intervals.append(runner.effective_idle_poll_interval)

    assert intervals == [60, 60, 60, 60, 60]
    assert runner._idle_streak == 0


def test_handle_idle_emits_agents_scan_events_when_specs_drift(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The IDLE handler must invoke ``reconcile_agents_md`` with the
    runner's ``log_event`` so the PR-260 drift scan reaches production.

    Regression for the PR-260 review feedback: the scan was wired into
    ``reconcile_agents_md`` but no daemon caller passed ``log_event_fn``,
    so violations were detected only by tests, never by the daemon.
    Pinning the IDLE-cycle invocation guarantees that pre-existing task
    specs containing AGENTS.md anti-patterns surface in the dashboard
    event log.
    """
    h._patch_subprocess(monkeypatch)

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-007.md").write_text(
        "# PR-007: Old spec\n\nSkip CI on this branch.\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs", lambda repo, **kw: []
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    asyncio.run(runner.handle_idle())

    scan_events = [
        entry["event"]
        for entry in runner.state.history
        if entry["event"].startswith("[AGENTS-SCAN]")
    ]
    assert any(
        "PR-007.md" in event and "skip_ci" in event for event in scan_events
    ), scan_events
    assert not (tmp_path / "AGENTS.md").exists(), (
        "dry_run=True means the daemon must not materialize AGENTS.md "
        "in the working tree on every IDLE cycle"
    )


def test_handle_idle_clean_tasks_dir_emits_no_agents_scan_events(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Silent on clean: the per-cycle scan must not flood the event log
    when no specs drift. ``_scan_existing_task_specs`` already enforces
    this contract; this test pins it through the IDLE call site so a
    future refactor cannot accidentally start logging summary events on
    every cycle."""
    h._patch_subprocess(monkeypatch)

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-008.md").write_text(
        "# PR-008: Clean spec\n\nNo anti-patterns here.\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs", lambda repo, **kw: []
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    asyncio.run(runner.handle_idle())

    scan_events = [
        entry["event"]
        for entry in runner.state.history
        if entry["event"].startswith("[AGENTS-SCAN]")
    ]
    assert scan_events == []


def test_handle_idle_swallows_os_error_in_agents_md_read(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Filesystem read errors on AGENTS.md must not crash IDLE.

    A directory left in place of AGENTS.md (network filesystem glitch,
    operator misstep, container volume mount mishap) makes
    ``Path.read_text`` raise ``IsADirectoryError`` (an ``OSError``
    subclass). The IDLE handler must catch it, surface a single
    explanatory event, and proceed with task selection so an unusual
    on-disk shape does not silently halt dispatch.
    """
    h._patch_subprocess(monkeypatch)

    (tmp_path / "AGENTS.md").mkdir()

    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs", lambda repo, **kw: []
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    scan_events = [
        entry["event"]
        for entry in runner.state.history
        if entry["event"].startswith("[AGENTS-SCAN]")
    ]
    assert any(
        "failed to read" in event for event in scan_events
    ), scan_events


def test_handle_idle_swallows_marker_error_in_agents_md(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Malformed managed markers in AGENTS.md must not crash IDLE.

    Operators can hand-edit AGENTS.md and accidentally break the
    managed-region markers. The IDLE handler must catch the resulting
    ``MarkerError`` from ``reconcile_agents_md``, surface a single
    explanatory event, and proceed with task selection so dispatch is
    not gated on a cosmetic markdown mistake.
    """
    h._patch_subprocess(monkeypatch)

    (tmp_path / "AGENTS.md").write_text(
        "<!-- pipeline-orchestrator: managed BEGIN section_a -->\n"
        "body\n"
        "<!-- pipeline-orchestrator: managed BEGIN section_b -->\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs", lambda repo, **kw: []
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    scan_events = [
        entry["event"]
        for entry in runner.state.history
        if entry["event"].startswith("[AGENTS-SCAN]")
    ]
    assert any(
        "malformed managed markers" in event for event in scan_events
    ), scan_events


def test_handle_idle_swallows_unicode_error_in_agents_md(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Non-UTF-8 bytes in AGENTS.md must not crash IDLE.

    ``Path.read_text`` decodes with the platform default, so an
    AGENTS.md (or task spec) authored on a non-UTF-8 host raises
    ``UnicodeDecodeError`` (a ``UnicodeError``). Without a guard this
    escapes ``reconcile_agents_md`` and aborts ``handle_idle``, which
    blocks task dispatch even though the periodic scan is intended to
    be non-blocking. Pin the catch and the warning shape so a future
    refactor cannot regress the IDLE cycle on a malformed encoding.
    """
    h._patch_subprocess(monkeypatch)

    (tmp_path / "AGENTS.md").write_bytes(b"\xff\xfe\xfd not utf-8\n")

    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs", lambda repo, **kw: []
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    scan_events = [
        entry["event"]
        for entry in runner.state.history
        if entry["event"].startswith("[AGENTS-SCAN]")
    ]
    assert any(
        "non-UTF-8" in event for event in scan_events
    ), scan_events


def test_handle_idle_suppresses_unchanged_agents_scan_output(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A repeated drift fingerprint must not emit fresh ``[AGENTS-SCAN]``
    events on every IDLE pass. ``log_event``'s consecutive-event dedup
    only collapses adjacent identical entries; other ``[INFRA]`` events
    interleave between scans so without a per-cycle fingerprint cache
    the same warnings would refill the 100-entry history cap and push
    out newer operational signals.
    """
    h._patch_subprocess(monkeypatch)

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-077.md").write_text(
        "# PR-077: Old spec\n\nSkip CI on this branch.\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs", lambda repo, **kw: []
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    asyncio.run(runner.handle_idle())
    first_pass_scan = [
        entry["event"]
        for entry in runner.state.history
        if entry["event"].startswith("[AGENTS-SCAN]")
    ]
    assert any(
        "PR-077.md" in event and "skip_ci" in event
        for event in first_pass_scan
    ), first_pass_scan

    asyncio.run(runner.handle_idle())
    second_pass_scan = [
        entry["event"]
        for entry in runner.state.history
        if entry["event"].startswith("[AGENTS-SCAN]")
    ]
    assert second_pass_scan == first_pass_scan, (
        "Identical drift on a second IDLE pass must not append new "
        "[AGENTS-SCAN] entries to history; cycle-to-cycle suppression "
        "is required to keep the 100-entry cap from rotating out fresh "
        "operational signals."
    )


def test_handle_idle_re_emits_agents_scan_when_drift_changes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """When the set of violations changes between cycles, the scan must
    re-emit so operators see the new state. Suppression keys on the full
    event fingerprint, so any change in violation type, file, or count
    surfaces immediately.
    """
    h._patch_subprocess(monkeypatch)

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    spec = tasks_dir / "PR-088.md"
    spec.write_text(
        "# PR-088: Old spec\n\nSkip CI on this branch.\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs", lambda repo, **kw: []
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    asyncio.run(runner.handle_idle())
    history_after_first = len([
        entry for entry in runner.state.history
        if entry["event"].startswith("[AGENTS-SCAN]")
    ])

    spec.write_text(
        "# PR-088: Old spec\n\nRun git commit --no-verify to skip hooks.\n",
        encoding="utf-8",
    )
    asyncio.run(runner.handle_idle())

    scan_events = [
        entry["event"]
        for entry in runner.state.history
        if entry["event"].startswith("[AGENTS-SCAN]")
    ]
    assert len(scan_events) > history_after_first
    assert any(
        "PR-088.md" in event and "no_verify_commit" in event
        for event in scan_events
    ), scan_events
