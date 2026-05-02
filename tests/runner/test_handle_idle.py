"""PR-224a: handle_idle handler tests for src/daemon/runner.py

Mechanical move from tests/test_runner.py. Helpers and fixtures still live in
tests/test_runner.py and are referenced via the ``h`` alias.
"""

from __future__ import annotations

import asyncio
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from src.coders import claude as claude_plugin_module
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

from tests import test_runner as h

claude_cli = claude_plugin_module.claude_cli


def test_handle_idle_no_tasks_leaves_state_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr(
        runner_module.github_client, "get_merged_prs", lambda repo, branch, refresh=False: []
    )

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
    checkout_idx = next(
        i for i, cmd in enumerate(commands) if cmd[:2] == ["git", "checkout"]
    )
    reset_idx = next(
        i for i, cmd in enumerate(commands) if cmd[:2] == ["git", "reset"]
    )
    assert fetch_idx < checkout_idx < reset_idx
    # No git pull anywhere: sync_to_main replaced it with reset --hard.
    assert not any(cmd[:2] == ["git", "pull"] for cmd in calls)
    # ``git reset --hard`` only removes tracked-file edits; untracked
    # files left by a crashed prior cycle would otherwise survive into
    # the next preflight as a dirty tree. ``git clean -fd`` after the
    # reset guarantees the working copy matches origin/{branch}.
    clean_idx = next(
        i for i, cmd in enumerate(commands) if cmd[:2] == ["git", "clean"]
    )
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
        path: str, model: str | None = None, timeout: int | None = None, **kwargs: object
    ) -> tuple[int, str, str]:
        claude_calls.append(path)
        return (0, "ok", "")

    monkeypatch.setattr(claude_cli, "run_planned_pr_async", fake_run_planned_pr)

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
        runner_module.github_client,
        "get_open_prs",
        _get_open_prs,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_merged_prs", lambda repo, branch, refresh=False: []
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
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
        "run_planned_pr_async",
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
        runner_module.github_client,
        "get_open_prs",
        _get_open_prs,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_merged_prs", lambda repo, branch, refresh=False: []
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_merged_prs", lambda repo, branch, refresh=False: []
    )
    monkeypatch.setattr(runner_module, "publish_repo_event", _fake_publish_repo_event)

    runner = h._make_runner()
    runner.handle_coding = _fake_handle_coding  # type: ignore[method-assign]

    asyncio.run(runner.handle_idle())
    asyncio.run(runner.publish_state())
    runner._set_queue_progress(1, 2)
    asyncio.run(runner.publish_state())

    assert published == [
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
    monkeypatch.setattr(idle_module, "get_merged_pr_ids", lambda *args, **kwargs: {"PR-001"})
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
    (tasks_dir / "PR-099.md").write_text(
        "Legacy fallback task body\n", encoding="utf-8"
    )

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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
    monkeypatch.setattr(idle_module, "get_merged_pr_ids", lambda *args, **kwargs: set())
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
        "# PR-001: Legacy task\n\n"
        "Branch: pr-001-legacy\n",
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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
        "# PR-001: Legacy task\n\n"
        "Branch: pr-001-legacy\n",
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

    monkeypatch.setattr(idle_module, "get_merged_pr_ids", lambda *args, **kwargs: set())
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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

    monkeypatch.setattr(idle_module, "get_merged_pr_ids", lambda *args, **kwargs: set())
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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

    monkeypatch.setattr(idle_module, "get_merged_pr_ids", lambda *args, **kwargs: set())
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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

    def fake_get_merged_pr_ids(repo_path: str, base_branch: str, candidate_pr_ids=None) -> set[str]:
        assert repo_path == str(tmp_path)
        assert base_branch == "main"
        assert set(candidate_pr_ids or ()) == {"PR-001", "PR-002"}
        return {"PR-001"}

    monkeypatch.setattr(idle_module, "get_merged_pr_ids", fake_get_merged_pr_ids)
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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

    monkeypatch.setattr(idle_module, "get_merged_pr_ids", lambda *args, **kwargs: set())
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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

    monkeypatch.setattr(idle_module, "get_merged_pr_ids", lambda *args, **kwargs: set())
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
        "Ignoring ghost legacy QUEUE.md entry PR-001" in entry.get("event", "")
        for entry in runner.state.history
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
        "# PR-002: Real legacy\n\n"
        "Some legacy body without structured headers.\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "get_merged_pr_ids", lambda *args, **kwargs: set())
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
        "Ignoring ghost legacy QUEUE.md entry PR-001" in entry.get("event", "")
        for entry in runner.state.history
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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [existing_pr],
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_merged_prs", lambda repo, branch, refresh=False: []
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_pr_metadata",
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

    monkeypatch.setattr(runner_module.github_client, "get_open_prs", _get_open_prs)
    monkeypatch.setattr(
        runner_module.github_client, "get_merged_prs", lambda repo, branch, refresh=False: []
    )
    monkeypatch.setattr(
        claude_cli,
        "run_planned_pr_async",
        h._async_cli_result(0, "ok", ""),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
    runner._auth_status_cache_expires_at = (
        datetime.now(timezone.utc) + timedelta(minutes=5)
    )
    runner.state.current_pr = PRInfo(
        number=1234,
        branch="some-other-branch",
        title="Unrelated manual PR from prior cycle",
    )
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.HUNG
    assert runner.state.error_message == (
        "Task PR-200 pinned to codex but coder unavailable"
    )
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

    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", _exploding_get_open_prs
    )

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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_merged_prs", lambda repo, branch, refresh=False: []
    )

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
        "Pending upload failed; skipping task dispatch to retry next cycle" in e["event"]
        for e in runner.state.history
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
    assert any(
        "sync_to_main after upload failed: resync broke" in e["event"]
        for e in runner.state.history
    )


def test_handle_idle_sets_error_when_queue_validation_fails_without_dag_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )
    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: (_ for _ in ()).throw(
            QueueValidationError(["tasks/QUEUE.md: malformed status"])
        ),
    )

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == (
        "Queue validation failed:\n"
        "  - tasks/QUEUE.md: malformed status"
    )
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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [reattached_pr],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_pr_metadata",
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
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo, **kw: [open_pr]
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_merged_prs", lambda repo, branch, refresh=False: []
    )

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
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo, **kw: []
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_merged_prs", lambda repo, branch, refresh=False: []
    )

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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [PRInfo(number=42, branch="fresh-branch")],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo, **kw: [open_pr]
    )

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
        runner_module.github_client,
        "get_open_prs",
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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
        lambda repo, branch, refresh=False: (_ for _ in ()).throw(RuntimeError("API down")),
    )
    derived_calls: list[list[PRInfo]] = []
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): (
            derived_calls.append(list(merged_prs)) or tasks
        ),
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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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

    assert not any(
        "merged PR check failed" in e["event"] for e in runner.state.history
    )
    assert not any(
        "merged-PR detection degraded" in e["event"]
        for e in runner.state.history
    )
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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
        idle_module._IDLE_MERGED_PR_304_WARN_AT - 1
    )
    asyncio.run(runner.handle_idle())

    assert any(
        "merged-PR detection degraded" in e["event"]
        for e in runner.state.history
    )
    assert not any(
        "merged PR check failed" in e["event"] for e in runner.state.history
    )
    assert runner._idle_merged_pr_304_streak == (
        idle_module._IDLE_MERGED_PR_304_WARN_AT
    )


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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
    runner._idle_merged_pr_304_streak = (
        idle_module._IDLE_MERGED_PR_304_WARN_EVERY - 1
    )
    asyncio.run(runner.handle_idle())

    assert runner._idle_merged_pr_304_streak == (
        idle_module._IDLE_MERGED_PR_304_WARN_EVERY
    )
    assert not any(
        "merged-PR detection degraded" in e["event"]
        for e in runner.state.history
    )


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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
        idle_module._IDLE_MERGED_PR_304_WARN_AT
        + idle_module._IDLE_MERGED_PR_304_WARN_EVERY
        - 1
    )
    asyncio.run(runner.handle_idle())

    assert runner._idle_merged_pr_304_streak == (
        idle_module._IDLE_MERGED_PR_304_WARN_AT
        + idle_module._IDLE_MERGED_PR_304_WARN_EVERY
    )
    assert any(
        "merged-PR detection degraded" in e["event"]
        for e in runner.state.history
    )


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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
        lambda repo, branch, refresh=False: (_ for _ in ()).throw(
            RuntimeError("API down")
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
    runner._idle_merged_pr_304_streak = 7
    asyncio.run(runner.handle_idle())

    assert runner._idle_merged_pr_304_streak == 0
    assert any(
        "merged PR check failed" in e["event"] for e in runner.state.history
    )


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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: (_ for _ in ()).throw(RuntimeError("API down")),
    )
    merged_calls = {"n": 0}

    def fail_get_merged_prs(repo, branch, refresh=False):
        merged_calls["n"] += 1
        raise AssertionError("get_merged_prs must not run when get_open_prs failed")

    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
        fail_get_merged_prs,
    )

    runner = h._make_runner()
    runner._idle_merged_pr_304_streak = 9
    asyncio.run(runner.handle_idle())

    assert merged_calls["n"] == 0
    assert runner._idle_merged_pr_304_streak == 0
    assert not any(
        "merged-PR detection degraded" in e["event"]
        for e in runner.state.history
    )


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
        raise AssertionError(
            "get_merged_prs must not run when pending queue sync is unresolved"
        )

    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
        fail_get_merged_prs,
    )

    runner = h._make_runner()
    runner.state.pending_queue_sync_branch = "queue-sync/pr-120"
    runner._resolve_pending_queue_sync = fake_resolve  # type: ignore[method-assign]
    runner._idle_merged_pr_304_streak = 9

    asyncio.run(runner.handle_idle())

    assert runner._idle_merged_pr_304_streak == 0
    assert not any(
        "merged-PR detection degraded" in e["event"]
        for e in runner.state.history
    )


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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
            idle_module.QueueValidationError(
                ["Queue validation failed:\n- malformed queue"]
            )
        ),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
    assert any(
        "Queue validation failed after DAG selection" in entry["event"]
        for entry in runner.state.history
    )


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
            idle_module.QueueValidationError(
                ["tasks/QUEUE.md: PR-123 does not match task file"]
            )
        ),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
    assert any(
        "Task status derivation failed after DAG selection" in entry["event"]
        for entry in runner.state.history
    )


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
            idle_module.QueueValidationError(
                ["tasks/QUEUE.md: PR-123 does not match task file"]
            )
        ),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
            idle_module.QueueValidationError(
                ["Queue validation failed:\n- malformed queue"]
            )
        ),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = h._make_runner()
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None
    assert runner.state.current_task is None
    assert runner.state.queue_done == 1
    assert runner.state.queue_total == 1
    assert any(
        "Queue validation failed after DAG selection" in entry["event"]
        for entry in runner.state.history
    )


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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: Legacy queue task\n", encoding="utf-8"
    )
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
        lambda path, **kw: (_ for _ in ()).throw(
            idle_module.QueueValidationError(["Queue validation failed"])
        ),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
        lambda path, **kw: (_ for _ in ()).throw(
            idle_module.QueueValidationError(["Queue validation failed"])
        ),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
        lambda path, **kw: (_ for _ in ()).throw(
            idle_module.QueueValidationError(["Queue validation failed"])
        ),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
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
        runner_module.github_client,
        "get_open_prs",
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
        runner_module.github_client,
        "get_merged_prs",
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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: (_ for _ in ()).throw(RuntimeError("API down")),
    )

    runner = h._make_runner()
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner._idle_dispatch_deferred is True
