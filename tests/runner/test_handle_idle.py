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
from src.daemon import main_commit_audit
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


def test_handle_idle_main_commit_audit_invoked_at_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])
    monkeypatch.setattr("src.github.prs.get_merged_prs", lambda repo, branch, refresh=False: [])
    list_calls: list[tuple[str, int, str]] = []

    def fake_list(owner_repo: str, lookback_n: int, branch: str) -> list[str]:
        list_calls.append((owner_repo, lookback_n, branch))
        return ["sha20"]

    monkeypatch.setattr(idle_module, "list_recent_main_commit_shas", fake_list)
    audit_calls: list[tuple[str, list[str], set[str], str]] = []

    def fake_audit(
        owner_repo: str,
        shas: list[str],
        audited_shas: set[str],
        branch: str,
    ):
        audit_calls.append((owner_repo, shas, audited_shas, branch))
        return [], shas

    monkeypatch.setattr(idle_module, "audit_main_commit_shas", fake_audit)

    runner = h._make_runner()
    for _ in range(19):
        asyncio.run(runner.handle_idle())

    assert audit_calls == []

    asyncio.run(runner.handle_idle())

    assert audit_calls == [("octo/demo", ["sha20"], set(), "main")]
    assert list_calls == [("octo/demo", 10, "main")]


def test_handle_idle_main_commit_audit_findings_logged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])
    monkeypatch.setattr("src.github.prs.get_merged_prs", lambda repo, branch, refresh=False: [])
    monkeypatch.setattr(
        idle_module,
        "list_recent_main_commit_shas",
        lambda repo, lookback_n, branch: ["abc1234"],
    )
    finding = main_commit_audit.MainCommitAuditFinding(
        sha="abc1234",
        short_sha="abc1234",
        message_first_line="direct hotfix",
        parent_count=1,
        pr_number=None,
        violation_category="direct_commit_no_pr",
        rule="revert",
    )
    monkeypatch.setattr(
        idle_module,
        "audit_main_commit_shas",
        lambda owner_repo, shas, audited_shas, branch: ([finding], shas),
    )

    runner = h._make_runner()
    for _ in range(20):
        asyncio.run(runner.handle_idle())

    assert any(
        entry["event"].startswith(
            "[AUDIT] [MAIN-COMMIT-AUDIT] VIOLATION direct_commit_no_pr: abc1234"
        )
        for entry in runner.state.history
    )


def test_handle_idle_main_commit_audit_failure_logged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])
    monkeypatch.setattr("src.github.prs.get_merged_prs", lambda repo, branch, refresh=False: [])

    attempts = 0

    def fail_list(owner_repo: str, lookback_n: int, branch: str) -> list[str]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("gh unavailable")
        return ["retry-sha"]

    monkeypatch.setattr(idle_module, "list_recent_main_commit_shas", fail_list)
    audit_calls: list[tuple[str, list[str], set[str], str]] = []

    def fake_audit(
        owner_repo: str,
        shas: list[str],
        audited_shas: set[str],
        branch: str,
    ):
        audit_calls.append((owner_repo, shas, audited_shas, branch))
        return [], shas

    monkeypatch.setattr(idle_module, "audit_main_commit_shas", fake_audit)

    runner = h._make_runner()
    for _ in range(20):
        asyncio.run(runner.handle_idle())

    assert any(
        entry["event"].startswith(
            "[AUDIT] [MAIN-COMMIT-AUDIT] Skipping audit for octo/demo"
        )
        for entry in runner.state.history
    )

    asyncio.run(runner.handle_idle())

    assert audit_calls == [("octo/demo", ["retry-sha"], set(), "main")]
    assert runner._main_commit_audit_counter == 0


def test_handle_idle_main_commit_audit_repeated_failures_return_to_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])
    monkeypatch.setattr("src.github.prs.get_merged_prs", lambda repo, branch, refresh=False: [])
    attempts = 0

    def fail_list(owner_repo: str, lookback_n: int, branch: str) -> list[str]:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("gh unavailable")

    monkeypatch.setattr(idle_module, "list_recent_main_commit_shas", fail_list)

    runner = h._make_runner()
    for _ in range(20):
        asyncio.run(runner.handle_idle())

    assert attempts == 1
    assert runner._main_commit_audit_counter == 19
    assert runner._main_commit_audit_retry_pending is True

    asyncio.run(runner.handle_idle())

    assert attempts == 2
    assert runner._main_commit_audit_counter == 0
    assert runner._main_commit_audit_retry_pending is False

    asyncio.run(runner.handle_idle())

    assert attempts == 2
    assert runner._main_commit_audit_counter == 1


def test_resolve_rate_limit_error_state_clears_rate_limit_message() -> None:
    runner = h._make_runner()
    runner.state.error_message = "rate limit exceeded for claude-3.5-sonnet"

    should_return = asyncio.run(
        runner._resolve_rate_limit_error_state(
            log_prefix="[RATE-LIMIT]",
            label="Rate limit expired, resuming",
        )
    )

    assert should_return is False
    assert runner.state.error_message is None
    assert any(
        "[RATE-LIMIT] cleared error_message "
        "(Rate limit expired, resuming, cleared legacy rate-limit error): "
        "rate limit exceeded for claude-3.5-sonnet" == entry["event"]
        for entry in runner.state.history
    )


def test_resolve_rate_limit_error_state_preserves_non_rate_limit_message() -> None:
    runner = h._make_runner()
    runner.state.error_message = "git push failed: branch protection"

    should_return = asyncio.run(
        runner._resolve_rate_limit_error_state(
            log_prefix="[RATE-LIMIT]",
            label="Rate limit expired, resuming",
        )
    )

    assert should_return is True
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "git push failed: branch protection"
    assert any(
        "[RATE-LIMIT] Rate limit expired, resuming -> ERROR "
        "(preserved context): git push failed: branch protection."
        == entry["event"]
        for entry in runner.state.history
    )


def test_resolve_rate_limit_error_state_429_pattern() -> None:
    runner = h._make_runner()
    runner.state.error_message = "HTTP 429 too many requests"

    should_return = asyncio.run(
        runner._resolve_rate_limit_error_state(
            log_prefix="[RATE-LIMIT]",
            label="Rate limit expired, resuming",
        )
    )

    assert should_return is False
    assert runner.state.error_message is None


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
    h._stub_dag_select(monkeypatch, task=task)

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


def test_select_next_task_clears_crashed_marker_when_frontmatter_is_todo(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.undo()
    task_dir = tmp_path / "tasks"
    task_dir.mkdir()
    (task_dir / "PR-001.md").write_text(
        "---\n"
        "status: TODO\n"
        "---\n\n"
        "# PR-001: Retry crashed task\n"
        "Branch: pr-001-retry\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        idle_module,
        "_resolve_merged_state",
        lambda *args, **kwargs: _merged_state(),
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = []
    runner._idle_merged_prs = []
    runner._crashed_task_pr_ids.add("PR-001")

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is not None
    assert task.pr_id == "PR-001"
    assert task.status == TaskStatus.TODO
    assert "PR-001" not in runner._crashed_task_pr_ids


def test_handle_idle_sets_queue_counters_with_mixed_statuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    tasks = [
        QueueTask(pr_id="PR-001", title="Done1", status=TaskStatus.DONE, branch="pr-001"),
        QueueTask(pr_id="PR-002", title="Done2", status=TaskStatus.DONE, branch="pr-002"),
        QueueTask(pr_id="PR-003", title="Todo", status=TaskStatus.TODO, branch="pr-003"),
    ]
    h._stub_dag_select(monkeypatch, task=tasks[2], tasks=tasks)
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


def test_idle_does_not_write_queue_md(
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
    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state())
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
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]

    asyncio.run(runner.handle_idle())

    assert not (tasks_dir / "QUEUE.md").exists()


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
    h._stub_dag_select(monkeypatch, task=task)

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
    h._stub_dag_select(monkeypatch, task=task)

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


def test_handle_idle_transitions_to_error_when_pinned_coder_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """PR-317: A task pinned to ``codex`` whose coder is unavailable parks
    the runner in ERROR with subsource=infra_failure.

    The terminal state migrated from IDLE to ERROR so the picker stops
    re-selecting the same pinned task on every cycle while the coder auth
    is broken — the prior ESCALATE→IDLE→re-pick loop is closed."""
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
    h._stub_dag_select(monkeypatch, task=task)
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

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.skip_ai_error_diagnose is True
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
    h._stub_dag_select(monkeypatch, task=task)

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
    task = QueueTask(
        pr_id="PR-123",
        title="Keep dispatching",
        branch="pr-123-keep-dispatching",
        status=TaskStatus.TODO,
    )
    h._stub_dag_select(monkeypatch, task=task)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: (_ for _ in ()).throw(RuntimeError("API down")),
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
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == task.pr_id
    assert runner.state.current_pr.number == 5
    assert coding_called["v"] is True
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
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: (_ for _ in ()).throw(RuntimeError("API down")),
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



def test_mark_task_done_in_snapshot_no_op_without_current_task() -> None:
    """No current task ⇒ no snapshot mutation, no error."""
    runner = h._make_runner()
    runner.state.current_queue = [
        QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)
    ]
    runner._mark_task_done_in_snapshot()
    assert runner.state.current_queue[0].status == TaskStatus.DOING


def test_mark_task_done_in_snapshot_no_op_when_snapshot_missing() -> None:
    """Empty/None snapshot ⇒ no-op."""
    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )
    runner.state.current_queue = None
    runner._mark_task_done_in_snapshot()
    assert runner.state.current_queue is None


def test_mark_task_done_in_snapshot_flips_matching_task_to_done() -> None:
    """Matching pr_id flips to DONE; other tasks left untouched."""
    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )
    runner.state.current_queue = [
        QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING),
        QueueTask(pr_id="PR-002", title="other", status=TaskStatus.TODO),
    ]
    runner._mark_task_done_in_snapshot()
    statuses = {q.pr_id: q.status for q in runner.state.current_queue}
    assert statuses == {"PR-001": TaskStatus.DONE, "PR-002": TaskStatus.TODO}


def test_mark_task_done_in_snapshot_skips_when_task_already_done() -> None:
    """If the task is already DONE, do not rewrite it (idempotent)."""
    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DONE
    )
    original = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DONE)
    runner.state.current_queue = [original]
    runner._mark_task_done_in_snapshot()
    assert runner.state.current_queue[0] is original


def test_mark_task_done_in_snapshot_refreshes_snapshot_timestamp() -> None:
    """Mutation re-stamps current_queue_snapshot_at via __setattr__ hook.

    Regression: in-place ``snapshot[index] = ...`` bypasses the hook,
    leaving ``current_queue_snapshot_at`` pinned to the pre-mutation
    time even though queue contents changed. Dashboard consumers that
    treat ``snapshot_at`` as a change token would miss the update.
    """
    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )
    runner.state.current_queue = [
        QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING),
    ]
    stale_stamp = datetime.now(timezone.utc) - timedelta(hours=1)
    runner.state.current_queue_snapshot_at = stale_stamp

    runner._mark_task_done_in_snapshot()

    refreshed = runner.state.current_queue_snapshot_at
    assert refreshed is not None
    assert refreshed > stale_stamp


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
    pre-existed with ``AGENTS.md`` + ``tasks/`` + ``scripts/ci.sh``
    but no ``scripts/make-review-artifacts.sh``
    (or missing scaffold-owned entries in ``.gitignore``) must NOT be
    classified as scaffolded: the daemon would otherwise skip
    scaffold_repo permanently, leaving those files uncreated, and
    the first ``make-review-artifacts.sh`` run would dirty the
    working tree until ``preflight`` forces ERROR.
    """
    base = tmp_path / "partial"
    base.mkdir()
    (base / "AGENTS.md").write_text("# AGENTS\n")
    (base / "tasks").mkdir()
    (base / "scripts").mkdir()
    (base / "scripts" / "ci.sh").write_text("#!/usr/bin/env bash\n")
    # Missing: scripts/make-review-artifacts.sh and .gitignore.
    assert runner_module._repo_looks_scaffolded(str(base)) is False

    # Add the missing review-artifacts script — still missing .gitignore.
    (base / "scripts" / "make-review-artifacts.sh").write_text("#!/usr/bin/env bash\n")
    assert runner_module._repo_looks_scaffolded(str(base)) is False

    # Add a .gitignore that does NOT mention the scaffold-owned entries.
    (base / ".gitignore").write_text("node_modules/\n*.pyc\n")
    assert runner_module._repo_looks_scaffolded(str(base)) is False

    # Only artifacts/ is not enough: generated QUEUE.md must also stay ignored.
    (base / ".gitignore").write_text("node_modules/\n*.pyc\nartifacts/\n")
    assert runner_module._repo_looks_scaffolded(str(base)) is False

    # Finally append tasks/QUEUE.md — now fully scaffolded.
    (base / ".gitignore").write_text(
        "node_modules/\n*.pyc\nartifacts/\ntasks/QUEUE.md\n"
    )
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


def test_select_next_task_from_dag_returns_none_when_no_headers_present(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """``tasks/`` exists but every PR-*.md fails to parse as structured."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-legacy.md").write_text(
        "Just some legacy text without a header.\n",
        encoding="utf-8",
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is None


def test_picker_does_not_pick_task_with_unresolved_deps(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Missing-file deps stay visible in the snapshot but are not picked."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: Blocked\n\n"
        "Branch: pr-001\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: PR-MISSING\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state())

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = []
    runner._idle_merged_prs = []

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is None
    assert runner._idle_dag_tasks == [
        QueueTask(
            pr_id="PR-001",
            title="Blocked",
            status=TaskStatus.TODO,
            task_file="tasks/PR-001.md",
            depends_on=["PR-MISSING"],
            unresolved_deps=["PR-MISSING"],
            branch="pr-001",
        )
    ]


def test_picker_keeps_unresolved_cycle_idle(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Unresolved blocked tasks stay visible without triggering cycle errors."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: First blocked\n\n"
        "Branch: pr-001\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: PR-002\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-002.md").write_text(
        "# PR-002: Second blocked\n\n"
        "Branch: pr-002\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: PR-001, PR-MISSING\n"
        "- Priority: 2\n"
        "- Coder: any\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state())

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = []
    runner._idle_merged_prs = []

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is None
    assert runner._idle_dag_tasks == [
        QueueTask(
            pr_id="PR-001",
            title="First blocked",
            status=TaskStatus.TODO,
            task_file="tasks/PR-001.md",
            depends_on=["PR-002"],
            unresolved_deps=["PR-MISSING"],
            branch="pr-001",
        ),
        QueueTask(
            pr_id="PR-002",
            title="Second blocked",
            status=TaskStatus.TODO,
            task_file="tasks/PR-002.md",
            depends_on=["PR-001", "PR-MISSING"],
            unresolved_deps=["PR-MISSING"],
            branch="pr-002",
        ),
    ]


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

    retained, unresolved_deps_map = (
        idle_module.IdleMixin._filter_dag_headers_with_available_dependencies(
            headers,
            {"PR-LEGACY"},
            tmp_path,
            set(),
        )
    )

    assert retained == headers
    assert unresolved_deps_map == {
        "PR-001": ["PR-LEGACY"],
        "PR-002": ["PR-LEGACY"],
    }


def test_filter_retains_headers_with_missing_deps(
    tmp_path: Path,
) -> None:
    headers = [
        TaskHeader(
            pr_id="PR-002",
            title="Depends on missing",
            branch="pr-002",
            task_type="feature",
            complexity="low",
            depends_on=["PR-MISSING"],
            priority=1,
            coder="any",
        ),
    ]

    retained, unresolved_deps_map = (
        idle_module.IdleMixin._filter_dag_headers_with_available_dependencies(
            headers,
            set(),
            tmp_path,
            merged_pr_ids=set(),
        )
    )

    assert retained == headers
    assert unresolved_deps_map == {"PR-002": ["PR-MISSING"]}


def test_filter_dag_headers_keeps_task_when_dependency_is_merged(
    tmp_path: Path,
) -> None:
    """A merged dependency satisfies the requirement even if the spec is
    no longer present in the structured set."""
    headers = [
        TaskHeader(
            pr_id="PR-002",
            title="Depends on merged",
            branch="pr-002",
            task_type="feature",
            complexity="low",
            depends_on=["PR-MERGED"],
            priority=1,
            coder="any",
        ),
    ]

    retained, unresolved_deps_map = (
        idle_module.IdleMixin._filter_dag_headers_with_available_dependencies(
            headers,
            set(),
            tmp_path,
            merged_pr_ids={"PR-MERGED"},
        )
    )

    assert [h.pr_id for h in retained] == ["PR-002"]
    assert unresolved_deps_map == {}


def test_filter_dag_headers_keeps_task_when_dependency_file_missing(
    tmp_path: Path,
) -> None:
    """A dependency that is neither structured, merged, nor a known
    legacy file is surfaced as unresolved metadata."""
    headers = [
        TaskHeader(
            pr_id="PR-002",
            title="Depends on missing",
            branch="pr-002",
            task_type="feature",
            complexity="low",
            depends_on=["PR-MISSING"],
            priority=1,
            coder="any",
        ),
    ]

    retained, unresolved_deps_map = (
        idle_module.IdleMixin._filter_dag_headers_with_available_dependencies(
            headers,
            set(),
            tmp_path,
            merged_pr_ids=set(),
        )
    )

    assert retained == headers
    assert unresolved_deps_map == {"PR-002": ["PR-MISSING"]}


def test_filter_dag_headers_keeps_task_when_dependency_file_exists(
    tmp_path: Path,
) -> None:
    (tmp_path / "PR-001.md").write_text("legacy content\n", encoding="utf-8")
    headers = [
        TaskHeader(
            pr_id="PR-002",
            title="Depends on existing legacy file",
            branch="pr-002",
            task_type="feature",
            complexity="low",
            depends_on=["PR-001"],
            priority=1,
            coder="any",
        ),
    ]

    retained, unresolved_deps_map = (
        idle_module.IdleMixin._filter_dag_headers_with_available_dependencies(
            headers,
            set(),
            tmp_path,
            merged_pr_ids=set(),
        )
    )

    assert retained == headers
    assert unresolved_deps_map == {}


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


def test_idle_cycle_does_not_emit_agents_scan_events(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs", lambda repo, **kw: []
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-009.md").write_text(
        "# PR-009: Old spec\n\nSkip CI on this branch.\n",
        encoding="utf-8",
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


def test_idle_cycle_does_not_call_scan_method(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs", lambda repo, **kw: []
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = h._make_runner()
    calls = 0

    def fake_scan() -> None:
        nonlocal calls
        calls += 1

    monkeypatch.setattr(runner, "_scan_task_specs_for_agents_md_drift", fake_scan)

    asyncio.run(runner.handle_idle())

    assert calls == 0


def test_agents_scan_method_emits_events_when_specs_drift(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The scan method buffers reconcile events through the runner log."""
    h._patch_subprocess(monkeypatch)

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-007.md").write_text(
        "# PR-007: Old spec\n\nSkip CI on this branch.\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "src.github.prs.get_open_prs", lambda repo, **kw: []
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._scan_task_specs_for_agents_md_drift()

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


def test_agents_scan_method_clean_tasks_dir_emits_no_events(
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

    monkeypatch.setattr(
        "src.github.prs.get_open_prs", lambda repo, **kw: []
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._scan_task_specs_for_agents_md_drift()

    scan_events = [
        entry["event"]
        for entry in runner.state.history
        if entry["event"].startswith("[AGENTS-SCAN]")
    ]
    assert scan_events == []


def test_agents_scan_method_swallows_os_error_in_agents_md_read(
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

    monkeypatch.setattr(
        "src.github.prs.get_open_prs", lambda repo, **kw: []
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._scan_task_specs_for_agents_md_drift()

    assert runner.state.state == PipelineState.IDLE
    scan_events = [
        entry["event"]
        for entry in runner.state.history
        if entry["event"].startswith("[AGENTS-SCAN]")
    ]
    assert any(
        "failed to read" in event for event in scan_events
    ), scan_events


def test_agents_scan_method_swallows_marker_error_in_agents_md(
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

    monkeypatch.setattr(
        "src.github.prs.get_open_prs", lambda repo, **kw: []
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._scan_task_specs_for_agents_md_drift()

    assert runner.state.state == PipelineState.IDLE
    scan_events = [
        entry["event"]
        for entry in runner.state.history
        if entry["event"].startswith("[AGENTS-SCAN]")
    ]
    assert any(
        "malformed managed markers" in event for event in scan_events
    ), scan_events


def test_agents_scan_method_swallows_unicode_error_in_agents_md(
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

    monkeypatch.setattr(
        "src.github.prs.get_open_prs", lambda repo, **kw: []
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._scan_task_specs_for_agents_md_drift()

    assert runner.state.state == PipelineState.IDLE
    scan_events = [
        entry["event"]
        for entry in runner.state.history
        if entry["event"].startswith("[AGENTS-SCAN]")
    ]
    assert any(
        "non-UTF-8" in event for event in scan_events
    ), scan_events


def test_agents_scan_method_suppresses_unchanged_output(
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

    monkeypatch.setattr(
        "src.github.prs.get_open_prs", lambda repo, **kw: []
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    runner._scan_task_specs_for_agents_md_drift()
    first_pass_scan = [
        entry["event"]
        for entry in runner.state.history
        if entry["event"].startswith("[AGENTS-SCAN]")
    ]
    assert any(
        "PR-077.md" in event and "skip_ci" in event
        for event in first_pass_scan
    ), first_pass_scan

    runner._scan_task_specs_for_agents_md_drift()
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


def test_agents_scan_method_re_emits_when_drift_changes(
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

    monkeypatch.setattr(
        "src.github.prs.get_open_prs", lambda repo, **kw: []
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    runner._scan_task_specs_for_agents_md_drift()
    history_after_first = len([
        entry for entry in runner.state.history
        if entry["event"].startswith("[AGENTS-SCAN]")
    ])

    spec.write_text(
        "# PR-088: Old spec\n\nRun git commit --no-verify to skip hooks.\n",
        encoding="utf-8",
    )
    runner._scan_task_specs_for_agents_md_drift()

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
