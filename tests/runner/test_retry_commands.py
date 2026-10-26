"""Daemon contract tests for durable operator Retry commands."""

from __future__ import annotations

import asyncio
import subprocess
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from src.cancellation import (
    CancellationCause,
    cause_key,
    current_run_started_at_key,
    task_spec_content_hash,
)
from src.daemon import retry_commands as retry_module
from src.daemon.retry_commands import RetryDispatch
from src.inhibitor import InhibitorType, WorkInhibitor
from src.keyspace import control_stop, pipeline_state, retry_command, retry_command_pending
from src.models import (
    CIStatus,
    PipelineState,
    PRInfo,
    QueueTask,
    RepoState,
    ReviewStatus,
    TaskStatus,
)
from src.publication_migration import confirmable as migration_confirmable
from src.publication_migration import enqueue as enqueue_migration
from src.publication_migration import migration_binding
from src.retry_commands import (
    RetryCommandStatus,
    RetryEffectStage,
    RetryExecutionState,
    enqueue_retry_command,
    load_retry_command,
    new_retry_command,
    retry_failure_identity,
)
from src.subsource_registry import SuppressionReason
from src.suppression import SuppressionRecord
from src.usage import UsageSnapshot

from tests.runner import _helpers as h

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _task_text(*, task_id: str = "PR-42", branch: str = "fix/pr-42") -> str:
    return (
        "---\n"
        "status: ERROR\n"
        "blocked_reason: daemon\n"
        "---\n\n"
        f"# {task_id}: Retry daemon failure\n\n"
        f"Branch: {branch}\n"
        "- Type: bugfix\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 2\n"
        "- Coder: codex\n\n"
        "## Problem\n"
        "Retry the failed task.\n"
    )


def _command(
    text: str,
    *,
    task_id: str = "PR-42",
    branch: str = "fix/pr-42",
    bound_pr: PRInfo | None = None,
):
    task = QueueTask(pr_id=task_id, title="Retry", status=TaskStatus.ERROR)
    state = RepoState(name="octo__demo", url="https://github.com/octo/demo",
                      current_task=task, current_pr=bound_pr, error_message="prior failure")
    return new_retry_command(
        repo_slug="octo__demo",
        task_id=task_id,
        task_file=f"tasks/{task_id}.md",
        task_branch=branch,
        task_fingerprint=task_spec_content_hash(text),
        request_binding="b" * 64,
        failure_id=retry_failure_identity(task, state, None),
        retry_cap=3,
        bound_pr_number=bound_pr.number if bound_pr else None,
        bound_pr_branch=bound_pr.branch if bound_pr else None,
        bound_pr_head_sha=bound_pr.head_sha if bound_pr else None,
        now=NOW,
    )


async def _prepared_runner(
    tmp_path: Path,
    *,
    command: Any | None = None,
    text: str | None = None,
):
    runner = h._make_runner()
    repo = tmp_path / "repo"
    tasks = repo / "tasks"
    tasks.mkdir(parents=True)
    text = text or _task_text()
    (tasks / "PR-42.md").write_text(text, encoding="utf-8")
    runner.repo_path = str(repo)
    runner.state.state = PipelineState.ERROR
    task = QueueTask(
        pr_id="PR-42",
        title="Retry daemon failure",
        status=TaskStatus.ERROR,
        task_file="tasks/PR-42.md",
        branch="fix/pr-42",
        priority=2,
    )
    runner.state.current_task = task
    runner.state.current_queue = [task]
    runner.state.error_message = "prior failure"
    command = command or _command(text)
    if command.bound_pr_number is not None:
        runner.state.current_pr = PRInfo(
            number=command.bound_pr_number, branch=command.bound_pr_branch,
            pr_id=command.task_id, head_sha=command.bound_pr_head_sha,
        )
    await enqueue_retry_command(runner.redis, command)
    return runner, command, task, repo


def _allow_validation(monkeypatch: pytest.MonkeyPatch, runner: Any, text: str) -> None:
    monkeypatch.setattr(runner, "_retry_worktree_dirty", lambda: (False, ""))
    monkeypatch.setattr(runner, "_origin_retry_task_text", lambda command: text)


async def _none_record(task_id: str) -> None:
    return None


def _async_value(value: Any):
    async def _return(*args: Any, **kwargs: Any):
        return value

    return _return


def _completed(stdout: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(["git"], 0, stdout, "")


@pytest.mark.asyncio
async def test_consumer_uses_durable_queue_without_pubsub(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, command, _task, _repo = await _prepared_runner(tmp_path)
    seen: list[str] = []

    async def apply(claimed):
        seen.append(claimed.command_id)
        return RetryDispatch.CODING

    monkeypatch.setattr(runner, "_apply_retry_command", apply)
    assert await runner._consume_retry_command() == RetryDispatch.CODING
    assert seen == [command.command_id]
    claimed = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert claimed is not None
    assert claimed.status == RetryCommandStatus.PROCESSING


@pytest.mark.asyncio
async def test_consumer_handles_store_claim_and_empty_queue_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, command, _task, _repo = await _prepared_runner(tmp_path)

    async def list_boom(*args):
        raise RuntimeError("down")

    monkeypatch.setattr(retry_module, "list_pending_retry_commands", list_boom)
    assert await runner._consume_retry_command() == RetryDispatch.NONE
    monkeypatch.setattr(
        retry_module, "list_pending_retry_commands", _async_value([])
    )
    assert await runner._consume_retry_command() == RetryDispatch.NONE
    assert runner._active_retry_command_id is None

    async def list_one(*args):
        return [command]

    async def claim_boom(*args):
        raise RuntimeError("down")

    monkeypatch.setattr(retry_module, "list_pending_retry_commands", list_one)
    monkeypatch.setattr(retry_module, "claim_retry_command", claim_boom)
    assert await runner._consume_retry_command() == RetryDispatch.HANDLED
    monkeypatch.setattr(retry_module, "claim_retry_command", _async_value(None))
    assert await runner._consume_retry_command() == RetryDispatch.HANDLED


@pytest.mark.asyncio
async def test_consumer_reconciles_applied_records_before_and_after_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, command, _task, _repo = await _prepared_runner(tmp_path)
    command.status = RetryCommandStatus.APPLIED
    called: list[str] = []

    async def reconcile(current):
        called.append(current.command_id)
        return RetryDispatch.WATCH

    monkeypatch.setattr(runner, "_reconcile_applied_retry", reconcile)
    monkeypatch.setattr(
        retry_module, "list_pending_retry_commands", _async_value([command])
    )
    assert await runner._consume_retry_command() == RetryDispatch.WATCH

    queued = command.model_copy(update={"status": RetryCommandStatus.QUEUED})
    monkeypatch.setattr(
        retry_module, "list_pending_retry_commands", _async_value([queued])
    )
    monkeypatch.setattr(
        retry_module, "claim_retry_command", _async_value(command)
    )
    assert await runner._consume_retry_command() == RetryDispatch.WATCH
    assert called == [command.command_id, command.command_id]


@pytest.mark.asyncio
async def test_defer_and_fail_are_operator_visible_and_pending_aware(
    tmp_path: Path,
) -> None:
    runner, command, _task, _repo = await _prepared_runner(tmp_path)
    assert (
        await runner._defer_retry_command(command, "manual pause active")
        == RetryDispatch.HANDLED
    )
    deferred = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert deferred is not None
    assert deferred.status == RetryCommandStatus.DEFERRED
    assert deferred.outcome_reason == "manual pause active"
    assert command.command_id in runner.redis.zsets[retry_command_pending(runner.name)]

    assert (
        await runner._fail_retry_command(deferred, "task changed")
        == RetryDispatch.HANDLED
    )
    failed = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert failed is not None
    assert failed.status == RetryCommandStatus.FAILED
    assert failed.execution_state == RetryExecutionState.FAILED
    assert command.command_id not in runner.redis.zsets[retry_command_pending(runner.name)]


def test_retry_task_path_rejects_escape_symlink_and_missing(
    tmp_path: Path,
) -> None:
    runner = h._make_runner()
    repo = tmp_path / "repo"
    tasks = repo / "tasks"
    tasks.mkdir(parents=True)
    good = tasks / "PR-42.md"
    good.write_text(_task_text(), encoding="utf-8")
    runner.repo_path = str(repo)
    command = _command(good.read_text(encoding="utf-8"))
    assert runner._retry_task_path(command) == good.resolve()

    command.task_file = "../outside.md"
    assert runner._retry_task_path(command) is None
    command.task_file = str(good.resolve())
    assert runner._retry_task_path(command) is None
    command.task_file = "tasks/missing.md"
    assert runner._retry_task_path(command) is None
    link = tasks / "link.md"
    link.symlink_to(good)
    command.task_file = "tasks/link.md"
    assert runner._retry_task_path(command) is None


def test_retry_task_path_handles_resolution_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    command = _command(_task_text())
    original = Path.resolve
    calls = {"count": 0}

    def fail_candidate(path: Path, *args: Any, **kwargs: Any):
        calls["count"] += 1
        if calls["count"] == 1:
            return original(path, *args, **kwargs)
        raise OSError("cannot resolve")

    monkeypatch.setattr(Path, "resolve", fail_candidate)
    assert runner._retry_task_path(command) is None


def test_worktree_and_origin_git_checks_preserve_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    command = _command(_task_text())
    calls: list[tuple[str, ...]] = []

    def git(repo: str, *args: str, **kwargs: Any):
        calls.append(args)
        if args[0] == "status":
            return _completed(" M work.py\n")
        return _completed(_task_text())

    monkeypatch.setattr(retry_module.git_ops, "_git", git)
    assert runner._retry_worktree_dirty() == (True, "M work.py")
    assert runner._origin_retry_task_text(command) == _task_text()
    assert calls[-1][1] == "origin/main:tasks/PR-42.md"

    monkeypatch.setattr(
        retry_module.git_ops,
        "_git",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("git failed")),
    )
    assert runner._retry_worktree_dirty() == (None, "git failed")
    assert runner._origin_retry_task_text(command) is None


@pytest.mark.asyncio
async def test_validation_accepts_exact_canonical_task_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    text = _task_text()
    runner, command, task, _repo = await _prepared_runner(tmp_path, text=text)
    _allow_validation(monkeypatch, runner, text)
    validated = await runner._validate_retry_command(command)
    assert not isinstance(validated, RetryDispatch)
    header, rebuilt = validated
    assert header.pr_id == task.pr_id
    assert rebuilt.task_file == task.task_file
    assert rebuilt.status == TaskStatus.ERROR


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mutation", "status", "phrase"),
    [
        ("repo", RetryCommandStatus.FAILED, "repository binding"),
        ("inactive", RetryCommandStatus.DEFERRED, "disabled"),
        ("sync", RetryCommandStatus.DEFERRED, "Queue synchronization"),
        ("other", RetryCommandStatus.DEFERRED, "Another task"),
        ("inspect", RetryCommandStatus.DEFERRED, "Could not inspect"),
        ("dirty", RetryCommandStatus.DEFERRED, "dirty"),
        ("missing", RetryCommandStatus.FAILED, "missing or unsafe"),
        ("parse", RetryCommandStatus.FAILED, "cannot be parsed"),
        ("identity", RetryCommandStatus.FAILED, "identity or branch"),
        ("fingerprint", RetryCommandStatus.FAILED, "fingerprint changed"),
        ("origin_missing", RetryCommandStatus.DEFERRED, "Cannot verify"),
        ("origin_changed", RetryCommandStatus.FAILED, "Base-branch"),
    ],
)
async def test_validation_reports_binding_and_safe_point_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    status: RetryCommandStatus,
    phrase: str,
) -> None:
    text = _task_text()
    runner, command, _task, repo = await _prepared_runner(tmp_path, text=text)
    _allow_validation(monkeypatch, runner, text)
    if mutation == "repo":
        command.repo_slug = "other"
    elif mutation == "inactive":
        runner.repo_config = runner.repo_config.model_copy(update={"active": False})
    elif mutation == "sync":
        runner.state.pending_queue_sync_branch = "queue-sync"
    elif mutation == "other":
        runner.state.current_task = QueueTask(
            pr_id="PR-99", title="other", status=TaskStatus.DOING
        )
        runner.state.state = PipelineState.CODING
    elif mutation == "inspect":
        monkeypatch.setattr(runner, "_retry_worktree_dirty", lambda: (None, "boom"))
    elif mutation == "dirty":
        monkeypatch.setattr(
            runner, "_retry_worktree_dirty", lambda: (True, " M work.py")
        )
    elif mutation == "missing":
        command.task_file = "tasks/missing.md"
    elif mutation == "parse":
        (repo / command.task_file).write_text("broken", encoding="utf-8")
    elif mutation == "identity":
        command.task_branch = "fix/other"
    elif mutation == "fingerprint":
        command.task_fingerprint = "0" * 64
    elif mutation == "origin_missing":
        monkeypatch.setattr(runner, "_origin_retry_task_text", lambda command: None)
    elif mutation == "origin_changed":
        monkeypatch.setattr(
            runner,
            "_origin_retry_task_text",
            lambda command: _task_text(branch="fix/changed"),
        )
    result = await runner._validate_retry_command(command)
    expected_dispatch = (
        RetryDispatch.ACTIVE_TASK_DEFERRED
        if mutation == "other"
        else RetryDispatch.HANDLED
    )
    assert result == expected_dispatch
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored is not None
    assert stored.status == status
    assert phrase.lower() in stored.outcome_reason.lower()


@pytest.mark.asyncio
async def test_blocker_respects_suppression_and_all_active_inhibitors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, command, _task, _repo = await _prepared_runner(tmp_path)
    record = SuppressionRecord(
        task_id=command.task_id,
        reason=SuppressionReason.GUARDRAIL,
        created_at=NOW,
        detail={"task_spec_hash": command.task_fingerprint},
    )

    async def suppression(task_id: str):
        return record

    monkeypatch.setattr(runner, "_suppression_record_for_task", suppression)
    assert await runner._retry_blocker(command) == RetryDispatch.HANDLED
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored is not None and "revised task" in stored.outcome_reason

    async def no_suppression(task_id: str):
        return None

    monkeypatch.setattr(runner, "_suppression_record_for_task", no_suppression)
    slowdown = WorkInhibitor(
        inhibitor_type=InhibitorType.GITHUB_BUDGET_SLOWDOWN,
        reason_text="slow",
        source_key="budget",
    )
    pause = WorkInhibitor(
        inhibitor_type=InhibitorType.USER_PAUSE,
        reason_text="operator paused",
        source_key="state",
    )
    monkeypatch.setattr(
        retry_module,
        "derive_active_inhibitors",
        _async_value([slowdown, pause]),
    )
    assert await runner._retry_blocker(command) == RetryDispatch.HANDLED
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored is not None
    assert "user_pause" in stored.outcome_reason
    assert "slowdown" not in stored.outcome_reason

    monkeypatch.setattr(
        retry_module, "derive_active_inhibitors", _async_value([slowdown])
    )
    assert await runner._retry_blocker(command) is None


@pytest.mark.asyncio
async def test_blocker_defers_when_suppression_or_inhibitor_reads_fail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, command, _task, _repo = await _prepared_runner(tmp_path)

    async def suppression_boom(task_id: str):
        raise RuntimeError("suppression down")

    monkeypatch.setattr(runner, "_suppression_record_for_task", suppression_boom)
    assert await runner._retry_blocker(command) == RetryDispatch.HANDLED

    monkeypatch.setattr(runner, "_suppression_record_for_task", _none_record)

    async def inhibitor_boom(*args):
        raise RuntimeError("inhibitor down")

    monkeypatch.setattr(retry_module, "derive_active_inhibitors", inhibitor_boom)
    assert await runner._retry_blocker(command) == RetryDispatch.HANDLED


def test_matching_prs_requires_branch_and_compatible_task_id() -> None:
    command = _command(_task_text())
    matching = PRInfo(number=1, branch="fix/pr-42", pr_id="PR-42")
    title_only = PRInfo(number=2, branch="fix/pr-42", pr_id=None)
    wrong_task = PRInfo(number=3, branch="fix/pr-42", pr_id="PR-99")
    wrong_branch = PRInfo(number=4, branch="fix/other", pr_id="PR-42")
    assert retry_module.RetryCommandMixin._matching_prs(
        command, [matching, title_only, wrong_task, wrong_branch]
    ) == [matching, title_only]


@pytest.mark.asyncio
async def test_bound_open_pr_is_preserved_and_checked_for_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bound = PRInfo(
        number=42,
        branch="fix/pr-42",
        pr_id="PR-42",
        head_sha="abc123",
    )
    runner, command, _task, _repo = await _prepared_runner(
        tmp_path, command=_command(_task_text(), bound_pr=bound)
    )
    monkeypatch.setattr(retry_module.gh_prs, "get_open_prs", lambda *args: [bound])
    assert await runner._select_retry_continuation(command) == ("watch", bound)

    wrong = bound.model_copy(update={"branch": "fix/other"})
    monkeypatch.setattr(retry_module.gh_prs, "get_open_prs", lambda *args: [wrong])
    assert await runner._select_retry_continuation(command) == RetryDispatch.HANDLED
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored is not None and stored.status == RetryCommandStatus.FAILED

    command.status = RetryCommandStatus.QUEUED
    runner.redis.store[retry_command(runner.name, command.command_id)] = (
        command.model_dump_json()
    )
    changed_head = bound.model_copy(update={"head_sha": "changed"})
    monkeypatch.setattr(
        retry_module.gh_prs, "get_open_prs", lambda *args: [changed_head]
    )
    assert await runner._select_retry_continuation(command) == RetryDispatch.HANDLED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("pr_state", "expected_status", "phrase"),
    [
        (None, RetryCommandStatus.DEFERRED, "unavailable"),
        ("MERGED", RetryCommandStatus.FAILED, "already merged"),
        ("CLOSED", RetryCommandStatus.DEFERRED, "closed without merge"),
        ("OPEN", RetryCommandStatus.DEFERRED, "unexpected state"),
    ],
)
async def test_bound_non_open_pr_state_is_explicit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    pr_state: str | None,
    expected_status: RetryCommandStatus,
    phrase: str,
) -> None:
    bound = PRInfo(number=42, branch="fix/pr-42", head_sha="abc123")
    runner, command, _task, _repo = await _prepared_runner(
        tmp_path, command=_command(_task_text(), bound_pr=bound)
    )
    monkeypatch.setattr(retry_module.gh_prs, "get_open_prs", lambda *args: [])
    monkeypatch.setattr(
        retry_module.gh_prs, "get_pr_state", lambda *args: pr_state
    )
    assert await runner._select_retry_continuation(command) == RetryDispatch.HANDLED
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored is not None
    assert stored.status == expected_status
    assert phrase in stored.outcome_reason


@pytest.mark.asyncio
async def test_unbound_pr_reconciliation_handles_ambiguous_merged_and_new_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, command, _task, _repo = await _prepared_runner(tmp_path)
    first = PRInfo(number=41, branch="fix/pr-42", pr_id="PR-42")
    second = PRInfo(number=42, branch="fix/pr-42")
    monkeypatch.setattr(
        retry_module.gh_prs, "get_open_prs", lambda *args: [first, second]
    )
    assert await runner._select_retry_continuation(command) == RetryDispatch.HANDLED

    monkeypatch.setattr(retry_module.gh_prs, "get_open_prs", lambda *args: [first])
    assert await runner._select_retry_continuation(command) == ("watch", first)

    monkeypatch.setattr(retry_module.gh_prs, "get_open_prs", lambda *args: [])
    monkeypatch.setattr(retry_module.gh_prs, "get_merged_prs", lambda *args, **kw: [first])
    assert await runner._select_retry_continuation(command) == RetryDispatch.HANDLED

    command.status = RetryCommandStatus.QUEUED
    runner.redis.store[retry_command(runner.name, command.command_id)] = (
        command.model_dump_json()
    )
    monkeypatch.setattr(retry_module.gh_prs, "get_merged_prs", lambda *args, **kw: [])
    assert await runner._select_retry_continuation(command) == ("coding", None)


@pytest.mark.asyncio
async def test_pr_reconciliation_defers_github_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, command, _task, _repo = await _prepared_runner(tmp_path)
    monkeypatch.setattr(
        retry_module.gh_prs,
        "get_open_prs",
        lambda *args: (_ for _ in ()).throw(RuntimeError("github down")),
    )
    assert await runner._select_retry_continuation(command) == RetryDispatch.HANDLED

    monkeypatch.setattr(retry_module.gh_prs, "get_open_prs", lambda *args: [])
    monkeypatch.setattr(
        retry_module.gh_prs,
        "get_merged_prs",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("github down")),
    )
    assert await runner._select_retry_continuation(command) == RetryDispatch.HANDLED


@pytest.mark.asyncio
async def test_coder_availability_and_usage_gates_defer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, command, task, _repo = await _prepared_runner(tmp_path)
    header = SimpleNamespace(coder="codex")

    async def refresh() -> None:
        return None

    monkeypatch.setattr(runner, "_refresh_auth_status_cache", refresh)
    monkeypatch.setattr(retry_module, "resolve_active_coder", lambda *args, **kw: None)
    assert (
        await runner._ensure_retry_coder_available(command, header, task)
        == RetryDispatch.HANDLED
    )

    resolution = SimpleNamespace(name="codex", plugin=object())
    monkeypatch.setattr(
        retry_module, "resolve_active_coder", lambda *args, **kw: resolution
    )

    async def blocked(**kwargs):
        return False

    monkeypatch.setattr(runner, "usage_gate", blocked)
    assert (
        await runner._ensure_retry_coder_available(command, header, task)
        == RetryDispatch.HANDLED
    )

    async def allowed(**kwargs):
        return True

    monkeypatch.setattr(runner, "usage_gate", allowed)
    assert await runner._ensure_retry_coder_available(command, header, task) == (
        "codex",
        resolution.plugin,
    )


@pytest.mark.asyncio
async def test_coding_retry_applies_once_and_clears_only_task_local_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    text = _task_text()
    runner, command, _task, _repo = await _prepared_runner(tmp_path, text=text)
    _allow_validation(monkeypatch, runner, text)
    monkeypatch.setattr(runner, "_suppression_record_for_task", _none_record)
    monkeypatch.setattr(retry_module, "derive_active_inhibitors", _async_value([]))
    monkeypatch.setattr(retry_module.gh_prs, "get_open_prs", lambda *args: [])
    monkeypatch.setattr(retry_module.gh_prs, "get_merged_prs", lambda *args, **kw: [])

    async def coder(*args):
        return "codex", object()

    commits: list[tuple[str, str]] = []

    async def commit(task, status: str, reason: str):
        commits.append((status, reason))
        return True

    monkeypatch.setattr(runner, "_ensure_retry_coder_available", coder)
    monkeypatch.setattr(runner, "_commit_task_status_change", commit)
    runner.redis.store[cause_key(runner.name, command.task_id)] = CancellationCause(
        category="ERROR",
        payload={"subsource": "crash"},
    ).to_redis()
    command.failure_id = retry_failure_identity(
        runner.state.current_task, runner.state,
        CancellationCause.from_redis(runner.redis.store[cause_key(runner.name, command.task_id)]),
    )
    runner.redis.store[retry_command(runner.name, command.command_id)] = command.model_dump_json()
    runner.redis.store[f"diagnose_exhausted:{runner.name}:{command.task_id}"] = "1"
    runner.redis.store["error_rate_events"] = "global-history-must-survive"
    runner._crashed_task_pr_ids.add(command.task_id)
    runner._user_stopped_task_pr_ids.add(command.task_id)
    runner._status_write_failed_task_pr_ids.add(command.task_id)

    assert await runner._consume_retry_command() == RetryDispatch.CODING
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored is not None
    assert stored.status == RetryCommandStatus.APPLIED
    assert stored.effect_stage == RetryEffectStage.APPLIED
    assert stored.retry_count == 1
    assert stored.selected_continuation == "coding"
    assert stored.execution_state == RetryExecutionState.PENDING
    assert commits == [("TODO", f"operator Retry {command.command_id}")]
    assert cause_key(runner.name, command.task_id) not in runner.redis.store
    assert f"diagnose_exhausted:{runner.name}:{command.task_id}" not in runner.redis.store
    assert runner.redis.store["error_rate_events"] == "global-history-must-survive"
    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task is not None
    assert runner.state.current_task.status == TaskStatus.DOING
    assert stored.reset_counters == [
        "error_diagnose_attempts",
        "error_soft_skip_attempts",
        "review_timeout_repost",
    ]

    # Applied commands reconcile instead of reserving or committing again.
    assert await runner._consume_retry_command() == RetryDispatch.CODING
    assert commits == [("TODO", f"operator Retry {command.command_id}")]
    assert runner.redis.store[f"metrics:retry_count:{runner.name}:PR-42"] == "1"


@pytest.mark.asyncio
async def test_stage_and_applied_acknowledgement_require_command_record(
    tmp_path: Path,
) -> None:
    runner, command, task, _repo = await _prepared_runner(tmp_path)
    runner.redis.store.pop(retry_command(runner.name, command.command_id))
    with pytest.raises(RuntimeError, match="recording stage"):
        await runner._record_retry_stage(
            command, RetryEffectStage.STATUS_COMMITTED, "status committed"
        )
    with pytest.raises(RuntimeError, match="before acknowledgement"):
        await runner._mark_retry_applied(command, "coding", [])

    runner.state.current_queue = []
    runner._set_retry_task_snapshot(task)
    assert [item.pr_id for item in runner.state.current_queue or []] == ["PR-42"]


@pytest.mark.asyncio
async def test_watch_retry_preserves_pr_and_resets_pr_local_counters(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    text = _task_text()
    pr = PRInfo(
        number=42,
        branch="fix/pr-42",
        pr_id="PR-42",
        head_sha="abc123",
        fix_iteration_count=4,
        no_push_fix_count=3,
        watch_retrigger_count=2,
    )
    runner, command, _task, _repo = await _prepared_runner(
        tmp_path,
        text=text,
        command=_command(text, bound_pr=pr),
    )
    runner._start_current_run_record("codex", "")
    assert runner._current_run_record is not None
    runner._current_run_record.fix_iterations = 4
    await runner._checkpoint_current_run_record()
    _allow_validation(monkeypatch, runner, text)
    monkeypatch.setattr(runner, "_suppression_record_for_task", _none_record)
    monkeypatch.setattr(retry_module, "derive_active_inhibitors", _async_value([]))
    monkeypatch.setattr(retry_module.gh_prs, "get_open_prs", lambda *args: [pr])

    async def commit(*args):
        return True

    monkeypatch.setattr(runner, "_commit_task_status_change", commit)
    assert await runner._consume_retry_command() == RetryDispatch.WATCH
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 42
    assert runner.state.current_pr.fix_iteration_count == 0
    assert runner.state.current_pr.no_push_fix_count == 0
    assert runner.state.current_pr.watch_retrigger_count == 0
    assert runner._current_run_record.fix_iterations == 4
    assert runner._current_run_record.fix_recovery_baseline == 4
    restored = await runner._metrics_store.get(runner._current_run_record.run_id)
    assert restored is not None and restored.fix_recovery_baseline == 4
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored is not None
    assert stored.selected_continuation == "watch"
    assert "fix_iteration_count" in stored.reset_counters


@pytest.mark.asyncio
async def test_watch_retry_budget_checkpoint_failure_keeps_command_pending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    text = _task_text()
    pr = PRInfo(number=42, branch="fix/pr-42", pr_id="PR-42", head_sha="abc123")
    runner, command, _task, _repo = await _prepared_runner(
        tmp_path, text=text, command=_command(text, bound_pr=pr)
    )
    runner._start_current_run_record("codex", "")
    record = runner._current_run_record
    assert record is not None
    record.fix_iterations = 3
    _allow_validation(monkeypatch, runner, text)
    monkeypatch.setattr(runner, "_suppression_record_for_task", _none_record)
    monkeypatch.setattr(retry_module, "derive_active_inhibitors", _async_value([]))
    monkeypatch.setattr(retry_module.gh_prs, "get_open_prs", lambda *args: [pr])
    monkeypatch.setattr(runner, "_commit_task_status_change", _async_value(True))

    async def checkpoint_failed():
        raise RuntimeError("metrics down")

    monkeypatch.setattr(runner, "_checkpoint_current_run_record", checkpoint_failed)
    assert await runner._consume_retry_command() == RetryDispatch.HANDLED
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored is not None and stored.status == RetryCommandStatus.DEFERRED
    assert stored.effect_stage == RetryEffectStage.STATUS_COMMITTED
    assert record.fix_recovery_baseline == 0
    assert runner.state.state == PipelineState.ERROR


@pytest.mark.asyncio
async def test_watch_retry_defers_when_accounting_restore_is_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    text = _task_text()
    pr = PRInfo(number=42, branch="fix/pr-42", pr_id="PR-42", head_sha="abc123")
    runner, command, _task, _repo = await _prepared_runner(
        tmp_path, text=text, command=_command(text, bound_pr=pr)
    )
    _allow_validation(monkeypatch, runner, text)
    monkeypatch.setattr(runner, "_suppression_record_for_task", _none_record)
    monkeypatch.setattr(retry_module, "derive_active_inhibitors", _async_value([]))
    monkeypatch.setattr(retry_module.gh_prs, "get_open_prs", lambda *args: [pr])
    monkeypatch.setattr(runner, "_commit_task_status_change", _async_value(True))

    async def metrics_down(**_kwargs):
        raise RuntimeError("metrics down")

    monkeypatch.setattr(runner._metrics_store, "recent", metrics_down)
    assert await runner._consume_retry_command() == RetryDispatch.HANDLED
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored is not None and stored.status == RetryCommandStatus.DEFERRED
    assert stored.effect_stage == RetryEffectStage.STATUS_COMMITTED
    assert "accounting" in stored.outcome_reason
    assert runner.state.state == PipelineState.ERROR


@pytest.mark.asyncio
@pytest.mark.parametrize("continuation", ["watch", "coding"])
async def test_retry_replaces_unrelated_cached_run_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, continuation: str,
) -> None:
    runner, command, task, _repo = await _prepared_runner(tmp_path)
    runner._start_current_run_record("codex", "")
    unrelated = runner._current_run_record
    assert unrelated is not None
    unrelated.task_id = "PR-999"
    pr = PRInfo(number=42, branch=task.branch, pr_id=task.pr_id)
    monkeypatch.setattr(runner, "_validate_retry_command", _async_value((object(), task)))
    monkeypatch.setattr(runner, "_retry_blocker", _async_value(None))
    monkeypatch.setattr(
        runner, "_select_retry_continuation",
        _async_value((continuation, pr if continuation == "watch" else None)),
    )
    monkeypatch.setattr(
        runner, "_ensure_retry_coder_available", _async_value(("codex", object()))
    )
    monkeypatch.setattr(runner, "_commit_task_status_change", _async_value(True))
    monkeypatch.setattr(runner, "_clear_retry_failure_evidence", _async_value(None))
    expected = RetryDispatch.WATCH if continuation == "watch" else RetryDispatch.CODING
    assert await runner._apply_retry_command(command) == expected
    assert runner._current_run_record is None
    assert runner.state.active_invocation is None

    if continuation == "watch":
        runner._current_run_record = unrelated
        assert await runner._reconcile_applied_retry(command) == RetryDispatch.WATCH
        assert runner._current_run_record is None


@pytest.mark.asyncio
async def test_watch_retry_keeps_fix_binding_when_pr_observation_is_unknown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, command, task, _repo = await _prepared_runner(tmp_path)
    pr = PRInfo(number=42, branch=task.branch, pr_id=task.pr_id)
    runner._start_current_run_record("codex", "")
    runner._current_run_record.fix_iterations = 1
    runner._capture_invocation_snapshot("codex", {}, phase="fix", fix_iteration=1)
    await runner._checkpoint_current_run_record()
    monkeypatch.setattr(runner, "_validate_retry_command", _async_value((object(), task)))
    monkeypatch.setattr(runner, "_retry_blocker", _async_value(None))
    monkeypatch.setattr(runner, "_select_retry_continuation", _async_value(("watch", pr)))
    monkeypatch.setattr(runner, "_commit_task_status_change", _async_value(True))
    monkeypatch.setattr(runner, "_clear_retry_failure_evidence", _async_value(None))
    assert await runner._apply_retry_command(command) == RetryDispatch.WATCH
    assert runner.state.active_invocation.phase == "fix"
    runner.state.active_invocation = None
    assert await runner._reconcile_applied_retry(command) == RetryDispatch.WATCH
    assert runner.state.active_invocation.phase == "fix"
    runner.state.active_invocation = None
    monkeypatch.setattr(retry_module.gh_prs, "get_open_prs", lambda *_a, **_k: [])
    monkeypatch.setattr(retry_module.gh_prs, "get_pr_state", lambda *_a: None)
    monkeypatch.setattr(retry_module.gh_prs, "is_pr_merged", lambda *_a: None)
    await runner.handle_watch()
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_task.pr_id == task.pr_id
    assert runner.state.current_pr.number == pr.number
    runner.state.active_invocation = runner._current_run_record.invocations[-1]
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    restarted = h._make_runner()
    restarted.redis = runner.redis
    restarted._metrics_store = runner._metrics_store
    restarted.repo_path = runner.repo_path
    await restarted._hydrate_current_task_from_persisted_state()
    await restarted._apply_recovery_decisions(
        [task.model_copy(update={"status": TaskStatus.TODO})], []
    )
    assert restarted.state.state == PipelineState.WATCH
    await restarted.handle_watch()
    assert restarted.state.state == PipelineState.WATCH
    assert restarted.state.current_pr.number == pr.number


@pytest.mark.asyncio
async def test_retry_admits_historical_pr_with_durable_exact_proposal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, command, task, _repo = await _prepared_runner(tmp_path)
    snapshot = RepoState(url=runner.state.url, name=runner.name,
                         state=PipelineState.IDLE)
    await runner.redis.set(pipeline_state(runner.name), snapshot.model_dump_json())
    assert await runner._hydrate_current_task_from_persisted_state()
    assert runner._publication_snapshot_pr_unbound
    runner._publication_recovery_cutoff_at = NOW
    runner.state.current_task = task
    runner.state.state = PipelineState.ERROR
    published = NOW - timedelta(days=1)
    pr = PRInfo(
        number=42, branch=task.branch, pr_id=task.pr_id, head_sha="a" * 40,
        publication_observed=True, publication_at=published,
        publication_repo=runner.owner_repo,
        publication_head_repo=runner.owner_repo,
        publication_ref=f"refs/heads/{task.branch}",
        publication_head_sha="a" * 40,
        publication_provenance="github-activity:branch_creation",
    )
    monkeypatch.setattr(runner, "_validate_retry_command", _async_value((object(), task)))
    monkeypatch.setattr(runner, "_retry_blocker", _async_value(None))
    monkeypatch.setattr(retry_module.gh_prs, "get_open_prs", lambda *_a, **_k: [pr])
    monkeypatch.setattr(runner, "_commit_task_status_change", _async_value(True))
    monkeypatch.setattr(runner, "_clear_retry_failure_evidence", _async_value(None))
    assert await runner._apply_retry_command(command) == RetryDispatch.WATCH
    proposal = runner.state.publication_migration
    assert proposal is not None
    assert proposal.source == "pr_undiscovered_snapshot"
    assert proposal.published_at == published
    assert migration_confirmable(proposal)
    assert runner.state.publication_watermark is None
    await runner.publish_state()
    saved = RepoState.model_validate_json(await runner.redis.get(pipeline_state(runner.name)))
    assert saved.publication_migration == proposal
    assert runner._validate_publication_observation(
        pr.number, pr.head_sha, pr.publication_at, None,
        head_repo=pr.publication_head_repo, ref=pr.publication_ref,
        observed_repo=pr.publication_repo, observed_head_sha=pr.publication_head_sha,
        provenance=pr.publication_provenance,
    )[0] is None
    monkeypatch.setattr(
        retry_module.gh_prs, "get_pr_head_publication", lambda *_a, **_k:
        retry_module.gh_prs.HeadPublicationEvidence(
            repo=pr.publication_repo, pr_number=pr.number,
            head_repo=pr.publication_head_repo, ref=pr.publication_ref,
            head_sha=pr.head_sha, published_at=published,
            provenance=pr.publication_provenance,
        ),
    )
    binding = migration_binding(proposal)
    await enqueue_migration(runner.redis, runner.name, binding)
    assert await runner._consume_publication_migration_command()
    floor = runner.state.publication_watermark
    assert floor is not None and floor.published_at == published
    assert runner.state.publication_migration is None
    assert await runner._reconcile_applied_retry(command) == RetryDispatch.WATCH
    assert runner.state.publication_watermark == floor
    assert runner.state.publication_migration is None
    assert runner._validate_publication_observation(
        pr.number, pr.head_sha, pr.publication_at, None,
        head_repo=pr.publication_head_repo, ref=pr.publication_ref,
        observed_repo=pr.publication_repo, observed_head_sha=pr.publication_head_sha,
        provenance=pr.publication_provenance,
    ) == (published, None)


@pytest.mark.asyncio
async def test_applied_retry_preserves_unknown_migration_until_matching_activity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, command, task, _repo = await _prepared_runner(tmp_path)
    snapshot = RepoState(url=runner.state.url, name=runner.name,
                         state=PipelineState.IDLE)
    await runner.redis.set(pipeline_state(runner.name), snapshot.model_dump_json())
    assert await runner._hydrate_current_task_from_persisted_state()
    runner.state.current_task = task
    runner.state.state = PipelineState.ERROR
    pr = PRInfo(
        number=42, branch=task.branch, pr_id=task.pr_id, head_sha="a" * 40,
        publication_observed=True, publication_reason="activity unavailable",
    )
    monkeypatch.setattr(runner, "_validate_retry_command", _async_value((object(), task)))
    monkeypatch.setattr(runner, "_retry_blocker", _async_value(None))
    monkeypatch.setattr(retry_module.gh_prs, "get_open_prs", lambda *_a, **_k: [pr])
    assert await runner._reconcile_applied_retry(command) == RetryDispatch.WATCH
    proposal = runner.state.publication_migration
    assert proposal is not None and proposal.published_at is None
    assert proposal.source == "pr_undiscovered_snapshot"
    await runner.publish_state()
    saved = RepoState.model_validate_json(await runner.redis.get(pipeline_state(runner.name)))
    assert saved.publication_migration == proposal
    assert saved.publication_watermark is None
    # A later matching poll completes the same proposal without a new Retry.
    complete = pr.model_copy(update={
        "publication_at": NOW - timedelta(days=1),
        "publication_repo": runner.owner_repo,
        "publication_head_repo": runner.owner_repo,
        "publication_ref": f"refs/heads/{task.branch}",
        "publication_head_sha": pr.head_sha,
        "publication_provenance": "github-activity:branch_creation",
        "publication_reason": None,
    })
    runner._admit_observed_pr(complete)
    assert migration_confirmable(runner.state.publication_migration)
    assert runner.state.publication_watermark is None


@pytest.mark.asyncio
async def test_watch_retry_starts_active_record_after_completed_fix_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, command, task, _repo = await _prepared_runner(tmp_path)
    pr = PRInfo(number=42, branch=task.branch, pr_id=task.pr_id)
    runner._start_current_run_record("codex", "")
    completed = runner._current_run_record
    completed.fix_iterations = 3
    runner._capture_invocation_snapshot("codex", {}, phase="fix", fix_iteration=3)
    completed.ended_at = NOW.isoformat()
    completed.exit_reason = "timeout"
    completed.cause = "TIMEOUT"
    completed.tokens_in = 120
    completed.tokens_out = 45
    completed.head_sha = "a" * 40
    completed.diff_lines_added = 8
    await runner._checkpoint_current_run_record()
    monkeypatch.setattr(runner, "_validate_retry_command", _async_value((object(), task)))
    monkeypatch.setattr(runner, "_retry_blocker", _async_value(None))
    monkeypatch.setattr(runner, "_select_retry_continuation", _async_value(("watch", pr)))
    monkeypatch.setattr(runner, "_commit_task_status_change", _async_value(True))
    monkeypatch.setattr(runner, "_clear_retry_failure_evidence", _async_value(None))
    assert await runner._apply_retry_command(command) == RetryDispatch.WATCH
    current = runner._current_run_record
    assert current.run_id != completed.run_id
    assert current.ended_at is None
    assert current.fix_iterations == current.fix_recovery_baseline == 3
    assert current.invocations == []
    assert current.coder_session_id != completed.coder_session_id
    assert current.exit_reason == "" and current.cause == "CRASH"
    assert current.tokens_in == current.tokens_out == 0
    assert current.head_sha == "" and current.diff_lines_added == 0
    assert runner.state.active_invocation is None
    assert (await runner._metrics_store.get(completed.run_id)).ended_at == NOW.isoformat()
    runner._current_run_record = current
    assert await runner._reconcile_applied_retry(command) == RetryDispatch.WATCH
    assert runner.state.active_invocation is None
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    restarted = h._make_runner()
    restarted.redis = runner.redis
    restarted._metrics_store = runner._metrics_store
    restarted.repo_path = runner.repo_path
    await restarted._hydrate_current_task_from_persisted_state()
    await restarted._apply_recovery_decisions(
        [task.model_copy(update={"status": TaskStatus.TODO})], []
    )
    assert restarted.state.state == PipelineState.WATCH
    assert restarted._current_run_record.fix_recovery_baseline == 3


@pytest.mark.asyncio
async def test_applied_watch_retry_defers_when_accounting_restore_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, command, task, _repo = await _prepared_runner(tmp_path)
    pr = PRInfo(number=42, branch=task.branch, pr_id=task.pr_id)
    monkeypatch.setattr(runner, "_validate_retry_command", _async_value((object(), task)))
    monkeypatch.setattr(runner, "_retry_blocker", _async_value(None))
    monkeypatch.setattr(runner, "_select_retry_continuation", _async_value(("watch", pr)))

    async def unavailable(**_kwargs):
        raise RuntimeError("metrics down")

    monkeypatch.setattr(runner._metrics_store, "recent", unavailable)
    assert await runner._reconcile_applied_retry(command) == RetryDispatch.HANDLED
    assert runner.state.state == PipelineState.ERROR
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored is not None and stored.status == RetryCommandStatus.DEFERRED


@pytest.mark.asyncio
async def test_apply_defers_each_durable_side_effect_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    text = _task_text()
    runner, command, task, _repo = await _prepared_runner(tmp_path, text=text)
    monkeypatch.setattr(
        runner, "_validate_retry_command", _async_value((object(), task))
    )
    monkeypatch.setattr(runner, "_retry_blocker", _async_value(None))
    monkeypatch.setattr(
        runner, "_select_retry_continuation", _async_value(("coding", None))
    )
    monkeypatch.setattr(
        runner,
        "_ensure_retry_coder_available",
        _async_value(("codex", object())),
    )

    runner.redis.store[f"metrics:retry_count:{runner.name}:{command.task_id}"] = "3"
    assert await runner._apply_retry_command(command) == RetryDispatch.HANDLED
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored is not None and stored.status == RetryCommandStatus.FAILED

    # A fresh command reaches status persistence and remains resumable there.
    command = _command(text)
    command.command_id = "status-failure"
    command.request_binding = "c" * 64
    await enqueue_retry_command(runner.redis, command)
    runner.redis.store[f"metrics:retry_count:{runner.name}:{command.task_id}"] = "0"

    async def commit_false(*args):
        return False

    monkeypatch.setattr(runner, "_commit_task_status_change", commit_false)
    assert await runner._apply_retry_command(command) == RetryDispatch.HANDLED
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored is not None
    assert stored.effect_stage == RetryEffectStage.RETRY_RESERVED
    assert stored.status == RetryCommandStatus.DEFERRED

    async def commit_true(*args):
        return True

    monkeypatch.setattr(runner, "_commit_task_status_change", commit_true)
    async def stage_boom(*args):
        raise RuntimeError("redis down")

    monkeypatch.setattr(runner, "_record_retry_stage", stage_boom)
    assert await runner._apply_retry_command(stored) == RetryDispatch.HANDLED


@pytest.mark.asyncio
async def test_apply_propagates_validation_blocker_pr_and_coder_decisions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, command, task, _repo = await _prepared_runner(tmp_path)
    cases = [
        ("validate", RetryDispatch.HANDLED),
        ("blocker", RetryDispatch.HANDLED),
        ("continuation", RetryDispatch.HANDLED),
        ("coder", RetryDispatch.HANDLED),
    ]
    for stage, expected in cases:
        monkeypatch.setattr(
            runner, "_validate_retry_command", _async_value((object(), task))
        )
        monkeypatch.setattr(runner, "_retry_blocker", _async_value(None))
        monkeypatch.setattr(
            runner, "_select_retry_continuation", _async_value(("coding", None))
        )
        monkeypatch.setattr(
            runner,
            "_ensure_retry_coder_available",
            _async_value(("codex", object())),
        )
        if stage == "validate":
            monkeypatch.setattr(
                runner, "_validate_retry_command", _async_value(expected)
            )
        elif stage == "blocker":
            monkeypatch.setattr(runner, "_retry_blocker", _async_value(expected))
        elif stage == "continuation":
            monkeypatch.setattr(
                runner, "_select_retry_continuation", _async_value(expected)
            )
        else:
            monkeypatch.setattr(
                runner, "_ensure_retry_coder_available", _async_value(expected)
            )
        assert await runner._apply_retry_command(command) == expected


@pytest.mark.asyncio
async def test_apply_defers_reservation_cleanup_and_acknowledgement_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, command, task, _repo = await _prepared_runner(tmp_path)
    monkeypatch.setattr(
        runner, "_validate_retry_command", _async_value((object(), task))
    )
    monkeypatch.setattr(runner, "_retry_blocker", _async_value(None))
    monkeypatch.setattr(
        runner, "_select_retry_continuation", _async_value(("coding", None))
    )
    monkeypatch.setattr(
        runner,
        "_ensure_retry_coder_available",
        _async_value(("codex", object())),
    )

    async def reserve_boom(*args, **kwargs):
        raise RuntimeError("redis down")

    monkeypatch.setattr(retry_module, "reserve_retry_attempt", reserve_boom)
    assert await runner._apply_retry_command(command) == RetryDispatch.HANDLED

    command.effect_stage = RetryEffectStage.STATUS_COMMITTED

    async def reserve_existing(*args, **kwargs):
        return command

    async def cleanup_boom(*args):
        raise RuntimeError("cleanup down")

    monkeypatch.setattr(retry_module, "reserve_retry_attempt", reserve_existing)
    monkeypatch.setattr(runner, "_clear_retry_failure_evidence", cleanup_boom)
    assert await runner._apply_retry_command(command) == RetryDispatch.HANDLED

    async def cleanup_ok(*args):
        return None

    async def mark_boom(*args):
        raise RuntimeError("ack down")

    monkeypatch.setattr(runner, "_clear_retry_failure_evidence", cleanup_ok)
    monkeypatch.setattr(runner, "_mark_retry_applied", mark_boom)
    assert await runner._apply_retry_command(command) == RetryDispatch.HANDLED


@pytest.mark.asyncio
async def test_applied_reconciliation_never_duplicates_uncertain_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, command, task, _repo = await _prepared_runner(tmp_path)
    command.status = RetryCommandStatus.APPLIED
    command.effect_stage = RetryEffectStage.APPLIED
    command.execution_state = RetryExecutionState.RUNNING
    runner.redis.store[retry_command(runner.name, command.command_id)] = (
        command.model_dump_json()
    )
    monkeypatch.setattr(
        runner, "_validate_retry_command", _async_value((object(), task))
    )
    monkeypatch.setattr(runner, "_retry_blocker", _async_value(None))
    monkeypatch.setattr(
        runner, "_select_retry_continuation", _async_value(("coding", None))
    )
    runner.redis.store[current_run_started_at_key(runner.name, command.task_id)] = (
        NOW.isoformat()
    )
    assert await runner._reconcile_applied_retry(command) == RetryDispatch.HANDLED
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored is not None
    assert stored.execution_state == RetryExecutionState.UNCERTAIN
    assert "will not be executed twice" in stored.outcome_reason

    command.execution_state = RetryExecutionState.PENDING
    runner.redis.store[retry_command(runner.name, command.command_id)] = (
        command.model_dump_json()
    )

    async def coder(*args):
        return "codex", object()

    monkeypatch.setattr(runner, "_ensure_retry_coder_available", coder)
    assert await runner._reconcile_applied_retry(command) == RetryDispatch.CODING
    assert runner.state.state == PipelineState.CODING


@pytest.mark.asyncio
async def test_applied_reconciliation_propagates_checks_and_recovers_watch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, command, task, _repo = await _prepared_runner(tmp_path)
    pr = PRInfo(number=42, branch="fix/pr-42", fix_iteration_count=3)
    for stage in ("validate", "blocker", "continuation", "coder"):
        monkeypatch.setattr(
            runner, "_validate_retry_command", _async_value((object(), task))
        )
        monkeypatch.setattr(runner, "_retry_blocker", _async_value(None))
        monkeypatch.setattr(
            runner, "_select_retry_continuation", _async_value(("coding", None))
        )
        monkeypatch.setattr(
            runner,
            "_ensure_retry_coder_available",
            _async_value(("codex", object())),
        )
        if stage == "validate":
            monkeypatch.setattr(
                runner, "_validate_retry_command", _async_value(RetryDispatch.HANDLED)
            )
        elif stage == "blocker":
            monkeypatch.setattr(
                runner, "_retry_blocker", _async_value(RetryDispatch.HANDLED)
            )
        elif stage == "continuation":
            monkeypatch.setattr(
                runner,
                "_select_retry_continuation",
                _async_value(RetryDispatch.HANDLED),
            )
        else:
            monkeypatch.setattr(
                runner,
                "_ensure_retry_coder_available",
                _async_value(RetryDispatch.HANDLED),
            )
        assert await runner._reconcile_applied_retry(command) == RetryDispatch.HANDLED

    monkeypatch.setattr(
        runner, "_validate_retry_command", _async_value((object(), task))
    )
    monkeypatch.setattr(runner, "_retry_blocker", _async_value(None))
    monkeypatch.setattr(
        runner, "_select_retry_continuation", _async_value(("watch", pr))
    )
    assert await runner._reconcile_applied_retry(command) == RetryDispatch.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.fix_iteration_count == 0


@pytest.mark.asyncio
async def test_uncertain_reconciliation_tolerates_missing_run_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, command, task, _repo = await _prepared_runner(tmp_path)
    command.execution_state = RetryExecutionState.UNCERTAIN
    runner.redis.store[retry_command(runner.name, command.command_id)] = (
        command.model_dump_json()
    )
    monkeypatch.setattr(
        runner, "_validate_retry_command", _async_value((object(), task))
    )
    monkeypatch.setattr(runner, "_retry_blocker", _async_value(None))
    monkeypatch.setattr(
        runner, "_select_retry_continuation", _async_value(("coding", None))
    )

    async def marker_boom(*args):
        raise RuntimeError("redis down")

    monkeypatch.setattr(retry_module, "get_current_run_started_at", marker_boom)
    assert await runner._reconcile_applied_retry(command) == RetryDispatch.HANDLED


@pytest.mark.asyncio
async def test_execution_markers_and_outcomes_are_durable(
    tmp_path: Path,
) -> None:
    runner, command, _task, _repo = await _prepared_runner(tmp_path)
    runner._active_retry_command_id = command.command_id
    assert await runner._start_retry_coding_execution() is True
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored is not None
    assert stored.execution_state == RetryExecutionState.RUNNING
    assert stored.execution_started_at is not None

    for dispatch, state, expected, pending in [
        (RetryDispatch.WATCH, PipelineState.WATCH, RetryExecutionState.WATCHING, False),
        (RetryDispatch.CODING, PipelineState.WATCH, RetryExecutionState.WATCHING, False),
        (RetryDispatch.CODING, PipelineState.PAUSED, RetryExecutionState.PENDING, True),
        (RetryDispatch.CODING, PipelineState.ERROR, RetryExecutionState.FAILED, False),
        (RetryDispatch.CODING, PipelineState.IDLE, RetryExecutionState.COMPLETED, False),
    ]:
        runner.state.state = state
        await runner._finish_retry_dispatch(dispatch)
        stored = await load_retry_command(runner.redis, runner.name, command.command_id)
        assert stored is not None and stored.execution_state == expected
        is_pending = command.command_id in runner.redis.zsets[
            retry_command_pending(runner.name)
        ]
        assert is_pending is pending


@pytest.mark.asyncio
async def test_execution_marker_failures_refuse_dispatch_but_remain_recoverable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, command, _task, _repo = await _prepared_runner(tmp_path)
    runner._active_retry_command_id = None
    assert await runner._start_retry_coding_execution() is False
    await runner._finish_retry_dispatch(RetryDispatch.CODING)

    runner._active_retry_command_id = command.command_id

    async def update_boom(*args, **kwargs):
        raise RuntimeError("redis down")

    monkeypatch.setattr(retry_module, "update_retry_command", update_boom)
    assert await runner._start_retry_coding_execution() is False
    await runner._finish_retry_dispatch(RetryDispatch.CODING)


@pytest.mark.asyncio
async def test_deferred_retry_allows_active_watch_and_later_resumes_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retry_task_id = "PR-396"
    retry_branch = "fix/pr-396"
    retry_text = _task_text(task_id=retry_task_id, branch=retry_branch)
    retry_pr = PRInfo(
        number=556,
        branch=retry_branch,
        pr_id=retry_task_id,
        head_sha="retry-head",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
        diff_scanned_at_sha="retry-head",
    )
    command = _command(
        retry_text,
        task_id=retry_task_id,
        branch=retry_branch,
        bound_pr=retry_pr,
    )
    cause = CancellationCause(category="ERROR", payload={"subsource": "crash"})
    command.failure_id = retry_failure_identity(
        QueueTask(pr_id=retry_task_id, title="Retry", status=TaskStatus.ERROR), None, cause
    )
    runner = h._make_runner()
    repo = tmp_path / "repo"
    task_path = repo / command.task_file
    task_path.parent.mkdir(parents=True)
    task_path.write_text(retry_text, encoding="utf-8")
    runner.repo_path = str(repo)
    runner._recovered = True

    active_task = QueueTask(
        pr_id="PR-400",
        title="Active pipeline task",
        status=TaskStatus.DOING,
        task_file="tasks/PR-400.md",
        branch="fix/pr-400",
        priority=1,
    )
    active_pr = PRInfo(
        number=559,
        branch=active_task.branch,
        pr_id=active_task.pr_id,
        head_sha="active-head",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
        diff_scanned_at_sha="active-head",
    )
    runner.state.state = PipelineState.WATCH
    runner.state.current_task = active_task
    runner.state.current_queue = [active_task]
    runner.state.current_pr = active_pr
    runner._last_push_at = datetime.now(timezone.utc) - timedelta(minutes=10)
    runner._last_push_at_pr_number = active_pr.number
    runner.redis.store[cause_key(runner.name, retry_task_id)] = cause.to_redis()
    await enqueue_retry_command(runner.redis, command)

    monkeypatch.setattr(runner, "ensure_repo_cloned", _async_value(None))
    monkeypatch.setattr(runner, "_check_github_api_budget", _async_value(True))
    monkeypatch.setattr(runner, "preflight", _async_value(True))
    monkeypatch.setattr(runner, "_retry_worktree_dirty", lambda: (False, ""))
    monkeypatch.setattr(
        runner, "_origin_retry_task_text", lambda current: retry_text
    )
    monkeypatch.setattr(
        retry_module.gh_prs, "get_pr_state", lambda *args: "OPEN"
    )
    monkeypatch.setattr(
        retry_module.gh_prs,
        "get_last_push_age_seconds",
        lambda *args: 0,
    )

    watch_polls: list[int] = []

    def get_open_prs(*args, **kwargs):
        if (
            runner.state.current_task is not None
            and runner.state.current_task.pr_id == active_task.pr_id
        ):
            watch_polls.append(active_pr.number)
            review = (
                ReviewStatus.CHANGES_REQUESTED
                if len(watch_polls) >= 3
                else ReviewStatus.PENDING
            )
            return [active_pr.model_copy(update={"review_status": review})]
        return [retry_pr]

    monkeypatch.setattr(retry_module.gh_prs, "get_open_prs", get_open_prs)
    recent = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: (
            [
                {
                    "user": {
                        "id": 199175422,
                        "login": "chatgpt-codex-connector[bot]",
                    },
                    "body": "P1: fresh review finding",
                    "created_at": recent,
                }
            ]
            if "issues" in path and "/559/" in path
            else []
        ),
    )
    fix_calls: list[tuple[str, int]] = []

    async def handle_fix() -> None:
        assert runner.state.current_task is not None
        assert runner.state.current_pr is not None
        fix_calls.append(
            (runner.state.current_task.pr_id, runner.state.current_pr.number)
        )

    monkeypatch.setattr(runner, "handle_fix", handle_fix)
    status_commits: list[tuple[str, str]] = []

    async def commit_status(task, status: str, reason: str, **kwargs):
        status_commits.append((task.pr_id, status))
        return True

    monkeypatch.setattr(runner, "_commit_task_status_change", commit_status)

    for _ in range(3):
        await runner._run_cycle_body()

    deferred = await load_retry_command(
        runner.redis, runner.name, command.command_id
    )
    assert deferred is not None
    assert watch_polls == [559, 559, 559]
    assert fix_calls == [(active_task.pr_id, active_pr.number)]
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == active_task.pr_id
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == active_pr.number
    assert deferred.status == RetryCommandStatus.DEFERRED
    assert deferred.request_binding == command.request_binding
    assert "Another task (PR-400) is active in WATCH" in deferred.outcome_reason
    assert deferred.retry_count is None
    assert deferred.effect_stage == RetryEffectStage.NONE
    assert deferred.execution_state == RetryExecutionState.NOT_STARTED
    assert deferred.execution_started_at is None
    assert deferred.processing_owner is None
    assert command.command_id in runner.redis.zsets[
        retry_command_pending(runner.name)
    ]
    assert status_commits == []

    runner.state.state = PipelineState.IDLE
    runner.state.current_task = None
    runner.state.current_pr = None
    await runner._run_cycle_body()

    resumed = await load_retry_command(
        runner.redis, runner.name, command.command_id
    )
    assert resumed is not None
    assert resumed.status == RetryCommandStatus.APPLIED
    assert resumed.selected_continuation == "watch"
    assert resumed.retry_count == 1
    assert resumed.execution_state == RetryExecutionState.WATCHING
    assert status_commits == [(retry_task_id, "TODO")]
    assert command.command_id not in runner.redis.zsets[
        retry_command_pending(runner.name)
    ]

    runner._last_push_at = datetime.now(timezone.utc)
    runner._last_push_at_pr_number = retry_pr.number
    await runner._run_cycle_body()
    still_resumed = await load_retry_command(
        runner.redis, runner.name, command.command_id
    )
    assert still_resumed is not None
    assert still_resumed.retry_count == 1
    assert status_commits == [(retry_task_id, "TODO")]


@pytest.mark.asyncio
async def test_run_cycle_keeps_other_handled_retry_outcomes_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, _command_record, _task, _repo = await _prepared_runner(tmp_path)
    runner._recovered = True
    ordinary_cycle_calls: list[str] = []
    monkeypatch.setattr(runner, "ensure_repo_cloned", _async_value(None))
    monkeypatch.setattr(
        runner, "_refresh_user_paused_from_redis", _async_value(None)
    )
    monkeypatch.setattr(
        runner, "_consume_retry_command", _async_value(RetryDispatch.HANDLED)
    )
    monkeypatch.setattr(runner, "publish_state", _async_value(None))

    async def check_budget() -> bool:
        ordinary_cycle_calls.append("budget")
        return True

    monkeypatch.setattr(runner, "_check_github_api_budget", check_budget)

    await runner._run_cycle_body()

    assert ordinary_cycle_calls == []


@pytest.mark.asyncio
async def test_run_cycle_dispatches_parked_error_command_exactly_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, command, _task, _repo = await _prepared_runner(tmp_path)
    runner._recovered = True
    coding_calls: list[str] = []

    async def ensure() -> None:
        return None

    async def refresh_pause() -> None:
        return None

    async def consume() -> RetryDispatch:
        runner._active_retry_command_id = command.command_id
        runner.state.state = PipelineState.CODING
        return RetryDispatch.CODING

    async def coding() -> None:
        coding_calls.append(command.command_id)
        runner.state.state = PipelineState.WATCH

    async def publish() -> None:
        return None

    monkeypatch.setattr(runner, "ensure_repo_cloned", ensure)
    monkeypatch.setattr(runner, "_refresh_user_paused_from_redis", refresh_pause)
    monkeypatch.setattr(runner, "_consume_retry_command", consume)
    monkeypatch.setattr(runner, "handle_coding", coding)
    monkeypatch.setattr(runner, "publish_state", publish)
    await runner._run_cycle_body()
    assert coding_calls == [command.command_id]
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored is not None
    assert stored.execution_state == RetryExecutionState.WATCHING


@pytest.mark.asyncio
async def test_run_cycle_finishes_watch_retry_dispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, _command_record, _task, _repo = await _prepared_runner(tmp_path)
    runner._recovered = True
    finished: list[RetryDispatch] = []
    monkeypatch.setattr(runner, "ensure_repo_cloned", _async_value(None))
    monkeypatch.setattr(
        runner, "_refresh_user_paused_from_redis", _async_value(None)
    )
    monkeypatch.setattr(
        runner, "_consume_retry_command", _async_value(RetryDispatch.WATCH)
    )
    monkeypatch.setattr(runner, "publish_state", _async_value(None))

    async def finish(dispatch: RetryDispatch) -> None:
        finished.append(dispatch)

    monkeypatch.setattr(runner, "_finish_retry_dispatch", finish)
    await runner._run_cycle_body()
    assert finished == [RetryDispatch.WATCH]


@pytest.mark.asyncio
async def test_run_cycle_propagates_cancellation_during_retry_coding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, _command_record, _task, _repo = await _prepared_runner(tmp_path)
    runner._recovered = True
    monkeypatch.setattr(runner, "ensure_repo_cloned", _async_value(None))
    monkeypatch.setattr(
        runner, "_refresh_user_paused_from_redis", _async_value(None)
    )
    monkeypatch.setattr(
        runner, "_consume_retry_command", _async_value(RetryDispatch.CODING)
    )
    monkeypatch.setattr(
        runner, "_start_retry_coding_execution", _async_value(True)
    )
    monkeypatch.setattr(runner, "publish_state", _async_value(None))

    async def cancel() -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(runner, "handle_coding", cancel)
    with pytest.raises(asyncio.CancelledError):
        await runner._run_cycle_body()


@pytest.mark.asyncio
@pytest.mark.parametrize("available", [False, True])
@pytest.mark.parametrize("completed", [False, True])
async def test_retry_reconciles_retained_publication_before_replacing_invocation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, available: bool, completed: bool,
) -> None:
    from src.github import cache

    pr = PRInfo(number=42, branch="fix/pr-42", pr_id="PR-42", head_sha="a" * 40)
    runner, command, task, _repo = await _prepared_runner(
        tmp_path, command=_command(_task_text(), bound_pr=pr),
    )
    runner._start_current_run_record("codex", "")
    invocation = runner._capture_invocation_snapshot("codex", {}, phase="coding")
    runner._begin_publication_attempt(started_at=NOW, baseline_numbers=set())
    attempt = runner.state.publication_attempt
    attempt.pr_number, attempt.candidate, attempt.head_sha = pr.number, pr, pr.head_sha
    attempt.failure = "prior failure"
    attempt.exit_classification = "failure"
    if completed:
        runner._current_run_record.ended_at = NOW.isoformat()
    checkpoint = runner._checkpoint_current_run_record

    async def checkpoint_after_consumption():
        if available:
            saved = RepoState.model_validate_json(
                await runner.redis.get(pipeline_state(runner.name))
            )
            assert not saved.publication_attempt.pending
            assert saved.active_invocation == invocation
        await checkpoint()

    monkeypatch.setattr(runner, "_checkpoint_current_run_record", checkpoint_after_consumption)

    monkeypatch.setattr(cache, "_etag_get", lambda path: {
        "number": 42, "state": "open", "merged": False,
        "base": {"ref": "main", "repo": {"full_name": "octo/demo"}},
        "head": {"ref": pr.branch, "sha": pr.head_sha},
    } if available else {})
    monkeypatch.setattr(cache, "_etag_payload_observed_at", lambda *_a: None)
    monkeypatch.setattr(runner, "_validate_retry_command", _async_value((object(), task)))
    monkeypatch.setattr(runner, "_retry_blocker", _async_value(None))
    monkeypatch.setattr(runner, "_commit_task_status_change", _async_value(True))
    monkeypatch.setattr(runner, "_clear_retry_failure_evidence", _async_value(None))
    monkeypatch.setattr(retry_module.gh_prs, "get_open_prs", lambda *_a: [])
    result = await runner._apply_retry_command(command)
    if available:
        assert result == RetryDispatch.WATCH
        assert runner.state.current_pr.number == 42
        assert runner.state.state == PipelineState.WATCH
        assert not attempt.pending
        assert attempt.failure == "prior failure"
        assert runner.state.active_invocation is None
    else:
        assert result == RetryDispatch.HANDLED
        assert attempt.pending
        assert runner.state.active_invocation == invocation
        assert runner.state.state == PipelineState.ERROR


@pytest.mark.asyncio
async def test_retry_does_not_rebind_retained_work_to_a_different_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, command, _task, _repo = await _prepared_runner(tmp_path)
    runner._start_current_run_record("codex", "")
    runner._capture_invocation_snapshot("codex", {}, phase="coding")
    runner._begin_publication_attempt(started_at=NOW, baseline_numbers=set())
    pr = PRInfo(number=42, branch="fix/pr-42", pr_id="PR-42", head_sha="a" * 40)
    attempt = runner.state.publication_attempt
    attempt.pr_number, attempt.candidate, attempt.head_sha = pr.number, pr, pr.head_sha
    monkeypatch.setattr(retry_module.gh_prs, "get_exact_pr_outcome", lambda *args:
        retry_module.gh_prs.ExactPROutcome("octo/demo", "main", 42, "a" * 40,
                                         retry_module.gh_prs.ExactPRState.OPEN, True))
    command.bound_pr_number = 99
    assert await runner._select_retry_continuation(command) == RetryDispatch.HANDLED
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored.status == RetryCommandStatus.FAILED
    assert attempt.pending
    assert runner.state.current_pr.number == 42


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["cause", "message", "state", "pr", "storage"])
async def test_delayed_retry_preserves_new_failure_without_reserving(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str,
) -> None:
    runner, command, task, repo = await _prepared_runner(tmp_path)
    _allow_validation(monkeypatch, runner, _task_text())
    original = (repo / command.task_file).read_bytes()
    if change == "cause":
        cause = CancellationCause(category="ERROR", payload={"subsource": "crash", "run_id": "new"})
        runner.redis.store[cause_key(runner.name, task.pr_id)] = cause.to_redis()
    elif change == "message":
        runner.state.error_message = "a newer failure"
    elif change == "state":
        runner.state.state = PipelineState.CODING
    elif change == "pr":
        runner.state.current_pr = PRInfo(number=99, branch=task.branch, pr_id=task.pr_id)
    else:
        async def unavailable(*args, **kwargs):
            raise RuntimeError("failure storage unavailable")
        monkeypatch.setattr(retry_module, "get_cancellation_cause", unavailable)
    before = runner.state.model_copy(deep=True)
    assert await runner._consume_retry_command() == RetryDispatch.HANDLED
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored.status == (RetryCommandStatus.DEFERRED if change == "storage" else RetryCommandStatus.FAILED)
    assert stored.retry_count is None
    assert stored.effect_stage == RetryEffectStage.NONE
    assert runner.state.model_dump(exclude={"history"}) == before.model_dump(exclude={"history"})
    assert (repo / command.task_file).read_bytes() == original
    if change == "cause":
        assert runner.redis.store[cause_key(runner.name, task.pr_id)] == cause.to_redis()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", [
    RetryEffectStage.RETRY_RESERVED, RetryEffectStage.STATUS_COMMITTED, RetryEffectStage.APPLIED,
])
async def test_new_cause_is_stale_even_after_retry_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stage: RetryEffectStage,
) -> None:
    runner, command, task, _repo = await _prepared_runner(tmp_path)
    _allow_validation(monkeypatch, runner, _task_text())
    command.effect_stage = stage
    command.retry_count = 1
    runner.redis.store[retry_command(runner.name, command.command_id)] = command.model_dump_json()
    cause = CancellationCause(category="ERROR", payload={"subsource": "crash", "run_id": "new"})
    runner.redis.store[cause_key(runner.name, task.pr_id)] = cause.to_redis()
    assert await runner._consume_retry_command() == RetryDispatch.HANDLED
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored.status == RetryCommandStatus.FAILED
    assert stored.retry_count == 1
    assert runner.redis.store[cause_key(runner.name, task.pr_id)] == cause.to_redis()


@pytest.mark.asyncio
@pytest.mark.parametrize("hold", ["other_coder", "pause", "cleanup", "storage"])
async def test_owned_pr_retry_scopes_inhibitors_and_preserves_repo_holds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, hold: str,
) -> None:
    pr = PRInfo(number=42, branch="fix/pr-42", pr_id="PR-42", head_sha="abc")
    runner, command, _task, _repo = await _prepared_runner(
        tmp_path, command=_command(_task_text(), bound_pr=pr),
    )
    _allow_validation(monkeypatch, runner, _task_text())
    monkeypatch.setattr(retry_module.gh_prs, "get_open_prs", lambda *args: [pr])
    monkeypatch.setattr(runner, "_commit_task_status_change", _async_value(True))
    runner.state.rate_limited_coder_until = {"claude": datetime.now(timezone.utc) + timedelta(hours=1)}
    runner.app_config.daemon.spend_ceiling_session_percent = 80
    runner.state.usage_session_percent = 100
    if hold == "pause":
        runner.state.user_paused = True
    elif hold == "cleanup":
        monkeypatch.setattr(runner, "_hold_for_coder_cleanup", _async_value(True))
    elif hold == "storage":
        async def unavailable(*args, **kwargs):
            raise RuntimeError("storage unavailable")
        monkeypatch.setattr(retry_module, "derive_active_inhibitors", unavailable)
    outcome = await runner._consume_retry_command()
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    if hold == "other_coder":
        assert outcome == RetryDispatch.WATCH
        assert stored.status == RetryCommandStatus.APPLIED
        assert stored.retry_count == 1
        assert runner.state.current_pr.number == pr.number
    else:
        assert outcome == RetryDispatch.HANDLED
        assert stored.status == RetryCommandStatus.DEFERRED
        assert stored.retry_count is None


@pytest.mark.asyncio
async def test_coding_retry_only_checks_selected_coder_inhibitor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, command, task, _repo = await _prepared_runner(tmp_path)
    monkeypatch.setattr(runner, "_refresh_auth_status_cache", _async_value(None))
    monkeypatch.setattr(retry_module, "resolve_active_coder", lambda *args, **kwargs:
                        SimpleNamespace(name="codex", plugin=object()))
    monkeypatch.setattr(runner, "usage_gate", _async_value(True))
    header = SimpleNamespace(coder="codex")
    runner.app_config.daemon.spend_ceiling_session_percent = 80
    runner.state.usage_session_percent = 100
    runner.state.rate_limited_coder_until = {"claude": datetime.now(timezone.utc) + timedelta(hours=1)}
    assert isinstance(await runner._ensure_retry_coder_available(command, header, task), tuple)
    runner.state.rate_limited_coder_until["codex"] = datetime.now(timezone.utc) + timedelta(hours=1)
    assert await runner._ensure_retry_coder_available(command, header, task) == RetryDispatch.HANDLED


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["pr", "status"])
async def test_same_cause_does_not_override_changed_pr_or_terminal_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str,
) -> None:
    runner, command, task, repo = await _prepared_runner(tmp_path)
    cause = CancellationCause(category="ERROR", payload={"subsource": "crash"})
    command.failure_id = retry_failure_identity(task, runner.state, cause)
    runner.redis.store[cause_key(runner.name, task.pr_id)] = cause.to_redis()
    runner.redis.store[retry_command(runner.name, command.command_id)] = command.model_dump_json()
    text = _task_text()
    if change == "pr":
        runner.state.current_pr = PRInfo(number=99, branch=task.branch, pr_id=task.pr_id)
    else:
        text = text.replace("status: ERROR", "status: DONE").replace("blocked_reason: daemon\n", "")
        (repo / command.task_file).write_text(text)
    _allow_validation(monkeypatch, runner, text)
    assert await runner._consume_retry_command() == RetryDispatch.HANDLED
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored.status == RetryCommandStatus.FAILED
    assert "stale" in stored.outcome_reason
    assert stored.retry_count is None


@pytest.mark.asyncio
async def test_retry_restart_after_cleanup_deferral_keeps_one_allowance_and_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = PRInfo(number=42, branch="fix/pr-42", pr_id="PR-42", head_sha="abc")
    runner, command, _task, _repo = await _prepared_runner(
        tmp_path, command=_command(_task_text(), bound_pr=pr),
    )
    commits = []

    async def commit(*args, **kwargs):
        commits.append(args[1])
        return True

    async def cleanup_failure(*args):
        raise RuntimeError("cleanup store unavailable")

    _allow_validation(monkeypatch, runner, _task_text())
    monkeypatch.setattr(retry_module.gh_prs, "get_open_prs", lambda *args: [pr])
    monkeypatch.setattr(runner, "_commit_task_status_change", commit)
    monkeypatch.setattr(runner, "_clear_retry_failure_evidence", cleanup_failure)
    assert await runner._consume_retry_command() == RetryDispatch.HANDLED
    deferred = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert deferred.effect_stage == RetryEffectStage.STATUS_COMMITTED
    assert deferred.retry_count == 1

    restarted = h._make_runner()
    restarted.redis, restarted.repo_path = runner.redis, runner.repo_path
    restarted.state = RepoState.model_validate_json(runner.state.model_dump_json())
    _allow_validation(monkeypatch, restarted, _task_text())
    monkeypatch.setattr(restarted, "_commit_task_status_change", commit)
    assert await restarted._consume_retry_command() == RetryDispatch.WATCH
    stored = await load_retry_command(restarted.redis, restarted.name, command.command_id)
    assert stored.command_id == command.command_id
    assert stored.request_binding == command.request_binding
    assert stored.status == RetryCommandStatus.APPLIED
    assert stored.retry_count == 1
    assert commits == ["TODO"]
    assert restarted.redis.store[f"metrics:retry_count:{runner.name}:{command.task_id}"] == "1"


@pytest.mark.asyncio
@pytest.mark.parametrize("applied", [False, True])
@pytest.mark.parametrize("hold", ["pause", "stop", "budget"])
async def test_global_holds_precede_publication_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, applied: bool, hold: str,
) -> None:
    runner, command, _task, _repo = await _prepared_runner(tmp_path)
    _allow_validation(monkeypatch, runner, _task_text())
    runner._start_current_run_record("codex", "")
    runner._capture_invocation_snapshot("codex", {}, phase="coding")
    runner._begin_publication_attempt(started_at=NOW, baseline_numbers=set())
    attempt = runner.state.publication_attempt
    attempt.head_sha = "a" * 40
    attempt.exit_classification = "failure"
    if applied:
        command.effect_stage = RetryEffectStage.APPLIED
    if hold == "pause":
        runner.state.user_paused = True
    elif hold == "stop":
        runner.redis.store[control_stop(runner.name)] = "1"
    else:
        monkeypatch.setattr(retry_module, "derive_active_inhibitors", _async_value([
            WorkInhibitor(inhibitor_type=InhibitorType.GITHUB_BUDGET_PAUSE,
                          source_key="github-budget", reason_text="budget exhausted"),
        ]))
    recovered = []

    async def recover():
        recovered.append(True)
        return False

    monkeypatch.setattr(runner, "_resolve_published_attempt", _async_value(False))
    monkeypatch.setattr(runner, "_recover_branch_without_pr", recover)
    method = runner._reconcile_applied_retry if applied else runner._apply_retry_command
    assert await method(command) == RetryDispatch.HANDLED
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored.status == RetryCommandStatus.DEFERRED
    assert stored.retry_count is None
    assert attempt.pending
    assert recovered == []


@pytest.mark.asyncio
@pytest.mark.parametrize("new_failure", [False, True])
async def test_reserved_retry_restart_after_lost_status_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, new_failure: bool,
) -> None:
    runner, command, task, repo = await _prepared_runner(tmp_path)
    _allow_validation(monkeypatch, runner, _task_text())
    monkeypatch.setattr(runner, "_select_retry_continuation", _async_value(("coding", None)))
    monkeypatch.setattr(runner, "_ensure_retry_coder_available", _async_value(("codex", object())))
    commits = []
    todo = _task_text().replace("status: ERROR", "status: TODO").replace("blocked_reason: daemon\n", "")

    async def commit(*args):
        commits.append(args[1])
        (repo / command.task_file).write_text(todo)
        return True

    async def lost_checkpoint(*args):
        raise RuntimeError("checkpoint storage unavailable")

    monkeypatch.setattr(runner, "_commit_task_status_change", commit)
    monkeypatch.setattr(runner, "_record_retry_stage", lost_checkpoint)
    assert await runner._consume_retry_command() == RetryDispatch.HANDLED
    reserved = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert reserved.effect_stage == RetryEffectStage.RETRY_RESERVED
    assert reserved.retry_count == 1

    restarted = h._make_runner()
    restarted.redis, restarted.repo_path = runner.redis, runner.repo_path
    restarted.state = RepoState.model_validate_json(runner.state.model_dump_json())
    restarted.state.current_queue = [task.model_copy(update={"status": TaskStatus.TODO})]
    if new_failure:
        restarted.state.error_message = "a newer failure"
    _allow_validation(monkeypatch, restarted, todo)
    monkeypatch.setattr(restarted, "_select_retry_continuation", _async_value(("coding", None)))
    monkeypatch.setattr(restarted, "_ensure_retry_coder_available", _async_value(("codex", object())))
    monkeypatch.setattr(restarted, "_commit_task_status_change", commit)
    outcome = await restarted._consume_retry_command()
    stored = await load_retry_command(restarted.redis, restarted.name, command.command_id)
    assert stored.command_id == command.command_id
    assert stored.request_binding == command.request_binding
    assert stored.retry_count == 1
    assert restarted.redis.store[f"metrics:retry_count:{runner.name}:{command.task_id}"] == "1"
    assert outcome == (RetryDispatch.HANDLED if new_failure else RetryDispatch.CODING)
    assert stored.status == (RetryCommandStatus.FAILED if new_failure else RetryCommandStatus.APPLIED)
    assert commits == (["TODO"] if new_failure else ["TODO", "TODO"])


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "message", "cause", "pr", "head"])
@pytest.mark.parametrize("lost_checkpoint", [False, True])
async def test_recovered_retry_publication_preserves_fallback_failure_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str | None,
    lost_checkpoint: bool,
) -> None:
    runner, command, task, repo = await _prepared_runner(tmp_path)
    _allow_validation(monkeypatch, runner, _task_text())
    runner._start_current_run_record("codex", "")
    runner._capture_invocation_snapshot("codex", {}, phase="coding")
    runner._begin_publication_attempt(started_at=NOW, baseline_numbers=set())
    pr = PRInfo(number=42, branch=task.branch, pr_id=task.pr_id, head_sha="a" * 40)
    attempt = runner.state.publication_attempt
    attempt.pr_number, attempt.candidate, attempt.head_sha = pr.number, pr, pr.head_sha
    attempt.exit_classification = "failure"
    monkeypatch.setattr(retry_module.gh_prs, "get_exact_pr_outcome", lambda *args:
        retry_module.gh_prs.ExactPROutcome("octo/demo", "main", 42, pr.head_sha,
                                         retry_module.gh_prs.ExactPRState.OPEN, True))
    monkeypatch.setattr(retry_module.gh_prs, "get_open_prs", lambda *args: [pr])
    commits = []
    record_stage = runner._record_retry_stage
    todo = _task_text().replace("status: ERROR", "status: TODO").replace("blocked_reason: daemon\n", "")

    async def commit(*args):
        commits.append(args[1])
        if lost_checkpoint:
            (repo / command.task_file).write_text(todo)
        return lost_checkpoint or len(commits) > 1

    async def checkpoint(*args):
        if len(commits) == 1:
            raise RuntimeError("checkpoint unavailable")
        return await record_stage(*args)

    monkeypatch.setattr(runner, "_commit_task_status_change", commit)
    monkeypatch.setattr(runner, "_record_retry_stage", checkpoint)
    assert await runner._consume_retry_command() == RetryDispatch.HANDLED
    reserved = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert reserved.effect_stage == RetryEffectStage.RETRY_RESERVED
    assert reserved.bound_pr_number == pr.number
    assert reserved.failure_id == command.failure_id
    assert runner.state.current_pr.number == pr.number
    assert not attempt.pending
    if lost_checkpoint:
        runner.state.current_queue = [task.model_copy(update={"status": TaskStatus.TODO})]
        _allow_validation(monkeypatch, runner, todo)
    if change == "message":
        runner.state.error_message = "new failure"
    elif change == "cause":
        runner.redis.store[cause_key(runner.name, task.pr_id)] = CancellationCause(
            category="ERROR", payload={"subsource": "crash", "run_id": "new"},
        ).to_redis()
    elif change == "pr":
        runner.state.current_pr = pr.model_copy(update={"number": 99})
    elif change == "head":
        runner.state.current_pr = pr.model_copy(update={"head_sha": "b" * 40})

    result = await runner._consume_retry_command()
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored.failure_id == command.failure_id
    assert stored.request_binding == command.request_binding
    assert stored.retry_count == 1
    assert runner.redis.store[f"metrics:retry_count:{runner.name}:{task.pr_id}"] == "1"
    if change:
        assert result == RetryDispatch.HANDLED
        assert stored.status == RetryCommandStatus.FAILED
        assert "stale" in stored.outcome_reason
        assert commits == ["TODO"]
    else:
        assert result == RetryDispatch.WATCH
        assert stored.status == RetryCommandStatus.APPLIED
        assert runner.state.state == PipelineState.WATCH
        assert runner.state.current_pr.number == pr.number
        assert commits == ["TODO", "TODO"]


@pytest.mark.asyncio
@pytest.mark.parametrize("limit_kind", ["session", "weekly"])
async def test_spend_deferred_coding_retry_preserves_failure_and_resumes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, limit_kind: str,
) -> None:
    runner, command, task, repo = await _prepared_runner(tmp_path)
    _allow_validation(monkeypatch, runner, _task_text())
    monkeypatch.setattr(retry_module.gh_prs, "get_open_prs", lambda *args: [])
    monkeypatch.setattr(retry_module.gh_prs, "get_merged_prs", lambda *args, **kw: [])
    monkeypatch.setattr(runner, "_refresh_auth_status_cache", _async_value(None))
    monkeypatch.setattr(retry_module, "resolve_active_coder", lambda *args, **kw:
                        SimpleNamespace(name="codex", plugin=object()))
    monkeypatch.setattr(runner, "_commit_task_status_change", _async_value(True))
    setattr(runner.app_config.daemon, f"spend_ceiling_{limit_kind}_percent", 50)
    resets_at = int((datetime.now(timezone.utc) + timedelta(hours=1)).timestamp())
    snapshot = UsageSnapshot(**{
        "session_percent": None, "session_resets_at": None,
        "weekly_percent": None, "weekly_resets_at": None,
        f"{limit_kind}_percent": 60,
        f"{limit_kind}_resets_at": resets_at,
        "fetched_at": 0,
    })
    runner._codex_usage_provider = h._FakeUsageProvider(snapshot=snapshot)
    original = (repo / command.task_file).read_bytes()

    # The real spend gate publishes PAUSED; neither that snapshot nor another
    # consumption may lose the fallback failure identity or reserve an attempt.
    for _ in range(2):
        assert await runner._consume_retry_command() == RetryDispatch.HANDLED
        stored = await load_retry_command(runner.redis, runner.name, command.command_id)
        assert stored.status == RetryCommandStatus.DEFERRED
        assert stored.failure_id == command.failure_id
        assert stored.request_binding == command.request_binding
        assert stored.retry_count is None
        assert stored.effect_stage == RetryEffectStage.NONE
        assert runner.state.state == PipelineState.PAUSED
        assert runner.state.error_message == "prior failure"
        assert task.status == TaskStatus.ERROR
        assert (repo / command.task_file).read_bytes() == original
        published = RepoState.model_validate_json(runner.redis.store[pipeline_state(runner.name)])
        assert published.error_message == "prior failure"
        assert retry_failure_identity(task, published, None) == command.failure_id

    # Once the recorded window expires, the same command checks fresh usage
    # and resumes exactly once. No cancellation cause supplies an alternate ID.
    expired = datetime.now(timezone.utc) - timedelta(seconds=1)
    runner.state.rate_limited_until = expired
    runner.state.rate_limited_coder_until["codex"] = expired
    runner._codex_usage_provider = h._FakeUsageProvider(
        snapshot=replace(snapshot, **{f"{limit_kind}_percent": 10})
    )
    assert await runner._consume_retry_command() == RetryDispatch.CODING
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored.status == RetryCommandStatus.APPLIED
    assert stored.command_id == command.command_id
    assert stored.failure_id == command.failure_id
    assert stored.request_binding == command.request_binding
    assert stored.retry_count == 1
    assert runner.state.error_message is None
    assert runner.state.state == PipelineState.CODING
    assert runner.redis.store[f"metrics:retry_count:{runner.name}:{command.task_id}"] == "1"
