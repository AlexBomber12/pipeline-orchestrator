"""Daemon contract tests for durable operator Retry commands."""

from __future__ import annotations

import asyncio
import subprocess
from datetime import datetime, timezone
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
from src.keyspace import retry_command, retry_command_pending
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus
from src.retry_commands import (
    RetryCommandStatus,
    RetryEffectStage,
    RetryExecutionState,
    enqueue_retry_command,
    load_retry_command,
    new_retry_command,
)
from src.subsource_registry import SuppressionReason
from src.suppression import SuppressionRecord
from src.task_attempts import load_attempt, new_attempt, save_attempt

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
    attempt_id: str | None = None,
):
    return new_retry_command(
        repo_slug="octo__demo",
        task_id=task_id,
        task_file=f"tasks/{task_id}.md",
        task_branch=branch,
        task_fingerprint=task_spec_content_hash(text),
        request_binding="b" * 64,
        failure_id="f" * 64,
        retry_cap=3,
        failure_subsource="crash",
        failure_created_at=NOW.isoformat(),
        bound_pr_number=bound_pr.number if bound_pr else None,
        bound_pr_branch=bound_pr.branch if bound_pr else None,
        bound_pr_head_sha=bound_pr.head_sha if bound_pr else None,
        attempt_id=attempt_id,
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
    assert result == RetryDispatch.HANDLED
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
    attempt_id = "replacement-attempt"
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
        command=_command(text, bound_pr=pr, attempt_id=attempt_id),
    )
    attempt = new_attempt(
        runner.repo_config.url,
        runner.state.current_task,
        text,
        attempt_id=attempt_id,
        previous_rejection="reject-binding",
    )
    await save_attempt(runner.redis, runner.name, attempt, expected=None)
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
    assert runner.state.current_task is not None
    assert runner.state.current_task.attempt_id == attempt_id
    await runner._save_current_run_record("success_merged")
    completed = await load_attempt(runner.redis, runner.name, "PR-42")
    assert completed is not None
    assert completed.completed
    assert completed.attempt_id == attempt_id
    assert completed.pr_number == 42
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored is not None
    assert stored.selected_continuation == "watch"
    assert "fix_iteration_count" in stored.reset_counters


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
