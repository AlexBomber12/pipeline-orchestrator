"""Confirmed FIX idle timeouts resume the same PR within the dispatch budget."""

from __future__ import annotations

import asyncio
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest
from src import codex_cli
from src.cancellation import (
    CancellationCause,
    cause_key,
    record_cancellation_cause,
)
from src.coders.claude import claude_cli
from src.daemon import retry_commands as retry_module
from src.daemon.retry_commands import RetryDispatch
from src.github import prs as gh_prs
from src.inhibitor import InhibitorType, WorkInhibitor
from src.keyspace import pipeline_state
from src.models import CIStatus, PipelineState, PRInfo, QueueTask, TaskStatus
from src.queue_parser import write_frontmatter_status
from src.subsource_registry import SuppressionReason

from tests.runner import _helpers as h


async def _published_coding_runner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.app_config.daemon.fix_iteration_cap = 3
    task_path = tmp_path / "tasks" / "PR-458.md"
    task_path.parent.mkdir(parents=True)
    task_path.write_text(
        "---\nstatus: DOING\n---\n\n# PR-458: Recover FIX\n\n"
        "Branch: fix/pr-458\n- Type: bugfix\n- Complexity: low\n"
        "- Depends on: none\n- Priority: 1\n- Coder: claude\n",
        encoding="utf-8",
    )
    # These recovery tests use an unversioned temporary task file. Model the
    # confirmed status write without invoking the isolated Git transport.
    async def commit_status(
        _self, _task, status, _reason, blocked_reason=None,
    ) -> bool:
        write_frontmatter_status(task_path, status, blocked_reason)
        return True

    monkeypatch.setattr(type(runner), "_commit_task_status_change", commit_status)
    monkeypatch.setattr(
        runner, "_remote_frontmatter_error_status_for_current_task",
        runner._frontmatter_error_status_for_current_task,
    )
    task = QueueTask(
        pr_id="PR-458", title="Recover FIX", status=TaskStatus.DOING,
        task_file="tasks/PR-458.md", branch="fix/pr-458", priority=1,
    )
    pr = PRInfo(number=458, branch=task.branch, pr_id=task.pr_id, head_sha="a" * 40)
    runner.state.current_task = task
    runner.state.current_queue = [task]
    runner.state.state = PipelineState.CODING
    runner._start_current_run_record("claude", "opus")
    runner._capture_invocation_snapshot("claude", {}, phase="coding")
    monkeypatch.setattr(runner, "_rehydrate_last_push_at", lambda _pr: None)
    monkeypatch.setattr(runner, "_post_codex_review", lambda _number: None)
    await runner._handoff_coding_pr_to_watch(pr, "Published PR -> WATCH")
    assert runner.state.state == PipelineState.WATCH
    assert runner._current_run_record.ended_at is not None
    return runner, task, pr, task_path


async def _watch_requires_fix(runner, pr, monkeypatch: pytest.MonkeyPatch) -> None:
    pr.ci_status = CIStatus.FAILURE
    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_a, **_k: [pr])
    monkeypatch.setattr(gh_prs, "get_pr_state", lambda *_a: "OPEN")
    monkeypatch.setattr(runner, "_approval_filtered_pr", lambda found: found)
    monkeypatch.setattr(runner, "_rehydrate_quarantine_from_pr_labels", h._async_cli_result(None))
    monkeypatch.setattr(runner, "_detect_external_quarantine_release", h._async_cli_result(None))
    monkeypatch.setattr(runner, "_maybe_reclassify_stuck_pending", h._async_cli_result(None))
    monkeypatch.setattr(runner, "_scan_pr_diff_once", h._async_cli_result(None))
    monkeypatch.setattr(runner, "_rehydrate_last_push_at", lambda _pr: None)
    await runner.handle_watch()


@pytest.mark.asyncio
async def test_published_coding_record_allows_two_real_fix_dispatches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, task, pr, _path = await _published_coding_runner(tmp_path, monkeypatch)
    coding_record = runner._current_run_record
    coding_record.tokens_in = 120
    coding_record.diff_lines_added = 8
    await runner._checkpoint_current_run_record()
    calls: list[int] = []

    async def fix(*_args, **_kwargs):
        calls.append(pr.number)
        return 0, "", ""

    monkeypatch.setattr(claude_cli, "fix_review_async", fix)
    monkeypatch.setattr("src.github.comments.post_comment", lambda *_a, **_k: None)
    await _watch_requires_fix(runner, pr, monkeypatch)
    assert runner.state.state == PipelineState.WATCH
    assert calls == [pr.number]
    fix_record = runner._current_run_record
    assert fix_record.run_id != coding_record.run_id
    assert fix_record.tokens_in == 0 and fix_record.diff_lines_added == 0
    await runner.publish_state()
    restarted = h._make_runner()
    restarted.redis = runner.redis
    restarted._metrics_store = runner._metrics_store
    restarted.repo_path = runner.repo_path
    await restarted._hydrate_current_task_from_persisted_state()
    await restarted._apply_recovery_decisions([task], [pr])
    assert restarted.state.state == PipelineState.WATCH
    await _watch_requires_fix(restarted, pr, monkeypatch)
    assert calls == [pr.number, pr.number]
    assert restarted._current_run_record.fix_iterations == 2
    historical = await runner._metrics_store.get(coding_record.run_id)
    assert historical.ended_at is not None
    assert historical.exit_reason == "coding_complete"
    assert historical.fix_iterations == 0
    assert historical.tokens_in == 120 and historical.diff_lines_added == 8


@pytest.mark.asyncio
async def test_published_coding_fix_records_selected_coder_and_base_sha(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, _task, pr, _path = await _published_coding_runner(tmp_path, monkeypatch)
    coding_record = runner._current_run_record
    codex = runner._registry.get("codex")
    runner.app_config.daemon.coder_settings = {"codex": {"model": "gpt-5.4"}}
    monkeypatch.setattr(runner, "_get_coder", lambda **_kwargs: ("codex", codex))
    monkeypatch.setattr(runner, "_reserve_coder_credentials", lambda *_a, **_k: True)
    monkeypatch.setattr(runner, "_git_rev_parse", lambda ref: "b" * 40)
    calls: list[int] = []

    async def fix(*_args, **kwargs):
        calls.append(kwargs["pr_id"])
        return 0, "", ""

    monkeypatch.setattr(codex_cli, "fix_review_async", fix)
    monkeypatch.setattr("src.github.comments.post_comment", lambda *_a, **_k: None)
    await _watch_requires_fix(runner, pr, monkeypatch)
    assert calls == ["PR-458"]
    record = await runner._metrics_store.get(runner._current_run_record.run_id)
    assert record.profile_id == f"codex:{codex.resolve_model(runner.app_config.daemon)}:container"
    assert record.base_sha == "b" * 40
    assert coding_record.profile_id == "claude:opus:container"


@pytest.mark.asyncio
async def test_published_coding_timeout_recovers_then_dispatches_fix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, task, pr, task_path = await _published_coding_runner(tmp_path, monkeypatch)

    async def hangs(*_args, **_kwargs):
        await asyncio.Future()
        return 0, "", ""

    async def timeout(_number, _limit, target, idle_flag):
        await asyncio.sleep(0)
        idle_flag["timed_out"] = True
        target.cancel()

    monkeypatch.setattr(claude_cli, "fix_review_async", hangs)
    monkeypatch.setattr(runner, "_monitor_fix_idle", timeout)
    monkeypatch.setattr("src.github.comments.post_comment", lambda *_a, **_k: None)
    await _watch_requires_fix(runner, pr, monkeypatch)
    assert runner.state.state == PipelineState.ERROR
    assert "blocked_reason: fix_idle_timeout" in task_path.read_text()
    await runner.publish_state()
    restarted = h._make_runner()
    restarted.redis = runner.redis
    restarted._metrics_store = runner._metrics_store
    restarted.repo_path = runner.repo_path
    await restarted._hydrate_current_task_from_persisted_state()
    await restarted._apply_recovery_decisions(
        [task.model_copy(update={"status": TaskStatus.ERROR})], [pr]
    )
    monkeypatch.setattr(restarted, "_retry_worktree_dirty", lambda: (False, ""))
    monkeypatch.setattr(
        restarted, "_remote_frontmatter_error_status_for_current_task",
        restarted._frontmatter_error_status_for_current_task,
    )
    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_a, **_k: [pr])
    await restarted.handle_error()
    assert restarted.state.state == PipelineState.WATCH
    assert "status: TODO" in task_path.read_text()
    assert restarted._current_run_record.ended_at is None
    assert restarted._current_run_record.fix_iterations == 1
    calls: list[int] = []

    async def fix(*_args, **_kwargs):
        calls.append(pr.number)
        return 0, "", ""

    monkeypatch.setattr(claude_cli, "fix_review_async", fix)
    await _watch_requires_fix(restarted, pr, monkeypatch)
    assert calls == [pr.number]
    assert restarted._current_run_record.fix_iterations == 2


@pytest.mark.asyncio
async def test_published_coding_cannot_replenish_exhausted_fix_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, _task, pr, task_path = await _published_coding_runner(tmp_path, monkeypatch)
    runner._current_run_record.fix_iterations = runner.app_config.daemon.fix_iteration_cap
    await runner._checkpoint_current_run_record()
    calls: list[int] = []

    async def fix(*_args, **_kwargs):
        calls.append(pr.number)
        return 0, "", ""

    monkeypatch.setattr(claude_cli, "fix_review_async", fix)
    await _watch_requires_fix(runner, pr, monkeypatch)
    assert calls == []
    assert runner._current_run_record.fix_iterations == 3
    assert runner._current_run_record.ended_at is not None
    assert runner.state.state == PipelineState.ERROR
    assert "status: ERROR" in task_path.read_text()


@pytest.mark.asyncio
async def test_published_coding_defers_fix_if_new_record_cannot_persist(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, _task, pr, _path = await _published_coding_runner(tmp_path, monkeypatch)
    coding_record = runner._current_run_record
    calls: list[int] = []

    async def fix(*_args, **_kwargs):
        calls.append(pr.number)
        return 0, "", ""

    async def unavailable():
        raise RuntimeError("metrics unavailable")

    monkeypatch.setattr(claude_cli, "fix_review_async", fix)
    monkeypatch.setattr(runner, "_checkpoint_current_run_record", unavailable)
    await _watch_requires_fix(runner, pr, monkeypatch)
    assert calls == []
    assert runner.state.state == PipelineState.WATCH
    assert runner._current_run_record.run_id == coding_record.run_id
    assert coding_record.ended_at is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("record_problem", ["completed", "unrelated"])
async def test_published_coding_rejects_ineligible_historical_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, record_problem: str,
) -> None:
    runner, _task, pr, _path = await _published_coding_runner(tmp_path, monkeypatch)
    record = runner._current_run_record
    if record_problem == "completed":
        record.exit_reason = "success_merged"
    else:
        record.task_id = "PR-999"
    calls: list[int] = []

    async def fix(*_args, **_kwargs):
        calls.append(pr.number)
        return 0, "", ""

    monkeypatch.setattr(claude_cli, "fix_review_async", fix)
    await _watch_requires_fix(runner, pr, monkeypatch)
    assert calls == []
    assert runner.state.state == PipelineState.WATCH
    assert runner._current_run_record.run_id == record.run_id


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", ["branch", "task_id"])
async def test_published_coding_rejects_unrelated_fix_pr(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mismatch: str,
) -> None:
    runner, _task, pr, _path = await _published_coding_runner(tmp_path, monkeypatch)
    historical = runner._current_run_record
    if mismatch == "branch":
        runner.state.current_pr = pr.model_copy(update={"branch": "fix/pr-999"})
    else:
        runner.state.current_pr = pr.model_copy(update={"pr_id": "PR-999"})

    async def allowed(*_args, **_kwargs):
        return True

    monkeypatch.setattr(runner, "_get_coder", lambda **_kwargs: ("claude", object()))
    monkeypatch.setattr(runner, "usage_gate", allowed)
    await runner.handle_fix()
    assert runner.state.state == PipelineState.WATCH
    assert runner._current_run_record.run_id == historical.run_id
    assert runner._current_run_record.fix_iterations == 0


async def _timeout_runner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, count: int = 1,
    baseline: int = 0,
):
    runner = h._make_runner()
    runner.app_config.daemon.fix_iteration_cap = 3
    runner.repo_path = str(tmp_path)
    task_path = tmp_path / "tasks" / "PR-458.md"
    task_path.parent.mkdir(parents=True)
    task_path.write_text(
        "---\nstatus: ERROR\nblocked_reason: fix_idle_timeout\n---\n\n"
        "# PR-458: Recover FIX\n\nBranch: fix/pr-458\n"
        "- Type: bugfix\n- Complexity: low\n- Depends on: none\n"
        "- Priority: 1\n- Coder: codex\n",
        encoding="utf-8",
    )
    task = QueueTask(
        pr_id="PR-458", title="Recover FIX", status=TaskStatus.ERROR,
        task_file="tasks/PR-458.md", branch="fix/pr-458", priority=1,
    )
    pr = PRInfo(number=458, branch=task.branch, pr_id=task.pr_id, head_sha="a" * 40)
    runner.state.current_task = task
    runner.state.current_queue = [task]
    runner.state.current_pr = pr
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "FIX idle timeout: no new push for 60s of active FIX time"
    runner._start_current_run_record("codex", "")
    runner._current_run_record.fix_iterations = count
    runner._current_run_record.fix_recovery_baseline = baseline
    runner._capture_invocation_snapshot("codex", {}, phase="fix", fix_iteration=count)
    await runner._checkpoint_current_run_record()
    await record_cancellation_cause(
        runner.redis, runner.name, task.pr_id,
        CancellationCause(
            category="ERROR",
            payload={
                "subsource": "fix_idle_timeout", "pr_number": pr.number,
                "branch": pr.branch, "run_id": runner._current_run_record.run_id,
            },
        ),
    )
    monkeypatch.setattr(runner, "_retry_worktree_dirty", lambda: (False, ""))
    async def no_inhibitors(*_args):
        return []

    monkeypatch.setattr(retry_module, "derive_active_inhibitors", no_inhibitors)
    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_a, **_k: [pr])

    async def commit(_task, status, _reason):
        write_frontmatter_status(task_path, status)
        return True

    monkeypatch.setattr(runner, "_commit_task_status_change", commit)
    monkeypatch.setattr(
        runner, "_remote_frontmatter_error_status_for_current_task",
        runner._frontmatter_error_status_for_current_task,
    )
    return runner, task, pr, task_path


@pytest.mark.asyncio
async def test_timeout_admission_reads_remote_base_after_isolated_status_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, task, _pr, task_path = await _timeout_runner(tmp_path, monkeypatch)
    remote = tmp_path / "remote.git"

    def git(*args: str, cwd: Path = tmp_path) -> str:
        return subprocess.run(
            ["git", *args], cwd=cwd, check=True, capture_output=True, text=True,
        ).stdout

    git("init", "--bare", str(remote))
    git("init", "-b", "main")
    git("config", "user.name", "Test User")
    git("config", "user.email", "test@example.com")
    git("remote", "add", "origin", str(remote))
    task_path.write_text(
        task_path.read_text().replace(
            "status: ERROR\nblocked_reason: fix_idle_timeout", "status: DOING"
        )
    )
    git("add", task.task_file)
    git("commit", "-m", "seed task")
    git("push", "-u", "origin", "main")
    runner.repo_config.url = str(remote)
    runner.repo_config.branch = "main"
    original_head = git("rev-parse", "HEAD").strip()
    original_writer = type(runner)._commit_task_status_change.__get__(runner)
    assert await original_writer(
        task, "ERROR", "FIX idle timeout",
        blocked_reason=SuppressionReason.FIX_IDLE_TIMEOUT,
    )
    assert "status: DOING" in task_path.read_text()
    assert "status: ERROR" in git(
        "--git-dir", str(remote), "show", f"main:{task.task_file}"
    )
    monkeypatch.setattr(runner, "_commit_task_status_change", original_writer)
    monkeypatch.setattr(
        runner, "_remote_frontmatter_error_status_for_current_task",
        type(runner)._remote_frontmatter_error_status_for_current_task.__get__(runner),
    )
    assert runner._remote_frontmatter_error_status_for_current_task() == (
        "error", SuppressionReason.FIX_IDLE_TIMEOUT
    ), runner.state.history

    await runner.handle_error()

    assert runner.state.state == PipelineState.WATCH, runner.state.history
    assert "status: TODO" in git(
        "--git-dir", str(remote), "show", f"main:{task.task_file}"
    )
    assert "status: DOING" in task_path.read_text()
    assert git("rev-parse", "HEAD").strip() == original_head
    assert await runner.redis.get(cause_key(runner.name, task.pr_id)) is None


@pytest.mark.asyncio
async def test_remote_timeout_status_reader_rejects_changed_base_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, task, _pr, task_path = await _timeout_runner(tmp_path, monkeypatch)
    remote = tmp_path / "remote.git"

    def git(*args: str) -> str:
        return subprocess.run(
            ["git", *args], cwd=tmp_path, check=True, capture_output=True, text=True,
        ).stdout

    git("init", "--bare", str(remote))
    git("init", "-b", "main")
    git("config", "user.name", "Test User")
    git("config", "user.email", "test@example.com")
    git("remote", "add", "origin", str(remote))
    git("add", task.task_file)
    git("commit", "-m", "seed task")
    git("push", "-u", "origin", "main")
    runner.repo_config.url = str(remote)
    runner.repo_config.branch = "main"
    reader = type(runner)._remote_frontmatter_error_status_for_current_task.__get__(runner)

    runner.state.current_task = None
    assert reader() == (None, None)
    runner.state.current_task = task.model_copy(update={"task_file": "tasks/PR-999.md"})
    assert reader() == (None, None)
    runner.state.current_task = task
    runner.repo_config.branch = "-unsafe"
    assert reader() == (None, None)
    runner.repo_config.branch = "main"
    runner.repo_config.url = str(tmp_path / "different.git")
    assert reader() == (None, None)
    runner.repo_config.url = str(remote)

    git("rm", task.task_file)
    git("commit", "-m", "remove task")
    git("push", "origin", "main")
    assert reader() == (None, None)

    task_path.parent.mkdir(exist_ok=True)
    task_path.write_text(
        "---\nstatus: ERROR\nblocked_reason: fix_idle_timeout\n---\n\n"
        "# PR-458: Recover FIX\n\nBranch: fix/pr-999\n"
        "- Type: bugfix\n- Complexity: low\n- Depends on: none\n"
        "- Priority: 1\n- Coder: codex\n"
    )
    git("add", task.task_file)
    git("commit", "-m", "replace with different branch")
    git("push", "origin", "main")
    assert reader() == (None, None)


@pytest.mark.asyncio
async def test_timeout_admits_same_pr_then_watch_uses_fresh_ci(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, task, pr, task_path = await _timeout_runner(tmp_path, monkeypatch)
    calls: list[str] = []
    monkeypatch.setattr(runner, "handle_coding", lambda: calls.append("coding"))
    await runner.handle_error()
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr.number == 458
    assert runner.state.current_task.pr_id == task.pr_id
    assert "status: TODO" in task_path.read_text()
    assert await runner.redis.get(cause_key(runner.name, task.pr_id)) is None
    assert calls == []

    monkeypatch.setattr(gh_prs, "get_pr_state", lambda *_a: "OPEN")
    monkeypatch.setattr(runner, "_approval_filtered_pr", lambda found: found)
    monkeypatch.setattr(runner, "_rehydrate_quarantine_from_pr_labels", h._async_cli_result(None))
    monkeypatch.setattr(runner, "_detect_external_quarantine_release", h._async_cli_result(None))
    monkeypatch.setattr(runner, "_maybe_reclassify_stuck_pending", h._async_cli_result(None))
    monkeypatch.setattr(runner, "_scan_pr_diff_once", h._async_cli_result(None))
    monkeypatch.setattr(runner, "_rehydrate_last_push_at", lambda _pr: None)

    async def fix():
        calls.append("fix")

    monkeypatch.setattr(runner, "handle_fix", fix)
    await runner.handle_watch()
    assert calls == []
    pr.ci_status = CIStatus.FAILURE
    await runner.handle_watch()
    assert calls == ["fix"]


@pytest.mark.asyncio
async def test_exhausted_count_keeps_error_and_operator_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, task, _pr, task_path = await _timeout_runner(tmp_path, monkeypatch, count=3)
    for _ in range(2):
        await runner.handle_error()
    assert runner.state.state == PipelineState.ERROR
    assert "status: ERROR" in task_path.read_text()
    assert "blocked_reason: fix_idle_timeout" in task_path.read_text()
    assert await runner.redis.get(cause_key(runner.name, task.pr_id))
    assert any("Operator Retry required" in e["event"] for e in runner.state.history)


@pytest.mark.asyncio
async def test_operator_retry_baseline_preserves_history_and_renews_allowance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, _task, _pr, _path = await _timeout_runner(
        tmp_path, monkeypatch, count=4, baseline=3
    )
    await runner.handle_error()
    assert runner.state.state == PipelineState.WATCH
    assert runner._current_run_record.fix_iterations == 4
    assert runner._current_run_record.fix_recovery_baseline == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "blocker",
    [
        "paused", "stop", "cleanup", "status", "redis", "identity",
        "escalate", "guardrail", "binding", "dirty", "header", "record",
    ],
)
async def test_unsafe_recovery_keeps_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, blocker: str,
) -> None:
    runner, task, pr, task_path = await _timeout_runner(tmp_path, monkeypatch)
    if blocker == "paused":
        runner.state.user_paused = True
    elif blocker == "stop":
        async def stopped(*_args):
            return [WorkInhibitor(
                inhibitor_type=InhibitorType.USER_STOP,
                source_key="control:stop", reason_text="operator stop",
            )]

        monkeypatch.setattr(retry_module, "derive_active_inhibitors", stopped)
    elif blocker == "cleanup":
        async def unconfirmed():
            return True

        monkeypatch.setattr(runner, "_hold_for_coder_cleanup", unconfirmed)
    elif blocker == "status":
        async def failed_write(*_args):
            return False

        monkeypatch.setattr(runner, "_commit_task_status_change", failed_write)
    elif blocker == "redis":
        async def unavailable(*_args, **_kwargs):
            raise RuntimeError("redis down")

        monkeypatch.setattr(retry_module, "get_cancellation_cause", unavailable)
    elif blocker in {"escalate", "guardrail"}:
        original = await runner.redis.get(cause_key(runner.name, task.pr_id))
        cause = CancellationCause.from_redis(original)
        cause.payload["subsource"] = "coder_escalate" if blocker == "escalate" else "guardrail"
        await runner.redis.set(cause_key(runner.name, task.pr_id), cause.to_redis())
    elif blocker == "binding":
        original = await runner.redis.get(cause_key(runner.name, task.pr_id))
        cause = CancellationCause.from_redis(original)
        cause.payload["pr_number"] = 999
        await runner.redis.set(cause_key(runner.name, task.pr_id), cause.to_redis())
    elif blocker == "dirty":
        monkeypatch.setattr(runner, "_retry_worktree_dirty", lambda: (True, " M work.py"))
    elif blocker == "header":
        monkeypatch.setattr(
            runner, "_remote_frontmatter_error_status_for_current_task",
            lambda: (None, None),
        )
    elif blocker == "record":
        runner._current_run_record = None

        async def no_record():
            return None

        monkeypatch.setattr(runner, "_restore_current_run_record", no_record)
    else:
        monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_a, **_k: [
            pr.model_copy(update={"pr_id": "PR-999"})
        ])
    await runner.handle_error()
    assert runner.state.state == PipelineState.ERROR
    assert "status: ERROR" in task_path.read_text()
    assert await runner.redis.get(cause_key(runner.name, task.pr_id))


@pytest.mark.asyncio
async def test_missing_cause_or_bound_pr_never_admits_error_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, task, _pr, task_path = await _timeout_runner(tmp_path, monkeypatch)
    await runner.redis.delete(cause_key(runner.name, task.pr_id))
    await runner.handle_error()
    assert runner.state.state == PipelineState.ERROR
    assert "status: ERROR" in task_path.read_text()
    runner.state.current_pr = None
    await runner.handle_error()
    assert runner.state.state == PipelineState.ERROR


@pytest.mark.asyncio
async def test_failed_error_status_write_cannot_be_mistaken_for_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, task, _pr, task_path = await _timeout_runner(tmp_path, monkeypatch)
    write_frontmatter_status(task_path, "TODO")
    runner._status_write_failed_task_pr_ids.add(task.pr_id)
    writes: list[str] = []

    async def write(_task, status, _reason, **_kwargs):
        writes.append(status)
        return False

    monkeypatch.setattr(runner, "_commit_task_status_change", write)
    await runner.handle_error()
    assert writes == ["ERROR"]
    assert runner.state.state == PipelineState.ERROR
    assert await runner.redis.get(cause_key(runner.name, task.pr_id))

    async def repaired(_task, status, _reason, **_kwargs):
        write_frontmatter_status(
            task_path, status,
            SuppressionReason.FIX_IDLE_TIMEOUT if status == "ERROR" else None,
        )
        return True

    monkeypatch.setattr(runner, "_commit_task_status_change", repaired)
    await runner.handle_error()
    assert runner.state.state == PipelineState.WATCH
    assert task.pr_id not in runner._status_write_failed_task_pr_ids


@pytest.mark.asyncio
async def test_legacy_error_dispatch_invokes_timeout_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, _task, _pr, task_path = await _timeout_runner(tmp_path, monkeypatch)
    runner._recovered = True

    async def no_op(*_args, **_kwargs):
        return None

    async def false(*_args, **_kwargs):
        return False

    async def true(*_args, **_kwargs):
        return True

    async def no_retry(*_args, **_kwargs):
        return RetryDispatch.NONE

    monkeypatch.setattr(runner, "ensure_repo_cloned", no_op)
    monkeypatch.setattr(runner, "_refresh_user_paused_from_redis", no_op)
    monkeypatch.setattr(runner, "_consume_approval_command", false)
    async def pending_retry(*_args, **_kwargs):
        return RetryDispatch.HANDLED

    monkeypatch.setattr(runner, "_consume_retry_command", pending_retry)
    monkeypatch.setattr(runner, "_check_github_api_budget", true)
    monkeypatch.setattr(runner, "preflight", true)
    await runner._run_cycle_body()
    assert runner.state.state == PipelineState.ERROR
    assert "status: ERROR" in task_path.read_text()
    monkeypatch.setattr(runner, "_consume_retry_command", no_retry)
    await runner._run_cycle_body()
    assert runner.state.state == PipelineState.WATCH


@pytest.mark.asyncio
async def test_restart_restores_timeout_binding_and_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, task, pr, _path = await _timeout_runner(tmp_path, monkeypatch)
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    restarted = h._make_runner()
    restarted.redis = runner.redis
    restarted._metrics_store = runner._metrics_store
    restarted.repo_path = runner.repo_path
    await restarted._hydrate_current_task_from_persisted_state()
    await restarted._apply_recovery_decisions([task], [pr])
    assert restarted.state.state == PipelineState.ERROR
    assert restarted.state.current_pr.number == 458
    assert restarted._current_run_record.fix_iterations == 1
    monkeypatch.setattr(restarted, "_retry_worktree_dirty", lambda: (False, ""))
    monkeypatch.setattr(
        restarted, "_remote_frontmatter_error_status_for_current_task",
        restarted._frontmatter_error_status_for_current_task,
    )
    monkeypatch.setattr(restarted, "_commit_task_status_change", runner._commit_task_status_change)
    await restarted.handle_error()
    assert restarted.state.state == PipelineState.WATCH
    assert restarted._current_run_record.fix_iterations == 1


@pytest.mark.asyncio
async def test_restart_cannot_replenish_exhausted_dispatch_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, task, pr, task_path = await _timeout_runner(tmp_path, monkeypatch, count=3)
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    restarted = h._make_runner()
    restarted.redis = runner.redis
    restarted._metrics_store = runner._metrics_store
    restarted.repo_path = runner.repo_path
    await restarted._hydrate_current_task_from_persisted_state()
    await restarted._apply_recovery_decisions([task], [pr])
    await restarted.handle_error()
    assert restarted.state.state == PipelineState.ERROR
    assert restarted._current_run_record.fix_iterations == 3
    assert "blocked_reason: fix_idle_timeout" in task_path.read_text()
    assert await restarted.redis.get(cause_key(runner.name, task.pr_id))


@pytest.mark.asyncio
@pytest.mark.parametrize("saved_state", [PipelineState.FIX, PipelineState.WATCH])
async def test_restart_between_timeout_write_and_state_publish_recovers_only_confirmed_cause(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, saved_state: PipelineState,
) -> None:
    runner, task, pr, _path = await _timeout_runner(tmp_path, monkeypatch)
    runner.state.state = saved_state
    runner.state.error_message = None
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    restarted = h._make_runner()
    restarted.redis = runner.redis
    restarted._metrics_store = runner._metrics_store
    restarted.repo_path = runner.repo_path
    await restarted._hydrate_current_task_from_persisted_state()
    await restarted._apply_recovery_decisions([task], [pr])
    assert restarted.state.state == PipelineState.ERROR
    assert restarted.state.current_pr.number == pr.number
    assert restarted.state.error_message.startswith("FIX idle timeout")


@pytest.mark.asyncio
async def test_restart_with_unavailable_timeout_cause_does_not_admit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, task, pr, _path = await _timeout_runner(tmp_path, monkeypatch)
    runner.state.state = PipelineState.FIX
    runner.state.error_message = None
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    restarted = h._make_runner()
    restarted.redis = runner.redis
    restarted._metrics_store = runner._metrics_store
    restarted.repo_path = runner.repo_path
    await restarted._hydrate_current_task_from_persisted_state()

    async def unavailable(*_args, **_kwargs):
        raise RuntimeError("redis down")

    monkeypatch.setattr("src.daemon.recovery.get_cancellation_cause", unavailable)
    monkeypatch.setattr(retry_module, "get_cancellation_cause", unavailable)
    await restarted._apply_recovery_decisions([task], [pr])
    assert restarted.state.state == PipelineState.ERROR
    assert restarted.state.current_pr.number == pr.number
    await restarted.handle_error()
    assert restarted.state.state == PipelineState.ERROR
    assert "blocked_reason: fix_idle_timeout" in (tmp_path / task.task_file).read_text()


@pytest.mark.asyncio
@pytest.mark.parametrize("cause_available", [True, False])
async def test_restart_preserves_error_when_timeout_status_write_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cause_available: bool,
) -> None:
    runner, task, pr, task_path = await _timeout_runner(tmp_path, monkeypatch)
    write_frontmatter_status(task_path, "TODO")
    runner.state.state = PipelineState.FIX
    runner.state.error_message = None
    runner._status_write_failed_task_pr_ids.add(task.pr_id)
    await runner._persist_status_write_failed_task_pr_ids()
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    restarted = h._make_runner()
    restarted.redis = runner.redis
    restarted._metrics_store = runner._metrics_store
    restarted.repo_path = runner.repo_path
    await restarted._hydrate_current_task_from_persisted_state()
    await restarted._hydrate_status_write_failed_task_pr_ids()

    async def unavailable(*_args, **_kwargs):
        raise RuntimeError("cause read unavailable")

    if not cause_available:
        monkeypatch.setattr("src.daemon.recovery.get_cancellation_cause", unavailable)
    await restarted._apply_recovery_decisions(
        [task.model_copy(update={"status": TaskStatus.DOING})], [pr]
    )
    assert restarted.state.state == PipelineState.ERROR
    if cause_available:
        assert restarted.state.error_message.startswith("FIX idle timeout")
    else:
        assert restarted.state.error_message == "status:ERROR write failed; operator Retry required"
    assert restarted.state.current_pr.number == pr.number
    assert task.pr_id in restarted._status_write_failed_task_pr_ids
    assert "status: TODO" in task_path.read_text()
    assert await restarted.redis.get(cause_key(runner.name, task.pr_id))


@pytest.mark.asyncio
async def test_restart_does_not_relabel_review_status_failure_as_fix_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, task, pr, task_path = await _timeout_runner(tmp_path, monkeypatch)
    write_frontmatter_status(task_path, "TODO")
    await runner.redis.delete(cause_key(runner.name, task.pr_id))
    runner.state.state = PipelineState.WATCH
    runner.state.error_message = None
    await runner._mark_status_write_failed_task(
        task, blocked_reason=SuppressionReason.REVIEW_TIMEOUT,
        ensure_suppression=False,
    )
    restarted = h._make_runner()
    restarted.redis = runner.redis
    restarted._metrics_store = runner._metrics_store
    restarted.repo_path = runner.repo_path
    await restarted._hydrate_current_task_from_persisted_state()
    await restarted._hydrate_status_write_failed_task_pr_ids()
    await restarted._apply_recovery_decisions(
        [task.model_copy(update={"status": TaskStatus.DOING})], [pr]
    )
    assert restarted.state.state == PipelineState.ERROR
    assert restarted.state.error_message.startswith("review_timeout:")
    assert task.pr_id in restarted._status_write_failed_task_pr_ids
    reasons: list[SuppressionReason] = []

    async def commit(_task, status, _reason, *, blocked_reason):
        reasons.append(blocked_reason)
        write_frontmatter_status(task_path, status, blocked_reason)
        return True

    monkeypatch.setattr(restarted, "_commit_task_status_change", commit)
    await restarted.handle_error()
    assert reasons == [SuppressionReason.REVIEW_TIMEOUT]
    assert "blocked_reason: review_timeout" in task_path.read_text()
    assert restarted.state.state == PipelineState.ERROR


@pytest.mark.asyncio
async def test_unknown_status_write_failure_parks_until_write_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, task, _pr, task_path = await _timeout_runner(tmp_path, monkeypatch)
    runner.state.error_message = "status:ERROR write failed; operator Retry required"
    runner._status_write_failed_task_pr_ids.add(task.pr_id)
    writes: list[SuppressionReason] = []

    async def failing(_task, _status, _context, *, blocked_reason):
        writes.append(blocked_reason)
        return False

    monkeypatch.setattr(runner, "_commit_task_status_change", failing)
    await runner.handle_error()
    assert runner.state.state == PipelineState.ERROR
    assert task.pr_id in runner._status_write_failed_task_pr_ids
    assert writes == [SuppressionReason.CRASH]

    async def repaired(_task, status, _context, *, blocked_reason):
        write_frontmatter_status(task_path, status, blocked_reason)
        return True

    monkeypatch.setattr(runner, "_commit_task_status_change", repaired)
    await runner.handle_error()
    assert runner.state.state == PipelineState.ERROR
    assert task.pr_id not in runner._status_write_failed_task_pr_ids
    assert "blocked_reason: crash" in task_path.read_text()


@pytest.mark.asyncio
@pytest.mark.parametrize("header", ["guardrail", "missing"])
async def test_restart_does_not_relabel_other_or_unreadable_error_as_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, header: str,
) -> None:
    runner, task, pr, task_path = await _timeout_runner(tmp_path, monkeypatch)
    runner.state.state = PipelineState.FIX
    runner.state.error_message = None
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    if header == "guardrail":
        write_frontmatter_status(task_path, "ERROR", SuppressionReason.GUARDRAIL)
    else:
        task_path.unlink()
    restarted = h._make_runner()
    restarted.redis = runner.redis
    restarted._metrics_store = runner._metrics_store
    restarted.repo_path = runner.repo_path
    await restarted._hydrate_current_task_from_persisted_state()
    await restarted._apply_recovery_decisions([task], [pr])
    assert restarted.state.current_pr is None
    assert restarted.state.state == PipelineState.IDLE


@pytest.mark.asyncio
@pytest.mark.parametrize("accounting", ["unavailable", "missing", "exhausted"])
async def test_watch_fix_dispatch_requires_persisted_remaining_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, accounting: str,
) -> None:
    count = 3 if accounting == "exhausted" else 1
    runner, _task, _pr, _path = await _timeout_runner(
        tmp_path, monkeypatch, count=count
    )
    runner.state.state = PipelineState.WATCH

    async def no_op(*_args, **_kwargs):
        return None

    async def allowed(*_args, **_kwargs):
        return True

    monkeypatch.setattr(runner, "_refresh_auth_status_cache", no_op)
    monkeypatch.setattr(runner, "_get_coder", lambda **_kwargs: ("codex", object()))
    monkeypatch.setattr(runner, "usage_gate", allowed)
    runner._current_run_record = None
    if accounting == "unavailable":
        async def unavailable(**_kwargs):
            raise RuntimeError("metrics unavailable")

        monkeypatch.setattr(runner._metrics_store, "recent", unavailable)
    elif accounting == "missing":
        async def missing(**_kwargs):
            return []

        monkeypatch.setattr(runner._metrics_store, "recent", missing)
    else:
        seen: list[int] = []

        async def escalated(pr):
            seen.append(pr.number)

        monkeypatch.setattr(runner, "_escalate_fix_iteration_cap", escalated)
    await runner.handle_fix()
    if accounting in {"unavailable", "missing"}:
        assert runner.state.state == PipelineState.WATCH
        message = "accounting unavailable" if accounting == "unavailable" else "accounting missing"
        assert any(message in e["event"] for e in runner.state.history)
    else:
        assert seen == [458]


@pytest.mark.asyncio
async def test_watch_fix_defers_on_mismatched_active_invocation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, _task, _pr, _path = await _timeout_runner(tmp_path, monkeypatch)
    runner.state.state = PipelineState.WATCH
    runner.state.active_invocation = replace(
        runner.state.active_invocation, run_id="unrelated-run"
    )

    async def allowed(*_args, **_kwargs):
        return True

    monkeypatch.setattr(runner, "_refresh_auth_status_cache", h._async_cli_result(None))
    monkeypatch.setattr(runner, "_get_coder", lambda **_kwargs: ("codex", object()))
    monkeypatch.setattr(runner, "usage_gate", allowed)
    await runner.handle_fix()
    assert runner.state.state == PipelineState.WATCH
    assert runner._current_run_record.fix_iterations == 1


@pytest.mark.asyncio
async def test_restart_finishes_committed_admission_without_new_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, task, pr, task_path = await _timeout_runner(tmp_path, monkeypatch)

    async def delete_failed(*_args):
        raise RuntimeError("redis delete down")

    original_delete = retry_module.delete_cancellation_cause
    monkeypatch.setattr(retry_module, "delete_cancellation_cause", delete_failed)
    await runner.handle_error()
    assert runner.state.state == PipelineState.ERROR
    assert "status: TODO" in task_path.read_text()
    assert await runner.redis.get(cause_key(runner.name, task.pr_id))
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())

    restarted = h._make_runner()
    restarted.redis = runner.redis
    restarted._metrics_store = runner._metrics_store
    restarted.repo_path = runner.repo_path
    await restarted._hydrate_current_task_from_persisted_state()
    await restarted._apply_recovery_decisions(
        [task.model_copy(update={"status": TaskStatus.TODO})], [pr]
    )
    assert restarted.state.state == PipelineState.ERROR
    monkeypatch.setattr(restarted, "_retry_worktree_dirty", lambda: (False, ""))
    monkeypatch.setattr(
        restarted, "_remote_frontmatter_error_status_for_current_task",
        restarted._frontmatter_error_status_for_current_task,
    )
    monkeypatch.setattr(retry_module, "delete_cancellation_cause", original_delete)
    await restarted.handle_error()
    assert restarted.state.state == PipelineState.WATCH
    assert restarted._current_run_record.fix_iterations == 1
    assert await restarted.redis.get(cause_key(runner.name, task.pr_id)) is None


@pytest.mark.asyncio
async def test_restart_watch_keeps_bound_closed_pr_out_of_coding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, task, pr, _path = await _timeout_runner(tmp_path, monkeypatch)
    await runner.handle_error()
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
    assert restarted.state.current_pr.number == pr.number
    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_a, **_k: [])
    monkeypatch.setattr(gh_prs, "get_pr_state", lambda *_a: "CLOSED")
    seen: list[int] = []

    async def resolved(bound, state):
        assert state == "CLOSED"
        seen.append(bound.number)

    monkeypatch.setattr(restarted, "_handle_external_pr_resolution", resolved)
    await restarted.handle_watch()
    assert seen == [pr.number]


@pytest.mark.asyncio
async def test_unknown_pr_state_keeps_recovered_fix_in_watch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, _task, pr, _path = await _timeout_runner(tmp_path, monkeypatch)
    await runner.handle_error()
    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_a, **_k: [])
    monkeypatch.setattr(gh_prs, "get_pr_state", lambda *_a: None)
    monkeypatch.setattr(gh_prs, "is_pr_merged", lambda *_a: None)
    await runner.handle_watch()
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr.number == pr.number


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["MERGED", "CLOSED", None])
async def test_external_completion_uses_existing_terminal_handler(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, terminal: str | None,
) -> None:
    runner, _task, pr, _path = await _timeout_runner(tmp_path, monkeypatch)
    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_a, **_k: [])
    monkeypatch.setattr(gh_prs, "get_pr_state", lambda *_a: terminal)
    seen: list[tuple[int, str]] = []

    async def resolved(bound, state):
        seen.append((bound.number, state))

    monkeypatch.setattr(runner, "_handle_external_pr_resolution", resolved)
    await runner.handle_error()
    assert seen == ([(pr.number, terminal)] if terminal else [])
    assert runner.state.state == PipelineState.ERROR
