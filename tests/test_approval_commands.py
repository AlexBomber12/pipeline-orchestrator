"""Approval regressions use real checkouts; external execution stays mocked."""

from __future__ import annotations

import asyncio
import os
import subprocess
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from src.approval_commands import (
    ApprovalChanged,
    approval_index,
    approval_key,
    approval_recent_index,
    approval_task_path,
    build_approval,
    enqueue_approval,
    failure_identity,
    list_approvals,
    load_approval,
)
from src.cancellation.storage import CancellationCause, cause_key, index_key
from src.daemon import approval_commands as daemon_approval
from src.daemon import recovery as daemon_recovery
from src.daemon.guardrails import GuardrailViolation
from src.daemon.quarantine import OBSOLETE_SIZE_LABEL, continued_size_label
from src.keyspace import control_stop, pipeline_state
from src.models import PipelineState, PRInfo, QueueTask, RepoState, TaskStatus
from src.task_status import MergedState

from tests.runner import _helpers as h


@pytest.fixture(autouse=True)
def isolated_daemon_process_view(monkeypatch):
    """Model the daemon PID namespace, keeping real test child processes visible.

    Hosted CI has unrelated non-dumpable processes under the same UID. They
    correctly block a host-wide production probe but are not part of this
    temporary daemon's execution ownership. Permission-denial behavior is
    exercised separately; the real child/cwd regression remains unmocked.
    """
    original = Path.iterdir

    def iterdir(path):
        entries = original(path)
        if path != Path("/proc"):
            yield from entries
            return
        for entry in entries:
            if not entry.name.isdigit():
                continue
            try:
                fields = (entry / "stat").read_text().rsplit(")", 1)[1].split()
                if int(entry.name) == os.getpid() or int(fields[1]) == os.getpid():
                    yield entry
            except (OSError, IndexError):
                continue

    monkeypatch.setattr(Path, "iterdir", iterdir)


def git(repo, *args):
    return subprocess.run(
        ["git", "--no-optional-locks", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def snapshot(repo):
    return (
        git(repo, "rev-parse", "--abbrev-ref", "HEAD"),
        git(repo, "rev-parse", "HEAD"),
        git(repo, "show-ref"),
        (repo / ".git/index").read_bytes(),
        {
            str(p.relative_to(repo)): p.read_bytes()
            for p in repo.rglob("*")
            if p.is_file() and ".git" not in p.relative_to(repo).parts
        },
    )


async def recover_without_redis(repo, remote, pr, monkeypatch, *, suffix="fresh"):
    """Rebuild from the remote task header and modeled live GitHub PR."""
    fresh_repo = repo.parent / suffix
    git(repo.parent, "clone", "--branch", "main", str(remote), str(fresh_repo))
    fresh = h._make_runner()
    fresh.repo_path = str(fresh_repo)
    fresh.repo_config.url = fresh.state.url = str(remote)
    monkeypatch.setattr(daemon_recovery.gh_prs, "get_open_prs", lambda *a, **k: [pr])
    monkeypatch.setattr(
        daemon_recovery, "_resolve_merged_state", lambda *a, **k: MergedState(set(), set(), True),
    )
    assert fresh.redis.store == {}
    assert await fresh.recover_state()
    return fresh


def use_real_status_writer(runner, remote, monkeypatch):
    runner.repo_config.url = runner.state.url = str(remote)
    monkeypatch.setattr(
        runner, "_commit_task_status_change",
        runner.__class__._commit_task_status_change.__get__(runner),
    )


@pytest.fixture
async def approval(tmp_path, monkeypatch):
    remote = tmp_path / "remote.git"
    repo = tmp_path / "repo"
    git(tmp_path, "init", "--bare", str(remote))
    git(tmp_path, "init", "-b", "main", str(repo))
    git(repo, "config", "user.name", "Approval Test")
    git(repo, "config", "user.email", "approval@example.test")
    (repo / "tasks").mkdir()
    (repo / "tasks/PR-42.md").write_text(
        "---\nstatus: ERROR\nblocked_reason: guardrail\n---\n\n"
        "# PR-42: Preserve work\nBranch: fix/pr-42\n- Type: bugfix\n"
        "- Complexity: low\n- Depends on: none\n\n## Requirements\nKeep useful work.\n"
    )
    (repo / "implementation.py").write_text("original\n")
    git(repo, "add", ".")
    git(repo, "commit", "-m", "base")
    git(repo, "remote", "add", "origin", str(remote))
    git(repo, "push", "-u", "origin", "main")
    git(repo, "checkout", "-b", "fix/pr-42")
    (repo / "implementation.py").write_text("implementation\n")
    git(repo, "commit", "-am", "implementation")
    git(repo, "push", "-u", "origin", "fix/pr-42")
    runner = h._make_runner()
    runner.repo_path = str(repo)
    runner._recovered = True
    runner.state.state = PipelineState.ERROR
    runner.state.current_task = QueueTask(
        pr_id="PR-42",
        title="Preserve work",
        task_file="tasks/PR-42.md",
        branch="fix/pr-42",
        status=TaskStatus.ERROR,
    )
    runner.state.current_pr = PRInfo(
        number=42,
        branch="fix/pr-42",
        pr_id="PR-42",
        head_sha=git(repo, "rev-parse", "HEAD"),
        fix_iteration_count=2,
        no_push_fix_count=1,
        quarantine_labels={"quarantine:large_diff"},
    )
    runner.state.quarantined_prs = {42, 99}
    runner.state.error_message = "guardrail failure"
    cause = CancellationCause(
        category="ERROR",
        payload={
            "subsource": "guardrail",
            "category": "large_diff_threshold",
            "excerpt": "+1800 LOC",
        },
    )
    raw = cause.to_redis()
    await runner.redis.set(cause_key(runner.name, "PR-42"), raw)
    await runner.redis.zadd(index_key(runner.name), {"PR-42": 1})
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    command = build_approval(runner.name, runner.state, raw, repo)
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a: [command.pr])
    monkeypatch.setattr(daemon_approval.gh_runner, "run_gh", lambda *a, **k: "")
    monkeypatch.setattr(runner, "ensure_repo_cloned", AsyncMock(side_effect=AssertionError("approval must not clone")))
    monkeypatch.setattr(runner, "preflight", AsyncMock(side_effect=AssertionError("approval must not reset")))
    monkeypatch.setattr(runner, "handle_coding", AsyncMock(side_effect=AssertionError("approval must not code")))
    monkeypatch.setattr(runner, "handle_watch", AsyncMock())
    monkeypatch.setattr(runner, "_check_github_api_budget", AsyncMock(return_value=True))
    monkeypatch.setattr(runner, "_commit_task_status_change", AsyncMock(return_value=True))
    return runner, command, repo, remote


@pytest.mark.parametrize("single", [False, True])
async def test_dirty_approval_preserves_every_kind_of_work_across_cycles(approval, single):
    runner, command, repo, remote = approval
    runner.repo_config.feature_flags.use_single_error_exit = single
    # Local-only commit plus distinct staged/unstaged edits to the same file.
    (repo / "local-only.txt").write_text("valuable committed work")
    git(repo, "add", "local-only.txt")
    git(repo, "commit", "-m", "local only")
    (repo / "implementation.py").write_text("staged\n")
    git(repo, "add", "implementation.py")
    (repo / "implementation.py").write_text("unstaged\n")
    (repo / "notes.txt").write_text("untracked")
    with (repo / "tasks/PR-42.md").open("a") as f:
        f.write("Uncommitted revised requirements.\n")
    before, remote_before = snapshot(repo), git(remote, "show-ref")
    await enqueue_approval(runner.redis, command)
    for _ in range(5):
        await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "deferred" and "Checkout has" in saved.reason
    assert snapshot(repo) == before and git(remote, "show-ref") == remote_before
    assert await runner.redis.get(cause_key(runner.name, "PR-42"))
    assert runner.state.state == PipelineState.ERROR


@pytest.mark.parametrize("single", [False, True])
async def test_clean_approval_and_restart_resume_exact_pr_without_git_writes(approval, single):
    runner, command, repo, remote = approval
    runner.repo_config.feature_flags.use_single_error_exit = single
    before, remote_before = snapshot(repo), git(remote, "show-ref")
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "applied"
    assert not await runner.redis.get(cause_key(runner.name, "PR-42"))
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr.number == 42
    assert runner.state.current_pr.fix_iteration_count == 2
    assert runner.state.current_pr.no_push_fix_count == 1
    assert runner.state.quarantined_prs == {99}
    assert RepoState.model_validate_json(await runner.redis.get(pipeline_state(runner.name))) == runner.state
    # Publication cannot restore the old ERROR snapshot.
    await runner.publish_state()
    assert (
        RepoState.model_validate_json(await runner.redis.get(pipeline_state(runner.name))).state == PipelineState.WATCH
    )
    assert (await enqueue_approval(runner.redis, command)).status == "applied"
    for _ in range(2):
        await runner._run_cycle_body()
    assert runner.handle_watch.await_count == 2
    # Recreate daemon memory while retaining only Redis and the real checkout.
    runner._recovered = False
    runner.state = RepoState(url=runner.repo_config.url, name=runner.name)
    runner._approval_receipt = None
    await runner._run_cycle_body()
    assert runner.state.current_pr.number == 42 and runner.state.state == PipelineState.WATCH
    assert snapshot(repo) == before and git(remote, "show-ref") == remote_before


@pytest.mark.parametrize("single", [False, True])
@pytest.mark.parametrize("blocker", ["pause", "stop", "process", "unknown", "disabled", "queue", "active"])
async def test_approval_defers_for_execution_and_inhibitors(approval, monkeypatch, single, blocker):
    runner, command, repo, _ = approval
    runner.repo_config.feature_flags.use_single_error_exit = single
    await enqueue_approval(runner.redis, command)
    if blocker == "pause":
        runner.state.user_paused = True
    elif blocker == "stop":
        await runner.redis.set(control_stop(runner.name), "1")
    elif blocker == "process":
        runner._current_coder_process = object()
    elif blocker == "unknown":
        monkeypatch.setattr(daemon_approval, "checkout_process_blocker", lambda _: "Ownership is uncertain")
    elif blocker == "disabled":
        runner.repo_config.active = False
    elif blocker == "queue":
        runner.state.pending_queue_sync_branch = "queue-done-test"
    elif blocker == "active":
        runner.state.state = PipelineState.FIX
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    before = snapshot(repo)
    await runner._run_cycle_body()
    assert snapshot(repo) == before
    assert (await load_approval(runner.redis, runner.name, command.binding)).status == "deferred"
    assert await runner.redis.get(cause_key(runner.name, "PR-42"))


@pytest.mark.parametrize("changed", ["task", "pr", "failure", "repo", "task_id", "pr_id"])
async def test_stale_approval_never_clears_other_work(approval, monkeypatch, changed):
    runner, command, repo, _ = approval
    await enqueue_approval(runner.redis, command)
    if changed == "task":
        (repo / "tasks/PR-42.md").write_text((repo / "tasks/PR-42.md").read_text() + "Revised spec\n")
        git(repo, "commit", "-am", "revised requirements")
    elif changed == "pr":
        monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a: [])
        monkeypatch.setattr(daemon_approval.gh_prs, "get_pr_state", lambda *a: "CLOSED")
    elif changed == "failure":
        await runner.redis.set(
            cause_key(runner.name, "PR-42"),
            CancellationCause(category="ERROR", payload={"subsource": "operator_reject"}).to_redis(),
        )
    elif changed == "repo":
        command.repo_url = "https://github.com/other/repo.git"
        await runner.redis.set(approval_key(runner.name, command.binding), command.model_dump_json())
    elif changed == "task_id":
        runner.state.current_task.pr_id = "PR-other"
    elif changed == "pr_id":
        other = command.pr.model_copy(update={"pr_id": "PR-other"})
        monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a: [other])
    before = await runner.redis.get(cause_key(runner.name, "PR-42"))
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, command.binding)).status == "failed"
    assert await runner.redis.get(cause_key(runner.name, "PR-42")) == before


async def test_lost_exec_reply_recovers_receipt_without_duplicate_effect(approval, monkeypatch):
    runner, command, repo, _ = approval
    await enqueue_approval(runner.redis, command)
    original = runner.redis.transaction
    lost = False

    async def transaction(fn, *keys, **kwargs):
        nonlocal lost
        result = await original(fn, *keys, **kwargs)
        if len(keys) == 5 and not lost:
            lost = True
            raise ConnectionError("EXEC reply lost")
        return result

    monkeypatch.setattr(runner.redis, "transaction", transaction)
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, command.binding)).status == "applied"
    # Reconcile the durable WATCH outcome even when this process kept ERROR.
    await runner._run_cycle_body()
    assert runner.state.state == PipelineState.WATCH
    assert runner.redis.deleted.count(cause_key(runner.name, "PR-42")) == 1


async def test_git_and_redis_failure_do_not_acknowledge_application(approval, monkeypatch):
    runner, command, repo, _ = approval
    await enqueue_approval(runner.redis, command)
    monkeypatch.setattr(daemon_approval.git_ops, "_git", lambda *a: (_ for _ in ()).throw(OSError("git failed")))
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, command.binding)).status == "deferred"
    monkeypatch.setattr(runner.redis, "transaction", AsyncMock(side_effect=ConnectionError("Redis down")))
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, command.binding)).status == "deferred"
    assert runner.state.state == PipelineState.ERROR
    assert await runner.redis.get(cause_key(runner.name, "PR-42"))


async def test_approval_filters_only_exact_finding_and_bound_head(approval):
    runner, command, _, _ = approval
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    approved = GuardrailViolation(1, "large_diff_threshold", "+1800 LOC", "threshold")
    unrelated = GuardrailViolation(1, "large_diff_threshold", "different finding", "threshold")
    assert runner._approval_allows_violation(command.pr, approved)
    assert not runner._approval_allows_violation(command.pr, unrelated)
    assert not runner._approval_allows_violation(command.pr.model_copy(update={"head_sha": "new"}), approved)
    labels = {"quarantine:large_diff", "quarantine:workflow"}
    assert runner._unapproved_labels(command, labels) == {"quarantine:workflow"}


async def test_live_orphan_process_defers_without_touching_checkout(approval):
    runner, command, repo, _ = approval
    await enqueue_approval(runner.redis, command)
    proc = subprocess.Popen(["sleep", "30"], cwd=repo)
    try:
        before = snapshot(repo)
        await runner._run_cycle_body()
        saved = await load_approval(runner.redis, runner.name, command.binding)
        assert saved.status == "deferred" and str(proc.pid) in saved.reason
        assert snapshot(repo) == before
    finally:
        proc.terminate()
        proc.wait()


async def test_accepted_request_survives_restart_without_wake(approval):
    runner, command, _, _ = approval
    await enqueue_approval(runner.redis, command)
    runner._recovered = False
    runner.state = RepoState(url=runner.repo_config.url, name=runner.name)
    await runner._run_cycle_body()
    assert runner.state.state == PipelineState.WATCH
    assert (await load_approval(runner.redis, runner.name, command.binding)).status == "applied"


async def test_new_failure_during_application_is_fenced(approval, monkeypatch):
    runner, command, _, _ = approval
    await enqueue_approval(runner.redis, command)
    original = runner.redis.transaction
    replacement = CancellationCause(category="ERROR", payload={"subsource": "guardrail", "excerpt": "new"}).to_redis()

    async def transaction(fn, *keys, **kwargs):
        if len(keys) == 5:
            await runner.redis.set(cause_key(runner.name, "PR-42"), replacement)
        return await original(fn, *keys, **kwargs)

    monkeypatch.setattr(runner.redis, "transaction", transaction)
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, command.binding)).status == "failed"
    assert await runner.redis.get(cause_key(runner.name, "PR-42")) == replacement


@pytest.mark.parametrize("control", ["pause", "stop", "task"])
async def test_control_change_at_commit_is_fenced(approval, monkeypatch, control):
    runner, command, _, _ = approval
    await enqueue_approval(runner.redis, command)
    original = runner.redis.transaction

    async def transaction(fn, *keys, **kwargs):
        if len(keys) == 5:
            state = runner.state.model_copy(deep=True)
            if control == "pause":
                state.user_paused = True
            elif control == "task":
                state.current_task.pr_id = "PR-new"
            else:
                await runner.redis.set(control_stop(runner.name), "1")
            await runner.redis.set(pipeline_state(runner.name), state.model_dump_json())
        return await original(fn, *keys, **kwargs)

    monkeypatch.setattr(runner.redis, "transaction", transaction)
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, command.binding)).status != "applied"
    assert await runner.redis.get(cause_key(runner.name, "PR-42"))


async def test_failed_command_keeps_preservation_boundary(approval):
    runner, command, repo, _ = approval
    command.status = "failed"
    command.active = False
    await enqueue_approval(runner.redis, command)
    (repo / "notes").write_text("preserve me")
    before = snapshot(repo)
    for _ in range(4):
        await runner._run_cycle_body()
    assert snapshot(repo) == before


async def test_approved_watch_defers_if_work_appears_after_application(approval):
    runner, command, repo, _ = approval
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    (repo / "notes").write_text("new work")
    before = snapshot(repo)
    for _ in range(4):
        await runner._run_cycle_body()
    assert snapshot(repo) == before
    runner.handle_watch.assert_not_awaited()


async def test_unrelated_quarantine_is_preserved(approval):
    runner, command, _, _ = approval
    command.pr.quarantine_labels.add("quarantine:workflow")
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    assert runner.state.quarantined_prs == {42, 99}
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "deferred" and "another quarantine" in saved.reason
    assert await runner.redis.get(cause_key(runner.name, "PR-42"))


async def test_http_approval_acceptance_is_durable_and_has_no_git_side_effects(approval, monkeypatch, tmp_path):
    from fastapi.testclient import TestClient
    from src.web import app as web_app
    from src.web.routes import repo_control

    from tests.web.test_guardrail_decision_endpoint import _aioredis_factory, _GuardrailRedis

    runner, command, repo, remote = approval
    config = tmp_path / "config.yml"
    config.write_text("repositories:\n  - url: https://github.com/octo/demo.git\n    branch: main\n")
    monkeypatch.setattr(web_app, "CONFIG_PATH", config)
    # Actual web endpoint uses its managed directory layout.
    target = repo.parent / runner.name
    repo.rename(target)
    repo = target
    monkeypatch.setattr(web_app, "REPOS_DIR", str(repo.parent))
    redis = _GuardrailRedis(runner.redis.store)
    monkeypatch.setattr(web_app, "aioredis", _aioredis_factory(redis))
    monkeypatch.setattr(web_app, "publish_wake", AsyncMock(side_effect=ConnectionError("lost wake")))
    (repo / "notes").write_text("untracked valuable work")
    (repo / "implementation.py").write_text("staged\n")
    git(repo, "add", "implementation.py")
    (repo / "implementation.py").write_text("unstaged\n")
    (repo / "tasks/PR-42.md").write_text((repo / "tasks/PR-42.md").read_text() + "Revised local requirements\n")
    # Approving a displayed revision still must not change or commit it.
    command = build_approval(runner.name, runner.state, command.failure, repo)
    before, remote_before = snapshot(repo), git(remote, "show-ref")
    with TestClient(web_app.app) as client:
        with monkeypatch.context() as scoped:
            scoped.setattr(
                repo_control.subprocess, "run", lambda *a, **k: pytest.fail("HTTP must not execute Git/GitHub")
            )
            for _ in range(2):
                response = client.post(
                    f"/repos/{runner.name}/guardrail/PR-42/decision",
                    data={
                        "decision": "approve",
                        "binding": command.binding,
                    },
                )
                assert response.status_code == 202 and "waiting for the daemon" in response.text
    assert snapshot(repo) == before and git(remote, "show-ref") == remote_before
    assert RepoState.model_validate_json(redis.store[pipeline_state(runner.name)]).state == PipelineState.ERROR
    assert redis.store[cause_key(runner.name, "PR-42")]
    assert len(await list_approvals(redis, runner.name)) == 1
    # HTTP acceptance is consumable by the actual daemon without its wake message.
    runner.redis = redis
    runner.repo_path = str(repo)
    await runner._run_cycle_body()
    assert (await load_approval(redis, runner.name, command.binding)).status == "deferred"


@pytest.mark.parametrize("path", ["../outside", "/tmp/outside", "other/task.md"])
def test_task_path_cannot_escape_checkout(tmp_path, path):
    with pytest.raises(ApprovalChanged):
        approval_task_path(tmp_path, path)


def test_task_path_rejects_symlinks(tmp_path):
    (tmp_path / "tasks").symlink_to(tmp_path)
    with pytest.raises(ApprovalChanged):
        approval_task_path(tmp_path, "tasks/task.md")


@pytest.mark.parametrize("change", ["cause", "task", "branch", "head"])
async def test_build_rejects_ambiguous_identity(approval, change):
    runner, command, repo, _ = approval
    raw = command.failure
    if change == "cause":
        raw = CancellationCause(category="ERROR", payload={"subsource": "crash"}).to_redis()
    elif change == "task":
        runner.state.current_task = None
    elif change == "branch":
        runner.state.current_pr.branch = "different"
    else:
        runner.state.current_pr.head_sha = ""
    with pytest.raises(ApprovalChanged):
        build_approval(runner.name, runner.state, raw, repo)


@pytest.mark.parametrize("changed", ["cause", "state"])
async def test_enqueue_revalidates_binding(approval, changed):
    runner, command, _, _ = approval
    if changed == "cause":
        await runner.redis.delete(cause_key(runner.name, "PR-42"))
    else:
        runner.state.current_pr.number = 99
        await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    with pytest.raises(ApprovalChanged):
        await enqueue_approval(runner.redis, command)
    assert not await load_approval(runner.redis, runner.name, command.binding)


async def test_missing_recovery_record_fails_closed(approval):
    runner, command, _, _ = approval
    runner.redis.zsets[approval_index(runner.name)] = {command.binding.encode(): 1}
    with pytest.raises(ApprovalChanged, match="missing"):
        await list_approvals(runner.redis, runner.name)


async def test_size_only_store_recovers_without_operator_approval(approval):
    runner, _, _, _ = approval
    assert await runner._consume_approval_command()
    assert runner.state.state == PipelineState.WATCH
    assert await runner.redis.get(cause_key(runner.name, "PR-42")) is None


async def test_size_recovery_removes_only_obsolete_github_label(approval, monkeypatch):
    runner, command, _, _ = approval
    marker = continued_size_label(command.pr.head_sha)
    calls = []
    monkeypatch.setattr(daemon_approval.gh_runner, "run_gh", lambda args, **kwargs: calls.append((args, kwargs)))
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, command.binding)).status == "applied"
    assert calls == [
        (["label", "create", marker, "--color", "0E8A16",
          "--description", "Legacy size decision continued; CI and review still required"],
         {"repo": runner.owner_repo}),
        (["pr", "edit", "42", "--add-label", marker], {"repo": runner.owner_repo}),
        (["pr", "edit", "42", "--remove-label", OBSOLETE_SIZE_LABEL], {"repo": runner.owner_repo}),
    ]


async def test_size_recovery_creates_marker_without_obsolete_label(approval, monkeypatch):
    runner, command, _, _ = approval
    command.pr.quarantine_labels.clear()
    runner.state.current_pr.quarantine_labels.clear()
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    calls = []
    monkeypatch.setattr(daemon_approval.gh_runner, "run_gh", lambda args, **kwargs: calls.append(args))
    await runner._run_cycle_body()
    marker = continued_size_label(command.pr.head_sha)
    assert (await load_approval(runner.redis, runner.name, command.binding)).status == "applied"
    assert [call[:4] for call in calls] == [
        ["label", "create", marker, "--color"],
        ["pr", "edit", "42", "--add-label"],
    ]


async def test_aborted_size_approval_never_publishes_continuation_marker(approval, monkeypatch):
    runner, command, _, _ = approval
    command.pr.quarantine_labels.clear()
    runner.state.current_pr.quarantine_labels.clear()
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    calls = []
    monkeypatch.setattr(daemon_approval.gh_runner, "run_gh", lambda args, **kwargs: calls.append(args))
    original = runner.redis.transaction

    async def abort_commit(fn, *keys, **kwargs):
        if len(keys) == 5:
            raise RuntimeError("approval transaction aborted")
        return await original(fn, *keys, **kwargs)

    monkeypatch.setattr(runner.redis, "transaction", abort_commit)
    await runner._run_cycle_body()
    assert calls == []
    assert all(continued_size_label(command.pr.head_sha) not in call for call in calls)
    assert runner.state.state == PipelineState.ERROR
    assert await runner.redis.get(cause_key(runner.name, "PR-42"))


@pytest.mark.parametrize("interrupt", ["replaced_cause", "abrupt_exit", "task", "pr"])
async def test_uncommitted_size_migration_stays_error_after_redis_loss(
    approval, monkeypatch, interrupt,
):
    runner, command, repo, remote = approval
    use_real_status_writer(runner, remote, monkeypatch)
    runner.state.current_pr.quarantine_labels.clear()
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    command = build_approval(
        runner.name, runner.state, await runner.redis.get(cause_key(runner.name, "PR-42")), repo,
    )
    command.pr.quarantine_labels.clear()
    await enqueue_approval(runner.redis, command)
    labels = set()
    live = command.pr.model_copy(update={"quarantine_labels": labels})
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a, **k: [live])
    gh_calls = []
    monkeypatch.setattr(daemon_approval.gh_runner, "run_gh", lambda args, **k: gh_calls.append(args))
    original = runner.redis.transaction

    async def interrupt_at_commit(fn, *keys, **kwargs):
        if len(keys) == 5:
            if interrupt == "abrupt_exit":
                raise KeyboardInterrupt("abrupt exit before Redis commit")
            if interrupt == "replaced_cause":
                await runner.redis.set(
                    cause_key(runner.name, "PR-42"),
                    CancellationCause(
                        category="ERROR", payload={"subsource": "guardrail", "category": "repo_delete"},
                    ).to_redis(),
                )
                # The new guardrail's best-effort GitHub label write failed.
            elif interrupt == "task":
                (repo / "tasks/PR-42.md").write_text(
                    (repo / "tasks/PR-42.md").read_text() + "Changed requirements\n"
                )
            else:
                state = runner.state.model_copy(deep=True)
                state.current_pr.number = 99
                await runner.redis.set(pipeline_state(runner.name), state.model_dump_json())
        return await original(fn, *keys, **kwargs)

    monkeypatch.setattr(runner.redis, "transaction", interrupt_at_commit)
    if interrupt == "abrupt_exit":
        with pytest.raises(KeyboardInterrupt):
            await runner._run_cycle_body()
    else:
        await runner._run_cycle_body()
    assert gh_calls == []
    assert git(remote, "show", "main:tasks/PR-42.md").startswith("---\nstatus: ERROR")
    if interrupt == "replaced_cause":
        assert "repo_delete" in await runner.redis.get(cause_key(runner.name, "PR-42"))
    fresh = await recover_without_redis(repo, remote, live, monkeypatch, suffix=f"fresh-{interrupt}")
    assert fresh.state.state == PipelineState.ERROR
    assert fresh.state.current_task.pr_id == "PR-42"
    assert fresh.state.current_pr.number == 42
    assert fresh.state.current_task.status == TaskStatus.ERROR


@pytest.mark.parametrize("obsolete_label", [False, True])
async def test_committed_size_migration_recovers_after_redis_loss(
    approval, monkeypatch, obsolete_label,
):
    runner, _, repo, remote = approval
    use_real_status_writer(runner, remote, monkeypatch)
    labels = {OBSOLETE_SIZE_LABEL} if obsolete_label else set()
    runner.state.current_pr.quarantine_labels = set(labels)
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    command = build_approval(
        runner.name, runner.state, await runner.redis.get(cause_key(runner.name, "PR-42")), repo,
    )
    await enqueue_approval(runner.redis, command)
    live = command.pr.model_copy(update={"quarantine_labels": labels})
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a, **k: [live])

    def update_labels(args, **kwargs):
        if "--add-label" in args:
            labels.add(args[-1])
        elif "--remove-label" in args:
            labels.discard(args[-1])

    monkeypatch.setattr(daemon_approval.gh_runner, "run_gh", update_labels)
    await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "applied" and saved.task_status_committed, runner.state.history[-5:]
    assert git(remote, "show", "main:tasks/PR-42.md").startswith("---\nstatus: TODO")
    assert labels == {continued_size_label(command.pr.head_sha)}
    fresh = await recover_without_redis(repo, remote, live, monkeypatch, suffix=f"fresh-{obsolete_label}")
    assert fresh.state.state == PipelineState.WATCH
    assert fresh.state.current_task.status == TaskStatus.DOING
    assert fresh.state.current_pr.number == 42


@pytest.mark.parametrize("legacy_cause", [False, True])
async def test_size_status_write_failure_retries_then_recovers_without_redis(approval, monkeypatch, legacy_cause):
    runner, _, repo, remote = approval
    use_real_status_writer(runner, remote, monkeypatch)
    if legacy_cause:
        runner.state.error_message = "[GUARDRAIL] tier=2 large_diff_threshold: +1800 LOC"
        await runner.redis.set(
            cause_key(runner.name, "PR-42"),
            CancellationCause(
                category="ERROR",
                payload={"subsource": "guardrail", "source": "github_quarantine_label", "pr_number": 42},
            ).to_redis(),
        )
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    command = build_approval(
        runner.name, runner.state, await runner.redis.get(cause_key(runner.name, "PR-42")), repo,
    )
    await enqueue_approval(runner.redis, command)
    writer = runner._commit_task_status_change
    attempts = 0

    async def flaky_writer(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        return False if attempts <= 2 else await writer(*args, **kwargs)

    monkeypatch.setattr(runner, "_commit_task_status_change", flaky_writer)
    labels = {OBSOLETE_SIZE_LABEL}
    live = command.pr.model_copy(update={"quarantine_labels": labels})
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a, **k: [live])
    monkeypatch.setattr(daemon_approval.gh_runner, "run_gh", lambda *a, **k: None)
    await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.size_only and not saved.task_status_committed
    assert git(remote, "show", "main:tasks/PR-42.md").startswith("---\nstatus: ERROR")
    before = await recover_without_redis(repo, remote, live, monkeypatch, suffix="fresh-before-retry")
    assert before.state.state == PipelineState.ERROR
    await runner._run_cycle_body()
    assert attempts == 2
    runner.handle_watch.assert_not_awaited()
    assert not (await load_approval(runner.redis, runner.name, command.binding)).task_status_committed
    await runner._run_cycle_body()
    assert attempts == 3
    assert (await load_approval(runner.redis, runner.name, command.binding)).task_status_committed
    assert git(remote, "show", "main:tasks/PR-42.md").startswith("---\nstatus: TODO")
    after = await recover_without_redis(repo, remote, live, monkeypatch, suffix="fresh-after-retry")
    assert after.state.state == PipelineState.WATCH


async def test_size_marker_write_failure_retries_after_durable_clearance(approval, monkeypatch):
    runner, _, repo, remote = approval
    use_real_status_writer(runner, remote, monkeypatch)
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    command = build_approval(
        runner.name, runner.state, await runner.redis.get(cause_key(runner.name, "PR-42")), repo,
    )
    await enqueue_approval(runner.redis, command)
    labels = {OBSOLETE_SIZE_LABEL}
    live = command.pr.model_copy(update={"quarantine_labels": labels})
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a, **k: [live])
    fail = True

    def flaky_github(args, **kwargs):
        nonlocal fail
        if fail:
            raise OSError("GitHub label unavailable")
        if "--add-label" in args:
            labels.add(args[-1])
        elif "--remove-label" in args:
            labels.discard(args[-1])

    monkeypatch.setattr(daemon_approval.gh_runner, "run_gh", flaky_github)
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, command.binding)).task_status_committed
    assert labels == {OBSOLETE_SIZE_LABEL}
    fresh = await recover_without_redis(repo, remote, live, monkeypatch, suffix="fresh-marker-failed")
    assert fresh.state.state == PipelineState.WATCH
    fail = False
    await runner._run_cycle_body()
    assert labels == {continued_size_label(command.pr.head_sha)}


async def test_size_status_confirmation_rechecks_binding_and_new_cause(approval, monkeypatch):
    runner, command, _, _ = approval
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    applied = await load_approval(runner.redis, runner.name, command.binding)
    assert applied.status == "applied"
    unconfirmed = applied.model_copy(update={"task_status_committed": False})
    await runner.redis.set(approval_key(runner.name, command.binding), unconfirmed.model_dump_json())
    changed_repo = unconfirmed.model_copy(update={"repo_url": "https://example.invalid/other.git"})
    assert not await runner._finish_size_approval(changed_repo, set())
    runner.state.current_pr.number = 99
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    assert not await runner._finish_size_approval(unconfirmed, set())
    runner.state.current_pr.number = command.pr.number
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())

    async def newer_cause_after_status_write(*args, **kwargs):
        await runner.redis.set(
            cause_key(runner.name, command.task.pr_id),
            CancellationCause(
                category="ERROR", payload={"subsource": "guardrail", "category": "repo_delete"},
            ).to_redis(),
        )
        return True

    monkeypatch.setattr(runner, "_commit_task_status_change", newer_cause_after_status_write)
    with pytest.raises(ApprovalChanged, match="before status confirmation"):
        await runner._finish_size_approval(unconfirmed, set())
    assert not (await load_approval(runner.redis, runner.name, command.binding)).task_status_committed


async def test_new_guardrail_after_success_overrides_stale_size_marker(approval, monkeypatch):
    runner, _, repo, remote = approval
    use_real_status_writer(runner, remote, monkeypatch)
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    command = build_approval(
        runner.name, runner.state, await runner.redis.get(cause_key(runner.name, "PR-42")), repo,
    )
    await enqueue_approval(runner.redis, command)
    labels = {OBSOLETE_SIZE_LABEL}
    live = command.pr.model_copy(update={"quarantine_labels": labels})
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a, **k: [live])

    def update_labels(args, **kwargs):
        if "--add-label" in args:
            labels.add(args[-1])
        elif "--remove-label" in args:
            labels.discard(args[-1])

    monkeypatch.setattr(daemon_approval.gh_runner, "run_gh", update_labels)
    await runner._run_cycle_body()
    assert labels == {continued_size_label(command.pr.head_sha)}
    assert await runner._commit_task_status_change(
        command.task, "ERROR", "new repository deletion guardrail", blocked_reason="guardrail",
    )
    assert git(remote, "show", "main:tasks/PR-42.md").startswith("---\nstatus: ERROR")
    # The newer guardrail's GitHub label write fails; only the old size-ok
    # label survives. It must not authorize the newer ERROR task.
    fresh = await recover_without_redis(repo, remote, live, monkeypatch, suffix="fresh-new-guardrail")
    assert fresh.state.state == PipelineState.ERROR
    assert fresh.state.current_task.status == TaskStatus.ERROR


async def test_status_clearance_rehydrates_stale_base_ref_after_redis_loss(approval, monkeypatch):
    runner, _, repo, remote = approval
    use_real_status_writer(runner, remote, monkeypatch)
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    command = build_approval(
        runner.name, runner.state, await runner.redis.get(cause_key(runner.name, "PR-42")), repo,
    )
    await enqueue_approval(runner.redis, command)
    live = command.pr.model_copy(update={"quarantine_labels": {OBSOLETE_SIZE_LABEL}})
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a, **k: [live])
    monkeypatch.setattr(daemon_approval.gh_runner, "run_gh", lambda *a, **k: None)
    stale_base = git(repo, "rev-parse", "refs/remotes/origin/main")
    await runner._run_cycle_body()
    assert git(repo, "rev-parse", "refs/remotes/origin/main") == stale_base
    fresh = h._make_runner()
    fresh.repo_path = str(repo)
    fresh.repo_config.url = fresh.state.url = str(remote)
    monkeypatch.setattr(daemon_recovery.gh_prs, "get_open_prs", lambda *a, **k: [live])
    monkeypatch.setattr(
        daemon_recovery, "_resolve_merged_state", lambda *a, **k: MergedState(set(), set(), True),
    )
    assert fresh.redis.store == {}
    assert await fresh.recover_state()
    assert fresh.state.state == PipelineState.WATCH
    assert git(repo, "rev-parse", "refs/remotes/origin/main") != stale_base


async def test_size_recovery_keeps_committed_status_when_marker_is_unavailable(approval, monkeypatch):
    runner, command, _, _ = approval
    command.pr.quarantine_labels.clear()
    runner.state.current_pr.quarantine_labels.clear()
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    monkeypatch.setattr(
        daemon_approval.gh_runner, "run_gh",
        lambda *a, **k: (_ for _ in ()).throw(OSError("offline")),
    )
    await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "applied" and saved.task_status_committed
    assert runner.state.state == PipelineState.WATCH
    assert await runner.redis.get(cause_key(runner.name, "PR-42")) is None


async def test_size_recovery_removes_label_missing_from_error_snapshot(approval, monkeypatch):
    runner, command, _, _ = approval
    marker = continued_size_label(command.pr.head_sha)
    runner.state.current_pr.quarantine_labels.clear()
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    calls = []
    monkeypatch.setattr(daemon_approval.gh_runner, "run_gh", lambda args, **kwargs: calls.append(args))
    await runner._run_cycle_body()
    assert runner.state.state == PipelineState.WATCH
    assert [call[:4] for call in calls] == [
        ["label", "create", marker, "--color"],
        ["pr", "edit", "42", "--add-label"],
        ["pr", "edit", "42", "--remove-label"],
    ]
    assert (await load_approval(runner.redis, runner.name, command.binding)).status == "applied"


async def test_size_label_cleanup_failure_does_not_reblock_watch(approval, monkeypatch):
    runner, command, _, _ = approval
    monkeypatch.setattr(daemon_approval.gh_runner, "run_gh", lambda *a, **k: (_ for _ in ()).throw(OSError("offline")))
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, command.binding)).status == "applied"
    assert runner.state.state == PipelineState.WATCH
    assert any("Size label migration deferred" in event["event"] for event in runner.state.history)


async def test_size_label_is_retained_if_continuation_marker_cannot_be_added(approval, monkeypatch):
    runner, command, _, _ = approval
    calls = []

    def run_gh(args, **kwargs):
        calls.append(args)
        if "--add-label" in args:
            raise OSError("GitHub label update unavailable")

    monkeypatch.setattr(daemon_approval.gh_runner, "run_gh", run_gh)
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, command.binding)).status == "applied"
    assert runner.state.state == PipelineState.WATCH
    assert len(calls) == 2
    assert "--add-label" in calls[-1]
    assert all("--remove-label" not in args for args in calls)


async def test_existing_continuation_label_still_allows_size_label_cleanup(approval, monkeypatch):
    runner, command, _, _ = approval
    calls = []

    def run_gh(args, **kwargs):
        calls.append(args)
        if args[:2] == ["label", "create"]:
            raise RuntimeError("label already exists")

    monkeypatch.setattr(daemon_approval.gh_runner, "run_gh", run_gh)
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, command.binding)).status == "applied"
    assert [args[3] for args in calls[1:]] == ["--add-label", "--remove-label"]


@pytest.mark.parametrize("restart", [False, True])
async def test_unrelated_rescue_branch_does_not_block_bound_pr(approval, restart):
    runner, command, repo, remote = approval
    git(repo, "checkout", "main")
    git(repo, "checkout", "-b", "rescue/old-work")
    (repo / "rescue.txt").write_text("preserve historical work\n")
    git(repo, "add", "rescue.txt")
    git(repo, "commit", "-m", "preserved rescue")
    rescue_sha = git(repo, "rev-parse", "HEAD")
    git(repo, "checkout", "main")
    before, remote_before = snapshot(repo), git(remote, "show-ref")
    await enqueue_approval(runner.redis, command)
    if restart:
        runner._recovered = False
        runner.state = RepoState(name=runner.name, url=runner.repo_config.url)
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, command.binding)).status == "applied"
    assert runner.state.state == PipelineState.WATCH
    assert git(repo, "rev-parse", "refs/heads/rescue/old-work") == rescue_sha
    assert snapshot(repo) == before and git(remote, "show-ref") == remote_before


async def test_main_checkout_does_not_hide_unpublished_bound_branch(approval):
    runner, command, repo, remote = approval
    (repo / "implementation.py").write_text("unpublished task change\n")
    git(repo, "commit", "-am", "unpublished task work")
    bound_sha = git(repo, "rev-parse", "refs/heads/fix/pr-42")
    git(repo, "checkout", "main")
    before, remote_before = snapshot(repo), git(remote, "show-ref")
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "deferred" and "Bound PR branch" in saved.reason
    assert git(repo, "rev-parse", "refs/heads/fix/pr-42") == bound_sha
    assert snapshot(repo) == before and git(remote, "show-ref") == remote_before


async def test_bound_checkout_rejects_invalid_or_unverifiable_ref(approval, monkeypatch):
    runner, command, repo, _ = approval
    changed = command.model_copy(deep=True)
    changed.task.branch = "other-branch"
    assert "disagree" in daemon_approval.bound_checkout_work_blocker(str(repo), changed)
    assert "disagree" in daemon_approval.bound_checkout_work_blocker_for_ref(
        str(repo), "bad..branch", {command.pr.head_sha}
    )
    original = daemon_approval.git_ops._git

    def failed_ref(path, *args, **kwargs):
        if args[:2] == ("rev-parse", "--verify") and len(args) == 3:
            return subprocess.CompletedProcess(args, 1, "", "ref unavailable")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(daemon_approval.git_ops, "_git", failed_ref)
    assert "could not be verified" in daemon_approval.bound_checkout_work_blocker(str(repo), command)


async def test_detached_unpublished_head_is_preserved(approval):
    runner, command, repo, _ = approval
    git(repo, "checkout", "--detach", "HEAD")
    (repo / "detached.txt").write_text("unpublished detached work\n")
    git(repo, "add", "detached.txt")
    git(repo, "commit", "-m", "detached work")
    before = snapshot(repo)
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "deferred" and "HEAD has local-only commits" in saved.reason
    assert snapshot(repo) == before


async def test_size_only_recovery_rejects_revised_task_generation(approval):
    runner, _, repo, _ = approval
    git(repo, "checkout", "main")
    with (repo / "tasks/PR-42.md").open("a") as handle:
        handle.write("Revised requirements after PR publication.\n")
    git(repo, "commit", "-am", "revise task")
    git(repo, "push", "origin", "main")
    await runner._run_cycle_body()
    assert runner.state.state == PipelineState.ERROR
    assert await runner.redis.get(cause_key(runner.name, "PR-42"))
    approvals = await list_approvals(runner.redis, runner.name)
    assert len(approvals) == 1
    assert approvals[0].status == "failed"
    assert "task generation" in approvals[0].reason


async def test_missing_github_snapshot_defers_without_clearing_size_block(approval, monkeypatch):
    runner, command, _, _ = approval
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a: [])
    monkeypatch.setattr(daemon_approval.gh_prs, "get_pr_state", lambda *a: None)
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "deferred" and "waiting for GitHub evidence" in saved.reason
    assert await runner.redis.get(cause_key(runner.name, "PR-42"))


@pytest.mark.parametrize("terminal", ["MERGED", "CLOSED"])
@pytest.mark.parametrize("restart", [False, True])
async def test_applied_approval_reconciles_confirmed_external_resolution(
    approval, monkeypatch, terminal, restart,
):
    runner, command, repo, _ = approval
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a: [])
    monkeypatch.setattr(daemon_approval.gh_prs, "get_pr_state", lambda *a: terminal)
    commit_status = AsyncMock(return_value=True)
    monkeypatch.setattr(runner, "_commit_task_status_change", commit_status)
    save_record = AsyncMock()
    monkeypatch.setattr(runner, "_save_current_run_record", save_record)
    if restart:
        runner._recovered = False
        runner.state = RepoState(name=runner.name, url=runner.repo_config.url)
        await runner._run_cycle_body()
    before = snapshot(repo)
    await runner._run_cycle_body()
    for _ in range(2):
        assert not await runner._consume_approval_command()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "applied" and not saved.active
    assert not await list_approvals(runner.redis, runner.name)
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None and runner.state.current_pr is None
    assert save_record.await_count == 1
    assert save_record.await_args.args == ("success_merged" if terminal == "MERGED" else "closed_unmerged",)
    assert commit_status.await_count == (1 if terminal == "MERGED" else 0)
    assert snapshot(repo) == before


@pytest.mark.parametrize("terminal", ["MERGED", "CLOSED"])
async def test_terminal_approval_clones_base_after_pr_branch_deletion(
    approval, monkeypatch, terminal,
):
    runner, command, repo, remote = approval
    runner.repo_config.url = runner.state.url = command.repo_url = str(remote)
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    preserved = repo.with_name("preserved-pr-checkout")
    before = snapshot(repo)
    repo.rename(preserved)
    git(remote, "update-ref", "refs/pull/42/head", command.pr.head_sha)
    git(remote, "update-ref", "-d", "refs/heads/fix/pr-42")
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a: [])
    monkeypatch.setattr(daemon_approval.gh_prs, "get_pr_state", lambda *a: terminal)
    monkeypatch.setattr(runner, "_save_current_run_record", AsyncMock())
    commit_status = AsyncMock(return_value=True)
    monkeypatch.setattr(runner, "_commit_task_status_change", commit_status)

    assert await runner._consume_approval_command()

    assert runner.state.state == PipelineState.IDLE, (
        (await load_approval(runner.redis, runner.name, command.binding)).reason, runner.state.history[-5:]
    )
    assert not (await load_approval(runner.redis, runner.name, command.binding)).active
    assert git(repo, "branch", "--show-current") == "main"
    assert snapshot(preserved) == before
    assert commit_status.await_count == (1 if terminal == "MERGED" else 0)


@pytest.mark.parametrize("terminal", [None, "OPEN"])
async def test_applied_approval_keeps_unknown_pr_evidence_retryable(approval, monkeypatch, terminal):
    runner, command, _, _ = approval
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a: [])
    monkeypatch.setattr(daemon_approval.gh_prs, "get_pr_state", lambda *a: terminal)
    for _ in range(3):
        await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.active and saved.status == "applied"
    assert runner.state.state == PipelineState.WATCH
    runner.handle_watch.assert_not_awaited()


@pytest.mark.parametrize("blocker", ["dirty", "binding", "changed_terminal", "handler"])
async def test_terminal_approval_waits_for_safe_reconciliation(approval, monkeypatch, blocker):
    runner, command, repo, _ = approval
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a: [])
    states = iter(["MERGED", "OPEN"] if blocker == "changed_terminal" else ["MERGED"] * 3)
    monkeypatch.setattr(daemon_approval.gh_prs, "get_pr_state", lambda *a: next(states))
    if blocker == "dirty":
        (repo / "operator-notes").write_text("unpublished\n")
    elif blocker == "binding":
        runner.state.current_pr.pr_id = "PR-other"
    elif blocker == "handler":
        monkeypatch.setattr(
            runner, "_handle_external_pr_resolution", AsyncMock(side_effect=RuntimeError("record unavailable")),
        )
    assert await runner._consume_approval_command()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.active and saved.status == "applied"
    assert runner.state.state == PipelineState.WATCH
    assert "deferred" in saved.reason


async def test_terminal_resolution_receipt_recovers_after_lost_retirement(approval, monkeypatch):
    runner, command, _, _ = approval
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a: [])
    monkeypatch.setattr(daemon_approval.gh_prs, "get_pr_state", lambda *a: "CLOSED")
    monkeypatch.setattr(runner, "_save_current_run_record", AsyncMock())
    retire = runner._retire_completed_approval
    monkeypatch.setattr(
        runner, "_retire_completed_approval", AsyncMock(side_effect=RuntimeError("receipt write lost")),
    )
    assert await runner._consume_approval_command()
    assert runner.state.state == PipelineState.IDLE
    monkeypatch.setattr(runner, "_retire_completed_approval", retire)
    assert not await runner._consume_approval_command()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert not saved.active and not await list_approvals(runner.redis, runner.name)


def external_published_head(repo, remote):
    external = repo.parent / "external-new-head"
    git(repo.parent, "clone", "--branch", "fix/pr-42", str(remote), str(external))
    git(external, "config", "user.name", "External Author")
    git(external, "config", "user.email", "external@example.test")
    (external / "implementation.py").write_text("externally published work\n")
    git(external, "commit", "-am", "publish externally")
    head = git(external, "rev-parse", "HEAD")
    git(external, "push", "origin", "fix/pr-42")
    assert subprocess.run(
        ["git", "-C", str(repo), "cat-file", "-e", head], capture_output=True,
    ).returncode != 0
    return head


@pytest.mark.parametrize("automatic", [False, True])
@pytest.mark.parametrize("transient_failure", [False, True])
async def test_missing_verified_head_fetches_without_changing_checkout_or_refs(
    approval, monkeypatch, automatic, transient_failure,
):
    runner, command, repo, remote = approval
    head = external_published_head(repo, remote)
    runner.state.current_pr.head_sha = head
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    fresh = command.pr.model_copy(update={"head_sha": head})
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a: [fresh])
    if not automatic:
        command = build_approval(
            runner.name, runner.state,
            await runner.redis.get(cause_key(runner.name, "PR-42")), repo,
        )
        await enqueue_approval(runner.redis, command)
    git(repo, "update-ref", "refs/rescue/operator", git(repo, "rev-parse", "HEAD"))
    before = snapshot(repo)
    original = subprocess.run
    failed = False

    def maybe_fail_fetch(args, **kwargs):
        nonlocal failed
        if transient_failure and not failed and isinstance(args, list) and "fetch" in args:
            failed = True
            return subprocess.CompletedProcess(args, 1, "", "transient remote error")
        return original(args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(daemon_approval.subprocess, "run", maybe_fail_fetch)
        await runner._run_cycle_body()
        if transient_failure:
            saved = (await list_approvals(runner.redis, runner.name))[0]
            assert saved.status == "deferred" and "fetch is unavailable" in saved.reason
            assert await runner.redis.get(cause_key(runner.name, "PR-42"))
            assert snapshot(repo) == before
            await runner._run_cycle_body()
    if automatic:
        command = (await list_approvals(runner.redis, runner.name))[0]
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "applied"
    assert runner.state.state == PipelineState.WATCH
    assert not await runner.redis.get(cause_key(runner.name, "PR-42"))
    assert snapshot(repo) == before
    assert git(remote, "show-ref")


@pytest.mark.parametrize("interference", ["pr", "state", "memory", "task", "dirty"])
async def test_missing_head_fetch_rechecks_binding_and_unpublished_work(
    approval, monkeypatch, interference,
):
    runner, _, repo, remote = approval
    head = external_published_head(repo, remote)
    runner.state.current_pr.head_sha = head
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    command = build_approval(
        runner.name, runner.state, await runner.redis.get(cause_key(runner.name, "PR-42")), repo,
    )
    await enqueue_approval(runner.redis, command)
    current = command.pr.model_copy(update={"head_sha": head})
    heads = [current]
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a: heads)
    original = subprocess.run

    def fetch_then_interfere(args, **kwargs):
        result = original(args, **kwargs)
        if isinstance(args, list) and "fetch" in args and result.returncode == 0:
            if interference == "pr":
                heads[0] = current.model_copy(update={"head_sha": "f" * 40})
            elif interference == "state":
                state = runner.state.model_copy(deep=True)
                state.current_pr.number = 99
                asyncio.run(runner.redis.set(pipeline_state(runner.name), state.model_dump_json()))
            elif interference == "memory":
                runner.state.current_pr.number = 99
            elif interference == "task":
                (repo / "tasks/PR-42.md").write_text(
                    (repo / "tasks/PR-42.md").read_text() + "New requirement\n"
                )
            else:
                (repo / "operator-notes").write_text("unpublished\n")
        return result

    with monkeypatch.context() as patch:
        patch.setattr(daemon_approval.subprocess, "run", fetch_then_interfere)
        await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status in {"failed", "deferred"}
    assert await runner.redis.get(cause_key(runner.name, "PR-42"))
    assert runner.state.state == PipelineState.ERROR


async def test_untrusted_published_head_cannot_be_fetched(approval, monkeypatch):
    runner, _, repo, _ = approval
    runner.state.current_pr.head_sha = "not-a-sha"
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    command = build_approval(
        runner.name, runner.state, await runner.redis.get(cause_key(runner.name, "PR-42")), repo,
    )
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a: [command.pr])
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "deferred" and "HEAD is invalid" in saved.reason
    assert await runner.redis.get(cause_key(runner.name, "PR-42"))


async def test_terminal_pr_can_resolve_when_task_object_needs_fetch(approval, monkeypatch):
    runner, command, _, _ = approval
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a: [])
    monkeypatch.setattr(daemon_approval.gh_prs, "get_pr_state", lambda *a: "CLOSED")
    monkeypatch.setattr(runner, "_save_current_run_record", AsyncMock())
    original = daemon_approval.git_ops._git
    first = True

    def unavailable_once(path, *args, **kwargs):
        nonlocal first
        if args and args[0] == "show" and first:
            first = False
            return subprocess.CompletedProcess(args, 128, "", "object unavailable")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(daemon_approval.git_ops, "_git", unavailable_once)
    assert await runner._consume_approval_command()
    assert runner.state.state == PipelineState.IDLE
    assert not (await load_approval(runner.redis, runner.name, command.binding)).active


async def test_terminal_state_change_during_head_fetch_stays_retryable(approval, monkeypatch):
    runner, command, _, _ = approval
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a: [])
    states = iter(["CLOSED", "MERGED"])
    monkeypatch.setattr(daemon_approval.gh_prs, "get_pr_state", lambda *a: next(states))
    original = daemon_approval.git_ops._git
    first = True

    def unavailable_once(path, *args, **kwargs):
        nonlocal first
        if args and args[0] == "show" and first:
            first = False
            return subprocess.CompletedProcess(args, 128, "", "object unavailable")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(daemon_approval.git_ops, "_git", unavailable_once)
    assert await runner._consume_approval_command()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.active and "deferred" in saved.reason
    assert runner.state.state == PipelineState.WATCH


async def test_automatic_size_recovery_keeps_unreadable_or_rebound_task(approval):
    runner, _, repo, _ = approval
    runner.repo_config.url = "https://github.com/other/repo.git"
    assert not await runner._consume_approval_command()
    assert any("repository binding changed" in event["event"] for event in runner.state.history)
    runner.repo_config.url = runner.state.url
    (repo / "tasks/PR-42.md").unlink()
    assert not await runner._consume_approval_command()
    assert any("Size-only decision retained" in event["event"] for event in runner.state.history)
    assert await runner.redis.get(cause_key(runner.name, "PR-42"))


async def test_changed_bound_pr_branch_fails_without_clearing_cause(approval, monkeypatch):
    runner, command, _, _ = approval
    changed = command.pr.model_copy(update={"branch": "different-branch"})
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a: [changed])
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "failed" and "branch changed" in saved.reason
    assert await runner.redis.get(cause_key(runner.name, "PR-42"))


async def test_unavailable_published_task_generation_defers(approval, monkeypatch):
    runner, command, _, _ = approval
    original = daemon_approval.git_ops._git

    def missing_task(path, *args, **kwargs):
        if args and args[0] == "show":
            return subprocess.CompletedProcess(args, 128, "", "missing object")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(daemon_approval.git_ops, "_git", missing_task)
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "deferred" and "task generation is unavailable" in saved.reason
    assert await runner.redis.get(cause_key(runner.name, "PR-42"))


async def test_second_checkout_check_defers_if_work_arrives_during_application(approval, monkeypatch):
    runner, command, _, _ = approval
    await enqueue_approval(runner.redis, command)
    monkeypatch.setattr(
        runner, "_approval_blocker", AsyncMock(side_effect=[None, "Checkout changed during verification."])
    )
    await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "deferred" and "Checkout changed" in saved.reason
    assert await runner.redis.get(cause_key(runner.name, "PR-42"))


async def test_published_later_push_releases_old_approval_with_stale_tracking_ref(
    approval, monkeypatch,
):
    runner, command, repo, _ = approval
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    (repo / "implementation.py").write_text("later published fix\n")
    git(repo, "commit", "-am", "later fix")
    new_head = git(repo, "rev-parse", "HEAD")
    git(repo, "push", "origin", "fix/pr-42")
    git(repo, "update-ref", "refs/remotes/origin/fix/pr-42", command.pr.head_sha)
    fresh = command.pr.model_copy(update={"head_sha": new_head})
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a: [fresh])
    await runner._run_cycle_body()
    assert runner.handle_watch.await_count == 1
    assert runner._approval_receipt is None
    assert (await list_approvals(runner.redis, runner.name))[0].status == "applied"
    assert runner._current_approval(fresh) is None
    assert runner.state.state == PipelineState.WATCH


@pytest.mark.parametrize("restart_before_observation", [False, True])
async def test_external_later_push_releases_receipt_without_local_commit(
    approval, monkeypatch, restart_before_observation,
):
    runner, command, repo, remote = approval
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    external = repo.parent / "external"
    git(repo.parent, "clone", "--branch", "fix/pr-42", str(remote), str(external))
    git(external, "config", "user.name", "External Author")
    git(external, "config", "user.email", "external@example.test")
    (external / "implementation.py").write_text("external fix\n")
    git(external, "commit", "-am", "external fix")
    new_head = git(external, "rev-parse", "HEAD")
    git(external, "push", "origin", "fix/pr-42")
    assert subprocess.run(
        ["git", "-C", str(repo), "cat-file", "-e", new_head], capture_output=True,
    ).returncode != 0
    fresh = command.pr.model_copy(update={"head_sha": new_head})
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a: [fresh])
    before = snapshot(repo)
    if restart_before_observation:
        runner._recovered = False
        runner.state = RepoState(name=runner.name, url=runner.repo_config.url)
        runner._approval_receipt = None
        await runner._run_cycle_body()
        assert runner.state.state == PipelineState.WATCH
    await runner._run_cycle_body()
    assert runner.handle_watch.await_count == 1
    assert runner._approval_receipt is None
    assert (await list_approvals(runner.redis, runner.name))[0].status == "applied"
    assert runner._current_approval(fresh) is None
    assert runner.state.state == PipelineState.WATCH
    runner._recovered = False
    runner.state = RepoState(name=runner.name, url=runner.repo_config.url)
    runner._approval_receipt = None
    await runner._run_cycle_body()
    assert runner.state.state == PipelineState.WATCH
    await runner._run_cycle_body()
    assert runner.handle_watch.await_count == 2
    assert runner.state.state == PipelineState.WATCH
    assert runner._current_approval(fresh) is None
    assert snapshot(repo) == before


@pytest.mark.parametrize("restart", [False, True])
async def test_size_only_recovery_retains_other_quarantine(approval, restart):
    runner, command, repo, _ = approval
    runner.state.current_pr.quarantine_labels.add("quarantine:workflow")
    command.pr.quarantine_labels.add("quarantine:workflow")
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    if restart:
        runner._recovered = False
        runner.state = RepoState(name=runner.name, url=runner.repo_config.url)
    await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "deferred" and "another quarantine" in saved.reason
    assert await runner.redis.get(cause_key(runner.name, "PR-42"))
    assert runner.state.state != PipelineState.WATCH
    assert (repo / "tasks/PR-42.md").exists()


async def test_legacy_label_suppression_recovers_only_matching_size_error(approval, monkeypatch):
    runner, command, _, _ = approval
    legacy = CancellationCause(
        category="ERROR",
        payload={"subsource": "guardrail", "source": "github_quarantine_label", "pr_number": 42},
    ).to_redis()
    runner.state.error_message = "[GUARDRAIL] tier=2 large_diff_threshold: +1800 LOC"
    await runner.redis.set(cause_key(runner.name, "PR-42"), legacy)
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    assert daemon_approval.is_size_only_cause(legacy, runner.state)
    runner.state.current_pr.quarantine_labels.add("quarantine:workflow")
    assert not daemon_approval.is_size_only_cause(legacy, runner.state)
    runner.state.current_pr.quarantine_labels.remove("quarantine:workflow")
    calls = []
    monkeypatch.setattr(daemon_approval.gh_runner, "run_gh", lambda args, **kwargs: calls.append(args))
    await runner._run_cycle_body()
    assert runner.state.state == PipelineState.WATCH
    assert await runner.redis.get(cause_key(runner.name, "PR-42")) is None
    assert (await load_approval(runner.redis, runner.name, command.binding)) is None
    assert [call[:4] for call in calls] == [
        ["label", "create", continued_size_label(command.pr.head_sha), "--color"],
        ["pr", "edit", "42", "--add-label"],
        ["pr", "edit", "42", "--remove-label"],
    ]


@pytest.mark.parametrize("prior", [PipelineState.CODING, PipelineState.FIX])
async def test_restart_does_not_assume_prior_execution_finished(approval, prior):
    runner, command, _, _ = approval
    await enqueue_approval(runner.redis, command)
    runner.state.state = prior
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    runner._recovered = False
    runner.state = RepoState(url=runner.repo_config.url, name=runner.name)
    await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "deferred" and "ownership is uncertain" in saved.reason


@pytest.mark.parametrize("single", [False, True])
@pytest.mark.parametrize("dirty", [False, True])
async def test_upload_retires_unapplied_approval_without_discarding_work(approval, single, dirty):
    from src.keyspace import upload_pending

    runner, command, repo, remote = approval
    runner.repo_config.feature_flags.use_single_error_exit = single
    await enqueue_approval(runner.redis, command)
    if dirty:
        (repo / "operator-notes").write_text("valuable work")
    before, remote_before = snapshot(repo), git(remote, "show-ref")
    await runner.redis.set(upload_pending(runner.name), "pending revised spec")
    await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "failed" and "upload superseded" in saved.reason
    assert await runner._consume_approval_command() == dirty
    assert await runner.redis.get(upload_pending(runner.name)) == "pending revised spec"
    assert await runner.redis.get(cause_key(runner.name, "PR-42"))
    assert snapshot(repo) == before and git(remote, "show-ref") == remote_before


async def test_work_starting_during_github_read_defers(approval, monkeypatch):
    runner, command, _, _ = approval
    await enqueue_approval(runner.redis, command)

    def lookup(*args):
        runner._current_coder_process = object()
        return [command.pr]

    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lookup)
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, command.binding)).status == "deferred"


@pytest.mark.parametrize("change", ["upload", "spec"])
async def test_late_revision_blocks_commit(approval, monkeypatch, change):
    from src.keyspace import upload_pending

    runner, command, repo, _ = approval
    await enqueue_approval(runner.redis, command)
    original = runner.redis.transaction

    async def transaction(fn, *keys, **kwargs):
        if len(keys) == 5:
            if change == "upload":
                await runner.redis.set(upload_pending(runner.name), "revision")
            else:
                (repo / "tasks/PR-42.md").write_text("changed just before transaction")
        return await original(fn, *keys, **kwargs)

    monkeypatch.setattr(runner.redis, "transaction", transaction)
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, command.binding)).status != "applied"
    assert await runner.redis.get(cause_key(runner.name, "PR-42"))


async def test_applied_receipt_never_clears_a_new_failure(approval):
    runner, command, _, _ = approval
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    new = CancellationCause(category="ERROR", payload={"subsource": "guardrail", "excerpt": "another finding"})
    await runner.redis.set(cause_key(runner.name, "PR-42"), new.to_redis())
    await runner._run_cycle_body()
    assert runner._approval_receipt is None
    assert await runner.redis.get(cause_key(runner.name, "PR-42")) == new.to_redis()
    runner.handle_watch.assert_not_awaited()


async def test_applied_receipt_ends_when_later_transition_supersedes_it(approval):
    runner, command, _, _ = approval
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    runner.state.state = PipelineState.ERROR
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    runner._recovered = False
    await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "applied" and not saved.active
    assert not await runner._consume_approval_command()


async def test_old_receipts_do_not_bind_a_different_active_task(approval):
    runner, command, _, _ = approval
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    runner.state.current_task.pr_id = "PR-other"
    assert not await runner._consume_approval_command()
    command.status = "failed"
    command.active = False
    await runner.redis.set(approval_key(runner.name, command.binding), command.model_dump_json())
    assert not await runner._consume_approval_command()


async def test_changed_task_invalidates_cached_permission(approval):
    runner, command, repo, _ = approval
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    (repo / "tasks/PR-42.md").write_text("different specification")
    assert runner._current_approval(command.pr) is None
    assert runner._approval_filtered_pr(command.pr).quarantine_labels == set()


def test_process_inspection_handles_exits_and_uncertain_ownership(tmp_path, monkeypatch):
    from types import SimpleNamespace

    class Process:
        name = "123456"

        def __truediv__(self, other):
            return self

        def resolve(self, **kwargs):
            raise self.error

        def stat(self):
            return SimpleNamespace(st_uid=self.uid)

    process = Process()
    monkeypatch.setattr(daemon_approval.Path, "iterdir", lambda _: [process])
    process.error = FileNotFoundError()
    assert daemon_approval.checkout_process_blocker(str(tmp_path)) is None
    process.error = PermissionError()
    process.uid = daemon_approval.os.getuid()
    assert "Cannot establish ownership" in daemon_approval.checkout_process_blocker(str(tmp_path))
    process.uid = -1
    assert daemon_approval.checkout_process_blocker(str(tmp_path)) is None


async def test_dashboard_retains_pending_deferred_applied_and_failed_results(approval, monkeypatch):
    from src.cancellation.storage import GuardrailPending
    from src.web import app as web_app
    from src.web.routes import dashboard

    from tests.web.test_guardrail_panel import _render_panel

    runner, command, repo, _ = approval
    target = repo.parent / runner.name
    repo.rename(target)
    runner.repo_path = str(target)
    monkeypatch.setattr(web_app, "REPOS_DIR", str(target.parent))
    pending = GuardrailPending(
        repo_slug=runner.name, task_id="PR-42", rule="large_diff_threshold", excerpt="+1800 LOC", recorded_at=1
    )
    monkeypatch.setattr(dashboard, "list_pending_guardrail_decisions", AsyncMock(return_value=[pending]))
    views = await dashboard._build_guardrail_pending_view(runner.redis, runner.name, runner.state)
    assert views[0]["approval_binding"] == command.binding
    assert "approve-btn" in _render_panel(guardrail_pending=views)
    await enqueue_approval(runner.redis, command)
    for status in ("pending", "deferred", "failed"):
        command.status = status
        command.reason = f"Visible {status} reason"
        await runner.redis.set(approval_key(runner.name, command.binding), command.model_dump_json())
        views = await dashboard._build_guardrail_pending_view(runner.redis, runner.name, runner.state)
        assert views[0]["approval"]["status"] == status
        assert f"Visible {status} reason" in _render_panel(guardrail_pending=views)
    command.status = "pending"
    await runner.redis.set(approval_key(runner.name, command.binding), command.model_dump_json())
    await runner._run_cycle_body()
    monkeypatch.setattr(dashboard, "list_pending_guardrail_decisions", AsyncMock(return_value=[]))
    views = await dashboard._build_guardrail_pending_view(runner.redis, runner.name, runner.state)
    assert views[0]["approval"]["status"] == "applied"
    html = _render_panel(guardrail_pending=views)
    assert 'data-approval-status="applied"' in html and "CI and review" in html
    assert "reject-btn" not in html
    # An unrelated applied receipt must not hide either action for a new cause.
    await runner.redis.set(
        cause_key(runner.name, "PR-42"),
        CancellationCause(
            category="ERROR", payload={"subsource": "guardrail", "category": "workflow_destruction"},
        ).to_redis(),
    )
    monkeypatch.setattr(dashboard, "list_pending_guardrail_decisions", AsyncMock(return_value=[pending]))
    views = await dashboard._build_guardrail_pending_view(runner.redis, runner.name, runner.state)
    assert views[0]["approval_binding"] != command.binding
    html = _render_panel(guardrail_pending=views)
    assert "approve-btn" in html and "reject-btn" in html


async def test_dashboard_unreadable_task_cannot_offer_unbound_approval(approval, monkeypatch):
    from src.cancellation.storage import GuardrailPending
    from src.web.routes import dashboard

    runner, command, _, _ = approval
    pending = GuardrailPending(repo_slug=runner.name, task_id="PR-42", rule="test", excerpt="x", recorded_at=1)
    monkeypatch.setattr(dashboard, "list_pending_guardrail_decisions", AsyncMock(return_value=[pending]))
    views = await dashboard._build_guardrail_pending_view(runner.redis, runner.name, runner.state)
    assert not views[0].get("approval_binding")


async def test_http_stale_binding_and_redis_failure_are_explicit(approval, monkeypatch):
    from redis.exceptions import RedisError
    from src.web import app as web_app
    from src.web.routes import repo_control

    runner, command, repo, _ = approval
    target = repo.parent / runner.name
    repo.rename(target)
    monkeypatch.setattr(web_app, "REPOS_DIR", str(target.parent))
    monkeypatch.setattr(
        repo_control, "_resolve_repo_task_path", AsyncMock(return_value=(target / "tasks/PR-42.md", "tasks/PR-42.md"))
    )
    # Missing/stale browser token never enqueues a request for the new decision.
    for token in ("", "stale-token"):
        response = await repo_control._approve_guardrail_decision(
            runner.name, "PR-42", runner.repo_config, runner.redis, token
        )
        assert response.status_code == 409
    await enqueue_approval(runner.redis, command)
    response = await repo_control._approve_guardrail_decision(
        runner.name, "PR-other", runner.repo_config, runner.redis, command.binding
    )
    assert response.status_code == 409
    await runner.redis.delete(pipeline_state(runner.name))
    response = await repo_control._approve_guardrail_decision(runner.name, "PR-42", runner.repo_config, runner.redis)
    assert response.status_code == 409
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    monkeypatch.setattr(repo_control, "build_approval", lambda *a: (_ for _ in ()).throw(OSError("unreadable task")))
    response = await repo_control._approve_guardrail_decision(runner.name, "PR-42", runner.repo_config, runner.redis)
    assert response.status_code == 503 and "Cannot read task" in response.body.decode()
    monkeypatch.setattr(runner.redis, "get", AsyncMock(side_effect=RedisError("unavailable")))
    response = await repo_control._approve_guardrail_decision(
        runner.name, "PR-42", runner.repo_config, runner.redis, command.binding
    )
    assert response.status_code == 503


async def test_reject_best_effort_label_failure_is_still_tolerated(monkeypatch):
    from src.web.routes import repo_control

    monkeypatch.setattr(repo_control, "_gh_subprocess", lambda *args: (1, "network error"))
    await repo_control._gh_best_effort("test", 42, "label", [])


async def test_watch_rescans_and_keeps_unrelated_findings(approval, monkeypatch):
    from src.daemon.handlers import watch

    runner, command, _, _ = approval
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    first = GuardrailViolation(1, "large_diff_threshold", "+1800 LOC", "threshold")
    other = GuardrailViolation(1, "workflow_destruction", "workflow changed", "workflow")
    monkeypatch.setattr(watch.guardrails, "_DIFF_PATTERNS", {"populated": object()})
    monkeypatch.setattr(watch.gh_prs, "get_pr_diff", lambda *a: "diff")
    monkeypatch.setattr(watch.guardrails, "scan_pr_diff", lambda *a, **k: [first, other])
    monkeypatch.setattr(watch, "apply_quarantine_label_for_violation", lambda *a: None)
    monkeypatch.setattr(runner, "_transition_to_error", AsyncMock())
    monkeypatch.setattr(runner, "_suppress_task", AsyncMock())
    assert await runner._scan_pr_diff_once()
    cause = runner._transition_to_error.call_args.kwargs["cancellation_cause"]
    assert cause.payload["category"] == "workflow_destruction"
    assert runner.state.current_pr.diff_scanned_at_sha == command.pr.head_sha


async def test_restart_of_failed_approval_preserves_work(approval):
    runner, command, repo, _ = approval
    command.status = "failed"
    command.active = False
    await enqueue_approval(runner.redis, command)
    (repo / "work").write_text("valuable work")
    before = snapshot(repo)
    runner._recovered = False
    runner.state = RepoState(name=runner.name, url=runner.repo_config.url)
    await runner._run_cycle_body()
    assert snapshot(repo) == before
    runner.ensure_repo_cloned.assert_not_awaited()


async def test_restart_retains_current_counters_and_queue_identity(approval):
    runner, command, _, _ = approval
    runner.state.current_queue = [
        runner.state.current_task,
        QueueTask(pr_id="PR-other", title="other", status=TaskStatus.TODO),
    ]
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    assert runner.state.current_queue[0].status == TaskStatus.DOING
    assert runner.state.current_queue[1].pr_id == "PR-other"
    runner.state.current_pr.watch_retrigger_count = 3
    await runner.publish_state()
    runner._recovered = False
    runner.state = RepoState(name=runner.name, url=runner.repo_config.url)
    await runner._run_cycle_body()
    assert runner.state.current_pr.watch_retrigger_count == 3


async def test_approval_cycles_are_serialized(approval):
    runner, command, _, _ = approval
    await enqueue_approval(runner.redis, command)
    await asyncio.gather(runner.run_cycle(), runner.run_cycle())
    assert runner.state.state == PipelineState.WATCH
    assert runner.redis.deleted.count(cause_key(runner.name, "PR-42")) == 1
    assert runner.handle_watch.await_count == 1


def test_failure_identity_ignores_json_field_order():
    assert failure_identity('{"a":1,"b":2}') == failure_identity(b'{"b":2,"a":1}')


def test_size_cause_does_not_accept_malformed_or_mixed_findings():
    assert not daemon_approval.is_size_only_cause(None)
    assert not daemon_approval.is_size_only_cause(
        CancellationCause(
            category="ERROR", payload={"subsource": "guardrail", "reason_text": "GUARDRAIL:broken"}
        ).to_redis()
    )
    assert not daemon_approval.is_size_only_cause(
        CancellationCause(
            category="ERROR",
            payload={"subsource": "guardrail", "category": "large_diff_threshold", "rule": "workflow_destruction"},
        ).to_redis()
    )


@pytest.mark.parametrize("single", [False, True])
async def test_clean_local_only_commits_are_preserved_by_deferral(approval, single):
    runner, command, repo, remote = approval
    runner.repo_config.feature_flags.use_single_error_exit = single
    (repo / "local-only.txt").write_text("valuable committed work")
    git(repo, "add", ".")
    git(repo, "commit", "-m", "local only")
    before, remote_before = snapshot(repo), git(remote, "show-ref")
    await enqueue_approval(runner.redis, command)
    for _ in range(4):
        await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "deferred" and "local-only commits" in saved.reason
    assert snapshot(repo) == before and git(remote, "show-ref") == remote_before


async def test_applied_continuation_reports_deferral_and_resume(approval):
    runner, command, repo, _ = approval
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    note = repo / "notes"
    note.write_text("temporary operator work")
    await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "applied" and "continuation deferred:" in saved.reason
    note.unlink()
    await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "applied" and "watching the existing PR" in saved.reason


async def test_quarantine_added_during_application_is_not_cleared(approval, monkeypatch):
    runner, command, _, _ = approval
    await enqueue_approval(runner.redis, command)
    original = runner.redis.transaction

    async def transaction(fn, *keys, **kwargs):
        if len(keys) == 5:
            state = runner.state.model_copy(deep=True)
            state.current_pr.quarantine_labels.add("quarantine:workflow")
            await runner.redis.set(pipeline_state(runner.name), state.model_dump_json())
        return await original(fn, *keys, **kwargs)

    monkeypatch.setattr(runner.redis, "transaction", transaction)
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, command.binding)).status == "deferred"
    assert await runner.redis.get(cause_key(runner.name, "PR-42"))


async def test_coder_reason_text_approval_matches_only_its_finding(approval):
    runner, command, _, _ = approval
    raw = CancellationCause(
        category="ERROR",
        payload={
            "subsource": "guardrail",
            "reason_text": "GUARDRAIL: large_diff_threshold: +1800 LOC",
        },
    ).to_redis()
    command.failure = failure_identity(raw)
    await runner.redis.set(cause_key(runner.name, "PR-42"), raw)
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, command.binding)).status == "applied"
    assert runner._approval_allows_violation(
        command.pr, GuardrailViolation(1, "large_diff_threshold", "+1800 LOC", "rule")
    )
    assert runner.state.quarantined_prs == {99}


async def test_watch_interprets_receipt_without_mutating_github_snapshot(approval):
    runner, command, _, _ = approval
    command.pr.is_escalated = True
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    filtered = runner._approval_filtered_pr(command.pr)
    assert filtered.quarantine_labels == set()
    assert not filtered.is_escalated
    assert command.pr.quarantine_labels == {"quarantine:large_diff"}
    assert command.pr.is_escalated


async def test_changed_spec_after_application_has_visible_deferral(approval):
    runner, command, repo, _ = approval
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    (repo / "tasks/PR-42.md").write_text("revised specification")
    git(repo, "commit", "-am", "revised locally")
    before = snapshot(repo)
    await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert "continuation deferred:" in saved.reason and "Task specification changed" in saved.reason
    assert snapshot(repo) == before


async def test_atomic_approval_clears_the_real_suppression_store_and_merge_gate(approval, monkeypatch):
    from src.daemon.handlers.merge import MergeMixin
    from src.subsource_registry import SuppressionReason

    runner, command, repo, _ = approval
    await runner._suppress_task(
        "PR-42",
        SuppressionReason.GUARDRAIL,
        {
            "pr_number": 42,
            "category": "large_diff_threshold",
            "excerpt": "+1800 LOC",
        },
    )
    assert (await runner._suppression_record_for_task("PR-42")).reason == SuppressionReason.GUARDRAIL
    raw = await runner.redis.get(cause_key(runner.name, "PR-42"))
    command = build_approval(runner.name, runner.state, raw, repo)
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    assert await runner._suppression_record_for_task("PR-42") is None
    assert "status: ERROR" in (repo / "tasks/PR-42.md").read_text()

    # Exercise the real MERGE suppression gate, then stop before any Git write.
    class ReachedMerge(BaseException):
        pass

    def stop_at_git(*args, **kwargs):
        raise ReachedMerge

    monkeypatch.setattr(daemon_approval.git_ops, "_git", stop_at_git)
    with pytest.raises(ReachedMerge):
        await MergeMixin.handle_merge(runner)


@pytest.mark.parametrize("action", ["reject", "upload", "retry"])
@pytest.mark.parametrize("restart", [False, True])
async def test_later_operator_action_supersedes_failed_approval(approval, action, restart):
    from datetime import timedelta

    from src.keyspace import upload_pending
    from src.retry_commands import enqueue_retry_command

    from tests.test_retry_commands import _new

    runner, command, _, _ = approval
    command.status = "failed"
    command.active = False
    await enqueue_approval(runner.redis, command)
    if action == "reject":
        await runner.redis.set(
            cause_key(runner.name, "PR-42"),
            CancellationCause(
                category="ERROR",
                payload={"subsource": "operator_reject"},
            ).to_redis(),
        )
    elif action == "upload":
        await runner.redis.set(upload_pending(runner.name), "later upload")
    else:
        retry = _new()
        retry.requested_at = command.requested_at + timedelta(seconds=1)
        await enqueue_retry_command(runner.redis, retry)
    if restart:
        runner._recovered = False
        runner.state = RepoState(name=runner.name, url=runner.repo_config.url)
    assert not await runner._consume_approval_command()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "failed" and saved.superseded
    assert not await runner._inactive_approval_holds(saved)
    assert not await runner._consume_approval_command()
    assert await runner.redis.get(cause_key(runner.name, "PR-42"))


@pytest.mark.parametrize("work", ["dirty", "coder", "unknown"])
async def test_superseding_action_waits_for_preserved_work(approval, monkeypatch, work):
    from src.keyspace import upload_pending

    runner, command, repo, _ = approval
    command.status = "failed"
    command.active = False
    await enqueue_approval(runner.redis, command)
    await runner.redis.set(upload_pending(runner.name), "later upload")
    if work == "dirty":
        (repo / "notes").write_text("preserve this")
    elif work == "coder":
        runner._current_coder_process = object()
    else:
        monkeypatch.setattr(daemon_approval, "checkout_process_blocker", lambda _: "ownership uncertain")
    before = snapshot(repo)
    assert await runner._consume_approval_command()
    assert snapshot(repo) == before
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert not saved.superseded and "Later operator action is waiting" in saved.reason


@pytest.mark.parametrize("single", [False, True])
@pytest.mark.parametrize("restart", [False, True])
async def test_upload_after_applied_approval_does_not_deadlock_watch(approval, single, restart):
    from src.keyspace import upload_pending
    runner, command, _, _ = approval
    runner.repo_config.feature_flags.use_single_error_exit = single
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    await runner.redis.set(upload_pending(runner.name), "staged upload for the IDLE consumer")
    if restart:
        runner._recovered = False
        runner.state = RepoState(name=runner.name, url=runner.repo_config.url)
        await runner._run_cycle_body()
        assert runner.state.state == PipelineState.WATCH
    await runner._run_cycle_body()
    runner.handle_watch.assert_awaited_once()
    assert await runner.redis.get(upload_pending(runner.name))
    # Completion of WATCH leaves the normal IDLE/upload path reachable.
    runner.state.state = PipelineState.IDLE
    runner.state.current_task = None
    runner.state.current_pr = None
    assert not await runner._consume_approval_command()


@pytest.mark.parametrize("single", [False, True])
@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize("deferred", [False, True])
async def test_rejection_supersedes_upload_blocked_approval(approval, single, restart, deferred):
    from src.keyspace import upload_pending

    runner, command, repo, remote = approval
    runner.repo_config.feature_flags.use_single_error_exit = single
    await enqueue_approval(runner.redis, command)
    if deferred:
        runner._current_coder_process = object()
        await runner._run_cycle_body()
        runner._current_coder_process = None
        assert (await load_approval(runner.redis, runner.name, command.binding)).status == "deferred"
    await runner.redis.set(upload_pending(runner.name), "staged upload for the IDLE consumer")
    rejection = CancellationCause(category="ERROR", payload={"subsource": "operator_reject"}).to_redis()
    await runner.redis.set(cause_key(runner.name, "PR-42"), rejection)
    if restart:
        runner._recovered = False
        runner.state = RepoState(name=runner.name, url=runner.repo_config.url)
    before, remote_before = snapshot(repo), git(remote, "show-ref")
    await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "failed" and not saved.active
    assert "Pending failure changed" in saved.reason
    # The existing control consumers can now run; approval does not consume
    # the newer rejection or upload and does not change the checkout.
    assert not await runner._consume_approval_command()
    assert (await load_approval(runner.redis, runner.name, command.binding)).superseded
    assert await runner.redis.get(cause_key(runner.name, "PR-42")) == rejection
    assert await runner.redis.get(upload_pending(runner.name))
    assert snapshot(repo) == before and git(remote, "show-ref") == remote_before


@pytest.mark.parametrize("single", [False, True])
async def test_restored_redis_approval_clones_absent_checkout_without_scaffolding(approval, single):
    runner, _, repo, remote = approval
    runner.repo_config.feature_flags.use_single_error_exit = single
    runner.repo_config.url = runner.state.url = str(remote)
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    raw = await runner.redis.get(cause_key(runner.name, "PR-42"))
    command = build_approval(runner.name, runner.state, raw, repo)
    await enqueue_approval(runner.redis, command)
    before, remote_before = snapshot(repo), git(remote, "show-ref")
    backup = repo.with_name("preserved-original")
    repo.rename(backup)
    runner._recovered = False
    runner.state = RepoState(name=runner.name, url=runner.repo_config.url)
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, command.binding)).status == "applied"
    assert runner.state.current_pr.number == 42 and runner.state.state == PipelineState.WATCH
    assert git(repo, "branch", "--show-current") == "fix/pr-42"
    assert git(repo, "rev-parse", "HEAD") == command.pr.head_sha
    assert not git(repo, "status", "--porcelain")
    assert snapshot(backup) == before and git(remote, "show-ref") == remote_before
    runner.ensure_repo_cloned.assert_not_awaited()


@pytest.mark.parametrize("concurrent_work", [False, True])
async def test_approval_clone_failure_defers_without_cleanup(approval, monkeypatch, concurrent_work):
    runner, command, repo, remote = approval
    await enqueue_approval(runner.redis, command)
    backup = repo.with_name("preserved-original")
    repo.rename(backup)
    before, remote_before = snapshot(backup), git(remote, "show-ref")
    original = subprocess.run

    def clone(args, **kwargs):
        assert args[:2] == ["git", "clone"]
        if concurrent_work:
            repo.mkdir()
            (repo / "operator-notes").write_text("preserve concurrent work")
            return original([*args[:-2], str(remote), args[-1]], **kwargs)
        raise subprocess.CalledProcessError(1, args, stderr="clone unavailable")

    with monkeypatch.context() as patch:
        patch.setattr(daemon_approval.subprocess, "run", clone)
        await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "deferred" and "Application could not be verified" in saved.reason
    assert await runner.redis.get(cause_key(runner.name, "PR-42"))
    assert snapshot(backup) == before and git(remote, "show-ref") == remote_before
    if concurrent_work:
        assert (repo / "operator-notes").read_text() == "preserve concurrent work"


@pytest.mark.parametrize("single", [False, True])
@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize("dirty", [False, True])
async def test_rejection_retires_applied_approval_while_preserving_work(approval, single, restart, dirty):
    runner, command, repo, remote = approval
    runner.repo_config.feature_flags.use_single_error_exit = single
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    rejection = CancellationCause(category="ERROR", payload={"subsource": "operator_reject"}).to_redis()
    await runner.redis.set(cause_key(runner.name, "PR-42"), rejection)
    if dirty:
        (repo / "operator-notes").write_text("preserve work after rejection")
    before, remote_before = snapshot(repo), git(remote, "show-ref")
    if restart:
        runner._recovered = False
        runner.state = RepoState(name=runner.name, url=runner.repo_config.url)
    await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "applied" and not saved.active
    assert await runner._consume_approval_command() == dirty
    assert (await load_approval(runner.redis, runner.name, command.binding)).superseded == (not dirty)
    assert await runner.redis.get(cause_key(runner.name, "PR-42")) == rejection
    assert snapshot(repo) == before and git(remote, "show-ref") == remote_before
    runner.handle_watch.assert_not_awaited()


async def test_retired_approval_allows_normal_clone_when_checkout_is_absent(approval):
    runner, command, repo, _ = approval
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    await runner.redis.set(
        cause_key(runner.name, "PR-42"),
        CancellationCause(category="ERROR", payload={"subsource": "operator_reject"}).to_redis(),
    )
    repo.rename(repo.with_name("preserved-original"))
    await runner._run_cycle_body()
    assert not await runner._consume_approval_command()
    assert not repo.exists()


@pytest.mark.parametrize("single", [False, True])
@pytest.mark.parametrize("restart", ["none", "before_acceptance", "after_acceptance"])
async def test_successive_approvals_retain_only_independently_approved_findings(approval, monkeypatch, single, restart):
    runner, first, repo, remote = approval
    runner.repo_config.feature_flags.use_single_error_exit = single
    await enqueue_approval(runner.redis, first)
    await runner._run_cycle_body()
    raw = CancellationCause(
        category="ERROR",
        payload={"subsource": "guardrail", "category": "workflow_destruction", "excerpt": "workflow change"},
    ).to_redis()
    runner.state.state = PipelineState.ERROR
    runner.state.current_pr.quarantine_labels.add("quarantine:workflow")
    await runner.redis.set(cause_key(runner.name, "PR-42"), raw)
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    decision_state = runner.state.model_copy(deep=True)
    if restart == "before_acceptance":
        runner._recovered = False
        runner.state = RepoState(name=runner.name, url=runner.repo_config.url)
        await runner._run_cycle_body()
        assert (await load_approval(runner.redis, runner.name, first.binding)).active
        assert await runner.redis.get(cause_key(runner.name, "PR-42")) == raw
    second = build_approval(runner.name, decision_state, raw, repo)
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a: [second.pr])
    await enqueue_approval(runner.redis, second)
    before, remote_before = snapshot(repo), git(remote, "show-ref")
    if restart != "none":
        runner._recovered = False
        runner._approval_history = []
        runner._approval_receipt = None
        runner.state = RepoState(name=runner.name, url=runner.repo_config.url)
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, second.binding)).status == "applied"
    assert runner._approval_filtered_pr(second.pr).quarantine_labels == set()
    for category, excerpt in [("large_diff_threshold", "+1800 LOC"), ("workflow_destruction", "workflow change")]:
        assert runner._approval_allows_violation(second.pr, GuardrailViolation(1, category, excerpt, "rule"))
    assert not runner._approval_allows_violation(
        second.pr, GuardrailViolation(1, "large_diff_threshold", "different finding", "rule")
    )
    assert runner._unapproved_labels(second, {"quarantine:other"}) == {"quarantine:other"}
    assert second.pr.quarantine_labels == {"quarantine:large_diff", "quarantine:workflow"}
    assert snapshot(repo) == before and git(remote, "show-ref") == remote_before
    # The retired size label never blocks, regardless of an approval's scope.
    changed = second.model_copy(deep=True)
    changed.pr.head_sha = "other-head"
    assert runner._unapproved_labels(changed, {"quarantine:large_diff"}) == set()
    assert runner._unapproved_labels(changed, {continued_size_label(second.pr.head_sha)}) == set()
    changed.pr.head_sha = second.pr.head_sha
    changed.task_fingerprint = "revised-specification"
    assert runner._unapproved_labels(changed, {"quarantine:large_diff"}) == set()


@pytest.mark.parametrize("single", [False, True])
@pytest.mark.parametrize("unified", [False, True])
@pytest.mark.parametrize("limit", ["rate", "spend"])
async def test_coder_limits_do_not_block_approved_watch(approval, monkeypatch, single, unified, limit):
    import time

    from src.inhibitor import derive_active_inhibitors
    from src.usage import UsageSnapshot

    runner, command, _, _ = approval
    runner.repo_config.feature_flags.use_single_error_exit = single
    runner.repo_config.feature_flags.use_unified_inhibitor_check = unified
    if limit == "rate":
        runner.state.rate_limit_reactive_coder = "claude"
    else:
        runner.app_config.daemon.spend_ceiling_session_percent = 80
        runner.state.usage_session_percent = 90
        now = time.time()
        snapshot = UsageSnapshot(
            session_percent=90, session_resets_at=int(now + 600),
            weekly_percent=0, weekly_resets_at=int(now + 900), fetched_at=now,
        )
        for provider in (runner._claude_usage_provider, runner._codex_usage_provider):
            monkeypatch.setattr(provider, "fetch", lambda: snapshot)
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, command.binding)).status == "applied"
    await runner._run_cycle_body()
    runner.handle_watch.assert_awaited_once()
    # Limits remain available to the existing coder/retrigger gates.
    assert await derive_active_inhibitors(runner.state, runner.redis, runner.app_config.daemon)
    if unified or limit == "rate":
        assert runner._watch_retrigger_inhibited("claude")


@pytest.mark.parametrize("single", [False, True])
async def test_merge_restart_restores_approval_and_rechecks_watch_gates(approval, single):
    runner, command, repo, remote = approval
    runner.repo_config.feature_flags.use_single_error_exit = single
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    runner.state.state = PipelineState.MERGE
    assert not await runner._consume_approval_command()
    await runner.publish_state()
    before, remote_before = snapshot(repo), git(remote, "show-ref")
    runner._recovered = False
    runner.state = RepoState(name=runner.name, url=runner.repo_config.url)
    runner._approval_receipt = None
    runner._approval_history = []
    await runner._run_cycle_body()
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "applied" and saved.active
    assert runner.state.state == PipelineState.WATCH and runner.state.current_pr.number == 42
    assert RepoState.model_validate_json(await runner.redis.get(pipeline_state(runner.name))) == runner.state
    assert runner._approval_filtered_pr(command.pr).quarantine_labels == set()
    await runner._run_cycle_body()
    runner.handle_watch.assert_awaited_once()
    assert runner.redis.deleted.count(cause_key(runner.name, "PR-42")) == 1
    assert snapshot(repo) == before and git(remote, "show-ref") == remote_before


@pytest.mark.parametrize("single", [False, True])
@pytest.mark.parametrize("blocker", ["none", "pause", "stop", "process", "unknown", "missing_state"])
async def test_resume_reconciles_stop_latch_only_when_execution_is_quiescent(approval, monkeypatch, single, blocker):
    runner, command, repo, _ = approval
    runner.repo_config.feature_flags.use_single_error_exit = single
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    runner._stop_requested = True
    if blocker == "pause":
        runner.state.user_paused = True
        await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    elif blocker == "stop":
        await runner.redis.set(control_stop(runner.name), "1")
    elif blocker == "process":
        runner._current_coder_process = object()
    elif blocker == "unknown":
        monkeypatch.setattr(daemon_approval, "checkout_process_blocker", lambda _: "ownership uncertain")
    elif blocker == "missing_state":
        await runner.redis.delete(pipeline_state(runner.name))
    before = snapshot(repo)
    await runner._run_cycle_body()
    assert runner._stop_requested == (blocker != "none")
    assert runner.handle_watch.await_count == (1 if blocker == "none" else 0)
    assert snapshot(repo) == before
    saved = await load_approval(runner.redis, runner.name, command.binding)
    assert saved.status == "applied"


async def test_completed_approvals_leave_active_index_but_keep_bounded_recent_history(approval, monkeypatch):
    from datetime import timedelta

    runner, command, _, _ = approval
    # Keep the legacy size incident out of this index-cleanup test.
    runner.state.state = PipelineState.IDLE
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    for i in range(40):
        old = command.model_copy(deep=True)
        old.binding = f"past-{i}"
        old.task.pr_id = f"PR-old-{i}"
        old.status = "applied" if i % 2 else "failed"
        old.active = old.status == "applied"
        old.requested_at += timedelta(seconds=i + 1)
        await runner.redis.set(approval_key(runner.name, old.binding), old.model_dump_json())
        await runner.redis.zadd(approval_index(runner.name), {old.binding: old.requested_at.timestamp()})
        await runner.redis.zadd(approval_recent_index(runner.name), {old.binding: -old.requested_at.timestamp()})
    assert not await runner._consume_approval_command()
    assert not await list_approvals(runner.redis, runner.name)
    get = AsyncMock(wraps=runner.redis.get)
    monkeypatch.setattr(runner.redis, "get", get)
    assert not await runner._consume_approval_command()
    get.reset_mock()
    recent = await list_approvals(runner.redis, runner.name, recent=True)
    assert [c.binding for c in recent] == [f"past-{i}" for i in range(20, 40)]
    assert get.await_count == 20
    assert await load_approval(runner.redis, runner.name, "past-0") is not None
    # A live request stays recoverable independently of the recent display limit.
    runner.state.state = PipelineState.ERROR
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, command.binding)).status == "applied"
    assert [c.binding for c in await list_approvals(runner.redis, runner.name)] == [command.binding]


@pytest.mark.parametrize("single", [False, True])
@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize("lost_reply", [False, True])
async def test_stale_fix_head_refreshes_binding_without_approving_new_head(
    approval, monkeypatch, single, restart, lost_reply,
):
    runner, command, repo, remote = approval
    runner.repo_config.feature_flags.use_single_error_exit = single
    (repo / "implementation.py").write_text("pushed FIX work\n")
    git(repo, "commit", "-am", "FIX pushed before guardrail")
    git(repo, "push", "origin", "fix/pr-42")
    fresh = command.pr.model_copy(update={"head_sha": git(repo, "rev-parse", "HEAD")})
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a: [fresh])
    await enqueue_approval(runner.redis, command)
    if lost_reply:
        original = runner.redis.transaction
        lost = False

        async def transaction(fn, *keys, **kwargs):
            nonlocal lost
            result = await original(fn, *keys, **kwargs)
            if len(keys) == 2 and not lost:
                lost = True
                raise ConnectionError("head refresh EXEC reply lost")
            return result

        monkeypatch.setattr(runner.redis, "transaction", transaction)
    if restart:
        runner._recovered = False
        runner.state = RepoState(name=runner.name, url=runner.repo_config.url)
    before, remote_before = snapshot(repo), git(remote, "show-ref")
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, command.binding)).status == (
        "deferred" if lost_reply else "failed"
    )
    published = RepoState.model_validate_json(await runner.redis.get(pipeline_state(runner.name)))
    assert published.current_pr.head_sha == fresh.head_sha
    assert published.state == PipelineState.ERROR
    if not lost_reply:
        assert runner.state == published
    raw = await runner.redis.get(cause_key(runner.name, "PR-42"))
    assert failure_identity(raw) == command.failure
    # The failed old binding still protects useful work while the UI offers
    # a distinct request for the actual pushed head.
    if not lost_reply:
        assert await runner._consume_approval_command()
    assert snapshot(repo) == before and git(remote, "show-ref") == remote_before
    replacement = build_approval(runner.name, published, raw, repo)
    assert replacement.binding != command.binding and replacement.pr.head_sha == fresh.head_sha
    await enqueue_approval(runner.redis, replacement)
    if lost_reply and restart:
        runner._recovered = False
        runner._approval_head_refresh_uncertain = False
        runner.state = RepoState(name=runner.name, url=runner.repo_config.url)
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, replacement.binding)).status == "applied"
    assert runner.state.current_pr.number == 42
    assert snapshot(repo) == before and git(remote, "show-ref") == remote_before


@pytest.mark.parametrize("change", ["cause", "pr", "head"])
async def test_head_refresh_cannot_overwrite_a_new_decision(approval, change):
    runner, command, _, _ = approval
    if change == "cause":
        await runner.redis.set(cause_key(runner.name, "PR-42"), '{"payload":{"subsource":"operator_reject"}}')
    else:
        changed = runner.state.model_copy(deep=True)
        if change == "pr":
            changed.current_pr.number = 99
        else:
            changed.current_pr.head_sha = "another-newer-head"
        await runner.redis.set(pipeline_state(runner.name), changed.model_dump_json())
    before = await runner.redis.get(pipeline_state(runner.name))
    with pytest.raises(ApprovalChanged, match="decision changed"):
        await runner._publish_refreshed_guardrail_head(command, "new-head")
    assert await runner.redis.get(pipeline_state(runner.name)) == before


async def test_fix_head_refresh_failure_preserves_current_snapshot(approval, monkeypatch):
    runner, command, _, _ = approval
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a: (_ for _ in ()).throw(OSError("offline")))
    await runner._refresh_guardrail_head_after_fix()
    assert runner.state.current_pr.head_sha == command.pr.head_sha
    runner.state.current_pr = None
    await runner._refresh_guardrail_head_after_fix()


async def test_unavailable_pr_head_stays_retryable(approval, monkeypatch):
    runner, command, _, _ = approval
    await enqueue_approval(runner.redis, command)
    monkeypatch.setattr(
        daemon_approval.gh_prs, "get_open_prs", lambda *a: [command.pr.model_copy(update={"head_sha": ""})],
    )
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, command.binding)).status == "deferred"
    monkeypatch.setattr(daemon_approval.gh_prs, "get_open_prs", lambda *a: [command.pr])
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, command.binding)).status == "applied"
