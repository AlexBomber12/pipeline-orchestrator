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
from src.daemon.guardrails import GuardrailViolation
from src.keyspace import control_stop, pipeline_state
from src.models import PipelineState, PRInfo, QueueTask, RepoState, TaskStatus

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
        git(repo, "symbolic-ref", "HEAD"),
        git(repo, "rev-parse", "HEAD"),
        git(repo, "show-ref"),
        (repo / ".git/index").read_bytes(),
        {
            str(p.relative_to(repo)): p.read_bytes()
            for p in repo.rglob("*")
            if p.is_file() and ".git" not in p.relative_to(repo).parts
        },
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
    monkeypatch.setattr(runner, "ensure_repo_cloned", AsyncMock(side_effect=AssertionError("approval must not clone")))
    monkeypatch.setattr(runner, "preflight", AsyncMock(side_effect=AssertionError("approval must not reset")))
    monkeypatch.setattr(runner, "handle_coding", AsyncMock(side_effect=AssertionError("approval must not code")))
    monkeypatch.setattr(runner, "handle_watch", AsyncMock())
    monkeypatch.setattr(runner, "_check_github_api_budget", AsyncMock(return_value=True))
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


async def test_empty_store_leaves_pipeline_unchanged(approval):
    runner, _, _, _ = approval
    assert not await runner._consume_approval_command()


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
    assert runner._approval_filtered_pr(command.pr) == command.pr


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
    # Earlier permission cannot bleed into a different HEAD or specification.
    changed = second.model_copy(deep=True)
    changed.pr.head_sha = "other-head"
    assert runner._unapproved_labels(changed, {"quarantine:large_diff"}) == {"quarantine:large_diff"}
    changed.pr.head_sha = second.pr.head_sha
    changed.task_fingerprint = "revised-specification"
    assert runner._unapproved_labels(changed, {"quarantine:large_diff"}) == {"quarantine:large_diff"}


@pytest.mark.parametrize("single", [False, True])
@pytest.mark.parametrize("unified", [False, True])
@pytest.mark.parametrize("limit", ["rate", "spend"])
async def test_coder_limits_do_not_block_approved_watch(approval, monkeypatch, single, unified, limit):
    from types import SimpleNamespace

    from src.inhibitor import derive_active_inhibitors

    runner, command, _, _ = approval
    runner.repo_config.feature_flags.use_single_error_exit = single
    runner.repo_config.feature_flags.use_unified_inhibitor_check = unified
    if limit == "rate":
        runner.state.rate_limit_reactive_coder = "claude"
    else:
        runner.app_config.daemon.spend_ceiling_session_percent = 80
        runner.state.usage_session_percent = 90
        snapshot = SimpleNamespace(
            session_percent=90, session_resets_at=None, weekly_percent=0, weekly_resets_at=None,
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
    get.assert_not_awaited()
    recent = await list_approvals(runner.redis, runner.name, recent=True)
    assert [c.binding for c in recent] == [f"past-{i}" for i in range(20, 40)]
    assert get.await_count == 20
    assert await load_approval(runner.redis, runner.name, "past-0") is not None
    # A live request stays recoverable independently of the recent display limit.
    await enqueue_approval(runner.redis, command)
    await runner._run_cycle_body()
    assert (await load_approval(runner.redis, runner.name, command.binding)).status == "applied"
    assert [c.binding for c in await list_approvals(runner.redis, runner.name)] == [command.binding]
