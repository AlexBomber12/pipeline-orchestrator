"""Final Reject uses real HTTP routes and temporary Git repositories.

Only GitHub transport, coder execution, and process namespace visibility are
controlled. Transaction races are additionally exercised against isolated
Redis by tests-manual/rejection/verify_redis.py.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI
from src.cancellation.storage import CancellationCause, cause_key, index_key
from src.daemon import git_ops
from src.daemon import rejection_commands as daemon_reject
from src.keyspace import pipeline_state
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus
from src.rejection_commands import build_rejection, enqueue_rejection, load_rejection, rejection_key
from src.task_attempts import AttemptChanged, attempt_key, load_attempt
from src.web import app as web_app
from src.web.routes import repo_control, uploads

from tests.runner import _helpers as h
from tests.test_approval_commands import git, isolated_daemon_process_view  # noqa: F401


@pytest.fixture
async def rejected(tmp_path, monkeypatch):
    repo = tmp_path / "octo__demo"
    remote = tmp_path / "remote.git"
    git(tmp_path, "init", "--bare", str(remote))
    git(tmp_path, "init", "-b", "main", str(repo))
    git(repo, "config", "user.name", "Rejection Test")
    git(repo, "config", "user.email", "reject@example.test")
    (repo / "tasks").mkdir()
    task_path = repo / "tasks/PR-42.md"
    task_path.write_text(
        "---\nstatus: ERROR\nblocked_reason: guardrail\n---\n\n"
        "# PR-42: Reusable task\nBranch: fix/pr-42\n- Type: bugfix\n"
        "- Complexity: low\n- Depends on: none\n\n## Requirements\nOriginal specification.\n"
    )
    git(repo, "add", ".")
    git(repo, "commit", "-m", "initial specifications")
    git(repo, "remote", "add", "origin", str(remote))
    git(repo, "push", "-u", "origin", "main")
    git(repo, "checkout", "-b", "fix/pr-42")
    (repo / "abandoned-marker.txt").write_text("must not enter new attempt")
    git(repo, "add", ".")
    git(repo, "commit", "-m", "abandoned implementation")
    git(repo, "push", "-u", "origin", "fix/pr-42")
    sha = git(repo, "rev-parse", "HEAD")
    runner = h._make_runner()
    runner.repo_path = str(repo)
    runner._recovered = True
    runner.state.state = PipelineState.ERROR
    runner.state.current_task = QueueTask(
        pr_id="PR-42",
        title="Reusable task",
        task_file="tasks/PR-42.md",
        branch="fix/pr-42",
        status=TaskStatus.ERROR,
    )
    runner.state.current_queue = [runner.state.current_task.model_copy(deep=True)]
    runner.state.current_pr = PRInfo(
        number=42, pr_id="PR-42", branch="fix/pr-42", head_sha=sha, url="https://github.com/octo/demo/pull/42"
    )
    cause = CancellationCause(
        category="ERROR",
        task_id="PR-42",
        repo_slug=runner.name,
        payload={"subsource": "guardrail", "rule": "large_diff_threshold", "excerpt": "+2000 LOC"},
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    await runner.redis.set(cause_key(runner.name, "PR-42"), cause.to_redis())
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    command = build_rejection(runner.name, runner.state, cause.to_redis(), repo)
    github = {"state": "open", "merged_at": None, "calls": [], "new_pr": None}

    def transport(args, repo=None, **kwargs):
        github["calls"].append(args)
        if args[:1] == ["api"]:
            return {
                "number": 42,
                "state": github["state"],
                "merged_at": github["merged_at"],
                "head": {"ref": "fix/pr-42", "sha": sha, "repo": {"full_name": "octo/demo"}},
                "base": {"ref": "main", "repo": {"full_name": "octo/demo"}},
            }
        if args[:2] == ["pr", "close"]:
            assert args == ["pr", "close", "42"]
            github["state"] = "closed"
            return ""
        if args[:2] == ["pr", "list"]:
            return []
        if args[:2] == ["pr", "create"]:
            assert github["state"] == "closed"
            github["new_pr"] = PRInfo(
                number=43, pr_id="PR-42", branch="fix/pr-42", head_sha=git(repo_path, "rev-parse", "HEAD")
            )
            return "https://github.com/octo/demo/pull/43"
        pytest.fail(f"Unexpected GitHub call: {args}")

    repo_path = repo
    monkeypatch.setattr(daemon_reject.gh_runner, "run_gh", transport)
    # Git's actual origin remains a local bare remote. Only its logical
    # ownership read is mapped to the configured GitHub identity.
    original_git = git_ops._git

    def git_transport(path, *args, **kwargs):
        if args == ("remote", "get-url", "origin"):
            return h._FakeCompletedProcess(stdout=runner.repo_config.url)
        return original_git(path, *args, **kwargs)

    monkeypatch.setattr(git_ops, "_git", git_transport)
    monkeypatch.setattr("src.task_admission.gh_pr_get_merged_branches", lambda *a: set())
    monkeypatch.setattr("src.task_admission.gh_prs.get_merged_prs", lambda *a, **k: [])
    config = tmp_path / "config.yml"
    config.write_text("repositories:\n  - url: https://github.com/octo/demo.git\n    branch: main\n")
    monkeypatch.setattr(web_app, "CONFIG_PATH", config)
    monkeypatch.setattr(web_app, "REPOS_DIR", str(tmp_path))
    monkeypatch.setattr(web_app, "UPLOADS_DIR", str(tmp_path / "uploads"))
    monkeypatch.setattr(web_app, "publish_wake", AsyncMock())
    monkeypatch.setattr(
        repo_control, "_load_current_queue_snapshot", AsyncMock(return_value=(runner.state.current_queue, None))
    )
    app = FastAPI()
    app.include_router(repo_control.router)
    app.include_router(uploads.router)
    app.state.redis = runner.redis
    return runner, command, repo, remote, github, app


async def post_reject(fixture, *, alternate=False, binding=None):
    runner, command, _, _, _, app = fixture
    route = (
        f"/repos/{runner.name}/tasks/PR-42/guardrail/reject"
        if alternate
        else f"/repos/{runner.name}/guardrail/PR-42/decision"
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        return await client.post(route, data={"decision": "reject", "binding": binding or command.binding})


@pytest.mark.parametrize("single", [False, True])
async def test_connected_http_reject_rewrite_clean_base_and_new_pr(rejected, monkeypatch, single):
    runner, command, repo, remote, github, app = rejected
    runner.repo_config.feature_flags.use_single_error_exit = single
    response = await post_reject(rejected)
    assert response.status_code == 202 and "accepted" in response.text
    assert github["calls"] == []
    assert (await post_reject(rejected, alternate=True)).status_code == 202
    await runner._run_cycle_body()
    stored = await load_rejection(runner.redis, runner.name, command.binding)
    assert stored.status == "rejected" and stored.released
    assert runner.state.current_task is None and runner.state.current_pr is None
    assert github["state"] == "closed"
    assert sum(call[:2] == ["pr", "close"] for call in github["calls"]) == 1
    # Expiring rich display records cannot clear the permanent rejection.
    await runner.redis.delete(cause_key(runner.name, "PR-42"))
    await runner.redis.delete(index_key(runner.name))
    assert await runner._reconcile_git_admissions() == {"PR-42"}
    old_attempt = await load_attempt(runner.redis, runner.name, "PR-42")
    new_text = (
        (repo / "tasks/PR-42.md")
        .read_text()
        .replace("Original specification.", "Rewritten specification.")
        .replace("status: ERROR", "status: TODO")
        .replace("blocked_reason: guardrail\n", "")
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            f"/repos/{runner.name}/upload-tasks", files={"files": ("PR-42.md", new_text, "text/markdown")}
        )
    assert response.status_code == 200, response.text
    assert await runner.process_pending_uploads() is True
    attempt = await load_attempt(runner.redis, runner.name, "PR-42")
    assert attempt.attempt_id != old_attempt.attempt_id and not attempt.admission_pending
    assert attempt.previous_rejection == command.binding and attempt.rejection is None
    assert await runner._reconcile_git_admissions() == set()
    received = []

    async def coder(path, pr_id, task_file, task_body, **kwargs):
        received.append(task_body)
        git(repo, "checkout", "-b", "fix/pr-42", "origin/main")
        assert not (repo / "abandoned-marker.txt").exists()
        (repo / "new-implementation.txt").write_text("fresh")
        git(repo, "add", ".")
        git(repo, "commit", "-m", "new implementation")
        git(repo, "push", "-u", "origin", "fix/pr-42")
        return 0, "ok", ""

    monkeypatch.setattr(h.claude_cli, "run_auto_pr_async", coder)
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda *a, **k: [github["new_pr"]] if github["new_pr"] else [])
    monkeypatch.setattr(runner, "_post_codex_review", lambda *a, **k: True)
    # The normal post-coder path observes a pushed branch and creates a PR.
    original_sleep = asyncio.sleep

    async def short_sleep(_delay):
        await original_sleep(0)

    monkeypatch.setattr("src.daemon.handlers.coding.asyncio.sleep", short_sleep)
    await runner.handle_idle()
    assert received == [new_text]
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr.number == 43
    assert github["state"] == "closed"
    assert git(remote, "show", "refs/heads/fix/pr-42:new-implementation.txt") == "fresh"
    assert (await post_reject(rejected)).status_code == 202
    assert github["state"] == "closed" and runner.state.current_pr.number == 43


async def test_stale_decision_and_repeated_submission(rejected):
    runner, command, repo, _, github, _ = rejected
    (repo / "tasks/PR-42.md").write_text((repo / "tasks/PR-42.md").read_text() + "Changed\n")
    assert (await post_reject(rejected)).status_code == 409
    assert not github["calls"]
    assert await load_attempt(runner.redis, runner.name, "PR-42") is None
    assert (await post_reject(rejected, binding="0" * 64)).status_code == 409


@pytest.mark.parametrize("failure", ["close_timeout", "lookup_timeout", "still_open", "merged"])
async def test_closure_ambiguity_survives_restart(rejected, monkeypatch, failure):
    runner, command, _, _, github, _ = rejected
    await post_reject(rejected)
    original = daemon_reject.gh_runner.run_gh

    def transport(args, *a, **k):
        if failure == "lookup_timeout" and args[0] == "api":
            raise TimeoutError()
        if args[:2] == ["pr", "close"]:
            if failure == "close_timeout":
                github["state"] = "closed"
                raise TimeoutError()
            if failure == "still_open":
                return ""
        if failure == "merged":
            github["merged_at"] = "2026-09-01T00:00:00Z"
            github["state"] = "closed"
        return original(args, *a, **k)

    monkeypatch.setattr(daemon_reject.gh_runner, "run_gh", transport)
    await runner._consume_rejection_commands()
    stored = await load_rejection(runner.redis, runner.name, command.binding)
    assert stored.status == ("merged" if failure == "merged" else "deferred")
    if failure == "merged":
        assert (await load_attempt(runner.redis, runner.name, "PR-42")).completed
        return
    fresh = h._make_runner()
    fresh.repo_path, fresh.redis = runner.repo_path, runner.redis
    monkeypatch.setattr(daemon_reject.gh_runner, "run_gh", original)
    await fresh._consume_rejection_commands()
    stored = await load_rejection(runner.redis, runner.name, command.binding)
    assert stored.status == ("closing" if failure == "still_open" else "rejected")


async def test_unknown_process_defers_rejection(rejected, monkeypatch):
    runner, command, _, _, github, _ = rejected
    await post_reject(rejected)
    monkeypatch.setattr(daemon_reject, "checkout_process_blocker", lambda path: "Orphan process remains")
    await runner._consume_rejection_commands()
    assert not github["calls"]
    assert (await load_rejection(runner.redis, runner.name, command.binding)).status == "deferred"


@pytest.mark.parametrize(
    "change",
    [
        "missing_binding",
        "missing_cause",
        "missing_state",
        "different_task",
        "different_failure",
        "different_pr",
        "non_guardrail",
        "redis_unavailable",
        "wake_unavailable",
    ],
)
async def test_http_reject_is_exact_and_acknowledges_only_durable_intent(rejected, monkeypatch, change):
    from redis.exceptions import ConnectionError

    runner, command, _, _, github, app = rejected
    expected = 409
    if change == "missing_cause":
        await runner.redis.delete(cause_key(runner.name, "PR-42"))
    elif change == "missing_state":
        await runner.redis.delete(pipeline_state(runner.name))
    elif change in {"different_task", "different_pr"}:
        if change == "different_task":
            runner.state.current_task.pr_id = "PR-43"
        else:
            runner.state.current_pr.number = 43
        await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    elif change in {"different_failure", "non_guardrail"}:
        cause = CancellationCause.from_redis(await runner.redis.get(cause_key(runner.name, "PR-42")))
        cause.payload["subsource" if change == "non_guardrail" else "rule"] = "changed"
        await runner.redis.set(cause_key(runner.name, "PR-42"), cause.to_redis())
    elif change == "redis_unavailable":
        monkeypatch.setattr(runner.redis, "get", AsyncMock(side_effect=ConnectionError("offline")))
        expected = 503
    elif change == "wake_unavailable":
        monkeypatch.setattr(web_app, "publish_wake", AsyncMock(side_effect=ConnectionError("offline")))
        expected = 202
    if change == "missing_binding":
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post(f"/repos/{runner.name}/tasks/PR-42/guardrail/reject")
    else:
        response = await post_reject(rejected)
    assert response.status_code == expected, response.text
    assert not github["calls"]
    if expected == 202:
        assert await load_rejection(runner.redis, runner.name, command.binding)


@pytest.mark.parametrize("ambiguous", [False, True])
async def test_pre_pr_reject_confirms_absence_before_releasing_branch(rejected, monkeypatch, ambiguous):
    from src.task_attempts import new_attempt, save_attempt

    runner, _, repo, _, github, _ = rejected
    runner.state.current_pr = None
    attempt = new_attempt(
        runner.repo_config.url, runner.state.current_task, (repo / "tasks/PR-42.md").read_text(), started=True
    )
    runner.state.current_task.attempt_id = attempt.attempt_id
    await save_attempt(runner.redis, runner.name, attempt, expected=None)
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    command = build_rejection(runner.name, runner.state, await runner.redis.get(cause_key(runner.name, "PR-42")), repo)
    await enqueue_rejection(runner.redis, command)
    if ambiguous:
        original = daemon_reject.gh_runner.run_gh
        monkeypatch.setattr(
            daemon_reject.gh_runner,
            "run_gh",
            lambda args, *a, **kw: [{"number": 99}] if args[:2] == ["pr", "list"] else original(args, *a, **kw),
        )
    await runner._consume_rejection_commands()
    first = await load_rejection(runner.redis, runner.name, command.binding)
    assert not first.released
    assert first.status == ("deferred" if ambiguous else "closing")
    await runner._consume_rejection_commands()
    result = await load_rejection(runner.redis, runner.name, command.binding)
    assert result.released is (not ambiguous)
    if not ambiguous:
        assert result.branch_head == git(repo, "rev-parse", "fix/pr-42")
        assert "no PR was created" in result.reason
    assert all(call[:2] != ["pr", "close"] for call in github["calls"])


@pytest.mark.parametrize(
    "change",
    [
        "foreign_repository",
        "other_task",
        "dirty",
        "pr_identity",
        "pr_response",
        "receipt_missing",
        "index_missing",
        "redis_error",
    ],
)
async def test_reconciliation_holds_unsafe_checkout_or_unverifiable_identity(rejected, monkeypatch, change):
    runner, command, repo, _, github, _ = rejected
    await post_reject(rejected)
    if change == "foreign_repository":
        runner.repo_config.url = "https://github.com/other/repository.git"
    elif change == "other_task":
        runner.state.current_task.pr_id = "PR-99"
    elif change == "dirty":
        (repo / "unrelated.txt").write_text("preserve")
    elif change in {"pr_identity", "pr_response"}:
        original = daemon_reject.gh_runner.run_gh

        def transport(args, *a, **kw):
            data = original(args, *a, **kw)
            if args[0] == "api":
                if change == "pr_response":
                    return []
                data["head"]["sha"] = "unexpected"
            return data

        monkeypatch.setattr(daemon_reject.gh_runner, "run_gh", transport)
    elif change == "receipt_missing":
        await runner.redis.delete(attempt_key(runner.name, "PR-42"))
    elif change == "index_missing":
        await runner.redis.delete(rejection_key(runner.name, command.binding))
    else:
        monkeypatch.setattr(runner.redis, "zrangebyscore", AsyncMock(side_effect=OSError("unavailable")))
    assert await runner._consume_rejection_commands()
    result = await load_rejection(runner.redis, runner.name, command.binding)
    assert result is None or not result.released
    if change == "dirty":
        assert (repo / "unrelated.txt").read_text() == "preserve"
    assert runner.state.current_task is not None


async def test_enqueued_duplicate_and_cas_replacement_are_idempotent(rejected):
    from src.task_attempts import save_attempt

    runner, command, *_ = rejected
    first = await enqueue_rejection(runner.redis, command)
    assert await enqueue_rejection(runner.redis, command) == first
    attempt = await load_attempt(runner.redis, runner.name, "PR-42")
    assert await save_attempt(runner.redis, runner.name, attempt, expected=None) == attempt
    with pytest.raises(AttemptChanged):
        await save_attempt(runner.redis, runner.name, attempt.model_copy(update={"completed": True}), expected=None)
    stale = command.model_copy(update={"binding": "other-decision"})
    with pytest.raises(AttemptChanged):
        await enqueue_rejection(runner.redis, stale)


@pytest.mark.parametrize("change", ["task_branch", "pr_branch", "no_head", "no_legacy_identity"])
async def test_legacy_reject_never_guesses_missing_identity(rejected, change):
    runner, command, repo, *_ = rejected
    if change == "task_branch":
        runner.state.current_task.branch = "different"
    elif change == "pr_branch":
        runner.state.current_pr.branch = "different"
    elif change == "no_head":
        runner.state.current_pr.head_sha = ""
    else:
        runner.state.current_pr = None
    with pytest.raises(AttemptChanged):
        build_rejection(runner.name, runner.state, command.failure, repo)


async def test_expired_display_history_still_fences_recovery_and_direct_retry(rejected, monkeypatch):
    await post_reject(rejected)
    runner, command, *_ = rejected
    await runner.redis.delete(cause_key(runner.name, "PR-42"))
    assert await runner._attempt_execution_blocked()
    tasks = await runner._fence_recovery_tasks(
        [runner.state.current_task.model_copy(update={"status": TaskStatus.TODO})]
    )
    assert tasks[0].status == TaskStatus.ERROR
    await runner.redis.delete(attempt_key(runner.name, "PR-42"))
    monkeypatch.setattr(runner.redis, "get", AsyncMock(side_effect=OSError("unavailable")))
    assert await runner._attempt_execution_blocked()


@pytest.mark.parametrize("read_point", ["receipt", "state"])
async def test_duplicate_committed_between_command_reads_returns_existing_result(rejected, monkeypatch, read_point):
    runner, command, *_ = rejected
    original_get = h._FakePipeline.get
    delivered = False

    async def interleaved_get(pipe, key):
        nonlocal delivered
        trigger = attempt_key(runner.name, "PR-42") if read_point == "receipt" else pipeline_state(runner.name)
        if key == trigger and not delivered:
            delivered = True
            # Simulate a second HTTP delivery completing during the first
            # delivery's read phase. Real WATCH races run in verify_redis.py.
            monkeypatch.setattr(h._FakePipeline, "get", original_get)
            await enqueue_rejection(runner.redis, command)
        return await original_get(pipe, key)

    monkeypatch.setattr(h._FakePipeline, "get", interleaved_get)
    result = await enqueue_rejection(runner.redis, command)
    assert delivered and result.binding == command.binding


async def test_different_failure_cannot_be_overwritten_by_an_already_built_command(rejected):
    runner, command, *_ = rejected
    await runner.redis.set(
        cause_key(runner.name, "PR-42"),
        CancellationCause(category="ERROR", payload={"subsource": "guardrail", "rule": "new failure"}).to_redis(),
    )
    with pytest.raises(AttemptChanged, match="active decision changed"):
        await enqueue_rejection(runner.redis, command)
    assert await load_attempt(runner.redis, runner.name, "PR-42") is None


async def test_rejection_from_other_attempt_never_stops_new_owner(rejected):
    runner, command, *_ = rejected
    await enqueue_rejection(runner.redis, command)
    attempt = await load_attempt(runner.redis, runner.name, "PR-42")
    await runner.redis.set(
        attempt_key(runner.name, "PR-42"),
        attempt.model_copy(update={"attempt_id": "new", "rejection": None}).model_dump_json(),
    )
    assert not await runner._consume_rejection_commands()
    assert not (await load_rejection(runner.redis, runner.name, command.binding)).released


async def test_http_reject_waits_for_attempt_owned_child_exit_before_close(rejected, tmp_path):
    import os
    import subprocess
    import sys

    from src.daemon.attempt_processes import ATTEMPT_ENV

    runner, command, _, _, github, _ = rejected
    child = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(300)"],
        cwd=tmp_path,
        env={**os.environ, ATTEMPT_ENV: command.attempt_id},
    )
    try:
        assert (await post_reject(rejected)).status_code == 202
        await runner._consume_rejection_commands()
        operation = await load_rejection(runner.redis, runner.name, command.binding)
        assert operation.status == "deferred" and not operation.released
        assert not github["calls"]
        child.wait(timeout=5)
        await runner._consume_rejection_commands()
        assert (await load_rejection(runner.redis, runner.name, command.binding)).released
        assert github["state"] == "closed"
    finally:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=5)


@pytest.mark.parametrize("single", [False, True])
@pytest.mark.parametrize("control", ["pause", "stop"])
async def test_rejection_finishes_without_clearing_manual_inhibitors(rejected, monkeypatch, single, control):
    from src.keyspace import control_stop

    runner, command, *_ = rejected
    runner.repo_config.feature_flags.use_single_error_exit = single
    if control == "pause":
        runner.state.user_paused = True
        await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    else:
        await runner.redis.set(control_stop(runner.name), "1")
    assert (await post_reject(rejected)).status_code == 202
    await runner._run_cycle_body()
    assert (await load_rejection(runner.redis, runner.name, command.binding)).released
    if control == "pause":
        assert runner.state.user_paused
        monkeypatch.setattr(runner, "preflight", AsyncMock(side_effect=AssertionError("must stay paused")))
        await runner._run_cycle_body()
    else:
        assert await runner.redis.get(control_stop(runner.name)) == "1"


async def test_rejection_cancels_supervised_coder_without_pause_or_diagnosis(rejected, monkeypatch):
    runner, command, *_ = rejected
    name, plugin = runner._get_coder()
    runner._current_breach_dir = ""
    runner._current_breach_run_id = ""

    async def coder(*args, **kwargs):
        await enqueue_rejection(runner.redis, command)
        raise asyncio.CancelledError

    monkeypatch.setattr(plugin, "run_auto_pr", coder)
    kwargs = dict(
        target_branch="fix/pr-42", current_pr_id="PR-42", pr_id="PR-42", task_file="tasks/PR-42.md", task_body="body"
    )
    assert await runner._run_coder_with_supervision(name, plugin, {}, **kwargs) is None
    assert not runner.state.user_paused
    assert not runner._stop_requested
    assert await runner._run_coder_with_supervision(name, plugin, {}, **kwargs) is None
    assert not await runner._daemon_create_pr_for_branch("fix/pr-42", "claude")
    assert not await runner._start_retry_coding_execution()


async def test_live_supervision_cancels_dispatch_as_soon_as_rejection_is_accepted(rejected):
    runner, command, *_ = rejected

    async def pending():
        await asyncio.Event().wait()

    task = asyncio.create_task(pending())
    await enqueue_rejection(runner.redis, command)
    await runner._monitor_stop_request(task)
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not runner.state.user_paused


async def test_coder_preparation_rereads_rejection_before_dispatch(rejected, monkeypatch):
    runner, _, repo, *_ = rejected
    original = runner._prepare_coder_invocation

    async def prepare(*args, **kwargs):
        result = await original(*args, **kwargs)
        command = build_rejection(
            runner.name, runner.state, await runner.redis.get(cause_key(runner.name, "PR-42")), repo
        )
        await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
        await enqueue_rejection(runner.redis, command)
        return result

    monkeypatch.setattr(runner, "_prepare_coder_invocation", prepare)
    monkeypatch.setattr(h.claude_cli, "run_auto_pr_async", AsyncMock(side_effect=AssertionError("must not dispatch")))
    await runner.handle_coding()
    assert await runner._attempt_execution_blocked()


async def test_rejected_receipt_prevents_status_reactivation_even_if_retry_was_already_reserved(rejected, monkeypatch):
    runner, _, repo, *_ = rejected
    await post_reject(rejected)
    before = (repo / "tasks/PR-42.md").read_bytes()
    assert not await runner._commit_task_status_change(runner.state.current_task, "TODO", "stale Retry")
    assert (repo / "tasks/PR-42.md").read_bytes() == before
    monkeypatch.setattr(runner.redis, "get", AsyncMock(side_effect=OSError("unavailable")))
    assert not await runner._commit_task_status_change(runner.state.current_task, "TODO", "unknown ownership")


async def test_reject_history_preserves_reason_text_only_guardrail_metadata(rejected):
    runner, _, repo, *_ = rejected
    cause = CancellationCause(
        category="ERROR",
        task_id="PR-42",
        repo_slug=runner.name,
        payload={"subsource": "guardrail", "reason_text": "GUARDRAIL: large_diff: changed 2000 lines"},
    )
    await runner.redis.set(cause_key(runner.name, "PR-42"), cause.to_redis())
    command = build_rejection(runner.name, runner.state, cause.to_redis(), repo)
    await enqueue_rejection(runner.redis, command)
    stored = CancellationCause.from_redis(await runner.redis.get(cause_key(runner.name, "PR-42")))
    assert stored.payload["original_rule"] == "large_diff"
    assert stored.payload["original_excerpt"] == "changed 2000 lines"


async def test_dashboard_and_http_use_receipt_identity_and_final_operation_history(rejected):
    from src.task_attempts import new_attempt, save_attempt
    from src.web.routes import dashboard

    runner, _, repo, *_ = rejected
    attempt = new_attempt(runner.repo_config.url, runner.state.current_task, (repo / "tasks/PR-42.md").read_text())
    await save_attempt(runner.redis, runner.name, attempt, expected=None)
    await runner.redis.zadd(index_key(runner.name), {"PR-42": datetime.now(timezone.utc).timestamp()})
    views = await dashboard._build_guardrail_pending_view(runner.redis, runner.name, runner.state)
    binding = views[0]["rejection_binding"]
    response = await post_reject(rejected, binding=binding)
    assert response.status_code == 202
    views = await dashboard._build_guardrail_pending_view(runner.redis, runner.name, runner.state)
    assert views[-1]["rejection"]["status"] == "accepted"
    view = await repo_control._task_view(runner.state.current_task, runner.name, runner.redis)
    assert view["cancellation_subsource"] == "operator_reject"
    response = await repo_control._reject_guardrail_decision(
        runner.name, "PR-99", runner.repo_config, runner.redis, binding
    )
    assert response.status_code == 409


@pytest.mark.parametrize("case", ["ambiguous_creation", "divergent_refs", "late_process"])
async def test_pre_pr_closure_waits_for_ambiguous_effects_and_checkout_ownership(rejected, monkeypatch, case):
    from src.task_attempts import new_attempt, save_attempt

    runner, _, repo, _, github, _ = rejected
    runner.state.current_pr = None
    attempt = new_attempt(
        runner.repo_config.url,
        runner.state.current_task,
        (repo / "tasks/PR-42.md").read_text(),
        started=True,
        pr_creation_pending=case == "ambiguous_creation",
    )
    runner.state.current_task.attempt_id = attempt.attempt_id
    await save_attempt(runner.redis, runner.name, attempt, expected=None)
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    command = build_rejection(runner.name, runner.state, await runner.redis.get(cause_key(runner.name, "PR-42")), repo)
    await enqueue_rejection(runner.redis, command)
    if case == "divergent_refs":
        git(repo, "commit", "--allow-empty", "-m", "local unpublished commit")
    elif case == "late_process":
        calls = 0

        def blocker(path):
            nonlocal calls
            calls += 1
            return "A process appeared before checkout release" if calls == 3 else None

        monkeypatch.setattr(daemon_reject, "checkout_process_blocker", blocker)
    await runner._consume_rejection_commands()
    await runner._consume_rejection_commands()
    result = await load_rejection(runner.redis, runner.name, command.binding)
    assert result.status == "deferred" and not result.released
    assert all(call[:2] != ["pr", "close"] for call in github["calls"])


async def test_pending_pr_creation_does_not_invoke_error_diagnosis(rejected):
    from src.task_attempts import new_attempt, save_attempt

    runner, _, repo, *_ = rejected
    runner.state.current_pr = None
    attempt = new_attempt(
        runner.repo_config.url,
        runner.state.current_task,
        (repo / "tasks/PR-42.md").read_text(),
        started=True,
        pr_creation_pending=True,
    )
    await save_attempt(runner.redis, runner.name, attempt, expected=None)
    await runner.handle_error()
    assert any("acknowledgement unresolved" in item["event"] for item in runner.state.history)


async def test_history_lookup_failure_remains_a_crash_without_inventing_a_rejection(rejected, monkeypatch):
    runner, *_ = rejected
    monkeypatch.setattr("src.daemon.recovery.get_cancellation_cause", AsyncMock(side_effect=OSError("unavailable")))
    assert await runner._dispatch_recovery_branch("PR-42") == "crash"


async def test_rejected_attempt_rejects_direct_retry_and_reset_routes(rejected, monkeypatch):
    runner, _, _, _, _, app = rejected
    assert (await post_reject(rejected)).status_code == 202
    await runner.redis.delete(cause_key(runner.name, "PR-42"))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        for route in [
            f"/repos/{runner.name}/tasks/PR-42/retry",
            f"/repos/{runner.name}/reset-to-idle",
            f"/api/reset-task/{runner.name}/PR-42",
        ]:
            response = await client.post(route, data={"retry_binding": "0" * 64})
            assert response.status_code == 409, response.text
            assert "Reject is final" in response.text
        original = runner.redis.get

        async def unavailable(key):
            if key == attempt_key(runner.name, "PR-42"):
                raise OSError("receipt unavailable")
            return await original(key)

        monkeypatch.setattr(runner.redis, "get", unavailable)
        response = await client.post(f"/repos/{runner.name}/tasks/PR-42/retry", data={"retry_binding": "0" * 64})
        assert response.status_code == 503 and "ownership" in response.text
        response = await client.post(f"/repos/{runner.name}/reset-to-idle")
        assert response.status_code == 503 and "Attempt state unavailable" in response.text


async def test_retry_cannot_acknowledge_when_failure_changes_during_its_reads(rejected, monkeypatch):
    runner, _, _, _, _, app = rejected
    original = repo_control.get_cancellation_cause
    reads = 0

    async def lookup(*args, **kwargs):
        nonlocal reads
        reads += 1
        if reads == 2:
            raise OSError("failure read unavailable")
        return await original(*args, **kwargs)

    monkeypatch.setattr(repo_control, "get_cancellation_cause", lookup)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(f"/repos/{runner.name}/tasks/PR-42/retry", data={"retry_binding": "0" * 64})
    assert response.status_code == 503, response.text
    assert "prior failure evidence" in response.text


async def test_legacy_approve_url_uses_bound_durable_command_and_preserves_work(rejected):
    from src.approval_commands import build_approval, load_approval

    runner, _, repo, _, github, app = rejected
    command = build_approval(runner.name, runner.state, await runner.redis.get(cause_key(runner.name, "PR-42")), repo)
    before = git(repo, "status", "--porcelain"), git(repo, "rev-parse", "HEAD")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        route = f"/repos/{runner.name}/tasks/PR-42/guardrail/accept-once"
        assert (await client.post(route)).status_code == 409
        response = await client.post(route, data={"binding": command.binding})
        assert response.status_code == 202, response.text
    assert await load_approval(runner.redis, runner.name, command.binding)
    assert before == (git(repo, "status", "--porcelain"), git(repo, "rev-parse", "HEAD"))
    assert not github["calls"]
    await post_reject(rejected)
    await runner._consume_rejection_commands()
    assert not await runner._consume_approval_command()
    assert github["state"] == "closed"
