"""Final Reject uses real HTTP routes and temporary Git repositories.

Only GitHub transport, coder execution, and process namespace visibility are
controlled. Transaction races are additionally exercised against isolated
Redis by tests-manual/rejection/verify_redis.py.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI
from src.cancellation.storage import CancellationCause, cause_key, index_key
from src.daemon import git_ops
from src.daemon import rejection_commands as daemon_reject
from src.keyspace import pipeline_state
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus
from src.rejection_commands import (
    build_rejection,
    enqueue_rejection,
    list_pending_rejections,
    list_rejections,
    load_rejection,
    rejection_index,
    rejection_key,
    rejection_pending_backfill_key,
    rejection_pending_index,
)
from src.task_attempts import AttemptChanged, attempt_key, load_attempt
from src.web import app as web_app
from src.web.routes import repo_control, uploads

from tests.runner import _helpers as h
from tests.test_approval_commands import git, isolated_daemon_process_view  # noqa: F401


def test_recorded_rejection_identity_skips_bad_entries_and_binding_mismatches(tmp_path):
    from src.rejection_commands import recorded_rejection_identity

    (tmp_path / "tasks").mkdir()
    (tmp_path / "tasks/rejections.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "repository": "octo/demo",
                "base_branch": "main",
                "rejections": {
                    "PR-42": [
                        {"fingerprint": "match", "rejection_binding": "wanted"},
                        {"fingerprint": "match", "rejection_binding": "other"},
                        "bad",
                    ]
                },
            }
        )
    )

    assert recorded_rejection_identity(
        tmp_path,
        "octo/demo",
        "main",
        "PR-42",
        fingerprint="match",
        binding="wanted",
    ) == {"fingerprint": "match", "rejection_binding": "wanted"}


def raw_attempt_pr(pr, *, created_at=None, state="open", merged_at=None):
    return {
        "number": pr.number,
        "state": state,
        "merged_at": merged_at,
        "created_at": created_at or datetime.now(timezone.utc).isoformat(),
        "head": {"ref": pr.branch, "sha": pr.head_sha, "repo": {"full_name": "octo/demo"}},
        "base": {"ref": "main", "repo": {"full_name": "octo/demo"}},
    }


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
        if args[:3] == ["api", "--paginate", "--slurp"]:
            return [github.get("attempt_prs", [])] if "/pulls?" in args[-1] else [[]]
        if args[:1] == ["api"]:
            for row in github.get("attempt_prs", []):
                if args[1].endswith(f"/{row['number']}"):
                    return row
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
            for row in github.get("attempt_prs", []):
                if row["number"] == 42:
                    row["state"] = "closed"
            return ""
        if args[:2] == ["pr", "list"]:
            return []
        if args[:2] == ["pr", "create"]:
            assert github["state"] == "closed"
            github["new_pr"] = PRInfo(
                number=43, pr_id="PR-42", branch="fix/pr-42", head_sha=git(repo_path, "rev-parse", "HEAD")
            )
            github["attempt_prs"] = [raw_attempt_pr(github["new_pr"])]
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


async def test_run_cycle_restores_missing_checkout_before_rejection(rejected, monkeypatch):
    import shutil
    from pathlib import Path

    runner, *_ = rejected
    assert (await post_reject(rejected)).status_code == 202
    shutil.rmtree(runner.repo_path)
    calls = []

    async def ensure_checkout():
        calls.append("ensure")
        Path(runner.repo_path, ".git").mkdir(parents=True)

    async def consume_rejection():
        calls.append("reject")
        assert Path(runner.repo_path, ".git").is_dir()
        return True

    monkeypatch.setattr(runner, "ensure_repo_cloned", ensure_checkout)
    monkeypatch.setattr(runner, "_consume_rejection_commands", consume_rejection)

    await runner._run_cycle_body()

    assert calls == ["ensure", "reject"]


async def test_run_cycle_defers_checkout_restore_when_rejection_listing_fails(rejected, monkeypatch):
    import shutil

    runner, *_ = rejected
    shutil.rmtree(runner.repo_path)
    monkeypatch.setattr(
        "src.daemon.runner.list_pending_rejections",
        AsyncMock(side_effect=OSError("redis unavailable")),
    )
    ensure = AsyncMock(side_effect=AssertionError("checkout restore requires a confirmed pending rejection"))
    monkeypatch.setattr(runner, "ensure_repo_cloned", ensure)
    monkeypatch.setattr(runner, "_consume_rejection_commands", AsyncMock(return_value=True))

    await runner._run_cycle_body()

    ensure.assert_not_awaited()


async def test_run_cycle_clone_failure_before_rejection_transitions_error(rejected, monkeypatch):
    import shutil

    runner, *_ = rejected
    assert (await post_reject(rejected)).status_code == 202
    shutil.rmtree(runner.repo_path)
    monkeypatch.setattr(runner, "ensure_repo_cloned", AsyncMock(side_effect=RuntimeError("clone failed")))
    consume_rejection = AsyncMock(side_effect=AssertionError("rejection must wait for checkout restore"))
    monkeypatch.setattr(runner, "_consume_rejection_commands", consume_rejection)

    await runner._run_cycle_body()

    runner.ensure_repo_cloned.assert_awaited_once()
    consume_rejection.assert_not_awaited()
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "clone failed"


async def test_pending_rejection_index_is_retired_after_release(rejected):
    runner, command, *_ = rejected
    assert (await post_reject(rejected)).status_code == 202
    assert [item.binding for item in await list_pending_rejections(runner.redis, runner.name)] == [command.binding]

    await runner._consume_rejection_commands()

    stored = await load_rejection(runner.redis, runner.name, command.binding)
    assert stored.released
    assert await list_pending_rejections(runner.redis, runner.name) == []
    assert [item.binding for item in await list_rejections(runner.redis, runner.name)] == [command.binding]


async def test_rejection_release_resets_task_local_counters(rejected):
    runner, *_ = rejected
    runner._error_diagnose_count = 3
    runner._error_skip_active = True
    runner._idle_dispatch_deferred = True
    assert (await post_reject(rejected)).status_code == 202

    await runner._consume_rejection_commands()

    assert runner.state.current_task is None
    assert runner._error_diagnose_count == 0
    assert runner._error_skip_active is False
    assert runner._idle_dispatch_deferred is False


async def test_pending_rejection_index_backfills_legacy_unreleased_and_prunes_released(rejected):
    runner, command, *_ = rejected
    assert (await post_reject(rejected)).status_code == 202
    await runner.redis.zrem(rejection_pending_index(runner.name), command.binding)
    await runner.redis.delete(rejection_pending_backfill_key(runner.name))

    pending = await list_pending_rejections(runner.redis, runner.name)

    assert [item.binding for item in pending] == [command.binding]
    indexed = await runner.redis.zrangebyscore(rejection_pending_index(runner.name), "-inf", "+inf")
    assert [item.decode() if isinstance(item, bytes) else item for item in indexed] == [command.binding]
    stored = await load_rejection(runner.redis, runner.name, command.binding)
    stored.released = True
    await runner.redis.set(rejection_key(runner.name, command.binding), stored.model_dump_json())
    await runner.redis.zadd(rejection_pending_index(runner.name), {command.binding: command.requested_at.timestamp()})

    assert await list_pending_rejections(runner.redis, runner.name) == []
    assert await runner.redis.zrangebyscore(rejection_pending_index(runner.name), "-inf", "+inf") == []


async def test_pending_rejection_backfill_missing_receipt_fails_closed(rejected):
    runner, command, *_ = rejected
    assert (await post_reject(rejected)).status_code == 202
    await runner.redis.zrem(rejection_pending_index(runner.name), command.binding)
    await runner.redis.delete(rejection_pending_backfill_key(runner.name))
    await runner.redis.delete(rejection_key(runner.name, command.binding))

    with pytest.raises(AttemptChanged, match="missing"):
        await list_pending_rejections(runner.redis, runner.name)


async def test_pending_rejection_backfill_skips_released_history(rejected):
    runner, command, *_ = rejected
    assert (await post_reject(rejected)).status_code == 202
    await runner.redis.zrem(rejection_pending_index(runner.name), command.binding)
    await runner.redis.delete(rejection_pending_backfill_key(runner.name))
    stored = await load_rejection(runner.redis, runner.name, command.binding)
    stored.released = True
    await runner.redis.set(rejection_key(runner.name, command.binding), stored.model_dump_json())

    assert await list_pending_rejections(runner.redis, runner.name) == []


async def test_historical_rejection_index_missing_receipt_fails_closed(rejected):
    runner, command, *_ = rejected
    assert (await post_reject(rejected)).status_code == 202
    await runner.redis.delete(rejection_key(runner.name, command.binding))

    with pytest.raises(AttemptChanged, match="missing"):
        await list_rejections(runner.redis, runner.name)


async def test_recent_rejection_history_reads_only_bounded_index_window(rejected):
    runner, command, *_ = rejected
    bindings = []
    for index in range(25):
        binding = f"{index:064x}"
        stored = command.model_copy(
            update={
                "binding": binding,
                "requested_at": command.requested_at + timedelta(seconds=index),
            }
        )
        bindings.append(binding)
        await runner.redis.set(rejection_key(runner.name, binding), stored.model_dump_json())
        await runner.redis.zadd(rejection_index(runner.name), {binding: stored.requested_at.timestamp()})

    calls = []

    async def zrevrange(key, start, stop):
        calls.append((key, start, stop))
        ordered = sorted(runner.redis.zsets[key].items(), key=lambda item: item[1], reverse=True)
        return [member for member, _ in ordered][start : stop + 1]

    runner.redis.zrevrange = zrevrange

    assert await list_rejections(runner.redis, runner.name, limit=0) == []
    recent = await list_rejections(runner.redis, runner.name, limit=20)

    assert calls == [(rejection_index(runner.name), 0, 19)]
    assert [command.binding for command in recent] == bindings[-20:]


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
        .replace("blocked_reason: operator_reject\n", "")
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
    assert attempt.coder_dispatched is False
    assert await runner._reconcile_git_admissions() == set()
    received = []

    async def coder(path, pr_id, task_file, task_body, **kwargs):
        received.append(task_body)
        assert (await load_attempt(runner.redis, runner.name, pr_id)).coder_dispatched is True
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


async def test_final_rejection_commits_operator_reject_marker_for_redis_loss(rejected):
    runner, command, repo, *_ = rejected

    assert (await post_reject(rejected)).status_code == 202
    await runner._run_cycle_body()

    stored = await load_rejection(runner.redis, runner.name, command.binding)
    assert stored.status == "rejected" and stored.released
    assert "blocked_reason: operator_reject" in git(repo, "show", "origin/main:tasks/PR-42.md")
    git(repo, "checkout", "main")
    task_path = repo / "tasks/PR-42.md"
    task_path.write_text(
        task_path.read_text()
        .replace("status: ERROR", "status: TODO")
        .replace("blocked_reason: operator_reject\n", "")
    )
    git(repo, "commit", "-am", "operator clears reject status only")
    git(repo, "push", "origin", "main")

    await runner.redis.delete(attempt_key(runner.name, "PR-42"))
    await runner.redis.delete(rejection_key(runner.name, command.binding))
    await runner.redis.delete(cause_key(runner.name, "PR-42"))
    await runner.redis.delete(index_key(runner.name))

    await runner._snapshot_accepted_specs()
    recovered = await load_attempt(runner.redis, runner.name, "PR-42")

    assert recovered.rejection == command.binding
    assert recovered.fingerprint == command.fingerprint
    assert await runner._reconcile_git_admissions() == {"PR-42"}


async def test_concurrent_rewrite_rejection_manifest_fences_restored_rejected_bytes(rejected):
    runner, command, repo, *_ = rejected
    original = (repo / "tasks/PR-42.md").read_text()
    git(repo, "checkout", "main")
    (repo / "tasks/PR-42.md").write_text(
        original.replace("Original specification.", "Concurrent replacement.")
        .replace("status: ERROR", "status: TODO")
        .replace("blocked_reason: guardrail\n", "")
    )
    git(repo, "commit", "-am", "operator rewrites before reject releases")
    git(repo, "push", "origin", "main")
    git(repo, "checkout", "fix/pr-42")

    assert (await post_reject(rejected)).status_code == 202
    await runner._run_cycle_body()

    stored = await load_rejection(runner.redis, runner.name, command.binding)
    manifest = json.loads(git(repo, "show", "origin/main:tasks/rejections.json"))
    assert stored.status == "rejected" and stored.released
    entry = manifest["rejections"]["PR-42"][0]
    assert entry["fingerprint"] == command.fingerprint
    assert entry["rejection_binding"] == command.binding
    assert entry["base_commit"] == stored.base_commit
    assert entry["branch_head"] == stored.branch_head
    assert "operator_reject" not in git(repo, "show", "origin/main:tasks/PR-42.md")

    git(repo, "checkout", "main")
    (repo / "tasks/PR-42.md").write_text(
        original.replace("status: ERROR", "status: TODO").replace("blocked_reason: guardrail\n", "")
    )
    git(repo, "commit", "-am", "restore rejected bytes without redis")
    git(repo, "push", "origin", "main")
    await runner.redis.delete(attempt_key(runner.name, "PR-42"))
    await runner.redis.delete(rejection_key(runner.name, command.binding))
    await runner.redis.delete(cause_key(runner.name, "PR-42"))
    await runner.redis.delete(index_key(runner.name))

    await runner._snapshot_accepted_specs()
    recovered = await load_attempt(runner.redis, runner.name, "PR-42")
    assert recovered.rejection == command.binding


@pytest.mark.parametrize(
    ("manifest", "message"),
    [
        ([], "manifest is invalid"),
        (
            {"schema_version": 1, "repository": "other/repo", "base_branch": "main", "rejections": {}},
            "does not match this runner",
        ),
        (
            {
                "schema_version": 1,
                "repository": "octo/demo",
                "base_branch": "main",
                "rejections": {"PR-42": {}},
            },
            "task record is invalid",
        ),
    ],
)
async def test_rejection_identity_manifest_invalid_shapes_defer(rejected, manifest, message):
    runner, command, repo, *_ = rejected
    git(repo, "checkout", "main")
    path = repo / "tasks/rejections.json"
    path.write_text(json.dumps(manifest))
    git(repo, "add", "tasks/rejections.json")
    git(repo, "commit", "-m", "invalid rejection manifest")
    git(repo, "push", "origin", "main")

    with pytest.raises(AttemptChanged, match=message):
        await runner._commit_rejection_identity(command)


async def test_rejection_identity_commit_is_idempotent(rejected):
    runner, command, *_ = rejected

    assert (await post_reject(rejected)).status_code == 202
    await runner._run_cycle_body()
    stored = await load_rejection(runner.redis, runner.name, command.binding)

    assert await runner._commit_rejection_identity(stored)


async def test_rejection_identity_commit_returns_false_on_git_failure(rejected, monkeypatch):
    runner, command, *_ = rejected
    original_git = daemon_reject.git_ops._git

    def failing_git(path, *args, **kwargs):
        if args[:2] == ("fetch", "origin"):
            raise OSError("git unavailable")
        return original_git(path, *args, **kwargs)

    monkeypatch.setattr(daemon_reject.git_ops, "_git", failing_git)

    assert not await runner._commit_rejection_identity(command)


async def test_rejection_defers_when_operator_marker_commit_fails(rejected, monkeypatch):
    runner, command, _, _, github, _ = rejected

    assert (await post_reject(rejected)).status_code == 202
    monkeypatch.setattr(runner, "_commit_task_status_change", AsyncMock(return_value=False))

    await runner._consume_rejection_commands()
    stored = await load_rejection(runner.redis, runner.name, command.binding)

    assert stored.status == "deferred" and not stored.released
    assert "Operator rejection marker could not be committed" in stored.reason
    assert github["state"] == "closed"


async def test_rejection_defers_when_rejection_identity_commit_fails(rejected, monkeypatch):
    runner, command, _, _, github, _ = rejected

    assert (await post_reject(rejected)).status_code == 202
    monkeypatch.setattr(runner, "_commit_task_status_change", AsyncMock(return_value=True))
    monkeypatch.setattr(runner, "_commit_rejection_identity", AsyncMock(return_value=False))

    await runner._consume_rejection_commands()
    stored = await load_rejection(runner.redis, runner.name, command.binding)

    assert stored.status == "deferred" and not stored.released
    assert "Operator rejection identity could not be committed" in stored.reason
    assert github["state"] == "closed"


async def test_pending_rejection_backfill_marker_written_after_success(rejected):
    runner, command, *_ = rejected
    assert (await post_reject(rejected)).status_code == 202
    await runner.redis.zrem(rejection_pending_index(runner.name), command.binding)
    original_zrangebyscore = runner.redis.zrangebyscore
    calls = 0

    async def flaky_zrangebyscore(key, *args, **kwargs):
        nonlocal calls
        if key == rejection_index(runner.name):
            calls += 1
            if calls == 1:
                raise OSError("redis unavailable")
        return await original_zrangebyscore(key, *args, **kwargs)

    runner.redis.zrangebyscore = flaky_zrangebyscore
    with pytest.raises(OSError):
        await list_pending_rejections(runner.redis, runner.name)
    assert await runner.redis.get(rejection_pending_backfill_key(runner.name)) is None

    pending = await list_pending_rejections(runner.redis, runner.name)
    assert [item.binding for item in pending] == [command.binding]
    assert await runner.redis.get(rejection_pending_backfill_key(runner.name)) == "1"


async def test_pending_rejection_backfill_marker_read_failure_returns_pending_only(rejected):
    runner, command, *_ = rejected
    assert (await post_reject(rejected)).status_code == 202
    await runner.redis.zrem(rejection_pending_index(runner.name), command.binding)
    original_get = runner.redis.get

    async def failing_get(key):
        if key == rejection_pending_backfill_key(runner.name):
            raise OSError("redis unavailable")
        return await original_get(key)

    runner.redis.get = failing_get

    assert await list_pending_rejections(runner.redis, runner.name) == []


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
        runner.repo_config.url, runner.state.current_task, (repo / "tasks/PR-42.md").read_text(),
        started=False, coder_dispatched=False,
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
            lambda args, *a, **kw: [{"number": 99}] if "--slurp" in args else original(args, *a, **kw),
        )
    await runner._consume_rejection_commands()
    first = await load_rejection(runner.redis, runner.name, command.binding)
    assert first.released is (not ambiguous)
    assert first.status == ("deferred" if ambiguous else "rejected")
    await runner._consume_rejection_commands()
    result = await load_rejection(runner.redis, runner.name, command.binding)
    assert result.released is (not ambiguous)
    if not ambiguous:
        assert result.branch_head == git(repo, "rev-parse", "fix/pr-42")
        assert "no PR was created" in result.reason
    assert all(call[:2] != ["pr", "close"] for call in github["calls"])


async def test_dispatched_pre_pr_reject_confirms_absence_then_releases(rejected, monkeypatch):
    from src.task_attempts import new_attempt, save_attempt

    runner, _, repo, _, github, _ = rejected
    runner.state.current_pr = None
    attempt = new_attempt(
        runner.repo_config.url,
        runner.state.current_task,
        (repo / "tasks/PR-42.md").read_text(),
        started=True,
        coder_dispatched=True,
    )
    runner.state.current_task.attempt_id = attempt.attempt_id
    await save_attempt(runner.redis, runner.name, attempt, expected=None)
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    command = build_rejection(runner.name, runner.state, await runner.redis.get(cause_key(runner.name, "PR-42")), repo)
    await enqueue_rejection(runner.redis, command)

    original = daemon_reject.gh_runner.run_gh
    monkeypatch.setattr(
        daemon_reject.gh_runner,
        "run_gh",
        lambda args, *a, **kw: [[]] if "--slurp" in args else original(args, *a, **kw),
    )

    await runner._consume_rejection_commands()
    first = await load_rejection(runner.redis, runner.name, command.binding)
    assert first.status == "deferred"
    assert first.absence_confirmed and not first.released
    first.absence_confirmed_at = datetime.now(timezone.utc) - timedelta(seconds=61)
    await runner.redis.set(rejection_key(runner.name, command.binding), first.model_dump_json())

    await runner._consume_rejection_commands()
    result = await load_rejection(runner.redis, runner.name, command.binding)
    assert result.status == "rejected"
    assert result.released
    assert result.branch_head == git(repo, "rev-parse", "fix/pr-42")
    assert "no PR was created" in result.reason
    assert all(call[:2] != ["pr", "close"] for call in github["calls"])


async def test_reject_persists_base_anchor_before_no_pr_discovery_failure(rejected, monkeypatch):
    from src.task_attempts import new_attempt, save_attempt

    runner, _, repo, *_ = rejected
    runner.state.current_pr = None
    attempt = new_attempt(
        runner.repo_config.url,
        runner.state.current_task,
        (repo / "tasks/PR-42.md").read_text(),
        started=True,
        coder_dispatched=True,
    )
    runner.state.current_task.attempt_id = attempt.attempt_id
    await save_attempt(runner.redis, runner.name, attempt, expected=None)
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    command = build_rejection(runner.name, runner.state, await runner.redis.get(cause_key(runner.name, "PR-42")), repo)
    await enqueue_rejection(runner.redis, command)

    def unavailable(*args, **kwargs):
        raise RuntimeError("discovery unavailable")

    monkeypatch.setattr(daemon_reject, "discover_attempt_pr", unavailable)

    await runner._consume_rejection_commands()

    stored = await load_rejection(runner.redis, runner.name, command.binding)
    assert stored.status == "deferred"
    assert stored.base_commit == git(repo, "rev-parse", "origin/main")


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


@pytest.mark.parametrize("case", ["ambiguous_creation", "known_pr", "divergent_refs", "late_process"])
async def test_pre_pr_closure_waits_for_ambiguous_effects_and_checkout_ownership(rejected, monkeypatch, case):
    from src.task_attempts import new_attempt, save_attempt

    runner, _, repo, _, github, _ = rejected
    runner.state.current_pr = None
    attempt = new_attempt(
        runner.repo_config.url,
        runner.state.current_task,
        (repo / "tasks/PR-42.md").read_text(),
        started=False,
        pr_creation_pending=case == "ambiguous_creation",
        pr_number=42 if case == "known_pr" else None,
        coder_dispatched=False,
    )
    runner.state.current_task.attempt_id = attempt.attempt_id
    await save_attempt(runner.redis, runner.name, attempt, expected=None)
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    command = build_rejection(runner.name, runner.state, await runner.redis.get(cause_key(runner.name, "PR-42")), repo)
    await enqueue_rejection(runner.redis, command)
    if case == "known_pr":
        assert (await load_attempt(runner.redis, runner.name, "PR-42")).pr_number == 42
    if case == "divergent_refs":
        git(repo, "commit", "--allow-empty", "-m", "local unpublished commit")
    elif case == "late_process":
        calls = 0

        def blocker(path):
            nonlocal calls
            calls += 1
            return "A process appeared before checkout release" if calls >= 2 else None

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


@pytest.mark.parametrize(
    "case",
    [
        "pause",
        "stop",
        "budget",
        "rejected",
        "ambiguous",
        "fork_while_hidden",
        "changed_while_polling",
        "pause_while_polling",
        "stop_while_polling",
        "cas_conflict",
        "skip_trigger",
    ],
)
async def test_pending_pr_reconciliation_respects_controls_and_attempt_changes(rejected, monkeypatch, case):
    from src.daemon.handlers import error
    from src.keyspace import control_stop
    from src.task_attempts import new_attempt, save_attempt

    runner, _, repo, _, _, _ = rejected
    candidate = runner.state.current_pr.model_copy(deep=True)
    runner.state.current_pr = None
    attempt = new_attempt(
        runner.repo_config.url,
        runner.state.current_task,
        (repo / "tasks/PR-42.md").read_text(),
        started=True,
        pr_creation_pending=True,
    )
    runner.state.current_task.attempt_id = attempt.attempt_id
    await save_attempt(runner.redis, runner.name, attempt, expected=None)
    await runner.publish_state()
    calls = []
    reviews = []

    def pause():
        paused = runner.state.model_copy(deep=True)
        paused.user_paused = True
        runner.redis.store[pipeline_state(runner.name)] = paused.model_dump_json()

    if case == "pause":
        pause()
    elif case == "stop":
        await runner.redis.set(control_stop(runner.name), "1")
    elif case == "budget":
        monkeypatch.setattr(runner, "_check_github_api_budget", AsyncMock(return_value=False))
    elif case == "rejected":
        command = build_rejection(
            runner.name, runner.state, await runner.redis.get(cause_key(runner.name, "PR-42")), repo
        )
        await enqueue_rejection(runner.redis, command)
    elif case == "cas_conflict":

        async def conflict(redis, name, updated, *, expected):
            concurrent = expected.model_copy(update={"rejection": "concurrent-reject"})
            await save_attempt(redis, name, concurrent, expected=expected)
            return await save_attempt(redis, name, updated, expected=expected)

        monkeypatch.setattr(error, "save_attempt", conflict)
    elif case == "skip_trigger":
        monkeypatch.setattr(runner, "_should_skip_codex_review_post", lambda number: True)

    row = raw_attempt_pr(candidate)
    if case == "fork_while_hidden":
        row["head"]["repo"]["full_name"] = "outsider/demo"

    def get_attempt_prs(args, *a, **kwargs):
        if args[:3] != ["api", "--paginate", "--slurp"]:
            return row
        calls.append("lookup")
        if case == "changed_while_polling":
            concurrent = attempt.model_copy(update={"rejection": "concurrent-reject"})
            runner.redis.store[attempt_key(runner.name, "PR-42")] = concurrent.model_dump_json()
        elif case == "pause_while_polling":
            pause()
        elif case == "stop_while_polling":
            runner.redis.store[control_stop(runner.name)] = "1"
        if case == "ambiguous":
            return [[row, {**row, "number": 43}]]
        return [[row]]

    monkeypatch.setattr(daemon_reject.gh_runner, "run_gh", get_attempt_prs)
    monkeypatch.setattr(runner, "_post_codex_review", lambda number: reviews.append(number))
    assert await runner._reconcile_pending_pr_creation()
    current = await load_attempt(runner.redis, runner.name, "PR-42")
    if case == "skip_trigger":
        assert runner.state.state == PipelineState.WATCH
        assert not current.pr_creation_pending
    else:
        assert runner.state.state == PipelineState.ERROR
        assert runner.state.current_pr is None
        assert current.pr_creation_pending
    assert reviews == []
    assert len(calls) == (0 if case in {"pause", "stop", "budget", "rejected"} else 1)
    if "pause" in case or "stop" in case:
        assert runner.state.user_paused


@pytest.mark.parametrize("single", [False, True])
@pytest.mark.parametrize("lost_close_ack", [False, True])
@pytest.mark.parametrize("pending_creation", [False, True])
@pytest.mark.parametrize("dispatch_marker", [None, True])
async def test_guardrail_before_pr_tracking_resolves_current_pr_and_ignores_history(
    rejected, monkeypatch, single, lost_close_ack, pending_creation, dispatch_marker
):
    from datetime import timedelta

    from src.task_attempts import new_attempt, save_attempt

    runner, _, repo, _, github, _ = rejected
    current_pr = runner.state.current_pr.model_copy(deep=True)
    runner.state.current_pr = None
    runner.repo_config.feature_flags.use_single_error_exit = single
    attempt = new_attempt(
        runner.repo_config.url,
        runner.state.current_task,
        (repo / "tasks/PR-42.md").read_text(),
        started=True,
        pr_creation_pending=pending_creation,
        coder_dispatched=dispatch_marker,
    )
    runner.state.current_task.attempt_id = attempt.attempt_id
    await save_attempt(runner.redis, runner.name, attempt, expected=None)
    old = raw_attempt_pr(
        current_pr.model_copy(update={"number": 41}),
        state="closed",
        created_at=(attempt.accepted_at - timedelta(days=1)).isoformat(),
    )
    fork = raw_attempt_pr(current_pr.model_copy(update={"number": 88}))
    fork["head"]["repo"]["full_name"] = "outsider/demo"
    github["attempt_prs"] = [old, fork, raw_attempt_pr(current_pr)]
    # This is coder output, not an executed command. The real scanner parks
    # before the normal PR lookup, recreating the reported tracking gap.
    await runner._post_coder_resolution(
        "claude",
        0,
        "gh repo create octo/demo\n",
        "",
        target_branch="fix/pr-42",
        current_pr_id="PR-42",
    )
    assert runner.state.state == PipelineState.ERROR and runner.state.current_pr is None
    await runner.publish_state()
    command = build_rejection(runner.name, runner.state, await runner.redis.get(cause_key(runner.name, "PR-42")), repo)
    assert command.pr is None
    assert (await post_reject(rejected, binding=command.binding)).status_code == 202
    original_transport = daemon_reject.gh_runner.run_gh
    visible = False

    def transport(args, *a, **kwargs):
        if not visible and args[:3] == ["api", "--paginate", "--slurp"] and "/pulls?" in args[-1]:
            return [[]]
        result = original_transport(args, *a, **kwargs)
        if lost_close_ack and args[:2] == ["pr", "close"]:
            raise TimeoutError("close reply lost")
        return result

    monkeypatch.setattr(daemon_reject.gh_runner, "run_gh", transport)
    # Visibility may lag for arbitrarily many queries, including after restart.
    # A prior daemon's negative observation is not durable non-creation proof.
    pending = await load_rejection(runner.redis, runner.name, command.binding)
    pending.absence_confirmed = True
    await runner.redis.set(rejection_key(runner.name, command.binding), pending.model_dump_json())
    for cycle in range(4):
        if cycle == 2:
            fresh = h._make_runner()
            fresh.repo_path, fresh.redis = runner.repo_path, runner.redis
            fresh.repo_config.feature_flags.use_single_error_exit = single
            runner = fresh
        await runner._consume_rejection_commands()
        stored = await load_rejection(runner.redis, runner.name, command.binding)
        assert stored.status == "deferred" and not stored.released and stored.pr is None
        assert github["state"] == "open"
    visible = True
    await runner._consume_rejection_commands()
    stored = await load_rejection(runner.redis, runner.name, command.binding)
    assert stored.pr.number == 42
    if lost_close_ack:
        assert stored.status == "deferred" and not stored.released
        fresh = h._make_runner()
        fresh.repo_path, fresh.redis = runner.repo_path, runner.redis
        fresh.repo_config.feature_flags.use_single_error_exit = single
        await fresh._consume_rejection_commands()
    stored = await load_rejection(runner.redis, runner.name, command.binding)
    assert stored.status == "rejected" and stored.released
    assert sum(call[:2] == ["pr", "close"] for call in github["calls"]) == 1
    assert old["state"] == "closed" and fork["state"] == "open"


@pytest.mark.parametrize("single", [False, True])
@pytest.mark.parametrize("terminal", ["closed", "merged"])
async def test_pending_creation_reconciles_terminal_pr_and_releases_ownership(rejected, monkeypatch, single, terminal):
    from src.task_attempts import new_attempt, save_attempt

    runner, _, repo, _, github, _ = rejected
    pr = runner.state.current_pr.model_copy(deep=True)
    runner.state.current_pr = None
    runner.repo_config.feature_flags.use_single_error_exit = single
    attempt = new_attempt(
        runner.repo_config.url,
        runner.state.current_task,
        (repo / "tasks/PR-42.md").read_text(),
        started=True,
        pr_creation_pending=True,
    )
    runner.state.current_task.attempt_id = attempt.attempt_id
    await save_attempt(runner.redis, runner.name, attempt, expected=None)
    await runner.redis.delete(cause_key(runner.name, "PR-42"))
    await runner.publish_state()
    github["attempt_prs"] = [
        raw_attempt_pr(
            pr,
            state="closed",
            merged_at=datetime.now(timezone.utc).isoformat() if terminal == "merged" else None,
        )
    ]
    reviews = []
    monkeypatch.setattr(runner, "_post_codex_review", lambda number: reviews.append(number))
    await runner._run_cycle_body()
    receipt = await load_attempt(runner.redis, runner.name, "PR-42")
    assert not receipt.pr_creation_pending
    assert receipt.pr_number == 42 and receipt.completed is (terminal == "merged")
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None and runner.state.current_pr is None
    assert not reviews
    assert not any(call[:2] in (["pr", "create"], ["pr", "close"]) for call in github["calls"])
    if terminal == "merged":
        assert runner.state.current_queue[0].status == TaskStatus.DONE


@pytest.mark.parametrize(
    "case",
    [
        "fork_only",
        "old_only",
        "other_branch",
        "wrong_base",
        "wrong_target_repo",
        "wrong_receipt_repo",
        "unexpected_head",
        "multiple",
        "bad_pages",
        "changed_exact",
        "invalid_state",
        "known_other_number",
        "known_number",
        "known_number_ignores_wrong_base_history",
        "old_wrong_base_with_new_match",
        "fork_and_owned",
        "missing_branch",
        "terminal_missing_branch",
    ],
)
async def test_pr_discovery_requires_attempt_repository_base_time_and_head(rejected, monkeypatch, case):
    from copy import deepcopy

    from src.daemon.attempt_prs import discover_attempt_pr
    from src.task_attempts import new_attempt

    runner, _, repo, remote, github, _ = rejected
    pr = runner.state.current_pr
    attempt = new_attempt(
        runner.repo_config.url,
        runner.state.current_task,
        (repo / "tasks/PR-42.md").read_text(),
        started=True,
        pr_creation_pending=True,
    )
    row = raw_attempt_pr(pr)
    rows = [row]
    if case == "fork_only":
        row["head"]["repo"]["full_name"] = "outsider/demo"
    elif case == "old_only":
        row["created_at"] = (attempt.accepted_at - timedelta(days=1)).isoformat()
        row["state"] = "closed"
    elif case == "other_branch":
        row["head"]["ref"] = "other"
    elif case == "wrong_base":
        row["base"]["ref"] = "other"
    elif case == "wrong_target_repo":
        row["base"]["repo"]["full_name"] = "another/repo"
    elif case == "wrong_receipt_repo":
        attempt.repo_url = "https://github.com/another/repo.git"
    elif case == "unexpected_head":
        row["head"]["sha"] = "f" * 40
    elif case == "multiple":
        rows.append({**deepcopy(row), "number": 43})
    elif case == "invalid_state":
        row["state"] = "unknown"
    elif case in {"known_other_number", "known_number"}:
        attempt.pr_number = 99 if case == "known_other_number" else pr.number
    elif case == "known_number_ignores_wrong_base_history":
        attempt.pr_number = pr.number
        historical = deepcopy(row)
        historical["number"] = 41
        historical["base"]["ref"] = "release"
        historical["state"] = "closed"
        rows = [historical, row]
    elif case == "old_wrong_base_with_new_match":
        historical = deepcopy(row)
        historical["number"] = 41
        historical["created_at"] = (attempt.accepted_at - timedelta(days=1)).isoformat()
        historical["base"]["ref"] = "release"
        historical["state"] = "closed"
        rows = [historical, row]
    elif case == "fork_and_owned":
        fork = deepcopy(row)
        fork["number"] = 88
        fork["head"]["repo"]["full_name"] = "outsider/demo"
        rows.insert(0, fork)
    elif case in {"missing_branch", "terminal_missing_branch"}:
        if case == "terminal_missing_branch":
            row["state"] = "closed"
            row["merged_at"] = datetime.now(timezone.utc).isoformat()
        git(repo, "checkout", "main")
        git(repo, "update-ref", "-d", "refs/heads/fix/pr-42")
        git(remote, "update-ref", "-d", "refs/heads/fix/pr-42")

    def transport(args, *a, **kwargs):
        if "--slurp" in args:
            return {} if case == "bad_pages" else [rows]
        if case == "changed_exact":
            changed = deepcopy(row)
            changed["head"]["repo"]["full_name"] = "outsider/demo"
            return changed
        return row

    monkeypatch.setattr(daemon_reject.gh_runner, "run_gh", transport)
    if case in {"fork_only", "old_only", "other_branch", "known_other_number"}:
        assert discover_attempt_pr(str(repo), runner.owner_repo, "main", attempt) is None
    elif case in {
        "known_number",
        "known_number_ignores_wrong_base_history",
        "old_wrong_base_with_new_match",
        "fork_and_owned",
        "terminal_missing_branch",
    }:
        assert discover_attempt_pr(str(repo), runner.owner_repo, "main", attempt)["number"] == 42
    else:
        with pytest.raises(AttemptChanged):
            discover_attempt_pr(str(repo), runner.owner_repo, "main", attempt)


@pytest.mark.parametrize("push_timing", ["before_request", "after_request"])
@pytest.mark.parametrize("lost_close_ack", [False, True])
async def test_reject_rebinds_a_quiescent_same_attempt_push_and_preserves_replay(
    rejected, monkeypatch, push_timing, lost_close_ack
):
    from tests.test_task_admission import rewritten, stage

    runner, command, repo, remote, github, _ = rejected
    original_head = command.pr.head_sha
    if push_timing == "after_request":
        assert (await post_reject(rejected)).status_code == 202
    (repo / "late-attempt-work.txt").write_text("pushed after the decision was rendered")
    git(repo, "add", "late-attempt-work.txt")
    git(repo, "commit", "-m", "late attempt push")
    git(repo, "push", "origin", "fix/pr-42")
    updated_head = git(repo, "rev-parse", "HEAD")
    github["attempt_prs"] = [raw_attempt_pr(command.pr.model_copy(update={"head_sha": updated_head}))]
    if push_timing == "before_request":
        assert (await post_reject(rejected)).status_code == 202
    original_transport = daemon_reject.gh_runner.run_gh

    def transport(args, *a, **kwargs):
        result = original_transport(args, *a, **kwargs)
        if lost_close_ack and args[:2] == ["pr", "close"]:
            raise TimeoutError("close response lost after successful closure")
        return result

    monkeypatch.setattr(daemon_reject.gh_runner, "run_gh", transport)
    await runner._consume_rejection_commands()
    stored = await load_rejection(runner.redis, runner.name, command.binding)
    assert stored.initial_head_sha == original_head
    assert stored.pr.head_sha == updated_head and stored.branch_head == updated_head
    assert stored.pr.number == command.pr.number and stored.binding == command.binding
    if lost_close_ack:
        assert stored.status == "deferred" and not stored.released
        fresh = h._make_runner()
        fresh.repo_path, fresh.redis = runner.repo_path, runner.redis
        await fresh._consume_rejection_commands()
        runner.state = fresh.state
    stored = await load_rejection(runner.redis, runner.name, command.binding)
    assert stored.status == "rejected" and stored.released
    assert (await post_reject(rejected)).status_code == 202
    assert sum(call[:2] == ["pr", "close"] for call in github["calls"]) == 1
    assert (await stage(rejected, rewritten(repo))).status_code == 200
    assert await runner.process_pending_uploads()
    attempt = await load_attempt(runner.redis, runner.name, "PR-42")
    runner.state.current_task = attempt.task
    assert await runner._prepare_task_attempt((repo / "tasks/PR-42.md").read_text())
    assert "refs/heads/fix/pr-42" not in git(remote, "show-ref")
    assert not (repo / "late-attempt-work.txt").exists()


@pytest.mark.parametrize("mismatch", ["remote_only", "local_missing", "api_only"])
async def test_rejection_head_refresh_requires_local_and_remote_ownership(rejected, mismatch):
    runner, command, repo, _, github, _ = rejected
    original_head = command.pr.head_sha
    assert (await post_reject(rejected)).status_code == 202
    (repo / "external-update.txt").write_text("unattributed update")
    git(repo, "add", "external-update.txt")
    git(repo, "commit", "-m", "external update")
    new_head = git(repo, "rev-parse", "HEAD")
    if mismatch != "api_only":
        git(repo, "push", "origin", "fix/pr-42")
    git(repo, "checkout", "main")
    if mismatch == "local_missing":
        git(repo, "update-ref", "-d", "refs/heads/fix/pr-42")
    else:
        git(repo, "update-ref", "refs/heads/fix/pr-42", original_head)
    github["attempt_prs"] = [raw_attempt_pr(command.pr.model_copy(update={"head_sha": new_head}))]
    await runner._consume_rejection_commands()
    stored = await load_rejection(runner.redis, runner.name, command.binding)
    assert stored.status == "deferred" and not stored.released
    assert stored.pr.head_sha == original_head
    assert not any(call[:2] == ["pr", "close"] for call in github["calls"])


async def test_merged_pr_with_new_head_records_completion_without_rebinding_or_closing(rejected):
    runner, command, _, _, github, _ = rejected
    assert (await post_reject(rejected)).status_code == 202
    github["attempt_prs"] = [
        raw_attempt_pr(
            command.pr.model_copy(update={"head_sha": "f" * 40}),
            state="closed",
            merged_at=datetime.now(timezone.utc).isoformat(),
        )
    ]
    await runner._consume_rejection_commands()
    stored = await load_rejection(runner.redis, runner.name, command.binding)
    assert stored.status == "merged" and stored.released
    assert (await load_attempt(runner.redis, runner.name, "PR-42")).completed
    assert not any(call[:2] == ["pr", "close"] for call in github["calls"])


@pytest.mark.parametrize("changed", ["repository", "base", "number", "missing_head"])
async def test_head_refresh_never_changes_the_bound_pr_identity(rejected, monkeypatch, changed):
    runner, command, _, _, github, _ = rejected
    assert (await post_reject(rejected)).status_code == 202
    row = raw_attempt_pr(command.pr)
    if changed == "repository":
        row["head"]["repo"]["full_name"] = "outsider/demo"
    elif changed == "base":
        row["base"]["ref"] = "another-base"
    elif changed == "number":
        row["number"] = 99
    else:
        row["head"]["sha"] = ""
    original_transport = daemon_reject.gh_runner.run_gh

    def transport(args, *a, **kwargs):
        if args == ["api", f"repos/{runner.owner_repo}/pulls/42"]:
            return row
        return original_transport(args, *a, **kwargs)

    monkeypatch.setattr(daemon_reject.gh_runner, "run_gh", transport)
    await runner._consume_rejection_commands()
    stored = await load_rejection(runner.redis, runner.name, command.binding)
    assert stored.status == "deferred" and not stored.released
    assert stored.pr == command.pr
    assert not any(call[:2] == ["pr", "close"] for call in github["calls"])
