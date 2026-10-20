"""Tests for MERGE cleanup metadata."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from src.cancellation import retry_count_key, task_spec_hash_key
from src.daemon import git_ops as git_ops_module
from src.daemon import runner as runner_module
from src.daemon.handlers import merge as merge_module
from src.daemon.retry_commands import RetryDispatch
from src.models import PipelineState, PRInfo, QueueTask, ReviewStatus, TaskStatus

from tests.runner import _helpers as h

_CONFIRM_REMOTE_MERGE = merge_module.MergeMixin._confirm_remote_merge


def test_merge_cleans_up_hash_and_retry_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        "src.github.prs.merge_pr", lambda repo, num, expected_head: None
    )
    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_mark_task_done_in_snapshot",
        lambda self: None,
    )

    runner = h._make_runner()
    runner.redis.store[task_spec_hash_key("octo__demo", "PR-001")] = "abc123"
    runner.redis.store[retry_count_key("octo__demo", "PR-001")] = "2"
    runner.state.state = PipelineState.MERGE
    runner.state.current_pr = PRInfo(number=5, branch="pr-001", head_sha="a" * 40)
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="Sample",
        status=TaskStatus.DOING,
    )

    runner._start_current_run_record("claude", "opus")
    asyncio.run(runner.handle_merge())

    assert task_spec_hash_key("octo__demo", "PR-001") not in runner.redis.store
    assert retry_count_key("octo__demo", "PR-001") not in runner.redis.store


def test_merge_logs_cleanup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        "src.github.prs.merge_pr", lambda repo, num, expected_head: None
    )
    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_mark_task_done_in_snapshot",
        lambda self: None,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.MERGE
    runner.state.current_pr = PRInfo(number=5, branch="pr-001", head_sha="a" * 40)
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="Sample",
        status=TaskStatus.DOING,
    )
    events: list[str] = []

    def record_event(message: str, **_kwargs: object) -> None:
        events.append(message)

    monkeypatch.setattr(runner, "log_event", record_event)

    async def fail_delete(key: str) -> int:
        raise RuntimeError("redis down")

    runner.redis.delete = fail_delete

    runner._start_current_run_record("claude", "opus")
    asyncio.run(runner.handle_merge())

    assert any("Failed to clear retry metadata for PR-001" in event for event in events)


def test_merge_passes_observed_full_head_to_merge_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    expected_head = "d" * 40
    calls: list[tuple[str, int, str]] = []
    monkeypatch.setattr(
        "src.github.prs.merge_pr",
        lambda repo, num, expected_head_sha: calls.append(
            (repo, num, expected_head_sha)
        ),
    )
    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_mark_task_done_in_snapshot",
        lambda self: None,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.MERGE
    runner.state.current_pr = PRInfo(
        number=5,
        branch="pr-001",
        head_sha=expected_head,
    )
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="Sample",
        status=TaskStatus.DOING,
    )

    asyncio.run(runner.handle_merge())

    assert calls == [("octo/demo", 5, expected_head)]


def test_merge_expected_head_mismatch_returns_to_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    invalidated: list[str] = []

    def stale_head(repo: str, num: int, expected_head_sha: str) -> None:
        raise merge_module.gh_prs.ExpectedHeadMismatch("head changed")

    monkeypatch.setattr("src.github.prs.merge_pr", stale_head)
    monkeypatch.setattr(
        "src.github.cache._invalidate_etag_cache",
        lambda prefix: invalidated.append(prefix),
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.MERGE
    runner.state.current_pr = PRInfo(
        number=5,
        branch="pr-001",
        head_sha="e" * 40,
        review_status=ReviewStatus.APPROVED,
    )
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="Sample",
        status=TaskStatus.DOING,
    )

    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr.review_status == ReviewStatus.PENDING
    assert invalidated == ["repos/octo/demo/pulls"]
    assert any(
        "Expected HEAD mismatch" in entry.get("event", "")
        for entry in runner.state.history
    )


def test_post_codex_review_bypass_author_dedup_skips_dedup_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        "src.github.prs.get_pr_metadata",
        lambda repo, num: {
            "author": "AlexBomber12",
            "head_sha": "abc123",
            "head_commit_date": "2026-01-01T00:00:00Z",
        },
    )
    monkeypatch.setattr(
        "src.github.comments.has_recent_codex_review_request",
        lambda repo, num, pr_author, after_iso: True,
    )
    posted: list[tuple[str, int, str]] = []
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )

    runner = h._make_runner()
    result = runner._post_codex_review_result(42, bypass_author_dedup=True)

    assert posted == [("octo/demo", 42, "@codex review")]
    assert result == (True, True, None)
    assert not any(
        "Skipping duplicate" in entry.get("event", "")
        for entry in runner.state.history
    )


def test_post_codex_review_without_bypass_still_honors_author_dedup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        "src.github.prs.get_pr_metadata",
        lambda repo, num: {
            "author": "AlexBomber12",
            "head_sha": "abc123",
            "head_commit_date": "2026-01-01T00:00:00Z",
        },
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_head_publication",
        lambda *_a, **kw: SimpleNamespace(
            repo="octo/demo", head_repo="octo/demo", ref="refs/heads/pr-42",
            head_sha=kw["expected_head_sha"],
            published_at=datetime(2026, 1, 1, tzinfo=timezone.utc)
        ),
    )
    monkeypatch.setattr(
        "src.github.comments.has_recent_codex_review_request",
        lambda repo, num, pr_author, after_iso: True,
    )
    monkeypatch.setattr(
        "src.github.comments.get_recent_codex_review_request_time",
        lambda repo, num, pr_author, after_iso: None,
    )
    posted: list[tuple[str, int, str]] = []
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )

    runner = h._make_runner()
    success, posted_flag, retry_at = runner._post_codex_review_result(42)

    assert posted == []
    assert success is True
    assert posted_flag is False
    assert retry_at is None
    assert any(
        "Skipping duplicate @codex review for PR #42; "
        "PR author already requested review for this head."
        in entry.get("event", "")
        for entry in runner.state.history
    )


def test_pre_merge_sync_path_passes_bypass_author_dedup_true(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_kwargs: list[dict[str, object]] = []

    def fake_git(repo_path: str, *args: str, **kwargs: object):
        if args[:1] == ("merge",) and len(args) > 1 and str(args[1]).startswith("origin/"):
            return h._FakeCompletedProcess(
                stdout="Merge made by the 'ort' strategy.\n",
                returncode=0,
            )
        if args[:2] == ("rev-parse", "HEAD"):
            return h._FakeCompletedProcess(
                stdout="deadbeefcafe\n", returncode=0,
            )
        return h._FakeCompletedProcess(stdout="", returncode=0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(merge_module, "retry_transient", lambda op, **_: op())
    monkeypatch.setattr(
        "src.github.cache._invalidate_etag_cache",
        lambda prefix: None,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.MERGE
    runner.state.current_pr = PRInfo(number=7, branch="pr-007")

    def fake_post(pr_number: int, **kwargs: object) -> bool:
        captured_kwargs.append({"pr_number": pr_number, **kwargs})
        return True

    monkeypatch.setattr(runner, "_post_codex_review", fake_post)

    asyncio.run(runner.handle_merge())

    assert captured_kwargs == [
        {"pr_number": 7, "bypass_author_dedup": True}
    ]
    assert any(
        entry.get("event", "").startswith(
            "[MERGE] Bypass-requesting fresh @codex review on new head"
        )
        for entry in runner.state.history
    )


def test_pre_merge_sync_logs_unknown_head_when_rev_parse_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rev_parse_calls = {"n": 0}

    def fake_git(repo_path: str, *args: str, **kwargs: object):
        if args[:1] == ("merge",) and len(args) > 1 and str(args[1]).startswith("origin/"):
            return h._FakeCompletedProcess(
                stdout="Merge made by the 'ort' strategy.\n",
                returncode=0,
            )
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["n"] += 1
            raise RuntimeError("rev-parse HEAD failed")
        return h._FakeCompletedProcess(stdout="", returncode=0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(merge_module, "retry_transient", lambda op, **_: op())
    monkeypatch.setattr(
        "src.github.cache._invalidate_etag_cache",
        lambda prefix: None,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.MERGE
    runner.state.current_pr = PRInfo(number=11, branch="pr-011")
    monkeypatch.setattr(runner, "_post_codex_review", lambda *a, **kw: True)

    asyncio.run(runner.handle_merge())

    assert any(
        "[MERGE] Bypass-requesting fresh @codex review on new head <unknown>"
        in entry.get("event", "")
        for entry in runner.state.history
    )


@pytest.fixture(autouse=True)
def confirmed_remote_merge(monkeypatch):
    async def confirmed(self):
        return True

    monkeypatch.setattr("src.daemon.handlers.merge.MergeMixin._confirm_remote_merge", confirmed)


@pytest.mark.parametrize("case", [
    "success", "lost", "open", "closed", "head", "base", "repo",
    "unknown", "status_false", "status_error", "metrics_error",
])
def test_merge_component_requires_exact_remote_proof(monkeypatch, case):
    """Exercise production merge action, exact reader and handler with a gh transport."""
    import json

    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(merge_module.MergeMixin, "_confirm_remote_merge", _CONFIRM_REMOTE_MERGE)
    runner = h._make_runner()
    runner.state.state = PipelineState.MERGE
    runner.state.current_pr = PRInfo(number=5, branch="pr-001", head_sha="a" * 40,
                                     review_status=ReviewStatus.APPROVED)
    task = QueueTask(pr_id="PR-001", title="Sample", status=TaskStatus.DOING,
                     task_file="tasks/PR-001.md")
    runner.state.current_task = task
    runner.state.current_queue = [task]
    runner._start_current_run_record("claude", "opus")
    calls = []
    writes = []
    outcomes = []
    payload = {
        "number": 5, "base": {"ref": "main", "repo": {"full_name": "octo/demo"}},
        "head": {"sha": "a" * 40}, "state": "closed", "merged": True,
        "merge_commit_sha": "b" * 40,
    }
    if case == "open":
        payload.update(state="open", merged=False)
    if case == "closed":
        payload["merged"] = False
    if case == "head":
        payload["head"]["sha"] = "c" * 40
    if case == "base":
        payload["base"]["ref"] = "release"
    if case == "repo":
        payload["base"]["repo"]["full_name"] = "other/repo"
    reads_fail = case == "unknown"

    def transport(args, **kwargs):
        calls.append((args, kwargs))
        if args[:2] == ["pr", "merge"] and case in {"lost", "unknown"}:
            raise RuntimeError("response lost")
        if args[:1] == ["api"]:
            assert args[1] == "repos/octo/demo/pulls/5"
            if reads_fail:
                raise RuntimeError("read unavailable")
            return "HTTP/2.0 200 OK\r\nETag: v1\r\n\r\n" + json.dumps(payload)
        return ""

    async def status(*args, **kwargs):
        writes.append(args)
        if case == "status_error":
            raise RuntimeError("status push failed")
        return case != "status_false"

    async def save(reason, **kwargs):
        if case == "metrics_error" and not outcomes:
            outcomes.append("failed save")
            raise RuntimeError("metrics unavailable")
        outcomes.append(reason)

    monkeypatch.setattr(merge_module.gh_runner, "run_gh", transport)
    monkeypatch.setattr(runner, "_commit_task_status_change", status)
    monkeypatch.setattr(runner, "_save_current_run_record", save)
    monkeypatch.setattr(merge_module, "log_merged_pr", lambda record: None)
    asyncio.run(runner.handle_merge())
    merge_calls = [args for args, _ in calls if args[:2] == ["pr", "merge"]]
    assert merge_calls == [["pr", "merge", "5", "--squash", "--delete-branch",
                            "--match-head-commit", "a" * 40]]
    assert calls[1][1]["repo"] == "octo/demo"
    if case in {"unknown", "base", "repo", "metrics_error"}:
        assert runner.state.state == PipelineState.MERGE
        assert runner.state.current_task is task
        if case != "metrics_error":
            assert not writes and not outcomes
            assert task.status == TaskStatus.DOING
        # Reconciliation cannot issue another merge or touch the deleted branch.
        monkeypatch.setattr(git_ops_module, "_git", lambda *a, **kw: pytest.fail("git during reconciliation"))
        monkeypatch.setattr(runner, "_compute_diff_stats", lambda base: {})
        if case == "unknown":
            from unittest.mock import AsyncMock

            runner._recovered = True
            for method in ("ensure_repo_cloned", "_refresh_user_paused_from_redis"):
                monkeypatch.setattr(runner, method, AsyncMock())
            for method in ("_hold_for_coder_cleanup", "_consume_approval_command",
                           "_consume_publication_migration_command"):
                monkeypatch.setattr(runner, method, AsyncMock(return_value=False))
            monkeypatch.setattr(runner, "_consume_retry_command", AsyncMock(return_value=RetryDispatch.NONE))
            monkeypatch.setattr(runner, "_check_github_api_budget", AsyncMock(return_value=True))
            monkeypatch.setattr(runner, "preflight", AsyncMock(return_value=True))
            monkeypatch.setattr(runner, "handle_idle", AsyncMock(side_effect=AssertionError("duplicate discovery")))
            asyncio.run(runner._run_cycle_body())
            assert runner.state.state == PipelineState.MERGE
            assert runner.state.current_task is task
            assert not writes and not outcomes
        reads_fail = False
        payload["base"] = {"ref": "main", "repo": {"full_name": "octo/demo"}}
        if case == "unknown":
            asyncio.run(runner._run_cycle_body())
        else:
            asyncio.run(runner.handle_merge())
        assert len([a for a, _ in calls if a[:2] == ["pr", "merge"]]) == 1
        assert runner.state.state == PipelineState.IDLE
    elif case in {"open", "head"}:
        assert runner.state.state == PipelineState.WATCH
        assert runner.state.current_pr.review_status == ReviewStatus.PENDING
        assert runner.state.current_task is task
        assert not writes and not outcomes
    elif case == "closed":
        assert runner.state.state == PipelineState.IDLE
        assert outcomes == ["closed_unmerged"] and not writes
        assert task.status == TaskStatus.DOING
    else:
        assert runner.state.state == PipelineState.IDLE
        assert writes[0][1] == "DONE"
        assert outcomes == ["success_merged"]
        assert runner.state.current_queue[0].status == TaskStatus.DONE


@pytest.mark.parametrize("failure", ["read", "missing", "wrong_task", "wrong_repo", "wrong_run"])
def test_confirmed_merge_retries_bound_record_before_releasing_ownership(monkeypatch, failure):
    from unittest.mock import AsyncMock

    runner = h._make_runner()
    task = QueueTask(pr_id="PR-401", title="Merge", branch="pr-401",
                     task_file="tasks/PR-401.md", status=TaskStatus.DONE)
    runner.state.current_task = task
    runner.state.current_pr = PRInfo(number=629, branch=task.branch, head_sha="a" * 40)
    runner.state.state = PipelineState.MERGE
    runner.state.merge_phase = "post_merge_cleanup"
    runner._start_current_run_record("claude", "opus")
    runner._capture_invocation_snapshot("claude", {"model": "opus"}, phase="coding")
    record = runner._current_run_record
    runner._current_run_record = None
    bad = None
    if failure.startswith("wrong_"):
        from dataclasses import replace

        bad = replace(record, **{{"wrong_task": "task_id", "wrong_repo": "repo_name",
                                  "wrong_run": "run_id"}[failure]: "other"})
    lookup = (AsyncMock(side_effect=RuntimeError("metrics unavailable"))
              if failure == "read" else AsyncMock(return_value=bad))
    monkeypatch.setattr(runner._metrics_store, "get", lookup)
    monkeypatch.setattr(runner._metrics_store, "recent", AsyncMock(side_effect=AssertionError("wrong run fallback")))
    save = AsyncMock()
    monkeypatch.setattr(runner._metrics_store, "save", save)
    monkeypatch.setattr(runner, "_commit_task_status_change", AsyncMock(return_value=True))
    monkeypatch.setattr(runner, "_compute_diff_stats", lambda base: {})
    monkeypatch.setattr(merge_module, "log_merged_pr", lambda record: None)
    asyncio.run(runner.handle_merge())
    assert runner.state.state == PipelineState.MERGE
    assert runner.state.current_task is task
    save.assert_not_awaited()
    lookup.assert_awaited_once_with(record.run_id)

    # Recovery retries the exact key even if the recency index was never saved.
    lookup.side_effect = None
    lookup.return_value = record
    asyncio.run(runner.handle_merge())
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert record.exit_reason == "success_merged"
    save.assert_awaited_once_with(record)
