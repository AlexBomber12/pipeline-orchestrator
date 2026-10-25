"""Tests for MERGE cleanup metadata."""

from __future__ import annotations

import asyncio
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from src.cancellation import retry_count_key, task_spec_hash_key
from src.daemon import git_ops as git_ops_module
from src.daemon import runner as runner_module
from src.daemon.handlers import merge as merge_module
from src.daemon.retry_commands import RetryDispatch
from src.models import PipelineState, PRInfo, QueueTask, ReviewStatus, TaskStatus

from tests.runner import _helpers as h


def test_conflict_commit_requires_finished_merge_of_both_heads(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()

    def git(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", *args], cwd=repo, check=check, text=True,
            capture_output=True,
        )

    git("init", "-q", "-b", "main")
    git("config", "user.email", "test@example.com")
    git("config", "user.name", "Test")
    (repo / "example.txt").write_text("base\n")
    git("add", "example.txt")
    git("commit", "-qm", "base")
    git("checkout", "-qb", "feature")
    (repo / "example.txt").write_text("feature\n")
    git("commit", "-qam", "feature")
    old_head = git("rev-parse", "HEAD").stdout.strip()
    git("checkout", "main")
    (repo / "example.txt").write_text("main\n")
    git("commit", "-qam", "main")
    git("update-ref", "refs/remotes/origin/main", "HEAD")
    git("checkout", "feature")
    assert git("merge", "origin/main", check=False).returncode != 0

    with pytest.raises(RuntimeError, match="merge in progress"):
        merge_module._validated_conflict_commit(str(repo), "feature", old_head, "main")

    (repo / "example.txt").write_text("feature and main\n")
    git("add", "example.txt")
    git("commit", "-qm", "resolve conflict")
    new_head = git("rev-parse", "HEAD").stdout.strip()
    assert merge_module._validated_conflict_commit(
        str(repo), "feature", old_head, "main"
    ) == new_head
    (repo / "example.txt").write_text("unpublished edit\n")
    with pytest.raises(RuntimeError, match="unpublished"):
        merge_module._validated_conflict_commit(str(repo), "feature", old_head, "main")


def test_bound_pr_checkout_rejects_mismatched_task_branch() -> None:
    runner = h._make_runner()
    task = QueueTask(pr_id="PR-404", title="conflict", branch="other",
                     status=TaskStatus.ERROR)
    pr = PRInfo(number=636, branch="feature")
    assert runner._bound_pr_has_unpublished_work(task, pr) is False


@pytest.mark.parametrize("uncertainty", ["merge_probe", "head_probe", "remote_head"])
def test_bound_pr_checkout_preserves_uncertain_git_evidence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, uncertainty: str,
) -> None:
    def git(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(["git", *args], cwd=tmp_path, check=True,
                              text=True, capture_output=True)

    git("init", "-q", "-b", "main")
    git("config", "user.email", "test@example.com")
    git("config", "user.name", "Test")
    (tmp_path / "example.txt").write_text("clean\n")
    git("add", "example.txt")
    git("commit", "-qm", "base")
    head = git("rev-parse", "HEAD").stdout.strip()
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    task = QueueTask(pr_id="PR-404", title="conflict", branch="main",
                     status=TaskStatus.DOING)
    pr = PRInfo(number=636, branch="main", pr_id=task.pr_id,
                head_sha="" if uncertainty == "remote_head" else head)
    original_git = git_ops_module._git

    def uncertain(repo_path: str, *args: str, **kwargs: object):
        if uncertainty == "merge_probe" and args == (
            "rev-parse", "-q", "--verify", "MERGE_HEAD",
        ):
            return subprocess.CompletedProcess(args, 2, "", "")
        if uncertainty == "head_probe" and args == ("rev-parse", "HEAD"):
            return subprocess.CompletedProcess(args, 0, "", "")
        return original_git(repo_path, *args, **kwargs)

    monkeypatch.setattr(git_ops_module, "_git", uncertain)
    assert runner._bound_pr_has_unpublished_work(task, pr) is True


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        ("branch", "checked-out branch"),
        ("unmerged", "unmerged paths"),
        ("unchanged", "new commit"),
        ("ancestor", "does not contain"),
    ],
)
def test_conflict_commit_rejects_unverified_git_states(
    monkeypatch: pytest.MonkeyPatch, failure: str, expected: str
) -> None:
    def fake_git(_repo: str, *args: str, **_kwargs: object):
        if args[:2] == ("branch", "--show-current"):
            return h._FakeCompletedProcess(stdout="wrong" if failure == "branch" else "feature")
        if args[:3] == ("rev-parse", "-q", "--verify"):
            return h._FakeCompletedProcess(returncode=1)
        if args[:2] == ("ls-files", "-u"):
            return h._FakeCompletedProcess(stdout="unmerged" if failure == "unmerged" else "")
        if args[:2] == ("rev-parse", "HEAD"):
            return h._FakeCompletedProcess(stdout="a" * 40 if failure == "unchanged" else "b" * 40)
        if args[:2] == ("merge-base", "--is-ancestor"):
            return h._FakeCompletedProcess(returncode=1 if failure == "ancestor" else 0)
        return h._FakeCompletedProcess()

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    with pytest.raises(RuntimeError, match=expected):
        merge_module._validated_conflict_commit("repo", "feature", "a" * 40, "main")


@pytest.mark.parametrize(
    ("coder_result", "expected"),
    [
        ((-1, "", "Timeout after 300s"), "exit -1"),
        ((0, "finished", ""), "resolution incomplete"),
        ((17, "", "Bearer synthetic-credential-canary " + "x" * 600_000), "classification nonzero_exit"),
    ],
)
def test_merge_timeout_preserves_failure_and_does_not_rediagnose(
    monkeypatch: pytest.MonkeyPatch,
    coder_result: tuple[int, str, str],
    expected: str,
) -> None:
    git_calls: list[tuple[str, ...]] = []

    def fake_git(_repo: str, *args: str, **_kwargs: object):
        git_calls.append(args)
        if args[:2] == ("merge", "origin/main"):
            return h._FakeCompletedProcess(
                returncode=1, stdout="CONFLICT (content): example.txt"
            )
        if args[:2] == ("rev-parse", "HEAD"):
            return h._FakeCompletedProcess(stdout="a" * 40)
        return h._FakeCompletedProcess()

    runner = h._make_runner(coder="codex")
    plugin = runner._registry.get("codex")
    invoked: list[str] = []

    async def timed_out(prompt: str, *_args: object, **_kwargs: object):
        invoked.append(prompt)
        return coder_result

    async def allow(*_args: object, **_kwargs: object) -> bool:
        return True

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(runner, "_refresh_auth_status_cache", allow)
    monkeypatch.setattr(runner, "usage_gate", allow)
    monkeypatch.setattr(plugin, "run_prompt", timed_out)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=636, branch="pr-404", head_sha="a" * 40)
    runner.state.current_task = QueueTask(
        pr_id="PR-404", title="conflict", status=TaskStatus.DOING
    )
    runner._error_diagnose_count = 2

    asyncio.run(runner.handle_merge())
    assert invoked and "Do not push, run CI" in invoked[0]
    assert runner.state.coder == "codex"
    assert runner.state.state == PipelineState.ERROR
    assert expected in (runner.state.error_message or "")
    assert "synthetic-credential-canary" not in (runner.state.error_message or "")
    assert "synthetic-credential-canary" not in str(runner.state.history)
    assert len(runner.state.model_dump_json().encode()) < 20_000
    assert not any(args[:2] == ("merge", "--abort") for args in git_calls)
    failure = runner.state.error_message
    for _ in range(2):
        asyncio.run(runner.handle_error())
        assert runner.state.state == PipelineState.ERROR
        assert runner.state.error_message == failure
        assert runner._error_diagnose_count == 2
    assert len(invoked) == 1
    assert not any(args[:1] == ("push",) for args in git_calls)


def test_resolver_failure_diagnostic_uses_only_recognized_fields() -> None:
    message = merge_module._resolver_failure_message(
        "invalid credential-like coder name!", 10**100,
        "Bearer synthetic-credential-canary Timeout after 300s", "",
        rate_limited=False,
    )
    assert "coder configured" in message
    assert "exit unknown" in message
    assert "classification timeout" in message
    assert "Timeout after 300s" in message
    assert "synthetic-credential-canary" not in message
    assert len(message) < 200
    no_output = merge_module._resolver_failure_message(
        "codex", -1, "", "", rate_limited=False,
    )
    assert "classification timeout" in no_output


def test_operator_stopped_merge_retains_failure_until_play(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_git(_repo: str, *args: str, **_kwargs: object):
        if args[:2] == ("merge", "origin/main"):
            return h._FakeCompletedProcess(returncode=1, stdout="CONFLICT")
        if args[:2] == ("rev-parse", "HEAD"):
            return h._FakeCompletedProcess(stdout="a" * 40)
        return h._FakeCompletedProcess()

    runner = h._make_runner(coder="codex")
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=636, branch="pr-404", head_sha="a" * 40)
    runner.state.current_task = QueueTask(
        pr_id="PR-404", title="conflict", status=TaskStatus.DOING,
    )

    async def allow(*_args: object, **_kwargs: object) -> bool:
        return True

    async def stopped(invocation: object, **_kwargs: object) -> None:
        invocation.close()  # type: ignore[attr-defined]
        runner._stop_requested = True
        runner.state.user_paused = True
        runner.state.state = PipelineState.PAUSED

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(runner, "_refresh_auth_status_cache", allow)
    monkeypatch.setattr(runner, "usage_gate", allow)
    monkeypatch.setattr(runner, "_await_auxiliary_coder", stopped)

    asyncio.run(runner.handle_merge())
    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.error_message.startswith("Merge conflict resolution interrupted")
    runner.state.user_paused = False
    asyncio.run(runner.handle_paused())
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message.startswith("Merge conflict resolution interrupted")


def test_merge_failure_park_survives_non_ai_error_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = (
        "Merge conflict resolution failed (exit -1): Timeout after 300s; "
        "operator Retry required"
    )
    runner.state.current_task = QueueTask(
        pr_id="PR-404", title="conflict", status=TaskStatus.ERROR
    )

    async def no_obsolete() -> None:
        return None

    async def no_suppression() -> tuple[None, bool]:
        return None, False

    monkeypatch.setattr(runner, "_maybe_resume_obsolete_ci_error", no_obsolete)
    monkeypatch.setattr(runner, "_error_suppression_reason", no_suppression)
    monkeypatch.setattr(
        runner, "_remote_frontmatter_error_status_for_current_task",
        lambda: ("error", None, "revision"),
    )
    runner.app_config.daemon.error_handler_use_ai = False
    asyncio.run(runner._handle_single_error_exit())
    assert runner.state.state == PipelineState.ERROR
    assert "Timeout after 300s" in (runner.state.error_message or "")


def test_merge_error_repairs_failed_status_write_before_parking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = (
        "Merge conflict resolution failed (exit -1): Timeout after 300s; "
        "operator Retry required"
    )
    runner.state.current_task = QueueTask(
        pr_id="PR-404", title="conflict", status=TaskStatus.DOING,
        task_file="tasks/PR-404.md",
    )
    runner._status_write_failed_task_pr_ids.add("PR-404")
    runner._error_diagnose_count = 2
    write = AsyncMock(side_effect=[False, True])
    monkeypatch.setattr(runner, "_commit_task_status_change", write)

    asyncio.run(runner.handle_error())
    assert runner._status_write_failed_task_pr_ids == {"PR-404"}
    assert runner.state.state == PipelineState.ERROR
    asyncio.run(runner.handle_error())
    assert write.await_count == 2
    assert runner._status_write_failed_task_pr_ids == set()
    assert runner.state.error_message.startswith("Merge conflict resolution failed")
    assert runner._error_diagnose_count == 2


@pytest.mark.parametrize("remote_status", [None, "todo"])
def test_non_ai_merge_error_repairs_failed_status_before_remote_read(
    monkeypatch: pytest.MonkeyPatch, remote_status: str | None,
) -> None:
    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "Merge conflict resolution incomplete: unmerged paths; operator Retry required"
    runner.state.current_task = QueueTask(
        pr_id="PR-404", title="conflict", status=TaskStatus.DOING,
        task_file="tasks/PR-404.md",
    )
    runner._status_write_failed_task_pr_ids.add("PR-404")
    runner.app_config.daemon.error_handler_use_ai = False

    async def no_obsolete() -> None:
        return None

    async def no_suppression() -> tuple[None, bool]:
        return None, False

    monkeypatch.setattr(runner, "_maybe_resume_obsolete_ci_error", no_obsolete)
    monkeypatch.setattr(runner, "_error_suppression_reason", no_suppression)
    monkeypatch.setattr(
        runner, "_remote_frontmatter_error_status_for_current_task",
        lambda: (remote_status, None, None),
    )
    write = AsyncMock(return_value=True)
    monkeypatch.setattr(runner, "_commit_task_status_change", write)

    asyncio.run(runner._handle_single_error_exit())

    write.assert_awaited_once()
    assert runner._status_write_failed_task_pr_ids == set()
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message.startswith("Merge conflict resolution incomplete")


@pytest.mark.parametrize(
    "scenario", ["timeout", "rate_limit", "operator_stop", "fetch_timeout",
                 "fetch_lost_message"],
)
def test_dirty_merge_failure_survives_repeated_full_daemon_cycles(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, scenario: str,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()

    def git(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", *args], cwd=repo, check=check, text=True,
            capture_output=True,
        )

    git("init", "-q", "-b", "main")
    git("config", "user.email", "test@example.com")
    git("config", "user.name", "Test")
    (repo / "example.txt").write_text("base\n")
    git("add", "example.txt")
    git("commit", "-qm", "base")
    git("checkout", "-qb", "feature")
    (repo / "example.txt").write_text("feature\n")
    git("commit", "-qam", "feature")
    feature_head = git("rev-parse", "HEAD").stdout.strip()
    git("checkout", "main")
    (repo / "example.txt").write_text("main\n")
    git("commit", "-qam", "main")
    git("update-ref", "refs/remotes/origin/main", "HEAD")
    git("checkout", "feature")
    assert git("merge", "origin/main", check=False).returncode != 0
    unpublished = (repo / "example.txt").read_text()

    runner = h._make_runner()
    runner.repo_path = str(repo)
    runner._recovered = True
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = (
        "Merge conflict resolution interrupted by operator Stop; "
        "unpublished work retained; operator Retry required"
        if scenario == "operator_stop" else
        "Prior failure detail unavailable"
        if scenario == "fetch_lost_message" else
        "Merge conflict resolution failed (exit -1): "
        + ("Error: 429 Too Many Requests" if scenario == "rate_limit"
           else "Timeout after 300s")
        + "; operator Retry required"
    )
    if scenario in {"rate_limit", "operator_stop"}:
        runner.state.state = PipelineState.PAUSED
        if scenario == "rate_limit":
            runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(hours=1)
            runner.state.rate_limit_reactive_coder = "claude"
        else:
            runner.state.user_paused = True
    runner.state.current_pr = PRInfo(
        number=636, branch="feature", head_sha=feature_head
    )
    runner.state.current_task = QueueTask(
        pr_id="PR-404", title="conflict", status=TaskStatus.DOING,
        branch="feature",
    )
    runner._error_diagnose_count = 2
    runner._consecutive_dirty_cycles = 2
    for method in (
        "_hold_for_coder_cleanup", "_consume_approval_command",
        "_consume_publication_migration_command", "_maybe_resume_review_timeout",
    ):
        monkeypatch.setattr(runner, method, AsyncMock(return_value=False))
    monkeypatch.setattr(runner, "_consume_retry_command", AsyncMock(return_value=RetryDispatch.NONE))
    if scenario in {"fetch_timeout", "fetch_lost_message"}:
        calls = 0

        async def fetch_once_fails() -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("git fetch timed out")

        monkeypatch.setattr(runner, "ensure_repo_cloned", fetch_once_fails)
    else:
        monkeypatch.setattr(runner, "ensure_repo_cloned", AsyncMock())
    monkeypatch.setattr(runner, "_refresh_user_paused_from_redis", AsyncMock())
    monkeypatch.setattr(runner, "_check_github_api_budget", AsyncMock(return_value=True))
    monkeypatch.setattr(runner, "_maybe_resume_obsolete_ci_error", AsyncMock(return_value=None))
    monkeypatch.setattr(runner, "_refresh_auth_status_cache", AsyncMock())

    for cycle in range(4):
        asyncio.run(runner._run_cycle_body())
        expected_state = (
            PipelineState.PAUSED if scenario in {"rate_limit", "operator_stop"}
            else PipelineState.ERROR
        )
        assert runner.state.state == expected_state
        assert {"timeout": "Timeout after 300s", "rate_limit": "429",
                "operator_stop": "operator Stop", "fetch_timeout": "Timeout after 300s",
                "fetch_lost_message": "Published task failure unresolved"}[scenario] in (
            runner.state.error_message or ""
        )
        if scenario in {"fetch_timeout", "fetch_lost_message"}:
            assert any("git fetch timed out" in event["event"]
                       for event in runner.state.history)
        assert runner._error_diagnose_count == 2
        assert runner._consecutive_dirty_cycles == (
            2 if scenario == "operator_stop"
            or (scenario in {"fetch_timeout", "fetch_lost_message"}
                and cycle == 0) else 0
        )
        assert git("rev-parse", "HEAD").stdout.strip() == feature_head
        assert (repo / "example.txt").read_text() == unpublished
        assert git("ls-files", "-u").stdout
    if scenario in {"rate_limit", "operator_stop"}:
        if scenario == "rate_limit":
            runner.state.rate_limited_until = datetime.now(timezone.utc) - timedelta(seconds=1)
        else:
            runner.state.user_paused = False
        asyncio.run(runner.handle_paused())
        assert runner.state.state == PipelineState.ERROR
        assert ("429" if scenario == "rate_limit" else "operator Stop") in (
            runner.state.error_message or ""
        )
        asyncio.run(runner._run_cycle_body())
        assert runner.state.state == PipelineState.ERROR
        assert runner._consecutive_dirty_cycles == 0
        assert (repo / "example.txt").read_text() == unpublished

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
    rev_parse_calls = {"n": 0}

    def fake_git(repo_path: str, *args: str, **kwargs: object):
        if args[:1] == ("merge",) and len(args) > 1 and str(args[1]).startswith("origin/"):
            return h._FakeCompletedProcess(
                stdout="Merge made by the 'ort' strategy.\n",
                returncode=0,
            )
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["n"] += 1
            return h._FakeCompletedProcess(
                stdout=("a" if rev_parse_calls["n"] == 1 else "b") * 40,
                returncode=0,
            )
        if args[:2] == ("ls-remote", "origin"):
            return h._FakeCompletedProcess(stdout=f"{'b' * 40}\trefs/heads/pr-007\n")
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


def test_pre_merge_sync_refuses_progress_when_rev_parse_fails(
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

    assert runner.state.state == PipelineState.ERROR
    assert not any(
        "Bypass-requesting fresh @codex review" in entry.get("event", "")
        for entry in runner.state.history
    )


def test_pre_merge_sync_rejects_remote_head_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rev_parse_calls = 0
    pushed: list[bool] = []

    def fake_git(_repo: str, *args: str, **_kwargs: object):
        nonlocal rev_parse_calls
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls += 1
            return h._FakeCompletedProcess(
                stdout=("a" if rev_parse_calls == 1 else "b") * 40
            )
        if args[:2] == ("merge", "origin/main"):
            return h._FakeCompletedProcess(stdout="Merge made by the 'ort' strategy")
        if args[:2] == ("push", "origin"):
            pushed.append(True)
        if args[:2] == ("ls-remote", "origin"):
            return h._FakeCompletedProcess(stdout=f"{'a' * 40}\trefs/heads/pr-007\n")
        return h._FakeCompletedProcess()

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(merge_module, "retry_transient", lambda op, **_: op())
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=7, branch="pr-007")
    runner.state.state = PipelineState.WATCH

    asyncio.run(runner.handle_merge())

    assert pushed
    assert runner.state.state == PipelineState.ERROR
    assert "remote HEAD does not match" in (runner.state.error_message or "")
    assert not any(
        "Bypass-requesting fresh @codex review" in entry.get("event", "")
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
        "merged_at": "2020-01-31T23:59:00Z",
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
        if case == "status_error" and len(writes) == 1:
            raise RuntimeError("status push failed")
        return case not in {"status_false", "open"} or len(writes) > 1

    async def save(reason, **kwargs):
        if case == "metrics_error" and not outcomes:
            outcomes.append("failed save")
            raise RuntimeError("metrics unavailable")
        outcomes.append(reason)

    monkeypatch.setattr(merge_module.gh_runner, "run_gh", transport)
    monkeypatch.setattr(runner, "_commit_task_status_change", status)
    monkeypatch.setattr(runner, "_save_current_run_record", save)
    analytics = []
    monkeypatch.setattr(merge_module, "log_merged_pr", analytics.append)
    asyncio.run(runner.handle_merge())
    merge_calls = [args for args, _ in calls if args[:2] == ["pr", "merge"]]
    assert merge_calls == [["pr", "merge", "5", "--squash", "--delete-branch",
                            "--match-head-commit", "a" * 40]]
    assert calls[1][1]["repo"] == "octo/demo"
    if case in {"unknown", "open", "base", "repo", "metrics_error", "status_error", "status_false"}:
        assert runner.state.state == PipelineState.MERGE
        assert runner.state.current_task is task
        if case in {"status_error", "status_false"}:
            assert len(writes) == 1 and not outcomes
            assert runner.state.merge_phase == "post_merge_cleanup"
            assert task.status == TaskStatus.DOING
            assert runner._current_run_record is not None
        elif case != "metrics_error":
            assert not writes and not outcomes
            assert task.status == TaskStatus.DOING
        # Reconciliation cannot issue another merge or touch the deleted branch.
        monkeypatch.setattr(git_ops_module, "_git", lambda *a, **kw: pytest.fail("git during reconciliation"))
        monkeypatch.setattr(runner, "_compute_diff_stats", lambda base, head_ref="HEAD": {"files_touched_count": 0})
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
        if case == "open":
            # Queue admission is not terminal proof. Repeated OPEN reads retain
            # the attempt without another merge command or a DONE write.
            for _ in range(2):
                asyncio.run(runner.handle_merge())
                assert runner.state.state == PipelineState.MERGE
                assert runner.state.merge_phase == "merging"
                assert runner.state.current_task is task
                assert not writes and not outcomes
            payload.update(state="closed", merged=True)
            asyncio.run(runner.handle_merge())
            assert runner.state.merge_phase == "post_merge_cleanup"
            assert runner.state.current_task is task
            assert len(writes) == 1 and not outcomes
        reads_fail = False
        payload["base"] = {"ref": "main", "repo": {"full_name": "octo/demo"}}
        if case == "unknown":
            asyncio.run(runner._run_cycle_body())
        else:
            asyncio.run(runner.handle_merge())
        assert len([a for a, _ in calls if a[:2] == ["pr", "merge"]]) == 1
        assert runner.state.state == PipelineState.IDLE
        assert outcomes[-1] == "success_merged"
        assert runner.state.current_queue[0].status == TaskStatus.DONE
        if case in {"status_error", "status_false", "open"}:
            assert len(writes) == 2
    elif case == "head":
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
        assert analytics[0]["merged_at"] == "2020-01-31T23:59:00+00:00"
        assert runner.state.current_queue[0].status == TaskStatus.DONE


@pytest.mark.parametrize("failure", ["read", "wrong_task", "wrong_repo", "wrong_run", "checkpoint"])
def test_confirmed_merge_retries_bound_record_before_releasing_ownership(monkeypatch, failure):
    from unittest.mock import AsyncMock

    runner = h._make_runner()
    task = QueueTask(pr_id="PR-401", title="Merge", branch="pr-401",
                     task_file="tasks/PR-401.md", status=TaskStatus.DOING)
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
              if failure == "read" else AsyncMock(return_value=record if failure == "checkpoint" else bad))
    monkeypatch.setattr(runner._metrics_store, "get", lookup)
    monkeypatch.setattr(runner._metrics_store, "recent", AsyncMock(side_effect=AssertionError("wrong run fallback")))
    save = AsyncMock(side_effect=RuntimeError("checkpoint unavailable") if failure == "checkpoint" else None)
    monkeypatch.setattr(runner._metrics_store, "save", save)
    status = AsyncMock(return_value=True)
    monkeypatch.setattr(runner, "_commit_task_status_change", status)
    monkeypatch.setattr(runner, "_compute_diff_stats", lambda base, head_ref="HEAD": {"files_touched_count": 0})
    monkeypatch.setattr(merge_module, "log_merged_pr", lambda record: None)
    asyncio.run(runner.handle_merge())
    assert runner.state.state == PipelineState.MERGE
    assert runner.state.current_task is task
    status.assert_not_awaited()
    assert task.status == TaskStatus.DOING
    if failure == "checkpoint":
        save.assert_awaited_once_with(record)
    else:
        save.assert_not_awaited()
    lookup.assert_awaited_once_with(record.run_id)

    # Recovery retries the exact key even if the recency index was never saved.
    lookup.side_effect = None
    lookup.return_value = record
    save.side_effect = None
    save.reset_mock()
    asyncio.run(runner.handle_merge())
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert record.exit_reason == "success_merged"
    assert save.await_count == 2  # Durable checkpoint, then final success.
    status.assert_awaited_once()


@pytest.mark.parametrize("bound", [False, True])
@pytest.mark.parametrize("unrelated_recent", [False, True])
def test_confirmed_merge_reconstructs_missing_accounting_before_done(monkeypatch, bound, unrelated_recent):
    from unittest.mock import AsyncMock

    runner = h._make_runner()
    task = QueueTask(pr_id="PR-401", title="Merge", branch="pr-401",
                     task_file="tasks/PR-401.md", status=TaskStatus.DOING)
    runner.state.current_task = task
    runner.state.current_pr = PRInfo(number=629, branch=task.branch, head_sha="a" * 40)
    runner.state.state = PipelineState.MERGE
    runner.state.merge_phase = "post_merge_cleanup"
    if bound:
        runner._start_current_run_record("claude", "opus")
        runner._capture_invocation_snapshot("claude", {"model": "opus"}, phase="coding")
    invocation = runner.state.active_invocation
    runner._current_run_record = None
    monkeypatch.setattr(runner._metrics_store, "get", AsyncMock(return_value=None))
    if unrelated_recent:
        from dataclasses import replace

        runner._start_current_run_record("claude", "opus")
        unrelated = replace(runner._current_run_record, task_id="PR-999")
        runner._current_run_record = None
    recent = AsyncMock(return_value=[unrelated] if unrelated_recent else [])
    task_runs = AsyncMock(return_value=[])
    monkeypatch.setattr(runner._metrics_store, "recent", recent)
    monkeypatch.setattr(runner._metrics_store, "list_task_runs", task_runs)
    save = AsyncMock(side_effect=RuntimeError("checkpoint unavailable"))
    monkeypatch.setattr(runner._metrics_store, "save", save)
    status = AsyncMock(return_value=True)
    monkeypatch.setattr(runner, "_commit_task_status_change", status)
    monkeypatch.setattr(runner, "_compute_diff_stats", lambda base, head_ref="HEAD": {"files_touched_count": 0})
    monkeypatch.setattr(merge_module, "log_merged_pr", lambda record: None)

    asyncio.run(runner.handle_merge())
    record = runner._current_run_record
    assert record.repo_name == runner.name and record.task_id == task.pr_id
    assert record.profile_id == "recovery:unknown:container"
    assert record.tokens_in == record.tokens_out == 0
    if bound:
        assert record.run_id == invocation.run_id
        assert record.invocations == [invocation]
    else:
        assert record.run_id and not record.invocations
    assert runner.state.state == PipelineState.MERGE
    assert runner.state.current_task is task
    assert task.status == TaskStatus.DOING
    status.assert_not_awaited()
    save.assert_awaited_once_with(record)
    if bound:
        recent.assert_not_awaited()
        task_runs.assert_not_awaited()
    else:
        task_runs.assert_awaited_once_with(runner.name, task.pr_id)

    save.side_effect = None
    save.reset_mock()
    asyncio.run(runner.handle_merge())
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert record.exit_reason == "success_merged"
    status.assert_awaited_once()
    assert save.await_count == 2


@pytest.mark.parametrize("phase", ["merging", "post_merge_cleanup"])
@pytest.mark.parametrize("checkout", ["retained", "fresh", "fetch_failure", "wrong_head"])
def test_recovered_merge_stats_use_retained_head_from_base_checkout(
    monkeypatch, tmp_path, phase, checkout,
):
    import subprocess
    from unittest.mock import AsyncMock

    repo_path = tmp_path

    def git(*args):
        return subprocess.run(
            ["git", *args], cwd=repo_path, check=True, capture_output=True, text=True,
        ).stdout.strip()

    git("init", "-b", "main")
    git("config", "user.email", "test@example.com")
    git("config", "user.name", "Test")
    (tmp_path / "base.txt").write_text("base\n")
    git("add", ".")
    git("commit", "-m", "base")
    git("checkout", "-b", "pr-401")
    (tmp_path / "feature.py").write_text("first\nsecond\n")
    git("add", ".")
    git("commit", "-m", "feature")
    head = git("rev-parse", "HEAD")
    # Match gh's squash-and-delete behavior: the checkout returns to base,
    # whose history contains the change but does not contain the PR head.
    git("checkout", "main")
    git("merge", "--squash", "pr-401")
    git("commit", "-m", "squashed feature")
    git("branch", "-D", "pr-401")
    git("update-ref", "refs/remotes/origin/main", "HEAD")
    git("update-ref", "refs/pull/629/head", head)
    if checkout != "retained":
        source = repo_path
        git("clone", "--single-branch", "--no-local", str(source), str(tmp_path / "fresh"))
        repo_path = tmp_path / "fresh"
        missing = subprocess.run(
            ["git", "cat-file", "-e", head], cwd=repo_path, capture_output=True,
        )
        assert missing.returncode != 0
        if checkout == "fetch_failure":
            git("remote", "set-url", "origin", str(tmp_path / "missing"))
        elif checkout == "wrong_head":
            # Even a successful fetch must not substitute a different head.
            subprocess.run(
                ["git", "update-ref", "refs/pull/629/head", "main"],
                cwd=source, check=True, capture_output=True,
            )

    runner = h._make_runner()
    runner.repo_path = str(repo_path)
    task = QueueTask(pr_id="PR-401", title="Merge", status=TaskStatus.DOING,
                     task_file="tasks/PR-401.md")
    runner.state.current_task = task
    runner.state.current_pr = PRInfo(number=629, branch="pr-401", head_sha=head)
    runner.state.state = PipelineState.MERGE
    runner.state.merge_phase = phase
    runner._start_current_run_record("claude", "opus")
    record = runner._current_run_record
    write = AsyncMock(return_value=True)
    monkeypatch.setattr(runner, "_commit_task_status_change", write)
    monkeypatch.setattr(merge_module, "log_merged_pr", lambda record: None)
    real_git = git_ops_module._git
    fetches = []

    def fetch_only(path, *args, **kwargs):
        assert args == ("fetch", "origin", "refs/pull/629/head")
        fetches.append(args)
        return real_git(path, *args, **kwargs)

    monkeypatch.setattr(git_ops_module, "_git", fetch_only)
    monkeypatch.setattr(merge_module.gh_prs, "merge_pr", lambda *a: pytest.fail("duplicate merge"))

    assert runner._compute_diff_stats("main")["files_touched_count"] == 0
    asyncio.run(runner.handle_merge())

    if checkout in {"fetch_failure", "wrong_head"}:
        assert runner.state.state == PipelineState.MERGE
        assert runner.state.current_task is task
        assert runner._current_run_record is record
        assert task.status == TaskStatus.DOING
        write.assert_not_awaited()
        assert record.exit_reason != "success_merged"
        git("remote", "set-url", "origin", str(source))
        subprocess.run(
            ["git", "update-ref", "refs/pull/629/head", head],
            cwd=source, check=True, capture_output=True,
        )
        asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.IDLE
    assert len(fetches) == {"retained": 0, "fresh": 1, "fetch_failure": 2, "wrong_head": 2}[checkout]
    assert record.exit_reason == "success_merged"
    assert record.head_sha == head
    saved = asyncio.run(runner._metrics_store.get(record.run_id))
    assert saved.head_sha == head
    assert record.files_touched_count == 1
    assert record.diff_lines_added == 2
    assert record.diff_lines_deleted == 0
    assert record.languages_touched == ["python"]


def test_restart_after_outcome_append_logs_once(monkeypatch, isolate_analytics_dir):
    """A process exit after the append leaves a replayable cleanup snapshot."""
    import json
    from unittest.mock import AsyncMock

    from src.analytics import log_merged_pr
    from src.keyspace import pipeline_state
    from src.metrics import MetricsStore
    from src.models import RepoState

    h._patch_subprocess(monkeypatch)
    original = h._make_runner()
    original.state.current_task = QueueTask(
        pr_id="PR-401", title="Merge", branch="pr-401", status=TaskStatus.DOING,
    )
    original.state.current_pr = PRInfo(
        number=629, pr_id="PR-401", branch="pr-401", head_sha="a" * 40,
    )
    original.state.state = PipelineState.MERGE
    original._start_current_run_record("claude", "opus")
    run_id = original._current_run_record.run_id

    def append_then_exit(record):
        log_merged_pr(record)
        raise SystemExit("crash before clearing task")

    monkeypatch.setattr(merge_module, "log_merged_pr", append_then_exit)
    with pytest.raises(SystemExit, match="crash before clearing task"):
        asyncio.run(original._finish_confirmed_merge({"files_touched_count": 1}))

    runner = h._make_runner()
    runner.redis = original.redis
    runner._metrics_store = MetricsStore(runner.redis)
    runner.state = RepoState.model_validate_json(
        runner.redis.store[pipeline_state(runner.name)],
    )
    assert runner.state.merge_phase == "post_merge_cleanup"
    assert runner.state.current_task.pr_id == "PR-401"
    assert runner._current_run_record is None
    monkeypatch.setattr(merge_module, "log_merged_pr", log_merged_pr)
    monkeypatch.setattr(runner, "_confirm_remote_merge", AsyncMock(return_value=True))
    monkeypatch.setattr(runner, "_compute_diff_stats", lambda *a: {"files_touched_count": 1})
    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    saved = asyncio.run(runner._metrics_store.get(run_id))
    assert saved.exit_reason == "success_merged"
    assert saved.head_sha == "a" * 40
    rows = [json.loads(line) for path in isolate_analytics_dir.glob("*.jsonl")
            for line in path.read_text().splitlines()]
    assert len(rows) == 1
    assert rows[0]["pr_id"] == "PR-401"


@pytest.mark.parametrize("redis_reset", [False, True])
def test_done_header_retains_recoverable_run_without_pipeline_snapshot(
    monkeypatch, isolate_analytics_dir, redis_reset,
):
    """DONE plus GitHub must recover even after all Redis state is lost."""
    from unittest.mock import AsyncMock

    from src.metrics import MetricsStore
    from src.models import RepoState

    original = h._make_runner()
    task = QueueTask(pr_id="PR-401", title="Merge", branch="pr-401",
                     task_file="tasks/PR-401.md", status=TaskStatus.DOING)
    original.state.current_task = task
    original.state.current_pr = PRInfo(number=629, pr_id=task.pr_id,
                                       branch=task.branch, head_sha="a" * 40)
    original.state.state = PipelineState.MERGE
    original._start_current_run_record("claude", "opus")
    run_id = original._current_run_record.run_id
    durable_save = original._metrics_store.save

    async def fail_success(record):
        if record.exit_reason == "success_merged":
            raise RuntimeError("success save unavailable")
        await durable_save(record)

    async def write_done(*args):
        records = await original._metrics_store.list_task_runs(original.name, task.pr_id)
        assert len(records) == 1 and records[0].run_id == run_id
        assert records[0].exit_reason != "success_merged"
        task.status = TaskStatus.DONE
        return True

    monkeypatch.setattr(original._metrics_store, "save", fail_success)
    monkeypatch.setattr(original, "_commit_task_status_change", write_done)
    asyncio.run(original._finish_confirmed_merge({"files_touched_count": 1}))
    assert task.status == TaskStatus.DONE
    assert original.state.current_task is task

    # Discard the entire pipeline snapshot and in-memory record. Only the
    # terminal header, indexed checkpoint and remote merged identity survive.
    runner = h._make_runner()
    if not redis_reset:
        runner.redis = original.redis
    runner._metrics_store = MetricsStore(runner.redis)
    runner.state = RepoState(url=original.state.url, name=original.state.name)
    runner._recovery_task_headers = {task.pr_id: SimpleNamespace(frontmatter_status="done")}
    monkeypatch.setattr(merge_module.gh_prs, "get_merged_prs_discovery", lambda *a:
                        merge_module.gh_prs.PRDiscovery(
                            runner.owner_repo, "main", [original.state.current_pr], True))
    assert asyncio.run(runner._recover_header_merge_attempt([task], []))
    recovered = runner._persisted_merge_recovery
    assert recovered is not None
    runner.state = recovered
    monkeypatch.setattr(runner, "_compute_diff_stats", lambda *a: {"files_touched_count": 1})
    monkeypatch.setattr(runner, "_commit_task_status_change", AsyncMock(return_value=True))
    from src.analytics import log_merged_pr

    monkeypatch.setattr(merge_module, "log_merged_pr", log_merged_pr)
    asyncio.run(runner.handle_merge())
    assert runner.state.state == PipelineState.IDLE
    if redis_reset:
        records = asyncio.run(runner._metrics_store.list_task_runs(runner.name, task.pr_id))
        assert len(records) == 1
        assert records[0].run_id != run_id
        run_id = records[0].run_id
    saved = asyncio.run(runner._metrics_store.get(run_id))
    assert saved.exit_reason == "success_merged"
    assert saved.files_touched_count == 1
    assert len(list(isolate_analytics_dir.glob("*.jsonl"))) == 1
    # A second Redis loss must recognize the durable completion as historical.
    runner._persisted_merge_recovery = None
    runner._metrics_store = MetricsStore(h._make_runner().redis)
    assert asyncio.run(runner._recover_header_merge_attempt([task], []))
    assert runner._persisted_merge_recovery is None

@pytest.mark.parametrize("admission", [False, True, None])
def test_failed_merge_command_retries_only_without_admission(monkeypatch, admission):
    from unittest.mock import AsyncMock, Mock

    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(merge_module.MergeMixin, "_confirm_remote_merge", _CONFIRM_REMOTE_MERGE)
    runner = h._make_runner()
    runner.state.state = PipelineState.MERGE
    runner.state.current_pr = PRInfo(number=5, branch="feature", head_sha="a" * 40,
                                     is_cross_repository=True)
    command = Mock(side_effect=RuntimeError("authentication failed"))
    monkeypatch.setattr(merge_module.gh_prs, "merge_pr", command)
    monkeypatch.setattr(merge_module.gh_prs, "get_exact_pr_outcome", lambda *a: merge_module.gh_prs.ExactPROutcome(
        runner.owner_repo, "main", 5, "a" * 40, merge_module.gh_prs.ExactPRState.OPEN, True,
    ))
    probe = Mock(return_value=admission)
    monkeypatch.setattr(merge_module.gh_prs, "get_pr_merge_admission", probe)
    monkeypatch.setattr(runner, "publish_state", AsyncMock())
    asyncio.run(runner.handle_merge())
    assert command.call_count == 1
    # Round-trip persisted failure evidence before the next cycle.
    runner.state = type(runner.state).model_validate_json(runner.state.model_dump_json())
    assert runner.state.merge_command_failed is (admission is not False)
    assert runner.state.state == (PipelineState.WATCH if admission is False else PipelineState.MERGE)
    asyncio.run(runner.handle_merge())
    assert command.call_count == (2 if admission is False else 1)
    assert probe.call_count == 2
    if admission is None:
        probe.return_value = False
        asyncio.run(runner.handle_merge())
        assert runner.state.state == PipelineState.WATCH
        assert command.call_count == 1
