"""Tests for MERGE cleanup metadata."""

from __future__ import annotations

import asyncio

import pytest
from src.cancellation import retry_count_key, task_spec_hash_key
from src.daemon import git_ops as git_ops_module
from src.daemon import runner as runner_module
from src.daemon.handlers import merge as merge_module
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus

from tests.runner import _helpers as h


def test_merge_cleans_up_hash_and_retry_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr("src.github.prs.merge_pr", lambda repo, num: None)
    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_mark_task_done_in_snapshot",
        lambda self: None,
    )

    runner = h._make_runner()
    runner.redis.store[task_spec_hash_key("octo__demo", "PR-001")] = "abc123"
    runner.redis.store[retry_count_key("octo__demo", "PR-001")] = "2"
    runner.state.state = PipelineState.MERGE
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="Sample",
        status=TaskStatus.DOING,
    )

    asyncio.run(runner.handle_merge())

    assert task_spec_hash_key("octo__demo", "PR-001") not in runner.redis.store
    assert retry_count_key("octo__demo", "PR-001") not in runner.redis.store


def test_merge_logs_cleanup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr("src.github.prs.merge_pr", lambda repo, num: None)
    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_mark_task_done_in_snapshot",
        lambda self: None,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.MERGE
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
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

    asyncio.run(runner.handle_merge())

    assert any("Failed to clear retry metadata for PR-001" in event for event in events)


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
