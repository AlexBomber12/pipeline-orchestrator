"""Tests for MERGE cleanup metadata."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from src.cancellation import retry_count_key, task_spec_hash_key
from src.daemon import runner as runner_module
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
    monkeypatch.setattr(runner, "log_event", events.append)

    async def fail_delete(key: str) -> int:
        raise RuntimeError("redis down")

    runner.redis.delete = fail_delete

    asyncio.run(runner.handle_merge())

    assert any("Failed to clear retry metadata for PR-001" in event for event in events)


def test_pre_merge_sync_bypasses_author_dedup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pre-merge sync just rewrote HEAD locally; the GitHub REST API
    can return a stale ``head_commit_date`` for several seconds after
    the push, which would otherwise let an operator's recent
    ``@codex review`` (anchored on the prior head) suppress the fresh
    trigger for the new merge commit. ``handle_merge`` must bypass the
    PR-author dedup at this site so Codex actually reviews the merged
    code instead of falling through to ESCALATE on a stale anchor."""
    def fake_git(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:2] == ["git", "merge"] and "origin/main" in cmd:
            return h._FakeCompletedProcess(
                args=cmd,
                stdout="Merge made by the 'ort' strategy.\n",
                returncode=0,
            )
        if cmd[:3] == ["git", "rev-parse", "HEAD"]:
            return h._FakeCompletedProcess(
                args=cmd,
                stdout="1dbcff4abcdef0\n",
                returncode=0,
            )
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_git)

    monkeypatch.setattr(
        "src.github.prs.get_pr_metadata",
        lambda repo, num: {
            "author": "octo-cat",
            "head_sha": "a023d70stale",
            "head_commit_date": "2026-05-12T23:14:00Z",
        },
    )
    monkeypatch.setattr(
        "src.github.comments.has_recent_codex_review_request",
        lambda *args, **kwargs: True,
    )

    posted: list[tuple[str, int, str]] = []
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, num, body: posted.append((repo, num, body)),
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.MERGE
    runner.state.current_pr = PRInfo(number=420, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING,
    )

    asyncio.run(runner.handle_merge())

    assert posted == [(runner.owner_repo, 420, "@codex review")], (
        "pre-merge sync must bypass PR-author dedup and post @codex review "
        "on the new merge commit"
    )
    assert runner.state.state == PipelineState.WATCH


def test_post_codex_review_respects_author_dedup_outside_pre_merge_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Outside the pre-merge sync call site, the daemon does not know
    whether the GitHub REST API reflects the latest head, so the
    PR-author dedup must keep blocking duplicate ``@codex review``
    triggers. Calling ``_post_codex_review`` without ``bypass_author_dedup``
    must skip posting when the dedup matches."""
    def fake_git(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:3] == ["git", "rev-parse", "HEAD"]:
            return h._FakeCompletedProcess(
                args=cmd,
                stdout="head-sha-deadbeef\n",
                returncode=0,
            )
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_git)

    monkeypatch.setattr(
        "src.github.prs.get_pr_metadata",
        lambda repo, num: {
            "author": "octo-cat",
            "head_sha": "head-sha-deadbeef",
            "head_commit_date": "2026-05-12T23:14:00Z",
        },
    )
    monkeypatch.setattr(
        "src.github.comments.has_recent_codex_review_request",
        lambda *args, **kwargs: True,
    )
    monkeypatch.setattr(
        "src.daemon.handlers.hung.gh_comments.has_recent_codex_review_request",
        lambda *args, **kwargs: True,
    )

    posted: list[tuple[str, int, str]] = []
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, num, body: posted.append((repo, num, body)),
    )

    runner = h._make_runner()

    result = runner._post_codex_review(420)

    assert result is True, (
        "dedup-skip path returns True (treated as success) so non-fatal "
        "call sites do not transition to ERROR"
    )
    assert posted == [], (
        "non-sync call site must still honor PR-author dedup and not "
        "post a duplicate @codex review"
    )
