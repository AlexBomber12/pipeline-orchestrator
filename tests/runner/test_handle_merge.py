"""Tests for MERGE cleanup metadata."""

from __future__ import annotations

import asyncio

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
