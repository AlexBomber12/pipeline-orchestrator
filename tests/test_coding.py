from __future__ import annotations

import asyncio

import pytest
from src.daemon.handlers import coding as coding_module
from src.models import PipelineState, QueueTask, TaskStatus

from tests import test_runner as h


@pytest.fixture(autouse=True)
def _reset_counts() -> None:
    coding_module._NO_PR_RETRY_COUNTS.clear()
    yield
    coding_module._NO_PR_RETRY_COUNTS.clear()


def _runner(monkeypatch: pytest.MonkeyPatch):
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(h.claude_cli, "run_planned_pr_async", h._async_cli_result(0, "ok", ""))
    monkeypatch.setattr(h.runner_module.github_client, "get_open_prs", lambda repo, **kw: [])

    async def _sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(h.runner_module.asyncio, "sleep", _sleep)
    runner = h._make_runner()
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001")
    return runner


def test_first_no_pr_failure_does_not_block(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = _runner(monkeypatch)
    asyncio.run(runner.handle_coding())
    assert runner.state.state == PipelineState.ERROR
    assert "no PR found" in (runner.state.error_message or "")
    assert coding_module._NO_PR_RETRY_COUNTS == {"PR-001": 1}
    coding_module._clear_stale_no_pr_retry_count(runner, "PR-002")
    assert coding_module._NO_PR_RETRY_COUNTS == {}
    assert runner._last_no_pr_failed_pr_id is None


def test_second_no_pr_failure_enters_hung(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = _runner(monkeypatch)
    coding_module._clear_stale_no_pr_retry_count(runner, None)
    asyncio.run(runner.handle_coding())
    asyncio.run(runner.handle_coding())
    blocked = "Task PR-001 blocked: coder failed to create PR 2 times in a row. Manual intervention required."
    assert runner.state.state == PipelineState.HUNG
    assert runner.state.error_message == blocked
    assert coding_module._NO_PR_RETRY_COUNTS == {"PR-001": 2}
    assert any(entry["event"] == blocked for entry in runner.state.history)
