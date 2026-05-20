from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import pytest
from src.daemon.runner import PipelineRunner
from src.models import (
    CIStatus,
    PipelineState,
    PRInfo,
    QueueTask,
    ReviewStatus,
    TaskStatus,
)
from tests.runner import _helpers as h


def _watch_pr(number: int = 443) -> PRInfo:
    return PRInfo(
        number=number,
        branch=f"pr-{number}",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
    )


def _seed_watch_runner(
    monkeypatch: pytest.MonkeyPatch,
    pr: PRInfo,
    *,
    pr_state: str | None,
) -> PipelineRunner:
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])
    monkeypatch.setattr(
        "src.github.prs.get_pr_state",
        lambda repo, number: pr_state,
    )
    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=pr.number, branch=pr.branch)
    runner.state.current_task = QueueTask(
        pr_id=f"PR-{pr.number:03d}",
        title="external resolution",
        status=TaskStatus.DOING,
        branch=pr.branch,
        task_file=f"tasks/PR-{pr.number:03d}.md",
    )
    return runner


def _stub_publish(runner: PipelineRunner) -> list[str]:
    calls: list[str] = []

    async def fake_publish() -> None:
        calls.append("publish")

    runner.publish_state = fake_publish  # type: ignore[method-assign]
    return calls


def test_external_merge_releases_task_writes_status_done(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = _watch_pr(number=1)
    runner = _seed_watch_runner(monkeypatch, pr, pr_state="MERGED")
    status_writes: list[tuple[QueueTask, str, str]] = []

    async def fake_commit(
        self: PipelineRunner,
        current_task: QueueTask,
        status: str,
        reason: str,
    ) -> bool:
        status_writes.append((current_task, status, reason))
        return True

    monkeypatch.setattr(
        PipelineRunner,
        "_commit_task_status_change",
        fake_commit,
    )
    monkeypatch.setattr(
        PipelineRunner,
        "_mark_task_done_in_snapshot",
        lambda self: None,
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert [status for _task, status, _reason in status_writes] == ["DONE"]


def test_external_close_releases_task_without_status_done(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = _watch_pr(number=2)
    runner = _seed_watch_runner(monkeypatch, pr, pr_state="CLOSED")
    status_writes: list[tuple[Any, ...]] = []

    async def fake_commit(*args: Any, **kwargs: Any) -> bool:
        status_writes.append(args)
        return True

    monkeypatch.setattr(
        PipelineRunner,
        "_commit_task_status_change",
        fake_commit,
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert status_writes == []


def test_open_state_continues_normal_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = _watch_pr(number=3)
    runner = _seed_watch_runner(monkeypatch, pr, pr_state="OPEN")
    calls: list[str] = []

    async def fake_reclassify(self: PipelineRunner, found: PRInfo) -> None:
        calls.append("reclassify")

    async def fake_scan(self: PipelineRunner) -> bool:
        calls.append("scan")
        return True

    monkeypatch.setattr(
        PipelineRunner,
        "_maybe_reclassify_stuck_pending",
        fake_reclassify,
    )
    monkeypatch.setattr(PipelineRunner, "_scan_pr_diff_once", fake_scan)

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.WATCH
    assert calls == ["reclassify", "scan"]


def test_get_pr_state_returns_none_continues_normal_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = _watch_pr(number=4)
    runner = _seed_watch_runner(monkeypatch, pr, pr_state=None)
    calls: list[str] = []

    async def fake_reclassify(self: PipelineRunner, found: PRInfo) -> None:
        calls.append("reclassify")

    async def fake_scan(self: PipelineRunner) -> bool:
        calls.append("scan")
        return True

    monkeypatch.setattr(
        PipelineRunner,
        "_maybe_reclassify_stuck_pending",
        fake_reclassify,
    )
    monkeypatch.setattr(PipelineRunner, "_scan_pr_diff_once", fake_scan)

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.WATCH
    assert calls == ["reclassify", "scan"]


def test_log_event_on_external_merged(monkeypatch: pytest.MonkeyPatch) -> None:
    pr = _watch_pr(number=5)
    runner = _seed_watch_runner(monkeypatch, pr, pr_state="MERGED")
    monkeypatch.setattr(
        PipelineRunner,
        "_commit_task_status_change",
        lambda *args, **kwargs: True,
    )

    asyncio.run(runner.handle_watch())

    assert any(
        "externally resolved as MERGED; releasing task" in entry["event"]
        for entry in runner.state.history
    )


def test_log_event_on_external_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    pr = _watch_pr(number=6)
    runner = _seed_watch_runner(monkeypatch, pr, pr_state="CLOSED")

    asyncio.run(runner.handle_watch())

    assert any(
        "externally resolved as CLOSED; releasing task" in entry["event"]
        for entry in runner.state.history
    )


def test_publish_state_called_after_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = _watch_pr(number=7)
    runner = _seed_watch_runner(monkeypatch, pr, pr_state="CLOSED")
    publish_calls = _stub_publish(runner)

    asyncio.run(runner.handle_watch())

    assert publish_calls == ["publish"]


def test_external_resolution_clears_pr_without_current_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = _watch_pr(number=9)
    runner = _seed_watch_runner(monkeypatch, pr, pr_state="CLOSED")
    runner.state.current_task = None
    runner.state.current_pr = PRInfo(number=pr.number, branch=pr.branch)

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None


def test_disappeared_pr_uses_direct_terminal_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])
    monkeypatch.setattr("src.github.prs.get_pr_state", lambda repo, number: "MERGED")
    is_pr_merged_calls: list[int] = []
    monkeypatch.setattr(
        "src.github.prs.is_pr_merged",
        lambda repo, number: is_pr_merged_calls.append(number) or None,
    )
    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=8, branch="pr-008")
    runner.state.current_task = QueueTask(
        pr_id="PR-008",
        title="external resolution",
        status=TaskStatus.DOING,
        branch="pr-008",
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert is_pr_merged_calls == []
