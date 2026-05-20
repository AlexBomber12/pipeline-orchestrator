"""Regression tests for ERROR soft-skip banner clearing."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from src.models import PipelineState, QueueTask, TaskStatus
from src.usage import UsageSnapshot
from tests.runner import _helpers as h


def _task(task_id: str = "PR-001") -> QueueTask:
    return QueueTask(pr_id=task_id, title=task_id, status=TaskStatus.DOING)


def _make_error_runner(
    *,
    context: str = "get_open_prs failed: HTTP 504",
    with_task: bool = False,
) -> Any:
    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = context
    if with_task:
        runner.state.current_task = _task()
    return runner


def _force_auxiliary_coder_rate_limited(runner: Any) -> None:
    snapshot = UsageSnapshot(
        session_percent=99,
        session_resets_at=0,
        weekly_percent=10,
        weekly_resets_at=0,
        fetched_at=0.0,
    )
    runner._claude_usage_provider = h._FakeUsageProvider(snapshot=snapshot)
    runner._codex_usage_provider = h._FakeUsageProvider(snapshot=snapshot)


def test_infra_soft_skip_clears_error_message() -> None:
    runner = _make_error_runner()

    asyncio.run(runner.handle_error("gh api failed after 3 attempts: HTTP 504"))

    assert runner.state.error_message is None
    assert runner.state.state == PipelineState.IDLE


def test_rate_limit_soft_skip_clears_error_message() -> None:
    runner = _make_error_runner()

    asyncio.run(runner.handle_error("rate limit exceeded"))

    assert runner.state.error_message is None
    assert runner.state.state == PipelineState.IDLE


def test_timeout_soft_skip_clears_error_message() -> None:
    runner = _make_error_runner()

    asyncio.run(runner.handle_error("operation timeout after 60s"))

    assert runner.state.error_message is None
    assert runner.state.state == PipelineState.IDLE


def test_infra_soft_skip_log_event_emitted() -> None:
    runner = _make_error_runner()

    asyncio.run(runner.handle_error("gh api failed after 3 attempts: HTTP 504"))

    assert any(
        entry["event"].startswith("[ERROR] cleared error_message")
        and "infra error soft-skip to IDLE" in entry["event"]
        for entry in runner.state.history
    )


def test_clear_error_message_called_after_state_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_error_runner()
    calls: list[str] = []

    async def fake_clear_error_message_on_recovery(**_kwargs: object) -> None:
        assert runner.state.state == PipelineState.IDLE
        calls.append("clear")
        runner.state.error_message = None

    monkeypatch.setattr(
        runner,
        "_clear_error_message_on_recovery",
        fake_clear_error_message_on_recovery,
    )

    asyncio.run(runner.handle_error("rate limit exceeded"))

    assert calls == ["clear"]


def test_publish_state_called_after_clear(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = _make_error_runner()
    calls: list[str] = []

    async def fake_clear_error_message_on_recovery(**_kwargs: object) -> None:
        calls.append("clear")
        runner.state.error_message = None

    async def fake_publish_state() -> None:
        calls.append("publish")

    monkeypatch.setattr(
        runner,
        "_clear_error_message_on_recovery",
        fake_clear_error_message_on_recovery,
    )
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.handle_error("operation timeout after 60s"))

    assert calls == ["clear", "publish"]


def test_current_task_preserved_through_soft_skip() -> None:
    runner = _make_error_runner(with_task=True)
    task = runner.state.current_task

    asyncio.run(runner.handle_error("gh api failed after 3 attempts: HTTP 504"))

    assert runner.state.current_task == task
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None


def test_existing_rate_limit_diagnose_skip_branch_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_error_runner(context="coder subprocess crashed")
    _force_auxiliary_coder_rate_limited(runner)
    calls: list[dict[str, object]] = []
    original_clear = runner._clear_error_message_on_recovery

    async def recording_clear_error_message_on_recovery(**kwargs: object) -> None:
        calls.append(dict(kwargs))
        await original_clear(**kwargs)

    monkeypatch.setattr(
        runner,
        "_clear_error_message_on_recovery",
        recording_clear_error_message_on_recovery,
    )

    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None
    assert calls == [
        {
            "log_prefix": "[ERROR]",
            "reason": "rate-limited diagnosis skipped to IDLE",
        }
    ]
