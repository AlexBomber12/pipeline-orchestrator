"""PR-307b: ``_transition_to_error`` guardrail notification integration tests.

PR-307a built ``send_guardrail_notification`` in isolation. PR-307b wires
it into ``_transition_to_error`` so a GUARDRAIL cause fires the webhook
(when configured) as a side-effect of the ERROR transition.

These tests assert the integration contract:
- delivery is invoked once with the right kwargs when configured + tier
  >= min_tier;
- delivery is skipped when the webhook URL is unset, the tier is below
  the minimum, the cause is not a GUARDRAIL, or no task is active;
- timeouts and exceptions are caught and logged without crashing the
  transition.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from src.daemon import runner as runner_module
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus

from tests.runner import _helpers as h


def _install_publish_state_spy(runner) -> list[None]:
    calls: list[None] = []

    async def fake_publish() -> None:
        calls.append(None)

    runner.publish_state = fake_publish  # type: ignore[method-assign]
    return calls


def _install_save_run_record_spy(runner) -> list[str]:
    calls: list[str] = []

    async def fake_save(exit_reason: str, **_: object) -> None:
        calls.append(exit_reason)

    runner._save_current_run_record = fake_save  # type: ignore[method-assign]
    return calls


def _runner_with_notification_config(
    *,
    webhook_url: str | None = "https://example.com/webhook",
    min_tier: int = 1,
    timeout_seconds: float = 5.0,
    dashboard_base_url: str | None = "https://dash.example.com",
):
    runner = h._make_runner()
    runner.app_config.daemon.guardrail_notification_webhook_url = webhook_url
    runner.app_config.daemon.guardrail_notification_min_tier = min_tier
    runner.app_config.daemon.guardrail_notification_timeout_seconds = timeout_seconds
    runner.app_config.daemon.dashboard_base_url = dashboard_base_url
    _install_publish_state_spy(runner)
    _install_save_run_record_spy(runner)
    return runner


def _set_active_task(runner, *, pr_id: str = "PR-042", with_pr: bool = True) -> None:
    runner.state.current_task = QueueTask(
        pr_id=pr_id, title="active", status=TaskStatus.DOING,
    )
    if with_pr:
        runner.state.current_pr = PRInfo(number=119, branch="pr-042")


def test_transition_to_error_guardrail_calls_notification_when_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_notification_config()
    _set_active_task(runner)
    mock = AsyncMock(return_value=None)
    monkeypatch.setattr(runner_module, "send_guardrail_notification", mock)

    asyncio.run(
        runner._transition_to_error(
            "GUARDRAIL: tier=2 large_diff_threshold: +1500 LOC across 30 files"
        )
    )

    assert runner.state.state == PipelineState.ERROR
    mock.assert_awaited_once()
    kwargs = mock.await_args.kwargs
    assert kwargs["webhook_url"] == "https://example.com/webhook"
    assert kwargs["repo_name"] == runner.name
    assert kwargs["pr_id"] == "PR-042"
    assert kwargs["pr_number"] == 119
    assert kwargs["owner_repo"] == runner.owner_repo
    assert kwargs["tier"] == 2
    assert kwargs["category"] == "large_diff_threshold"
    assert kwargs["timeout_seconds"] == 5.0
    assert kwargs["dashboard_base_url"] == "https://dash.example.com"
    assert "+1500 LOC" in kwargs["excerpt"]
    assert kwargs["rule"] == "large_diff_threshold"


def test_transition_to_error_bracketed_guardrail_prefix_calls_notification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_notification_config()
    _set_active_task(runner)
    mock = AsyncMock(return_value=None)
    monkeypatch.setattr(runner_module, "send_guardrail_notification", mock)

    asyncio.run(
        runner._transition_to_error(
            "[GUARDRAIL] tier=2 large_diff_threshold: +1500 LOC across 30 files"
        )
    )

    mock.assert_awaited_once()
    kwargs = mock.await_args.kwargs
    assert kwargs["tier"] == 2
    assert kwargs["category"] == "large_diff_threshold"
    assert "+1500 LOC" in kwargs["excerpt"]


def test_transition_to_error_guardrail_skips_notification_when_webhook_url_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_notification_config(webhook_url=None)
    _set_active_task(runner)
    mock = AsyncMock(return_value=None)
    monkeypatch.setattr(runner_module, "send_guardrail_notification", mock)

    asyncio.run(
        runner._transition_to_error(
            "GUARDRAIL: tier=2 large_diff_threshold: +1500 LOC"
        )
    )

    mock.assert_not_awaited()


def test_transition_to_error_guardrail_skips_notification_when_below_min_tier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_notification_config(min_tier=2)
    _set_active_task(runner)
    mock = AsyncMock(return_value=None)
    monkeypatch.setattr(runner_module, "send_guardrail_notification", mock)

    asyncio.run(
        runner._transition_to_error(
            "GUARDRAIL: tier=1 governance_file_tampering: AGENTS.md modified"
        )
    )

    mock.assert_not_awaited()


def test_transition_to_error_guardrail_notification_timeout_is_logged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_notification_config()
    _set_active_task(runner)

    async def slow_send(**_: object) -> None:
        raise asyncio.TimeoutError()

    monkeypatch.setattr(runner_module, "send_guardrail_notification", slow_send)

    asyncio.run(
        runner._transition_to_error(
            "GUARDRAIL: tier=2 large_diff_threshold: +1500 LOC"
        )
    )

    assert runner.state.state == PipelineState.ERROR
    assert any(
        e["event"] == "[GUARDRAIL] notification webhook timed out"
        for e in runner.state.history
    )


def test_transition_to_error_guardrail_notification_exception_is_logged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_notification_config()
    _set_active_task(runner)

    async def boom_send(**_: object) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(runner_module, "send_guardrail_notification", boom_send)

    asyncio.run(
        runner._transition_to_error(
            "GUARDRAIL: tier=2 large_diff_threshold: +1500 LOC"
        )
    )

    assert runner.state.state == PipelineState.ERROR
    assert any(
        e["event"] == "[GUARDRAIL] notification webhook failed: boom"
        for e in runner.state.history
    )


def test_transition_to_error_non_guardrail_cause_skips_notification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_notification_config()
    _set_active_task(runner)
    mock = AsyncMock(return_value=None)
    monkeypatch.setattr(runner_module, "send_guardrail_notification", mock)

    asyncio.run(
        runner._transition_to_error("FIX iteration cap reached")
    )

    mock.assert_not_awaited()


def test_transition_to_error_guardrail_no_current_task_skips_notification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_notification_config()
    runner.state.current_task = None
    mock = AsyncMock(return_value=None)
    monkeypatch.setattr(runner_module, "send_guardrail_notification", mock)

    asyncio.run(
        runner._transition_to_error(
            "GUARDRAIL: tier=2 large_diff_threshold: +1500 LOC"
        )
    )

    mock.assert_not_awaited()


def test_transition_to_error_guardrail_no_current_pr_passes_pr_number_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_with_notification_config()
    _set_active_task(runner, with_pr=False)
    mock = AsyncMock(return_value=None)
    monkeypatch.setattr(runner_module, "send_guardrail_notification", mock)

    asyncio.run(
        runner._transition_to_error(
            "GUARDRAIL: tier=2 large_diff_threshold: +1500 LOC"
        )
    )

    mock.assert_awaited_once()
    assert mock.await_args.kwargs["pr_number"] is None
