"""Tests for the unified proactive usage gate."""

from __future__ import annotations

import asyncio
import inspect
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from src.config import DaemonConfig, load_config
from src.models import PipelineState
from src.usage import UsageSnapshot

from tests.runner import _helpers as h


def _snapshot(
    *,
    session_percent: int = 10,
    weekly_percent: int = 10,
    session_resets_at: int = 1_900_000_000,
    weekly_resets_at: int = 1_900_000_600,
) -> UsageSnapshot:
    return UsageSnapshot(
        session_percent=session_percent,
        session_resets_at=session_resets_at,
        weekly_percent=weekly_percent,
        weekly_resets_at=weekly_resets_at,
        fetched_at=int(datetime.now(timezone.utc).timestamp()),
    )


def _pause_fingerprint(runner: Any) -> tuple[PipelineState, datetime | None, str | None]:
    return (
        runner.state.state,
        runner.state.rate_limited_until,
        runner.state.rate_limit_reactive_coder,
    )


def test_rate_limit_threshold_pauses() -> None:
    runner = h._make_runner()
    runner.app_config.daemon.rate_limit_session_pause_percent = 80
    runner._claude_usage_provider = h._FakeUsageProvider(
        snapshot=_snapshot(session_percent=80),
    )

    assert asyncio.run(runner.usage_gate(proactive_coder="claude")) is False

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.rate_limit_reactive is False
    assert runner.state.rate_limit_reactive_coder == "claude"
    assert any(
        "Proactive pause: session usage at 80%" in e["event"]
        for e in runner.state.history
    )


def test_spend_ceiling_threshold_pauses() -> None:
    runner = h._make_runner()
    runner.app_config.daemon.rate_limit_session_pause_percent = 95
    runner.app_config.daemon.spend_ceiling_session_percent = 70
    runner._claude_usage_provider = h._FakeUsageProvider(
        snapshot=_snapshot(session_percent=70),
    )

    assert asyncio.run(runner.usage_gate(proactive_coder="claude")) is False

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.rate_limit_reactive is False
    assert runner.state.rate_limit_reactive_coder == "claude"
    assert any(
        "[SPEND-CEILING] claude session cap reached" in e["event"]
        for e in runner.state.history
    )


def test_both_thresholds_same_pause_state() -> None:
    rate_runner = h._make_runner()
    rate_runner.app_config.daemon.rate_limit_session_pause_percent = 80
    rate_runner._claude_usage_provider = h._FakeUsageProvider(
        snapshot=_snapshot(session_percent=80),
    )

    spend_runner = h._make_runner()
    spend_runner.app_config.daemon.rate_limit_session_pause_percent = 95
    spend_runner.app_config.daemon.spend_ceiling_session_percent = 80
    spend_runner._claude_usage_provider = h._FakeUsageProvider(
        snapshot=_snapshot(session_percent=80),
    )

    assert asyncio.run(rate_runner.usage_gate(proactive_coder="claude")) is False
    assert asyncio.run(spend_runner.usage_gate(proactive_coder="claude")) is False

    assert _pause_fingerprint(rate_runner) == _pause_fingerprint(spend_runner)
    assert (
        rate_runner.state.rate_limited_coder_until
        == spend_runner.state.rate_limited_coder_until
    )


def test_warning_webhook_preserved(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = h._make_runner()
    runner.app_config.daemon.spend_ceiling_session_percent = 80
    runner.app_config.daemon.spend_ceiling_warning_percent = 80
    runner.app_config.daemon.guardrail_notification_webhook_url = "https://example.test/hook"
    runner._claude_usage_provider = h._FakeUsageProvider(
        snapshot=_snapshot(session_percent=64),
    )
    calls: list[dict[str, Any]] = []

    async def fake_send(**kwargs: Any) -> None:
        calls.append(kwargs)

    monkeypatch.setattr("src.daemon.rate_limit.send_spend_ceiling_warning", fake_send)

    assert asyncio.run(runner.usage_gate(proactive_coder="claude")) is True
    assert asyncio.run(runner.usage_gate(proactive_coder="claude")) is True

    assert len(calls) == 1
    assert calls[0]["limit_kind"] == "session"
    assert calls[0]["current_percent"] == 64


def test_reactive_429_still_separate() -> None:
    import src.daemon.handlers.coding as coding
    import src.daemon.handlers.fix as fix

    coding_source = inspect.getsource(coding.CodingMixin)
    fix_source = inspect.getsource(fix.FixMixin.handle_fix)

    assert "_detect_rate_limit(" in coding_source
    assert "coder_name=coder_name" in coding_source
    assert "_detect_rate_limit(" in fix_source
    assert "coder_name=coder_name" in fix_source


def test_legacy_config_fields_parse(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        "\n".join(
            [
                "daemon:",
                "  rate_limit_session_pause_percent: 81",
                "  rate_limit_weekly_pause_percent: 91",
                "  spend_ceiling_session_percent: 71",
                "  spend_ceiling_weekly_percent: 86",
                "  spend_ceiling_warning_percent: 66",
            ]
        ),
        encoding="utf-8",
    )

    cfg = load_config(str(config_path))

    assert cfg.daemon.usage_gate_rate_limit_session_pause_percent == 81
    assert cfg.daemon.usage_gate_rate_limit_weekly_pause_percent == 91
    assert cfg.daemon.usage_gate_spend_ceiling_session_percent == 71
    assert cfg.daemon.usage_gate_spend_ceiling_weekly_percent == 86
    assert cfg.daemon.spend_ceiling_warning_percent == 66


def test_behavior_identical_to_pre_merge() -> None:
    snapshots = [
        (_snapshot(session_percent=69, weekly_percent=10), True),
        (_snapshot(session_percent=80, weekly_percent=10), False),
        (_snapshot(session_percent=75, weekly_percent=90), False),
        (_snapshot(session_percent=70, weekly_percent=50), False),
        (_snapshot(session_percent=64, weekly_percent=64), True),
    ]

    for snapshot, expected in snapshots:
        runner = h._make_runner()
        runner.app_config.daemon.rate_limit_session_pause_percent = 80
        runner.app_config.daemon.rate_limit_weekly_pause_percent = 90
        runner.app_config.daemon.spend_ceiling_session_percent = 70
        runner.app_config.daemon.spend_ceiling_weekly_percent = 70
        runner._claude_usage_provider = h._FakeUsageProvider(snapshot=snapshot)

        assert asyncio.run(runner.usage_gate(proactive_coder="claude")) is expected
        assert (runner.state.state != PipelineState.PAUSED) is expected


def test_no_config_field_deleted() -> None:
    fields = DaemonConfig.model_fields

    assert "rate_limit_session_pause_percent" in fields
    assert "rate_limit_weekly_pause_percent" in fields
    assert "spend_ceiling_session_percent" in fields
    assert "spend_ceiling_weekly_percent" in fields
    assert "spend_ceiling_warning_percent" in fields

    cfg = DaemonConfig(
        rate_limit_session_pause_percent=82,
        rate_limit_weekly_pause_percent=92,
        spend_ceiling_session_percent=72,
        spend_ceiling_weekly_percent=87,
        spend_ceiling_warning_percent=67,
    )
    assert cfg.usage_gate_rate_limit_session_pause_percent == 82
    assert cfg.usage_gate_rate_limit_weekly_pause_percent == 92
    assert cfg.usage_gate_spend_ceiling_session_percent == 72
    assert cfg.usage_gate_spend_ceiling_weekly_percent == 87
    assert cfg.spend_ceiling_warning_percent == 67
