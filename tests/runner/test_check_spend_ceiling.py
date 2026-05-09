"""Tests for the spend-ceiling dispatch gate."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import pytest
from src.daemon.notifications import send_spend_ceiling_warning
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


def _enable_warning_webhook(runner: Any, *, cap: int = 80) -> None:
    runner.app_config.daemon.spend_ceiling_session_percent = cap
    runner.app_config.daemon.spend_ceiling_warning_percent = 80
    runner.app_config.daemon.guardrail_notification_webhook_url = "https://example.test/hook"


def test_check_spend_ceiling_disabled_returns_true(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()

    async def boom(coder_name: str) -> UsageSnapshot | None:
        raise AssertionError("snapshot should not be fetched")

    monkeypatch.setattr(runner, "_fetch_usage_snapshot", boom)

    assert asyncio.run(runner._check_spend_ceiling("claude")) is True


@pytest.mark.parametrize(
    ("cap_field", "snapshot", "expected"),
    [
        ("spend_ceiling_session_percent", _snapshot(session_percent=72), "session"),
        ("spend_ceiling_weekly_percent", _snapshot(weekly_percent=85), "weekly"),
    ],
)
def test_check_spend_ceiling_cap_exceeded_returns_false(
    monkeypatch: pytest.MonkeyPatch,
    cap_field: str,
    snapshot: UsageSnapshot,
    expected: str,
) -> None:
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    setattr(runner.app_config.daemon, cap_field, 70)
    runner._claude_usage_provider = h._FakeUsageProvider(snapshot=snapshot)

    assert asyncio.run(runner._check_spend_ceiling("claude")) is False
    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.error_message is None
    assert any(
        f"[SPEND-CEILING] claude {expected} cap reached" in event["event"]
        for event in runner.state.history
    )


def test_check_spend_ceiling_below_caps_returns_true(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.app_config.daemon.spend_ceiling_session_percent = 70
    runner.app_config.daemon.spend_ceiling_weekly_percent = 70
    runner._claude_usage_provider = h._FakeUsageProvider(
        snapshot=_snapshot(session_percent=69, weekly_percent=60),
    )
    warnings: list[str] = []

    async def fake_warning(coder_name: str, snapshot: UsageSnapshot) -> None:
        warnings.append(coder_name)

    monkeypatch.setattr(runner, "_maybe_send_ceiling_warning", fake_warning)

    assert asyncio.run(runner._check_spend_ceiling("claude")) is True
    assert warnings == ["claude"]


def test_check_spend_ceiling_snapshot_unavailable_fail_open() -> None:
    runner = h._make_runner()
    runner.app_config.daemon.spend_ceiling_session_percent = 70
    runner._claude_usage_provider = h._FakeUsageProvider(snapshot=None)

    assert asyncio.run(runner._check_spend_ceiling("claude")) is True


def test_maybe_send_ceiling_warning_fires_at_warning_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    _enable_warning_webhook(runner)
    calls: list[dict[str, Any]] = []

    async def fake_send(**kwargs: Any) -> None:
        calls.append(kwargs)

    monkeypatch.setattr("src.daemon.rate_limit.send_spend_ceiling_warning", fake_send)

    asyncio.run(runner._maybe_send_ceiling_warning("claude", _snapshot(session_percent=64)))

    assert len(calls) == 1
    assert calls[0]["limit_kind"] == "session"
    assert calls[0]["current_percent"] == 64


def test_maybe_send_ceiling_warning_dedup_prevents_double_fire(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    _enable_warning_webhook(runner)
    calls: list[dict[str, Any]] = []

    async def fake_send(**kwargs: Any) -> None:
        calls.append(kwargs)

    monkeypatch.setattr("src.daemon.rate_limit.send_spend_ceiling_warning", fake_send)
    snapshot = _snapshot(session_percent=64)

    asyncio.run(runner._maybe_send_ceiling_warning("claude", snapshot))
    asyncio.run(runner._maybe_send_ceiling_warning("claude", snapshot))

    assert len(calls) == 1


@pytest.mark.parametrize("webhook_url", ["https://example.test/hook", None])
def test_maybe_send_ceiling_warning_no_fire_paths(
    monkeypatch: pytest.MonkeyPatch,
    webhook_url: str | None,
) -> None:
    runner = h._make_runner()
    runner.app_config.daemon.spend_ceiling_session_percent = 80
    runner.app_config.daemon.spend_ceiling_warning_percent = 80
    runner.app_config.daemon.guardrail_notification_webhook_url = webhook_url
    calls: list[dict[str, Any]] = []

    async def fake_send(**kwargs: Any) -> None:
        calls.append(kwargs)

    monkeypatch.setattr("src.daemon.rate_limit.send_spend_ceiling_warning", fake_send)

    asyncio.run(runner._maybe_send_ceiling_warning("claude", _snapshot(session_percent=50)))

    assert calls == []


def test_maybe_send_ceiling_warning_redis_error_skips_send(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RaisingRedis(h._FakeRedis):
        async def set(
            self,
            key: str,
            value: str,
            ex: int | None = None,
            nx: bool = False,
        ) -> bool | None:
            raise RuntimeError("redis down")

    runner = h._make_runner()
    runner.redis = RaisingRedis()
    _enable_warning_webhook(runner)
    calls: list[dict[str, Any]] = []

    async def fake_send(**kwargs: Any) -> None:
        calls.append(kwargs)

    monkeypatch.setattr("src.daemon.rate_limit.send_spend_ceiling_warning", fake_send)

    asyncio.run(runner._maybe_send_ceiling_warning("claude", _snapshot(session_percent=80)))

    assert calls == []


def test_maybe_send_ceiling_warning_logs_notification_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    _enable_warning_webhook(runner)

    async def fake_send(**kwargs: Any) -> None:
        raise RuntimeError("webhook down")

    monkeypatch.setattr("src.daemon.rate_limit.send_spend_ceiling_warning", fake_send)

    asyncio.run(runner._maybe_send_ceiling_warning("claude", _snapshot(session_percent=80)))

    dedup_key = "warn:spend_ceiling:claude:session:1900000000"
    assert dedup_key not in runner.redis.store
    assert runner.redis.deleted == [dedup_key]
    assert any(
        "[SPEND-CEILING] warn notification failed: webhook down" in entry["event"]
        for entry in runner.state.history
    )


def test_maybe_send_ceiling_warning_swallows_dedup_cleanup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class CleanupFailingRedis(h._FakeRedis):
        async def delete(self, key: str) -> int:
            raise RuntimeError("redis delete down")

    runner = h._make_runner()
    runner.redis = CleanupFailingRedis()
    _enable_warning_webhook(runner)

    async def fake_send(**kwargs: Any) -> None:
        raise RuntimeError("webhook down")

    monkeypatch.setattr("src.daemon.rate_limit.send_spend_ceiling_warning", fake_send)

    asyncio.run(runner._maybe_send_ceiling_warning("claude", _snapshot(session_percent=80)))

    assert any(
        "[SPEND-CEILING] warn dedup cleanup failed: redis delete down" in entry["event"]
        for entry in runner.state.history
    )
    assert any(
        "[SPEND-CEILING] warn notification failed: webhook down" in entry["event"]
        for entry in runner.state.history
    )


def test_send_spend_ceiling_warning_payload_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[tuple[str, dict[str, Any], float]] = []

    class FakeClient:
        def __init__(self, timeout: float) -> None:
            self.timeout = timeout

        async def __aenter__(self) -> "FakeClient":
            return self

        async def __aexit__(self, *args: object) -> None:
            return None

        async def post(self, webhook_url: str, json: dict[str, Any]) -> "FakeResponse":
            posted.append((webhook_url, json, self.timeout))
            return FakeResponse()

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr("src.daemon.notifications.httpx.AsyncClient", FakeClient)

    asyncio.run(
        send_spend_ceiling_warning(
            webhook_url="https://example.test/hook",
            coder_name="claude",
            limit_kind="session",
            current_percent=64,
            cap_percent=80,
            warning_percent=80,
            timeout_seconds=3.0,
        )
    )

    assert posted[0][0] == "https://example.test/hook"
    assert posted[0][1]["event"] == "spend_ceiling_warning"
    assert posted[0][1]["text"].startswith("SPEND CEILING WARNING: claude session")
    assert posted[0][2] == 3.0


def test_send_spend_ceiling_warning_raises_for_non_success_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeClient:
        def __init__(self, timeout: float) -> None:
            self.timeout = timeout

        async def __aenter__(self) -> "FakeClient":
            return self

        async def __aexit__(self, *args: object) -> None:
            return None

        async def post(self, webhook_url: str, json: dict[str, Any]) -> "FakeResponse":
            return FakeResponse()

    class FakeResponse:
        def raise_for_status(self) -> None:
            raise RuntimeError("server error")

    monkeypatch.setattr("src.daemon.notifications.httpx.AsyncClient", FakeClient)

    with pytest.raises(RuntimeError, match="server error"):
        asyncio.run(
            send_spend_ceiling_warning(
                webhook_url="https://example.test/hook",
                coder_name="claude",
                limit_kind="session",
                current_percent=64,
                cap_percent=80,
                warning_percent=80,
                timeout_seconds=3.0,
            )
        )
