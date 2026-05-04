"""Tests for PR-255 Cancellation policy v1 - SignalSource composition.

Each SignalSource is verified in isolation, then ``is_operator_available``
is exercised across the policy branches:

- ManualOverride wins over every other source.
- AVAILABLE wins over AWAY among the remaining sources.
- A source raising ``Exception`` is treated as ``None`` (defer).
- Default verdict is AVAILABLE so a fully-broken observability stack
  never pauses coder work.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest

from src.cancellation import availability as availability_module
from src.cancellation.availability import (
    ActiveHoursSource,
    AvailabilityState,
    HeartbeatSource,
    ManualOverrideSource,
    is_operator_available,
)


class _FakeRedis:
    """Minimal async redis stand-in for SignalSource tests."""

    def __init__(
        self,
        *,
        get_value: Any = None,
        exists_value: int = 0,
        raise_on_get: bool = False,
        raise_on_exists: bool = False,
    ) -> None:
        self._get_value = get_value
        self._exists_value = exists_value
        self._raise_on_get = raise_on_get
        self._raise_on_exists = raise_on_exists

    async def get(self, key: str) -> Any:
        if self._raise_on_get:
            raise RuntimeError("boom")
        return self._get_value

    async def exists(self, key: str) -> int:
        if self._raise_on_exists:
            raise RuntimeError("boom")
        return self._exists_value


class _FixedNowDatetime:
    """``datetime`` shim that returns a fixed now and forwards everything else."""

    def __init__(self, fixed: datetime) -> None:
        self._fixed = fixed

    def now(self, tz: Any = None) -> datetime:
        return self._fixed.astimezone(tz) if tz is not None else self._fixed


# ---------------------------------------------------------------------------
# ManualOverrideSource
# ---------------------------------------------------------------------------


async def test_manual_override_available() -> None:
    source = ManualOverrideSource(redis_client=_FakeRedis(get_value="AVAILABLE"))
    assert await source.query() is AvailabilityState.AVAILABLE


async def test_manual_override_away() -> None:
    source = ManualOverrideSource(redis_client=_FakeRedis(get_value="AWAY"))
    assert await source.query() is AvailabilityState.AWAY


async def test_manual_override_auto_defers() -> None:
    source = ManualOverrideSource(redis_client=_FakeRedis(get_value="AUTO"))
    assert await source.query() is None


async def test_manual_override_missing_returns_none() -> None:
    source = ManualOverrideSource(redis_client=_FakeRedis(get_value=None))
    assert await source.query() is None


async def test_manual_override_redis_failure_returns_none() -> None:
    source = ManualOverrideSource(redis_client=_FakeRedis(raise_on_get=True))
    assert await source.query() is None


async def test_manual_override_decodes_bytes_payload() -> None:
    source = ManualOverrideSource(redis_client=_FakeRedis(get_value=b"AWAY"))
    assert await source.query() is AvailabilityState.AWAY


# ---------------------------------------------------------------------------
# HeartbeatSource
# ---------------------------------------------------------------------------


async def test_heartbeat_present_returns_available() -> None:
    source = HeartbeatSource(redis_client=_FakeRedis(exists_value=1))
    assert await source.query() is AvailabilityState.AVAILABLE


async def test_heartbeat_absent_returns_none() -> None:
    source = HeartbeatSource(redis_client=_FakeRedis(exists_value=0))
    assert await source.query() is None


async def test_heartbeat_redis_failure_returns_none() -> None:
    source = HeartbeatSource(redis_client=_FakeRedis(raise_on_exists=True))
    assert await source.query() is None


# ---------------------------------------------------------------------------
# ActiveHoursSource
# ---------------------------------------------------------------------------


async def test_active_hours_within_window(monkeypatch: pytest.MonkeyPatch) -> None:
    fixed = datetime.fromisoformat("2026-05-04T14:30:00+02:00")
    monkeypatch.setattr(availability_module, "datetime", _FixedNowDatetime(fixed))
    source = ActiveHoursSource(start_hour=9, end_hour=21, timezone_name="Europe/Rome")
    assert await source.query() is AvailabilityState.AVAILABLE


async def test_active_hours_outside_window(monkeypatch: pytest.MonkeyPatch) -> None:
    fixed = datetime.fromisoformat("2026-05-04T03:00:00+02:00")
    monkeypatch.setattr(availability_module, "datetime", _FixedNowDatetime(fixed))
    source = ActiveHoursSource(start_hour=9, end_hour=21, timezone_name="Europe/Rome")
    assert await source.query() is AvailabilityState.AWAY


async def test_active_hours_invalid_timezone_returns_none() -> None:
    source = ActiveHoursSource(timezone_name="Not/A/Real/Zone")
    assert await source.query() is None


# ---------------------------------------------------------------------------
# is_operator_available composition
# ---------------------------------------------------------------------------


class _StaticSource:
    """SignalSource that returns a pinned verdict (or raises) for tests."""

    def __init__(
        self,
        name: str,
        verdict: AvailabilityState | None = None,
        *,
        raises: bool = False,
    ) -> None:
        self.name = name
        self._verdict = verdict
        self._raises = raises

    async def query(self) -> AvailabilityState | None:
        if self._raises:
            raise RuntimeError("boom")
        return self._verdict


async def test_composition_manual_overrides_others() -> None:
    sources = [
        _StaticSource("manual_override", AvailabilityState.AWAY),
        _StaticSource("heartbeat", AvailabilityState.AVAILABLE),
    ]
    assert await is_operator_available(sources) is AvailabilityState.AWAY


async def test_composition_manual_available_overrides_active_hours_away() -> None:
    sources = [
        _StaticSource("manual_override", AvailabilityState.AVAILABLE),
        _StaticSource("active_hours", AvailabilityState.AWAY),
    ]
    assert await is_operator_available(sources) is AvailabilityState.AVAILABLE


async def test_composition_available_wins_among_others() -> None:
    sources = [
        _StaticSource("manual_override", None),
        _StaticSource("heartbeat", None),
        _StaticSource("active_hours", AvailabilityState.AVAILABLE),
    ]
    assert await is_operator_available(sources) is AvailabilityState.AVAILABLE


async def test_composition_away_when_only_away_signals() -> None:
    sources = [
        _StaticSource("manual_override", None),
        _StaticSource("active_hours", AvailabilityState.AWAY),
    ]
    assert await is_operator_available(sources) is AvailabilityState.AWAY


async def test_composition_default_available_when_all_defer() -> None:
    sources = [
        _StaticSource("manual_override", None),
        _StaticSource("heartbeat", None),
    ]
    assert await is_operator_available(sources) is AvailabilityState.AVAILABLE


async def test_composition_failure_safe_when_all_raise() -> None:
    sources = [
        _StaticSource("manual_override", raises=True),
        _StaticSource("heartbeat", raises=True),
        _StaticSource("active_hours", raises=True),
    ]
    assert await is_operator_available(sources) is AvailabilityState.AVAILABLE


async def test_composition_failure_overrides_away_signal() -> None:
    """A source raising must not let another source's AWAY win.

    If Redis is down (manual + heartbeat raise) while ActiveHoursSource
    legitimately returns AWAY, returning AWAY would pause coder work
    because of an observability outage. Failure-safe policy requires
    AVAILABLE in this case.
    """
    sources = [
        _StaticSource("manual_override", raises=True),
        _StaticSource("heartbeat", raises=True),
        _StaticSource("active_hours", AvailabilityState.AWAY),
    ]
    assert await is_operator_available(sources) is AvailabilityState.AVAILABLE


async def test_composition_manual_override_still_wins_when_other_source_raises() -> (
    None
):
    """Failure-safe must not override an explicit operator manual decision."""
    sources = [
        _StaticSource("manual_override", AvailabilityState.AWAY),
        _StaticSource("heartbeat", raises=True),
    ]
    assert await is_operator_available(sources) is AvailabilityState.AWAY
