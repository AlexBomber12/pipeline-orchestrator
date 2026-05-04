"""Operator availability signal sources - Cancellation policy v1.

Protocol-driven design lets future sources (calendar, Slack presence,
phone DnD) plug in without changing composition logic. Three v1
sources cover the common cases. Failure-safe by design: any source
exception treated as AVAILABLE.

PR-255.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Protocol
from zoneinfo import ZoneInfo


class AvailabilityState(str, Enum):
    AVAILABLE = "AVAILABLE"
    AWAY = "AWAY"


class SignalSource(Protocol):
    """Source of an availability signal."""

    name: str

    async def query(self) -> AvailabilityState | None:
        """Return signal or None to defer.

        None = "I have no opinion, defer to other sources".
        AVAILABLE = "I am sure operator is available".
        AWAY = "I am sure operator is unavailable".
        """
        ...


@dataclass
class ManualOverrideSource:
    """Operator-set 3-state override.

    Reads ``operator_override`` Redis key:
    - "AVAILABLE" -> AvailabilityState.AVAILABLE
    - "AWAY" -> AvailabilityState.AWAY
    - "AUTO" or missing -> None (defer)
    """

    redis_client: Any = None
    name: str = "manual_override"

    async def query(self) -> AvailabilityState | None:
        try:
            raw = await self.redis_client.get("operator_override")
        except Exception:
            return None
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        if raw == "AVAILABLE":
            return AvailabilityState.AVAILABLE
        if raw == "AWAY":
            return AvailabilityState.AWAY
        return None


@dataclass
class HeartbeatSource:
    """Dashboard visit heartbeat with TTL.

    Reads ``operator_heartbeat`` Redis key (set by dashboard middleware
    on each authenticated request, TTL N minutes). Present = AVAILABLE,
    absent = None (defer rather than declaring AWAY, since absence
    could be legitimate AVAILABLE-but-not-clicking).
    """

    redis_client: Any = None
    name: str = "heartbeat"

    async def query(self) -> AvailabilityState | None:
        try:
            present = await self.redis_client.exists("operator_heartbeat")
        except Exception:
            return None
        return AvailabilityState.AVAILABLE if present else None


@dataclass
class ActiveHoursSource:
    """Config-tunable active hours window per timezone."""

    start_hour: int = 9
    end_hour: int = 21
    timezone_name: str = "Europe/Rome"
    name: str = "active_hours"

    async def query(self) -> AvailabilityState | None:
        try:
            tz = ZoneInfo(self.timezone_name)
            now = datetime.now(tz=tz)
            if self.start_hour <= now.hour < self.end_hour:
                return AvailabilityState.AVAILABLE
            return AvailabilityState.AWAY
        except Exception:
            return None


async def is_operator_available(
    sources: list[SignalSource],
) -> AvailabilityState:
    """Compose multiple signal sources into a single availability verdict.

    Policy:
    - ManualOverrideSource takes precedence over all others (operator
      explicit will).
    - Among the rest, AVAILABLE wins if any source says AVAILABLE.
    - If any source raised, bias to AVAILABLE so an observability outage
      cannot let a single AWAY signal pause work (failure-safe).
    - Otherwise AWAY if any source says AWAY.
    - Default AVAILABLE (failure-safe).

    PR-255.
    """
    manual_verdict: AvailabilityState | None = None
    other_verdicts: list[AvailabilityState] = []
    any_failed = False

    for source in sources:
        try:
            verdict = await source.query()
        except Exception:
            any_failed = True
            continue
        if verdict is None:
            continue
        if source.name == "manual_override":
            manual_verdict = verdict
        else:
            other_verdicts.append(verdict)

    if manual_verdict is not None:
        return manual_verdict
    if AvailabilityState.AVAILABLE in other_verdicts:
        return AvailabilityState.AVAILABLE
    if any_failed:
        return AvailabilityState.AVAILABLE
    if AvailabilityState.AWAY in other_verdicts:
        return AvailabilityState.AWAY
    return AvailabilityState.AVAILABLE
