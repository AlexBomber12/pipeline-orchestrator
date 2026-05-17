"""Typed inhibitor model for the 8 throttle mechanisms.

Single source of truth for what blocks daemon dispatch. Replaces
scattered if-branches in runner.py with a uniform typed list of active
inhibitors per repo. Consumer wiring lands in PR-327..PR-330.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, Optional

from pydantic import BaseModel, Field, field_validator

if TYPE_CHECKING:
    from src.config import DaemonConfig
    from src.models import RepoState


class InhibitorType(str, Enum):
    USER_PAUSE = "user_pause"
    USER_STOP = "user_stop"
    RATE_LIMIT = "rate_limit"
    SPEND_CEILING = "spend_ceiling"
    GITHUB_BUDGET_PAUSE = "github_budget_pause"
    GITHUB_BUDGET_SLOWDOWN = "github_budget_slowdown"
    CASCADE_PANIC = "cascade_panic"
    ERROR_RATE_AUTO_PAUSE = "error_rate_auto_pause"


class WorkInhibitor(BaseModel):
    inhibitor_type: InhibitorType
    coder_affected: Optional[str] = None
    expires_at: Optional[datetime] = None
    reason_text: str = ""
    source_key: str = Field(
        description="Redis key from which this inhibitor was derived."
    )

    @field_validator("expires_at")
    @classmethod
    def _normalize_expires_at_to_utc(
        cls, value: Optional[datetime]
    ) -> Optional[datetime]:
        # Naive datetimes (e.g. from ISO strings without offset) would raise
        # TypeError when compared against the aware UTC `now` used below.
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @staticmethod
    def _coerce_now_to_utc(now: Optional[datetime]) -> datetime:
        # Naive `now` from callers using datetime.utcnow() would crash the
        # comparison/arithmetic below because expires_at is UTC-aware.
        if now is None:
            return datetime.now(timezone.utc)
        if now.tzinfo is None:
            return now.replace(tzinfo=timezone.utc)
        return now.astimezone(timezone.utc)

    def is_blocking_now(self, now: Optional[datetime] = None) -> bool:
        """Return True if expires_at is None or in the future."""
        if self.expires_at is None:
            return True
        return self.expires_at > self._coerce_now_to_utc(now)

    def time_remaining_seconds(self, now: Optional[datetime] = None) -> Optional[float]:
        """Return seconds until expires_at, None if no expiry."""
        if self.expires_at is None:
            return None
        delta = self.expires_at - self._coerce_now_to_utc(now)
        return max(0.0, delta.total_seconds())

    def is_per_coder(self) -> bool:
        """Return True if this inhibitor affects a specific coder only."""
        return self.coder_affected is not None

    class Config:
        frozen = True


async def derive_active_inhibitors(
    state: "RepoState",
    redis: Any,
    cfg: "DaemonConfig",
    now: Optional[datetime] = None,
) -> list[WorkInhibitor]:
    """Walk current Redis + state, return typed list of active inhibitors.

    Read-only derivation across the eight throttle mechanisms tracked by
    the daemon today. Mirrors the legacy if-branches in ``runner.py`` and
    the per-handler checks without changing any state.

    PR-327 introduces this helper with no callers; PR-328 wires it into
    ``publish_state`` via the new ``RepoState.active_inhibitors`` field
    and PR-329..PR-330 migrate the dispatcher to consume the list.

    The spec ships a skeleton against future ``RepoState`` fields
    (``rate_limited_until_by_coder``, ``spend_ceiling_session_pct``,
    ``error_rate_auto_paused_at``). Until those fields land, this PR
    derives from the existing equivalents: ``rate_limited_coder_until``,
    ``usage_session_percent``/``usage_weekly_percent`` against the
    configured caps, and the ``error_rate_last_auto_pause:`` Redis key.
    The PR description records this so PR-328's field landing can
    swap-in cleanly without a behaviour change.
    """
    # Imported lazily to keep ``src.inhibitor`` free of runtime
    # dependencies on ``src.daemon`` for non-derivation callers.
    from src.daemon.error_rate_tracker import last_auto_pause_key
    from src.daemon.github_rate_limit import BUDGET_REDIS_KEY, read_budget
    from src.keyspace import control_stop, daemon_panic_state

    current = now if now is not None else datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    inhibitors: list[WorkInhibitor] = []

    if state.user_paused:
        inhibitors.append(
            WorkInhibitor(
                inhibitor_type=InhibitorType.USER_PAUSE,
                reason_text="Operator paused",
                source_key=f"state:{state.name}.user_paused",
            )
        )

    stop_key = control_stop(state.name)
    stop_ttl = await redis.ttl(stop_key)
    if stop_ttl is not None and stop_ttl > 0:
        inhibitors.append(
            WorkInhibitor(
                inhibitor_type=InhibitorType.USER_STOP,
                expires_at=current + timedelta(seconds=int(stop_ttl)),
                reason_text="Operator requested stop",
                source_key=stop_key,
            )
        )

    for coder, until in state.rate_limited_coder_until.items():
        until_aware = (
            until.replace(tzinfo=timezone.utc) if until.tzinfo is None else until
        )
        if until_aware > current:
            inhibitors.append(
                WorkInhibitor(
                    inhibitor_type=InhibitorType.RATE_LIMIT,
                    coder_affected=coder,
                    expires_at=until_aware,
                    reason_text=f"{coder} rate-limited",
                    source_key=(
                        f"state:{state.name}.rate_limited_coder_until.{coder}"
                    ),
                )
            )

    session_cap = cfg.spend_ceiling_session_percent
    weekly_cap = cfg.spend_ceiling_weekly_percent
    session_pct = state.usage_session_percent or 0
    weekly_pct = state.usage_weekly_percent or 0
    session_breached = session_cap is not None and session_pct >= session_cap
    weekly_breached = weekly_cap is not None and weekly_pct >= weekly_cap
    if session_breached or weekly_breached:
        inhibitors.append(
            WorkInhibitor(
                inhibitor_type=InhibitorType.SPEND_CEILING,
                reason_text=(
                    f"Spend ceiling reached "
                    f"(session={session_pct}%, weekly={weekly_pct}%)"
                ),
                source_key=f"state:{state.name}.usage_*_percent",
            )
        )

    budget = await read_budget(redis)
    if budget is not None:
        github_pct = budget.remaining_percent
        if github_pct < cfg.github_api_pause_threshold_percent:
            inhibitors.append(
                WorkInhibitor(
                    inhibitor_type=InhibitorType.GITHUB_BUDGET_PAUSE,
                    expires_at=budget.reset_at,
                    reason_text=f"GitHub budget at {github_pct:.0f}%",
                    source_key=BUDGET_REDIS_KEY,
                )
            )
        elif github_pct < cfg.github_api_slowdown_threshold_percent:
            inhibitors.append(
                WorkInhibitor(
                    inhibitor_type=InhibitorType.GITHUB_BUDGET_SLOWDOWN,
                    expires_at=budget.reset_at,
                    reason_text=(
                        f"GitHub budget at {github_pct:.0f}%, slowdown active"
                    ),
                    source_key=BUDGET_REDIS_KEY,
                )
            )

    panic_key = daemon_panic_state()
    if await redis.exists(panic_key):
        inhibitors.append(
            WorkInhibitor(
                inhibitor_type=InhibitorType.CASCADE_PANIC,
                reason_text="Cascade panic mode auto-stop",
                source_key=panic_key,
            )
        )

    error_rate_key = last_auto_pause_key(state.name)
    if await redis.exists(error_rate_key):
        inhibitors.append(
            WorkInhibitor(
                inhibitor_type=InhibitorType.ERROR_RATE_AUTO_PAUSE,
                reason_text="Auto-paused due to ERROR rate threshold",
                source_key=error_rate_key,
            )
        )

    return inhibitors
