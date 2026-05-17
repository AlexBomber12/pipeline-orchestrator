"""Typed inhibitor model for the 8 throttle mechanisms.

Single source of truth for what blocks daemon dispatch. Replaces
scattered if-branches in runner.py with a uniform typed list of active
inhibitors per repo. Consumer wiring lands in PR-327..PR-330.
"""

from __future__ import annotations

import json
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
    derives from the existing equivalents: ``rate_limited_coder_until``
    and ``usage_session_percent``/``usage_weekly_percent`` against the
    configured caps. ``ERROR_RATE_AUTO_PAUSE`` derivation is deferred:
    ``mark_auto_pause`` writes ``error_rate_last_auto_pause:<repo>`` once
    and nothing clears it on Resume, so the historical marker cannot
    distinguish an active auto-pause from a manual pause that happened
    after an earlier auto-resume. The inhibitor will be emitted once a
    dedicated ``state.error_rate_auto_paused_at`` field lands.
    """
    # Imported lazily to keep ``src.inhibitor`` free of runtime
    # dependencies on ``src.daemon`` for non-derivation callers.
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
    # Mirror ``runner._pop_stop_request``: it gates dispatch on ``GET``
    # returning a truthy payload, returning ``False`` on read errors or a
    # falsy value (e.g. an empty string). Reading the TTL alone is not
    # enough — a present-but-empty key would stop the inhibitor list and
    # the dispatcher's blocking decision from agreeing once consumers wire
    # this helper as a source of truth. The TTL is read only after a
    # truthy GET so we can populate ``expires_at``; TTL == -1 (no expiry)
    # and TTL == 0 (final sub-second window) both surface as inhibitors
    # without an ``expires_at`` so they cannot be mistaken for stale.
    try:
        raw_stop = await redis.get(stop_key)
    except Exception:
        raw_stop = None
    if raw_stop:
        try:
            stop_ttl = await redis.ttl(stop_key)
        except Exception:
            stop_ttl = None
        expires_at: Optional[datetime] = None
        if stop_ttl is not None and stop_ttl > 0:
            expires_at = current + timedelta(seconds=int(stop_ttl))
        inhibitors.append(
            WorkInhibitor(
                inhibitor_type=InhibitorType.USER_STOP,
                expires_at=expires_at,
                reason_text="Operator requested stop",
                source_key=stop_key,
            )
        )

    # Mirror ``selector._is_rate_limited``: the typed per-coder dict is the
    # primary source, but the legacy global expiry, reactive marker, and
    # ``rate_limited_coders`` set still gate dispatch on upgraded repos
    # whose persisted state predates ``rate_limited_coder_until``. PR-328
    # retires the legacy fields once dispatcher migration completes.
    rate_limited_seen: set[str] = set()
    for coder, until in state.rate_limited_coder_until.items():
        # Presence of a typed entry short-circuits the selector even when
        # the expiry has elapsed (selector returns ``False`` without
        # consulting the legacy fields). Mark the coder seen unconditionally
        # so a stale typed entry does not let legacy branches resurrect a
        # spurious ``RATE_LIMIT`` inhibitor here either.
        rate_limited_seen.add(coder)
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

    legacy_until = state.rate_limited_until
    legacy_until_aware: Optional[datetime] = None
    if legacy_until is not None:
        legacy_until_aware = (
            legacy_until.replace(tzinfo=timezone.utc)
            if legacy_until.tzinfo is None
            else legacy_until
        )

    reactive_coder = state.rate_limit_reactive_coder
    if (
        legacy_until_aware is not None
        and reactive_coder is None
        and legacy_until_aware > current
        and "claude" not in rate_limited_seen
    ):
        rate_limited_seen.add("claude")
        inhibitors.append(
            WorkInhibitor(
                inhibitor_type=InhibitorType.RATE_LIMIT,
                coder_affected="claude",
                expires_at=legacy_until_aware,
                reason_text="claude rate-limited",
                source_key=f"state:{state.name}.rate_limited_until",
            )
        )

    if reactive_coder is not None and reactive_coder not in rate_limited_seen:
        rate_limited_seen.add(reactive_coder)
        inhibitors.append(
            WorkInhibitor(
                inhibitor_type=InhibitorType.RATE_LIMIT,
                coder_affected=reactive_coder,
                reason_text=f"{reactive_coder} rate-limited",
                source_key=f"state:{state.name}.rate_limit_reactive_coder",
            )
        )

    for coder in state.rate_limited_coders:
        if coder in rate_limited_seen:
            continue
        rate_limited_seen.add(coder)
        inhibitors.append(
            WorkInhibitor(
                inhibitor_type=InhibitorType.RATE_LIMIT,
                coder_affected=coder,
                reason_text=f"{coder} rate-limited",
                source_key=f"state:{state.name}.rate_limited_coders",
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
    # ``RateLimitBudget.reset_at`` is always UTC-aware from both
    # ``from_headers`` and ``from_redis_payload`` factories, so no tz
    # normalisation is required here. Mirror
    # ``runner._check_github_api_budget`` which gates both threshold
    # branches on ``now < budget.reset_at``: stale snapshots whose reset
    # has elapsed never throttle the runner, so they must not surface as
    # active inhibitors either.
    if budget is not None and current < budget.reset_at:
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
    # Mirror ``cascade_monitor.check_cascade_escalate_state``: a present key
    # is not sufficient — the daemon only treats panic as active when the
    # JSON payload parses to a dict with ``enabled=True``. Stale, disabled,
    # or malformed records leave dispatch unblocked, so they must not
    # surface as CASCADE_PANIC inhibitors here either.
    raw_panic = await redis.get(panic_key)
    if raw_panic is not None:
        try:
            parsed_panic = json.loads(raw_panic)
        except (TypeError, ValueError):
            parsed_panic = None
        if isinstance(parsed_panic, dict) and parsed_panic.get("enabled"):
            inhibitors.append(
                WorkInhibitor(
                    inhibitor_type=InhibitorType.CASCADE_PANIC,
                    reason_text="Cascade panic mode auto-stop",
                    source_key=panic_key,
                )
            )

    # ``ERROR_RATE_AUTO_PAUSE`` is intentionally not derived here. The only
    # signal currently available is the ``error_rate_last_auto_pause:<repo>``
    # Redis key written by ``mark_auto_pause``; nothing clears it on Resume,
    # so combining it with ``state.user_paused`` would misclassify a later
    # operator-initiated pause as an auto-pause whenever the repo had ever
    # auto-paused in the past. Emitting this inhibitor requires a signal
    # tied to the live pause (e.g. a future ``state.error_rate_auto_paused_at``
    # field cleared on Resume); until that lands, omit the entry rather than
    # publish inaccurate inhibitor semantics to UI/automation consumers.

    return inhibitors
