"""Typed inhibitor model for the 8 throttle mechanisms.

Single source of truth for what blocks daemon dispatch. Replaces
scattered if-branches in runner.py with a uniform typed list of active
inhibitors per repo. Consumer wiring lands in PR-327..PR-330.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


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
