"""GitHub API rate-limit budget tracking.

Reads ``x-ratelimit-*`` headers (or the ``rate_limit`` REST endpoint
payload) into a typed :class:`RateLimitBudget`, persists the latest
observation to Redis, and provides helpers the daemon's poll loop uses
to adapt polling cadence to the remaining installation budget.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

logger = logging.getLogger(__name__)

#: Single Redis key used by the dashboard and poll loop. The daemon
#: writes the most recent observation here regardless of ``installation_id``
#: because the dashboard does not know which installation backs each repo;
#: a single budget per gh-CLI auth is the operational unit anyway.
BUDGET_REDIS_KEY = "github_rate_limit_budget"


@dataclass(frozen=True)
class RateLimitBudget:
    """Snapshot of an installation's remaining GitHub API requests."""

    installation_id: str | None
    remaining: int
    limit: int
    reset_at: datetime

    @property
    def remaining_percent(self) -> float:
        if self.limit <= 0:
            return 100.0
        return (self.remaining / self.limit) * 100.0

    @classmethod
    def from_headers(
        cls,
        headers: Mapping[str, str],
        installation_id: str | None = None,
    ) -> "RateLimitBudget":
        lower = {str(k).lower(): v for k, v in headers.items()}
        remaining = _coerce_int(lower.get("x-ratelimit-remaining"), default=5000)
        limit = _coerce_int(lower.get("x-ratelimit-limit"), default=5000)
        reset_ts = _coerce_int(lower.get("x-ratelimit-reset"), default=0)
        return cls(
            installation_id=installation_id,
            remaining=remaining,
            limit=limit,
            reset_at=datetime.fromtimestamp(reset_ts, tz=timezone.utc),
        )

    def to_redis_payload(self) -> str:
        return json.dumps(
            {
                "installation_id": self.installation_id,
                "remaining": self.remaining,
                "limit": self.limit,
                "reset_at": int(self.reset_at.timestamp()),
            }
        )

    @classmethod
    def from_redis_payload(cls, raw: str) -> "RateLimitBudget | None":
        try:
            data = json.loads(raw)
        except (TypeError, ValueError):
            return None
        if not isinstance(data, dict):
            return None
        try:
            return cls(
                installation_id=data.get("installation_id"),
                remaining=int(data["remaining"]),
                limit=int(data["limit"]),
                reset_at=datetime.fromtimestamp(
                    int(data["reset_at"]), tz=timezone.utc
                ),
            )
        except (KeyError, TypeError, ValueError):
            return None


def _coerce_int(value: object, *, default: int) -> int:
    """Return ``int(value)`` or ``default`` for missing/malformed input."""
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


async def read_budget(redis_client: Any) -> RateLimitBudget | None:
    """Return the most recent budget observation, or ``None`` if absent."""
    if redis_client is None:
        return None
    try:
        raw = await redis_client.get(BUDGET_REDIS_KEY)
    except Exception:
        return None
    if not raw:
        return None
    return RateLimitBudget.from_redis_payload(raw)


async def write_budget(redis_client: Any, budget: RateLimitBudget) -> None:
    """Persist ``budget`` for dashboard and cross-runner readers."""
    if redis_client is None:
        return
    try:
        await redis_client.set(BUDGET_REDIS_KEY, budget.to_redis_payload())
    except Exception:
        logger.warning("Failed to persist GitHub API budget", exc_info=True)
