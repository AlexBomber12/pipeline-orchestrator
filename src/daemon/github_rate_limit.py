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

#: Per-bucket Redis keys. The daemon throttles on the constrained min via
#: ``BUDGET_REDIS_KEY``; the dashboard renders REST and GraphQL separately
#: so an operator can see *which* surface is near exhaustion.
BUDGET_REST_REDIS_KEY = "github_rate_limit_budget_rest"
BUDGET_GRAPHQL_REDIS_KEY = "github_rate_limit_budget_graphql"

#: Cross-runner refresh-lock key. Set with ``NX`` + ``EX`` so only one runner
#: per TTL window probes ``gh api rate_limit``; the rest read the result via
#: ``read_budget``. Without this, probe traffic scales linearly with repo
#: count and can itself exhaust the rate limit it is meant to protect.
REFRESH_LOCK_REDIS_KEY = "github_rate_limit_refresh_lock"

#: Per-repo Redis list of recent GraphQL points burned per polling cycle.
#: One key per repo so the list survives daemon restarts and stays bounded
#: independently from the global budget snapshot.
BURNS_REDIS_KEY_PREFIX = "github_rate_limit_burns:"

#: Default cap on the per-repo cycle-burn list. Twenty entries spans roughly
#: twenty minutes of polling at the default cadence — enough to read the
#: trend after a config change without unbounded memory growth.
BURNS_MAX_ENTRIES = 20


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


async def _read_budget_at(redis_client: Any, key: str) -> RateLimitBudget | None:
    if redis_client is None:
        return None
    try:
        raw = await redis_client.get(key)
    except Exception:
        return None
    if not raw:
        return None
    return RateLimitBudget.from_redis_payload(raw)


async def _write_budget_at(
    redis_client: Any, budget: RateLimitBudget, key: str
) -> None:
    if redis_client is None:
        return
    try:
        await redis_client.set(key, budget.to_redis_payload())
    except Exception:
        logger.warning("Failed to persist GitHub API budget at %s", key, exc_info=True)


async def read_budget(redis_client: Any) -> RateLimitBudget | None:
    """Return the most recent budget observation, or ``None`` if absent."""
    return await _read_budget_at(redis_client, BUDGET_REDIS_KEY)


async def write_budget(redis_client: Any, budget: RateLimitBudget) -> None:
    """Persist ``budget`` for dashboard and cross-runner readers."""
    await _write_budget_at(redis_client, budget, BUDGET_REDIS_KEY)


async def read_rest_budget(redis_client: Any) -> RateLimitBudget | None:
    """Return the most recent REST/core bucket snapshot, or ``None``."""
    return await _read_budget_at(redis_client, BUDGET_REST_REDIS_KEY)


async def write_rest_budget(redis_client: Any, budget: RateLimitBudget) -> None:
    """Persist the REST/core bucket so the dashboard can render it alone."""
    await _write_budget_at(redis_client, budget, BUDGET_REST_REDIS_KEY)


async def read_graphql_budget(redis_client: Any) -> RateLimitBudget | None:
    """Return the most recent GraphQL bucket snapshot, or ``None``."""
    return await _read_budget_at(redis_client, BUDGET_GRAPHQL_REDIS_KEY)


async def write_graphql_budget(redis_client: Any, budget: RateLimitBudget) -> None:
    """Persist the GraphQL bucket so the dashboard can render it alone."""
    await _write_budget_at(redis_client, budget, BUDGET_GRAPHQL_REDIS_KEY)


async def try_claim_refresh_lock(redis_client: Any, ttl_seconds: int) -> bool:
    """Atomically claim the right to refresh the budget for ``ttl_seconds``.

    Returns ``True`` when the caller should perform ``fetch_rate_limit_budget``
    and persist the result via :func:`write_budget`; ``False`` when another
    runner already holds the lock and the caller should fall back to reading
    the most recent observation via :func:`read_budget`. Returns ``True`` when
    Redis is unavailable so a single-runner setup keeps refreshing normally.
    """
    if redis_client is None:
        return True
    try:
        result = await redis_client.set(
            REFRESH_LOCK_REDIS_KEY, "1", nx=True, ex=ttl_seconds
        )
    except Exception:
        return True
    return bool(result)


async def record_cycle_burn(
    redis_client: Any,
    repo_name: str,
    delta: int,
    *,
    max_entries: int = BURNS_MAX_ENTRIES,
) -> None:
    """Persist ``delta`` to the bounded recent-burn list for ``repo_name``.

    ``delta`` is the GraphQL points consumed during one polling cycle,
    derived from ``budget_before.remaining - budget_after.remaining``.
    Negative values (the rate-limit window reset between observations) and
    non-integer inputs are normalised to ``0`` so the metric never drives
    operators to spurious decisions. The Redis list is trimmed to
    ``max_entries`` entries to bound memory. Failures are swallowed:
    observability code must never crash the runner.
    """
    if redis_client is None:
        return
    try:
        normalized = max(0, int(delta))
    except (TypeError, ValueError):
        normalized = 0
    key = f"{BURNS_REDIS_KEY_PREFIX}{repo_name}"
    try:
        await redis_client.lpush(key, str(normalized))
        await redis_client.ltrim(key, 0, max_entries - 1)
    except Exception:
        logger.warning(
            "Failed to record GraphQL cycle burn for %s", repo_name, exc_info=True
        )


async def recent_cycle_burns(
    redis_client: Any,
    repo_name: str,
    *,
    max_entries: int = BURNS_MAX_ENTRIES,
) -> list[int]:
    """Return the last ``max_entries`` cycle deltas (newest-first), or ``[]``.

    Malformed entries are skipped so a single corrupted payload cannot blank
    the metric for the operator.
    """
    if redis_client is None:
        return []
    key = f"{BURNS_REDIS_KEY_PREFIX}{repo_name}"
    try:
        raw = await redis_client.lrange(key, 0, max_entries - 1)
    except Exception:
        return []
    result: list[int] = []
    for item in raw or []:
        try:
            result.append(max(0, int(item)))
        except (TypeError, ValueError):
            continue
    return result


async def release_refresh_lock(redis_client: Any) -> None:
    """Release the refresh lock so another runner can probe immediately.

    Called when the lock holder failed to obtain a snapshot (for example a
    transient ``gh api rate_limit`` failure). Holding the lock for the full
    TTL after a failed probe would silently disable rate-limit protection
    during exactly the conditions the protection is meant to cover, since
    every runner would see ``read_budget`` return ``None`` and proceed
    normally for up to ``ttl_seconds``.
    """
    if redis_client is None:
        return
    try:
        await redis_client.delete(REFRESH_LOCK_REDIS_KEY)
    except Exception:
        logger.warning(
            "Failed to release GitHub API rate-limit refresh lock", exc_info=True
        )
