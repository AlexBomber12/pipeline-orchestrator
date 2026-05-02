"""GitHub API rate-limit budget readers.

Owns the ``gh api rate_limit`` data-source path: parses the REST/core and
GraphQL buckets into :class:`RateLimitBudget` instances. The throttling
policy that decides what to do with the budget lives in the daemon
runner.
"""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone

from src.daemon.github_rate_limit import RateLimitBudget, read_budget
from src.github import gh_runner


async def get_current_rate_limit_budget(
    redis_client: object,
) -> RateLimitBudget | None:
    """Return the most recent GitHub API rate-limit budget from Redis.

    Returns ``None`` when no observation has been persisted yet (daemon
    just started, the rate_limit fetch failed, or Redis is unavailable).
    Callers treat ``None`` as "no data, proceed normally".
    """
    return await read_budget(redis_client)


def fetch_rate_limit_buckets() -> tuple[RateLimitBudget | None, RateLimitBudget | None]:
    """Fetch ``gh api rate_limit`` and return ``(rest_core, graphql)`` buckets.

    Either bucket may be ``None`` when the bucket is missing or its payload
    is malformed. Returns ``(None, None)`` if the gh CLI call fails or the
    response itself is unparseable so callers can treat the result as
    "no data" without distinguishing failure modes.
    """
    try:
        raw = gh_runner.run_gh(
            [
                "api",
                "rate_limit",
                "--jq",
                "{core: .resources.core, graphql: .resources.graphql}",
            ]
        )
    except (RuntimeError, subprocess.TimeoutExpired, OSError):
        return None, None
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return None, None
    if not isinstance(raw, dict):
        return None, None
    core = _parse_rate_limit_bucket(raw.get("core"))
    graphql = _parse_rate_limit_bucket(raw.get("graphql"))
    return core, graphql


def fetch_rate_limit_budget() -> RateLimitBudget | None:
    """Fetch ``gh api rate_limit`` and return a parsed :class:`RateLimitBudget`.

    Returns the more constrained of the REST/core and GraphQL buckets so the
    daemon throttles before either is exhausted. Hot-path polling here uses
    GraphQL-heavy ``gh`` commands (``gh pr list --json …``), which consume
    GraphQL points independently of the REST/core bucket; tracking only
    ``rate.*`` (== ``resources.core``) would let GraphQL exhaustion slip
    through. Returns ``None`` if the gh CLI call fails or returns an
    unparseable payload — callers treat that as "no data".
    """
    core, graphql = fetch_rate_limit_buckets()
    candidates = [b for b in (core, graphql) if b is not None]
    if not candidates:
        return None
    return min(candidates, key=lambda b: b.remaining_percent)


def _parse_rate_limit_bucket(raw: object) -> RateLimitBudget | None:
    """Parse one ``resources.<bucket>`` entry from ``gh api rate_limit``."""
    if not isinstance(raw, dict):
        return None
    try:
        remaining = int(raw["remaining"])
        limit = int(raw["limit"])
        reset_ts = int(raw["reset"])
    except (KeyError, TypeError, ValueError):
        return None
    return RateLimitBudget(
        installation_id=None,
        remaining=remaining,
        limit=limit,
        reset_at=datetime.fromtimestamp(reset_ts, tz=timezone.utc),
    )
