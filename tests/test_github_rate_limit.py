"""Tests for src/daemon/github_rate_limit.py."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from src.daemon.github_rate_limit import (
    BUDGET_REDIS_KEY,
    REFRESH_LOCK_REDIS_KEY,
    RateLimitBudget,
    read_budget,
    try_claim_refresh_lock,
    write_budget,
)


def _budget(**overrides: Any) -> RateLimitBudget:
    base = {
        "installation_id": "abc",
        "remaining": 4500,
        "limit": 5000,
        "reset_at": datetime(2026, 4, 26, 18, 0, tzinfo=timezone.utc),
    }
    base.update(overrides)
    return RateLimitBudget(**base)  # type: ignore[arg-type]


def test_remaining_percent_basic() -> None:
    budget = _budget(remaining=2500, limit=5000)
    assert budget.remaining_percent == 50.0


def test_remaining_percent_zero_limit_returns_full() -> None:
    budget = _budget(remaining=0, limit=0)
    assert budget.remaining_percent == 100.0


def test_remaining_percent_negative_limit_returns_full() -> None:
    budget = _budget(remaining=10, limit=-1)
    assert budget.remaining_percent == 100.0


def test_from_headers_happy_path() -> None:
    headers = {
        "x-ratelimit-remaining": "4321",
        "x-ratelimit-limit": "5000",
        "x-ratelimit-reset": "1745683200",
    }
    budget = RateLimitBudget.from_headers(headers, installation_id="xyz")
    assert budget.installation_id == "xyz"
    assert budget.remaining == 4321
    assert budget.limit == 5000
    assert budget.reset_at == datetime.fromtimestamp(1745683200, tz=timezone.utc)


def test_from_headers_is_case_insensitive() -> None:
    headers = {
        "X-RateLimit-Remaining": "10",
        "X-RateLimit-Limit": "5000",
        "X-RateLimit-Reset": "1745683200",
    }
    budget = RateLimitBudget.from_headers(headers)
    assert budget.remaining == 10
    assert budget.installation_id is None


def test_from_headers_missing_headers_default_to_5000() -> None:
    budget = RateLimitBudget.from_headers({})
    assert budget.remaining == 5000
    assert budget.limit == 5000
    assert budget.reset_at == datetime.fromtimestamp(0, tz=timezone.utc)


def test_from_headers_malformed_values_default_safely() -> None:
    headers = {
        "x-ratelimit-remaining": "not-an-int",
        "x-ratelimit-limit": "",
        "x-ratelimit-reset": None,
    }
    budget = RateLimitBudget.from_headers(headers)
    assert budget.remaining == 5000
    assert budget.limit == 5000
    assert budget.reset_at == datetime.fromtimestamp(0, tz=timezone.utc)


def test_to_and_from_redis_payload_roundtrip() -> None:
    original = _budget(remaining=42, limit=5000)
    decoded = RateLimitBudget.from_redis_payload(original.to_redis_payload())
    assert decoded == original


def test_from_redis_payload_returns_none_for_invalid_json() -> None:
    assert RateLimitBudget.from_redis_payload("not-json") is None


def test_from_redis_payload_returns_none_for_non_dict() -> None:
    assert RateLimitBudget.from_redis_payload("[1, 2, 3]") is None


def test_from_redis_payload_returns_none_for_missing_keys() -> None:
    assert RateLimitBudget.from_redis_payload('{"remaining": 1}') is None


def test_from_redis_payload_returns_none_for_malformed_int() -> None:
    payload = '{"remaining": "abc", "limit": 5000, "reset_at": 0}'
    assert RateLimitBudget.from_redis_payload(payload) is None


class _FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.set_failure = False
        self.get_failure = False

    async def set(
        self,
        key: str,
        value: str,
        ex: int | None = None,
        nx: bool = False,
    ) -> bool | None:
        if self.set_failure:
            raise RuntimeError("redis offline")
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True

    async def get(self, key: str) -> str | None:
        if self.get_failure:
            raise RuntimeError("redis offline")
        return self.store.get(key)


def test_write_and_read_budget_roundtrip() -> None:
    redis = _FakeRedis()
    budget = _budget()
    asyncio.run(write_budget(redis, budget))
    assert BUDGET_REDIS_KEY in redis.store
    loaded = asyncio.run(read_budget(redis))
    assert loaded == budget


def test_read_budget_returns_none_when_redis_is_none() -> None:
    assert asyncio.run(read_budget(None)) is None


def test_read_budget_returns_none_when_no_observation() -> None:
    redis = _FakeRedis()
    assert asyncio.run(read_budget(redis)) is None


def test_read_budget_returns_none_when_redis_get_raises() -> None:
    redis = _FakeRedis()
    redis.get_failure = True
    assert asyncio.run(read_budget(redis)) is None


def test_read_budget_returns_none_on_corrupt_payload() -> None:
    redis = _FakeRedis()
    redis.store[BUDGET_REDIS_KEY] = "not-json"
    assert asyncio.run(read_budget(redis)) is None


def test_write_budget_no_op_when_redis_is_none() -> None:
    asyncio.run(write_budget(None, _budget()))


def test_write_budget_swallows_redis_failure() -> None:
    redis = _FakeRedis()
    redis.set_failure = True
    asyncio.run(write_budget(redis, _budget()))
    assert BUDGET_REDIS_KEY not in redis.store


def test_try_claim_refresh_lock_first_caller_wins() -> None:
    redis = _FakeRedis()
    assert asyncio.run(try_claim_refresh_lock(redis, ttl_seconds=60)) is True
    assert REFRESH_LOCK_REDIS_KEY in redis.store
    # Second caller within the TTL window must be denied.
    assert asyncio.run(try_claim_refresh_lock(redis, ttl_seconds=60)) is False


def test_try_claim_refresh_lock_returns_true_when_redis_is_none() -> None:
    """No-redis deployments fall back to per-runner refreshes (single-runner)."""
    assert asyncio.run(try_claim_refresh_lock(None, ttl_seconds=60)) is True


def test_try_claim_refresh_lock_returns_true_on_redis_failure() -> None:
    """Redis hiccup must not block the rate-limit observation entirely."""
    redis = _FakeRedis()
    redis.set_failure = True
    assert asyncio.run(try_claim_refresh_lock(redis, ttl_seconds=60)) is True
