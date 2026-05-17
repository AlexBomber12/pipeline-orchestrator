"""Unit tests for ``derive_active_inhibitors`` (PR-327).

The function walks current Redis state plus :class:`RepoState` into a
typed list of currently-active :class:`WorkInhibitor` instances. It is
read-only and has no callers yet (PR-328 wires the first one).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.config import DaemonConfig
from src.daemon.error_rate_tracker import last_auto_pause_key
from src.daemon.github_rate_limit import BUDGET_REDIS_KEY, RateLimitBudget
from src.inhibitor import (
    InhibitorType,
    WorkInhibitor,
    derive_active_inhibitors,
)
from src.keyspace import control_stop, daemon_panic_state
from src.models import RepoState
from tests.runner._helpers import _FakeRedis


def _make_state(**overrides: object) -> RepoState:
    payload: dict[str, object] = {
        "url": "https://github.com/octo/demo",
        "name": "octo__demo",
    }
    payload.update(overrides)
    return RepoState(**payload)  # type: ignore[arg-type]


def _set_github_budget(
    redis: _FakeRedis, *, remaining_percent: float, reset_at: datetime | None = None
) -> None:
    """Persist a ``RateLimitBudget`` snapshot at the canonical key.

    ``remaining_percent`` is expressed as a 0..100 figure and converted
    into a ``remaining/limit`` pair so the derivation's
    ``budget.remaining_percent`` lookup matches the test parametrisation.
    """
    limit = 1000
    remaining = int(round(limit * remaining_percent / 100))
    when = reset_at if reset_at is not None else datetime(
        2030, 1, 1, tzinfo=timezone.utc
    )
    budget = RateLimitBudget(
        installation_id=None,
        remaining=remaining,
        limit=limit,
        reset_at=when,
    )
    redis.store[BUDGET_REDIS_KEY] = budget.to_redis_payload()


@pytest.mark.asyncio
async def test_derive_returns_empty_when_no_inhibitors_active() -> None:
    redis = _FakeRedis()
    cfg = DaemonConfig()
    state = _make_state()

    result = await derive_active_inhibitors(state, redis, cfg)

    assert result == []


@pytest.mark.asyncio
async def test_derive_returns_user_pause_when_state_user_paused() -> None:
    redis = _FakeRedis()
    cfg = DaemonConfig()
    state = _make_state(user_paused=True)

    result = await derive_active_inhibitors(state, redis, cfg)

    assert len(result) == 1
    assert result[0].inhibitor_type is InhibitorType.USER_PAUSE
    assert result[0].source_key == "state:octo__demo.user_paused"


@pytest.mark.asyncio
async def test_derive_returns_user_stop_when_control_stop_key_present() -> None:
    redis = _FakeRedis()
    await redis.set(control_stop("octo__demo"), "1", ex=60)
    cfg = DaemonConfig()
    now = datetime(2030, 6, 15, 12, 0, tzinfo=timezone.utc)
    state = _make_state()

    result = await derive_active_inhibitors(state, redis, cfg, now=now)

    stops = [i for i in result if i.inhibitor_type is InhibitorType.USER_STOP]
    assert len(stops) == 1
    stop = stops[0]
    assert stop.expires_at == now + timedelta(seconds=60)
    assert stop.source_key == control_stop("octo__demo")


@pytest.mark.asyncio
async def test_derive_returns_rate_limit_per_coder() -> None:
    redis = _FakeRedis()
    cfg = DaemonConfig()
    future = datetime.now(timezone.utc) + timedelta(minutes=10)
    state = _make_state(rate_limited_coder_until={"claude": future})

    result = await derive_active_inhibitors(state, redis, cfg)

    matches = [i for i in result if i.inhibitor_type is InhibitorType.RATE_LIMIT]
    assert len(matches) == 1
    assert matches[0].coder_affected == "claude"
    assert matches[0].expires_at == future


@pytest.mark.asyncio
async def test_derive_returns_spend_ceiling_when_session_pct_at_100() -> None:
    redis = _FakeRedis()
    cfg = DaemonConfig(spend_ceiling_session_percent=100)
    state = _make_state(usage_session_percent=100)

    result = await derive_active_inhibitors(state, redis, cfg)

    assert any(
        inh.inhibitor_type is InhibitorType.SPEND_CEILING for inh in result
    )


@pytest.mark.asyncio
async def test_derive_returns_github_budget_pause_below_5_pct() -> None:
    redis = _FakeRedis()
    _set_github_budget(redis, remaining_percent=3)
    cfg = DaemonConfig()
    state = _make_state()

    result = await derive_active_inhibitors(state, redis, cfg)

    kinds = [inh.inhibitor_type for inh in result]
    assert InhibitorType.GITHUB_BUDGET_PAUSE in kinds
    assert InhibitorType.GITHUB_BUDGET_SLOWDOWN not in kinds


@pytest.mark.asyncio
async def test_derive_returns_github_budget_slowdown_between_5_and_20_pct() -> None:
    redis = _FakeRedis()
    _set_github_budget(redis, remaining_percent=10)
    cfg = DaemonConfig()
    state = _make_state()

    result = await derive_active_inhibitors(state, redis, cfg)

    kinds = [inh.inhibitor_type for inh in result]
    assert InhibitorType.GITHUB_BUDGET_SLOWDOWN in kinds
    assert InhibitorType.GITHUB_BUDGET_PAUSE not in kinds


@pytest.mark.asyncio
async def test_derive_returns_no_github_inhibitor_above_20_pct() -> None:
    redis = _FakeRedis()
    _set_github_budget(redis, remaining_percent=50)
    cfg = DaemonConfig()
    state = _make_state()

    result = await derive_active_inhibitors(state, redis, cfg)

    kinds = [inh.inhibitor_type for inh in result]
    assert InhibitorType.GITHUB_BUDGET_PAUSE not in kinds
    assert InhibitorType.GITHUB_BUDGET_SLOWDOWN not in kinds


@pytest.mark.asyncio
async def test_derive_returns_cascade_panic_when_key_exists() -> None:
    redis = _FakeRedis()
    redis.store[daemon_panic_state()] = "{\"enabled\": true}"
    cfg = DaemonConfig()
    state = _make_state()

    result = await derive_active_inhibitors(state, redis, cfg)

    assert any(
        inh.inhibitor_type is InhibitorType.CASCADE_PANIC for inh in result
    )


@pytest.mark.asyncio
async def test_derive_returns_error_rate_auto_pause_when_state_field_set() -> None:
    redis = _FakeRedis()
    redis.store[last_auto_pause_key("octo__demo")] = "1700000000.0"
    cfg = DaemonConfig()
    state = _make_state()

    result = await derive_active_inhibitors(state, redis, cfg)

    matches: list[WorkInhibitor] = [
        inh for inh in result
        if inh.inhibitor_type is InhibitorType.ERROR_RATE_AUTO_PAUSE
    ]
    assert len(matches) == 1
    assert matches[0].source_key == last_auto_pause_key("octo__demo")


@pytest.mark.asyncio
async def test_derive_accepts_naive_now_by_normalizing_to_utc() -> None:
    redis = _FakeRedis()
    await redis.set(control_stop("octo__demo"), "1", ex=60)
    cfg = DaemonConfig()
    state = _make_state()
    naive_now = datetime.utcnow()
    assert naive_now.tzinfo is None

    result = await derive_active_inhibitors(state, redis, cfg, now=naive_now)

    stops = [i for i in result if i.inhibitor_type is InhibitorType.USER_STOP]
    assert len(stops) == 1
    assert stops[0].expires_at is not None
    assert stops[0].expires_at.tzinfo is timezone.utc


@pytest.mark.asyncio
async def test_derive_skips_expired_per_coder_rate_limits() -> None:
    redis = _FakeRedis()
    cfg = DaemonConfig()
    past = datetime.now(timezone.utc) - timedelta(minutes=10)
    state = _make_state(rate_limited_coder_until={"claude": past})

    result = await derive_active_inhibitors(state, redis, cfg)

    assert not any(
        inh.inhibitor_type is InhibitorType.RATE_LIMIT for inh in result
    )


@pytest.mark.asyncio
async def test_derive_returns_multiple_inhibitors_when_stacked() -> None:
    redis = _FakeRedis()
    _set_github_budget(redis, remaining_percent=3)
    cfg = DaemonConfig()
    future = datetime.now(timezone.utc) + timedelta(minutes=10)
    state = _make_state(
        user_paused=True,
        rate_limited_coder_until={"claude": future},
    )

    result = await derive_active_inhibitors(state, redis, cfg)

    kinds = {inh.inhibitor_type for inh in result}
    assert InhibitorType.USER_PAUSE in kinds
    assert InhibitorType.RATE_LIMIT in kinds
    assert InhibitorType.GITHUB_BUDGET_PAUSE in kinds
