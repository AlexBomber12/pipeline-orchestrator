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
async def test_derive_returns_user_stop_when_control_stop_key_has_no_expiry() -> None:
    """Mirror ``runner._pop_stop_request``: a ``GET`` on the key stops dispatch
    regardless of TTL, so a key with no expiry (``TTL = -1``, common after a
    manual Redis write) must still surface as an active USER_STOP inhibitor.
    """
    redis = _FakeRedis()
    redis.store[control_stop("octo__demo")] = "1"
    cfg = DaemonConfig()
    state = _make_state()

    result = await derive_active_inhibitors(state, redis, cfg)

    stops = [i for i in result if i.inhibitor_type is InhibitorType.USER_STOP]
    assert len(stops) == 1
    assert stops[0].expires_at is None
    assert stops[0].source_key == control_stop("octo__demo")


@pytest.mark.asyncio
async def test_derive_returns_user_stop_when_control_stop_key_ttl_is_zero() -> None:
    """Keys in their final sub-second window (``TTL = 0``) still stop dispatch
    via ``GET``, so the derivation must emit USER_STOP with no remaining
    expiry rather than dropping the inhibitor.
    """
    redis = _FakeRedis()
    stop_key = control_stop("octo__demo")
    redis.store[stop_key] = "1"
    redis.ttls[stop_key] = 0
    cfg = DaemonConfig()
    state = _make_state()

    result = await derive_active_inhibitors(state, redis, cfg)

    stops = [i for i in result if i.inhibitor_type is InhibitorType.USER_STOP]
    assert len(stops) == 1
    assert stops[0].expires_at is None


@pytest.mark.asyncio
async def test_derive_skips_user_stop_when_payload_is_empty_string() -> None:
    """Mirror ``runner._pop_stop_request``: it gates on ``GET`` returning a
    truthy payload (``if not raw: return False``). A present-but-empty key
    leaves the runner free to dispatch, so the derivation must not emit a
    USER_STOP inhibitor that would desync the typed list from the
    dispatcher's blocking decision.
    """
    redis = _FakeRedis()
    stop_key = control_stop("octo__demo")
    redis.store[stop_key] = ""
    redis.ttls[stop_key] = 60
    cfg = DaemonConfig()
    state = _make_state()

    result = await derive_active_inhibitors(state, redis, cfg)

    assert not any(
        inh.inhibitor_type is InhibitorType.USER_STOP for inh in result
    )


@pytest.mark.asyncio
async def test_derive_skips_user_stop_when_get_raises() -> None:
    """``_pop_stop_request`` swallows ``GET`` errors and returns ``False`` so
    a transient Redis failure cannot stop the runner. The derivation must
    mirror that: surfacing USER_STOP from a key whose payload could not be
    read would block automation that consumes this helper without any
    corresponding runner-side stop.
    """

    class _RaisingGetRedis(_FakeRedis):
        async def get(self, key: str) -> str | None:
            if key == control_stop("octo__demo"):
                raise RuntimeError("redis offline")
            return await super().get(key)

    redis = _RaisingGetRedis()
    redis.ttls[control_stop("octo__demo")] = 60
    cfg = DaemonConfig()
    state = _make_state()

    result = await derive_active_inhibitors(state, redis, cfg)

    assert not any(
        inh.inhibitor_type is InhibitorType.USER_STOP for inh in result
    )


@pytest.mark.asyncio
async def test_derive_returns_user_stop_without_expiry_when_ttl_raises() -> None:
    """A successful truthy ``GET`` must still emit USER_STOP even if the
    follow-up TTL read fails: ``_pop_stop_request`` would block dispatch in
    that case, so the typed list must surface the inhibitor with no
    ``expires_at`` rather than dropping it because of a TTL-side error.
    """

    class _RaisingTtlRedis(_FakeRedis):
        async def ttl(self, key: str) -> int:
            if key == control_stop("octo__demo"):
                raise RuntimeError("ttl unavailable")
            return await super().ttl(key)

    redis = _RaisingTtlRedis()
    redis.store[control_stop("octo__demo")] = "1"
    cfg = DaemonConfig()
    state = _make_state()

    result = await derive_active_inhibitors(state, redis, cfg)

    stops = [i for i in result if i.inhibitor_type is InhibitorType.USER_STOP]
    assert len(stops) == 1
    assert stops[0].expires_at is None
    assert stops[0].source_key == control_stop("octo__demo")


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
async def test_derive_skips_cascade_panic_when_payload_disabled() -> None:
    """Mirror ``check_cascade_escalate_state``: a panic record with ``enabled=False``
    means the daemon has cleared the gate, so the derivation must not surface a
    stale CASCADE_PANIC inhibitor that would desync UI/automation from the
    runner's actual dispatch state.
    """
    redis = _FakeRedis()
    redis.store[daemon_panic_state()] = "{\"enabled\": false}"
    cfg = DaemonConfig()
    state = _make_state()

    result = await derive_active_inhibitors(state, redis, cfg)

    assert not any(
        inh.inhibitor_type is InhibitorType.CASCADE_PANIC for inh in result
    )


@pytest.mark.asyncio
async def test_derive_skips_cascade_panic_when_payload_missing_enabled_flag() -> None:
    """A dict payload without the ``enabled`` key is treated as not-enabled
    by ``check_cascade_escalate_state`` (``existing.get('enabled')`` is
    falsy), so the inhibitor must be omitted.
    """
    redis = _FakeRedis()
    redis.store[daemon_panic_state()] = "{\"reason\": \"legacy\"}"
    cfg = DaemonConfig()
    state = _make_state()

    result = await derive_active_inhibitors(state, redis, cfg)

    assert not any(
        inh.inhibitor_type is InhibitorType.CASCADE_PANIC for inh in result
    )


@pytest.mark.asyncio
async def test_derive_skips_cascade_panic_when_threshold_disables_detector() -> None:
    """Mirror ``check_cascade_escalate_state``: when ``cascade_escalate_threshold``
    is ``<= 0`` the daemon returns ``False`` immediately without reading the
    panic record, so a stale ``daemon:panic_state`` (e.g. left behind after
    the operator lowered the threshold to disable cascade detection) must not
    surface as a CASCADE_PANIC inhibitor here either.
    """
    redis = _FakeRedis()
    redis.store[daemon_panic_state()] = "{\"enabled\": true}"
    cfg = DaemonConfig(cascade_escalate_threshold=0)
    state = _make_state()

    result = await derive_active_inhibitors(state, redis, cfg)

    assert not any(
        inh.inhibitor_type is InhibitorType.CASCADE_PANIC for inh in result
    )


@pytest.mark.asyncio
async def test_derive_skips_cascade_panic_when_payload_is_malformed() -> None:
    """Malformed/non-dict payloads are ignored by ``_read_panic_state``;
    the derivation must agree rather than emit a spurious inhibitor.
    """
    cfg = DaemonConfig()
    state = _make_state()

    for payload in ("not-json", "\"a string\"", "[1, 2, 3]", "null"):
        redis = _FakeRedis()
        redis.store[daemon_panic_state()] = payload

        result = await derive_active_inhibitors(state, redis, cfg)

        assert not any(
            inh.inhibitor_type is InhibitorType.CASCADE_PANIC for inh in result
        ), f"unexpected CASCADE_PANIC for payload {payload!r}"


@pytest.mark.asyncio
async def test_derive_does_not_emit_error_rate_auto_pause_from_historical_marker() -> None:
    """``ERROR_RATE_AUTO_PAUSE`` derivation is deferred in PR-327.

    ``mark_auto_pause`` writes ``error_rate_last_auto_pause:<repo>`` once
    and nothing clears it on Resume, so combining the marker with
    ``state.user_paused`` would misclassify a later operator-initiated
    pause as an auto-pause whenever the repo had ever auto-paused. The
    derivation must omit the inhibitor in both the user-paused and
    user-resumed cases until a dedicated live-pause signal exists.
    """
    cfg = DaemonConfig()

    for user_paused in (True, False):
        redis = _FakeRedis()
        redis.store[last_auto_pause_key("octo__demo")] = "1700000000.0"
        state = _make_state(user_paused=user_paused)

        result = await derive_active_inhibitors(state, redis, cfg)

        assert not any(
            inh.inhibitor_type is InhibitorType.ERROR_RATE_AUTO_PAUSE
            for inh in result
        ), f"unexpected ERROR_RATE_AUTO_PAUSE with user_paused={user_paused}"


@pytest.mark.asyncio
async def test_derive_skips_github_budget_when_snapshot_reset_at_elapsed() -> None:
    """Stale budget snapshots must not surface as active inhibitors.

    ``runner._check_github_api_budget`` gates both pause and slowdown
    branches on ``now < budget.reset_at``; the derivation must agree so
    the typed list cannot disagree with the runner's blocking decision.
    """
    redis = _FakeRedis()
    now = datetime(2030, 6, 15, 12, 0, tzinfo=timezone.utc)
    _set_github_budget(
        redis,
        remaining_percent=3,
        reset_at=now - timedelta(minutes=1),
    )
    cfg = DaemonConfig()
    state = _make_state()

    result = await derive_active_inhibitors(state, redis, cfg, now=now)

    kinds = [inh.inhibitor_type for inh in result]
    assert InhibitorType.GITHUB_BUDGET_PAUSE not in kinds
    assert InhibitorType.GITHUB_BUDGET_SLOWDOWN not in kinds


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
async def test_derive_returns_rate_limit_from_legacy_rate_limited_until() -> None:
    """Upgraded repos that only have the legacy global expiry still gate dispatch.

    ``selector._is_rate_limited`` falls back to ``state.rate_limited_until``
    when ``rate_limit_reactive_coder`` is unset and the coder is ``"claude"``,
    so the typed list must surface a matching ``RATE_LIMIT`` inhibitor.
    """
    redis = _FakeRedis()
    cfg = DaemonConfig()
    future = datetime.now(timezone.utc) + timedelta(minutes=10)
    state = _make_state(rate_limited_until=future)

    result = await derive_active_inhibitors(state, redis, cfg)

    matches = [i for i in result if i.inhibitor_type is InhibitorType.RATE_LIMIT]
    assert len(matches) == 1
    assert matches[0].coder_affected == "claude"
    assert matches[0].expires_at == future
    assert matches[0].source_key == "state:octo__demo.rate_limited_until"


@pytest.mark.asyncio
async def test_derive_skips_legacy_rate_limited_until_when_reactive_coder_set() -> None:
    """Mirror selector: with a reactive marker the global expiry no longer gates claude.

    The selector returns ``True`` only via the ``rate_limit_reactive_coder``
    branch in this case, so the derivation must emit one inhibitor for the
    reactive coder and nothing for ``"claude"`` via the global expiry.
    """
    redis = _FakeRedis()
    cfg = DaemonConfig()
    future = datetime.now(timezone.utc) + timedelta(minutes=10)
    state = _make_state(
        rate_limited_until=future,
        rate_limit_reactive_coder="codex",
    )

    result = await derive_active_inhibitors(state, redis, cfg)

    matches = [i for i in result if i.inhibitor_type is InhibitorType.RATE_LIMIT]
    assert len(matches) == 1
    assert matches[0].coder_affected == "codex"
    assert matches[0].source_key == "state:octo__demo.rate_limit_reactive_coder"


@pytest.mark.asyncio
async def test_derive_skips_expired_legacy_rate_limited_until() -> None:
    redis = _FakeRedis()
    cfg = DaemonConfig()
    past = datetime.now(timezone.utc) - timedelta(minutes=10)
    state = _make_state(rate_limited_until=past)

    result = await derive_active_inhibitors(state, redis, cfg)

    assert not any(
        inh.inhibitor_type is InhibitorType.RATE_LIMIT for inh in result
    )


@pytest.mark.asyncio
async def test_derive_returns_rate_limit_from_legacy_rate_limited_coders_set() -> None:
    redis = _FakeRedis()
    cfg = DaemonConfig()
    state = _make_state(rate_limited_coders={"codex"})

    result = await derive_active_inhibitors(state, redis, cfg)

    matches = [i for i in result if i.inhibitor_type is InhibitorType.RATE_LIMIT]
    assert len(matches) == 1
    assert matches[0].coder_affected == "codex"
    assert matches[0].expires_at is None
    assert matches[0].source_key == "state:octo__demo.rate_limited_coders"


@pytest.mark.asyncio
async def test_derive_dedupes_rate_limit_when_typed_and_legacy_overlap() -> None:
    """Typed per-coder dict wins when the same coder appears in both sources."""
    redis = _FakeRedis()
    cfg = DaemonConfig()
    future = datetime.now(timezone.utc) + timedelta(minutes=10)
    state = _make_state(
        rate_limited_coder_until={"claude": future},
        rate_limit_reactive_coder="claude",
        rate_limited_coders={"claude"},
    )

    result = await derive_active_inhibitors(state, redis, cfg)

    matches = [i for i in result if i.inhibitor_type is InhibitorType.RATE_LIMIT]
    assert len(matches) == 1
    assert matches[0].coder_affected == "claude"
    assert matches[0].expires_at == future
    assert matches[0].source_key == (
        "state:octo__demo.rate_limited_coder_until.claude"
    )


@pytest.mark.asyncio
async def test_derive_expired_typed_entry_short_circuits_legacy_fields() -> None:
    """Mirror selector short-circuit on the typed per-coder dict.

    ``selector._is_rate_limited`` returns ``False`` once the typed entry for
    a coder has expired, without consulting the legacy ``rate_limited_until``,
    ``rate_limit_reactive_coder``, or ``rate_limited_coders`` fields. The
    derivation must agree so persisted mixed state (e.g. an expired
    ``rate_limited_coder_until['codex']`` paired with a stale legacy
    ``rate_limit_reactive_coder='codex'``) does not surface a spurious
    ``RATE_LIMIT`` inhibitor that would block dispatch after consumers
    switch to this helper.
    """
    redis = _FakeRedis()
    cfg = DaemonConfig()
    past = datetime.now(timezone.utc) - timedelta(minutes=10)
    state = _make_state(
        rate_limited_coder_until={"codex": past},
        rate_limit_reactive_coder="codex",
        rate_limited_coders={"codex"},
    )

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
