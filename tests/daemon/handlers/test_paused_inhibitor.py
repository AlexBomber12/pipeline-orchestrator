"""PR-330b: handle_paused inhibitor migration tests.

Covers the per-repo ``feature_flags.use_unified_inhibitor_check`` flag
that selects between the legacy ``self.state.user_paused`` /
``rate_limited_until`` exit branches and the unified
``is_work_inhibited(state, coder=None)`` helper from ``src.inhibitor``.

PAUSED is a global state: any active inhibitor (per-coder or global)
keeps the repo paused, and a clean inhibitor list collapses to a single
IDLE transition. The 8 throttle scenarios mirror ``InhibitorType``;
each is exercised under both flag values. ``GITHUB_BUDGET_SLOWDOWN`` is
covered separately because it is throttle-only and must not keep PAUSED
on its own.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from src.config import FeatureFlags
from src.inhibitor import InhibitorType, WorkInhibitor
from src.models import PipelineState

from tests.runner import _helpers as h


def _future() -> datetime:
    return datetime.now(timezone.utc) + timedelta(minutes=30)


def _inhibitor(kind: InhibitorType, coder: str | None = None) -> WorkInhibitor:
    per_coder_default = "claude" if kind == InhibitorType.RATE_LIMIT else None
    coder_affected = coder if coder is not None else per_coder_default
    return WorkInhibitor(
        inhibitor_type=kind,
        coder_affected=coder_affected,
        expires_at=_future() if coder_affected is not None else None,
        reason_text=f"{kind.value} test fixture",
        source_key=f"test:{kind.value}",
    )


def _enable_flag(runner: Any) -> None:
    runner.repo_config = runner.repo_config.model_copy(
        update={
            "feature_flags": FeatureFlags(use_unified_inhibitor_check=True),
        }
    )


def _disable_flag(runner: Any) -> None:
    runner.repo_config = runner.repo_config.model_copy(
        update={
            "feature_flags": FeatureFlags(use_unified_inhibitor_check=False),
        }
    )


def _seed_paused(runner: Any) -> None:
    runner.state.state = PipelineState.PAUSED


_THROTTLE_TYPES = list(InhibitorType)
# ``GITHUB_BUDGET_SLOWDOWN`` is a polling-cadence throttle, not a
# dispatch block: ``_check_github_api_budget`` already skips one-in-N
# cycles between the slowdown and pause thresholds, and the legacy
# PAUSED exit conditions never gated on the slowdown either. The
# unified gate filters it out, so the parametrize-blocks suite skips it.
_BLOCKING_TYPES = [
    kind
    for kind in _THROTTLE_TYPES
    if kind != InhibitorType.GITHUB_BUDGET_SLOWDOWN
]


def test_handle_paused_stays_paused_when_user_pause_active_legacy() -> None:
    runner = h._make_runner()
    _disable_flag(runner)
    _seed_paused(runner)
    runner.state.user_paused = True
    runner.state.active_inhibitors = [_inhibitor(InhibitorType.USER_PAUSE)]

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.PAUSED
    assert not any(
        e["event"].startswith("[INFRA] PAUSED inhibited by")
        for e in runner.state.history
    )


def test_handle_paused_stays_paused_when_user_pause_active_unified() -> None:
    runner = h._make_runner()
    _enable_flag(runner)
    _seed_paused(runner)
    runner.state.user_paused = True
    runner.state.active_inhibitors = [_inhibitor(InhibitorType.USER_PAUSE)]

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.PAUSED
    assert any(
        e["event"].startswith("[INFRA] PAUSED inhibited by")
        and "user_pause" in e["event"]
        for e in runner.state.history
    )


def test_handle_paused_exits_to_idle_when_all_clear_legacy() -> None:
    runner = h._make_runner()
    _disable_flag(runner)
    _seed_paused(runner)
    runner.state.user_paused = False
    runner.state.rate_limited_until = None
    runner.state.active_inhibitors = []

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.IDLE


def test_handle_paused_exits_to_idle_when_all_clear_unified() -> None:
    runner = h._make_runner()
    _enable_flag(runner)
    _seed_paused(runner)
    runner.state.user_paused = False
    runner.state.active_inhibitors = []

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.IDLE
    assert any(
        e["event"] == "[INFRA] PAUSED inhibitors cleared -> IDLE."
        for e in runner.state.history
    )


@pytest.mark.parametrize(
    "kind", _BLOCKING_TYPES, ids=[k.value for k in _BLOCKING_TYPES]
)
def test_handle_paused_flag_off_stays_paused_per_inhibitor(
    kind: InhibitorType,
) -> None:
    """Legacy path stays PAUSED on the ``user_paused`` short-circuit.

    The legacy PAUSED exit conditions only consult ``user_paused`` and
    ``rate_limited_until``; the other throttle types are gated at the
    runner-cycle layer. Each parametrized fixture therefore sets
    ``user_paused=True`` so the legacy branch fires identically across
    all blocking inhibitor types, giving a uniform stay-PAUSED baseline
    to compare against the unified path.
    """
    runner = h._make_runner()
    _disable_flag(runner)
    _seed_paused(runner)
    runner.state.user_paused = True
    runner.state.active_inhibitors = [_inhibitor(kind)]

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.PAUSED


@pytest.mark.parametrize(
    "kind", _BLOCKING_TYPES, ids=[k.value for k in _BLOCKING_TYPES]
)
def test_handle_paused_flag_on_stays_paused_per_inhibitor(
    kind: InhibitorType,
) -> None:
    """Unified path stays PAUSED on any active blocking inhibitor."""
    runner = h._make_runner()
    _enable_flag(runner)
    _seed_paused(runner)
    runner.state.active_inhibitors = [_inhibitor(kind)]

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.PAUSED
    assert any(
        e["event"].startswith("[INFRA] PAUSED inhibited by")
        and kind.value in e["event"]
        for e in runner.state.history
    )


def test_handle_paused_partial_clearance_stays_paused() -> None:
    """Unified: user_pause cleared but rate_limit still active → stay PAUSED.

    The "AND" semantics of PAUSED exit mean clearing one inhibitor is
    not enough; ``is_work_inhibited`` still returns ``blocked=True`` while
    any entry remains in ``active_inhibitors``.
    """
    runner = h._make_runner()
    _enable_flag(runner)
    _seed_paused(runner)
    runner.state.user_paused = False
    runner.state.active_inhibitors = [
        WorkInhibitor(
            inhibitor_type=InhibitorType.RATE_LIMIT,
            coder_affected="claude",
            expires_at=_future(),
            reason_text="claude rate-limited",
            source_key="state:octo__demo.rate_limited_coder_until.claude",
        )
    ]

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.PAUSED
    assert any(
        e["event"].startswith("[INFRA] PAUSED inhibited by")
        and "rate_limit" in e["event"]
        for e in runner.state.history
    )


def test_handle_paused_flag_on_slowdown_alone_exits_to_idle() -> None:
    """Unified: slowdown alone is throttle-only and must not keep PAUSED.

    Mirrors the IDLE handler's slowdown filter: when only the
    ``GITHUB_BUDGET_SLOWDOWN`` inhibitor is active, the unified gate
    must not treat it as a blocker — otherwise the daemon would stay
    PAUSED indefinitely instead of resuming normal polling.
    """
    runner = h._make_runner()
    _enable_flag(runner)
    _seed_paused(runner)
    runner.state.active_inhibitors = [
        WorkInhibitor(
            inhibitor_type=InhibitorType.GITHUB_BUDGET_SLOWDOWN,
            expires_at=_future(),
            reason_text="GitHub budget slowdown",
            source_key="github:rate_limit:budget",
        )
    ]

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.IDLE
    assert not any(
        e["event"].startswith("[INFRA] PAUSED inhibited by")
        for e in runner.state.history
    )


def test_handle_paused_flag_on_per_coder_rate_limit_keeps_paused() -> None:
    """A per-coder rate limit (any coder) keeps the repo PAUSED.

    PAUSED is a global state, so the unified gate calls
    ``is_work_inhibited`` with ``coder=None`` — every active inhibitor
    matches, including per-coder entries for a single coder.
    """
    runner = h._make_runner()
    _enable_flag(runner)
    _seed_paused(runner)
    runner.state.active_inhibitors = [
        WorkInhibitor(
            inhibitor_type=InhibitorType.RATE_LIMIT,
            coder_affected="codex",
            expires_at=_future(),
            reason_text="codex rate-limited",
            source_key="state:octo__demo.rate_limited_coder_until.codex",
        )
    ]

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.PAUSED
    assert any(
        e["event"].startswith("[INFRA] PAUSED inhibited by")
        and "rate_limit" in e["event"]
        for e in runner.state.history
    )


def test_handle_paused_flag_on_clears_stale_rate_limit_metadata() -> None:
    """Unified resume must clear ``rate_limited_*`` scalars.

    Regression for review feedback on PR-330b: when the window in
    ``rate_limited_coder_until`` is past, ``derive_active_inhibitors``
    returns no blocker and the unified branch transitions to IDLE. The
    legacy scalar fields persist, however, and ``run_cycle`` then keeps
    reading ``rate_limited_until != None`` as a live pause (forcing
    ``ERROR -> PAUSED``) until ``_check_rate_limit`` happens to run.
    Mirror the legacy expired-window resume and clear all three
    scalars before setting IDLE.
    """
    runner = h._make_runner()
    _enable_flag(runner)
    _seed_paused(runner)
    runner.state.user_paused = False
    past = datetime.now(timezone.utc) - timedelta(minutes=5)
    runner.state.rate_limited_until = past
    runner.state.rate_limit_reactive = True
    runner.state.rate_limit_reactive_coder = "claude"
    runner.state.active_inhibitors = []

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.rate_limited_until is None
    assert runner.state.rate_limit_reactive is False
    assert runner.state.rate_limit_reactive_coder is None


def test_handle_paused_flag_on_logs_only_once_while_paused() -> None:
    """Repeat calls under unified path should not spam the event log."""
    runner = h._make_runner()
    _enable_flag(runner)
    _seed_paused(runner)
    runner.state.active_inhibitors = [_inhibitor(InhibitorType.USER_PAUSE)]

    asyncio.run(runner.handle_paused())
    asyncio.run(runner.handle_paused())

    inhibited_events = [
        e
        for e in runner.state.history
        if e["event"].startswith("[INFRA] PAUSED inhibited by")
    ]
    assert len(inhibited_events) == 1
