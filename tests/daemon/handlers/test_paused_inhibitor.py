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

from src.config import CoderType, FeatureFlags
from src.inhibitor import InhibitorType, WorkInhibitor
from src.keyspace import control_stop
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus

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
    # The unified gate ignores ``USER_PAUSE`` entries when
    # ``state.user_paused`` is ``False`` (stale-Play guard added in the
    # PR-330b review-feedback fix); the scalar source-of-truth must
    # agree with the inhibitor fixture for the ``user_pause`` case.
    if kind == InhibitorType.USER_PAUSE:
        runner.state.user_paused = True
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


def test_handle_paused_flag_on_selected_coder_rate_limit_keeps_paused() -> None:
    """A per-coder rate limit for the selected coder keeps the repo PAUSED.

    Per-coder rate limits are scoped to the coder that would otherwise
    dispatch. This preserves the legacy fallback behavior while still
    preventing dispatch against the limited coder.
    """
    runner = h._make_runner()
    _enable_flag(runner)
    _seed_paused(runner)
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


def test_handle_paused_flag_on_other_coder_rate_limit_resumes_and_preserves_marker() -> None:
    """Unified PAUSED exit preserves fallback to another eligible coder."""
    runner = h._make_runner(coder=CoderType.CODEX)
    _enable_flag(runner)
    _seed_paused(runner)
    future = datetime.now(timezone.utc) + timedelta(minutes=20)
    runner.state.rate_limited_until = future
    runner.state.rate_limit_reactive = True
    runner.state.rate_limit_reactive_coder = "claude"
    runner.state.rate_limited_coders = {"claude"}
    runner.state.active_inhibitors = [
        WorkInhibitor(
            inhibitor_type=InhibitorType.RATE_LIMIT,
            coder_affected="claude",
            expires_at=future,
            reason_text="claude rate-limited",
            source_key="state:octo__demo.rate_limited_until",
        )
    ]

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.rate_limited_until is None
    assert runner.state.rate_limit_reactive is False
    assert runner.state.rate_limit_reactive_coder is None
    assert runner.state.rate_limited_coders == {"claude"}
    assert runner.state.rate_limited_coder_until == {"claude": future}


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


def test_handle_paused_flag_on_clears_stale_per_coder_rate_limit_markers() -> None:
    """Unified resume must clear ``rate_limited_coders`` and ``rate_limited_coder_until``.

    Regression for review feedback on PR-330b: when a per-coder limit
    has just expired, ``derive_active_inhibitors`` drops the entry from
    ``active_inhibitors`` and the unified branch transitions to IDLE.
    ``selector._is_rate_limited`` consults
    ``rate_limited_coder_until.get(name)`` first and falls through to
    ``name in rate_limited_coders`` when the dict has no entry, so any
    stale marker left in either container would block
    ``eligible_coders`` for a repo pinned to that coder (the IDLE loop
    then logs ``no eligible coder`` indefinitely).
    """
    runner = h._make_runner()
    _enable_flag(runner)
    _seed_paused(runner)
    runner.state.user_paused = False
    past = datetime.now(timezone.utc) - timedelta(minutes=5)
    runner.state.rate_limited_coders = {"codex"}
    runner.state.rate_limited_coder_until = {"codex": past}
    runner.state.active_inhibitors = []

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.rate_limited_coders == set()
    assert runner.state.rate_limited_coder_until == {}


def test_handle_paused_flag_on_routes_non_rate_limit_error_to_error() -> None:
    """Unified resume must re-enter ERROR when ``error_message`` is non-rate-limit.

    Regression for review feedback on PR-330b: ``run_cycle`` parks the
    runner in PAUSED whenever an ERROR cycle observes
    ``rate_limited_until is not None``, preserving the original
    ``error_message``. When the inhibitors clear, a non-rate-limit
    ``error_message`` means the underlying fault is still unresolved
    and the runner must transition back to ERROR — the legacy
    expired-window path enforces this via
    ``_resolve_rate_limit_error_state`` and the unified path must
    match. Otherwise the daemon would silently dispatch from IDLE with
    an unresolved error context still in state.
    """
    runner = h._make_runner()
    _enable_flag(runner)
    _seed_paused(runner)
    runner.state.user_paused = False
    runner.state.active_inhibitors = []
    runner.state.error_message = "Build failed: missing dependency X"

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "Build failed: missing dependency X"
    assert not any(
        e["event"] == "[INFRA] PAUSED inhibitors cleared -> IDLE."
        for e in runner.state.history
    )


def test_handle_paused_flag_on_clears_rate_limit_error_message_and_idles() -> None:
    """Unified resume must clear a rate-limit-shaped ``error_message`` and IDLE.

    Companion to ``test_handle_paused_flag_on_routes_non_rate_limit_error_to_error``:
    when ``error_message`` is itself the legacy rate-limit string,
    ``_resolve_rate_limit_error_state`` drops it and lets the IDLE
    transition proceed — matching the legacy expired-window path.
    """
    runner = h._make_runner()
    _enable_flag(runner)
    _seed_paused(runner)
    runner.state.user_paused = False
    runner.state.active_inhibitors = []
    runner.state.error_message = "API rate limit exceeded (429)"

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None
    assert any(
        "cleared legacy rate-limit" in e["event"]
        for e in runner.state.history
    )


def test_handle_paused_flag_on_logs_only_once_while_paused() -> None:
    """Repeat calls under unified path should not spam the event log."""
    runner = h._make_runner()
    _enable_flag(runner)
    _seed_paused(runner)
    runner.state.user_paused = True
    runner.state.active_inhibitors = [_inhibitor(InhibitorType.USER_PAUSE)]

    asyncio.run(runner.handle_paused())
    asyncio.run(runner.handle_paused())

    inhibited_events = [
        e
        for e in runner.state.history
        if e["event"].startswith("[INFRA] PAUSED inhibited by")
    ]
    assert len(inhibited_events) == 1


def test_handle_paused_flag_on_resumes_to_watch_when_pr_matches_task() -> None:
    """Unified resume: matching ``current_pr.branch``/``current_task.branch`` → WATCH.

    Regression for review feedback on PR-330b: the legacy expired-window
    path resumes to WATCH when the paused runner was mid-watch on the
    active PR, and the unified gate must preserve that split. Forcing
    IDLE here would defer monitoring of the live PR (or, if queue
    selection picks a different actionable task, redirect execution).
    """
    runner = h._make_runner()
    _enable_flag(runner)
    _seed_paused(runner)
    runner.state.user_paused = False
    runner.state.active_inhibitors = []
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="active task",
        status=TaskStatus.DOING,
        branch="pr-001-feature",
    )
    runner.state.current_pr = PRInfo(number=1, branch="pr-001-feature")

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.WATCH
    assert any(
        e["event"] == "[INFRA] PAUSED inhibitors cleared -> WATCH."
        for e in runner.state.history
    )


def test_handle_paused_flag_on_ignores_stale_user_pause_after_play() -> None:
    """Stale ``USER_PAUSE`` entry must not lag a Play press by one cycle.

    Regression for the PR-330b review feedback: ``publish_state`` rebuilds
    ``state.active_inhibitors`` at the END of each cycle, but
    ``_run_cycle_body`` refreshes ``state.user_paused`` from Redis at
    cycle START. When an operator presses Play, ``user_paused`` flips to
    ``False`` while a ``USER_PAUSE`` entry from the previous publish is
    still sitting in ``active_inhibitors`` — and because the entry has
    no ``expires_at``, ``is_blocking_now`` would otherwise treat it as
    live and keep the runner PAUSED through an extra cycle. The legacy
    path read the fresh scalar directly and resumed in the same tick;
    the unified gate must match.
    """
    runner = h._make_runner()
    _enable_flag(runner)
    _seed_paused(runner)
    runner.state.user_paused = False
    runner.state.active_inhibitors = [_inhibitor(InhibitorType.USER_PAUSE)]

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.IDLE
    assert any(
        e["event"] == "[INFRA] PAUSED inhibitors cleared -> IDLE."
        for e in runner.state.history
    )
    assert not any(
        e["event"].startswith("[INFRA] PAUSED inhibited by")
        for e in runner.state.history
    )


def test_handle_paused_flag_on_ignores_stale_user_stop_after_play() -> None:
    """A cleared stop control key must not leave PAUSED stuck forever."""
    runner = h._make_runner()
    _enable_flag(runner)
    _seed_paused(runner)
    runner.state.user_paused = False
    runner.state.active_inhibitors = [
        WorkInhibitor(
            inhibitor_type=InhibitorType.USER_STOP,
            expires_at=_future(),
            reason_text="Operator requested stop",
            source_key=control_stop(runner.name),
        )
    ]

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.IDLE
    assert any(
        e["event"] == "[INFRA] PAUSED inhibitors cleared -> IDLE."
        for e in runner.state.history
    )
    assert not any(
        e["event"].startswith("[INFRA] PAUSED inhibited by")
        for e in runner.state.history
    )


def test_handle_paused_flag_on_preserves_user_stop_when_stop_key_read_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Redis read failure must not accidentally clear a stop inhibitor."""
    runner = h._make_runner()
    _enable_flag(runner)
    _seed_paused(runner)
    runner.state.user_paused = False
    runner.state.active_inhibitors = [
        WorkInhibitor(
            inhibitor_type=InhibitorType.USER_STOP,
            expires_at=_future(),
            reason_text="Operator requested stop",
            source_key=control_stop(runner.name),
        )
    ]

    async def raise_on_stop_key(key: str) -> str | None:
        if key == control_stop(runner.name):
            raise RuntimeError("redis unavailable")
        return runner.redis.store.get(key)

    monkeypatch.setattr(runner.redis, "get", raise_on_stop_key)

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.PAUSED
    assert any(
        e["event"].startswith("[INFRA] PAUSED inhibited by")
        and "user_stop" in e["event"]
        for e in runner.state.history
    )


def test_handle_paused_flag_on_empty_snapshot_with_live_rate_limit_stays_paused() -> None:
    """Empty inhibitor snapshot + future rate-limit scalar → stay PAUSED.

    Regression for review feedback on PR-330b:
    ``runner._serialize_latest_state`` force-sets
    ``state.active_inhibitors`` to ``[]`` when
    ``derive_active_inhibitors`` raises (Redis read error, future
    field-shape drift). Trusting that snapshot would treat a
    genuinely-rate-limited repo as "all clear", wipe
    ``rate_limited_until`` / per-coder markers, and let the daemon
    immediately dispatch against an active limit. The unified gate
    must cross-check the snapshot against the legacy rate-limit
    scalars (the source of truth for the window) and stay PAUSED when
    a future expiry remains.
    """
    runner = h._make_runner()
    _enable_flag(runner)
    _seed_paused(runner)
    runner.state.user_paused = False
    future = datetime.now(timezone.utc) + timedelta(minutes=30)
    runner.state.rate_limited_until = future
    runner.state.rate_limit_reactive = True
    runner.state.rate_limit_reactive_coder = "claude"
    runner.state.rate_limited_coders = {"claude"}
    runner.state.rate_limited_coder_until = {"claude": future}
    runner.repo_config = runner.repo_config.model_copy(
        update={"disabled_coders": ["codex"]}
    )
    runner.state.active_inhibitors = []

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.rate_limited_until == future
    assert runner.state.rate_limit_reactive is True
    assert runner.state.rate_limit_reactive_coder == "claude"
    assert runner.state.rate_limited_coders == {"claude"}
    assert runner.state.rate_limited_coder_until == {"claude": future}
    assert any(
        "inhibitor snapshot disagrees with live rate-limit scalars"
        in e["event"]
        for e in runner.state.history
    )


def test_handle_paused_flag_on_slowdown_only_snapshot_with_live_rate_limit_stays_paused() -> None:
    """Slowdown-only snapshot + future rate-limit scalar → stay PAUSED.

    Regression for review feedback on PR-330b: keying the
    stale-snapshot guard on the raw ``blocking`` list missed the case
    where ``active_inhibitors`` contains only filtered non-blockers
    (e.g. ``GITHUB_BUDGET_SLOWDOWN``) while ``rate_limited_until`` or
    ``rate_limited_coder_until`` still points to a future window. With
    ``not blocking`` False the guard fell through, cleared every
    ``rate_limited_*`` field, and transitioned to IDLE — resuming
    dispatch against an active rate limit. The guard must key off the
    post-filter blocking set (``hard_blocking``) so a snapshot that
    looks empty for dispatch purposes still flags the disagreement.
    """
    runner = h._make_runner()
    _enable_flag(runner)
    _seed_paused(runner)
    runner.state.user_paused = False
    future = datetime.now(timezone.utc) + timedelta(minutes=30)
    runner.state.rate_limited_until = future
    runner.state.rate_limit_reactive = True
    runner.state.rate_limit_reactive_coder = "claude"
    runner.state.rate_limited_coders = {"claude"}
    runner.state.rate_limited_coder_until = {"claude": future}
    runner.repo_config = runner.repo_config.model_copy(
        update={"disabled_coders": ["codex"]}
    )
    runner.state.active_inhibitors = [
        WorkInhibitor(
            inhibitor_type=InhibitorType.GITHUB_BUDGET_SLOWDOWN,
            expires_at=future,
            reason_text="GitHub budget slowdown",
            source_key="github:rate_limit:budget",
        )
    ]

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.rate_limited_until == future
    assert runner.state.rate_limit_reactive is True
    assert runner.state.rate_limit_reactive_coder == "claude"
    assert runner.state.rate_limited_coders == {"claude"}
    assert runner.state.rate_limited_coder_until == {"claude": future}
    assert any(
        "inhibitor snapshot disagrees with live rate-limit scalars"
        in e["event"]
        for e in runner.state.history
    )


def test_handle_paused_flag_on_slowdown_only_snapshot_with_per_coder_future_limit_stays_paused() -> None:
    """Slowdown-only snapshot + future per-coder expiry → stay PAUSED.

    Per-coder variant of the slowdown-only regression: when only
    ``rate_limited_coder_until`` has a future entry,
    ``derive_active_inhibitors`` would normally emit a ``RATE_LIMIT``
    inhibitor for that coder. Its absence alongside slowdown-only
    snapshot contents indicates a publish-time disagreement that the
    guard must catch.
    """
    runner = h._make_runner()
    _enable_flag(runner)
    _seed_paused(runner)
    runner.state.user_paused = False
    future = datetime.now(timezone.utc) + timedelta(minutes=15)
    runner.state.rate_limited_until = None
    runner.state.rate_limited_coder_until = {"claude": future}
    runner.repo_config = runner.repo_config.model_copy(
        update={"disabled_coders": ["codex"]}
    )
    runner.state.active_inhibitors = [
        WorkInhibitor(
            inhibitor_type=InhibitorType.GITHUB_BUDGET_SLOWDOWN,
            expires_at=future,
            reason_text="GitHub budget slowdown",
            source_key="github:rate_limit:budget",
        )
    ]

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.rate_limited_coder_until == {"claude": future}


def test_handle_paused_flag_on_empty_snapshot_with_per_coder_future_limit_stays_paused() -> None:
    """Empty snapshot + future per-coder expiry → stay PAUSED.

    Companion to the global-scalar variant: the guard also fires when
    only ``rate_limited_coder_until`` has a future entry (per-coder
    pause path), since ``derive_active_inhibitors`` would have emitted
    a RATE_LIMIT inhibitor for that coder under normal operation.
    """
    runner = h._make_runner()
    _enable_flag(runner)
    _seed_paused(runner)
    runner.state.user_paused = False
    future = datetime.now(timezone.utc) + timedelta(minutes=15)
    runner.state.rate_limited_until = None
    runner.state.rate_limited_coder_until = {"claude": future}
    runner.repo_config = runner.repo_config.model_copy(
        update={"disabled_coders": ["codex"]}
    )
    runner.state.active_inhibitors = []

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.rate_limited_coder_until == {"claude": future}


def test_handle_paused_flag_on_empty_snapshot_with_user_paused_stays_paused() -> None:
    """Empty inhibitor snapshot + ``user_paused=True`` → stay PAUSED.

    Regression for review feedback on PR-330b:
    ``runner._serialize_latest_state`` force-sets
    ``state.active_inhibitors`` to ``[]`` when
    ``derive_active_inhibitors`` raises. The previous ``stale_snapshot``
    guard only fired when ``user_paused`` was ``False``, so a real
    operator-held pause would fall through to the IDLE/WATCH transition
    below. ``state.user_paused`` is the scalar source of truth for the
    USER_PAUSE inhibitor — the unified gate must honor it independently
    of the inhibitor snapshot, matching the legacy path which read the
    scalar directly.
    """
    runner = h._make_runner()
    _enable_flag(runner)
    _seed_paused(runner)
    runner.state.user_paused = True
    runner.state.active_inhibitors = []

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.PAUSED
    assert any(
        e["event"].startswith("[INFRA] PAUSED inhibited by")
        and "user_pause" in e["event"]
        for e in runner.state.history
    )
    assert not any(
        e["event"] == "[INFRA] PAUSED inhibitors cleared -> IDLE."
        for e in runner.state.history
    )


def test_handle_paused_flag_on_user_paused_with_only_slowdown_stays_paused() -> None:
    """Manual pause must not be bypassed by non-blocking slowdown entries.

    Regression for review feedback on PR-330b: ``state.user_paused`` is
    refreshed at cycle start while ``active_inhibitors`` is rebuilt at
    cycle end, so right after an operator presses Pause the snapshot
    can hold only ``GITHUB_BUDGET_SLOWDOWN`` (a polling-cadence
    throttle the unified gate filters out for dispatch decisions). The
    previous empty-snapshot guard short-circuited only when
    ``blocking`` was empty, so a snapshot containing slowdown alone
    fell through to the IDLE/WATCH transition and resumed against an
    operator-held pause. The scalar must be honored independently of
    the snapshot contents.
    """
    runner = h._make_runner()
    _enable_flag(runner)
    _seed_paused(runner)
    runner.state.user_paused = True
    runner.state.active_inhibitors = [
        WorkInhibitor(
            inhibitor_type=InhibitorType.GITHUB_BUDGET_SLOWDOWN,
            expires_at=_future(),
            reason_text="GitHub budget slowdown",
            source_key="github:rate_limit:budget",
        )
    ]

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.PAUSED
    assert any(
        e["event"].startswith("[INFRA] PAUSED inhibited by")
        and "user_pause" in e["event"]
        for e in runner.state.history
    )
    assert not any(
        e["event"] == "[INFRA] PAUSED inhibitors cleared -> IDLE."
        or e["event"] == "[INFRA] PAUSED inhibitors cleared -> WATCH."
        for e in runner.state.history
    )


def test_handle_paused_flag_on_resumes_to_idle_when_pr_branch_diverges() -> None:
    """Unified resume: divergent PR/task branches → IDLE (queue re-selection).

    Companion to the WATCH variant: when ``current_pr.branch`` does not
    match ``current_task.branch`` (or either is unset), the runner falls
    back to the IDLE task-selection/sync path, mirroring the legacy
    expired-window resume.
    """
    runner = h._make_runner()
    _enable_flag(runner)
    _seed_paused(runner)
    runner.state.user_paused = False
    runner.state.active_inhibitors = []
    runner.state.current_task = QueueTask(
        pr_id="PR-002",
        title="other task",
        status=TaskStatus.DOING,
        branch="pr-002-other",
    )
    runner.state.current_pr = PRInfo(number=9, branch="pr-001-feature")

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.IDLE
    assert any(
        e["event"] == "[INFRA] PAUSED inhibitors cleared -> IDLE."
        for e in runner.state.history
    )
