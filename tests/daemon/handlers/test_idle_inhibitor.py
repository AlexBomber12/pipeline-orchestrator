"""PR-330a: handle_idle inhibitor migration tests.

Covers the per-repo ``feature_flags.use_unified_inhibitor_check`` flag
that selects between the legacy ``self.state.user_paused`` check and the
unified ``is_work_inhibited(state)`` helper from ``src.inhibitor``.

The 8 throttle scenarios mirror ``InhibitorType``; each is exercised
under both flag values. Tests verify behavioral parity: in the legacy
path the test additionally sets ``state.user_paused`` so the existing
guard fires; in the unified path the inhibitor alone is enough.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from src.config import FeatureFlags
from src.inhibitor import InhibitorType, WorkInhibitor
from src.models import (
    CIStatus,
    PipelineState,
    PRInfo,
    QueueTask,
    ReviewStatus,
    TaskStatus,
)

from tests.runner import _helpers as h


def _future() -> datetime:
    return datetime.now(timezone.utc) + timedelta(minutes=30)


def _inhibitor(kind: InhibitorType) -> WorkInhibitor:
    per_coder = kind == InhibitorType.RATE_LIMIT
    return WorkInhibitor(
        inhibitor_type=kind,
        coder_affected="claude" if per_coder else None,
        expires_at=_future() if per_coder else None,
        reason_text=f"{kind.value} test fixture",
        source_key=f"test:{kind.value}",
    )


def _inhibitors_for(kind: InhibitorType) -> list[WorkInhibitor]:
    """Return inhibitors that block every registered coder.

    ``RATE_LIMIT`` is per-coder, so it must be seeded for both
    ``claude`` and ``codex`` to actually halt IDLE dispatch — the
    unified gate falls through whenever any coder is still eligible.
    All other inhibitor types are global, so a single entry is enough.
    """
    if kind == InhibitorType.RATE_LIMIT:
        return [
            WorkInhibitor(
                inhibitor_type=kind,
                coder_affected="claude",
                expires_at=_future(),
                reason_text="claude rate-limited",
                source_key="state:test.rate_limited_coder_until.claude",
            ),
            WorkInhibitor(
                inhibitor_type=kind,
                coder_affected="codex",
                expires_at=_future(),
                reason_text="codex rate-limited",
                source_key="state:test.rate_limited_coder_until.codex",
            ),
        ]
    return [_inhibitor(kind)]


def _enable_flag(runner: Any) -> None:
    runner.repo_config = runner.repo_config.model_copy(
        update={
            "feature_flags": FeatureFlags(use_unified_inhibitor_check=True),
        }
    )


def _disable_flag(runner: Any) -> None:
    runner.repo_config = runner.repo_config.model_copy(
        update={"feature_flags": FeatureFlags(use_unified_inhibitor_check=False)}
    )


def _stub_dispatchable_world(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub git + GitHub layer so handle_idle can reach dispatch."""
    h._patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-330a-test",
        title="Dispatch when no inhibitor",
        status=TaskStatus.TODO,
        branch="pr-330a-test",
    )
    h._stub_dag_select(monkeypatch, task=task)

    async def fake_run_auto_pr(
        path: str, *_args: object, **_kwargs: object
    ) -> tuple[int, str, str]:
        return (0, "ok", "")

    monkeypatch.setattr(
        h.claude_cli, "run_auto_pr_async", fake_run_auto_pr
    )

    opened = PRInfo(
        number=99,
        branch="pr-330a-test",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )
    call_count = {"n": 0}

    def _get_open_prs(repo: str, **_kwargs: Any) -> list[PRInfo]:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return []
        return [opened]

    monkeypatch.setattr("src.github.prs.get_open_prs", _get_open_prs)
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )


_THROTTLE_TYPES = list(InhibitorType)


@pytest.mark.parametrize("kind", _THROTTLE_TYPES, ids=[k.value for k in _THROTTLE_TYPES])
def test_handle_idle_flag_off_uses_legacy_user_pause_check(
    kind: InhibitorType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Legacy path returns on ``state.user_paused`` for every scenario.

    The legacy IDLE guard is the ``user_paused`` short-circuit only; the
    other 7 throttle types are gated at the runner-cycle layer in
    ``_run_cycle_body`` and not by handle_idle. Each parametrized
    fixture therefore sets ``user_paused=True`` so the legacy branch
    fires identically across the 8 scenarios, giving a uniform
    no-dispatch baseline to compare against the unified path.
    """
    runner = h._make_runner()
    _disable_flag(runner)
    runner.state.user_paused = True
    runner.state.active_inhibitors = _inhibitors_for(kind)

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert not any(
        e["event"].startswith("[INFRA] IDLE inhibited by")
        for e in runner.state.history
    )


@pytest.mark.parametrize("kind", _THROTTLE_TYPES, ids=[k.value for k in _THROTTLE_TYPES])
def test_handle_idle_flag_on_uses_helper(
    kind: InhibitorType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unified path returns when every registered coder is blocked."""
    runner = h._make_runner()
    _enable_flag(runner)
    runner.state.active_inhibitors = _inhibitors_for(kind)

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert any(
        e["event"].startswith("[INFRA] IDLE inhibited by")
        and kind.value in e["event"]
        for e in runner.state.history
    )


def test_handle_idle_flag_off_uses_legacy_rate_limit_check_claude() -> None:
    runner = h._make_runner()
    _disable_flag(runner)
    runner.state.user_paused = True
    runner.state.active_inhibitors = [
        WorkInhibitor(
            inhibitor_type=InhibitorType.RATE_LIMIT,
            coder_affected="claude",
            expires_at=_future(),
            reason_text="claude rate-limited",
            source_key="state:octo__demo.rate_limited_coder_until.claude",
        )
    ]

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None


def test_handle_idle_flag_off_uses_legacy_rate_limit_check_codex() -> None:
    runner = h._make_runner()
    _disable_flag(runner)
    runner.state.user_paused = True
    runner.state.active_inhibitors = [
        WorkInhibitor(
            inhibitor_type=InhibitorType.RATE_LIMIT,
            coder_affected="codex",
            expires_at=_future(),
            reason_text="codex rate-limited",
            source_key="state:octo__demo.rate_limited_coder_until.codex",
        )
    ]

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None


def test_handle_idle_flag_on_per_coder_rate_limit_does_not_block_other_coder_claude(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A claude-only rate limit must leave codex eligible for dispatch.

    Mirrors the legacy ``selector._is_rate_limited`` per-coder filter:
    inhibitors scoped to a single coder cannot stop the IDLE handler
    when another coder is still runnable.
    """
    _stub_dispatchable_world(monkeypatch)
    runner = h._make_runner()
    _enable_flag(runner)
    runner.state.active_inhibitors = [
        WorkInhibitor(
            inhibitor_type=InhibitorType.RATE_LIMIT,
            coder_affected="claude",
            expires_at=_future(),
            reason_text="claude rate-limited",
            source_key="state:octo__demo.rate_limited_coder_until.claude",
        )
    ]

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.WATCH
    assert not any(
        e["event"].startswith("[INFRA] IDLE inhibited by")
        for e in runner.state.history
    )


def test_handle_idle_flag_on_per_coder_rate_limit_does_not_block_other_coder_codex(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A codex-only rate limit must leave claude eligible for dispatch."""
    _stub_dispatchable_world(monkeypatch)
    runner = h._make_runner()
    _enable_flag(runner)
    runner.state.active_inhibitors = [
        WorkInhibitor(
            inhibitor_type=InhibitorType.RATE_LIMIT,
            coder_affected="codex",
            expires_at=_future(),
            reason_text="codex rate-limited",
            source_key="state:octo__demo.rate_limited_coder_until.codex",
        )
    ]

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.WATCH
    assert not any(
        e["event"].startswith("[INFRA] IDLE inhibited by")
        for e in runner.state.history
    )


def test_handle_idle_flag_on_returns_when_every_coder_rate_limited() -> None:
    """Per-coder inhibitors for *all* coders collapse to a global block."""
    runner = h._make_runner()
    _enable_flag(runner)
    runner.state.active_inhibitors = [
        WorkInhibitor(
            inhibitor_type=InhibitorType.RATE_LIMIT,
            coder_affected="claude",
            expires_at=_future(),
            reason_text="claude rate-limited",
            source_key="state:octo__demo.rate_limited_coder_until.claude",
        ),
        WorkInhibitor(
            inhibitor_type=InhibitorType.RATE_LIMIT,
            coder_affected="codex",
            expires_at=_future(),
            reason_text="codex rate-limited",
            source_key="state:octo__demo.rate_limited_coder_until.codex",
        ),
    ]

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert any(
        e["event"].startswith("[INFRA] IDLE inhibited by")
        and "rate_limit" in e["event"]
        for e in runner.state.history
    )


def test_handle_idle_both_paths_emit_same_log_event() -> None:
    """Legacy and unified paths must both short-circuit on user-pause.

    The legacy path emits no event (silent ``return`` on
    ``state.user_paused``), and the unified path emits a single
    ``[INFRA] IDLE inhibited by ['user_pause']`` event. Both surface the
    same observable outcome to the runner — IDLE state preserved, no
    task dispatched — so the dashboard ``state`` field remains
    structurally equivalent across the flag flip.
    """
    legacy = h._make_runner()
    _disable_flag(legacy)
    legacy.state.user_paused = True
    legacy.state.active_inhibitors = _inhibitors_for(InhibitorType.USER_PAUSE)
    asyncio.run(legacy.handle_idle())

    unified = h._make_runner()
    _enable_flag(unified)
    unified.state.user_paused = True
    unified.state.active_inhibitors = _inhibitors_for(InhibitorType.USER_PAUSE)
    asyncio.run(unified.handle_idle())

    assert legacy.state.state == unified.state.state == PipelineState.IDLE
    assert legacy.state.current_task is unified.state.current_task is None
    assert any(
        e["event"].startswith("[INFRA] IDLE inhibited by")
        and "user_pause" in e["event"]
        for e in unified.state.history
    )


def test_handle_idle_flag_on_dispatches_when_no_inhibitor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_dispatchable_world(monkeypatch)
    runner = h._make_runner()
    _enable_flag(runner)
    runner.state.active_inhibitors = []

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 99


def test_handle_idle_flag_off_dispatches_when_no_inhibitor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_dispatchable_world(monkeypatch)
    runner = h._make_runner()
    _disable_flag(runner)
    runner.state.user_paused = False
    runner.state.active_inhibitors = []

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 99


def test_handle_idle_flag_on_pinned_task_short_circuits_when_pin_rate_limited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Task pin narrows the gate to the pinned coder only.

    Regression for the PR-330a review: when the active task pins
    ``claude`` and claude is rate-limited, the unified gate must
    short-circuit IDLE rather than letting dispatch proceed and
    parking the pinned task in ERROR as "coder unavailable". The
    legacy gate iterated over every registered coder, so codex
    appearing unblocked masked the per-coder claude rate limit.
    """
    runner = h._make_runner()
    monkeypatch.setattr(runner, "_active_task_coder_pin", lambda: "claude")
    _enable_flag(runner)
    runner.state.active_inhibitors = [
        WorkInhibitor(
            inhibitor_type=InhibitorType.RATE_LIMIT,
            coder_affected="claude",
            expires_at=_future(),
            reason_text="claude rate-limited",
            source_key="state:octo__demo.rate_limited_coder_until.claude",
        )
    ]

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert any(
        e["event"].startswith("[INFRA] IDLE inhibited by")
        and "rate_limit" in e["event"]
        for e in runner.state.history
    )


def test_handle_idle_flag_on_disabled_coders_short_circuits_on_remaining_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``disabled_coders`` narrows the gate to the runnable coder.

    Regression for the PR-330a review: with codex disabled in the
    repo config and claude rate-limited, the only candidate dispatch
    would consider is claude — which is blocked — so the unified gate
    must short-circuit. The pre-fix gate iterated over every
    registered coder and saw codex unblocked, letting IDLE proceed.
    """
    runner = h._make_runner(disabled_coders=["codex"])
    _enable_flag(runner)
    runner.state.active_inhibitors = [
        WorkInhibitor(
            inhibitor_type=InhibitorType.RATE_LIMIT,
            coder_affected="claude",
            expires_at=_future(),
            reason_text="claude rate-limited",
            source_key="state:octo__demo.rate_limited_coder_until.claude",
        )
    ]

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert any(
        e["event"].startswith("[INFRA] IDLE inhibited by")
        and "rate_limit" in e["event"]
        for e in runner.state.history
    )


def test_handle_idle_flag_on_short_circuits_when_no_candidate_coders() -> None:
    """All registered coders disabled in the repo → IDLE inhibited.

    Without any candidate coder, the gate must short-circuit rather
    than proceed to dispatch (which would otherwise try the legacy
    fallback in ``_get_coder`` and ultimately fail downstream).
    """
    runner = h._make_runner(disabled_coders=["claude", "codex"])
    _enable_flag(runner)
    runner.state.active_inhibitors = []

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert any(
        e["event"] == "[INFRA] IDLE inhibited by no eligible coder"
        for e in runner.state.history
    )


def test_handle_idle_flag_on_pinned_task_proceeds_when_pin_unblocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A claude-pinned task with codex rate-limited still dispatches.

    The gate must not surface codex's per-coder rate limit when the
    task pin makes codex irrelevant to dispatch.
    """
    _stub_dispatchable_world(monkeypatch)
    runner = h._make_runner()
    monkeypatch.setattr(runner, "_active_task_coder_pin", lambda: "claude")
    _enable_flag(runner)
    runner.state.active_inhibitors = [
        WorkInhibitor(
            inhibitor_type=InhibitorType.RATE_LIMIT,
            coder_affected="codex",
            expires_at=_future(),
            reason_text="codex rate-limited",
            source_key="state:octo__demo.rate_limited_coder_until.codex",
        )
    ]

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.WATCH
    assert not any(
        e["event"].startswith("[INFRA] IDLE inhibited by")
        for e in runner.state.history
    )
