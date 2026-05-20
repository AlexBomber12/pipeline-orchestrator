"""PR-330c: WATCH inhibitor migration tests."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from src.config import FeatureFlags
from src.daemon.github_rate_limit import write_budget
from src.inhibitor import (
    InhibitorType,
    WorkInhibitor,
    derive_active_inhibitors,
)
from src.models import CIStatus, PipelineState, PRInfo, ReviewStatus
from tests.runner import _helpers as h


def _future() -> datetime:
    return datetime.now(timezone.utc) + timedelta(minutes=30)


def _set_flag(runner: Any, enabled: bool) -> None:
    runner.repo_config = runner.repo_config.model_copy(
        update={
            "feature_flags": FeatureFlags(
                use_unified_inhibitor_check=enabled
            )
        }
    )


def _github_budget(kind: InhibitorType) -> WorkInhibitor:
    return WorkInhibitor(
        inhibitor_type=kind,
        expires_at=_future(),
        reason_text=f"{kind.value} test fixture",
        source_key="github_rate_limit_budget",
    )


def _seed_github_budget(runner: Any, remaining: int, limit: int = 5000) -> None:
    budget = h._budget(remaining=remaining, limit=limit)
    h._set_budget(runner, budget)
    asyncio.run(write_budget(runner.redis, budget))


def _rate_limit(coder: str) -> WorkInhibitor:
    return WorkInhibitor(
        inhibitor_type=InhibitorType.RATE_LIMIT,
        coder_affected=coder,
        expires_at=_future(),
        reason_text=f"{coder} rate-limited",
        source_key=f"state:test.rate_limited_coder_until.{coder}",
    )


async def _run_cycle_without_repo_io(runner: Any) -> None:
    async def _noop() -> None:
        return None

    runner.ensure_repo_cloned = _noop  # type: ignore[method-assign]
    runner.publish_state = _noop  # type: ignore[method-assign]
    await runner._run_cycle_body()


def test_handle_watch_skips_polling_when_github_budget_pause_legacy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    _set_flag(runner, False)
    runner.state.state = PipelineState.WATCH
    runner.app_config.daemon.github_api_pause_threshold_percent = 5
    h._set_budget(runner, h._budget(remaining=150, limit=5000))  # 3%
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda *_args, **_kwargs: pytest.fail("GitHub API was polled"),
    )

    asyncio.run(_run_cycle_without_repo_io(runner))

    assert runner._github_api_pause_attempts == 1


def test_handle_watch_skips_polling_when_github_budget_pause_unified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    _set_flag(runner, True)
    runner.state.state = PipelineState.WATCH
    _seed_github_budget(runner, remaining=150)  # 3%
    runner.state.active_inhibitors = [
        _github_budget(InhibitorType.GITHUB_BUDGET_PAUSE)
    ]
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda *_args, **_kwargs: pytest.fail("GitHub API was polled"),
    )

    asyncio.run(_run_cycle_without_repo_io(runner))

    assert runner._github_api_pause_attempts == 1


def test_handle_watch_unified_refreshes_budget_before_inhibitor_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    _set_flag(runner, True)
    runner.state.state = PipelineState.WATCH
    runner.app_config.daemon.github_api_pause_threshold_percent = 5
    runner.app_config.daemon.github_api_slowdown_threshold_percent = 20
    runner.state.active_inhibitors = []
    fetched = h._budget(remaining=150, limit=5000)  # 3%
    monkeypatch.setattr(
        "src.github.rate_limit.fetch_rate_limit_buckets",
        lambda: (fetched, None),
    )

    proceed = asyncio.run(runner._check_github_api_budget())

    assert proceed is False
    assert runner._github_api_pause_attempts == 1
    assert [
        inh.inhibitor_type for inh in runner.state.active_inhibitors
    ] == [InhibitorType.GITHUB_BUDGET_PAUSE]


@pytest.mark.parametrize(
    ("remaining", "expected", "proceed"),
    [
        (150, InhibitorType.GITHUB_BUDGET_PAUSE, False),
        (500, InhibitorType.GITHUB_BUDGET_SLOWDOWN, True),
        (4500, None, True),
    ],
)
def test_handle_watch_unified_uses_refreshed_budget_when_redis_read_misses(
    remaining: int,
    expected: InhibitorType | None,
    proceed: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    _set_flag(runner, True)
    runner.state.state = PipelineState.WATCH
    runner.app_config.daemon.github_api_pause_threshold_percent = 5
    runner.app_config.daemon.github_api_slowdown_threshold_percent = 20
    fetched = h._budget(remaining=remaining, limit=5000)
    monkeypatch.setattr(
        "src.github.rate_limit.fetch_rate_limit_buckets",
        lambda: (fetched, None),
    )

    async def _missing(_key: str) -> None:
        return None

    runner.redis.get = _missing  # type: ignore[method-assign]

    assert asyncio.run(runner._check_github_api_budget()) is proceed

    budget_types = [
        inh.inhibitor_type
        for inh in runner.state.active_inhibitors
        if inh.inhibitor_type
        in (
            InhibitorType.GITHUB_BUDGET_PAUSE,
            InhibitorType.GITHUB_BUDGET_SLOWDOWN,
        )
    ]
    assert budget_types == ([] if expected is None else [expected])


def test_handle_watch_unified_refreshed_budget_overrides_stale_inhibitor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    _set_flag(runner, True)
    runner.state.state = PipelineState.WATCH
    runner.app_config.daemon.github_api_pause_threshold_percent = 5
    runner.app_config.daemon.github_api_slowdown_threshold_percent = 20
    stale = _github_budget(InhibitorType.GITHUB_BUDGET_SLOWDOWN)
    fetched = h._budget(remaining=150, limit=5000)  # 3%
    monkeypatch.setattr(
        "src.github.rate_limit.fetch_rate_limit_buckets",
        lambda: (fetched, None),
    )

    async def _stale_inhibitors(*_args: object, **_kwargs: object) -> list[WorkInhibitor]:
        return [stale]

    monkeypatch.setattr(
        "src.daemon.runner.derive_active_inhibitors",
        _stale_inhibitors,
    )

    assert asyncio.run(runner._check_github_api_budget()) is False

    budget_types = [
        inh.inhibitor_type
        for inh in runner.state.active_inhibitors
        if inh.inhibitor_type
        in (
            InhibitorType.GITHUB_BUDGET_PAUSE,
            InhibitorType.GITHUB_BUDGET_SLOWDOWN,
        )
    ]
    assert budget_types == [InhibitorType.GITHUB_BUDGET_PAUSE]


def test_handle_watch_unified_budget_check_tolerates_derive_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    _set_flag(runner, True)
    runner.state.state = PipelineState.WATCH
    runner.app_config.daemon.github_api_pause_threshold_percent = 5
    runner.app_config.daemon.github_api_slowdown_threshold_percent = 20
    fetched = h._budget(remaining=150, limit=5000)  # 3%
    monkeypatch.setattr(
        "src.github.rate_limit.fetch_rate_limit_buckets",
        lambda: (fetched, None),
    )

    async def _raise(*_args: object, **_kwargs: object) -> list[WorkInhibitor]:
        raise RuntimeError("redis unavailable")

    monkeypatch.setattr("src.daemon.runner.derive_active_inhibitors", _raise)

    assert asyncio.run(runner._check_github_api_budget()) is False
    assert [
        inh.inhibitor_type for inh in runner.state.active_inhibitors
    ] == [InhibitorType.GITHUB_BUDGET_PAUSE]


def test_handle_watch_slowdown_multiplier_at_5_pct_legacy() -> None:
    runner = h._make_runner(poll_interval_sec=60)
    _set_flag(runner, False)
    h._configure_watch_adaptive_defaults(runner)
    runner.state.state = PipelineState.WATCH
    runner._watch_entered_at = (
        datetime.now(timezone.utc) - timedelta(minutes=10)
    )
    runner.app_config.daemon.github_api_pause_threshold_percent = 5
    runner.app_config.daemon.github_api_slowdown_threshold_percent = 20
    runner.app_config.daemon.github_api_slowdown_multiplier = 5
    h._set_budget(runner, h._budget(remaining=500, limit=5000))  # 10%

    assert asyncio.run(runner._check_github_api_budget()) is True

    assert runner._github_api_slowdown_attempts == 1
    assert runner.effective_watch_poll_interval == 300


def test_handle_watch_slowdown_multiplier_at_5_pct_unified() -> None:
    runner = h._make_runner(poll_interval_sec=60)
    _set_flag(runner, True)
    h._configure_watch_adaptive_defaults(runner)
    runner.state.state = PipelineState.WATCH
    runner._watch_entered_at = (
        datetime.now(timezone.utc) - timedelta(minutes=10)
    )
    runner.app_config.daemon.github_api_slowdown_multiplier = 5
    _seed_github_budget(runner, remaining=500)  # 10%
    runner.state.active_inhibitors = [
        _github_budget(InhibitorType.GITHUB_BUDGET_SLOWDOWN)
    ]

    assert asyncio.run(runner._check_github_api_budget()) is True

    assert runner._github_api_slowdown_attempts == 1
    assert runner.effective_watch_poll_interval == 300


@pytest.mark.parametrize(
    ("github_pct", "expected"),
    [
        (4.9, InhibitorType.GITHUB_BUDGET_PAUSE),
        (5.0, InhibitorType.GITHUB_BUDGET_SLOWDOWN),
        (5.1, InhibitorType.GITHUB_BUDGET_SLOWDOWN),
        (19.9, InhibitorType.GITHUB_BUDGET_SLOWDOWN),
        (20.0, None),
        (20.1, None),
    ],
)
@pytest.mark.parametrize("flag", [False, True])
def test_handle_watch_github_budget_boundaries(
    github_pct: float,
    expected: InhibitorType | None,
    flag: bool,
) -> None:
    runner = h._make_runner()
    _set_flag(runner, flag)
    runner.state.state = PipelineState.WATCH
    runner.app_config.daemon.github_api_pause_threshold_percent = 5
    runner.app_config.daemon.github_api_slowdown_threshold_percent = 20
    budget = h._budget(remaining=int(github_pct * 100), limit=10000)

    if flag:
        asyncio.run(write_budget(runner.redis, budget))
        h._set_budget(runner, budget)
        runner.state.active_inhibitors = asyncio.run(
            derive_active_inhibitors(
                runner.state,
                runner.redis,
                runner.app_config.daemon,
            )
        )
    else:
        h._set_budget(runner, budget)

    proceed = asyncio.run(runner._check_github_api_budget())

    if expected == InhibitorType.GITHUB_BUDGET_PAUSE:
        assert proceed is False
        assert runner._github_api_pause_attempts == 1
    elif expected == InhibitorType.GITHUB_BUDGET_SLOWDOWN:
        assert proceed is True
        assert runner._github_api_slowdown_attempts == 1
    else:
        assert proceed is True
        assert runner._github_api_pause_attempts == 0
        assert runner._github_api_slowdown_attempts == 0


def test_handle_watch_unified_budget_resets_pause_counter() -> None:
    runner = h._make_runner()
    _set_flag(runner, True)
    runner._github_api_pause_attempts = 1

    assert asyncio.run(runner._check_github_api_budget()) is True

    assert runner._github_api_pause_attempts == 0


def test_handle_watch_unified_slowdown_skips_non_watch_cycles() -> None:
    runner = h._make_runner()
    _set_flag(runner, True)
    runner.state.state = PipelineState.IDLE
    runner.app_config.daemon.github_api_slowdown_multiplier = 5
    _seed_github_budget(runner, remaining=500)  # 10%
    runner.state.active_inhibitors = [
        _github_budget(InhibitorType.GITHUB_BUDGET_SLOWDOWN)
    ]

    decisions = [
        asyncio.run(runner._check_github_api_budget()) for _ in range(2)
    ]

    assert decisions == [True, False]
    assert runner._github_api_slowdown_cycle == 2


def test_handle_watch_unified_budget_resets_slowdown_counter() -> None:
    runner = h._make_runner()
    _set_flag(runner, True)
    runner._github_api_slowdown_attempts = 2
    runner._github_api_slowdown_cycle = 1

    assert asyncio.run(runner._check_github_api_budget()) is True

    assert runner._github_api_slowdown_attempts == 0
    assert runner._github_api_slowdown_cycle == 0


def _seed_stale_review(runner: Any, coder: str) -> list[int]:
    runner.state.state = PipelineState.WATCH
    runner.state.coder = coder
    runner.state.current_pr = PRInfo(
        number=42,
        branch="pr-042",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        head_sha="abc1234",
        last_activity=datetime.now(timezone.utc) - timedelta(minutes=30),
    )
    runner.app_config.daemon.stale_review_threshold_min = 10
    runner.app_config.daemon.watch_retrigger_cap = 3

    posted: list[int] = []

    def fake_post(
        number: int,
        **_kwargs: object,
    ) -> tuple[bool, bool, datetime | None]:
        posted.append(number)
        return True, True, None

    runner._post_codex_review_result = fake_post  # type: ignore[method-assign]
    return posted


def test_handle_watch_skips_retrigger_when_coder_rate_limited_legacy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    _set_flag(runner, False)
    posted = _seed_stale_review(runner, "claude")
    runner.state.rate_limited_coder_until["claude"] = _future()
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda *_args, **_kwargs: 60 * 30,
    )

    result = asyncio.run(runner._maybe_retrigger_stale_review(42))

    assert result is False
    assert posted == []


def test_handle_watch_skips_retrigger_when_coder_rate_limited_unified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    _set_flag(runner, True)
    posted = _seed_stale_review(runner, "claude")
    runner.state.active_inhibitors = [_rate_limit("claude")]
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda *_args, **_kwargs: 60 * 30,
    )

    result = asyncio.run(runner._maybe_retrigger_stale_review(42))

    assert result is False
    assert posted == []


@pytest.mark.parametrize("flag", [False, True])
def test_handle_watch_allows_retrigger_for_unaffected_coder(
    flag: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    _set_flag(runner, flag)
    posted = _seed_stale_review(runner, "codex")
    if flag:
        runner.state.active_inhibitors = [_rate_limit("claude")]
    else:
        runner.state.rate_limited_coder_until["claude"] = _future()
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda *_args, **_kwargs: 60 * 30,
    )

    result = asyncio.run(runner._maybe_retrigger_stale_review(42))

    assert result is True
    assert posted == [42]


def test_handle_watch_skips_bot_error_retrigger_when_coder_rate_limited_unified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    _set_flag(runner, True)
    runner.state.coder = "claude"
    runner.state.active_inhibitors = [_rate_limit("claude")]
    posted: list[int] = []
    runner._post_codex_review_result = (  # type: ignore[method-assign]
        lambda number, **_kwargs: (posted.append(number) is None, True, None)
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda *_args, **_kwargs: [
            {
                "user": {"login": "chatgpt-codex-connector[bot]"},
                "body": "Something went wrong. Try again",
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        ],
    )

    result = runner._maybe_retrigger_on_codex_bot_error(42)

    assert result is False
    assert posted == []


def test_watch_retrigger_legacy_normalizes_naive_per_coder_until() -> None:
    runner = h._make_runner()
    _set_flag(runner, False)
    runner.state.rate_limited_coder_until["claude"] = datetime.now() + timedelta(
        minutes=30
    )

    assert runner._watch_retrigger_inhibited("claude") is True


def test_watch_retrigger_legacy_uses_global_claude_rate_limit() -> None:
    runner = h._make_runner()
    _set_flag(runner, False)
    runner.state.rate_limited_until = datetime.now() + timedelta(minutes=30)
    runner.state.rate_limit_reactive_coder = None

    assert runner._watch_retrigger_inhibited("claude") is True


def test_watch_retrigger_legacy_uses_reactive_coder_marker() -> None:
    runner = h._make_runner()
    _set_flag(runner, False)
    runner.state.rate_limit_reactive_coder = "codex"

    assert runner._watch_retrigger_inhibited("codex") is True
