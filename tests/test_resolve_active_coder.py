from __future__ import annotations

import random
import re

import pytest
from src.coder_registry import CoderRegistry
from src.config import AppConfig, CoderType, DaemonConfig, RepoConfig
from src.daemon import selector as selector_module
from src.daemon.selector import (
    CoderPurpose,
    SelectionContext,
    resolve_active_coder,
    resolve_pause_coder,
    select_coder,
)
from src.models import RepoState


class _Plugin:
    def __init__(self, name: str, status: str = "ok") -> None:
        self.name = name
        self.display_name = name.title()
        self.models = [""]
        self._status = status

    async def run_planned_pr(self, *args: object, **kwargs: object) -> tuple[int, str, str]:
        return (0, "", "")

    async def run_auto_pr(self, *args: object, **kwargs: object) -> tuple[int, str, str]:
        return (0, "", "")

    async def fix_review(self, *args: object, **kwargs: object) -> tuple[int, str, str]:
        return (0, "", "")

    def check_auth(self) -> dict[str, str]:
        return {"status": self._status}

    def create_usage_provider(self, **kwargs: object) -> None:
        return None

    def rate_limit_patterns(self) -> list[re.Pattern[str]]:
        return [re.compile("limit")]


def _ctx(
    *,
    repo_coder: CoderType | None = None,
    daemon_coder: CoderType = CoderType.CLAUDE,
    disabled_coders: list[str] | None = None,
    priorities: dict[str, int] | None = None,
    epsilon: float = 0.0,
    seed: int = 1,
    limited: set[str] | None = None,
    auth: dict[str, str] | None = None,
) -> SelectionContext:
    registry = CoderRegistry()
    auth = auth or {}
    for name in ("claude", "codex"):
        registry.register(_Plugin(name, status=auth.get(name, "ok")))
    repo = RepoConfig(
        url="https://github.com/octo/demo.git",
        coder=repo_coder,
        disabled_coders=disabled_coders,
    )
    app = AppConfig(
        repositories=[],
        daemon=DaemonConfig(
            coder=daemon_coder,
            auto_fallback=True,
            coder_priority=priorities or {"claude": 70, "codex": 80},
            exploration_epsilon=epsilon,
        ),
    )
    state = RepoState(
        url=repo.url,
        name="octo__demo",
        rate_limited_coders=limited or set(),
    )
    return SelectionContext(
        registry=registry,
        repo_config=repo,
        app_config=app,
        state=state,
        rng=random.Random(seed),
        auth_statuses={name: {"status": "ok"} for name in ("claude", "codex")},
    )


def test_dispatch_respects_spec_pin() -> None:
    ctx = _ctx(repo_coder=CoderType.CODEX, priorities={"claude": 100, "codex": 1})

    resolution = resolve_active_coder(ctx, purpose=CoderPurpose.DISPATCH)

    assert resolution is not None
    assert resolution.name == "codex"
    assert resolution.reason == "pinned"


def test_dispatch_fallback_when_pin_ineligible() -> None:
    ctx = _ctx(repo_coder=CoderType.CODEX, limited={"codex"})

    resolution = resolve_active_coder(ctx, purpose=CoderPurpose.DISPATCH)

    assert resolution is not None
    assert resolution.name == "claude"
    assert resolution.reason == "fallback"


def test_auxiliary_claude_first() -> None:
    ctx = _ctx(
        daemon_coder=CoderType.CODEX,
        priorities={"claude": 1, "codex": 100},
        epsilon=0.5,
    )

    resolution = resolve_active_coder(ctx, purpose=CoderPurpose.AUXILIARY)

    assert resolution is not None
    assert resolution.name == "claude"
    assert resolution.reason == "ranked"
    ctx.state.rate_limited_coders.add("claude")
    fallback = resolve_active_coder(ctx, purpose=CoderPurpose.AUXILIARY)
    assert fallback is not None
    assert fallback.name == "codex"


def test_auxiliary_reason_respects_explicit_pin() -> None:
    ctx = _ctx()
    ctx.task_coder_pin = "codex"

    resolution = resolve_active_coder(ctx, purpose=CoderPurpose.AUXILIARY)

    assert resolution is not None
    assert resolution.name == "codex"
    assert resolution.reason == "pinned"


def test_display_matches_dispatch() -> None:
    ctx = _ctx(epsilon=0.5, seed=1)
    before = ctx.rng.getstate()

    display = resolve_active_coder(ctx, purpose=CoderPurpose.DISPLAY)

    assert ctx.rng.getstate() == before
    dispatch = resolve_active_coder(ctx, purpose=CoderPurpose.DISPATCH)
    assert display is not None
    assert dispatch is not None
    assert display.name == dispatch.name


def test_pause_coder_not_hardcoded_claude() -> None:
    ctx = _ctx(repo_coder=CoderType.CODEX, disabled_coders=["claude"])

    resolution = resolve_pause_coder(ctx)

    assert resolution.name == "codex"


def test_display_unknown_state_coder_has_no_plugin() -> None:
    ctx = _ctx()
    ctx.state.coder = "ghost"

    resolution = resolve_active_coder(ctx, purpose=CoderPurpose.DISPLAY)

    assert resolution is not None
    assert resolution.name == "ghost"
    assert resolution.plugin is None


def test_resolution_carries_reason() -> None:
    ctx = _ctx(priorities={"claude": 1, "codex": 100})

    resolution = resolve_active_coder(ctx, purpose=CoderPurpose.DISPATCH)

    assert resolution is not None
    assert resolution.reason in {"pinned", "ranked", "fallback", "exploration"}
    assert resolution.reason == "ranked"


def test_all_migrated_sites_agree() -> None:
    ctx = _ctx(repo_coder=CoderType.CODEX, disabled_coders=["claude"])

    dispatch = resolve_active_coder(ctx, purpose=CoderPurpose.DISPATCH)
    display = resolve_active_coder(ctx, purpose=CoderPurpose.DISPLAY)
    pause = resolve_pause_coder(ctx)

    assert dispatch is not None
    assert display is not None
    assert {dispatch.name, display.name, pause.name} == {"codex"}


def test_equivalence_with_select_coder() -> None:
    for limited in (set(), {"claude"}, {"codex"}):
        ctx = _ctx(limited=limited)
        resolution = resolve_active_coder(ctx, purpose=CoderPurpose.DISPATCH)
        selected = select_coder(ctx)
        assert (resolution.name if resolution else None) == (
            selected[0] if selected else None
        )


def test_no_new_selection_behavior(monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = _ctx()
    calls: list[str] = []

    def fake_eligible(_: SelectionContext) -> list[str]:
        calls.append("eligible")
        return ["claude", "codex"]

    def fake_rank(eligible: list[str], _: SelectionContext) -> list[str]:
        calls.append("rank")
        return list(reversed(eligible))

    monkeypatch.setattr(selector_module, "eligible_coders", fake_eligible)
    monkeypatch.setattr(selector_module, "rank_coders", fake_rank)

    resolution = resolve_active_coder(ctx, purpose=CoderPurpose.DISPATCH)

    assert resolution is not None
    assert resolution.name == "codex"
    assert calls == ["eligible", "rank"]
