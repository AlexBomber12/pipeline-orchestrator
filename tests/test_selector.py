from __future__ import annotations

import random
import re
from datetime import datetime, timedelta, timezone

from src.coder_registry import CoderRegistry
from src.config import AppConfig, DaemonConfig, RepoConfig, CoderType
from src.daemon.selector import (
    SelectionContext,
    eligible_coders,
    rank_coders,
    select_coder,
)
from src.models import RepoState


class _Plugin:
    def __init__(self, name: str, status: str = "ok") -> None:
        self.name = name
        self.display_name = name.title()
        self.models = [""]
        self._status = status

    async def run_planned_pr(
        self, repo_path: str, model: str | None, timeout: int, **kwargs: object
    ) -> tuple[int, str, str]:
        return (0, repo_path, model or str(timeout))

    async def fix_review(
        self, repo_path: str, model: str | None, timeout: int | None = None, **kwargs: object
    ) -> tuple[int, str, str]:
        return (0, repo_path, model or str(timeout))

    def check_auth(self) -> dict[str, str]:
        if self._status == "raise":
            raise RuntimeError("boom")
        return {"status": self._status}

    def create_usage_provider(self, **kwargs: object) -> None:
        return None

    def rate_limit_patterns(self) -> list[re.Pattern[str]]:
        return [re.compile("limit")]


def _ctx(
    *,
    auto_fallback: bool = True,
    repo_coder: CoderType | None = None,
    daemon_coder: CoderType = CoderType.CLAUDE,
    disabled_coders: list[str] | None = None,
    priorities: dict[str, int] | None = None,
    epsilon: float = 0.15,
    limited: set[str] | None = None,
    auth: dict[str, str] | None = None,
    seed: int = 1,
) -> SelectionContext:
    registry = CoderRegistry()
    auth = auth or {}
    for name in ("claude", "codex", "gemini"):
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
            auto_fallback=auto_fallback,
            coder_priority=priorities or {"claude": 70, "codex": 80, "gemini": 60},
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
    )


def test_single_eligible_returns_self() -> None:
    ctx = _ctx(limited={"claude", "gemini"})

    assert eligible_coders(ctx) == ["codex"]
    assert rank_coders(["codex"], ctx) == ["codex"]


def test_rate_limited_excluded() -> None:
    ctx = _ctx(limited={"codex"})

    assert "codex" not in eligible_coders(ctx)


def test_expired_per_coder_window_does_not_block_selection() -> None:
    ctx = _ctx(limited={"claude", "codex"})
    ctx.state.rate_limited_coder_until = {
        "claude": datetime.now(timezone.utc) - timedelta(minutes=1),
        "codex": datetime.now(timezone.utc) + timedelta(minutes=5),
    }

    eligible = eligible_coders(ctx)

    assert "claude" in eligible
    assert "codex" not in eligible


def test_auth_failed_excluded() -> None:
    ctx = _ctx(auth={"codex": "failed"})

    assert "codex" not in eligible_coders(ctx)


def test_auth_error_excluded() -> None:
    ctx = _ctx(auth={"codex": "error"})

    assert "codex" not in eligible_coders(ctx)


def test_cached_auth_statuses_avoid_hot_path_probes() -> None:
    ctx = _ctx()
    ctx.auth_statuses = {
        "claude": {"status": "ok"},
        "codex": {"status": "error"},
        "gemini": {"status": "ok"},
    }

    def boom() -> dict[str, str]:
        raise AssertionError("selector should use cached auth status")

    ctx.registry.get("codex").check_auth = boom  # type: ignore[method-assign]

    assert "codex" not in eligible_coders(ctx)


def test_pinned_without_fallback_returns_only_pinned() -> None:
    ctx = _ctx(auto_fallback=False, repo_coder=CoderType.CODEX, limited={"codex"})

    assert eligible_coders(ctx) == ["codex"]


def test_pinned_with_fallback_returns_pinned_first_then_others() -> None:
    ctx = _ctx(auto_fallback=True, repo_coder=CoderType.CODEX)

    assert eligible_coders(ctx) == ["codex", "claude", "gemini"]


def test_priority_ranks_higher_first_when_no_exploration() -> None:
    ctx = _ctx(
        daemon_coder=CoderType.CODEX,
        priorities={"claude": 50, "codex": 90, "gemini": 80},
        epsilon=0.0,
    )

    assert rank_coders(["claude", "gemini", "codex"], ctx) == [
        "codex",
        "gemini",
        "claude",
    ]


def test_disabled_for_repo_excluded() -> None:
    ctx = _ctx(disabled_coders=["gemini"])

    assert "gemini" not in eligible_coders(ctx)


def test_no_eligible_returns_none() -> None:
    ctx = _ctx(limited={"claude", "codex", "gemini"})

    assert select_coder(ctx) is None


def test_new_plugin_added_without_selector_changes() -> None:
    ctx = _ctx()
    ctx.registry.register(_Plugin("qwen"))
    ctx.app_config.daemon.coder_priority["qwen"] = 75

    assert "qwen" in eligible_coders(ctx)


def test_epsilon_zero_always_greedy() -> None:
    ctx = _ctx(
        daemon_coder=CoderType.CODEX,
        priorities={"claude": 20, "codex": 90, "gemini": 30},
        epsilon=0.0,
    )

    orders = [rank_coders(["claude", "codex", "gemini"], ctx) for _ in range(10)]
    assert all(order[0] == "codex" for order in orders)


def test_epsilon_one_always_explores() -> None:
    ctx = _ctx(
        daemon_coder=CoderType.CODEX,
        priorities={"claude": 20, "codex": 90, "gemini": 30},
        epsilon=0.5,
        seed=2,
    )
    ctx.app_config.daemon.exploration_epsilon = 1.0

    orders = [rank_coders(["claude", "codex", "gemini"], ctx) for _ in range(10)]
    assert all(order[0] != "codex" for order in orders)


def test_epsilon_seed_deterministic() -> None:
    left = _ctx(
        daemon_coder=CoderType.CODEX,
        priorities={"claude": 20, "codex": 90, "gemini": 30},
        epsilon=0.4,
        seed=7,
    )
    right = _ctx(
        daemon_coder=CoderType.CODEX,
        priorities={"claude": 20, "codex": 90, "gemini": 30},
        epsilon=0.4,
        seed=7,
    )

    left_orders = [
        rank_coders(["claude", "codex", "gemini"], left) for _ in range(8)
    ]
    right_orders = [
        rank_coders(["claude", "codex", "gemini"], right) for _ in range(8)
    ]

    assert left_orders == right_orders
