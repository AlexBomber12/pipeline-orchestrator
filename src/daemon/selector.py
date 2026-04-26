"""Pure coder-selection helpers.

The selector only depends on registry/config/state inputs passed in via
``SelectionContext`` so ranking decisions stay deterministic in tests
and easy to evolve independently from the runner lifecycle.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timezone

from src.coder_registry import CoderPlugin, CoderRegistry
from src.config import AppConfig, CoderType, RepoConfig
from src.models import RepoState

_RUNTIME_SUPPORTED_CODERS = {coder.value for coder in CoderType}


@dataclass
class SelectionContext:
    registry: CoderRegistry
    repo_config: RepoConfig
    app_config: AppConfig
    state: RepoState
    rng: random.Random
    auth_statuses: dict[str, dict[str, str]] | None = None
    # Hard pin from the active task header (`Coder:` field).
    # ``"claude"`` / ``"codex"`` short-circuit selection; ``"any"`` and
    # ``None`` defer to repo / global defaults so unpinned tasks keep
    # the legacy auto_fallback behavior.
    task_coder_pin: str | None = None


def eligible_coders(ctx: SelectionContext) -> list[str]:
    """Return the currently runnable coder names in preference order."""
    task_pin = ctx.task_coder_pin
    if task_pin in ("claude", "codex"):
        if _coder_runtime_ok(task_pin, ctx):
            return [task_pin]
        return []

    pinned = ctx.repo_config.coder
    preferred = pinned or ctx.app_config.daemon.coder
    if not ctx.app_config.daemon.auto_fallback:
        return [preferred.value]

    result = [
        name
        for name in ctx.registry.coder_names()
        if _coder_runtime_ok(name, ctx)
    ]

    if pinned is not None and pinned.value in result:
        return [pinned.value] + [name for name in result if name != pinned.value]
    return result


def _coder_runtime_ok(name: str, ctx: SelectionContext) -> bool:
    """Return True when *name* passes every per-coder runtime gate."""
    return (
        _supports_runtime(name)
        and not _is_rate_limited(name, ctx.state)
        and not _auth_failed(name, ctx.registry, ctx.auth_statuses)
        and not _is_disabled_for_repo(name, ctx.repo_config)
    )


def rank_coders(eligible: list[str], ctx: SelectionContext) -> list[str]:
    """Return eligible coders ordered by priority or exploration choice."""
    if len(eligible) <= 1:
        return list(eligible)

    greedy = _greedy_order(eligible, ctx)
    if _pinned_coder_name(ctx) in eligible:
        return greedy

    epsilon = ctx.app_config.daemon.exploration_epsilon
    if epsilon <= 0:
        return greedy

    if ctx.rng.random() < epsilon:
        non_top = list(greedy[1:])
        ctx.rng.shuffle(non_top)
        chosen = non_top[0]
        remainder = [name for name in greedy if name != chosen]
        return [chosen, *remainder]

    return greedy


def select_coder(ctx: SelectionContext) -> tuple[str, CoderPlugin] | None:
    """Return the top-ranked eligible coder and plugin, if any."""
    eligible = eligible_coders(ctx)
    if not eligible:
        return None
    name = rank_coders(eligible, ctx)[0]
    return name, ctx.registry.get(name)


def rank_auxiliary_coders(
    eligible: list[str], ctx: SelectionContext
) -> list[str]:
    """Return eligible coders for daemon auxiliary work.

    Auxiliary work should stay close to legacy behavior when possible, so
    Claude remains the first choice and Codex is the explicit fallback.
    Exploration is never applied on this path.
    """
    preferred = [
        name for name in ("claude", "codex") if name in eligible
    ]
    remainder = [
        name for name in _sort_by_priority(eligible, ctx)
        if name not in preferred
    ]
    return [*preferred, *remainder]


def select_auxiliary_coder(
    ctx: SelectionContext,
) -> tuple[str, CoderPlugin] | None:
    """Return the best eligible coder for diagnose/merge helper work."""
    eligible = eligible_coders(ctx)
    if not eligible:
        return None
    name = rank_auxiliary_coders(eligible, ctx)[0]
    return name, ctx.registry.get(name)


def _sort_by_priority(eligible: list[str], ctx: SelectionContext) -> list[str]:
    priorities = ctx.app_config.daemon.coder_priority
    return sorted(
        eligible,
        key=lambda name: (priorities.get(name, 0), name),
        reverse=True,
    )


def _greedy_order(eligible: list[str], ctx: SelectionContext) -> list[str]:
    preferred_name = _preferred_coder_name(ctx)
    if preferred_name in eligible:
        remainder = [name for name in eligible if name != preferred_name]
        return [preferred_name, *_sort_by_priority(remainder, ctx)]
    return _sort_by_priority(eligible, ctx)


def _preferred_coder_name(ctx: SelectionContext) -> str:
    preferred = ctx.repo_config.coder
    if preferred is not None:
        return preferred.value
    return ctx.app_config.daemon.coder.value


def _pinned_coder_name(ctx: SelectionContext) -> str | None:
    pinned = ctx.repo_config.coder
    if pinned is None:
        return None
    return pinned.value


def _is_rate_limited(name: str, state: RepoState) -> bool:
    until = state.rate_limited_coder_until.get(name)
    if until is not None:
        return until > datetime.now(timezone.utc)
    if (
        state.rate_limited_until is not None
        and state.rate_limit_reactive_coder is None
        and name == "claude"
    ):
        return state.rate_limited_until > datetime.now(timezone.utc)
    if state.rate_limit_reactive_coder == name:
        return True
    return name in state.rate_limited_coders


def _auth_failed(
    name: str,
    registry: CoderRegistry,
    auth_statuses: dict[str, dict[str, str]] | None = None,
) -> bool:
    if auth_statuses is not None and name in auth_statuses:
        return auth_statuses[name].get("status") != "ok"
    try:
        status = registry.get(name).check_auth()
    except Exception:
        return True
    return status.get("status") != "ok"


def _is_disabled_for_repo(name: str, repo_config: RepoConfig) -> bool:
    disabled = repo_config.disabled_coders or []
    return name in disabled


def _supports_runtime(name: str) -> bool:
    return name in _RUNTIME_SUPPORTED_CODERS
