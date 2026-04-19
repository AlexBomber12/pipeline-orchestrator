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
from src.config import AppConfig, RepoConfig
from src.models import RepoState


@dataclass
class SelectionContext:
    registry: CoderRegistry
    repo_config: RepoConfig
    app_config: AppConfig
    state: RepoState
    rng: random.Random
    auth_statuses: dict[str, dict[str, str]] | None = None


def eligible_coders(ctx: SelectionContext) -> list[str]:
    """Return the currently runnable coder names in preference order."""
    pinned = ctx.repo_config.coder
    if pinned is not None and not ctx.app_config.daemon.auto_fallback:
        return [pinned.value]

    result: list[str] = []
    for name in ctx.registry.coder_names():
        if _is_rate_limited(name, ctx.state):
            continue
        if _auth_failed(name, ctx.registry, ctx.auth_statuses):
            continue
        if _is_disabled_for_repo(name, ctx.repo_config):
            continue
        result.append(name)

    if pinned is not None and pinned.value in result:
        return [pinned.value] + [name for name in result if name != pinned.value]
    return result


def rank_coders(eligible: list[str], ctx: SelectionContext) -> list[str]:
    """Return eligible coders ordered by priority or exploration choice."""
    if len(eligible) <= 1:
        return list(eligible)

    greedy = _greedy_order(eligible, ctx)
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


def _sort_by_priority(eligible: list[str], ctx: SelectionContext) -> list[str]:
    priorities = ctx.app_config.daemon.coder_priority
    return sorted(
        eligible,
        key=lambda name: (priorities.get(name, 0), name),
        reverse=True,
    )


def _greedy_order(eligible: list[str], ctx: SelectionContext) -> list[str]:
    preferred = ctx.repo_config.coder
    preferred_name = (
        preferred.value
        if preferred is not None
        else ctx.app_config.daemon.coder.value
    )
    if preferred_name in eligible:
        remainder = [name for name in eligible if name != preferred_name]
        return [preferred_name, *_sort_by_priority(remainder, ctx)]
    return _sort_by_priority(eligible, ctx)


def _is_rate_limited(name: str, state: RepoState) -> bool:
    until = state.rate_limited_coder_until.get(name)
    if until is not None:
        return until > datetime.now(timezone.utc)
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
