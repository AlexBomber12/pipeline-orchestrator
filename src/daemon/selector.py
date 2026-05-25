"""Pure coder-selection helpers.

The selector only depends on registry/config/state inputs passed in via
``SelectionContext`` so ranking decisions stay deterministic in tests
and easy to evolve independently from the runner lifecycle.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

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


class CoderPurpose(str, Enum):
    DISPATCH = "dispatch"
    AUXILIARY = "auxiliary"
    DISPLAY = "display"


@dataclass(frozen=True)
class CoderResolution:
    name: str
    plugin: CoderPlugin | None
    reason: str


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


def candidate_coders(ctx: SelectionContext) -> list[str]:
    """Return coder names dispatch would consider, skipping runtime probes.

    Mirrors the static narrowing applied by :func:`eligible_coders` —
    task pin, repo pin (with ``auto_fallback`` off), and
    ``disabled_coders`` — but does not consult per-coder rate-limit or
    auth state. Callers that gate dispatch on the typed inhibitor list
    use this to align the gate's coder set with the dispatcher's
    candidate set without re-triggering a synchronous ``check_auth``
    probe on every cycle. Rate-limit semantics still flow through
    ``is_work_inhibited`` per returned coder.
    """
    task_pin = ctx.task_coder_pin
    if task_pin in ("claude", "codex"):
        return [task_pin]

    pinned = ctx.repo_config.coder
    preferred = pinned or ctx.app_config.daemon.coder
    if not ctx.app_config.daemon.auto_fallback:
        return [preferred.value]

    return [
        name
        for name in ctx.registry.coder_names()
        if _supports_runtime(name) and not _is_disabled_for_repo(name, ctx.repo_config)
    ]


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
    resolution = resolve_active_coder(ctx, purpose=CoderPurpose.DISPATCH)
    if resolution is None or resolution.plugin is None:
        return None
    return resolution.name, resolution.plugin


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
    resolution = resolve_active_coder(ctx, purpose=CoderPurpose.AUXILIARY)
    if resolution is None or resolution.plugin is None:
        return None
    return resolution.name, resolution.plugin


def resolve_active_coder(
    ctx: SelectionContext, *, purpose: CoderPurpose
) -> CoderResolution | None:
    """Resolve the active coder for the requested daemon purpose."""
    if (
        purpose is CoderPurpose.DISPLAY
        and ctx.task_coder_pin not in ("claude", "codex")
        and ctx.state.coder
    ):
        name = ctx.state.coder
        return CoderResolution(
            name=name,
            plugin=_plugin_or_none(ctx.registry, name),
            reason="pinned",
        )

    if purpose is CoderPurpose.AUXILIARY:
        eligible = eligible_coders(ctx)
        if not eligible:
            return None
        name = rank_auxiliary_coders(eligible, ctx)[0]
        return CoderResolution(
            name=name,
            plugin=ctx.registry.get(name),
            reason=_non_exploring_resolution_reason(name, ctx),
        )

    eligible = eligible_coders(ctx)
    if not eligible:
        return None

    if purpose is CoderPurpose.DISPLAY:
        state = ctx.rng.getstate()
        try:
            ranked = rank_coders(eligible, ctx)
        finally:
            ctx.rng.setstate(state)
    else:
        ranked = rank_coders(eligible, ctx)

    name = ranked[0]
    return CoderResolution(
        name=name,
        plugin=ctx.registry.get(name),
        reason=_resolution_reason(name, eligible, ranked, ctx),
    )


def resolve_pause_coder(ctx: SelectionContext) -> CoderResolution:
    """Resolve the coder used for legacy pause/fallback attribution."""
    if ctx.state.rate_limit_reactive_coder:
        name = ctx.state.rate_limit_reactive_coder
        return CoderResolution(
            name=name,
            plugin=_plugin_or_none(ctx.registry, name),
            reason="pinned",
        )

    name = _preferred_coder_name(ctx)
    return CoderResolution(
        name=name,
        plugin=_plugin_or_none(ctx.registry, name),
        reason="fallback",
    )


def _resolution_reason(
    name: str,
    eligible: list[str],
    ranked: list[str],
    ctx: SelectionContext,
) -> str:
    explicit_pin = ctx.task_coder_pin
    if explicit_pin in ("claude", "codex"):
        return "pinned" if name == explicit_pin else "fallback"

    repo_pin = _pinned_coder_name(ctx)
    if repo_pin is not None:
        return "pinned" if name == repo_pin else "fallback"

    greedy = _greedy_order(eligible, ctx)
    if ranked and greedy and ranked[0] != greedy[0]:
        return "exploration"
    return "ranked"


def _non_exploring_resolution_reason(name: str, ctx: SelectionContext) -> str:
    explicit_pin = ctx.task_coder_pin
    if explicit_pin in ("claude", "codex"):
        return "pinned" if name == explicit_pin else "fallback"

    repo_pin = _pinned_coder_name(ctx)
    if repo_pin is not None:
        return "pinned" if name == repo_pin else "fallback"
    return "ranked"


def _plugin_or_none(registry: CoderRegistry, name: str) -> CoderPlugin | None:
    try:
        return registry.get(name)
    except KeyError:
        return None


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
