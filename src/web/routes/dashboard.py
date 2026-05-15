"""Dashboard rendering routes and supporting view helpers.

Owns the high-traffic surfaces of the operator dashboard: the home grid,
repo detail page, JSON status endpoints, and the HTMX partials that swap
into them. Repo-state reads go through ``src.web.services.repo_state``;
mutation routes (pause/resume/stop/coder) live in
``src.web.routes.repo_control``.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Literal

import redis.asyncio as aioredis
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse

from src.cancellation import list_recent_cancellations
from src.cancellation.availability import (
    ActiveHoursSource,
    HeartbeatSource,
    ManualOverrideSource,
    is_operator_available,
)
from src.cancellation.storage import (
    GuardrailPending,
    get_cancellation_cause,
    list_pending_guardrail_decisions,
)
from src.coders import build_coder_registry
from src.config import AppConfig, RepoConfig, load_config
from src.daemon.github_rate_limit import (
    RateLimitBudget,
    read_graphql_budget,
    read_rest_budget,
    recent_cycle_burns,
)
from src.events.sse import format_sse_comment, format_sse_event
from src.keyspace import cli_log_latest, daemon_panic_state
from src.metrics import MetricsStore, RunRecord
from src.models import PipelineState, RepoState
from src.utils import repo_slug_from_url
from src.web.services.coder import _effective_coder_name
from src.web.services.repo_state import (
    _find_repo_config_by_name,
    compute_repo_dependents_count,
)

AVAILABILITY_CHANNEL = "orchestrator:availability:changed"
AVAILABILITY_OVERRIDE_KEY = "operator_override"
AVAILABILITY_VALID_STATES = ("AVAILABLE", "AWAY", "AUTO")
AVAILABILITY_SSE_KEEPALIVE_SECONDS = 15.0
AVAILABILITY_SSE_POLL_INTERVAL_SECONDS = 0.1

router = APIRouter()

_HISTORY_LIMIT = 100
_METRICS_PANEL_LIMIT = 20
_METRICS_SCAN_LIMIT = 100
_CANCELLATIONS_WINDOW_DAYS = 7
_CANCELLATIONS_MAX = 50

# PR-310: subsource_filter dropdown groups (UI vocabulary) projected onto
# the canonical PR-315 ``payload.subsource`` vocabulary. ``daemon`` covers
# every detector that fires automatically (review_timeout, FIX timers,
# no-push deadlock, infra streak, raw daemon crash); ``coder`` is the
# explicit ``ESCALATE:`` marker; ``guardrail`` and ``operator_reject``
# map one-to-one. ``""`` (empty string from the "All" option) skips
# filtering entirely.
_SUBSOURCE_FILTER_GROUPS: dict[str, frozenset[str]] = {
    "guardrail": frozenset({"guardrail"}),
    "coder": frozenset({"coder_escalate"}),
    "daemon": frozenset(
        {
            "crash",
            "review_timeout",
            "fix_idle_timeout",
            "fix_iteration_cap",
            "no_push_deadlock",
            "infra_failure",
        }
    ),
    "operator_reject": frozenset({"operator_reject"}),
}

_ACTIVE_RUN_STATES = {
    PipelineState.PREFLIGHT,
    PipelineState.CODING,
    PipelineState.WATCH,
    PipelineState.FIX,
    PipelineState.MERGE,
    PipelineState.PAUSED,
}


def _page_rendered_at_iso() -> str:
    """Server-side ISO-Z timestamp embedded in pages with a live SSE
    consumer. Clients compare event timestamps to this cutoff to decide
    whether a frame is already reflected in the rendered HTML or
    represents a fresh update — anchoring suppression to server time
    avoids both client-clock skew and the page-render→SSE-subscribe gap
    in which legitimate live events would otherwise be discarded.

    Capture this cutoff BEFORE reading repo state so an event published
    in the gap between snapshot read and cutoff generation cannot be
    suppressed despite not being reflected in the rendered HTML; events
    older than the cutoff are then guaranteed to predate the snapshot
    read and thus already appear in the rendered HTML.
    """
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _daemon_default_coder_name(config: AppConfig) -> str:
    """Return the daemon-level default coder name."""
    return config.daemon.coder.value


def _repo_coder_form_value(repo_config: RepoConfig | None) -> str:
    """Return the raw repo-level coder selection for the detail form."""
    if repo_config is None or repo_config.coder is None:
        return "any"
    return repo_config.coder.value


def _repo_coder_label(coder: str | None) -> str:
    """Return the repo-header display label for a coder selection."""
    if coder == "any":
        return "Any (bandit)"
    if coder == "claude":
        return "Claude CLI"
    if coder == "codex":
        return "Codex"
    return coder or ""


def _active_rate_limit_coder(
    state: RepoState, effective_coder: str
) -> str | None:
    """Return the active coder whose rate-limit data should be shown."""
    if state.current_task is None:
        return None
    if state.coder:
        return state.coder
    if effective_coder == "any":
        return None
    return effective_coder


def _active_repo_coder(state: RepoState) -> str | None:
    """Return the runtime coder only while a repo is actively executing work."""
    if state.current_task is None or state.state not in _ACTIVE_RUN_STATES:
        return None
    return state.coder


def _coder_rate_limit_supported(coder: str | None) -> bool:
    """Return whether ``coder`` has meaningful rate-limit usage data."""
    return coder in {"claude", "codex"}


_GUARDRAIL_EXCERPT_MAX_CHARS = 200
_GUARDRAIL_PENDING_LIMIT = 100
# Mirrors ``_extract_guardrail_metadata`` in ``src.web.routes.repo_control``:
# coding.py and fix.py emit guardrail causes carrying only
# ``payload.reason_text = "GUARDRAIL: {category}: {excerpt}"`` (watch.py
# emits structured ``rule``/``excerpt`` directly), so without this parse the
# panel renders blank rule/excerpt for the common CODING/FIX-flagged rows.
_GUARDRAIL_REASON_RE = re.compile(r"^GUARDRAIL:\s*([^:]+):\s*(.+)$")


def _resolve_guardrail_metadata(payload: dict[str, Any]) -> tuple[str, str]:
    """Return ``(rule, excerpt)`` from a guardrail cause payload.

    Falls back through ``rule`` -> ``category`` -> parsed ``reason_text``
    so CODING/FIX-emitted causes (which carry only ``reason_text``) still
    produce non-empty panel rows. Kept in lockstep with
    ``_extract_guardrail_metadata`` in ``src.web.routes.repo_control``.
    """
    rule = payload.get("rule") or payload.get("category") or ""
    excerpt = payload.get("excerpt", "") or ""
    if rule and excerpt:
        return rule, excerpt
    reason = payload.get("reason_text", "")
    if isinstance(reason, str):
        match = _GUARDRAIL_REASON_RE.match(reason)
        if match:
            if not rule:
                rule = match.group(1).strip()
            if not excerpt:
                excerpt = match.group(2).strip()
    return rule, excerpt


async def _recover_guardrail_metadata(
    redis_client: aioredis.Redis,
    repo_name: str,
    entry: GuardrailPending,
) -> GuardrailPending:
    """Patch ``entry`` with reason_text-derived rule/excerpt when missing.

    ``list_pending_guardrail_decisions`` reads only the structured
    ``rule``/``excerpt`` payload fields, so CODING/FIX-emitted causes
    (``payload.reason_text`` only) arrive at the view layer with blank
    rule and excerpt. Re-fetch the cause and apply the same fallback the
    approve/reject endpoints use; degrade silently to the original entry
    if the cause has already vanished (TTL expiry, concurrent decision).
    """
    if entry.rule and entry.excerpt:
        return entry
    try:
        cause = await get_cancellation_cause(
            redis_client, repo_name, entry.task_id
        )
    except Exception:
        return entry
    if cause is None or not isinstance(cause.payload, dict):
        return entry
    rule, excerpt = _resolve_guardrail_metadata(cause.payload)
    if rule == entry.rule and excerpt == entry.excerpt:
        return entry
    return replace(entry, rule=rule, excerpt=excerpt)


def _truncate_guardrail_excerpt(excerpt: str) -> str:
    """Cap a guardrail excerpt to ``_GUARDRAIL_EXCERPT_MAX_CHARS`` characters.

    The full text remains in the cancellation cause record for audit; the
    UI cap matches the GuardrailViolation.excerpt design limit so a
    pathological payload cannot blow up panel layout. The ellipsis costs
    one character so the visible cutoff is ``MAX - 1`` to keep the total
    rendered length at or below ``MAX``.
    """
    if len(excerpt) <= _GUARDRAIL_EXCERPT_MAX_CHARS:
        return excerpt
    return excerpt[: _GUARDRAIL_EXCERPT_MAX_CHARS - 1] + "…"


def _format_guardrail_relative_time(
    recorded_at: int,
    *,
    now: datetime | None = None,
) -> str:
    """Render ``recorded_at`` as an operator-friendly relative label.

    Returns "just now" for sub-minute deltas, ``N minutes ago`` /
    ``N hours ago`` / ``N days ago`` for larger ones. Future timestamps
    (clock skew between daemon and dashboard) collapse to "just now"
    rather than rendering "in N min" so the panel never confuses the
    operator with a negative-age entry.
    """
    current = now if now is not None else datetime.now(timezone.utc)
    delta_seconds = int(current.timestamp()) - int(recorded_at)
    if delta_seconds < 60:
        return "just now"
    minutes = delta_seconds // 60
    if minutes < 60:
        suffix = "minute" if minutes == 1 else "minutes"
        return f"{minutes} {suffix} ago"
    hours = minutes // 60
    if hours < 24:
        suffix = "hour" if hours == 1 else "hours"
        return f"{hours} {suffix} ago"
    days = hours // 24
    suffix = "day" if days == 1 else "days"
    return f"{days} {suffix} ago"


def _serialize_guardrail_pending(
    entry: GuardrailPending,
    *,
    current_pr_url: str | None,
    is_active: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return the per-entry view model the guardrail panel template renders.

    ``current_pr_url`` is the GitHub PR URL only when the entry corresponds
    to the repo's active PR (per the daemon's ``RepoState``); otherwise the
    URL is unknown without a synchronous ``gh`` lookup, so the template
    falls back to plain text for the PR ID. ``is_active`` mirrors the
    approve endpoint's accept gate at ``repo_control._approve_guardrail
    _decision`` (entry's PR id matches ``state.current_task.pr_id`` AND
    ``state.current_pr`` is set); only active rows render the Approve
    button so historical entries cannot be clicked into a guaranteed 409.
    The excerpt is truncated to the panel cap here so the template can
    render it verbatim without re-checking length.
    """
    return {
        "pr_id": entry.task_id,
        "rule": entry.rule,
        "excerpt": _truncate_guardrail_excerpt(entry.excerpt),
        "recorded_at": entry.recorded_at,
        "recorded_at_text": _format_guardrail_relative_time(
            entry.recorded_at, now=now
        ),
        "pr_url": current_pr_url,
        "is_active": is_active,
    }


async def _build_guardrail_pending_view(
    redis_client: aioredis.Redis | None,
    repo_name: str,
    state: RepoState,
    *,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Return serialized guardrail-pending entries for a repo's panel.

    Empty list when Redis is unavailable or the helper raises so the
    dashboard never 500s on a transient outage; the operator still sees
    the rest of the per-repo page and the next refresh recovers. Only
    the entry matching the repo's active task/PR gets a ``pr_url`` —
    other entries fall back to plain-text PR IDs because the GH PR
    number for a non-current PR is not stored in ``RepoState``.
    """
    if redis_client is None:
        return []
    try:
        pending = await list_pending_guardrail_decisions(
            redis_client, repo_name, limit=_GUARDRAIL_PENDING_LIMIT
        )
    except Exception:
        # Mirror partial_repo_cancellations: degrade silently so a Redis
        # outage or a partial test double never breaks repo-detail
        # rendering. The next poll of /partials/repo/{name} recovers.
        return []
    pending = [
        await _recover_guardrail_metadata(redis_client, repo_name, entry)
        for entry in pending
    ]
    current_pr_url: str | None = None
    current_task_pr_id: str | None = None
    # The approve endpoint requires ``state.current_pr`` (not its URL) — see
    # ``_approve_guardrail_decision`` in ``repo_control.py``. Track that gate
    # separately from ``current_pr_url`` so an entry can be approve-eligible
    # even when ``current_pr.url`` is empty.
    active_pr_id: str | None = None
    if state.current_task is not None and state.current_pr is not None:
        active_pr_id = state.current_task.pr_id
        if state.current_pr.url:
            current_pr_url = state.current_pr.url
            current_task_pr_id = state.current_task.pr_id
    return [
        _serialize_guardrail_pending(
            entry,
            current_pr_url=(
                current_pr_url if entry.task_id == current_task_pr_id else None
            ),
            is_active=entry.task_id == active_pr_id,
            now=now,
        )
        for entry in pending
    ]


async def _build_recent_graphql_burns_view(
    redis_client: aioredis.Redis | None,
    repo_name: str,
) -> dict[str, Any] | None:
    """Return the recent GraphQL cycle-burn summary for ``repo_name``.

    Returns ``None`` when no cycle burns have been recorded yet so the
    template can hide the inline metric. ``avg`` and ``max`` are computed
    here for dashboard convenience and rounded so the display is stable
    across renders.
    """
    burns = await recent_cycle_burns(redis_client, repo_name)
    if not burns:
        return None
    avg = sum(burns) / len(burns)
    return {
        "burns": burns,
        "avg": round(avg, 1),
        "max": max(burns),
    }


def _resource_zone(percent_used: float | None) -> str:
    """Map ``percent_used`` to one of ``green|yellow|red|none``.

    Boundaries: ``<70`` green, ``70<=pct<90`` yellow, ``>=90`` red. ``None``
    (resource value unknown) collapses to ``none`` so the chip can render
    a neutral placeholder rather than mis-coloring a missing reading.
    """
    if percent_used is None:
        return "none"
    if percent_used < 70:
        return "green"
    if percent_used < 90:
        return "yellow"
    return "red"


def _budget_chip(
    budget: RateLimitBudget | None,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    if budget is None:
        return {
            "remaining": None,
            "limit": None,
            "percent_used": None,
            "reset_unix": None,
            "zone": "none",
        }
    # Snapshots whose ``reset_at`` has already passed describe a window
    # GitHub has since rolled over. The remaining/limit pair is no longer
    # informative — the daemon stops throttling once ``now >= reset_at`` —
    # so render the chip as neutral until the next successful probe rather
    # than letting a stale low-remaining reading hold a critical zone.
    current = now if now is not None else datetime.now(timezone.utc)
    if budget.reset_at <= current:
        return {
            "remaining": None,
            "limit": None,
            "percent_used": None,
            "reset_unix": None,
            "zone": "none",
        }
    pct_used = max(0.0, min(100.0, 100.0 - float(budget.remaining_percent)))
    return {
        "remaining": budget.remaining,
        "limit": budget.limit,
        "percent_used": round(pct_used, 1),
        "reset_unix": int(budget.reset_at.timestamp()),
        "zone": _resource_zone(pct_used),
    }


def _coder_usage_chip(
    states: list[RepoState],
    *,
    coder: Literal["claude", "codex"],
    window: Literal["session", "weekly"],
) -> dict[str, Any]:
    """Aggregate coder usage across active repos for the chip row.

    Coder repos share one account so any active coder state's
    snapshot is representative of the whole account. The most recently
    updated one wins so the chip reflects the freshest observation.

    Inactive repos are excluded: their usage fields are not refreshed by the
    runner once ``repo_config.active`` flips false, but ``last_updated`` is
    still bumped each publish cycle. Without this gate, a disabled repo's
    stale snapshot can win the timestamp race and the chip would render an
    outdated percentage/reset for the live account.
    """
    candidate: RepoState | None = None
    for state in states:
        if (state.coder or "") != coder:
            continue
        if not state.active:
            continue
        if window == "session" and state.usage_session_percent is None:
            continue
        if window == "weekly" and state.usage_weekly_percent is None:
            continue
        if candidate is None or state.last_updated > candidate.last_updated:
            candidate = state
    if candidate is None:
        return {
            "remaining": None,
            "limit": None,
            "percent_used": None,
            "reset_unix": None,
            "zone": "none",
        }
    if window == "session":
        used = candidate.usage_session_percent or 0
        reset_unix = candidate.usage_session_resets_at
    else:
        used = candidate.usage_weekly_percent or 0
        reset_unix = candidate.usage_weekly_resets_at
    pct_used = max(0.0, min(100.0, float(used)))
    return {
        "remaining": int(round(100.0 - pct_used)),
        "limit": 100,
        "percent_used": round(pct_used, 1),
        "reset_unix": reset_unix,
        "zone": _resource_zone(pct_used),
    }


def _claude_usage_chip(
    states: list[RepoState],
    *,
    window: Literal["session", "weekly"],
) -> dict[str, Any]:
    return _coder_usage_chip(states, coder="claude", window=window)


def _codex_usage_chip(
    states: list[RepoState],
    *,
    window: Literal["session", "weekly"],
) -> dict[str, Any]:
    return _coder_usage_chip(states, coder="codex", window=window)


async def _build_resources_view(
    redis_client: aioredis.Redis | None,
    states: list[RepoState],
) -> dict[str, dict[str, Any]]:
    """Return the resource payload for the dashboard chip row.

    Each entry exposes ``remaining``, ``limit``, ``percent_used``,
    ``reset_unix`` and a precomputed ``zone`` (``green|yellow|red|none``).
    Missing data renders as ``percent_used: None`` plus ``zone: none``
    rather than crashing the dashboard or hiding the chip.
    """
    rest = await read_rest_budget(redis_client)
    graphql = await read_graphql_budget(redis_client)
    now = datetime.now(timezone.utc)
    return {
        "github_rest": _budget_chip(rest, now=now),
        "github_graphql": _budget_chip(graphql, now=now),
        "claude_5h": _claude_usage_chip(states, window="session"),
        "claude_weekly": _claude_usage_chip(states, window="weekly"),
        "codex_5h": _codex_usage_chip(states, window="session"),
        "codex_weekly": _codex_usage_chip(states, window="weekly"),
    }


async def _repo_template_context(
    name: str,
    redis_client: aioredis.Redis | None,
    config_path: str | None = None,
    *,
    include_metrics: bool = False,
    coder_update_message: str | None = None,
) -> dict[str, Any]:
    """Return template context for repo detail renders."""
    if config_path is None:
        config_path = _app.CONFIG_PATH
    config = await asyncio.to_thread(load_config, config_path)
    state = await _app.get_repo_state(name, redis_client, config_path)
    repo_config = _find_repo_config_by_name(config, name)
    effective_coder = _effective_coder_name(repo_config, config)
    active_rate_limit_coder = _active_rate_limit_coder(state, effective_coder)
    show_rate_limit_badge = _coder_rate_limit_supported(active_rate_limit_coder) and (
        state.usage_session_percent is not None
        or state.usage_weekly_percent is not None
        or state.usage_api_degraded
    )
    recent_graphql_burns = await _build_recent_graphql_burns_view(
        redis_client, name
    )
    resources = await _build_resources_view(redis_client, [state])
    guardrail_pending = await _build_guardrail_pending_view(
        redis_client, name, state
    )
    selected_repo_coder = _repo_coder_form_value(repo_config)
    active_repo_coder = _active_repo_coder(state)
    return {
        "repo": state,
        "recent_graphql_burns": recent_graphql_burns,
        "resources": resources,
        "guardrail_pending": guardrail_pending,
        "repo_config": repo_config,
        "daemon": config.daemon,
        "coders": build_coder_registry().list_coders(),
        "effective_coder": effective_coder,
        "active_rate_limit_coder": active_rate_limit_coder,
        "active_rate_limit_coder_label": (
            "Claude" if active_rate_limit_coder == "claude" else "Codex"
        ),
        "show_rate_limit_badge": show_rate_limit_badge,
        "selected_repo_coder": selected_repo_coder,
        "selected_repo_coder_label": _repo_coder_label(selected_repo_coder),
        "active_repo_coder": active_repo_coder,
        "active_repo_coder_label": _repo_coder_label(active_repo_coder),
        "inherit_coder": _daemon_default_coder_name(config),
        "coder_update_message": coder_update_message,
        "metrics_records": (
            await _recent_repo_metrics_payload(name, redis_client)
            if include_metrics
            else []
        ),
        "repo_name": name,
    }


def _parse_iso8601(value: str | None) -> datetime | None:
    """Return an aware datetime for an ISO-8601 string when possible."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _format_duration_ms(duration_ms: int | None) -> str:
    """Render a short human-readable duration for the metrics table."""
    if duration_ms is None:
        return "—"
    total_seconds = duration_ms // 1000
    if total_seconds <= 0:
        return "<1s"
    if total_seconds < 60:
        return f"{total_seconds}s"
    minutes, seconds = divmod(total_seconds, 60)
    if minutes < 60:
        return f"{minutes}m" if seconds == 0 else f"{minutes}m {seconds}s"
    hours, remaining_minutes = divmod(minutes, 60)
    if remaining_minutes == 0:
        return f"{hours}h"
    return f"{hours}h {remaining_minutes}m"


def _profile_parts(profile_id: str) -> tuple[str, str]:
    """Split ``coder:model:...`` into displayable coder/model fields."""
    parts = profile_id.split(":")
    coder = parts[0] if parts and parts[0] else "unknown"
    model = parts[1] if len(parts) > 1 and parts[1] else "unknown"
    return coder, model


def _exit_reason_label(exit_reason: str) -> str:
    """Normalize stored exit reasons into concise UI labels."""
    labels = {
        "closed_unmerged": "closed without merge",
        "success_merged": "merged",
        "rate_limit": "rate limit",
        "error": "error",
    }
    return labels.get(exit_reason, exit_reason.replace("_", " ") or "unknown")


def _exit_reason_classes(exit_reason: str) -> str:
    """Return badge classes for the exit reason column."""
    if exit_reason == "closed_unmerged":
        return "bg-fail/15 text-fail border-fail/30"
    if "merged" in exit_reason:
        return "bg-ok/15 text-ok border-ok/30"
    if "rate_limit" in exit_reason:
        return "bg-warn/15 text-warn border-warn/30"
    if "error" in exit_reason:
        return "bg-fail/15 text-fail border-fail/30"
    return "bg-white/5 text-gray-300 border-white/10"


def _serialize_run_record(record: RunRecord) -> dict[str, Any]:
    """Return one run record payload for JSON and Jinja rendering."""
    coder, model = _profile_parts(record.profile_id)
    payload = asdict(record)
    payload.update(
        {
            "coder": coder,
            "model": model,
            "duration_text": _format_duration_ms(record.duration_ms),
            "exit_reason_label": _exit_reason_label(record.exit_reason),
            "exit_reason_classes": _exit_reason_classes(record.exit_reason),
        }
    )
    return payload


async def _recent_repo_metrics_payload(
    name: str,
    redis_client: aioredis.Redis | None,
) -> list[dict[str, Any]]:
    """Return the latest completed PR run records for one repo."""
    if redis_client is None:
        return []
    store = MetricsStore(redis_client)
    try:
        records = await store.recent(
            task_id="PR",
            limit=_METRICS_SCAN_LIMIT,
            repo_name=name,
        )
    except Exception:
        return []
    completed = [
        record
        for record in records
        if record.ended_at is not None
        and record.exit_reason in _TERMINAL_METRICS_EXIT_REASONS
    ]
    completed.sort(
        key=lambda record: _parse_iso8601(record.ended_at)
        or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    return [
        _serialize_run_record(record)
        for record in completed[:_METRICS_PANEL_LIMIT]
    ]


_MERGE_EVENT_MARKER = "Merged PR"
_ITERATION_EVENT_MARKER = "Fix pushed, iteration"
_TERMINAL_METRICS_EXIT_REASONS = frozenset(
    {"error", "rate_limit", "success_merged", "closed_unmerged"}
)
_ACTIVE_STATES = frozenset(
    {PipelineState.CODING, PipelineState.WATCH, PipelineState.FIX}
)
_ALERT_STATES = frozenset({PipelineState.ERROR})
_ACTIVITY_FEED_LIMIT = 50
# Sentinel used as the sort key for feed entries whose ``time`` field is
# a legacy/unparseable value. Pushing them to epoch-start makes sure they
# sink to the bottom of the newest-first feed, so a repo that happens to
# be actively updated (and therefore has a recent ``last_updated``) can
# never float its legacy history above genuinely new entries from other
# repos during a mixed-format upgrade window.
_FEED_UNKNOWN_TIME = datetime.min.replace(tzinfo=timezone.utc)
_REPO_BADGE_PALETTE = (
    "bg-accent/15 text-accent border-accent/30",
    "bg-ok/15 text-ok border-ok/30",
    "bg-warn/15 text-warn border-warn/30",
    "bg-fail/15 text-fail border-fail/30",
    "bg-hung/15 text-hung border-hung/30",
    "bg-sky-500/15 text-sky-300 border-sky-500/30",
    "bg-purple-500/15 text-purple-300 border-purple-500/30",
    "bg-pink-500/15 text-pink-300 border-pink-500/30",
)


def _parse_history_time(value: str) -> datetime | None:
    """Parse a history entry's ``time`` field into an aware ``datetime``.

    The daemon writes ISO-8601 UTC timestamps (via ``datetime.isoformat``),
    but older fixtures and pre-PR-013 payloads may store a bare ``HH:MM:SS``
    clock string. Return ``None`` in the latter case so the caller can
    decide whether to fall back to the owning repo's ``last_updated`` or
    simply skip the entry for date-aware stats.
    """
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _format_history_time(value: str) -> str:
    """Return an ``HH:MM:SS`` display string for a history ``time`` field."""
    parsed = _parse_history_time(value)
    if parsed is None:
        return value
    return parsed.astimezone(timezone.utc).strftime("%H:%M:%S")


def _repo_badge_abbrev(name: str) -> str:
    """Return the 3-char uppercase repo name badge shown in the feed."""
    if not name:
        return "???"
    return name[:3].upper()


def _repo_badge_style(name: str) -> str:
    """Return a Tailwind class string for ``name``'s activity-feed badge.

    Hash-based so each repo gets a stable colour across reloads without
    having to persist anything. We hash with SHA-1 (not security-sensitive,
    just a well-distributed bucket function) and modulo into the palette.
    """
    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()
    idx = int(digest, 16) % len(_REPO_BADGE_PALETTE)
    return _REPO_BADGE_PALETTE[idx]


def _activity_feed_entry(
    state: RepoState, entry: dict[str, Any]
) -> tuple[datetime, dict[str, Any]]:
    """Build one activity-feed item plus its sort key.

    Entries whose ``time`` field cannot be parsed (legacy ``HH:MM:SS``
    payloads written before ISO timestamps landed) get a
    ``_FEED_UNKNOWN_TIME`` sentinel for sort ordering. That sinks them to
    the bottom of the newest-first feed instead of pinning them to the
    owning repo's ``last_updated`` — an active repo's ``last_updated``
    tracks "now", which would otherwise float every legacy entry to the
    top and drown out genuinely recent events from other repos.
    """
    time_str = str(entry.get("time", ""))
    parsed = _parse_history_time(time_str)
    sort_key = parsed or _FEED_UNKNOWN_TIME
    return sort_key, {
        "repo_name": state.name,
        "repo_abbrev": _repo_badge_abbrev(state.name),
        "repo_style": _repo_badge_style(state.name),
        "time": _format_history_time(time_str),
        "state": entry.get("state", PipelineState.IDLE.value),
        "event": entry.get("event", ""),
    }


def _build_activity_feed(
    states: list[RepoState],
) -> list[dict[str, Any]]:
    """Merge histories across repos into one newest-first feed.

    Each entry carries the owning repo name, a 3-char abbreviation and a
    stable colour class so the template can render a repo badge without
    reaching back into Python. Capped at ``_ACTIVITY_FEED_LIMIT`` to keep
    the HTMX payload small.
    """
    items: list[tuple[datetime, dict[str, Any]]] = []
    for state in states:
        for entry in state.history:
            items.append(_activity_feed_entry(state, entry))
    items.sort(key=lambda item: item[0], reverse=True)
    return [payload for _, payload in items[:_ACTIVITY_FEED_LIMIT]]


_ALERT_KIND_ORDER = {"ERROR": 0}


def _format_alert_duration(seconds: int) -> str:
    """Return a human-readable duration for an alert card.

    Used by the alerts panel to render how long a repo has been in the
    ERROR state. Negative inputs (clock skew, a state whose
    ``last_updated`` is a few ms in the future) are clamped to zero so
    the UI never renders "-3 sec".
    """
    if seconds < 0:
        seconds = 0
    if seconds < 60:
        return f"{seconds} sec"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} min"
    hours = minutes // 60
    remaining = minutes % 60
    if remaining == 0:
        return f"{hours}h"
    return f"{hours}h {remaining}min"


def _most_recent_transition_into(
    history: list[dict[str, Any]], target_state: str
) -> datetime | None:
    """Return the time of the most recent transition INTO ``target_state``.

    History is appended in chronological order by ``Runner.log_event``.
    A "transition" here is the first entry of the most recent consecutive
    run of ``target_state`` entries — subsequent entries in the same run
    are just repeat polls where the state didn't actually change. Scans
    forward and overwrites the candidate every time a new run starts so
    the final value is the start of the latest run.

    Returns ``None`` if ``history`` has no ``target_state`` entries, or
    if every such entry carries an unparseable ``time`` field (legacy
    ``HH:MM:SS`` payloads written before PR-013's ISO conversion).
    """
    run_start: datetime | None = None
    prev_state: str | None = None
    for entry in history:
        current = str(entry.get("state", ""))
        if current == target_state and prev_state != target_state:
            parsed = _parse_history_time(str(entry.get("time", "")))
            if parsed is not None:
                run_start = parsed
        prev_state = current
    return run_start


def _alert_reference_time(state: RepoState) -> datetime:
    """Return the "since" timestamp an alert card should display.

    Scan ``state.history`` for the most recent transition into the
    current state. This matters because ``publish_state``
    rewrites ``state.last_updated`` on every daemon cycle (see
    ``src/daemon/runner.py``), so using ``last_updated`` as the "since"
    timestamp would make an hours-old ERROR card display "a few sec"
    forever and break duration-based sorting in the alerts bucket.

    Falls through to ``state.last_updated`` only when history carries
    no matching transition (a bootstrap cycle or a legacy payload with
    unparseable timestamps) — in that case "now-ish" is the best signal
    we have and the alert still renders.
    """
    transition = _most_recent_transition_into(
        state.history, state.state.value
    )
    if transition is not None:
        return transition
    return state.last_updated


def _build_alerts(states: list[RepoState]) -> list[dict[str, Any]]:
    """Collect alert cards for every repo currently in ERROR.

    Each alert dict is self-contained so the template does not need to
    reach back into ``RepoState``. Sort by duration descending so the
    longest-standing problem bubbles to the top.
    """
    now = datetime.now(timezone.utc)
    alerts: list[dict[str, Any]] = []

    for state in states:
        if state.state not in _ALERT_STATES:
            continue
        kind = state.state.value
        since = _alert_reference_time(state)
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        duration_sec = int((now - since).total_seconds())
        alert: dict[str, Any] = {
            "kind": kind,
            "repo_name": state.name,
            "repo_url": f"/repo/{state.name}",
            "duration_seconds": max(duration_sec, 0),
            "duration_text": _format_alert_duration(duration_sec),
            "since_iso": since.astimezone(timezone.utc).isoformat(),
        }
        alert["error_message"] = state.error_message or ""
        alerts.append(alert)

    alerts.sort(
        key=lambda a: (
            _ALERT_KIND_ORDER.get(a["kind"], 99),
            -a["duration_seconds"],
        )
    )
    return alerts


def _compute_stats(states: list[RepoState]) -> dict[str, Any]:
    """Aggregate cross-repo stats for the dashboard cards and JSON API.

    Derives everything from in-memory ``RepoState`` objects:

    * ``repos`` - number of configured repositories.
    * ``active`` - repos currently in CODING/WATCH/FIX.
    * ``alerts`` - repos currently in ERROR.
    * ``done_today`` / ``done_week`` - count of ``Merged PR`` events in the
      merged history whose timestamp falls inside the window. Entries
      without a parseable ``time`` are excluded from the windows but still
      counted in ``per_repo[].done_total``.
    * ``avg_iterations_per_merge`` - total ``Fix pushed, iteration`` events
      divided by total merges. Zero if no merges have happened yet.
    """
    now = datetime.now(timezone.utc)
    today = now.date()
    week_start = now - timedelta(days=7)

    repos_count = len(states)
    active_count = sum(1 for s in states if s.state in _ACTIVE_STATES)
    alerts_count = sum(1 for s in states if s.state in _ALERT_STATES)

    merges_today = 0
    merges_week = 0
    merges_total = 0
    iterations_total = 0
    per_repo: list[dict[str, Any]] = []

    for state in states:
        done_total = 0
        events_today = 0
        for entry in state.history:
            event = str(entry.get("event", ""))
            parsed = _parse_history_time(str(entry.get("time", "")))
            if _MERGE_EVENT_MARKER in event:
                done_total += 1
                merges_total += 1
                if parsed is not None:
                    if parsed.astimezone(timezone.utc).date() == today:
                        merges_today += 1
                    if parsed >= week_start:
                        merges_week += 1
            if _ITERATION_EVENT_MARKER in event:
                iterations_total += 1
            if parsed is not None and parsed.astimezone(
                timezone.utc
            ).date() == today:
                events_today += 1
        per_repo.append(
            {
                "name": state.name,
                "state": state.state.value,
                "done_total": done_total,
                "events_today": events_today,
            }
        )

    if merges_total:
        avg_iterations = round(iterations_total / merges_total, 2)
    else:
        avg_iterations = 0.0

    return {
        "repos": repos_count,
        "active": active_count,
        "alerts": alerts_count,
        "done_today": merges_today,
        "done_week": merges_week,
        "avg_iterations_per_merge": avg_iterations,
        "per_repo": per_repo,
    }


async def _read_panic_state_for_banner(
    redis_client: aioredis.Redis | None,
) -> dict[str, Any] | None:
    """Return the daemon cascade panic record for the dashboard banner.

    Mirrors ``cascade_monitor._read_panic_state`` semantics (bytes/str
    decode, JSON parse, dict guard) without importing the private helper.
    Returns ``None`` when there is no record, when Redis is unavailable,
    or when the value cannot be decoded — the banner template treats all
    of those uniformly by rendering nothing.
    """
    if redis_client is None:
        return None
    try:
        raw = await redis_client.get(daemon_panic_state())
    except Exception:
        return None
    if raw is None:
        return None
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(parsed, dict):
        return None
    return parsed if parsed.get("enabled") else None


@router.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    page_rendered_at = _page_rendered_at_iso()
    states, redis_warning = await _app.get_all_repo_states(
        redis_client, _app.CONFIG_PATH
    )
    stats = _compute_stats(states)
    alerts = _build_alerts(states)
    latest_alert = min(alerts, key=lambda a: a["duration_seconds"]) if alerts else None
    resources = await _build_resources_view(redis_client, states)
    panic_state = await _read_panic_state_for_banner(redis_client)
    return _app.templates.TemplateResponse(
        request,
        "index.html",
        {
            "title": "Dashboard",
            "repos": states,
            "stats": stats,
            "latest_alert": latest_alert,
            "redis_warning": redis_warning,
            "resources": resources,
            "page_rendered_at": page_rendered_at,
            "panic_state": panic_state,
        },
    )


@router.get("/api/states")
async def api_states(request: Request) -> JSONResponse:
    redis_client = getattr(request.app.state, "redis", None)
    states, _warning = await _app.get_all_repo_states(
        redis_client, _app.CONFIG_PATH
    )
    return JSONResponse([s.model_dump(mode="json") for s in states])


@router.get("/api/stats")
async def api_stats(request: Request) -> JSONResponse:
    redis_client = getattr(request.app.state, "redis", None)
    states, _warning = await _app.get_all_repo_states(
        redis_client, _app.CONFIG_PATH
    )
    return JSONResponse(_compute_stats(states))


@router.get("/api/alerts")
async def api_alerts(request: Request) -> JSONResponse:
    """Lightweight alert summary for the dashboard's checkAlerts poll.

    Returns ``{"has_alerts": bool, "count": int}``. Sized independently of
    the repo count so the per-poll cost does not grow with deployment size,
    unlike ``/api/states`` which serializes every ``RepoState`` in full.
    The alert criterion mirrors ``_ALERT_STATES`` (ERROR) so the badge
    and the dashboard ``alerts`` panel agree on what an alert is.
    """
    redis_client = getattr(request.app.state, "redis", None)
    states, _warning = await _app.get_all_repo_states(
        redis_client, _app.CONFIG_PATH
    )
    count = sum(1 for s in states if s.state in _ALERT_STATES)
    return JSONResponse({"has_alerts": count > 0, "count": count})


async def _availability_sources(
    redis_client: Any, cfg: AppConfig
) -> list[Any]:
    """Build the canonical SignalSource list that drives the chip composition.

    Mirrors the daemon-side composition so the dashboard verdict stays in
    lock-step with the source of truth: ManualOverrideSource first (so an
    explicit operator decision wins), then the heartbeat sentinel, then
    the active-hours window.

    Redis-backed sources are omitted entirely when ``redis_client`` is
    ``None``. Including them in that case would make every query() raise
    inside ``is_operator_available``, flipping ``any_failed`` true and
    biasing the verdict to AVAILABLE — which would misreport the operator
    as available during off-hours whenever Redis is unavailable. Skipping
    them lets ``ActiveHoursSource`` compose alone from the actually
    available signal.
    """
    sources: list[Any] = []
    if redis_client is not None:
        sources.append(ManualOverrideSource(redis_client=redis_client))
        sources.append(HeartbeatSource(redis_client=redis_client))
    sources.append(
        ActiveHoursSource(
            start_hour=cfg.daemon.operator_active_hours_start,
            end_hour=cfg.daemon.operator_active_hours_end,
            timezone_name=cfg.daemon.operator_timezone,
        )
    )
    return sources


async def _read_manual_override(redis_client: Any) -> str:
    """Return the raw ``operator_override`` value as the chip's manual label.

    UI distinguishes AUTO (compose-only) from explicit AVAILABLE/AWAY so
    the operator can see whether their click stuck. Redis errors collapse
    to AUTO so a transient outage cannot misrepresent the override as
    pinned.
    """
    if redis_client is None:
        return "AUTO"
    try:
        raw = await redis_client.get(AVAILABILITY_OVERRIDE_KEY)
    except Exception:
        return "AUTO"
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    if raw in ("AVAILABLE", "AWAY"):
        return raw
    return "AUTO"


@router.get("/api/availability")
async def api_availability_get(request: Request) -> JSONResponse:
    """Return the composed availability state plus the raw manual override.

    The chip reads both: ``composed_state`` drives the dot color, and
    ``manual_override`` drives the label and the click-cycle next state
    so AUTO renders distinctly from a pinned AVAILABLE/AWAY even when
    they happen to compose the same way.
    """
    redis_client = getattr(request.app.state, "redis", None)
    cfg = load_config(_app.CONFIG_PATH)
    sources = await _availability_sources(redis_client, cfg)
    composed = await is_operator_available(sources)
    manual_override = await _read_manual_override(redis_client)
    return JSONResponse(
        {
            "composed_state": composed.value,
            "manual_override": manual_override,
        }
    )


@router.post("/api/availability/{state}")
async def api_availability_set(state: str, request: Request) -> JSONResponse:
    """Set the manual override and publish a wake on the global channel.

    POST not GET because the next-state semantics depend on current
    state — the chip cycles AUTO → AVAILABLE → AWAY → AUTO — so this is
    a non-idempotent RPC-style call rather than a fetch.

    Override write failures (set/delete) collapse to 503 so the chip
    surfaces the outage with the same status code as the redis-missing
    branch rather than bubbling a 500 that would break click handling.
    Wake fan-out (publish) is best-effort: the override is already
    persisted by the time we get there, so a publish failure must NOT
    flip the response to 503 — that would tell the chip the click
    failed, prompting a retry that drives an unintended extra state
    transition while the 60s poll safety net would have synced other
    tabs anyway.
    """
    if state not in AVAILABILITY_VALID_STATES:
        return JSONResponse({"error": "invalid_state"}, status_code=400)
    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is None:
        return JSONResponse(
            {"error": "redis_unavailable"}, status_code=503
        )
    try:
        if state == "AUTO":
            await redis_client.delete(AVAILABILITY_OVERRIDE_KEY)
        else:
            await redis_client.set(AVAILABILITY_OVERRIDE_KEY, state)
    except Exception:
        return JSONResponse(
            {"error": "redis_unavailable"}, status_code=503
        )
    # Wake all open dashboard tabs so the chip syncs without a reload.
    # A failure here is decoupled from the override write outcome — see
    # docstring — so swallow it and let the 60s poll converge.
    try:
        await redis_client.publish(AVAILABILITY_CHANNEL, state)
    except Exception:
        pass
    return JSONResponse({"manual_override": state})


@router.get("/api/availability/events")
async def api_availability_events(request: Request) -> Response:
    """SSE stream that wakes connected chips on availability changes.

    Subscribes once to ``orchestrator:availability:changed`` and forwards
    every published value as an ``availability_changed`` event. Falls
    back to keepalive comments so intermediaries do not idle the
    connection out. Returns 503 when Redis is not configured so the
    chip's own 60s polling safety net keeps the UI fresh.
    """
    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is None:
        return Response("Redis unavailable", status_code=503)
    pubsub = redis_client.pubsub()
    try:
        await pubsub.subscribe(AVAILABILITY_CHANNEL)
    except Exception:
        try:
            await pubsub.aclose()
        except Exception:
            pass
        return Response("Redis unavailable", status_code=503)

    async def _stream() -> AsyncIterator[bytes]:
        try:
            last_keepalive = asyncio.get_running_loop().time()
            while True:
                if await _is_request_disconnected(request):
                    return
                try:
                    message = await pubsub.get_message(
                        ignore_subscribe_messages=True,
                        timeout=AVAILABILITY_SSE_POLL_INTERVAL_SECONDS,
                    )
                except Exception:
                    return
                if message is not None:
                    data = message.get("data")
                    if isinstance(data, bytes):
                        data = data.decode("utf-8")
                    if isinstance(data, str):
                        payload = json.dumps(
                            {
                                "type": "availability_changed",
                                "manual_override": data,
                            }
                        )
                        yield format_sse_event(payload)
                        last_keepalive = asyncio.get_running_loop().time()
                        continue
                now = asyncio.get_running_loop().time()
                if now - last_keepalive >= AVAILABILITY_SSE_KEEPALIVE_SECONDS:
                    yield format_sse_comment("keepalive")
                    last_keepalive = now
        finally:
            try:
                await pubsub.unsubscribe(AVAILABILITY_CHANNEL)
            except Exception:
                pass
            try:
                await pubsub.aclose()
            except Exception:
                pass

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


async def _is_request_disconnected(request: Request) -> bool:
    """Best-effort check for client disconnect compatible with TestClient.

    ``Request.is_disconnected`` returns a coroutine in real ASGI but the
    TestClient may surface a sync False; fall through to ``False`` so the
    SSE generator does not block on an attribute that does not exist.
    """
    checker = getattr(request, "is_disconnected", None)
    if checker is None:
        return False
    result = checker()
    if asyncio.iscoroutine(result):
        return bool(await result)
    return bool(result)


@router.get("/api/repos/{name}/events")
async def api_repo_events(name: str, request: Request) -> Response:
    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is None:
        return Response("Redis unavailable", status_code=503)
    try:
        stream = await _app.stream_repo_events(redis_client, name, request)
    except _app.RepoEventsUnavailableError:
        return Response("Redis unavailable", status_code=503)
    return StreamingResponse(
        stream,
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


async def _augment_causes_with_dependents(
    repo: str, causes: list
) -> list[dict[str, Any]]:
    """Return ``causes`` as dicts with a ``dependents_count`` field attached.

    PR-257: each cause carries the count of queue tasks transitively
    blocked by its ``task_id`` so the dashboard can sort canceled roots
    by downstream-blast radius. Computation is best-effort: a missing
    or unreadable queue degrades to zero counts rather than failing
    the surfacing of the cards themselves.
    """
    canceled_ids = {c.task_id for c in causes}
    counts = await compute_repo_dependents_count(
        _app.REPOS_DIR, repo, canceled_ids
    )
    augmented: list[dict[str, Any]] = []
    for cause in causes:
        record = asdict(cause)
        record["dependents_count"] = counts.get(cause.task_id, 0)
        augmented.append(record)
    return augmented


@router.get("/api/cancellations/{repo}")
async def api_cancellations(repo: str, request: Request) -> JSONResponse:
    """Return recent cancellation causes for a repo (last 7 days, max 50).

    Closes the OBS-BE expanded loop: PR-252 stored the cause, PR-253 wired
    detection writes, this endpoint surfaces them so the dashboard can
    render structured "what happened" cards instead of leaving operators
    to read Redis by hand.

    Gated on ``config.yml``: ``cancellation_index:*`` keys live up to 30
    days, so a repo removed (or mistyped) would otherwise surface stale
    cards from its post-removal window. Reading a repo absent from the
    config returns ``[]``, mirroring the redis-None short-circuit.

    Each entry carries a ``dependents_count`` field (PR-257) reflecting
    the number of queue snapshot tasks transitively blocked by the
    canceled root, so clients can prioritize the highest-blast-radius
    cards.
    """
    if _find_repo_config_by_name(load_config(_app.CONFIG_PATH), repo) is None:
        return JSONResponse([])
    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is None:
        return JSONResponse([])
    since = datetime.now(timezone.utc) - timedelta(
        days=_CANCELLATIONS_WINDOW_DAYS
    )
    try:
        causes = await list_recent_cancellations(redis_client, repo, since)
    except Exception:
        # Degrade gracefully when Redis is configured but unreachable at
        # request time, matching the redis_client-is-None branch above.
        return JSONResponse([])
    return JSONResponse(
        await _augment_causes_with_dependents(
            repo, causes[:_CANCELLATIONS_MAX]
        )
    )


@router.get("/partials/redis-banner", response_class=HTMLResponse)
async def partial_redis_banner(request: Request) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    redis_warning: str | None = None
    if redis_client is None:
        redis_warning = "Redis not configured"
    else:
        try:
            await redis_client.ping()
        except Exception:
            redis_warning = "Redis connection lost"
    return _app.templates.TemplateResponse(
        request,
        "components/redis_banner.html",
        {"redis_warning": redis_warning},
    )


@router.get("/partials/repo-list", response_class=HTMLResponse)
async def partial_repo_list(request: Request) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    states, redis_warning = await _app.get_all_repo_states(
        redis_client, _app.CONFIG_PATH
    )
    resources = await _build_resources_view(redis_client, states)
    return _app.templates.TemplateResponse(
        request,
        "components/repo_cards.html",
        {
            "repos": states,
            "redis_warning": redis_warning,
            "resources": resources,
        },
    )


@router.get("/partials/stats", response_class=HTMLResponse)
async def partial_stats(request: Request) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    states, _warning = await _app.get_all_repo_states(
        redis_client, _app.CONFIG_PATH
    )
    stats = _compute_stats(states)
    alerts = _build_alerts(states)
    latest_alert = min(alerts, key=lambda a: a["duration_seconds"]) if alerts else None
    return _app.templates.TemplateResponse(
        request,
        "components/status_bar.html",
        {"stats": stats, "latest_alert": latest_alert},
    )


@router.get("/partials/activity-feed", response_class=HTMLResponse)
async def partial_activity_feed(request: Request) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    states, _warning = await _app.get_all_repo_states(
        redis_client, _app.CONFIG_PATH
    )
    feed = _build_activity_feed(states)
    return _app.templates.TemplateResponse(
        request,
        "components/activity_feed.html",
        {"feed": feed},
    )


@router.get("/partials/alerts", response_class=HTMLResponse)
async def partial_alerts(request: Request) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    states, _warning = await _app.get_all_repo_states(
        redis_client, _app.CONFIG_PATH
    )
    alerts = _build_alerts(states)
    return _app.templates.TemplateResponse(
        request,
        "components/alerts_panel.html",
        {"alerts": alerts},
    )


@router.get("/repo/{name}", response_class=HTMLResponse)
async def repo_detail(request: Request, name: str) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    page_rendered_at = _page_rendered_at_iso()
    context = await _app._repo_template_context(
        name,
        redis_client,
        include_metrics=True,
    )
    return _app.templates.TemplateResponse(
        request,
        "repo.html",
        {
            "title": name,
            **context,
            "events": list(context["repo"].history),
            "page_rendered_at": page_rendered_at,
        },
    )


@router.get("/repo/{name}/metrics")
async def repo_metrics(request: Request, name: str) -> JSONResponse:
    redis_client = getattr(request.app.state, "redis", None)
    return JSONResponse(await _recent_repo_metrics_payload(name, redis_client))


@router.get("/partials/repo/{name}", response_class=HTMLResponse)
async def partial_repo_detail(request: Request, name: str) -> HTMLResponse:
    """Return ONLY the repo summary cards for the 5s HTMX poll.

    Deliberately does not include the event log: the log self-polls via
    ``/partials/repo/{name}/events`` so its scroll position survives
    across summary refreshes (innerHTML swaps on the polling container
    would otherwise wipe the log and reset its scroll on every tick).
    """
    redis_client = getattr(request.app.state, "redis", None)
    context = await _app._repo_template_context(name, redis_client)
    return _app.templates.TemplateResponse(
        request,
        "components/repo_summary.html",
        context,
    )


@router.get(
    "/partials/repo/{name}/metrics",
    response_class=HTMLResponse,
)
async def partial_repo_metrics(request: Request, name: str) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    return _app.templates.TemplateResponse(
        request,
        "components/pr_metrics.html",
        {
            "repo_name": name,
            "metrics_records": await _recent_repo_metrics_payload(
                name, redis_client
            ),
        },
    )


@router.get(
    "/partials/repo/{name}/events",
    response_class=HTMLResponse,
)
async def partial_repo_events(request: Request, name: str) -> HTMLResponse:
    """Return the list fragment swapped into the event log's scroll wrapper.

    The wrapper polls this endpoint every 5s with ``hx-swap="innerHTML"``,
    so the response is just the ``<ul>`` of entries (or the empty-state
    paragraph) — not the surrounding ``<section>`` — which keeps the
    wrapper itself mounted and its scrollTop intact across ticks.
    """
    redis_client = getattr(request.app.state, "redis", None)
    state = await _app.get_repo_state(name, redis_client, _app.CONFIG_PATH)
    return _app.templates.TemplateResponse(
        request,
        "components/event_list.html",
        {
            "repo": state,
            "events": list(state.history),
            # ``oob=True`` switches the template into "poll response" mode:
            # it emits an hx-swap-oob count span so the header's
            # "N events" label refreshes with the list instead of going
            # stale at the initial page load's value.
            "oob": True,
        },
    )


@router.get("/partials/repo/{name}/cli-log", response_class=HTMLResponse)
async def repo_cli_log(request: Request, name: str) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    cfg = load_config(_app.CONFIG_PATH)
    if not any(repo_slug_from_url(r.url) == name for r in cfg.repositories):
        return HTMLResponse(
            '<p class="text-sm text-gray-500 italic">No CLI log available.</p>'
        )
    log_text = ""
    if redis_client is not None:
        try:
            raw = await redis_client.get(cli_log_latest(name))
            if raw is not None:
                log_text = raw if isinstance(raw, str) else raw.decode()
        except Exception:
            log_text = ""
    if not log_text:
        return HTMLResponse(
            '<p class="text-sm text-gray-500 italic">No CLI log available.</p>'
        )
    import html as html_mod

    escaped = html_mod.escape(log_text)
    return HTMLResponse(
        f'<pre class="bg-black text-green-400 font-mono text-xs p-4'
        f' overflow-auto max-h-96 rounded">{escaped}</pre>'
    )


@router.get(
    "/partials/repo/{name}/cancellations",
    response_class=HTMLResponse,
)
async def partial_repo_cancellations(
    name: str, request: Request
) -> HTMLResponse:
    """Render recent cancellation cards into the lazy-loaded section.

    The ``<details>`` wrapper in repo.html triggers this fetch on the
    first reveal; closing and reopening the section refetches. The
    cancellation surface is operator-review, not real-time, so no SSE
    wake or periodic poll is needed for v1 (PR-254 implementation note).

    Gated on ``config.yml`` for the same reason as ``api_cancellations``
    above: stale ``cancellation_index:*`` keys (TTL up to 30 days) must
    not resurface as cards for repos that were removed from the config.

    PR-310: accepts a ``subsource_filter`` query param drawn from the
    cancellation history dropdown. Unknown values fall back to "All"
    (no filter) so a malformed URL never 4xx-s the partial. The filter
    is applied after the storage read so the 7-day window and 50-item
    cap still bound the candidate set.
    """
    redis_client = getattr(request.app.state, "redis", None)
    subsource_filter = request.query_params.get("subsource_filter", "") or ""
    causes: list = []
    repo_configured = (
        _find_repo_config_by_name(load_config(_app.CONFIG_PATH), name)
        is not None
    )
    if repo_configured and redis_client is not None:
        since = datetime.now(timezone.utc) - timedelta(
            days=_CANCELLATIONS_WINDOW_DAYS
        )
        try:
            causes = (
                await list_recent_cancellations(redis_client, name, since)
            )[:_CANCELLATIONS_MAX]
        except Exception:
            # Redis temporarily unreachable: render the empty-state placeholder
            # so the HTMX swap target stays stable instead of 5xx-ing the panel.
            causes = []
    allowed_subsources = _SUBSOURCE_FILTER_GROUPS.get(subsource_filter)
    if allowed_subsources is not None:
        causes = [
            cause
            for cause in causes
            if isinstance(getattr(cause, "payload", None), dict)
            and cause.payload.get("subsource") in allowed_subsources
        ]
    augmented = (
        await _augment_causes_with_dependents(name, causes) if causes else []
    )
    return _app.templates.TemplateResponse(
        request,
        "components/cancellation_history.html",
        {
            "causes": augmented,
            "subsource_filter": subsource_filter,
            "repo_name": name,
        },
    )


# Imported at end-of-file so all ``@router`` decorators above have already
# populated ``router.routes`` before ``app.py`` reaches
# ``app.include_router(_dashboard_routes.router)``. FastAPI snapshots
# ``router.routes`` at include time, so an early import would let app.py
# load this module while it is still partial (router empty) and silently
# drop every endpoint declared below the import.
from src.web import app as _app  # noqa: E402
