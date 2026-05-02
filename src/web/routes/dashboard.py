"""Dashboard rendering routes and supporting view helpers.

Owns the high-traffic surfaces of the operator dashboard: the home grid,
repo detail page, JSON status endpoints, and the HTMX partials that swap
into them. Repo-state reads go through ``src.web.services.repo_state``;
mutation routes (pause/resume/stop/coder) live in
``src.web.routes.repo_control``.
"""

from __future__ import annotations

import hashlib
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

import redis.asyncio as aioredis
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse

from src.coders import build_coder_registry
from src.config import AppConfig, RepoConfig, load_config
from src.daemon.github_rate_limit import (
    RateLimitBudget,
    read_graphql_budget,
    read_rest_budget,
    recent_cycle_burns,
)
from src.keyspace import cli_log_latest
from src.metrics import MetricsStore, RunRecord
from src.models import PipelineState, RepoState
from src.utils import repo_slug_from_url
from src.web import app as _app
from src.web.services.coder import _effective_coder_name
from src.web.services.repo_state import _find_repo_config_by_name

router = APIRouter()

_HISTORY_LIMIT = 100
_METRICS_PANEL_LIMIT = 20
_METRICS_SCAN_LIMIT = 100

_ACTIVE_RUN_STATES = {
    PipelineState.CODING,
    PipelineState.WATCH,
    PipelineState.FIX,
    PipelineState.MERGE,
}


def _format_reset_unix(reset_unix: int | None) -> str:
    """Render ``reset_unix`` as ``HH:MM UTC`` for resource-chip tooltips.

    Returns ``"unknown"`` for ``None`` so the template still has a string
    to interpolate; the chip template only renders the "Resets at" line
    when ``reset_unix`` is truthy, so this branch is defensive.
    """
    if not reset_unix:
        return "unknown"
    return datetime.fromtimestamp(int(reset_unix), tz=timezone.utc).strftime(
        "%H:%M UTC"
    )


def _daemon_default_coder_name(config: AppConfig) -> str:
    """Return the daemon-level default coder name."""
    return config.daemon.coder.value


def _repo_coder_form_value(repo_config: RepoConfig | None) -> str:
    """Return the raw repo-level coder selection for the detail form."""
    if repo_config is None or repo_config.coder is None:
        return "any"
    return repo_config.coder.value


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


def _coder_rate_limit_supported(coder: str | None) -> bool:
    """Return whether ``coder`` has meaningful rate-limit usage data."""
    return coder in {"claude", "codex"}


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


def _resource_zone(percent_remaining: float | None) -> str:
    """Map ``percent_remaining`` to one of ``green|amber|red|none``.

    Boundaries: ``>=50`` green, ``20<=pct<50`` amber, ``<20`` red. ``None``
    (resource value unknown) collapses to ``none`` so the chip can render
    a neutral placeholder rather than mis-coloring a missing reading.
    """
    if percent_remaining is None:
        return "none"
    if percent_remaining >= 50:
        return "green"
    if percent_remaining >= 20:
        return "amber"
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
            "percent_remaining": None,
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
            "percent_remaining": None,
            "reset_unix": None,
            "zone": "none",
        }
    pct = budget.remaining_percent
    return {
        "remaining": budget.remaining,
        "limit": budget.limit,
        "percent_remaining": round(pct, 1),
        "reset_unix": int(budget.reset_at.timestamp()),
        "zone": _resource_zone(pct),
    }


def _claude_usage_chip(
    states: list[RepoState],
    *,
    window: Literal["session", "weekly"],
) -> dict[str, Any]:
    """Aggregate Claude usage across active Claude repos for the chip row.

    All Claude repos share one OAuth account so any active Claude state's
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
        if (state.coder or "") != "claude":
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
            "percent_remaining": None,
            "reset_unix": None,
            "zone": "none",
        }
    if window == "session":
        used = candidate.usage_session_percent or 0
        reset_unix = candidate.usage_session_resets_at
    else:
        used = candidate.usage_weekly_percent or 0
        reset_unix = candidate.usage_weekly_resets_at
    pct_remaining = max(0.0, min(100.0, 100.0 - float(used)))
    return {
        "remaining": int(round(pct_remaining)),
        "limit": 100,
        "percent_remaining": round(pct_remaining, 1),
        "reset_unix": reset_unix,
        "zone": _resource_zone(pct_remaining),
    }


async def _build_resources_view(
    redis_client: aioredis.Redis | None,
    states: list[RepoState],
) -> dict[str, dict[str, Any]]:
    """Return the four-resource payload for the dashboard chip row.

    Each entry exposes ``remaining``, ``limit``, ``percent_remaining``,
    ``reset_unix`` and a precomputed ``zone`` (``green|amber|red|none``).
    Missing data renders as ``percent_remaining: None`` plus ``zone: none``
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
    config = load_config(config_path)
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
    return {
        "repo": state,
        "recent_graphql_burns": recent_graphql_burns,
        "repo_config": repo_config,
        "daemon": config.daemon,
        "coders": build_coder_registry().list_coders(),
        "effective_coder": effective_coder,
        "active_rate_limit_coder": active_rate_limit_coder,
        "active_rate_limit_coder_label": (
            "Claude" if active_rate_limit_coder == "claude" else "Codex"
        ),
        "show_rate_limit_badge": show_rate_limit_badge,
        "selected_repo_coder": _repo_coder_form_value(repo_config),
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
_ALERT_STATES = frozenset({PipelineState.HUNG, PipelineState.ERROR})
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


_ALERT_KIND_ORDER = {"ERROR": 0, "HUNG": 1}


def _format_alert_duration(seconds: int) -> str:
    """Return a human-readable duration for an alert card.

    Used by the alerts panel to render how long a repo has been in the
    ERROR/HUNG state. Negative inputs (clock skew, a state whose
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

    HUNG prefers ``current_pr.last_activity`` (the daemon's own
    hung-detection signal) when it is set. Otherwise — and for every
    ERROR card — scan ``state.history`` for the most recent transition
    into the current state. This matters because ``publish_state``
    rewrites ``state.last_updated`` on every daemon cycle (see
    ``src/daemon/runner.py``), so using ``last_updated`` as the "since"
    timestamp would make an hours-old ERROR card display "a few sec"
    forever and break duration-based sorting in the alerts bucket.

    Falls through to ``state.last_updated`` only when history carries
    no matching transition (a bootstrap cycle or a legacy payload with
    unparseable timestamps) — in that case "now-ish" is the best signal
    we have and the alert still renders.
    """
    if (
        state.state == PipelineState.HUNG
        and state.current_pr is not None
        and state.current_pr.last_activity is not None
    ):
        return state.current_pr.last_activity
    transition = _most_recent_transition_into(
        state.history, state.state.value
    )
    if transition is not None:
        return transition
    return state.last_updated


def _build_alerts(states: list[RepoState]) -> list[dict[str, Any]]:
    """Collect alert cards for every repo currently in HUNG or ERROR.

    Each alert dict is self-contained so the template does not need to
    reach back into ``RepoState``. Sort order: ERROR first (highest
    severity), then HUNG, then by duration descending so the longest-
    standing problem bubbles to the top of its severity bucket.
    """
    now = datetime.now(timezone.utc)
    alerts: list[dict[str, Any]] = []

    for state in states:
        if state.state not in _ALERT_STATES:
            continue
        kind = state.state.value  # "ERROR" or "HUNG"
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
        if kind == "ERROR":
            alert["error_message"] = state.error_message or ""
        else:
            pr = state.current_pr
            if pr is not None:
                alert["pr_number"] = pr.number
                alert["pr_url"] = pr.url
                alert["review_status"] = pr.review_status.value
            else:
                alert["pr_number"] = None
                alert["pr_url"] = ""
                alert["review_status"] = ""
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
    * ``alerts`` - repos currently in HUNG/ERROR.
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


@router.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    states, redis_warning = await _app.get_all_repo_states(
        redis_client, _app.CONFIG_PATH
    )
    stats = _compute_stats(states)
    alerts = _build_alerts(states)
    latest_alert = min(alerts, key=lambda a: a["duration_seconds"]) if alerts else None
    resources = await _build_resources_view(redis_client, states)
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
