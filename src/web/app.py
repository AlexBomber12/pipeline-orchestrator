"""FastAPI dashboard application.

Read-only web UI that lists configured repositories and their pipeline state.
State is published to Redis by the daemon; if a repository has no entry in
Redis the dashboard renders a default ``IDLE`` state derived from
``config.yml``.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import subprocess
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, AsyncIterator

import redis.asyncio as aioredis
from fastapi import FastAPI, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from src.config import (
    add_repository,
    load_config,
    remove_repository,
    update_daemon_config,
    update_repository,
)
from src.models import PipelineState, RepoState
from src.utils import repo_name_from_url

DEFAULT_REDIS_URL = "redis://localhost:6379/0"
CONFIG_PATH = "config.yml"
REPOS_DIR = "/data/repos"

_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))


def _default_repo_state(name: str, url: str) -> RepoState:
    """Return a default ``IDLE`` state for ``name``/``url``."""
    return RepoState(
        url=url,
        name=name,
        state=PipelineState.IDLE,
        current_task=None,
        current_pr=None,
        error_message=None,
        last_updated=datetime.now(timezone.utc),
    )


async def get_repo_state(
    name: str,
    redis_client: aioredis.Redis | None,
    config_path: str = CONFIG_PATH,
) -> RepoState:
    """Return the state for a single repo by name.

    Looks the repo up in ``config.yml`` to recover the canonical URL, then
    tries to fetch ``pipeline:{name}`` from Redis. Falls back to a default
    ``IDLE`` state if the repo is unknown, Redis is unavailable, or the
    stored payload cannot be decoded. Redis is not consulted for repos
    missing from ``config.yml`` so a stale ``pipeline:{name}`` key left
    over from a removed repo cannot resurface as live state.
    """
    cfg = load_config(config_path)
    url = ""
    found = False
    for repo in cfg.repositories:
        if repo_name_from_url(repo.url) == name:
            url = repo.url
            found = True
            break

    if found and redis_client is not None:
        try:
            payload = await redis_client.get(f"pipeline:{name}")
        except Exception:
            payload = None
        if payload:
            try:
                return RepoState.model_validate_json(payload)
            except Exception:
                pass

    return _default_repo_state(name, url)


async def get_all_repo_states(
    redis_client: aioredis.Redis | None,
    config_path: str = CONFIG_PATH,
) -> list[RepoState]:
    """Return the list of repo states for every repo in ``config.yml``.

    For each configured repo, look up ``pipeline:{name}`` in Redis. If the
    key is missing or Redis is unavailable, fall back to a default ``IDLE``
    state with the current timestamp. Once a Redis read fails inside a single
    request, further Redis lookups are skipped so an unreachable broker
    cannot turn each configured repo into another timing-out call.
    """
    cfg = load_config(config_path)
    states: list[RepoState] = []
    redis_available = redis_client is not None

    for repo in cfg.repositories:
        name = repo_name_from_url(repo.url)
        state: RepoState | None = None

        if redis_available:
            try:
                payload = await redis_client.get(f"pipeline:{name}")
            except Exception:
                payload = None
                redis_available = False
            if payload:
                try:
                    state = RepoState.model_validate_json(payload)
                except Exception:
                    state = None

        if state is None:
            state = _default_repo_state(name, repo.url)

        states.append(state)

    return states


_MERGE_EVENT_MARKER = "Merged PR"
_ITERATION_EVENT_MARKER = "Fix pushed, iteration"
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


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    redis_url = os.environ.get("REDIS_URL", DEFAULT_REDIS_URL)
    client = aioredis.from_url(redis_url, decode_responses=True)
    app.state.redis = client
    try:
        yield
    finally:
        try:
            await client.aclose()
        except Exception:
            pass


app = FastAPI(title="Pipeline Orchestrator", lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    states = await get_all_repo_states(redis_client)
    stats = _compute_stats(states)
    alerts = _build_alerts(states)
    latest_alert = min(alerts, key=lambda a: a["duration_seconds"]) if alerts else None
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "title": "Dashboard",
            "repos": states,
            "stats": stats,
            "latest_alert": latest_alert,
        },
    )


@app.get("/api/states")
async def api_states(request: Request) -> JSONResponse:
    redis_client = getattr(request.app.state, "redis", None)
    states = await get_all_repo_states(redis_client)
    return JSONResponse([s.model_dump(mode="json") for s in states])


@app.get("/api/stats")
async def api_stats(request: Request) -> JSONResponse:
    redis_client = getattr(request.app.state, "redis", None)
    states = await get_all_repo_states(redis_client)
    return JSONResponse(_compute_stats(states))


@app.get("/partials/repo-list", response_class=HTMLResponse)
async def partial_repo_list(request: Request) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    states = await get_all_repo_states(redis_client)
    return templates.TemplateResponse(
        request,
        "components/repo_cards.html",
        {"repos": states},
    )


@app.get("/partials/stats", response_class=HTMLResponse)
async def partial_stats(request: Request) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    states = await get_all_repo_states(redis_client)
    stats = _compute_stats(states)
    alerts = _build_alerts(states)
    latest_alert = min(alerts, key=lambda a: a["duration_seconds"]) if alerts else None
    return templates.TemplateResponse(
        request,
        "components/status_bar.html",
        {"stats": stats, "latest_alert": latest_alert},
    )


@app.get("/partials/activity-feed", response_class=HTMLResponse)
async def partial_activity_feed(request: Request) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    states = await get_all_repo_states(redis_client)
    feed = _build_activity_feed(states)
    return templates.TemplateResponse(
        request,
        "components/activity_feed.html",
        {"feed": feed},
    )


@app.get("/partials/alerts", response_class=HTMLResponse)
async def partial_alerts(request: Request) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    states = await get_all_repo_states(redis_client)
    alerts = _build_alerts(states)
    return templates.TemplateResponse(
        request,
        "components/alerts_panel.html",
        {"alerts": alerts},
    )


@app.get("/repo/{name}", response_class=HTMLResponse)
async def repo_detail(request: Request, name: str) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    state = await get_repo_state(name, redis_client)
    return templates.TemplateResponse(
        request,
        "repo.html",
        {
            "title": name,
            "repo": state,
            "events": list(state.history),
        },
    )


@app.get("/partials/repo/{name}", response_class=HTMLResponse)
async def partial_repo_detail(request: Request, name: str) -> HTMLResponse:
    """Return ONLY the repo summary cards for the 5s HTMX poll.

    Deliberately does not include the event log: the log self-polls via
    ``/partials/repo/{name}/events`` so its scroll position survives
    across summary refreshes (innerHTML swaps on the polling container
    would otherwise wipe the log and reset its scroll on every tick).
    """
    redis_client = getattr(request.app.state, "redis", None)
    state = await get_repo_state(name, redis_client)
    return templates.TemplateResponse(
        request,
        "components/repo_summary.html",
        {"repo": state},
    )


@app.get(
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
    state = await get_repo_state(name, redis_client)
    return templates.TemplateResponse(
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


@app.get("/partials/repo/{name}/cli-log", response_class=HTMLResponse)
async def repo_cli_log(request: Request, name: str) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    cfg = load_config(CONFIG_PATH)
    if not any(repo_name_from_url(r.url) == name for r in cfg.repositories):
        return HTMLResponse(
            '<p class="text-sm text-gray-500 italic">No CLI log available.</p>'
        )
    log_text = ""
    if redis_client is not None:
        try:
            raw = await redis_client.get(f"cli_log:{name}:latest")
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


def _render_settings_repo_list(request: Request) -> HTMLResponse:
    """Render the settings repo list for a successful mutation response.

    The response includes an OOB clear of ``#settings-error`` so that any
    error banner left over from a prior 422/503 mutation is wiped as soon
    as a subsequent mutation succeeds (otherwise HTMX keeps the stale
    message because success responses only swap ``#settings-repo-list``).
    The daemon block is passed alongside the repo list so the
    ``review_timeout_min`` input can render the daemon-level default as
    placeholder text whenever a repo has not opted into a per-repo
    override.
    """
    cfg = load_config(CONFIG_PATH)
    return templates.TemplateResponse(
        request,
        "components/settings_repo_list_response.html",
        {"repos": cfg.repositories, "daemon": cfg.daemon},
    )


def _render_settings_error(
    request: Request, message: str, status_code: int
) -> HTMLResponse:
    cfg = load_config(CONFIG_PATH)
    return templates.TemplateResponse(
        request,
        "components/settings_error.html",
        {
            "message": message,
            "repos": cfg.repositories,
            "daemon": cfg.daemon,
        },
        status_code=status_code,
    )


@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request) -> HTMLResponse:
    cfg = load_config(CONFIG_PATH)
    return templates.TemplateResponse(
        request,
        "settings.html",
        {
            "title": "Settings",
            "repos": cfg.repositories,
            "daemon": cfg.daemon,
        },
    )


def _render_settings_daemon(request: Request) -> HTMLResponse:
    """Render the daemon settings form for a successful response.

    Uses ``settings_daemon_response.html`` rather than the bare
    ``settings_daemon.html`` partial so HTMX also receives an OOB clear
    of ``#settings-daemon-error`` — otherwise an error banner left over
    from a prior 422/503 PUT would keep hanging around after a subsequent
    successful mutation.
    """
    cfg = load_config(CONFIG_PATH)
    return templates.TemplateResponse(
        request,
        "components/settings_daemon_response.html",
        {"daemon": cfg.daemon},
    )


def _render_settings_daemon_error(
    request: Request, message: str, status_code: int
) -> HTMLResponse:
    cfg = load_config(CONFIG_PATH)
    return templates.TemplateResponse(
        request,
        "components/settings_daemon_error.html",
        {"daemon": cfg.daemon, "message": message},
        status_code=status_code,
    )


@app.get("/partials/settings/daemon", response_class=HTMLResponse)
async def partial_settings_daemon(request: Request) -> HTMLResponse:
    return _render_settings_daemon(request)


@app.put("/settings/daemon", response_class=HTMLResponse)
async def put_settings_daemon(
    request: Request,
    poll_interval_sec: str | None = Form(None),
    review_timeout_min: str | None = Form(None),
    hung_fallback_codex_review: str | None = Form(None),
    error_handler_use_ai: str | None = Form(None),
    planned_pr_timeout_sec: str | None = Form(None),
    fix_review_timeout_sec: str | None = Form(None),
) -> HTMLResponse:
    """Update daemon settings.

    Mirrors ``put_settings_repo`` in accepting every field as ``str | None``
    so that a cleared number input (``review_timeout_min=``) is handled as
    a no-op instead of tripping FastAPI's request parser. Numeric fields
    must stay strictly positive: a zero or negative ``poll_interval_sec``
    would busy-loop the daemon, and a zero or negative ``review_timeout_min``
    would flag every in-flight PR as hung the moment it is created.
    """
    updates: dict[str, Any] = {}
    try:
        if poll_interval_sec is not None and poll_interval_sec != "":
            updates["poll_interval_sec"] = _coerce_int(
                poll_interval_sec, "poll_interval_sec", min_value=1
            )
        if review_timeout_min is not None and review_timeout_min != "":
            updates["review_timeout_min"] = _coerce_int(
                review_timeout_min, "review_timeout_min", min_value=1
            )
        if (
            hung_fallback_codex_review is not None
            and hung_fallback_codex_review != ""
        ):
            updates["hung_fallback_codex_review"] = _coerce_bool(
                hung_fallback_codex_review, "hung_fallback_codex_review"
            )
        if error_handler_use_ai is not None and error_handler_use_ai != "":
            updates["error_handler_use_ai"] = _coerce_bool(
                error_handler_use_ai, "error_handler_use_ai"
            )
        if (
            planned_pr_timeout_sec is not None
            and planned_pr_timeout_sec != ""
        ):
            updates["planned_pr_timeout_sec"] = _coerce_int(
                planned_pr_timeout_sec, "planned_pr_timeout_sec", min_value=1
            )
        if (
            fix_review_timeout_sec is not None
            and fix_review_timeout_sec != ""
        ):
            updates["fix_review_timeout_sec"] = _coerce_int(
                fix_review_timeout_sec, "fix_review_timeout_sec", min_value=1
            )
    except ValueError as exc:
        return _render_settings_daemon_error(request, str(exc), 422)

    try:
        update_daemon_config(path=CONFIG_PATH, **updates)
    except ValueError as exc:
        return _render_settings_daemon_error(request, str(exc), 422)
    except OSError as exc:
        return _render_settings_daemon_error(
            request, f"Failed to write config.yml: {exc}", 503
        )
    return _render_settings_daemon(request)


_AUTH_CHECK_TIMEOUT_SEC = 5


def _run_auth_command(
    cmd: list[str], env: dict[str, str] | None = None
) -> tuple[int, str, str]:
    """Run ``cmd`` for an auth status probe and return (rc, stdout, stderr).

    Any failure to spawn (``FileNotFoundError``, ``PermissionError``) or
    the subprocess exceeding ``_AUTH_CHECK_TIMEOUT_SEC`` is reported as a
    non-zero return code so the caller can render a red status dot without
    crashing the request.
    """
    try:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=_AUTH_CHECK_TIMEOUT_SEC,
            check=False,
            env=env,
        )
    except FileNotFoundError:
        return 127, "", f"{cmd[0]} not found"
    except PermissionError as exc:
        return 126, "", str(exc)
    except subprocess.TimeoutExpired:
        return 124, "", f"{cmd[0]} timed out after {_AUTH_CHECK_TIMEOUT_SEC}s"
    return completed.returncode, completed.stdout or "", completed.stderr or ""


def _auth_probe_env(**overrides: str) -> dict[str, str]:
    """Return the environment block used for an auth CLI probe.

    ``docker-compose.yml`` only sets ``CLAUDE_CONFIG_DIR`` / ``GH_CONFIG_DIR``
    on the ``daemon`` service; the ``web`` service inherits none of them and
    would otherwise probe the wrong credential location (the web container's
    home directory, not ``/data/auth``). Reading the paths from ``config.yml``
    and injecting them into the subprocess environment keeps the dashboard
    in lock-step with whatever auth context the daemon was built to use, so
    "Authorized" on the dashboard matches "the daemon can actually run".
    """
    env = os.environ.copy()
    env.update(overrides)
    return env


def _check_claude_auth() -> dict[str, str]:
    """Probe the ``claude`` CLI and report its authorization status."""
    cfg = load_config(CONFIG_PATH)
    env = _auth_probe_env(CLAUDE_CONFIG_DIR=cfg.auth.claude_config_dir)
    rc, stdout, stderr = _run_auth_command(["claude", "--version"], env=env)
    if rc == 0:
        output = (stdout or stderr).strip()
        detail = output.splitlines()[0] if output else "claude CLI available"
        return {"status": "ok", "detail": detail}
    detail = (stderr or stdout).strip() or "claude CLI not available"
    return {"status": "error", "detail": detail}


def _check_gh_auth() -> dict[str, str]:
    """Probe the ``gh`` CLI and report its authorization status."""
    cfg = load_config(CONFIG_PATH)
    env = _auth_probe_env(GH_CONFIG_DIR=cfg.auth.gh_config_dir)
    rc, stdout, stderr = _run_auth_command(
        ["gh", "auth", "status"], env=env
    )
    # ``gh auth status`` prints its report to stderr on recent versions and
    # to stdout on older ones, so merge both streams before scanning.
    combined = f"{stdout}\n{stderr}".strip()
    if rc == 0 and "Logged in" in combined:
        detail = ""
        for line in combined.splitlines():
            stripped = line.strip()
            if "Logged in" in stripped:
                detail = stripped
                break
        return {"status": "ok", "detail": detail or "Logged in"}
    if combined:
        detail = combined.splitlines()[0].strip()
    else:
        detail = "gh CLI not configured"
    return {"status": "error", "detail": detail}


async def _collect_auth_status() -> dict[str, dict[str, str]]:
    """Return ``{"claude": ..., "gh": ...}`` auth status dicts.

    Each probe invokes a blocking ``subprocess.run`` call with a 5s
    timeout, so they would block the event loop if awaited directly from
    an async handler. Dispatching them through ``asyncio.to_thread`` and
    ``asyncio.gather`` moves the blocking work onto the default thread
    pool and runs both probes concurrently, so the dashboard's 30s HTMX
    auth-status poll cannot stall the worker for up to ~10s (two serial
    5s timeouts) whenever a CLI is missing or slow.
    """
    claude, gh = await asyncio.gather(
        asyncio.to_thread(_check_claude_auth),
        asyncio.to_thread(_check_gh_auth),
    )
    return {"claude": claude, "gh": gh}


@app.get("/api/auth-status")
async def api_auth_status() -> JSONResponse:
    return JSONResponse(await _collect_auth_status())


@app.get("/partials/settings/auth-status", response_class=HTMLResponse)
async def partial_settings_auth_status(request: Request) -> HTMLResponse:
    auth = await _collect_auth_status()
    return templates.TemplateResponse(
        request,
        "components/settings_auth.html",
        {"auth": auth},
    )


@app.get("/partials/settings/repo-list", response_class=HTMLResponse)
async def partial_settings_repo_list(request: Request) -> HTMLResponse:
    return _render_settings_repo_list(request)


def _render_config_write_error(
    request: Request, exc: OSError
) -> HTMLResponse:
    """Render ``settings_error.html`` for a failed ``save_config`` write.

    In the default ``docker-compose.yml`` the ``web`` service gets
    ``config.yml`` bind-mounted read-write, but operators still run into
    ``PermissionError`` / ``OSError`` in hardened deployments (file owned by
    another uid, host filesystem mounted read-only, disk full, etc.). Catch
    those so a failed write renders the HTML error partial with status 503
    instead of FastAPI's default JSON 500.
    """
    return _render_settings_error(
        request,
        f"Failed to write config.yml: {exc}",
        503,
    )


@app.post("/settings/repos", response_class=HTMLResponse)
async def post_settings_repo(
    request: Request,
    url: str = Form(...),
    branch: str = Form("main"),
    auto_merge: bool = Form(True),
) -> HTMLResponse:
    try:
        add_repository(
            url,
            path=CONFIG_PATH,
            branch=branch,
            auto_merge=auto_merge,
        )
    except ValueError as exc:
        return _render_settings_error(request, str(exc), 422)
    except OSError as exc:
        return _render_config_write_error(request, exc)
    return _render_settings_repo_list(request)


@app.delete("/settings/repos", response_class=HTMLResponse)
async def delete_settings_repo(
    request: Request, url: str
) -> HTMLResponse:
    """Remove a repository by its full URL.

    The URL is the unique key in the config (basenames can collide across
    owners), so settings mutations key off the normalized URL instead of
    the repo name.
    """
    try:
        remove_repository(url, path=CONFIG_PATH)
    except ValueError as exc:
        return _render_settings_error(request, str(exc), 404)
    except OSError as exc:
        return _render_config_write_error(request, exc)
    return _render_settings_repo_list(request)


_BOOL_TRUE = {"true", "1", "yes", "on"}
_BOOL_FALSE = {"false", "0", "no", "off"}


def _coerce_bool(value: str, field: str) -> bool:
    lowered = value.strip().lower()
    if lowered in _BOOL_TRUE:
        return True
    if lowered in _BOOL_FALSE:
        return False
    raise ValueError(f"{field} must be a boolean")


def _coerce_int(value: str, field: str, min_value: int | None = None) -> int:
    try:
        parsed = int(value.strip())
    except ValueError as exc:
        raise ValueError(f"{field} must be an integer") from exc
    if min_value is not None and parsed < min_value:
        raise ValueError(f"{field} must be at least {min_value}")
    return parsed


@app.put("/settings/repos", response_class=HTMLResponse)
async def put_settings_repo(
    request: Request,
    url: str,
    branch: str | None = Form(None),
    auto_merge: str | None = Form(None),
    review_timeout_min: str | None = Form(None),
) -> HTMLResponse:
    """Update a repository by its full URL.

    The URL is the unique key in the config (basenames can collide across
    owners), so settings mutations key off the normalized URL instead of
    the repo name.

    All fields are taken as ``str | None`` rather than their final types
    so a triggered HTMX change event for a single input (which only sends
    that one field) doesn't trip FastAPI's request parser on the fields
    it didn't send. Semantics per field:

    * ``None`` (field absent from the form payload): leave the stored
      value alone.
    * Non-empty string: parse and update.
    * Empty string on ``review_timeout_min``: clear the per-repo override
      so the runner falls back to ``daemon.review_timeout_min`` — this is
      the only way for an upgraded deployment (whose existing
      ``config.yml`` still has explicit per-repo values) to opt a repo
      into the daemon-level default after PR-016.
    * Empty string on any other field: no-op. ``branch`` / ``auto_merge``
      are required values.
    """
    updates: dict[str, object | None] = {}
    if branch is not None and branch != "":
        updates["branch"] = branch
    try:
        if auto_merge is not None and auto_merge != "":
            updates["auto_merge"] = _coerce_bool(auto_merge, "auto_merge")
        if review_timeout_min is not None:
            if review_timeout_min == "":
                updates["review_timeout_min"] = None
            else:
                updates["review_timeout_min"] = _coerce_int(
                    review_timeout_min, "review_timeout_min", min_value=1
                )
    except ValueError as exc:
        return _render_settings_error(request, str(exc), 422)

    try:
        update_repository(url, path=CONFIG_PATH, **updates)
    except ValueError as exc:
        message = str(exc)
        status = 404 if message.startswith("Repository not found") else 422
        return _render_settings_error(request, message, status)
    except OSError as exc:
        return _render_config_write_error(request, exc)
    return _render_settings_repo_list(request)


# ---------------------------------------------------------------------------
# Upload tasks
# ---------------------------------------------------------------------------

_UPLOAD_MAX_TOTAL_BYTES = 1_000_000  # 1 MB
_ALLOWED_TASK_PATTERN = r"^(QUEUE\.md|PR-[A-Za-z0-9._-]+\.md)$"
UPLOADS_DIR = "/data/uploads"

import json as _json  # noqa: E402 — kept near usage
import re as _re  # noqa: E402 — kept near usage
import shutil  # noqa: E402 — kept near usage
import uuid as _uuid  # noqa: E402 — kept near usage

_upload_locks: dict[str, asyncio.Lock] = {}


def _get_upload_lock(repo_name: str) -> asyncio.Lock:
    if repo_name not in _upload_locks:
        _upload_locks[repo_name] = asyncio.Lock()
    return _upload_locks[repo_name]


def _render_upload_error(
    request: Request, message: str, status_code: int, repo_name: str = ""
) -> HTMLResponse:
    response = templates.TemplateResponse(
        request,
        "components/upload_error.html",
        {"message": message},
        status_code=status_code,
    )
    if repo_name:
        css_name = _re.sub(r"([.#\[\]:>+~(){}|^$*!])", r"\\\1", repo_name)
        response.headers["HX-Retarget"] = f"#upload-error-{css_name}"
        response.headers["HX-Reswap"] = "innerHTML"
    return response


def _render_upload_success(
    request: Request, message: str, repo_name: str
) -> HTMLResponse:
    response = templates.TemplateResponse(
        request,
        "components/upload_success.html",
        {"message": message},
    )
    css_name = _re.sub(r"([.#\[\]:>+~(){}|^$*!])", r"\\\1", repo_name)
    response.headers["HX-Retarget"] = f"#upload-error-{css_name}"
    response.headers["HX-Reswap"] = "innerHTML"
    return response


@app.post("/repos/{name}/upload-tasks", response_class=HTMLResponse)
async def upload_tasks(
    request: Request, name: str, files: list[UploadFile] = []
) -> HTMLResponse:
    cfg = load_config(CONFIG_PATH)
    found = False
    for repo in cfg.repositories:
        if repo_name_from_url(repo.url) == name:
            found = True
            break

    if not found:
        return _render_upload_error(request, f"Repository '{name}' not found", 404, repo_name=name)

    repo_path = f"{REPOS_DIR}/{name}"
    if not Path(repo_path).is_dir():
        return _render_upload_error(
            request, f"Repository '{name}' is not cloned", 422, repo_name=name
        )

    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is None:
        return _render_upload_error(
            request,
            "Cannot verify repo state (Redis unavailable). Upload blocked.",
            503,
            repo_name=name,
        )
    try:
        raw = await redis_client.get(f"pipeline:{name}")
    except Exception:
        return _render_upload_error(
            request,
            "Cannot verify repo state (Redis error). Upload blocked.",
            503,
            repo_name=name,
        )
    if raw:
        try:
            repo_state = RepoState.model_validate_json(raw)
        except Exception:
            return _render_upload_error(
                request,
                "Cannot verify repo state (corrupt data). Upload blocked.",
                503,
                repo_name=name,
            )
    else:
        return _render_upload_error(
            request,
            "Cannot verify repo state (no state recorded). Upload blocked.",
            503,
            repo_name=name,
        )
    if repo_state.state != PipelineState.IDLE:
        return _render_upload_error(
            request,
            f"Cannot upload while repo is {repo_state.state.value}. Wait until IDLE.",
            422,
            repo_name=name,
        )

    if not files:
        return _render_upload_error(request, "No files uploaded", 422, repo_name=name)

    # Validate file names and sizes (stream chunks to enforce limit early)
    has_queue = False
    total_size = 0
    file_contents: list[tuple[str, bytes]] = []
    _CHUNK = 64 * 1024
    for f in files:
        fname = f.filename or ""
        if not _re.match(_ALLOWED_TASK_PATTERN, fname):
            return _render_upload_error(
                request,
                f"Invalid file name: '{fname}'. Only QUEUE.md and PR-*.md allowed.",
                422,
                repo_name=name,
            )
        chunks: list[bytes] = []
        while True:
            chunk = await f.read(_CHUNK)
            if not chunk:
                break
            total_size += len(chunk)
            if total_size > _UPLOAD_MAX_TOTAL_BYTES:
                return _render_upload_error(
                    request, "Total upload size exceeds 1 MB", 422, repo_name=name
                )
            chunks.append(chunk)
        content = b"".join(chunks)
        file_contents.append((fname, content))
        if fname == "QUEUE.md":
            has_queue = True

    if not has_queue:
        return _render_upload_error(
            request, "QUEUE.md is required in the upload", 422, repo_name=name
        )

    # Stage files to /data/uploads/{repo}/ and enqueue for daemon processing.
    # Git write operations are handled by the daemon to preserve the
    # dashboard's read-only contract with the repository working trees.
    lock = _get_upload_lock(name)
    async with lock:
        submission_id = _uuid.uuid4().hex[:12]
        staging_dir = Path(UPLOADS_DIR) / name / submission_id
        await asyncio.to_thread(staging_dir.mkdir, parents=True, exist_ok=True)

        for fname, content in file_contents:
            await asyncio.to_thread((staging_dir / fname).write_bytes, content)

        new_files = [fn for fn, _ in file_contents]
        pending_key = f"upload:{name}:pending"
        try:
            existing_raw = await redis_client.get(pending_key)
        except Exception:
            existing_raw = None

        if existing_raw:
            try:
                existing = _json.loads(existing_raw)
                old_staging = Path(existing["staging_dir"])
                for old_fn in existing.get("files", []):
                    if old_fn not in new_files and (old_staging / old_fn).is_file():
                        await asyncio.to_thread(
                            shutil.copy2,
                            str(old_staging / old_fn),
                            str(staging_dir / old_fn),
                        )
                        new_files.append(old_fn)
            except Exception:
                pass

        manifest = {
            "repo": name,
            "files": new_files,
            "staging_dir": str(staging_dir),
        }
        try:
            await redis_client.set(
                pending_key,
                _json.dumps(manifest),
            )
        except Exception:
            return _render_upload_error(
                request,
                "Failed to enqueue upload (Redis error).",
                503,
                repo_name=name,
            )

    return _render_upload_success(
        request,
        "Tasks queued. The daemon will commit and push on its next cycle.",
        repo_name=name,
    )
