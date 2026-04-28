"""FastAPI dashboard application.

Read-only web UI that lists configured repositories and their pipeline state.
State is published to Redis by the daemon; if a repository has no entry in
Redis the dashboard renders a default ``IDLE`` state derived from
``config.yml``.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import logging
import os
import re
import subprocess
import tempfile
import zipfile
import zlib
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Literal

import redis.asyncio as aioredis
from fastapi import FastAPI, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from fastapi.templating import Jinja2Templates

from src.coders import build_coder_registry
from src.coders.claude import ClaudePlugin
from src.coders.codex import CodexPlugin
from src.config import (
    AppConfig,
    DaemonConfig,
    RepoConfig,
    add_repository,
    load_config,
    remove_repository,
    update_daemon_config,
    update_repository,
)
from src.daemon.github_rate_limit import read_budget
from src.events import publish_repo_event
from src.events.sse import RepoEventsUnavailableError, stream_repo_events
from src.metrics import MetricsStore, RunRecord
from src.models import PipelineState, RepoState, TaskStatus
from src.queue_parser import (
    QueueValidationError,
    parse_queue,
    parse_queue_text,
    parse_task_header,
)
from src.utils import repo_slug_from_url

DEFAULT_REDIS_URL = "redis://localhost:6379/0"
CONFIG_PATH = os.environ.get("PO_CONFIG_PATH", "config.yml")
REPOS_DIR = "/data/repos"
_TASK_PR_ID_PATTERN = re.compile(r"^PR-[A-Za-z0-9_.-]+$")
logger = logging.getLogger(__name__)

_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))
templates.env.globals["utcnow"] = lambda: datetime.now(timezone.utc)
_HISTORY_LIMIT = 100
_METRICS_PANEL_LIMIT = 20
_METRICS_SCAN_LIMIT = 100
_CODER_LABELS = {
    "any": "Any (bandit picks per-PR)",
    "claude": "Claude CLI",
    "codex": "Codex CLI",
}
_DEFERRED_CODER_SWITCH_STATES = {
    PipelineState.CODING,
    PipelineState.WATCH,
    PipelineState.FIX,
    PipelineState.MERGE,
    PipelineState.HUNG,
    PipelineState.PAUSED,
}
_ACTIVE_RUN_STATES = {
    PipelineState.CODING,
    PipelineState.WATCH,
    PipelineState.FIX,
    PipelineState.MERGE,
}


def _default_repo_state(
    name: str, url: str, *, error: str | None = None
) -> RepoState:
    """Return a default ``IDLE`` state for ``name``/``url``."""
    return RepoState(
        url=url,
        name=name,
        state=PipelineState.ERROR if error else PipelineState.IDLE,
        current_task=None,
        current_pr=None,
        error_message=error,
        last_updated=datetime.now(timezone.utc),
    )


async def _get_repo_state_safe(
    redis_client: aioredis.Redis, name: str, url: str
) -> tuple[RepoState, str | None]:
    """Return (state, warning). Warning is non-None when state is synthetic."""
    try:
        raw = await redis_client.get(f"pipeline:{name}")
    except Exception:
        st = _default_repo_state(name, url)
        st.state = PipelineState.PREFLIGHT
        st.error_message = "Redis unavailable — state unknown"
        return st, "Redis unavailable"
    if raw is None:
        st = _default_repo_state(name, url)
        st.state = PipelineState.PREFLIGHT
        st.error_message = "Waiting for daemon to initialize"
        return st, "Awaiting daemon initialization"
    try:
        return RepoState.model_validate_json(raw), None
    except Exception:
        return (
            _default_repo_state(name, url, error="State decode failed"),
            "State decode error",
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
        if repo_slug_from_url(repo.url) == name:
            url = repo.url
            found = True
            break

    if found and redis_client is not None:
        state, _warning = await _get_repo_state_safe(redis_client, name, url)
        return state

    return _default_repo_state(name, url)


def _find_repo_config_by_name(
    config: AppConfig, name: str
) -> RepoConfig | None:
    """Return the configured repo whose slug matches ``name``."""
    for repo in config.repositories:
        if repo_slug_from_url(repo.url) == name:
            return repo
    return None


class _RepoStateMutationError(Exception):
    """Sentinel for control-plane mutations that should become HTTP responses."""

    def __init__(self, message: str, status_code: int = 503) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code


async def _apply_repo_control_update(
    request: Request,
    name: str,
) -> tuple[aioredis.Redis, str, str]:
    """Return the Redis client plus the pipeline state key for ``name``."""
    cfg = load_config(CONFIG_PATH)
    repo = _find_repo_config_by_name(cfg, name)
    if repo is None:
        raise _RepoStateMutationError("Repository not found", status_code=404)
    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is None:
        raise _RepoStateMutationError("Redis unavailable", status_code=503)
    return redis_client, f"pipeline:{name}", repo.url


async def _update_repo_pause_state(
    request: Request,
    name: str,
    *,
    user_paused: bool,
    stop_action: Literal["leave", "clear", "set"] = "leave",
    event_message: str | Callable[[RepoState], str] | None = None,
) -> RepoState | Response:
    """Atomically update ``user_paused`` and any stop-signal side effect."""

    try:
        redis_client, state_key, repo_url = await _apply_repo_control_update(
            request, name
        )
    except _RepoStateMutationError as exc:
        return HTMLResponse(exc.message, status_code=exc.status_code)

    stop_key = f"control:{name}:stop"
    watch_keys: tuple[str, ...] = (state_key, stop_key) if stop_action != "leave" else (state_key,)
    failure_message = {
        "leave": "Failed to update repository state",
        "clear": "Failed to clear stop request",
        "set": "Failed to queue stop request",
    }[stop_action]

    try:
        async def _transaction(pipe: Any) -> RepoState:
            raw = await pipe.get(state_key)
            if raw is None:
                state = _default_repo_state(name, repo_url)
            else:
                try:
                    state = RepoState.model_validate_json(raw)
                except Exception as exc:
                    raise _RepoStateMutationError("Repository state unavailable") from exc

            state.user_paused = user_paused
            if event_message is not None:
                message = event_message(state) if callable(event_message) else event_message
                _append_history_entry(state, message)

            pipe.multi()
            pipe.set(state_key, state.model_dump_json())
            if stop_action == "clear":
                pipe.delete(stop_key)
            elif stop_action == "set":
                pipe.set(stop_key, "1", ex=60)
            return state

        state = await redis_client.transaction(
            _transaction,
            *watch_keys,
            value_from_callable=True,
        )
    except _RepoStateMutationError as exc:
        return HTMLResponse(exc.message, status_code=exc.status_code)
    except Exception:
        return HTMLResponse(failure_message, status_code=503)

    if event_message is not None:
        message = event_message(state) if callable(event_message) else event_message
        await _publish_history_entry_event(
            name,
            state,
            message,
            redis_client,
        )

    return state


def _append_history_entry(state: RepoState, event: str) -> None:
    """Mirror ``Runner.log_event`` for web control-plane history entries."""
    now = datetime.now(timezone.utc).isoformat()
    current_state = state.state.value
    last_entry = state.history[-1] if state.history else None
    if (
        last_entry is not None
        and last_entry.get("state") == current_state
        and last_entry.get("event") == event
    ):
        last_entry["count"] = int(last_entry.get("count", 1)) + 1
        last_entry["last_seen_at"] = now
        return

    state.history.append(
        {
            "time": now,
            "state": current_state,
            "event": event,
            "count": 1,
            "last_seen_at": now,
        }
    )
    if len(state.history) > _HISTORY_LIMIT:
        state.history = state.history[-_HISTORY_LIMIT:]


async def _publish_history_entry_event(
    name: str,
    state: RepoState,
    event_message: str,
    redis_client: aioredis.Redis,
) -> None:
    """Publish a live history update for repo-detail subscribers."""
    try:
        await publish_repo_event(
            name,
            "history_updated",
            {
                "state": state.state.value,
                "event": event_message,
            },
            redis_client,
        )
    except Exception:
        logger.warning("Failed to publish repo history update", exc_info=True)


def _resume_event_message(state: RepoState) -> str:
    """Return the user-facing event emitted by the Play control."""
    if state.state in _ACTIVE_RUN_STATES:
        return "Pause canceled. Continue current run."
    return "Resumed. Taking next task from queue."


def _effective_coder_name(
    repo_config: RepoConfig | None, config: AppConfig
) -> str:
    """Return the effective coder name for a repo."""
    if repo_config is not None and repo_config.coder is not None:
        return repo_config.coder.value
    return config.daemon.coder.value


def _daemon_default_coder_name(config: AppConfig) -> str:
    """Return the daemon-level default coder name."""
    return config.daemon.coder.value


def _repo_coder_form_value(repo_config: RepoConfig | None) -> str:
    """Return the raw repo-level coder selection for the detail form."""
    if repo_config is None or repo_config.coder is None:
        return "any"
    return repo_config.coder.value


def _coder_display_name(coder: str) -> str:
    """Return the UI label for a coder selection."""
    return _CODER_LABELS.get(coder, coder)


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


async def _build_github_api_budget_view(
    redis_client: aioredis.Redis | None,
    config: AppConfig,
) -> dict[str, Any] | None:
    """Return a render-ready budget snapshot for the dashboard.

    Returns ``None`` when no observation has been persisted yet so the
    template can hide the bar entirely. Color bucket maps to the same
    daemon thresholds the poll loop uses, so the dashboard's red/amber
    indicator matches the daemon's actual pause/slowdown trigger points.
    """
    budget = await read_budget(redis_client)
    if budget is None:
        return None
    pct = budget.remaining_percent
    pause_pct = config.daemon.github_api_pause_threshold_percent
    slowdown_pct = config.daemon.github_api_slowdown_threshold_percent
    now = datetime.now(timezone.utc)
    if now >= budget.reset_at:
        bucket = "ok"
    elif pct < pause_pct:
        bucket = "critical"
    elif pct < slowdown_pct:
        bucket = "low"
    else:
        bucket = "ok"
    return {
        "remaining": budget.remaining,
        "limit": budget.limit,
        "percent": round(pct, 1),
        "reset_at": budget.reset_at,
        "bucket": bucket,
    }


async def get_all_repo_states(
    redis_client: aioredis.Redis | None,
    config_path: str = CONFIG_PATH,
) -> tuple[list[RepoState], str | None]:
    """Return ``(states, redis_warning)`` for every repo in ``config.yml``.

    ``redis_warning`` is non-None when Redis is entirely unavailable; it is
    intended for a top-level dashboard banner. Per-repo degradation (key
    missing, decode failure) is encoded in the individual ``RepoState``
    objects via ``error_message``.
    """
    cfg = load_config(config_path)
    states: list[RepoState] = []
    redis_available = redis_client is not None
    redis_warning: str | None = None

    if redis_client is not None:
        try:
            await redis_client.ping()
        except Exception:
            redis_available = False
            redis_warning = "Redis connection lost"

    for repo in cfg.repositories:
        name = repo_slug_from_url(repo.url)
        state: RepoState | None = None

        if redis_available:
            state, warning = await _get_repo_state_safe(
                redis_client, name, repo.url
            )
            if warning == "Redis unavailable":
                redis_available = False
                redis_warning = "Redis connection lost"

        if state is None:
            state = _default_repo_state(name, repo.url)
            state.state = PipelineState.PREFLIGHT
            state.error_message = "Redis unavailable — state unknown"

        states.append(state)

    return states, redis_warning


def _build_coder_rows(
    config: AppConfig, auth: dict[str, dict[str, str]]
) -> list[dict[str, Any]]:
    """Return coder rows for the settings table and JSON API."""
    rows: list[dict[str, Any]] = []
    for plugin in build_coder_registry().list_coders():
        selected_model = (
            config.daemon.claude_model
            if plugin.name == "claude"
            else config.daemon.codex_model
        )
        model_options = [model for model in plugin.models if model != ""]
        rows.append(
            {
                "name": plugin.name,
                "display_name": plugin.display_name,
                "models": model_options,
                "selected_model": selected_model,
                "auth": auth.get(
                    plugin.name,
                    {
                        "status": "error",
                        "detail": f"{plugin.display_name} unavailable",
                    },
                ),
                "is_default": config.daemon.coder.value == plugin.name,
            }
        )
    return rows


def _validate_coder_model(
    model: str,
    *,
    field_name: str,
    plugin: ClaudePlugin | CodexPlugin,
    default_model: str | None = None,
) -> str:
    """Return a supported model value for ``plugin``."""
    if model == "":
        return default_model if default_model is not None else model
    allowed_models = {candidate for candidate in plugin.models if candidate != ""}
    if model not in allowed_models:
        raise ValueError(
            f"{field_name} must be one of: {', '.join(sorted(allowed_models))}"
        )
    return model


async def _repo_template_context(
    name: str,
    redis_client: aioredis.Redis | None,
    config_path: str = CONFIG_PATH,
    *,
    include_metrics: bool = False,
    coder_update_message: str | None = None,
) -> dict[str, Any]:
    """Return template context for repo detail renders."""
    config = load_config(config_path)
    state = await get_repo_state(name, redis_client, config_path=config_path)
    repo_config = _find_repo_config_by_name(config, name)
    effective_coder = _effective_coder_name(repo_config, config)
    active_rate_limit_coder = _active_rate_limit_coder(state, effective_coder)
    show_rate_limit_badge = _coder_rate_limit_supported(active_rate_limit_coder) and (
        state.usage_session_percent is not None
        or state.usage_weekly_percent is not None
        or state.usage_api_degraded
    )
    return {
        "repo": state,
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
    states, redis_warning = await get_all_repo_states(redis_client)
    stats = _compute_stats(states)
    alerts = _build_alerts(states)
    latest_alert = min(alerts, key=lambda a: a["duration_seconds"]) if alerts else None
    github_api_budget = await _build_github_api_budget_view(
        redis_client, load_config(CONFIG_PATH)
    )
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "title": "Dashboard",
            "repos": states,
            "stats": stats,
            "latest_alert": latest_alert,
            "redis_warning": redis_warning,
            "github_api_budget": github_api_budget,
        },
    )


@app.get("/api/states")
async def api_states(request: Request) -> JSONResponse:
    redis_client = getattr(request.app.state, "redis", None)
    states, _warning = await get_all_repo_states(redis_client)
    return JSONResponse([s.model_dump(mode="json") for s in states])


@app.get("/api/stats")
async def api_stats(request: Request) -> JSONResponse:
    redis_client = getattr(request.app.state, "redis", None)
    states, _warning = await get_all_repo_states(redis_client)
    return JSONResponse(_compute_stats(states))


@app.get("/api/repos/{name}/events")
async def api_repo_events(name: str, request: Request) -> Response:
    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is None:
        return Response("Redis unavailable", status_code=503)
    try:
        stream = await stream_repo_events(redis_client, name, request)
    except RepoEventsUnavailableError:
        return Response("Redis unavailable", status_code=503)
    return StreamingResponse(
        stream,
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/partials/redis-banner", response_class=HTMLResponse)
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
    return templates.TemplateResponse(
        request,
        "components/redis_banner.html",
        {"redis_warning": redis_warning},
    )


@app.get("/partials/repo-list", response_class=HTMLResponse)
async def partial_repo_list(request: Request) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    states, redis_warning = await get_all_repo_states(redis_client)
    github_api_budget = await _build_github_api_budget_view(
        redis_client, load_config(CONFIG_PATH)
    )
    return templates.TemplateResponse(
        request,
        "components/repo_cards.html",
        {
            "repos": states,
            "redis_warning": redis_warning,
            "github_api_budget": github_api_budget,
        },
    )


@app.get("/partials/stats", response_class=HTMLResponse)
async def partial_stats(request: Request) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    states, _warning = await get_all_repo_states(redis_client)
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
    states, _warning = await get_all_repo_states(redis_client)
    feed = _build_activity_feed(states)
    return templates.TemplateResponse(
        request,
        "components/activity_feed.html",
        {"feed": feed},
    )


@app.get("/partials/alerts", response_class=HTMLResponse)
async def partial_alerts(request: Request) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    states, _warning = await get_all_repo_states(redis_client)
    alerts = _build_alerts(states)
    return templates.TemplateResponse(
        request,
        "components/alerts_panel.html",
        {"alerts": alerts},
    )


@app.get("/repo/{name}", response_class=HTMLResponse)
async def repo_detail(request: Request, name: str) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    context = await _repo_template_context(
        name,
        redis_client,
        include_metrics=True,
    )
    return templates.TemplateResponse(
        request,
        "repo.html",
        {
            "title": name,
            **context,
            "events": list(context["repo"].history),
        },
    )


@app.post("/repos/{name}/pause")
async def pause_repo(request: Request, name: str) -> Response:
    result = await _update_repo_pause_state(
        request,
        name,
        user_paused=True,
        event_message=(
            "Pause requested. Finishing current PR cycle "
            "(may take several iterations)."
        ),
    )
    if isinstance(result, Response):
        return result
    return JSONResponse({"ok": True, "user_paused": True})


@app.post("/repos/{name}/resume")
async def resume_repo(request: Request, name: str) -> Response:
    result = await _update_repo_pause_state(
        request,
        name,
        user_paused=False,
        stop_action="clear",
        event_message=_resume_event_message,
    )
    if isinstance(result, Response):
        return result
    return JSONResponse({"ok": True, "user_paused": False})


@app.post("/repos/{name}/stop")
async def stop_repo(request: Request, name: str) -> Response:
    result = await _update_repo_pause_state(
        request,
        name,
        user_paused=True,
        stop_action="set",
        event_message="Stop requested. Aborting run; working tree may be left dirty.",
    )
    if isinstance(result, Response):
        return result
    return JSONResponse({"ok": True, "user_paused": True})


@app.post("/repos/{name}/coder", response_class=HTMLResponse)
async def post_repo_detail_coder(
    request: Request,
    name: str,
    coder: str = Form(...),
) -> HTMLResponse:
    cfg = load_config(CONFIG_PATH)
    repo = _find_repo_config_by_name(cfg, name)
    if repo is None:
        return HTMLResponse("Repository not found", status_code=404)

    if coder == "any":
        updated_coder: str | None = None
    elif coder in ("claude", "codex"):
        updated_coder = coder
    else:
        return HTMLResponse(
            "coder must be one of: any, claude, codex",
            status_code=422,
        )

    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is None:
        return HTMLResponse("Redis unavailable", status_code=503)
    if hasattr(redis_client, "ping"):
        try:
            await redis_client.ping()
        except Exception:
            return HTMLResponse("Redis unavailable", status_code=503)

    try:
        update_repository(repo.url, path=CONFIG_PATH, coder=updated_coder)
    except OSError as exc:
        return HTMLResponse(f"Failed to write config.yml: {exc}", status_code=503)

    dirty_key = f"control:{name}:config_dirty"
    state_key = f"pipeline:{name}"
    try:
        await redis_client.set(dirty_key, "1")
        raw_state = await redis_client.get(state_key)
        if raw_state:
            state = RepoState.model_validate_json(raw_state)
            if state.state not in _DEFERRED_CODER_SWITCH_STATES:
                refreshed = load_config(CONFIG_PATH)
                refreshed_repo = _find_repo_config_by_name(refreshed, name)
                if refreshed_repo is not None:
                    effective_coder = _effective_coder_name(
                        refreshed_repo, refreshed
                    )

                    async def _transaction(pipe: Any) -> None:
                        latest_raw = await pipe.get(state_key)
                        if latest_raw is None:
                            return
                        latest_state = RepoState.model_validate_json(latest_raw)
                        if latest_state.state in _DEFERRED_CODER_SWITCH_STATES:
                            return
                        latest_state.coder = effective_coder
                        pipe.multi()
                        pipe.set(state_key, latest_state.model_dump_json())

                    await redis_client.transaction(_transaction, state_key)
    except Exception:
        logger.warning("Failed to refresh repo state after coder update", exc_info=True)

    try:
        await publish_repo_event(
            name,
            "config_reloaded",
            {
                "coder": coder,
                "effective_coder": updated_coder or cfg.daemon.coder.value,
            },
            redis_client,
        )
    except Exception:
        logger.warning("Failed to publish coder update event", exc_info=True)

    current_state = await get_repo_state(name, redis_client, config_path=CONFIG_PATH)
    applies_after_current_pr = (
        current_state.state in _DEFERRED_CODER_SWITCH_STATES
    )
    message = f"Switching to {_coder_display_name(coder)}"
    if applies_after_current_pr:
        message += " - applies after current PR completes."
    else:
        message += "."

    context = await _repo_template_context(
        name,
        redis_client,
        coder_update_message=message,
    )
    return templates.TemplateResponse(
        request,
        "components/repo_summary.html",
        context,
    )


@app.get("/repo/{name}/metrics")
async def repo_metrics(request: Request, name: str) -> JSONResponse:
    redis_client = getattr(request.app.state, "redis", None)
    return JSONResponse(await _recent_repo_metrics_payload(name, redis_client))


@app.get("/partials/repo/{name}", response_class=HTMLResponse)
async def partial_repo_detail(request: Request, name: str) -> HTMLResponse:
    """Return ONLY the repo summary cards for the 5s HTMX poll.

    Deliberately does not include the event log: the log self-polls via
    ``/partials/repo/{name}/events`` so its scroll position survives
    across summary refreshes (innerHTML swaps on the polling container
    would otherwise wipe the log and reset its scroll on every tick).
    """
    redis_client = getattr(request.app.state, "redis", None)
    context = await _repo_template_context(name, redis_client)
    return templates.TemplateResponse(
        request,
        "components/repo_summary.html",
        context,
    )


@app.get(
    "/partials/repo/{name}/metrics",
    response_class=HTMLResponse,
)
async def partial_repo_metrics(request: Request, name: str) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    return templates.TemplateResponse(
        request,
        "components/pr_metrics.html",
        {
            "repo_name": name,
            "metrics_records": await _recent_repo_metrics_payload(
                name, redis_client
            ),
        },
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
    if not any(repo_slug_from_url(r.url) == name for r in cfg.repositories):
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


@app.get("/repos/{name}/tasks", response_class=HTMLResponse)
async def list_repo_tasks(request: Request, name: str) -> Response:
    """Return the repo's tasks grouped by status as an HTML fragment."""
    cfg = load_config(CONFIG_PATH)
    if _find_repo_config_by_name(cfg, name) is None:
        return HTMLResponse(
            '<p class="text-sm italic text-fail">Repository not found.</p>',
            status_code=404,
        )
    queue_path = Path(REPOS_DIR) / name / "tasks" / "QUEUE.md"
    try:
        tasks = parse_queue(str(queue_path))
    except (OSError, UnicodeDecodeError):
        # A non-UTF-8 or otherwise unreadable QUEUE.md (bad manual edit,
        # interrupted merge, lost permissions) must not 500 the entire
        # Tasks panel — return a controlled fragment instead. Use 503 so
        # the global htmx:beforeSwap hook in base.html swaps the fragment
        # in (it only enables swap for 404/422/503).
        return HTMLResponse(
            '<p class="text-sm italic text-fail">Unable to read'
            " tasks/QUEUE.md.</p>",
            status_code=503,
        )
    grouped = {
        "doing": [t for t in tasks if t.status == TaskStatus.DOING],
        "todo": [t for t in tasks if t.status == TaskStatus.TODO],
        "done": [t for t in tasks if t.status == TaskStatus.DONE],
    }
    return templates.TemplateResponse(
        request,
        "components/tasks_panel.html",
        {
            "repo_name": name,
            "tasks_by_status": grouped,
            "tasks_total": len(tasks),
        },
    )


def _resolve_repo_task_path(name: str, pr_id: str) -> tuple[Path, str] | None:
    """Return the on-disk task file for ``pr_id`` honoring queue mappings.

    Honors the queue's ``- Tasks file:`` value when present (the runner
    accepts task files whose name differs from ``{pr_id}.md``), otherwise
    falls back to ``tasks/{pr_id}.md``. Rejects any path that escapes the
    repo's ``tasks/`` directory or traverses a symlink so the dashboard
    cannot be coaxed into reading host files via a planted symlink.

    Returns a ``(absolute_path, display_name)`` tuple where ``display_name``
    is the repo-relative posix path of the resolved file, suitable for
    showing in the viewer header so reviewers see the actual file name
    rather than a hardcoded ``{pr_id}.md`` label.
    """
    repo_root = Path(REPOS_DIR) / name
    tasks_dir = repo_root / "tasks"
    if not tasks_dir.is_dir():
        return None
    tasks_dir_resolved = tasks_dir.resolve()

    relative_str: str | None = None
    try:
        queued_tasks = parse_queue(str(tasks_dir / "QUEUE.md"))
    except (OSError, UnicodeDecodeError):
        # A broken QUEUE.md must not block direct lookups by `{pr_id}.md`
        # — fall through to the default filename so a single malformed
        # queue file does not also kill task-file viewing.
        queued_tasks = []
    for task in queued_tasks:
        if task.pr_id == pr_id and task.task_file:
            relative_str = task.task_file
            break
    if relative_str is None:
        relative_str = f"tasks/{pr_id}.md"

    candidate = repo_root / relative_str
    try:
        relative_parts = candidate.relative_to(repo_root).parts
    except ValueError:
        return None
    walk = repo_root
    for part in relative_parts:
        walk = walk / part
        if walk.is_symlink():
            return None
    if not candidate.is_file():
        return None
    resolved = candidate.resolve()
    try:
        within_tasks = resolved.relative_to(tasks_dir_resolved)
    except ValueError:
        return None
    display_name = (Path("tasks") / within_tasks).as_posix()
    return resolved, display_name


@app.get("/repos/{name}/tasks/{pr_id}", response_class=HTMLResponse)
async def view_repo_task(
    request: Request, name: str, pr_id: str
) -> Response:
    """Return one task file's contents as a preformatted HTML fragment."""
    if not _TASK_PR_ID_PATTERN.match(pr_id):
        return HTMLResponse(
            '<p class="text-sm italic text-fail">Invalid task identifier.</p>',
            status_code=400,
        )
    cfg = load_config(CONFIG_PATH)
    if _find_repo_config_by_name(cfg, name) is None:
        return HTMLResponse(
            '<p class="text-sm italic text-fail">Repository not found.</p>',
            status_code=404,
        )
    resolved = _resolve_repo_task_path(name, pr_id)
    if resolved is None:
        return HTMLResponse(
            '<p class="text-sm italic text-gray-500">Task file not found.</p>',
            status_code=404,
        )
    task_path, task_filename = resolved
    try:
        content = task_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        # Permissions / non-UTF-8 / file vanished between resolution and
        # read: surface a user-facing error fragment instead of letting
        # HTMX swap in a 500 stack trace. Use 503 so the global
        # htmx:beforeSwap hook in base.html swaps the fragment in (it
        # only enables swap for 404/422/503).
        return HTMLResponse(
            '<p class="text-sm italic text-fail">Unable to read task'
            " file.</p>",
            status_code=503,
        )
    return templates.TemplateResponse(
        request,
        "components/task_content.html",
        {
            "repo_name": name,
            "pr_id": pr_id,
            "task_filename": task_filename,
            "content": content,
        },
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
    auth = await _collect_auth_status()
    coder_rows = _build_coder_rows(cfg, auth)
    return templates.TemplateResponse(
        request,
        "settings.html",
        {
            "title": "Settings",
            "repos": cfg.repositories,
            "daemon": cfg.daemon,
            "coders": coder_rows,
            "auth": auth,
        },
    )


def _default_auth_status() -> dict[str, dict[str, str]]:
    """Return placeholder auth status entries when no cached probe exists."""
    unavailable = {"status": "error", "detail": "Status unavailable"}
    return {
        "claude": dict(unavailable),
        "codex": dict(unavailable),
        "gh": dict(unavailable),
    }


_AUTH_STATUS_CACHE: dict[str, dict[str, str]] | None = None


def _get_cached_auth_status() -> dict[str, dict[str, str]]:
    """Return the last collected auth status, if available."""
    source = _AUTH_STATUS_CACHE or _default_auth_status()
    return {key: dict(value) for key, value in source.items()}


async def _settings_daemon_template_context(
    request: Request,
    *,
    use_cached_auth: bool = False,
) -> dict[str, Any]:
    cfg = load_config(CONFIG_PATH)
    auth = (
        _get_cached_auth_status()
        if use_cached_auth
        else await _collect_auth_status()
    )
    return {
        "daemon": cfg.daemon,
        "coders": _build_coder_rows(cfg, auth),
        "auth": auth,
    }


async def _render_settings_daemon_response(request: Request) -> HTMLResponse:
    """Render the daemon settings form for a successful mutation response.

    Successful PUTs re-render both the daemon form and the coder controls.
    The coder block is sent as an out-of-band swap so HTMX resets any
    radio/select state that the browser changed optimistically before the
    server accepted the update.
    """
    return templates.TemplateResponse(
        request,
        "components/settings_daemon_response.html",
        await _settings_daemon_template_context(request, use_cached_auth=True),
    )


async def _render_settings_daemon_error(
    request: Request, message: str, status_code: int
) -> HTMLResponse:
    context = await _settings_daemon_template_context(
        request, use_cached_auth=True
    )
    return templates.TemplateResponse(
        request,
        "components/settings_daemon_error.html",
        {**context, "message": message},
        status_code=status_code,
    )


@app.get("/partials/settings/daemon", response_class=HTMLResponse)
async def partial_settings_daemon(request: Request) -> HTMLResponse:
    context = await _settings_daemon_template_context(request)
    return templates.TemplateResponse(
        request,
        "components/settings_daemon_response.html",
        context,
    )


@app.get("/partials/settings/coders", response_class=HTMLResponse)
async def partial_settings_coders(request: Request) -> HTMLResponse:
    context = await _settings_daemon_template_context(request)
    return templates.TemplateResponse(
        request,
        "components/settings_coders_wrapper.html",
        context,
    )


@app.put("/settings/daemon", response_class=HTMLResponse)
async def put_settings_daemon(
    request: Request,
    poll_interval_sec: str | None = Form(None),
    review_timeout_min: str | None = Form(None),
    auto_fallback: str | None = Form(None),
    hung_fallback_codex_review: str | None = Form(None),
    error_handler_use_ai: str | None = Form(None),
    planned_pr_timeout_sec: str | None = Form(None),
    fix_idle_timeout_sec: str | None = Form(None),
    exploration_epsilon: str | None = Form(None),
    rate_limit_session_pause_percent: str | None = Form(None),
    rate_limit_weekly_pause_percent: str | None = Form(None),
    coder: str | None = Form(None),
    claude_model: str | None = Form(None),
    codex_model: str | None = Form(None),
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
            auto_fallback is not None
            and auto_fallback != ""
        ):
            updates["auto_fallback"] = _coerce_bool(
                auto_fallback, "auto_fallback"
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
            fix_idle_timeout_sec is not None
            and fix_idle_timeout_sec != ""
        ):
            updates["fix_idle_timeout_sec"] = _coerce_int(
                fix_idle_timeout_sec, "fix_idle_timeout_sec", min_value=1
            )
        if (
            exploration_epsilon is not None
            and exploration_epsilon != ""
        ):
            updates["exploration_epsilon"] = _coerce_float(
                exploration_epsilon,
                "exploration_epsilon",
                min_value=0.0,
                max_value=0.5,
            )
        if (
            rate_limit_session_pause_percent is not None
            and rate_limit_session_pause_percent != ""
        ):
            updates["rate_limit_session_pause_percent"] = _coerce_int(
                rate_limit_session_pause_percent,
                "rate_limit_session_pause_percent",
                min_value=50,
                max_value=100,
            )
        if (
            rate_limit_weekly_pause_percent is not None
            and rate_limit_weekly_pause_percent != ""
        ):
            updates["rate_limit_weekly_pause_percent"] = _coerce_int(
                rate_limit_weekly_pause_percent,
                "rate_limit_weekly_pause_percent",
                min_value=50,
                max_value=100,
            )
        if coder is not None and coder != "":
            if coder not in ("claude", "codex"):
                raise ValueError("coder must be 'claude' or 'codex'")
            updates["coder"] = coder
        if claude_model is not None:
            updates["claude_model"] = _validate_coder_model(
                claude_model,
                field_name="claude_model",
                plugin=ClaudePlugin(),
                default_model=DaemonConfig().claude_model,
            )
        if codex_model is not None:
            updates["codex_model"] = _validate_coder_model(
                codex_model,
                field_name="codex_model",
                plugin=CodexPlugin(),
            )
    except ValueError as exc:
        return await _render_settings_daemon_error(request, str(exc), 422)

    try:
        update_daemon_config(path=CONFIG_PATH, **updates)
    except ValueError as exc:
        return await _render_settings_daemon_error(request, str(exc), 422)
    except OSError as exc:
        return await _render_settings_daemon_error(
            request, f"Failed to write config.yml: {exc}", 503
        )
    return await _render_settings_daemon_response(request)


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


def _first_probe_line(text: str) -> str:
    """Return the first meaningful line from CLI probe output."""
    for line in text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.lower().startswith("warning:"):
            return stripped
    return ""


def _check_claude_auth() -> dict[str, str]:
    """Probe the ``claude`` CLI and report its authorization status."""
    return ClaudePlugin().check_auth(config_path=CONFIG_PATH)


def _check_codex_auth() -> dict[str, str]:
    """Probe the ``codex`` CLI and report its authorization status."""
    return CodexPlugin().check_auth(config_path=CONFIG_PATH)


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
    claude, codex, gh = await asyncio.gather(
        asyncio.to_thread(_check_claude_auth),
        asyncio.to_thread(_check_codex_auth),
        asyncio.to_thread(_check_gh_auth),
    )
    global _AUTH_STATUS_CACHE
    _AUTH_STATUS_CACHE = {"claude": claude, "codex": codex, "gh": gh}
    return _get_cached_auth_status()


@app.get("/api/auth-status")
async def api_auth_status() -> JSONResponse:
    return JSONResponse(await _collect_auth_status())


@app.get("/api/coders")
async def api_coders() -> JSONResponse:
    cfg = load_config(CONFIG_PATH)
    auth = await _collect_auth_status()
    return JSONResponse({"coders": _build_coder_rows(cfg, auth)})


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


def _coerce_int(
    value: str,
    field: str,
    min_value: int | None = None,
    max_value: int | None = None,
) -> int:
    try:
        parsed = int(value.strip())
    except ValueError as exc:
        raise ValueError(f"{field} must be an integer") from exc
    if min_value is not None and parsed < min_value:
        raise ValueError(f"{field} must be at least {min_value}")
    if max_value is not None and parsed > max_value:
        raise ValueError(f"{field} must be at most {max_value}")
    return parsed


def _coerce_float(
    value: str,
    field: str,
    min_value: float | None = None,
    max_value: float | None = None,
) -> float:
    try:
        parsed = float(value.strip())
    except ValueError as exc:
        raise ValueError(f"{field} must be a number") from exc
    if min_value is not None and parsed < min_value:
        raise ValueError(f"{field} must be at least {min_value}")
    if max_value is not None and parsed > max_value:
        raise ValueError(f"{field} must be at most {max_value}")
    return parsed


@app.put("/settings/repos", response_class=HTMLResponse)
async def put_settings_repo(
    request: Request,
    url: str,
    branch: str | None = Form(None),
    auto_merge: str | None = Form(None),
    review_timeout_min: str | None = Form(None),
    allow_merge_without_checks: str | None = Form(None),
    coder: str | None = Form(None),
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
        if allow_merge_without_checks is not None and allow_merge_without_checks != "":
            updates["allow_merge_without_checks"] = _coerce_bool(
                allow_merge_without_checks, "allow_merge_without_checks"
            )
        if review_timeout_min is not None:
            if review_timeout_min == "":
                updates["review_timeout_min"] = None
            else:
                updates["review_timeout_min"] = _coerce_int(
                    review_timeout_min, "review_timeout_min", min_value=1
                )
        if coder is not None:
            if coder == "":
                updates["coder"] = None
            elif coder in ("claude", "codex"):
                updates["coder"] = coder
            else:
                raise ValueError("coder must be 'claude', 'codex', or empty")
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


@app.put("/settings/repo/{name}", response_class=HTMLResponse)
async def put_repo_detail_coder(
    request: Request,
    name: str,
    coder: str | None = Form(None),
) -> HTMLResponse:
    cfg = load_config(CONFIG_PATH)
    repo = _find_repo_config_by_name(cfg, name)
    if repo is None:
        return HTMLResponse("Repository not found", status_code=404)

    updates: dict[str, object | None] = {}
    if coder is not None:
        if coder == "":
            updates["coder"] = None
        elif coder in ("claude", "codex"):
            updates["coder"] = coder
        else:
            return HTMLResponse(
                "coder must be 'claude', 'codex', or empty",
                status_code=422,
            )

    try:
        update_repository(repo.url, path=CONFIG_PATH, **updates)
    except OSError as exc:
        return HTMLResponse(f"Failed to write config.yml: {exc}", status_code=503)

    redis_client = getattr(request.app.state, "redis", None)
    context = await _repo_template_context(name, redis_client)
    return templates.TemplateResponse(
        request,
        "components/repo_summary.html",
        context,
    )


# ---------------------------------------------------------------------------
# Upload tasks
# ---------------------------------------------------------------------------

_UPLOAD_MAX_TOTAL_BYTES = 1_000_000  # 1 MB
_TASK_UPLOAD_PATTERN = r"^PR-[A-Za-z0-9._-]+\.md$"
_ALLOWED_TASK_PATTERN = (
    rf"^(QUEUE\.md|AGENTS\.md|CLAUDE\.md|{_TASK_UPLOAD_PATTERN[1:-1]})$"
)
UPLOADS_DIR = "/data/uploads"

import json as _json  # noqa: E402 — kept near usage
import re as _re  # noqa: E402 — kept near usage
import shutil  # noqa: E402 — kept near usage
import time as _time  # noqa: E402 — kept near usage
import uuid as _uuid  # noqa: E402 — kept near usage

_STAGING_MAX_AGE_HOURS = 24

_upload_locks: dict[str, asyncio.Lock] = {}


def sweep_abandoned_staging(
    uploads_root: str,
    active_staging_dirs: set[str],
    max_age_hours: int = _STAGING_MAX_AGE_HOURS,
) -> int:
    """Remove staging directories older than *max_age_hours* with no active key.

    *active_staging_dirs* is the set of staging directory paths that are
    currently referenced by a Redis upload manifest.  These are preserved
    regardless of age.

    Returns the count of directories removed.
    """
    root = Path(uploads_root)
    if not root.is_dir():
        return 0
    now = _time.time()
    cutoff = now - max_age_hours * 3600
    removed = 0
    for repo_dir in root.iterdir():
        if not repo_dir.is_dir():
            continue
        for entry in repo_dir.iterdir():
            if not entry.is_dir():
                continue
            if str(entry) in active_staging_dirs:
                continue
            try:
                mtime = entry.stat().st_mtime
            except OSError:
                continue
            if mtime < cutoff:
                shutil.rmtree(entry, ignore_errors=True)
                removed += 1
    return removed


def _get_upload_lock(repo_name: str) -> asyncio.Lock:
    if repo_name not in _upload_locks:
        _upload_locks[repo_name] = asyncio.Lock()
    return _upload_locks[repo_name]


def _escape_css_identifier(value: str) -> str:
    return _re.sub(r"([.#\[\]:>+~(){}|^$*!])", r"\\\1", value)


def _upload_feedback_target(repo_name: str) -> str:
    css_name = _escape_css_identifier(repo_name)
    return f"#upload-feedback-{css_name}"


templates.env.globals["css_escape"] = _escape_css_identifier
templates.env.globals["upload_feedback_target"] = _upload_feedback_target


def _format_upload_message_lines(message: str) -> list[str]:
    return [line for line in message.splitlines() if line.strip()]


def _unique_filenames(filenames: list[str]) -> list[str]:
    return list(dict.fromkeys(filenames))


def _task_upload_summary(task_filenames: list[str]) -> str:
    if not task_filenames:
        return ""

    def _sort_key(filename: str) -> tuple[int, int | str]:
        match = _re.fullmatch(r"PR-(\d+)\.md", filename)
        if match:
            return (0, int(match.group(1)))
        return (1, filename)

    ordered = sorted(task_filenames, key=_sort_key)
    labels = [filename.removesuffix(".md") for filename in ordered]
    if len(labels) == 1:
        return labels[0]
    pr_numbers: list[int] = []
    for filename in ordered:
        match = _re.fullmatch(r"PR-(\d+)\.md", filename)
        if not match:
            return ", ".join(labels)
        pr_numbers.append(int(match.group(1)))

    if all(
        current == previous + 1
        for previous, current in zip(pr_numbers, pr_numbers[1:], strict=False)
    ):
        return f"{labels[0]} through {labels[-1]}"
    return ", ".join(labels)


def _build_upload_success_message(
    filenames: list[str], repo_state: PipelineState
) -> str:
    task_filenames = _unique_filenames(
        [
            filename for filename in filenames if _re.fullmatch(_TASK_UPLOAD_PATTERN, filename)
        ]
    )
    helper_filenames = _unique_filenames(
        [
            filename for filename in filenames if not _re.fullmatch(_TASK_UPLOAD_PATTERN, filename)
        ]
    )

    task_count = len(task_filenames)
    noun = "file" if task_count == 1 else "files"
    summary = _task_upload_summary(task_filenames)
    if summary:
        lines = [f"Accepted {task_count} task {noun} ({summary})."]
    else:
        lines = [f"Accepted {task_count} task {noun}."]

    if helper_filenames:
        helper_noun = "file" if len(helper_filenames) == 1 else "files"
        lines.append(
            f"Also uploaded helper {helper_noun}: {', '.join(sorted(helper_filenames))}."
        )
    if repo_state == PipelineState.IDLE:
        lines.append(
            "Daemon will commit on the next poll cycle (up to 60 seconds)."
        )
    else:
        lines.append(
            "Daemon is currently "
            f"{repo_state.value}. Files will be committed when it returns to IDLE."
        )
    lines.append("Auto-dismissing in 30 seconds.")
    return "\n".join(lines)


def _render_upload_error(
    request: Request, message: str, status_code: int, repo_name: str = ""
) -> HTMLResponse:
    response = templates.TemplateResponse(
        request,
        "components/upload_error.html",
        {"message": message, "message_lines": _format_upload_message_lines(message)},
        status_code=status_code,
    )
    if repo_name:
        response.headers["HX-Retarget"] = _upload_feedback_target(repo_name)
        response.headers["HX-Reswap"] = "innerHTML"
    return response


def _render_upload_success(
    request: Request, message: str, repo_name: str
) -> HTMLResponse:
    response = templates.TemplateResponse(
        request,
        "components/upload_success.html",
        {"message": message, "message_lines": _format_upload_message_lines(message)},
    )
    response.headers["HX-Retarget"] = _upload_feedback_target(repo_name)
    response.headers["HX-Reswap"] = "innerHTML"
    return response


@app.post("/repos/{name}/upload-tasks", response_class=HTMLResponse)
async def upload_tasks(
    request: Request, name: str, files: list[UploadFile] = []
) -> HTMLResponse:
    cfg = load_config(CONFIG_PATH)
    found = False
    for repo in cfg.repositories:
        if repo_slug_from_url(repo.url) == name:
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
    if not files:
        return _render_upload_error(request, "No files uploaded", 422, repo_name=name)

    # Validate file names and sizes (stream chunks to enforce limit early)
    total_size = 0
    staged_size = 0
    file_contents: list[tuple[str, bytes]] = []
    _CHUNK = 64 * 1024
    for f in files:
        fname = f.filename or ""
        content_type = (f.content_type or "").lower()
        if fname.lower().endswith(".zip") or content_type in {
            "application/zip",
            "application/x-zip-compressed",
        }:
            zip_chunks: list[bytes] = []
            zip_size = 0
            while True:
                chunk = await f.read(_CHUNK)
                if not chunk:
                    break
                zip_size += len(chunk)
                total_size += len(chunk)
                if zip_size > _UPLOAD_MAX_TOTAL_BYTES or total_size > _UPLOAD_MAX_TOTAL_BYTES:
                    return _render_upload_error(
                        request, "Total upload size exceeds 1 MB", 422, repo_name=name
                    )
                zip_chunks.append(chunk)
            try:
                with zipfile.ZipFile(io.BytesIO(b"".join(zip_chunks))) as archive:
                    extracted_file_count = 0
                    for entry in archive.infolist():
                        entry_name = entry.filename
                        if entry.is_dir():
                            continue
                        if "/" in entry_name or "\\" in entry_name:
                            return _render_upload_error(
                                request,
                                f"Zip entry '{entry_name}' must not contain path separators.",
                                422,
                                repo_name=name,
                            )
                        if not _re.match(_ALLOWED_TASK_PATTERN, entry_name):
                            return _render_upload_error(
                                request,
                                f"Invalid file name: '{entry_name}'. Only QUEUE.md, AGENTS.md, "
                                "CLAUDE.md, and PR-*.md allowed.",
                                422,
                                repo_name=name,
                            )
                        if staged_size + entry.file_size > _UPLOAD_MAX_TOTAL_BYTES:
                            return _render_upload_error(
                                request, "Total upload size exceeds 1 MB", 422, repo_name=name
                            )
                        try:
                            chunks: list[bytes] = []
                            entry_size = 0
                            with archive.open(entry) as zipped_file:
                                while True:
                                    chunk = zipped_file.read(_CHUNK)
                                    if not chunk:
                                        break
                                    entry_size += len(chunk)
                                    if staged_size + entry_size > _UPLOAD_MAX_TOTAL_BYTES:
                                        return _render_upload_error(
                                            request, "Total upload size exceeds 1 MB", 422, repo_name=name
                                        )
                                    chunks.append(chunk)
                        except (
                            EOFError,
                            NotImplementedError,
                            OSError,
                            RuntimeError,
                            zlib.error,
                        ):
                            return _render_upload_error(
                                request,
                                f"Uploaded zip '{fname}' contains corrupt, encrypted, "
                                "unsupported, or unreadable entries.",
                                400,
                                repo_name=name,
                            )
                        staged_size += entry_size
                        file_contents.append((entry_name, b''.join(chunks)))
                        extracted_file_count += 1
                    if extracted_file_count == 0:
                        return _render_upload_error(
                            request,
                            f"Uploaded zip '{fname}' does not contain any task files.",
                            422,
                            repo_name=name,
                        )
            except (UnicodeDecodeError, zipfile.BadZipFile):
                return _render_upload_error(
                    request, f"Uploaded zip '{fname}' is corrupt or unreadable.", 400, repo_name=name
                )
            except zipfile.LargeZipFile:
                return _render_upload_error(
                    request, f"Uploaded zip '{fname}' is too large to extract.", 400, repo_name=name
                )
            continue
        if not _re.match(_ALLOWED_TASK_PATTERN, fname):
            return _render_upload_error(
                request,
                f"Invalid file name: '{fname}'. Only QUEUE.md, AGENTS.md, "
                "CLAUDE.md, and PR-*.md allowed.",
                422,
                repo_name=name,
            )
        chunks: list[bytes] = []
        while True:
            chunk = await f.read(_CHUNK)
            if not chunk:
                break
            total_size += len(chunk)
            staged_size += len(chunk)
            if total_size > _UPLOAD_MAX_TOTAL_BYTES or staged_size > _UPLOAD_MAX_TOTAL_BYTES:
                return _render_upload_error(
                    request, "Total upload size exceeds 1 MB", 422, repo_name=name
                )
            chunks.append(chunk)
        content = b"".join(chunks)
        file_contents.append((fname, content))

    # Validate the *last* uploaded QUEUE.md with strict mode before staging.
    # Staging writes every file in order, so if multiple QUEUE.md parts are
    # present the last one wins on disk.  We must validate that final copy.
    queue_bytes: bytes | None = None
    for fname, content in file_contents:
        if fname == "QUEUE.md":
            queue_bytes = content
    if queue_bytes is not None:
        try:
            queue_text = queue_bytes.decode("utf-8")
        except UnicodeDecodeError:
            return _render_upload_error(
                request,
                "QUEUE.md is not valid UTF-8",
                400,
                repo_name=name,
            )
        try:
            parse_queue_text(queue_text, strict=True)
        except QueueValidationError as exc:
            issues_text = "\n".join(exc.issues)
            return _render_upload_error(
                request,
                f"QUEUE.md validation failed:\n{issues_text}",
                400,
                repo_name=name,
            )

    task_uploads: dict[str, bytes] = {}
    for fname, content in file_contents:
        if _re.fullmatch(_TASK_UPLOAD_PATTERN, fname):
            task_uploads[fname] = content

    for fname, content in task_uploads.items():
        try:
            task_text = content.decode("utf-8")
        except UnicodeDecodeError:
            return _render_upload_error(
                request,
                f"{fname} is not valid UTF-8",
                400,
                repo_name=name,
            )
        with tempfile.TemporaryDirectory() as tmpdir:
            task_path = Path(tmpdir) / fname
            task_path.write_text(task_text, encoding="utf-8")
            try:
                parse_task_header(task_path)
            except QueueValidationError as exc:
                issues = [
                    issue.replace(str(task_path), fname) for issue in exc.issues
                ]
                if any("missing Depends on" in issue for issue in issues):
                    return _render_upload_error(
                        request,
                        f"Task file validation failed: {fname}: missing Depends on field.\n"
                        "Use 'Depends on: none' for tasks with no dependencies.",
                        400,
                        repo_name=name,
                    )
                return _render_upload_error(
                    request,
                    "Task file validation failed:\n" + "\n".join(issues),
                    400,
                    repo_name=name,
                )

    # Stage files to /data/uploads/{repo}/ and enqueue for daemon processing.
    # Git write operations are handled by the daemon to preserve the
    # dashboard's read-only contract with the repository working trees.
    lock = _get_upload_lock(name)
    async with lock:
        # Best-effort sweep of abandoned staging directories.
        # Collect active staging dirs for ALL repos so the sweep does not
        # accidentally remove another repo's still-pending directory.
        try:
            active_dirs: set[str] = set()
            pending_keys: list[bytes] = []
            async for pkey in redis_client.scan_iter(match="upload:*:pending"):
                pending_keys.append(pkey)
            for pkey in pending_keys:
                try:
                    raw_sweep = await redis_client.get(pkey)
                    if raw_sweep:
                        active_dirs.add(_json.loads(raw_sweep)["staging_dir"])
                except Exception:
                    pass
            max_age = cfg.daemon.upload_staging_max_age_hours
            await asyncio.to_thread(
                sweep_abandoned_staging, UPLOADS_DIR, active_dirs, max_age
            )
        except Exception:
            pass

        submission_id = _uuid.uuid4().hex[:12]
        staging_dir = Path(UPLOADS_DIR) / name / submission_id
        await asyncio.to_thread(staging_dir.mkdir, parents=True, exist_ok=True)

        committed = False
        try:
            for fname, content in file_contents:
                await asyncio.to_thread((staging_dir / fname).write_bytes, content)

            uploaded_filenames = [fn for fn, _ in file_contents]
            manifest_filenames = list(uploaded_filenames)
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
                        if old_fn not in manifest_filenames and (old_staging / old_fn).is_file():
                            await asyncio.to_thread(
                                shutil.copy2,
                                str(old_staging / old_fn),
                                str(staging_dir / old_fn),
                            )
                            manifest_filenames.append(old_fn)
                except Exception:
                    pass

            manifest = {
                "repo": name,
                "files": manifest_filenames,
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
            committed = True
        finally:
            if not committed:
                await asyncio.to_thread(
                    shutil.rmtree, str(staging_dir), True
                )

    return _render_upload_success(
        request,
        _build_upload_success_message(uploaded_filenames, repo_state.state),
        repo_name=name,
    )
