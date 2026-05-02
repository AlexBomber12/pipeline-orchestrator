"""FastAPI dashboard application.

Read-only web UI that lists configured repositories and their pipeline state.
State is published to Redis by the daemon; if a repository has no entry in
Redis the dashboard renders a default ``IDLE`` state derived from
``config.yml``.

Routing is split across submodules: dashboard rendering and HTMX partials
live in ``src.web.routes.dashboard``; repo control mutations
(pause/resume/stop/coder/tasks) live in ``src.web.routes.repo_control``;
settings, uploads, and onboarding remain inline here pending PR-225b.
"""

from __future__ import annotations

import asyncio
import io
import logging
import os
import re
import subprocess
import tempfile
import zipfile
import zlib
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator

import redis.asyncio as aioredis
from fastapi import FastAPI, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from src.coder_registry import CoderPlugin
from src.coders import build_coder_registry
from src.config import (
    AppConfig,
    DaemonConfig,
    add_repository,
    load_config,
    remove_repository,
    update_daemon_config,
    update_repository,
)
from src.events import (
    publish_repo_event,  # noqa: F401 — accessed by routes via _app.publish_repo_event
    publish_wake,
)
from src.events.sse import (
    RepoEventsUnavailableError,  # noqa: F401 — accessed by routes via _app.RepoEventsUnavailableError
    stream_repo_events,  # noqa: F401 — accessed by routes via _app.stream_repo_events
)
from src.keyspace import (
    pipeline_state,
    upload_pending,
)
from src.models import PipelineState, RepoState
from src.onboarding.markdown_sections import MarkerError
from src.onboarding.reconciliation import reconcile_agents_md
from src.queue_parser import (
    QueueValidationError,
    parse_queue,  # noqa: F401 — accessed by routes via _app.parse_queue
    parse_queue_text,
    parse_task_header,
)
from src.utils import repo_slug_from_url
from src.web.services.config_updates import apply_config_mutation

DEFAULT_REDIS_URL = "redis://localhost:6379/0"
CONFIG_PATH = os.environ.get("PO_CONFIG_PATH", "config.yml")
REPOS_DIR = "/data/repos"
_REPO_SLUG_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9_.-]*__[A-Za-z0-9][A-Za-z0-9_.-]*$"
)
logger = logging.getLogger(__name__)

_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))
templates.env.globals["utcnow"] = lambda: datetime.now(timezone.utc)


def _format_reset_unix(reset_unix: int | None) -> str:
    """Render ``reset_unix`` as ``HH:MM UTC`` for resource-chip tooltips.

    Returns ``"unknown"`` for ``None`` so the template still has a string
    to interpolate; the chip template only renders the "Resets at" line
    when ``reset_unix`` is truthy, so this branch is defensive.

    Defined here rather than in ``src.web.routes.dashboard`` because the
    Jinja filter has to be registered at module load time and the dashboard
    module is partial at that point whenever the route module is the entry
    point of the import (it imports this module via ``from src.web import
    app as _app``).
    """
    if not reset_unix:
        return "unknown"
    return datetime.fromtimestamp(int(reset_unix), tz=timezone.utc).strftime(
        "%H:%M UTC"
    )


templates.env.filters["format_reset"] = _format_reset_unix

# Routers and shared helpers are imported AFTER the module-level constants
# (CONFIG_PATH, REPOS_DIR, templates, etc.) are bound, because the route
# modules reference this module via ``from src.web import app as _app`` and
# expect those attributes to exist when route handlers fire at request time.
#
# Only module-level imports (``import ... as ...``) are used for the route
# modules — any ``from src.web.routes.<name> import <symbol>`` would access
# attributes on a partially initialized module whenever the route module is
# the entry point of the import (see PEP 562 ``__getattr__`` below for the
# backward-compatible re-exports tests rely on).
from src.web.routes import dashboard as _dashboard_routes  # noqa: E402
from src.web.routes import repo_control as _repo_control_routes  # noqa: E402
from src.web.services.repo_state import (  # noqa: E402,F401
    _default_repo_state,
    _find_repo_config_by_name,
    _get_repo_state_safe,
    get_all_repo_states,
    get_repo_state,
)

_DASHBOARD_REEXPORTS = frozenset(
    {
        "_active_rate_limit_coder",
        "_alert_reference_time",
        "_budget_chip",
        "_build_activity_feed",
        "_build_alerts",
        "_build_recent_graphql_burns_view",
        "_build_resources_view",
        "_claude_usage_chip",
        "_coder_rate_limit_supported",
        "_compute_stats",
        "_daemon_default_coder_name",
        "_effective_coder_name",
        "_exit_reason_classes",
        "_exit_reason_label",
        "_format_alert_duration",
        "_format_duration_ms",
        "_format_history_time",
        "_most_recent_transition_into",
        "_parse_history_time",
        "_parse_iso8601",
        "_profile_parts",
        "_recent_repo_metrics_payload",
        "_repo_badge_abbrev",
        "_repo_badge_style",
        "_repo_coder_form_value",
        "_repo_template_context",
        "_resource_zone",
        "_serialize_run_record",
    }
)

_REPO_CONTROL_REEXPORTS = frozenset(
    {
        "_append_history_entry",
        "_apply_repo_control_update",
        "_coder_display_name",
        "_publish_history_entry_event",
        "_RepoStateMutationError",
        "_resolve_repo_task_path",
        "_resume_event_message",
        "_update_repo_pause_state",
    }
)


def __getattr__(name: str) -> Any:
    """Lazily resolve names re-exported from route submodules.

    Eagerly importing them with ``from src.web.routes.<name> import ...``
    would create a circular import: each route module imports back into
    this one via ``from src.web import app as _app``, so attribute access
    on a partial module raises ``ImportError`` whenever the route module
    is the entry point of the import. Resolving lazily defers the lookup
    until both modules have finished initializing.
    """
    if name in _DASHBOARD_REEXPORTS:
        return getattr(_dashboard_routes, name)
    if name in _REPO_CONTROL_REEXPORTS:
        return getattr(_repo_control_routes, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


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
    plugin: CoderPlugin,
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
app.include_router(_dashboard_routes.router)
app.include_router(_repo_control_routes.router)


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
        if claude_model is not None or codex_model is not None:
            registry = build_coder_registry()
            if claude_model is not None:
                updates["claude_model"] = _validate_coder_model(
                    claude_model,
                    field_name="claude_model",
                    plugin=registry.get("claude"),
                    default_model=DaemonConfig().claude_model,
                )
            if codex_model is not None:
                updates["codex_model"] = _validate_coder_model(
                    codex_model,
                    field_name="codex_model",
                    plugin=registry.get("codex"),
                )
    except ValueError as exc:
        return await _render_settings_daemon_error(request, str(exc), 422)

    try:
        refreshed_cfg = update_daemon_config(path=CONFIG_PATH, **updates)
    except ValueError as exc:
        return await _render_settings_daemon_error(request, str(exc), 422)
    except OSError as exc:
        return await _render_settings_daemon_error(
            request, f"Failed to write config.yml: {exc}", 503
        )

    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is not None:
        repo_names = [
            repo_slug_from_url(repo.url) for repo in refreshed_cfg.repositories
        ]
        await apply_config_mutation(
            redis_client=redis_client,
            affected_repo_names=repo_names,
            event_type="settings",
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
    return build_coder_registry().get("claude").check_auth(
        config_path=CONFIG_PATH
    )


def _check_codex_auth() -> dict[str, str]:
    """Probe the ``codex`` CLI and report its authorization status."""
    return build_coder_registry().get("codex").check_auth(
        config_path=CONFIG_PATH
    )


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

    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is not None:
        await apply_config_mutation(
            redis_client=redis_client,
            affected_repo_names=[repo_slug_from_url(url)],
            event_type="settings",
        )
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
    if redis_client is not None:
        await apply_config_mutation(
            redis_client=redis_client,
            affected_repo_names=[name],
            event_type="settings",
        )
    context = await _dashboard_routes._repo_template_context(
        name, redis_client
    )
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
        raw = await redis_client.get(pipeline_state(name))
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

    aggregated_issues: list[str] = []
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
                for issue in exc.issues:
                    aggregated_issues.append(
                        issue.replace(str(task_path), fname)
                    )

    if aggregated_issues:
        # Cap at 50 entries so a misbehaving batch upload cannot fill the
        # dashboard error toast with thousands of lines. The Depends-on
        # hint is keyed off the full aggregated list, not the capped slice,
        # so a relevant issue beyond the truncation boundary still surfaces
        # the guidance line.
        has_missing_depends_on = any(
            "missing Depends on" in issue for issue in aggregated_issues
        )
        capped = aggregated_issues[:50]
        truncated = len(aggregated_issues) - len(capped)
        if (
            len(aggregated_issues) == 1
            and has_missing_depends_on
        ):
            return _render_upload_error(
                request,
                f"Task file validation failed: {capped[0]} field.\n"
                "Use 'Depends on: none' for tasks with no dependencies.",
                400,
                repo_name=name,
            )
        body = "Task file validation failed:\n" + "\n".join(capped)
        if truncated > 0:
            body += f"\n... and {truncated} more error(s) (truncated)"
        if has_missing_depends_on:
            body += "\nUse 'Depends on: none' for tasks with no dependencies."
        return _render_upload_error(request, body, 400, repo_name=name)

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
            pending_key = upload_pending(name)
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

    try:
        await publish_wake(redis_client, name, "upload")
    except Exception:
        logger.warning(
            "publish_wake failed for %s; daemon will pick up upload on next tick",
            name,
            exc_info=True,
        )

    return _render_upload_success(
        request,
        _build_upload_success_message(uploaded_filenames, repo_state.state),
        repo_name=name,
    )


def _resolve_onboarding_target(repo_name: str) -> Path | None:
    """Return the AGENTS.md path for ``repo_name`` if it is safe to touch.

    Returns ``None`` when ``repo_name`` fails the slug regex, is not
    listed in ``config.yml``, would resolve outside ``REPOS_DIR``, or
    the on-disk repo directory is not an existing git checkout (no
    ``.git`` entry). The combination of regex, config-membership check,
    and ``relative_to`` resolution is the path-traversal sandbox: any
    single layer alone would be insufficient because a malformed config
    entry, a permissive regex, or a symlink under ``REPOS_DIR`` could
    each individually allow escape. The ``.git`` check additionally
    prevents apply from creating a fresh non-git directory under
    ``REPOS_DIR`` — that would later trip ``ensure_repo_cloned`` into
    running ``git fetch`` against a non-repo and parking the daemon in
    an error state.
    """
    if not _REPO_SLUG_PATTERN.fullmatch(repo_name):
        return None
    cfg = load_config(CONFIG_PATH)
    known_slugs = {repo_slug_from_url(repo.url) for repo in cfg.repositories}
    if repo_name not in known_slugs:
        return None
    repos_root = Path(REPOS_DIR).resolve()
    repo_dir = (Path(REPOS_DIR) / repo_name).resolve()
    target = repo_dir / "AGENTS.md"
    try:
        target.relative_to(repos_root)
    except ValueError:
        return None
    if not repo_dir.is_dir() or not (repo_dir / ".git").exists():
        return None
    # ``target.relative_to`` only validates the textual path; if AGENTS.md
    # itself is a symlink, ``read_text``/``write_text`` would follow it and
    # could read or overwrite a file outside REPOS_DIR. Reject symlinked
    # AGENTS.md outright so reconciliation only ever touches a regular
    # file under operator control.
    if target.is_symlink():
        return None
    # ``read_text`` on a directory or other non-regular path raises
    # ``IsADirectoryError`` / ``OSError`` rather than ``FileNotFoundError``,
    # which would bubble up as a 500. A repo can legitimately contain an
    # ``AGENTS.md/`` directory, so reject any non-regular existing target
    # at the resolver to keep the endpoints' 4xx contract intact.
    if target.exists() and not target.is_file():
        return None
    return target


@app.post("/onboarding/preview")
async def onboarding_preview(repo_name: str = Form(...)) -> JSONResponse:
    """Return what onboarding reconciliation would change in AGENTS.md.

    Form field ``repo_name`` is the repo slug (``owner__repo``). The
    endpoint never writes; the response payload contains the full
    proposed file body and a unified diff so the operator can decide
    whether to call :func:`onboarding_apply`.
    """
    target = _resolve_onboarding_target(repo_name)
    if target is None:
        return JSONResponse(
            {"error": "Unknown or invalid repo_name"}, status_code=422
        )
    try:
        proposed, diff = reconcile_agents_md(target, dry_run=True)
    except MarkerError as exc:
        return JSONResponse(
            {"error": f"Malformed managed markers in AGENTS.md: {exc}"},
            status_code=422,
        )
    return JSONResponse(
        {
            "applied": False,
            "diff": diff,
            "proposed_content": proposed,
        }
    )


@app.post("/onboarding/apply")
async def onboarding_apply(repo_name: str = Form(...)) -> JSONResponse:
    """Write the reconciled AGENTS.md for ``repo_name`` to disk."""
    target = _resolve_onboarding_target(repo_name)
    if target is None:
        return JSONResponse(
            {"error": "Unknown or invalid repo_name"}, status_code=422
        )
    try:
        final, diff = reconcile_agents_md(target, dry_run=False)
    except MarkerError as exc:
        return JSONResponse(
            {"error": f"Malformed managed markers in AGENTS.md: {exc}"},
            status_code=422,
        )
    return JSONResponse(
        {
            "applied": True,
            "diff": diff,
            "proposed_content": final,
        }
    )
