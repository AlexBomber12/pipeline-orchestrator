"""FastAPI dashboard application.

Read-only web UI that lists configured repositories and their pipeline state.
State is published to Redis by the daemon; if a repository has no entry in
Redis the dashboard renders a default ``IDLE`` state derived from
``config.yml``.

Routing is split across submodules:

* ``src.web.routes.dashboard`` — dashboard rendering and HTMX partials.
* ``src.web.routes.repo_control`` — repo control mutations
  (pause/resume/stop/coder/tasks).
* ``src.web.routes.settings`` — daemon and per-repo settings, auth
  status, coder dropdown.
* ``src.web.routes.uploads`` — task file upload route.
* ``src.web.routes.onboarding`` — AGENTS.md reconciliation preview/apply.

Re-exports of helpers moved out are resolved lazily via :func:`__getattr__`
so existing tests that reach for ``web_app.X`` continue to work after the
split (``X`` is loaded on demand from whichever submodule now owns it).
"""

from __future__ import annotations

import logging
import os
import subprocess  # noqa: F401 — re-exported via web_app.subprocess for tests
import zipfile  # noqa: F401 — re-exported via web_app.zipfile for tests
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator

import redis.asyncio as aioredis
from fastapi import FastAPI
from fastapi.templating import Jinja2Templates

from src.config import (
    add_repository,  # noqa: F401 — accessed by routes via _app.add_repository
    load_config,  # noqa: F401 — accessed by routes via _app.load_config
    remove_repository,  # noqa: F401 — accessed by routes via _app.remove_repository
    update_daemon_config,  # noqa: F401 — accessed by routes via _app.update_daemon_config
    update_repository,  # noqa: F401 — accessed by routes via _app.update_repository
)
from src.events import (
    publish_repo_event,  # noqa: F401 — accessed by routes via _app.publish_repo_event
    publish_wake,  # noqa: F401 — accessed by routes via _app.publish_wake
)
from src.events.sse import (
    RepoEventsUnavailableError,  # noqa: F401 — accessed by routes via _app.RepoEventsUnavailableError
    stream_repo_events,  # noqa: F401 — accessed by routes via _app.stream_repo_events
)
from src.web.services import (
    upload_validation as _upload_validation_service,
)
from src.web.services.config_updates import (
    apply_config_mutation,  # noqa: F401 — accessed by routes via _app.apply_config_mutation
)

DEFAULT_REDIS_URL = "redis://localhost:6379/0"
CONFIG_PATH = os.environ.get("PO_CONFIG_PATH", "config.yml")
REPOS_DIR = "/data/repos"
UPLOADS_DIR = "/data/uploads"
_UPLOAD_MAX_TOTAL_BYTES = 1_000_000  # 1 MB
logger = logging.getLogger(__name__)

_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))
templates.env.globals["utcnow"] = lambda: datetime.now(timezone.utc)


def _format_reset_unix(
    reset_unix: int | None,
    *,
    now: datetime | None = None,
) -> str:
    """Render ``reset_unix`` as a smart relative/absolute reset label.

    Returns ``"unknown"`` for ``None`` so the template still has a string
    to interpolate; chip templates only render the reset line when
    ``reset_unix`` is truthy, so this branch is defensive.

    Defined here rather than in ``src.web.routes.dashboard`` because the
    Jinja filter has to be registered at module load time and the dashboard
    module is partial at that point whenever the route module is the entry
    point of the import (it imports this module via ``from src.web import
    app as _app``).
    """
    if not reset_unix:
        return "unknown"
    current = now if now is not None else datetime.now(timezone.utc)
    reset_at = datetime.fromtimestamp(int(reset_unix), tz=timezone.utc)
    seconds_until_reset = (reset_at - current).total_seconds()
    if 0 <= seconds_until_reset < 24 * 60 * 60:
        minutes = max(1, int(seconds_until_reset // 60))
        if minutes < 60:
            return f"resets in {minutes} min"
        hours = minutes // 60
        remaining_minutes = minutes % 60
        if remaining_minutes == 0:
            return f"resets in {hours}h"
        return f"resets in {hours}h {remaining_minutes}m"
    return reset_at.strftime("resets %b %-d, %I:%M %p")


templates.env.filters["format_reset"] = _format_reset_unix

# Templates expose ``css_escape`` and ``upload_feedback_target`` as Jinja
# globals so dashboard partials can build htmx hx-target selectors that
# survive repo names with dots or other CSS-special characters. The
# concrete implementations live in ``src.web.services.upload_validation``;
# they are registered here to keep the call near the ``Jinja2Templates``
# object that owns the environment.
templates.env.globals["css_escape"] = _upload_validation_service._escape_css_identifier
templates.env.globals["upload_feedback_target"] = (
    _upload_validation_service._upload_feedback_target
)

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
from src.web.routes import onboarding as _onboarding_routes  # noqa: E402
from src.web.routes import repo_control as _repo_control_routes  # noqa: E402
from src.web.routes import settings as _settings_routes  # noqa: E402
from src.web.routes import uploads as _uploads_routes  # noqa: E402
from src.web.services import auth_probe as _auth_probe  # noqa: E402
from src.web.services import upload_validation as _upload_validation  # noqa: E402
from src.web.services.repo_state import (  # noqa: E402,F401
    _default_repo_state,
    _find_repo_config_by_name,
    _get_repo_state_safe,
    get_all_repo_states,
    get_repo_state,
)

_DASHBOARD_REEXPORTS = frozenset(
    {
        "_active_repo_coder",
        "_active_rate_limit_coder",
        "_alert_reference_time",
        "_budget_chip",
        "_build_activity_feed",
        "_build_alerts",
        "_build_recent_graphql_burns_view",
        "_build_resources_view",
        "_claude_usage_chip",
        "_codex_usage_chip",
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

_SETTINGS_REEXPORTS = frozenset(
    {
        "_build_coder_rows",
        "_coerce_bool",
        "_coerce_float",
        "_coerce_int",
        "_render_config_write_error",
        "_render_settings_daemon_error",
        "_render_settings_daemon_response",
        "_render_settings_error",
        "_render_settings_repo_list",
        "_settings_daemon_template_context",
        "_validate_coder_model",
    }
)

_AUTH_PROBE_REEXPORTS = frozenset(
    {
        "_AUTH_CHECK_TIMEOUT_SEC",
        "_AUTH_STATUS_CACHE",
        "_auth_probe_env",
        "_check_claude_auth",
        "_check_codex_auth",
        "_check_gh_auth",
        "_collect_auth_status",
        "_default_auth_status",
        "_first_probe_line",
        "_get_cached_auth_status",
        "_run_auth_command",
    }
)

_UPLOAD_VALIDATION_REEXPORTS = frozenset(
    {
        "_ALLOWED_TASK_PATTERN",
        "_STAGING_MAX_AGE_HOURS",
        "_TASK_UPLOAD_PATTERN",
        "_build_upload_success_message",
        "_escape_css_identifier",
        "_format_upload_message_lines",
        "_task_upload_summary",
        "_unique_filenames",
        "_upload_feedback_target",
        "sweep_abandoned_staging",
    }
)

_UPLOAD_ROUTE_REEXPORTS = frozenset(
    {
        "_get_upload_lock",
        "_render_upload_error",
        "_render_upload_success",
        "_upload_locks",
    }
)

_ONBOARDING_REEXPORTS = frozenset(
    {
        "_REPO_SLUG_PATTERN",
        "_resolve_onboarding_target",
    }
)


def __getattr__(name: str) -> Any:
    """Lazily resolve names re-exported from route and service submodules.

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
    if name in _SETTINGS_REEXPORTS:
        return getattr(_settings_routes, name)
    if name in _AUTH_PROBE_REEXPORTS:
        return getattr(_auth_probe, name)
    if name in _UPLOAD_VALIDATION_REEXPORTS:
        return getattr(_upload_validation, name)
    if name in _UPLOAD_ROUTE_REEXPORTS:
        return getattr(_uploads_routes, name)
    if name in _ONBOARDING_REEXPORTS:
        return getattr(_onboarding_routes, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


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


_OPERATOR_HEARTBEAT_MUTATION_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
_TRUSTED_OPERATOR_FETCH_SITES = frozenset({"same-origin", "same-site"})


def _is_operator_driven_request(request) -> bool:
    """True iff the request carries a signal that an operator browser drove it.

    The repo has no formal auth, so we filter automated traffic
    (health checks, scrape pollers, ``curl`` probes) by requiring at
    least one indicator that a browser session is in the loop:

    * Mutating methods (POST/PUT/PATCH/DELETE) — only the dashboard
      exposes these, and they require explicit operator action.
    * ``HX-Request: true`` — set by HTMX, only present when the
      dashboard's own JS is the caller.
    * ``Sec-Fetch-Site: same-origin`` or ``same-site`` — sent by modern
      browsers when the navigation/fetch originated within the dashboard
      itself. ``cross-site`` (third-party embed) and ``none`` (direct
      address-bar navigation, bookmark, automated tools spoofing the
      header) are rejected: neither implies the operator is actively
      using the dashboard, and accepting them would let an embedded
      page or a curl probe pin ``operator_heartbeat`` alive and defeat
      off-hours AWAY.

    HEAD/OPTIONS (probes, CORS preflight) are excluded, and a bare GET
    without browser headers is rejected so a load balancer hitting
    ``/api/states`` cannot keep the heartbeat key alive when no
    operator is present.
    """
    method = request.method
    if method in _OPERATOR_HEARTBEAT_MUTATION_METHODS:
        return True
    if method != "GET":
        return False
    headers = request.headers
    if headers.get("hx-request") is not None:
        return True
    if headers.get("sec-fetch-site") in _TRUSTED_OPERATOR_FETCH_SITES:
        return True
    return False


@app.middleware("http")
async def operator_heartbeat_middleware(request, call_next):
    """Refresh ``operator_heartbeat`` Redis key on operator dashboard traffic.

    Powers the ``HeartbeatSource`` half of the Cancellation-policy
    availability composition (PR-255). The dashboard is GET-dominated
    (main page, ``/api/*`` polling, ``/partials/*``), so restricting
    refresh to mutating methods would let an actively-watching operator
    expire after the TTL. We instead refresh on any successful 2xx
    response that :func:`_is_operator_driven_request` classifies as
    operator-driven — explicit mutations, HTMX polls from the dashboard,
    or a same-origin/same-site browser GET. Automated GET pollers,
    health checks, third-party embeds (``cross-site``), and direct
    address-bar navigations (``none``) are skipped so they cannot pin
    the key alive when no operator is present.

    Best-effort: a Redis outage must not break the request, so all
    storage errors are swallowed.
    """
    response = await call_next(request)
    if not _is_operator_driven_request(request):
        return response
    if not (200 <= response.status_code < 300):
        return response
    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is not None:
        try:
            await redis_client.set("operator_heartbeat", "1", ex=300)
        except Exception:
            pass
    return response


app.include_router(_dashboard_routes.router)
app.include_router(_repo_control_routes.router)
app.include_router(_settings_routes.router)
app.include_router(_uploads_routes.router)
app.include_router(_onboarding_routes.router)
