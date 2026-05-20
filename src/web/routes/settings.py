"""Settings routes for daemon-level config, per-repo config, and auth status.

Mirrors the responsibilities removed from ``src.web.app`` in PR-225b: the
settings page render, daemon and per-repo config mutations, the auth
status JSON/HTML probes, and the coder dropdown. Pause/resume/stop and
upload routes live in their own modules; only config-shaped endpoints
that respond to ``/settings/*``, ``/partials/settings/*``, or
``/api/{auth-status,coders}`` belong here.
"""

from __future__ import annotations

import asyncio
import html
import json
import re
import time
from datetime import datetime, timezone
from importlib import metadata
from typing import Any

import httpx
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.audit.webhook_log import write_webhook_audit
from src.coder_registry import CoderPlugin
from src.coders import build_coder_registry
from src.config import (
    AppConfig,
    DaemonConfig,
    load_config,
)
from src.daemon.sandbox import is_bubblewrap_available
from src.utils import repo_slug_from_url
from src.web.services.auth_probe import (
    _collect_auth_status,
    _get_cached_auth_status,
)
from src.web.services.config_writer import (
    delete_daemon_fields,
    write_daemon_field,
)
from src.web.services.repo_state import _find_repo_config_by_name

router = APIRouter()

SPEND_CEILING_FIELDS = (
    "spend_ceiling_session_percent",
    "spend_ceiling_weekly_percent",
    "spend_ceiling_warning_percent",
)
# Session/weekly are ``int | None`` in ``DaemonConfig``; blank input disables
# the ceiling by deleting the key so the Pydantic default (None) applies.
# Warning has a non-None default and must stay present, so it is excluded.
OPTIONAL_SPEND_CEILING_FIELDS = frozenset(
    {
        "spend_ceiling_session_percent",
        "spend_ceiling_weekly_percent",
    }
)

# PR-344b: guardrail webhook fields exposed via the Settings UI. The
# four names mirror ``DaemonConfig`` so HTMX inputs round-trip through
# ``write_daemon_field`` without a remap layer. The two URL fields are
# ``str | None`` in the model and follow the same blank-clears-the-key
# convention as the optional spend ceilings.
NOTIFICATION_WEBHOOK_FIELDS = (
    "guardrail_notification_webhook_url",
    "guardrail_notification_min_tier",
    "guardrail_notification_timeout_seconds",
    "dashboard_base_url",
)
OPTIONAL_NOTIFICATION_WEBHOOK_FIELDS = frozenset(
    {
        "guardrail_notification_webhook_url",
        "dashboard_base_url",
    }
)

# PR-344c: sandbox/backup/audit fields exposed via the Settings UI. The
# spec named ``git_bundle_backup_retention_days`` but no such field
# exists in ``DaemonConfig``; ``git_bundle_backup_daily_retention`` is
# the actual knob (and the one a Pydantic reload will honor), so the UI
# binds to that name with the spec's 1-365 day range applied here.
# ``git_bundle_backup_dir`` is required by the IDLE scheduler alongside
# the enabled flag (see ``_run_git_bundle_backup_if_due``: bundles are
# skipped unless both are set), so the UI exposes the directory too —
# otherwise a default install can flip the toggle on with no effect.
SANDBOX_BACKUP_FIELDS = (
    "coder_filesystem_isolation",
    "git_bundle_backup_enabled",
    "git_bundle_backup_dir",
    "git_bundle_backup_daily_retention",
    "main_commit_audit_interval_idle_cycles",
)
# ``git_bundle_backup_dir`` is ``str | None`` in ``DaemonConfig``; a
# blank input clears the key so Pydantic reloads None and the scheduler
# treats backups as unconfigured.
OPTIONAL_SANDBOX_BACKUP_FIELDS = frozenset({"git_bundle_backup_dir"})
# HTML checkboxes omit the field entirely when unchecked. The endpoint
# resolves an absent value to ``False`` for these fields instead of the
# default "field is required" 400, otherwise the UI could never toggle a
# sandbox/backup flag off.
BOOLEAN_CONFIG_FIELDS = frozenset(
    {
        "coder_filesystem_isolation",
        "git_bundle_backup_enabled",
    }
)

EDITABLE_CONFIG_FIELDS = frozenset(
    SPEND_CEILING_FIELDS + NOTIFICATION_WEBHOOK_FIELDS + SANDBOX_BACKUP_FIELDS
)
OPTIONAL_EDITABLE_CONFIG_FIELDS = (
    OPTIONAL_SPEND_CEILING_FIELDS
    | OPTIONAL_NOTIFICATION_WEBHOOK_FIELDS
    | OPTIONAL_SANDBOX_BACKUP_FIELDS
)

_HTTP_URL_RE = re.compile(r"^https?://", re.IGNORECASE)

_BOOL_TRUE = {"true", "1", "yes", "on"}
_BOOL_FALSE = {"false", "0", "no", "off"}


def _get_daemon_version() -> str:
    """Return the installed package version for synthetic webhook payloads."""
    try:
        return metadata.version("pipeline-orchestrator")
    except metadata.PackageNotFoundError:
        return "unknown"


def _json_payload_size_bytes(payload: dict[str, Any]) -> int:
    return len(
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    )


def _coerce_bool(value: str, field: str) -> bool:
    lowered = value.strip().lower()
    if lowered in _BOOL_TRUE:
        return True
    if lowered in _BOOL_FALSE:
        return False
    raise ValueError(f"{field} must be a boolean")


def _sandbox_actual_state(daemon: DaemonConfig) -> str:
    """Return the runtime sandbox state for the Settings badge.

    ``disabled`` when isolation is toggled off, ``unavailable`` when
    isolation is on but ``bwrap`` cannot actually create a sandbox on
    this host (binary missing from PATH, user namespaces disabled,
    seccomp profile blocking the smoke test, etc.), and ``active``
    otherwise. The availability probe is the same
    :func:`src.daemon.sandbox.is_bubblewrap_available` smoke test the
    coder dispatch path runs before wrapping a command, so the badge
    cannot say "active" while coders silently launch outside the
    sandbox. Template suppresses the badge entirely when this returns
    ``disabled`` so the checkbox label stays unornamented.
    """
    if not daemon.coder_filesystem_isolation:
        return "disabled"
    if not is_bubblewrap_available():
        return "unavailable"
    return "active"


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
    cfg = load_config(_app.CONFIG_PATH)
    return _app.templates.TemplateResponse(
        request,
        "components/settings_repo_list_response.html",
        {"repos": cfg.repositories, "daemon": cfg.daemon},
    )


def _render_settings_error(
    request: Request, message: str, status_code: int
) -> HTMLResponse:
    cfg = load_config(_app.CONFIG_PATH)
    return _app.templates.TemplateResponse(
        request,
        "components/settings_error.html",
        {
            "message": message,
            "repos": cfg.repositories,
            "daemon": cfg.daemon,
        },
        status_code=status_code,
    )


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


async def _settings_daemon_template_context(
    request: Request,
    *,
    use_cached_auth: bool = False,
) -> dict[str, Any]:
    cfg = load_config(_app.CONFIG_PATH)
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
    return _app.templates.TemplateResponse(
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
    return _app.templates.TemplateResponse(
        request,
        "components/settings_daemon_error.html",
        {**context, "message": message},
        status_code=status_code,
    )


@router.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request) -> HTMLResponse:
    cfg = load_config(_app.CONFIG_PATH)
    auth = await _collect_auth_status()
    coder_rows = _build_coder_rows(cfg, auth)
    return _app.templates.TemplateResponse(
        request,
        "settings.html",
        {
            "title": "Settings",
            "repos": cfg.repositories,
            "daemon": cfg.daemon,
            "coders": coder_rows,
            "auth": auth,
            "sandbox_actual_state": _sandbox_actual_state(cfg.daemon),
        },
    )


@router.get("/partials/settings/daemon", response_class=HTMLResponse)
async def partial_settings_daemon(request: Request) -> HTMLResponse:
    context = await _settings_daemon_template_context(request)
    return _app.templates.TemplateResponse(
        request,
        "components/settings_daemon_response.html",
        context,
    )


@router.get("/partials/settings/coders", response_class=HTMLResponse)
async def partial_settings_coders(request: Request) -> HTMLResponse:
    context = await _settings_daemon_template_context(request)
    return _app.templates.TemplateResponse(
        request,
        "components/settings_coders_wrapper.html",
        context,
    )


@router.post("/settings/webhook/test", response_class=HTMLResponse)
async def test_webhook(request: Request) -> HTMLResponse:
    """POST a synthetic test payload to the configured operator webhook.

    The URL is operator-configured, so this endpoint can target internal
    hosts. That SSRF shape is acceptable while the dashboard remains
    LAN-only and operator-only; revisit before exposing it to less-trusted
    users.
    """
    cfg = load_config(_app.CONFIG_PATH)
    form = await request.form()
    submitted_url = form.get("guardrail_notification_webhook_url")
    url = (
        str(submitted_url).strip()
        if submitted_url is not None
        else cfg.daemon.guardrail_notification_webhook_url
    )
    if not url:
        return HTMLResponse(
            '<span class="text-fail">No URL configured</span>',
            status_code=200,
        )

    test_payload = {
        "event": "webhook_test",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "text": "Synthetic test from pipeline-orchestrator settings page",
        "daemon_version": _get_daemon_version(),
    }
    timeout_sec = cfg.daemon.guardrail_notification_timeout_seconds
    payload_size_bytes = _json_payload_size_bytes(test_payload)
    start = time.monotonic()
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                url, json=test_payload, timeout=timeout_sec
            )
        elapsed_ms = (time.monotonic() - start) * 1000
        response_excerpt = response.text[:200]
        await asyncio.to_thread(
            write_webhook_audit,
            event_type="webhook_test",
            webhook_url=url,
            payload_size_bytes=payload_size_bytes,
            attempt_number=1,
            http_status=response.status_code,
            response_excerpt=response_excerpt,
            elapsed_ms=elapsed_ms,
        )
        if response.is_success:
            return HTMLResponse(
                f'<span class="text-ok">✓ HTTP {response.status_code} '
                f"in {elapsed_ms:.0f}ms</span>"
            )
        excerpt = html.escape(response.text[:80])
        return HTMLResponse(
            f'<span class="text-fail">✗ HTTP {response.status_code}: '
            f"{excerpt}</span>"
        )
    except (httpx.HTTPError, httpx.InvalidURL) as exc:
        elapsed_ms = (time.monotonic() - start) * 1000
        error_excerpt = f"{type(exc).__name__}: {str(exc)[:100]}"
        await asyncio.to_thread(
            write_webhook_audit,
            event_type="webhook_test",
            webhook_url=url,
            payload_size_bytes=payload_size_bytes,
            attempt_number=1,
            http_status=None,
            response_excerpt=error_excerpt,
            elapsed_ms=elapsed_ms,
        )
        rendered_error = html.escape(f"{type(exc).__name__}: {str(exc)[:80]}")
        return HTMLResponse(f'<span class="text-fail">✗ {rendered_error}</span>')


@router.put("/settings/daemon", response_class=HTMLResponse)
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
            percent = _coerce_int(
                exploration_epsilon,
                "exploration_epsilon",
                min_value=0,
                max_value=50,
            )
            updates["exploration_epsilon"] = percent / 100.0
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
        refreshed_cfg = _app.update_daemon_config(
            path=_app.CONFIG_PATH, **updates
        )
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
        await _app.apply_config_mutation(
            redis_client=redis_client,
            affected_repo_names=repo_names,
            event_type="settings",
        )
    return await _render_settings_daemon_response(request)


@router.get("/api/auth-status")
async def api_auth_status() -> JSONResponse:
    return JSONResponse(await _collect_auth_status())


@router.get("/api/coders")
async def api_coders() -> JSONResponse:
    cfg = load_config(_app.CONFIG_PATH)
    auth = await _collect_auth_status()
    return JSONResponse({"coders": _build_coder_rows(cfg, auth)})


@router.get("/partials/settings/auth-status", response_class=HTMLResponse)
async def partial_settings_auth_status(request: Request) -> HTMLResponse:
    auth = await _collect_auth_status()
    return _app.templates.TemplateResponse(
        request,
        "components/settings_auth.html",
        {"auth": auth},
    )


@router.get("/partials/settings/repo-list", response_class=HTMLResponse)
async def partial_settings_repo_list(request: Request) -> HTMLResponse:
    return _render_settings_repo_list(request)


@router.post("/settings/repos", response_class=HTMLResponse)
async def post_settings_repo(
    request: Request,
    url: str = Form(...),
    branch: str = Form("main"),
    auto_merge: bool = Form(True),
) -> HTMLResponse:
    try:
        _app.add_repository(
            url,
            path=_app.CONFIG_PATH,
            branch=branch,
            auto_merge=auto_merge,
        )
    except ValueError as exc:
        return _render_settings_error(request, str(exc), 422)
    except OSError as exc:
        return _render_config_write_error(request, exc)
    return _render_settings_repo_list(request)


@router.delete("/settings/repos", response_class=HTMLResponse)
async def delete_settings_repo(
    request: Request, url: str
) -> HTMLResponse:
    """Remove a repository by its full URL.

    The URL is the unique key in the config (basenames can collide across
    owners), so settings mutations key off the normalized URL instead of
    the repo name.
    """
    try:
        _app.remove_repository(url, path=_app.CONFIG_PATH)
    except ValueError as exc:
        return _render_settings_error(request, str(exc), 404)
    except OSError as exc:
        return _render_config_write_error(request, exc)
    return _render_settings_repo_list(request)


@router.put("/settings/repos", response_class=HTMLResponse)
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
        _app.update_repository(url, path=_app.CONFIG_PATH, **updates)
    except ValueError as exc:
        message = str(exc)
        status = 404 if message.startswith("Repository not found") else 422
        return _render_settings_error(request, message, status)
    except OSError as exc:
        return _render_config_write_error(request, exc)

    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is not None:
        await _app.apply_config_mutation(
            redis_client=redis_client,
            affected_repo_names=[repo_slug_from_url(url)],
            event_type="settings",
        )
    return _render_settings_repo_list(request)


def _coerce_percent(value: str, field: str) -> int:
    """Parse a 1-100 integer percent value or raise ``ValueError``.

    Range matches the Pydantic constraint on
    ``DaemonConfig.spend_ceiling_*_percent`` (``ge=1, le=100``), so a value
    accepted here is guaranteed to load cleanly on the daemon's next
    inotify-driven reload.
    """
    parsed = _coerce_int(value, field, min_value=1, max_value=100)
    return parsed


def _coerce_absolute_path(value: str, field: str) -> str:
    """Validate an absolute filesystem path for a directory config field.

    ``git_bundle_backup_dir`` is joined with the repo name and passed to
    ``Path(...)`` by the backup scheduler; a relative path would resolve
    against the daemon process cwd, which differs between local runs and
    the container, and a value containing a NUL byte would crash the
    open call. Reject both up front so the operator gets a 400 instead
    of a silent misconfiguration that the scheduler skips at runtime.
    """
    cleaned = value.strip()
    if "\x00" in cleaned or "\n" in cleaned:
        raise ValueError(f"{field} must not contain newlines or NUL bytes")
    if not cleaned.startswith("/"):
        raise ValueError(f"{field} must be an absolute path (start with /)")
    return cleaned


def _coerce_http_url(value: str, field: str) -> str:
    """Validate an http(s) URL string for a webhook/dashboard config field.

    Strips surrounding whitespace and requires an ``http://`` or
    ``https://`` prefix. The error message never echoes the submitted
    value back: a paste-and-typo on a Slack webhook URL would otherwise
    end up in the operator's browser tab and HTTP access logs as a
    plaintext secret. The blank-string case is handled by the caller via
    ``delete_daemon_fields`` so this function rejects any non-prefixed
    input as a 400.
    """
    cleaned = value.strip()
    if not _HTTP_URL_RE.match(cleaned):
        raise ValueError(f"{field} must start with http:// or https://")
    return cleaned


def _coerce_config_field(field: str, raw: str) -> Any:
    """Dispatch ``raw`` to the validator for ``field``.

    All four notification fields and the three spend-ceiling fields run
    through this helper so the endpoint stays uniform. The dispatch table
    mirrors the Pydantic constraints declared on ``DaemonConfig`` so a
    value accepted here loads cleanly on the next inotify reload.
    """
    if field in SPEND_CEILING_FIELDS:
        return _coerce_percent(raw, field)
    if field in ("guardrail_notification_webhook_url", "dashboard_base_url"):
        return _coerce_http_url(raw, field)
    if field == "guardrail_notification_min_tier":
        # DaemonConfig declares ``ge=1, le=2`` — the consumer in
        # ``runner.py`` compares ``parsed["tier"] >= min_tier``, so the
        # value is an int, not a string enum. Operators see "P1"/"P2"
        # labels in the dropdown; the submitted form value is the int.
        return _coerce_int(raw, field, min_value=1, max_value=2)
    if field == "guardrail_notification_timeout_seconds":
        # Model field is ``float`` with ``ge=1.0, le=30.0``; accept
        # integers in that range. ruamel.yaml writes the value as-is, and
        # Pydantic coerces int→float on the next load.
        return _coerce_int(raw, field, min_value=1, max_value=30)
    if field in BOOLEAN_CONFIG_FIELDS:
        return _coerce_bool(raw, field)
    if field == "git_bundle_backup_dir":
        return _coerce_absolute_path(raw, field)
    if field == "git_bundle_backup_daily_retention":
        return _coerce_int(raw, field, min_value=1, max_value=365)
    if field == "main_commit_audit_interval_idle_cycles":
        return _coerce_int(raw, field, min_value=1, max_value=100)
    raise ValueError(f"Unhandled field: {field}")


@router.post("/settings/config/{field}", response_class=HTMLResponse)
async def update_config_field(request: Request, field: str) -> HTMLResponse:
    """Update a single allow-listed ``daemon.*`` field via ruamel round-trip.

    Allow-list covers ``spend_ceiling_*`` (PR-344a) and the four
    guardrail-notification fields (PR-344b). Other ``daemon.*`` mutations
    go through ``PUT /settings/daemon``, which validates the full
    ``DaemonConfig`` via Pydantic but loses comments on save. This endpoint
    trades the broader validation surface for comment preservation, which
    matters for fields operators read alongside the YAML body.
    """
    if field not in EDITABLE_CONFIG_FIELDS:
        return HTMLResponse(
            "Field not editable via this endpoint", status_code=400
        )
    form = await request.form()
    raw = form.get(field)
    is_blank = isinstance(raw, str) and raw.strip() == ""
    # HTML checkboxes send no form data when unchecked. Default the
    # absent value to "false" so the operator can toggle the flag off via
    # the UI; without this, an unchecked POST would 400 with "field is
    # required" and the toggle would never persist.
    if raw is None and field in BOOLEAN_CONFIG_FIELDS:
        raw = "false"
        is_blank = False
    # Operators clear optional inputs (session/weekly ceilings; webhook URL
    # or dashboard URL) to disable that signal; HTMX posts the blank value,
    # so treat it as a deletion request rather than a 400.
    if is_blank and field in OPTIONAL_EDITABLE_CONFIG_FIELDS:
        try:
            delete_daemon_fields(_app.CONFIG_PATH, [field])
        except OSError as exc:
            return HTMLResponse(
                f"Failed to write config.yml: {exc}", status_code=503
            )
    else:
        if raw is None or not isinstance(raw, str) or raw.strip() == "":
            return HTMLResponse(f"{field} is required", status_code=400)
        try:
            value = _coerce_config_field(field, raw)
        except ValueError as exc:
            return HTMLResponse(str(exc), status_code=400)

        # The IDLE scheduler (`_run_git_bundle_backup_if_due`) skips
        # backups unless both the enabled flag and the directory are
        # set. Enabling the toggle without a configured directory would
        # therefore claim "backups are on" while no bundle is ever
        # created. Reject the write so the operator must set the
        # directory first.
        if (
            field == "git_bundle_backup_enabled"
            and value is True
            and not load_config(_app.CONFIG_PATH).daemon.git_bundle_backup_dir
        ):
            return HTMLResponse(
                "git_bundle_backup_dir must be set before enabling backups",
                status_code=400,
            )

        try:
            write_daemon_field(_app.CONFIG_PATH, field, value)
        except OSError as exc:
            return HTMLResponse(
                f"Failed to write config.yml: {exc}", status_code=503
            )

    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is not None:
        cfg = load_config(_app.CONFIG_PATH)
        repo_names = [repo_slug_from_url(repo.url) for repo in cfg.repositories]
        await _app.apply_config_mutation(
            redis_client=redis_client,
            affected_repo_names=repo_names,
            event_type="settings",
        )
    # Toggling isolation flips the badge state (disabled ↔ unavailable ↔
    # active). Setting/clearing ``git_bundle_backup_dir`` flips the
    # "backup directory required" warning and the enabled-toggle's
    # ``disabled`` attribute, so it shares the same re-render path. The
    # remaining allow-listed fields have no badge or gating, so they
    # keep the lightweight "Updated" body and the input's existing
    # ``hx-swap="none"`` discards it.
    if field in ("coder_filesystem_isolation", "git_bundle_backup_dir"):
        cfg = load_config(_app.CONFIG_PATH)
        return _app.templates.TemplateResponse(
            request,
            "components/settings_sandbox_backup.html",
            {
                "daemon": cfg.daemon,
                "sandbox_actual_state": _sandbox_actual_state(cfg.daemon),
            },
        )
    return HTMLResponse("Updated", status_code=200)


@router.post(
    "/settings/config/reset/spend_ceiling", response_class=HTMLResponse
)
async def reset_spend_ceiling(request: Request) -> HTMLResponse:
    """Reset all three ``spend_ceiling_*`` fields to their Pydantic defaults.

    Deletes the keys from ``config.yml`` so Pydantic's defaults
    (``None`` for session/weekly, ``80`` for warning) take effect on the
    next ``load_config``. Re-renders the Spending controls section as an
    HTMX outerHTML swap target.
    """
    try:
        delete_daemon_fields(_app.CONFIG_PATH, list(SPEND_CEILING_FIELDS))
    except OSError as exc:
        return HTMLResponse(
            f"Failed to write config.yml: {exc}", status_code=503
        )

    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is not None:
        cfg = load_config(_app.CONFIG_PATH)
        repo_names = [repo_slug_from_url(repo.url) for repo in cfg.repositories]
        await _app.apply_config_mutation(
            redis_client=redis_client,
            affected_repo_names=repo_names,
            event_type="settings",
        )

    cfg = load_config(_app.CONFIG_PATH)
    return _app.templates.TemplateResponse(
        request,
        "components/settings_spend_ceiling.html",
        {"daemon": cfg.daemon},
    )


@router.put("/settings/repo/{name}", response_class=HTMLResponse)
async def put_repo_detail_coder(
    request: Request,
    name: str,
    coder: str | None = Form(None),
) -> HTMLResponse:
    cfg = load_config(_app.CONFIG_PATH)
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
        _app.update_repository(repo.url, path=_app.CONFIG_PATH, **updates)
    except OSError as exc:
        return HTMLResponse(f"Failed to write config.yml: {exc}", status_code=503)

    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is not None:
        await _app.apply_config_mutation(
            redis_client=redis_client,
            affected_repo_names=[name],
            event_type="settings",
        )
    context = await _app._repo_template_context(name, redis_client)
    return _app.templates.TemplateResponse(
        request,
        "components/repo_summary.html",
        context,
    )


# Imported at end-of-file so all ``@router`` decorators above have already
# populated ``router.routes`` before ``app.py`` reaches
# ``app.include_router(_settings_routes.router)``. FastAPI snapshots
# ``router.routes`` at include time, so an early import would let app.py
# load this module while it is still partial (router empty) and silently
# drop every endpoint declared below the import.
from src.web import app as _app  # noqa: E402
