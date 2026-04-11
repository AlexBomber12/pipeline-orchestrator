"""FastAPI dashboard application.

Read-only web UI that lists configured repositories and their pipeline state.
State is published to Redis by the daemon; if a repository has no entry in
Redis the dashboard renders a default ``IDLE`` state derived from
``config.yml``.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator

import redis.asyncio as aioredis
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from src.config import (
    add_repository,
    load_config,
    remove_repository,
    update_repository,
)
from src.models import PipelineState, RepoState
from src.utils import repo_name_from_url

DEFAULT_REDIS_URL = "redis://localhost:6379/0"
CONFIG_PATH = "config.yml"

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
    return templates.TemplateResponse(
        request,
        "index.html",
        {"title": "Dashboard", "repos": states},
    )


@app.get("/api/states")
async def api_states(request: Request) -> JSONResponse:
    redis_client = getattr(request.app.state, "redis", None)
    states = await get_all_repo_states(redis_client)
    return JSONResponse([s.model_dump(mode="json") for s in states])


@app.get("/partials/repo-list", response_class=HTMLResponse)
async def partial_repo_list(request: Request) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    states = await get_all_repo_states(redis_client)
    return templates.TemplateResponse(
        request,
        "components/repo_list.html",
        {"repos": states},
    )


@app.get("/repo/{name}", response_class=HTMLResponse)
async def repo_detail(request: Request, name: str) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    state = await get_repo_state(name, redis_client)
    return templates.TemplateResponse(
        request,
        "repo.html",
        {"title": name, "repo": state},
    )


@app.get("/partials/repo/{name}", response_class=HTMLResponse)
async def partial_repo_detail(request: Request, name: str) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    state = await get_repo_state(name, redis_client)
    return templates.TemplateResponse(
        request,
        "components/repo_detail.html",
        {"repo": state},
    )


def _render_settings_repo_list(request: Request) -> HTMLResponse:
    """Render the settings repo list for a successful mutation response.

    The response includes an OOB clear of ``#settings-error`` so that any
    error banner left over from a prior 422/503 mutation is wiped as soon
    as a subsequent mutation succeeds (otherwise HTMX keeps the stale
    message because success responses only swap ``#settings-repo-list``).
    """
    cfg = load_config(CONFIG_PATH)
    return templates.TemplateResponse(
        request,
        "components/settings_repo_list_response.html",
        {"repos": cfg.repositories},
    )


def _render_settings_error(
    request: Request, message: str, status_code: int
) -> HTMLResponse:
    cfg = load_config(CONFIG_PATH)
    return templates.TemplateResponse(
        request,
        "components/settings_error.html",
        {"message": message, "repos": cfg.repositories},
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
    poll_interval_sec: str | None = Form(None),
) -> HTMLResponse:
    """Update a repository by its full URL.

    The URL is the unique key in the config (basenames can collide across
    owners), so settings mutations key off the normalized URL instead of
    the repo name.

    All fields are taken as ``str | None`` rather than their final types so
    that an empty form value (``review_timeout_min=``, sent when a user
    clears a numeric input) is handled here as a no-op update instead of
    tripping FastAPI's request parser, which would return a raw JSON 422
    that HTMX then swaps into the repo list and wedges the UI.
    """
    updates: dict[str, object] = {}
    if branch is not None and branch != "":
        updates["branch"] = branch
    try:
        if auto_merge is not None and auto_merge != "":
            updates["auto_merge"] = _coerce_bool(auto_merge, "auto_merge")
        # Both numerics must stay strictly positive. The HTML ``min="1"``
        # is client-side only, and the daemon's hung-detection treats any
        # PR with ``elapsed_min >= review_timeout_min`` as hung, so a
        # persisted zero or negative value would flag every PR on that
        # repo as hung the moment it's created.
        if review_timeout_min is not None and review_timeout_min != "":
            updates["review_timeout_min"] = _coerce_int(
                review_timeout_min, "review_timeout_min", min_value=1
            )
        if poll_interval_sec is not None and poll_interval_sec != "":
            updates["poll_interval_sec"] = _coerce_int(
                poll_interval_sec, "poll_interval_sec", min_value=1
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
