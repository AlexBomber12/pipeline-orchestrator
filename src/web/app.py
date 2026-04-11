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
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from src.config import load_config
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
