"""Repository state lookup helpers shared across web route modules.

These helpers translate between Redis ``pipeline:{name}`` payloads and the
``RepoState`` model used by every dashboard render and control endpoint.
The dashboard reads them through ``get_all_repo_states`` for the home grid;
individual repo pages and the control plane go through ``get_repo_state``;
the control mutation routes use ``_default_repo_state`` to seed a fresh
state when Redis has nothing yet.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import redis.asyncio as aioredis

from src.config import AppConfig, RepoConfig, load_config
from src.keyspace import pipeline_state
from src.models import PipelineState, RepoState
from src.utils import repo_slug_from_url


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
        raw = await redis_client.get(pipeline_state(name))
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
    config_path: str | None = None,
) -> RepoState:
    """Return the state for a single repo by name.

    Looks the repo up in ``config.yml`` to recover the canonical URL, then
    tries to fetch ``pipeline:{name}`` from Redis. Falls back to a default
    ``IDLE`` state if the repo is unknown, Redis is unavailable, or the
    stored payload cannot be decoded. Redis is not consulted for repos
    missing from ``config.yml`` so a stale ``pipeline:{name}`` key left
    over from a removed repo cannot resurface as live state.
    """
    if config_path is None:
        from src.web import app as _app

        config_path = _app.CONFIG_PATH
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


async def get_all_repo_states(
    redis_client: aioredis.Redis | None,
    config_path: str | None = None,
) -> tuple[list[RepoState], str | None]:
    """Return ``(states, redis_warning)`` for every repo in ``config.yml``.

    ``redis_warning`` is non-None when Redis is entirely unavailable; it is
    intended for a top-level dashboard banner. Per-repo degradation (key
    missing, decode failure) is encoded in the individual ``RepoState``
    objects via ``error_message``.
    """
    if config_path is None:
        from src.web import app as _app

        config_path = _app.CONFIG_PATH

    # Synchronous YAML read offloaded to a thread so the event loop
    # stays responsive during config inspection. PR-240.
    cfg = await asyncio.to_thread(load_config, config_path)

    redis_available = redis_client is not None
    redis_warning: str | None = None

    if redis_client is not None:
        try:
            await redis_client.ping()
        except Exception:
            redis_available = False
            redis_warning = "Redis connection lost"

    repos = list(cfg.repositories)

    async def _resolve(repo: RepoConfig):
        name = repo_slug_from_url(repo.url)
        if redis_available:
            state, warning = await _get_repo_state_safe(
                redis_client, name, repo.url
            )
            return name, repo.url, state, warning
        return name, repo.url, None, None

    # Run per-repo lookups concurrently. asyncio.gather preserves
    # order so the returned states list matches cfg.repositories
    # ordering. PR-240.
    results = await asyncio.gather(*[_resolve(r) for r in repos])

    states: list[RepoState] = []
    for name, repo_url, state, warning in results:
        if warning == "Redis unavailable":
            redis_available = False
            redis_warning = "Redis connection lost"
        if state is None:
            state = _default_repo_state(name, repo_url)
            state.state = PipelineState.PREFLIGHT
            state.error_message = "Redis unavailable — state unknown"
        states.append(state)

    return states, redis_warning
