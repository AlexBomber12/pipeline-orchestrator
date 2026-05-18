"""Daemon-level operator control endpoints.

PR-308b: ``POST /daemon/panic/resume`` clears the cascade panic state so
dispatch resumes without waiting for the auto-resume cooldown. PR-339
adds daemon-wide pause/resume/stop and drain-progress endpoints so the
operator can coordinate a deploy or full incident pause with a single
request instead of clicking once per repo.

Lives in its own router because the surface is daemon-wide, not
per-repo — the existing ``repo_control`` module strictly handles
per-repo mutations.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

from src.config import load_config
from src.events import publish_wake
from src.keyspace import control_stop, daemon_panic_state, pipeline_state
from src.models import PipelineState, RepoState
from src.utils import repo_slug_from_url
from src.web.services.repo_state import _default_repo_state

router = APIRouter()
logger = logging.getLogger(__name__)

_STOP_TTL_SECONDS = 60


@router.post("/daemon/panic/resume", status_code=204)
async def resume_panic(request: Request) -> Response:
    """Clear the cascade ESCALATE panic state.

    Idempotent: if no panic state is active (Redis missing the key, or
    Redis itself unavailable), still returns 204 so the dashboard's
    HTMX button remains a no-op on repeat clicks. Logs the operator
    intervention so audit trails capture who broke the panic.
    """
    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is None:
        return Response(status_code=204)
    try:
        existed = bool(await redis_client.delete(daemon_panic_state()))
    except Exception:
        logger.warning("Failed to clear cascade panic state", exc_info=True)
        return Response(status_code=204)
    if existed:
        logger.info("Operator cleared cascade panic state")
    return Response(status_code=204)


def _config_path() -> str:
    from src.web import app as _app

    return _app.CONFIG_PATH


async def _load_repo_state(
    redis_client: Any, name: str, url: str
) -> RepoState:
    """Return the current ``RepoState`` for ``name`` or a default IDLE one.

    Falls back to a fresh default on Redis errors and decode failures so a
    single corrupt repo entry does not break a daemon-wide sweep. The
    daemon-wide endpoints walk every configured repo sequentially; treating
    one bad payload as terminal would leave later repos untouched and the
    operator with no signal about which calls succeeded.
    """
    try:
        raw = await redis_client.get(pipeline_state(name))
    except Exception:
        logger.warning("Failed to read state for %s", name, exc_info=True)
        return _default_repo_state(name, url)
    if raw is None:
        return _default_repo_state(name, url)
    try:
        return RepoState.model_validate_json(raw)
    except Exception:
        logger.warning("State decode failed for %s", name, exc_info=True)
        return _default_repo_state(name, url)


async def _write_user_paused(
    redis_client: Any,
    name: str,
    url: str,
    *,
    user_paused: bool,
    clear_stop: bool,
) -> None:
    """Update ``user_paused`` on a single repo's pipeline state.

    Uses Redis WATCH/MULTI/EXEC so a concurrent daemon ``publish_state``
    write landing between our read and write aborts the transaction and
    the callback retries with fresh state. Without CAS, a daemon write
    could clobber the pause/resume flip we just applied, or our write
    could overwrite fresher task metadata the daemon just persisted.
    Mirrors the per-repo ``_update_repo_pause_state`` pattern in
    ``repo_control.py``.
    """
    state_key = pipeline_state(name)
    stop_key = control_stop(name)
    watch_keys: tuple[str, ...] = (
        (state_key, stop_key) if clear_stop else (state_key,)
    )

    async def _transaction(pipe: Any) -> None:
        # Let read errors propagate out to the outer try/except so the
        # repo's pause/resume update is skipped on transient Redis read
        # failures. Swallowing the error here and falling back to a
        # default ``RepoState`` would erase live fields like
        # ``current_task``, ``current_pr``, and rate-limit metadata for
        # an active repo when the read hiccups.
        raw = await pipe.get(state_key)
        if raw is None:
            state = _default_repo_state(name, url)
        else:
            try:
                state = RepoState.model_validate_json(raw)
            except Exception:
                logger.warning(
                    "State decode failed for %s", name, exc_info=True
                )
                state = _default_repo_state(name, url)
        state.user_paused = user_paused
        pipe.multi()
        pipe.set(state_key, state.model_dump_json())
        if clear_stop:
            pipe.delete(stop_key)

    try:
        await redis_client.transaction(_transaction, *watch_keys)
    except Exception:
        logger.warning(
            "Failed to update pipeline state for %s", name, exc_info=True
        )


async def _publish_wake_safe(
    redis_client: Any, name: str, event_type: str
) -> None:
    try:
        await publish_wake(redis_client, name, event_type)
    except Exception:
        logger.warning(
            "publish_wake failed for %s during %s; daemon will pick up "
            "on next tick",
            name,
            event_type,
            exc_info=True,
        )


@router.post("/daemon/pause")
async def daemon_pause(request: Request) -> JSONResponse:
    """Pause every configured repo at once.

    Walks ``config.repositories`` sequentially and sets ``user_paused=True``
    on each repo's pipeline state. Publishes a wake event so the daemon
    picks up the new state without waiting for the next poll tick. A repo
    onboarded between the start and end of this call is not affected;
    operators running this during a deploy should re-issue the request if
    new repos appear mid-flight.
    """
    cfg = await asyncio.to_thread(load_config, _config_path())
    redis_client = getattr(request.app.state, "redis", None)
    affected: list[str] = []
    for repo in cfg.repositories:
        name = repo_slug_from_url(repo.url)
        if redis_client is not None:
            await _write_user_paused(
                redis_client,
                name,
                repo.url,
                user_paused=True,
                clear_stop=False,
            )
            await _publish_wake_safe(redis_client, name, "daemon_pause_all")
        affected.append(name)
    return JSONResponse({"affected": affected, "count": len(affected)})


@router.post("/daemon/resume")
async def daemon_resume(request: Request) -> JSONResponse:
    """Resume every configured repo at once.

    Mirrors :func:`daemon_pause`: clears ``user_paused`` and deletes any
    pending ``control:{name}:stop`` flag so a previously-issued daemon-wide
    stop does not survive the resume.
    """
    cfg = await asyncio.to_thread(load_config, _config_path())
    redis_client = getattr(request.app.state, "redis", None)
    affected: list[str] = []
    for repo in cfg.repositories:
        name = repo_slug_from_url(repo.url)
        if redis_client is not None:
            await _write_user_paused(
                redis_client,
                name,
                repo.url,
                user_paused=False,
                clear_stop=True,
            )
            await _publish_wake_safe(redis_client, name, "daemon_resume_all")
        affected.append(name)
    return JSONResponse({"affected": affected, "count": len(affected)})


@router.post("/daemon/stop")
async def daemon_stop(request: Request) -> JSONResponse:
    """Request a hard stop on every configured repo at once.

    Writes ``control:{name}:stop`` with a 60s TTL for each repo; the
    daemon's ``_monitor_stop_request`` consumes the flag mid-cycle and
    aborts any in-flight coder run.
    """
    cfg = await asyncio.to_thread(load_config, _config_path())
    redis_client = getattr(request.app.state, "redis", None)
    affected: list[str] = []
    for repo in cfg.repositories:
        name = repo_slug_from_url(repo.url)
        if redis_client is not None:
            try:
                await redis_client.set(
                    control_stop(name), "1", ex=_STOP_TTL_SECONDS
                )
            except Exception:
                logger.warning(
                    "Failed to write stop control for %s",
                    name,
                    exc_info=True,
                )
        affected.append(name)
    return JSONResponse({"affected": affected, "count": len(affected)})


@router.get("/daemon/drain-progress")
async def daemon_drain_progress(request: Request) -> JSONResponse:
    """Return per-repo drain status for UI polling during a daemon pause.

    A repo is ``draining`` only when the operator pressed pause while
    the runner was mid-cycle: ``state == PAUSED``, ``user_paused`` is
    set, and a ``current_task`` is still attached. ``user_paused`` is
    the discriminator ``handle_paused`` itself uses to distinguish
    operator pauses from rate-limit pauses (rate-limited repos can sit
    in ``PAUSED`` with ``current_task`` retained), so excluding it
    would surface rate-limit waits as "draining" to deploy operators
    polling this endpoint.
    """
    cfg = await asyncio.to_thread(load_config, _config_path())
    redis_client = getattr(request.app.state, "redis", None)
    repos: list[dict[str, Any]] = []
    for repo in cfg.repositories:
        name = repo_slug_from_url(repo.url)
        if redis_client is not None:
            state = await _load_repo_state(redis_client, name, repo.url)
        else:
            state = _default_repo_state(name, repo.url)
        current_task_id = (
            state.current_task.pr_id if state.current_task is not None else None
        )
        draining = (
            state.state == PipelineState.PAUSED
            and state.user_paused
            and current_task_id is not None
        )
        repos.append(
            {
                "name": name,
                "state": state.state.value,
                "draining": draining,
                "current_task_id": current_task_id,
            }
        )
    return JSONResponse({"repos": repos})
