"""Repository control-plane routes (pause / resume / stop / coder / tasks).

Mutation endpoints invoked from repo cards on the dashboard. The dashboard
reads-only paths (rendering, JSON status, partials) live in
``src.web.routes.dashboard``; these routes change Redis state, write
``config.yml``, and publish wake events for the daemon.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Literal

import redis.asyncio as aioredis
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

from src.config import load_config
from src.keyspace import control_recover, control_stop, pipeline_state
from src.models import PipelineState, RepoState, TaskStatus
from src.web.services.coder import _effective_coder_name
from src.web.services.repo_state import (
    _default_repo_state,
    _find_repo_config_by_name,
)

router = APIRouter()

_HISTORY_LIMIT = 100
_TASK_PR_ID_PATTERN = re.compile(r"^PR-[A-Za-z0-9_.-]+$")

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
_CODER_LABELS = {
    "any": "Any (bandit picks per-PR)",
    "claude": "Claude CLI",
    "codex": "Codex CLI",
}


def _coder_display_name(coder: str) -> str:
    """Return the UI label for a coder selection."""
    return _CODER_LABELS.get(coder, coder)


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
    cfg = load_config(_app.CONFIG_PATH)
    repo = _find_repo_config_by_name(cfg, name)
    if repo is None:
        raise _RepoStateMutationError("Repository not found", status_code=404)
    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is None:
        raise _RepoStateMutationError("Redis unavailable", status_code=503)
    return redis_client, pipeline_state(name), repo.url


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
        await _app.publish_repo_event(
            name,
            "history_updated",
            {
                "state": state.state.value,
                "event": event_message,
            },
            redis_client,
        )
    except Exception:
        _app.logger.warning("Failed to publish repo history update", exc_info=True)


def _resume_event_message(state: RepoState) -> str:
    """Return the user-facing event emitted by the Play control."""
    if state.state in _ACTIVE_RUN_STATES:
        return "Pause canceled. Continue current run."
    return "Resumed. Taking next task from queue."


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

    stop_key = control_stop(name)
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


@router.post("/repos/{name}/pause")
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
    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is not None:
        try:
            await _app.publish_wake(redis_client, name, "pause")
        except Exception:
            _app.logger.warning(
                "publish_wake failed for %s; daemon will pick up pause on next tick",
                name,
                exc_info=True,
            )
    return JSONResponse({"ok": True, "user_paused": True})


@router.post("/repos/{name}/resume")
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
    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is not None:
        try:
            await _app.publish_wake(redis_client, name, "resume")
        except Exception:
            _app.logger.warning(
                "publish_wake failed for %s; daemon will pick up resume on next tick",
                name,
                exc_info=True,
            )
    return JSONResponse({"ok": True, "user_paused": False})


@router.post("/repos/{name}/stop")
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
    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is not None:
        try:
            await _app.publish_wake(redis_client, name, "stop")
        except Exception:
            _app.logger.warning(
                "publish_wake failed for %s; daemon will pick up stop on next tick",
                name,
                exc_info=True,
            )
    return JSONResponse({"ok": True, "user_paused": True})


@router.post("/repos/{name}/recover")
async def recover_repo(request: Request, name: str) -> Response:
    """Operator-initiated recovery from HUNG state (PR-247).

    Validates current state == HUNG (HTTP 400 otherwise — recovery is
    HUNG-specific and never applies to CODING/FIX/WATCH/MERGE/IDLE/ERROR),
    then writes a one-shot ``control:{name}:recover`` flag and publishes
    a wake event so ``handle_hung`` picks up the signal on the next
    daemon tick and performs the actual transition (clear current task,
    return to IDLE). The HTTP response confirms the signal was queued —
    not that the transition has already executed — so the client UI must
    refresh state after receiving 200 to observe the new IDLE.
    """
    try:
        redis_client, state_key, repo_url = await _apply_repo_control_update(
            request, name
        )
    except _RepoStateMutationError as exc:
        return HTMLResponse(exc.message, status_code=exc.status_code)

    try:
        raw = await redis_client.get(state_key)
    except Exception:
        return HTMLResponse("Failed to read repository state", status_code=503)
    if raw is None:
        state = _default_repo_state(name, repo_url)
    else:
        try:
            state = RepoState.model_validate_json(raw)
        except Exception:
            return HTMLResponse("Repository state unavailable", status_code=503)

    if state.state != PipelineState.HUNG:
        return JSONResponse(
            {
                "error": "recovery_only_from_hung",
                "current_state": state.state.value,
            },
            status_code=400,
        )

    recover_key = control_recover(name)
    try:
        await redis_client.set(recover_key, "1", ex=300)
    except Exception:
        return HTMLResponse(
            "Failed to queue recovery request", status_code=503
        )

    try:
        await _app.publish_wake(redis_client, name, "recover")
    except Exception:
        _app.logger.warning(
            "publish_wake failed for %s; daemon will pick up recover on "
            "next tick",
            name,
            exc_info=True,
        )

    return JSONResponse({"ok": True, "queued_state": "IDLE"})


@router.post("/repos/{name}/coder", response_class=HTMLResponse)
async def post_repo_detail_coder(
    request: Request,
    name: str,
    coder: str = Form(...),
) -> HTMLResponse:
    cfg = load_config(_app.CONFIG_PATH)
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
        _app.update_repository(repo.url, path=_app.CONFIG_PATH, coder=updated_coder)
    except OSError as exc:
        return HTMLResponse(f"Failed to write config.yml: {exc}", status_code=503)

    await _app.apply_config_mutation(
        redis_client=redis_client,
        affected_repo_names=[name],
        event_type="coder_swap",
    )

    state_key = pipeline_state(name)
    try:
        raw_state = await redis_client.get(state_key)
        if raw_state:
            state = RepoState.model_validate_json(raw_state)
            if state.state not in _DEFERRED_CODER_SWITCH_STATES:
                refreshed = load_config(_app.CONFIG_PATH)
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
        _app.logger.warning("Failed to refresh repo state after coder update", exc_info=True)

    try:
        await _app.publish_repo_event(
            name,
            "config_reloaded",
            {
                "coder": coder,
                "effective_coder": updated_coder or cfg.daemon.coder.value,
            },
            redis_client,
        )
    except Exception:
        _app.logger.warning("Failed to publish coder update event", exc_info=True)

    current_state = await _app.get_repo_state(name, redis_client, config_path=_app.CONFIG_PATH)
    applies_after_current_pr = (
        current_state.state in _DEFERRED_CODER_SWITCH_STATES
    )
    message = f"Switching to {_coder_display_name(coder)}"
    if applies_after_current_pr:
        message += " - applies after current PR completes."
    else:
        message += "."

    context = await _app._repo_template_context(
        name,
        redis_client,
        coder_update_message=message,
    )
    return _app.templates.TemplateResponse(
        request,
        "components/repo_summary.html",
        context,
    )


@router.get("/repos/{name}/tasks", response_class=HTMLResponse)
async def list_repo_tasks(request: Request, name: str) -> Response:
    """Return the repo's tasks grouped by status as an HTML fragment."""
    cfg = load_config(_app.CONFIG_PATH)
    if _find_repo_config_by_name(cfg, name) is None:
        return HTMLResponse(
            '<p class="text-sm italic text-fail">Repository not found.</p>',
            status_code=404,
        )
    queue_path = Path(_app.REPOS_DIR) / name / "tasks" / "QUEUE.md"
    try:
        tasks = _app.parse_queue(str(queue_path))
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
        "canceled": [t for t in tasks if t.status == TaskStatus.CANCELED],
    }
    return _app.templates.TemplateResponse(
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
    repo_root = Path(_app.REPOS_DIR) / name
    tasks_dir = repo_root / "tasks"
    if not tasks_dir.is_dir():
        return None
    tasks_dir_resolved = tasks_dir.resolve()

    relative_str: str | None = None
    try:
        queued_tasks = _app.parse_queue(str(tasks_dir / "QUEUE.md"))
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


@router.get("/repos/{name}/tasks/{pr_id}", response_class=HTMLResponse)
async def view_repo_task(
    request: Request, name: str, pr_id: str
) -> Response:
    """Return one task file's contents as a preformatted HTML fragment."""
    if not _TASK_PR_ID_PATTERN.match(pr_id):
        return HTMLResponse(
            '<p class="text-sm italic text-fail">Invalid task identifier.</p>',
            status_code=400,
        )
    cfg = load_config(_app.CONFIG_PATH)
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
    return _app.templates.TemplateResponse(
        request,
        "components/task_content.html",
        {
            "repo_name": name,
            "pr_id": pr_id,
            "task_filename": task_filename,
            "content": content,
        },
    )


# Imported at end-of-file so all ``@router`` decorators above have already
# populated ``router.routes`` before ``app.py`` reaches
# ``app.include_router(_repo_control_routes.router)``. FastAPI snapshots
# ``router.routes`` at include time, so an early import would let app.py
# load this module while it is still partial (router empty) and silently
# drop every endpoint declared below the import.
from src.web import app as _app  # noqa: E402
