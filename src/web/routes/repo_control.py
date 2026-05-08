"""Repository control-plane routes (pause / resume / stop / coder / tasks).

Mutation endpoints invoked from repo cards on the dashboard. The dashboard
reads-only paths (rendering, JSON status, partials) live in
``src.web.routes.dashboard``; these routes change Redis state, write
``config.yml``, and publish wake events for the daemon.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Literal

import redis.asyncio as aioredis
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

from src.cancellation.storage import delete_cancellation_cause
from src.config import load_config
from src.keyspace import control_stop, pipeline_state, status_write_failed_tasks
from src.models import PipelineState, QueueTask, RepoState, TaskStatus
from src.queue_parser import write_frontmatter_status
from src.web.services.coder import _effective_coder_name
from src.web.services.repo_state import (
    _default_repo_state,
    _find_repo_config_by_name,
)

router = APIRouter()

_HISTORY_LIMIT = 100
_TASK_PR_ID_PATTERN = re.compile(r"^PR-[A-Za-z0-9_.-]+$")
_QUEUE_NOT_READY_FRAGMENT = (
    '<p class="text-sm italic text-gray-500">Queue not yet computed; '
    "daemon syncing.</p>"
)
_QUEUE_NOT_READY_JSON = {"error": "Queue not yet computed"}
_RETRY_TTL_SECONDS = 30 * 24 * 3600
_RETRY_RESERVATION_TTL_SECONDS = 30 * 60

_DEFERRED_CODER_SWITCH_STATES = {
    PipelineState.CODING,
    PipelineState.WATCH,
    PipelineState.FIX,
    PipelineState.MERGE,
    PipelineState.PAUSED,
}
_ACTIVE_RUN_STATES = {
    PipelineState.PREFLIGHT,
    PipelineState.CODING,
    PipelineState.WATCH,
    PipelineState.FIX,
    PipelineState.MERGE,
    PipelineState.PAUSED,
}
_RETRY_BUSY_STATES = _ACTIVE_RUN_STATES - {PipelineState.PAUSED}
_CODER_LABELS = {
    "any": "Any (bandit picks per-PR)",
    "claude": "Claude CLI",
    "codex": "Codex CLI",
}

_QueueSource = Literal["snapshot"]


def _coder_display_name(coder: str) -> str:
    """Return the UI label for a coder selection."""
    return _CODER_LABELS.get(coder, coder)


async def _load_current_queue_snapshot(
    name: str,
) -> tuple[list[QueueTask] | None, str | None]:
    """Return the current queue snapshot and snapshot timestamp for ``name``.

    ``snapshot_at`` is sourced from ``RepoState.current_queue_snapshot_at``
    so it reflects the time the snapshot was actually written, not
    ``RepoState.last_updated`` (a per-cycle daemon heartbeat that rolls
    forward every runner cycle even when ``current_queue`` is unchanged).
    Returns ``(None, None)`` if Redis has no usable snapshot yet.
    """
    redis_client = getattr(_app.app.state, "redis", None)
    if redis_client is None:
        return None, None
    try:
        raw = await redis_client.get(pipeline_state(name))
    except Exception:
        return None, None
    if raw is None:
        return None, None
    try:
        state = RepoState.model_validate_json(raw)
    except Exception:
        return None, None
    if state.current_queue is None:
        return None, None
    snapshot_at = (
        state.current_queue_snapshot_at.isoformat()
        if state.current_queue_snapshot_at is not None
        else None
    )
    return state.current_queue, snapshot_at


def _queue_json_payload(
    name: str,
    tasks: list[QueueTask],
    *,
    snapshot_at: str | None,
) -> dict[str, object]:
    return {
        "repo": name,
        "snapshot_at": snapshot_at,
        "queue": [task.model_dump(mode="json") for task in tasks],
        "source": "snapshot",
    }


class _RepoStateMutationError(Exception):
    """Sentinel for control-plane mutations that should become HTTP responses."""

    def __init__(self, message: str, status_code: int = 503) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class _RetryCapExceeded(Exception):
    """Raised when the operator retry counter has reached its configured cap."""

    def __init__(self, current: int, cap: int) -> None:
        super().__init__("retry cap reached")
        self.current = current
        self.cap = cap


class _TaskNotRetryable(Exception):
    """Raised when a retry request targets a task that is not retryable."""


def _retry_count_key(repo_slug: str, task_id: str) -> str:
    return f"metrics:retry_count:{repo_slug}:{task_id}"


def _retry_fingerprint_key(repo_slug: str, task_id: str) -> str:
    return f"metrics:retry_fingerprint:{repo_slug}:{task_id}"


def _retry_reservation_key(repo_slug: str) -> str:
    return f"control:retry_reservation:{repo_slug}"


def _repo_busy_for_retry(state: RepoState) -> bool:
    if state.state in _RETRY_BUSY_STATES:
        return True
    return state.state == PipelineState.PAUSED and not state.user_paused


def _decode_retry_count(raw: object) -> int:
    if raw is None:
        return 0
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return 0


async def _get_retry_count(
    redis_client: aioredis.Redis,
    repo_slug: str,
    task_id: str,
    fingerprint: str | None = None,
) -> int:
    try:
        raw = await redis_client.get(_retry_count_key(repo_slug, task_id))
        if fingerprint is not None:
            stored_fingerprint = _decode_redis_text(
                await redis_client.get(_retry_fingerprint_key(repo_slug, task_id))
            )
            if stored_fingerprint is not None and stored_fingerprint != fingerprint:
                return 0
    except Exception:
        return 0
    return _decode_retry_count(raw)


def _decode_redis_text(raw: object) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, bytes):
        return raw.decode("utf-8")
    return str(raw)


def _task_retry_fingerprint(task_path: Path) -> str:
    lines = task_path.read_text(encoding="utf-8").splitlines(keepends=True)
    first_content_index = next(
        (index for index, raw_line in enumerate(lines) if raw_line.strip()),
        None,
    )
    if first_content_index is None or lines[first_content_index].rstrip() != "---":
        normalized = lines
    else:
        normalized = []
        in_frontmatter = True
        for index, raw_line in enumerate(lines):
            if index > first_content_index and in_frontmatter and raw_line.rstrip() == "---":
                in_frontmatter = False
            if in_frontmatter and re.match(r"^status:\s*", raw_line.rstrip()):
                continue
            normalized.append(raw_line)
    return hashlib.sha256("".join(normalized).encode("utf-8")).hexdigest()


async def _await_if_needed(result: Any) -> Any:
    if inspect.isawaitable(result):
        return await result
    return result


async def _reserve_repo_for_retry(
    redis_client: aioredis.Redis,
    repo_slug: str,
    repo_url: str,
) -> bool:
    """Atomically pause an inactive repo while retry rewrites its worktree."""
    state_key = pipeline_state(repo_slug)
    reservation_key = _retry_reservation_key(repo_slug)
    acquired = await _await_if_needed(
        redis_client.set(
            reservation_key,
            "1",
            ex=_RETRY_RESERVATION_TTL_SECONDS,
            nx=True,
        )
    )
    if not acquired:
        raise _RepoStateMutationError(
            "Repository retry already in progress; retry later.",
            status_code=409,
        )

    async def _transaction(pipe: Any) -> bool:
        raw = await pipe.get(state_key)
        if raw is None:
            state = _default_repo_state(repo_slug, repo_url)
        else:
            try:
                state = RepoState.model_validate_json(raw)
            except Exception as exc:
                raise _RepoStateMutationError("Failed to read repository state") from exc
        if _repo_busy_for_retry(state):
            raise _RepoStateMutationError(
                "Repository is busy; retry later.",
                status_code=409,
            )
        previous_user_paused = state.user_paused
        state.user_paused = True
        pipe.multi()
        await _await_if_needed(pipe.set(state_key, state.model_dump_json()))
        return previous_user_paused

    try:
        return await redis_client.transaction(
            _transaction,
            state_key,
            value_from_callable=True,
        )
    except Exception:
        await _await_if_needed(redis_client.delete(reservation_key))
        raise


async def _release_repo_retry_reservation(
    redis_client: aioredis.Redis,
    repo_slug: str,
    previous_user_paused: bool,
) -> None:
    """Restore the pre-retry pause bit unless a daemon has become active."""
    state_key = pipeline_state(repo_slug)
    reservation_key = _retry_reservation_key(repo_slug)

    async def _transaction(pipe: Any) -> None:
        raw = await pipe.get(state_key)
        if raw is None:
            return
        try:
            state = RepoState.model_validate_json(raw)
        except Exception:
            return
        if state.state in _ACTIVE_RUN_STATES:
            return
        state.user_paused = previous_user_paused
        pipe.multi()
        await _await_if_needed(pipe.set(state_key, state.model_dump_json()))

    try:
        await redis_client.transaction(_transaction, state_key)
    except Exception:
        pass
    finally:
        try:
            await _await_if_needed(redis_client.delete(reservation_key))
        except Exception:
            pass


async def _increment_retry_count(
    redis_client: aioredis.Redis,
    repo_slug: str,
    task_id: str,
    cap: int,
    fingerprint: str | None = None,
) -> int:
    key = _retry_count_key(repo_slug, task_id)
    fingerprint_key = _retry_fingerprint_key(repo_slug, task_id)

    async def _transaction(pipe: Any) -> int:
        stored_fingerprint = (
            _decode_redis_text(await pipe.get(fingerprint_key)) if fingerprint is not None else None
        )
        current = _decode_retry_count(await pipe.get(key))
        if fingerprint is not None and stored_fingerprint != fingerprint:
            current = 0
        if current >= cap:
            raise _RetryCapExceeded(current, cap)
        next_count = current + 1
        pipe.multi()
        if fingerprint is not None:
            pipe.set(fingerprint_key, fingerprint, ex=_RETRY_TTL_SECONDS)
        pipe.set(key, str(next_count), ex=_RETRY_TTL_SECONDS)
        return next_count

    return await redis_client.transaction(
        _transaction,
        key,
        fingerprint_key,
        value_from_callable=True,
    )


async def _decrement_retry_count(
    redis_client: aioredis.Redis,
    repo_slug: str,
    task_id: str,
) -> None:
    key = _retry_count_key(repo_slug, task_id)

    async def _transaction(pipe: Any) -> None:
        current = _decode_retry_count(await pipe.get(key))
        pipe.multi()
        if current <= 1:
            result = pipe.delete(key)
        else:
            result = pipe.set(key, str(current - 1), ex=_RETRY_TTL_SECONDS)
        if inspect.isawaitable(result):
            await result

    await redis_client.transaction(_transaction, key)


async def _release_retry_reservation(
    redis_client: aioredis.Redis,
    repo_slug: str,
    task_id: str,
) -> None:
    try:
        await _decrement_retry_count(redis_client, repo_slug, task_id)
    except Exception:
        pass


async def _clear_status_write_failed_retry_marker(
    redis_client: aioredis.Redis,
    repo_slug: str,
    task_id: str,
) -> None:
    """Remove a successfully retried task from the status-write-failed set."""
    key = status_write_failed_tasks(repo_slug)
    try:
        decoded = _decode_redis_text(await redis_client.get(key))
        if decoded is None:
            return
        task_ids = json.loads(decoded)
        if not isinstance(task_ids, list):
            return
        if all(str(item) != task_id for item in task_ids):
            return
        remaining = sorted({str(item) for item in task_ids if str(item) != task_id})
        if remaining:
            await _await_if_needed(redis_client.set(key, json.dumps(remaining)))
        else:
            await _await_if_needed(redis_client.delete(key))
    except Exception:
        pass


def _is_nothing_to_commit(exc: subprocess.CalledProcessError) -> bool:
    output = "\n".join(str(part) for part in (exc.stdout, exc.stderr) if part)
    return "nothing to commit" in output.lower()


def _git_output(result: subprocess.CompletedProcess[str]) -> str:
    return "\n".join(str(part) for part in (result.stdout, result.stderr) if part)


def _read_task_frontmatter_status(task_path: Path) -> TaskStatus | None:
    lines = task_path.read_text(encoding="utf-8").splitlines()
    first_content_index = next(
        (index for index, raw_line in enumerate(lines) if raw_line.strip()),
        None,
    )
    if first_content_index is None or lines[first_content_index].rstrip() != "---":
        return None

    for raw_line in lines[first_content_index + 1 :]:
        if raw_line.rstrip() == "---":
            return None
        status_match = re.match(r"^status:\s*(.+?)\s*$", raw_line.rstrip())
        if status_match is None:
            continue
        raw_status = status_match.group(1).split("#", 1)[0].strip().strip("\"'")
        try:
            return TaskStatus(raw_status.upper())
        except ValueError:
            return None
    return None


def _head_commit_subject(repo_root: Path) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo_root), "log", "-1", "--pretty=%s"],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _restore_retry_error_status(task_path: Path) -> None:
    try:
        write_frontmatter_status(task_path, "ERROR")
    except (OSError, ValueError):
        pass


def _checkout_retry_base_task(
    repo_root: Path,
    base_branch: str,
    relative_task: Path,
) -> None:
    subprocess.run(
        ["git", "-C", str(repo_root), "fetch", "origin", base_branch],
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        ["git", "-C", str(repo_root), "checkout", base_branch],
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        ["git", "-C", str(repo_root), "reset", "--mixed", f"origin/{base_branch}"],
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        [
            "git",
            "-C",
            str(repo_root),
            "checkout",
            f"origin/{base_branch}",
            "--",
            relative_task.as_posix(),
        ],
        check=True,
        capture_output=True,
        text=True,
    )


def _commit_and_push_retry_reset(
    repo_root: Path,
    relative_task: Path,
    commit_subject: str,
    base_branch: str,
) -> None:
    replayed_existing_commit = False
    subprocess.run(
        ["git", "-C", str(repo_root), "add", relative_task.as_posix()],
        check=True,
        capture_output=True,
        text=True,
    )
    try:
        subprocess.run(
            [
                "git",
                "-C",
                str(repo_root),
                "commit",
                "-m",
                commit_subject,
                "-m",
                "[skip ci]",
                "--",
                relative_task.as_posix(),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        if not _is_nothing_to_commit(exc):
            raise
        if _head_commit_subject(repo_root) != commit_subject:
            raise _TaskNotRetryable
        replayed_existing_commit = True

    push_result = subprocess.run(
        ["git", "-C", str(repo_root), "push", "origin", f"HEAD:{base_branch}"],
        check=True,
        capture_output=True,
        text=True,
    )
    if replayed_existing_commit and "everything up-to-date" in _git_output(
        push_result
    ).lower():
        raise _TaskNotRetryable


async def _task_view(
    task: QueueTask,
    repo_name: str,
    redis_client: aioredis.Redis | None,
) -> dict[str, object]:
    retry_count = 0
    if task.status == TaskStatus.ERROR and redis_client is not None:
        retry_fingerprint = None
        resolved = await _resolve_repo_task_path(repo_name, task.pr_id)
        if resolved is not None:
            task_path, _task_filename = resolved
            try:
                retry_fingerprint = _task_retry_fingerprint(task_path)
            except (OSError, UnicodeError):
                retry_fingerprint = None
        retry_count = await _get_retry_count(
            redis_client,
            repo_name,
            task.pr_id,
            retry_fingerprint,
        )
    return {
        **task.model_dump(mode="json"),
        "retry_count": retry_count,
    }


async def _build_tasks_panel_context(
    name: str,
    tasks: list[QueueTask],
    *,
    redis_client: aioredis.Redis | None,
    retry_cap: int,
) -> dict[str, object]:
    async def _views_for(status: TaskStatus) -> list[dict[str, object]]:
        return [
            await _task_view(task, name, redis_client)
            for task in tasks
            if task.status == status
        ]

    grouped = {
        "doing": await _views_for(TaskStatus.DOING),
        "todo": await _views_for(TaskStatus.TODO),
        "done": await _views_for(TaskStatus.DONE),
        "error": await _views_for(TaskStatus.ERROR),
    }
    return {
        "repo_name": name,
        "tasks_by_status": grouped,
        "tasks_total": len(tasks),
        "retry_cap": retry_cap,
    }


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
    tasks, _snapshot_at = await _load_current_queue_snapshot(name)
    if tasks is None:
        return HTMLResponse(
            _QUEUE_NOT_READY_FRAGMENT,
            status_code=503,
        )
    return _app.templates.TemplateResponse(
        request,
        "components/tasks_panel.html",
        await _build_tasks_panel_context(
            name,
            tasks,
            redis_client=getattr(request.app.state, "redis", None),
            retry_cap=cfg.daemon.retry_button_cap,
        ),
    )


@router.post("/repos/{name}/tasks/{pr_id}/retry", response_class=HTMLResponse)
async def retry_repo_task(request: Request, name: str, pr_id: str) -> Response:
    """Reset an ERROR task to queued when an operator requests a retry."""
    if not _TASK_PR_ID_PATTERN.match(pr_id):
        return HTMLResponse("Invalid task identifier", status_code=400)

    cfg = load_config(_app.CONFIG_PATH)
    repo_config = _find_repo_config_by_name(cfg, name)
    if repo_config is None:
        return HTMLResponse("Repository not found", status_code=404)

    resolved = await _resolve_repo_task_path(name, pr_id)
    if resolved is None:
        return HTMLResponse("Task file not found", status_code=404)
    task_path, _task_filename = resolved

    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is None:
        return HTMLResponse("Redis unavailable", status_code=503)

    repo_root = Path(_app.REPOS_DIR) / name
    try:
        relative_task = task_path.relative_to(repo_root)
    except ValueError:
        return HTMLResponse("Task file not found", status_code=404)

    cap = cfg.daemon.retry_button_cap
    try:
        current_status = _read_task_frontmatter_status(task_path)
    except (OSError, UnicodeError):
        return HTMLResponse("Failed to read task status", status_code=503)
    if current_status not in {TaskStatus.ERROR, TaskStatus.TODO}:
        return HTMLResponse("Task is not in ERROR", status_code=409)

    retry_reserved = True
    try:
        previous_user_paused = await _reserve_repo_for_retry(
            redis_client,
            name,
            repo_config.url,
        )
    except _RepoStateMutationError as exc:
        return HTMLResponse(exc.message, status_code=exc.status_code)
    except Exception:
        return HTMLResponse("Failed to read repository state", status_code=503)

    try:
        try:
            await asyncio.to_thread(
                _checkout_retry_base_task,
                repo_root,
                repo_config.branch,
                relative_task,
            )
        except subprocess.CalledProcessError:
            return HTMLResponse("Failed to commit retry change", status_code=503)
        if not task_path.is_file():
            return HTMLResponse("Task file not found", status_code=404)

        try:
            current_status = _read_task_frontmatter_status(task_path)
            retry_fingerprint = _task_retry_fingerprint(task_path)
        except (OSError, UnicodeError):
            return HTMLResponse("Failed to read task status", status_code=503)
        if current_status not in {TaskStatus.ERROR, TaskStatus.TODO}:
            return HTMLResponse("Task is not in ERROR", status_code=409)
        rewrote_status = current_status == TaskStatus.ERROR

        try:
            next_count = await _increment_retry_count(
                redis_client,
                name,
                pr_id,
                cap,
                retry_fingerprint,
            )
        except _RetryCapExceeded:
            return HTMLResponse(
                "Retry cap reached. Edit task spec or delete to proceed.",
                status_code=409,
            )
        except Exception:
            return HTMLResponse("Failed to update retry counter", status_code=503)

        try:
            if current_status == TaskStatus.ERROR:
                write_frontmatter_status(task_path, "TODO")
        except Exception:
            await _release_retry_reservation(redis_client, name, pr_id)
            return HTMLResponse("Failed to update task status", status_code=503)

        commit_subject = f"[RETRY] {pr_id} cleared by operator (attempt {next_count}/{cap})"
        try:
            await asyncio.to_thread(
                _commit_and_push_retry_reset,
                repo_root,
                relative_task,
                commit_subject,
                repo_config.branch,
            )
        except _TaskNotRetryable:
            if rewrote_status:
                _restore_retry_error_status(task_path)
            if retry_reserved:
                await _release_retry_reservation(redis_client, name, pr_id)
            return HTMLResponse("Task is not in ERROR", status_code=409)
        except subprocess.CalledProcessError:
            if rewrote_status:
                _restore_retry_error_status(task_path)
            if retry_reserved:
                await _release_retry_reservation(redis_client, name, pr_id)
            return HTMLResponse("Failed to commit retry change", status_code=503)

        try:
            await delete_cancellation_cause(redis_client, name, pr_id)
        except Exception:
            pass
        await _clear_status_write_failed_retry_marker(redis_client, name, pr_id)

        tasks, _snapshot_at = await _load_current_queue_snapshot(name)
        if tasks is None:
            tasks = [
                QueueTask(
                    pr_id=pr_id,
                    title=pr_id,
                    status=TaskStatus.TODO,
                    task_file=relative_task.as_posix(),
                )
            ]
        else:
            tasks = [
                task.model_copy(update={"status": TaskStatus.TODO})
                if task.pr_id == pr_id
                else task
                for task in tasks
            ]

        return _app.templates.TemplateResponse(
            request,
            "components/tasks_panel.html",
            await _build_tasks_panel_context(
                name,
                tasks,
                redis_client=redis_client,
                retry_cap=cap,
            ),
        )
    finally:
        await _release_repo_retry_reservation(
            redis_client,
            name,
            previous_user_paused,
        )


@router.get("/api/repo/{name}/queue", response_class=JSONResponse)
async def api_repo_queue(name: str) -> Response:
    """Return the repo's queue snapshot as JSON."""
    cfg = load_config(_app.CONFIG_PATH)
    if _find_repo_config_by_name(cfg, name) is None:
        return JSONResponse({"error": "Repository not found"}, status_code=404)

    tasks, snapshot_at = await _load_current_queue_snapshot(name)
    if tasks is None:
        return JSONResponse(_QUEUE_NOT_READY_JSON, status_code=503)
    return JSONResponse(
        _queue_json_payload(
            name,
            tasks,
            snapshot_at=snapshot_at,
        )
    )


async def _resolve_repo_task_path(name: str, pr_id: str) -> tuple[Path, str] | None:
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

    def _try_candidate(relative_str: str) -> tuple[Path, str] | None:
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

    # Try mappings in priority order: snapshot, default. The snapshot
    # reflects the queue at the last IDLE cycle, so during the window
    # after a task file is renamed but before the next snapshot refresh
    # the snapshot's ``task_file`` value can point at a file that no
    # longer exists. In that case fall back to ``tasks/{pr_id}.md``
    # instead of returning a 404.
    snapshot_tasks, _snapshot_at = await _load_current_queue_snapshot(name)
    if snapshot_tasks is not None:
        for task in snapshot_tasks:
            if task.pr_id == pr_id and task.task_file:
                hit = _try_candidate(task.task_file)
                if hit is not None:
                    return hit
                break

    return _try_candidate(f"tasks/{pr_id}.md")


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
    resolved = await _resolve_repo_task_path(name, pr_id)
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
