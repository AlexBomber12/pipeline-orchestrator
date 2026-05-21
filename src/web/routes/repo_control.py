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
from fastapi import APIRouter, Form, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from redis.exceptions import RedisError

from src.audit.operator_actions import write_audit_record, write_operator_action_audit
from src.cancellation.storage import (
    READ_REFRESH_TTL_SECONDS,
    TTL_SECONDS,
    CancellationCause,
    cause_key,
    current_run_started_at_key,
    delete_cancellation_cause,
    get_cancellation_cause,
    index_key,
    list_pending_guardrail_decisions,
    prune_dead_index_members,
    record_cancellation_cause,
)
from src.config import load_config
from src.github import gh_runner
from src.github import prs as gh_prs
from src.inhibitor import derive_active_inhibitors
from src.keyspace import (
    control_stop,
    legacy_recovered_tasks,
    pipeline_state,
    status_write_failed_tasks,
)
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
# Reset is for stuck tasks (DOING, ERROR). When status is TODO or DONE and
# Redis carries no per-task state, the task is not stuck and reset would
# either be a no-op (TODO) or destructively re-queue completed work (DONE).
_RESET_NON_STUCK_STATUSES = frozenset({TaskStatus.TODO, TaskStatus.DONE})
_QUEUE_NOT_READY_FRAGMENT = (
    '<p class="text-sm italic text-gray-500">Queue not yet computed; '
    "daemon syncing.</p>"
)
_QUEUE_NOT_READY_JSON = {"error": "Queue not yet computed"}
_RETRY_TTL_SECONDS = 30 * 24 * 3600
_RETRY_RESERVATION_TTL_SECONDS = 30 * 60
_RETRY_GIT_TIMEOUT_SECONDS = 60


def _split_gh_label_output(result: object) -> list[str]:
    if isinstance(result, str):
        return [label.strip() for label in result.splitlines() if label.strip()]
    if isinstance(result, list):
        return [str(label).strip() for label in result if str(label).strip()]
    return []

_DEFERRED_CODER_SWITCH_STATES = {
    PipelineState.CODING,
    PipelineState.WATCH,
    PipelineState.FIX,
    PipelineState.MERGE,
    PipelineState.PAUSED,
}


def _diagnose_exhausted_key(repo_slug: str, task_id: str) -> str:
    return f"diagnose_exhausted:{repo_slug}:{task_id}"
_ACTIVE_RUN_STATES = {
    PipelineState.PREFLIGHT,
    PipelineState.CODING,
    PipelineState.WATCH,
    PipelineState.FIX,
    PipelineState.MERGE,
    PipelineState.PAUSED,
}
_RETRY_BUSY_STATES = (_ACTIVE_RUN_STATES - {PipelineState.PAUSED}) | {
    PipelineState.ERROR
}
_CODER_LABELS = {
    "any": "Any (bandit picks per-PR)",
    "claude": "Claude CLI",
    "codex": "Codex CLI",
}
_PAUSE_CONTROL_EVENT_PREFIXES = (
    "Pause requested.",
    "Resume requested.",
    "Stop requested.",
)
_OPERATOR_CLEARABLE_INHIBITORS = frozenset({"user_pause", "user_stop"})

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
    if state.user_paused:
        return True
    if state.state in _RETRY_BUSY_STATES:
        return True
    return state.state == PipelineState.PAUSED


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
    reservation_started_at = datetime.now(timezone.utc).isoformat()
    acquired = await _await_if_needed(
        redis_client.set(
            reservation_key,
            reservation_started_at,
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


def _has_pause_control_after_reservation(
    state: RepoState,
    reservation_started_at: str | None,
) -> bool:
    if reservation_started_at is None:
        return False
    try:
        reservation_time = datetime.fromisoformat(reservation_started_at)
    except ValueError:
        return False
    for entry in reversed(state.history):
        event = str(entry.get("event", ""))
        if not event.startswith(_PAUSE_CONTROL_EVENT_PREFIXES):
            continue
        raw_time = entry.get("last_seen_at") or entry.get("time")
        if raw_time is None:
            continue
        try:
            event_time = datetime.fromisoformat(str(raw_time))
        except ValueError:
            continue
        return event_time > reservation_time
    return False


async def _release_repo_retry_reservation(
    redis_client: aioredis.Redis,
    repo_slug: str,
    previous_user_paused: bool,
) -> None:
    """Restore the pre-retry pause bit unless a daemon has become active."""
    state_key = pipeline_state(repo_slug)
    reservation_key = _retry_reservation_key(repo_slug)
    try:
        reservation_started_at = _decode_redis_text(await redis_client.get(reservation_key))
    except Exception:
        reservation_started_at = None

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
        if _has_pause_control_after_reservation(state, reservation_started_at):
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
        if (
            fingerprint is not None
            and stored_fingerprint is not None
            and stored_fingerprint != fingerprint
        ):
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
    keys = (status_write_failed_tasks(repo_slug), legacy_recovered_tasks(repo_slug))
    try:
        for key in keys:
            decoded = _decode_redis_text(await redis_client.get(key))
            if decoded is None:
                continue
            task_ids = json.loads(decoded)
            if not isinstance(task_ids, list):
                continue
            if all(str(item) != task_id for item in task_ids):
                continue
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


def _called_process_output(exc: subprocess.CalledProcessError) -> str:
    return "\n".join(str(part) for part in (exc.stdout, exc.stderr) if part)


def _is_missing_task_pathspec(exc: subprocess.CalledProcessError) -> bool:
    output = _called_process_output(exc).lower()
    return "pathspec" in output and "did not match any file" in output


def _run_retry_git(repo_root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(repo_root), *args],
        check=True,
        capture_output=True,
        text=True,
        timeout=_RETRY_GIT_TIMEOUT_SECONDS,
    )


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


def _task_status_from_snapshot(
    tasks: list[QueueTask] | None,
    pr_id: str,
) -> TaskStatus | None:
    if tasks is None:
        return None
    for task in tasks:
        if task.pr_id == pr_id:
            return task.status
    return None


def _is_retryable_task_status(
    frontmatter_status: TaskStatus | None,
    snapshot_status: TaskStatus | None,
) -> bool:
    if frontmatter_status in {TaskStatus.ERROR, TaskStatus.TODO}:
        return True
    return frontmatter_status is None and snapshot_status in {
        TaskStatus.ERROR,
        TaskStatus.TODO,
    }


def _head_commit_subject(repo_root: Path) -> str:
    result = _run_retry_git(repo_root, "log", "-1", "--pretty=%s")
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
    _run_retry_git(repo_root, "fetch", "origin", base_branch)
    _run_retry_git(repo_root, "checkout", "-f", base_branch)
    _run_retry_git(repo_root, "reset", "--hard", f"origin/{base_branch}")
    _run_retry_git(
        repo_root,
        "checkout",
        f"origin/{base_branch}",
        "--",
        relative_task.as_posix(),
    )


def _commit_and_push_retry_reset(
    repo_root: Path,
    relative_task: Path,
    commit_subject: str,
    base_branch: str,
) -> None:
    replayed_existing_commit = False
    _run_retry_git(repo_root, "add", relative_task.as_posix())
    try:
        _run_retry_git(
            repo_root,
            "commit",
            "-m",
            commit_subject,
            "-m",
            "[skip ci]",
            "--",
            relative_task.as_posix(),
        )
    except subprocess.CalledProcessError as exc:
        if not _is_nothing_to_commit(exc):
            raise
        if _head_commit_subject(repo_root) != commit_subject:
            raise _TaskNotRetryable
        replayed_existing_commit = True

    push_result = _run_retry_git(repo_root, "push", "origin", f"HEAD:{base_branch}")
    if replayed_existing_commit and "everything up-to-date" in _git_output(
        push_result
    ).lower():
        raise _TaskNotRetryable


def _reset_retry_worktree(repo_root: Path, base_branch: str) -> None:
    _run_retry_git(repo_root, "reset", "--hard", f"origin/{base_branch}")


async def _task_view(
    task: QueueTask,
    repo_name: str,
    redis_client: aioredis.Redis | None,
) -> dict[str, object]:
    retry_count = 0
    cancellation_subsource: str | None = None
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
        # PR-310: read payload.subsource so tasks_panel.html can split the
        # ERROR group into a guardrail subgroup (operator decision needed)
        # vs other (automatic failure). Best-effort: Redis errors leave the
        # task in the "other" bucket rather than 5xx-ing the panel.
        # PR-345 follow-up: ``refresh_ttl=False`` — this is an aggregate
        # display read across every ERROR task, not an explicit per-record
        # investigation, so it must not push records out to the 90-day
        # forensic ceiling on every panel render.
        try:
            cause = await get_cancellation_cause(
                redis_client, repo_name, task.pr_id, refresh_ttl=False
            )
        except Exception:
            cause = None
        if cause is not None and isinstance(cause.payload, dict):
            raw_subsource = cause.payload.get("subsource")
            if isinstance(raw_subsource, str) and raw_subsource:
                cancellation_subsource = raw_subsource
    return {
        **task.model_dump(mode="json"),
        "retry_count": retry_count,
        "cancellation_subsource": cancellation_subsource,
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


async def _render_repo_card(request: Request, name: str) -> HTMLResponse:
    redis_client = getattr(request.app.state, "redis", None)
    state = await _app.get_repo_state(name, redis_client, _app.CONFIG_PATH)
    if redis_client is not None:
        try:
            cfg = await asyncio.to_thread(load_config, _app.CONFIG_PATH)
            state.active_inhibitors = await derive_active_inhibitors(
                state, redis_client, cfg.daemon
            )
        except Exception:
            _app.logger.warning(
                "Failed to refresh inhibitors while rendering repo card for %s",
                name,
                exc_info=True,
            )
            state.active_inhibitors = []
    return _app.templates.TemplateResponse(
        request,
        "components/repo_cards.html",
        {
            "repos": [state],
            "redis_warning": None,
            "resources": {},
            "cancellation_subsources": {},
            "subsource_lookup": _app._subsource_lookup,
            "drain_progress": {},
            "inhibitor_labels": _app.INHIBITOR_LABELS,
            "card_only": True,
        },
    )


@router.post("/repos/{name}/quarantine/{pr_number}/release")
async def release_quarantine(
    name: str,
    pr_number: int,
    request: Request,
) -> JSONResponse:
    """Operator-triggered release of a quarantined PR."""
    cfg = load_config(_app.CONFIG_PATH)
    repo_config = _find_repo_config_by_name(cfg, name)
    if repo_config is None:
        return JSONResponse({"error": "repo not found"}, status_code=404)
    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is None:
        return JSONResponse({"error": "redis unavailable"}, status_code=503)

    state_key = pipeline_state(name)
    try:
        raw_state = await redis_client.get(state_key)
    except RedisError:
        return JSONResponse({"error": "redis unavailable"}, status_code=503)
    if raw_state is None:
        return JSONResponse({"status": "not_quarantined", "pr": pr_number})
    try:
        state = RepoState.model_validate_json(raw_state)
    except Exception:
        return JSONResponse({"error": "repository state unavailable"}, status_code=503)
    if pr_number not in state.quarantined_prs:
        return JSONResponse({"status": "not_quarantined", "pr": pr_number})

    try:
        owner_repo = gh_runner.get_repo_full_name(repo_config.url)
    except ValueError:
        owner_repo = None
    if owner_repo is not None:
        try:
            labels = _split_gh_label_output(
                gh_runner.run_gh(
                    [
                        "pr",
                        "view",
                        str(pr_number),
                        "--json",
                        "labels",
                        "-q",
                        ".labels[].name",
                    ],
                    repo=owner_repo,
                )
            )
            for label in labels:
                if label.startswith("quarantine:"):
                    gh_runner.run_gh(
                        [
                            "pr",
                            "edit",
                            str(pr_number),
                            "--remove-label",
                            label,
                        ],
                        repo=owner_repo,
                    )
        except Exception as exc:
            _app.logger.warning(
                "Failed to remove quarantine labels for %s PR #%s: %s",
                name,
                pr_number,
                exc,
            )

    event_message = f"Quarantine released for PR #{pr_number}."

    async def _transaction(pipe: Any) -> RepoState | None:
        raw = await pipe.get(state_key)
        if raw is None:
            return None
        current = RepoState.model_validate_json(raw)
        if pr_number not in current.quarantined_prs:
            return current
        current.quarantined_prs.discard(pr_number)
        _append_history_entry(current, event_message)
        pipe.multi()
        pipe.set(state_key, current.model_dump_json())
        return current

    try:
        updated_state = await redis_client.transaction(
            _transaction,
            state_key,
            value_from_callable=True,
        )
    except Exception:
        return JSONResponse({"error": "failed to update state"}, status_code=503)

    write_operator_action_audit(
        action="quarantine_release",
        repo=name,
        pr=pr_number,
        operator_session_id=request.headers.get("X-Session-Id", "unknown"),
    )
    if updated_state is not None:
        await _publish_history_entry_event(
            name,
            updated_state,
            event_message,
            redis_client,
        )
    return JSONResponse({"status": "released", "pr": pr_number})


@router.post("/repos/{name}/inhibitors/clear/{inhibitor_type}")
async def clear_inhibitor(
    request: Request, name: str, inhibitor_type: str
) -> Response:
    if inhibitor_type not in _OPERATOR_CLEARABLE_INHIBITORS:
        return HTMLResponse("Inhibitor not operator-clearable", status_code=400)

    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is None:
        return HTMLResponse("Redis not configured", status_code=503)

    try:
        redis_client, _state_key, _repo_url = await _apply_repo_control_update(
            request, name
        )
    except _RepoStateMutationError as exc:
        return HTMLResponse(exc.message, status_code=exc.status_code)

    if inhibitor_type == "user_pause":
        result = await _update_repo_pause_state(
            request,
            name,
            user_paused=False,
            event_message="Operator pause cleared.",
        )
        if isinstance(result, Response):
            return result
    else:
        try:
            await redis_client.delete(control_stop(name))
        except Exception:
            return HTMLResponse("Failed to clear stop request", status_code=503)

    try:
        await _app.publish_wake(redis_client, name, "inhibitor_cleared")
    except Exception:
        _app.logger.warning(
            "publish_wake failed for %s; daemon will pick up inhibitor clear on next tick",
            name,
            exc_info=True,
        )

    return await _render_repo_card(request, name)


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
    snapshot_tasks, _snapshot_at = await _load_current_queue_snapshot(name)
    snapshot_status = _task_status_from_snapshot(snapshot_tasks, pr_id)
    if not _is_retryable_task_status(current_status, snapshot_status):
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
        except subprocess.CalledProcessError as exc:
            if _is_missing_task_pathspec(exc):
                return HTMLResponse("Task file not found", status_code=404)
            return HTMLResponse("Failed to commit retry change", status_code=503)
        except subprocess.TimeoutExpired:
            return HTMLResponse("Failed to commit retry change", status_code=503)
        if not task_path.is_file():
            return HTMLResponse("Task file not found", status_code=404)

        try:
            current_status = _read_task_frontmatter_status(task_path)
            retry_fingerprint = _task_retry_fingerprint(task_path)
        except (OSError, UnicodeError):
            return HTMLResponse("Failed to read task status", status_code=503)
        if not _is_retryable_task_status(current_status, snapshot_status):
            return HTMLResponse("Task is not in ERROR", status_code=409)
        rewrote_status = current_status != TaskStatus.TODO

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
            if current_status != TaskStatus.TODO:
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
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            reset_failed = False
            try:
                await asyncio.to_thread(
                    _reset_retry_worktree,
                    repo_root,
                    repo_config.branch,
                )
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                reset_failed = True
            if rewrote_status and reset_failed:
                _restore_retry_error_status(task_path)
            if retry_reserved:
                await _release_retry_reservation(redis_client, name, pr_id)
            return HTMLResponse("Failed to commit retry change", status_code=503)

        try:
            await delete_cancellation_cause(redis_client, name, pr_id)
        except Exception:
            pass
        try:
            await redis_client.delete(_diagnose_exhausted_key(name, pr_id))
        except Exception:
            pass
        await _clear_status_write_failed_retry_marker(redis_client, name, pr_id)
        try:
            await _app.publish_wake(redis_client, name, "retry")
        except Exception:
            _app.logger.warning(
                "publish_wake failed for %s; daemon will pick up retry on next tick",
                name,
                exc_info=True,
            )

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


def _reset_status_write_failed_retry_key(repo_slug: str, task_id: str) -> str:
    return f"metrics:status_write_failed_retry:{repo_slug}:{task_id}"


def _reset_keys_for_task(repo_slug: str, task_id: str) -> list[str]:
    return [
        cause_key(repo_slug, task_id),
        _retry_count_key(repo_slug, task_id),
        _retry_fingerprint_key(repo_slug, task_id),
        current_run_started_at_key(repo_slug, task_id),
        _reset_status_write_failed_retry_key(repo_slug, task_id),
    ]


def _reset_stuck_state_keys(repo_slug: str, task_id: str) -> list[str]:
    # Retry-history keys (metrics:retry_count, metrics:retry_fingerprint) live
    # for 30 days for any task that was ever retried, including DONE tasks.
    # Treating them as "stuck" state would let reset re-queue completed work
    # whose only Redis footprint is normal retry history. Reset must only
    # authorize on per-task state the daemon actively keeps to indicate a
    # task is stuck (cancellation cause, in-flight run marker, parked-task
    # fallback marker). Retry-history keys are still cleared opportunistically
    # in _reset_keys_for_task.
    return [
        cause_key(repo_slug, task_id),
        current_run_started_at_key(repo_slug, task_id),
        _reset_status_write_failed_retry_key(repo_slug, task_id),
    ]


async def _reset_has_any_redis_state(
    redis_client: aioredis.Redis,
    repo_slug: str,
    task_id: str,
) -> bool:
    for key in _reset_stuck_state_keys(repo_slug, task_id):
        if await redis_client.get(key) is not None:
            return True
    score = await redis_client.zscore(index_key(repo_slug), task_id)
    if score is not None:
        return True
    return await _reset_has_status_write_failed_marker(
        redis_client, repo_slug, task_id
    )


async def _reset_has_status_write_failed_marker(
    redis_client: aioredis.Redis,
    repo_slug: str,
    task_id: str,
) -> bool:
    # Daemon can park a task via status_write_failed_tasks:{repo} or the
    # legacy recovered_tasks:{repo} set alone — the per-task fallback keys
    # may be absent. If the eligibility probe ignored that, reset would
    # exit early with 400 and never clear the marker, leaving the task
    # forced back to ERROR on the next dispatch.
    for key in (
        status_write_failed_tasks(repo_slug),
        legacy_recovered_tasks(repo_slug),
    ):
        raw = await redis_client.get(key)
        try:
            decoded = _decode_redis_text(raw)
        except UnicodeDecodeError:
            # Corrupt non-UTF-8 bytes mirror the invalid-JSON path:
            # treat as absent so reset stays usable for recovery.
            continue
        if decoded is None:
            continue
        try:
            task_ids = json.loads(decoded)
        except (TypeError, ValueError):
            continue
        if not isinstance(task_ids, list):
            continue
        if any(str(item) == task_id for item in task_ids):
            return True
    return False


async def _capture_reset_diagnostic_snapshot(
    pipe: aioredis.client.Pipeline,
    repo_slug: str,
    task_id: str,
) -> dict[str, Any]:
    """Snapshot pre-destruction retry_count and cancellation_cause.

    Read on the pipeline that holds the WATCH for the destructive
    DELETE, after ``pipe.watch(...)`` and before ``pipe.multi()``. Both
    keys (``metrics:retry_count``, ``cancellation:``) are already part
    of ``_reset_keys_for_task`` and therefore watched, so a concurrent
    writer that mutates either between this read and EXEC aborts the
    transaction (``WatchError``). The caller surfaces 409 and the audit
    record is skipped rather than logging stale forensic data that
    misrepresents the state actually cleared.
    """
    raw_retry = await pipe.get(_retry_count_key(repo_slug, task_id))
    retry_count = _decode_retry_count(raw_retry)

    raw_cause = await pipe.get(cause_key(repo_slug, task_id))
    cause_dump: dict[str, Any] = {}
    if raw_cause is not None:
        try:
            cause = CancellationCause.from_redis(raw_cause)
        except (ValueError, TypeError, UnicodeDecodeError):
            cause = None
        if cause is not None:
            cause_dump = {
                "category": cause.category,
                "payload": (
                    cause.payload if isinstance(cause.payload, dict) else {}
                ),
            }
    return {
        "retry_count": retry_count,
        "cancellation_cause": cause_dump,
    }


async def _reset_close_orphan_pr(
    name: str,
    task_id: str,
    repo_config: Any,
    redis_client: aioredis.Redis,
) -> int | None:
    """Close the active GitHub PR for ``task_id`` if it is still open.

    Mirrors the orphan detection used by ``/api/diagnostic``: resolves
    against ``RepoState.current_pr`` (the single canonical pointer the
    daemon maintains) and only closes if ``gh pr view`` reports ``OPEN``.
    Best-effort: any lookup failure returns ``None`` so the reset still
    completes.
    """
    try:
        raw_state = await redis_client.get(pipeline_state(name))
    except RedisError:
        return None
    if raw_state is None:
        return None
    try:
        state = RepoState.model_validate_json(raw_state)
    except Exception:
        return None
    if state.current_task is None or state.current_pr is None:
        return None
    if state.current_task.pr_id != task_id:
        return None
    pr_number = state.current_pr.number
    try:
        owner_repo = gh_runner.get_repo_full_name(repo_config.url)
    except ValueError:
        return None
    info = await asyncio.to_thread(gh_prs.pr_state, owner_repo, pr_number)
    if info is None or info.get("state") != "OPEN":
        return None
    args = [
        "gh",
        "pr",
        "close",
        str(pr_number),
        "--repo",
        owner_repo,
        "--comment",
        "Closed by operator reset",
    ]
    try:
        rc, _output = await asyncio.to_thread(_gh_subprocess, args)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if rc != 0:
        return None
    return pr_number


def _reset_partial_response(
    deleted_keys: list[str],
    closed_pr_number: int | None,
    *,
    error: str,
) -> JSONResponse:
    return JSONResponse(
        {
            "error": error,
            "deleted_keys": deleted_keys,
            "closed_pr_number": closed_pr_number,
            "frontmatter_pushed": False,
            "partial_reset": True,
        },
        status_code=503,
    )


@router.post("/api/reset-task/{name}/{task_id}")
async def reset_task(
    request: Request,
    name: str,
    task_id: str,
    close_orphan_pr: bool = Query(False),
) -> JSONResponse:
    """Atomic destructive reset for stuck tasks.

    Drops every per-task Redis key that retry leaves in place, removes
    the task from the cancellation index, optionally closes a stale
    open PR, and rewrites the task frontmatter to ``TODO`` so the daemon
    re-dispatches. The destructive git operations execute under the same
    repo-level reservation used by retry, so daemon/coder activity cannot
    race the worktree mutations. Returns 409 if the repo is already busy
    or if a concurrent writer touches one of the watched keys between
    read and execute. Returns 400 if there is nothing to reset
    (frontmatter at a non-stuck status — ``TODO``, ``DONE``, or missing
    entirely (treated as ``TODO``) — and Redis state empty). Returns 503 with
    a ``partial_reset`` flag if Redis succeeds but the subsequent git
    push fails — Redis state is gone but frontmatter has not been pushed.
    """
    if not _TASK_PR_ID_PATTERN.match(task_id):
        return JSONResponse({"error": "invalid task id"}, status_code=400)

    cfg = load_config(_app.CONFIG_PATH)
    repo_config = _find_repo_config_by_name(cfg, name)
    if repo_config is None:
        return JSONResponse({"error": "repo not found"}, status_code=404)

    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is None:
        return JSONResponse({"error": "redis unavailable"}, status_code=503)

    resolved = await _resolve_repo_task_path(name, task_id)
    if resolved is None:
        return JSONResponse({"error": "task file not found"}, status_code=404)
    task_path, _task_filename = resolved
    repo_root = Path(_app.REPOS_DIR) / name
    try:
        relative_task = task_path.relative_to(repo_root)
    except ValueError:
        return JSONResponse({"error": "task file not found"}, status_code=404)

    keys_to_delete = _reset_keys_for_task(name, task_id)

    try:
        current_status = _read_task_frontmatter_status(task_path)
    except (OSError, UnicodeError):
        return JSONResponse({"error": "failed to read task status"}, status_code=503)

    try:
        had_redis_state = await _reset_has_any_redis_state(
            redis_client, name, task_id
        )
    except RedisError:
        return JSONResponse({"error": "redis unavailable"}, status_code=503)

    # Missing frontmatter is treated as TODO elsewhere (see
    # _is_retryable_task_status), so a task with no frontmatter must not
    # bypass the "nothing to reset" guard and trigger a destructive
    # checkout/write/push when Redis carries no stuck-state markers.
    effective_status = (
        current_status if current_status is not None else TaskStatus.TODO
    )
    if effective_status in _RESET_NON_STUCK_STATUSES and not had_redis_state:
        status_label = (
            current_status.value
            if current_status is not None
            else "TODO (no frontmatter)"
        )
        return JSONResponse(
            {
                "error": (
                    f"task is {status_label} with no stuck state, "
                    "nothing to reset"
                )
            },
            status_code=400,
        )

    try:
        previous_user_paused = await _reserve_repo_for_retry(
            redis_client,
            name,
            repo_config.url,
        )
    except _RepoStateMutationError as exc:
        return JSONResponse({"error": exc.message}, status_code=exc.status_code)
    except RedisError:
        return JSONResponse({"error": "redis unavailable"}, status_code=503)

    diagnostic_snapshot: dict[str, Any] = {}
    try:
        cancellation_index = index_key(name)
        try:
            async with redis_client.pipeline(transaction=True) as pipe:
                await pipe.watch(*keys_to_delete, cancellation_index)
                # PR-336 follow-up: snapshot retry_count and cancellation
                # cause inside the same watched window as the destructive
                # DELETE. Both keys are watched, so any concurrent mutation
                # between this read and EXEC aborts the transaction and the
                # audit record is skipped — preventing stale
                # retry_count_at_reset/subsource_at_reset values from being
                # logged as the state that was cleared.
                diagnostic_snapshot = await _capture_reset_diagnostic_snapshot(
                    pipe, name, task_id
                )
                pipe.multi()
                for key in keys_to_delete:
                    pipe.delete(key)
                pipe.zrem(cancellation_index, task_id)
                try:
                    await pipe.execute()
                except aioredis.WatchError:
                    return JSONResponse(
                        {"error": "concurrent_modification"},
                        status_code=409,
                    )
        except RedisError:
            return JSONResponse({"error": "redis unavailable"}, status_code=503)

        # PR-334: scheduler gates on the persisted status-write-failed set
        # (status_write_failed_tasks:{repo} / recovered_tasks:{repo}); the
        # per-task fallback marker dropped above is never read. Clear the
        # persisted set entry so a reset task can be dispatched again. The
        # helper swallows Redis errors; treat marker cleanup as best-effort
        # in line with retry's behavior.
        await _clear_status_write_failed_retry_marker(redis_client, name, task_id)

        closed_pr_number: int | None = None

        try:
            await asyncio.to_thread(
                _checkout_retry_base_task,
                repo_root,
                repo_config.branch,
                relative_task,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            return _reset_partial_response(
                keys_to_delete,
                closed_pr_number,
                error="Redis cleared, frontmatter NOT pushed (checkout failed)",
            )

        wrote_frontmatter = False
        try:
            post_checkout_status = _read_task_frontmatter_status(task_path)
            if post_checkout_status != TaskStatus.TODO:
                write_frontmatter_status(task_path, "TODO")
                wrote_frontmatter = True
        except (OSError, ValueError, UnicodeError):
            return _reset_partial_response(
                keys_to_delete,
                closed_pr_number,
                error="Redis cleared, frontmatter NOT pushed (write failed)",
            )

        # PR-334: when checkout leaves the task already at TODO and we did
        # not rewrite frontmatter, there is nothing to commit. Skip the
        # commit/push instead of letting "_commit_and_push_retry_reset"
        # raise _TaskNotRetryable on "nothing to commit" — Redis is
        # already clean, so this is a successful reset, not a partial.
        if wrote_frontmatter:
            commit_subject = f"[RESET] {task_id} cleared by operator"
            try:
                await asyncio.to_thread(
                    _commit_and_push_retry_reset,
                    repo_root,
                    relative_task,
                    commit_subject,
                    repo_config.branch,
                )
            except (
                _TaskNotRetryable,
                subprocess.CalledProcessError,
                subprocess.TimeoutExpired,
            ):
                try:
                    await asyncio.to_thread(
                        _reset_retry_worktree, repo_root, repo_config.branch
                    )
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                    pass
                return _reset_partial_response(
                    keys_to_delete,
                    closed_pr_number,
                    error="Redis cleared, frontmatter NOT pushed (push failed)",
                )

        # PR-334: close the orphan PR only after the destructive git path has
        # succeeded. Closing earlier means a later checkout/write/push failure
        # surfaces 503 partial_reset with the PR already closed and no
        # rollback available — operators are left with frontmatter still
        # pointing at the stuck state and an unexpectedly closed PR.
        if close_orphan_pr:
            closed_pr_number = await _reset_close_orphan_pr(
                name, task_id, repo_config, redis_client
            )

        try:
            await _app.publish_wake(redis_client, name, "reset")
        except Exception:
            _app.logger.warning(
                "publish_wake failed for %s; daemon will pick up reset on next tick",
                name,
                exc_info=True,
            )

        subsource_at_reset = (
            diagnostic_snapshot.get("cancellation_cause", {})
            .get("payload", {})
            .get("subsource")
        )
        write_audit_record(
            action="reset_task",
            repo_slug=name,
            task_id=task_id,
            payload={
                "deleted_keys": keys_to_delete,
                "closed_pr_number": closed_pr_number,
                # Reflect the actual git outcome: when checkout already
                # left the task at TODO we skip the commit/push, so the
                # audit log must not claim a push happened.
                "frontmatter_pushed": wrote_frontmatter,
                "retry_count_at_reset": diagnostic_snapshot.get("retry_count"),
                "subsource_at_reset": subsource_at_reset,
            },
        )

        return JSONResponse(
            {
                "deleted_keys": keys_to_delete,
                "closed_pr_number": closed_pr_number,
                "frontmatter_pushed": True,
            }
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


@router.get(
    "/api/repo/{name}/guardrail/pending",
    response_class=JSONResponse,
)
async def get_repo_guardrail_pending(request: Request, name: str) -> Response:
    """List pending guardrail decisions for a repo, oldest first.

    Returns a JSON object with shape::

        {
            "pending": [
                {
                    "pr_id": "PR-296",
                    "rule": "large_diff_threshold",
                    "excerpt": "+1800 LOC across 35 files",
                    "recorded_at": 1746789012
                },
                ...
            ]
        }

    The list is sorted ascending by ``recorded_at`` so operators triage
    in arrival order. Bounded by the underlying helper's ``limit`` (100).

    Returns 404 if the repo name does not match any configured repo.
    """
    cfg = load_config(_app.CONFIG_PATH)
    if _find_repo_config_by_name(cfg, name) is None:
        return JSONResponse({"error": "repo not found"}, status_code=404)
    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is None:
        return JSONResponse({"error": "redis unavailable"}, status_code=503)
    try:
        pending = await list_pending_guardrail_decisions(redis_client, name)
    except RedisError:
        return JSONResponse({"error": "redis unavailable"}, status_code=503)
    payload = {
        "pending": [
            {
                "pr_id": entry.task_id,
                "rule": entry.rule,
                "excerpt": entry.excerpt,
                "recorded_at": entry.recorded_at,
            }
            for entry in pending
        ]
    }
    return JSONResponse(payload)


_NO_PENDING_GUARDRAIL = "PR has no pending guardrail decision"


def _gh_subprocess(args: list[str]) -> tuple[int, str]:
    result = subprocess.run(
        args,
        capture_output=True,
        text=True,
        timeout=_RETRY_GIT_TIMEOUT_SECONDS,
        check=False,
    )
    return result.returncode, _git_output(result)


_TASK_BRANCH_HEADER_RE = re.compile(r"^Branch\s*:\s*(.+?)\s*$")
_TASK_BODY_HEADING_RE = re.compile(r"^#{2,}\s")


def _read_task_branch(task_path: Path) -> str | None:
    """Return the ``Branch:`` header value declared in a task file.

    The canonical metadata section lives in the preamble between the
    YAML frontmatter and the first ``## `` (or deeper) heading. Scanning
    is bounded to that preamble so a ``Branch:``-like line in the body
    (code blocks, prose, examples) cannot drive a ``gh pr close`` against
    an unrelated PR.
    """
    try:
        text = task_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None
    for raw_line in text.splitlines():
        stripped = raw_line.rstrip()
        if _TASK_BODY_HEADING_RE.match(stripped):
            return None
        match = _TASK_BRANCH_HEADER_RE.match(stripped)
        if match:
            branch = match.group(1).strip()
            return branch or None
    return None


def _gh_lookup_pr_number_by_branch(
    branch: str, owner_repo: str | None = None
) -> int | None:
    """Return the unique open PR number for ``branch`` in ``owner_repo``.

    ``gh pr list --head <branch>`` does not accept the ``owner:branch``
    disambiguator (https://cli.github.com/manual/gh_pr_list), so a branch
    name that collides with one in a fork can match multiple PRs and a
    naive first-match would let ``_reject_guardrail_decision`` close the
    wrong PR. Daemon-pushed PRs always have their head in the configured
    base repo, so filter ``headRepositoryOwner.login`` to the base
    owner and require exactly one match — zero or multiple matches yield
    ``None`` so the caller skips the destructive ``gh pr close``.
    """
    if not owner_repo:
        return None
    base_owner, sep, _ = owner_repo.partition("/")
    if not sep or not base_owner:
        return None
    args = [
        "gh", "pr", "list",
        "--head", branch,
        "--state", "open",
        "--repo", owner_repo,
        "--json", "number,headRefName,headRepositoryOwner",
    ]
    try:
        rc, output = _gh_subprocess(args)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if rc != 0:
        return None
    try:
        payload = json.loads(output or "[]")
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, list):
        return None
    matches: list[int] = []
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        number = entry.get("number")
        if not isinstance(number, int):
            continue
        head_ref = entry.get("headRefName")
        if isinstance(head_ref, str) and head_ref != branch:
            continue
        owner_obj = entry.get("headRepositoryOwner")
        owner_login = (
            owner_obj.get("login") if isinstance(owner_obj, dict) else None
        )
        if owner_login != base_owner:
            continue
        matches.append(number)
    if len(matches) == 1:
        return matches[0]
    return None


def _checkout_guardrail_approve_base(
    repo_root: Path, base_branch: str
) -> None:
    # The daemon worktree is typically on the task/PR branch; without this
    # reset the subsequent push of HEAD would fast-forward base_branch with
    # PR commits (or fail under branch protection).
    _run_retry_git(repo_root, "fetch", "origin", base_branch)
    _run_retry_git(repo_root, "checkout", "-f", base_branch)
    _run_retry_git(repo_root, "reset", "--hard", f"origin/{base_branch}")


def _commit_guardrail_approve(
    repo_root: Path, relative_task: Path, commit_subject: str, base_branch: str
) -> None:
    _run_retry_git(repo_root, "add", relative_task.as_posix())
    try:
        _run_retry_git(
            repo_root, "commit", "-m", commit_subject, "-m", "[skip ci]",
            "--", relative_task.as_posix(),
        )
    except subprocess.CalledProcessError as exc:
        if not _is_nothing_to_commit(exc):
            raise
    _run_retry_git(repo_root, "push", "origin", f"HEAD:{base_branch}")


def _validated_guardrail_cause(raw: object) -> CancellationCause | None:
    if raw is None:
        return None
    try:
        cause = CancellationCause.from_redis(raw)  # type: ignore[arg-type]
    except Exception:
        return None
    payload = cause.payload if isinstance(cause.payload, dict) else None
    if payload is None or payload.get("subsource") != "guardrail":
        return None
    return cause


_GUARDRAIL_REASON_RE = re.compile(r"^GUARDRAIL:\s*([^:]+):\s*(.+)$")


def _extract_guardrail_metadata(payload: dict[str, Any]) -> tuple[str, str]:
    """Return ``(rule, excerpt)`` from a guardrail cause payload.

    The daemon writes guardrail causes in two shapes: watch.py emits
    structured ``category``/``excerpt`` fields while coding.py and fix.py
    emit only ``reason_text="GUARDRAIL: {category}: {excerpt}"``. The
    operator_reject record must preserve identifying signal regardless of
    which shape produced the original cause, so fall back through
    ``rule`` -> ``category`` -> parsed ``reason_text``.
    """
    rule = payload.get("rule") or payload.get("category") or ""
    excerpt = payload.get("excerpt", "") or ""
    if rule and excerpt:
        return rule, excerpt
    reason = payload.get("reason_text", "")
    if isinstance(reason, str):
        match = _GUARDRAIL_REASON_RE.match(reason)
        if match:
            if not rule:
                rule = match.group(1).strip()
            if not excerpt:
                excerpt = match.group(2).strip()
    return rule, excerpt


async def _gh_best_effort(
    name: str, pr_number: int, what: str, args: list[str]
) -> None:
    try:
        rc, output = await asyncio.to_thread(_gh_subprocess, args)
        if rc != 0:
            _app.logger.warning(
                "Best-effort %s failed for %s PR #%s: %s",
                what, name, pr_number, output.strip(),
            )
    except (OSError, subprocess.TimeoutExpired) as exc:
        _app.logger.warning(
            "Best-effort %s raised for %s PR #%s: %s",
            what, name, pr_number, exc,
        )


async def _approve_guardrail_decision(
    name: str, pr_id: str, repo_config: Any, redis_client: aioredis.Redis
) -> Response:
    # Wrap initial Redis reads so a transient outage surfaces as 503
    # instead of an uncaught 500 — the dashboard relies on deterministic
    # error codes to keep the operator's decision handle usable.
    try:
        raw_cause = await redis_client.get(cause_key(name, pr_id))
    except RedisError:
        return HTMLResponse("Redis unavailable", status_code=503)
    if _validated_guardrail_cause(raw_cause) is None:
        return HTMLResponse(_NO_PENDING_GUARDRAIL, status_code=404)

    state_key = pipeline_state(name)
    try:
        raw_state = await redis_client.get(state_key)
    except RedisError:
        return HTMLResponse("Redis unavailable", status_code=503)
    state: RepoState | None = None
    if raw_state is not None:
        try:
            state = RepoState.model_validate_json(raw_state)
        except Exception:
            state = None
    if (
        state is None
        or state.current_task is None
        or state.current_task.pr_id != pr_id
        or state.current_pr is None
    ):
        return HTMLResponse(
            "Open PR not active in daemon state; reject and re-upload spec instead",
            status_code=409,
        )
    pr_number = state.current_pr.number
    try:
        owner_repo: str | None = gh_runner.get_repo_full_name(repo_config.url)
    except ValueError:
        owner_repo = None

    resolved = await _resolve_repo_task_path(name, pr_id)
    if resolved is None:
        return HTMLResponse("Task file not found", status_code=404)
    task_path, task_filename = resolved
    repo_root = Path(_app.REPOS_DIR) / name
    relative_task = Path(task_filename)

    # CAS-claim the decision BEFORE any side effects. The pipeline below
    # atomically (a) re-validates the cause is still ``subsource=guardrail``
    # and (b) deletes the cause + index entry. If a concurrent reject (or
    # another approve) modifies the cause between our initial read and the
    # EXEC, the WatchError surfaces here and no side effects run. The
    # reverse order — side effects first, CAS last — is split-brain prone:
    # a concurrent reject could land while we are pushing ``status: TODO``
    # to main, then our CAS would fail with 409 but the frontmatter commit
    # has already escaped, re-queueing work that was explicitly rejected.
    original_cause: CancellationCause | None = None
    try:
        async with redis_client.pipeline(transaction=True) as pipe:
            await pipe.watch(cause_key(name, pr_id))
            watched_raw = await pipe.get(cause_key(name, pr_id))
            watched_cause = _validated_guardrail_cause(watched_raw)
            if watched_cause is None:
                await pipe.unwatch()
                return HTMLResponse(_NO_PENDING_GUARDRAIL, status_code=404)
            original_cause = watched_cause
            pipe.multi()
            pipe.delete(cause_key(name, pr_id))
            pipe.zrem(index_key(name), pr_id)
            try:
                await pipe.execute()
            except aioredis.WatchError:
                return HTMLResponse(
                    "Concurrent state change detected; please retry the decision",
                    status_code=409,
                )
    except RedisError:
        return HTMLResponse("Redis unavailable", status_code=503)

    async def _restore_cause() -> None:
        # Best-effort rollback so the operator does not lose the decision
        # handle when a transient git/frontmatter failure aborts approve
        # after the CAS-delete. If the restore itself fails, log and let
        # the side-effect error surface — the operator can fall back to
        # editing Redis by hand or to the per-task Retry button.
        try:
            await record_cancellation_cause(
                redis_client, name, pr_id, original_cause
            )
        except Exception:
            _app.logger.warning(
                "Failed to restore guardrail cause for %s %s after"
                " side-effect failure",
                name, pr_id, exc_info=True,
            )

    if owner_repo is not None:
        await _gh_best_effort(
            name, pr_number, "escalated-label removal",
            ["gh", "api", "-X", "DELETE",
             f"repos/{owner_repo}/issues/{pr_number}/labels/escalated"],
        )

    try:
        await asyncio.to_thread(
            _checkout_guardrail_approve_base,
            repo_root, repo_config.branch,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        _app.logger.warning(
            "Failed to checkout base branch for %s %s: %s",
            name, pr_id, exc,
        )
        await _restore_cause()
        return HTMLResponse(
            "Failed to commit guardrail decision", status_code=503
        )

    try:
        await asyncio.to_thread(write_frontmatter_status, task_path, "TODO")
    except (OSError, ValueError) as exc:
        await _restore_cause()
        return HTMLResponse(
            f"Failed to update task status: {exc}", status_code=503
        )

    commit_subject = (
        f"chore(tasks): guardrail decision approve for {pr_id} [skip ci]"
    )
    try:
        await asyncio.to_thread(
            _commit_guardrail_approve,
            repo_root, relative_task, commit_subject, repo_config.branch,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        _app.logger.warning(
            "Failed to commit guardrail approve for %s %s: %s",
            name, pr_id, exc,
        )
        # Mirror the retry endpoint: a failed commit/push can leave the
        # worktree with staged frontmatter changes or a local commit
        # ahead of origin. Hard-reset back to origin/{base_branch} so the
        # checkout is clean for the next daemon git operation.
        try:
            await asyncio.to_thread(
                _reset_retry_worktree, repo_root, repo_config.branch,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            _app.logger.warning(
                "Failed to reset worktree after guardrail approve"
                " commit failure for %s %s",
                name, pr_id, exc_info=True,
            )
        await _restore_cause()
        return HTMLResponse(
            "Failed to commit guardrail decision", status_code=503
        )

    async def _transition(pipe: Any) -> None:
        raw = await pipe.get(state_key)
        if raw is None:
            return
        try:
            current = RepoState.model_validate_json(raw)
        except Exception:
            return
        if current.current_task is None or current.current_task.pr_id != pr_id:
            return
        current.state = PipelineState.WATCH
        pipe.multi()
        pipe.set(state_key, current.model_dump_json())

    try:
        await redis_client.transaction(_transition, state_key)
    except Exception:
        _app.logger.warning(
            "Failed to transition %s to WATCH after guardrail approve",
            name, exc_info=True,
        )
    try:
        await _app.publish_wake(redis_client, name, "guardrail_decision")
    except Exception:
        _app.logger.warning(
            "publish_wake failed for %s; daemon will pick up next tick",
            name, exc_info=True,
        )
    return HTMLResponse("", status_code=204)


async def _reject_guardrail_decision(
    name: str, pr_id: str, repo_config: Any, redis_client: aioredis.Redis
) -> Response:
    try:
        raw_cause = await redis_client.get(cause_key(name, pr_id))
    except RedisError:
        return HTMLResponse("Redis unavailable", status_code=503)
    cause = _validated_guardrail_cause(raw_cause)
    if cause is None:
        return HTMLResponse(_NO_PENDING_GUARDRAIL, status_code=404)

    try:
        owner_repo: str | None = gh_runner.get_repo_full_name(repo_config.url)
    except ValueError:
        owner_repo = None

    pr_number: int | None = None
    state: RepoState | None = None
    try:
        raw_state = await redis_client.get(pipeline_state(name))
    except RedisError:
        return HTMLResponse("Redis unavailable", status_code=503)
    if raw_state is not None:
        try:
            state = RepoState.model_validate_json(raw_state)
        except Exception:
            state = None
    if (
        state is not None
        and state.current_task is not None
        and state.current_task.pr_id == pr_id
        and state.current_pr is not None
    ):
        pr_number = state.current_pr.number
    if pr_number is None:
        head_branch: str | None = None
        if (
            state is not None
            and state.current_task is not None
            and state.current_task.pr_id == pr_id
        ):
            head_branch = state.current_task.branch or None
        if head_branch is None:
            task_resolved = await _resolve_repo_task_path(name, pr_id)
            if task_resolved is not None:
                head_branch = await asyncio.to_thread(
                    _read_task_branch, task_resolved[0]
                )
        if head_branch and owner_repo is not None:
            pr_number = await asyncio.to_thread(
                _gh_lookup_pr_number_by_branch, head_branch, owner_repo
            )

    original_rule, original_excerpt = _extract_guardrail_metadata(cause.payload)
    new_cause = CancellationCause(
        category="ERROR",
        payload={
            "subsource": "operator_reject",
            "reason_text": (
                f"Operator rejected guardrail-flagged PR #{pr_number} via dashboard"
                if pr_number is not None
                else "Operator rejected guardrail-flagged PR via dashboard"
            ),
            "original_rule": original_rule,
            "original_excerpt": original_excerpt,
        },
        created_at=datetime.now(timezone.utc).isoformat(),
        task_id=pr_id,
        repo_slug=name,
    )
    serialized = new_cause.to_redis()
    score = datetime.fromisoformat(new_cause.created_at).timestamp()
    # CAS-guard the operator_reject write: a concurrent approve can
    # CAS-delete the guardrail cause between our initial read and this
    # write, and an unguarded ``set`` would resurrect a cancellation key
    # the approve flow just dropped — leaving the task in split-brain
    # state (frontmatter pushed to TODO by approve, but cause flipped to
    # operator_reject and PR closed by reject). WATCH the cause key and
    # re-validate it is still ``subsource=guardrail`` immediately before
    # writing; if it changed, surface 409 so the operator retries.
    try:
        async with redis_client.pipeline(transaction=True) as pipe:
            await pipe.watch(cause_key(name, pr_id))
            watched_raw = await pipe.get(cause_key(name, pr_id))
            if _validated_guardrail_cause(watched_raw) is None:
                await pipe.unwatch()
                return HTMLResponse(
                    "Concurrent state change detected; please retry the decision",
                    status_code=409,
                )
            pipe.multi()
            pipe.set(cause_key(name, pr_id), serialized, ex=TTL_SECONDS)
            pipe.zadd(index_key(name), {pr_id: score})
            pipe.expire(index_key(name), READ_REFRESH_TTL_SECONDS)
            try:
                await pipe.execute()
            except aioredis.WatchError:
                return HTMLResponse(
                    "Concurrent state change detected; please retry the decision",
                    status_code=409,
                )
    except RedisError:
        return HTMLResponse("Redis unavailable", status_code=503)

    # Best-effort liveness-based housekeeping shares semantics with
    # ``record_cancellation_cause`` and runs outside MULTI/EXEC because
    # EXISTS-driven liveness checks need readback values the transaction
    # queue cannot return. A RedisError here must NOT abort the reject
    # flow: the CAS write has already flipped the cause to
    # ``operator_reject``, so returning 503 would skip the PR close
    # side-effect, and a retry would see no pending guardrail decision —
    # leaving the rejected PR open with the operator's decision already
    # persisted.
    try:
        await prune_dead_index_members(redis_client, name)
    except RedisError:
        _app.logger.warning(
            "Failed to prune cancellation index for %s after reject CAS"
            " write succeeded; continuing with PR close",
            name, exc_info=True,
        )

    if pr_number is not None and owner_repo is not None:
        await _gh_best_effort(
            name, pr_number, "gh pr close",
            ["gh", "pr", "close", str(pr_number),
             "--repo", owner_repo,
             "--comment", "Guardrail violation rejected by operator"],
        )
    return HTMLResponse("", status_code=204)


@router.post(
    "/repos/{name}/guardrail/{pr_id}/decision",
    response_class=HTMLResponse,
)
async def post_repo_guardrail_decision(
    request: Request,
    name: str,
    pr_id: str,
    decision: str = Form(...),
) -> Response:
    """Operator-driven approve/reject of a guardrail-flagged PR."""
    if not _TASK_PR_ID_PATTERN.match(pr_id):
        return HTMLResponse("Invalid task identifier", status_code=400)
    if decision not in ("approve", "reject"):
        return HTMLResponse(
            f"Invalid decision: {decision!r}; expected 'approve' or 'reject'",
            status_code=400,
        )
    cfg = load_config(_app.CONFIG_PATH)
    repo_config = _find_repo_config_by_name(cfg, name)
    if repo_config is None:
        return HTMLResponse("Repository not found", status_code=404)
    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is None:
        return HTMLResponse("Redis unavailable", status_code=503)
    if decision == "approve":
        return await _approve_guardrail_decision(
            name, pr_id, repo_config, redis_client
        )
    return await _reject_guardrail_decision(
        name, pr_id, repo_config, redis_client
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
