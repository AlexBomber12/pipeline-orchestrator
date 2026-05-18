"""Diagnostic API endpoint exposing per-task Redis state for triage.

Read-only surface that collapses the 8+ Redis keys an operator currently
greps manually via ``redis-cli`` into one structured JSON document. The
endpoint has no side effects; consumers include PR-333's dashboard
diagnostic panel and ad-hoc operator curl usage. Authorization piggybacks
on whatever the dashboard middleware enforces — no new auth surface here.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import asdict
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from redis.exceptions import RedisError

from src.cancellation.storage import (
    cause_key,
    current_run_started_at_key,
    get_cancellation_cause,
    retry_count_key,
)
from src.config import load_config
from src.github import gh_runner
from src.github import prs as gh_prs
from src.keyspace import pipeline_state, status_write_failed_tasks
from src.models import RepoState
from src.subsource_registry import lookup as lookup_subsource
from src.web.services.repo_state import _find_repo_config_by_name

router = APIRouter()

_TASK_PR_ID_PATTERN = re.compile(r"^PR-[A-Za-z0-9_.-]+$")
_TASK_BRANCH_HEADER_RE = re.compile(r"^Branch\s*:\s*(.+?)\s*$")
_TASK_BODY_HEADING_RE = re.compile(r"^#{2,}\s")
_STATUS_LINE_RE = re.compile(r"^status:\s*(.+?)\s*$")
_STATUS_KEY_RE = re.compile(r"^status:\s*")


def _retry_fingerprint_key(repo_slug: str, task_id: str) -> str:
    return f"metrics:retry_fingerprint:{repo_slug}:{task_id}"


def _attempt_count_key(repo_slug: str, task_id: str) -> str:
    return f"metrics:attempt_count:{repo_slug}:{task_id}"


def _decode_text(raw: object) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, bytes):
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            # Diagnostic must not 500 when a Redis key holds non-UTF8 bytes;
            # the endpoint exists precisely for triage under inconsistent state.
            return None
    return str(raw)


def _decode_int(raw: object) -> int:
    text = _decode_text(raw)
    if text is None:
        return 0
    try:
        return max(0, int(text))
    except (TypeError, ValueError):
        return 0


def _resolve_task_path(repos_dir: str, name: str, task_id: str) -> Path | None:
    candidate = Path(repos_dir) / name / "tasks" / f"{task_id}.md"
    if not candidate.is_file():
        return None
    return candidate


def _read_frontmatter_status(task_path: Path) -> str | None:
    try:
        text = task_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None
    lines = text.splitlines()
    first = next((i for i, line in enumerate(lines) if line.strip()), None)
    if first is None or lines[first].rstrip() != "---":
        return None
    for raw_line in lines[first + 1:]:
        stripped = raw_line.rstrip()
        if stripped == "---":
            return None
        match = _STATUS_LINE_RE.match(stripped)
        if match is None:
            continue
        value = match.group(1).split("#", 1)[0].strip().strip("\"'")
        return value.upper() or None
    return None


def _current_spec_fingerprint(task_path: Path) -> str | None:
    try:
        lines = task_path.read_text(encoding="utf-8").splitlines(keepends=True)
    except (OSError, UnicodeDecodeError):
        return None
    first = next((i for i, raw in enumerate(lines) if raw.strip()), None)
    if first is None or lines[first].rstrip() != "---":
        normalized = lines
    else:
        normalized = []
        in_frontmatter = True
        for index, raw in enumerate(lines):
            if index > first and in_frontmatter and raw.rstrip() == "---":
                in_frontmatter = False
            if in_frontmatter and _STATUS_KEY_RE.match(raw.rstrip()):
                continue
            normalized.append(raw)
    return hashlib.sha256("".join(normalized).encode("utf-8")).hexdigest()


async def _maybe_ttl(redis_client: Any, key: str) -> int | None:
    try:
        ttl = await redis_client.ttl(key)
    except Exception:
        return None
    if isinstance(ttl, int) and ttl >= 0:
        return ttl
    return None


def _status_write_failed_for(raw: str | None, task_id: str) -> bool:
    if raw is None:
        return False
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return False
    if not isinstance(parsed, list):
        return False
    return any(str(item) == task_id for item in parsed)


def _extract_subsource(cause: Any) -> str | None:
    if cause is None or not isinstance(cause.payload, dict):
        return None
    raw = cause.payload.get("subsource")
    if isinstance(raw, str) and raw:
        return raw
    return None


async def _resolve_pr_for_task(
    repo_config: Any,
    task_id: str,
    state: RepoState | None,
) -> dict[str, Any] | None:
    """Look up the active PR's state for ``task_id`` via ``gh pr view``.

    Resolves only against ``RepoState.current_pr`` because that is the
    sole canonical pointer the daemon maintains for the running task; a
    branch-based gh search would race with operator edits to the task
    file and is out of scope for the read-only diagnostic surface.
    """
    if state is None or state.current_task is None or state.current_pr is None:
        return None
    if state.current_task.pr_id != task_id:
        return None
    pr_number = state.current_pr.number
    pr_url = state.current_pr.url
    try:
        owner_repo = gh_runner.get_repo_full_name(repo_config.url)
    except ValueError:
        return None
    info = await asyncio.to_thread(gh_prs.pr_state, owner_repo, pr_number)
    if info is None:
        return None
    return {
        "number": pr_number,
        "state": info.get("state"),
        "url": pr_url,
    }


async def _build_diagnostic_payload(
    name: str,
    task_id: str,
    request: Request,
) -> tuple[int, dict[str, Any]]:
    """Gather the diagnostic payload for ``task_id``.

    Returns ``(status_code, body)``. The JSON endpoint and the HTMX
    partial endpoint share this helper so a single source produces both
    the machine-readable and the rendered-template surfaces.
    """
    if not _TASK_PR_ID_PATTERN.match(task_id):
        return 400, {"error": "invalid task id"}

    cfg = load_config(_app.CONFIG_PATH)
    repo_config = _find_repo_config_by_name(cfg, name)
    if repo_config is None:
        return 404, {"error": "repo not found"}

    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is None:
        return 503, {"error": "redis unavailable"}

    try:
        raw_state = await redis_client.get(pipeline_state(name))
        try:
            cause = await get_cancellation_cause(redis_client, name, task_id)
        except (json.JSONDecodeError, TypeError, ValueError, UnicodeDecodeError):
            # Malformed/legacy JSON at cancellation:<repo>:<task> must not
            # abort the diagnostic payload — operators rely on the rest of
            # the triage fields precisely when Redis state is inconsistent.
            cause = None
        retry_count_raw = await redis_client.get(retry_count_key(name, task_id))
        retry_fp_raw = await redis_client.get(
            _retry_fingerprint_key(name, task_id)
        )
        started_raw = await redis_client.get(
            current_run_started_at_key(name, task_id)
        )
        attempt_raw = await redis_client.get(_attempt_count_key(name, task_id))
        status_write_raw = await redis_client.get(status_write_failed_tasks(name))
        ttls = {
            "cancellation_cause": await _maybe_ttl(
                redis_client, cause_key(name, task_id)
            ),
            "retry_count": await _maybe_ttl(
                redis_client, retry_count_key(name, task_id)
            ),
            "retry_fingerprint": await _maybe_ttl(
                redis_client, _retry_fingerprint_key(name, task_id)
            ),
            "current_run_started_at": await _maybe_ttl(
                redis_client, current_run_started_at_key(name, task_id)
            ),
            "attempt_count": await _maybe_ttl(
                redis_client, _attempt_count_key(name, task_id)
            ),
        }
    except RedisError:
        return 503, {"error": "redis unavailable"}

    state: RepoState | None = None
    if raw_state is not None:
        try:
            state = RepoState.model_validate_json(raw_state)
        except Exception:
            state = None

    retry_fp = _decode_text(retry_fp_raw)
    task_path = _resolve_task_path(_app.REPOS_DIR, name, task_id)
    if task_path is not None:
        frontmatter_status = _read_frontmatter_status(task_path)
        current_fp = _current_spec_fingerprint(task_path)
    else:
        frontmatter_status = None
        current_fp = None

    fingerprint_matches = (
        retry_fp is not None
        and current_fp is not None
        and retry_fp == current_fp
    )

    subsource = _extract_subsource(cause)
    meta = lookup_subsource(subsource) if subsource is not None else None

    payload = {
        "repo_slug": name,
        "task_id": task_id,
        "frontmatter_status": frontmatter_status,
        "cancellation_cause": asdict(cause) if cause is not None else None,
        "subsource_metadata": asdict(meta) if meta is not None else None,
        "retry_count": _decode_int(retry_count_raw),
        "retry_fingerprint": retry_fp,
        "retry_fingerprint_matches_current_spec": fingerprint_matches,
        "current_run_started_at": _decode_text(started_raw),
        "attempt_count": _decode_int(attempt_raw),
        "status_write_failed": _status_write_failed_for(
            _decode_text(status_write_raw), task_id
        ),
        "skip_ai_error_diagnose": (
            state.skip_ai_error_diagnose if state is not None else False
        ),
        "_error_diagnose_count": 0,
        "_error_skip_count": 0,
        "current_pr": await _resolve_pr_for_task(repo_config, task_id, state),
        "ttls": ttls,
    }
    return 200, payload


@router.get("/api/diagnostic/{name}/{task_id}")
async def diagnostic_state(
    name: str,
    task_id: str,
    request: Request,
) -> JSONResponse:
    """Return all per-task Redis state for ``task_id`` as one JSON document."""
    status, payload = await _build_diagnostic_payload(name, task_id, request)
    return JSONResponse(payload, status_code=status)


@router.get(
    "/repos/{name}/tasks/{task_id}/reset-confirm",
    response_class=HTMLResponse,
)
async def reset_confirm(
    name: str,
    task_id: str,
    request: Request,
) -> Response:
    """Render the reset confirmation modal (PR-335).

    Triggered by the diagnostic panel Reset button (PR-333). Reuses the
    diagnostic payload helper so the modal can list the exact keys the
    destructive reset endpoint (PR-334) will delete and flag an orphan
    PR when ``current_pr`` is still OPEN. The submit button is gated by
    a typed-confirmation input in the template; the endpoint itself is
    a pure read.
    """
    status, payload = await _build_diagnostic_payload(name, task_id, request)
    if status != 200:
        message = payload.get("error", "diagnostic unavailable")
        return HTMLResponse(
            f'<p class="text-xs italic text-fail">{message}</p>',
            status_code=status,
        )
    current_pr = payload.get("current_pr")
    orphan_pr = (
        current_pr
        if isinstance(current_pr, dict) and current_pr.get("state") == "OPEN"
        else None
    )
    return _app.templates.TemplateResponse(
        request,
        "components/reset_modal.html",
        {
            "diagnostic": payload,
            "orphan_pr": orphan_pr,
        },
    )


@router.get(
    "/partials/repo/{name}/tasks/{task_id}/diagnostic",
    response_class=HTMLResponse,
)
async def diagnostic_partial(
    name: str,
    task_id: str,
    request: Request,
) -> Response:
    """Render the diagnostic panel for ``task_id`` as an HTML fragment.

    Companion HTMX surface for the JSON endpoint. PR-334 shipped the
    destructive reset endpoint and PR-335 ships the typed-confirmation
    modal that gates it, so the reset button now renders by default;
    the macro still accepts the flag so callers (tests, future surfaces)
    can opt out without a template rewrite.
    """
    status, payload = await _build_diagnostic_payload(name, task_id, request)
    if status != 200:
        message = payload.get("error", "diagnostic unavailable")
        return HTMLResponse(
            f'<p class="text-xs italic text-fail">{message}</p>',
            status_code=status,
        )
    cfg = load_config(_app.CONFIG_PATH)
    return _app.templates.TemplateResponse(
        request,
        "components/diagnostic_partial.html",
        {
            "diagnostic": payload,
            "retry_cap": cfg.daemon.retry_button_cap,
            "reset_button_enabled": True,
        },
    )


# End-of-file import mirrors repo_control: the route decorator must run
# before app.py snapshots ``router.routes`` at include time, so importing
# the partially-initialized ``_app`` here avoids a circular import while
# still giving the handler access to module-level constants
# (``CONFIG_PATH``, ``REPOS_DIR``) at request time.
from src.web import app as _app  # noqa: E402
