"""Task file upload route.

Owns ``POST /repos/{name}/upload-tasks``: accepts task ``.md`` and ``.zip``
payloads from the dashboard, validates filenames and size, validates each
task header, stages files under ``/data/uploads/{repo}/{submission}/``,
records a Redis manifest, and publishes a wake event so the daemon
commits the new tasks on its next IDLE boundary.
"""

from __future__ import annotations

import asyncio
import io
import json
import re
import shutil
import tempfile
import uuid
import zipfile
import zlib
from pathlib import Path

from fastapi import APIRouter, Request, UploadFile
from fastapi.responses import HTMLResponse

from src.cancellation import (
    get_task_spec_hash,
    task_spec_content_hash,
)
from src.events import publish_wake
from src.keyspace import pipeline_state, upload_pending
from src.models import RepoState, TaskStatus
from src.queue_parser import (
    QueueValidationError,
    parse_task_header,
)
from src.utils import repo_slug_from_url
from src.web.services.upload_validation import (
    _ALLOWED_TASK_PATTERN,
    _TASK_UPLOAD_PATTERN,
    _build_upload_success_message,
    _format_upload_message_lines,
    _upload_feedback_target,
    sweep_abandoned_staging,
)

router = APIRouter()

_upload_locks: dict[str, asyncio.Lock] = {}


def _get_upload_lock(repo_name: str) -> asyncio.Lock:
    """Return the per-repo asyncio lock used to serialize upload writes."""
    if repo_name not in _upload_locks:
        _upload_locks[repo_name] = asyncio.Lock()
    return _upload_locks[repo_name]


def _render_upload_error(
    request: Request, message: str, status_code: int, repo_name: str = ""
) -> HTMLResponse:
    response = _app.templates.TemplateResponse(
        request,
        "components/upload_error.html",
        {"message": message, "message_lines": _format_upload_message_lines(message)},
        status_code=status_code,
    )
    if repo_name:
        response.headers["HX-Retarget"] = _upload_feedback_target(repo_name)
        response.headers["HX-Reswap"] = "innerHTML"
    return response


def _render_upload_success(
    request: Request, message: str, repo_name: str
) -> HTMLResponse:
    response = _app.templates.TemplateResponse(
        request,
        "components/upload_success.html",
        {"message": message, "message_lines": _format_upload_message_lines(message)},
    )
    response.headers["HX-Retarget"] = _upload_feedback_target(repo_name)
    response.headers["HX-Reswap"] = "innerHTML"
    return response


def _task_id_from_filename(fname: str) -> str:
    return fname[:-3]


def _is_task_in_error_state(repo_state: RepoState, task_id: str) -> bool:
    if (
        repo_state.current_task is not None
        and repo_state.current_task.pr_id == task_id
        and (
            repo_state.current_task.status == TaskStatus.ERROR
            or repo_state.state.value == "ERROR"
        )
    ):
        return True
    if repo_state.current_queue:
        return any(
            task.pr_id == task_id and task.status == TaskStatus.ERROR
            for task in repo_state.current_queue
        )
    return False


@router.post("/repos/{name}/upload-tasks", response_class=HTMLResponse)
async def upload_tasks(
    request: Request, name: str, files: list[UploadFile] = []
) -> HTMLResponse:
    cfg = _app.load_config(_app.CONFIG_PATH)
    found = False
    for repo in cfg.repositories:
        if repo_slug_from_url(repo.url) == name:
            found = True
            break

    if not found:
        return _render_upload_error(request, f"Repository '{name}' not found", 404, repo_name=name)

    repo_path = f"{_app.REPOS_DIR}/{name}"
    if not Path(repo_path).is_dir():
        return _render_upload_error(
            request, f"Repository '{name}' is not cloned", 422, repo_name=name
        )

    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is None:
        return _render_upload_error(
            request,
            "Cannot verify repo state (Redis unavailable). Upload blocked.",
            503,
            repo_name=name,
        )
    try:
        raw = await redis_client.get(pipeline_state(name))
    except Exception:
        return _render_upload_error(
            request,
            "Cannot verify repo state (Redis error). Upload blocked.",
            503,
            repo_name=name,
        )
    if raw:
        try:
            repo_state = RepoState.model_validate_json(raw)
        except Exception:
            return _render_upload_error(
                request,
                "Cannot verify repo state (corrupt data). Upload blocked.",
                503,
                repo_name=name,
            )
    else:
        return _render_upload_error(
            request,
            "Cannot verify repo state (no state recorded). Upload blocked.",
            503,
            repo_name=name,
        )
    if not files:
        return _render_upload_error(request, "No files uploaded", 422, repo_name=name)

    max_total_bytes = _app._UPLOAD_MAX_TOTAL_BYTES

    # Validate file names and sizes (stream chunks to enforce limit early)
    total_size = 0
    staged_size = 0
    file_contents: list[tuple[str, bytes]] = []
    _CHUNK = 64 * 1024
    for f in files:
        fname = f.filename or ""
        content_type = (f.content_type or "").lower()
        if fname.lower().endswith(".zip") or content_type in {
            "application/zip",
            "application/x-zip-compressed",
        }:
            zip_chunks: list[bytes] = []
            zip_size = 0
            while True:
                chunk = await f.read(_CHUNK)
                if not chunk:
                    break
                zip_size += len(chunk)
                total_size += len(chunk)
                if zip_size > max_total_bytes or total_size > max_total_bytes:
                    return _render_upload_error(
                        request, "Total upload size exceeds 1 MB", 422, repo_name=name
                    )
                zip_chunks.append(chunk)
            try:
                with zipfile.ZipFile(io.BytesIO(b"".join(zip_chunks))) as archive:
                    extracted_file_count = 0
                    for entry in archive.infolist():
                        entry_name = entry.filename
                        if entry.is_dir():
                            continue
                        if "/" in entry_name or "\\" in entry_name:
                            return _render_upload_error(
                                request,
                                f"Zip entry '{entry_name}' must not contain path separators.",
                                422,
                                repo_name=name,
                            )
                        if not re.match(_ALLOWED_TASK_PATTERN, entry_name):
                            return _render_upload_error(
                                request,
                                f"Invalid file name: '{entry_name}'. Only AGENTS.md, "
                                "CLAUDE.md, and PR-*.md allowed.",
                                422,
                                repo_name=name,
                            )
                        if staged_size + entry.file_size > max_total_bytes:
                            return _render_upload_error(
                                request, "Total upload size exceeds 1 MB", 422, repo_name=name
                            )
                        try:
                            chunks: list[bytes] = []
                            entry_size = 0
                            with archive.open(entry) as zipped_file:
                                while True:
                                    chunk = zipped_file.read(_CHUNK)
                                    if not chunk:
                                        break
                                    entry_size += len(chunk)
                                    if staged_size + entry_size > max_total_bytes:
                                        return _render_upload_error(
                                            request, "Total upload size exceeds 1 MB", 422, repo_name=name
                                        )
                                    chunks.append(chunk)
                        except (
                            EOFError,
                            NotImplementedError,
                            OSError,
                            RuntimeError,
                            zlib.error,
                        ):
                            return _render_upload_error(
                                request,
                                f"Uploaded zip '{fname}' contains corrupt, encrypted, "
                                "unsupported, or unreadable entries.",
                                400,
                                repo_name=name,
                            )
                        staged_size += entry_size
                        file_contents.append((entry_name, b''.join(chunks)))
                        extracted_file_count += 1
                    if extracted_file_count == 0:
                        return _render_upload_error(
                            request,
                            f"Uploaded zip '{fname}' does not contain any task files.",
                            422,
                            repo_name=name,
                        )
            except (UnicodeDecodeError, zipfile.BadZipFile):
                return _render_upload_error(
                    request, f"Uploaded zip '{fname}' is corrupt or unreadable.", 400, repo_name=name
                )
            except zipfile.LargeZipFile:
                return _render_upload_error(
                    request, f"Uploaded zip '{fname}' is too large to extract.", 400, repo_name=name
                )
            continue
        if not re.match(_ALLOWED_TASK_PATTERN, fname):
            return _render_upload_error(
                request,
                f"Invalid file name: '{fname}'. Only AGENTS.md, "
                "CLAUDE.md, and PR-*.md allowed.",
                422,
                repo_name=name,
            )
        chunks: list[bytes] = []
        while True:
            chunk = await f.read(_CHUNK)
            if not chunk:
                break
            total_size += len(chunk)
            staged_size += len(chunk)
            if total_size > max_total_bytes or staged_size > max_total_bytes:
                return _render_upload_error(
                    request, "Total upload size exceeds 1 MB", 422, repo_name=name
                )
            chunks.append(chunk)
        content = b"".join(chunks)
        file_contents.append((fname, content))

    task_uploads: dict[str, bytes] = {}
    for fname, content in file_contents:
        if re.fullmatch(_TASK_UPLOAD_PATTERN, fname):
            task_uploads[fname] = content

    aggregated_issues: list[str] = []
    parsed_task_ids: dict[str, str] = {}
    parsed_task_texts: dict[str, str] = {}
    for fname, content in task_uploads.items():
        try:
            task_text = content.decode("utf-8")
        except UnicodeDecodeError:
            return _render_upload_error(
                request,
                f"{fname} is not valid UTF-8",
                400,
                repo_name=name,
            )
        with tempfile.TemporaryDirectory() as tmpdir:
            task_path = Path(tmpdir) / fname
            task_path.write_text(task_text, encoding="utf-8")
            try:
                header = parse_task_header(task_path)
                parsed_task_ids[fname] = header.pr_id
                parsed_task_texts[fname] = task_text
            except QueueValidationError as exc:
                for issue in exc.issues:
                    aggregated_issues.append(
                        issue.replace(str(task_path), fname)
                    )

    if aggregated_issues:
        # Cap at 50 entries so a misbehaving batch upload cannot fill the
        # dashboard error toast with thousands of lines. The Depends-on
        # hint is keyed off the full aggregated list, not the capped slice,
        # so a relevant issue beyond the truncation boundary still surfaces
        # the guidance line.
        has_missing_depends_on = any(
            "missing Depends on" in issue for issue in aggregated_issues
        )
        capped = aggregated_issues[:50]
        truncated = len(aggregated_issues) - len(capped)
        if (
            len(aggregated_issues) == 1
            and has_missing_depends_on
        ):
            return _render_upload_error(
                request,
                f"Task file validation failed: {capped[0]} field.\n"
                "Use 'Depends on: none' for tasks with no dependencies.",
                400,
                repo_name=name,
            )
        body = "Task file validation failed:\n" + "\n".join(capped)
        if truncated > 0:
            body += f"\n... and {truncated} more error(s) (truncated)"
        if has_missing_depends_on:
            body += "\nUse 'Depends on: none' for tasks with no dependencies."
        return _render_upload_error(request, body, 400, repo_name=name)

    hash_rejections: list[str] = []
    accepted_file_contents: list[tuple[str, bytes]] = []
    accepted_task_hashes: dict[str, str] = {}
    for fname, content in file_contents:
        if fname not in task_uploads:
            accepted_file_contents.append((fname, content))
            continue
        task_id = parsed_task_ids.get(fname, _task_id_from_filename(fname))
        uploaded_hash = task_spec_content_hash(parsed_task_texts[fname])
        try:
            existing_hash = await get_task_spec_hash(redis_client, name, task_id)
        except Exception:
            return _render_upload_error(
                request,
                "Cannot verify task spec hash (Redis error). Upload blocked.",
                503,
                repo_name=name,
            )
        if (
            existing_hash == uploaded_hash
            and _is_task_in_error_state(repo_state, task_id)
        ):
            hash_rejections.append(
                f"{fname}: File unchanged. Use Retry button to re-attempt without changes."
            )
            continue
        accepted_file_contents.append((fname, content))
        accepted_task_hashes[task_id] = uploaded_hash

    if hash_rejections and not accepted_file_contents:
        return _render_upload_error(
            request,
            "\n".join(hash_rejections),
            409,
            repo_name=name,
        )

    file_contents = accepted_file_contents

    # Stage files to /data/uploads/{repo}/ and enqueue for daemon processing.
    # Git write operations are handled by the daemon to preserve the
    # dashboard's read-only contract with the repository working trees.
    lock = _get_upload_lock(name)
    async with lock:
        # Best-effort sweep of abandoned staging directories.
        # Collect active staging dirs for ALL repos so the sweep does not
        # accidentally remove another repo's still-pending directory.
        try:
            active_dirs: set[str] = set()
            pending_keys: list[bytes] = []
            async for pkey in redis_client.scan_iter(match="upload:*:pending"):
                pending_keys.append(pkey)
            for pkey in pending_keys:
                try:
                    raw_sweep = await redis_client.get(pkey)
                    if raw_sweep:
                        active_dirs.add(json.loads(raw_sweep)["staging_dir"])
                except Exception:
                    pass
            max_age = cfg.daemon.upload_staging_max_age_hours
            await asyncio.to_thread(
                sweep_abandoned_staging, _app.UPLOADS_DIR, active_dirs, max_age
            )
        except Exception:
            pass

        submission_id = uuid.uuid4().hex[:12]
        staging_dir = Path(_app.UPLOADS_DIR) / name / submission_id
        await asyncio.to_thread(staging_dir.mkdir, parents=True, exist_ok=True)

        committed = False
        try:
            for fname, content in file_contents:
                await asyncio.to_thread((staging_dir / fname).write_bytes, content)

            uploaded_filenames = [fn for fn, _ in file_contents]
            manifest_filenames = list(uploaded_filenames)
            manifest_task_hashes: dict[str, str] = {}
            pending_key = upload_pending(name)
            try:
                existing_raw = await redis_client.get(pending_key)
            except Exception:
                existing_raw = None

            if existing_raw:
                try:
                    existing = json.loads(existing_raw)
                    existing_task_hashes = existing.get("task_hashes", {})
                    if isinstance(existing_task_hashes, dict):
                        manifest_task_hashes.update(
                            {
                                str(task_id): str(task_hash)
                                for task_id, task_hash in existing_task_hashes.items()
                            }
                        )
                    old_staging = Path(existing["staging_dir"])
                    for old_fn in existing.get("files", []):
                        if old_fn not in manifest_filenames and (old_staging / old_fn).is_file():
                            await asyncio.to_thread(
                                shutil.copy2,
                                str(old_staging / old_fn),
                                str(staging_dir / old_fn),
                            )
                            manifest_filenames.append(old_fn)
                except Exception:
                    pass

            manifest_task_hashes.update(accepted_task_hashes)
            manifest = {
                "repo": name,
                "files": manifest_filenames,
                "staging_dir": str(staging_dir),
                "task_hashes": manifest_task_hashes,
            }
            try:
                await redis_client.set(
                    pending_key,
                    json.dumps(manifest),
                )
            except Exception:
                return _render_upload_error(
                    request,
                    "Failed to enqueue upload (Redis error).",
                    503,
                    repo_name=name,
                )
            committed = True
        finally:
            if not committed:
                await asyncio.to_thread(
                    shutil.rmtree, str(staging_dir), True
                )

    try:
        await publish_wake(redis_client, name, "upload")
    except Exception:
        _app.logger.warning(
            "publish_wake failed for %s; daemon will pick up upload on next tick",
            name,
            exc_info=True,
        )

    success_message = _build_upload_success_message(uploaded_filenames, repo_state.state)
    if hash_rejections:
        return _render_upload_error(
            request,
            success_message + "\n" + "\n".join(hash_rejections),
            409,
            repo_name=name,
        )
    return _render_upload_success(request, success_message, repo_name=name)


# Imported at end-of-file so all ``@router`` decorators above have already
# populated ``router.routes`` before ``app.py`` reaches
# ``app.include_router(_uploads_routes.router)``. FastAPI snapshots
# ``router.routes`` at include time, so an early import would let app.py
# load this module while it is still partial (router empty) and silently
# drop every endpoint declared below the import.
from src.web import app as _app  # noqa: E402
