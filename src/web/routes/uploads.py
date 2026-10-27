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

from fastapi import APIRouter, Form, Request, UploadFile
from fastapi.responses import HTMLResponse

from src.cancellation import (
    get_task_spec_hash,
    task_spec_content_hash,
)
from src.events import publish_wake
from src.github.prs import extract_queue_pr_id
from src.keyspace import pipeline_state, upload_pending, upload_pending_count
from src.mcp.scans import scan_for_conflicts
from src.models import RepoState, TaskStatus
from src.queue_parser import (
    QueueValidationError,
    TaskHeader,
    parse_task_header,
)
from src.task_budget import validate_task_frontmatter_budget
from src.task_status import get_merged_pr_ids
from src.utils import repo_slug_from_url
from src.web.services.upload_validation import (
    _ALLOWED_TASK_PATTERN,
    _TASK_UPLOAD_PATTERN,
    _build_upload_success_message,
    _format_upload_message_lines,
    _upload_feedback_target,
    effective_recorded_completions,
    effective_task_headers,
    preserve_terminal_status_on_collision,
    sweep_abandoned_staging,
    validate_effective_task_graph,
)

router = APIRouter()

_upload_locks: dict[str, asyncio.Lock] = {}
_MISSING = object()

_ENQUEUE_UPLOAD_PENDING_LUA = """
redis.call("set", KEYS[1], ARGV[1])
if ARGV[2] == "" then
    redis.call("del", KEYS[2])
else
    redis.call("set", KEYS[2], ARGV[2])
end
return 1
"""


def _get_upload_lock(repo_name: str) -> asyncio.Lock:
    """Return the per-repo asyncio lock used to serialize upload writes."""
    if repo_name not in _upload_locks:
        _upload_locks[repo_name] = asyncio.Lock()
    return _upload_locks[repo_name]


def _render_upload_error(
    request: Request,
    message: str,
    status_code: int,
    repo_name: str = "",
    validation_errors: list[dict[str, object]] | None = None,
) -> HTMLResponse:
    response = _app.templates.TemplateResponse(
        request,
        "components/upload_error.html",
        {
            "message": message,
            "message_lines": _format_upload_message_lines(message),
            "validation_errors": validation_errors or [],
        },
        status_code=status_code,
    )
    if repo_name:
        response.headers["HX-Retarget"] = _upload_feedback_target(repo_name)
        response.headers["HX-Reswap"] = "innerHTML"
    return response


def _render_upload_success(
    request: Request,
    message: str,
    repo_name: str,
    uploaded_files: list[str] | None = None,
    commit_subject: str = "",
) -> HTMLResponse:
    response = _app.templates.TemplateResponse(
        request,
        "components/upload_success.html",
        {
            "message": message,
            "message_lines": _format_upload_message_lines(message),
            "uploaded_files": uploaded_files or [],
            "commit_subject": commit_subject,
        },
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


def _upload_commit_subject(subject: str, uploaded_count: int) -> str:
    custom_subject = subject.strip()
    if custom_subject:
        return custom_subject
    return f"tasks: upload batch ({uploaded_count} files)"


def _redis_text(value: object) -> object:
    if isinstance(value, bytes):
        return value.decode()
    return value


async def _rollback_upload_manifest(
    redis_client: object,
    pending_key: str,
    manifest_json: str,
    previous_manifest: object,
) -> None:
    """Best-effort restore for fallback clients without Redis Lua support."""
    try:
        current = await redis_client.get(pending_key)  # type: ignore[attr-defined]
        if _redis_text(current) != manifest_json:
            return
        if previous_manifest is None:
            await redis_client.delete(pending_key)  # type: ignore[attr-defined]
        else:
            await redis_client.set(  # type: ignore[attr-defined]
                pending_key,
                previous_manifest,
            )
    except Exception:
        pass


async def _enqueue_upload_manifest(
    redis_client: object,
    *,
    pending_key: str,
    manifest_json: str,
    count_key: str,
    pending_count: int | None,
    previous_manifest: object = _MISSING,
) -> None:
    """Atomically publish the upload manifest and dashboard pending count."""
    count_value = "" if pending_count is None else str(pending_count)
    try:
        await redis_client.eval(  # type: ignore[attr-defined]
            _ENQUEUE_UPLOAD_PENDING_LUA,
            2,
            pending_key,
            count_key,
            manifest_json,
            count_value,
        )
        return
    except Exception:
        pass

    if previous_manifest is _MISSING:
        previous_manifest = await redis_client.get(pending_key)  # type: ignore[attr-defined]
    await redis_client.set(pending_key, manifest_json)  # type: ignore[attr-defined]
    try:
        if pending_count is None:
            try:
                await redis_client.delete(count_key)  # type: ignore[attr-defined]
            except AttributeError:
                pass
        else:
            await redis_client.set(count_key, count_value)  # type: ignore[attr-defined]
    except Exception:
        await _rollback_upload_manifest(
            redis_client,
            pending_key,
            manifest_json,
            previous_manifest,
        )
        raise


def _validate_zip_member(entry_name: str) -> str | None:
    parts = entry_name.replace("\\", "/").split("/")
    if entry_name.startswith("/") or ".." in parts:
        return f"Zip entry '{entry_name}' must not use absolute paths or '..'."
    if "/" in entry_name or "\\" in entry_name:
        return f"Zip entry '{entry_name}' must not contain path separators."
    if not re.match(_ALLOWED_TASK_PATTERN, entry_name):
        return (
            f"Invalid file name: '{entry_name}'. Only AGENTS.md, "
            "CLAUDE.md, and PR-*.md allowed."
        )
    return None


def _format_task_validation_errors(
    validation_errors: list[dict[str, object]],
) -> str:
    capped = validation_errors[:50]
    truncated = len(validation_errors) - len(capped)
    first_issue = ""
    if len(validation_errors) == 1 and len(validation_errors[0]["errors"]) == 1:
        fname = str(validation_errors[0]["file"])
        issue = str(validation_errors[0]["errors"][0])
        separator = " " if issue.startswith("is ") else ": "
        suffix = " field." if issue == "missing Depends on" else ""
        first_issue = f"{fname}{separator}{issue}{suffix}"
    lines = [
        f"Task file validation failed: {first_issue}"
        if first_issue
        else "Task file validation failed:"
    ]
    for entry in capped:
        fname = str(entry["file"])
        for issue_obj in entry["errors"]:
            issue = str(issue_obj)
            separator = " " if issue.startswith("is ") else ": "
            lines.append(f"{fname}{separator}{issue}")
    if truncated > 0:
        lines.append(f"... and {truncated} more error(s) (truncated)")
    all_issues = [
        str(issue)
        for entry in validation_errors
        for issue in entry["errors"]
    ]
    if any("missing Depends on" in issue for issue in all_issues):
        lines.append("Use 'Depends on: none' for tasks with no dependencies.")
    return "\n".join(lines)


def _add_validation_error(
    errors_by_file: dict[str, list[str]], fname: str, issue: str
) -> None:
    errors_by_file.setdefault(fname, []).append(issue)


def _extend_validation_errors(
    errors_by_file: dict[str, list[str]],
    new_errors: dict[str, list[str]],
) -> None:
    for fname, issues in new_errors.items():
        errors_by_file.setdefault(fname, []).extend(issues)


async def _graph_validation_errors(
    repo_path: str, repo_branch: str, incoming: dict[str, TaskHeader],
    pending_manifest: str | bytes | None, incoming_texts: dict[str, str],
) -> dict[str, list[str]]:
    headers = effective_task_headers(Path(repo_path) / "tasks", incoming, pending_manifest)
    candidates = {
        pr_id for header in headers.values() for pr_id in (header.pr_id, *header.depends_on)
    }
    if candidates:
        try:
            merged = set(await asyncio.to_thread(
                get_merged_pr_ids, repo_path, repo_branch, candidates,
            ))
        except Exception:
            merged = set()
    else:
        merged = set()
    try:
        merged |= await asyncio.to_thread(
            effective_recorded_completions, repo_path, repo_branch, candidates,
            incoming_texts, pending_manifest,
        )
    except (OSError, RuntimeError) as exc:
        return {fname: [f"Cannot verify completion records: {exc}"] for fname in incoming}
    return validate_effective_task_graph(headers, merged_pr_ids=merged)


@router.post("/repos/{name}/upload-tasks", response_class=HTMLResponse)
async def upload_tasks(
    request: Request,
    name: str,
    files: list[UploadFile] = [],
    subject: str = Form(""),
) -> HTMLResponse:
    cfg = _app.load_config(_app.CONFIG_PATH)
    found = False
    repo_branch = "main"
    for repo in cfg.repositories:
        if repo_slug_from_url(repo.url) == name:
            found = True
            repo_branch = repo.branch
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

    if subject.strip() and extract_queue_pr_id(subject) is not None:
        return _render_upload_error(
            request,
            "Upload commit subject must not start with a queue PR ID prefix.",
            400,
            repo_name=name,
        )

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
                    safe_members: list[zipfile.ZipInfo] = []
                    extracted_file_count = 0
                    for entry in archive.infolist():
                        entry_name = entry.filename
                        if entry.is_dir():
                            continue
                        member_error = _validate_zip_member(entry_name)
                        if member_error is not None:
                            return _render_upload_error(
                                request,
                                member_error,
                                422,
                                repo_name=name,
                            )
                        if staged_size + entry.file_size > max_total_bytes:
                            return _render_upload_error(
                                request, "Total upload size exceeds 1 MB", 422, repo_name=name
                            )
                        safe_members.append(entry)
                        extracted_file_count += 1
                    if extracted_file_count == 0:
                        return _render_upload_error(
                            request,
                            f"Uploaded zip '{fname}' does not contain any task files.",
                            422,
                            repo_name=name,
                        )
                    with tempfile.TemporaryDirectory(prefix="upload-", dir="/tmp"):
                        for entry in safe_members:
                            entry_name = entry.filename
                            try:
                                chunks = []
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
                            file_contents.append((entry_name, b"".join(chunks)))
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

    errors_by_file: dict[str, list[str]] = {}
    task_uploads: dict[str, bytes] = {}
    for fname, content in file_contents:
        if re.fullmatch(_TASK_UPLOAD_PATTERN, fname):
            if fname in task_uploads:
                _add_validation_error(errors_by_file, fname, "duplicate task id/filename in upload")
            task_uploads[fname] = content

    parsed_task_ids: dict[str, str] = {}
    parsed_task_texts: dict[str, str] = {}
    parsed_headers: dict[str, TaskHeader] = {}
    for fname, content in task_uploads.items():
        try:
            task_text = content.decode("utf-8")
        except UnicodeDecodeError:
            _add_validation_error(errors_by_file, fname, "is not valid UTF-8")
            continue
        with tempfile.TemporaryDirectory() as tmpdir:
            task_path = Path(tmpdir) / fname
            task_path.write_text(task_text, encoding="utf-8")
            try:
                header = parse_task_header(task_path)
                for issue in validate_task_frontmatter_budget(
                    task_text, complexity=header.complexity
                ):
                    _add_validation_error(errors_by_file, fname, issue)
                parsed_task_ids[fname] = header.pr_id
                parsed_task_texts[fname] = task_text
                parsed_headers[fname] = header
            except QueueValidationError as exc:
                for issue in exc.issues:
                    _add_validation_error(
                        errors_by_file,
                        fname,
                        issue.replace(f"{task_path}: ", ""),
                    )
                continue
        for violation in scan_for_conflicts(task_text):
            _add_validation_error(
                errors_by_file,
                fname,
                f"AGENTS.md anti-pattern {violation.violation_type}: "
                f"{violation.rule}",
            )

    try:
        pending_manifest = await redis_client.get(upload_pending(name))
    except Exception:
        pending_manifest = None
    _extend_validation_errors(errors_by_file, await _graph_validation_errors(
        repo_path, repo_branch, parsed_headers, pending_manifest, parsed_task_texts,
    ))

    if errors_by_file:
        validation_errors = [
            {"file": fname, "errors": issues}
            for fname, issues in sorted(errors_by_file.items())
        ]
        status_code = (
            409
            if all(
                str(issue).startswith("File unchanged.")
                for entry in validation_errors
                for issue in entry["errors"]
            )
            else 400
        )
        return _render_upload_error(
            request,
            _format_task_validation_errors(validation_errors),
            status_code,
            repo_name=name,
            validation_errors=validation_errors,
        )

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
            _add_validation_error(
                errors_by_file,
                fname,
                "File unchanged. Use Retry button to re-attempt without changes.",
            )
            continue
        accepted_file_contents.append((fname, content))
        accepted_task_hashes[task_id] = uploaded_hash

    if errors_by_file:
        validation_errors = [
            {"file": fname, "errors": issues}
            for fname, issues in sorted(errors_by_file.items())
        ]
        status_code = (
            409
            if all(
                str(issue).startswith("File unchanged.")
                for entry in validation_errors
                for issue in entry["errors"]
            )
            else 400
        )
        return _render_upload_error(
            request,
            _format_task_validation_errors(validation_errors),
            status_code,
            repo_name=name,
            validation_errors=validation_errors,
        )

    file_contents = accepted_file_contents

    # Per OBS-CY: a re-upload of a spec at status:TODO must not regress an
    # already-merged copy on disk. Before staging, rewrite task-file contents
    # whose destination already carries a terminal ``DONE`` frontmatter so the
    # daemon's later overwrite preserves that status. ``status: ERROR`` is
    # intentionally NOT preserved here — re-upload is the documented retry
    # signal (see ``src/daemon/repo_ops.py``) and the incoming TODO must reach
    # the daemon for the task to leave ERROR.
    preserved_collisions: list[tuple[str, str]] = []
    rewritten_contents: list[tuple[str, bytes]] = []
    tasks_dir = Path(repo_path) / "tasks"
    for fname, content in file_contents:
        if not re.fullmatch(_TASK_UPLOAD_PATTERN, fname):
            rewritten_contents.append((fname, content))
            continue
        # Use the text already decoded and validated above.
        upload_text = parsed_task_texts[fname]
        new_text, preserved_status = preserve_terminal_status_on_collision(
            tasks_dir / fname, upload_text
        )
        if preserved_status is None:
            rewritten_contents.append((fname, content))
            continue
        preserved_collisions.append((fname, preserved_status))
        rewritten_contents.append((fname, new_text.encode("utf-8")))
    file_contents = rewritten_contents
    if preserved_collisions:
        _app.logger.info(
            "Upload preserved terminal frontmatter status for %s in repo %s: %s",
            len(preserved_collisions),
            name,
            ", ".join(
                f"{fname}={status}" for fname, status in preserved_collisions
            ),
        )

    # Stage files to /data/uploads/{repo}/ and enqueue for daemon processing.
    # Git write operations are handled by the daemon to preserve the
    # dashboard's read-only contract with the repository working trees.
    lock = _get_upload_lock(name)
    async with lock:
        pending_key = upload_pending(name)
        try:
            existing_raw = await redis_client.get(pending_key)
        except Exception:
            return _render_upload_error(
                request, "Cannot validate pending task graph (Redis error). Upload blocked.",
                503, repo_name=name,
            )
        graph_errors = await _graph_validation_errors(
            repo_path, repo_branch, parsed_headers, existing_raw, parsed_task_texts,
        )
        if graph_errors:
            validation_errors = [
                {"file": fname, "errors": issues}
                for fname, issues in sorted(graph_errors.items())
            ]
            return _render_upload_error(
                request, _format_task_validation_errors(validation_errors), 400,
                repo_name=name, validation_errors=validation_errors,
            )
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
                "commit_subject": _upload_commit_subject(
                    subject,
                    len(uploaded_filenames),
                ),
            }
            manifest_json = json.dumps(manifest)
            pending_count = (
                len(manifest_filenames)
                if existing_raw is not None or repo_state.state.value != "IDLE"
                else None
            )
            try:
                await _enqueue_upload_manifest(
                    redis_client,
                    pending_key=pending_key,
                    manifest_json=manifest_json,
                    count_key=upload_pending_count(name),
                    pending_count=pending_count,
                    previous_manifest=existing_raw,
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
    return _render_upload_success(
        request,
        success_message,
        repo_name=name,
        uploaded_files=sorted(uploaded_filenames),
        commit_subject=_upload_commit_subject(
            subject,
            len(uploaded_filenames),
        ),
    )


# Imported at end-of-file so all ``@router`` decorators above have already
# populated ``router.routes`` before ``app.py`` reaches
# ``app.include_router(_uploads_routes.router)``. FastAPI snapshots
# ``router.routes`` at include time, so an early import would let app.py
# load this module while it is still partial (router empty) and silently
# drop every endpoint declared below the import.
from src.web import app as _app  # noqa: E402
