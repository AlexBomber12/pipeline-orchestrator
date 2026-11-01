"""Validation and bookkeeping helpers for the upload-tasks endpoint.

The dashboard upload route streams files into ``/data/uploads/<repo>/``
before the daemon picks them up. This module centralises the rules for
acceptable filenames, total upload size, the success-message renderer,
and the abandoned-staging sweep that runs on every upload to keep the
staging tree from accumulating orphaned directories.
"""

from __future__ import annotations

import json
import re
import shutil
import time
from dataclasses import replace
from io import StringIO
from pathlib import Path

from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from src.cancellation import task_spec_content_hash
from src.completion_evidence import get_recorded_completions
from src.dag import build_task_dag
from src.models import PipelineState
from src.queue_parser import (
    QueueValidationError,
    TaskHeader,
    parse_existing_task_header,
)
from src.task_status import merged_split_parent_aliases

_TASK_UPLOAD_PATTERN = r"^PR-[A-Za-z0-9._-]+\.md$"
_ALLOWED_TASK_PATTERN = (
    rf"^(AGENTS\.md|CLAUDE\.md|{_TASK_UPLOAD_PATTERN[1:-1]})$"
)
_STAGING_MAX_AGE_HOURS = 24
_FRONTMATTER_STATUS_LINE = re.compile(r"^status:\s*(.+?)\s*$", re.IGNORECASE)
_TERMINAL_FRONTMATTER_STATUSES = frozenset({"DONE"})


def task_spec_replay_matches(stored: str, incoming: str) -> bool:
    """Recognize status-writer YAML reflow without changing stored identities.

    Only YAML presentation and daemon-owned fields may differ. The complete
    Markdown body must match; a replay never replaces the accepted file.
    """
    if task_spec_content_hash(stored) == task_spec_content_hash(incoming):
        return True
    pattern = r"\A---\n(.*?)^---\n(.*)\Z"
    parts = [re.match(pattern, text, re.MULTILINE | re.DOTALL) for text in (stored, incoming)]
    if not all(parts) or parts[0][2] != parts[1][2]:
        return False
    try:
        values = [YAML(typ="safe").load(part[1]) for part in parts]
    except YAMLError:
        return False
    if not all(isinstance(value, dict) for value in values):
        return False
    for value in values:
        for key in ("status", "blocked_reason"):
            value.pop(key, None)
    # Python equality treats True, 1 and 1.0 as equal. Safe YAML emission
    # retains scalar types and sorts mapping keys, without migrating hashes.
    canonical = []
    for value in values:
        stream = StringIO()
        YAML(typ="safe").dump(value, stream)
        canonical.append(stream.getvalue())
    return canonical[0] == canonical[1]


def _strip_inline_frontmatter_comment(value: str) -> str:
    """Drop an inline ``#`` comment from a frontmatter value.

    Mirrors ``queue_parser._normalize_frontmatter_status`` so the upload
    collision guard agrees with the canonical task parser on values like
    ``status: done # reviewer override``. ``#`` only starts a comment when
    it sits outside a quoted span and is preceded by whitespace (or is the
    first character).
    """
    quote: str | None = None
    for index, char in enumerate(value):
        if char in {"'", '"'}:
            if quote is None:
                quote = char
            elif quote == char:
                quote = None
        elif char == "#" and quote is None and (
            index == 0 or value[index - 1].isspace()
        ):
            return value[:index]
    return value


def read_frontmatter_status(content: str) -> str | None:
    """Return the uppercase frontmatter ``status`` for *content*, or ``None``.

    Tolerant of quoted values, inline ``#`` comments, and trailing whitespace.
    Returns ``None`` when there is no leading ``---`` block, no closing
    ``---``, or no ``status`` field inside the block.

    Mirrors ``queue_parser.parse_task_header``: leading blank lines before the
    opening ``---`` are skipped, and when multiple ``status:`` lines appear
    inside the block the LAST one wins (so collision preservation agrees with
    the daemon's view of the on-disk task status).
    """
    lines = content.splitlines()
    opening_index: int | None = next(
        (index for index, raw_line in enumerate(lines) if raw_line.strip()),
        None,
    )
    if opening_index is None or lines[opening_index].rstrip() != "---":
        return None
    status: str | None = None
    for line in lines[opening_index + 1 :]:
        stripped = line.rstrip()
        if stripped == "---":
            return status
        match = _FRONTMATTER_STATUS_LINE.match(stripped)
        if match:
            value = _strip_inline_frontmatter_comment(match.group(1)).strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
            value = value.strip().upper()
            status = value or None
    return None


def _replace_frontmatter_status(content: str, status: str) -> str:
    """Return *content* with its frontmatter ``status`` set to *status*.

    If *content* lacks a complete ``---`` block, a minimal one is prepended.
    Other frontmatter fields are preserved. Leading blank lines before the
    opening ``---`` are tolerated (matching ``parse_task_header`` and
    ``read_frontmatter_status``) so an upload that starts with whitespace
    above a valid frontmatter block is edited in place instead of getting
    a second, conflicting frontmatter prepended.
    """
    lines = content.splitlines(keepends=True)
    opening_index = next(
        (index for index, raw_line in enumerate(lines) if raw_line.strip()),
        None,
    )
    if opening_index is None or lines[opening_index].rstrip() != "---":
        return f"---\nstatus: {status}\n---\n\n{content}"
    closing_index: int | None = None
    for index in range(opening_index + 1, len(lines)):
        if lines[index].rstrip() == "---":
            closing_index = index
            break
    if closing_index is None:
        return f"---\nstatus: {status}\n---\n\n{content}"
    rewrote = False
    new_fm_lines: list[str] = []
    for line in lines[opening_index + 1 : closing_index]:
        if _FRONTMATTER_STATUS_LINE.match(line.rstrip()):
            ending = "\r\n" if line.endswith("\r\n") else "\n" if line.endswith("\n") else ""
            new_fm_lines.append(f"status: {status}{ending}")
            rewrote = True
        else:
            new_fm_lines.append(line)
    if not rewrote:
        ending = "\n" if not new_fm_lines or new_fm_lines[-1].endswith("\n") else ""
        new_fm_lines.append(f"status: {status}{ending or chr(10)}")
    return (
        "".join(lines[: opening_index + 1])
        + "".join(new_fm_lines)
        + "".join(lines[closing_index:])
    )


def preserve_terminal_status_on_collision(
    existing_path: Path, upload_content: str
) -> tuple[str, str | None]:
    """Return ``(content_to_stage, preserved_status_or_None)``.

    If *existing_path* exists with frontmatter ``status: DONE``, the uploaded
    body is kept but the terminal status is re-attached so the next daemon
    commit cannot regress a merged task to ``TODO``. ``status: ERROR`` is
    intentionally NOT preserved: re-uploading a task spec is the documented
    retry signal (``src/daemon/repo_ops.py``), and the daemon needs the
    incoming ``status: TODO`` so the task becomes dispatchable again instead
    of staying pinned in ERROR. Returns the upload content unchanged in every
    other case.
    """
    if not existing_path.is_file():
        return upload_content, None
    try:
        existing_text = existing_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return upload_content, None
    existing_status = read_frontmatter_status(existing_text)
    if existing_status not in _TERMINAL_FRONTMATTER_STATUSES:
        return upload_content, None
    return _replace_frontmatter_status(upload_content, existing_status), existing_status


def _escape_css_identifier(value: str) -> str:
    """Escape ``value`` so it is safe as a CSS identifier suffix."""
    return re.sub(r"([.#\[\]:>+~(){}|^$*!])", r"\\\1", value)


def _upload_feedback_target(repo_name: str) -> str:
    """Return the HTMX target selector for a repo's upload feedback panel."""
    css_name = _escape_css_identifier(repo_name)
    return f"#upload-feedback-{css_name}"


def _format_upload_message_lines(message: str) -> list[str]:
    """Return non-empty lines from ``message`` for templated rendering."""
    return [line for line in message.splitlines() if line.strip()]


def _unique_filenames(filenames: list[str]) -> list[str]:
    """Return ``filenames`` with duplicates removed, preserving order."""
    return list(dict.fromkeys(filenames))


def _task_upload_summary(task_filenames: list[str]) -> str:
    """Render an operator-facing summary of accepted task filenames."""
    if not task_filenames:
        return ""

    def _sort_key(filename: str) -> tuple[int, int | str]:
        match = re.fullmatch(r"PR-(\d+)\.md", filename)
        if match:
            return (0, int(match.group(1)))
        return (1, filename)

    ordered = sorted(task_filenames, key=_sort_key)
    labels = [filename.removesuffix(".md") for filename in ordered]
    if len(labels) == 1:
        return labels[0]
    pr_numbers: list[int] = []
    for filename in ordered:
        match = re.fullmatch(r"PR-(\d+)\.md", filename)
        if not match:
            return ", ".join(labels)
        pr_numbers.append(int(match.group(1)))

    if all(
        current == previous + 1
        for previous, current in zip(pr_numbers, pr_numbers[1:], strict=False)
    ):
        return f"{labels[0]} through {labels[-1]}"
    return ", ".join(labels)


def _build_upload_success_message(
    filenames: list[str], repo_state: PipelineState
) -> str:
    """Compose the multi-line success message rendered after a good upload."""
    task_filenames = _unique_filenames(
        [
            filename
            for filename in filenames
            if re.fullmatch(_TASK_UPLOAD_PATTERN, filename)
        ]
    )
    helper_filenames = _unique_filenames(
        [
            filename
            for filename in filenames
            if not re.fullmatch(_TASK_UPLOAD_PATTERN, filename)
        ]
    )

    task_count = len(task_filenames)
    noun = "file" if task_count == 1 else "files"
    summary = _task_upload_summary(task_filenames)
    if summary:
        lines = [f"Accepted {task_count} task {noun} ({summary})."]
    else:
        lines = [f"Accepted {task_count} task {noun}."]

    if helper_filenames:
        helper_noun = "file" if len(helper_filenames) == 1 else "files"
        lines.append(
            f"Also uploaded helper {helper_noun}: {', '.join(sorted(helper_filenames))}."
        )
    if repo_state == PipelineState.IDLE:
        lines.append(
            "Daemon will commit on the next poll cycle (up to 60 seconds)."
        )
    else:
        lines.append(
            "Daemon is currently "
            f"{repo_state.value}. Files will be committed when it returns to IDLE."
        )
    lines.append("Auto-dismissing in 30 seconds.")
    return "\n".join(lines)


def sweep_abandoned_staging(
    uploads_root: str,
    active_staging_dirs: set[str],
    max_age_hours: int = _STAGING_MAX_AGE_HOURS,
) -> int:
    """Remove staging directories older than *max_age_hours* with no active key.

    *active_staging_dirs* is the set of staging directory paths that are
    currently referenced by a Redis upload manifest.  These are preserved
    regardless of age.

    Returns the count of directories removed.
    """
    root = Path(uploads_root)
    if not root.is_dir():
        return 0
    now = time.time()
    cutoff = now - max_age_hours * 3600
    removed = 0
    for repo_dir in root.iterdir():
        if not repo_dir.is_dir():
            continue
        for entry in repo_dir.iterdir():
            if not entry.is_dir():
                continue
            if str(entry) in active_staging_dirs:
                continue
            try:
                mtime = entry.stat().st_mtime
            except OSError:
                continue
            if mtime < cutoff:
                shutil.rmtree(entry, ignore_errors=True)
                removed += 1
    return removed


def _parse_existing_task_header(path: Path) -> TaskHeader | None:
    try:
        return parse_existing_task_header(path)
    except QueueValidationError:
        return _parse_legacy_existing_task_header(path)
    except (OSError, UnicodeDecodeError):
        return None


def _parse_legacy_existing_task_header(path: Path) -> TaskHeader | None:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError):
        return None

    header_match: re.Match[str] | None = None
    branch = ""
    for raw_line in lines:
        line = raw_line.rstrip()
        if header_match is None:
            header_match = re.match(r"^#\s+(PR-[A-Za-z0-9_.-]+):\s*(.+?)\s*$", line)
            continue
        if not line.strip():
            continue
        branch_match = re.match(r"^Branch\s*:\s*(.*?)\s*$", line)
        if branch_match:
            branch = branch_match.group(1).strip()
            break
        if line.startswith("#") or line.startswith("- "):
            break
    if header_match is None or not branch:
        return None
    return TaskHeader(
        pr_id=header_match.group(1),
        title=header_match.group(2),
        # Retain legacy dependency identity without inventing executable metadata.
        branch="",
        task_type="feature",
        complexity="medium",
        depends_on=[],
        priority=3,
        coder="any",
    )


def _effective_task_paths(
    tasks_dir: Path, pending_manifest: str | bytes | None,
) -> dict[str, Path]:
    """Resolve stored and pending task paths with pending replacements winning."""
    paths = {path.name: path for path in tasks_dir.glob("PR-*.md") if path.is_file()}
    if pending_manifest:
        try:
            manifest = json.loads(pending_manifest)
            files = manifest.get("files", [])
            staging = manifest.get("staging_dir")
            if isinstance(files, list) and isinstance(staging, str):
                for fname in files:
                    if isinstance(fname, str) and re.fullmatch(_TASK_UPLOAD_PATTERN, fname):
                        path = Path(staging) / fname
                        if path.is_file():
                            paths[fname] = path
        except (ValueError, TypeError, AttributeError):
            pass
    return paths


def effective_recorded_completions(
    repo_path: str, base_branch: str, candidates: set[str],
    incoming: dict[str, str], pending_manifest: str | bytes | None = None,
) -> set[str]:
    """Verify receipts against the exact effective bytes that will be committed."""
    tasks_dir = Path(repo_path) / "tasks"
    paths = _effective_task_paths(tasks_dir, pending_manifest)
    contents = {}
    for fname, path in paths.items():
        if fname in incoming:
            continue
        stored = tasks_dir / fname
        if path != stored and stored.is_file() and task_spec_replay_matches(
            stored.read_text(encoding="utf-8"), path.read_text(encoding="utf-8")
        ):
            path = stored
        contents[Path(fname).stem] = path.read_bytes()
    for fname, content in incoming.items():
        stored = tasks_dir / fname
        if stored.is_file() and task_spec_replay_matches(stored.read_text(encoding="utf-8"), content):
            contents[Path(fname).stem] = stored.read_bytes()
            continue
        content, _ = preserve_terminal_status_on_collision(stored, content)
        contents[Path(fname).stem] = content.encode("utf-8")
    return get_recorded_completions(
        repo_path, base_branch, Path(repo_path).name.replace("__", "/", 1),
        candidates, task_contents=contents,
    )


def effective_task_headers(
    tasks_dir: Path,
    incoming: dict[str, TaskHeader],
    pending_manifest: str | bytes | None = None,
) -> dict[str, TaskHeader]:
    """Overlay pending and incoming replacements by filename, retaining edges."""
    paths = _effective_task_paths(tasks_dir, pending_manifest)
    headers = {
        fname: header
        for fname, path in paths.items()
        if fname not in incoming
        for header in [_parse_existing_task_header(path)]
        if header is not None
    }
    for fname, header in incoming.items():
        # Admission must use the same terminal-status preservation as staging.
        if preserve_terminal_status_on_collision(tasks_dir / fname, "")[1] == "DONE":
            header = replace(header, frontmatter_status="done")
        headers[fname] = header
    return headers


def validate_effective_task_graph(
    headers: dict[str, TaskHeader], *, merged_pr_ids: set[str],
) -> dict[str, list[str]]:
    """Validate identities and the executable graph using canonical DAG rules.

    Only positively evidenced completions are leaves; DONE frontmatter alone
    does not prove completion. Synthetic legacy headers retain dependency
    identity but have no executable branch to reserve.
    """
    errors: dict[str, list[str]] = {}
    known = {header.pr_id for header in headers.values()}
    completed = set(merged_pr_ids)
    completed |= merged_split_parent_aliases(
        structured_pr_ids=known, merged_pr_ids=merged_pr_ids,
    )
    branches: dict[str, str] = {}
    for fname, header in headers.items():
        if Path(fname).stem != header.pr_id:
            errors.setdefault(fname, []).append(
                f"header PR ID {header.pr_id!r} does not match task file {Path(fname).stem!r}"
            )
        if header.pr_id in completed:
            continue
        if header.branch and header.branch in branches:
            errors.setdefault(fname, []).append(
                f"duplicate branch {header.branch!r}: used by {branches[header.branch]} and {header.pr_id}"
            )
        if header.branch:
            branches[header.branch] = header.pr_id
        for dependency in header.depends_on:
            if dependency not in known | completed:
                errors.setdefault(fname, []).append(
                    f"Depends on {dependency} which is not in this upload and not in tasks/."
                )
    dag_headers = [
        replace(header, depends_on=[] if header.pr_id in completed else [
            dep for dep in header.depends_on if dep not in completed
        ])
        for header in headers.values()
    ]
    try:
        build_task_dag(dag_headers)
    except ValueError as exc:
        for fname in headers:
            errors.setdefault(fname, []).append(str(exc))
    return errors
