"""Validation and bookkeeping helpers for the upload-tasks endpoint.

The dashboard upload route streams files into ``/data/uploads/<repo>/``
before the daemon picks them up. This module centralises the rules for
acceptable filenames, total upload size, the success-message renderer,
and the abandoned-staging sweep that runs on every upload to keep the
staging tree from accumulating orphaned directories.
"""

from __future__ import annotations

import re
import shutil
import time
from pathlib import Path

from src.models import PipelineState

_TASK_UPLOAD_PATTERN = r"^PR-[A-Za-z0-9._-]+\.md$"
_ALLOWED_TASK_PATTERN = (
    rf"^(AGENTS\.md|CLAUDE\.md|{_TASK_UPLOAD_PATTERN[1:-1]})$"
)
_STAGING_MAX_AGE_HOURS = 24
_FRONTMATTER_STATUS_LINE = re.compile(r"^status:\s*(.+?)\s*$", re.IGNORECASE)
_TERMINAL_FRONTMATTER_STATUSES = frozenset({"DONE", "ERROR"})


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
    """
    lines = content.splitlines()
    if not lines or lines[0].rstrip() != "---":
        return None
    status: str | None = None
    for line in lines[1:]:
        stripped = line.rstrip()
        if stripped == "---":
            return status
        if status is not None:
            continue
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
    Other frontmatter fields are preserved.
    """
    lines = content.splitlines(keepends=True)
    if not lines or lines[0].rstrip() != "---":
        return f"---\nstatus: {status}\n---\n\n{content}"
    closing_index: int | None = None
    for index, line in enumerate(lines[1:], start=1):
        if line.rstrip() == "---":
            closing_index = index
            break
    if closing_index is None:
        return f"---\nstatus: {status}\n---\n\n{content}"
    rewrote = False
    new_fm_lines: list[str] = []
    for line in lines[1:closing_index]:
        if _FRONTMATTER_STATUS_LINE.match(line.rstrip()):
            ending = "\r\n" if line.endswith("\r\n") else "\n" if line.endswith("\n") else ""
            new_fm_lines.append(f"status: {status}{ending}")
            rewrote = True
        else:
            new_fm_lines.append(line)
    if not rewrote:
        ending = "\n" if not new_fm_lines or new_fm_lines[-1].endswith("\n") else ""
        new_fm_lines.append(f"status: {status}{ending or chr(10)}")
    return lines[0] + "".join(new_fm_lines) + "".join(lines[closing_index:])


def preserve_terminal_status_on_collision(
    existing_path: Path, upload_content: str
) -> tuple[str, str | None]:
    """Return ``(content_to_stage, preserved_status_or_None)``.

    If *existing_path* exists with frontmatter ``status: DONE`` or
    ``status: ERROR``, the uploaded body is kept but the terminal status is
    re-attached so the next daemon commit cannot regress a merged/crashed
    task to ``TODO``. Otherwise the upload content is returned unchanged.
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
