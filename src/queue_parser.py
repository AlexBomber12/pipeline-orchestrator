"""Parser for tasks/QUEUE.md and task selection logic."""

from __future__ import annotations

import re
from pathlib import Path

from src.models import QueueTask, TaskStatus

_HEADER_RE = re.compile(r"^##\s+(PR-[A-Za-z0-9_.-]+):\s*(.+?)\s*$")
_FIELD_RE = re.compile(r"^-\s*([A-Za-z ]+?)\s*:\s*(.*?)\s*$")
_STATUS_LINE_RE = re.compile(
    r"^(-\s*status\s*:\s*)(\S+)(.*)$", re.IGNORECASE
)


def parse_queue(queue_path: str) -> list[QueueTask]:
    """Parse a QUEUE.md file and return its tasks in document order.

    Returns an empty list if the file does not exist.
    """
    path = Path(queue_path)
    if not path.is_file():
        return []
    return parse_queue_text(path.read_text(encoding="utf-8"))


def parse_queue_text(text: str) -> list[QueueTask]:
    """Parse QUEUE.md content from an in-memory string.

    Same grammar as ``parse_queue``; split out so callers that already
    have QUEUE.md content in memory — e.g. ``PipelineRunner.recover_state``
    reading it from ``origin/{branch}`` via ``git show`` to avoid a
    destructive checkout/reset during recovery — can parse it without
    writing anything back to the working tree.
    """
    tasks: list[QueueTask] = []
    current: dict | None = None

    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        header_match = _HEADER_RE.match(line)
        if header_match:
            if current is not None:
                tasks.append(_build_task(current))
            current = {
                "pr_id": header_match.group(1),
                "title": header_match.group(2),
                "status": TaskStatus.TODO,
                "task_file": None,
                "depends_on": [],
                "branch": None,
            }
            continue

        if current is None:
            continue

        field_match = _FIELD_RE.match(line)
        if not field_match:
            continue

        key = field_match.group(1).strip().lower()
        value = field_match.group(2).strip()

        if key == "status":
            try:
                current["status"] = TaskStatus(value.upper())
            except ValueError:
                current["status"] = TaskStatus.TODO
        elif key == "tasks file":
            current["task_file"] = value or None
        elif key == "branch":
            current["branch"] = value or None
        elif key == "depends on":
            current["depends_on"] = [
                dep.strip() for dep in value.split(",") if dep.strip()
            ]

    if current is not None:
        tasks.append(_build_task(current))

    return tasks


def get_next_task(tasks: list[QueueTask]) -> QueueTask | None:
    """Pick the next task to work on from a parsed queue.

    - If any task is DOING, return the earliest DOING task.
    - Otherwise, return the earliest TODO task whose dependencies are all DONE.
    - Return None if no task is eligible.
    """
    for task in tasks:
        if task.status == TaskStatus.DOING:
            return task

    done_ids = {task.pr_id for task in tasks if task.status == TaskStatus.DONE}

    for task in tasks:
        if task.status != TaskStatus.TODO:
            continue
        if all(dep in done_ids for dep in task.depends_on):
            return task

    return None


def mark_task_done(content: str, pr_id: str) -> str | None:
    """Return ``content`` with ``pr_id``'s status flipped to ``DONE``.

    Returns ``None`` when the task is absent, already ``DONE`` in every
    status line, or has no status line at all. Matches the same flexible
    field ordering/case/spacing as ``parse_queue_text``: finds the task
    by its ``## {pr_id}:`` header and rewrites every ``- status:`` line
    within that section to ``DONE``.

    Whole-section sweep rather than first-match so behaviour mirrors
    ``parse_queue_text``, which walks every field line and lets later
    ``- Status:`` entries overwrite earlier ones — if a malformed
    section contains duplicates (e.g. ``DONE`` followed by ``TODO``)
    the parser would still consider the task selectable, so the
    remediation path must flip every occurrence. Non-``DONE`` tokens
    (including malformed values like ``TODO,``) are also updated, so
    entries that parser-fallback treats as ``TODO`` cannot silently
    remain selectable.
    """
    lines = content.splitlines(keepends=True)
    in_target = False
    status_indices: list[int] = []
    for i, raw in enumerate(lines):
        stripped = raw.rstrip("\r\n")
        header = _HEADER_RE.match(stripped)
        if header:
            if in_target:
                break
            in_target = header.group(1) == pr_id
            continue
        if not in_target:
            continue
        if _STATUS_LINE_RE.match(stripped):
            status_indices.append(i)

    if not status_indices:
        return None

    def _value_upper(idx: int) -> str:
        body = lines[idx].rstrip("\r\n")
        match = _STATUS_LINE_RE.match(body)
        assert match is not None
        # parse_queue_text reads the whole value after the colon and
        # falls back to TODO when it does not match a TaskStatus member,
        # so a line like `- Status: DONE # merged` is selectable. Join
        # groups(2)+(3) here so the "already DONE" check sees the full
        # value and rewrites decorated tokens instead of skipping them.
        return (match.group(2) + match.group(3)).strip().upper()

    if all(_value_upper(i) == "DONE" for i in status_indices):
        return None

    for i in status_indices:
        raw = lines[i]
        if raw.endswith("\r\n"):
            ending = "\r\n"
        elif raw.endswith("\n"):
            ending = "\n"
        else:
            ending = ""
        body = raw[: len(raw) - len(ending)]
        match = _STATUS_LINE_RE.match(body)
        assert match is not None
        lines[i] = f"{match.group(1)}DONE{ending}"
    return "".join(lines)


def _build_task(data: dict) -> QueueTask:
    return QueueTask(
        pr_id=data["pr_id"],
        title=data["title"],
        status=data["status"],
        task_file=data["task_file"],
        depends_on=data["depends_on"],
        branch=data["branch"],
    )
