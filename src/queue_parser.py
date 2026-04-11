"""Parser for tasks/QUEUE.md and task selection logic."""

from __future__ import annotations

import re
from pathlib import Path

from src.models import QueueTask, TaskStatus

_HEADER_RE = re.compile(r"^##\s+(PR-[A-Za-z0-9_.-]+):\s*(.+?)\s*$")
_FIELD_RE = re.compile(r"^-\s*([A-Za-z ]+?)\s*:\s*(.*?)\s*$")


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


def _build_task(data: dict) -> QueueTask:
    return QueueTask(
        pr_id=data["pr_id"],
        title=data["title"],
        status=data["status"],
        task_file=data["task_file"],
        depends_on=data["depends_on"],
        branch=data["branch"],
    )
