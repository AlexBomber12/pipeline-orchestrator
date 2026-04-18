"""Parser for tasks/QUEUE.md and task selection logic."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path

from src.models import QueueTask, TaskStatus

logger = logging.getLogger(__name__)


class QueueValidationError(ValueError):
    """Raised when queue contents cannot be unambiguously parsed."""

    def __init__(self, issues: list[str]):
        self.issues = issues
        super().__init__(
            "Queue validation failed:\n"
            + "\n".join(f"  - {i}" for i in issues)
        )


_HEADER_RE = re.compile(r"^##\s+(PR-[A-Za-z0-9_.-]+):\s*(.+?)\s*$")
_TASK_HEADER_RE = re.compile(r"^#\s+(PR-[A-Za-z0-9_.-]+):\s*(.+?)\s*$")
_FIELD_RE = re.compile(r"^-\s*([A-Za-z ]+?)\s*:\s*(.*?)\s*$")
_TASK_BRANCH_RE = re.compile(r"^Branch\s*:\s*(.*?)\s*$")
_STATUS_LINE_RE = re.compile(
    r"^(-\s*status\s*:\s*)(\S*)(.*)$", re.IGNORECASE
)
_TASK_TYPE_VALUES = {"bugfix", "feature", "architecture", "refactor", "docs"}
_COMPLEXITY_VALUES = {"low", "medium", "high"}
_CODER_VALUES = {"claude", "codex", "any"}


@dataclass(frozen=True)
class TaskHeader:
    pr_id: str
    title: str
    branch: str
    task_type: str
    complexity: str
    depends_on: list[str]
    priority: int
    coder: str


def parse_queue(
    queue_path: str, *, strict: bool = False
) -> list[QueueTask]:
    """Parse a QUEUE.md file and return its tasks in document order.

    Returns an empty list if the file does not exist.  When *strict* is
    ``True``, unknown status values raise ``QueueValidationError`` instead
    of degrading to TODO, and the parsed queue is validated for duplicates,
    missing dependencies, and cycles.
    """
    path = Path(queue_path)
    if not path.is_file():
        return []
    return parse_queue_text(path.read_text(encoding="utf-8"), strict=strict)


def parse_queue_text(
    text: str, *, strict: bool = False
) -> list[QueueTask]:
    """Parse QUEUE.md content from an in-memory string.

    Same grammar as ``parse_queue``; split out so callers that already
    have QUEUE.md content in memory — e.g. ``PipelineRunner.recover_state``
    reading it from ``origin/{branch}`` via ``git show`` to avoid a
    destructive checkout/reset during recovery — can parse it without
    writing anything back to the working tree.

    When *strict* is ``True``, unknown status values are collected and
    raised as a ``QueueValidationError`` after parsing, and the parsed
    queue is also run through ``validate_queue``.
    """
    tasks: list[QueueTask] = []
    current: dict | None = None
    strict_issues: list[str] = []

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
            raw_status = value.upper()
            if raw_status in {s.value for s in TaskStatus}:
                current["status"] = TaskStatus(raw_status)
            elif strict:
                strict_issues.append(
                    f"unknown status {value!r} for {current['pr_id']}"
                )
                current["status"] = TaskStatus.TODO
            else:
                logger.warning(
                    "Unknown status %r for %s, treating as TODO",
                    raw_status,
                    current["pr_id"],
                )
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

    if strict:
        validate_queue(tasks, _extra_issues=strict_issues)

    return tasks


def parse_task_header(path: str | Path) -> TaskHeader:
    """Parse structured metadata from a task file header."""
    task_path = Path(path)
    lines = task_path.read_text(encoding="utf-8").splitlines()
    issues: list[str] = []
    header_match: re.Match[str] | None = None
    fields: dict[str, str] = {}

    for raw_line in lines:
        line = raw_line.rstrip()
        if header_match is None:
            header_match = _TASK_HEADER_RE.match(line)
            continue

        branch_match = _TASK_BRANCH_RE.match(line)
        if branch_match:
            fields["branch"] = branch_match.group(1).strip()
            continue

        field_match = _FIELD_RE.match(line)
        if not field_match:
            continue
        key = field_match.group(1).strip().lower()
        fields[key] = field_match.group(2).strip()

    if header_match is None:
        raise QueueValidationError(
            [f"{task_path}: missing task header like '# PR-123: Title'"]
        )

    pr_id = header_match.group(1)
    title = header_match.group(2)

    branch = fields.get("branch")
    if not branch:
        issues.append(f"{task_path}: missing Branch")

    task_type = fields.get("type")
    if not task_type:
        issues.append(f"{task_path}: missing Type")
    elif task_type not in _TASK_TYPE_VALUES:
        issues.append(
            f"{task_path}: invalid Type {task_type!r}; expected one of "
            f"{sorted(_TASK_TYPE_VALUES)}"
        )

    complexity = fields.get("complexity")
    if not complexity:
        issues.append(f"{task_path}: missing Complexity")
    elif complexity not in _COMPLEXITY_VALUES:
        issues.append(
            f"{task_path}: invalid Complexity {complexity!r}; expected one of "
            f"{sorted(_COMPLEXITY_VALUES)}"
        )

    depends_raw = fields.get("depends on")
    if depends_raw is None:
        issues.append(f"{task_path}: missing Depends on")
        depends_on: list[str] = []
    elif depends_raw.lower() == "none":
        depends_on = []
    else:
        depends_on = [
            dep.strip() for dep in depends_raw.split(",") if dep.strip()
        ]

    priority_raw = fields.get("priority")
    if priority_raw in {None, ""}:
        priority = 3
    else:
        try:
            priority = int(priority_raw)
        except ValueError:
            issues.append(
                f"{task_path}: invalid Priority {priority_raw!r}; expected integer"
            )
            priority = 3
        else:
            if not 1 <= priority <= 5:
                issues.append(
                    f"{task_path}: invalid Priority {priority}; expected 1-5"
                )

    coder = fields.get("coder") or "any"
    if coder not in _CODER_VALUES:
        issues.append(
            f"{task_path}: invalid Coder {coder!r}; expected one of "
            f"{sorted(_CODER_VALUES)}"
        )

    if issues:
        raise QueueValidationError(issues)

    return TaskHeader(
        pr_id=pr_id,
        title=title,
        branch=branch,
        task_type=task_type,
        complexity=complexity,
        depends_on=depends_on,
        priority=priority,
        coder=coder,
    )


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


def validate_queue(
    tasks: list[QueueTask],
    *,
    _extra_issues: list[str] | None = None,
) -> None:
    """Raise ``QueueValidationError`` if the parsed queue is ambiguous.

    Checks: no duplicate ``pr_id``, no duplicate ``branch``, all
    dependencies reference known ``pr_id`` values, no dependency cycles.
    ``_extra_issues`` is an internal hook for ``parse_queue_text`` to
    forward status-parse errors collected during the parsing pass.
    """
    issues: list[str] = list(_extra_issues) if _extra_issues else []

    # Duplicate pr_id
    seen_ids: dict[str, int] = {}
    for idx, task in enumerate(tasks, start=1):
        if task.pr_id in seen_ids:
            issues.append(
                f"duplicate pr_id {task.pr_id!r} at task #{idx} "
                f"(first seen at task #{seen_ids[task.pr_id]})"
            )
        else:
            seen_ids[task.pr_id] = idx

    # Duplicate branch
    seen_branches: dict[str, str] = {}
    for task in tasks:
        if not task.branch:
            continue
        if task.branch in seen_branches:
            issues.append(
                f"duplicate branch {task.branch!r}: used by "
                f"{seen_branches[task.branch]} and {task.pr_id}"
            )
        else:
            seen_branches[task.branch] = task.pr_id

    # Missing dependencies
    known_ids = set(seen_ids.keys())
    for task in tasks:
        for dep in task.depends_on:
            if dep not in known_ids:
                issues.append(
                    f"{task.pr_id} depends on unknown task {dep!r}"
                )

    # Cycle detection
    cycle = _find_dependency_cycle(tasks)
    if cycle:
        issues.append("dependency cycle: " + " -> ".join(cycle))

    if issues:
        raise QueueValidationError(issues)


def _find_dependency_cycle(tasks: list[QueueTask]) -> list[str] | None:
    """Return cycle path if a dependency cycle exists, else None.

    Uses iterative DFS to avoid RecursionError on large queues.
    """
    graph = {t.pr_id: list(t.depends_on) for t in tasks}
    visited: set[str] = set()

    for start in graph:
        if start in visited:
            continue
        visiting: set[str] = set()
        path: list[str] = []
        # Stack entries: (node, neighbor_index)
        stack: list[tuple[str, int]] = [(start, 0)]
        visiting.add(start)
        path.append(start)

        while stack:
            node, idx = stack[-1]
            neighbors = graph.get(node, [])
            if idx < len(neighbors):
                stack[-1] = (node, idx + 1)
                neighbor = neighbors[idx]
                if neighbor in visiting:
                    cycle_start = path.index(neighbor)
                    return path[cycle_start:] + [neighbor]
                if neighbor not in visited:
                    visiting.add(neighbor)
                    path.append(neighbor)
                    stack.append((neighbor, 0))
            else:
                stack.pop()
                path.pop()
                visiting.remove(node)
                visited.add(node)

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
