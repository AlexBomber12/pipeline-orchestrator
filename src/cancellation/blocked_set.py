"""Dependency-aware blocked set computation.

When a task is canceled, transitively dependent tasks become blocked
until the canceled root is reactivated or dependents are updated.
This module computes the closure across the repo's task graph so the
dashboard can surface "what is blocked by what".

PR-257.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable


@dataclass
class TaskNode:
    task_id: str
    depends_on: list[str]
    is_canceled: bool


def compute_blocked_set(tasks: Iterable[TaskNode]) -> dict[str, str]:
    """Return mapping ``{blocked_task_id: blocking_canceled_root_id}``.

    A task is blocked if any task in its transitive ``Depends-on``
    closure is canceled. The mapping points to the first canceled root
    encountered while walking the closure (closest in the dependency
    chain by traversal order). Tasks that are themselves canceled are
    not included in the result — they are roots, not blocked.

    PR-257.
    """
    by_id = {t.task_id: t for t in tasks}
    result: dict[str, str] = {}

    def find_canceled_root(start: str) -> str | None:
        visited: set[str] = set()
        stack = [start]
        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)
            node = by_id[current]
            for dep in node.depends_on:
                dep_node = by_id.get(dep)
                if dep_node is None:
                    continue
                if dep_node.is_canceled:
                    return dep
                stack.append(dep)
        return None

    for task in by_id.values():
        if task.is_canceled:
            continue
        root = find_canceled_root(task.task_id)
        if root is not None:
            result[task.task_id] = root
    return result


def compute_dependents_count(tasks: Iterable[TaskNode]) -> dict[str, int]:
    """Return ``{task_id: count_of_blocked_descendants}`` for canceled tasks.

    For each canceled task, counts how many non-canceled tasks have it
    as the blocking root in their transitive Depends-on closure. Used
    for dashboard sort: canceled tasks blocking many dependents are
    higher priority to unblock.
    """
    blocked = compute_blocked_set(tasks)
    counts: dict[str, int] = defaultdict(int)
    for _blocked_id, root_id in blocked.items():
        counts[root_id] += 1
    return dict(counts)
