"""Dependency DAG helpers for task header selection."""

from __future__ import annotations

import re

from src.models import TaskStatus
from src.queue_parser import TaskHeader

_PR_NUMBER_RE = re.compile(r"^PR-(\d+)(.*)$")


def build_task_dag(headers: list[TaskHeader]) -> dict[str, list[str]]:
    """Build a dependency adjacency list from task headers.

    Raises ``ValueError`` when task identifiers are duplicated, when a task
    references an unknown dependency, or when a dependency cycle exists.
    """
    dag: dict[str, list[str]] = {}

    for header in headers:
        if header.pr_id in dag:
            raise ValueError(f"duplicate task id: {header.pr_id}")
        dag[header.pr_id] = list(header.depends_on)

    known_ids = set(dag)
    for pr_id, dependencies in dag.items():
        for dependency in dependencies:
            if dependency not in known_ids:
                raise ValueError(
                    f"{pr_id} depends on unknown task {dependency}"
                )

    cycle = detect_cycle(dag)
    if cycle is not None:
        raise ValueError("dependency cycle: " + " -> ".join(cycle))

    return dag


def get_eligible_tasks(
    headers: list[TaskHeader],
    statuses: dict[str, TaskStatus],
) -> list[TaskHeader]:
    """Return TODO tasks whose dependencies are all DONE.

    Results are sorted by ascending priority (1 is highest priority), then by
    ascending PR number within the same priority.
    """
    dag = build_task_dag(headers)
    done_ids = {
        pr_id for pr_id, status in statuses.items() if status == TaskStatus.DONE
    }

    eligible = [
        header
        for header in headers
        if statuses.get(header.pr_id) == TaskStatus.TODO
        and all(dependency in done_ids for dependency in dag[header.pr_id])
    ]
    eligible.sort(key=lambda header: (header.priority, _pr_sort_key(header.pr_id)))
    return eligible


def detect_cycle(dag: dict[str, list[str]]) -> list[str] | None:
    """Return a cycle path from an adjacency list, or ``None`` when acyclic."""
    visited: set[str] = set()

    for start in dag:
        if start in visited:
            continue

        visiting: set[str] = {start}
        path: list[str] = [start]
        stack: list[tuple[str, int]] = [(start, 0)]

        while stack:
            node, index = stack[-1]
            neighbors = dag.get(node, [])

            if index >= len(neighbors):
                stack.pop()
                path.pop()
                visiting.remove(node)
                visited.add(node)
                continue

            neighbor = neighbors[index]
            stack[-1] = (node, index + 1)

            if neighbor in visiting:
                cycle_start = path.index(neighbor)
                return path[cycle_start:] + [neighbor]

            if neighbor in visited or neighbor not in dag:
                continue

            visiting.add(neighbor)
            path.append(neighbor)
            stack.append((neighbor, 0))

    return None


def _pr_sort_key(pr_id: str) -> tuple[int, int | str, str]:
    match = _PR_NUMBER_RE.match(pr_id)
    if match is None:
        return (1, pr_id, "")
    return (0, int(match.group(1)), match.group(2))
