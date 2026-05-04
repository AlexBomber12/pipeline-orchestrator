"""Tests for src.cancellation.blocked_set (PR-257)."""

from __future__ import annotations

from src.cancellation.blocked_set import (
    TaskNode,
    compute_blocked_set,
    compute_dependents_count,
)


def _node(task_id: str, deps: list[str], canceled: bool = False) -> TaskNode:
    return TaskNode(task_id=task_id, depends_on=deps, is_canceled=canceled)


def test_simple_chain() -> None:
    tasks = [
        _node("A", [], canceled=True),
        _node("B", ["A"]),
        _node("C", ["B"]),
    ]
    assert compute_blocked_set(tasks) == {"B": "A", "C": "A"}


def test_diamond_dependency() -> None:
    tasks = [
        _node("A", [], canceled=True),
        _node("B", ["A"]),
        _node("C", ["A"]),
        _node("D", ["B", "C"]),
    ]
    assert compute_blocked_set(tasks) == {"B": "A", "C": "A", "D": "A"}


def test_canceled_task_not_in_blocked_set() -> None:
    tasks = [
        _node("A", [], canceled=True),
        _node("B", ["A"]),
    ]
    blocked = compute_blocked_set(tasks)
    assert "A" not in blocked
    assert blocked == {"B": "A"}


def test_dependents_count_aggregates() -> None:
    # A canceled, B-F all depend (transitively) on A.
    tasks = [
        _node("A", [], canceled=True),
        _node("B", ["A"]),
        _node("C", ["A"]),
        _node("D", ["B"]),
        _node("E", ["C"]),
        _node("F", ["D"]),
    ]
    counts = compute_dependents_count(tasks)
    assert counts == {"A": 5}


def test_no_cancellations_empty_result() -> None:
    tasks = [
        _node("A", []),
        _node("B", ["A"]),
        _node("C", ["B"]),
    ]
    assert compute_blocked_set(tasks) == {}
    assert compute_dependents_count(tasks) == {}


def test_circular_dependency_does_not_loop() -> None:
    """Two non-canceled tasks pointing at each other must not infinite-loop."""
    tasks = [
        _node("A", ["B"]),
        _node("B", ["A"]),
    ]
    # Neither is canceled, so neither is blocked. The traversal must
    # terminate via the visited-set guard rather than recursing forever.
    assert compute_blocked_set(tasks) == {}


def test_unknown_dependency_id_ignored() -> None:
    """A depends_on entry that names an unknown task id is silently skipped.

    The traversal walks ``depends_on`` in order; the GHOST entry sits
    before the canceled root so the unknown-id branch fires before the
    early ``return`` finds the canceled match.
    """
    tasks = [
        _node("A", [], canceled=True),
        _node("B", ["GHOST", "A"]),
    ]
    assert compute_blocked_set(tasks) == {"B": "A"}


def test_canceled_root_with_no_dependents() -> None:
    """Canceled task that nothing depends on yields zero count."""
    tasks = [
        _node("A", [], canceled=True),
        _node("B", []),
    ]
    assert compute_blocked_set(tasks) == {}
    assert compute_dependents_count(tasks) == {}


def test_multiple_canceled_roots_separate_counts() -> None:
    tasks = [
        _node("A", [], canceled=True),
        _node("B", [], canceled=True),
        _node("C", ["A"]),
        _node("D", ["B"]),
        _node("E", ["B"]),
    ]
    counts = compute_dependents_count(tasks)
    assert counts == {"A": 1, "B": 2}
