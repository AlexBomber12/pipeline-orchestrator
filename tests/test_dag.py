from __future__ import annotations

from src.dag import build_task_dag, detect_cycle, get_eligible_tasks
from src.models import TaskStatus
from src.queue_parser import TaskHeader


def _header(
    pr_id: str,
    *,
    depends_on: list[str] | None = None,
    priority: int = 3,
) -> TaskHeader:
    suffix = pr_id.lower().replace(".", "-")
    return TaskHeader(
        pr_id=pr_id,
        title=f"{pr_id} task",
        branch=f"{suffix}-branch",
        task_type="feature",
        complexity="medium",
        depends_on=list(depends_on or []),
        priority=priority,
        coder="any",
    )


def test_eligible_respects_dependencies() -> None:
    headers = [
        _header("PR-001"),
        _header("PR-002", depends_on=["PR-001"]),
    ]

    eligible = get_eligible_tasks(
        headers,
        {
            "PR-001": TaskStatus.DONE,
            "PR-002": TaskStatus.TODO,
        },
    )

    assert [header.pr_id for header in eligible] == ["PR-002"]


def test_eligible_blocks_unmet_deps() -> None:
    headers = [
        _header("PR-001"),
        _header("PR-002", depends_on=["PR-001"]),
    ]

    eligible = get_eligible_tasks(
        headers,
        {
            "PR-001": TaskStatus.DOING,
            "PR-002": TaskStatus.TODO,
        },
    )

    assert eligible == []


def test_eligible_sorted_by_priority() -> None:
    headers = [
        _header("PR-003", priority=3),
        _header("PR-001", priority=1),
    ]

    eligible = get_eligible_tasks(
        headers,
        {
            "PR-001": TaskStatus.TODO,
            "PR-003": TaskStatus.TODO,
        },
    )

    assert [header.pr_id for header in eligible] == ["PR-001", "PR-003"]


def test_eligible_sorted_by_pr_number_within_priority() -> None:
    headers = [
        _header("PR-010", priority=2),
        _header("PR-002", priority=2),
        _header("PR-001", priority=2),
    ]

    eligible = get_eligible_tasks(
        headers,
        {
            "PR-010": TaskStatus.TODO,
            "PR-002": TaskStatus.TODO,
            "PR-001": TaskStatus.TODO,
        },
    )

    assert [header.pr_id for header in eligible] == [
        "PR-001",
        "PR-002",
        "PR-010",
    ]


def test_cycle_detection() -> None:
    dag = {
        "PR-001": ["PR-002"],
        "PR-002": ["PR-003"],
        "PR-003": ["PR-001"],
    }

    assert detect_cycle(dag) == ["PR-001", "PR-002", "PR-003", "PR-001"]


def test_no_cycle_in_valid_dag() -> None:
    headers = [
        _header("PR-001"),
        _header("PR-002", depends_on=["PR-001"]),
        _header("PR-003", depends_on=["PR-002"]),
    ]

    dag = build_task_dag(headers)

    assert dag == {
        "PR-001": [],
        "PR-002": ["PR-001"],
        "PR-003": ["PR-002"],
    }
    assert detect_cycle(dag) is None


def test_build_task_dag_rejects_duplicate_task_ids() -> None:
    headers = [
        _header("PR-001"),
        _header("PR-001"),
    ]

    try:
        build_task_dag(headers)
    except ValueError as exc:
        assert str(exc) == "duplicate task id: PR-001"
    else:
        raise AssertionError("expected duplicate task id to raise ValueError")


def test_build_task_dag_rejects_unknown_dependencies() -> None:
    headers = [
        _header("PR-001", depends_on=["PR-999"]),
    ]

    try:
        build_task_dag(headers)
    except ValueError as exc:
        assert str(exc) == "PR-001 depends on unknown task PR-999"
    else:
        raise AssertionError("expected unknown dependency to raise ValueError")


def test_build_task_dag_rejects_dependency_cycles() -> None:
    headers = [
        _header("PR-001", depends_on=["PR-002"]),
        _header("PR-002", depends_on=["PR-001"]),
    ]

    try:
        build_task_dag(headers)
    except ValueError as exc:
        assert str(exc) == "dependency cycle: PR-001 -> PR-002 -> PR-001"
    else:
        raise AssertionError("expected dependency cycle to raise ValueError")


def test_detect_cycle_skips_already_visited_components() -> None:
    dag = {
        "PR-003": ["PR-002"],
        "PR-002": ["PR-001"],
        "PR-001": [],
    }

    assert detect_cycle(dag) is None


def test_eligible_sorts_non_standard_pr_ids_after_standard_ones() -> None:
    headers = [
        _header("PR-010", priority=1),
        _header("TASK-002", priority=1),
        _header("PR-002", priority=1),
    ]

    eligible = get_eligible_tasks(
        headers,
        {
            "PR-010": TaskStatus.TODO,
            "TASK-002": TaskStatus.TODO,
            "PR-002": TaskStatus.TODO,
        },
    )

    assert [header.pr_id for header in eligible] == [
        "PR-002",
        "PR-010",
        "TASK-002",
    ]
