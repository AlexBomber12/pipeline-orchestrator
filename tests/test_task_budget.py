"""Tests for declared small-task budget validation."""

from __future__ import annotations

from copy import deepcopy

import pytest
from src.task_budget import validate_task_budget


def _budget(**overrides: object) -> dict[str, object]:
    budget: dict[str, object] = {
        "version": 1,
        "production_lines": 120,
        "test_lines": 180,
        "other_lines": 40,
        "production_files": 2,
        "total_files": 4,
    }
    budget.update(overrides)
    return budget


@pytest.mark.parametrize(
    ("budget", "complexity"),
    [
        (_budget(), "low"),
        (_budget(production_lines=151), "low"),
        (
            _budget(
                production_lines=200,
                test_lines=250,
                other_lines=50,
                production_files=3,
                total_files=6,
            ),
            "low",
        ),
        (_budget(rationale="  Local and understood.  "), "medium"),
        (_budget(rationale="optional"), "low"),
    ],
)
def test_accepts_valid_budgets(budget: object, complexity: str) -> None:
    assert validate_task_budget(budget, complexity=complexity) == []


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        (
            {"production_lines": 201},
            "production_lines must be <= 200; got 201",
        ),
        (
            {"test_lines": 341},
            "production_lines + test_lines + other_lines must be <= 500; got 501",
        ),
        (
            {"production_files": 4},
            "production_files must be <= 3; got 4",
        ),
        ({"total_files": 7}, "total_files must be <= 6; got 7"),
        (
            {"production_files": 3, "total_files": 2},
            "production_files must be <= total_files; got 3 > 2",
        ),
    ],
)
def test_reports_each_exceeded_limit_independently(
    overrides: dict[str, int], expected: str
) -> None:
    assert validate_task_budget(_budget(**overrides), complexity="low") == [
        expected
    ]


def test_reports_multiple_independent_violations() -> None:
    errors = validate_task_budget(
        _budget(
            production_lines=201,
            test_lines=300,
            other_lines=0,
            production_files=7,
            total_files=2,
        ),
        complexity="high",
    )

    assert errors == [
        "production_lines must be <= 200; got 201",
        "production_lines + test_lines + other_lines must be <= 500; got 501",
        "production_files must be <= 3; got 7",
        "production_files must be <= total_files; got 7 > 2",
        "high complexity is not allowed; split the task",
    ]


@pytest.mark.parametrize("value", [2, True, 1.0, "1", None])
def test_rejects_malformed_version_types(value: object) -> None:
    assert validate_task_budget(_budget(version=value), complexity="low") == [
        "version must be integer 1"
    ]


@pytest.mark.parametrize("value", [True, 2.5, "10", None, -1])
def test_rejects_malformed_count_types_without_arithmetic_errors(
    value: object,
) -> None:
    assert validate_task_budget(
        _budget(test_lines=value), complexity="low"
    ) == ["test_lines must be a nonnegative integer"]


def test_reports_missing_and_sorted_unknown_fields() -> None:
    budget = _budget(a_extra=1, z_extra=2)
    del budget["version"]
    del budget["production_lines"]

    assert validate_task_budget(budget, complexity="low") == [
        "task_budget is missing required field: version",
        "task_budget is missing required field: production_lines",
        "task_budget contains unknown fields: 'a_extra', 'z_extra'",
    ]


@pytest.mark.parametrize("budget", [None, [], "budget"])
def test_rejects_non_mapping_budget(budget: object) -> None:
    assert validate_task_budget(budget, complexity="low") == [
        "task_budget must be a mapping"
    ]


@pytest.mark.parametrize("rationale", [None, 7, [], "", "   "])
def test_medium_requires_nonempty_string_rationale(rationale: object) -> None:
    budget = _budget()
    if rationale is not None:
        budget["rationale"] = rationale

    assert validate_task_budget(budget, complexity="medium") == [
        "rationale must be a nonempty string for medium complexity"
    ]


def test_low_rejects_non_string_rationale() -> None:
    assert validate_task_budget(_budget(rationale=7), complexity="low") == [
        "rationale must be a string when provided for low complexity"
    ]


@pytest.mark.parametrize(
    ("complexity", "expected"),
    [
        ("high", "high complexity is not allowed; split the task"),
        ("LOW", "complexity must be 'low' or 'medium'; got 'LOW'"),
        ("unknown", "complexity must be 'low' or 'medium'; got 'unknown'"),
    ],
)
def test_rejects_high_and_unknown_complexity(
    complexity: str, expected: str
) -> None:
    assert validate_task_budget(_budget(), complexity=complexity) == [expected]


def test_does_not_mutate_input() -> None:
    budget = _budget(rationale="  local  ")
    original = deepcopy(budget)

    validate_task_budget(budget, complexity="medium")

    assert budget == original
