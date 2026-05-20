"""Tests for task header type and complexity synonyms."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.queue_parser import QueueValidationError, parse_task_header


def _write_task_file(
    tmp_path: Path, *, task_type: str = "feature", complexity: str = "medium"
) -> Path:
    task_path = tmp_path / "PR-999.md"
    task_path.write_text(
        f"""# PR-999: Synonym coverage

Branch: pr-999-synonym-coverage
- Type: {task_type}
- Complexity: {complexity}
- Depends on: none
- Priority: 2
- Coder: codex
""",
        encoding="utf-8",
    )
    return task_path


def test_type_infra_maps_to_config(tmp_path: Path) -> None:
    header = parse_task_header(_write_task_file(tmp_path, task_type="infra"))
    assert header.task_type == "config"


def test_type_infra_uppercase_maps_to_config(tmp_path: Path) -> None:
    header = parse_task_header(_write_task_file(tmp_path, task_type="Infra"))
    assert header.task_type == "config"


@pytest.mark.parametrize(
    ("synonym", "canonical"),
    [
        ("bug", "bugfix"),
        ("fix", "bugfix"),
        ("chore", "refactor"),
        ("feat", "feature"),
        ("task", "feature"),
    ],
)
def test_existing_type_synonyms_unchanged(
    tmp_path: Path, synonym: str, canonical: str
) -> None:
    header = parse_task_header(_write_task_file(tmp_path, task_type=synonym))
    assert header.task_type == canonical


def test_complexity_small_maps_to_low(tmp_path: Path) -> None:
    header = parse_task_header(_write_task_file(tmp_path, complexity="small"))
    assert header.complexity == "low"


def test_complexity_large_maps_to_high(tmp_path: Path) -> None:
    header = parse_task_header(_write_task_file(tmp_path, complexity="large"))
    assert header.complexity == "high"


@pytest.mark.parametrize(
    ("synonym", "canonical"),
    [
        ("s", "low"),
        ("m", "medium"),
        ("l", "high"),
        ("xs", "low"),
        ("xl", "high"),
    ],
)
def test_complexity_t_shirt_sizes(
    tmp_path: Path, synonym: str, canonical: str
) -> None:
    header = parse_task_header(_write_task_file(tmp_path, complexity=synonym))
    assert header.complexity == canonical


@pytest.mark.parametrize(
    ("synonym", "canonical"),
    [
        ("Small", "low"),
        ("XL", "high"),
    ],
)
def test_complexity_uppercase_maps(
    tmp_path: Path, synonym: str, canonical: str
) -> None:
    header = parse_task_header(_write_task_file(tmp_path, complexity=synonym))
    assert header.complexity == canonical


@pytest.mark.parametrize("complexity", ["low", "medium", "high"])
def test_canonical_complexity_values_pass_through(
    tmp_path: Path, complexity: str
) -> None:
    header = parse_task_header(_write_task_file(tmp_path, complexity=complexity))
    assert header.complexity == complexity


@pytest.mark.parametrize(
    "task_type",
    ["bugfix", "feature", "refactor", "config", "docs", "architecture", "ux"],
)
def test_canonical_type_values_pass_through(tmp_path: Path, task_type: str) -> None:
    header = parse_task_header(_write_task_file(tmp_path, task_type=task_type))
    assert header.task_type == task_type


def test_unknown_complexity_raises(tmp_path: Path) -> None:
    with pytest.raises(QueueValidationError, match="expected one of"):
        parse_task_header(_write_task_file(tmp_path, complexity="xxxxl"))


def test_unknown_type_raises(tmp_path: Path) -> None:
    with pytest.raises(QueueValidationError, match="expected one of"):
        parse_task_header(_write_task_file(tmp_path, task_type="morple"))


def test_validate_task_spec_accepts_synonyms() -> None:
    from src.mcp.tools.functional import validate_task_spec

    result = validate_task_spec(
        """# PR-999: Synonym validation

Branch: pr-999-synonym-validation
- Type: infra
- Complexity: small
- Depends on: none
- Priority: 2
- Coder: codex

## Problem

Example.
"""
    )

    assert result == {
        "valid": True,
        "errors": [],
        "schema_errors": [],
        "agents_violations": [],
    }
