"""Validate declared small-task budgets without performing any I/O."""

from __future__ import annotations

from collections.abc import Mapping

_REQUIRED_FIELDS = (
    "version",
    "production_lines",
    "test_lines",
    "other_lines",
    "production_files",
    "total_files",
)
_COUNT_FIELDS = _REQUIRED_FIELDS[1:]
_LINE_FIELDS = ("production_lines", "test_lines", "other_lines")
_ALLOWED_FIELDS = frozenset((*_REQUIRED_FIELDS, "rationale"))
_MISSING = object()


def _is_nonnegative_integer(value: object) -> bool:
    return type(value) is int and value >= 0


def validate_task_budget(budget: object, *, complexity: str) -> list[str]:
    """Return all applicable errors in a declared version 1 task budget."""
    errors: list[str] = []
    rationale: object = _MISSING

    if not isinstance(budget, Mapping):
        errors.append("task_budget must be a mapping")
    else:
        for field in _REQUIRED_FIELDS:
            if field not in budget:
                errors.append(f"task_budget is missing required field: {field}")

        unknown_fields = sorted(
            (repr(field) for field in budget if field not in _ALLOWED_FIELDS)
        )
        if unknown_fields:
            errors.append(
                "task_budget contains unknown fields: " + ", ".join(unknown_fields)
            )

        if "version" in budget:
            version = budget["version"]
            if type(version) is not int or version != 1:
                errors.append("version must be integer 1")

        valid_counts: dict[str, int] = {}
        for field in _COUNT_FIELDS:
            if field not in budget:
                continue
            value = budget[field]
            if not _is_nonnegative_integer(value):
                errors.append(f"{field} must be a nonnegative integer")
            else:
                valid_counts[field] = value

        production_lines = valid_counts.get("production_lines")
        if production_lines is not None and production_lines > 200:
            errors.append(
                f"production_lines must be <= 200; got {production_lines}"
            )

        if all(field in valid_counts for field in _LINE_FIELDS):
            total_lines = sum(valid_counts[field] for field in _LINE_FIELDS)
            if total_lines > 500:
                errors.append(
                    "production_lines + test_lines + other_lines must be <= 500; "
                    f"got {total_lines}"
                )

        production_files = valid_counts.get("production_files")
        total_files = valid_counts.get("total_files")
        if production_files is not None and production_files > 3:
            errors.append(
                f"production_files must be <= 3; got {production_files}"
            )
        if total_files is not None and total_files > 6:
            errors.append(f"total_files must be <= 6; got {total_files}")
        if (
            production_files is not None
            and total_files is not None
            and production_files > total_files
        ):
            errors.append(
                "production_files must be <= total_files; "
                f"got {production_files} > {total_files}"
            )

        rationale = budget.get("rationale", _MISSING)

    if complexity == "low":
        if rationale is not _MISSING and not isinstance(rationale, str):
            errors.append("rationale must be a string when provided for low complexity")
    elif complexity == "medium":
        if not isinstance(rationale, str) or not rationale.strip():
            errors.append(
                "rationale must be a nonempty string for medium complexity"
            )
    elif complexity == "high":
        errors.append("high complexity is not allowed; split the task")
    else:
        errors.append(
            f"complexity must be 'low' or 'medium'; got {complexity!r}"
        )

    return errors
