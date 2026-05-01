"""Schema definition and validator for merged-PR outcome records.

The schema is append-only: new fields may be added at the end of
``OUTCOME_FIELDS`` and ``OUTCOME_FIELD_TYPES`` and ``OUTCOME_SCHEMA_VERSION``
must be bumped. Existing fields must never change name or type — historic
JSONL files would otherwise become unparseable. Renaming a field requires
a migration script over the existing partitions.
"""

from __future__ import annotations

# Schema version. Increment when a new field is appended; never reuse a
# value. Future analytics layers may use this to gate per-version readers.
OUTCOME_SCHEMA_VERSION: int = 1

# Per-field type constraint. ``None`` is always permitted alongside the
# declared type — missing data is written as ``null`` (never omitted) per
# the schema contract.
OUTCOME_FIELD_TYPES: dict[str, type | tuple[type, ...]] = {
    "pr_id": str,
    "task_id_hash": str,
    "repo_slug": str,
    "merged_at": str,
    "coder": str,
    "coder_model_string": str,
    "coder_extension_version": str,
    "task_type": str,
    "task_complexity": str,
    "fix_iterations": int,
    "ci_runs_total": int,
    "ci_runs_failed": int,
    "wall_clock_seconds": int,
    "files_changed": int,
    "lines_added": int,
    "lines_removed": int,
    "review_blocker_count": int,
    "review_nit_count": int,
    "codex_review_iterations": int,
    "tokens_estimate": int,
    "outcome": str,
}

# Canonical field order. JSON files are written with sorted keys for
# stable diffs, but this list documents the intended schema and is used
# by ``validate_outcome_record`` to assert presence.
OUTCOME_FIELDS: list[str] = list(OUTCOME_FIELD_TYPES)


class OutcomeValidationError(ValueError):
    """Raised when an outcome record fails schema validation."""


def validate_outcome_record(record: dict) -> None:
    """Validate ``record`` against the outcome schema.

    Required: every key in ``OUTCOME_FIELDS`` is present and the value is
    either ``None`` or matches the declared type. Extra keys are rejected
    so a typo in a new caller cannot silently drop data into the log.
    """
    if not isinstance(record, dict):
        raise OutcomeValidationError(
            f"record must be a dict, got {type(record).__name__}"
        )
    missing = [field for field in OUTCOME_FIELDS if field not in record]
    if missing:
        raise OutcomeValidationError(
            f"missing required fields: {missing}"
        )
    extra = [key for key in record if key not in OUTCOME_FIELD_TYPES]
    if extra:
        raise OutcomeValidationError(
            f"unknown fields: {extra}"
        )
    type_errors: list[str] = []
    for field, expected in OUTCOME_FIELD_TYPES.items():
        value = record[field]
        if value is None:
            continue
        # ``bool`` is a subclass of ``int`` in Python; reject it explicitly
        # for numeric fields so a True/False sneaking in cannot be silently
        # written as 1/0 in the JSONL.
        if expected is int and isinstance(value, bool):
            type_errors.append(
                f"{field}: bool not allowed for int field"
            )
            continue
        if not isinstance(value, expected):
            type_errors.append(
                f"{field}: expected {expected.__name__ if isinstance(expected, type) else expected}, "
                f"got {type(value).__name__}"
            )
    if type_errors:
        raise OutcomeValidationError(
            "type errors: " + "; ".join(type_errors)
        )
