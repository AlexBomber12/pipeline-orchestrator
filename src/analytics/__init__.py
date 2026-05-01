"""Append-only structured outcome log for merged PRs.

The package provides per-month JSONL persistence for merged-PR outcomes
under ``/data/analytics/``. Storage is pure persistence: no analysis
layer, no upload, no telemetry. The schema is documented in
``docs/analytics-schema.md`` and intentionally records the
``coder``/``coder_model_string``/``coder_extension_version`` triple so
future analytics can default-filter to the current model version and
avoid unsafe cross-version aggregation.
"""

from src.analytics.outcome_logger import log_merged_pr
from src.analytics.schema import (
    OUTCOME_FIELDS,
    OUTCOME_SCHEMA_VERSION,
    validate_outcome_record,
)

__all__ = [
    "OUTCOME_FIELDS",
    "OUTCOME_SCHEMA_VERSION",
    "log_merged_pr",
    "validate_outcome_record",
]
