from __future__ import annotations

import pytest
from src.cancellation import (
    _LEGACY_CATEGORY_TO_SUBSOURCE,
    SUBSOURCE_VOCABULARY,
)
from src.daemon.handlers.error import ErrorCategory
from src.subsource_registry import (
    _REGISTRY,
    GroupBucket,
    Severity,
    SuppressionReason,
    error_category_to_reason,
)

_KNOWN_SEVERITIES: set[Severity] = {"low", "medium", "high"}
_KNOWN_GROUP_BUCKETS: set[GroupBucket] = {
    "coder",
    "daemon",
    "guardrail",
    "operator_reject",
}

_ORIGINAL_METADATA_SNAPSHOT = {
    "crash": ("Daemon crash", "daemon"),
    "coder_escalate": ("Coder escalated", "coder"),
    "guardrail": ("Guardrail violation", "guardrail"),
    "review_timeout": ("Stale review", "daemon"),
    "fix_idle_timeout": ("FIX idle timeout", "daemon"),
    "fix_iteration_cap": ("FIX iteration cap", "daemon"),
    "no_push_deadlock": ("No-push deadlock", "daemon"),
    "infra_failure": ("Infrastructure failure", "daemon"),
    "daemon": ("Legacy daemon escalation", "daemon"),
    "watch_retrigger_cap": ("WATCH retrigger cap", "daemon"),
    "operator_reject": ("Operator rejected", "operator_reject"),
}


def test_vocabulary_derives_from_registry() -> None:
    assert SUBSOURCE_VOCABULARY == frozenset(_REGISTRY)


def test_registry_includes_previously_missing() -> None:
    assert {"daemon", "operator_reject", "watch_retrigger_cap"} <= (
        SUBSOURCE_VOCABULARY
    )


def test_guardrail_is_canonical_reason() -> None:
    assert SuppressionReason.GUARDRAIL == "guardrail"
    assert SuppressionReason.GUARDRAIL.value in _REGISTRY


def test_every_error_category_maps() -> None:
    for category in ErrorCategory:
        reason = error_category_to_reason(category)
        assert isinstance(reason, SuppressionReason)
        assert reason.value in _REGISTRY


def test_legacy_map_targets_canonical() -> None:
    for subsource in _LEGACY_CATEGORY_TO_SUBSOURCE.values():
        assert subsource in SUBSOURCE_VOCABULARY


@pytest.mark.parametrize(
    "reason",
    [
        SuppressionReason.DIAGNOSE_EXHAUSTED,
        SuppressionReason.OPERATOR_STOPPED,
        SuppressionReason.RATE_LIMIT,
    ],
)
def test_new_reasons_have_full_metadata(reason: SuppressionReason) -> None:
    meta = _REGISTRY[reason.value]
    assert meta.user_label
    assert meta.severity in _KNOWN_SEVERITIES
    assert meta.recovery_hint
    assert meta.group_bucket in _KNOWN_GROUP_BUCKETS


def test_existing_subsource_metadata_unchanged() -> None:
    for name, (user_label, group_bucket) in _ORIGINAL_METADATA_SNAPSHOT.items():
        meta = _REGISTRY[name]
        assert meta.user_label == user_label
        assert meta.group_bucket == group_bucket


def test_no_import_cycle() -> None:
    import src.cancellation
    import src.daemon.handlers.error
    import src.subsource_registry

    assert src.cancellation.SUBSOURCE_VOCABULARY
    assert src.daemon.handlers.error.ErrorCategory.OTHER
    assert src.subsource_registry.SuppressionReason.GUARDRAIL
