from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest
from src.cancellation import SUBSOURCE_VOCABULARY
from src.subsource_registry import (
    SubsourceMetadata,
    all_subsources,
    canonical_subsources,
    group_for,
    lookup,
)

_KNOWN_GROUP_BUCKETS = {"coder", "daemon", "guardrail", "operator_reject"}
_KNOWN_SEVERITIES = {"low", "medium", "high"}


def test_registry_contains_all_subsources() -> None:
    assert len(all_subsources()) == 14


def test_canonical_subsources_match_vocabulary() -> None:
    assert canonical_subsources() == SUBSOURCE_VOCABULARY


@pytest.mark.parametrize("name", sorted(all_subsources()))
def test_lookup_returns_metadata_for_each_known_name(name: str) -> None:
    meta = lookup(name)
    assert meta is not None
    assert isinstance(meta, SubsourceMetadata)
    assert meta.name == name


def test_lookup_returns_none_for_unknown_name() -> None:
    assert lookup("not_a_real_subsource") is None


@pytest.mark.parametrize("name", sorted(all_subsources()))
def test_each_entry_has_nonempty_user_label_and_recovery_hint(name: str) -> None:
    meta = lookup(name)
    assert meta is not None
    assert meta.user_label
    assert meta.recovery_hint


def test_legacy_category_present_for_pre_migration_subsources_only() -> None:
    for legacy_name in (
        "crash",
        "coder_escalate",
        "guardrail",
        "review_timeout",
        "fix_idle_timeout",
        "fix_iteration_cap",
        "no_push_deadlock",
        "infra_failure",
    ):
        meta = lookup(legacy_name)
        assert meta is not None
        assert meta.legacy_category is not None

    for postdate_name in (
        "watch_retrigger_cap",
        "operator_reject",
        "diagnose_exhausted",
        "operator_stopped",
        "rate_limit",
    ):
        meta = lookup(postdate_name)
        assert meta is not None
        assert meta.legacy_category is None


@pytest.mark.parametrize("name", sorted(all_subsources()))
def test_group_for_returns_known_bucket(name: str) -> None:
    bucket = group_for(name)
    assert bucket in _KNOWN_GROUP_BUCKETS


def test_group_for_returns_none_for_unknown_name() -> None:
    assert group_for("not_a_real") is None


def test_metadata_is_frozen() -> None:
    meta = lookup("crash")
    assert meta is not None
    hash(meta)
    with pytest.raises(FrozenInstanceError):
        meta.name = "mutated"  # type: ignore[misc]


@pytest.mark.parametrize("name", sorted(all_subsources()))
def test_severity_values_are_constrained(name: str) -> None:
    meta = lookup(name)
    assert meta is not None
    assert meta.severity in _KNOWN_SEVERITIES
