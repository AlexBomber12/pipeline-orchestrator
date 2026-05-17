"""Typed metadata registry for cancellation subsource vocabulary.

Single source of truth for the 11 subsource strings emitted by daemon
write sites and rendered by operator-facing surfaces. Replaces scattered
inline metadata previously duplicated across cancellation_card.html
template branches, dashboard._SUBSOURCE_FILTER_GROUPS, and
runner._SUBSOURCE_TO_LEGACY_RUN_CAUSE.

Per PR-307a follow-up debt comment in src/daemon/notifications.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Severity = Literal["low", "medium", "high"]
GroupBucket = Literal["coder", "daemon", "guardrail", "operator_reject"]


@dataclass(frozen=True)
class SubsourceMetadata:
    name: str
    user_label: str
    severity: Severity
    recovery_hint: str
    group_bucket: GroupBucket
    legacy_category: str | None
    is_canonical: bool


_REGISTRY: dict[str, SubsourceMetadata] = {
    "crash": SubsourceMetadata(
        name="crash",
        user_label="Daemon crash",
        severity="high",
        recovery_hint="Check daemon logs for crash cause then Retry.",
        group_bucket="daemon",
        legacy_category="CRASH",
        is_canonical=True,
    ),
    "coder_escalate": SubsourceMetadata(
        name="coder_escalate",
        user_label="Coder escalated",
        severity="medium",
        recovery_hint="Review coder stdout for ESCALATE reason then revise spec or Retry.",
        group_bucket="coder",
        legacy_category="ESCALATE",
        is_canonical=True,
    ),
    "guardrail": SubsourceMetadata(
        name="guardrail",
        user_label="Guardrail violation",
        severity="high",
        recovery_hint="Review guardrail finding in cancellation payload then revise spec.",
        group_bucket="guardrail",
        legacy_category="ESCALATE",
        is_canonical=True,
    ),
    "review_timeout": SubsourceMetadata(
        name="review_timeout",
        user_label="Stale review",
        severity="medium",
        recovery_hint="Push a manual review or close the PR then Retry.",
        group_bucket="coder",
        legacy_category="TIMEOUT",
        is_canonical=True,
    ),
    "fix_idle_timeout": SubsourceMetadata(
        name="fix_idle_timeout",
        user_label="FIX idle timeout",
        severity="medium",
        recovery_hint="Coder did not push within fix_idle_timeout_sec. Revise spec or Retry.",
        group_bucket="coder",
        legacy_category="TIMEOUT",
        is_canonical=True,
    ),
    "fix_iteration_cap": SubsourceMetadata(
        name="fix_iteration_cap",
        user_label="FIX iteration cap",
        severity="medium",
        recovery_hint="FIX cycle exceeded fix_iteration_cap iterations. Revise spec or split.",
        group_bucket="coder",
        legacy_category="TIMEOUT",
        is_canonical=True,
    ),
    "no_push_deadlock": SubsourceMetadata(
        name="no_push_deadlock",
        user_label="No-push deadlock",
        severity="medium",
        recovery_hint="Coder claimed fix without git push. Verify spec clarity then Retry.",
        group_bucket="coder",
        legacy_category="NO_PUSH_DEADLOCK",
        is_canonical=True,
    ),
    "infra_failure": SubsourceMetadata(
        name="infra_failure",
        user_label="Infrastructure failure",
        severity="high",
        recovery_hint="Repeated INFRA failures past grace. Check network or GitHub status then Retry.",
        group_bucket="daemon",
        legacy_category="INFRA",
        is_canonical=True,
    ),
    "daemon": SubsourceMetadata(
        name="daemon",
        user_label="Legacy daemon escalation",
        severity="medium",
        recovery_hint="Pre-migration record. Retry or revise spec.",
        group_bucket="daemon",
        legacy_category="ESCALATE",
        is_canonical=False,
    ),
    "watch_retrigger_cap": SubsourceMetadata(
        name="watch_retrigger_cap",
        user_label="WATCH retrigger cap",
        severity="medium",
        recovery_hint="Stale review retrigger exceeded debounce cap. Push manual review or close PR.",
        group_bucket="coder",
        legacy_category=None,
        is_canonical=False,
    ),
    "operator_reject": SubsourceMetadata(
        name="operator_reject",
        user_label="Operator rejected",
        severity="low",
        recovery_hint="Operator rejected the guardrail-flagged change. Revise spec then Retry.",
        group_bucket="operator_reject",
        legacy_category=None,
        is_canonical=False,
    ),
}


def lookup(name: str) -> SubsourceMetadata | None:
    """Return metadata for ``name`` or None if name is not a known subsource."""
    return _REGISTRY.get(name)


def all_subsources() -> frozenset[str]:
    """Return all 11 known subsource names (canonical plus out-of-vocab)."""
    return frozenset(_REGISTRY.keys())


def canonical_subsources() -> frozenset[str]:
    """Return the 8 canonical subsource names matching SUBSOURCE_VOCABULARY."""
    return frozenset(
        name for name, meta in _REGISTRY.items() if meta.is_canonical
    )


def group_for(name: str) -> str | None:
    """Return dashboard group bucket for ``name`` or None if unknown."""
    meta = _REGISTRY.get(name)
    return meta.group_bucket if meta is not None else None
