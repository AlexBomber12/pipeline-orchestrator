"""Typed metadata registry for cancellation subsource vocabulary.

Single source of truth for the subsource strings emitted by daemon write
sites and rendered by operator-facing surfaces. Replaces scattered
inline metadata previously duplicated across cancellation_card.html
template branches, dashboard._SUBSOURCE_FILTER_GROUPS, and
runner._SUBSOURCE_TO_LEGACY_RUN_CAUSE.

Per PR-307a follow-up debt comment in src/daemon/notifications.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from src.daemon.handlers.error import ErrorCategory

Severity = Literal["low", "medium", "high"]
GroupBucket = Literal["coder", "daemon", "guardrail", "operator_reject"]


class SuppressionReason(StrEnum):
    CRASH = "crash"
    CODER_ESCALATE = "coder_escalate"
    GUARDRAIL = "guardrail"
    REVIEW_TIMEOUT = "review_timeout"
    FIX_IDLE_TIMEOUT = "fix_idle_timeout"
    FIX_ITERATION_CAP = "fix_iteration_cap"
    NO_PUSH_DEADLOCK = "no_push_deadlock"
    INFRA_FAILURE = "infra_failure"
    DAEMON = "daemon"
    WATCH_RETRIGGER_CAP = "watch_retrigger_cap"
    OPERATOR_REJECT = "operator_reject"
    DIAGNOSE_EXHAUSTED = "diagnose_exhausted"
    OPERATOR_STOPPED = "operator_stopped"
    RATE_LIMIT = "rate_limit"


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
    SuppressionReason.CRASH.value: SubsourceMetadata(
        name=SuppressionReason.CRASH.value,
        user_label="Daemon crash",
        severity="high",
        recovery_hint="Check daemon logs for crash cause then Retry.",
        group_bucket="daemon",
        legacy_category="CRASH",
        is_canonical=True,
    ),
    SuppressionReason.CODER_ESCALATE.value: SubsourceMetadata(
        name=SuppressionReason.CODER_ESCALATE.value,
        user_label="Coder escalated",
        severity="medium",
        recovery_hint="Review coder stdout for ESCALATE reason then revise spec or Retry.",
        group_bucket="coder",
        legacy_category="ESCALATE",
        is_canonical=True,
    ),
    SuppressionReason.GUARDRAIL.value: SubsourceMetadata(
        name=SuppressionReason.GUARDRAIL.value,
        user_label="Guardrail violation",
        severity="high",
        recovery_hint="Review guardrail finding in cancellation payload then revise spec.",
        group_bucket="guardrail",
        legacy_category="ESCALATE",
        is_canonical=True,
    ),
    SuppressionReason.REVIEW_TIMEOUT.value: SubsourceMetadata(
        name=SuppressionReason.REVIEW_TIMEOUT.value,
        user_label="Stale review",
        severity="medium",
        recovery_hint="Push a manual review or close the PR then Retry.",
        group_bucket="daemon",
        legacy_category="TIMEOUT",
        is_canonical=True,
    ),
    SuppressionReason.FIX_IDLE_TIMEOUT.value: SubsourceMetadata(
        name=SuppressionReason.FIX_IDLE_TIMEOUT.value,
        user_label="FIX idle timeout",
        severity="medium",
        recovery_hint="Coder did not push within fix_idle_timeout_sec. Revise spec or Retry.",
        group_bucket="daemon",
        legacy_category="TIMEOUT",
        is_canonical=True,
    ),
    SuppressionReason.FIX_ITERATION_CAP.value: SubsourceMetadata(
        name=SuppressionReason.FIX_ITERATION_CAP.value,
        user_label="FIX iteration cap",
        severity="medium",
        recovery_hint="FIX cycle exceeded fix_iteration_cap iterations. Revise spec or split.",
        group_bucket="daemon",
        legacy_category="TIMEOUT",
        is_canonical=True,
    ),
    SuppressionReason.NO_PUSH_DEADLOCK.value: SubsourceMetadata(
        name=SuppressionReason.NO_PUSH_DEADLOCK.value,
        user_label="No-push deadlock",
        severity="medium",
        recovery_hint="Coder claimed fix without git push. Verify spec clarity then Retry.",
        group_bucket="daemon",
        legacy_category="NO_PUSH_DEADLOCK",
        is_canonical=True,
    ),
    SuppressionReason.INFRA_FAILURE.value: SubsourceMetadata(
        name=SuppressionReason.INFRA_FAILURE.value,
        user_label="Infrastructure failure",
        severity="high",
        recovery_hint="Repeated INFRA failures past grace. Check network or GitHub status then Retry.",
        group_bucket="daemon",
        legacy_category="INFRA",
        is_canonical=True,
    ),
    SuppressionReason.DAEMON.value: SubsourceMetadata(
        name=SuppressionReason.DAEMON.value,
        user_label="Legacy daemon escalation",
        severity="medium",
        recovery_hint="Pre-migration record. Retry or revise spec.",
        group_bucket="daemon",
        legacy_category="ESCALATE",
        is_canonical=False,
    ),
    SuppressionReason.WATCH_RETRIGGER_CAP.value: SubsourceMetadata(
        name=SuppressionReason.WATCH_RETRIGGER_CAP.value,
        user_label="WATCH retrigger cap",
        severity="medium",
        recovery_hint="Stale review retrigger exceeded debounce cap. Push manual review or close PR.",
        group_bucket="daemon",
        legacy_category=None,
        is_canonical=False,
    ),
    SuppressionReason.OPERATOR_REJECT.value: SubsourceMetadata(
        name=SuppressionReason.OPERATOR_REJECT.value,
        user_label="Operator rejected",
        severity="low",
        recovery_hint="Operator rejected the guardrail-flagged change. Revise spec then Retry.",
        group_bucket="operator_reject",
        legacy_category=None,
        is_canonical=False,
    ),
    SuppressionReason.DIAGNOSE_EXHAUSTED.value: SubsourceMetadata(
        name=SuppressionReason.DIAGNOSE_EXHAUSTED.value,
        user_label="Diagnosis exhausted",
        severity="medium",
        recovery_hint="Diagnosis retry ceiling reached. Inspect ERROR logs then revise spec or Retry.",
        group_bucket="daemon",
        legacy_category=None,
        is_canonical=True,
    ),
    SuppressionReason.OPERATOR_STOPPED.value: SubsourceMetadata(
        name=SuppressionReason.OPERATOR_STOPPED.value,
        user_label="Operator stopped",
        severity="low",
        recovery_hint="Operator stopped the run. Review context then Retry when ready.",
        group_bucket="operator_reject",
        legacy_category=None,
        is_canonical=True,
    ),
    SuppressionReason.RATE_LIMIT.value: SubsourceMetadata(
        name=SuppressionReason.RATE_LIMIT.value,
        user_label="Rate limit",
        severity="medium",
        recovery_hint="Wait for the active coder or GitHub rate-limit window to reset, then Retry.",
        group_bucket="daemon",
        legacy_category=None,
        is_canonical=True,
    ),
}


def _assert_registry_matches_enum() -> None:
    enum_values = frozenset(reason.value for reason in SuppressionReason)
    registry_values = frozenset(_REGISTRY)
    if registry_values != enum_values:  # pragma: no cover - import-time invariant
        missing = sorted(enum_values - registry_values)
        extra = sorted(registry_values - enum_values)
        raise RuntimeError(
            "SuppressionReason and subsource registry drifted: "
            f"missing={missing}, extra={extra}"
        )


_assert_registry_matches_enum()


def error_category_to_reason(category: "ErrorCategory") -> SuppressionReason:
    """Map ERROR handler categories onto the canonical suppression taxonomy."""
    from src.daemon.handlers.error import ErrorCategory

    mapping = {
        ErrorCategory.RATE_LIMIT: SuppressionReason.RATE_LIMIT,
        # ``TIMEOUT`` is produced by a broad text match in the ERROR handler.
        # Until R1.5 separates CLI timeouts from review-timeout parking, keep
        # the legacy TIMEOUT bucket label by mapping to ``review_timeout``.
        ErrorCategory.TIMEOUT: SuppressionReason.REVIEW_TIMEOUT,
        ErrorCategory.OOM: SuppressionReason.CRASH,
        ErrorCategory.AUTH_FAILURE: SuppressionReason.INFRA_FAILURE,
        ErrorCategory.CI_FAILURE: SuppressionReason.INFRA_FAILURE,
        ErrorCategory.GHOST_PUSH: SuppressionReason.INFRA_FAILURE,
        ErrorCategory.STALE_BRANCH: SuppressionReason.INFRA_FAILURE,
        ErrorCategory.CLI_NOT_FOUND: SuppressionReason.INFRA_FAILURE,
        ErrorCategory.GIT_ERROR: SuppressionReason.INFRA_FAILURE,
        ErrorCategory.OTHER: SuppressionReason.CRASH,
    }
    return mapping[category]


def lookup(name: str) -> SubsourceMetadata | None:
    """Return metadata for ``name`` or None if name is not a known subsource."""
    return _REGISTRY.get(name)


def all_subsources() -> frozenset[str]:
    """Return all known subsource names."""
    return frozenset(_REGISTRY.keys())


def canonical_subsources() -> frozenset[str]:
    """Return canonical subsource names."""
    return all_subsources()


def group_for(name: str) -> str | None:
    """Return dashboard group bucket for ``name`` or None if unknown."""
    meta = _REGISTRY.get(name)
    return meta.group_bucket if meta is not None else None
