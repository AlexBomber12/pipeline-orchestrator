"""PR-318: recovery dispatches on ``payload.subsource`` not ``category``.

PR-315 collapsed ``CancellationCause.category`` to a single ``ERROR`` value
with detector identity moved to ``payload.subsource``. Recovery used to
branch on the legacy ``category`` field; PR-318 cuts that over to
``payload.subsource`` via the ``classify_cancellation_subsource`` and
``recovery_branch_for_subsource`` helpers exported from
``src.cancellation``. These tests pin the dispatch contract — including
the defensive paths for legacy records, empty subsource fields, and
non-ERROR category values — so a future regression that reintroduces
``category``-based branching trips a unit test before reaching the dashboard.
"""

from __future__ import annotations

import pytest
from src.cancellation import (
    SUBSOURCE_VOCABULARY,
    CancellationCause,
    classify_cancellation_subsource,
    recovery_branch_for_subsource,
)

NON_CRASH_SUBSOURCES = tuple(sorted(SUBSOURCE_VOCABULARY - {"crash"}))


def test_recovery_dispatches_crash_subsource_to_crash_branch() -> None:
    """``subsource="crash"`` routes to the crash recovery branch."""
    cause = CancellationCause(
        category="ERROR",
        payload={"subsource": "crash", "error_message": "subprocess died"},
    )
    subsource = classify_cancellation_subsource(cause)
    assert subsource == "crash"
    assert recovery_branch_for_subsource(subsource) == "crash"


@pytest.mark.parametrize("subsource", NON_CRASH_SUBSOURCES)
def test_recovery_dispatches_other_subsource_to_operator_attention(
    subsource: str,
) -> None:
    """Every non-crash subsource routes to the operator-attention branch."""
    cause = CancellationCause(
        category="ERROR",
        payload={"subsource": subsource, "reason_text": "deliberately parked"},
    )
    derived = classify_cancellation_subsource(cause)
    assert derived == subsource
    assert recovery_branch_for_subsource(derived) == "operator_attention"


def test_recovery_handles_missing_subsource_field() -> None:
    """A cause with no ``payload.subsource`` degrades to operator-attention.

    PR-318 edge case: empty subsource is treated as a generic ERROR and
    must not silently re-enter the crash-specific recovery branch.
    """
    cause = CancellationCause(category="ERROR", payload={})
    subsource = classify_cancellation_subsource(cause)
    assert subsource == ""
    assert recovery_branch_for_subsource(subsource) == "operator_attention"


def test_recovery_handles_none_cause() -> None:
    """``classify_cancellation_subsource(None)`` returns the empty sentinel."""
    subsource = classify_cancellation_subsource(None)
    assert subsource == ""
    assert recovery_branch_for_subsource(subsource) == "operator_attention"


@pytest.mark.parametrize(
    ("legacy_category", "expected_subsource"),
    [
        ("CRASH", "crash"),
        ("ESCALATE", "coder_escalate"),
        ("TIMEOUT", "review_timeout"),
        ("INFRA", "infra_failure"),
        ("NO_PUSH_DEADLOCK", "no_push_deadlock"),
    ],
)
def test_recovery_handles_legacy_escalate_category_record(
    legacy_category: str, expected_subsource: str
) -> None:
    """Pre-PR-315 records (raw legacy ``category`` field) still classify.

    A record that escaped the ``escalate_to_error`` startup migration
    carries one of the five legacy ``category`` values directly. The
    helper warns about the missed migration and translates the legacy
    category to its canonical subsource so dispatch still works.
    """
    logged: list[str] = []
    cause = CancellationCause(category=legacy_category, payload={})

    subsource = classify_cancellation_subsource(cause, log=logged.append)

    assert subsource == expected_subsource
    assert logged, "expected a defensive warning for the non-ERROR category"
    assert "non-ERROR category" in logged[0]
    assert legacy_category in logged[0]


def test_recovery_handles_legacy_category_in_payload_field() -> None:
    """A migrated cause with ``legacy_category`` in payload still classifies.

    The ``escalate_to_error`` migration preserves the original detector
    value as ``payload.legacy_category``. If a record reaches recovery
    with ``category=ERROR`` and no ``subsource`` but with a recoverable
    ``legacy_category``, the helper falls back to that hint.
    """
    cause = CancellationCause(
        category="ERROR",
        payload={"legacy_category": "ESCALATE", "reason_text": "old record"},
    )
    subsource = classify_cancellation_subsource(cause)
    assert subsource == "coder_escalate"
    assert recovery_branch_for_subsource(subsource) == "operator_attention"


def test_recovery_logs_warning_on_non_error_category() -> None:
    """``category != "ERROR"`` emits an [INFRA] warning via the log sink."""
    logged: list[str] = []
    cause = CancellationCause(
        category="ESCALATE",
        payload={"subsource": "coder_escalate"},
    )

    classify_cancellation_subsource(cause, log=logged.append)

    assert len(logged) == 1
    line = logged[0]
    assert line.startswith("[INFRA]")
    assert "non-ERROR category" in line
    assert "PR-315 migration" in line
    assert "ESCALATE" in line


def test_recovery_warning_falls_back_to_payload_subsource_first() -> None:
    """When category is legacy but payload.subsource is canonical, prefer subsource.

    A record may have been partially migrated (subsource set, category
    not rewritten). Prefer the canonical subsource over the legacy
    category translation so the dispatch matches the detector's intent.
    """
    logged: list[str] = []
    cause = CancellationCause(
        category="CRASH",
        payload={"subsource": "fix_iteration_cap"},
    )

    subsource = classify_cancellation_subsource(cause, log=logged.append)

    assert subsource == "fix_iteration_cap"
    assert recovery_branch_for_subsource(subsource) == "operator_attention"
    assert logged and "non-ERROR category" in logged[0]


def test_recovery_warning_falls_back_to_legacy_when_subsource_unknown() -> None:
    """A non-ERROR category with an unknown subsource still falls back."""
    logged: list[str] = []
    cause = CancellationCause(
        category="ESCALATE",
        payload={"subsource": "made_up_subsource"},
    )

    subsource = classify_cancellation_subsource(cause, log=logged.append)

    assert subsource == "coder_escalate"
    assert logged and "non-ERROR category" in logged[0]


def test_recovery_unknown_category_and_subsource_returns_empty() -> None:
    """Non-ERROR category + unknown subsource + no legacy hint returns ``""``."""
    logged: list[str] = []
    cause = CancellationCause(
        category="MADE_UP_CATEGORY",
        payload={"subsource": "also_not_real"},
    )

    subsource = classify_cancellation_subsource(cause, log=logged.append)

    assert subsource == ""
    assert recovery_branch_for_subsource(subsource) == "operator_attention"
    assert logged and "non-ERROR category" in logged[0]


def test_recovery_default_logger_warns_when_no_log_sink_provided(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Without an explicit ``log`` argument the helper warns the module logger.

    Lets a caller outside the recovery handler (where no per-runner
    ``log_event`` is in scope) still surface a missed-migration record
    via the standard logging pipeline.
    """
    import logging as _logging

    cause = CancellationCause(category="ESCALATE", payload={})

    with caplog.at_level(_logging.WARNING, logger="src.cancellation"):
        subsource = classify_cancellation_subsource(cause)

    assert subsource == "coder_escalate"
    assert any(
        "non-ERROR category" in record.message for record in caplog.records
    )


def test_recovery_branch_empty_subsource_routes_to_operator_attention() -> None:
    """The empty string sentinel maps to the operator-attention branch."""
    assert recovery_branch_for_subsource("") == "operator_attention"


def test_recovery_branch_unknown_subsource_routes_to_operator_attention() -> None:
    """An unknown subsource value cannot re-enter the crash branch."""
    assert (
        recovery_branch_for_subsource("not_a_real_subsource")
        == "operator_attention"
    )


def test_subsource_vocabulary_matches_documented_set() -> None:
    """``SUBSOURCE_VOCABULARY`` matches the eight documented values."""
    assert SUBSOURCE_VOCABULARY == frozenset(
        {
            "crash",
            "coder_escalate",
            "guardrail",
            "review_timeout",
            "fix_idle_timeout",
            "fix_iteration_cap",
            "no_push_deadlock",
            "infra_failure",
        }
    )
