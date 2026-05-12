"""Cancellation card template — PR-319 subsource-dispatched rendering.

After PR-315 every Redis cancellation record carries ``category='ERROR'``
and detector identity moves to ``payload.subsource`` from the stable
vocabulary (``crash``, ``coder_escalate``, ``guardrail``,
``review_timeout``, ``fix_idle_timeout``, ``fix_iteration_cap``,
``no_push_deadlock``, ``infra_failure``). PR-319 rewrites the template
to dispatch on that subsource so operators see a meaningful badge and
descriptive message per cancellation, with a ``Legacy: <CATEGORY>``
fallback for pre-migration records the ``escalate_to_error`` migration
left intact.
"""

from __future__ import annotations

from src.cancellation.storage import CancellationCause
from src.web import app as web_app


def _render(cause: CancellationCause) -> str:
    macro = web_app.templates.env.get_template(
        "components/cancellation_card.html"
    ).module.cancellation_card
    return macro(cause)


def _error_cause(payload: dict[str, object]) -> CancellationCause:
    return CancellationCause(
        category="ERROR",
        payload=payload,
        created_at="2026-05-11T12:00:00+00:00",
        task_id="PR-999",
        repo_slug="example__alpha",
    )


def test_card_renders_crash_subsource() -> None:
    rendered = _render(
        _error_cause(
            {
                "subsource": "crash",
                "error_message": "Traceback: TimeoutError",
            }
        )
    )

    assert "cause-crash" in rendered
    assert "Daemon crash" in rendered
    assert "Traceback: TimeoutError" in rendered


def test_card_renders_review_timeout_with_elapsed() -> None:
    rendered = _render(
        _error_cause(
            {
                "subsource": "review_timeout",
                "elapsed_min": 90,
                "reason_text": "PR #5 hung after 90m (review=EYES, ci=PENDING)",
            }
        )
    )

    assert "cause-stale" in rendered
    assert "Stale review" in rendered
    assert "90m" in rendered
    assert "PR #5 hung after 90m" in rendered


def test_card_renders_fix_iteration_cap_with_iteration_count() -> None:
    rendered = _render(
        _error_cause(
            {
                "subsource": "fix_iteration_cap",
                "iteration_count": 8,
                "fix_iteration_cap": 8,
                "pr_number": 42,
            }
        )
    )

    assert "cause-deadlock" in rendered
    assert "FIX iteration cap" in rendered
    assert "8 iterations" in rendered


def test_card_renders_legacy_category_for_pre_migration_records() -> None:
    rendered = _render(
        _error_cause(
            {
                "legacy_category": "ESCALATE",
                "reason_text": "Tier 1 guardrail tripped",
            }
        )
    )

    assert "cause-legacy" in rendered
    assert "Legacy: ESCALATE" in rendered
    assert "Tier 1 guardrail tripped" in rendered


def test_card_renders_generic_error_when_no_payload_detail() -> None:
    rendered = _render(_error_cause({}))

    assert "cause-error" in rendered
    assert "Cancellation reason not recorded" in rendered
    # No legacy_category → no "Legacy:" prefix on the fallback badge.
    assert "Legacy:" not in rendered


def test_card_renders_no_push_deadlock_with_attempts() -> None:
    rendered = _render(
        _error_cause(
            {
                "subsource": "no_push_deadlock",
                "attempts": 3,
                "pr_number": 7,
            }
        )
    )

    assert "cause-deadlock" in rendered
    assert "No-push deadlock" in rendered
    assert "3 consecutive cycles" in rendered


def test_card_renders_coder_escalate_with_reason() -> None:
    rendered = _render(
        _error_cause(
            {
                "subsource": "coder_escalate",
                "reason_text": "out of scope",
            }
        )
    )

    assert "cause-escalate" in rendered
    assert "Coder escalate" in rendered
    assert "out of scope" in rendered


def test_card_renders_guardrail_with_reason() -> None:
    rendered = _render(
        _error_cause(
            {
                "subsource": "guardrail",
                "reason_text": "GUARDRAIL: deletion: rm -rf /",
            }
        )
    )

    assert "cause-escalate" in rendered
    assert "Guardrail violation" in rendered
    assert "GUARDRAIL: deletion: rm -rf /" in rendered


def test_card_renders_fix_idle_timeout_with_duration() -> None:
    rendered = _render(
        _error_cause(
            {
                "subsource": "fix_idle_timeout",
                "duration_elapsed_sec": 1800,
                "limit_type": "fix_idle",
            }
        )
    )

    assert "cause-stale" in rendered
    assert "FIX idle timeout" in rendered
    assert "1800s" in rendered


def test_card_renders_infra_failure_with_subsystem() -> None:
    rendered = _render(
        _error_cause(
            {
                "subsource": "infra_failure",
                "subsystem": "gh_api",
                "retry_count": 3,
                "error_message": "rate limit exceeded",
            }
        )
    )

    assert "cause-infra" in rendered
    assert "Infrastructure failure" in rendered
    assert "gh_api" in rendered
    assert "rate limit exceeded" in rendered


def test_card_wrapper_carries_subsource_class() -> None:
    rendered = _render(
        _error_cause(
            {
                "subsource": "crash",
                "error_message": "boom",
            }
        )
    )

    assert "subsource-crash" in rendered


def test_card_wrapper_carries_unknown_subsource_class_for_blank_payload() -> None:
    rendered = _render(_error_cause({}))

    assert "subsource-unknown" in rendered
