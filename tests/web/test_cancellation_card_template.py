"""Cancellation card template — PR-315 ERROR safety-net branch.

After the PR-315 migration every Redis cancellation record carries
``category == 'ERROR'``. PR-319 will replace the safety-net with full
per-subsource rendering, but until then the template must still produce
a usable card so operators see forensic detail (subsource, legacy
category, reason text) instead of an empty payload div.
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


def test_cancellation_card_renders_error_with_subsource_fallback() -> None:
    rendered = _render(
        _error_cause(
            {
                "subsource": "review_timeout",
                "reason_text": "WATCH review_timeout exceeded after 20 min",
            }
        )
    )

    assert "review_timeout" in rendered
    assert "WATCH review_timeout exceeded after 20 min" in rendered
    assert "Cancellation reason not recorded" not in rendered


def test_cancellation_card_renders_error_with_legacy_category_fallback() -> None:
    rendered = _render(
        _error_cause(
            {
                "subsource": "guardrail",
                "legacy_category": "ESCALATE",
                "reason_text": "Tier 1 guardrail tripped",
            }
        )
    )

    assert "Legacy: ESCALATE" in rendered
    assert "Tier 1 guardrail tripped" in rendered


def test_cancellation_card_renders_error_with_no_payload_detail() -> None:
    rendered = _render(_error_cause({}))

    assert "Cancellation reason not recorded" in rendered
    # No legacy/subsource → fallback badge text is the literal "Error".
    assert "Error" in rendered


def test_cancellation_card_renders_error_with_error_message_fallback() -> None:
    rendered = _render(
        _error_cause(
            {
                "subsource": "crash",
                "error_message": "Traceback: TimeoutError",
            }
        )
    )

    assert "crash" in rendered
    assert "Traceback: TimeoutError" in rendered


def test_cancellation_card_renders_error_with_excerpt_fallback() -> None:
    rendered = _render(
        _error_cause(
            {
                "subsource": "coder_escalate",
                "excerpt": "ESCALATE: out of scope",
            }
        )
    )

    assert "coder_escalate" in rendered
    assert "ESCALATE: out of scope" in rendered
