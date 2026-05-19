"""PR-351: ensure the Copy button on the cancellation card is wired to a
clipboard handler in ``base.html``.

The cancellation card emits ``<button data-clipboard-value="...">Copy</button>``
for the crash-backup branch. Without a matching client-side listener that
button is inert, so this module guards the wiring at the template level:

* the rendered cancellation card carries ``data-clipboard-value`` with the
  branch name, and
* ``base.html`` ships a delegated click handler that reads that attribute
  and calls the Clipboard API (with a legacy ``execCommand('copy')``
  fallback for non-secure contexts).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from src.web import app as web_app

BASE_HTML = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "web"
    / "templates"
    / "base.html"
)


def _render_card(cause: Any) -> str:
    macro = web_app.templates.env.get_template(
        "components/cancellation_card.html"
    ).module.cancellation_card
    return macro(cause)


def test_cancellation_card_emits_clipboard_value_attribute() -> None:
    """Rendered Copy button carries the branch name in
    ``data-clipboard-value`` so the delegated handler has something to
    read. The augmented-cause dict shape mirrors what
    ``_augment_causes_with_dependents`` hands the template."""
    branch = "crash-backup/PR-042/20260519-120000"
    cause = {
        "category": "ERROR",
        "task_id": "PR-042",
        "repo_slug": "octo__demo",
        "created_at": "2026-05-19T12:00:00+00:00",
        "payload": {"subsource": "crash"},
        "dependents_count": 0,
        "recovery_backup_branch": branch,
    }

    rendered = _render_card(cause)

    assert f'data-clipboard-value="{branch}"' in rendered


def test_base_html_registers_delegated_clipboard_listener() -> None:
    """``base.html`` installs a document-level click listener that reads
    ``data-clipboard-value`` from the closest ancestor and calls a copy
    helper, so htmx-swapped buttons activate without re-binding."""
    body = BASE_HTML.read_text(encoding="utf-8")

    assert "data-clipboard-value" in body, (
        "base.html must reference the data-clipboard-value attribute"
    )
    assert "closest('[data-clipboard-value]')" in body, (
        "expected event delegation via closest('[data-clipboard-value]')"
    )
    assert "document.addEventListener('click'" in body, (
        "expected a document-level click listener so htmx-swapped buttons "
        "do not need re-binding"
    )


def test_base_html_uses_clipboard_api_with_legacy_fallback() -> None:
    """The handler prefers ``navigator.clipboard.writeText`` and falls
    back to ``document.execCommand('copy')`` for non-secure contexts
    where the async API is gated."""
    body = BASE_HTML.read_text(encoding="utf-8")

    assert "navigator.clipboard" in body
    assert "writeText" in body
    assert "execCommand('copy')" in body
    assert "isSecureContext" in body


def test_base_html_provides_copy_feedback() -> None:
    """The handler swaps the button label to ``Copied!`` so the click is
    not silently dropped from the operator's point of view."""
    body = BASE_HTML.read_text(encoding="utf-8")

    assert "'Copied!'" in body
