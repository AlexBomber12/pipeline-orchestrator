"""Tests for operator-facing onboarding documentation.

The doc lives at ``docs/onboarding-existing-projects.md`` (the file
created by PR-192c). PR-273 aligns it with the AUTO PR / PLANNED PR /
MICRO PR / FIX FEEDBACK four-trigger model so an operator following the
guide sees the same trigger contract that the daemon-managed AGENTS.md
sections enforce at runtime.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
ONBOARDING_DOC = REPO_ROOT / "docs" / "onboarding-existing-projects.md"


def test_onboarding_doc_lists_four_triggers() -> None:
    """The onboarding guide names all four trigger phrases. Without this
    pin, doc drift could leave operators with stale instructions that
    omit AUTO PR even after the daemon-managed sections have been
    rolled out."""
    body = ONBOARDING_DOC.read_text()
    for trigger in ("AUTO PR", "PLANNED PR", "MICRO PR", "FIX FEEDBACK"):
        assert trigger in body, f"missing trigger {trigger!r}"


def test_onboarding_doc_clarifies_auto_pr_is_daemon_mode() -> None:
    """The guide explains that AUTO PR is the daemon's invocation mode,
    so an operator does not mistake it for a manual editor trigger."""
    body = ONBOARDING_DOC.read_text()
    assert "daemon" in body.lower()
    assert "AUTO PR" in body
