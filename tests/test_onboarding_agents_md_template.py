"""Tests for src/onboarding/agents_md_template.py."""

from __future__ import annotations

import pytest
from src.onboarding.agents_md_template import (
    AGENTS_MD_PATH,
    MANAGED_SECTIONS,
    daemon_managed_content,
)
from src.onboarding.markdown_sections import (
    apply_managed_regions,
    extract_managed_regions,
)

EXPECTED_SECTION_NAMES = {
    "quick_rules",
    "work_modes",
    "daemon_mode",
    "ci_gates",
    "codex_review_gate",
    "escalate_protocol",
    "branch_naming",
    "auto_pr_runbook",
    "planned_pr_runbook",
    "micro_pr_runbook",
    "review_fix_runbook",
    "queue_stability_rules",
}


def test_managed_sections_contains_all_expected_names() -> None:
    assert set(MANAGED_SECTIONS) == EXPECTED_SECTION_NAMES


def test_daemon_managed_content_returns_content_for_every_managed_section() -> None:
    regions = daemon_managed_content()
    assert set(regions) == EXPECTED_SECTION_NAMES
    for name in MANAGED_SECTIONS:
        assert regions[name].strip(), f"section {name!r} unexpectedly empty"


def test_daemon_managed_content_applies_repo_specific_overrides() -> None:
    overrides = {"work_modes": "\n## Work Modes\nrepo-slug: example/repo\n"}
    regions = daemon_managed_content(overrides)
    assert regions["work_modes"] == overrides["work_modes"]
    assert "## Daemon Mode" in regions["daemon_mode"]


def test_round_trip_extract_then_apply_is_identity() -> None:
    original = AGENTS_MD_PATH.read_text()
    regions = daemon_managed_content()
    assert regions == extract_managed_regions(original)
    assert apply_managed_regions(original, regions) == original


def test_daemon_managed_content_rejects_unknown_override_keys() -> None:
    with pytest.raises(ValueError, match="work_mode"):
        daemon_managed_content({"work_mode": "typo, should not be accepted"})


# ---------------------------------------------------------------------------
# PR-271: AUTO PR rollout content assertions.
#
# These tests pin the daemon-managed AGENTS.md content that the
# reconciliation framework propagates to managed repos so the four-trigger
# model (AUTO PR / PLANNED PR / MICRO PR / FIX FEEDBACK) cannot drift back
# to the pre-PR-271 three-trigger shape without a test failure.
# ---------------------------------------------------------------------------


def test_work_modes_section_lists_four_triggers() -> None:
    """The Work Modes block enumerates AUTO PR, PLANNED PR, MICRO PR, and
    FIX FEEDBACK so the daemon's invocation mode is documented alongside
    the manual triggers."""
    work_modes = daemon_managed_content()["work_modes"]
    assert "AUTO PR" in work_modes
    assert "PLANNED PR" in work_modes
    assert "MICRO PR" in work_modes
    assert "FIX FEEDBACK" in work_modes


def test_auto_pr_runbook_section_present() -> None:
    """The AUTO PR runbook header is rendered in the daemon-managed content
    so reconciliation propagates it to every onboarded repo."""
    regions = daemon_managed_content()
    assert "auto_pr_runbook" in regions
    assert "## AUTO PR runbook" in regions["auto_pr_runbook"]


def test_auto_pr_runbook_forbids_queue_md_consultation() -> None:
    """Pin the anti-scope-expansion rule at content level so future drift
    surfaces in tests. The AUTO PR runbook must explicitly tell the coder
    NOT to consult ``tasks/QUEUE.md`` for task selection."""
    runbook = daemon_managed_content()["auto_pr_runbook"]
    assert "Do NOT read `tasks/QUEUE.md`" in runbook


def test_quick_rules_mentions_auto_pr_for_daemon() -> None:
    """The Quick rules block clarifies that the daemon's invocation mode
    is AUTO PR with explicit Task/File headers and inline body."""
    quick_rules = daemon_managed_content()["quick_rules"]
    assert "AUTO PR" in quick_rules
    assert "pipeline-orchestrator daemon" in quick_rules
