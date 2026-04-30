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
    "work_modes",
    "daemon_mode",
    "ci_gates",
    "codex_review_gate",
    "escalate_protocol",
    "branch_naming",
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
