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
    "forbidden_actions",
    "auto_pr_runbook",
    "planned_pr_runbook",
    "micro_pr_runbook",
    "review_fix_runbook",
    "queue_stability_rules",
}


def test_managed_sections_contains_all_expected_names() -> None:
    assert set(MANAGED_SECTIONS) == EXPECTED_SECTION_NAMES


def test_managed_sections_includes_forbidden_actions() -> None:
    assert "forbidden_actions" in MANAGED_SECTIONS


def test_managed_sections_order_places_forbidden_actions_after_branch_naming() -> None:
    assert MANAGED_SECTIONS.index("forbidden_actions") == (
        MANAGED_SECTIONS.index("branch_naming") + 1
    )


def test_daemon_managed_content_returns_content_for_every_managed_section() -> None:
    regions = daemon_managed_content()
    assert set(regions) == EXPECTED_SECTION_NAMES
    for name in MANAGED_SECTIONS:
        assert regions[name].strip(), f"section {name!r} unexpectedly empty"


def test_daemon_managed_content_returns_forbidden_actions_section() -> None:
    regions = daemon_managed_content()
    content = regions["forbidden_actions"]

    assert content.strip()
    assert "Forbidden actions" in content
    assert "GUARDRAIL: <category>:" in content


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


def test_onboarded_review_contract_allows_bound_success_and_preserves_later_blockers():
    generated = apply_managed_regions("# Project instructions\n", daemon_managed_content())
    regions = extract_managed_regions(generated)
    assert "explicit successful reviews bound to the current HEAD" in regions["quick_rules"]
    assert "Codex review approval valid" in regions["ci_gates"]
    gate = regions["codex_review_gate"]
    assert "Didn't find any major issues." in gate
    assert "Abbreviated SHAs must resolve unambiguously to HEAD" in gate
    assert "Later actionable findings or negative reviews still block approval" in gate
    assert "repeated review requests do not erase a valid verdict" in gate
    assert "A matching login alone does not establish trust" in gate
    assert "valid current-HEAD approval" in regions["review_fix_runbook"]
    assert 'green" only when' not in generated
    assert "until a non-stale thumbs up" not in generated


STARTER_PATH = AGENTS_MD_PATH.parent / "templates" / "AGENTS.md"


def _starter_review_gate() -> str:
    text = STARTER_PATH.read_text()
    return text.split("### Codex Review gate (GitHub PR)", 1)[1].split(
        "### Required review artifacts", 1
    )[0].strip()


def test_starter_review_gate_matches_canonical_source() -> None:
    gate = daemon_managed_content()["codex_review_gate"]
    assert _starter_review_gate() == gate.split(
        "## Codex Review gate (GitHub PR)", 1
    )[1].strip()


@pytest.mark.parametrize("representation", ["canonical", "starter", "generated"])
def test_review_instructions_preserve_actor_head_and_handoff_contract(
    representation: str,
) -> None:
    content = {
        "canonical": AGENTS_MD_PATH.read_text(),
        "starter": STARTER_PATH.read_text(),
        "generated": apply_managed_regions("# User instructions\n", daemon_managed_content()),
    }[representation]
    for required in (
        "requires a valid current-change trusted `+1` before merge",
        "daemon.trusted_reviewer_identities",
        "stable numeric ID",
        "Logins are diagnostic labels",
        "same configured actor policy",
        "proven current-HEAD publication",
        "same SHA",
        "remains pending",
        "No automatic trust-migration",
        "Abbreviated SHAs must resolve unambiguously to HEAD",
        "There is no fallback to an arbitrary author comment",
        "comment body MUST be exactly the string `@codex review`",
        "Manual fix loop (used only when `FIX FEEDBACK` does not contain the daemon handoff marker)",
        "AUTO PR and daemon FIX coders do not post or wait for review",
        "they publish once, report evidence, and exit",
        "Do not post `@codex review`, poll GitHub CI or review",
        "required trusted `+1` exist without later blocking review evidence",
        "Do not wait for\n  another review or act on findings that arrive during the invocation",
        "estimates and expected paths are planning signals, not automatic",
        "runtime guardrails",
    ):
        assert required in content
    for obsolete in (
        "If none exist, use the first PR author comment",
        "compares the reaction timestamp against the latest push timestamp",
        "already exists for the current HEAD SHA",
        '"codex" in login',
    ):
        assert obsolete not in content


def test_review_regeneration_preserves_user_bytes_and_unrelated_modes() -> None:
    regions = daemon_managed_content()
    old_regions = dict(regions, codex_review_gate="\nobsolete review policy\n")
    preamble = "User preamble: café\n\n".encode("utf-8")
    suffix = b"\n## User rules\nKeep exactly:  two spaces.\n"
    original = preamble.decode("utf-8") + apply_managed_regions("", old_regions)
    original += suffix.decode("utf-8")
    review_update = {"codex_review_gate": regions["codex_review_gate"]}
    updated = apply_managed_regions(original, review_update)
    assert updated == original.replace(
        old_regions["codex_review_gate"], regions["codex_review_gate"]
    )
    updated_bytes = updated.encode("utf-8")
    assert updated_bytes.startswith(preamble)
    assert updated_bytes.endswith(suffix)
    assert updated.count(
        "<!-- pipeline-orchestrator: managed BEGIN codex_review_gate -->"
    ) == 1
    assert updated.count(
        "<!-- pipeline-orchestrator: managed END codex_review_gate -->"
    ) == 1
    assert extract_managed_regions(updated) == regions
    assert apply_managed_regions(updated, regions) == updated
