"""Canonical content of pipeline-orchestrator's daemon-managed AGENTS.md sections.

PR-192b wraps each daemon-owned section of this repo's ``AGENTS.md`` in
``pipeline-orchestrator: managed`` markers (the framework from PR-192a).
Onboarding flows read the canonical content from here, optionally apply
per-repo overrides for placeholders like ``{repo_slug}``, and use
``apply_managed_regions`` to inject the sections into a target repo's
existing AGENTS.md without disturbing user-authored content.
"""

from __future__ import annotations

from pathlib import Path

from src.onboarding.markdown_sections import extract_managed_regions

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
AGENTS_MD_PATH = REPO_ROOT / "AGENTS.md"

MANAGED_SECTIONS: tuple[str, ...] = (
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
)


def daemon_managed_content(
    repo_specific_overrides: dict[str, str] | None = None,
) -> dict[str, str]:
    """Return ``{section_name: region_text}`` for every managed section.

    The content is extracted live from this repo's ``AGENTS.md`` so the
    template stays in lock-step with the source of truth. Pass
    ``repo_specific_overrides`` to replace individual sections (for
    example with ``{repo_slug}`` already filled in) before applying the
    regions to a target repo's AGENTS.md.
    """
    regions = extract_managed_regions(AGENTS_MD_PATH.read_text())
    if repo_specific_overrides is not None:
        regions.update(repo_specific_overrides)
    return regions
