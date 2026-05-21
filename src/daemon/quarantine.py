"""GitHub quarantine labels for daemon-detected guardrail violations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.audit.operator_actions import write_operator_action_audit
from src.daemon.fix_escalation import (
    _exception_text,
    _is_label_already_exists_error,
)
from src.daemon.guardrails import GuardrailViolation
from src.github import gh_runner

if TYPE_CHECKING:
    from src.daemon.runner import PipelineRunner

_QUARANTINE_LABEL_COLOR = "B60205"

_CATEGORY_LABEL_ALIASES = {
    "large_diff_threshold": "large_diff",
    "mass_file_deletion": "mass_deletion",
    "test_file_deletion": "mass_deletion",
    "governance_file_modified": "governance",
    "workflow_destruction": "workflow",
}


def quarantine_label_for_category(category: str) -> str:
    """Return the quarantine label name for a guardrail category."""
    return f"quarantine:{_CATEGORY_LABEL_ALIASES.get(category, category)}"


def apply_quarantine_label_for_violation(
    runner: "PipelineRunner",
    pr_number: int,
    violation: GuardrailViolation,
) -> bool:
    """Apply a quarantine label and write a GUARDRAIL comment on the PR.

    GitHub-side writes are best effort. The durable daemon protection is
    ``RepoState.quarantined_prs`` plus the MERGE gate.
    """
    label = quarantine_label_for_category(violation.category)
    try:
        gh_runner.run_gh(
            [
                "label",
                "create",
                label,
                "--color",
                _QUARANTINE_LABEL_COLOR,
                "--description",
                f"Daemon quarantined: {violation.rule}",
            ],
            repo=runner.owner_repo,
        )
    except Exception as exc:
        if not _is_label_already_exists_error(exc):
            runner.log_event(
                f"[GUARDRAIL] label create failed: {_exception_text(exc)}."
            )
    try:
        gh_runner.run_gh(
            ["pr", "edit", str(pr_number), "--add-label", label],
            repo=runner.owner_repo,
        )
    except Exception as exc:
        runner.log_event(
            f"[GUARDRAIL] label apply on PR #{pr_number} failed: {exc}."
        )
        return False
    try:
        body = (
            f"## QUARANTINE: {violation.category}\n\n"
            "This PR has been flagged by the daemon's guardrail layer.\n\n"
            f"**Rule:** {violation.rule}\n\n"
            f"**Details:**\n```\n{violation.excerpt}\n```\n\n"
            "This PR will not be auto-merged by the daemon. To release "
            f"the quarantine, either remove the `{label}` label manually "
            f"or POST to `/repos/{{name}}/quarantine/{pr_number}/release`."
        )
        gh_runner.run_gh(
            ["pr", "comment", str(pr_number), "--body", body],
            repo=runner.owner_repo,
        )
    except Exception as exc:
        runner.log_event(
            f"[GUARDRAIL] comment post on PR #{pr_number} failed: {exc}."
        )
    write_operator_action_audit(
        action="quarantine_apply",
        repo=runner.name,
        pr=pr_number,
        category=violation.category,
        rule=violation.rule,
    )
    return True
