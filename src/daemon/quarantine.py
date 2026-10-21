"""GitHub quarantine labels for daemon-detected guardrail violations."""

from __future__ import annotations

import re
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

OBSOLETE_SIZE_LABEL = "quarantine:large_diff"
CONTINUED_SIZE_PREFIX = "size-ok:"
FIXING_SIZE_PREFIX = "size-fix:"


def continued_size_label(head_sha: str) -> str:
    """Identify the exact PR revision whose size decision was continued."""
    if re.fullmatch(r"[0-9a-fA-F]{40}", head_sha) is None:
        raise ValueError("A published PR HEAD is required for size continuation")
    return f"{CONTINUED_SIZE_PREFIX}{head_sha.lower()}"


def is_continued_size_label(label: str) -> bool:
    return label.startswith(CONTINUED_SIZE_PREFIX)


def fixing_size_label(head_sha: str) -> str:
    return continued_size_label(head_sha).replace(CONTINUED_SIZE_PREFIX, FIXING_SIZE_PREFIX, 1)


def is_fixing_size_label(label: str) -> bool:
    return label.startswith(FIXING_SIZE_PREFIX)


def blocking_quarantine_labels(labels: set[str]) -> set[str]:
    """Ignore only the former size decision; retain every other quarantine."""
    return {label for label in labels if label != OBSOLETE_SIZE_LABEL and not is_continued_size_label(label)}


def quarantine_label_for_category(category: str) -> str:
    """Return the quarantine label name for a guardrail category."""
    return f"quarantine:{_CATEGORY_LABEL_ALIASES.get(category, category)}"


def apply_quarantine_label_for_violation(
    runner: "PipelineRunner",
    pr_number: int,
    violation: GuardrailViolation,
) -> bool:
    """Apply a quarantine label and write a GUARDRAIL comment on the PR.

    GitHub-side writes are best effort. The durable daemon protection is the
    guardrail suppression record plus the MERGE gate.
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
