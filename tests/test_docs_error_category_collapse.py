"""Tests pinning doc updates for the single-ERROR cancellation model.

After PR-314..PR-319 collapsed CancellationCause to a single ERROR
category plus ``payload.subsource`` forensic detail, the documentation
served via MCP (``get_task_schema``, ``get_agents_md_template``) must
reflect the new vocabulary so LLM clients generate task specs and
operator guidance against the current contract.
"""

from __future__ import annotations

from pathlib import Path

from src.onboarding.agents_md_template import daemon_managed_content

REPO_ROOT = Path(__file__).resolve().parent.parent
TASK_SCHEMA_MD = REPO_ROOT / "docs" / "TASK_SCHEMA.md"
OPERATIONS_MD = REPO_ROOT / "docs" / "operations.md"

SUBSOURCE_VOCABULARY = (
    "crash",
    "coder_escalate",
    "guardrail",
    "review_timeout",
    "fix_idle_timeout",
    "fix_iteration_cap",
    "no_push_deadlock",
    "infra_failure",
)


def test_task_schema_md_contains_subsource_table() -> None:
    body = TASK_SCHEMA_MD.read_text()
    assert "Cancellation cause classification" in body
    assert 'category="ERROR"' in body
    assert "payload.subsource" in body
    for subsource in SUBSOURCE_VOCABULARY:
        assert f"`{subsource}`" in body, f"missing subsource {subsource!r}"


def test_task_schema_md_mentions_legacy_category_for_historical_records() -> None:
    body = TASK_SCHEMA_MD.read_text()
    assert "payload.legacy_category" in body
    assert "Legacy" in body


def test_agents_md_template_escalate_protocol_no_idle_reference() -> None:
    regions = daemon_managed_content()
    section = regions["escalate_protocol"]
    assert "IDLE escalation" not in section
    assert "transition the runner to idle" not in section.lower()
    assert "HUNG" not in section
    assert "ERROR" in section


def test_agents_md_template_escalate_protocol_points_at_retry_affordance() -> None:
    section = daemon_managed_content()["escalate_protocol"]
    assert "Retry button" in section
    assert "re-uploading" in section or "re-upload" in section
    assert "subsource=coder_escalate" in section


def test_operations_md_recovery_flow_describes_retry_button() -> None:
    body = OPERATIONS_MD.read_text()
    assert "Recovery from ERROR" in body
    assert "Retry button" in body
    assert "Re-upload spec" in body or "re-upload spec" in body.lower()
