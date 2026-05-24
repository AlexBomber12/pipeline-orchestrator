"""Tests for the scan integration in validate_task_spec.

These exercise the new ``agents_violations`` channel in the MCP tool's
return shape (PR-259). Schema validation behavior is covered by
``tests/mcp/test_functional_tools.py``.
"""

from __future__ import annotations


_VALID_SPEC = """---
status: TODO
---

# PR-999: Example task

Branch: pr-999-example
- Type: feature
- Complexity: low
- Depends on: none
- Priority: 3
- Coder: claude

## Problem

Example.
"""


def test_validate_returns_violations_alongside_schema():
    """A schema-valid spec with anti-patterns reports violations."""
    from src.mcp.tools.functional import validate_task_spec

    body = _VALID_SPEC + "\nRun `gh pr create --draft` to open the PR.\n"
    result = validate_task_spec(body)

    assert result["schema_errors"] == []
    assert any(
        v["type"] == "draft_pr_flag" for v in result["agents_violations"]
    )


def test_validate_task_spec_still_emits_agents_violations():
    """Upload-time validation still invokes the AGENTS anti-pattern scan."""
    from src.mcp.tools.functional import validate_task_spec

    body = _VALID_SPEC + "\nRun `gh pr create --draft` to open the PR.\n"
    result = validate_task_spec(body)

    assert result["agents_violations"]


def test_valid_field_false_when_violations_present():
    """``valid`` is False whenever any violation surfaces."""
    from src.mcp.tools.functional import validate_task_spec

    body = _VALID_SPEC + "\nIf needed, git commit --no-verify -m 'fix'.\n"
    result = validate_task_spec(body)

    assert result["valid"] is False
    assert result["agents_violations"]


def test_schema_and_violations_both_reported():
    """Schema errors and agents violations stack independently."""
    from src.mcp.tools.functional import validate_task_spec

    bad = _VALID_SPEC.replace("- Type: feature", "- Type: nonsense")
    bad += "\ngh pr create --draft --title wip\n"
    result = validate_task_spec(bad)

    assert result["valid"] is False
    assert result["schema_errors"]
    assert any(
        v["type"] == "draft_pr_flag" for v in result["agents_violations"]
    )


def test_violation_dict_shape():
    """Each violation entry exposes type, excerpt, and rule keys."""
    from src.mcp.tools.functional import validate_task_spec

    body = _VALID_SPEC + "\ngit commit --no-verify\n"
    result = validate_task_spec(body)

    assert result["agents_violations"]
    entry = result["agents_violations"][0]
    assert set(entry.keys()) == {"type", "excerpt", "rule"}


def test_legacy_errors_key_aliases_schema_errors():
    """PR-246 callers read ``result['errors']``; PR-259 must keep it.

    The key is always present (empty list on success) so legacy code
    that does ``result['errors']`` does not ``KeyError`` on the happy
    path; on failure it carries the same diagnostics as
    ``schema_errors``.
    """
    from src.mcp.tools.functional import validate_task_spec

    ok = validate_task_spec(_VALID_SPEC)
    assert ok["errors"] == []
    assert ok["errors"] == ok["schema_errors"]

    bad = _VALID_SPEC.replace("- Type: feature", "- Type: nonsense")
    failed = validate_task_spec(bad)
    assert failed["errors"]
    assert failed["errors"] == failed["schema_errors"]
