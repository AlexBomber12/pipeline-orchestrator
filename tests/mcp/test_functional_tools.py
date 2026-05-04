"""Tests for functional MCP tools."""

from __future__ import annotations

from unittest.mock import patch

import pytest


# ---- validate_task_spec ----

_VALID_SPEC = """# PR-999: Example task

Branch: pr-999-example
- Type: refactor
- Complexity: low
- Depends on: none
- Priority: 2
- Coder: claude

## Problem

Example.
"""


def test_validate_task_spec_accepts_valid_content():
    from src.mcp.tools.functional import validate_task_spec

    result = validate_task_spec(_VALID_SPEC)
    assert result == {"valid": True}


def test_validate_task_spec_rejects_missing_branch_field():
    from src.mcp.tools.functional import validate_task_spec

    bad = _VALID_SPEC.replace("Branch: pr-999-example\n", "")
    result = validate_task_spec(bad)
    assert result["valid"] is False
    assert len(result["errors"]) >= 1


def test_validate_task_spec_rejects_unknown_type():
    from src.mcp.tools.functional import validate_task_spec

    bad = _VALID_SPEC.replace("- Type: refactor", "- Type: nonsense")
    result = validate_task_spec(bad)
    assert result["valid"] is False
    assert any("type" in e.lower() or "nonsense" in e.lower() for e in result["errors"])


def test_validate_task_spec_rejects_freeform_depends_on():
    """Operator session 2026-05-02: ``Depends on: all P1 merged`` was rejected.

    The validator should reject natural-language depends-on strings;
    only ``none`` or ``PR-XXX(,PR-XXX)*`` is accepted.
    """
    from src.mcp.tools.functional import validate_task_spec

    bad = _VALID_SPEC.replace(
        "- Depends on: none", "- Depends on: all P1 merged"
    )
    result = validate_task_spec(bad)
    assert result["valid"] is False


def test_validate_task_spec_accepts_synonym_for_type():
    """Synonym map: ``bug`` should normalize to ``bugfix``."""
    from src.mcp.tools.functional import validate_task_spec

    spec = _VALID_SPEC.replace("- Type: refactor", "- Type: bug")
    result = validate_task_spec(spec)
    assert result == {"valid": True}


# ---- suggest_next_pr_number ----


def test_suggest_next_pr_number_empty_dir(tmp_path):
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    (fake_root / "owner__repo" / "tasks").mkdir(parents=True)

    with patch.object(functional, "_REPOS_ROOT", fake_root):
        assert functional.suggest_next_pr_number("owner__repo") == 1


def test_suggest_next_pr_number_missing_tasks_dir(tmp_path):
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    (fake_root / "owner__repo").mkdir(parents=True)
    # No tasks/ subdir.

    with patch.object(functional, "_REPOS_ROOT", fake_root):
        assert functional.suggest_next_pr_number("owner__repo") == 1


def test_suggest_next_pr_number_returns_max_plus_one(tmp_path):
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    tasks = fake_root / "owner__repo" / "tasks"
    tasks.mkdir(parents=True)
    for name in ("PR-001.md", "PR-100.md", "PR-236.md"):
        (tasks / name).write_text("# stub")

    with patch.object(functional, "_REPOS_ROOT", fake_root):
        assert functional.suggest_next_pr_number("owner__repo") == 237


def test_suggest_next_pr_number_handles_letter_suffixes(tmp_path):
    """PR-219a and PR-219b both count as integer 219; max is 219."""
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    tasks = fake_root / "owner__repo" / "tasks"
    tasks.mkdir(parents=True)
    for name in ("PR-218.md", "PR-219a.md", "PR-219b.md"):
        (tasks / name).write_text("# stub")

    with patch.object(functional, "_REPOS_ROOT", fake_root):
        assert functional.suggest_next_pr_number("owner__repo") == 220


def test_suggest_next_pr_number_ignores_non_pr_files(tmp_path):
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    tasks = fake_root / "owner__repo" / "tasks"
    tasks.mkdir(parents=True)
    for name in ("PR-005.md", "QUEUE.md", "README.md", "notes.txt"):
        (tasks / name).write_text("# stub")

    with patch.object(functional, "_REPOS_ROOT", fake_root):
        assert functional.suggest_next_pr_number("owner__repo") == 6


def test_suggest_next_pr_number_ignores_subdirectories(tmp_path):
    """Directories named like PR files should not be counted."""
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    tasks = fake_root / "owner__repo" / "tasks"
    tasks.mkdir(parents=True)
    (tasks / "PR-005.md").write_text("# stub")
    (tasks / "PR-999.md").mkdir()

    with patch.object(functional, "_REPOS_ROOT", fake_root):
        assert functional.suggest_next_pr_number("owner__repo") == 6


def test_suggest_next_pr_number_rejects_path_traversal(tmp_path):
    from src.mcp.tools.functional import suggest_next_pr_number

    with pytest.raises(ValueError, match="Invalid repo slug"):
        suggest_next_pr_number("../etc")
    with pytest.raises(ValueError, match="Invalid repo slug"):
        suggest_next_pr_number("owner/repo")
    with pytest.raises(ValueError, match="Invalid repo slug"):
        suggest_next_pr_number("owner\\repo")


def test_functional_tools_registered_with_mcp_server():
    """Both tools appear in the MCP server's tool registry after import."""
    import asyncio

    from src.mcp.server import mcp
    from src.mcp.tools import functional  # noqa: F401

    tools = asyncio.run(mcp.list_tools())
    tool_names = {t.name for t in tools}
    assert "validate_task_spec" in tool_names
    assert "suggest_next_pr_number" in tool_names
