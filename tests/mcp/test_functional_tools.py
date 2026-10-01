"""Tests for functional MCP tools."""

from __future__ import annotations

from unittest.mock import patch

import pytest

# ---- validate_task_spec ----

_VALID_BUDGET = """task_budget:
  version: 1
  production_lines: 60
  test_lines: 90
  other_lines: 10
  production_files: 1
  total_files: 2
"""

_VALID_SPEC = f"""---
status: TODO
{_VALID_BUDGET}---

# PR-999: Example task

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
    assert result == {
        "valid": True,
        "errors": [],
        "schema_errors": [],
        "agents_violations": [],
    }


def test_validate_task_spec_accepts_valid_medium_budget():
    from src.mcp.tools.functional import validate_task_spec

    spec = _VALID_SPEC.replace("- Complexity: low", "- Complexity: medium")
    spec = spec.replace(
        "  total_files: 2\n",
        "  total_files: 2\n"
        "  rationale: Local change with a known implementation approach.\n",
    )

    assert validate_task_spec(spec)["valid"] is True


def test_validate_task_spec_requires_budget_in_opening_frontmatter():
    from src.mcp.tools.functional import validate_task_spec

    missing = _VALID_SPEC.replace(_VALID_BUDGET, "")
    body_only = missing + f"\n```yaml\n{_VALID_BUDGET}```\n"

    for spec in (missing, body_only):
        result = validate_task_spec(spec)
        assert result["valid"] is False
        assert result["errors"] == result["schema_errors"]
        assert result["schema_errors"] == [
            "frontmatter is missing required task_budget mapping"
        ]


@pytest.mark.parametrize(
    ("old", "new", "expected"),
    [
        (
            "  production_lines: 60",
            "  production_lines: 201",
            "production_lines must be <= 200; got 201",
        ),
        (
            "  test_lines: 90",
            "  test_lines: true",
            "test_lines must be a nonnegative integer",
        ),
        (
            "- Complexity: low",
            "- Complexity: high",
            "high complexity is not allowed; split the task",
        ),
    ],
)
def test_validate_task_spec_rejects_invalid_budgets(old, new, expected):
    from src.mcp.tools.functional import validate_task_spec

    result = validate_task_spec(_VALID_SPEC.replace(old, new))

    assert result["valid"] is False
    assert result["errors"] == result["schema_errors"]
    assert expected in result["schema_errors"]


@pytest.mark.parametrize(
    ("frontmatter", "expected"),
    [
        ("status: TODO\nmetadata: [broken\n", "invalid YAML frontmatter"),
        ("status: TODO\nstatus: DONE\n", "duplicate key"),
        ("- not-a-mapping\n", "frontmatter must be a mapping"),
        ("status: TODO\nmetadata: !unsupported value\n", "could not determine"),
    ],
)
def test_validate_task_spec_rejects_unsafe_or_malformed_frontmatter(
    frontmatter, expected
):
    from src.mcp.tools.functional import validate_task_spec

    spec = _VALID_SPEC.replace(f"status: TODO\n{_VALID_BUDGET}", frontmatter)
    result = validate_task_spec(spec)

    assert result["valid"] is False
    assert any(expected in error for error in result["schema_errors"])


def test_validate_task_spec_accepts_leading_blank_lines():
    from src.mcp.tools.functional import validate_task_spec

    assert validate_task_spec("\n \n" + _VALID_SPEC)["valid"] is True


def test_validate_task_spec_rejects_missing_branch_field():
    from src.mcp.tools.functional import validate_task_spec

    bad = _VALID_SPEC.replace("Branch: pr-999-example\n", "")
    result = validate_task_spec(bad)
    assert result["valid"] is False
    assert len(result["schema_errors"]) >= 1


def test_validate_task_spec_rejects_unknown_type():
    from src.mcp.tools.functional import validate_task_spec

    bad = _VALID_SPEC.replace("- Type: refactor", "- Type: nonsense")
    result = validate_task_spec(bad)
    assert result["valid"] is False
    assert any(
        "type" in e.lower() or "nonsense" in e.lower()
        for e in result["schema_errors"]
    )


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
    assert result == {
        "valid": True,
        "errors": [],
        "schema_errors": [],
        "agents_violations": [],
    }


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
    with pytest.raises(ValueError, match="Invalid repo slug"):
        suggest_next_pr_number("..")


def test_suggest_next_pr_number_accepts_dotted_slug(tmp_path):
    """Slugs matching the onboarding regex must be accepted.

    The onboarding validator (``src/web/routes/onboarding.py``) allows
    ``[A-Za-z0-9_.-]`` on each side of ``__``, so ``owner__foo..bar``
    is a configured-repo slug; the MCP tool must not reject it just
    because the substring ``..`` appears.
    """
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    tasks = fake_root / "owner__foo..bar" / "tasks"
    tasks.mkdir(parents=True)
    (tasks / "PR-007.md").write_text("# stub")

    with patch.object(functional, "_REPOS_ROOT", fake_root):
        assert functional.suggest_next_pr_number("owner__foo..bar") == 8


def test_functional_tools_registered_with_mcp_server():
    """Both tools appear in the MCP server's tool registry after import."""
    import asyncio

    from src.mcp.server import mcp
    from src.mcp.tools import functional  # noqa: F401

    tools = asyncio.run(mcp.list_tools())
    tool_names = {t.name for t in tools}
    assert "validate_task_spec" in tool_names
    assert "suggest_next_pr_number" in tool_names
