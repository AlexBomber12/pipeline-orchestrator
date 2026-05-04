"""Tests for read-only MCP tools."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest


def test_get_task_schema_returns_markdown_content():
    """get_task_schema reads TASK_SCHEMA.md and returns its content."""
    from src.mcp.tools.readonly import get_task_schema

    # In test environment the path differs from the container's
    # ``/app/docs/TASK_SCHEMA.md`` mount; patch to the actual file in
    # this repo.
    real_path = Path(__file__).resolve().parents[2] / "docs" / "TASK_SCHEMA.md"
    with patch("src.mcp.tools.readonly._TASK_SCHEMA_PATH", real_path):
        content = get_task_schema()

    assert content.startswith("# Task file schema")
    assert "Branch:" in content
    assert "Type:" in content
    assert "synonym" in content.lower()


def test_get_task_schema_raises_when_file_missing(tmp_path):
    """get_task_schema raises FileNotFoundError when schema is missing."""
    from src.mcp.tools import readonly

    missing_path = tmp_path / "does-not-exist.md"
    with patch.object(readonly, "_TASK_SCHEMA_PATH", missing_path):
        with pytest.raises(FileNotFoundError, match="TASK_SCHEMA.md not found"):
            readonly.get_task_schema()


def test_get_agents_md_template_returns_managed_content_for_orchestrator():
    """get_agents_md_template wraps daemon_managed_content correctly."""
    from src.mcp.tools.readonly import get_agents_md_template

    content = get_agents_md_template("AlexBomber12__pipeline-orchestrator")

    assert "<!-- pipeline-orchestrator: managed BEGIN" in content
    assert "<!-- pipeline-orchestrator: managed END" in content


def test_get_agents_md_template_handles_arbitrary_slug():
    """get_agents_md_template works for any repo slug, not just orchestrator self."""
    from src.mcp.tools.readonly import get_agents_md_template

    content = get_agents_md_template("AlexBomber12__megaraid-dashboard")
    assert "<!-- pipeline-orchestrator: managed BEGIN" in content


def test_get_agents_md_template_includes_all_managed_sections():
    """Every section in MANAGED_SECTIONS appears once with BEGIN and END markers."""
    from src.mcp.tools.readonly import get_agents_md_template
    from src.onboarding.agents_md_template import MANAGED_SECTIONS

    content = get_agents_md_template("AlexBomber12__pipeline-orchestrator")
    for name in MANAGED_SECTIONS:
        assert f"<!-- pipeline-orchestrator: managed BEGIN {name} -->" in content
        assert f"<!-- pipeline-orchestrator: managed END {name} -->" in content


def test_readonly_tools_registered_with_mcp_server():
    """Both tools appear in the MCP server's tool registry after import."""
    import asyncio

    from src.mcp.server import mcp

    # Importing the readonly module triggers @mcp.tool() registration.
    from src.mcp.tools import readonly  # noqa: F401

    tools = asyncio.run(mcp.list_tools())
    tool_names = {t.name for t in tools}
    assert "get_task_schema" in tool_names
    assert "get_agents_md_template" in tool_names
