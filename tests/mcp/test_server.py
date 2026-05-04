"""Smoke tests for the MCP server scaffold.

These tests verify that the server can be instantiated and that the
healthcheck tool is registered. They do not start the HTTP transport
(that requires a running event loop and port binding); transport
correctness is covered by future integration tests.
"""

from __future__ import annotations

import asyncio


def test_mcp_server_imports() -> None:
    """Verify the server module imports without side effects."""
    from src.mcp import server

    assert server.mcp is not None
    assert server.mcp.name == "pipeline-orchestrator"


def test_healthcheck_tool_registered() -> None:
    """Verify the healthcheck tool is discoverable via the server registry."""
    from src.mcp.server import mcp

    tools = asyncio.run(mcp.list_tools())
    tool_names = [t.name for t in tools]
    assert "healthcheck" in tool_names


def test_healthcheck_returns_status_ok() -> None:
    """Verify the healthcheck function returns the expected payload."""
    from src.mcp.server import healthcheck

    result = healthcheck()
    assert result["status"] == "ok"
    assert result["service"] == "pipeline-orchestrator-mcp"
    assert "version" in result
