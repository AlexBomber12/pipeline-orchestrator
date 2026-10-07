"""Smoke tests for the MCP server scaffold.

These tests verify that the server can be instantiated and that the
healthcheck tool is registered. They do not start the HTTP transport
(that requires a running event loop and port binding); transport
correctness is covered by future integration tests.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys

import pytest


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


def test_runtime_diagnostics_can_be_disabled_for_restricted_instances(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp import server

    monkeypatch.delenv("MCP_RUNTIME_DIAGNOSTICS", raising=False)
    assert server._runtime_diagnostics_enabled() is False
    monkeypatch.setenv("MCP_RUNTIME_DIAGNOSTICS", "off")
    assert server._runtime_diagnostics_enabled() is False
    monkeypatch.setenv("MCP_RUNTIME_DIAGNOSTICS", "yes")
    assert server._runtime_diagnostics_enabled() is True

    monkeypatch.delenv("MCP_SERVER_PORT", raising=False)
    assert server._server_port() == 5173
    monkeypatch.setenv("MCP_SERVER_PORT", "5174")
    assert server._server_port() == 5174
    monkeypatch.setenv("MCP_SERVER_PORT", "0")
    with pytest.raises(ValueError, match="between 1 and 65535"):
        server._server_port()


def test_restricted_mcp_instance_does_not_register_runtime_diagnostics() -> None:
    script = (
        "import asyncio, json; "
        "from src.mcp.server import mcp; "
        "print(json.dumps(sorted(tool.name for tool in asyncio.run(mcp.list_tools()))))"
    )
    environment = {**os.environ, "MCP_RUNTIME_DIAGNOSTICS": "0"}

    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )
    names = json.loads(completed.stdout.strip().splitlines()[-1])

    assert "healthcheck" in names
    assert "get_task_schema" in names
    assert "get_orchestrator_status" not in names


def test_opted_in_mcp_instance_registers_runtime_diagnostics() -> None:
    script = (
        "import asyncio, json; "
        "from src.mcp.server import mcp; "
        "print(json.dumps(sorted(tool.name for tool in asyncio.run(mcp.list_tools()))))"
    )
    environment = {**os.environ, "MCP_RUNTIME_DIAGNOSTICS": "1"}

    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )
    names = json.loads(completed.stdout.strip().splitlines()[-1])

    assert "get_orchestrator_status" in names
    assert "list_orchestrator_logs" not in names
    assert "read_orchestrator_log" not in names
