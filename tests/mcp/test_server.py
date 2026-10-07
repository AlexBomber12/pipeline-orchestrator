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
from pathlib import Path

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


def test_remote_diagnostics_configuration_is_explicit_and_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp import server
    from src.mcp.cloudflare_access import CloudflareAccessConfig

    monkeypatch.delenv("MCP_REMOTE_DIAGNOSTICS", raising=False)
    assert server._remote_diagnostics_enabled() is False
    assert server._remote_access_config() is None
    assert server._transport_security(None) is None

    monkeypatch.setenv("MCP_REMOTE_DIAGNOSTICS", "sometimes")
    with pytest.raises(ValueError, match="must be a boolean"):
        server._remote_diagnostics_enabled()

    monkeypatch.setenv("MCP_REMOTE_DIAGNOSTICS", "yes")
    monkeypatch.setenv("MCP_CLOUDFLARE_ACCESS_ISSUER", "https://operators.cloudflareaccess.com")
    monkeypatch.setenv("MCP_CLOUDFLARE_ACCESS_AUDIENCE", "a" * 64)
    monkeypatch.setenv("MCP_PUBLIC_HOSTNAME", "MCP.EXAMPLE.COM")
    remote_access = server._remote_access_config()
    assert remote_access == (
        CloudflareAccessConfig("https://operators.cloudflareaccess.com", "a" * 64),
        "mcp.example.com",
    )
    security = server._transport_security(remote_access)
    assert security is not None
    assert security.enable_dns_rebinding_protection is True
    assert "mcp.example.com" in security.allowed_hosts
    assert security.allowed_origins == ["https://mcp.example.com"]

    for hostname in ("", "localhost", "https://mcp.example.com", "mcp.example.com:443", "-mcp.example.com"):
        monkeypatch.setenv("MCP_PUBLIC_HOSTNAME", hostname)
        with pytest.raises(ValueError, match="DNS hostname"):
            server._public_hostname()


def test_remote_listener_rejects_incomplete_configuration_before_import() -> None:
    script = "import src.mcp.server"
    environment = {
        **os.environ,
        "MCP_REMOTE_DIAGNOSTICS": "1",
        "MCP_PUBLIC_HOSTNAME": "mcp.example.com",
        "MCP_CLOUDFLARE_ACCESS_ISSUER": "",
        "MCP_CLOUDFLARE_ACCESS_AUDIENCE": "",
    }
    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    assert completed.returncode != 0
    assert "CloudflareAccessConfigurationError" in completed.stderr


def test_remote_listener_registers_diagnostics_and_transport_allowlist() -> None:
    script = (
        "import asyncio, json; "
        "from src.mcp.server import mcp; "
        "print(json.dumps({"
        "'tools': sorted(tool.name for tool in asyncio.run(mcp.list_tools())), "
        "'security': mcp.settings.transport_security.model_dump()}))"
    )
    environment = {
        **os.environ,
        "MCP_REMOTE_DIAGNOSTICS": "1",
        "MCP_PUBLIC_HOSTNAME": "mcp.example.com",
        "MCP_CLOUDFLARE_ACCESS_ISSUER": "https://operators.cloudflareaccess.com",
        "MCP_CLOUDFLARE_ACCESS_AUDIENCE": "a" * 64,
    }
    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )
    result = json.loads(completed.stdout.strip().splitlines()[-1])
    assert set(result["tools"]) == {
        "healthcheck",
        "validate_task_spec",
        "suggest_next_pr_number",
        "get_task_schema",
        "get_agents_md_template",
        "get_repo_task_status",
        "get_orchestrator_status",
    }
    assert result["security"]["enable_dns_rebinding_protection"] is True
    assert "mcp.example.com" in result["security"]["allowed_hosts"]


@pytest.mark.parametrize(
    ("remote_value", "remote_diagnostics", "remote_redis"),
    [
        ("0", "0", "unset"),
        ("1", "1", "redis://redis:6379/0"),
        (" true ", "1", "redis://redis:6379/0"),
    ],
)
def test_mcp_entrypoint_preserves_local_diagnostics_and_gates_remote_redis(
    tmp_path: Path,
    remote_value: str,
    remote_diagnostics: str,
    remote_redis: str,
) -> None:
    fake_python = tmp_path / "python"
    fake_python.write_text(
        "#!/usr/bin/env bash\n"
        'printf "%s|%s|%s\\n" "${MCP_SERVER_PORT}" "${MCP_RUNTIME_DIAGNOSTICS}" "${REDIS_URL:-unset}"\n'
        'if [[ "${MCP_SERVER_PORT}" == "5173" ]]; then sleep 0.1; exit 7; fi\n'
        "exec sleep 30\n",
        encoding="utf-8",
    )
    fake_python.chmod(0o755)
    environment = {
        **os.environ,
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "MCP_REMOTE_DIAGNOSTICS": remote_value,
        "REDIS_URL": "redis://redis:6379/0",
    }
    completed = subprocess.run(
        ["bash", "scripts/mcp-entrypoint.sh"],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
        timeout=5,
    )
    assert completed.returncode == 7
    assert f"5173|{remote_diagnostics}|{remote_redis}" in completed.stdout
    assert "5174|1|redis://redis:6379/0" in completed.stdout


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
