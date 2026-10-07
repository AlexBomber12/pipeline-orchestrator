"""MCP server entrypoint.

Run with ``python -m src.mcp.server``. HTTP transport is exposed on
``localhost:5174`` by the primary Compose service. That container also runs a
tunnel-compatible listener on port 5173, with optional protected diagnostics.

Tools register here via decorators in PR-245 and PR-246. This module
handles only server instantiation, healthcheck, and run loop.
"""

from __future__ import annotations

import logging
import os
import re

import uvicorn

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from src.mcp.cloudflare_access import (
    CloudflareAccessConfig,
    CloudflareAccessMiddleware,
    CloudflareAccessVerifier,
)

logger = logging.getLogger(__name__)


def _runtime_diagnostics_enabled() -> bool:
    """Keep sensitive diagnostics off explicitly restricted MCP instances."""
    value = os.environ.get("MCP_RUNTIME_DIAGNOSTICS", "0").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _remote_diagnostics_enabled() -> bool:
    """Return the explicit remote opt-in, rejecting ambiguous values."""
    value = os.environ.get("MCP_REMOTE_DIAGNOSTICS", "0").strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"", "0", "false", "no", "off"}:
        return False
    raise ValueError("MCP_REMOTE_DIAGNOSTICS must be a boolean")


def _server_port() -> int:
    value = int(os.environ.get("MCP_SERVER_PORT", "5173"))
    if not 1 <= value <= 65_535:
        raise ValueError("MCP_SERVER_PORT must be between 1 and 65535")
    return value


def _public_hostname() -> str:
    """Validate the tunnel hostname used by the SDK transport safeguards."""
    value = os.environ.get("MCP_PUBLIC_HOSTNAME", "").strip().lower()
    if (
        len(value) > 253
        or "." not in value
        or not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", value)
        or any(
            not label or len(label) > 63 or label.startswith("-") or label.endswith("-")
            for label in value.split(".")
        )
    ):
        raise ValueError("MCP_PUBLIC_HOSTNAME must be a DNS hostname without a scheme, path, or port")
    return value


def _remote_access_config() -> tuple[CloudflareAccessConfig, str] | None:
    if not _remote_diagnostics_enabled():
        return None
    config = CloudflareAccessConfig.from_values(
        os.environ.get("MCP_CLOUDFLARE_ACCESS_ISSUER"),
        os.environ.get("MCP_CLOUDFLARE_ACCESS_AUDIENCE"),
    )
    return config, _public_hostname()


def _transport_security(
    remote_access: tuple[CloudflareAccessConfig, str] | None,
) -> TransportSecuritySettings | None:
    """Keep SDK host/origin checks on for the public listener."""
    if remote_access is None:
        return None
    public_host = remote_access[1]
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=[
            "127.0.0.1:*",
            "localhost:*",
            "mcp",
            "mcp:5173",
            public_host,
            f"{public_host}:*",
        ],
        allowed_origins=[f"https://{public_host}"],
    )


_PORT = _server_port()
_REMOTE_ACCESS = _remote_access_config()
_TRANSPORT_SECURITY = _transport_security(_REMOTE_ACCESS)
mcp = FastMCP(
    "pipeline-orchestrator",
    host="0.0.0.0",
    port=_PORT,
    transport_security=_TRANSPORT_SECURITY,
)


@mcp.tool()
def healthcheck() -> dict[str, str]:
    """Return server liveness and version info.

    LLM clients can call this to confirm the MCP server is reachable
    before invoking other tools.
    """
    return {"status": "ok", "service": "pipeline-orchestrator-mcp", "version": "v1"}


# Tool module imports MUST happen after the ``mcp`` instance is created
# so the ``@mcp.tool()`` decorators can register against it. Keep these
# imports at module level so registration fires at server startup.
from src.mcp.tools import functional, readonly  # noqa: E402, F401

if _runtime_diagnostics_enabled() or _REMOTE_ACCESS is not None:
    from src.mcp.tools import diagnostics  # noqa: E402, F401  # pragma: no cover - subprocess startup test


def main() -> None:  # pragma: no cover - exercised only when running the server
    """Run the MCP server with HTTP transport on the configured port."""
    logging.basicConfig(level=logging.INFO)
    if _REMOTE_ACCESS is None:
        logger.info("Starting MCP server on 0.0.0.0:%d", _PORT)
        mcp.run(transport="streamable-http")
        return
    logger.info("Starting authenticated remote MCP server on 0.0.0.0:%d", _PORT)
    app = CloudflareAccessMiddleware(
        mcp.streamable_http_app(),
        CloudflareAccessVerifier(_REMOTE_ACCESS[0]),
    )
    uvicorn.run(app, host="0.0.0.0", port=_PORT, log_level="info")


if __name__ == "__main__":  # pragma: no cover
    main()
