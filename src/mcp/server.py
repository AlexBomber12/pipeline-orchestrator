"""MCP server entrypoint.

Run with ``python -m src.mcp.server``. HTTP transport is exposed on
``localhost:5173`` by the primary Compose service. That container also runs a
restricted compatibility listener for the optional tunnel.

Tools register here via decorators in PR-245 and PR-246. This module
handles only server instantiation, healthcheck, and run loop.
"""

from __future__ import annotations

import logging
import os

from mcp.server.fastmcp import FastMCP

logger = logging.getLogger(__name__)


def _runtime_diagnostics_enabled() -> bool:
    """Keep sensitive diagnostics off explicitly restricted MCP instances."""
    value = os.environ.get("MCP_RUNTIME_DIAGNOSTICS", "0").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _server_port() -> int:
    value = int(os.environ.get("MCP_SERVER_PORT", "5173"))
    if not 1 <= value <= 65_535:
        raise ValueError("MCP_SERVER_PORT must be between 1 and 65535")
    return value


_PORT = _server_port()
mcp = FastMCP("pipeline-orchestrator", host="0.0.0.0", port=_PORT)


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

if _runtime_diagnostics_enabled():
    from src.mcp.tools import diagnostics  # noqa: E402, F401  # pragma: no cover - subprocess startup test


def main() -> None:  # pragma: no cover - exercised only when running the server
    """Run the MCP server with HTTP transport on the configured port."""
    logging.basicConfig(level=logging.INFO)
    logger.info("Starting MCP server on 0.0.0.0:%d", _PORT)
    mcp.run(transport="streamable-http")


if __name__ == "__main__":  # pragma: no cover
    main()
