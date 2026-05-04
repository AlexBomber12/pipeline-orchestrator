"""MCP server entrypoint.

Run with ``python -m src.mcp.server``. HTTP transport on
``localhost:5173``. No authentication; deployment is self-hosted scope
only and the port is bound to localhost in docker compose.

Tools register here via decorators in PR-245 and PR-246. This module
handles only server instantiation, healthcheck, and run loop.
"""

from __future__ import annotations

import logging

from mcp.server.fastmcp import FastMCP

logger = logging.getLogger(__name__)

mcp = FastMCP("pipeline-orchestrator", host="0.0.0.0", port=5173)


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


def main() -> None:  # pragma: no cover - exercised only when running the server
    """Run the MCP server with HTTP transport on port 5173."""
    logging.basicConfig(level=logging.INFO)
    logger.info("Starting MCP server on 0.0.0.0:5173")
    mcp.run(transport="streamable-http")


if __name__ == "__main__":  # pragma: no cover
    main()
