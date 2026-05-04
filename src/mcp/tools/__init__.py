"""MCP tool implementations.

Tools register themselves with the FastMCP server at module import time
via the ``@mcp.tool()`` decorator. The server entrypoint
(``src.mcp.server``) imports each tool module so registrations fire
before ``mcp.run`` is called.
"""
