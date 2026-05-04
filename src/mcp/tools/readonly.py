"""Read-only MCP tools.

Wraps existing orchestrator content with no logic changes. Both tools
are pure: filesystem read or module function call, return string.
"""

from __future__ import annotations

import logging
from pathlib import Path

from src.mcp.server import mcp
from src.onboarding.agents_md_template import daemon_managed_content

logger = logging.getLogger(__name__)

# Path to TASK_SCHEMA.md inside the container. The MCP service mounts
# the orchestrator repo at ``/app`` (read-only per docker-compose.yml).
_TASK_SCHEMA_PATH = Path("/app/docs/TASK_SCHEMA.md")


@mcp.tool()
def get_task_schema() -> str:
    """Return the canonical task file format definition.

    Reads ``docs/TASK_SCHEMA.md`` from the orchestrator repo. LLM
    clients generating ``tasks/PR-*.md`` specs should call this tool
    for the authoritative format including type vocabulary, complexity
    levels, depends-on syntax, and synonym handling rules.
    """
    if not _TASK_SCHEMA_PATH.exists():
        raise FileNotFoundError(
            f"TASK_SCHEMA.md not found at {_TASK_SCHEMA_PATH}. "
            "Verify the docker-compose mount of the orchestrator repo."
        )
    return _TASK_SCHEMA_PATH.read_text(encoding="utf-8")


@mcp.tool()
def get_agents_md_template(repo_slug: str) -> str:
    """Return the daemon-managed AGENTS.md sections for a target repo.

    Wraps ``src.onboarding.agents_md_template.daemon_managed_content``
    and reassembles the section dict into a single markdown string with
    ``<!-- pipeline-orchestrator: managed BEGIN/END ... -->`` markers
    wrapping each region. ``repo_slug`` is accepted for forward
    compatibility with per-repo overrides; v1 returns the canonical
    content unchanged.
    """
    del repo_slug  # accepted for forward compatibility; unused in v1
    sections = daemon_managed_content()
    parts: list[str] = []
    for name, body in sections.items():
        parts.append(
            f"<!-- pipeline-orchestrator: managed BEGIN {name} -->"
            f"{body}"
            f"<!-- pipeline-orchestrator: managed END {name} -->"
        )
    return "\n".join(parts) + "\n"
