"""Functional MCP tools.

Tools with logic beyond a simple passthrough wrap. Both tools are
read-only with respect to filesystem state: ``validate_task_spec``
parses in-memory content; ``suggest_next_pr_number`` scans a
read-only mounted directory.
"""

from __future__ import annotations

import logging
import re
import tempfile
from pathlib import Path

from src.mcp.server import mcp
from src.queue_parser import (
    QueueValidationError,
    parse_task_header,
)

logger = logging.getLogger(__name__)

# Root path to managed repos inside the MCP container (mounted
# read-only per docker-compose.yml). Each subdirectory is one repo
# named ``<owner>__<repo>``.
_REPOS_ROOT = Path("/data/repos")

# Regex matching a task filename ``PR-<number>[<letter>].md``. The
# integer group is mandatory; the letter suffix (a/b/c for split PRs
# like PR-219a) is optional.
_PR_FILENAME = re.compile(r"^PR-(\d+)[a-z]?\.md$")

# Mirrors ``_REPO_SLUG_PATTERN`` in ``src/web/routes/onboarding.py``. A
# slug must start with an alphanumeric, contain only ``[A-Za-z0-9_.-]``
# on each side of the ``__`` separator, and contain no path separators.
# Anchoring on a leading alphanumeric implicitly rejects ``.`` and
# ``..`` traversal segments while still accepting otherwise valid names
# that happen to contain a ``..`` substring (e.g. ``owner__foo..bar``).
_REPO_SLUG_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9_.-]*__[A-Za-z0-9][A-Za-z0-9_.-]*$"
)


@mcp.tool()
def validate_task_spec(content: str) -> dict:
    """Validate a candidate task file body against the canonical schema.

    Wraps ``src.queue_parser.parse_task_header``. Returns a structured
    payload so MCP clients can surface errors to LLMs generating
    specs in real time, instead of waiting for upload-time rejection.

    Args:
        content: Full markdown body of a candidate ``PR-*.md`` file
            including the ``# PR-XXX: Title`` line and the header
            block (``Branch:``, ``- Type:``, ``- Complexity:``,
            ``- Depends on:``, ``- Priority:``, ``- Coder:``).

    Returns:
        ``{"valid": True}`` if the spec parses without error.
        ``{"valid": False, "errors": [<message>, ...]}`` when the
        parser raises ``QueueValidationError``. The error list
        contains one or more strings describing each violation.
    """
    # parse_task_header reads from disk; round-trip through a temp file
    # so the wrapper stays in src/mcp/ without modifying queue_parser.
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".md", delete=False, encoding="utf-8"
    ) as tmp:
        tmp.write(content)
        tmp_path = Path(tmp.name)
    try:
        parse_task_header(tmp_path)
    except QueueValidationError as exc:
        return {"valid": False, "errors": list(exc.issues)}
    finally:
        tmp_path.unlink(missing_ok=True)
    return {"valid": True}


@mcp.tool()
def suggest_next_pr_number(repo: str) -> int:
    """Return the next free PR number for a target repo.

    Scans ``/data/repos/<repo>/tasks/`` for files matching
    ``PR-<number>[<letter>].md`` and returns ``max + 1``. Subdivision
    suffixes (a/b/c) are collapsed to the integer; PR-219a and
    PR-219b both contribute integer 219.

    Args:
        repo: Repo slug in ``owner__repo`` form (e.g.
            ``AlexBomber12__pipeline-orchestrator``).

    Returns:
        Integer next-free PR number. Returns 1 if the tasks
        directory does not exist or contains no PR files.

    Raises:
        ValueError: if ``repo`` does not match the canonical
            ``owner__repo`` slug pattern. Defensive against malformed
            input even though the docker mount is read-only.
    """
    if not _REPO_SLUG_PATTERN.fullmatch(repo):
        raise ValueError(f"Invalid repo slug: {repo!r}")

    tasks_dir = _REPOS_ROOT / repo / "tasks"
    if not tasks_dir.is_dir():
        return 1

    max_num = 0
    for entry in tasks_dir.iterdir():
        if not entry.is_file():
            continue
        match = _PR_FILENAME.match(entry.name)
        if match is None:
            continue
        max_num = max(max_num, int(match.group(1)))

    return max_num + 1
