"""Functional MCP tools.

Tools with logic beyond a simple passthrough wrap. All tools are
read-only with respect to filesystem state: ``validate_task_spec``
parses in-memory content; ``suggest_next_pr_number`` and
``get_repo_task_status`` scan a read-only mounted directory.
"""

from __future__ import annotations

import logging
import re
import tempfile
from pathlib import Path
from typing import Annotated

from src.mcp.scans import scan_for_conflicts
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

# Frontmatter status reader regex. Mirrors the canonical reader in
# ``src/web/routes/repo_control.py`` so the MCP module stays free of
# FastAPI imports. See PR-338.
_FRONTMATTER_DELIMITER = "---"
_FRONTMATTER_STATUS_LINE = re.compile(r"^status:\s*(.+?)\s*$")
_CANONICAL_STATUSES = {"TODO", "DONE", "ERROR"}

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
        A dict with four keys:

        - ``valid``: ``True`` when the spec parses cleanly AND no
          AGENTS.md anti-pattern violations were detected.
        - ``errors``: legacy alias of ``schema_errors`` retained for
          MCP clients written against the PR-246 shape, which read
          ``result["errors"]`` directly. Always present (empty list
          when the spec parses cleanly) so legacy callers do not
          ``KeyError`` on the success path either.
        - ``schema_errors``: list of strings from
          ``QueueValidationError.issues`` when the parser rejects
          the spec; empty list otherwise.
        - ``agents_violations``: list of dicts (``type``, ``excerpt``,
          ``rule``) for each AGENTS.md anti-pattern matched in the
          spec body; empty list otherwise. PR-259.
    """
    # parse_task_header reads from disk; round-trip through a temp file
    # so the wrapper stays in src/mcp/ without modifying queue_parser.
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".md", delete=False, encoding="utf-8"
    ) as tmp:
        tmp.write(content)
        tmp_path = Path(tmp.name)
    schema_errors: list[str] = []
    try:
        parse_task_header(tmp_path)
    except QueueValidationError as exc:
        schema_errors = list(exc.issues)
    finally:
        tmp_path.unlink(missing_ok=True)

    violations = scan_for_conflicts(content)
    return {
        "valid": not schema_errors and not violations,
        # ``errors`` is a backward-compat alias of ``schema_errors``
        # for PR-246 callers; both list the same diagnostics.
        "errors": schema_errors,
        "schema_errors": schema_errors,
        "agents_violations": [
            {
                "type": v.violation_type,
                "excerpt": v.line_excerpt,
                "rule": v.rule,
            }
            for v in violations
        ],
    }


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


def _strip_inline_comment_and_quotes(value: str) -> str:
    """Mirror ``_normalize_frontmatter_status`` in ``src.queue_parser``.

    A ``#`` is treated as an inline comment delimiter only when it is at
    the start of the value or preceded by whitespace, and only outside
    of any surrounding single- or double-quoted region. After the
    optional comment strip, leading/trailing whitespace and a single
    matching pair of surrounding quotes are removed.
    """
    quote: str | None = None
    for index, char in enumerate(value):
        if char in {"'", '"'}:
            if quote is None:
                quote = char
            elif quote == char:
                quote = None
        elif char == "#" and quote is None and (
            index == 0 or value[index - 1].isspace()
        ):
            value = value[:index]
            break
    stripped = value.strip()
    if (
        len(stripped) >= 2
        and stripped[0] == stripped[-1]
        and stripped[0] in {"'", '"'}
    ):
        return stripped[1:-1]
    return stripped


def _read_frontmatter_status(task_path: Path) -> str:
    """Read the canonical uppercase status from a task file frontmatter.

    Returns ``"TODO"`` when frontmatter is absent or unterminated, the
    status key is missing, the value is unrecognized, or the file
    cannot be read or decoded. Mirrors ``parse_task_header`` in
    ``src/queue_parser.py``: status is only read from a CLOSED
    frontmatter block, and when multiple ``status:`` lines appear the
    LAST one wins. Kept local so the MCP package does not pull in
    FastAPI imports.
    """
    try:
        lines = task_path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError):
        return "TODO"

    first_content_index = next(
        (index for index, raw_line in enumerate(lines) if raw_line.strip()),
        None,
    )
    if (
        first_content_index is None
        or lines[first_content_index].rstrip() != _FRONTMATTER_DELIMITER
    ):
        return "TODO"

    frontmatter_end_index: int | None = None
    for index in range(first_content_index + 1, len(lines)):
        if lines[index].rstrip() == _FRONTMATTER_DELIMITER:
            frontmatter_end_index = index
            break
    if frontmatter_end_index is None:
        return "TODO"

    raw_status: str | None = None
    for raw_line in lines[first_content_index + 1 : frontmatter_end_index]:
        status_match = _FRONTMATTER_STATUS_LINE.match(raw_line.rstrip())
        if status_match is None:
            continue
        raw_status = _strip_inline_comment_and_quotes(status_match.group(1))

    if raw_status is None:
        return "TODO"
    canonical = raw_status.upper()
    if canonical in _CANONICAL_STATUSES:
        return canonical
    return "TODO"


@mcp.tool()
def get_repo_task_status(
    repo_slug: Annotated[
        str,
        "Repo slug in owner__repo form (e.g. AlexBomber12__pipeline-orchestrator).",
    ],
) -> dict[str, str]:
    """Return a map of ``task_id`` to canonical frontmatter status.

    Scans ``/data/repos/<repo_slug>/tasks/`` for ``PR-*.md`` files and
    reads each task's YAML frontmatter ``status:`` field. Values are
    normalized to uppercase canonical tokens (``TODO``, ``DONE``,
    ``ERROR``); files without frontmatter or with unreadable contents
    are reported as ``TODO``.

    LLM clients call this before preparing a spec-upload zip so that
    already-merged specs are not regressed to ``status: TODO``. The
    server-side upload guard from PR-337 still catches anything that
    slips through; this tool is the source-side prevention layer
    (PR-FUTURE-MCP-STATUS, OBS-DA).

    Returns an empty dict when the repo or its ``tasks/`` directory
    does not exist. Raises ``ValueError`` for slugs that do not match
    the canonical ``owner__repo`` pattern.
    """
    if not _REPO_SLUG_PATTERN.fullmatch(repo_slug):
        raise ValueError(f"Invalid repo_slug: {repo_slug!r}")

    tasks_dir = _REPOS_ROOT / repo_slug / "tasks"
    if not tasks_dir.is_dir():
        return {}

    result: dict[str, str] = {}
    for spec_path in tasks_dir.glob("PR-*.md"):
        if not spec_path.is_file():
            continue
        result[spec_path.stem] = _read_frontmatter_status(spec_path)
    return result
