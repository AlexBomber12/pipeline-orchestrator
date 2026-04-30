"""Manage daemon-controlled regions inside a markdown file.

Regions are delimited by HTML comment markers so they remain invisible
when the markdown is rendered, while staying easy to grep and diff:

    <!-- pipeline-orchestrator: managed BEGIN section_name -->
    ...region body...
    <!-- pipeline-orchestrator: managed END section_name -->

The text between the BEGIN and END markers is preserved byte-for-byte
when round-tripped through ``extract_managed_regions`` and
``apply_managed_regions``, which lets PR-192b safely overwrite specific
sections of an existing AGENTS.md without disturbing user content.
"""

from __future__ import annotations

import re

_MARKER_RE = re.compile(
    r"<!-- pipeline-orchestrator: managed (BEGIN|END) (\S+) -->"
)


class MarkerError(ValueError):
    """Raised when managed-region markers are malformed."""


def _start_marker(name: str) -> str:
    return f"<!-- pipeline-orchestrator: managed BEGIN {name} -->"


def _end_marker(name: str) -> str:
    return f"<!-- pipeline-orchestrator: managed END {name} -->"


def validate_no_user_content_inside_markers(content: str) -> None:
    """Raise ``MarkerError`` when marker pairs are not well-formed.

    Detects: a BEGIN inside another open region (nested), an END that
    does not close the currently-open BEGIN (missing or mismatched
    BEGIN), and a BEGIN that is never closed before end of file.
    """
    open_name: str | None = None
    for match in _MARKER_RE.finditer(content):
        kind, name = match.group(1), match.group(2)
        if kind == "BEGIN":
            if open_name is not None:
                raise MarkerError(
                    f"nested marker: BEGIN {name!r} opened while "
                    f"{open_name!r} is still open"
                )
            open_name = name
        else:
            if open_name != name:
                raise MarkerError(
                    f"END marker {name!r} does not close an open BEGIN"
                )
            open_name = None
    if open_name is not None:
        raise MarkerError(
            f"BEGIN marker {open_name!r} has no matching END"
        )


def extract_managed_regions(content: str) -> dict[str, str]:
    """Return ``{section_name: region_text}`` for every managed region."""
    validate_no_user_content_inside_markers(content)
    matches = list(_MARKER_RE.finditer(content))
    regions: dict[str, str] = {}
    for begin, end in zip(matches[0::2], matches[1::2]):
        regions[begin.group(2)] = content[begin.end():end.start()]
    return regions


def apply_managed_regions(
    content: str, regions: dict[str, str]
) -> str:
    """Return ``content`` with ``regions`` applied.

    Existing managed regions whose name appears in ``regions`` are
    replaced in place; their surrounding markers stay where they are.
    Names not yet present are appended at the end of the file.
    """
    existing = extract_managed_regions(content)
    matches = list(_MARKER_RE.finditer(content))

    replacements: list[tuple[int, int, str]] = []
    for begin, end in zip(matches[0::2], matches[1::2]):
        name = begin.group(2)
        if name in regions:
            replacements.append((begin.end(), end.start(), regions[name]))

    result = content
    for start, stop, new_text in reversed(replacements):
        result = result[:start] + new_text + result[stop:]

    for name, region_text in regions.items():
        if name in existing:
            continue
        if result and not result.endswith("\n"):
            result += "\n"
        result += (
            f"{_start_marker(name)}{region_text}{_end_marker(name)}\n"
        )

    return result
