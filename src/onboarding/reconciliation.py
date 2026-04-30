"""Dry-run + apply reconciliation for daemon-managed AGENTS.md sections.

This module is the operator-facing piece of the onboarding flow. PR-192a
built the section-marker framework (``markdown_sections``); PR-192b made
this repo's own AGENTS.md the canonical template
(``agents_md_template``). Here we use both to compute, and optionally
write, the reconciliation that brings a target repo's AGENTS.md into
alignment with the daemon-managed sections.

The primary entry point is :func:`reconcile_agents_md`. Dry-run is the
default and apply is intentionally identical to dry-run plus a single
``write_text`` so the diff that gets surfaced to the operator is
guaranteed to match what would actually be written.
"""

from __future__ import annotations

import difflib
from pathlib import Path

from src.onboarding.agents_md_template import daemon_managed_content
from src.onboarding.markdown_sections import apply_managed_regions


def _proposed_content(current_content: str) -> str:
    """Return what the AGENTS.md file would look like after reconciliation."""
    regions = daemon_managed_content()
    return apply_managed_regions(current_content, regions)


def _unified_diff_text(before: str, after: str, label: str) -> str:
    """Return a unified diff between ``before`` and ``after``."""
    before_lines = before.splitlines(keepends=True)
    after_lines = after.splitlines(keepends=True)
    diff = difflib.unified_diff(
        before_lines,
        after_lines,
        fromfile=f"a/{label}",
        tofile=f"b/{label}",
    )
    return "".join(diff)


def reconcile_agents_md(
    file_path: str | Path, *, dry_run: bool = True
) -> tuple[str, str]:
    """Reconcile daemon-managed sections in an AGENTS.md file.

    Reads ``file_path`` (treating a missing file as an empty AGENTS.md),
    computes the reconciled content via
    :func:`apply_managed_regions`, and returns ``(content, diff)``:

    - ``content`` is the proposed (or just-written) full file body.
    - ``diff`` is a unified diff comparing the file's prior content to
      ``content``. An empty diff means nothing would change.

    When ``dry_run`` is True (default) the file is left untouched. When
    ``dry_run`` is False the proposed content is written to
    ``file_path``; parent directories are created as needed so onboarding
    a brand-new repo path does not require a separate mkdir step.
    """
    path = Path(file_path)
    try:
        before = path.read_text()
    except FileNotFoundError:
        before = ""

    after = _proposed_content(before)
    diff = _unified_diff_text(before, after, str(path))

    if not dry_run:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(after)

    return after, diff
