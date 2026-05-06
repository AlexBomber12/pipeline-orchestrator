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
import re
from collections.abc import Callable
from pathlib import Path

from src.mcp.scans import scan_for_conflicts
from src.onboarding.agents_md_template import daemon_managed_content
from src.onboarding.markdown_sections import apply_managed_regions

# Mirrors the marker regex in ``markdown_sections``; duplicated here so the
# legacy migration can locate managed-region spans without leaking that
# private symbol across modules.
_MANAGED_MARKER_RE = re.compile(
    r"<!-- pipeline-orchestrator: managed (BEGIN|END) (\S+) -->"
)


# A pre-PR-271 onboarded repo's AGENTS.md (or a freshly scaffolded one whose
# template still carries the unmarked block) has a "Quick rules" heading
# followed by a dash-bullet list with no managed markers. Now that
# ``quick_rules`` is a managed section, ``apply_managed_regions`` would
# leave that legacy block in place and append a second managed copy at EOF,
# producing two conflicting copies. Detect the legacy form and strip it
# before reconciliation runs.
#
# The first bullet is anchored on the canonical legacy template phrase
# "Always choose a work mode" so user-authored sections that happen to use
# the same "Quick rules" heading + dash-bullet structure (but with their
# own first bullet) are left untouched. The new managed quick_rules opens
# with "When invoked by the pipeline-orchestrator daemon..." instead, so
# the narrow signature cannot match a managed block either.
_LEGACY_QUICK_RULES_RE = re.compile(
    r"""
    ^(?:[ \t]*\#{1,3}[ \t]+)?Quick[ \t]+rules[ \t]*\n   # heading line
    [ \t]*-[ \t]+Always[ \t]+choose[ \t]+a[ \t]+work[ \t]+mode[^\n]*\n
                                                         # legacy signature bullet
    (?:[ \t]*-[ \t][^\n]*\n)*                            # 0+ trailing bullets
    """,
    re.MULTILINE | re.VERBOSE,
)


def _strip_legacy_quick_rules(content: str) -> str:
    """Remove an unmarked legacy 'Quick rules' block outside managed regions.

    Operates only on segments that fall outside ``pipeline-orchestrator``
    managed markers so the regex cannot accidentally match the heading of
    the new managed ``quick_rules`` section. Each unmanaged segment is
    matched once; multiple unmarked blocks across separate segments would
    each get stripped.
    """
    matches = list(_MANAGED_MARKER_RE.finditer(content))
    if not matches:
        return _LEGACY_QUICK_RULES_RE.sub("", content, count=1)

    parts: list[str] = []
    cursor = 0
    for begin, end in zip(matches[0::2], matches[1::2]):
        outside = content[cursor:begin.start()]
        parts.append(_LEGACY_QUICK_RULES_RE.sub("", outside, count=1))
        parts.append(content[begin.start():end.end()])
        cursor = end.end()
    parts.append(
        _LEGACY_QUICK_RULES_RE.sub("", content[cursor:], count=1)
    )
    return "".join(parts)


def _proposed_content(current_content: str) -> str:
    """Return what the AGENTS.md file would look like after reconciliation."""
    regions = daemon_managed_content()
    migrated = _strip_legacy_quick_rules(current_content)
    return apply_managed_regions(migrated, regions)


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


def _scan_existing_task_specs(
    repo_path: Path,
    log_event_fn: Callable[[str], None],
) -> int:
    """Scan all ``tasks/PR-*.md`` files for AGENTS.md anti-patterns.

    Returns the number of files that contain at least one violation.
    Each individual violation emits one ``[AGENTS-SCAN]`` event via
    ``log_event_fn``; a trailing summary event is emitted when the count
    is non-zero so operators can spot drift in the dashboard event log
    without scrolling. Used at AGENTS.md reconciliation time to catch
    drift between rules and pre-existing task specs that were never
    re-validated by the inline MCP scanner from PR-259. PR-260.
    """
    tasks_dir = repo_path / "tasks"
    if not tasks_dir.is_dir():
        return 0

    files_with_violations = 0
    for spec_file in sorted(tasks_dir.glob("PR-*.md")):
        try:
            body = spec_file.read_text(encoding="utf-8")
        except OSError:
            continue
        except UnicodeError as exc:
            log_event_fn(
                f"[AGENTS-SCAN] Skipping {spec_file.name}: non-UTF-8 "
                f"content ({exc})."
            )
            continue
        violations = scan_for_conflicts(body)
        if not violations:
            continue
        files_with_violations += 1
        for v in violations:
            log_event_fn(
                f"[AGENTS-SCAN] {spec_file.name}: {v.violation_type} "
                f'violates rule "{v.rule}" - line excerpt: '
                f"{v.line_excerpt!r}"
            )
    if files_with_violations > 0:
        log_event_fn(
            f"[AGENTS-SCAN] {files_with_violations} task spec file(s) "
            f"in {repo_path}/tasks/ contain AGENTS.md anti-pattern "
            f"violations. Operator review recommended."
        )
    return files_with_violations


def reconcile_agents_md(
    file_path: str | Path,
    *,
    dry_run: bool = True,
    log_event_fn: Callable[[str], None] | None = None,
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

    When ``log_event_fn`` is provided, after the rewrite finishes
    :func:`_scan_existing_task_specs` runs against ``<file_path>.parent``
    and emits ``[AGENTS-SCAN]`` events for every pre-existing task spec
    that violates an AGENTS.md anti-pattern. The hook is opt-in so the
    web preview/apply endpoints (which have no event-log target)
    continue to behave exactly as before; daemon-side callers that own
    a per-repo log function pass it through to surface drift between
    AGENTS.md and historical specs. PR-260.
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

    if log_event_fn is not None:
        _scan_existing_task_specs(path.parent, log_event_fn)

    return after, diff
