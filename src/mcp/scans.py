"""AGENTS.md anti-pattern scanner for MCP validate_task_spec.

Regex-based detection of phrases that indicate the task spec
contains instructions conflicting with AGENTS.md rules. Each
pattern carries a violation_type and human-readable message.
PR-259.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass
class ConflictViolation:
    violation_type: str
    line_excerpt: str
    rule: str  # AGENTS.md rule reference


_ANTI_PATTERNS: list[tuple[str, re.Pattern, str]] = [
    (
        "draft_pr_flag",
        re.compile(r"\bgh pr create[^\n]*--draft\b", re.IGNORECASE),
        "AGENTS.md prohibits opening PRs in draft state. PR-196.",
    ),
    (
        "draft_pr_text",
        re.compile(r"\bcreate (a |the )?draft PR\b", re.IGNORECASE),
        "AGENTS.md prohibits opening PRs in draft state. PR-196.",
    ),
    (
        "force_push_main",
        # Detect ``git push`` lines that force-push to ``main`` via
        # either documented form: the ``--force``/``-f`` flag (any
        # argument order) OR a leading ``+`` on the refspec, which
        # ``git push -h`` documents as equivalent force behavior
        # (e.g. ``+HEAD:main`` or ``+main``). Both arms require the
        # destination of a refspec token to actually resolve to
        # ``main`` (whitespace-bounded ``main`` or ``refs/heads/main``,
        # optionally prefixed by ``<src>:``), so unrelated branch
        # names that merely contain the substring ``main`` (e.g.
        # ``feature/main-fix``) are not flagged.
        re.compile(
            r"\bgit push\b"
            r"(?:"
            # Force-flag form (``--force`` / ``-f``) plus a refspec
            # whose dst resolves to ``main``. ``(?<!\S)`` and
            # ``(?![\w/:-])`` keep the token whitespace-bounded so
            # substrings inside ``feature/main-fix``, ``-main-``,
            # or ``main:other`` are not matched. The ``--force`` arm
            # uses a negative lookahead to exclude ``--force-if-includes``,
            # which by itself is a no-op (it only takes effect when
            # combined with ``--force-with-lease``); ``--force`` and
            # ``--force-with-lease`` are still matched.
            r"(?=[^\n]*(?:--force(?!-if-includes\b)\b|(?<!\S)-f(?!\S)))"
            r"(?=[^\n]*(?<!\S)(?:[^\s:]+:)?(?:refs/heads/)?main(?![\w/:-]))"
            r"|"
            # Plus-prefix force form: ``+<refspec>`` targeting main.
            r"(?=[^\n]*(?<!\S)\+(?:[^\s:]+:)?(?:refs/heads/)?main(?![\w/:-]))"
            r")",
            re.IGNORECASE,
        ),
        "AGENTS.md forbids force-push to main.",
    ),
    (
        "force_push_main_alt",
        re.compile(r"\bforce.push[^\n]*\bmain\b", re.IGNORECASE),
        "AGENTS.md forbids force-push to main.",
    ),
    (
        "no_verify_commit",
        re.compile(r"\bgit commit[^\n]*--no-verify\b", re.IGNORECASE),
        "AGENTS.md forbids bypassing pre-commit hooks.",
    ),
    (
        "skip_ci",
        re.compile(r"\b(skip|bypass|ignore) CI\b", re.IGNORECASE),
        "AGENTS.md requires green CI before merge.",
    ),
    (
        "skip_ci_commit_msg",
        # ``\b`` does not anchor before ``\[`` since ``[`` is non-word;
        # match the marker anywhere on the line instead.
        re.compile(r"\[skip ci\]|\[ci skip\]", re.IGNORECASE),
        "AGENTS.md requires green CI; [skip ci] markers are prohibited.",
    ),
    (
        "auto_merge_dirty",
        re.compile(r"\bauto.merge[^\n]*(dirty|red|failing)\b", re.IGNORECASE),
        "AGENTS.md prohibits merging with failing checks.",
    ),
]


def scan_for_conflicts(task_spec_body: str) -> list[ConflictViolation]:
    """Return list of detected AGENTS.md anti-pattern violations.

    Empty list when no violations found. Each match captures the
    violation type and an 80-char excerpt of the offending line for
    operator context.
    """
    violations: list[ConflictViolation] = []
    for line in task_spec_body.splitlines():
        for vtype, pattern, rule in _ANTI_PATTERNS:
            if pattern.search(line):
                excerpt = line.strip()[:80]
                violations.append(
                    ConflictViolation(
                        violation_type=vtype,
                        line_excerpt=excerpt,
                        rule=rule,
                    )
                )
    return violations
