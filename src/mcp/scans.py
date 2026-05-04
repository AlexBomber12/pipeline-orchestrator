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
        # Detect ``git push`` lines that force-push to ``main``. Both
        # arms walk only the tokens that belong to the ``git push``
        # command itself, treating the args as whitespace-separated
        # tokens that contain no command-terminator chars (``,;|&#``).
        # ``#`` starts a shell comment, so anything after it is prose,
        # not a refspec. The walks are bounded to at most 5 intermediate
        # arg tokens so that prose continuations such as
        # ``... feature/foo then open PR to main.`` cannot trickle the
        # ``main`` match past the actual command arg list, while still
        # tolerating realistic flag-heavy invocations such as
        # ``--no-verify --force --tags --set-upstream origin main``
        # (5 tokens before ``main``). Combined with the existing
        # separator terminators, this keeps prose mentions of ``main``
        # on the same line from being flagged when the command
        # genuinely targets a feature branch -- e.g.
        # ``git push --force-with-lease origin feature/foo, then PR to main``
        # or
        # ``git push --force-with-lease origin feature/foo # PR to main``.
        # The standard arm requires both a ``--force``/``-f`` flag
        # token AND a refspec token whose destination resolves to
        # ``main`` (whitespace-bounded ``main`` or ``refs/heads/main``,
        # optionally prefixed by ``<src>:``). The ``--force`` token
        # excludes ``--force-if-includes`` (alone a no-op) but still
        # matches ``--force`` and ``--force-with-lease[=ref]``. The
        # plus-prefix arm catches ``+<refspec>`` targeting ``main``,
        # which ``git push -h`` documents as equivalent force behavior.
        re.compile(
            r"\bgit push\b"
            r"(?:"
            # Standard force-flag form: --force/-f token AND main
            # refspec token, in any order, both within the command's
            # arg list.
            r"(?="
            r"(?:[ \t]+[^\s,;|&#]+){0,5}?"
            r"[ \t]+"
            r"(?:--force(?:-with-lease(?:=[^\s,;|&#]*)?)?|-f)"
            r"(?![\w-])"
            r")"
            r"(?="
            r"(?:[ \t]+[^\s,;|&#]+){0,5}?"
            r"[ \t]+"
            r"(?:[^\s:,;|&#]+:)?(?:refs/heads/)?main"
            r"(?![\w/:-])"
            r")"
            r"|"
            # Plus-prefix force form: +<refspec> with dst=main.
            r"(?="
            r"(?:[ \t]+[^\s,;|&#]+){0,5}?"
            r"[ \t]+"
            r"\+(?:[^\s:,;|&#]+:)?(?:refs/heads/)?main"
            r"(?![\w/:-])"
            r")"
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
