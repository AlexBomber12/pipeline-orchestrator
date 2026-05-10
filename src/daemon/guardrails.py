"""Guardrail scans over captured coder shell output.

This module intentionally does not apply negation suppression. The task-spec
validator in ``src/mcp/scans.py`` scans natural-language prose, where text like
"I will never run X" should not be treated as intent to do X. Guardrails scan
coder stdout after the coder process exits; a line containing a catalogued
command is treated as a record of an action that already happened, even when
surrounded by negating words.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class GuardrailViolation:
    tier: int
    category: str
    excerpt: str
    rule: str


_PROTECTED_DEFAULT_BRANCH = "main"
_PROTECTED_DEFAULT_BRANCH_RE = re.escape(_PROTECTED_DEFAULT_BRANCH)

_COMMAND_PREFIX_RE = r"(?m)^(?:[^\S\r\n]*(?:[$>]|[+]{2,})[^\S\r\n]*)?"
_GIT_PUSH_NOT_DRY_RUN_RE = (
    r"(?!(?:[ \t]+[^\s,;|&#]+)*?[ \t]+(?:--dry-run|-n)(?![\w-]))"
)

_TIER1_PATTERNS: dict[str, re.Pattern[str]] = {
    "repo_create": re.compile(
        _COMMAND_PREFIX_RE + r"gh[^\S\r\n]+repo[^\S\r\n]+create\b",
        re.IGNORECASE,
    ),
    "repo_delete": re.compile(
        _COMMAND_PREFIX_RE + r"gh[^\S\r\n]+repo[^\S\r\n]+delete\b",
        re.IGNORECASE,
    ),
    "branch_delete_main": re.compile(
        _COMMAND_PREFIX_RE
        + r"git push\b"
        + _GIT_PUSH_NOT_DRY_RUN_RE
        + r"(?:"
        r"(?="
        r"(?:[ \t]+[^\s,;|&#]+)*?"
        r"[ \t]+(?![-+])[^\s,;|&#]+"
        r"(?:[ \t]+[^\s,;|&#]+)*?"
        r"[ \t]+"
        rf"\+?:(?:refs/heads/)?{_PROTECTED_DEFAULT_BRANCH_RE}"
        r"(?![\w/:-]|\.\w)"
        r")"
        r"|"
        r"(?="
        r"(?:[ \t]+[^\s,;|&#]+)*?"
        r"[ \t]+(?:--delete|-d)(?![\w-])"
        r"(?:[ \t]+[^\s,;|&#]+)*?"
        r"[ \t]+(?![-+])[^\s,;|&#]+"
        r"(?:[ \t]+[^\s,;|&#]+)*?"
        r"[ \t]+"
        rf"(?:refs/heads/)?{_PROTECTED_DEFAULT_BRANCH_RE}"
        r"(?![\w/:-]|\.\w)"
        r")"
        r"|"
        r"(?="
        r"(?:[ \t]+[^\s,;|&#]+)*?"
        r"[ \t]+(?![-+])[^\s,;|&#]+"
        r"(?:[ \t]+[^\s,;|&#]+)*?"
        r"[ \t]+(?:--delete|-d)(?![\w-])"
        r"(?:[ \t]+[^\s,;|&#]+)*?"
        r"[ \t]+"
        rf"(?:refs/heads/)?{_PROTECTED_DEFAULT_BRANCH_RE}"
        r"(?![\w/:-]|\.\w)"
        r")"
        r")",
        re.IGNORECASE,
    ),
}

_TIER1_RULES: dict[str, str] = {
    "repo_create": "GitHub CLI repository creation invocation",
    "repo_delete": "GitHub CLI repository deletion invocation",
    "branch_delete_main": "Git push deletion targeting protected default branch",
}

_EXCERPT_LIMIT = 200


def _line_excerpt(coder_stdout: str, start: int, end: int) -> str:
    line_start = coder_stdout.rfind("\n", 0, start) + 1
    line_end = coder_stdout.find("\n", end)
    if line_end == -1:
        line_end = len(coder_stdout)
    return coder_stdout[line_start:line_end].strip()[:_EXCERPT_LIMIT]


def scan_stdout(coder_stdout: str) -> list[GuardrailViolation]:
    """Return guardrail violations found in captured coder stdout."""
    violations: list[GuardrailViolation] = []
    for category in sorted(_TIER1_PATTERNS):
        pattern = _TIER1_PATTERNS[category]
        for match in pattern.finditer(coder_stdout):
            violations.append(
                GuardrailViolation(
                    tier=1,
                    category=category,
                    excerpt=_line_excerpt(coder_stdout, match.start(), match.end()),
                    rule=_TIER1_RULES[category],
                )
            )
    return violations
