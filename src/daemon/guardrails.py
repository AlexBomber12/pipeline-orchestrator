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
_PROTECTED_BRANCH_REFSPEC_RE = (
    rf"(?:[^\s:,;|&#]+:)?(?:refs/heads/)?{_PROTECTED_DEFAULT_BRANCH_RE}"
    r"(?![\w/:-]|\.\w)"
)
_PROTECTED_BRANCH_POSITIONAL_RE = (
    r"(?:[ \t]+[^\s,;|&#]+)*?"
    r"[ \t]+(?![-+])[^\s,;|&#]+"
    r"(?:[ \t]+[^\s,;|&#]+)*?"
    r"[ \t]+"
    rf"{_PROTECTED_BRANCH_REFSPEC_RE}"
)

_TIER1_PATTERNS: dict[str, re.Pattern[str]] = {
    "repo_create": re.compile(
        _COMMAND_PREFIX_RE + r"gh[^\S\r\n]+repo[^\S\r\n]+create\b",
        re.IGNORECASE,
    ),
    "repo_delete": re.compile(
        r"\bgh[^\S\r\n]+repo[^\S\r\n]+delete\b",
        re.IGNORECASE,
    ),
    "force_push_main": re.compile(
        # Design parallel with src/mcp/scans.py ``force_push_main``:
        # require a force token and a refspec whose destination resolves
        # to the protected default branch, while avoiding remote-name
        # collisions such as ``git push main feature-branch``.
        r"\bgit push\b"
        r"(?:"
        r"(?="
        r"(?:[ \t]+[^\s,;|&#]+)*?"
        r"[ \t]+"
        r"(?:--force(?:-with-lease(?:=[^\s,;|&#]*)?)?|-f)"
        r"(?![\w-])"
        r")"
        r"(?="
        rf"{_PROTECTED_BRANCH_POSITIONAL_RE}"
        r")"
        r"|"
        r"(?="
        r"(?:[ \t]+[^\s,;|&#]+)*?"
        r"[ \t]+"
        rf"\+{_PROTECTED_BRANCH_REFSPEC_RE}"
        r")"
        r")",
        re.IGNORECASE,
    ),
    "branch_delete_main": re.compile(
        r"\bgit push\b"
        r"(?:"
        r"(?="
        r"(?:[ \t]+[^\s,;|&#]+)*?"
        r"[ \t]+(?![-+])[^\s,;|&#]+"
        r"(?:[ \t]+[^\s,;|&#]+)*?"
        r"[ \t]+"
        rf":(?:refs/heads/)?{_PROTECTED_DEFAULT_BRANCH_RE}"
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
        r")",
        re.IGNORECASE,
    ),
    "direct_commit_main": re.compile(
        r"\bgit[^\S\r\n]+commit\b",
        re.IGNORECASE,
    ),
}

_TIER1_RULES: dict[str, str] = {
    "branch_delete_main": "Git push deletion targeting protected default branch",
    "direct_commit_main": "Git commit followed by push to protected default branch without PR creation",
    "force_push_main": "Git force-push targeting protected default branch",
    "repo_create": "GitHub CLI repository creation invocation",
    "repo_delete": "GitHub CLI repository deletion invocation",
}

_EXCERPT_LIMIT = 200
_DIRECT_COMMIT_CATEGORY = "direct_commit_main"
_GH_PR_CREATE_RE = re.compile(r"\bgh[^\S\r\n]+pr[^\S\r\n]+create\b", re.IGNORECASE)
_GIT_PUSH_PROTECTED_BRANCH_RE = re.compile(
    r"\bgit push\b"
    r"(?="
    rf"{_PROTECTED_BRANCH_POSITIONAL_RE}"
    r")",
    re.IGNORECASE,
)


def _line_excerpt(coder_stdout: str, start: int, end: int) -> str:
    line_start = coder_stdout.rfind("\n", 0, start) + 1
    line_end = coder_stdout.find("\n", end)
    if line_end == -1:
        line_end = len(coder_stdout)
    return coder_stdout[line_start:line_end].strip()[:_EXCERPT_LIMIT]


def _detect_direct_commit_main(stdout: str) -> list[GuardrailViolation]:
    """Detect commit then push to default branch without an intervening PR."""
    violations: list[GuardrailViolation] = []
    lines = stdout.splitlines()

    for commit_index, commit_line in enumerate(lines):
        if not _TIER1_PATTERNS[_DIRECT_COMMIT_CATEGORY].search(commit_line):
            continue
        if re.search(r"(?<!\S)--amend(?!\S)", commit_line):
            continue

        for push_index in range(commit_index + 1, len(lines)):
            if not _GIT_PUSH_PROTECTED_BRANCH_RE.search(lines[push_index]):
                continue
            intermediate_lines = lines[commit_index + 1 : push_index]
            if any(_GH_PR_CREATE_RE.search(line) for line in intermediate_lines):
                break
            violations.append(
                GuardrailViolation(
                    tier=1,
                    category=_DIRECT_COMMIT_CATEGORY,
                    excerpt=commit_line.strip()[:_EXCERPT_LIMIT],
                    rule=_TIER1_RULES[_DIRECT_COMMIT_CATEGORY],
                )
            )
            break

    return violations


def scan_stdout(coder_stdout: str) -> list[GuardrailViolation]:
    """Return guardrail violations found in captured coder stdout."""
    violations: list[GuardrailViolation] = []
    for category in sorted(_TIER1_PATTERNS):
        if category == _DIRECT_COMMIT_CATEGORY:
            violations.extend(_detect_direct_commit_main(coder_stdout))
            continue
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
