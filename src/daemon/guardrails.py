"""Runtime guardrails for coder stdout.

This scanner observes shell output emitted by the coder process, so matches are
treated as actions taken rather than task-spec advice. Unlike
``src.mcp.scans.scan_for_conflicts``, no negation suppression is applied: if a
forbidden command appears in stdout, the daemon records a Tier 1 violation even
when nearby prose says not to run it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

_DEFAULT_BRANCH = "main"
_MAX_EXCERPT_CHARS = 200


@dataclass(frozen=True)
class GuardrailViolation:
    tier: int
    category: str
    excerpt: str
    rule: str


_REPO_CREATE = re.compile(r"\bgh\s+repo\s+create\b", re.IGNORECASE)
_REPO_DELETE = re.compile(r"\bgh\s+repo\s+delete\b", re.IGNORECASE)
_GH_PR_CREATE = re.compile(r"\bgh\s+pr\s+create\b", re.IGNORECASE)
_GIT_COMMIT = re.compile(r"\bgit\s+commit\b", re.IGNORECASE)
_GIT_PUSH = re.compile(r"\bgit\s+push\b", re.IGNORECASE)
_BRANCH_DELETE_COLON = re.compile(
    r"\bgit\s+push\s+origin\s+:[a-zA-Z0-9_/.-]*\b",
    re.IGNORECASE,
)
_BRANCH_DELETE_FLAG = re.compile(
    r"\bgit\s+push\s+(?:--delete|-d)\s+origin\s+[a-zA-Z0-9_/.-]+\b",
    re.IGNORECASE,
)

# Design parallel copied from src/mcp/scans.py:_ANTI_PATTERNS. Keep local for
# now so runtime stdout guardrails do not refactor task-spec validation.
_FORCE_PUSH_MAIN = re.compile(
    r"\bgit push\b"
    r"(?:"
    r"(?="
    r"(?:[ \t]+[^\s,;|&#]+)*?"
    r"[ \t]+"
    r"(?:--force(?:-with-lease(?:=[^\s,;|&#]*)?)?|-f)"
    r"(?![\w-])"
    r")"
    r"(?="
    r"(?:[ \t]+[^\s,;|&#]+)*?"
    r"[ \t]+(?![-+])[^\s,;|&#]+"
    r"(?:[ \t]+[^\s,;|&#]+)*?"
    r"[ \t]+"
    r"(?:[^\s:,;|&#]+:)?(?:refs/heads/)?main"
    r"(?![\w/:-]|\.\w)"
    r")"
    r"|"
    r"(?="
    r"(?:[ \t]+[^\s,;|&#]+)*?"
    r"[ \t]+"
    r"\+(?:[^\s:,;|&#]+:)?(?:refs/heads/)?main"
    r"(?![\w/:-]|\.\w)"
    r")"
    r")",
    re.IGNORECASE,
)
_FORCE_PUSH_MAIN_ALT = re.compile(r"\bforce.push[^\n]*\bmain\b", re.IGNORECASE)

_RULES = {
    "repo_create": "Coder stdout contains forbidden GitHub repository creation.",
    "repo_delete": "Coder stdout contains forbidden GitHub repository deletion.",
    "force_push_main": "Coder stdout contains a force push to the protected default branch.",
    "branch_delete_main": "Coder stdout contains deletion of the protected default branch.",
    "direct_commit_main": (
        "Coder stdout contains direct commit and push to the protected "
        "default branch without PR creation."
    ),
}


def _excerpt(line: str) -> str:
    return line.strip()[:_MAX_EXCERPT_CHARS]


def _violation(category: str, line: str) -> GuardrailViolation:
    return GuardrailViolation(
        tier=1,
        category=category,
        excerpt=_excerpt(line),
        rule=_RULES[category],
    )


def _command_tokens_after_git_push(line: str) -> list[str]:
    match = _GIT_PUSH.search(line)
    if match is None:
        return []
    command_tail = re.split(r"[,;|&#]", line[match.end() :], maxsplit=1)[0]
    return command_tail.split()


def _refspec_targets_default_branch(refspec: str) -> bool:
    refspec = refspec.lstrip("+")
    if ":" in refspec:
        refspec = refspec.rsplit(":", 1)[1]
    refspec = refspec.removeprefix("refs/heads/")
    return refspec == _DEFAULT_BRANCH


def _push_targets_default_branch(line: str) -> bool:
    tokens = _command_tokens_after_git_push(line)
    positionals: list[str] = []
    skip_next = False
    flags_with_values = {"--repo", "--receive-pack", "--exec"}
    for token in tokens:
        if skip_next:
            skip_next = False
            continue
        if token == "--":
            continue
        if token in flags_with_values:
            skip_next = True
            continue
        if token.startswith("-"):
            continue
        positionals.append(token)
    if len(positionals) < 2:
        return False
    return any(_refspec_targets_default_branch(token) for token in positionals[1:])


def _is_branch_delete_default(line: str) -> bool:
    if _BRANCH_DELETE_COLON.search(line) and _push_targets_default_branch(line):
        return True
    if _BRANCH_DELETE_FLAG.search(line):
        tokens = _command_tokens_after_git_push(line)
        try:
            origin_index = tokens.index("origin")
        except ValueError:
            return False
        return any(
            _refspec_targets_default_branch(token)
            for token in tokens[origin_index + 1 :]
        )
    return False


def _has_direct_commit_to_default_branch(lines: list[str], commit_index: int) -> bool:
    for push_index in range(commit_index + 1, len(lines)):
        push_line = lines[push_index]
        if not _GIT_PUSH.search(push_line):
            continue
        if not _push_targets_default_branch(push_line):
            continue
        between = "\n".join(lines[commit_index + 1 : push_index])
        return _GH_PR_CREATE.search(between) is None
    return False


def scan_stdout(coder_stdout: str) -> list[GuardrailViolation]:
    """Scan coder stdout for Tier 1 forbidden actions. Return all matches."""
    lines = coder_stdout.splitlines()
    violations: list[GuardrailViolation] = []
    direct_commit_recorded = False

    for index, line in enumerate(lines):
        if _REPO_CREATE.search(line):
            violations.append(_violation("repo_create", line))
        if _REPO_DELETE.search(line):
            violations.append(_violation("repo_delete", line))
        if _FORCE_PUSH_MAIN.search(line) or _FORCE_PUSH_MAIN_ALT.search(line):
            violations.append(_violation("force_push_main", line))
        if _is_branch_delete_default(line):
            violations.append(_violation("branch_delete_main", line))
        if (
            not direct_commit_recorded
            and _GIT_COMMIT.search(line)
            and "--amend" not in line
            and _has_direct_commit_to_default_branch(lines, index)
        ):
            violations.append(_violation("direct_commit_main", line))
            direct_commit_recorded = True

    return violations
