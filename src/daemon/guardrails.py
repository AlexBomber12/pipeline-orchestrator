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
_GH_PR_CREATE_COMMAND = re.compile(r"^\s*(?:[$+]\s*)?gh\s+pr\s+create\b", re.IGNORECASE)
_GIT_COMMIT = re.compile(r"\bgit\s+commit\b", re.IGNORECASE)
_GIT_PUSH = re.compile(r"\bgit\s+push\b", re.IGNORECASE)
# Design parallel to src/mcp/scans.py:_ANTI_PATTERNS. Keep local for now so
# runtime stdout guardrails do not refactor task-spec validation.
_FORCE_PUSH_ALT = re.compile(r"\bforce.push[^\n]*\b", re.IGNORECASE)

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


def _git_push_positionals(tokens: list[str]) -> list[str]:
    positionals: list[str] = []
    skip_next = False
    repo_value_next = False
    flags_with_values = {
        "--exec",
        "--push-option",
        "--receive-pack",
        "-o",
    }
    for token in tokens:
        if skip_next:
            skip_next = False
            continue
        if repo_value_next:
            repo_value_next = False
            positionals.append(token)
            continue
        if token == "--":
            continue
        if token == "--repo":
            repo_value_next = True
            continue
        if token.startswith("--repo="):
            repo = token.partition("=")[2]
            if repo:
                positionals.append(repo)
            continue
        if token in flags_with_values:
            skip_next = True
            continue
        if token.startswith("-"):
            continue
        positionals.append(token)
    return positionals


def _refspec_targets_default_branch(refspec: str, default_branch: str) -> bool:
    refspec = refspec.lstrip("+")
    if ":" in refspec:
        refspec = refspec.rsplit(":", 1)[1]
    refspec = refspec.removeprefix("refs/heads/")
    return refspec == default_branch


def _push_targets_default_branch(line: str, default_branch: str) -> bool:
    tokens = _command_tokens_after_git_push(line)
    positionals = _git_push_positionals(tokens)
    if len(positionals) < 2:
        return False
    return any(
        _refspec_targets_default_branch(token, default_branch)
        for token in positionals[1:]
    )


def _is_force_flag(token: str) -> bool:
    return bool(
        re.fullmatch(r"--force(?:-with-lease(?:=.*)?)?|-f", token, re.IGNORECASE)
    )


def _is_force_refspec_to_default_branch(refspec: str, default_branch: str) -> bool:
    return refspec.startswith("+") and _refspec_targets_default_branch(
        refspec, default_branch
    )


def _line_mentions_default_branch(line: str, default_branch: str) -> bool:
    escaped = re.escape(default_branch)
    return re.search(rf"(?<![\w/.-]){escaped}(?![\w/.-])", line) is not None


def _is_force_push_default(line: str, default_branch: str) -> bool:
    if _FORCE_PUSH_ALT.search(line) and _line_mentions_default_branch(
        line, default_branch
    ):
        return True
    tokens = _command_tokens_after_git_push(line)
    positionals = _git_push_positionals(tokens)
    if len(positionals) < 2:
        return False
    refspecs = positionals[1:]
    if any(
        _is_force_refspec_to_default_branch(token, default_branch)
        for token in refspecs
    ):
        return True
    return any(_is_force_flag(token) for token in tokens) and any(
        _refspec_targets_default_branch(token, default_branch) for token in refspecs
    )


def _is_delete_flag(token: str) -> bool:
    return token in {"--delete", "-d"}


def _is_delete_refspec_to_default_branch(refspec: str, default_branch: str) -> bool:
    return refspec.startswith(":") and _refspec_targets_default_branch(
        refspec, default_branch
    )


def _is_branch_delete_default(line: str, default_branch: str) -> bool:
    tokens = _command_tokens_after_git_push(line)
    positionals = _git_push_positionals(tokens)
    if len(positionals) < 2:
        return False
    refspecs = positionals[1:]
    if any(
        _is_delete_refspec_to_default_branch(token, default_branch)
        for token in refspecs
    ):
        return True
    return any(_is_delete_flag(token) for token in tokens) and any(
        _refspec_targets_default_branch(token, default_branch) for token in refspecs
    )


def _has_direct_commit_to_default_branch(
    lines: list[str],
    commit_index: int,
    default_branch: str,
) -> bool:
    for push_index in range(commit_index + 1, len(lines)):
        push_line = lines[push_index]
        if not _GIT_PUSH.search(push_line):
            continue
        if not _push_targets_default_branch(push_line, default_branch):
            continue
        between = lines[commit_index + 1 : push_index]
        return not any(_GH_PR_CREATE_COMMAND.search(line) for line in between)
    return False


def scan_stdout(
    coder_stdout: str,
    *,
    default_branch: str = _DEFAULT_BRANCH,
) -> list[GuardrailViolation]:
    """Scan coder stdout for Tier 1 forbidden actions. Return all matches."""
    lines = coder_stdout.splitlines()
    violations: list[GuardrailViolation] = []
    direct_commit_recorded = False

    for index, line in enumerate(lines):
        if _REPO_CREATE.search(line):
            violations.append(_violation("repo_create", line))
        if _REPO_DELETE.search(line):
            violations.append(_violation("repo_delete", line))
        if _is_force_push_default(line, default_branch):
            violations.append(_violation("force_push_main", line))
        if _is_branch_delete_default(line, default_branch):
            violations.append(_violation("branch_delete_main", line))
        if (
            not direct_commit_recorded
            and _GIT_COMMIT.search(line)
            and "--amend" not in line
            and _has_direct_commit_to_default_branch(lines, index, default_branch)
        ):
            violations.append(_violation("direct_commit_main", line))
            direct_commit_recorded = True

    return violations
