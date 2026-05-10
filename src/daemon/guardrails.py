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
import shlex
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
_GIT_PUSH_COMMAND_RE = re.compile(
    _COMMAND_PREFIX_RE + r"git[^\S\r\n]+push\b(?P<args>[^\r\n]*)",
    re.IGNORECASE,
)
_PUSH_VALUE_OPTIONS = {"-o", "--push-option", "--receive-pack", "--exec"}
_PUSH_VALUE_OPTION_PREFIXES = tuple(
    f"{option}=" for option in _PUSH_VALUE_OPTIONS if option != "-o"
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


def _push_args_tokens(args: str) -> list[str]:
    try:
        return shlex.split(args, comments=False, posix=True)
    except ValueError:
        return args.split()


def _short_option_cluster_contains(token: str, flag: str) -> bool:
    return (
        token.startswith("-")
        and not token.startswith("--")
        and not token.startswith("-o")
        and flag in token[1:]
    )


def _is_dry_run_token(token: str) -> bool:
    return token == "--dry-run" or _short_option_cluster_contains(token, "n")


def _is_delete_token(token: str) -> bool:
    return token == "--delete" or _short_option_cluster_contains(token, "d")


def _is_protected_branch_ref(token: str) -> bool:
    return token in {
        _PROTECTED_DEFAULT_BRANCH,
        f"refs/heads/{_PROTECTED_DEFAULT_BRANCH}",
    }


def _is_empty_source_protected_refspec(token: str) -> bool:
    refspec = token[1:] if token.startswith("+") else token
    return refspec.startswith(":") and _is_protected_branch_ref(refspec[1:])


def _is_positional_push_token(token: str) -> bool:
    return bool(token) and not token.startswith("-")


def _is_repository_token(token: str) -> bool:
    return _is_positional_push_token(token) and not _is_empty_source_protected_refspec(
        token
    )


def _push_tokens_without_option_values(tokens: list[str]) -> list[str]:
    filtered: list[str] = []
    skip_next = False
    for token in tokens:
        if skip_next:
            skip_next = False
            continue
        filtered.append(token)
        if token in _PUSH_VALUE_OPTIONS:
            skip_next = True
    return filtered


def _push_positionals(tokens: list[str]) -> list[str]:
    positionals: list[str] = []
    for token in _push_tokens_without_option_values(tokens):
        if token.startswith(_PUSH_VALUE_OPTION_PREFIXES):
            continue
        if _is_positional_push_token(token):
            positionals.append(token)
    return positionals


def _delete_flag_targets_protected_branch(tokens: list[str]) -> bool:
    filtered_tokens = _push_tokens_without_option_values(tokens)
    if not any(_is_delete_token(token) for token in filtered_tokens):
        return False
    positional = [
        token for token in filtered_tokens if _is_positional_push_token(token)
    ]
    if len(positional) < 2:
        return False
    return any(_is_protected_branch_ref(ref) for ref in positional[1:])


def _colon_refspec_targets_protected_branch(tokens: list[str]) -> bool:
    positional = _push_positionals(tokens)
    for index, token in enumerate(positional):
        if not _is_empty_source_protected_refspec(token):
            continue
        return any(_is_repository_token(previous) for previous in positional[:index])
    return False


def _scan_branch_delete_main(coder_stdout: str) -> list[GuardrailViolation]:
    violations: list[GuardrailViolation] = []
    for match in _GIT_PUSH_COMMAND_RE.finditer(coder_stdout):
        tokens = _push_args_tokens(match.group("args"))
        option_value_filtered_tokens = _push_tokens_without_option_values(tokens)
        if any(_is_dry_run_token(token) for token in option_value_filtered_tokens):
            continue
        if not (
            _colon_refspec_targets_protected_branch(tokens)
            or _delete_flag_targets_protected_branch(tokens)
        ):
            continue
        violations.append(
            GuardrailViolation(
                tier=1,
                category="branch_delete_main",
                excerpt=_line_excerpt(coder_stdout, match.start(), match.end()),
                rule=_TIER1_RULES["branch_delete_main"],
            )
        )
    return violations


def scan_stdout(coder_stdout: str) -> list[GuardrailViolation]:
    """Return guardrail violations found in captured coder stdout."""
    violations: list[GuardrailViolation] = _scan_branch_delete_main(coder_stdout)
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
