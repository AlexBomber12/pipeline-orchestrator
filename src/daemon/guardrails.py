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
_PROTECTED_BRANCH_REFSPEC_RE = (
    rf"(?:[^\s:,;|&#]+:)?(?:refs/heads/)?{_PROTECTED_DEFAULT_BRANCH_RE}"
    r"(?![\w/:-]|\.\w)"
)
_COMMAND_PREFIX_RE = r"(?m)^(?:[^\S\r\n]*(?:[$>]|[+]{2,})[^\S\r\n]*)?"
_SHELL_WORD_RE = r"(?:'[^'\r\n]*'|\"[^\"\r\n]*\"|[^'\"\s\r\n]+)"
_SHELL_ENV_ASSIGNMENT_RE = rf"[A-Za-z_][A-Za-z0-9_]*={_SHELL_WORD_RE}"
_SHELL_ENV_OPTION_RE = (
    rf"(?:-[i0]|--(?:ignore-environment|null)|"
    rf"(?:-u|--unset|-C|--chdir|-S|--split-string)"
    rf"(?:={_SHELL_WORD_RE}|[^\S\r\n]+{_SHELL_WORD_RE}))"
)
_SHELL_ENV_COMMAND_RE = rf"env(?:[^\S\r\n]+{_SHELL_ENV_OPTION_RE})*"
_SHELL_ENV_PREFIX_RE = (
    rf"(?:(?:{_SHELL_ENV_COMMAND_RE}|{_SHELL_ENV_ASSIGNMENT_RE})[^\S\r\n]+)*"
)
_GIT_COMMAND_RE = re.compile(
    _COMMAND_PREFIX_RE + _SHELL_ENV_PREFIX_RE + r"git\b(?P<args>[^\r\n]*)",
    re.IGNORECASE,
)
_GIT_VALUE_OPTIONS = {
    "-C",
    "-c",
    "--config-env",
    "--exec-path",
    "--git-dir",
    "--namespace",
    "--super-prefix",
    "--work-tree",
}
_GIT_VALUE_OPTION_PREFIXES = (
    "--config-env=",
    "--exec-path=",
    "--git-dir=",
    "--namespace=",
    "--super-prefix=",
    "--work-tree=",
)
_GIT_FLAG_OPTIONS = {
    "--bare",
    "--glob-pathspecs",
    "--icase-pathspecs",
    "--literal-pathspecs",
    "--no-optional-locks",
    "--no-pager",
    "--no-replace-objects",
    "--noglob-pathspecs",
    "--paginate",
    "--version",
    "-p",
    "-P",
    "-v",
}
_GIT_TERMINAL_OPTIONS = {"--help", "--html-path", "--man-path", "--version"}
_PUSH_VALUE_OPTIONS = {"-o", "--push-option", "--receive-pack", "--exec", "--repo"}
_PUSH_VALUE_OPTION_PREFIXES = tuple(
    f"{option}=" for option in _PUSH_VALUE_OPTIONS if option != "-o"
)
_SHELL_CONTROL_SEPARATORS = (";", "|", "&")

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
    "force_push_main": "Git force-push targeting protected default branch",
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
        tokens = shlex.split(args, comments=False, posix=True)
    except ValueError:
        tokens = args.split()
    normalized: list[str] = []
    for token in tokens:
        separator_offsets = [
            offset
            for separator in _SHELL_CONTROL_SEPARATORS
            if (offset := token.find(separator)) != -1
        ]
        if not separator_offsets:
            normalized.append(token)
            continue
        offset = min(separator_offsets)
        if offset:
            normalized.append(token[:offset])
        break
    return normalized


def _push_tokens_after_git_global_options(tokens: list[str]) -> list[str] | None:
    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token.lower() == "push":
            return tokens[index + 1 :]
        if token in _GIT_TERMINAL_OPTIONS:
            return None
        if token in _GIT_VALUE_OPTIONS:
            index += 2
            continue
        if token.startswith(_GIT_VALUE_OPTION_PREFIXES):
            index += 1
            continue
        if token in _GIT_FLAG_OPTIONS:
            index += 1
            continue
        return None
    return None


def _short_option_cluster_contains(token: str, flag: str) -> bool:
    return (
        token.startswith("-")
        and not token.startswith("--")
        and not token.startswith("-o")
        and flag in token[1:]
    )


def _is_dry_run_token(token: str) -> bool:
    return token == "--dry-run" or _short_option_cluster_contains(token, "n")


def _is_effective_dry_run(tokens: list[str]) -> bool:
    dry_run = False
    for token in tokens:
        if _is_dry_run_token(token):
            dry_run = True
        elif token == "--no-dry-run":
            dry_run = False
    return dry_run


def _is_delete_token(token: str) -> bool:
    return token == "--delete" or _short_option_cluster_contains(token, "d")


def _is_effective_delete(tokens: list[str]) -> bool:
    delete = False
    for token in tokens:
        if _is_delete_token(token):
            delete = True
        elif token == "--no-delete":
            delete = False
    return delete


def _is_force_token(token: str) -> bool:
    return (
        token == "--force"
        or token.startswith("--force-with-lease")
        or _short_option_cluster_contains(token, "f")
    )


def _is_effective_force(tokens: list[str]) -> bool:
    force = False
    for token in tokens:
        if _is_force_token(token):
            force = True
        elif token in {"--no-force", "--no-force-with-lease"}:
            force = False
    return force


def _is_protected_branch_ref(token: str) -> bool:
    return token in {
        _PROTECTED_DEFAULT_BRANCH,
        f"heads/{_PROTECTED_DEFAULT_BRANCH}",
        f"refs/heads/{_PROTECTED_DEFAULT_BRANCH}",
    }


def _is_empty_source_protected_refspec(token: str) -> bool:
    refspec = token[1:] if token.startswith("+") else token
    return refspec.startswith(":") and _is_protected_branch_ref(refspec[1:])


def _refspec_targets_protected_branch(token: str) -> bool:
    refspec = token[1:] if token.startswith("+") else token
    if refspec.startswith(":"):
        return False
    if ":" in refspec:
        _, destination = refspec.rsplit(":", 1)
        return _is_protected_branch_ref(destination)
    return _is_protected_branch_ref(refspec)


def _is_forced_protected_refspec(token: str) -> bool:
    return token.startswith("+") and _refspec_targets_protected_branch(token)


def _is_positional_push_token(token: str) -> bool:
    return bool(token) and not token.startswith("-")


def _tokens_before_end_of_options(tokens: list[str]) -> list[str]:
    if "--" not in tokens:
        return tokens
    return tokens[: tokens.index("--")]


def _has_repo_option(tokens: list[str]) -> bool:
    return any(token == "--repo" or token.startswith("--repo=") for token in tokens)


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
    option_tokens = _push_tokens_without_option_values(
        _tokens_before_end_of_options(tokens)
    )
    if not _is_effective_delete(option_tokens):
        return False
    positional = _push_positionals(tokens)
    if _has_repo_option(option_tokens):
        candidate_refs = positional
    elif len(positional) >= 2:
        candidate_refs = positional[1:]
    else:
        return False
    return any(_is_protected_branch_ref(ref) for ref in candidate_refs)


def _colon_refspec_targets_protected_branch(tokens: list[str]) -> bool:
    positional = _push_positionals(tokens)
    return any(_is_empty_source_protected_refspec(token) for token in positional)


def _force_flag_targets_protected_branch(tokens: list[str]) -> bool:
    option_tokens = _push_tokens_without_option_values(
        _tokens_before_end_of_options(tokens)
    )
    if not _is_effective_force(option_tokens):
        return False
    positional = _push_positionals(tokens)
    if _has_repo_option(option_tokens):
        candidate_refs = positional
    elif len(positional) >= 2:
        candidate_refs = positional[1:]
    else:
        return False
    return any(_refspec_targets_protected_branch(ref) for ref in candidate_refs)


def _plus_refspec_targets_protected_branch(tokens: list[str]) -> bool:
    positional = _push_positionals(tokens)
    return any(_is_forced_protected_refspec(token) for token in positional)


def _scan_branch_delete_main(coder_stdout: str) -> list[GuardrailViolation]:
    violations: list[GuardrailViolation] = []
    for match in _GIT_COMMAND_RE.finditer(coder_stdout):
        tokens = _push_tokens_after_git_global_options(
            _push_args_tokens(match.group("args"))
        )
        if tokens is None:
            continue
        option_value_filtered_tokens = _push_tokens_without_option_values(
            _tokens_before_end_of_options(tokens)
        )
        if _is_effective_dry_run(option_value_filtered_tokens):
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


def _scan_force_push_main(coder_stdout: str) -> list[GuardrailViolation]:
    violations: list[GuardrailViolation] = []
    for match in _GIT_COMMAND_RE.finditer(coder_stdout):
        tokens = _push_tokens_after_git_global_options(
            _push_args_tokens(match.group("args"))
        )
        if tokens is None:
            continue
        option_value_filtered_tokens = _push_tokens_without_option_values(
            _tokens_before_end_of_options(tokens)
        )
        if _is_effective_dry_run(option_value_filtered_tokens):
            continue
        if _is_effective_delete(option_value_filtered_tokens):
            continue
        if not (
            _force_flag_targets_protected_branch(tokens)
            or _plus_refspec_targets_protected_branch(tokens)
        ):
            continue
        violations.append(
            GuardrailViolation(
                tier=1,
                category="force_push_main",
                excerpt=_line_excerpt(coder_stdout, match.start(), match.end()),
                rule=_TIER1_RULES["force_push_main"],
            )
        )
    return violations


def scan_stdout(coder_stdout: str) -> list[GuardrailViolation]:
    """Return guardrail violations found in captured coder stdout."""
    violations: list[GuardrailViolation] = _scan_branch_delete_main(coder_stdout)
    violations.extend(_scan_force_push_main(coder_stdout))
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
