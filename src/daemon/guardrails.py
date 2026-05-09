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
        _COMMAND_PREFIX_RE + r"gh[^\S\r\n]+repo[^\S\r\n]+delete\b",
        re.IGNORECASE,
    ),
    "force_push_main": re.compile(
        # Design parallel with src/mcp/scans.py ``force_push_main``:
        # require a force token and a refspec whose destination resolves
        # to the protected default branch, while avoiding remote-name
        # collisions such as ``git push main feature-branch``.
        _COMMAND_PREFIX_RE
        + r"git push\b"
        + _GIT_PUSH_NOT_DRY_RUN_RE
        + r"(?:"
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
        _COMMAND_PREFIX_RE
        + r"git push\b"
        + _GIT_PUSH_NOT_DRY_RUN_RE
        + r"(?:"
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
        r"[ \t]+"
        rf"(?:refs/heads/)?{_PROTECTED_DEFAULT_BRANCH_RE}"
        r"(?![\w/:-]|\.\w)"
        r")"
        r")",
        re.IGNORECASE,
    ),
    "direct_commit_main": re.compile(
        _COMMAND_PREFIX_RE + r"git[^\S\r\n]+commit\b",
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
_GH_PR_CREATE_RE = re.compile(
    _COMMAND_PREFIX_RE + r"gh[^\S\r\n]+pr[^\S\r\n]+create\b",
    re.IGNORECASE,
)
_GH_PR_CREATE_NO_CREATE_FLAG_RE = re.compile(
    r"(?<!\S)(?:--dry-run(?:=[^\s]+)?|--help|-h)(?!\S)",
)
_GIT_COMMAND_RE = re.compile(_COMMAND_PREFIX_RE + r"git\b", re.IGNORECASE)
_GIT_PUSH_LINE_RE = re.compile(
    _COMMAND_PREFIX_RE
    + r"git push\b"
    + _GIT_PUSH_NOT_DRY_RUN_RE
    + r"(?P<args>(?:[ \t]+[^\s,;|&#]+)*)"
    r"[ \t]*(?=$|[,;|&#])",
    re.IGNORECASE,
)
_GIT_BRANCH_CHANGE_RE = re.compile(
    _COMMAND_PREFIX_RE
    + r"git[^\S\r\n]+(?:"
    r"(?:checkout|switch)[^\S\r\n]+(?:(?:-q|--quiet)[^\S\r\n]+)*(?P<previous>-)"
    r"|switch[^\S\r\n]+(?:(?:-q|--quiet)[^\S\r\n]+)*(?!-)(?P<branch>[^\s,;|&#]+)"
    r"|checkout[^\S\r\n]+(?:(?:-q|--quiet)[^\S\r\n]+)*(?!-)(?P<checkout_target>[^\s,;|&#]+)"
    r"|switch[^\S\r\n]+(?:(?:-q|--quiet)[^\S\r\n]+)*(?:-c|-C)[^\S\r\n]+(?P<switch_create>[^\s,;|&#]+)"
    r"|checkout[^\S\r\n]+(?:(?:-q|--quiet)[^\S\r\n]+)*(?:-b|-B)[^\S\r\n]+(?P<checkout_create>[^\s,;|&#]+)"
    r")"
    r"(?:[ \t]+[^\s,;|&#]+)*"
    r"[ \t]*(?=$|[,;|&#])",
    re.IGNORECASE,
)
_GIT_PUSH_PROTECTED_BRANCH_RE = re.compile(
    _COMMAND_PREFIX_RE
    + r"git push\b"
    + _GIT_PUSH_NOT_DRY_RUN_RE
    + r"(?="
    rf"{_PROTECTED_BRANCH_POSITIONAL_RE}"
    r")",
    re.IGNORECASE,
)
_GIT_BRANCH_CHANGE_FAILURE_RE = re.compile(
    r"(?im)(?:^error:|^fatal:|not switching branches|pathspec .* did not match)"
)
_GIT_CHECKOUT_BRANCH_SUCCESS_RE = re.compile(
    r"(?i)(?:"
    r"^Switched to (?:a new )?branch '(?P<switched>[^']+)'"
    r"|^Already on '(?P<already>[^']+)'"
    r")"
)


def _is_protected_current_branch(current_branch: str | None) -> bool:
    return current_branch == _PROTECTED_DEFAULT_BRANCH


def _git_push_line_info(line: str) -> tuple[bool, bool]:
    """Return ``(has_force_flag, has_no_explicit_refspec)`` for a push line."""
    match = _GIT_PUSH_LINE_RE.search(line)
    if not match:
        return False, False

    has_force_flag = False
    has_non_branch_mode = False
    positionals: list[str] = []
    for token in match.group("args").split():
        if token.startswith("-"):
            if re.fullmatch(r"--force(?:-with-lease(?:=.*)?)?|-f", token):
                has_force_flag = True
            if token in {"--all", "--mirror", "--tags"}:
                has_non_branch_mode = True
            continue
        positionals.append(token)
    has_current_branch_refspec = len(positionals) >= 2 and all(
        refspec in {"HEAD", "+HEAD"} for refspec in positionals[1:]
    )
    has_no_explicit_refspec = len(positionals) <= 1 or has_current_branch_refspec
    return has_force_flag, not has_non_branch_mode and has_no_explicit_refspec


def _branch_after_command(
    line: str,
    current_branch: str | None,
    previous_branch: str | None,
) -> tuple[str | None, bool] | None:
    match = _GIT_BRANCH_CHANGE_RE.search(line)
    if not match:
        return None
    if match.group("previous"):
        return (previous_branch or current_branch, False)
    checkout_target = match.group("checkout_target")
    if checkout_target:
        quiet_checkout = bool(
            re.search(r"(?i)\bcheckout[^\S\r\n]+(?:-q|--quiet)[^\S\r\n]+", line)
        )
        return (
            checkout_target,
            False if quiet_checkout else _looks_like_checkout_pathspec(checkout_target),
        )
    branch = (
        match.group("branch")
        or match.group("switch_create")
        or match.group("checkout_create")
    )
    return (branch, False)


def _looks_like_checkout_pathspec(target: str) -> bool:
    return (
        target in {".", ".."}
        or target.startswith(("./", "../", "/", ":"))
        or target.endswith("/")
        or "." in target
    )


def _apply_branch_change(
    current_branch: str | None,
    previous_branch: str | None,
    new_branch: str | None,
) -> tuple[str | None, str | None]:
    if new_branch == current_branch:
        return current_branch, previous_branch
    return new_branch, current_branch


def _line_contexts(
    stdout: str,
    initial_branch: str | None,
) -> list[tuple[int, str, int, int, str | None]]:
    contexts: list[tuple[int, str, int, int, str | None]] = []
    current_branch = initial_branch
    previous_branch: str | None = None
    pending_branch: tuple[str | None, bool] | None = None
    start = 0
    for index, raw_line in enumerate(stdout.splitlines(keepends=True)):
        line = raw_line.rstrip("\r\n")
        end = start + len(line)
        if pending_branch is not None and _GIT_BRANCH_CHANGE_FAILURE_RE.search(line):
            pending_branch = None
        if (
            pending_branch is not None
            and _GIT_COMMAND_RE.search(line)
            and _GIT_BRANCH_CHANGE_FAILURE_RE.search(stdout, end)
        ):
            pending_branch = None
        if pending_branch is not None and pending_branch[1]:
            success = _GIT_CHECKOUT_BRANCH_SUCCESS_RE.search(line)
            if success:
                pending_branch = (
                    success.group("switched") or success.group("already"),
                    False,
                )
        if pending_branch is not None and _GIT_COMMAND_RE.search(line):
            if pending_branch[1]:
                pending_branch = None
            if pending_branch is None:
                contexts.append((index, line, start, end, current_branch))
                branch_after_command = _branch_after_command(
                    line,
                    current_branch,
                    previous_branch,
                )
                if branch_after_command is not None:
                    pending_branch = branch_after_command
                start += len(raw_line)
                continue
            current_branch, previous_branch = _apply_branch_change(
                current_branch,
                previous_branch,
                pending_branch[0],
            )
            pending_branch = None
        contexts.append((index, line, start, end, current_branch))
        branch_after_command = _branch_after_command(
            line,
            current_branch,
            previous_branch,
        )
        if branch_after_command is not None:
            pending_branch = branch_after_command
        start += len(raw_line)
    return contexts


def _is_current_branch_push_to_protected(line: str, current_branch: str | None) -> bool:
    if not _is_protected_current_branch(current_branch):
        return False
    _has_force_flag, has_no_explicit_refspec = _git_push_line_info(line)
    return has_no_explicit_refspec


def _detect_force_push_current_branch(
    stdout: str,
    current_branch: str | None,
) -> list[GuardrailViolation]:
    violations: list[GuardrailViolation] = []
    for _index, line, start, end, branch in _line_contexts(stdout, current_branch):
        segment_branch = branch
        previous_branch: str | None = None
        for segment in _split_shell_segments(line):
            command = segment.strip()
            if command != line.strip() and _TIER1_PATTERNS["force_push_main"].search(command):
                violations.append(
                    GuardrailViolation(
                        tier=1,
                        category="force_push_main",
                        excerpt=_line_excerpt(stdout, start, end),
                        rule=_TIER1_RULES["force_push_main"],
                    )
                )
                break
            if (
                _is_protected_current_branch(segment_branch)
                and _GIT_PUSH_LINE_RE.search(command)
            ):
                has_force_flag, has_no_explicit_refspec = _git_push_line_info(
                    command
                )
                if has_force_flag and has_no_explicit_refspec:
                    violations.append(
                        GuardrailViolation(
                            tier=1,
                            category="force_push_main",
                            excerpt=_line_excerpt(stdout, start, end),
                            rule=_TIER1_RULES["force_push_main"],
                        )
                    )
                    break
            branch_after_command = _branch_after_command(
                command,
                segment_branch,
                previous_branch,
            )
            if branch_after_command is not None and not branch_after_command[1]:
                segment_branch, previous_branch = _apply_branch_change(
                    segment_branch,
                    previous_branch,
                    branch_after_command[0],
                )
    return violations


def _is_pr_create_command(line: str) -> bool:
    return bool(_GH_PR_CREATE_RE.search(line)) and not bool(
        _GH_PR_CREATE_NO_CREATE_FLAG_RE.search(line)
    )


def _split_shell_segments(text: str) -> list[str]:
    segments: list[str] = []
    start = 0
    quote: str | None = None
    escaped = False
    index = 0
    while index < len(text):
        char = text[index]
        if escaped:
            escaped = False
            index += 1
            continue
        if char == "\\":
            escaped = True
            index += 1
            continue
        if quote is not None:
            if char == quote:
                quote = None
            index += 1
            continue
        if char in {"'", '"'}:
            quote = char
            index += 1
            continue
        if char == ";" or text.startswith("&&", index) or text.startswith("||", index):
            segments.append(text[start:index])
            index += 2 if char in {"&", "|"} else 1
            start = index
            continue
        index += 1
    segments.append(text[start:])
    return segments


def _line_excerpt(coder_stdout: str, start: int, end: int) -> str:
    line_start = coder_stdout.rfind("\n", 0, start) + 1
    line_end = coder_stdout.find("\n", end)
    if line_end == -1:
        line_end = len(coder_stdout)
    return coder_stdout[line_start:line_end].strip()[:_EXCERPT_LIMIT]


def _detect_direct_commit_main(
    stdout: str,
    current_branch: str | None,
) -> list[GuardrailViolation]:
    """Detect commit then push to default branch without an intervening PR."""
    violations: list[GuardrailViolation] = []
    lines = stdout.splitlines()
    branch_by_line = {
        index: branch
        for index, _line, _start, _end, branch in _line_contexts(
            stdout,
            current_branch,
        )
    }

    for commit_index, commit_line in enumerate(lines):
        commit_match = _TIER1_PATTERNS[_DIRECT_COMMIT_CATEGORY].search(commit_line)
        if not commit_match:
            continue
        if re.search(r"(?<!\S)--amend(?!\S)", commit_line):
            continue

        same_line_after_commit = commit_line[commit_match.end() :]
        same_line_pr_created = False
        same_line_violation = False
        for same_line_segment in _split_shell_segments(same_line_after_commit)[1:]:
            same_line_command = same_line_segment.strip()
            if _is_pr_create_command(same_line_command):
                same_line_pr_created = True
                continue
            if same_line_pr_created:
                continue
            if (
                _GIT_PUSH_PROTECTED_BRANCH_RE.search(same_line_command)
                or _is_current_branch_push_to_protected(
                    same_line_command,
                    branch_by_line.get(commit_index),
                )
            ):
                violations.append(
                    GuardrailViolation(
                        tier=1,
                        category=_DIRECT_COMMIT_CATEGORY,
                        excerpt=commit_line.strip()[:_EXCERPT_LIMIT],
                        rule=_TIER1_RULES[_DIRECT_COMMIT_CATEGORY],
                    )
                )
                same_line_violation = True
                break
        if same_line_violation:
            continue

        for push_index in range(commit_index + 1, len(lines)):
            push_line = lines[push_index]
            if not (
                _GIT_PUSH_PROTECTED_BRANCH_RE.search(push_line)
                or _is_current_branch_push_to_protected(
                    push_line,
                    branch_by_line.get(push_index),
                )
            ):
                continue
            intermediate_lines = lines[commit_index + 1 : push_index]
            if any(_is_pr_create_command(line) for line in intermediate_lines):
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


def scan_stdout(
    coder_stdout: str,
    *,
    current_branch: str | None = None,
) -> list[GuardrailViolation]:
    """Return guardrail violations found in captured coder stdout."""
    violations: list[GuardrailViolation] = []
    for category in sorted(_TIER1_PATTERNS):
        if category == _DIRECT_COMMIT_CATEGORY:
            violations.extend(_detect_direct_commit_main(coder_stdout, current_branch))
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
        if category == "force_push_main":
            violations.extend(
                _detect_force_push_current_branch(coder_stdout, current_branch)
            )
    return violations
