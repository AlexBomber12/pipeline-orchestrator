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
}

_EXCERPT_LIMIT = 200
_WORKFLOW_PATH_END_RE = r'(?:"|(?=[ \t\r\n]))'
_DIFF_WORKFLOW_B_PATH_RE = (
    r'"?b/\.github/workflows/[^"/\r\n]+\.ya?ml' + _WORKFLOW_PATH_END_RE
)
_DIFF_WORKFLOW_A_PATH_RE = (
    r'"?a/\.github/workflows/[^"/\r\n]+\.ya?ml' + _WORKFLOW_PATH_END_RE
)
_DIFF_WORKFLOW_RENAME_PATH_RE = (
    r'"?\.github/workflows/[^"/\r\n]+\.ya?ml' + _WORKFLOW_PATH_END_RE
)
_WORKFLOW_WRITE_PERMISSION_SCOPES_RE = (
    r"[\"']?(?:actions|attestations|artifact-metadata|checks|code-quality|"
    r"contents|deployments|discussions|id-token|issues|packages|pages|"
    r"pull-requests|repository-projects|security-events|statuses|models|"
    r"vulnerability-alerts)[\"']?"
)
_YAML_ANCHOR_NAME_RE = r"&[A-Za-z_][A-Za-z0-9_-]*"
_YAML_SCALAR_ANCHOR_RE = r"(?:(?:&[A-Za-z_][A-Za-z0-9_-]*|![^\s#]+)[ \t]+)*"
_YAML_BLOCK_SCALAR_HEADER_RE = r"[|>](?:[1-9][+-]?|[+-][1-9]?)?"
_YAML_KEY_RE = re.compile(
    r"^(?P<indent>[ \t]*)(?P<quote>[\"']?)(?P<key>[^:\"'\r\n]+)"
    r"(?P=quote)[ \t]*:",
)
_YAML_PERMISSION_KEY_RE = re.compile(
    r"^[ \t]*[\"']?permissions[\"']?[ \t]*:",
    re.IGNORECASE,
)
_YAML_WRITE_SCOPE_RE = re.compile(
    r"^[ \t]*"
    + _WORKFLOW_WRITE_PERMISSION_SCOPES_RE
    + r"[ \t]*:[ \t]*"
    + _YAML_SCALAR_ANCHOR_RE
    + r"[\"']?write[\"']?,?(?:[ \t]*(?:#.*)?)?$",
    re.IGNORECASE,
)
_YAML_WRITE_SCOPE_BLOCK_RE = re.compile(
    r"^[ \t]*"
    + _WORKFLOW_WRITE_PERMISSION_SCOPES_RE
    + r"[ \t]*:[ \t]*"
    + _YAML_SCALAR_ANCHOR_RE
    + _YAML_BLOCK_SCALAR_HEADER_RE
    + r"(?:[ \t]*(?:#.*)?)?$",
    re.IGNORECASE,
)
_YAML_WRITE_BLOCK_VALUE_RE = re.compile(
    r"^[ \t]+[\"']?write[\"']?(?:[ \t]*(?:#.*)?)?$",
    re.IGNORECASE,
)
_YAML_NON_WRITE_SCOPE_RE = re.compile(
    r"^[ \t]*"
    + _WORKFLOW_WRITE_PERMISSION_SCOPES_RE
    + r"[ \t]*:[ \t]*"
    + _YAML_SCALAR_ANCHOR_RE
    + r"[\"']?(?:read|none)[\"']?,?(?:[ \t]*(?:#.*)?)?$",
    re.IGNORECASE,
)
_YAML_PERMISSION_SCOPE_ALIAS_RE = re.compile(
    r"^[ \t]*"
    + _WORKFLOW_WRITE_PERMISSION_SCOPES_RE
    + r"[ \t]*:[ \t]*\*[A-Za-z_][A-Za-z0-9_-]*[ \t]*(?:#.*)?$",
    re.IGNORECASE,
)
_YAML_VALUE_ALIAS_RE = re.compile(
    r":[ \t]*\*(?P<name>[A-Za-z_][A-Za-z0-9_-]*)[ \t]*(?:#.*)?$",
    re.IGNORECASE,
)
_YAML_ANCHOR_VALUE_RE = re.compile(
    r"&(?P<name>[A-Za-z_][A-Za-z0-9_-]*)[ \t]+(?P<value>[^\r\n#]+)",
    re.IGNORECASE,
)
_YAML_BLOCK_ANCHOR_RE = re.compile(
    r"&(?P<name>[A-Za-z_][A-Za-z0-9_-]*)[ \t]*(?:#.*)?$",
    re.IGNORECASE,
)
_YAML_JOBS_FLOW_PERMISSION_RE = re.compile(
    r"^[ \t]*[\"']?jobs[\"']?[ \t]*:[ \t]*"
    + _YAML_SCALAR_ANCHOR_RE
    + r"\{[^\r\n]*[\"']?permissions[\"']?[ \t]*:[ \t]*(?:"
    + _YAML_SCALAR_ANCHOR_RE
    + r"[\"']?write-all[\"']?"
    r"|"
    + _YAML_SCALAR_ANCHOR_RE
    + r"\{[^\r\n}]*"
    + _WORKFLOW_WRITE_PERMISSION_SCOPES_RE
    + r"[ \t]*:[ \t]*"
    + _YAML_SCALAR_ANCHOR_RE
    + r"[\"']?write[\"']?[^\r\n}]*\}"
    r")",
    re.IGNORECASE,
)
_PINNED_ACTION_REF_RE = re.compile(
    r"^(?:v\d+(?:\.\d+){0,2}|[0-9a-f]{40})$",
    re.IGNORECASE,
)
# Diff-content scan catalogue. PR-290b adds workflow YAML tampering checks;
# PR-290c and PR-301..PR-304 extend the same dispatcher with governance,
# supply-chain, secrets, large-diff, and mass-deletion entries.
_DIFF_PATTERNS: dict[str, re.Pattern[str]] = {
    "branch_protection_modification": re.compile(
        # Detect branch-protection metadata modification or deletion by
        # matching unified diff file headers for the governed files.
        r"(?m)^---[ \t]+a/\.github/(?:"
        r"branch[-_]protection[^ \r\n]*\.ya?ml"
        r"|settings\.ya?ml"
        r")[ \t]*\r?\n"
        r"\+\+\+[ \t]+(?:b/\.github/(?:branch[-_]protection[^ \r\n]*\.ya?ml"
        r"|settings\.ya?ml)|/dev/null)",
        re.IGNORECASE,
    ),
    "dangerous_action_external_install": re.compile(
        # Match added action references; pinned refs are filtered in
        # scan_pr_diff because this regex captures all candidate refs.
        r"(?m)^\+[ \t]*-?[ \t]*uses:[ \t]+"
        r"(?P<repo>[\w.-]+/[\w./-]+)"
        r"@(?P<ref>[^\s\r\n]+)",
        re.IGNORECASE,
    ),
    "permissions_escalation": re.compile(
        # Match `+`-prefixed lines (additions only) only inside workflow
        # YAML diff sections containing `permissions: write-all`
        # (top-level blanket write) OR a known permission scope set to
        # exactly `write`. Permission keys can appear at workflow top
        # level or under a job, so accepted indentation is bounded to
        # those YAML positions to avoid script-literal false positives.
        # Replacement hunks require visible `permissions:` context; unrelated
        # workflow YAML blocks can use the same keys outside `permissions`.
        r"(?ms)^diff --git[^\r\n]*[ \t]+"
        + _DIFF_WORKFLOW_B_PATH_RE
        + r"[^\r\n]*\r?\n"
        r"(?i:(?:(?:(?!^diff --git[ \t]).)*?^[ +][ \t]*"
        r"[\"']?permissions[\"']?[ \t]*:[ \t]*(?:"
        + _YAML_ANCHOR_NAME_RE
        + r"[ \t]*(?:\{[ \t]*)?|\{[ \t]*)?(?:#.*)?\r?\n"
        r"(?:^[ +][ \t]*(?:#.*)?\r?\n|^[ +\-][ \t]+"
        + _WORKFLOW_WRITE_PERMISSION_SCOPES_RE
        + r"[ \t]*:[^\r\n]*\r?\n)*^\+[ \t]+"
        + _WORKFLOW_WRITE_PERMISSION_SCOPES_RE
        + r"[ \t]*:[ \t]*"
        + _YAML_SCALAR_ANCHOR_RE
        + r"(?:[\"']?write[\"']?,?|"
        + _YAML_BLOCK_SCALAR_HEADER_RE
        + r"[ \t]*(?:#.*)?\r?\n"
        r"^\+[ \t]+[\"']?write[\"']?|\*[A-Za-z_][A-Za-z0-9_-]*)"
        r"|(?:(?!^diff --git[ \t]).)*?^\+[ \t]+\{[^\r\n}]*"
        + _WORKFLOW_WRITE_PERMISSION_SCOPES_RE
        + r"[ \t]*:[ \t]*"
        + _YAML_SCALAR_ANCHOR_RE
        + r"[\"']?write[\"']?[^\r\n}]*\}"
        r"|(?:(?!^diff --git[ \t]).)*?^\+[ \t]*"
        r"[\"']?permissions[\"']?[ \t]*:[ \t]*(?:"
        + _YAML_SCALAR_ANCHOR_RE
        + r"[\"']?write-all[\"']?"
        r"|"
        + _YAML_SCALAR_ANCHOR_RE
        + _YAML_BLOCK_SCALAR_HEADER_RE
        + r"[ \t]*(?:#.*)?\r?\n^\+[ \t]+[\"']?write-all[\"']?"
        r"|&[A-Za-z_][A-Za-z0-9_-]*[ \t]*\{[^\r\n}]*"
        + _WORKFLOW_WRITE_PERMISSION_SCOPES_RE
        + r"[ \t]*:[ \t]*"
        + _YAML_SCALAR_ANCHOR_RE
        + r"[\"']?write[\"']?[^\r\n}]*\}"
        r"|\*[A-Za-z_][A-Za-z0-9_-]*"
        r"|\{[^\r\n}]*"
        + _WORKFLOW_WRITE_PERMISSION_SCOPES_RE
        + r"[ \t]*:[ \t]*"
        + _YAML_SCALAR_ANCHOR_RE
        + r"[\"']?write[\"']?[^\r\n}]*\})"
        r"|(?:(?!^diff --git[ \t]).)*?^-[ \t]+"
        + _WORKFLOW_WRITE_PERMISSION_SCOPES_RE
        + r"[ \t]*:[ \t]*"
        + _YAML_SCALAR_ANCHOR_RE
        + r"[\"']?(?:read|none)[\"']?,?[ \t]*(?:#.*)?\r?\n"
        r"^\+[ \t]+"
        + _WORKFLOW_WRITE_PERMISSION_SCOPES_RE
        + r"[ \t]*:[ \t]*"
        + _YAML_SCALAR_ANCHOR_RE
        + r"[\"']?write[\"']?,?"
        r"|(?:(?!^diff --git[ \t]).)*?^\+[ \t]+"
        + _WORKFLOW_WRITE_PERMISSION_SCOPES_RE
        + r"[ \t]*:[ \t]*"
        + _YAML_SCALAR_ANCHOR_RE
        + r"[\"']?write[\"']?,?"
        r"|(?:(?!^diff --git[ \t]).)*?^\+[ \t]*"
        r"[\"']?jobs[\"']?[ \t]*:[ \t]*"
        + _YAML_SCALAR_ANCHOR_RE
        + r"\{(?:(?!^diff --git[ \t]).)*?[\"']?permissions[\"']?[ \t]*:[^\r\n]*"
        r"|(?:(?!^diff --git[ \t]).)*?^\+[ \t]+[\"']?[^:\"'\r\n]+[\"']?"
        r"[ \t]*:[ \t]*\{[^\r\n]*[\"']?permissions[\"']?[ \t]*:[^\r\n]*"
        r"|(?:(?!^diff --git[ \t]).)*?^\+[^\r\n]*"
        r"&[A-Za-z_][A-Za-z0-9_-]*[ \t]+[\"']?(?:write|write-all)[\"']?"
        r"(?:(?!^diff --git[ \t]).)*"
        r"|(?:(?!^diff --git[ \t]).)*?^[ +][^\r\n]*"
        r"&[A-Za-z_][A-Za-z0-9_-]*[ \t]*(?:#.*)?\r?\n"
        r"(?:(?!^diff --git[ \t]).)*?^\+[ \t]+[\"']?(?:write|write-all)[\"']?"
        r"(?:(?!^diff --git[ \t]).)*"
        r"))[ \t]*(?:#.*)?$",
    ),
    "workflow_destruction": re.compile(
        # Detect workflow YAML files being deleted entirely. In unified
        # diff format, a deletion shows `--- a/<path>` followed by
        # `+++ /dev/null` (where the deleted file's path appears in the
        # `--- a/` line and the special path `/dev/null` appears in the
        # `+++ b/` line). Require the surrounding `diff --git` section so
        # documentation snippets containing file-header text are not treated
        # as real workflow deletions. Pure rename diffs use `rename from`/
        # `rename to` metadata instead, but moving a workflow elsewhere still
        # removes it from GitHub Actions.
        r"(?ms)(?:^diff --git[^\r\n]*[ \t]+"
        + _DIFF_WORKFLOW_A_PATH_RE
        + r"[^\r\n]*\r?\n"
        r"(?:(?!^diff --git[ \t]).)*?^---[ \t]+"
        + _DIFF_WORKFLOW_A_PATH_RE
        + r"[ \t]*\r?\n"
        r"\+\+\+[ \t]+/dev/null\b"
        r"|^diff --git[^\r\n]*[ \t]+"
        + _DIFF_WORKFLOW_A_PATH_RE
        + r"[^\r\n]*\r?\n"
        r"(?:(?!^diff --git[ \t]).)*?^rename from[ \t]+"
        + _DIFF_WORKFLOW_RENAME_PATH_RE
        + r"[ \t]*\r?\n"
        r"rename to[ \t]+(?!"
        + _DIFF_WORKFLOW_RENAME_PATH_RE
        + r"[ \t]*$)"
        r"[^\r\n]+)"
    ),
}

_DIFF_RULES: dict[str, str] = {
    "branch_protection_modification": (
        "Branch protection metadata file modification or deletion in diff"
    ),
    "dangerous_action_external_install": (
        "Workflow uses unpinned external action reference "
        "(mutable ref like @main or @HEAD)"
    ),
    "permissions_escalation": "Workflow permission escalation in diff additions",
    "workflow_destruction": "Workflow YAML file deletion under .github/workflows/",
}


def _clip_excerpt(text: str) -> str:
    return text[:_EXCERPT_LIMIT]


def _action_ref_is_pinned(ref: str) -> bool:
    """Return True if ``ref`` is a semver tag or 40-char commit SHA."""
    return bool(_PINNED_ACTION_REF_RE.match(ref))


def _line_excerpt(coder_stdout: str, start: int, end: int) -> str:
    line_start = coder_stdout.rfind("\n", 0, start) + 1
    line_end = coder_stdout.find("\n", end)
    if line_end == -1:
        line_end = len(coder_stdout)
    return coder_stdout[line_start:line_end].strip()[:_EXCERPT_LIMIT]


def _diff_yaml_line(line: str) -> tuple[str, str] | None:
    if not line or line[0] not in {" ", "+", "-"}:
        return None
    if line.startswith(("+++", "---")):
        return None
    return line[0], line[1:]


def _diff_hunk_context_yaml_line(line: str) -> str | None:
    if not line.startswith("@@"):
        return None
    _, _separator, context = line.rpartition("@@")
    context = context.strip()
    if not context:
        return None
    return context if _yaml_key(context) is not None else None


def _yaml_key(line: str) -> tuple[int, str] | None:
    match = _YAML_KEY_RE.match(line)
    if match is None:
        return None
    return len(match.group("indent").expandtabs(2)), match.group("key").strip()


def _yaml_indent(line: str) -> int:
    return len(line[: len(line) - len(line.lstrip(" \t"))].expandtabs(2))


def _normalized_yaml_scalar(value: str) -> str:
    value = re.sub(
        r"^(?:(?:&[A-Za-z_][A-Za-z0-9_-]*|![^\s#]+)[ \t]+)*",
        "",
        value.strip(),
    )
    return value.rstrip(",").strip("\"'").lower()


def _mask_yaml_quoted_colon_values(value: str) -> str:
    chars = list(value)
    index = 0
    while index < len(chars):
        quote = chars[index]
        if quote not in {"'", '"'}:
            index += 1
            continue
        end = index + 1
        while end < len(chars):
            if chars[end] == quote and value[end - 1] != "\\":
                break
            end += 1
        if end >= len(chars):
            return "".join(chars)
        tail = value[end + 1 :].lstrip()
        if not tail.startswith(":") and ":" in value[index + 1 : end]:
            chars[index + 1 : end] = [" "] * (end - index - 1)
        index = end + 1
    return "".join(chars)


def _yaml_flow_permission_map_escalates(value: str) -> bool:
    value = _mask_yaml_quoted_colon_values(value)
    return bool(
        re.search(
            _WORKFLOW_WRITE_PERMISSION_SCOPES_RE
            + r"[ \t]*:[ \t]*"
            + _YAML_SCALAR_ANCHOR_RE
            + r"[\"']?write[\"']?",
            value,
            re.IGNORECASE,
        )
    )


def _yaml_anchor_values_before(
    lines: list[str], line_index: int
) -> dict[str, str]:
    anchors: dict[str, str] = {}
    for previous_index, previous_line in enumerate(lines[:line_index]):
        diff_line = _diff_yaml_line(previous_line)
        if diff_line is None:
            continue
        prefix, yaml_line = diff_line
        if prefix == "-":
            continue
        match = _YAML_ANCHOR_VALUE_RE.search(yaml_line)
        if match is not None:
            anchors[match.group("name")] = match.group("value").strip()
            continue
        block_match = _YAML_BLOCK_ANCHOR_RE.search(yaml_line)
        if block_match is None:
            continue
        anchor_indent = _yaml_indent(yaml_line)
        block_lines: list[str] = []
        for block_line in lines[previous_index + 1 : line_index]:
            block_diff_line = _diff_yaml_line(block_line)
            if block_diff_line is None:
                continue
            block_prefix, block_yaml_line = block_diff_line
            if block_prefix == "-":
                continue
            stripped_block_line = block_yaml_line.strip()
            if not stripped_block_line or stripped_block_line.startswith("#"):
                continue
            if _yaml_indent(block_yaml_line) <= anchor_indent:
                break
            block_lines.append(stripped_block_line)
        if block_lines:
            anchors[block_match.group("name")] = "\n".join(block_lines)
    return anchors


def _yaml_alias_value(line: str) -> str | None:
    match = _YAML_VALUE_ALIAS_RE.search(line)
    return None if match is None else match.group("name")


def _yaml_alias_resolves_to_escalation(
    lines: list[str], line_index: int, yaml_line: str, *, top_level_permissions: bool
) -> bool:
    alias_name = _yaml_alias_value(yaml_line)
    if alias_name is None:
        return False
    alias_value = _yaml_anchor_values_before(lines, line_index).get(alias_name)
    if alias_value is None:
        return False
    normalized_value = _normalized_yaml_scalar(alias_value)
    if top_level_permissions:
        return normalized_value == "write-all" or _yaml_flow_permission_map_escalates(
            alias_value
        )
    return normalized_value == "write"


def _visible_yaml_context(lines: list[str], line_index: int) -> list[tuple[int, str]]:
    context: list[tuple[int, str]] = []
    for previous_line in lines[:line_index]:
        hunk_context_line = _diff_hunk_context_yaml_line(previous_line)
        if hunk_context_line is not None:
            key = _yaml_key(hunk_context_line)
            if key is None:  # pragma: no cover - helper prefilters key-shaped lines
                continue
            context = [
                (existing_indent, existing_name)
                for existing_indent, existing_name in context
                if existing_indent < key[0]
            ]
            context.append(key)
            continue
        diff_line = _diff_yaml_line(previous_line)
        if diff_line is None:
            continue
        prefix, yaml_line = diff_line
        if prefix == "-":
            continue
        key = _yaml_key(yaml_line)
        if key is None:
            continue
        context = [
            (existing_indent, existing_name)
            for existing_indent, existing_name in context
            if existing_indent < key[0]
        ]
        context.append(key)
    return context


def _is_workflow_permission_key_context(
    lines: list[str], line_index: int, permission_line: str
) -> bool:
    key = _yaml_key(permission_line)
    if key is None:
        return False
    permission_indent, key_name = key
    if key_name.strip("\"'").lower() != "permissions":
        return False
    if permission_indent == 0:
        return True

    ancestors = [
        (indent, name.strip("\"'").lower())
        for indent, name in _visible_yaml_context(lines, line_index)
        if indent < permission_indent
    ]
    if not ancestors:
        return True
    root_indent = ancestors[0][0]
    jobs_index = next(
        (
            index
            for index, (indent, name) in enumerate(ancestors)
            if indent == root_indent and name == "jobs"
        ),
        None,
    )
    if jobs_index is None:
        return False
    return len(ancestors) == jobs_index + 2


def _flow_permission_alias_name(line: str) -> str | None:
    match = re.search(
        r"[\"']?permissions[\"']?[ \t]*:[ \t]*"
        r"\*(?P<name>[A-Za-z_][A-Za-z0-9_-]*)",
        line,
        re.IGNORECASE,
    )
    return None if match is None else match.group("name")


def _jobs_flow_fragment(lines: list[str], line_index: int, jobs_line: str) -> str:
    fragment = [jobs_line.strip()]
    brace_depth = jobs_line.count("{") - jobs_line.count("}")
    for next_line in lines[line_index + 1 :]:
        if brace_depth <= 0:
            break
        diff_line = _diff_yaml_line(next_line)
        if diff_line is None:
            continue
        prefix, yaml_line = diff_line
        if prefix == "-":
            continue
        fragment.append(yaml_line.strip())
        brace_depth += yaml_line.count("{") - yaml_line.count("}")
    return " ".join(fragment)


def _is_visible_root_jobs_key(
    lines: list[str], line_index: int, key_indent: int, key_name: str
) -> bool:
    if key_name.strip("\"'").lower() != "jobs":
        return False
    return not [
        ancestor
        for ancestor in _visible_yaml_context(lines, line_index)
        if ancestor[0] < key_indent
    ]


def _is_workflow_jobs_flow_permission_escalation(
    lines: list[str], line_index: int, jobs_line: str
) -> bool:
    key = _yaml_key(jobs_line)
    if key is None:
        return False
    jobs_indent, jobs_name = key
    if not _is_visible_root_jobs_key(lines, line_index, jobs_indent, jobs_name):
        return False
    flow_line = _jobs_flow_fragment(lines, line_index, jobs_line)
    if _YAML_JOBS_FLOW_PERMISSION_RE.match(_mask_yaml_quoted_colon_values(flow_line)):
        return True
    alias_name = _flow_permission_alias_name(flow_line)
    if alias_name is None:
        return False
    alias_value = _yaml_anchor_values_before(lines, line_index).get(alias_name)
    if alias_value is None:
        return False
    return _normalized_yaml_scalar(
        alias_value
    ) == "write-all" or _yaml_flow_permission_map_escalates(alias_value)


def _is_workflow_job_flow_permission_escalation(
    lines: list[str], line_index: int, job_line: str
) -> bool:
    key = _yaml_key(job_line)
    if key is None:
        return False
    job_indent, _job_name = key
    if job_indent == 0 or "{" not in job_line:
        return False
    ancestors = [
        (indent, name.strip("\"'").lower())
        for indent, name in _visible_yaml_context(lines, line_index)
        if indent < job_indent
    ]
    if len(ancestors) != 1 or ancestors[0][1] != "jobs":
        return False
    flow_line = f"jobs: {{ {job_line.strip()} }}"
    if _YAML_JOBS_FLOW_PERMISSION_RE.match(_mask_yaml_quoted_colon_values(flow_line)):
        return True
    alias_name = _flow_permission_alias_name(flow_line)
    if alias_name is None:
        return False
    alias_value = _yaml_anchor_values_before(lines, line_index).get(alias_name)
    if alias_value is None:
        return False
    return _normalized_yaml_scalar(
        alias_value
    ) == "write-all" or _yaml_flow_permission_map_escalates(alias_value)


def _visible_permission_alias_reference_escalates(
    lines: list[str], alias_name: str, *, top_level_permissions: bool
) -> bool:
    alias_ref_re = re.compile(rf"\*{re.escape(alias_name)}\b", re.IGNORECASE)
    scope_alias_re = re.compile(
        _WORKFLOW_WRITE_PERMISSION_SCOPES_RE
        + rf"[ \t]*:[ \t]*\*{re.escape(alias_name)}\b",
        re.IGNORECASE,
    )
    for index, line in enumerate(lines):
        diff_line = _diff_yaml_line(line)
        if diff_line is None:
            continue
        prefix, yaml_line = diff_line
        if prefix == "-" or alias_ref_re.search(yaml_line) is None:
            continue
        if top_level_permissions and _YAML_PERMISSION_KEY_RE.match(yaml_line):
            if _is_workflow_permission_key_context(lines, index, yaml_line):
                return True
        if not top_level_permissions and scope_alias_re.search(yaml_line):
            if _YAML_PERMISSION_KEY_RE.match(yaml_line):
                return _is_workflow_permission_key_context(lines, index, yaml_line)
            for parent_index in range(index - 1, -1, -1):
                parent_diff_line = _diff_yaml_line(lines[parent_index])
                if parent_diff_line is None:
                    continue
                parent_prefix, parent_yaml_line = parent_diff_line
                if parent_prefix == "-":
                    continue
                if not _YAML_PERMISSION_KEY_RE.match(parent_yaml_line):
                    continue
                return _is_workflow_permission_key_context(
                    lines, parent_index, parent_yaml_line
                )
    return False


def _anchor_value_edit_escalates(lines: list[str], yaml_line: str) -> bool:
    match = _YAML_ANCHOR_VALUE_RE.search(yaml_line)
    if match is None:
        return False
    alias_name = match.group("name")
    normalized_value = _normalized_yaml_scalar(match.group("value"))
    if normalized_value == "write-all":
        return _visible_permission_alias_reference_escalates(
            lines, alias_name, top_level_permissions=True
        )
    if normalized_value == "write":
        return _visible_permission_alias_reference_escalates(
            lines, alias_name, top_level_permissions=False
        )
    return False


def _block_anchor_value_edit_escalates(
    lines: list[str], line_index: int, yaml_line: str
) -> bool:
    normalized_value = _normalized_yaml_scalar(yaml_line)
    if normalized_value not in {"write", "write-all"}:
        return False
    value_indent = _yaml_indent(yaml_line)
    for previous_line in reversed(lines[:line_index]):
        diff_line = _diff_yaml_line(previous_line)
        if diff_line is None:
            continue
        prefix, previous_yaml_line = diff_line
        if prefix == "-":
            continue
        stripped_line = previous_yaml_line.strip()
        if not stripped_line or stripped_line.startswith("#"):
            continue
        previous_indent = _yaml_indent(previous_yaml_line)
        if previous_indent >= value_indent:
            continue
        block_match = _YAML_BLOCK_ANCHOR_RE.search(previous_yaml_line)
        if block_match is None:
            return False
        return _visible_permission_alias_reference_escalates(
            lines,
            block_match.group("name"),
            top_level_permissions=normalized_value == "write-all",
        )
    return False


def _anchor_value_edit_escalates_in_section(
    section_text: str, match_text: str
) -> bool:
    section_lines = section_text.splitlines()
    matched_lines = set(match_text.splitlines())
    for index, line in enumerate(section_lines):
        if line not in matched_lines:
            continue
        diff_line = _diff_yaml_line(line)
        if diff_line is None:
            continue
        prefix, yaml_line = diff_line
        if prefix != "+":
            continue
        if _anchor_value_edit_escalates(section_lines, yaml_line):
            return True
        if _block_anchor_value_edit_escalates(section_lines, index, yaml_line):
            return True
    return False


def _diff_file_section_at(diff_text: str, position: int) -> str:
    start = diff_text.rfind("\ndiff --git ", 0, position)
    if start == -1:
        start = 0
    else:
        start += 1
    end = diff_text.find("\ndiff --git ", position)
    if end == -1:
        end = len(diff_text)
    return diff_text[start:end]


def _replaces_read_scope_with_write(
    lines: list[str], line_index: int, scope_line: str
) -> bool:
    scope_key = _yaml_key(scope_line)
    if scope_key is None:
        return False
    scope_indent, scope_name = scope_key
    normalized_scope = scope_name.strip("\"'").lower()
    found_read_replacement = False
    for parent_index in range(line_index - 1, -1, -1):
        parent_diff_line = _diff_yaml_line(lines[parent_index])
        if parent_diff_line is None:
            continue
        parent_prefix, parent_yaml_line = parent_diff_line
        parent_key = _yaml_key(parent_yaml_line)
        if parent_key is None:
            continue
        parent_indent, parent_name = parent_key
        if (
            parent_prefix == "-"
            and parent_indent == scope_indent
            and parent_name.strip("\"'").lower() == normalized_scope
            and _YAML_NON_WRITE_SCOPE_RE.match(parent_yaml_line)
        ):
            found_read_replacement = True
            continue
        if parent_prefix == "-" or parent_indent >= scope_indent:
            continue
        if not found_read_replacement:
            return False
        if _YAML_PERMISSION_KEY_RE.match(parent_yaml_line):
            return _is_workflow_permission_key_context(
                lines, parent_index, parent_yaml_line
            )
        return False
    return found_read_replacement


def _is_contextless_permission_scope_addition(
    lines: list[str], line_index: int, scope_line: str
) -> bool:
    scope_key = _yaml_key(scope_line)
    if scope_key is None:
        return False
    scope_indent, _scope_name = scope_key
    if scope_indent not in {2, 6}:
        return False
    return not _visible_yaml_context(lines, line_index)


def _match_has_workflow_permission_context(match_text: str) -> bool:
    lines = match_text.splitlines()
    for index, line in enumerate(lines):
        diff_line = _diff_yaml_line(line)
        if diff_line is None:
            continue
        prefix, yaml_line = diff_line
        if prefix != "+":
            continue
        if _anchor_value_edit_escalates(lines, yaml_line):
            return True
        if _block_anchor_value_edit_escalates(lines, index, yaml_line):
            return True
        if _is_workflow_jobs_flow_permission_escalation(lines, index, yaml_line):
            return True
        if _is_workflow_job_flow_permission_escalation(lines, index, yaml_line):
            return True
        if _YAML_PERMISSION_KEY_RE.match(yaml_line) and (
            _is_workflow_permission_key_context(lines, index, yaml_line)
        ):
            if _yaml_alias_value(yaml_line) is not None:
                if _yaml_alias_resolves_to_escalation(
                    lines,
                    index,
                    yaml_line,
                    top_level_permissions=True,
                ):
                    return True
                continue
            return True
        is_write_scope = bool(_YAML_WRITE_SCOPE_RE.match(yaml_line))
        if not is_write_scope and _yaml_flow_permission_map_escalates(yaml_line):
            is_write_scope = True
        if not is_write_scope and _YAML_PERMISSION_SCOPE_ALIAS_RE.match(yaml_line):
            is_write_scope = _yaml_alias_resolves_to_escalation(
                lines,
                index,
                yaml_line,
                top_level_permissions=False,
            )
        if not is_write_scope and _YAML_WRITE_SCOPE_BLOCK_RE.match(yaml_line):
            for block_index in range(index + 1, len(lines)):
                block_diff_line = _diff_yaml_line(lines[block_index])
                if block_diff_line is None:
                    continue
                block_prefix, block_yaml_line = block_diff_line
                if block_prefix == "-":
                    continue
                is_write_scope = bool(_YAML_WRITE_BLOCK_VALUE_RE.match(block_yaml_line))
                break
        if not is_write_scope:
            continue
        for parent_index in range(index - 1, -1, -1):
            parent_diff_line = _diff_yaml_line(lines[parent_index])
            if parent_diff_line is None:
                continue
            parent_prefix, parent_yaml_line = parent_diff_line
            if parent_prefix == "-":
                continue
            parent_key = _yaml_key(parent_yaml_line)
            scope_key = _yaml_key(yaml_line)
            # Regex prefilters require key-shaped YAML lines here.
            if parent_key is None or scope_key is None:  # pragma: no cover
                continue
            parent_indent = parent_key[0]
            scope_indent = scope_key[0]
            if parent_indent >= scope_indent:
                continue
            if _YAML_PERMISSION_KEY_RE.match(parent_yaml_line):
                return _is_workflow_permission_key_context(
                    lines, parent_index, parent_yaml_line
                )
            break
        if _replaces_read_scope_with_write(lines, index, yaml_line):
            return True
        if _is_contextless_permission_scope_addition(lines, index, yaml_line):
            return True
    return False


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


def _is_protected_branch_ref(token: str) -> bool:
    return token in {
        _PROTECTED_DEFAULT_BRANCH,
        f"heads/{_PROTECTED_DEFAULT_BRANCH}",
        f"refs/heads/{_PROTECTED_DEFAULT_BRANCH}",
    }


def _is_empty_source_protected_refspec(token: str) -> bool:
    refspec = token[1:] if token.startswith("+") else token
    return refspec.startswith(":") and _is_protected_branch_ref(refspec[1:])


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


def scan_pr_diff(diff_text: str) -> list[GuardrailViolation]:
    """Scan PR diff content for prohibited patterns.

    Returns guardrail violations found in ``diff_text`` (the unified-diff
    format emitted by ``gh pr diff``). Mirrors :func:`scan_stdout` so
    callers can treat both signals uniformly. With the catalogue empty
    (PR-290a skeleton state) the result is always ``[]``; PR-290b/c and
    PR-301..PR-304 populate ``_DIFF_PATTERNS`` to add real detections.

    Patterns match against the full diff including ``+``/``-`` line
    prefixes so governance-file modifications, workflow YAML edits, and
    similar changes are visible regardless of which side of the diff
    they appear on.
    """
    violations: list[GuardrailViolation] = []
    for category in sorted(_DIFF_PATTERNS):
        pattern = _DIFF_PATTERNS[category]
        for match in pattern.finditer(diff_text):
            if category == "dangerous_action_external_install" and (
                _action_ref_is_pinned(match.group("ref"))
            ):
                continue
            if category == "permissions_escalation" and not (
                _match_has_workflow_permission_context(match.group(0))
                or _anchor_value_edit_escalates_in_section(
                    _diff_file_section_at(diff_text, match.start()), match.group(0)
                )
            ):
                continue
            violations.append(
                GuardrailViolation(
                    tier=1,
                    category=category,
                    excerpt=_clip_excerpt(match.group(0)),
                    rule=_DIFF_RULES.get(category, ""),
                )
            )
    return violations
