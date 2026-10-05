"""Read-only runtime diagnostics for the orchestrator MCP service.

The tools in this module deliberately read the producer-owned Redis keys and
files directly.  In particular, they do not use helpers that clean stale
indexes, refresh TTLs, synthesize healthy state, or otherwise mutate runtime
data while answering a diagnostic query.
"""

from __future__ import annotations

import base64
import json
import os
import re
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from itertools import islice
from pathlib import Path
from typing import Any

import redis.asyncio as aioredis

from src.config import AppConfig, RepoConfig, load_config
from src.events.disk_log import _resolve_events_dir
from src.events.publisher import EVENT_HISTORY_LIMIT
from src.keyspace import (
    cli_log_history,
    cli_log_latest,
    pipeline_state,
    repo_events_history,
    retry_command,
    retry_command_pending,
)
from src.mcp.server import mcp
from src.metrics import MetricsStore, RunRecord
from src.models import RepoState
from src.retry_commands import RetryCommand
from src.utils import repo_slug_from_url

_DEFAULT_REDIS_URL = "redis://localhost:6379/0"
_REPOS_ROOT = Path("/data/repos")
_REPO_SLUG_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*__[A-Za-z0-9][A-Za-z0-9_.-]*$")
_DISK_EVENT_SOURCE = re.compile(r"^events:disk/(\d{4}-\d{2}-\d{2})$")
_CLI_HISTORY_SOURCE_PREFIX = "cli:history/"
_MAX_STATUS_EVENTS = 25
_MAX_STATUS_RUNS = 20
_MAX_PENDING_RETRIES = 20
_MAX_LOG_SOURCES = 100
_MAX_READ_CHARS = 20_000
_MAX_EVENT_RECORD_CHARS = 4_000
_MAX_REDIS_EVENT_HISTORY_BYTES = 256 * 1024
_MAX_FILE_SCAN_BYTES = 256 * 1024
_MAX_PRIVATE_KEY_CONTEXT_BYTES = 1024 * 1024
_MAX_SENSITIVE_ASSIGNMENT_CONTEXT_BYTES = 1024 * 1024
_MAX_REDIS_SCAN_CALLS = 4
_MAX_REDIS_PENDING_KEYS = 200
_MAX_DISK_PARTITION_CANDIDATES = 200
_MAX_EMBEDDED_JSON_CANDIDATES = 64
_CLI_LATEST_TTL_SECONDS = 3600
_CLI_HISTORY_TTL_SECONDS = 86400
_HISTORY_CURSOR_PREFIX = "redis-history:"
_FILE_CURSOR_PREFIX = "file-record:"
_BOUNDED_EVENT_HISTORY_SCRIPT = """
local size = redis.call('MEMORY', 'USAGE', KEYS[1]) or 0
local total = redis.call('LLEN', KEYS[1])
if size > tonumber(ARGV[3]) then
  return {size, 1, total, {}}
end
return {size, 0, total, redis.call('LRANGE', KEYS[1], ARGV[1], ARGV[2])}
"""

_SENSITIVE_NAMES = (
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
    "token",
    "secret",
    "secret_key",
    "secret-key",
    "secretKey",
    "account_key",
    "account-key",
    "accountKey",
    "shared_access_key",
    "shared-access-key",
    "sharedAccessKey",
    "credential",
    "credentials",
    "access_token",
    "access-token",
    "refresh_token",
    "refresh-token",
    "auth_token",
    "auth-token",
    "api_key",
    "api-key",
    "client_secret",
    "client-secret",
    "password",
    "passwd",
    "passphrase",
    "key_data",
    "key-data",
    "private_key",
    "private-key",
    "aws_secret_access_key",
    "proxyAuthorization",
    "setCookie",
    "accessToken",
    "refreshToken",
    "authToken",
    "apiKey",
    "clientSecret",
    "privateKey",
    "awsSecretAccessKey",
    "secretAccessKey",
    "sessionToken",
    "keyData",
    "tls.key",
    "docker_auth_config",
    "docker-auth-config",
    "dockerAuthConfig",
)
_SENSITIVE_NAME_PATTERN = "|".join(re.escape(name) for name in _SENSITIVE_NAMES)
# Credential roles may have environment-style, dotted, or camelCase prefixes
# (for example DATABASE_PASSWORD, spring.datasource.password, or githubToken).
# Require the sensitive role to end the key so fields such as tokens_in remain
# ordinary counters.
_SENSITIVE_KEY_PATTERN = rf"[A-Za-z0-9_.-]*(?:{_SENSITIVE_NAME_PATTERN})"
_SENSITIVE_KEY = re.compile(rf"(?i)^(?:{_SENSITIVE_KEY_PATTERN})$")
_SENSITIVE_JSON_KEY_PREFIX = re.compile(
    rf"(?i)(?:[\"'](?:{_SENSITIVE_KEY_PATTERN})[\"'])\s*:\s*"
)
_PENDING_SENSITIVE_ASSIGNMENT = re.compile(
    rf"(?i)(?:[\"'](?:{_SENSITIVE_KEY_PATTERN})[\"']|"
    rf"(?<![A-Za-z0-9_.-])(?:{_SENSITIVE_KEY_PATTERN})(?![A-Za-z0-9_.-]))"
    r"\s*[:=][ \t]*(?:[|>][-+]?)?[ \t]*$"
)
_PENDING_YAML_SENSITIVE_ASSIGNMENT = re.compile(
    rf"(?i)^(?P<indent>[ \t]*)(?:-[ \t]+)?(?:[\"'](?:{_SENSITIVE_KEY_PATTERN})[\"']|"
    rf"(?:{_SENSITIVE_KEY_PATTERN}))\s*:[ \t]*$"
)
_PENDING_JSON_SENSITIVE_ASSIGNMENT = re.compile(
    rf'(?i)^[ \t]*"(?:{_SENSITIVE_KEY_PATTERN})"\s*:\s*$'
)
_BLOCK_SENSITIVE_ASSIGNMENT = re.compile(
    rf"(?i)^(?P<indent>[ \t]*)(?:-[ \t]+)?(?:[\"'](?:{_SENSITIVE_KEY_PATTERN})[\"']|"
    rf"(?:{_SENSITIVE_KEY_PATTERN}))\s*[:=][ \t]*"
    r"[|>](?:[1-9][-+]?|[-+][1-9]?|)[ \t]*(?:#.*)?$"
)
_QUOTED_SENSITIVE_ASSIGNMENT = re.compile(
    rf"(?i)(?:[\"'](?:{_SENSITIVE_KEY_PATTERN})[\"']|"
    rf"(?<![A-Za-z0-9_.-])(?:{_SENSITIVE_KEY_PATTERN})(?![A-Za-z0-9_.-]))"
    r"\s*[:=][ \t]*(?P<quote>\"\"\"|'''|[\"'])(?P<value>.*)$"
)
_PLAIN_SENSITIVE_ASSIGNMENT = re.compile(
    rf"(?i)^(?P<indent>[ \t]*)(?:-[ \t]+)?(?:[\"'](?:{_SENSITIVE_KEY_PATTERN})[\"']|"
    rf"(?:{_SENSITIVE_KEY_PATTERN}))\s*[:=][ \t]*(?P<value>(?![\"'|>])\S.*)$"
)
_PREFIXED_PLAIN_SENSITIVE_ASSIGNMENT = re.compile(
    rf"(?i)(?:[\"'](?:{_SENSITIVE_KEY_PATTERN})[\"']|"
    rf"(?<![A-Za-z0-9_.-])(?:{_SENSITIVE_KEY_PATTERN})(?![A-Za-z0-9_.-]))"
    r"\s*[:=][ \t]*(?P<value>(?![\"'|>])\S.*)$"
)
_REDACTION_RULES = (
    (
        re.compile(
            r"(?im)(\b(?:cookie|set-cookie|authorization|proxy-authorization)"
            r"[ \t]*:[ \t]*).*$"
        ),
        r"\1[REDACTED]",
    ),
    (
        re.compile(
            rf"(?i)(--(?:{_SENSITIVE_KEY_PATTERN})(?:=|[ \t]+))"
            r"(?:(?P<option_quote>[\"'])(?:\\[^\r\n]|(?!(?P=option_quote))[^\\\r\n])*"
            r"(?P=option_quote)?|[^\s]+)"
        ),
        r"\1[REDACTED]",
    ),
    (
        re.compile(
            r"(?i)((?<!\S)(?:-u|-U|--user|--proxy-user)(?:=|[ \t]+))"
            r"(?:(?P<user_quote>[\"'])(?:\\[^\r\n]|(?!(?P=user_quote))[^\\\r\n])*"
            r"(?P=user_quote)?|[^\s]+)"
        ),
        r"\1[REDACTED]",
    ),
    (
        re.compile(
            r"(?im)(\b(?:machine[ \t]+\S+|default)\b[^\r\n]*?\bpassword[ \t]+)(?![:=])"
            r"(?:(?P<netrc_quote>[\"'])(?:\\[^\r\n]|(?!(?P=netrc_quote))[^\\\r\n])*"
            r"(?P=netrc_quote)?|[^\s]+)"
        ),
        r"\1[REDACTED]",
    ),
    (
        re.compile(
            r"(?im)^([ \t]*(?:password|passwd)[ \t]+)(?![:=])"
            r"(?:(?P<netrc_line_quote>[\"'])(?:\\[^\r\n]|"
            r"(?!(?P=netrc_line_quote))[^\\\r\n])*(?P=netrc_line_quote)?|[^\s]+)"
        ),
        r"\1[REDACTED]",
    ),
    (
        re.compile(
            rf"(?i)([\"'](?:{_SENSITIVE_KEY_PATTERN})[\"']\s*:\s*)"
            r"(?P<json_quote>[\"'])(?:\\[^\r\n]|(?!(?P=json_quote))[^\\\r\n])*"
            r"(?P=json_quote)?"
        ),
        r"\1\g<json_quote>[REDACTED]\g<json_quote>",
    ),
    (
        re.compile(
            rf"(?im)((?<![A-Za-z0-9_-])(?:{_SENSITIVE_KEY_PATTERN})"
            r"(?![A-Za-z0-9_-])\s*[:=]\s*)"
            r"(?P<assignment_quote>\"\"\"|'''|[\"'])"
            r"(?:\\[^\r\n]|(?!(?P=assignment_quote))[^\\\r\n])*"
            r"(?P=assignment_quote)?"
        ),
        r"\1[REDACTED]",
    ),
    (
        re.compile(
            rf"(?im)((?<![A-Za-z0-9_-])(?:{_SENSITIVE_KEY_PATTERN})"
            r"(?![A-Za-z0-9_-])\s*[:=](?![ \t]*\[REDACTED\])[ \t]*)"
            r"(?:bearer[ \t]+|basic[ \t]+)?[^\r\n]*"
        ),
        r"\1[REDACTED]",
    ),
    (re.compile(r"(?i)\b(?:bearer|basic)\s+[A-Za-z0-9._~+/=-]+"), "[REDACTED]"),
    (
        re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b"),
        "[REDACTED]",
    ),
    (re.compile(r"\b(?:AKIA|ASIA|AGPA|AIDA|AROA|AIPA|ANPA|ANVA)[A-Z0-9]{16}\b"), "[REDACTED]"),
    (re.compile(r"\bsk-ant-[A-Za-z0-9_-]{30,}\b"), "[REDACTED]"),
    (re.compile(r"\bsk-[A-Za-z0-9_-]{48,}\b"), "[REDACTED]"),
    (re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b"), "[REDACTED]"),
    (
        re.compile(
            r"\bhttps://hooks\.slack(?:-gov)?\.com/(?:services/)?"
            r"T[A-Z0-9]+/B[A-Z0-9]+/[A-Za-z0-9]{24}\b"
        ),
        "[REDACTED]",
    ),
    (re.compile(r"\bsk_(?:test|live)_[A-Za-z0-9]{20,}\b"), "[REDACTED]"),
    (re.compile(r"\brk_(?:test|live)_[A-Za-z0-9]{20,}\b"), "[REDACTED]"),
    (re.compile(r"\bAIza[A-Za-z0-9_-]{35}\b"), "[REDACTED]"),
    (
        re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b"),
        "[REDACTED]",
    ),
    (
        re.compile(
            r"(?is)-----BEGIN [^-\r\n]*PRIVATE KEY(?: BLOCK)?-----.*?"
            r"-----END [^-\r\n]*PRIVATE KEY(?: BLOCK)?-----"
        ),
        "[REDACTED PRIVATE KEY]",
    ),
    (
        re.compile(r"(?is)-----BEGIN [^-\r\n]*PRIVATE KEY(?: BLOCK)?-----.*\Z"),
        "[REDACTED PRIVATE KEY]",
    ),
    (
        re.compile(r"(?is)\A.*?-----END [^-\r\n]*PRIVATE KEY(?: BLOCK)?-----"),
        "[REDACTED PRIVATE KEY]",
    ),
    (
        re.compile(r"(?i)([a-z][a-z0-9+.-]*://[^\s/:@]*:)[^\s/@]+(@)"),
        r"\1[REDACTED]\2",
    ),
)
_PRIVATE_KEY_BEGIN = re.compile(rb"-----BEGIN [^-\r\n]*PRIVATE KEY(?: BLOCK)?-----", re.IGNORECASE)
_PRIVATE_KEY_END = re.compile(rb"-----END [^-\r\n]*PRIVATE KEY(?: BLOCK)?-----", re.IGNORECASE)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_z(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _decode(raw: object) -> str:
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace")
    return str(raw)


def _validate_limit(value: int, *, maximum: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ValueError(f"{name} must be between 1 and {maximum}")
    return value


def _validate_cursor(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("cursor must be a non-negative integer")
    return value


def _new_redis_client() -> Any:
    return aioredis.from_url(
        os.environ.get("REDIS_URL", _DEFAULT_REDIS_URL),
        decode_responses=True,
    )


async def _close_redis(client: Any | None) -> None:
    if client is not None:
        await client.aclose()


def _configured_repositories() -> tuple[AppConfig, dict[str, RepoConfig]]:
    config = load_config()
    return config, {repo_slug_from_url(repo.url): repo for repo in config.repositories}


def _validate_configured_repo(repo_slug: str) -> tuple[AppConfig, RepoConfig]:
    if not _REPO_SLUG_PATTERN.fullmatch(repo_slug):
        raise ValueError(f"Invalid repo_slug: {repo_slug!r}")
    config, repositories = _configured_repositories()
    repo = repositories.get(repo_slug)
    if repo is None:
        raise ValueError(f"Repository is not configured: {repo_slug!r}")
    return config, repo


def _json_like_value_end(text: str, start: int) -> int:
    if start >= len(text):
        return start
    opener = text[start]
    if opener in {'"', "'"}:
        index = start + 1
        while index < len(text):
            if opener == '"' and text[index] == "\\":
                index += 2
                continue
            if text[index] == opener:
                return index + 1
            index += 1
        return len(text)
    if opener in "{[":
        stack = [opener]
        quote: str | None = None
        index = start + 1
        while index < len(text):
            character = text[index]
            if quote is not None:
                if quote == '"' and character == "\\":
                    index += 2
                    continue
                if character == quote:
                    quote = None
            elif character in {'"', "'"}:
                quote = character
            elif character in "{[":
                stack.append(character)
            elif character in "}]":
                expected = "{" if character == "}" else "["
                if stack[-1] != expected:
                    return len(text)
                stack.pop()
                if not stack:
                    return index + 1
            index += 1
        return len(text)
    index = start
    while index < len(text) and text[index] not in ",}]\r\n":
        index += 1
    return index


def _redact_malformed_keyed_values(text: str) -> tuple[str, int]:
    parts: list[str] = []
    cursor = 0
    search_from = 0
    count = 0
    while match := _SENSITIVE_JSON_KEY_PREFIX.search(text, search_from):
        value_start = match.end()
        value_end = _json_like_value_end(text, value_start)
        if value_end <= value_start:
            search_from = value_start
            continue
        if text[value_start:value_end] in {'"[REDACTED]"', "'[REDACTED]'"}:
            search_from = value_end
            continue
        parts.append(text[cursor:value_start])
        parts.append('"[REDACTED]"')
        cursor = value_end
        search_from = value_end
        count += 1
    if count == 0:
        return text, 0
    parts.append(text[cursor:])
    return "".join(parts), count


def _redact_text(text: str) -> tuple[str, int]:
    redacted = text
    count = 0
    for pattern, replacement in _REDACTION_RULES:
        redacted, replacements = pattern.subn(replacement, redacted)
        count += replacements
    redacted, replacements = _redact_malformed_keyed_values(redacted)
    return redacted, count + replacements


def _redact_all_values(value: Any) -> tuple[Any, int]:
    if isinstance(value, dict):
        result: dict[Any, Any] = {}
        count = 0
        for key, item in value.items():
            safe, replacements = _redact_all_values(item)
            result[key] = safe
            count += replacements
        return result, count
    if isinstance(value, list):
        result_list: list[Any] = []
        count = 0
        for item in value:
            safe, replacements = _redact_all_values(item)
            result_list.append(safe)
            count += replacements
        return result_list, count
    return "[REDACTED]", 1


def _redact_structure(value: Any, *, docker_auth_context: bool = False) -> tuple[Any, int]:
    if isinstance(value, str):
        safe, replacements, _ = _redact_log_content(value)
        return safe, replacements
    if isinstance(value, list):
        result: list[Any] = []
        count = 0
        for item in value:
            safe, replacements = _redact_structure(item, docker_auth_context=docker_auth_context)
            result.append(safe)
            count += replacements
        return result, count
    if isinstance(value, dict):
        result_dict: dict[Any, Any] = {}
        count = 0
        kubernetes_secret = any(
            isinstance(key, str)
            and key.casefold().replace("_", "").replace("-", "") == "kind"
            and isinstance(item, str)
            and item.casefold() == "secret"
            for key, item in value.items()
        )
        sensitive_named_value = any(
            isinstance(key, str)
            and key.casefold().replace("_", "").replace("-", "") == "name"
            and isinstance(item, str)
            and _SENSITIVE_KEY.fullmatch(item)
            for key, item in value.items()
        )
        for key, item in value.items():
            normalized_key = (
                key.casefold().replace("_", "").replace("-", "") if isinstance(key, str) else ""
            )
            docker_secret = docker_auth_context and normalized_key in {"auth", "identitytoken"}
            named_secret = sensitive_named_value and normalized_key == "value"
            kubernetes_secret_payload = kubernetes_secret and normalized_key in {
                "data",
                "stringdata",
            }
            if kubernetes_secret_payload:
                safe, replacements = _redact_all_values(item)
            elif (
                (isinstance(key, str) and _SENSITIVE_KEY.fullmatch(key))
                or docker_secret
                or named_secret
            ):
                safe, replacements = "[REDACTED]", 1
            else:
                safe, replacements = _redact_structure(
                    item,
                    docker_auth_context=docker_auth_context or normalized_key == "auths",
                )
            result_dict[key] = safe
            count += replacements
        return result_dict, count
    return value, 0


def _redact_logical_text(text: str) -> tuple[str, int]:
    """Structurally redact a complete JSON record, otherwise redact ordinary text."""
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError):
        safe_text, replacements = _redact_text(text)
        if replacements:
            return safe_text, replacements
        return _redact_embedded_structures(text)
    if not isinstance(parsed, (dict, list)):
        return _redact_text(text)
    safe, replacements = _redact_structure(parsed)
    if replacements == 0:
        return _redact_text(text)
    trailing_newline = "\r\n" if text.endswith("\r\n") else "\n" if text.endswith("\n") else ""
    return json.dumps(safe, ensure_ascii=False, separators=(",", ":"), default=str) + trailing_newline, replacements


def _redact_embedded_structures(text: str) -> tuple[str, int]:
    """Redact bounded JSON objects or arrays embedded in a prefixed log line."""
    candidates = [index for index, character in enumerate(text) if character in "{["]
    if len(candidates) > _MAX_EMBEDDED_JSON_CANDIDATES:
        trailing_newline = "\r\n" if text.endswith("\r\n") else "\n" if text.endswith("\n") else ""
        return "[CONTENT OMITTED: STRUCTURED REDACTION BOUND EXCEEDED]" + trailing_newline, 1

    decoder = json.JSONDecoder()
    parts: list[str] = []
    consumed = 0
    count = 0
    for start in candidates:
        if start < consumed:
            continue
        try:
            parsed, end = decoder.raw_decode(text, start)
        except (TypeError, ValueError):
            continue
        safe, replacements = _redact_structure(parsed)
        if replacements == 0:
            continue
        safe_prefix, prefix_replacements = _redact_text(text[consumed:start])
        parts.append(safe_prefix)
        parts.append(json.dumps(safe, ensure_ascii=False, separators=(",", ":"), default=str))
        consumed = end
        count += prefix_replacements + replacements

    if not parts:
        return _redact_text(text)
    safe_suffix, suffix_replacements = _redact_text(text[consumed:])
    parts.append(safe_suffix)
    return "".join(parts), count + suffix_replacements


def _private_key_state_before(handle: Any, offset: int) -> tuple[bool | None, int]:
    """Find the nearest PEM boundary before offset without loading the file."""
    search_end = offset
    suffix = b""
    scanned_bytes = 0
    while search_end > 0 and scanned_bytes < _MAX_PRIVATE_KEY_CONTEXT_BYTES:
        read_size = min(
            search_end,
            _MAX_FILE_SCAN_BYTES,
            _MAX_PRIVATE_KEY_CONTEXT_BYTES - scanned_bytes,
        )
        search_start = search_end - read_size
        handle.seek(search_start)
        chunk = handle.read(search_end - search_start)
        scanned_bytes += len(chunk)
        searchable = chunk + suffix
        markers = [
            *((match.start(), True) for match in _PRIVATE_KEY_BEGIN.finditer(searchable)),
            *((match.start(), False) for match in _PRIVATE_KEY_END.finditer(searchable)),
        ]
        if markers:
            return max(markers, key=lambda item: item[0])[1], scanned_bytes
        suffix = chunk[:512]
        search_end = search_start
    return (None if search_end > 0 else False), scanned_bytes


def _line_indent(raw_line: bytes) -> int:
    return len(raw_line) - len(raw_line.lstrip(b" \t"))


def _has_line_continuation(raw_line: bytes) -> bool:
    content = raw_line.rstrip(b"\r\n")
    trailing_backslashes = len(content) - len(content.rstrip(b"\\"))
    return trailing_backslashes % 2 == 1


def _has_closing_quote(value: str, quote: str) -> bool:
    index = 0
    while index < len(value):
        character = value[index]
        if quote in {'"', '"""'} and character == "\\":
            index += 2
            continue
        if len(quote) == 3 and value.startswith(quote, index):
            return True
        if len(quote) == 1 and character == quote:
            if quote == "'" and index + 1 < len(value) and value[index + 1] == "'":
                index += 2
                continue
            return True
        index += 1
    return False


def _sensitive_state_before(
    handle: Any,
    offset: int,
    raw: bytes,
) -> tuple[bool | None, bool | None, int | None, bool | None, str | None, int]:
    """Recover adjacent-value and YAML scalar state with one bounded read."""
    if offset <= 0:
        return False, False, None, False, None, 0
    search_start = max(0, offset - _MAX_SENSITIVE_ASSIGNMENT_CONTEXT_BYTES)
    handle.seek(search_start)
    context = handle.read(offset - search_start)
    scanned_bytes = len(context)
    if search_start > 0:
        newline = context.find(b"\n")
        if newline < 0:
            return None, None, None, None, None, scanned_bytes
        context = context[newline + 1 :]

    context_lines = context.splitlines()
    starts_with_sensitive_value: bool | None = None
    for raw_line in reversed(context_lines):
        if not raw_line.strip():
            continue
        line = raw_line.decode("utf-8", errors="replace")
        starts_with_sensitive_value = (
            _PENDING_SENSITIVE_ASSIGNMENT.search(line) is not None
            and _BLOCK_SENSITIVE_ASSIGNMENT.fullmatch(line) is None
        )
        break
    if starts_with_sensitive_value is None and search_start == 0:
        starts_with_sensitive_value = False

    block_state_known = search_start == 0
    active_block_indent: int | None = None
    for raw_line in context_lines:
        if not raw_line.strip():
            continue
        indent = _line_indent(raw_line)
        if active_block_indent is not None:
            if indent > active_block_indent:
                continue
            active_block_indent = None
        line = raw_line.decode("utf-8", errors="replace")
        indented_match = (
            _BLOCK_SENSITIVE_ASSIGNMENT.fullmatch(line)
            or _PLAIN_SENSITIVE_ASSIGNMENT.fullmatch(line)
            or _PENDING_YAML_SENSITIVE_ASSIGNMENT.fullmatch(line)
        )
        if indented_match is not None:
            active_block_indent = len(indented_match.group("indent"))
            block_state_known = True
        elif indent == 0:
            block_state_known = True

    first_content_line = next((line for line in raw.splitlines() if line.strip()), None)
    if first_content_line is None:
        starts_inside_sensitive_block: bool | None = False
        active_block_indent = None
    elif active_block_indent is not None and _line_indent(first_content_line) > active_block_indent:
        starts_inside_sensitive_block = True
    elif _line_indent(first_content_line) == 0 or block_state_known:
        starts_inside_sensitive_block = False
        active_block_indent = None
    else:
        starts_inside_sensitive_block = None
        active_block_indent = None

    if context_lines and _has_line_continuation(context_lines[-1]):
        continuation_start = len(context_lines) - 1
        while continuation_start > 0 and _has_line_continuation(context_lines[continuation_start - 1]):
            continuation_start -= 1
        continuation_line = context_lines[continuation_start].decode("utf-8", errors="replace")
        if _PREFIXED_PLAIN_SENSITIVE_ASSIGNMENT.search(continuation_line) is not None:
            starts_inside_sensitive_block = True
            active_block_indent = -1
        elif continuation_start == 0 and search_start > 0:
            starts_inside_sensitive_block = None
            active_block_indent = None

    quote_state_known = search_start == 0
    active_quote: str | None = None
    for raw_line in context_lines:
        line = raw_line.decode("utf-8", errors="replace")
        if active_quote is not None:
            if _has_closing_quote(line, active_quote):
                active_quote = None
                quote_state_known = True
            continue
        quoted_match = _QUOTED_SENSITIVE_ASSIGNMENT.search(line)
        if quoted_match is not None:
            quote = quoted_match.group("quote")
            if not _has_closing_quote(quoted_match.group("value"), quote):
                active_quote = quote
            quote_state_known = True

    if active_quote is not None:
        starts_inside_sensitive_quote: bool | None = True
    elif quote_state_known:
        starts_inside_sensitive_quote = False
    else:
        starts_inside_sensitive_quote = None

    return (
        starts_with_sensitive_value,
        starts_inside_sensitive_block,
        active_block_indent,
        starts_inside_sensitive_quote,
        active_quote,
        scanned_bytes,
    )


def _redacted_file_units(
    raw: bytes,
    *,
    starts_inside_private_key: bool | None,
    starts_with_sensitive_value: bool | None,
    starts_inside_sensitive_block: bool | None,
    sensitive_block_indent: int | None,
    starts_inside_sensitive_quote: bool | None,
    sensitive_quote: str | None,
    has_more_after_raw: bool,
    warnings: list[str],
) -> list[tuple[bytes, str, int]]:
    """Redact complete logical units while preserving their source byte sizes."""
    if starts_inside_private_key is None:
        warnings.append(
            "Private-key context exceeded the bounded backward scan; page content was omitted fail-closed."
        )
        return [(raw, "[CONTENT OMITTED: PRIVATE-KEY CONTEXT UNKNOWN]\n", 1)] if raw else []
    if starts_with_sensitive_value is None:
        warnings.append(
            "Sensitive-assignment context exceeded its bounded scan; page content was omitted fail-closed."
        )
        return [(raw, "[CONTENT OMITTED: SENSITIVE-ASSIGNMENT CONTEXT UNKNOWN]\n", 1)] if raw else []
    if starts_inside_sensitive_block is None:
        warnings.append(
            "Sensitive-block context exceeded its bounded scan; page content was omitted fail-closed."
        )
        return [(raw, "[CONTENT OMITTED: SENSITIVE-BLOCK CONTEXT UNKNOWN]\n", 1)] if raw else []
    if starts_inside_sensitive_quote is None:
        warnings.append(
            "Sensitive-quoted-scalar context exceeded its bounded scan; page content was omitted fail-closed."
        )
        return [(raw, "[CONTENT OMITTED: SENSITIVE-QUOTE CONTEXT UNKNOWN]\n", 1)] if raw else []
    raw_lines = raw.splitlines(keepends=True)
    units: list[tuple[bytes, str, int]] = []
    line_index = 0
    if starts_inside_sensitive_quote and sensitive_quote is not None:
        quote_end = 0
        while quote_end < len(raw_lines):
            quote_end += 1
            text_line = raw_lines[quote_end - 1].decode("utf-8", errors="replace")
            if _has_closing_quote(text_line, sensitive_quote):
                break
        raw_unit = b"".join(raw_lines[:quote_end])
        units.append((raw_unit, "[REDACTED SENSITIVE QUOTED SCALAR]\n", 1))
        line_index = quote_end
    elif starts_inside_sensitive_block and sensitive_block_indent is not None:
        block_end = 0
        if sensitive_block_indent == -1:
            while block_end < len(raw_lines):
                continued = _has_line_continuation(raw_lines[block_end])
                block_end += 1
                if not continued:
                    break
        else:
            while block_end < len(raw_lines):
                if raw_lines[block_end].strip() and _line_indent(raw_lines[block_end]) <= sensitive_block_indent:
                    break
                block_end += 1
        if block_end:
            raw_unit = b"".join(raw_lines[:block_end])
            marker = (
                "[REDACTED SENSITIVE CONTINUATION]\n"
                if sensitive_block_indent == -1
                else (
                    "[REDACTED SENSITIVE VALUE]\n"
                    if starts_with_sensitive_value
                    else "[REDACTED SENSITIVE BLOCK]\n"
                )
            )
            units.append((raw_unit, marker, 1))
            line_index = block_end
    elif starts_with_sensitive_value:
        value_index = 0
        while value_index < len(raw_lines) and not raw_lines[value_index].strip():
            value_index += 1
        if value_index < len(raw_lines):
            raw_unit = b"".join(raw_lines[: value_index + 1])
            units.append((raw_unit, "[REDACTED SENSITIVE VALUE]\n", 1))
            line_index = value_index + 1
    inside_private_key = starts_inside_private_key
    while line_index < len(raw_lines):
        raw_unit = raw_lines[line_index]
        if inside_private_key or _PRIVATE_KEY_BEGIN.search(raw_unit):
            end_index = line_index
            while end_index < len(raw_lines) and not _PRIVATE_KEY_END.search(raw_lines[end_index]):
                end_index += 1
            if end_index < len(raw_lines):
                raw_unit = b"".join(raw_lines[line_index : end_index + 1])
                line_index = end_index
                inside_private_key = False
            else:
                raw_unit = b"".join(raw_lines[line_index:])
                warnings.append(
                    "A private-key block crossed the bounded scan window; its visible segment was redacted."
                )
                line_index = len(raw_lines) - 1
                inside_private_key = True
            units.append((raw_unit, "[REDACTED PRIVATE KEY]\n", 1))
        else:
            text_unit = raw_unit.decode("utf-8", errors="replace")
            quoted_match = _QUOTED_SENSITIVE_ASSIGNMENT.search(text_unit.rstrip("\r\n"))
            if quoted_match is not None:
                quote = quoted_match.group("quote")
                if not _has_closing_quote(quoted_match.group("value"), quote):
                    quote_end = line_index + 1
                    while quote_end < len(raw_lines):
                        text_line = raw_lines[quote_end].decode("utf-8", errors="replace")
                        quote_end += 1
                        if _has_closing_quote(text_line, quote):
                            break
                    raw_unit = b"".join(raw_lines[line_index:quote_end])
                    units.append((raw_unit, "[REDACTED SENSITIVE QUOTED SCALAR]\n", 1))
                    line_index = quote_end
                    continue
            block_match = _BLOCK_SENSITIVE_ASSIGNMENT.fullmatch(text_unit.rstrip("\r\n"))
            if block_match is not None:
                block_indent = len(block_match.group("indent"))
                block_end = line_index + 1
                while block_end < len(raw_lines):
                    if raw_lines[block_end].strip() and _line_indent(raw_lines[block_end]) <= block_indent:
                        break
                    block_end += 1
                raw_unit = b"".join(raw_lines[line_index:block_end])
                units.append((raw_unit, "[REDACTED SENSITIVE BLOCK]\n", 1))
                line_index = block_end
                continue
            plain_match = _PLAIN_SENSITIVE_ASSIGNMENT.fullmatch(text_unit.rstrip("\r\n"))
            continued_assignment = _has_line_continuation(raw_unit)
            prefixed_continuation_match = (
                _PREFIXED_PLAIN_SENSITIVE_ASSIGNMENT.search(text_unit.rstrip("\r\n"))
                if continued_assignment
                else None
            )
            if plain_match is not None or prefixed_continuation_match is not None:
                scalar_indent = len(plain_match.group("indent")) if plain_match is not None else 0
                scalar_end = line_index + 1
                if continued_assignment:
                    continued = True
                    while scalar_end < len(raw_lines) and continued:
                        continued = _has_line_continuation(raw_lines[scalar_end])
                        scalar_end += 1
                    if continued and has_more_after_raw:
                        warnings.append(
                            "A backslash-continued sensitive assignment crossed the bounded page window; "
                            "its visible segment was redacted."
                        )
                else:
                    while scalar_end < len(raw_lines):
                        if raw_lines[scalar_end].strip() and _line_indent(raw_lines[scalar_end]) <= scalar_indent:
                            break
                        scalar_end += 1
                raw_unit = b"".join(raw_lines[line_index:scalar_end])
                safe_unit, replacements = _redact_logical_text(text_unit)
                units.append((raw_unit, safe_unit, replacements))
                line_index = scalar_end
                continue
            pending_yaml_match = _PENDING_YAML_SENSITIVE_ASSIGNMENT.fullmatch(text_unit.rstrip("\r\n"))
            if pending_yaml_match is not None:
                assignment_indent = len(pending_yaml_match.group("indent"))
                value_end = line_index + 1
                while value_end < len(raw_lines):
                    if raw_lines[value_end].strip() and _line_indent(raw_lines[value_end]) <= assignment_indent:
                        break
                    value_end += 1
                if value_end > line_index + 1:
                    raw_unit = b"".join(raw_lines[line_index:value_end])
                    units.append((raw_unit, "[REDACTED SENSITIVE ASSIGNMENT]\n", 1))
                    line_index = value_end
                    continue
                if (
                    _PENDING_JSON_SENSITIVE_ASSIGNMENT.fullmatch(text_unit.rstrip("\r\n"))
                    and value_end < len(raw_lines)
                ):
                    raw_unit = b"".join(raw_lines[line_index : value_end + 1])
                    units.append((raw_unit, "[REDACTED SENSITIVE ASSIGNMENT]\n", 1))
                    line_index = value_end + 1
                    continue
                if has_more_after_raw and value_end == len(raw_lines):
                    warnings.append(
                        "A sensitive YAML assignment crossed the bounded page window; its visible key was redacted."
                    )
                    units.append((raw_unit, "[REDACTED SENSITIVE ASSIGNMENT]\n", 1))
                    line_index += 1
                    continue
            elif _PENDING_SENSITIVE_ASSIGNMENT.search(text_unit):
                value_index = line_index + 1
                while value_index < len(raw_lines) and not raw_lines[value_index].strip():
                    value_index += 1
                if value_index < len(raw_lines):
                    raw_unit = b"".join(raw_lines[line_index : value_index + 1])
                    units.append((raw_unit, "[REDACTED SENSITIVE ASSIGNMENT]\n", 1))
                    line_index = value_index + 1
                    continue
                elif has_more_after_raw:
                    warnings.append(
                        "A sensitive assignment crossed the bounded page window; its visible key was redacted."
                    )
                    units.append((raw_unit, "[REDACTED SENSITIVE ASSIGNMENT]\n", 1))
                    line_index += 1
                    continue
            safe_unit, replacements = _redact_logical_text(text_unit)
            units.append((raw_unit, safe_unit, replacements))
        line_index += 1
    return units


def _redact_log_content(text: str) -> tuple[str, int, list[str]]:
    """Redact an in-memory log using the same logical-unit policy as files."""
    warnings: list[str] = []
    truncation_marker = "[truncated]\n"
    prefix = truncation_marker if text.startswith(truncation_marker) else ""
    content = text[len(prefix) :]
    leading_context: bool | None = None if prefix and content else False
    units = _redacted_file_units(
        content.encode("utf-8", errors="replace"),
        starts_inside_private_key=leading_context,
        starts_with_sensitive_value=leading_context,
        starts_inside_sensitive_block=leading_context,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=leading_context,
        sensitive_quote=None,
        has_more_after_raw=False,
        warnings=warnings,
    )
    return prefix + "".join(unit[1] for unit in units), sum(unit[2] for unit in units), warnings


def _error_text(exc: Exception) -> str:
    message = str(exc).strip()
    return message or type(exc).__name__


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _snapshot_metadata(
    state: RepoState | None,
    *,
    status: str,
    observed_at: datetime,
    stale_after_seconds: int,
    error: str | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "status": status,
        "observed_at": _iso_z(observed_at),
        "source_timestamp": None,
        "age_seconds": None,
        "stale_after_seconds": stale_after_seconds,
        "clock_skew_detected": False,
        "error": error,
        "meaning": (
            "Snapshot freshness only; last_updated is not evidence that a coder process is running or making progress."
        ),
    }
    if state is None:
        return result
    updated = state.last_updated
    if updated.tzinfo is None:
        updated = updated.replace(tzinfo=timezone.utc)
    updated = updated.astimezone(timezone.utc)
    age = (observed_at - updated).total_seconds()
    result.update(
        {
            "source_timestamp": _iso_z(updated),
            "age_seconds": round(age, 3),
            "clock_skew_detected": age < -5,
            "status": ("clock_skew" if age < -5 else "fresh" if age <= stale_after_seconds else "stale"),
        }
    )
    return result


def _stale_after_seconds(config: AppConfig, repo: RepoConfig) -> int:
    return max(
        300,
        repo.poll_interval_sec * 3,
        config.daemon.idle_extended_poll_interval_sec * 2,
        config.daemon.watch_slow_poll_interval_sec * 2,
    )


def _state_overview(
    slug: str,
    repo: RepoConfig,
    config: AppConfig,
    raw: object | None,
    *,
    redis_status: str,
    redis_error: str | None,
    observed_at: datetime,
) -> tuple[dict[str, Any], RepoState | None]:
    state: RepoState | None = None
    snapshot_status = redis_status
    snapshot_error = redis_error
    if redis_status == "available":
        if raw is None:
            snapshot_status = "missing"
            snapshot_error = "No pipeline state snapshot is stored for this repository."
        else:
            try:
                state = RepoState.model_validate_json(raw)
            except Exception as exc:
                snapshot_status = "malformed"
                snapshot_error = f"Stored pipeline state could not be decoded: {_error_text(exc)}"
            else:
                snapshot_status = "observed"
    stale_after = _stale_after_seconds(config, repo)
    snapshot = _snapshot_metadata(
        state,
        status=snapshot_status,
        observed_at=observed_at,
        stale_after_seconds=stale_after,
        error=snapshot_error,
    )
    configured_coder = (repo.coder or config.daemon.coder).value
    overview = {
        "repo_slug": slug,
        "url": repo.url,
        "configured": {
            "active": repo.active,
            "branch": repo.branch,
            "coder": configured_coder,
        },
        "snapshot": snapshot,
        "observed": {
            "state": state.state.value if state is not None else None,
            "active": state.active if state is not None else None,
            "user_paused": state.user_paused if state is not None else None,
            "coder": state.coder if state is not None else None,
            "current_task": (
                state.current_task.pr_id if state is not None and state.current_task is not None else None
            ),
            "current_pr": (state.current_pr.number if state is not None and state.current_pr is not None else None),
            "ci_status": (
                state.current_pr.ci_status.value if state is not None and state.current_pr is not None else None
            ),
            "review_status": (
                state.current_pr.review_status.value if state is not None and state.current_pr is not None else None
            ),
            "error": state.error_message if state is not None else None,
        },
        "integrity_warnings": (
            [f"Snapshot name {state.name!r} does not match configured slug {slug!r}."]
            if state is not None and state.name != slug
            else []
        ),
    }
    return overview, state


def _queue_summary(state: RepoState | None, observed_at: datetime) -> dict[str, Any]:
    if state is None or state.current_queue is None:
        return {
            "status": "unavailable",
            "source_timestamp": None,
            "snapshot_age_seconds": None,
            "reported_done": state.queue_done if state is not None else None,
            "reported_total": state.queue_total if state is not None else None,
            "counts_by_status": {},
            "tasks": [],
            "truncated": False,
        }
    timestamp = state.current_queue_snapshot_at
    age = None
    if timestamp is not None:
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        timestamp = timestamp.astimezone(timezone.utc)
        age = round((observed_at - timestamp).total_seconds(), 3)
    counts: dict[str, int] = {}
    for task in state.current_queue:
        counts[task.status.value] = counts.get(task.status.value, 0) + 1
    tasks = [task.model_dump(mode="json") for task in state.current_queue[:20]]
    return {
        "status": "available",
        "source_timestamp": _iso_z(timestamp) if timestamp is not None else None,
        "snapshot_age_seconds": age,
        "reported_done": state.queue_done,
        "reported_total": state.queue_total,
        "counts_by_status": counts,
        "tasks": tasks,
        "truncated": len(state.current_queue) > len(tasks),
    }


def _bounded_event(raw: object) -> dict[str, Any]:
    text = _decode(raw)
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError) as exc:
        safe_text, _, _ = _redact_log_content(text)
        return {
            "status": "malformed",
            "error": _error_text(exc),
            "raw_excerpt": safe_text[:500],
            "record_truncated": len(safe_text) > 500,
        }
    if not isinstance(parsed, dict):
        safe_parsed, _ = _redact_structure(parsed)
        safe_text = json.dumps(safe_parsed, ensure_ascii=False, default=str)
        return {
            "status": "malformed",
            "error": "Event record is not a JSON object.",
            "raw_excerpt": safe_text[:500],
            "record_truncated": len(safe_text) > 500,
        }
    original_serialized = json.dumps(parsed, ensure_ascii=False, sort_keys=True, default=str)
    safe_parsed, _ = _redact_structure(parsed)
    serialized = json.dumps(safe_parsed, ensure_ascii=False, sort_keys=True, default=str)
    if len(original_serialized) > _MAX_EVENT_RECORD_CHARS:
        return {
            "status": "valid",
            "timestamp": safe_parsed.get("timestamp"),
            "type": safe_parsed.get("type") or safe_parsed.get("event_type"),
            "record_excerpt": serialized[:_MAX_EVENT_RECORD_CHARS],
            "record_truncated": True,
        }
    return {"status": "valid", "record": safe_parsed, "record_truncated": False}


async def _read_bounded_event_history(
    redis_client: Any,
    repo_slug: str,
    *,
    start: int,
    stop: int,
) -> tuple[list[object], int, int, bool]:
    result = await redis_client.eval_ro(
        _BOUNDED_EVENT_HISTORY_SCRIPT,
        1,
        repo_events_history(repo_slug),
        start,
        stop,
        _MAX_REDIS_EVENT_HISTORY_BYTES,
    )
    if not isinstance(result, (list, tuple)) or len(result) != 4:
        raise RuntimeError("Redis returned a malformed bounded event-history response.")
    size_bytes = int(result[0])
    oversized = bool(int(result[1]))
    total = int(result[2])
    raw_events = result[3]
    if not isinstance(raw_events, (list, tuple)):
        raise RuntimeError("Redis returned malformed bounded event-history records.")
    return list(raw_events), total, size_bytes, oversized


async def _recent_events(redis_client: Any, repo_slug: str, limit: int) -> dict[str, Any]:
    try:
        raw_events, total, size_bytes, oversized = await _read_bounded_event_history(
            redis_client,
            repo_slug,
            start=0,
            stop=limit - 1,
        )
    except Exception as exc:
        return {
            "status": "unavailable",
            "source": "redis_event_history",
            "events": [],
            "error": _error_text(exc),
        }
    if oversized:
        return {
            "status": "oversized",
            "source": "redis_event_history",
            "events": [],
            "record_count": total,
            "size_bytes_estimate": size_bytes,
            "read_bound_bytes": _MAX_REDIS_EVENT_HISTORY_BYTES,
            "error": "Redis event history exceeds the bounded diagnostic read limit.",
        }
    events = [_bounded_event(raw) for raw in raw_events]
    return {
        "status": "available",
        "source": "redis_event_history",
        "newest_first": True,
        "history_cap": EVENT_HISTORY_LIMIT,
        "events": events,
        "malformed_records": sum(event["status"] == "malformed" for event in events),
        "possibly_truncated": total >= EVENT_HISTORY_LIMIT,
        "size_bytes_estimate": size_bytes,
        "read_bound_bytes": _MAX_REDIS_EVENT_HISTORY_BYTES,
        "error": None,
    }


def _retry_payload(command: RetryCommand, ttl_seconds: int) -> dict[str, Any]:
    payload = command.model_dump(mode="json")
    history = payload.pop("history", [])
    payload["recent_history"] = history[-5:]
    payload["history_truncated"] = len(history) > 5
    payload["ttl_seconds_remaining"] = ttl_seconds
    return payload


async def _pending_retries(redis_client: Any, repo_slug: str) -> dict[str, Any]:
    pending_key = retry_command_pending(repo_slug)
    try:
        total = int(await redis_client.zcard(pending_key))
        indexed = await redis_client.zrange(pending_key, 0, _MAX_PENDING_RETRIES - 1, withscores=True)
    except Exception as exc:
        return {
            "status": "unavailable",
            "count": None,
            "commands": [],
            "error": _error_text(exc),
            "read_only_note": "No stale index members were pruned.",
        }
    commands: list[dict[str, Any]] = []
    for row in indexed:
        raw_id, score = row
        command_id = _decode(raw_id)
        command_key = retry_command(repo_slug, command_id)
        try:
            raw = await redis_client.get(command_key)
            ttl = int(await redis_client.ttl(command_key))
        except Exception as exc:
            commands.append(
                {
                    "status": "unavailable",
                    "command_id": command_id,
                    "index_score": score,
                    "error": _error_text(exc),
                }
            )
            continue
        if raw is None:
            commands.append(
                {
                    "status": "missing_payload",
                    "command_id": command_id,
                    "index_score": score,
                    "error": "Pending index member has no retained command payload.",
                }
            )
            continue
        try:
            command = RetryCommand.model_validate_json(raw)
        except Exception as exc:
            commands.append(
                {
                    "status": "malformed",
                    "command_id": command_id,
                    "index_score": score,
                    "raw_excerpt": _decode(raw)[:500],
                    "error": _error_text(exc),
                }
            )
            continue
        commands.append(
            {
                "status": "available",
                "index_score": score,
                "command": _retry_payload(command, ttl),
            }
        )
    return {
        "status": "available",
        "count": total,
        "commands": commands,
        "truncated": total > len(indexed),
        "continuation": ({"next_index": len(indexed)} if total > len(indexed) else None),
        "error": None,
        "read_only_note": (
            "Index and payloads were read directly; missing or malformed members "
            "were reported without pruning or TTL refresh."
        ),
    }


def _run_payload(raw: object, run_id: str) -> dict[str, Any]:
    text = _decode(raw)
    try:
        decoded = json.loads(text)
        if not isinstance(decoded, dict):
            raise ValueError("Run record is not a JSON object.")
        record = RunRecord(**decoded)
    except Exception as exc:
        return {
            "status": "malformed",
            "run_id": run_id,
            "raw_excerpt": text[:500],
            "error": _error_text(exc),
        }
    payload = asdict(record)
    # Checkpointed in-progress records intentionally carry empty outcome/cause.
    # RunRecord's legacy migration maps an empty outcome to failed/CRASH on
    # construction, so restore the producer values for truthful diagnostics.
    if decoded.get("ended_at") is None:
        payload["outcome"] = decoded.get("outcome", "")
        payload["cause"] = decoded.get("cause")
    selected = {
        key: payload[key]
        for key in (
            "run_id",
            "task_id",
            "repo_name",
            "profile_id",
            "started_at",
            "ended_at",
            "duration_ms",
            "run_phase",
            "attempt_index",
            "fix_iterations",
            "exit_reason",
            "outcome",
            "cause",
            "cause_subsource",
            "base_sha",
            "head_sha",
        )
    }
    return {"status": "available", "record": selected}


async def _relevant_runs(
    redis_client: Any,
    repo_slug: str,
    task_id: str | None,
    limit: int,
) -> dict[str, Any]:
    index_key = MetricsStore._recent_key(task_id or "PR", repo_slug)
    scan_limit = min(200, max(20, limit * 4))
    try:
        raw_ids = await redis_client.lrange(index_key, 0, scan_limit - 1)
    except Exception as exc:
        return {
            "status": "unavailable",
            "task_filter": task_id,
            "records": [],
            "error": _error_text(exc),
        }
    records: list[dict[str, Any]] = []
    missing = 0
    for raw_id in raw_ids:
        run_id = _decode(raw_id)
        try:
            raw = await redis_client.get(MetricsStore._record_key(run_id))
        except Exception as exc:
            records.append({"status": "unavailable", "run_id": run_id, "error": _error_text(exc)})
            continue
        if raw is None:
            missing += 1
            continue
        item = _run_payload(raw, run_id)
        if task_id is not None and item["status"] == "available" and item["record"]["task_id"] != task_id:
            continue
        records.append(item)
        if len(records) >= limit:
            break
    return {
        "status": "available",
        "task_filter": task_id,
        "records": records,
        "missing_indexed_records": missing,
        "scanned_index_entries": len(raw_ids),
        "scan_limit": scan_limit,
        "truncated": len(raw_ids) >= scan_limit or len(records) >= limit,
        "error": None,
    }


def _state_history(state: RepoState | None, limit: int) -> dict[str, Any]:
    if state is None:
        return {"status": "unavailable", "newest_first": True, "events": []}
    selected = list(reversed(state.history[-limit:]))
    events: list[dict[str, Any]] = []
    for entry in selected:
        item = dict(entry)
        event = str(item.get("event", ""))
        if len(event) > 2_000:
            item["event"] = event[:2_000]
            item["record_truncated"] = True
        events.append(item)
    return {
        "status": "available",
        "newest_first": True,
        "events": events,
        "truncated": len(state.history) > len(events),
    }


def _state_detail(state: RepoState | None) -> dict[str, Any] | None:
    if state is None:
        return None
    return {
        "state": state.state.value,
        "active": state.active,
        "user_paused": state.user_paused,
        "coder": state.coder,
        "current_task": (state.current_task.model_dump(mode="json") if state.current_task is not None else None),
        "current_pr": (state.current_pr.model_dump(mode="json") if state.current_pr is not None else None),
        "error": state.error_message,
        "last_updated": _iso_z(state.last_updated),
        "merge_phase": state.merge_phase,
        "pending_queue_sync_branch": state.pending_queue_sync_branch,
        "pending_queue_sync_started_at": (
            _iso_z(state.pending_queue_sync_started_at) if state.pending_queue_sync_started_at is not None else None
        ),
        "upload_pending_count": state.upload_pending_count,
    }


def _progress_evidence(
    state: RepoState | None,
    runs: dict[str, Any],
    recent_events: dict[str, Any],
) -> dict[str, Any]:
    unfinished = [
        item["record"]
        for item in runs.get("records", [])
        if item.get("status") == "available" and item["record"].get("ended_at") is None
    ]
    event_timestamps: list[str] = []
    for item in recent_events.get("events", []):
        record = item.get("record") if isinstance(item, dict) else None
        timestamp = record.get("timestamp") if isinstance(record, dict) else None
        if timestamp is None and isinstance(item, dict):
            timestamp = item.get("timestamp")
        if isinstance(timestamp, str):
            event_timestamps.append(timestamp)
    return {
        "process_activity": "unknown",
        "current_task": (
            state.current_task.model_dump(mode="json") if state is not None and state.current_task is not None else None
        ),
        "current_pr": (
            state.current_pr.model_dump(mode="json") if state is not None and state.current_pr is not None else None
        ),
        "unfinished_run_records": unfinished,
        "latest_retained_event_at": max(event_timestamps) if event_timestamps else None,
        "interpretation": (
            "Task, PR, event, and unfinished run records are retained progress evidence. "
            "They do not prove a coder process is currently alive; MCP health and "
            "RepoState.last_updated are deliberately not used for that inference."
        ),
    }


@mcp.tool()
async def get_orchestrator_status(
    repo_slug: str | None = None,
    event_limit: int = 10,
    run_limit: int = 5,
) -> dict[str, Any]:
    """Return runtime status without mutating orchestrator state.

    Omit ``repo_slug`` for a compact overview of every configured repository.
    Supply a configured ``owner__repo`` slug for queue, inhibitor, event,
    pending-Retry, and run-record detail. Snapshot freshness is reported
    separately from progress evidence and never treated as coder liveness.
    """
    event_limit = _validate_limit(event_limit, maximum=_MAX_STATUS_EVENTS, name="event_limit")
    run_limit = _validate_limit(run_limit, maximum=_MAX_STATUS_RUNS, name="run_limit")
    observed_at = _utc_now()
    try:
        config, repositories = _configured_repositories()
    except Exception as exc:
        payload = {
            "observed_at": _iso_z(observed_at),
            "configuration": {"status": "unavailable", "error": _error_text(exc)},
            "redis": {"status": "not_checked", "error": None},
            "repositories": [],
            "detail": None,
        }
        safe, replacements = _redact_structure(payload)
        safe["redaction"] = {"applied": replacements > 0, "replacements": replacements}
        return safe
    if repo_slug is not None:
        if not _REPO_SLUG_PATTERN.fullmatch(repo_slug):
            raise ValueError(f"Invalid repo_slug: {repo_slug!r}")
        if repo_slug not in repositories:
            raise ValueError(f"Repository is not configured: {repo_slug!r}")

    client: Any | None = None
    redis_status = "available"
    redis_error: str | None = None
    state_raw: list[object | None] = [None] * len(repositories)
    slugs = list(repositories)
    try:
        client = _new_redis_client()
        state_raw = await client.mget([pipeline_state(slug) for slug in slugs])
    except Exception as exc:
        redis_status = "unavailable"
        redis_error = _error_text(exc)

    overviews: list[dict[str, Any]] = []
    states: dict[str, RepoState | None] = {}
    for index, slug in enumerate(slugs):
        overview, state = _state_overview(
            slug,
            repositories[slug],
            config,
            state_raw[index],
            redis_status=redis_status,
            redis_error=redis_error,
            observed_at=observed_at,
        )
        overviews.append(overview)
        states[slug] = state

    detail: dict[str, Any] | None = None
    if repo_slug is not None:
        state = states[repo_slug]
        if redis_status == "available" and client is not None:
            events = await _recent_events(client, repo_slug, event_limit)
            retries = await _pending_retries(client, repo_slug)
            runs = await _relevant_runs(
                client,
                repo_slug,
                state.current_task.pr_id if state is not None and state.current_task is not None else None,
                run_limit,
            )
        else:
            unavailable = {"status": "unavailable", "error": redis_error}
            events = {**unavailable, "source": "redis_event_history", "events": []}
            retries = {**unavailable, "count": None, "commands": []}
            runs = {**unavailable, "task_filter": None, "records": []}
        detail = {
            "repo_slug": repo_slug,
            "state": _state_detail(state),
            "queue": _queue_summary(state, observed_at),
            "inhibitors": (
                [item.model_dump(mode="json") for item in state.active_inhibitors] if state is not None else []
            ),
            "state_history": _state_history(state, event_limit),
            "recent_events": events,
            "pending_retries": retries,
            "run_records": runs,
            "coder_progress": _progress_evidence(state, runs, events),
        }

    await _close_redis(client)
    payload = {
        "observed_at": _iso_z(observed_at),
        "configuration": {"status": "available", "repository_count": len(slugs)},
        "redis": {"status": redis_status, "error": redis_error},
        "repositories": overviews,
        "detail": detail,
    }
    safe, replacements = _redact_structure(payload)
    safe["redaction"] = {"applied": replacements > 0, "replacements": replacements}
    return safe


def _ttl_metadata(ttl: int, observed_at: datetime) -> dict[str, Any]:
    return {
        "ttl_seconds_remaining": ttl if ttl >= 0 else None,
        "expires_at": (_iso_z(observed_at + timedelta(seconds=ttl)) if ttl >= 0 else None),
        "expiry_status": "expires" if ttl >= 0 else "persistent" if ttl == -1 else "missing",
    }


def _association(*, recorded: bool = False) -> dict[str, Any]:
    return {
        "recorded": recorded,
        "task_id": None,
        "run_id": None,
        "sha": None,
        "note": ("No task, run, or SHA association is recorded by this source." if not recorded else None),
    }


def _safe_path(root: Path, *parts: str) -> Path:
    resolved_root = root.resolve()
    unresolved = resolved_root
    for part in parts:
        unresolved /= part
        if unresolved.is_symlink():
            raise ValueError("Diagnostic file path must not contain symlinks.")
    candidate = unresolved.resolve()
    if not candidate.is_relative_to(resolved_root):
        raise ValueError("Resolved diagnostic path escapes its allowed root.")
    return candidate


def _safe_repo_directory(root: Path, repo_slug: str) -> Path:
    resolved_root = root.resolve()
    selected = resolved_root / repo_slug
    if selected.is_symlink() or selected.resolve() != selected:
        raise ValueError("Selected repository diagnostic directory must not be a symlink.")
    return selected


async def _redis_log_sources(
    client: Any, repo_slug: str, observed_at: datetime
) -> tuple[list[dict[str, Any]], list[str]]:
    sources: list[dict[str, Any]] = []
    warnings: list[str] = []
    latest_key = cli_log_latest(repo_slug)
    try:
        latest = await client.get(latest_key)
        latest_ttl = int(await client.ttl(latest_key))
        event_edges, event_count, event_size_bytes, event_oversized = await _read_bounded_event_history(
            client,
            repo_slug,
            start=0,
            stop=-1,
        )
    except Exception as exc:
        message = _error_text(exc)
        warnings.append(f"Redis diagnostic sources unavailable: {message}")
        sources.extend(
            [
                {
                    "source_id": "cli:latest",
                    "kind": "retained_cli_log",
                    "storage": "redis",
                    "availability": "unavailable",
                    "error": message,
                    "association": _association(),
                },
                {
                    "source_id": "events:redis",
                    "kind": "repository_event_history",
                    "storage": "redis",
                    "availability": "unavailable",
                    "error": message,
                    "association": _association(),
                },
            ]
        )
        return sources, warnings

    latest_text = _decode(latest) if latest is not None else ""
    sources.append(
        {
            "source_id": "cli:latest",
            "kind": "retained_cli_log",
            "storage": "redis",
            "availability": "available" if latest is not None else "missing_or_expired",
            "timestamps": {
                "recorded_at": None,
                **_ttl_metadata(latest_ttl, observed_at),
            },
            "size_chars": len(latest_text) if latest is not None else 0,
            "retention": {
                "producer_ttl_seconds": _CLI_LATEST_TTL_SECONDS,
                "truncated": latest_text.startswith("[truncated]\n"),
                "truncation_marker_preserved": True,
            },
            "association": _association(),
            "mutable": True,
        }
    )

    if event_oversized:
        warnings.append(
            "Redis event history exceeds the bounded diagnostic read limit; record details were not materialized."
        )
    event_timestamps: list[str] = []
    malformed_events = 0
    for raw in event_edges:
        item = _bounded_event(raw)
        if item["status"] == "malformed":
            malformed_events += 1
            continue
        record = item.get("record")
        timestamp = record.get("timestamp") if isinstance(record, dict) else item.get("timestamp")
        if isinstance(timestamp, str):
            event_timestamps.append(timestamp)
    sources.append(
        {
            "source_id": "events:redis",
            "kind": "repository_event_history",
            "storage": "redis",
            "availability": "oversized" if event_oversized else "available" if event_count else "empty",
            "timestamps": {
                "newest_at": max(event_timestamps) if event_timestamps else None,
                "oldest_at": min(event_timestamps) if event_timestamps else None,
                "expires_at": None,
            },
            "record_count": event_count,
            "size_bytes_estimate": event_size_bytes,
            "malformed_records": malformed_events,
            "retention": {
                "entry_cap": EVENT_HISTORY_LIMIT,
                "possibly_truncated": event_count >= EVENT_HISTORY_LIMIT,
                "expiry": "none",
            },
            "read_bound_bytes": _MAX_REDIS_EVENT_HISTORY_BYTES,
            "association": _association(),
            "mutable": True,
            "ordering": "newest_first",
        }
    )

    return sources, warnings


def _encode_history_cursor(scan_cursor: int, pending: list[str], *, started: bool) -> str:
    payload = json.dumps(
        {"scan_cursor": scan_cursor, "pending": pending, "started": started},
        separators=(",", ":"),
    ).encode()
    encoded = base64.urlsafe_b64encode(payload).decode().rstrip("=")
    return f"{_HISTORY_CURSOR_PREFIX}{encoded}"


def _decode_log_cursor(value: int | str) -> tuple[str, int | dict[str, Any]]:
    if isinstance(value, int) and not isinstance(value, bool):
        return "static", _validate_cursor(value)
    if not isinstance(value, str) or not value.startswith(_HISTORY_CURSOR_PREFIX):
        raise ValueError("cursor must be a non-negative static offset or a returned history cursor")
    encoded = value.removeprefix(_HISTORY_CURSOR_PREFIX)
    try:
        padding = "=" * (-len(encoded) % 4)
        raw = base64.b64decode(encoded + padding, altchars=b"-_", validate=True)
        payload = json.loads(raw)
    except (ValueError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("Invalid history cursor.") from exc
    if not isinstance(payload, dict):
        raise ValueError("Invalid history cursor.")
    scan_cursor = payload.get("scan_cursor")
    pending = payload.get("pending")
    started = payload.get("started")
    if (
        isinstance(scan_cursor, bool)
        or not isinstance(scan_cursor, int)
        or scan_cursor < 0
        or not isinstance(pending, list)
        or len(pending) > _MAX_REDIS_PENDING_KEYS
        or not isinstance(started, bool)
        or any(
            not isinstance(timestamp, str)
            or _parse_timestamp(timestamp) is None
            or "/" in timestamp
            for timestamp in pending
        )
    ):
        raise ValueError("Invalid history cursor.")
    return "history", {"scan_cursor": scan_cursor, "pending": pending, "started": started}


def _encode_file_cursor(source_id: str, source_offset: int, record_char_offset: int) -> str:
    payload = json.dumps(
        {
            "source_id": source_id,
            "source_offset": source_offset,
            "record_char_offset": record_char_offset,
        },
        separators=(",", ":"),
    ).encode()
    encoded = base64.urlsafe_b64encode(payload).decode().rstrip("=")
    return f"{_FILE_CURSOR_PREFIX}{encoded}"


def _decode_file_cursor(value: str, source_id: str) -> tuple[int, int]:
    if not value.startswith(_FILE_CURSOR_PREFIX):
        raise ValueError("Invalid filesystem continuation cursor.")
    encoded = value.removeprefix(_FILE_CURSOR_PREFIX)
    try:
        padding = "=" * (-len(encoded) % 4)
        raw = base64.b64decode(encoded + padding, altchars=b"-_", validate=True)
        payload = json.loads(raw)
    except (ValueError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError("Invalid filesystem continuation cursor.") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("source_id") != source_id
        or isinstance(payload.get("source_offset"), bool)
        or not isinstance(payload.get("source_offset"), int)
        or payload["source_offset"] < 0
        or isinstance(payload.get("record_char_offset"), bool)
        or not isinstance(payload.get("record_char_offset"), int)
        or payload["record_char_offset"] <= 0
    ):
        raise ValueError("Invalid filesystem continuation cursor.")
    return payload["source_offset"], payload["record_char_offset"]


async def _redis_history_page(
    client: Any,
    repo_slug: str,
    observed_at: datetime,
    *,
    cursor_state: dict[str, Any],
    limit: int,
) -> tuple[list[dict[str, Any]], list[str], str | None]:
    warnings: list[str] = []
    candidates = list(cursor_state["pending"])
    scan_cursor = int(cursor_state["scan_cursor"])
    started = bool(cursor_state["started"])
    completed = started and scan_cursor == 0
    scan_calls = 0
    malformed_keys = 0
    prefix = cli_log_history(repo_slug, "")

    try:
        while len(candidates) < limit and not completed and scan_calls < _MAX_REDIS_SCAN_CALLS:
            scan_cursor, raw_keys = await client.scan(
                cursor=scan_cursor,
                match=cli_log_history(repo_slug, "*"),
                count=max(10, limit),
            )
            scan_cursor = int(scan_cursor)
            started = True
            scan_calls += 1
            for raw_key in raw_keys:
                key = _decode(raw_key)
                if not key.startswith(prefix):
                    continue
                timestamp = key[len(prefix) :]
                if _parse_timestamp(timestamp) is None or "/" in timestamp:
                    malformed_keys += 1
                    continue
                if timestamp not in candidates:
                    candidates.append(timestamp)
            completed = scan_cursor == 0
    except Exception as exc:
        warnings.append(f"CLI history discovery incomplete: {_error_text(exc)}")
        completed = True

    if malformed_keys:
        warnings.append(
            f"Ignored {malformed_keys} malformed CLI history key(s) in the constrained repository namespace."
        )
    if len(candidates) > _MAX_REDIS_PENDING_KEYS:
        dropped = len(candidates) - _MAX_REDIS_PENDING_KEYS
        candidates = candidates[:_MAX_REDIS_PENDING_KEYS]
        warnings.append(
            f"Redis returned an oversized scan batch; {dropped} source identifier(s) were omitted from continuation."
        )

    selected = candidates[:limit]
    remaining = candidates[limit:]
    sources: list[dict[str, Any]] = []
    for timestamp in selected:
        key = cli_log_history(repo_slug, timestamp)
        try:
            value = await client.get(key)
            ttl = int(await client.ttl(key))
        except Exception as exc:
            sources.append(
                {
                    "source_id": f"{_CLI_HISTORY_SOURCE_PREFIX}{timestamp}",
                    "kind": "retained_cli_log",
                    "storage": "redis",
                    "availability": "unavailable",
                    "error": _error_text(exc),
                    "association": _association(),
                    "mutable": False,
                }
            )
            continue
        text = _decode(value) if value is not None else ""
        sources.append(
            {
                "source_id": f"{_CLI_HISTORY_SOURCE_PREFIX}{timestamp}",
                "kind": "retained_cli_log",
                "storage": "redis",
                "availability": "available" if value is not None else "missing_or_expired",
                "timestamps": {
                    "recorded_at": timestamp,
                    **_ttl_metadata(ttl, observed_at),
                },
                "size_chars": len(text),
                "retention": {
                    "producer_ttl_seconds": _CLI_HISTORY_TTL_SECONDS,
                    "truncated": text.startswith("[truncated]\n"),
                    "truncation_marker_preserved": True,
                },
                "association": _association(),
                "mutable": False,
                "ordering": "redis_scan",
            }
        )

    next_cursor = None
    if remaining or not completed:
        next_cursor = _encode_history_cursor(scan_cursor, remaining, started=started)
    return sources, warnings, next_cursor


def _file_log_sources(repo_slug: str) -> tuple[list[dict[str, Any]], list[str]]:
    sources: list[dict[str, Any]] = []
    warnings: list[str] = []
    try:
        event_dir = _safe_repo_directory(_resolve_events_dir(), repo_slug)
    except ValueError as exc:
        return sources, [_error_text(exc)]
    if event_dir.is_dir():
        try:
            with os.scandir(event_dir) as entries:
                candidates = list(islice(entries, _MAX_DISK_PARTITION_CANDIDATES + 1))
        except OSError as exc:
            candidates = []
            warnings.append(f"Could not discover disk event partitions: {_error_text(exc)}")
        if len(candidates) > _MAX_DISK_PARTITION_CANDIDATES:
            candidates.pop()
            warnings.append(
                "Disk event partition discovery reached its bounded candidate limit; "
                "additional partitions may be undiscovered but remain readable by an exact source ID."
            )
        for entry in sorted(candidates, key=lambda item: item.name, reverse=True):
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}\.jsonl", entry.name):
                continue
            try:
                resolved = _safe_path(event_dir, entry.name)
                stat = resolved.stat()
            except (OSError, ValueError) as exc:
                warnings.append(f"Could not inspect event partition {entry.name!r}: {_error_text(exc)}")
                continue
            date = Path(entry.name).stem
            sources.append(
                {
                    "source_id": f"events:disk/{date}",
                    "kind": "disk_event_log",
                    "storage": "filesystem",
                    "availability": "available",
                    "timestamps": {
                        "partition_date": date,
                        "modified_at": _iso_z(datetime.fromtimestamp(stat.st_mtime, timezone.utc)),
                        "expires_at": None,
                    },
                    "size_bytes": stat.st_size,
                    "retention": {"policy": "retained until file removal", "truncated": False},
                    "association": _association(),
                    "mutable": date == _utc_now().date().isoformat(),
                    "ordering": "oldest_first",
                }
            )

    try:
        repo_root = _safe_repo_directory(_REPOS_ROOT, repo_slug)
        ci_path = _safe_path(repo_root, "artifacts", "ci.log")
        ci_stat = ci_path.stat() if ci_path.is_file() else None
    except (OSError, ValueError) as exc:
        ci_stat = None
        warnings.append(f"Could not inspect artifacts/ci.log: {_error_text(exc)}")
    sources.append(
        {
            "source_id": "ci:artifact",
            "kind": "current_checkout_ci_artifact",
            "storage": "filesystem",
            "availability": "available" if ci_stat is not None else "missing",
            "timestamps": {
                "modified_at": (
                    _iso_z(datetime.fromtimestamp(ci_stat.st_mtime, timezone.utc)) if ci_stat is not None else None
                ),
                "expires_at": None,
            },
            "size_bytes": ci_stat.st_size if ci_stat is not None else 0,
            "retention": {
                "policy": "mutable checkout artifact; may be replaced by the next gate run",
                "truncated": False,
            },
            "association": _association(),
            "mutable": True,
        }
    )
    return sources, warnings


def _unretained_sources() -> list[dict[str, Any]]:
    return [
        {
            "source_id": "daemon:stdout",
            "kind": "daemon_stdout",
            "storage": "not_retained",
            "availability": "unavailable",
            "reason": "Daemon stdout is not persistently captured by the current deployment.",
            "association": _association(),
        },
        {
            "source_id": "cli:live",
            "kind": "live_cli_output",
            "storage": "not_retained",
            "availability": "unavailable",
            "reason": ("Live CLI output is not exposed; only completed retained CLI snapshots are available."),
            "association": _association(),
        },
    ]


@mcp.tool()
async def list_orchestrator_logs(
    repo_slug: str,
    cursor: int | str = 0,
    limit: int = 50,
) -> dict[str, Any]:
    """Discover retained log sources for one configured repository.

    Results are bounded and paginated. Each source reports timestamps,
    expiry/truncation behavior, mutability, and whether task/run/SHA identity
    was actually recorded by its producer.
    """
    _validate_configured_repo(repo_slug)
    cursor_kind, cursor_value = _decode_log_cursor(cursor)
    limit = _validate_limit(limit, maximum=_MAX_LOG_SOURCES, name="limit")
    observed_at = _utc_now()
    client: Any | None = None
    redis_warnings: list[str] = []
    file_warnings: list[str] = []
    known_static_total: int | None = None
    try:
        redis_startup_error: str | None = None
        try:
            client = _new_redis_client()
        except Exception as exc:
            redis_startup_error = _error_text(exc)
        if cursor_kind == "history":
            if client is None:
                page = [
                    {
                        "source_id": "redis:diagnostics",
                        "kind": "redis_diagnostic_sources",
                        "storage": "redis",
                        "availability": "unavailable",
                        "error": redis_startup_error,
                        "association": _association(),
                    }
                ]
                redis_warnings = [f"Redis diagnostic sources unavailable: {redis_startup_error}"]
                next_cursor = None
            else:
                page, redis_warnings, next_cursor = await _redis_history_page(
                    client,
                    repo_slug,
                    observed_at,
                    cursor_state=cursor_value,
                    limit=limit,
                )
            phase = "redis_history"
        else:
            if client is None:
                message = redis_startup_error
                redis_sources = [
                    {
                        "source_id": "redis:diagnostics",
                        "kind": "redis_diagnostic_sources",
                        "storage": "redis",
                        "availability": "unavailable",
                        "error": message,
                        "association": _association(),
                    }
                ]
                redis_warnings = [f"Redis diagnostic sources unavailable: {message}"]
            else:
                try:
                    redis_sources, redis_warnings = await _redis_log_sources(client, repo_slug, observed_at)
                except Exception as exc:
                    message = _error_text(exc)
                    redis_sources = [
                        {
                            "source_id": "redis:diagnostics",
                            "kind": "redis_diagnostic_sources",
                            "storage": "redis",
                            "availability": "unavailable",
                            "error": message,
                            "association": _association(),
                        }
                    ]
                    redis_warnings = [f"Redis diagnostic sources unavailable: {message}"]
            file_sources, file_warnings = _file_log_sources(repo_slug)
            sources = redis_sources + file_sources + _unretained_sources()
            sources.sort(
                key=lambda item: (
                    item["source_id"] not in {"cli:latest", "events:redis", "ci:artifact"},
                    item["source_id"],
                )
            )
            static_cursor = int(cursor_value)
            if static_cursor > len(sources):
                raise ValueError("Static source cursor is beyond the available source list.")
            known_static_total = len(sources)
            page = sources[static_cursor : static_cursor + limit]
            next_static = static_cursor + len(page)
            next_cursor = (
                next_static
                if next_static < len(sources)
                else _encode_history_cursor(0, [], started=False)
            )
            phase = "static"
    finally:
        await _close_redis(client)
    payload = {
        "observed_at": _iso_z(observed_at),
        "repo_slug": repo_slug,
        "sources": page,
        "warnings": redis_warnings + file_warnings,
        "pagination": {
            "phase": phase,
            "cursor": cursor,
            "limit": limit,
            "returned": len(page),
            "total": None,
            "known_static_total": known_static_total,
            "history_total": "unknown_without_full_scan",
            "next_cursor": next_cursor,
        },
        "gaps": [
            "Expired Redis CLI logs cannot be discovered after their keys disappear.",
            "Daemon stdout and live CLI streaming are not retained; persistent capture is follow-up work.",
        ],
    }
    safe, replacements = _redact_structure(payload)
    safe["redaction"] = {"applied": replacements > 0, "replacements": replacements}
    return safe


def _parse_cli_history_source(source_id: str) -> str | None:
    if not source_id.startswith(_CLI_HISTORY_SOURCE_PREFIX):
        return None
    timestamp = source_id[len(_CLI_HISTORY_SOURCE_PREFIX) :]
    if _parse_timestamp(timestamp) is None or "/" in timestamp:
        raise ValueError("Invalid CLI history source timestamp.")
    return timestamp


async def _read_redis_source(
    client: Any, repo_slug: str, source_id: str, observed_at: datetime
) -> tuple[str | None, dict[str, Any], list[str]]:
    warnings: list[str] = []
    if source_id == "cli:latest":
        key = cli_log_latest(repo_slug)
        raw = await client.get(key)
        ttl = int(await client.ttl(key))
        return (
            _decode(raw) if raw is not None else None,
            {
                "kind": "retained_cli_log",
                "storage": "redis",
                "availability": "available" if raw is not None else "missing_or_expired",
                "timestamps": {"recorded_at": None, **_ttl_metadata(ttl, observed_at)},
                "retention": {
                    "producer_ttl_seconds": _CLI_LATEST_TTL_SECONDS,
                    "truncation_marker_preserved": True,
                },
                "association": _association(),
                "mutable": True,
            },
            warnings,
        )
    timestamp = _parse_cli_history_source(source_id)
    if timestamp is not None:
        key = cli_log_history(repo_slug, timestamp)
        raw = await client.get(key)
        ttl = int(await client.ttl(key))
        return (
            _decode(raw) if raw is not None else None,
            {
                "kind": "retained_cli_log",
                "storage": "redis",
                "availability": "available" if raw is not None else "missing_or_expired",
                "timestamps": {"recorded_at": timestamp, **_ttl_metadata(ttl, observed_at)},
                "retention": {
                    "producer_ttl_seconds": _CLI_HISTORY_TTL_SECONDS,
                    "truncation_marker_preserved": True,
                },
                "association": _association(),
                "mutable": False,
            },
            warnings,
        )
    if source_id == "events:redis":
        raw_events, total, size_bytes, oversized = await _read_bounded_event_history(
            client,
            repo_slug,
            start=0,
            stop=-1,
        )
        if oversized:
            reason = "Redis event history exceeds the bounded diagnostic read limit."
            warnings.append(reason)
            return (
                None,
                {
                    "kind": "repository_event_history",
                    "storage": "redis",
                    "availability": "oversized",
                    "record_count": total,
                    "size_bytes_estimate": size_bytes,
                    "read_bound_bytes": _MAX_REDIS_EVENT_HISTORY_BYTES,
                    "retention": {
                        "entry_cap": EVENT_HISTORY_LIMIT,
                        "possibly_truncated": total >= EVENT_HISTORY_LIMIT,
                    },
                    "association": _association(),
                    "mutable": True,
                    "ordering": "newest_first",
                    "reason": reason,
                },
                warnings,
            )
        malformed = sum(_bounded_event(item)["status"] == "malformed" for item in raw_events)
        if malformed:
            warnings.append(f"Source contains {malformed} malformed event record(s).")
        content = "\n".join(_decode(item) for item in raw_events)
        return (
            content,
            {
                "kind": "repository_event_history",
                "storage": "redis",
                "availability": "available" if raw_events else "empty",
                "record_count": total,
                "size_bytes_estimate": size_bytes,
                "read_bound_bytes": _MAX_REDIS_EVENT_HISTORY_BYTES,
                "malformed_records": malformed,
                "retention": {
                    "entry_cap": EVENT_HISTORY_LIMIT,
                    "possibly_truncated": total >= EVENT_HISTORY_LIMIT,
                },
                "association": _association(),
                "mutable": True,
                "ordering": "newest_first",
            },
            warnings,
        )
    raise ValueError(f"Unknown Redis diagnostic source: {source_id!r}")


def _file_source_metadata(
    *,
    kind: str,
    mutable: bool,
    association: dict[str, Any],
    stat: Any,
    malformed: int | None,
) -> dict[str, Any]:
    return {
        "kind": kind,
        "storage": "filesystem",
        "availability": "available",
        "timestamps": {
            "modified_at": _iso_z(datetime.fromtimestamp(stat.st_mtime, timezone.utc)),
            "expires_at": None,
        },
        "size_bytes": stat.st_size,
        "malformed_records_in_page": malformed,
        "retention": {
            "policy": (
                "mutable checkout artifact; may be replaced by the next gate run"
                if kind == "current_checkout_ci_artifact"
                else "retained until file removal"
            ),
            "truncated": False,
        },
        "association": association,
        "mutable": mutable,
        "ordering": "oldest_first" if kind == "disk_event_log" else None,
        "page_window_bound_bytes": _MAX_FILE_SCAN_BYTES,
        "private_key_context": (
            "older chunks may be inspected up to the context bound; "
            "content is omitted fail-closed if state remains unknown"
        ),
        "private_key_context_bound_bytes": _MAX_PRIVATE_KEY_CONTEXT_BYTES,
        "sensitive_assignment_context": (
            "preceding context may be inspected to preserve redaction when a credential scalar "
            "straddles a page; content is omitted fail-closed if that state remains unknown"
        ),
        "sensitive_assignment_context_bound_bytes": _MAX_SENSITIVE_ASSIGNMENT_CONTEXT_BYTES,
    }


def _read_file_source(
    repo_slug: str,
    source_id: str,
    *,
    cursor: int,
    record_char_offset: int,
    cursor_token: int | str,
    max_chars: int,
    tail: bool,
) -> tuple[str | None, dict[str, Any], list[str], dict[str, Any] | None, int]:
    warnings: list[str] = []
    input_record_char_offset = record_char_offset
    if source_id == "ci:artifact":
        repo_root = _safe_repo_directory(_REPOS_ROOT, repo_slug)
        path = _safe_path(repo_root, "artifacts", "ci.log")
        kind = "current_checkout_ci_artifact"
        mutable = True
        association = _association()
    else:
        match = _DISK_EVENT_SOURCE.fullmatch(source_id)
        if match is None:
            raise ValueError(f"Unknown filesystem diagnostic source: {source_id!r}")
        date = match.group(1)
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("Invalid disk event partition date.") from exc
        repo_event_root = _safe_repo_directory(_resolve_events_dir(), repo_slug)
        path = _safe_path(repo_event_root, f"{date}.jsonl")
        kind = "disk_event_log"
        mutable = date == _utc_now().date().isoformat()
        association = _association()
    if not path.is_file():
        return (
            None,
            {
                "kind": kind,
                "storage": "filesystem",
                "availability": "missing",
                "association": association,
                "mutable": mutable,
            },
            warnings,
            None,
            0,
        )
    try:
        stat = path.stat()
        handle = path.open("rb")
    except OSError as exc:
        return (
            None,
            {
                "kind": kind,
                "storage": "filesystem",
                "availability": "unavailable",
                "error": _error_text(exc),
                "association": association,
                "mutable": mutable,
            },
            warnings,
            None,
            0,
        )
    with handle:
        if tail:
            raw_start = max(0, stat.st_size - _MAX_FILE_SCAN_BYTES)
            handle.seek(raw_start)
            raw = handle.read(_MAX_FILE_SCAN_BYTES)
            scanned_bytes = len(raw)
            page_start = raw_start
            if raw_start > 0:
                newline = raw.find(b"\n")
                if newline < 0:
                    warnings.append(
                        "Tail window intersects a source line larger than the bounded scan window; content was omitted."
                    )
                    raw = b""
                else:
                    page_start += newline + 1
                    raw = raw[newline + 1 :]
            starts_inside_private_key, context_scanned_bytes = _private_key_state_before(handle, page_start)
            (
                starts_with_sensitive_value,
                starts_inside_sensitive_block,
                sensitive_block_indent,
                starts_inside_sensitive_quote,
                sensitive_quote,
                assignment_context_scanned_bytes,
            ) = _sensitive_state_before(handle, page_start, raw)
            text = raw.decode("utf-8", errors="replace")
            redaction_units = _redacted_file_units(
                raw,
                starts_inside_private_key=starts_inside_private_key,
                starts_with_sensitive_value=starts_with_sensitive_value,
                starts_inside_sensitive_block=starts_inside_sensitive_block,
                sensitive_block_indent=sensitive_block_indent,
                starts_inside_sensitive_quote=starts_inside_sensitive_quote,
                sensitive_quote=sensitive_quote,
                has_more_after_raw=False,
                warnings=warnings,
            )
            redacted = "".join(unit[1] for unit in redaction_units)
            replacements = sum(unit[2] for unit in redaction_units)
            content = redacted[-max_chars:]
            malformed = (
                sum(_bounded_event(line)["status"] == "malformed" for line in text.splitlines() if line)
                if kind == "disk_event_log"
                else None
            )
            pagination = {
                "cursor": page_start,
                "requested_cursor": cursor_token,
                "cursor_unit": "source_byte",
                "max_chars": max_chars,
                "returned_chars": len(content),
                "source_size_bytes": stat.st_size,
                "scanned_bytes": scanned_bytes,
                "context_scanned_bytes": context_scanned_bytes + assignment_context_scanned_bytes,
                "private_key_context_scanned_bytes": context_scanned_bytes,
                "sensitive_assignment_context_scanned_bytes": assignment_context_scanned_bytes,
                "scan_limit_bytes": _MAX_FILE_SCAN_BYTES,
                "previous_cursor": (max(0, page_start - _MAX_FILE_SCAN_BYTES) if page_start > 0 else None),
                "next_cursor": None,
                "has_more": False,
                "has_older": page_start > 0,
                "tail": True,
            }
        else:
            requested_cursor = min(cursor, stat.st_size)
            page_start = requested_cursor
            if page_start > 0:
                handle.seek(page_start - 1)
                previous = handle.read(1)
            else:
                previous = b"\n"
            handle.seek(page_start)
            raw = handle.read(_MAX_FILE_SCAN_BYTES)
            scanned_bytes = len(raw)
            if previous != b"\n" and raw:
                newline = raw.find(b"\n")
                if newline < 0:
                    next_cursor = page_start + len(raw) if page_start + len(raw) < stat.st_size else None
                    warning = (
                        "Cursor intersects a source line larger than the bounded scan window; this segment was omitted."
                    )
                    warnings.append(warning)
                    source = _file_source_metadata(
                        kind=kind,
                        mutable=mutable,
                        association=association,
                        stat=stat,
                        malformed=0 if kind == "disk_event_log" else None,
                    )
                    marker = f"[source segment omitted: {_MAX_FILE_SCAN_BYTES} byte scan bound]\n"[:max_chars]
                    return (
                        marker,
                        source,
                        warnings,
                        {
                            "cursor": page_start,
                            "requested_cursor": cursor_token,
                            "cursor_unit": "source_byte",
                            "max_chars": max_chars,
                            "returned_chars": len(marker),
                            "source_size_bytes": stat.st_size,
                            "scanned_bytes": scanned_bytes,
                            "scan_limit_bytes": _MAX_FILE_SCAN_BYTES,
                            "previous_cursor": max(0, page_start - _MAX_FILE_SCAN_BYTES),
                            "next_cursor": next_cursor,
                            "has_more": next_cursor is not None,
                            "tail": False,
                        },
                        0,
                    )
                page_start += newline + 1
                raw = raw[newline + 1 :]

            raw_end = page_start + len(raw)
            if raw_end < stat.st_size and raw and not raw.endswith(b"\n"):
                newline = raw.rfind(b"\n")
                if newline < 0:
                    warnings.append("Source line exceeds the bounded scan window; this segment was omitted.")
                    next_cursor = page_start + len(raw)
                    source = _file_source_metadata(
                        kind=kind,
                        mutable=mutable,
                        association=association,
                        stat=stat,
                        malformed=0 if kind == "disk_event_log" else None,
                    )
                    marker = f"[source segment omitted: {_MAX_FILE_SCAN_BYTES} byte scan bound]\n"[:max_chars]
                    return (
                        marker,
                        source,
                        warnings,
                        {
                            "cursor": page_start,
                            "requested_cursor": cursor_token,
                            "cursor_unit": "source_byte",
                            "max_chars": max_chars,
                            "returned_chars": len(marker),
                            "source_size_bytes": stat.st_size,
                            "scanned_bytes": scanned_bytes,
                            "scan_limit_bytes": _MAX_FILE_SCAN_BYTES,
                            "previous_cursor": (max(0, page_start - _MAX_FILE_SCAN_BYTES) if page_start > 0 else None),
                            "next_cursor": next_cursor,
                            "has_more": True,
                            "tail": False,
                        },
                        0,
                    )
                raw = raw[: newline + 1]

            has_more_after_raw = page_start + len(raw) < stat.st_size
            starts_inside_private_key, context_scanned_bytes = _private_key_state_before(handle, page_start)
            (
                starts_with_sensitive_value,
                starts_inside_sensitive_block,
                sensitive_block_indent,
                starts_inside_sensitive_quote,
                sensitive_quote,
                assignment_context_scanned_bytes,
            ) = _sensitive_state_before(handle, page_start, raw)
            redaction_units = _redacted_file_units(
                raw,
                starts_inside_private_key=starts_inside_private_key,
                starts_with_sensitive_value=starts_with_sensitive_value,
                starts_inside_sensitive_block=starts_inside_sensitive_block,
                sensitive_block_indent=sensitive_block_indent,
                starts_inside_sensitive_quote=starts_inside_sensitive_quote,
                sensitive_quote=sensitive_quote,
                has_more_after_raw=has_more_after_raw,
                warnings=warnings,
            )

            parts: list[str] = []
            returned_chars = 0
            consumed_bytes = 0
            replacements = 0
            malformed = 0
            continuation_cursor: str | None = None
            for raw_unit, safe_unit, unit_replacements in redaction_units:
                unit_char_offset = record_char_offset if not parts and consumed_bytes == 0 else 0
                if unit_char_offset >= len(safe_unit):
                    raise ValueError("Filesystem continuation cursor no longer matches the source record.")
                remaining_unit = safe_unit[unit_char_offset:]
                available_chars = max_chars - returned_chars
                if parts and len(remaining_unit) > available_chars:
                    break
                if len(remaining_unit) > available_chars:
                    parts.append(remaining_unit[:available_chars])
                    returned_chars += available_chars
                    replacements += unit_replacements
                    continuation_cursor = _encode_file_cursor(
                        source_id,
                        page_start + consumed_bytes,
                        unit_char_offset + available_chars,
                    )
                    warnings.append(
                        "One source record exceeded max_chars; use the opaque next_cursor to continue it."
                    )
                    if kind == "disk_event_log":
                        malformed += sum(
                            _bounded_event(line)["status"] == "malformed"
                            for line in raw_unit.decode("utf-8", errors="replace").splitlines()
                            if line
                        )
                    break
                parts.append(remaining_unit)
                returned_chars += len(remaining_unit)
                consumed_bytes += len(raw_unit)
                replacements += unit_replacements
                record_char_offset = 0
                if kind == "disk_event_log":
                    malformed += sum(
                        _bounded_event(line)["status"] == "malformed"
                        for line in raw_unit.decode("utf-8", errors="replace").splitlines()
                        if line
                    )
                if returned_chars >= max_chars:
                    break
            content = "".join(parts)
            next_position = page_start + consumed_bytes
            next_cursor: int | str | None = continuation_cursor
            if next_cursor is None:
                next_cursor = next_position if next_position < stat.st_size else None
            pagination = {
                "cursor": page_start,
                "requested_cursor": cursor_token,
                "cursor_unit": "source_byte",
                "continuation_cursor_unit": "opaque_redacted_record_character",
                "source_byte_cursor": page_start,
                "record_character_offset": input_record_char_offset,
                "max_chars": max_chars,
                "returned_chars": len(content),
                "source_size_bytes": stat.st_size,
                "scanned_bytes": scanned_bytes,
                "context_scanned_bytes": context_scanned_bytes + assignment_context_scanned_bytes,
                "private_key_context_scanned_bytes": context_scanned_bytes,
                "sensitive_assignment_context_scanned_bytes": assignment_context_scanned_bytes,
                "scan_limit_bytes": _MAX_FILE_SCAN_BYTES,
                "previous_cursor": (max(0, page_start - _MAX_FILE_SCAN_BYTES) if page_start > 0 else None),
                "next_cursor": next_cursor,
                "has_more": next_cursor is not None,
                "tail": False,
            }

    if malformed:
        warnings.append(f"Page contains {malformed} malformed event record(s).")
    source = _file_source_metadata(
        kind=kind,
        mutable=mutable,
        association=association,
        stat=stat,
        malformed=malformed,
    )
    source["source_truncated"] = (
        cursor == 0 and input_record_char_offset == 0 and content.startswith("[truncated]\n")
    )
    return content, source, warnings, pagination, replacements


def _page_content(content: str, *, cursor: int, max_chars: int, tail: bool) -> tuple[str, dict[str, Any]]:
    total = len(content)
    start = max(0, total - max_chars) if tail else min(cursor, total)
    end = min(total, start + max_chars)
    page = content[start:end]
    return page, {
        "cursor": start,
        "requested_cursor": cursor,
        "cursor_unit": "redacted_character",
        "max_chars": max_chars,
        "returned_chars": len(page),
        "total_chars_after_redaction": total,
        "previous_cursor": max(0, start - max_chars) if start > 0 else None,
        "next_cursor": end if end < total else None,
        "has_more": end < total,
        "tail": tail,
    }


@mcp.tool()
async def read_orchestrator_log(
    repo_slug: str,
    source_id: str,
    cursor: int | str = 0,
    max_chars: int = 8_000,
    tail: bool = False,
) -> dict[str, Any]:
    """Read one discovered diagnostic source with bounded pagination.

    Redis cursors are character offsets in redacted content. Filesystem cursors
    are source-byte offsets or opaque record continuations returned by the
    preceding page. Set ``tail`` to read the final bounded window (useful for
    failure excerpts); tail mode requires the default cursor. Credential-like
    and Authorization values are redacted before return, while existing producer
    truncation markers are kept.
    """
    _validate_configured_repo(repo_slug)
    max_chars = _validate_limit(max_chars, maximum=_MAX_READ_CHARS, name="max_chars")
    if tail and cursor != 0:
        raise ValueError("cursor must be 0 when tail is true")
    observed_at = _utc_now()
    if source_id in {"daemon:stdout", "cli:live"}:
        reason = (
            "Daemon stdout is not persistently captured by the current deployment."
            if source_id == "daemon:stdout"
            else "Live CLI output is unavailable; use a retained cli:* source after the run completes."
        )
        return {
            "observed_at": _iso_z(observed_at),
            "repo_slug": repo_slug,
            "source_id": source_id,
            "source": {"availability": "unavailable", "reason": reason},
            "content": "",
            "pagination": None,
            "warnings": [reason],
            "redaction": {"applied": False, "replacements": 0},
        }

    content: str | None
    source: dict[str, Any]
    warnings: list[str]
    file_pagination: dict[str, Any] | None = None
    file_replacements = 0
    is_redis_source = source_id in {"cli:latest", "events:redis"} or source_id.startswith(
        _CLI_HISTORY_SOURCE_PREFIX
    )
    if is_redis_source:
        if not isinstance(cursor, int) or isinstance(cursor, bool):
            raise ValueError("Redis source cursor must be a non-negative integer.")
        redis_cursor = _validate_cursor(cursor)
        client: Any | None = None
        try:
            client = _new_redis_client()
            content, source, warnings = await _read_redis_source(client, repo_slug, source_id, observed_at)
        except ValueError:
            raise
        except Exception as exc:
            content = None
            source = {
                "availability": "unavailable",
                "storage": "redis",
                "error": _error_text(exc),
                "association": _association(),
            }
            warnings = [f"Redis source unavailable: {_error_text(exc)}"]
        finally:
            await _close_redis(client)
    else:
        if isinstance(cursor, str):
            file_cursor, record_char_offset = _decode_file_cursor(cursor, source_id)
        else:
            file_cursor = _validate_cursor(cursor)
            record_char_offset = 0
        content, source, warnings, file_pagination, file_replacements = _read_file_source(
            repo_slug,
            source_id,
            cursor=file_cursor,
            record_char_offset=record_char_offset,
            cursor_token=cursor,
            max_chars=max_chars,
            tail=tail,
        )

    if content is None:
        payload = {
            "observed_at": _iso_z(observed_at),
            "repo_slug": repo_slug,
            "source_id": source_id,
            "source": source,
            "content": "",
            "pagination": None,
            "warnings": warnings,
        }
        safe, replacements = _redact_structure(payload)
        safe["redaction"] = {"applied": replacements > 0, "replacements": replacements}
        return safe

    if file_pagination is not None:
        return {
            "observed_at": _iso_z(observed_at),
            "repo_slug": repo_slug,
            "source_id": source_id,
            "source": source,
            "content": content,
            "pagination": file_pagination,
            "warnings": warnings,
            "redaction": {
                "applied": file_replacements > 0,
                "replacements": file_replacements,
            },
        }

    redacted, replacements, redaction_warnings = _redact_log_content(content)
    warnings.extend(redaction_warnings)
    page, pagination = _page_content(redacted, cursor=redis_cursor, max_chars=max_chars, tail=tail)
    source["source_truncated"] = content.startswith("[truncated]\n")
    return {
        "observed_at": _iso_z(observed_at),
        "repo_slug": repo_slug,
        "source_id": source_id,
        "source": source,
        "content": page,
        "pagination": pagination,
        "warnings": warnings,
        "redaction": {"applied": replacements > 0, "replacements": replacements},
    }
