"""Read-only runtime diagnostics for the orchestrator MCP service.

The tools in this module deliberately read the producer-owned Redis keys and
files directly.  In particular, they do not use helpers that clean stale
indexes, refresh TTLs, synthesize healthy state, or otherwise mutate runtime
data while answering a diagnostic query.
"""

from __future__ import annotations

import base64
import binascii
import json
import math
import os
import re
import stat as stat_module
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from itertools import islice
from pathlib import Path
from typing import Any

import redis.asyncio as aioredis
import yaml

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
_MAX_REDIS_CLI_LOG_BYTES = 64 * 1024
_MAX_REDIS_STATE_BYTES = 1024 * 1024
_MAX_REDIS_RUN_RECORD_BYTES = 64 * 1024
_MAX_REDIS_RETRY_COMMAND_BYTES = 64 * 1024
_MAX_RETRY_INDEX_MEMBER_BYTES = 512
_MAX_RETRY_CURSOR_LOOKUP = 200
_MAX_RUN_INDEX_ENTRIES = 200
_MAX_RUN_INDEX_MEMBER_BYTES = 512
_MAX_FILE_SCAN_BYTES = 256 * 1024
_MAX_PRIVATE_KEY_CONTEXT_BYTES = 1024 * 1024
_MAX_SENSITIVE_ASSIGNMENT_CONTEXT_BYTES = 1024 * 1024
_MAX_REDIS_SCAN_CALLS = 4
_MAX_REDIS_PENDING_KEYS = 200
_MAX_REDIS_HISTORY_KEY_BYTES = 512
_MAX_DISK_PARTITION_CANDIDATES = 200
_MAX_EMBEDDED_JSON_CANDIDATES = 64
_MAX_STRUCTURED_DEPTH = 64
_MAX_YAML_FLOW_DEPTH = 64
_MAX_YAML_FLOW_TOKENS = 4_096
_MAX_YAML_BLOCK_MAPPING_LINES = 4_096
_MAX_YAML_BLOCK_TOKENS = 4_096
_MAX_YAML_PER_LINE_SCAN_CANDIDATES = 256
_MAX_REDACTION_PHYSICAL_LINES = 8_192
_MAX_RETRY_CURSOR_CHARS = 1_024
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
_BOUNDED_PENDING_RETRIES_SCRIPT = """
local total = redis.call('ZCARD', KEYS[1])
local first = 0
if ARGV[1] ~= '' then
  local cursor_score = tonumber(ARGV[1])
  local cursor_member = ARGV[2]
  local cursor_digest = ARGV[3]
  local cursor_index = ARGV[4]
  local low = 0
  local high = total
  while low < high do
    local middle = math.floor((low + high) / 2)
    local row = redis.call('ZRANGE', KEYS[1], middle, middle, 'WITHSCORES')
    if #row == 0 then
      high = middle
    else
      local score = tonumber(row[2])
      if score < cursor_score or (
        cursor_member ~= '' and score == cursor_score and row[1] <= cursor_member
      ) then
        low = middle + 1
      else
        high = middle
      end
    end
  end
  first = low
  if cursor_digest ~= '' then
    local found = false
    local lookup_limit = tonumber(ARGV[7])
    local search_start = first
    local search_end = math.min(total - 1, first + lookup_limit - 1)
    if cursor_index ~= '' then
      local center = tonumber(cursor_index)
      search_start = math.max(0, center - math.floor(lookup_limit / 2))
      search_end = math.min(total - 1, search_start + lookup_limit - 1)
      search_start = math.max(0, search_end - lookup_limit + 1)
    end
    for position = search_start, search_end do
      local row = redis.call('ZRANGE', KEYS[1], position, position, 'WITHSCORES')
      if #row > 0 and tonumber(row[2]) == cursor_score and redis.sha1hex(row[1]) == cursor_digest then
        first = position + 1
        found = true
        break
      end
    end
    if not found then
      return {total, -1, {}}
    end
  end
end
local raw = redis.call('ZRANGE', KEYS[1], first, first + tonumber(ARGV[5]) - 1, 'WITHSCORES')
local maximum = tonumber(ARGV[6])
local rows = {}
for offset = 1, #raw, 2 do
  local member = raw[offset]
  local size = string.len(member)
  local oversized = size > maximum
  table.insert(rows, oversized and '' or member)
  table.insert(rows, raw[offset + 1])
  table.insert(rows, first + math.floor((offset - 1) / 2))
  table.insert(rows, size)
  table.insert(rows, oversized and redis.sha1hex(member) or '')
end
return {total, first, rows}
"""
_BOUNDED_RUN_INDEX_SCRIPT = """
local total = redis.call('LLEN', KEYS[1])
local count = math.min(total, tonumber(ARGV[1]))
local maximum = tonumber(ARGV[2])
local rows = {}
for index = 0, count - 1 do
  local member = redis.call('LINDEX', KEYS[1], index)
  local size = string.len(member)
  table.insert(rows, index)
  table.insert(rows, size)
  table.insert(rows, size <= maximum and member or '')
end
return {total, rows}
"""
_BOUNDED_HISTORY_SCAN_SCRIPT = """
local result = redis.call('SCAN', ARGV[1], 'MATCH', ARGV[2], 'COUNT', ARGV[3])
local maximum_size = tonumber(ARGV[4])
local maximum_count = tonumber(ARGV[5])
local keys = {}
local oversized = 0
local dropped = 0
for _, key in ipairs(result[2]) do
  if string.len(key) > maximum_size then
    oversized = oversized + 1
  elseif #keys >= maximum_count then
    dropped = dropped + 1
  else
    table.insert(keys, key)
  end
end
return {result[1], keys, oversized, dropped}
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
    "shared_access_signature",
    "shared-access-signature",
    "sharedAccessSignature",
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
    "pwd",
    "sig",
    "signature",
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
    "_auth",
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
    r"\s*(?:\+=|[:=])[ \t]*(?:[|>][-+]?)?[ \t]*$"
)
_PENDING_YAML_SENSITIVE_ASSIGNMENT = re.compile(
    rf"(?i)^(?P<indent>[ \t]*)(?:-[ \t]+)?(?:[\"'](?:{_SENSITIVE_KEY_PATTERN})[\"']|"
    rf"(?:{_SENSITIVE_KEY_PATTERN}))\s*:[ \t]*$"
)
_PENDING_JSON_SENSITIVE_ASSIGNMENT = re.compile(
    rf'(?i)^[ \t]*"(?:{_SENSITIVE_KEY_PATTERN})"\s*:\s*$'
)
_YAML_KIND_ASSIGNMENT = re.compile(
    r"(?i)^(?P<indent>[ \t]*)(?P<dash>-[ \t]+)?(?:[\"']kind[\"']|kind)[ \t]*:[ \t]*"
    r"(?P<kind>[^#\r\n]*?)[ \t]*(?:#.*)?$"
)
_YAML_SECRET_PAYLOAD_ASSIGNMENT = re.compile(
    r"(?i)^(?P<indent>[ \t]*)(?:-[ \t]+)?"
    r"(?:[\"'](?:data|stringData)[\"']|(?:data|stringData))[ \t]*:[ \t]*"
    r"(?P<value>[^\r\n]*)$"
)
_YAML_DOCUMENT_BOUNDARY = re.compile(r"^(?:---|\.\.\.)[ \t]*(?:#.*)?$")
_YAML_MAPPING_ENTRY = re.compile(
    r"^[ \t]*(?:-[ \t]+)?(?:[\"']?[-A-Za-z0-9_.]+[\"']?)[ \t]*:"
)
_YAML_ENV_NAME = re.compile(
    r"(?i)^(?P<indent>[ \t]*)(?P<dash>-[ \t]+)?(?:[\"']name[\"']|name)[ \t]*:[ \t]*"
    r"(?P<name>[^#\r\n]*?)[ \t]*(?:#.*)?$"
)
_YAML_ENV_VALUE = re.compile(
    r"(?i)^(?P<indent>[ \t]*)(?P<dash>-[ \t]+)?(?:[\"']value[\"']|value)[ \t]*:[ \t]*"
    r"(?P<value>[^\r\n]*)$"
)
_YAML_SEQUENCE_ITEM_ONLY = re.compile(r"^(?P<indent>[ \t]*)-[ \t]*(?:#.*)?$")
_YAML_BLOCK_SEQUENCE_ITEM = re.compile(r"^[ \t]*-(?:[ \t]+|$)")
_YAML_BLOCK_MAPPING_LINE = re.compile(
    r"^[ \t]*(?:-[ \t]+)?(?:[\"']?[-A-Za-z0-9_.]+[\"']?)[ \t]*:(?:[ \t]|$)"
)
_YAML_EXPLICIT_SENSITIVE_KEY = re.compile(
    rf"(?i)^(?P<indent>[ \t]*)(?:-[ \t]+)?\?[ \t]+"
    rf"(?:[\"'](?:{_SENSITIVE_KEY_PATTERN})[\"']|(?:{_SENSITIVE_KEY_PATTERN}))"
    r"[ \t]*(?:#.*)?$"
)
_YAML_EXPLICIT_VALUE = re.compile(
    r"^(?P<indent>[ \t]*)(?:-[ \t]+)?\:[ \t]*(?P<value>[^\r\n]*)$"
)
_YAML_NODE_PREFIX = re.compile(r"^(?:&[^\s]+|![^\s]+)(?:\s+|$)")
_YAML_ALIAS_SCALAR = re.compile(r"^\*(?P<anchor>[^\s,\[\]{}]+)$")
_YAML_FLOW_KIND_SECRET = re.compile(
    r"(?i)(?:[{,][ \t]*)(?:[\"']?kind[\"']?)[ \t]*:[ \t]*"
    r"(?:[\"']?secret[\"']?)(?=[ \t]*[,}])"
)
_YAML_FLOW_SECRET_PAYLOAD = re.compile(
    r"(?i)(?:[{,][ \t]*)(?:[\"']?(?:data|stringData)[\"']?)[ \t]*:"
)
_HIGH_LINE_CONTEXTUAL_YAML = re.compile(
    r"(?i)(?:[\"']?(?:name|kind|data|stringData)[\"']?)[ \t]*:"
)
_HIGH_LINE_COMPLEX_SENSITIVE_ASSIGNMENT = re.compile(
    rf"(?im)(?:[\"'](?:{_SENSITIVE_KEY_PATTERN})[\"']|"
    rf"(?<![A-Za-z0-9_.-])(?:{_SENSITIVE_KEY_PATTERN})(?![A-Za-z0-9_.-]))"
    r"[ \t]*(?:\+=|[:=])[ \t]*(?:$|[|>]|\"|')"
)
_HIGH_LINE_EXPLICIT_YAML = re.compile(r"(?m)^[ \t]*(?:-[ \t]+)?[?:](?:[ \t]|$)")
_HIGH_LINE_ESCAPED_MAPPING_KEY = re.compile(r"(?m)^[^\r\n:]*\\[^\r\n:]*:")
_HIGH_LINE_ALIASED_MAPPING_KEY = re.compile(r"(?m)(?<![A-Za-z0-9_.-])\*[^\s,\[\]{}:]+[ \t]*:")
_BLOCK_SENSITIVE_ASSIGNMENT = re.compile(
    rf"(?i)^(?P<indent>[ \t]*)(?:-[ \t]+)?(?:[\"'](?:{_SENSITIVE_KEY_PATTERN})[\"']|"
    rf"(?:{_SENSITIVE_KEY_PATTERN}))\s*(?:\+=|[:=])[ \t]*"
    r"[|>](?:[1-9][-+]?|[-+][1-9]?|)[ \t]*(?:#.*)?$"
)
_QUOTED_SENSITIVE_ASSIGNMENT = re.compile(
    rf"(?i)(?:[\"'](?:{_SENSITIVE_KEY_PATTERN})[\"']|"
    rf"(?<![A-Za-z0-9_.-])(?:{_SENSITIVE_KEY_PATTERN})(?![A-Za-z0-9_.-]))"
    r"\s*(?:\+=|[:=])[ \t]*(?P<quote>\"\"\"|'''|[\"'])(?P<value>.*)$"
)
_PLAIN_SENSITIVE_ASSIGNMENT = re.compile(
    rf"(?i)^(?P<indent>[ \t]*)(?:-[ \t]+)?(?:[\"'](?:{_SENSITIVE_KEY_PATTERN})[\"']|"
    rf"(?:{_SENSITIVE_KEY_PATTERN}))\s*(?:\+=|[:=])[ \t]*(?P<value>(?![\"'|>])\S.*)$"
)
_PREFIXED_PLAIN_SENSITIVE_ASSIGNMENT = re.compile(
    rf"(?i)(?:[\"'](?:{_SENSITIVE_KEY_PATTERN})[\"']|"
    rf"(?<![A-Za-z0-9_.-])(?:{_SENSITIVE_KEY_PATTERN})(?![A-Za-z0-9_.-]))"
    r"\s*(?:\+=|[:=])[ \t]*(?P<value>(?![\"'|>])\S.*)$"
)
_FISH_SENSITIVE_ASSIGNMENT = re.compile(
    rf"(?im)^(?P<indent>[ \t]*)(?P<trace>\+[ \t]+)?(?P<prefix>set"
    rf"(?:[ \t]+(?:--|--?[A-Za-z-]+))*[ \t]+(?:{_SENSITIVE_KEY_PATTERN})[ \t]+)"
    r"(?P<value>\S.*)$"
)
_DOCKER_ENV_SENSITIVE_ASSIGNMENT = re.compile(
    rf"(?im)^(?P<indent>[ \t]*)(?P<prefix>ENV[ \t]+(?:{_SENSITIVE_KEY_PATTERN})[ \t]+)"
    r"(?P<value>\S.*)$"
)
_REDACTION_RULES = (
    (
        _FISH_SENSITIVE_ASSIGNMENT,
        r"\g<indent>\g<trace>\g<prefix>[REDACTED]",
    ),
    (
        _DOCKER_ENV_SENSITIVE_ASSIGNMENT,
        r"\g<indent>\g<prefix>[REDACTED]",
    ),
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
            r"(?![A-Za-z0-9_-])\s*(?:\+=|[:=])\s*)"
            r"(?P<assignment_quote>\"\"\"|'''|[\"'])"
            r"(?:\\[^\r\n]|(?!(?P=assignment_quote))[^\\\r\n])*"
            r"(?P=assignment_quote)?"
        ),
        r"\1[REDACTED]",
    ),
    (
        re.compile(
            rf"(?im)((?<![A-Za-z0-9_-])(?:{_SENSITIVE_KEY_PATTERN})"
            r"(?![A-Za-z0-9_-])\s*(?:\+=|[:=])"
            r"(?![ \t]*\[REDACTED\])[ \t]*)"
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


def _retry_cursor(
    score: float,
    member: str,
    *,
    member_sha1: str | None = None,
    member_index: int | None = None,
) -> str:
    if member_sha1 is not None:
        identity = {"member_sha1": member_sha1, "member_index": member_index}
    else:
        identity = {"member": member}
        if member_index is not None:
            identity["member_index"] = member_index
    payload = json.dumps(
        {"score": score, **identity},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def _validate_retry_cursor(value: str | None) -> tuple[float, str, str, int | None] | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value or len(value) > _MAX_RETRY_CURSOR_CHARS:
        raise ValueError("retry_cursor must be a nonempty bounded cursor string")
    try:
        padding = "=" * (-len(value) % 4)
        decoded = json.loads(base64.b64decode(value + padding, altchars=b"-_", validate=True))
        score = float(decoded["score"])
        member = decoded.get("member", "")
        member_sha1 = decoded.get("member_sha1", "")
        member_index = decoded.get("member_index")
    except (AttributeError, binascii.Error, KeyError, TypeError, ValueError) as exc:
        raise ValueError("retry_cursor is malformed") from exc
    if (
        not math.isfinite(score)
        or not isinstance(member, str)
        or not isinstance(member_sha1, str)
        or bool(member) == bool(member_sha1)
        or len(member) > _MAX_RETRY_CURSOR_CHARS
        or (member_sha1 and re.fullmatch(r"[0-9a-f]{40}", member_sha1) is None)
        or (member and member_index is not None)
        or (
            member_sha1
            and member_index is not None
            and (
                isinstance(member_index, bool)
                or not isinstance(member_index, int)
                or member_index < 0
            )
        )
    ):
        raise ValueError("retry_cursor is malformed")
    return score, member, member_sha1, member_index


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


def _redact_high_line_plain_text(
    text: str,
    *,
    starts_inside_private_key: bool,
) -> tuple[str, int] | None:
    """Fast-path dense ordinary logs without attempting per-line YAML analysis."""
    if starts_inside_private_key and _PRIVATE_KEY_END.search(text.encode()) is None:
        trailing_newline = "\n" if text.endswith(("\n", "\r")) else ""
        return f"[REDACTED PRIVATE KEY]{trailing_newline}", 1
    if (
        "\\\n" in text
        or "\\\r\n" in text
        or _HIGH_LINE_CONTEXTUAL_YAML.search(text) is not None
        or _HIGH_LINE_COMPLEX_SENSITIVE_ASSIGNMENT.search(text) is not None
        or _HIGH_LINE_EXPLICIT_YAML.search(text) is not None
        or _HIGH_LINE_ESCAPED_MAPPING_KEY.search(text) is not None
        or _HIGH_LINE_ALIASED_MAPPING_KEY.search(text) is not None
    ):
        return None
    return _redact_text(text)


def _structured_nesting_omission(text: str) -> tuple[str, int]:
    trailing_newline = "\r\n" if text.endswith("\r\n") else "\n" if text.endswith("\n") else ""
    return "[CONTENT OMITTED: STRUCTURED NESTING BOUND EXCEEDED]" + trailing_newline, 1


def _structured_depth_exceeded(value: Any) -> bool:
    stack: list[tuple[Any, int]] = [(value, 0)]
    while stack:
        item, depth = stack.pop()
        if not isinstance(item, (dict, list)):
            continue
        if depth >= _MAX_STRUCTURED_DEPTH:
            return True
        children = item.values() if isinstance(item, dict) else item
        stack.extend((child, depth + 1) for child in children)
    return False


def _redact_all_values(value: Any, *, depth: int = 0) -> tuple[Any, int]:
    if depth >= _MAX_STRUCTURED_DEPTH and isinstance(value, (dict, list)):
        return "[CONTENT OMITTED: STRUCTURED NESTING BOUND EXCEEDED]", 1
    if isinstance(value, dict):
        result: dict[Any, Any] = {}
        count = 0
        for key, item in value.items():
            safe, replacements = _redact_all_values(item, depth=depth + 1)
            result[key] = safe
            count += replacements
        return result, count
    if isinstance(value, list):
        result_list: list[Any] = []
        count = 0
        for item in value:
            safe, replacements = _redact_all_values(item, depth=depth + 1)
            result_list.append(safe)
            count += replacements
        return result_list, count
    return "[REDACTED]", 1


def _redact_structure(
    value: Any,
    *,
    docker_auth_context: bool = False,
    depth: int = 0,
) -> tuple[Any, int]:
    if depth >= _MAX_STRUCTURED_DEPTH and isinstance(value, (dict, list)):
        return "[CONTENT OMITTED: STRUCTURED NESTING BOUND EXCEEDED]", 1
    if isinstance(value, str):
        safe, replacements, _ = _redact_log_content(value)
        return safe, replacements
    if isinstance(value, list):
        result: list[Any] = []
        count = 0
        for item in value:
            safe, replacements = _redact_structure(
                item,
                docker_auth_context=docker_auth_context,
                depth=depth + 1,
            )
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
                safe, replacements = _redact_all_values(item, depth=depth + 1)
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
                    depth=depth + 1,
                )
            result_dict[key] = safe
            count += replacements
        return result_dict, count
    return value, 0


def _redact_logical_text(text: str) -> tuple[str, int]:
    """Structurally redact a complete JSON record, otherwise redact ordinary text."""
    try:
        parsed = json.loads(text)
    except RecursionError:
        return _structured_nesting_omission(text)
    except (TypeError, ValueError):
        safe_text, replacements = _redact_text(text)
        if replacements:
            return safe_text, replacements
        return _redact_embedded_structures(text)
    if not isinstance(parsed, (dict, list)):
        return _redact_text(text)
    if _structured_depth_exceeded(parsed):
        return _structured_nesting_omission(text)
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
        except RecursionError:
            return _structured_nesting_omission(text)
        except (TypeError, ValueError):
            continue
        if _structured_depth_exceeded(parsed):
            return _structured_nesting_omission(text)
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


def _yaml_flow_delta(text: str) -> int:
    """Count unquoted YAML flow-collection delimiters in one line."""
    delta = 0
    quote: str | None = None
    escaped = False
    for character in text:
        if escaped:
            escaped = False
            continue
        if quote == '"' and character == "\\":
            escaped = True
            continue
        if quote is not None:
            if character == quote:
                quote = None
            continue
        if character in {'"', "'"}:
            quote = character
        elif character in "{[":
            delta += 1
        elif character in "}]":
            delta -= 1
        elif character == "#":
            break
    return delta


def _yaml_flow_complexity_exceeded(text: str) -> bool:
    """Reject excessive flow nesting/tokens before invoking PyYAML."""
    depth = 0
    tokens = 0
    quote: str | None = None
    escaped = False
    comment = False
    for character in text:
        if character in "\r\n":
            comment = False
            continue
        if comment:
            continue
        if escaped:
            escaped = False
            continue
        if quote == '"' and character == "\\":
            escaped = True
            continue
        if quote is not None:
            if character == quote:
                quote = None
            continue
        if character in {'"', "'"}:
            quote = character
        elif character == "#":
            comment = True
        elif character in "[{":
            depth += 1
            tokens += 1
        elif character in "]}":
            depth = max(0, depth - 1)
            tokens += 1
        elif depth > 0 and character in ",:":
            tokens += 1
        if depth > _MAX_YAML_FLOW_DEPTH or tokens > _MAX_YAML_FLOW_TOKENS:
            return True
    return False


def _yaml_block_complexity_exceeded(text: str) -> bool:
    """Bound block-mapping tokenization before invoking PyYAML."""
    mapping_lines = 0
    block_tokens = 0
    per_line_scan_candidates = 0
    for line in text.splitlines():
        mapping_line = _YAML_BLOCK_MAPPING_LINE.match(line) is not None
        candidate = line.lstrip(" \t")
        if candidate.startswith("- "):
            candidate = candidate[2:].lstrip(" \t")
        explicit_key = candidate.startswith("? ")
        if mapping_line:
            mapping_lines += 1
        if (
            mapping_line
            or _YAML_BLOCK_SEQUENCE_ITEM.match(line) is not None
            or explicit_key
        ):
            block_tokens += 1
        if (
            "\\" in line
            and '"' in line
            and (":" in line or "?" in line)
        ) or (
            explicit_key
            and candidate[2:].lstrip(" \t").startswith(("!", "&"))
        ):
            per_line_scan_candidates += 1
        if (
            mapping_lines > _MAX_YAML_BLOCK_MAPPING_LINES
            or block_tokens > _MAX_YAML_BLOCK_TOKENS
            or per_line_scan_candidates > _MAX_YAML_PER_LINE_SCAN_CANDIDATES
        ):
            return True
    return False


def _yaml_parse_complexity_exceeded(text: str) -> bool:
    """Bound the flow and block syntax that may be sent to PyYAML."""
    return _yaml_flow_complexity_exceeded(text) or _yaml_block_complexity_exceeded(text)


def _contains_kubernetes_secret_payload(
    value: Any,
    *,
    depth: int = 0,
    seen: set[int] | None = None,
) -> bool:
    """Find a Kubernetes Secret payload in a parsed, bounded YAML value."""
    if not isinstance(value, (dict, list)) or depth >= _MAX_STRUCTURED_DEPTH:
        return False
    seen = set() if seen is None else seen
    identity = id(value)
    if identity in seen:
        return False
    seen.add(identity)
    if isinstance(value, dict):
        normalized = {
            key.casefold().replace("_", "").replace("-", ""): item
            for key, item in value.items()
            if isinstance(key, str)
        }
        if (
            isinstance(normalized.get("kind"), str)
            and normalized["kind"].casefold() == "secret"
            and ("data" in normalized or "stringdata" in normalized)
        ):
            return True
        children = value.values()
    else:
        children = value
    return any(
        _contains_kubernetes_secret_payload(child, depth=depth + 1, seen=seen)
        for child in children
    )


def _contains_sensitive_yaml_environment(
    value: Any,
    *,
    depth: int = 0,
    seen: set[int] | None = None,
) -> bool:
    """Find a sensitive YAML environment name/value mapping."""
    if not isinstance(value, (dict, list)) or depth >= _MAX_STRUCTURED_DEPTH:
        return False
    seen = set() if seen is None else seen
    identity = id(value)
    if identity in seen:
        return False
    seen.add(identity)
    if isinstance(value, dict):
        normalized = {
            key.casefold().replace("_", "").replace("-", ""): item
            for key, item in value.items()
            if isinstance(key, str)
        }
        name = normalized.get("name")
        if (
            isinstance(name, str)
            and _SENSITIVE_KEY.fullmatch(name) is not None
            and "value" in normalized
        ):
            return True
        children = value.values()
    else:
        children = value
    return any(
        _contains_sensitive_yaml_environment(child, depth=depth + 1, seen=seen)
        for child in children
    )


def _contains_sensitive_yaml_key(
    value: Any,
    *,
    depth: int = 0,
    seen: set[int] | None = None,
) -> bool:
    """Find a decoded sensitive mapping key in parsed YAML."""
    if not isinstance(value, (dict, list)) or depth >= _MAX_STRUCTURED_DEPTH:
        return False
    seen = set() if seen is None else seen
    identity = id(value)
    if identity in seen:
        return False
    seen.add(identity)
    if isinstance(value, dict):
        if any(isinstance(key, str) and _SENSITIVE_KEY.fullmatch(key) for key in value):
            return True
        children = value.values()
    else:
        children = value
    return any(
        _contains_sensitive_yaml_key(child, depth=depth + 1, seen=seen)
        for child in children
    )


def _yaml_node_sensitivity(root: Any) -> str | None:
    """Classify composed YAML nodes without constructing application tags."""
    found_secret = False
    found_environment = False
    found_assignment = False
    seen: set[int] = set()
    pending = [(root, 0)]
    while pending:
        node, depth = pending.pop()
        if (
            not isinstance(node, (yaml.nodes.MappingNode, yaml.nodes.SequenceNode))
            or depth >= _MAX_STRUCTURED_DEPTH
            or id(node) in seen
        ):
            continue
        seen.add(id(node))
        if isinstance(node, yaml.nodes.MappingNode):
            fields: dict[str, Any] = {}
            for key_node, value_node in node.value:
                if isinstance(key_node, yaml.nodes.ScalarNode):
                    key = str(key_node.value)
                    normalized = key.casefold().replace("_", "").replace("-", "")
                    fields[normalized] = value_node
                    if _SENSITIVE_KEY.fullmatch(key) is not None:
                        found_assignment = True
                pending.append((value_node, depth + 1))
            kind = fields.get("kind")
            if (
                isinstance(kind, yaml.nodes.ScalarNode)
                and str(kind.value).casefold() == "secret"
                and ("data" in fields or "stringdata" in fields)
            ):
                found_secret = True
            name = fields.get("name")
            if (
                isinstance(name, yaml.nodes.ScalarNode)
                and _SENSITIVE_KEY.fullmatch(str(name.value)) is not None
                and "value" in fields
            ):
                found_environment = True
        else:
            pending.extend((item, depth + 1) for item in node.value)
    if found_secret:
        return "KUBERNETES SECRET"
    if found_environment:
        return "YAML ENVIRONMENT VALUE"
    if found_assignment:
        return "YAML ASSIGNMENT"
    return None


def _yaml_flow_sensitivity(text: str) -> str | None:
    """Classify a complete bounded YAML flow collection."""
    stripped = text.strip()
    if "{" not in stripped and "[" not in stripped:
        return None
    if _yaml_flow_complexity_exceeded(stripped):
        return "YAML FLOW COMPLEXITY BOUND EXCEEDED"
    try:
        json_value = json.loads(stripped)
    except (RecursionError, TypeError, ValueError):
        pass
    else:
        if isinstance(json_value, (dict, list)):
            return None
    starts = [index for token in ("{", "[") if (index := stripped.find(token)) >= 0]
    candidates = [stripped]
    if starts and min(starts) > 0:
        candidates.append(stripped[min(starts) :])
    for candidate in candidates:
        try:
            parsed = yaml.safe_load(candidate)
        except (RecursionError, yaml.YAMLError):
            try:
                composed = yaml.compose(candidate, Loader=yaml.SafeLoader)
            except (RecursionError, yaml.YAMLError):
                continue
            sensitivity = _yaml_node_sensitivity(composed)
            if sensitivity is not None:
                return sensitivity
            continue
        if _contains_kubernetes_secret_payload(parsed):
            return "KUBERNETES SECRET"
        if _contains_sensitive_yaml_environment(parsed):
            return "YAML ENVIRONMENT VALUE"
        if _contains_sensitive_yaml_key(parsed):
            return "YAML ASSIGNMENT"
    if _YAML_FLOW_KIND_SECRET.search(stripped) and _YAML_FLOW_SECRET_PAYLOAD.search(stripped):
        return "KUBERNETES SECRET"
    return None


def _is_single_line_flow_yaml_secret(text: str) -> bool:
    """Recognize complete flow-style Secret manifests before ordinary redaction."""
    stripped = text.strip()
    lowered = stripped.casefold()
    if "{" not in stripped or "kind" not in lowered or "secret" not in lowered:
        return False
    return _yaml_flow_sensitivity(stripped) == "KUBERNETES SECRET"


def _yaml_payload_continuation(value: str) -> tuple[str, int | None] | None:
    """Classify a Secret payload value that continues onto following lines."""
    remainder = value.strip()
    while prefix := _YAML_NODE_PREFIX.match(remainder):
        remainder = remainder[prefix.end() :].lstrip()
    if not remainder or remainder.startswith(("|", ">")):
        return "block", None
    if remainder.startswith(("{", "[")):
        depth = _yaml_flow_delta(remainder)
        if depth > 0:
            return "flow", depth
    return None


def _yaml_node_scalar(value: str) -> str:
    """Normalize anchors and tags that decorate a plain YAML scalar."""
    remainder = value.strip()
    while prefix := _YAML_NODE_PREFIX.match(remainder):
        remainder = remainder[prefix.end() :].lstrip()
    return remainder.strip().strip("\"'")


def _yaml_anchor_definitions(lines: list[bytes]) -> list[tuple[int, str, str]]:
    """Tokenize real scalar-anchor definitions with their bounded line index."""
    document = b"".join(
        raw_line if raw_line.endswith((b"\n", b"\r")) else raw_line + b"\n"
        for raw_line in lines
    ).decode("utf-8", errors="replace")
    if _yaml_parse_complexity_exceeded(document):
        return []
    try:
        tokens = list(yaml.scan(document))
    except (RecursionError, yaml.YAMLError):
        return []
    definitions: list[tuple[int, str, str]] = []
    for index, token in enumerate(tokens):
        if not isinstance(token, yaml.tokens.AnchorToken):
            continue
        value_index = index + 1
        while value_index < len(tokens) and isinstance(tokens[value_index], yaml.tokens.TagToken):
            value_index += 1
        value_token = tokens[value_index] if value_index < len(tokens) else None
        if isinstance(value_token, yaml.tokens.ScalarToken):
            definitions.append((token.start_mark.line, token.value, str(value_token.value)))
        elif isinstance(value_token, yaml.tokens.AliasToken):
            definitions.append((token.start_mark.line, token.value, f"*{value_token.value}"))
    return definitions


def _yaml_mapping_scalar_values(lines: list[bytes], key: str) -> dict[int, str]:
    """Return scalar mapping values keyed by their source-line index."""
    document = b"".join(
        raw_line if raw_line.endswith((b"\n", b"\r")) else raw_line + b"\n"
        for raw_line in lines
    ).decode("utf-8", errors="replace")
    if _yaml_parse_complexity_exceeded(document):
        return {}
    try:
        tokens = list(yaml.scan(document))
    except (RecursionError, yaml.YAMLError):
        return {}
    values: dict[int, str] = {}
    for index, token in enumerate(tokens):
        if (
            not isinstance(token, yaml.tokens.ScalarToken)
            or str(token.value).casefold() != key.casefold()
            or index == 0
            or not isinstance(tokens[index - 1], yaml.tokens.KeyToken)
        ):
            continue
        value_index = index + 2
        while value_index < len(tokens) and isinstance(
            tokens[value_index], (yaml.tokens.AnchorToken, yaml.tokens.TagToken)
        ):
            value_index += 1
        value_token = tokens[value_index] if value_index < len(tokens) else None
        if isinstance(value_token, yaml.tokens.ScalarToken):
            values[token.start_mark.line] = str(value_token.value)
        elif isinstance(value_token, yaml.tokens.AliasToken):
            values[token.start_mark.line] = f"*{value_token.value}"
    return values


def _yaml_kind_entries(lines: list[bytes]) -> list[tuple[int, str, int, bool]]:
    """Return semantic kind mappings with source scope information."""
    semantic_values = _yaml_mapping_scalar_values(lines, "kind")
    entries: list[tuple[int, str, int, bool]] = []
    for line_index, value in semantic_values.items():
        raw_line = lines[line_index]
        text = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
        direct = _YAML_KIND_ASSIGNMENT.fullmatch(text)
        indent = len(direct.group("indent")) if direct is not None else _line_indent(raw_line)
        sequence_scope = (
            direct.group("dash") is not None
            if direct is not None
            else raw_line.lstrip().startswith(b"-")
        )
        entries.append((line_index, value, indent, sequence_scope))
    if semantic_values:
        return sorted(entries)
    for line_index, raw_line in enumerate(lines):
        text = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
        direct = _YAML_KIND_ASSIGNMENT.fullmatch(text)
        if direct is not None:
            entries.append(
                (
                    line_index,
                    direct.group("kind"),
                    len(direct.group("indent")),
                    direct.group("dash") is not None,
                )
            )
    return sorted(entries)


def _yaml_sensitive_assignment(text: str) -> tuple[int, str] | None:
    """Return indent and raw value for a decoded sensitive YAML mapping key."""
    if (
        "\\" not in text
        or '"' not in text
        or ":" not in text
        or _yaml_parse_complexity_exceeded(text)
    ):
        return None
    try:
        tokens = list(yaml.scan(text))
    except (RecursionError, yaml.YAMLError):
        return None
    for index, token in enumerate(tokens):
        if (
            not isinstance(token, yaml.tokens.ScalarToken)
            or not isinstance(token.value, str)
            or _SENSITIVE_KEY.fullmatch(token.value) is None
            or index == 0
            or not isinstance(tokens[index - 1], yaml.tokens.KeyToken)
        ):
            continue
        prefix = text[: token.start_mark.column]
        if re.fullmatch(r"[ \t]*(?:-[ \t]+)?", prefix) is None:
            continue
        value_token = tokens[index + 1]
        return len(prefix) - len(prefix.lstrip(" \t")), text[value_token.end_mark.column :].strip()
    return None


def _yaml_alias_sensitive_assignment(
    text: str,
    anchors: dict[str, str],
) -> tuple[int, str] | None:
    """Return an alias-key assignment when its key is sensitive or unresolved."""
    if "*" not in text or ":" not in text or _yaml_parse_complexity_exceeded(text):
        return None
    try:
        tokens = list(yaml.scan(text))
    except (RecursionError, yaml.YAMLError):
        return None
    for index, token in enumerate(tokens):
        if (
            not isinstance(token, yaml.tokens.AliasToken)
            or index == 0
            or not isinstance(tokens[index - 1], yaml.tokens.KeyToken)
            or index + 1 >= len(tokens)
            or not isinstance(tokens[index + 1], yaml.tokens.ValueToken)
        ):
            continue
        prefix = text[: token.start_mark.column]
        if re.fullmatch(r"[ \t]*(?:-[ \t]+)?", prefix) is None:
            continue
        resolved = _resolve_yaml_scalar(f"*{token.value}", anchors)
        if resolved is not None and _SENSITIVE_KEY.fullmatch(resolved) is None:
            return None
        value_token = tokens[index + 1]
        indent = len(prefix) - len(prefix.lstrip(" \t"))
        return indent, text[value_token.end_mark.column :].strip()
    return None


def _yaml_explicit_sensitive_key(text: str) -> int | None:
    """Return the indent of a sensitive scalar used as an explicit YAML key."""
    direct = _YAML_EXPLICIT_SENSITIVE_KEY.fullmatch(text)
    if direct is not None:
        return len(direct.group("indent"))
    candidate = text.lstrip(" \t")
    if candidate.startswith("- "):
        candidate = candidate[2:].lstrip(" \t")
    if not candidate.startswith("? ") or not candidate[2:].lstrip(" \t").startswith(
        ("!", "&", '"')
    ):
        return None
    try:
        tokens = list(yaml.scan(text))
    except (RecursionError, yaml.YAMLError):
        return None
    for index, token in enumerate(tokens):
        if (
            not isinstance(token, yaml.tokens.ScalarToken)
            or not isinstance(token.value, str)
            or _SENSITIVE_KEY.fullmatch(token.value) is None
        ):
            continue
        key_index = index - 1
        while key_index >= 0 and isinstance(
            tokens[key_index], (yaml.tokens.AnchorToken, yaml.tokens.TagToken)
        ):
            key_index -= 1
        if key_index < 0 or not isinstance(tokens[key_index], yaml.tokens.KeyToken):
            continue
        key_token = tokens[key_index]
        prefix = text[: key_token.start_mark.column]
        if (
            key_token.end_mark.column <= key_token.start_mark.column
            or re.fullmatch(r"[ \t]*(?:-[ \t]+)?", prefix) is None
        ):
            continue
        suffix = text[token.end_mark.column :]
        if re.fullmatch(r"[ \t]*(?:#.*)?", suffix) is not None:
            return len(prefix) - len(prefix.lstrip(" \t"))
    return None


def _yaml_explicit_value_end(
    raw_lines: list[bytes],
    value_index: int,
    *,
    minimum_indent: int,
    has_more_after_raw: bool,
) -> tuple[int, bool] | None:
    """Return the end of an explicit YAML value and whether it crosses the window."""
    while value_index < len(raw_lines) and not raw_lines[value_index].strip():
        value_index += 1
    if value_index >= len(raw_lines):
        return value_index, has_more_after_raw
    text = raw_lines[value_index].decode("utf-8", errors="replace").rstrip("\r\n")
    value_match = _YAML_EXPLICIT_VALUE.fullmatch(text)
    if value_match is None or len(value_match.group("indent")) < minimum_indent:
        return None
    value_end = value_index + 1
    continuation = _yaml_payload_continuation(value_match.group("value"))
    if continuation is None:
        return value_end, False
    mode, flow_depth = continuation
    if mode == "flow" and flow_depth is not None:
        while value_end < len(raw_lines) and flow_depth > 0:
            flow_depth += _yaml_flow_delta(
                raw_lines[value_end].decode("utf-8", errors="replace")
            )
            value_end += 1
        return value_end, flow_depth > 0 and has_more_after_raw
    value_indent = len(value_match.group("indent"))
    while value_end < len(raw_lines):
        if raw_lines[value_end].strip() and _line_indent(raw_lines[value_end]) <= value_indent:
            break
        value_end += 1
    return value_end, value_end == len(raw_lines) and has_more_after_raw


def _yaml_mapping_scalar_field(text: str) -> tuple[int, bool, str, str] | None:
    """Return one bounded semantic scalar-key field from a YAML line."""
    if ":" not in text or _yaml_parse_complexity_exceeded(text):
        return None
    try:
        tokens = list(yaml.scan(text))
    except (RecursionError, yaml.YAMLError):
        return None
    for index, token in enumerate(tokens):
        if not isinstance(token, yaml.tokens.ScalarToken):
            continue
        key_index = index - 1
        while key_index >= 0 and isinstance(
            tokens[key_index], (yaml.tokens.AnchorToken, yaml.tokens.TagToken)
        ):
            key_index -= 1
        if key_index < 0 or not isinstance(tokens[key_index], yaml.tokens.KeyToken):
            continue
        prefix = text[: tokens[key_index].start_mark.column]
        if re.fullmatch(r"[ \t]*(?:-[ \t]+)?", prefix) is None:
            continue
        value_index = index + 1
        if value_index >= len(tokens) or not isinstance(tokens[value_index], yaml.tokens.ValueToken):
            continue
        raw_value = text[tokens[value_index].end_mark.column :].strip()
        scalar_index = value_index + 1
        while scalar_index < len(tokens) and isinstance(
            tokens[scalar_index], (yaml.tokens.AnchorToken, yaml.tokens.TagToken)
        ):
            scalar_index += 1
        value_token = tokens[scalar_index] if scalar_index < len(tokens) else None
        if isinstance(value_token, yaml.tokens.ScalarToken):
            value = str(value_token.value)
        elif isinstance(value_token, yaml.tokens.AliasToken):
            value = f"*{value_token.value}"
        else:
            value = raw_value
        return (
            tokens[key_index].start_mark.column,
            prefix.lstrip().startswith("-"),
            str(token.value),
            value,
        )
    return None


def _yaml_has_payload_mapping_key(text: str) -> bool:
    """Return whether one YAML line resolves a data/stringData mapping key."""
    field = _yaml_mapping_scalar_field(text)
    if field is not None:
        normalized = field[2].casefold().replace("_", "").replace("-", "")
        return normalized in {"data", "stringdata"}
    try:
        tokens = list(yaml.scan(text))
    except (RecursionError, yaml.YAMLError):
        return False
    for index, token in enumerate(tokens):
        if not isinstance(token, yaml.tokens.ScalarToken):
            continue
        key_index = index - 1
        while key_index >= 0 and isinstance(
            tokens[key_index], (yaml.tokens.AnchorToken, yaml.tokens.TagToken)
        ):
            key_index -= 1
        if key_index >= 0 and isinstance(tokens[key_index], yaml.tokens.KeyToken):
            normalized = str(token.value).casefold().replace("_", "").replace("-", "")
            if normalized in {"data", "stringdata"}:
                return True
    return False


def _yaml_secret_payload_lines(lines: list[bytes]) -> set[int]:
    """Find Secret payload value lines using merge-aware YAML node composition."""
    requires_composition = False
    for raw_line in lines:
        candidate = raw_line.decode("utf-8", errors="replace").lstrip(" \t")
        if candidate.startswith("- "):
            candidate = candidate[2:].lstrip(" \t")
        key_source = candidate.split(":", 1)[0]
        if (
            candidate.startswith("<<:")
            or (candidate.startswith("*") and ":" in candidate)
            or (
                "*" in candidate
                and ":" in candidate
                and (field := _yaml_mapping_scalar_field(candidate)) is not None
                and _YAML_ALIAS_SCALAR.fullmatch(field[3]) is not None
            )
            or candidate.startswith(("? |", "? >"))
            or (
                (
                    candidate.startswith("? ")
                    or candidate.startswith(("!", "&"))
                    or (candidate.startswith('"') and "\\" in key_source)
                )
                and (
                    _yaml_has_payload_mapping_key(candidate)
                    or (
                        (field := _yaml_mapping_scalar_field(candidate)) is not None
                        and field[2].casefold() == "kind"
                    )
                )
            )
        ):
            requires_composition = True
            break
    if not requires_composition:
        return set()
    document = b"".join(
        raw_line if raw_line.endswith((b"\n", b"\r")) else raw_line + b"\n"
        for raw_line in lines
    ).decode("utf-8", errors="replace")
    if _yaml_parse_complexity_exceeded(document):
        return set()
    try:
        root = yaml.compose(document, Loader=yaml.SafeLoader)
    except (RecursionError, yaml.YAMLError):
        return set()

    kind_cache: dict[int, bool] = {}
    resolving: set[int] = set()

    def merged_mappings(node: Any) -> list[Any]:
        if isinstance(node, yaml.nodes.MappingNode):
            return [node]
        if isinstance(node, yaml.nodes.SequenceNode):
            return [item for item in node.value if isinstance(item, yaml.nodes.MappingNode)]
        return []

    def has_secret_kind(node: Any, depth: int = 0) -> bool:
        identity = id(node)
        if identity in kind_cache:
            return kind_cache[identity]
        if identity in resolving or depth >= _MAX_STRUCTURED_DEPTH:
            return True
        resolving.add(identity)
        direct_kind: str | None = None
        merges: list[Any] = []
        for key_node, value_node in node.value:
            if not isinstance(key_node, yaml.nodes.ScalarNode):
                continue
            normalized = str(key_node.value).casefold().replace("_", "").replace("-", "")
            if normalized == "kind" and isinstance(value_node, yaml.nodes.ScalarNode):
                direct_kind = str(value_node.value)
            elif key_node.tag == "tag:yaml.org,2002:merge" or key_node.value == "<<":
                merges.extend(merged_mappings(value_node))
        if direct_kind is not None:
            result = direct_kind.casefold() == "secret"
        else:
            result = any(has_secret_kind(item, depth + 1) for item in merges)
        resolving.remove(identity)
        kind_cache[identity] = result
        return result

    payload_lines: set[int] = set()
    visited: set[int] = set()
    payload_value_visited: set[int] = set()

    def include_payload_values(node: Any, depth: int = 0) -> None:
        identity = id(node)
        if identity in payload_value_visited or depth >= _MAX_STRUCTURED_DEPTH:
            return
        payload_value_visited.add(identity)
        if isinstance(node, yaml.nodes.ScalarNode):
            payload_lines.update(_yaml_node_line_span(node))
        elif isinstance(node, yaml.nodes.MappingNode):
            for _key_node, value_node in node.value:
                include_payload_values(value_node, depth + 1)
        elif isinstance(node, yaml.nodes.SequenceNode):
            for item in node.value:
                include_payload_values(item, depth + 1)

    def walk(node: Any, depth: int = 0) -> None:
        if not isinstance(node, (yaml.nodes.MappingNode, yaml.nodes.SequenceNode)):
            return
        identity = id(node)
        if identity in visited or depth >= _MAX_STRUCTURED_DEPTH:
            return
        visited.add(identity)
        if isinstance(node, yaml.nodes.MappingNode):
            secret = has_secret_kind(node)
            for key_node, value_node in node.value:
                if isinstance(key_node, yaml.nodes.ScalarNode):
                    normalized = (
                        str(key_node.value).casefold().replace("_", "").replace("-", "")
                    )
                    if secret and normalized in {"data", "stringdata"}:
                        payload_lines.update(_yaml_node_line_span(value_node))
                        include_payload_values(value_node)
                walk(value_node, depth + 1)
        else:
            for item in node.value:
                walk(item, depth + 1)

    walk(root)
    return payload_lines


def _yaml_composed_documents(lines: list[bytes]) -> list[Any]:
    """Compose bounded YAML documents for semantic redaction helpers."""
    document = b"".join(
        raw_line if raw_line.endswith((b"\n", b"\r")) else raw_line + b"\n"
        for raw_line in lines
    ).decode("utf-8", errors="replace")
    if _yaml_parse_complexity_exceeded(document):
        return []
    try:
        return list(yaml.compose_all(document, Loader=yaml.SafeLoader))
    except (RecursionError, yaml.YAMLError):
        return []


def _yaml_node_line_span(node: Any) -> range:
    start = node.start_mark.line
    end = node.end_mark.line
    if end == start or node.end_mark.column > 0:
        end += 1
    return range(start, max(start + 1, end))


def _yaml_sensitive_mapping_lines(lines: list[bytes]) -> set[int]:
    """Find value spans for multiline explicit sensitive YAML keys."""
    if not any(
        raw_line.decode("utf-8", errors="replace").lstrip().startswith(("? |", "? >"))
        for raw_line in lines
    ):
        return set()
    sensitive_lines: set[int] = set()
    visited: set[int] = set()

    def walk(node: Any, depth: int = 0) -> None:
        if not isinstance(node, (yaml.nodes.MappingNode, yaml.nodes.SequenceNode)):
            return
        identity = id(node)
        if identity in visited or depth >= _MAX_STRUCTURED_DEPTH:
            return
        visited.add(identity)
        if isinstance(node, yaml.nodes.MappingNode):
            for key_node, value_node in node.value:
                if (
                    isinstance(key_node, yaml.nodes.ScalarNode)
                    and _SENSITIVE_KEY.fullmatch(str(key_node.value)) is not None
                ):
                    sensitive_lines.update(_yaml_node_line_span(key_node))
                    sensitive_lines.update(_yaml_node_line_span(value_node))
                walk(value_node, depth + 1)
        else:
            for item in node.value:
                walk(item, depth + 1)

    for root in _yaml_composed_documents(lines):
        walk(root)
    return sensitive_lines


def _yaml_sensitive_env_lines(lines: list[bytes]) -> set[int]:
    """Find composed YAML environment items with sensitive name/value siblings."""
    requires_composition = False
    for raw_line in lines:
        candidate = raw_line.decode("utf-8", errors="replace").lstrip(" \t")
        if candidate.startswith("- "):
            candidate = candidate[2:].lstrip(" \t")
        if (
            candidate.startswith("<<:")
            or (candidate.startswith("*") and ":" in candidate)
            or (
                "*" in candidate
                and ":" in candidate
                and (field := _yaml_mapping_scalar_field(candidate)) is not None
                and field[2].casefold() == "value"
                and _YAML_ALIAS_SCALAR.fullmatch(field[3]) is not None
            )
            or candidate.startswith("? ")
            or (
                ":" in candidate
                and candidate.split(":", 1)[1].lstrip().startswith(("|", ">"))
                and (
                    "name" in candidate.casefold()
                    or ("\\" in candidate and '"' in candidate)
                )
                and (field := _yaml_mapping_scalar_field(candidate)) is not None
                and field[2].casefold() == "name"
            )
        ):
            requires_composition = True
            break
    if not requires_composition:
        return set()

    sensitive_lines: set[int] = set()
    visited: set[int] = set()
    field_cache: dict[int, tuple[dict[str, Any], bool]] = {}
    resolving: set[int] = set()

    def merged_mappings(node: Any) -> tuple[list[Any], bool]:
        if isinstance(node, yaml.nodes.MappingNode):
            return [node], False
        if isinstance(node, yaml.nodes.SequenceNode):
            mappings = [
                item for item in node.value if isinstance(item, yaml.nodes.MappingNode)
            ]
            return mappings, len(mappings) != len(node.value)
        return [], True

    def resolved_fields(
        node: Any,
        depth: int = 0,
    ) -> tuple[dict[str, Any], bool]:
        identity = id(node)
        if identity in field_cache:
            return field_cache[identity]
        if identity in resolving or depth >= _MAX_STRUCTURED_DEPTH:
            return {}, True
        resolving.add(identity)
        fields: dict[str, Any] = {}
        uncertain = False
        merges: list[Any] = []
        for key_node, value_node in node.value:
            if not isinstance(key_node, yaml.nodes.ScalarNode):
                continue
            key = str(key_node.value).casefold()
            if key_node.tag == "tag:yaml.org,2002:merge" or key == "<<":
                merged, malformed = merged_mappings(value_node)
                merges.extend(merged)
                uncertain = uncertain or malformed
        for merged in merges:
            merged_fields, merged_uncertain = resolved_fields(merged, depth + 1)
            uncertain = uncertain or merged_uncertain
            for key, value in merged_fields.items():
                fields.setdefault(key, value)
        for key_node, value_node in node.value:
            if not isinstance(key_node, yaml.nodes.ScalarNode):
                continue
            key = str(key_node.value).casefold()
            if key in {"name", "value"}:
                fields[key] = value_node
        resolving.remove(identity)
        result = fields, uncertain
        field_cache[identity] = result
        return result

    def walk(node: Any, depth: int = 0) -> None:
        if not isinstance(node, (yaml.nodes.MappingNode, yaml.nodes.SequenceNode)):
            return
        identity = id(node)
        if identity in visited or depth >= _MAX_STRUCTURED_DEPTH:
            return
        visited.add(identity)
        if isinstance(node, yaml.nodes.MappingNode):
            fields, uncertain = resolved_fields(node)
            name_node = fields.get("name")
            value_node = fields.get("value")
            if (
                value_node is not None
                and (
                    (name_node is None and uncertain)
                    or not isinstance(name_node, yaml.nodes.ScalarNode)
                    or _SENSITIVE_KEY.fullmatch(str(name_node.value)) is not None
                )
            ):
                sensitive_lines.update(_yaml_node_line_span(node))
                sensitive_lines.update(_yaml_node_line_span(value_node))
            for _key_node, value_node in node.value:
                walk(value_node, depth + 1)
        else:
            for item in node.value:
                walk(item, depth + 1)

    for root in _yaml_composed_documents(lines):
        walk(root)
    return sensitive_lines


def _yaml_scalar_anchors(lines: list[bytes]) -> dict[str, str]:
    """Collect scalar anchors from the current bounded YAML document."""
    document_start = 0
    for index, raw_line in enumerate(lines):
        line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
        if _YAML_DOCUMENT_BOUNDARY.fullmatch(line):
            document_start = index + 1
    return {
        name: value
        for _line, name, value in _yaml_anchor_definitions(lines[document_start:])
    }


def _yaml_anchor_state_by_line(
    lines: list[bytes],
    inherited: dict[str, str] | None,
) -> list[dict[str, str]]:
    """Map each bounded line to its document-local scalar anchor table."""
    states: list[dict[str, str]] = [{} for _ in lines]
    document_start = 0
    for boundary in range(len(lines) + 1):
        at_end = boundary == len(lines)
        line = "" if at_end else lines[boundary].decode("utf-8", errors="replace").rstrip("\r\n")
        if not at_end and _YAML_DOCUMENT_BOUNDARY.fullmatch(line) is None:
            continue
        anchors = dict(inherited or {}) if document_start == 0 else {}
        definitions: dict[int, list[tuple[str, str]]] = {}
        for local_line, name, value in _yaml_anchor_definitions(lines[document_start:boundary]):
            definitions.setdefault(local_line, []).append((name, value))
        for local_line in range(boundary - document_start):
            if local_line in definitions:
                anchors = dict(anchors)
                anchors.update(definitions[local_line])
            states[document_start + local_line] = anchors
        if not at_end:
            states[boundary] = {}
        document_start = boundary + 1
    return states


def _resolve_yaml_scalar(value: str, anchors: dict[str, str]) -> str | None:
    """Resolve a scalar alias through bounded, document-local anchor state."""
    resolved = _yaml_node_scalar(value)
    visited: set[str] = set()
    while alias := _YAML_ALIAS_SCALAR.fullmatch(resolved):
        name = alias.group("anchor")
        if name in visited or name not in anchors:
            return None
        visited.add(name)
        resolved = _yaml_node_scalar(anchors[name])
    return resolved


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


def _yaml_env_field(text: str) -> tuple[int, bool, str, str] | None:
    """Classify a literal or decorated YAML environment field."""
    name_match = _YAML_ENV_NAME.fullmatch(text)
    value_match = _YAML_ENV_VALUE.fullmatch(text)
    literal = name_match or value_match
    if literal is not None:
        field = "name" if name_match is not None else "value"
        value = literal.group(field)
        return (
            len(literal.group("indent")) + len(literal.group("dash") or ""),
            literal.group("dash") is not None,
            field,
            value,
        )
    if not any(marker in text for marker in ("!", "&", "\\")):
        return None
    semantic = _yaml_mapping_scalar_field(text)
    if semantic is None or semantic[2].casefold() not in {"name", "value"}:
        return None
    return semantic[0], semantic[1], semantic[2].casefold(), semantic[3]


def _yaml_env_item(
    raw_lines: list[bytes],
    start: int,
    *,
    has_more_after_raw: bool,
    yaml_anchors: dict[str, str] | None = None,
) -> tuple[int, bool, bool] | None:
    """Return a YAML env item boundary and whether its value is sensitive or uncertain."""
    first = raw_lines[start].decode("utf-8", errors="replace").rstrip("\r\n")
    first_field = _yaml_env_field(first)
    sequence_only = _YAML_SEQUENCE_ITEM_ONLY.fullmatch(first)
    is_sequence = sequence_only is not None or (
        first_field is not None and first_field[1]
    )
    is_page_fragment = start == 0 and first_field is not None
    if not is_sequence and not is_page_fragment:
        return None

    item_indent = _line_indent(raw_lines[start])
    fragment_indent = first_field[0] if first_field is not None else None
    end = start + 1
    while end < len(raw_lines):
        candidate = raw_lines[end]
        if candidate.strip():
            indent = _line_indent(candidate)
            if is_sequence:
                if indent <= item_indent:
                    break
            elif fragment_indent is not None and (
                indent < fragment_indent
                or (indent <= fragment_indent and candidate.lstrip().startswith(b"-"))
                or _YAML_DOCUMENT_BOUNDARY.fullmatch(
                    candidate.decode("utf-8", errors="replace").rstrip("\r\n")
                )
            ):
                break
        end += 1

    fields: list[tuple[int, str, str | None]] = []
    for raw_line in raw_lines[start:end]:
        line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
        field = _yaml_env_field(line)
        if field is None:
            continue
        effective_indent, _has_dash, field_name, field_value = field
        if field_name == "name":
            fields.append(
                (
                    effective_indent,
                    "name",
                    _resolve_yaml_scalar(field_value, yaml_anchors or {}),
                )
            )
        else:
            fields.append((effective_indent, "value", field_value))
    if not fields:
        return end, False, False
    direct_indent = min(field[0] for field in fields)
    direct_fields = [field for field in fields if field[0] == direct_indent]
    has_value = any(field[1] == "value" for field in direct_fields)
    names = [field[2] for field in direct_fields if field[1] == "name"]
    sensitive = has_value and any(
        name is None or _SENSITIVE_KEY.fullmatch(name) is not None for name in names
    )
    uncertain = has_value and not names and end == len(raw_lines) and has_more_after_raw
    return end, sensitive, uncertain


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
    if _yaml_parse_complexity_exceeded(context.decode("utf-8", errors="replace")):
        return None, None, None, None, None, scanned_bytes

    context_lines = context.splitlines()
    yaml_anchor_states = _yaml_anchor_state_by_line(context_lines, None)
    starts_with_sensitive_value: bool | None = None
    for line_index in range(len(context_lines) - 1, -1, -1):
        raw_line = context_lines[line_index]
        if not raw_line.strip():
            continue
        line = raw_line.decode("utf-8", errors="replace")
        yaml_assignment = _yaml_sensitive_assignment(line)
        alias_assignment = _yaml_alias_sensitive_assignment(
            line,
            yaml_anchor_states[line_index],
        )
        starts_with_sensitive_value = (
            _PENDING_SENSITIVE_ASSIGNMENT.search(line) is not None
            and _BLOCK_SENSITIVE_ASSIGNMENT.fullmatch(line) is None
        ) or (
            yaml_assignment is not None
            and (
                not yaml_assignment[1]
                or yaml_assignment[1].startswith(("|", ">"))
            )
        ) or (
            alias_assignment is not None
            and (
                not alias_assignment[1]
                or alias_assignment[1].startswith(("|", ">"))
            )
        ) or _yaml_explicit_sensitive_key(line) is not None
        break
    first_raw_line = next((line for line in raw.splitlines() if line.strip()), None)
    first_raw_field = (
        _yaml_env_field(first_raw_line.decode("utf-8", errors="replace"))
        if first_raw_line is not None
        else None
    )
    if first_raw_field is not None and first_raw_field[2] == "value":
        value_indent = first_raw_field[0]
        for line_index in range(len(context_lines) - 1, -1, -1):
            raw_line = context_lines[line_index]
            if not raw_line.strip():
                continue
            line = raw_line.decode("utf-8", errors="replace")
            name_field = _yaml_env_field(line)
            if name_field is not None and name_field[2] == "name" and (
                (
                    name_field[1]
                    and name_field[0] <= value_indent
                )
                or (
                    not name_field[1]
                    and name_field[0] == value_indent
                )
            ):
                name = _resolve_yaml_scalar(
                    name_field[3],
                    yaml_anchor_states[line_index],
                )
                starts_with_sensitive_value = name is None or _SENSITIVE_KEY.fullmatch(name) is not None
                break
            if raw_line.lstrip().startswith(b"-") and _line_indent(raw_line) <= value_indent:
                break
    if starts_with_sensitive_value is None and search_start == 0:
        starts_with_sensitive_value = False

    block_state_known = search_start == 0
    active_block_indent: int | None = None
    pending_explicit_indent: int | None = None
    for line_index, raw_line in enumerate(context_lines):
        if not raw_line.strip():
            continue
        indent = _line_indent(raw_line)
        if active_block_indent is not None:
            if indent > active_block_indent or (
                indent == active_block_indent and raw_line.lstrip().startswith(b"-")
            ):
                continue
            active_block_indent = None
        line = raw_line.decode("utf-8", errors="replace")
        if pending_explicit_indent is not None:
            explicit_value = _YAML_EXPLICIT_VALUE.fullmatch(line)
            if (
                explicit_value is not None
                and len(explicit_value.group("indent")) >= pending_explicit_indent
            ):
                continuation = _yaml_payload_continuation(explicit_value.group("value"))
                if continuation is not None:
                    active_block_indent = len(explicit_value.group("indent"))
                block_state_known = True
                pending_explicit_indent = None
                continue
            pending_explicit_indent = None
        indented_match = (
            _BLOCK_SENSITIVE_ASSIGNMENT.fullmatch(line)
            or _PLAIN_SENSITIVE_ASSIGNMENT.fullmatch(line)
            or _FISH_SENSITIVE_ASSIGNMENT.fullmatch(line)
            or _DOCKER_ENV_SENSITIVE_ASSIGNMENT.fullmatch(line)
            or _PENDING_YAML_SENSITIVE_ASSIGNMENT.fullmatch(line)
        )
        yaml_assignment = _yaml_sensitive_assignment(line)
        alias_assignment = _yaml_alias_sensitive_assignment(
            line,
            yaml_anchor_states[line_index],
        )
        explicit_key_indent = _yaml_explicit_sensitive_key(line)
        if explicit_key_indent is not None:
            pending_explicit_indent = explicit_key_indent
            block_state_known = True
        elif indented_match is not None:
            active_block_indent = len(indented_match.group("indent"))
            block_state_known = True
        elif yaml_assignment is not None:
            active_block_indent = yaml_assignment[0]
            block_state_known = True
        elif alias_assignment is not None:
            active_block_indent = alias_assignment[0]
            block_state_known = True
        elif _PENDING_SENSITIVE_ASSIGNMENT.search(line) is not None:
            active_block_indent = indent
            block_state_known = True
        elif indent == 0:
            block_state_known = True

    first_content_line = next((line for line in raw.splitlines() if line.strip()), None)
    first_explicit_value = (
        _YAML_EXPLICIT_VALUE.fullmatch(
            first_content_line.decode("utf-8", errors="replace").rstrip("\r\n")
        )
        if first_content_line is not None
        else None
    )
    if (
        pending_explicit_indent is not None
        and first_explicit_value is not None
        and len(first_explicit_value.group("indent")) >= pending_explicit_indent
    ):
        starts_with_sensitive_value = True

    if first_content_line is None:
        starts_inside_sensitive_block: bool | None = False
        active_block_indent = None
    elif active_block_indent is not None and (
        _line_indent(first_content_line) > active_block_indent
        or (
            _line_indent(first_content_line) == active_block_indent
            and first_content_line.lstrip().startswith(b"-")
        )
    ):
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
        if (
            _PREFIXED_PLAIN_SENSITIVE_ASSIGNMENT.search(continuation_line) is not None
            or _FISH_SENSITIVE_ASSIGNMENT.fullmatch(continuation_line) is not None
            or _DOCKER_ENV_SENSITIVE_ASSIGNMENT.fullmatch(continuation_line) is not None
        ):
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

    combined_lines = context_lines + raw.splitlines()
    raw_line_index = len(context_lines)
    semantic_sensitive_lines = _yaml_sensitive_mapping_lines(combined_lines)
    semantic_sensitive_lines.update(_yaml_sensitive_env_lines(combined_lines))
    first_raw_index = next(
        (
            raw_line_index + index
            for index, raw_line in enumerate(raw.splitlines())
            if raw_line.strip()
        ),
        None,
    )
    if first_raw_index is not None and first_raw_index in semantic_sensitive_lines:
        starts_with_sensitive_value = False
        starts_inside_sensitive_block = True
        active_block_indent = -2

    return (
        starts_with_sensitive_value,
        starts_inside_sensitive_block,
        active_block_indent,
        starts_inside_sensitive_quote,
        active_quote,
        scanned_bytes,
    )


def _kubernetes_yaml_state_before(
    handle: Any,
    offset: int,
    raw: bytes,
) -> tuple[
    bool | None,
    tuple[int, bool] | None,
    bool,
    bool | None,
    int | None,
    int | None,
    dict[str, str],
    int,
]:
    """Recover Kubernetes Secret YAML document and payload-block state."""
    if offset <= 0:
        return False, None, False, False, None, None, {}, 0
    search_start = max(0, offset - _MAX_SENSITIVE_ASSIGNMENT_CONTEXT_BYTES)
    handle.seek(search_start)
    context = handle.read(offset - search_start)
    scanned_bytes = len(context)
    if search_start > 0:
        newline = context.find(b"\n")
        if newline < 0:
            return None, None, False, None, None, None, {}, scanned_bytes
        context = context[newline + 1 :]
    if _yaml_parse_complexity_exceeded(context.decode("utf-8", errors="replace")):
        return None, None, False, None, None, None, {}, scanned_bytes

    state_known = search_start == 0
    secret_scopes: list[tuple[int, bool]] = []
    kind_seen = False
    payload_indent: int | None = None
    payload_flow_depth: int | None = None
    context_lines = context.splitlines(keepends=True)
    yaml_anchor_states = _yaml_anchor_state_by_line(context_lines, None)
    yaml_kind_entries = {
        line_index: (value, indent, sequence_scope)
        for line_index, value, indent, sequence_scope in _yaml_kind_entries(context_lines)
    }
    yaml_anchors = yaml_anchor_states[-1] if yaml_anchor_states else {}
    for context_index, raw_line in enumerate(context.splitlines()):
        if not raw_line.strip():
            continue
        indent = _line_indent(raw_line)
        if payload_indent is not None:
            if payload_flow_depth is not None:
                payload_flow_depth += _yaml_flow_delta(
                    raw_line.decode("utf-8", errors="replace")
                )
                if payload_flow_depth > 0:
                    continue
                payload_indent = None
                payload_flow_depth = None
                continue
            if indent > payload_indent:
                continue
            payload_indent = None
        while secret_scopes:
            scope_indent, sequence_scope = secret_scopes[-1]
            if (sequence_scope and indent <= scope_indent) or (
                not sequence_scope and indent < scope_indent
            ):
                secret_scopes.pop()
                continue
            break
        line = raw_line.decode("utf-8", errors="replace")
        if _YAML_DOCUMENT_BOUNDARY.fullmatch(line):
            state_known = True
            secret_scopes.clear()
            kind_seen = False
            payload_indent = None
            payload_flow_depth = None
            continue
        kind_match = _YAML_KIND_ASSIGNMENT.fullmatch(line)
        kind_entry = yaml_kind_entries.get(context_index)
        if kind_match is not None or kind_entry is not None:
            state_known = True
            raw_kind = kind_entry[0] if kind_entry is not None else kind_match.group("kind")
            kind = _resolve_yaml_scalar(
                raw_kind,
                yaml_anchor_states[context_index],
            )
            if kind is None or kind.casefold() == "secret":
                kind_indent = (
                    kind_entry[1] if kind_entry is not None else len(kind_match.group("indent"))
                )
                sequence_scope = (
                    kind_entry[2]
                    if kind_entry is not None
                    else kind_match.group("dash") is not None
                )
                secret_scopes.append(
                    (
                        kind_indent,
                        sequence_scope,
                    )
                )
            kind_seen = True
            payload_indent = None
            payload_flow_depth = None
            continue
        payload_match = _YAML_SECRET_PAYLOAD_ASSIGNMENT.fullmatch(line)
        if (secret_scopes or not kind_seen) and payload_match is not None:
            continuation = _yaml_payload_continuation(payload_match.group("value"))
            if continuation is not None:
                payload_indent = len(payload_match.group("indent"))
                payload_flow_depth = continuation[1]
            continue
        if (
            indent == 0
            and _YAML_EXPLICIT_VALUE.fullmatch(line) is None
            and not (line.lstrip().startswith(("#", "-")) or _YAML_MAPPING_ENTRY.match(line))
        ):
            state_known = True
            secret_scopes.clear()
            payload_indent = None
            payload_flow_depth = None

    combined_lines = context_lines + raw.splitlines(keepends=True)
    combined_semantic_payload_lines: set[int] = set()
    combined_flags = _kubernetes_yaml_payload_flags(
        combined_lines,
        starts_inside_secret=False,
        semantic_payload_lines=combined_semantic_payload_lines,
    )
    raw_line_index = len(context_lines)
    if payload_indent is None:
        for index in range(raw_line_index - 1, -1, -1):
            payload_match = _YAML_SECRET_PAYLOAD_ASSIGNMENT.fullmatch(
                combined_lines[index].decode("utf-8", errors="replace").rstrip("\r\n")
            )
            if payload_match is None:
                continue
            candidate_indent = len(payload_match.group("indent"))
            continuation = _yaml_payload_continuation(payload_match.group("value"))
            if continuation is None:
                continue
            mode, initial_flow_depth = continuation
            following_lines = combined_lines[index + 1 : raw_line_index]
            active_flow_depth = initial_flow_depth
            if mode == "flow" and active_flow_depth is not None:
                active_flow_depth += sum(
                    _yaml_flow_delta(line.decode("utf-8", errors="replace"))
                    for line in following_lines
                )
                active = active_flow_depth > 0
            else:
                active = all(
                    not line.strip() or _line_indent(line) > candidate_indent
                    for line in following_lines
                )
            if active:
                if any(combined_flags[index : raw_line_index + 1]):
                    payload_indent = candidate_indent
                    payload_flow_depth = active_flow_depth
                break

    first_content_line = next((line for line in raw.splitlines() if line.strip()), None)
    if (
        payload_indent is None
        and first_content_line is not None
        and raw_line_index in combined_semantic_payload_lines
    ):
        payload_indent = max(-1, _line_indent(first_content_line) - 1)
    inherited_secret_scope = secret_scopes[-1] if secret_scopes else None
    if inherited_secret_scope is not None and first_content_line is not None:
        first_text = first_content_line.decode("utf-8", errors="replace").rstrip("\r\n")
        scope_indent, sequence_scope = inherited_secret_scope
        first_indent = _line_indent(first_content_line)
        if _YAML_DOCUMENT_BOUNDARY.fullmatch(first_text) or (
            sequence_scope and first_indent <= scope_indent
        ) or (not sequence_scope and first_indent < scope_indent):
            inherited_secret_scope = None
    if first_content_line is None:
        starts_inside_payload: bool | None = False
        payload_indent = None
        payload_flow_depth = None
    elif payload_indent is not None and (
        payload_flow_depth is not None or _line_indent(first_content_line) > payload_indent
    ):
        starts_inside_payload = True
    elif state_known:
        starts_inside_payload = False
        payload_indent = None
        payload_flow_depth = None
    else:
        starts_inside_payload = None
        payload_indent = None
        payload_flow_depth = None

    return (
        (
            combined_flags[raw_line_index]
            if raw_line_index < len(combined_flags)
            else bool(secret_scopes)
        )
        if state_known
        else None,
        inherited_secret_scope,
        kind_seen,
        starts_inside_payload,
        payload_indent,
        payload_flow_depth,
        yaml_anchors,
        scanned_bytes,
    )


def _kubernetes_yaml_payload_flags(
    raw_lines: list[bytes],
    *,
    starts_inside_secret: bool,
    inherited_secret_scope: tuple[int, bool] | None = None,
    inherited_kind_seen: bool = False,
    semantic_payload_lines: set[int] | None = None,
) -> list[bool]:
    """Mark lines whose YAML document has, or may have, Secret payloads."""
    flags = [False] * len(raw_lines)
    document_start = 0
    inherited_secret = starts_inside_secret
    for boundary in range(len(raw_lines) + 1):
        at_end = boundary == len(raw_lines)
        line = "" if at_end else raw_lines[boundary].decode("utf-8", errors="replace").rstrip("\r\n")
        if not at_end and _YAML_DOCUMENT_BOUNDARY.fullmatch(line) is None:
            continue
        document_lines = raw_lines[document_start:boundary]
        yaml_anchor_states = _yaml_anchor_state_by_line(document_lines, None)
        semantic_secret_payloads = _yaml_secret_payload_lines(document_lines)
        kind_entries = _yaml_kind_entries(document_lines)
        has_payload = False
        for document_line in document_lines:
            decoded = document_line.decode("utf-8", errors="replace").rstrip("\r\n")
            if _YAML_SECRET_PAYLOAD_ASSIGNMENT.fullmatch(decoded) is not None:
                has_payload = True
        if inherited_secret or (not kind_entries and has_payload and not inherited_kind_seen):
            inherited_end = len(document_lines)
            if inherited_secret_scope is not None:
                scope_indent, sequence_scope = inherited_secret_scope
                for local_index, document_line in enumerate(document_lines):
                    if not document_line.strip():
                        continue
                    indent = _line_indent(document_line)
                    if (sequence_scope and indent <= scope_indent) or (
                        not sequence_scope and indent < scope_indent
                    ):
                        inherited_end = local_index
                        break
            for index in range(document_start, document_start + inherited_end):
                flags[index] = True
        for local_index, raw_kind, kind_indent, sequence_scope in kind_entries:
            kind = _resolve_yaml_scalar(
                raw_kind,
                yaml_anchor_states[local_index],
            )
            if kind is not None and kind.casefold() != "secret":
                continue
            scope_start = local_index
            if not sequence_scope:
                for previous in range(local_index - 1, -1, -1):
                    previous_line = document_lines[previous]
                    if previous_line.strip() and _line_indent(previous_line) < kind_indent:
                        scope_start = (
                            previous if previous_line.lstrip().startswith(b"-") else previous + 1
                        )
                        break
                    scope_start = previous
            scope_end = local_index + 1
            while scope_end < len(document_lines):
                following = document_lines[scope_end]
                if following.strip():
                    following_indent = _line_indent(following)
                    if (sequence_scope and following_indent <= kind_indent) or (
                        not sequence_scope and following_indent < kind_indent
                    ):
                        break
                scope_end += 1
            for index in range(document_start + scope_start, document_start + scope_end):
                flags[index] = True
        for local_index in semantic_secret_payloads:
            flags[document_start + local_index] = True
            if semantic_payload_lines is not None:
                semantic_payload_lines.add(document_start + local_index)
        document_start = boundary + 1
        inherited_secret = False
        inherited_secret_scope = None
        inherited_kind_seen = False
    return flags


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
    starts_inside_kubernetes_secret: bool | None = False,
    kubernetes_secret_scope: tuple[int, bool] | None = None,
    kubernetes_kind_context_known: bool = False,
    starts_inside_kubernetes_secret_data: bool | None = False,
    kubernetes_secret_data_indent: int | None = None,
    kubernetes_secret_data_flow_depth: int | None = None,
    yaml_scalar_anchors: dict[str, str] | None = None,
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
    if starts_inside_kubernetes_secret is None or starts_inside_kubernetes_secret_data is None:
        warnings.append(
            "Kubernetes Secret YAML context exceeded its bounded scan; page content was omitted fail-closed."
        )
        return [(raw, "[CONTENT OMITTED: KUBERNETES SECRET CONTEXT UNKNOWN]\n", 1)] if raw else []
    decoded_raw = raw.decode("utf-8", errors="replace")
    line_breaks = decoded_raw.count("\n") + decoded_raw.count("\r") - decoded_raw.count("\r\n")
    physical_lines = line_breaks + int(
        bool(decoded_raw) and not decoded_raw.endswith(("\n", "\r"))
    )
    if physical_lines > _MAX_REDACTION_PHYSICAL_LINES:
        fast_redaction = _redact_high_line_plain_text(
            decoded_raw,
            starts_inside_private_key=starts_inside_private_key,
        )
        if fast_redaction is not None:
            safe_text, replacements = fast_redaction
            return [(raw, safe_text, replacements)] if raw else []
        warnings.append(
            "Physical line count exceeded the bounded redaction work limit; "
            "page content was omitted fail-closed."
        )
        return [(raw, "[CONTENT OMITTED: PHYSICAL LINE BOUND EXCEEDED]\n", 1)] if raw else []
    if _yaml_flow_complexity_exceeded(decoded_raw):
        warnings.append(
            "YAML flow syntax exceeded the bounded parse complexity; page content was omitted fail-closed."
        )
        return [(raw, "[CONTENT OMITTED: YAML FLOW COMPLEXITY BOUND EXCEEDED]\n", 1)] if raw else []
    if _yaml_block_complexity_exceeded(decoded_raw):
        warnings.append(
            "YAML block syntax exceeded the bounded parse complexity; page content was omitted fail-closed."
        )
        return [(raw, "[CONTENT OMITTED: YAML BLOCK COMPLEXITY BOUND EXCEEDED]\n", 1)] if raw else []
    raw_lines = raw.splitlines(keepends=True)
    semantic_sensitive_yaml_lines = _yaml_sensitive_mapping_lines(raw_lines)
    semantic_sensitive_yaml_lines.update(_yaml_sensitive_env_lines(raw_lines))
    semantic_kubernetes_payload_lines: set[int] = set()
    kubernetes_payload_flags = _kubernetes_yaml_payload_flags(
        raw_lines,
        starts_inside_secret=bool(starts_inside_kubernetes_secret),
        inherited_secret_scope=kubernetes_secret_scope,
        inherited_kind_seen=kubernetes_kind_context_known,
        semantic_payload_lines=semantic_kubernetes_payload_lines,
    )
    units: list[tuple[bytes, str, int]] = []
    yaml_anchor_states = _yaml_anchor_state_by_line(raw_lines, yaml_scalar_anchors)
    line_index = 0
    if starts_inside_kubernetes_secret_data and kubernetes_secret_data_indent is not None:
        payload_end = 0
        if kubernetes_secret_data_flow_depth is not None:
            flow_depth = kubernetes_secret_data_flow_depth
            while payload_end < len(raw_lines) and flow_depth > 0:
                flow_depth += _yaml_flow_delta(
                    raw_lines[payload_end].decode("utf-8", errors="replace")
                )
                payload_end += 1
        else:
            while payload_end < len(raw_lines):
                if (
                    raw_lines[payload_end].strip()
                    and _line_indent(raw_lines[payload_end]) <= kubernetes_secret_data_indent
                ):
                    break
                payload_end += 1
        if payload_end:
            raw_unit = b"".join(raw_lines[:payload_end])
            units.append((raw_unit, "[REDACTED SENSITIVE KUBERNETES SECRET DATA]\n", 1))
            line_index = payload_end
    elif starts_inside_sensitive_quote and sensitive_quote is not None:
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
        if sensitive_block_indent == -2:
            while block_end < len(raw_lines):
                explicit_value = _YAML_EXPLICIT_VALUE.fullmatch(
                    raw_lines[block_end].decode("utf-8", errors="replace").rstrip("\r\n")
                )
                if explicit_value is None:
                    block_end += 1
                    continue
                explicit_end = _yaml_explicit_value_end(
                    raw_lines,
                    block_end,
                    minimum_indent=0,
                    has_more_after_raw=has_more_after_raw,
                )
                block_end = explicit_end[0] if explicit_end is not None else block_end + 1
                break
        elif sensitive_block_indent == -1:
            while block_end < len(raw_lines):
                continued = _has_line_continuation(raw_lines[block_end])
                block_end += 1
                if not continued:
                    break
        else:
            while block_end < len(raw_lines):
                if raw_lines[block_end].strip():
                    block_indent = _line_indent(raw_lines[block_end])
                    same_indent_sequence = (
                        block_indent == sensitive_block_indent
                        and raw_lines[block_end].lstrip().startswith(b"-")
                    )
                    if block_indent < sensitive_block_indent or (
                        block_indent == sensitive_block_indent and not same_indent_sequence
                    ):
                        break
                block_end += 1
        if block_end:
            raw_unit = b"".join(raw_lines[:block_end])
            marker = (
                "[REDACTED SENSITIVE CONTINUATION]\n"
                if sensitive_block_indent == -1
                else "[REDACTED SENSITIVE YAML VALUE]\n"
                if sensitive_block_indent == -2
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
            value_end = value_index + 1
            explicit_value = _YAML_EXPLICIT_VALUE.fullmatch(
                raw_lines[value_index].decode("utf-8", errors="replace").rstrip("\r\n")
            )
            crossed = False
            if explicit_value is not None:
                explicit_end = _yaml_explicit_value_end(
                    raw_lines,
                    value_index,
                    minimum_indent=0,
                    has_more_after_raw=has_more_after_raw,
                )
                if explicit_end is not None:
                    value_end, crossed = explicit_end
            raw_unit = b"".join(raw_lines[:value_end])
            units.append((raw_unit, "[REDACTED SENSITIVE VALUE]\n", 1))
            if crossed:
                warnings.append(
                    "An explicit YAML sensitive value crossed the bounded page window; "
                    "its visible segment was redacted."
                )
            line_index = value_end
    inside_private_key = starts_inside_private_key
    while line_index < len(raw_lines):
        raw_unit = raw_lines[line_index]
        text_unit = raw_unit.decode("utf-8", errors="replace")
        stripped_unit = text_unit.rstrip("\r\n")
        if line_index in semantic_sensitive_yaml_lines:
            semantic_end = line_index + 1
            while semantic_end in semantic_sensitive_yaml_lines:
                semantic_end += 1
            raw_unit = b"".join(raw_lines[line_index:semantic_end])
            units.append((raw_unit, "[REDACTED SENSITIVE YAML VALUE]\n", 1))
            line_index = semantic_end
            continue
        flow_end = line_index + 1
        flow_depth = _yaml_flow_delta(stripped_unit)
        if flow_depth > 0:
            while flow_end < len(raw_lines) and flow_depth > 0:
                flow_depth += _yaml_flow_delta(
                    raw_lines[flow_end].decode("utf-8", errors="replace")
                )
                flow_end += 1
        flow_unit = b"".join(raw_lines[line_index:flow_end])
        flow_sensitivity = _yaml_flow_sensitivity(
            flow_unit.decode("utf-8", errors="replace")
        )
        if flow_sensitivity is not None:
            units.append((flow_unit, f"[REDACTED SENSITIVE {flow_sensitivity}]\n", 1))
            if flow_depth > 0 and flow_end == len(raw_lines) and has_more_after_raw:
                warnings.append(
                    "A sensitive YAML flow collection crossed the bounded page window; "
                    "its visible segment was redacted."
                )
            line_index = flow_end
            continue
        if (
            flow_end > line_index + 1
            and flow_depth <= 0
            and stripped_unit.lstrip().startswith(("{", "["))
        ):
            safe_unit, replacements = _redact_logical_text(
                flow_unit.decode("utf-8", errors="replace")
            )
            units.append((flow_unit, safe_unit, replacements))
            line_index = flow_end
            continue
        explicit_key_indent = _yaml_explicit_sensitive_key(stripped_unit)
        if explicit_key_indent is not None:
            explicit_end = _yaml_explicit_value_end(
                raw_lines,
                line_index + 1,
                minimum_indent=explicit_key_indent,
                has_more_after_raw=has_more_after_raw,
            )
            if explicit_end is None:
                value_end = line_index + 1
                crossed = False
            else:
                value_end, crossed = explicit_end
            raw_unit = b"".join(raw_lines[line_index:value_end])
            units.append((raw_unit, "[REDACTED SENSITIVE YAML EXPLICIT VALUE]\n", 1))
            if crossed:
                warnings.append(
                    "An explicit YAML sensitive value crossed the bounded page window; "
                    "its visible segment was redacted."
                )
            line_index = value_end
            continue
        env_item = _yaml_env_item(
            raw_lines,
            line_index,
            has_more_after_raw=has_more_after_raw,
            yaml_anchors=yaml_anchor_states[line_index],
        )
        if env_item is not None:
            item_end, sensitive_env, uncertain_env = env_item
            if sensitive_env or uncertain_env:
                raw_unit = b"".join(raw_lines[line_index:item_end])
                units.append((raw_unit, "[REDACTED SENSITIVE YAML ENV VALUE]\n", 1))
                if uncertain_env:
                    warnings.append(
                        "A YAML environment item crossed the bounded page window before its name; "
                        "its visible value was redacted fail-closed."
                    )
                line_index = item_end
                continue
        payload_match = _YAML_SECRET_PAYLOAD_ASSIGNMENT.fullmatch(stripped_unit)
        if line_index in semantic_kubernetes_payload_lines and payload_match is None:
            payload_end = line_index + 1
            while payload_end in semantic_kubernetes_payload_lines:
                payload_end += 1
            raw_unit = b"".join(raw_lines[line_index:payload_end])
            units.append((raw_unit, "[REDACTED SENSITIVE KUBERNETES SECRET DATA]\n", 1))
            line_index = payload_end
            continue
        if kubernetes_payload_flags[line_index] and payload_match is not None:
            payload_value = payload_match.group("value").strip()
            continuation = _yaml_payload_continuation(payload_value)
            if payload_value and continuation is None:
                units.append((raw_unit, "[REDACTED SENSITIVE KUBERNETES SECRET DATA]\n", 1))
                line_index += 1
                continue
            payload_indent = len(payload_match.group("indent"))
            payload_end = line_index + 1
            flow_depth = continuation[1] if continuation is not None else None
            if flow_depth is not None:
                while payload_end < len(raw_lines) and flow_depth > 0:
                    flow_depth += _yaml_flow_delta(
                        raw_lines[payload_end].decode("utf-8", errors="replace")
                    )
                    payload_end += 1
            else:
                while payload_end < len(raw_lines):
                    if (
                        raw_lines[payload_end].strip()
                        and _line_indent(raw_lines[payload_end]) <= payload_indent
                    ):
                        break
                    payload_end += 1
            raw_unit = b"".join(raw_lines[line_index:payload_end])
            units.append((raw_unit, "[REDACTED SENSITIVE KUBERNETES SECRET DATA]\n", 1))
            if payload_end == len(raw_lines) and has_more_after_raw:
                warnings.append(
                    "A Kubernetes Secret YAML payload crossed the bounded page window; "
                    "its visible segment was redacted."
                )
            line_index = payload_end
            continue
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
            fish_match = _FISH_SENSITIVE_ASSIGNMENT.fullmatch(text_unit.rstrip("\r\n"))
            docker_env_match = _DOCKER_ENV_SENSITIVE_ASSIGNMENT.fullmatch(
                text_unit.rstrip("\r\n")
            )
            continued_assignment = _has_line_continuation(raw_unit)
            prefixed_continuation_match = (
                _PREFIXED_PLAIN_SENSITIVE_ASSIGNMENT.search(text_unit.rstrip("\r\n"))
                if continued_assignment
                else None
            )
            if (
                plain_match is not None
                or fish_match is not None
                or docker_env_match is not None
                or prefixed_continuation_match is not None
            ):
                scalar_indent = (
                    len(plain_match.group("indent"))
                    if plain_match is not None
                    else len(fish_match.group("indent"))
                    if fish_match is not None
                    else len(docker_env_match.group("indent"))
                    if docker_env_match is not None
                    else 0
                )
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
                    if raw_lines[value_end].strip():
                        value_indent = _line_indent(raw_lines[value_end])
                        same_indent_sequence = (
                            value_indent == assignment_indent
                            and raw_lines[value_end].lstrip().startswith(b"-")
                        )
                        if value_indent < assignment_indent or (
                            value_indent == assignment_indent and not same_indent_sequence
                        ):
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
                    assignment_indent = _line_indent(raw_unit)
                    value_end = value_index + 1
                    while value_end < len(raw_lines):
                        if (
                            raw_lines[value_end].strip()
                            and _line_indent(raw_lines[value_end]) <= assignment_indent
                        ):
                            break
                        value_end += 1
                    raw_unit = b"".join(raw_lines[line_index:value_end])
                    units.append((raw_unit, "[REDACTED SENSITIVE ASSIGNMENT]\n", 1))
                    line_index = value_end
                    continue
                elif has_more_after_raw:
                    warnings.append(
                        "A sensitive assignment crossed the bounded page window; its visible key was redacted."
                    )
                    units.append((raw_unit, "[REDACTED SENSITIVE ASSIGNMENT]\n", 1))
                    line_index += 1
                    continue
            yaml_assignment = _yaml_sensitive_assignment(text_unit.rstrip("\r\n"))
            alias_assignment = _yaml_alias_sensitive_assignment(
                text_unit.rstrip("\r\n"),
                yaml_anchor_states[line_index],
            )
            if alias_assignment is not None:
                assignment_indent = alias_assignment[0]
                value_end = line_index + 1
                while value_end < len(raw_lines):
                    if (
                        raw_lines[value_end].strip()
                        and _line_indent(raw_lines[value_end]) <= assignment_indent
                    ):
                        break
                    value_end += 1
                raw_unit = b"".join(raw_lines[line_index:value_end])
                units.append((raw_unit, "[REDACTED SENSITIVE YAML ALIAS VALUE]\n", 1))
                line_index = value_end
                continue
            if yaml_assignment is not None:
                assignment_indent = yaml_assignment[0]
                value_end = line_index + 1
                while value_end < len(raw_lines):
                    if (
                        raw_lines[value_end].strip()
                        and _line_indent(raw_lines[value_end]) <= assignment_indent
                    ):
                        break
                    value_end += 1
                raw_unit = b"".join(raw_lines[line_index:value_end])
                units.append((raw_unit, "[REDACTED SENSITIVE YAML ASSIGNMENT]\n", 1))
                line_index = value_end
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
    except RecursionError as exc:
        safe_text, _ = _structured_nesting_omission(text)
        return {
            "status": "malformed",
            "error": _error_text(exc),
            "raw_excerpt": safe_text[:500],
            "record_truncated": len(safe_text) > 500,
        }
    except (TypeError, ValueError) as exc:
        safe_text, _, _ = _redact_log_content(text)
        return {
            "status": "malformed",
            "error": _error_text(exc),
            "raw_excerpt": safe_text[:500],
            "record_truncated": len(safe_text) > 500,
        }
    if _structured_depth_exceeded(parsed):
        safe_text, _ = _structured_nesting_omission(text)
        return {
            "status": "malformed",
            "error": "Structured nesting exceeds the diagnostic bound.",
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


async def _read_bounded_redis_value(
    client: Any,
    key: str,
    maximum: int,
) -> tuple[object | None, int | None, bool]:
    """Read one Redis string without materializing more than its allowed bound."""
    reported_size = int(await client.strlen(key))
    if reported_size > maximum:
        return None, reported_size, True
    bounded = await client.getrange(key, 0, maximum)
    observed_size = max(reported_size, len(bounded or b""))
    if observed_size > maximum:
        return None, observed_size, True
    if observed_size == 0 and not await client.exists(key):
        return None, None, False
    return bounded, observed_size, False


async def _pending_retries(
    redis_client: Any,
    repo_slug: str,
    cursor: str | None = None,
) -> dict[str, Any]:
    pending_key = retry_command_pending(repo_slug)
    after = _validate_retry_cursor(cursor)
    try:
        page = await redis_client.eval_ro(
            _BOUNDED_PENDING_RETRIES_SCRIPT,
            1,
            pending_key,
            "" if after is None else repr(after[0]),
            "" if after is None else after[1],
            "" if after is None else after[2],
            "" if after is None or after[3] is None else after[3],
            _MAX_PENDING_RETRIES,
            _MAX_RETRY_INDEX_MEMBER_BYTES,
            _MAX_RETRY_CURSOR_LOOKUP,
        )
    except Exception as exc:
        return {
            "status": "unavailable",
            "count": None,
            "commands": [],
            "error": _error_text(exc),
            "read_only_note": "No stale index members were pruned.",
        }
    if not isinstance(page, (list, tuple)) or len(page) != 3:
        return {
            "status": "unavailable",
            "count": None,
            "commands": [],
            "error": "Redis returned a malformed pending-Retry page.",
            "read_only_note": "No stale index members were pruned.",
        }
    try:
        total = int(page[0])
        first_index = int(page[1])
        if first_index < 0:
            raise ValueError(
                "The oversized Retry cursor member is no longer locatable within the bounded scan."
            )
        flat_rows = list(page[2])
        if len(flat_rows) % 5:
            raise ValueError("Pending Retry rows are incomplete.")
        indexed = []
        for index in range(0, len(flat_rows), 5):
            raw_id = flat_rows[index]
            score = float(flat_rows[index + 1])
            source_index = int(flat_rows[index + 2])
            size_bytes = int(flat_rows[index + 3])
            member_sha1 = flat_rows[index + 4]
            observed_size = len(raw_id if isinstance(raw_id, bytes) else str(raw_id).encode())
            if observed_size > _MAX_RETRY_INDEX_MEMBER_BYTES:
                size_bytes = max(size_bytes, observed_size)
            oversized = size_bytes > _MAX_RETRY_INDEX_MEMBER_BYTES
            if oversized and (
                not isinstance(member_sha1, str)
                or re.fullmatch(r"[0-9a-f]{40}", member_sha1) is None
            ):
                raise ValueError("Oversized pending Retry row has no bounded cursor identity.")
            indexed.append(
                (raw_id, score, source_index, size_bytes, member_sha1, oversized)
            )
    except (TypeError, ValueError) as exc:
        return {
            "status": "unavailable",
            "count": None,
            "commands": [],
            "error": _error_text(exc),
            "read_only_note": "No stale index members were pruned.",
        }
    commands: list[dict[str, Any]] = []
    for row in indexed:
        raw_id, score, source_index, member_size_bytes, _member_sha1, member_oversized = row
        if member_oversized:
            commands.append(
                {
                    "status": "oversized_index_member",
                    "index": source_index,
                    "index_score": score,
                    "source_size_bytes": member_size_bytes,
                    "read_bound_bytes": _MAX_RETRY_INDEX_MEMBER_BYTES,
                    "error": (
                        f"Stored pending Retry index member is {member_size_bytes} bytes; "
                        f"the diagnostic read bound is {_MAX_RETRY_INDEX_MEMBER_BYTES} bytes."
                    ),
                }
            )
            continue
        command_id = _decode(raw_id)
        command_key = retry_command(repo_slug, command_id)
        try:
            raw, size_bytes, oversized = await _read_bounded_redis_value(
                redis_client,
                command_key,
                _MAX_REDIS_RETRY_COMMAND_BYTES,
            )
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
        if oversized:
            commands.append(
                {
                    "status": "oversized",
                    "command_id": command_id,
                    "index_score": score,
                    "source_size_bytes": size_bytes,
                    "read_bound_bytes": _MAX_REDIS_RETRY_COMMAND_BYTES,
                    "ttl_seconds_remaining": ttl,
                    "error": (
                        f"Stored Retry command is {size_bytes} bytes; "
                        f"the diagnostic read bound is {_MAX_REDIS_RETRY_COMMAND_BYTES} bytes."
                    ),
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
    next_index = first_index + len(indexed)
    next_cursor = None
    if indexed and next_index < total:
        last_id, last_score, _last_index, _last_size, last_sha1, last_oversized = indexed[-1]
        next_cursor = _retry_cursor(
            float(last_score),
            "" if last_oversized else _decode(last_id),
            member_sha1=last_sha1 if last_oversized else None,
            member_index=_last_index if last_oversized else None,
        )
    return {
        "status": "available",
        "count": total,
        "commands": commands,
        "cursor": cursor,
        "position_at_observation": first_index,
        "page_limit": _MAX_PENDING_RETRIES,
        "truncated": next_index < total,
        "continuation": ({"next_cursor": next_cursor} if next_cursor is not None else None),
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
    scan_limit = _MAX_RUN_INDEX_ENTRIES
    try:
        page = await redis_client.eval_ro(
            _BOUNDED_RUN_INDEX_SCRIPT,
            1,
            index_key,
            scan_limit,
            _MAX_RUN_INDEX_MEMBER_BYTES,
        )
        if not isinstance(page, (list, tuple)) or len(page) != 2:
            raise RuntimeError("Redis returned a malformed bounded run-index response.")
        total = int(page[0])
        flat_rows = list(page[1])
        if len(flat_rows) % 3:
            raise RuntimeError("Redis returned incomplete bounded run-index rows.")
        indexed_ids = []
        for row in range(0, len(flat_rows), 3):
            source_index = int(flat_rows[row])
            size_bytes = int(flat_rows[row + 1])
            raw_id = flat_rows[row + 2]
            observed_size = len(raw_id if isinstance(raw_id, bytes) else str(raw_id).encode())
            if observed_size > _MAX_RUN_INDEX_MEMBER_BYTES:
                size_bytes = max(size_bytes, observed_size)
            indexed_ids.append(
                (source_index, size_bytes, None if size_bytes > _MAX_RUN_INDEX_MEMBER_BYTES else raw_id)
            )
    except Exception as exc:
        return {
            "status": "unavailable",
            "task_filter": task_id,
            "records": [],
            "error": _error_text(exc),
        }
    records: list[dict[str, Any]] = []
    missing = 0
    scanned = 0
    oversized_index_members = 0
    for source_index, member_size_bytes, raw_id in indexed_ids:
        scanned += 1
        if raw_id is None:
            oversized_index_members += 1
            records.append(
                {
                    "status": "oversized_index_member",
                    "index": source_index,
                    "source_size_bytes": member_size_bytes,
                    "read_bound_bytes": _MAX_RUN_INDEX_MEMBER_BYTES,
                    "error": (
                        f"Stored run-index member is {member_size_bytes} bytes; "
                        f"the diagnostic read bound is {_MAX_RUN_INDEX_MEMBER_BYTES} bytes."
                    ),
                }
            )
            if len(records) >= limit:
                break
            continue
        run_id = _decode(raw_id)
        try:
            raw, size_bytes, oversized = await _read_bounded_redis_value(
                redis_client,
                MetricsStore._record_key(run_id),
                _MAX_REDIS_RUN_RECORD_BYTES,
            )
        except Exception as exc:
            records.append({"status": "unavailable", "run_id": run_id, "error": _error_text(exc)})
            if len(records) >= limit:
                break
            continue
        if oversized:
            records.append(
                {
                    "status": "oversized",
                    "run_id": run_id,
                    "source_size_bytes": size_bytes,
                    "read_bound_bytes": _MAX_REDIS_RUN_RECORD_BYTES,
                    "error": (
                        f"Stored run record is {size_bytes} bytes; "
                        f"the diagnostic read bound is {_MAX_REDIS_RUN_RECORD_BYTES} bytes."
                    ),
                }
            )
            if len(records) >= limit:
                break
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
        "oversized_index_members": oversized_index_members,
        "scanned_index_entries": scanned,
        "scan_limit": scan_limit,
        "truncated": total > scanned or len(records) >= limit,
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
    retry_cursor: str | None = None,
) -> dict[str, Any]:
    """Return runtime status without mutating orchestrator state.

    Omit ``repo_slug`` for a compact overview of every configured repository.
    Supply a configured ``owner__repo`` slug for queue, inhibitor, event,
    pending-Retry, and run-record detail. Snapshot freshness is reported
    separately from progress evidence and never treated as coder liveness.
    Pass a returned pending-Retry ``next_cursor`` as ``retry_cursor`` to read
    the next bounded page.
    """
    event_limit = _validate_limit(event_limit, maximum=_MAX_STATUS_EVENTS, name="event_limit")
    run_limit = _validate_limit(run_limit, maximum=_MAX_STATUS_RUNS, name="run_limit")
    _validate_retry_cursor(retry_cursor)
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
    state_statuses = ["unavailable"] * len(repositories)
    state_errors: list[str | None] = [None] * len(repositories)
    state_sizes: list[int | None] = [None] * len(repositories)
    slugs = list(repositories)
    try:
        try:
            client = _new_redis_client()
        except Exception as exc:
            redis_status = "unavailable"
            redis_error = _error_text(exc)
            state_errors = [redis_error] * len(repositories)
        else:
            for index, slug in enumerate(slugs):
                try:
                    raw, size_bytes, oversized = await _read_bounded_redis_value(
                        client,
                        pipeline_state(slug),
                        _MAX_REDIS_STATE_BYTES,
                    )
                except Exception as exc:
                    state_errors[index] = _error_text(exc)
                    continue
                state_sizes[index] = size_bytes
                if oversized:
                    state_statuses[index] = "oversized"
                    state_errors[index] = (
                        f"Stored pipeline state is {size_bytes} bytes; "
                        f"the diagnostic read bound is {_MAX_REDIS_STATE_BYTES} bytes."
                    )
                else:
                    state_statuses[index] = "available"
                    state_raw[index] = raw
            failed_reads = [
                error
                for status, error in zip(state_statuses, state_errors, strict=True)
                if status == "unavailable"
            ]
            if failed_reads:
                redis_status = "unavailable" if len(failed_reads) == len(slugs) else "partially_available"
                redis_error = next(
                    (error for error in failed_reads if error),
                    "One or more snapshot reads failed.",
                )

        overviews: list[dict[str, Any]] = []
        states: dict[str, RepoState | None] = {}
        for index, slug in enumerate(slugs):
            overview, state = _state_overview(
                slug,
                repositories[slug],
                config,
                state_raw[index],
                redis_status=state_statuses[index],
                redis_error=state_errors[index],
                observed_at=observed_at,
            )
            overview["snapshot"]["source_size_bytes"] = state_sizes[index]
            overview["snapshot"]["read_bound_bytes"] = _MAX_REDIS_STATE_BYTES
            overviews.append(overview)
            states[slug] = state

        detail: dict[str, Any] | None = None
        if repo_slug is not None:
            state = states[repo_slug]
            if client is not None:
                events = await _recent_events(client, repo_slug, event_limit)
                retries = await _pending_retries(client, repo_slug, retry_cursor)
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
                    [item.model_dump(mode="json") for item in state.active_inhibitors]
                    if state is not None
                    else []
                ),
                "state_history": _state_history(state, event_limit),
                "recent_events": events,
                "pending_retries": retries,
                "run_records": runs,
                "coder_progress": _progress_evidence(state, runs, events),
            }

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
    finally:
        await _close_redis(client)


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


async def _read_bounded_cli_value(client: Any, key: str) -> tuple[object | None, int, bool]:
    """Read at most the retained CLI producer cap without materializing oversized values."""
    reported_size = int(await client.strlen(key))
    bounded = await client.getrange(key, 0, _MAX_REDIS_CLI_LOG_BYTES)
    observed_size = max(reported_size, len(bounded or b""))
    oversized = observed_size > _MAX_REDIS_CLI_LOG_BYTES
    return (None if oversized else bounded), observed_size, oversized


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


def _open_diagnostic_file(root: Path, *parts: str) -> tuple[Any, os.stat_result]:
    """Open a fixed diagnostic file beneath an anchored root without following symlinks."""
    if not parts or any(
        not part or part in {".", ".."} or "/" in part or "\\" in part for part in parts
    ):
        raise ValueError("Invalid diagnostic file path component.")
    directory_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW
    file_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    directory_fd = os.open(root.resolve(), directory_flags)
    file_fd: int | None = None
    try:
        for part in parts[:-1]:
            next_fd = os.open(part, directory_flags, dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = next_fd
        file_fd = os.open(parts[-1], file_flags, dir_fd=directory_fd)
        opened_stat = os.fstat(file_fd)
        if not stat_module.S_ISREG(opened_stat.st_mode):
            raise OSError("Diagnostic source is not a regular file.")
        handle = os.fdopen(file_fd, "rb")
        file_fd = None
        return handle, opened_stat
    finally:
        if file_fd is not None:
            os.close(file_fd)
        os.close(directory_fd)


async def _redis_log_sources(
    client: Any, repo_slug: str, observed_at: datetime
) -> tuple[list[dict[str, Any]], list[str]]:
    sources: list[dict[str, Any]] = []
    warnings: list[str] = []
    latest_key = cli_log_latest(repo_slug)
    try:
        latest, latest_size_bytes, latest_oversized = await _read_bounded_cli_value(
            client, latest_key
        )
        latest_ttl = int(await client.ttl(latest_key))
    except Exception as exc:
        message = _error_text(exc)
        warnings.append(f"Redis retained CLI log unavailable: {message}")
        sources.append(
            {
                "source_id": "cli:latest",
                "kind": "retained_cli_log",
                "storage": "redis",
                "availability": "unavailable",
                "error": message,
                "association": _association(),
            }
        )
    else:
        latest_exists = latest_ttl != -2
        latest_text = _decode(latest) if latest is not None else ""
        if latest_oversized:
            warnings.append(
                "Retained CLI latest log exceeds the bounded diagnostic read limit; content was not materialized."
            )
        sources.append(
            {
                "source_id": "cli:latest",
                "kind": "retained_cli_log",
                "storage": "redis",
                "availability": (
                    "oversized"
                    if latest_oversized
                    else "available"
                    if latest_exists
                    else "missing_or_expired"
                ),
                "timestamps": {
                    "recorded_at": None,
                    **_ttl_metadata(latest_ttl, observed_at),
                },
                "size_chars": len(latest_text) if latest_exists and not latest_oversized else None,
                "size_bytes": latest_size_bytes,
                "read_bound_bytes": _MAX_REDIS_CLI_LOG_BYTES,
                "retention": {
                    "producer_ttl_seconds": _CLI_LATEST_TTL_SECONDS,
                    "truncated": (
                        latest_text.startswith("[truncated]\n") if not latest_oversized else None
                    ),
                    "truncation_marker_preserved": True,
                },
                "association": _association(),
                "mutable": True,
            }
        )

    try:
        event_edges, event_count, event_size_bytes, event_oversized = await _read_bounded_event_history(
            client,
            repo_slug,
            start=0,
            stop=-1,
        )
    except Exception as exc:
        message = _error_text(exc)
        warnings.append(f"Redis event history unavailable: {message}")
        sources.append(
            {
                "source_id": "events:redis",
                "kind": "repository_event_history",
                "storage": "redis",
                "availability": "unavailable",
                "error": message,
                "association": _association(),
            }
        )
        return sources, warnings

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


async def _bounded_history_scan(
    client: Any,
    cursor: int,
    match: str,
    count: int,
) -> tuple[int, list[object], int, int]:
    """Scan history keys without returning oversized names or batches."""
    result = await client.eval_ro(
        _BOUNDED_HISTORY_SCAN_SCRIPT,
        0,
        cursor,
        match,
        count,
        _MAX_REDIS_HISTORY_KEY_BYTES,
        _MAX_REDIS_PENDING_KEYS,
    )
    if not isinstance(result, (list, tuple)) or len(result) != 4:
        raise RuntimeError("Redis returned a malformed bounded history scan.")
    next_cursor = int(result[0])
    raw_keys = result[1]
    if not isinstance(raw_keys, (list, tuple)):
        raise RuntimeError("Redis returned malformed bounded history keys.")
    oversized = int(result[2])
    dropped = int(result[3])
    bounded_keys: list[object] = []
    for raw_key in raw_keys:
        observed_size = len(raw_key if isinstance(raw_key, bytes) else str(raw_key).encode())
        if observed_size > _MAX_REDIS_HISTORY_KEY_BYTES:
            oversized += 1
        elif len(bounded_keys) < _MAX_REDIS_PENDING_KEYS:
            bounded_keys.append(raw_key)
        else:
            dropped += 1
    return next_cursor, bounded_keys, oversized, dropped


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
    oversized_keys = 0
    dropped_keys = 0
    prefix = cli_log_history(repo_slug, "")

    try:
        while len(candidates) < limit and not completed and scan_calls < _MAX_REDIS_SCAN_CALLS:
            scan_cursor, raw_keys, oversized, dropped = await _bounded_history_scan(
                client,
                scan_cursor,
                cli_log_history(repo_slug, "*"),
                max(10, limit),
            )
            oversized_keys += oversized
            dropped_keys += dropped
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
    if oversized_keys:
        warnings.append(
            f"Ignored {oversized_keys} oversized CLI history key name(s) inside Redis."
        )
    if dropped_keys:
        warnings.append(
            f"Redis returned an oversized scan batch; {dropped_keys} source identifier(s) "
            "were omitted server-side."
        )
    selected = candidates[:limit]
    remaining = candidates[limit:]
    sources: list[dict[str, Any]] = []
    for timestamp in selected:
        key = cli_log_history(repo_slug, timestamp)
        try:
            value, size_bytes, oversized = await _read_bounded_cli_value(client, key)
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
        exists = ttl != -2
        text = _decode(value) if value is not None else ""
        if oversized:
            warnings.append(
                f"Retained CLI history log {timestamp} exceeds the bounded diagnostic read limit; "
                "content was not materialized."
            )
        sources.append(
            {
                "source_id": f"{_CLI_HISTORY_SOURCE_PREFIX}{timestamp}",
                "kind": "retained_cli_log",
                "storage": "redis",
                "availability": (
                    "oversized" if oversized else "available" if exists else "missing_or_expired"
                ),
                "timestamps": {
                    "recorded_at": timestamp,
                    **_ttl_metadata(ttl, observed_at),
                },
                "size_chars": len(text) if exists and not oversized else None,
                "size_bytes": size_bytes,
                "read_bound_bytes": _MAX_REDIS_CLI_LOG_BYTES,
                "retention": {
                    "producer_ttl_seconds": _CLI_HISTORY_TTL_SECONDS,
                    "truncated": text.startswith("[truncated]\n") if not oversized else None,
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
        raw, size_bytes, oversized = await _read_bounded_cli_value(client, key)
        ttl = int(await client.ttl(key))
        if oversized:
            reason = "Retained CLI log exceeds the bounded diagnostic read limit."
            warnings.append(reason)
            return (
                None,
                {
                    "kind": "retained_cli_log",
                    "storage": "redis",
                    "availability": "oversized",
                    "size_bytes": size_bytes,
                    "read_bound_bytes": _MAX_REDIS_CLI_LOG_BYTES,
                    "timestamps": {"recorded_at": None, **_ttl_metadata(ttl, observed_at)},
                    "retention": {
                        "producer_ttl_seconds": _CLI_LATEST_TTL_SECONDS,
                        "truncation_marker_preserved": True,
                    },
                    "association": _association(),
                    "mutable": True,
                    "reason": reason,
                },
                warnings,
            )
        return (
            _decode(raw) if ttl != -2 and raw is not None else None,
            {
                "kind": "retained_cli_log",
                "storage": "redis",
                "availability": "available" if ttl != -2 else "missing_or_expired",
                "size_bytes": size_bytes,
                "read_bound_bytes": _MAX_REDIS_CLI_LOG_BYTES,
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
        raw, size_bytes, oversized = await _read_bounded_cli_value(client, key)
        ttl = int(await client.ttl(key))
        if oversized:
            reason = "Retained CLI log exceeds the bounded diagnostic read limit."
            warnings.append(reason)
            return (
                None,
                {
                    "kind": "retained_cli_log",
                    "storage": "redis",
                    "availability": "oversized",
                    "size_bytes": size_bytes,
                    "read_bound_bytes": _MAX_REDIS_CLI_LOG_BYTES,
                    "timestamps": {"recorded_at": timestamp, **_ttl_metadata(ttl, observed_at)},
                    "retention": {
                        "producer_ttl_seconds": _CLI_HISTORY_TTL_SECONDS,
                        "truncation_marker_preserved": True,
                    },
                    "association": _association(),
                    "mutable": False,
                    "reason": reason,
                },
                warnings,
            )
        return (
            _decode(raw) if ttl != -2 and raw is not None else None,
            {
                "kind": "retained_cli_log",
                "storage": "redis",
                "availability": "available" if ttl != -2 else "missing_or_expired",
                "size_bytes": size_bytes,
                "read_bound_bytes": _MAX_REDIS_CLI_LOG_BYTES,
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
        open_root = _REPOS_ROOT
        open_parts = (repo_slug, "artifacts", "ci.log")
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
        open_root = _resolve_events_dir()
        open_parts = (repo_slug, f"{date}.jsonl")
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
        handle, stat = _open_diagnostic_file(open_root, *open_parts)
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
            (
                starts_inside_kubernetes_secret,
                kubernetes_secret_scope,
                kubernetes_kind_context_known,
                starts_inside_kubernetes_secret_data,
                kubernetes_secret_data_indent,
                kubernetes_secret_data_flow_depth,
                yaml_scalar_anchors,
                kubernetes_context_scanned_bytes,
            ) = _kubernetes_yaml_state_before(handle, page_start, raw)
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
                starts_inside_kubernetes_secret=starts_inside_kubernetes_secret,
                kubernetes_secret_scope=kubernetes_secret_scope,
                kubernetes_kind_context_known=kubernetes_kind_context_known,
                starts_inside_kubernetes_secret_data=starts_inside_kubernetes_secret_data,
                kubernetes_secret_data_indent=kubernetes_secret_data_indent,
                kubernetes_secret_data_flow_depth=kubernetes_secret_data_flow_depth,
                yaml_scalar_anchors=yaml_scalar_anchors,
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
                "context_scanned_bytes": (
                    context_scanned_bytes
                    + assignment_context_scanned_bytes
                    + kubernetes_context_scanned_bytes
                ),
                "private_key_context_scanned_bytes": context_scanned_bytes,
                "sensitive_assignment_context_scanned_bytes": assignment_context_scanned_bytes,
                "kubernetes_secret_context_scanned_bytes": kubernetes_context_scanned_bytes,
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
            (
                starts_inside_kubernetes_secret,
                kubernetes_secret_scope,
                kubernetes_kind_context_known,
                starts_inside_kubernetes_secret_data,
                kubernetes_secret_data_indent,
                kubernetes_secret_data_flow_depth,
                yaml_scalar_anchors,
                kubernetes_context_scanned_bytes,
            ) = _kubernetes_yaml_state_before(handle, page_start, raw)
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
                starts_inside_kubernetes_secret=starts_inside_kubernetes_secret,
                kubernetes_secret_scope=kubernetes_secret_scope,
                kubernetes_kind_context_known=kubernetes_kind_context_known,
                starts_inside_kubernetes_secret_data=starts_inside_kubernetes_secret_data,
                kubernetes_secret_data_indent=kubernetes_secret_data_indent,
                kubernetes_secret_data_flow_depth=kubernetes_secret_data_flow_depth,
                yaml_scalar_anchors=yaml_scalar_anchors,
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
                "cursor": cursor_token if isinstance(cursor_token, str) else page_start,
                "requested_cursor": cursor_token,
                "cursor_unit": "source_byte",
                "continuation_cursor_unit": "opaque_redacted_record_character",
                "source_byte_cursor": page_start,
                "record_character_offset": input_record_char_offset,
                "max_chars": max_chars,
                "returned_chars": len(content),
                "source_size_bytes": stat.st_size,
                "scanned_bytes": scanned_bytes,
                "context_scanned_bytes": (
                    context_scanned_bytes
                    + assignment_context_scanned_bytes
                    + kubernetes_context_scanned_bytes
                ),
                "private_key_context_scanned_bytes": context_scanned_bytes,
                "sensitive_assignment_context_scanned_bytes": assignment_context_scanned_bytes,
                "kubernetes_secret_context_scanned_bytes": kubernetes_context_scanned_bytes,
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
