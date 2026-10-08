"""Bounded, read-only structured runtime diagnostics for the MCP service.

This module reads producer-owned Redis records directly.  It intentionally
does not call helpers that refresh TTLs, prune indexes, or otherwise mutate
orchestrator state.  Every returned field is selected explicitly; free-form
producer text and arbitrary payload mappings are never returned.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import re
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from html import unescape
from typing import Any
from urllib.parse import unquote_plus

import redis.asyncio as aioredis

from src.cancellation import SUBSOURCE_VOCABULARY
from src.cancellation.storage import CATEGORIES, cause_key
from src.coder_ids import validate_coder_plugin_id
from src.config import AppConfig, RepoConfig, load_config
from src.keyspace import cli_log_latest, pipeline_state, retry_command, retry_command_pending
from src.mcp.server import mcp
from src.metrics import MetricsStore, RunRecord
from src.models import RepoState
from src.queue_parser import _PR_ID_RE
from src.retry_commands import RetryCommand
from src.utils import repo_slug_from_url

_DEFAULT_REDIS_URL = "redis://localhost:6379/0"
_REDIS_TIMEOUT_SECONDS = 5.0
_REPO_SLUG = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*__[A-Za-z0-9][A-Za-z0-9_.-]*$")
_TASK_ID = _PR_ID_RE
_SHA = re.compile(r"^[0-9a-fA-F]{40}$")

_MAX_RETRIES = 20
_MAX_RUNS = 20
_MAX_INHIBITORS = 20
_MAX_RUN_INDEX_ENTRIES = 200
_MAX_STATE_BYTES = 256 * 1024
_MAX_RETRY_BYTES = 64 * 1024
_MAX_CANCELLATION_BYTES = 64 * 1024
_MAX_RUN_BYTES = 64 * 1024
_MAX_INDEX_MEMBER_BYTES = 512
# The producer budgets 64 KiB before decoding a byte tail with replacement.
# Up to three split continuation bytes can expand from one to three bytes each.
_MAX_CLI_LOG_SOURCE_BYTES = (64 * 1024) + 6
_DEFAULT_CLI_LOG_TAIL_BYTES = 8 * 1024
_MAX_CLI_LOG_TAIL_BYTES = 32 * 1024
_MAX_JSON_PARSE_FAILURES = 64
# Malformed JSON fallback performs one boundary pass and one credential-context
# pass. Bound their cumulative character work to two producer-sized passes.
_MAX_JSON_FALLBACK_SCAN_CHARACTERS = 2 * _MAX_CLI_LOG_SOURCE_BYTES
_CLI_LOG_PRODUCER_TRUNCATION_MARKER = "[truncated]\n"

_REDACTED = "[REDACTED]"
_CREDENTIAL_DOCUMENT_OMITTED = "[credential document omitted]"
_TERMINAL_CONTROL_LINE_OMITTED = "[terminal control line omitted]"
_CREDENTIAL_DOCUMENT_KEYS = frozenset(
    {
        "accesstoken",
        "accountkey",
        "accesskey",
        "apikey",
        "auth",
        "authorization",
        "authtoken",
        "awssecretaccesskey",
        "clientsecret",
        "connectionstring",
        "credential",
        "credentials",
        "cookie",
        "idtoken",
        "jwt",
        "mysqlpwd",
        "oauthtoken",
        "passphrase",
        "password",
        "passwd",
        "privatekey",
        "privatekeyid",
        "proxyauthorization",
        "refreshtoken",
        "secret",
        "secretaccesskey",
        "setcookie",
        "sharedaccesssignature",
        "storagekey",
        "token",
        "tokens",
    }
)
_EXACT_CREDENTIAL_DOCUMENT_KEYS = frozenset(
    {"clientkeydata", "secretbinary", "secretstring", "sshpass"}
)
_JWK_ASYMMETRIC_KEY_TYPES = frozenset({"ec", "okp", "rsa"})
_JWK_PRIVATE_PARAMETERS = frozenset({"d", "dp", "dq", "oth", "p", "q", "qi"})
_PEM_CREDENTIAL_BOUNDARY = re.compile(
    r"-----(?P<boundary>BEGIN|END) "
    r"(?P<label>[A-Z0-9 ]{0,64}PRIVATE KEY|PGP PRIVATE KEY BLOCK)-----",
    re.IGNORECASE,
)
_SSH2_PRIVATE_KEY_BOUNDARY = re.compile(
    r"----[ \t]+(?P<boundary>BEGIN|END)[ \t]+SSH2"
    r"(?P<label>(?:[ \t]+ENCRYPTED)?[ \t]+PRIVATE[ \t]+KEY)[ \t]+----",
    re.IGNORECASE,
)
_TERMINAL_ESCAPE = re.compile(
    r"(?:\x1b(?:\]|P|X|\^|_).*?(?:\x07|\x1b\\|$)|"
    r"(?:\x1b\[|\x9b)[0-?]*[ -/]*[@-~]|"
    r"\x1b[ -/]*[@-~])",
    re.DOTALL,
)
_TERMINAL_CSI = re.compile(r"(?:\x1b\[|\x9b)[0-?]*[ -/]*(?P<final>[@-~])")
_TERMINAL_STATEFUL_ESCAPE = re.compile(r"\x1b[78DEHM]")
_C1_CONTROL_STRING = re.compile(r"[\x90\x98\x9d-\x9f].*?(?:\x9c|\x07|$)", re.DOTALL)
_JSON_CONTAINER_START = re.compile(r"[\[{]")
_JSON_UNICODE_ESCAPE = re.compile(r"\\u(?P<codepoint>[0-9a-fA-F]{4})")
_JSON_SIMPLE_ESCAPE = re.compile(r'\\(?P<escape>["\\/bfnrt])')
_YAML_DOCUMENT_BOUNDARY = re.compile(r"(?m)^(?:---|\.\.\.)[ \t]*(?:#.*)?(?:\n|$)")
_YAML_ALIAS = re.compile(r"(?<![A-Za-z0-9_.-])\*[^\s,\[\]{}#]+")
_YAML_COMMENT = re.compile(r"(?<!\S)#")
_YAML_ONLY_ESCAPED_MAPPING_KEY = re.compile(
    r'(?m)(?:^[ \t]*(?:-[ \t]+)?|[,{][ \t]*)"'
    r'(?:[^"\\\r\n]|\\[^\r\n]|\\(?:\r\n|\n)[ \t]*)*'
    r"\\(?:[0ave _NLP]|x[0-9a-fA-F]{2}|U[0-9a-fA-F]{8})"
    r'(?:[^"\\\r\n]|\\[^\r\n]|\\(?:\r\n|\n)[ \t]*)*"[ \t]*:'
)
_YAML_EXPLICIT_MAPPING_KEY = re.compile(
    r"(?im)^[ \t]*(?:-[ \t]+)?\?[ \t]+(?P<key>[^\r\n#]+?)"
    r"[ \t]*(?:#.*)?\n(?:[ \t]*(?:#.*)?\n)*[ \t]*:"
)
_YAML_MULTILINE_EXPLICIT_QUOTED_KEY = re.compile(
    r'(?m)(?:^[ \t]*(?:-[ \t]+)?|[,{][ \t]*)\?[ \t]+"'
    r'(?=(?:[^"\\\r\n]|\\[^\r\n])*(?:\\?\r?\n))'
    r'(?:[^"\\]|\\(?:\r\n|[\s\S]))*"[ \t]*'
    r'(?::|(?:#.*)?\r?\n(?:[ \t]*(?:#.*)?\r?\n)*[ \t]*:)'
)
_YAML_MULTILINE_EXPLICIT_SINGLE_QUOTED_KEY = re.compile(
    r"(?m)(?:^[ \t]*(?:-[ \t]+)?|[,{][ \t]*)\?[ \t]+'"
    r"(?=(?:[^'\r\n]|'')*(?:\r?\n))"
    r"(?:[^']|'')*'[ \t]*"
    r"(?::|(?:#.*)?\r?\n(?:[ \t]*(?:#.*)?\r?\n)*[ \t]*:)"
)
_YAML_EXPLICIT_PLAIN_KEY_START = re.compile(
    r"^[ \t]*(?:-[ \t]+|[,{][ \t]*)?\?[ \t]+"
    r"(?P<key>(?!['\"\[{|>])[^#\r\n]+)"
)
_YAML_ALIAS_MAPPING_KEY = re.compile(
    r"(?im)(?:^[ \t]*(?:-[ \t]+)?|[,{][ \t]*)"
    r"\*[^\s,\[\]{}#]+[ \t]*:"
)
_YAML_BLOCK_VALUE_INDICATOR = re.compile(
    r"^(?:[|>](?:[1-9][+-]?|[+-][1-9]?)?)?$"
)
_XML_NAME_CHARACTERS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.:-"
)
_XML_CREDENTIAL_SELECTOR_ATTRIBUTES = frozenset({"key", "name"})
_XML_DOCTYPE = re.compile(r"(?i)<!DOCTYPE(?:\s|>)")
_XML_UNRESOLVED_NAMED_ENTITY = re.compile(r"&[A-Za-z_:][A-Za-z0-9_.:-]*;")
_YAML_NODE_PROPERTY = r"(?:&[^\s,\[\]{}]+|!<[^>\r\n]+>|![^\s,\[\]{}]*)"
_YAML_NODE_PROPERTIES = rf"(?:{_YAML_NODE_PROPERTY}[ \t]+)*"
_YAML_FLOW_NODE_PROPERTIES = rf"(?:{_YAML_NODE_PROPERTY}[ \t\r\n]+)*"
_YAML_NODE_PROPERTIES_LINE = rf"{_YAML_NODE_PROPERTIES}(?:{_YAML_NODE_PROPERTY})?[ \t]*"
_YAML_NODE_PROPERTIES_ONLY = re.compile(
    rf"^{_YAML_NODE_PROPERTY}(?:[ \t]+{_YAML_NODE_PROPERTY})*$"
)
_YAML_BLOCK_EXPLICIT_MAPPING_KEY = re.compile(
    rf"(?im)^[ \t]*(?:-[ \t]+)?\?[ \t]+{_YAML_NODE_PROPERTIES}"
    r"[|>](?:[1-9][+-]?|[+-][1-9]?)?[ \t]*(?:#.*)?(?:\r?\n|$)"
)
_KUBERNETES_KIND_KEY_SCALAR = (
    r'''(?:kind|'kind'|"kind"|'''
    r'''"(?=[^"\r\n]*\\)(?:[^"\\\r\n]|\\[^\r\n])*")'''
)
_KUBERNETES_KIND_KEY = _YAML_NODE_PROPERTIES + _KUBERNETES_KIND_KEY_SCALAR
_KUBERNETES_BLOCK_KIND_PREFIX = (
    rf"(?im)^[ \t]*(?:-[ \t]+)?{_KUBERNETES_KIND_KEY}[ \t]*:[ \t]*"
)
_KUBERNETES_SECRET_KIND = re.compile(
    _KUBERNETES_BLOCK_KIND_PREFIX
    + _YAML_NODE_PROPERTIES
    + r"(?P<quote>['\"]?)Secret(?P=quote)"
    r"[ \t]*(?:#.*)?$"
)
# YAML double-quoted scalars can resolve escapes; omit instead of partially decoding.
_KUBERNETES_ESCAPED_QUOTED_KIND = re.compile(
    _KUBERNETES_BLOCK_KIND_PREFIX
    + _YAML_NODE_PROPERTIES
    + r'"(?=[^\r\n]*\\)'
)
_KUBERNETES_SECRET_BLOCK_KIND = re.compile(
    _KUBERNETES_BLOCK_KIND_PREFIX
    + r"[|>][0-9+-]{0,2}[ \t]*(?:#.*)?\n"
    r"(?:[ \t]*\n)*[ \t]+Secret[ \t]*(?:\n|$)"
)
_KUBERNETES_MULTILINE_SECRET_KIND = re.compile(
    _KUBERNETES_BLOCK_KIND_PREFIX
    + _YAML_NODE_PROPERTIES_LINE
    + r"(?:#.*)?\n(?:[ \t]*(?:#.*)?\n|[ \t]+"
    + _YAML_NODE_PROPERTY
    + rf"(?:[ \t]+{_YAML_NODE_PROPERTY})*[ \t]*(?:#.*)?\n)*[ \t]+"
    + _YAML_NODE_PROPERTIES
    + r"(?P<multiline_quote>['\"]?)Secret(?P=multiline_quote)"
    + r"[ \t]*(?:#.*)?(?:\n|$)"
)
_KUBERNETES_FLOW_KIND_PREFIX = (
    rf"(?im)(?:^|[{{,])[ \t\r\n]*(?:\?[ \t\r\n]+)?{_KUBERNETES_KIND_KEY}"
    r"[ \t\r\n]*:[ \t\r\n]*"
    + _YAML_FLOW_NODE_PROPERTIES
)
_KUBERNETES_FLOW_SECRET_KIND = re.compile(
    _KUBERNETES_FLOW_KIND_PREFIX
    + r"(?P<flow_quote>['\"]?)Secret(?P=flow_quote)"
    + r"[ \t\r\n]*(?:#[^\r\n]*)?[ \t\r\n]*(?=[,}])"
)
_KUBERNETES_FLOW_ESCAPED_QUOTED_KIND = re.compile(
    _KUBERNETES_FLOW_KIND_PREFIX + r'"(?=[^\r\n]*\\)'
)
_KUBERNETES_ALIAS_KIND = re.compile(
    rf"(?im)(?:^[ \t]*(?:-[ \t]+)?|[{{,][ \t\r\n]*){_KUBERNETES_KIND_KEY}"
    r"[ \t\r\n]*:[ \t\r\n]*\*[^\s,\[\]{}#]+"
)
_KUBERNETES_EXPLICIT_SECRET_KIND = re.compile(
    rf"(?im)^[ \t]*(?:-[ \t]+)?\?[ \t]+{_KUBERNETES_KIND_KEY}"
    r"[ \t]*(?:#.*)?\n"
    r"(?:[ \t]*\n)*[ \t]*:[ \t]*"
    + _YAML_NODE_PROPERTIES
    + r"(?P<explicit_quote>['\"]?)Secret(?P=explicit_quote)"
    r"[ \t]*(?:#.*)?$"
)
_AWS_CREDENTIAL_CSV_HEADER = re.compile(
    r'(?i)(?:^\ufeff?|,)[ \t]*"?access key id"?[ \t]*,[ \t]*'
    r'"?secret access key"?[ \t]*(?:,|$)'
)
_PUTTY_PRIVATE_KEY_START = re.compile(
    r"(?im)^PuTTY-User-Key-File-[1-3]:[^\r\n]*(?:\r?\n|$)"
)
_PUTTY_PRIVATE_KEY_END = re.compile(
    r"(?im)^Private-(?:MAC|Hash):[^\r\n]*(?:\r?\n|$)"
)
_URL_USERINFO = re.compile(
    r"(?i)(?P<scheme>(?:\b[a-z][a-z0-9+.-]*:)?//)[^/?#\s]+@"
)
_QUERY_PARAMETER_VALUE = re.compile(
    r"(?P<separator>[?&;])(?P<name>[^=&#;\s]+)=(?P<value>[^&#;\s]+)"
)
_PROVIDER_SIGNATURE_VALUE = re.compile(
    r"(?i)(?P<prefix>(?<![A-Za-z0-9])X-(?:Amz|Goog)-Signature"
    r"[ \t]*=[ \t]*)[^&#;\s]+"
)
_AUTHORIZATION_VALUE = re.compile(
    r"(?i)\b(?P<scheme>Bearer|Basic|Digest|Negotiate|ApiKey|Token)[ \t]+\S+"
)
_DIGEST_AUTHORIZATION = re.compile(r"(?i)(?<![A-Za-z0-9])Digest[ \t]+")
_CREDENTIAL_CLI_OPTION = re.compile(
    r"(?i)(?<!\S)(?:\$?['\"])?(?:--user|--proxy-user)(?:['\"]?[ \t]+|=)"
)
_SHELL_COMMAND_WORD = re.compile(r"[^ \t;&|<>()]+|[;&|<>()]")
_AWS_CONFIGURE_SET_CREDENTIAL = re.compile(
    r"(?i)(?<!\S)(?:[^ \t;&|<>]*[\\/])?aws(?:\.exe)?[ \t]+"
    r"(?:[^ \t;&|<>()]+[ \t]+){0,16}?"
    r"configure[ \t]+set[ \t]+"
    r"(?:profile\.[A-Za-z0-9_.-]+\.)?"
    r"(?:aws_access_key_id|aws_secret_access_key|aws_session_token)"
    r"[ \t]+[^ \t;&|<>()]+"
)
_CONNECTION_STRING_PWD_VALUE = re.compile(r"(?i);[ \t]*Pwd[ \t]*=")
_CONNECTION_STRING_LEADING_PWD_VALUE = re.compile(
    r"^[ \t]*(?!PWD[ \t]*=)(?P<prefix>(?i:Pwd)[ \t]*=)"
    r"(?=[^;\r\n]*;[ \t]*(?i:Server|Data[ \t]+Source|Address|Addr|"
    r"Network[ \t]+Address|Host|Database|Initial[ \t]+Catalog|User[ \t]+ID|"
    r"UID|Port|Driver|Provider)[ \t]*=)"
)
_SHELL_IFS_VALUE_BOUNDARY_PATTERN = r"\$(?:IFS\b|\{IFS[^}\r\n]{0,64}\})"
_SHELL_IFS_VALUE_BOUNDARY = re.compile(_SHELL_IFS_VALUE_BOUNDARY_PATTERN)
_SHELL_NORMALIZED_CURL_CREDENTIAL_OPTION = re.compile(
    r"(?i)(?<!\S)(?:\$?['\"])?"
    r"(?:--_*u_*s_*e_*r_*|--_*p_*r_*o_*x_*y_*-_*u_*s_*e_*r_*)"
    rf"(?:['\"]?[ \t]+|=|{_SHELL_IFS_VALUE_BOUNDARY_PATTERN})"
)
_NETRC_PASSWORD_VALUE = re.compile(r"(?i)(?<!\S)password[ \t]+")
_NETRC_PENDING_PASSWORD_VALUE = re.compile(r"(?i)(?<!\S)password[ \t]*$")
_EMPTY_SHELL_NAME_FRAGMENT = re.compile(
    r"\$\{[A-Za-z_][A-Za-z0-9_]*:?\+\}|\$\([ \t]*\)|``"
)
_INLINE_POWERSHELL_BACKTICK = re.compile(
    r"(?<=[A-Za-z0-9_.%+\[\]])`(?=[A-Za-z0-9_.%+\[\]])"
)
_EMBEDDED_SHELL_SUBSTITUTION = re.compile(
    r"(?<![ \t;&|<>])\$(?:\(|\{|[A-Za-z_][A-Za-z0-9_]*|[0-9*@$#?!-])"
)
_WORD_LEADING_SHELL_EXPANSION = re.compile(
    r"(?<![^ \t;&|<>()])\$(?:\{[A-Za-z_][A-Za-z0-9_]*\}|"
    r"[A-Za-z_][A-Za-z0-9_]*|[0-9*@$#?!-])"
)
_WORD_LEADING_CMD_EXPANSION = re.compile(
    r"(?<![^ \t;&|<>()])(?:%[A-Za-z_][A-Za-z0-9_]*(?::[^%\r\n]{0,64})?%|"
    r"![A-Za-z_][A-Za-z0-9_]*!|%[0-9*]|"
    r"%~[A-Za-z]*(?:\$[A-Za-z_][A-Za-z0-9_]*:)?[0-9])"
)
_SHELL_BRACE_EXPANSION = re.compile(
    r"(?:(?<=[^ \t;&|<>])\{[^{}\s;&|<>]{0,64}(?:,|\.\.)"
    r"[^{}\s;&|<>]{0,64}\}|\{[^{}\s;&|<>]{0,64}(?:,|\.\.)"
    r"[^{}\s;&|<>]{0,64}\}(?=[^ \t;&|<>]))"
)
_EMBEDDED_CMD_VARIABLE = re.compile(
    r"(?<![ \t;&|<>])(?:%[A-Za-z_][A-Za-z0-9_]*(?::[^%\r\n]{0,64})?%|"
    r"![A-Za-z_][A-Za-z0-9_]*!|%[0-9*]|"
    r"%~[A-Za-z]*(?:\$[A-Za-z_][A-Za-z0-9_]*:)?[0-9])"
)
_HEREDOC_START = re.compile(
    r"<<(?P<strip_tabs>-?)[ \t]*(?P<quote>['\"]?)"
    r"(?P<delimiter>[A-Za-z0-9_.+-]+)(?P=quote)(?=$|[ \t;|&()<>])"
)
_HEREDOC_OPERATOR = re.compile(r"(?<!<)<<-?(?!<)")
_SENSITIVE_MULTIWORD_LABEL = re.compile(
    r"(?i)(?<![A-Za-z0-9])(?P<quote>['\"]?)(?:"
    r"(?:api|oauth|access|refresh|id|auth)\s+(?:key|token)|"
    r"(?:client|private)\s+(?:key|secret)|"
    r"secret(?:\s+access)?\s+key|"
    r"proxy\s+authorization|set\s+cookie"
    r")(?P=quote)\s*[=:]"
)
_SENSITIVE_KEY_CHARACTERS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-%+[]"
)
_SENSITIVE_KEY_WRAPPERS = frozenset("\\\"']")
_JSON_SIMPLE_ESCAPE_VALUES = {
    '"': '"',
    "\\": "\\",
    "/": "/",
    "b": "\b",
    "f": "\f",
    "n": "\n",
    "r": "\r",
    "t": "\t",
}


class _JSONObjectPairs(list[tuple[str, object]]):
    """JSON object members retained in source order, including duplicates."""


_JSON_DECODER = json.JSONDecoder(object_pairs_hook=_JSONObjectPairs)
_RECOGNIZABLE_SECRET = tuple(
    re.compile(pattern)
    for pattern in (
        r"\bgh[pousr]_[A-Za-z0-9]{36}\b",
        r"\bgithub_pat_[A-Za-z0-9_]{82}\b",
        r"\bsk-ant-[A-Za-z0-9_-]{30,}\b",
        r"\bsk-(?!ant-)[A-Za-z0-9_-]{48,}\b",
        r"\bAIza[A-Za-z0-9_-]{35}\b",
        r"\bxox[boaprs]-(?:[0-9]+-){2,}[A-Za-z0-9-]{24,}\b",
        r"\b(?:sk|rk)_(?:test|live)_[A-Za-z0-9]{24,}\b",
        r"\beyJ[A-Za-z0-9_-]*\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b",
        r"\b(?:AKIA|ASIA|AGPA|AIDA|AROA|AIPA|ANPA|ANVA)[A-Z0-9]{16}\b",
        r"\b(?:glpat|gloas|gldt|glrt|glrtr|glcbt|glptt|glft|glimt|"
        r"glagent|glwt|glsoat|glffct)-[A-Za-z0-9_-]{20,}\b",
        r"\bpypi-[A-Za-z0-9_-]{85,}\b",
        r"\bnpm_[A-Za-z0-9]{36}\b",
        r"\bya29\.[A-Za-z0-9._-]{20,}\b",
        r"\bhttps://hooks\.slack(?:-gov)?\.com/(?:services/)?"
        r"T[A-Z0-9]+/B[A-Z0-9]+/[A-Za-z0-9]{24}\b",
    )
)

_KNOWN_CATEGORIES = frozenset(CATEGORIES)
_KNOWN_SUBSOURCES = frozenset(SUBSOURCE_VOCABULARY)

_BOUNDED_RETRY_INDEX_SCRIPT = """
local total = redis.call('ZCARD', KEYS[1])
local rows = redis.call('ZRANGE', KEYS[1], 0, tonumber(ARGV[1]) - 1, 'WITHSCORES')
local result = {}
for index = 1, #rows, 2 do
  local member = rows[index]
  local size = string.len(member)
  table.insert(result, size)
  table.insert(result, size <= tonumber(ARGV[2]) and member or '')
  table.insert(result, rows[index + 1])
end
return {total, result}
"""

_BOUNDED_RUN_INDEX_SCRIPT = """
local total = redis.call('LLEN', KEYS[1])
local result = {}
local stop = math.min(total, tonumber(ARGV[1])) - 1
for index = 0, stop do
  local member = redis.call('LINDEX', KEYS[1], index)
  local size = string.len(member)
  table.insert(result, size)
  table.insert(result, size <= tonumber(ARGV[2]) and member or '')
end
return {total, result}
"""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_z(value: datetime) -> str:
    normalized = value.astimezone(timezone.utc)
    return normalized.isoformat().replace("+00:00", "Z")


def _timestamp(value: object) -> datetime | None:
    try:
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, str) and value:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (OverflowError, ValueError):
        return None


def _timestamp_text(value: object) -> str | None:
    parsed = _timestamp(value)
    return _iso_z(parsed) if parsed is not None else None


def _positive_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _positive_number(value: object) -> int | None:
    result = _positive_int(value)
    return result if result is not None and result > 0 else None


def _task_id(value: object) -> str | None:
    return value if isinstance(value, str) and _TASK_ID.fullmatch(value) else None


def _sha(value: object) -> str | None:
    return value.lower() if isinstance(value, str) and _SHA.fullmatch(value) else None


def _uuid(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        return str(uuid.UUID(value))
    except (ValueError, AttributeError):
        return None


def _coder(value: object) -> str | None:
    try:
        return validate_coder_plugin_id(value)
    except ValueError:
        return None


def _subsource(value: object) -> str | None:
    return value if isinstance(value, str) and value in _KNOWN_SUBSOURCES else None


def _validate_limit(value: int, *, name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ValueError(f"{name} must be an integer between 1 and {maximum}")
    return value


def _decode(raw: object) -> str:
    if isinstance(raw, bytes):
        return raw.decode("utf-8")
    if isinstance(raw, str):
        return raw
    raise TypeError("Redis value is not text")


def _new_redis_client() -> Any:
    return aioredis.from_url(
        os.environ.get("REDIS_URL", _DEFAULT_REDIS_URL),
        decode_responses=False,
        socket_connect_timeout=_REDIS_TIMEOUT_SECONDS,
        socket_timeout=_REDIS_TIMEOUT_SECONDS,
    )


async def _close_redis(client: Any | None) -> None:
    if client is not None:
        try:
            await client.aclose()
        except Exception:
            pass


def _configured_repositories() -> tuple[AppConfig, dict[str, RepoConfig]]:
    config = load_config()
    repositories: dict[str, RepoConfig] = {}
    for repo in config.repositories:
        slug = repo_slug_from_url(repo.url)
        if not _REPO_SLUG.fullmatch(slug) or slug in repositories:
            raise ValueError("configured repository identity is invalid")
        repositories[slug] = repo
    return config, repositories


def _validate_repo_slug(repo_slug: object, repositories: dict[str, RepoConfig]) -> str:
    if not isinstance(repo_slug, str) or not _REPO_SLUG.fullmatch(repo_slug):
        raise ValueError("repo_slug must be a canonical owner__repo slug")
    if repo_slug not in repositories:
        raise ValueError("repo_slug is not configured")
    return repo_slug


async def _read_bounded_string(
    client: Any,
    key: str,
    maximum: int,
) -> tuple[object | None, int | None, bool]:
    """Read one string without allowing a concurrent growth past the bound."""
    reported_size = int(await client.strlen(key))
    if reported_size > maximum:
        return None, reported_size, True
    raw = await client.getrange(key, 0, maximum)
    observed_size = len(raw if isinstance(raw, bytes) else str(raw).encode())
    if observed_size > maximum:
        return None, max(reported_size, observed_size), True
    if observed_size == 0 and not await client.exists(key):
        return None, None, False
    return raw, max(reported_size, observed_size), False


def _stale_after_seconds(config: AppConfig, repo: RepoConfig) -> int:
    return max(
        300,
        repo.poll_interval_sec * 3,
        config.daemon.idle_extended_poll_interval_sec * 2,
        config.daemon.watch_slow_poll_interval_sec * 2,
    )


def _snapshot_unavailable(status: str, code: str) -> dict[str, Any]:
    return {
        "status": status,
        "code": code,
        "source_timestamp": None,
        "age_seconds": None,
        "stale_after_seconds": None,
        "clock_skew_detected": False,
        "source_size_bytes": None,
        "read_bound_bytes": _MAX_STATE_BYTES,
        "freshness_is_coder_activity": False,
    }


def _snapshot_result(
    raw: object | None,
    *,
    size_bytes: int | None,
    oversized: bool,
    slug: str,
    repo: RepoConfig,
    config: AppConfig,
    observed_at: datetime,
) -> tuple[dict[str, Any], RepoState | None]:
    if oversized:
        result = _snapshot_unavailable("oversized", "snapshot_size_limit")
        result["source_size_bytes"] = size_bytes
        return result, None
    if raw is None:
        return _snapshot_unavailable("missing", "snapshot_missing"), None
    try:
        decoded = json.loads(_decode(raw))
        if not isinstance(decoded, dict):
            raise ValueError
        if (
            not isinstance(decoded.get("state"), str)
            or not isinstance(decoded.get("active"), bool)
            or not isinstance(decoded.get("user_paused"), bool)
            or not isinstance(decoded.get("last_updated"), str)
        ):
            raise ValueError
        current_pr = decoded.get("current_pr")
        if current_pr is not None:
            if not isinstance(current_pr, dict):
                raise ValueError
            pr_number = current_pr.get("number")
            if isinstance(pr_number, bool) or not isinstance(pr_number, int):
                raise ValueError
        source_timestamp = _timestamp(decoded["last_updated"])
        if source_timestamp is None:
            raise ValueError
        state = RepoState.model_validate(decoded)
    except Exception:
        result = _snapshot_unavailable("malformed", "snapshot_invalid")
        result["source_size_bytes"] = size_bytes
        return result, None
    if state.name != slug or state.url != repo.url:
        result = _snapshot_unavailable("malformed", "snapshot_repository_mismatch")
        result["source_size_bytes"] = size_bytes
        return result, None

    age = (observed_at - source_timestamp).total_seconds()
    stale_after = _stale_after_seconds(config, repo)
    status = "clock_skew" if age < -5 else "fresh" if age <= stale_after else "stale"
    return (
        {
            "status": status,
            "code": None,
            "source_timestamp": _iso_z(source_timestamp),
            "age_seconds": round(age, 3),
            "stale_after_seconds": stale_after,
            "clock_skew_detected": age < -5,
            "source_size_bytes": size_bytes,
            "read_bound_bytes": _MAX_STATE_BYTES,
            "freshness_is_coder_activity": False,
        },
        state,
    )


async def _read_snapshot(
    client: Any,
    slug: str,
    repo: RepoConfig,
    config: AppConfig,
    observed_at: datetime,
) -> tuple[dict[str, Any], RepoState | None]:
    try:
        raw, size_bytes, oversized = await _read_bounded_string(client, pipeline_state(slug), _MAX_STATE_BYTES)
    except Exception:
        return _snapshot_unavailable("unavailable", "snapshot_read_failed"), None
    return _snapshot_result(
        raw,
        size_bytes=size_bytes,
        oversized=oversized,
        slug=slug,
        repo=repo,
        config=config,
        observed_at=observed_at,
    )


async def _read_snapshots(
    client: Any,
    repositories: dict[str, RepoConfig],
    config: AppConfig,
    observed_at: datetime,
) -> dict[str, tuple[dict[str, Any], RepoState | None]]:
    snapshots: dict[str, tuple[dict[str, Any], RepoState | None]] = {}
    try:
        async with asyncio.timeout(_REDIS_TIMEOUT_SECONDS):
            for slug, repo in repositories.items():
                snapshots[slug] = await _read_snapshot(client, slug, repo, config, observed_at)
    except TimeoutError:
        for slug in repositories:
            snapshots.setdefault(slug, (_snapshot_unavailable("unavailable", "snapshot_read_failed"), None))
    return snapshots


def _pipeline_view(state: RepoState | None) -> dict[str, Any] | None:
    if state is None:
        return None
    integrity_codes: list[str] = []
    coder = _coder(state.coder)
    if state.coder is not None and coder is None:
        integrity_codes.append("invalid_coder")

    task = None
    if state.current_task is not None:
        task_id = _task_id(state.current_task.pr_id)
        if task_id is None:
            integrity_codes.append("invalid_current_task_id")
        else:
            task = {"id": task_id, "status": state.current_task.status.value}

    pull_request = None
    if state.current_pr is not None:
        number = _positive_number(state.current_pr.number)
        head_sha = _sha(state.current_pr.head_sha)
        if number is None:
            integrity_codes.append("invalid_current_pr_number")
        if state.current_pr.head_sha and head_sha is None:
            integrity_codes.append("invalid_current_pr_sha")
        if number is not None:
            pull_request = {
                "number": number,
                "head_sha": head_sha,
                "ci_status": state.current_pr.ci_status.value,
                "review_status": state.current_pr.review_status.value,
            }

    return {
        "state": state.state.value,
        "active": state.active,
        "paused": state.user_paused,
        "coder": coder,
        "current_task": task,
        "current_pr": pull_request,
        "integrity_codes": integrity_codes,
        "coder_activity": "unknown",
    }


def _overview(
    slug: str,
    repo: RepoConfig,
    config: AppConfig,
    snapshot: dict[str, Any],
    state: RepoState | None,
) -> dict[str, Any]:
    return {
        "repo_slug": slug,
        "configured": {
            "active": repo.active,
            "coder": repo.coder or config.daemon.coder,
        },
        "snapshot": snapshot,
        "pipeline": _pipeline_view(state),
    }


def _inhibitors(state: RepoState | None, observed_at: datetime) -> dict[str, Any]:
    if state is None:
        return {
            "status": "unavailable",
            "records": [],
            "record_count": None,
            "truncated": False,
        }
    records: list[dict[str, Any]] = []
    for inhibitor in state.active_inhibitors[:_MAX_INHIBITORS]:
        affected = _coder(inhibitor.coder_affected)
        expires = inhibitor.expires_at
        records.append(
            {
                "type": inhibitor.inhibitor_type.value,
                "coder_scope": (
                    "all" if inhibitor.coder_affected is None else affected if affected is not None else "unknown"
                ),
                "expires_at": _timestamp_text(expires),
                "expired_at_observation": expires is not None and expires <= observed_at,
            }
        )
    return {
        "status": "available",
        "records": records,
        "record_count": len(state.active_inhibitors),
        "truncated": len(state.active_inhibitors) > len(records),
    }


def _ttl_fields(ttl: int) -> dict[str, Any]:
    return {
        "ttl_seconds_remaining": ttl if ttl >= 0 else None,
        "expiry_status": "expires" if ttl >= 0 else "persistent" if ttl == -1 else "missing",
    }


async def _current_cancellation(
    client: Any,
    slug: str,
    task_id: str | None,
) -> dict[str, Any]:
    if task_id is None:
        return {"status": "not_applicable", "classification": None}
    try:
        async with asyncio.timeout(_REDIS_TIMEOUT_SECONDS):
            return await _current_cancellation_before_timeout(client, slug, task_id)
    except TimeoutError:
        return {"status": "unavailable", "code": "cancellation_read_failed", "classification": None}


async def _current_cancellation_before_timeout(
    client: Any,
    slug: str,
    task_id: str,
) -> dict[str, Any]:
    key = cause_key(slug, task_id)
    try:
        raw, size_bytes, oversized = await _read_bounded_string(client, key, _MAX_CANCELLATION_BYTES)
        ttl = int(await client.ttl(key))
    except Exception:
        return {"status": "unavailable", "code": "cancellation_read_failed", "classification": None}
    if oversized:
        return {
            "status": "oversized",
            "code": "cancellation_size_limit",
            "classification": None,
            "source_size_bytes": size_bytes,
            "read_bound_bytes": _MAX_CANCELLATION_BYTES,
            **_ttl_fields(ttl),
        }
    if raw is None:
        return {"status": "missing", "code": "cancellation_missing", "classification": None}
    try:
        decoded = json.loads(_decode(raw))
        if not isinstance(decoded, dict):
            raise ValueError
        stored_task = _task_id(decoded.get("task_id"))
        stored_repo = decoded.get("repo_slug")
        category = decoded.get("category")
        payload = decoded.get("payload")
        created_at = _timestamp_text(decoded.get("created_at"))
        if stored_task != task_id or stored_repo != slug or created_at is None:
            raise ValueError
        if not isinstance(payload, dict):
            payload = {}
    except Exception:
        return {
            "status": "malformed",
            "code": "cancellation_invalid",
            "classification": None,
            "source_size_bytes": size_bytes,
            **_ttl_fields(ttl),
        }
    return {
        "status": "available",
        "code": None,
        "classification": {
            "category": (category if isinstance(category, str) and category in _KNOWN_CATEGORIES else "unclassified"),
            "subsource": _subsource(payload.get("subsource")) or "unclassified",
            "task_id": stored_task,
            "created_at": created_at,
        },
        "source_size_bytes": size_bytes,
        "read_bound_bytes": _MAX_CANCELLATION_BYTES,
        **_ttl_fields(ttl),
    }


def _retry_metadata(command: RetryCommand, ttl: int) -> dict[str, Any] | None:
    command_id = _uuid(command.command_id)
    task_id = _task_id(command.task_id)
    requested_at = _timestamp_text(command.requested_at)
    updated_at = _timestamp_text(command.updated_at)
    if command_id is None or task_id is None or requested_at is None or updated_at is None:
        return None
    return {
        "command_id": command_id,
        "task_id": task_id,
        "status": command.status.value,
        "requested_at": requested_at,
        "updated_at": updated_at,
        "failure_subsource": _subsource(command.failure_subsource)
        or ("unclassified" if command.failure_subsource is not None else None),
        "bound_pr_number": _positive_number(command.bound_pr_number),
        "bound_pr_head_sha": _sha(command.bound_pr_head_sha),
        "retry_count": _positive_int(command.retry_count),
        "retry_cap": _positive_number(command.retry_cap),
        "processing_attempts": _positive_int(command.processing_attempts),
        "effect_stage": command.effect_stage.value,
        "execution_state": command.execution_state.value,
        **_ttl_fields(ttl),
    }


async def _pending_retries(client: Any, slug: str, limit: int) -> dict[str, Any]:
    try:
        async with asyncio.timeout(_REDIS_TIMEOUT_SECONDS):
            return await _pending_retries_before_timeout(client, slug, limit)
    except TimeoutError:
        return {
            "status": "unavailable",
            "code": "retry_record_read_failed",
            "records": [],
            "record_count": None,
            "scanned_index_entries": 0,
            "truncated": False,
        }


async def _pending_retries_before_timeout(client: Any, slug: str, limit: int) -> dict[str, Any]:
    index_key = retry_command_pending(slug)
    try:
        page = await client.eval_ro(
            _BOUNDED_RETRY_INDEX_SCRIPT,
            1,
            index_key,
            limit,
            _MAX_INDEX_MEMBER_BYTES,
        )
        if not isinstance(page, (list, tuple)) or len(page) != 2:
            raise ValueError
        total = int(page[0])
        flat_rows = list(page[1])
        if total < 0 or len(flat_rows) % 3:
            raise ValueError
    except Exception:
        return {
            "status": "unavailable",
            "code": "retry_index_read_failed",
            "records": [],
            "record_count": None,
            "scanned_index_entries": 0,
            "truncated": False,
        }

    records: list[dict[str, Any]] = []
    invalid_records = 0
    for offset in range(0, len(flat_rows), 3):
        try:
            member_size = int(flat_rows[offset])
            member = flat_rows[offset + 1]
            score = float(flat_rows[offset + 2])
            if member_size < 0 or not math.isfinite(score):
                raise ValueError
        except (TypeError, ValueError):
            invalid_records += 1
            continue
        if member_size > _MAX_INDEX_MEMBER_BYTES:
            records.append(
                {
                    "status": "oversized_index_member",
                    "source_size_bytes": member_size,
                    "read_bound_bytes": _MAX_INDEX_MEMBER_BYTES,
                }
            )
            continue
        try:
            command_id = _uuid(_decode(member))
        except (TypeError, UnicodeDecodeError):
            command_id = None
        if command_id is None:
            invalid_records += 1
            continue
        key = retry_command(slug, command_id)
        try:
            raw, size_bytes, oversized = await _read_bounded_string(client, key, _MAX_RETRY_BYTES)
            ttl = int(await client.ttl(key))
        except Exception:
            records.append({"status": "unavailable", "code": "retry_record_read_failed", "command_id": command_id})
            continue
        if oversized:
            records.append(
                {
                    "status": "oversized",
                    "code": "retry_record_size_limit",
                    "command_id": command_id,
                    "source_size_bytes": size_bytes,
                    "read_bound_bytes": _MAX_RETRY_BYTES,
                    **_ttl_fields(ttl),
                }
            )
            continue
        if raw is None:
            records.append({"status": "missing", "code": "retry_record_missing", "command_id": command_id})
            continue
        try:
            decoded = json.loads(_decode(raw))
            if not isinstance(decoded, dict):
                raise ValueError
            for field in ("bound_pr_number", "retry_count", "retry_cap", "processing_attempts"):
                value = decoded.get(field)
                if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
                    raise ValueError
            if _timestamp(decoded.get("requested_at")) is None or _timestamp(decoded.get("updated_at")) is None:
                raise ValueError
            command = RetryCommand.model_validate(decoded)
        except Exception:
            records.append(
                {
                    "status": "malformed",
                    "code": "retry_record_invalid",
                    "command_id": command_id,
                    "source_size_bytes": size_bytes,
                    **_ttl_fields(ttl),
                }
            )
            continue
        metadata = _retry_metadata(command, ttl)
        if metadata is None or metadata["command_id"] != command_id or command.repo_slug != slug:
            records.append(
                {
                    "status": "malformed",
                    "code": "retry_record_identity_mismatch",
                    "command_id": command_id,
                    "source_size_bytes": size_bytes,
                    **_ttl_fields(ttl),
                }
            )
            continue
        records.append({"status": "available", "metadata": metadata, "source_size_bytes": size_bytes})
    return {
        "status": "available",
        "code": None,
        "records": records,
        "record_count": total,
        "scanned_index_entries": len(flat_rows) // 3,
        "invalid_records_omitted": invalid_records,
        "limit": limit,
        "truncated": total > len(flat_rows) // 3,
        "read_only": True,
    }


def _run_metadata(raw: object, expected_id: str, slug: str) -> dict[str, Any] | None:
    try:
        decoded = json.loads(_decode(raw))
        if not isinstance(decoded, dict):
            raise ValueError
        record = RunRecord(**decoded)
    except Exception:
        return None
    run_id = _uuid(record.run_id)
    task_id = _task_id(record.task_id)
    if run_id != expected_id or task_id is None or record.repo_name != slug:
        return None
    started_at = _timestamp_text(record.started_at)
    ended_at = _timestamp_text(record.ended_at) if record.ended_at is not None else None
    if started_at is None or (record.ended_at is not None and ended_at is None):
        return None
    payload = asdict(record)
    if decoded.get("ended_at") is None:
        payload["outcome"] = decoded.get("outcome", "")
        payload["cause"] = decoded.get("cause")
    attempt_index = _positive_number(payload["attempt_index"])
    if attempt_index is None:
        return None
    unfinished = ended_at is None
    cause_subsource = _subsource(payload["cause_subsource"])
    return {
        "run_id": run_id,
        "task_id": task_id,
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_ms": _positive_int(payload["duration_ms"]),
        "phase": payload["run_phase"],
        "attempt_index": attempt_index,
        "fix_iterations": _positive_int(payload["fix_iterations"]),
        "outcome": "in_progress" if unfinished else payload["outcome"],
        "cause": None if unfinished else payload["cause"],
        "cause_subsource": (
            None
            if unfinished
            else cause_subsource or ("unclassified" if payload["cause_subsource"] is not None else None)
        ),
        "base_sha": _sha(payload["base_sha"]),
        "head_sha": _sha(payload["head_sha"]),
    }


async def _run_records(
    client: Any,
    slug: str,
    task_filter: str | None,
    limit: int,
) -> dict[str, Any]:
    try:
        async with asyncio.timeout(_REDIS_TIMEOUT_SECONDS):
            return await _run_records_before_timeout(client, slug, task_filter, limit)
    except TimeoutError:
        return {
            "status": "unavailable",
            "code": "run_record_read_failed",
            "task_filter": task_filter,
            "records": [],
            "record_count": None,
            "scanned_index_entries": 0,
            "truncated": False,
        }


async def _run_records_before_timeout(
    client: Any,
    slug: str,
    task_filter: str | None,
    limit: int,
) -> dict[str, Any]:
    key = MetricsStore._recent_key(task_filter or "PR", slug)
    try:
        page = await client.eval_ro(
            _BOUNDED_RUN_INDEX_SCRIPT,
            1,
            key,
            _MAX_RUN_INDEX_ENTRIES,
            _MAX_INDEX_MEMBER_BYTES,
        )
        if not isinstance(page, (list, tuple)) or len(page) != 2:
            raise ValueError
        total = int(page[0])
        flat_rows = list(page[1])
        if total < 0 or len(flat_rows) % 2:
            raise ValueError
    except Exception:
        return {
            "status": "unavailable",
            "code": "run_index_read_failed",
            "task_filter": task_filter,
            "records": [],
            "record_count": None,
            "scanned_index_entries": 0,
            "truncated": False,
        }

    records: list[dict[str, Any]] = []
    invalid_records = 0
    scanned = 0
    for offset in range(0, len(flat_rows), 2):
        if len(records) >= limit:
            break
        scanned += 1
        try:
            member_size = int(flat_rows[offset])
            member = flat_rows[offset + 1]
            if member_size < 0:
                raise ValueError
        except (TypeError, ValueError):
            invalid_records += 1
            continue
        if member_size > _MAX_INDEX_MEMBER_BYTES:
            records.append(
                {
                    "status": "oversized_index_member",
                    "source_size_bytes": member_size,
                    "read_bound_bytes": _MAX_INDEX_MEMBER_BYTES,
                }
            )
            continue
        try:
            run_id = _uuid(_decode(member))
        except (TypeError, UnicodeDecodeError):
            run_id = None
        if run_id is None:
            invalid_records += 1
            continue
        try:
            raw, size_bytes, oversized = await _read_bounded_string(
                client, MetricsStore._record_key(run_id), _MAX_RUN_BYTES
            )
        except Exception:
            records.append({"status": "unavailable", "code": "run_record_read_failed", "run_id": run_id})
            continue
        if oversized:
            records.append(
                {
                    "status": "oversized",
                    "code": "run_record_size_limit",
                    "run_id": run_id,
                    "source_size_bytes": size_bytes,
                    "read_bound_bytes": _MAX_RUN_BYTES,
                }
            )
            continue
        if raw is None:
            records.append({"status": "missing", "code": "run_record_missing", "run_id": run_id})
            continue
        metadata = _run_metadata(raw, run_id, slug)
        if metadata is None:
            records.append(
                {
                    "status": "malformed",
                    "code": "run_record_invalid",
                    "run_id": run_id,
                    "source_size_bytes": size_bytes,
                }
            )
            continue
        if task_filter is not None and metadata["task_id"] != task_filter:
            continue
        records.append({"status": "available", "metadata": metadata, "source_size_bytes": size_bytes})
    return {
        "status": "available",
        "code": None,
        "task_filter": task_filter,
        "records": records,
        "record_count": total,
        "scanned_index_entries": scanned,
        "scan_limit": _MAX_RUN_INDEX_ENTRIES,
        "invalid_records_omitted": invalid_records,
        "limit": limit,
        "truncated": total > scanned,
        "read_only": True,
    }


def _source_summary(statuses: list[str]) -> str:
    failed = sum(status == "unavailable" for status in statuses)
    if failed == 0:
        return "available"
    if failed == len(statuses):
        return "unavailable"
    return "partially_available"


def _contains_credential_document_key(value: object) -> bool:
    pending = [value]
    embedded_json_work = 0
    while pending:
        current = pending.pop()
        if isinstance(current, (_JSONObjectPairs, dict)):
            items = list(current if isinstance(current, _JSONObjectPairs) else current.items())
            if any(key == "kind" and child == "Secret" for key, child in items) and any(
                key in {"data", "stringData"} for key, _child in items
            ):
                return True
            if any(key == "keyName" and child not in (None, "", False) for key, child in items) and any(
                key == "value" and child not in (None, "", False) for key, child in items
            ):
                return True
            key_vault_secret = any(
                key == "id"
                and isinstance(child, str)
                and child.lower().startswith("https://")
                and "/secrets/" in child.lower()
                for key, child in items
            )
            if key_vault_secret and any(
                key == "value" and child not in (None, "", False)
                for key, child in items
            ):
                return True
            jwk_key_types = {
                child.lower()
                for key, child in items
                if key == "kty" and isinstance(child, str)
            }
            if (
                jwk_key_types & _JWK_ASYMMETRIC_KEY_TYPES
                and any(
                    key in _JWK_PRIVATE_PARAMETERS and child not in (None, "", False)
                    for key, child in items
                )
            ) or (
                "oct" in jwk_key_types
                and any(key == "k" and child not in (None, "", False) for key, child in items)
            ):
                return True
            for key, child in items:
                if _is_sensitive_key(str(key)) and child not in (None, "", False):
                    return True
                if isinstance(child, (dict, list, str)):
                    pending.append(child)
        elif isinstance(current, list):
            if (
                len(current) >= 2
                and isinstance(current[0], str)
                and _is_sensitive_key(current[0])
                and current[1] not in (None, "", False)
            ):
                return True
            pending.extend(child for child in current if isinstance(child, (dict, list, str)))
        elif isinstance(current, str):
            try:
                decoded = _JSON_DECODER.decode(current)
            except (json.JSONDecodeError, RecursionError, ValueError):
                pass
            else:
                if isinstance(decoded, (dict, list, str)):
                    pending.append(decoded)
                    continue

            cursor = 0
            while match := _JSON_CONTAINER_START.search(current, cursor):
                remaining_work = (
                    _MAX_JSON_FALLBACK_SCAN_CHARACTERS - embedded_json_work
                )
                boundary = _json_container_end(current, match.start(), remaining_work)
                if boundary is None:
                    return True
                end, work = boundary
                embedded_json_work += work
                try:
                    embedded = _JSON_DECODER.decode(current[match.start() : end])
                except (json.JSONDecodeError, RecursionError, ValueError):
                    cursor = max(end, match.end())
                    continue
                if isinstance(embedded, (dict, list, str)):
                    pending.append(embedded)
                cursor = max(end, match.end())
    return False


def _omit_stateful_terminal_lines(text: str) -> tuple[str, int]:
    sanitized: list[str] = []
    lines = text.split("\n")
    for index, content in enumerate(lines):
        ending = "\n" if index < len(lines) - 1 else ""
        stateful_csi = any(match.group("final") != "m" for match in _TERMINAL_CSI.finditer(content))
        if "\r" in content or stateful_csi or _TERMINAL_STATEFUL_ESCAPE.search(content):
            sanitized.append(_TERMINAL_CONTROL_LINE_OMITTED)
            return "".join(sanitized), 1
        sanitized.append(f"{content}{ending}")
    return "".join(sanitized), 0


def _normalize_terminal_text(text: str) -> tuple[str, int]:
    text = text.replace("\r\n", "\n")
    text = text.replace("\u2028", "\n").replace("\u2029", "\n")
    text, omitted_lines = _omit_stateful_terminal_lines(text)
    text, removed = _TERMINAL_ESCAPE.subn("", text)
    removed += omitted_lines
    text, c1_removed = _C1_CONTROL_STRING.subn("", text)
    removed += c1_removed
    normalized: list[str] = []
    for character in text:
        if character == "\b":
            removed += 1
            if normalized and normalized[-1] != "\n":
                normalized.pop()
        elif (
            (ord(character) < 32 and character not in {"\n", "\t"})
            or "\x7f" <= character <= "\x9f"
        ):
            removed += 1
        else:
            normalized.append(character)
    return "".join(normalized), removed


def _omit_aws_credential_csv_documents(text: str) -> tuple[str, int]:
    lines = text.splitlines(keepends=True)
    sanitized: list[str] = []
    omitted = 0
    index = 0
    while index < len(lines):
        content = lines[index].rstrip("\r\n")
        if _AWS_CREDENTIAL_CSV_HEADER.search(content) is None:
            sanitized.append(lines[index])
            index += 1
            continue
        ending = lines[index][len(content) :]
        sanitized.append(f"{_CREDENTIAL_DOCUMENT_OMITTED}{ending}")
        omitted += 1
        index += 1
        while index < len(lines):
            row = lines[index].rstrip("\r\n")
            if not row.strip() or "," not in row:
                break
            index += 1
    return "".join(sanitized), omitted


def _omit_putty_private_key_documents(text: str) -> tuple[str, int]:
    ranges: list[tuple[int, int]] = []
    cursor = 0
    while True:
        start = _PUTTY_PRIVATE_KEY_START.search(text, cursor)
        if start is None:
            break
        end = _PUTTY_PRIVATE_KEY_END.search(text, start.end())
        nested_start = _PUTTY_PRIVATE_KEY_START.search(text, start.end())
        if nested_start is not None and (
            end is None or nested_start.start() < end.start()
        ):
            ranges.append((start.start(), len(text)))
            break
        if end is None:
            ranges.append((start.start(), len(text)))
            break
        ranges.append((start.start(), end.end()))
        cursor = end.end()
    return _omit_document_ranges(text, ranges)


def _omit_pem_credential_documents(text: str) -> tuple[str, int]:
    ranges: list[tuple[int, int]] = []
    open_boundaries: list[tuple[tuple[str, str], int]] = []

    def add_range(start: int, end: int) -> None:
        if start == 0:
            ranges.clear()
            ranges.append((start, end))
        else:
            ranges.append((start, end))

    boundaries = sorted(
        [
            *((match, "pem") for match in _PEM_CREDENTIAL_BOUNDARY.finditer(text)),
            *((match, "ssh2") for match in _SSH2_PRIVATE_KEY_BOUNDARY.finditer(text)),
        ],
        key=lambda item: item[0].start(),
    )
    for match, marker_format in boundaries:
        marker = (marker_format, " ".join(match.group("label").upper().split()))
        if match.group("boundary").upper() == "BEGIN":
            open_boundaries.append((marker, match.start()))
        elif not open_boundaries:
            add_range(0, match.end())
        elif marker == open_boundaries[-1][0]:
            _closed_marker, block_start = open_boundaries.pop()
            if not open_boundaries:
                add_range(block_start, match.end())
    if open_boundaries:
        add_range(open_boundaries[0][1], len(text))
    if not ranges:
        return text, 0

    parts: list[str] = []
    offset = 0
    for start, end in ranges:
        parts.extend((text[offset:start], _CREDENTIAL_DOCUMENT_OMITTED))
        offset = end
    parts.append(text[offset:])
    return "".join(parts), len(ranges)


def _is_sensitive_key(value: str) -> bool:
    """Recognize credential keys without a backtracking expression."""
    decoded = unquote_plus(value)
    decoded = _JSON_UNICODE_ESCAPE.sub(
        lambda match: chr(int(match.group("codepoint"), 16)),
        decoded,
    )
    decoded = _JSON_SIMPLE_ESCAPE.sub(
        lambda match: _JSON_SIMPLE_ESCAPE_VALUES[match.group("escape")],
        decoded,
    ).lower()
    key = "".join(
        character
        for character in decoded
        if "a" <= character <= "z" or "0" <= character <= "9"
    )
    return key in _EXACT_CREDENTIAL_DOCUMENT_KEYS or any(
        key == sensitive or key.endswith(sensitive)
        for sensitive in _CREDENTIAL_DOCUMENT_KEYS
    )


def _is_pending_yaml_value(value: str) -> bool:
    return (
        _YAML_BLOCK_VALUE_INDICATOR.fullmatch(value) is not None
        or _YAML_NODE_PROPERTIES_ONLY.fullmatch(value) is not None
    )


def _redact_sensitive_query_values(text: str) -> tuple[str, int]:
    redactions = 0

    def redact(match: re.Match[str]) -> str:
        nonlocal redactions
        if unquote_plus(match.group("name")).lower() not in {
            "sig",
            "signature",
            "x-amz-signature",
            "x-goog-signature",
        }:
            return match.group(0)
        redactions += 1
        return f"{match.group('separator')}{match.group('name')}={_REDACTED}"

    return _QUERY_PARAMETER_VALUE.sub(redact, text), redactions


def _has_recognizable_inline_credential(text: str) -> bool:
    """Recognize post-line redactions on a reconstructed logical shell line."""
    if (
        _URL_USERINFO.search(text) is not None
        or _PROVIDER_SIGNATURE_VALUE.search(text) is not None
        or _AUTHORIZATION_VALUE.search(text) is not None
        or any(pattern.search(text) is not None for pattern in _RECOGNIZABLE_SECRET)
    ):
        return True
    _sanitized, redactions = _redact_sensitive_query_values(text)
    return redactions > 0


def _has_shell_normalized_credential(text: str) -> bool:
    """Detect credentials joined by simple shell quote or escape removal."""
    empty_fragment = _EMPTY_SHELL_NAME_FRAGMENT.search(text)
    if (
        empty_fragment is None
        and "'" not in text
        and '"' not in text
        and "\\" not in text
        and "^" not in text
        and "`" not in text
    ):
        return False
    joined = _INLINE_POWERSHELL_BACKTICK.sub(
        "", _EMPTY_SHELL_NAME_FRAGMENT.sub("", text)
    ).translate(
        {
            ord("'"): None,
            ord('"'): None,
            ord("$"): None,
            ord("\\"): None,
            ord("^"): None,
        }
    )
    return _has_recognizable_inline_credential(joined)


def _has_backslash_ansi_c_word(text: str) -> bool:
    """Fail closed for bounded ANSI-C shell words that require decoding."""
    cursor = 0
    while True:
        opening = text.find("$'", cursor)
        if opening < 0:
            return False
        escaped = False
        index = opening + 2
        while index < len(text):
            character = text[index]
            if character == "\\":
                escaped = True
                index += 2
                continue
            if character == "'":
                if escaped:
                    return True
                cursor = index + 1
                break
            index += 1
        else:
            return escaped


def _has_word_leading_expansion_credential_option(line: str) -> bool:
    """Recognize an expansion that may disappear before a credential option."""
    normalized = list(line)
    reconstructed = False
    for pattern in (_WORD_LEADING_SHELL_EXPANSION, _WORD_LEADING_CMD_EXPANSION):
        for expansion in pattern.finditer(line):
            option_start = expansion.end()
            if option_start >= len(line) or line[option_start] != "-":
                continue
            option_end = option_start
            while (
                option_end < len(line)
                and line[option_end] in _SENSITIVE_KEY_CHARACTERS
            ):
                option_end += 1
            if _is_sensitive_key(line[option_start:option_end]):
                return True
            normalized[expansion.start() : expansion.end()] = " " * len(
                expansion.group(0)
            )
            reconstructed = True
    return reconstructed and _command_specific_credential_value_start(
        "".join(normalized)
    ) is not None


def _json_key_escape_length(value: str, index: int) -> int:
    if index + 1 >= len(value) or value[index] != "\\":
        return 0
    escaped = value[index + 1]
    if escaped in _JSON_SIMPLE_ESCAPE_VALUES:
        return 2
    if (
        escaped == "u"
        and index + 6 <= len(value)
        and all(character in "0123456789abcdefABCDEF" for character in value[index + 2 : index + 6])
    ):
        return 6
    return 0


def _normalize_shell_credential_names(line: str) -> str:
    """Join simple shell word fragments without changing source offsets."""
    normalized = list(line)
    for empty_fragment in _EMPTY_SHELL_NAME_FRAGMENT.finditer(line):
        normalized[empty_fragment.start() : empty_fragment.end()] = "_" * len(
            empty_fragment.group(0)
        )
    index = 0
    while index < len(line):
        quote = line[index]
        if quote not in {"'", '"'}:
            index += 1
            continue
        closing = line.find(quote, index + 1)
        if closing < 0:
            break
        fragment = line[index + 1 : closing]
        dollar_prefix = index > 0 and normalized[index - 1] == "$"
        dollar_joins_left = (
            dollar_prefix
            and index > 1
            and normalized[index - 2] in _SENSITIVE_KEY_CHARACTERS
        )
        joins_left = (
            index > 0 and normalized[index - 1] in _SENSITIVE_KEY_CHARACTERS
        ) or dollar_joins_left
        joins_right = (
            closing + 1 < len(line)
            and line[closing + 1] in _SENSITIVE_KEY_CHARACTERS
        )
        isolated_option = (
            fragment.startswith("-")
            and (index == 0 or line[index - 1] in " \t;&|<>()")
            and (
                closing + 1 == len(line)
                or line[closing + 1] in " \t;&|<>()"
            )
        )
        if (joins_left or joins_right or isolated_option) and all(
            character in _SENSITIVE_KEY_CHARACTERS for character in fragment
        ):
            normalized[index] = "_" if joins_left else " "
            normalized[closing] = "_"
            if dollar_prefix:
                normalized[index - 1] = "_" if dollar_joins_left else " "
        index = closing + 1

    word_start = 0
    while word_start < len(line):
        while word_start < len(line) and normalized[word_start] in " \t;&|<>()":
            word_start += 1
        word_end = word_start
        while word_end < len(line) and normalized[word_end] not in " \t;&|<>()":
            word_end += 1
        prefix_start = word_start
        while prefix_start < word_end and normalized[prefix_start] in "'\"":
            prefix_start += 1
        if normalized[prefix_start : prefix_start + 2] == ["-", "-"]:
            for cursor in range(prefix_start + 2, word_end - 1):
                if (
                    line[cursor] in {"\\", "^", "`"}
                    and line[cursor + 1] in _SENSITIVE_KEY_CHARACTERS
                ):
                    normalized[cursor] = "_"
        word_start = word_end
    return "".join(normalized)


def _yaml_single_quoted_sensitive_value_start(line: str) -> int | None:
    """Return the value offset for a sensitive YAML key with doubled quotes."""
    search_start = 0
    while True:
        opening = line.find("'", search_start)
        if opening < 0:
            return None
        parts: list[str] = []
        cursor = opening + 1
        while True:
            closing = line.find("'", cursor)
            if closing < 0:
                return None
            parts.append(line[cursor:closing])
            if closing + 1 < len(line) and line[closing + 1] == "'":
                parts.append("'")
                cursor = closing + 2
                continue
            delimiter = closing + 1
            while delimiter < len(line) and line[delimiter] in " \t":
                delimiter += 1
            if (
                delimiter < len(line)
                and line[delimiter] == ":"
                and _is_sensitive_key("".join(parts))
            ):
                return delimiter + 1
            search_start = closing + 1
            break


def _ansi_c_option_value_start(line: str) -> int | None:
    """Fail closed for backslash-bearing ANSI-C fragments in long options."""
    word_start = 0
    while word_start < len(line):
        while word_start < len(line) and line[word_start] in " \t;&|<>()":
            word_start += 1
        word_end = word_start
        while word_end < len(line) and line[word_end] not in " \t;&|<>()":
            word_end += 1
        word = line[word_start:word_end].lstrip("'\"")
        marker = word.find("$'")
        possible_long_option = word.startswith("--") or word.startswith("$'--")
        if possible_long_option and marker >= 0 and "\\" in word[marker + 2 :]:
            value_start = word_end
            while value_start < len(line) and line[value_start] in " \t":
                value_start += 1
            return value_start
        word_start = word_end
    return None


def _command_specific_credential_value_start(line: str) -> int | None:
    """Scan command words once for options whose meaning depends on the executable."""
    command: str | None = None
    pending_option: str | None = None
    for match in _SHELL_COMMAND_WORD.finditer(line):
        word = match.group(0)
        if word in ";&|<>()":
            command = None
            pending_option = None
            continue

        compact_case = word.strip("'\"").replace("_", "")
        if compact_case.startswith(("$'", '$"')):
            compact_case = compact_case[2:]
        compact = compact_case.lower()

        if pending_option is not None:
            selected_option = pending_option
            pending_option = None
            if selected_option in {
                "az-password",
                "curl-user",
                "docker-password",
                "mongosh-password",
                "redis-password",
                "sshpass-password",
            }:
                return match.start()
            if selected_option == "curl-cookie" and "=" in compact:
                return match.start()
            if (
                selected_option == "curl-certificate"
                and ":" in compact
                and compact.rsplit(":", 1)[-1]
            ):
                return match.start()
            if (
                selected_option == "openssl-password"
                and compact.startswith("pass:")
                and len(compact) > len("pass:")
            ):
                return match.start()
            continue

        executable = compact.replace("\\", "/").rsplit("/", 1)[-1]
        if executable.endswith(".exe"):
            executable = executable[:-4]
        if executable in {
            "az",
            "curl",
            "docker",
            "mongosh",
            "mysql",
            "mysqladmin",
            "mysqlcheck",
            "mysqldump",
            "mysqlimport",
            "mysqlshow",
            "openssl",
            "redis-cli",
            "sshpass",
        }:
            command = executable
            pending_option = None
            continue

        if command == "curl":
            if compact in {"-b", "--cookie"}:
                pending_option = "curl-cookie"
            elif compact_case == "-E" or compact in {"--cert", "--proxy-cert"}:
                pending_option = "curl-certificate"
            elif (
                compact_case.startswith("-E")
                and ":" in compact_case[len("-E") :]
                and compact_case.rsplit(":", 1)[-1]
                or any(
                    compact.startswith(prefix)
                    and ":" in compact[len(prefix) :]
                    and compact.rsplit(":", 1)[-1]
                    for prefix in ("--cert=", "--proxy-cert=")
                )
            ):
                return match.start()
            elif compact.startswith("-") and not compact.startswith("--"):
                short_options = compact[1:]
                user_index = short_options.find("u")
                if user_index >= 0 and all(
                    character in "#0123456789:abcdefghijklmnopqrstuvwxyz"
                    for character in short_options[:user_index]
                ):
                    if short_options[user_index + 1 :]:
                        return match.start()
                    pending_option = "curl-user"
            elif (
                compact.startswith("-b")
                and "=" in compact[2:]
                or compact.startswith("--cookie=")
                and "=" in compact[len("--cookie=") :]
            ):
                return match.start()
        elif command == "openssl":
            if compact in {"-passin", "-passout"}:
                pending_option = "openssl-password"
            elif any(
                compact.startswith(prefix) and len(compact) > len(prefix)
                for prefix in ("-passin=pass:", "-passout=pass:")
            ):
                return match.start()
        elif command == "mongosh":
            if compact in {"-p", "--password"}:
                pending_option = "mongosh-password"
            elif compact.startswith("-p") and len(compact) > len("-p"):
                return match.start()
        elif command is not None and command.startswith("mysql"):
            if compact_case.startswith("-p") and len(compact_case) > len("-p"):
                return match.start()
        elif command in {"az", "docker"}:
            if compact == "login":
                command = f"{command}-login"
        elif command in {"az-login", "docker-login"}:
            if compact in {"-p", "--password"}:
                pending_option = f"{command.removesuffix('-login')}-password"
            elif (
                compact.startswith("-p")
                and len(compact) > len("-p")
                or compact.startswith("--password=")
                and len(compact) > len("--password=")
            ):
                return match.start()
        elif command == "sshpass":
            if compact == "-p":
                pending_option = "sshpass-password"
            elif compact.startswith("-p") and len(compact) > len("-p"):
                return match.start()
        elif command == "redis-cli":
            if compact in {"-a", "--pass"}:
                pending_option = "redis-password"
            elif (
                compact.startswith("-a")
                and len(compact) > len("-a")
                or compact.startswith("--pass=")
                and len(compact) > len("--pass=")
            ):
                return match.start()
    return None


def _sensitive_value_start(line: str) -> int | None:
    """Return the value position for a sensitive context found in one line."""
    yaml_single_quoted = _yaml_single_quoted_sensitive_value_start(line)
    if yaml_single_quoted is not None:
        return yaml_single_quoted
    ansi_c_option = _ansi_c_option_value_start(line)
    if ansi_c_option is not None:
        return ansi_c_option
    normalized_line = _normalize_shell_credential_names(line)
    command_credential = _command_specific_credential_value_start(normalized_line)
    if command_credential is not None:
        return command_credential
    aws_credential = _AWS_CONFIGURE_SET_CREDENTIAL.search(line)
    if aws_credential is not None:
        return aws_credential.end()
    connection_pwd = _CONNECTION_STRING_PWD_VALUE.search(line)
    if connection_pwd is not None:
        return connection_pwd.end()
    leading_connection_pwd = _CONNECTION_STRING_LEADING_PWD_VALUE.search(line)
    if leading_connection_pwd is not None:
        return leading_connection_pwd.end("prefix")
    line = normalized_line
    netrc_password = _NETRC_PASSWORD_VALUE.search(line)
    if netrc_password is not None:
        return netrc_password.end()
    pending_netrc_password = _NETRC_PENDING_PASSWORD_VALUE.search(line)
    if pending_netrc_password is not None:
        return pending_netrc_password.end()

    credential_option = _CREDENTIAL_CLI_OPTION.search(line)
    if credential_option is not None:
        return credential_option.end()
    normalized_curl_option = _SHELL_NORMALIZED_CURL_CREDENTIAL_OPTION.search(line)
    if normalized_curl_option is not None:
        return normalized_curl_option.end()

    digest = _DIGEST_AUTHORIZATION.search(line)
    if digest is not None:
        return digest.end()

    multiword = _SENSITIVE_MULTIWORD_LABEL.search(line)
    if multiword is not None:
        return multiword.end()

    index = 0
    while index < len(line):
        escape_length = _json_key_escape_length(line, index)
        if line[index] not in _SENSITIVE_KEY_CHARACTERS and escape_length == 0:
            index += 1
            continue

        start = index
        while index < len(line):
            if line[index] in _SENSITIVE_KEY_CHARACTERS:
                index += 1
                continue
            escape_length = _json_key_escape_length(line, index)
            if escape_length == 0:
                break
            index += escape_length
        candidate = line[start:index]

        cursor = index
        while cursor < len(line) and line[cursor] in _SENSITIVE_KEY_WRAPPERS:
            cursor += 1
        whitespace_start = cursor
        while cursor < len(line) and line[cursor] in " \t":
            cursor += 1

        delimiter = line[cursor] if cursor < len(line) else ""
        sensitive_delimiter = delimiter in {"=", ":", ","}
        option_value = candidate.startswith("-") and (
            cursor > whitespace_start or delimiter in {"=", ":"}
        )
        powershell_parameter = "".join(
            character
            for character in candidate[1:].lower()
            if "a" <= character <= "z" or "0" <= character <= "9"
        )
        powershell_credential_prefix = (
            candidate.startswith("-")
            and not candidate.startswith("--")
            and len(powershell_parameter) >= 4
            and any(
                sensitive.startswith(powershell_parameter)
                for sensitive in _CREDENTIAL_DOCUMENT_KEYS
            )
        )
        if _is_sensitive_key(candidate) or powershell_credential_prefix:
            if sensitive_delimiter:
                return cursor + 1
            if option_value:
                return cursor
            ifs_boundary = _SHELL_IFS_VALUE_BOUNDARY.match(line, cursor)
            if ifs_boundary is not None:
                return ifs_boundary.end()

        index = max(index, cursor)
    return None


def _line_has_sensitive_context(line: str) -> bool:
    return _sensitive_value_start(line) is not None


def _unterminated_quote(value: str, quote: str | None = None) -> str | None:
    escaped = False
    index = 0
    while index < len(value):
        character = value[index]
        if escaped:
            escaped = False
            index += 1
        elif character == "\\":
            escaped = True
            index += 1
        elif quote in {"'''", '\"\"\"'}:
            if value.startswith(quote, index):
                index += 3
                quote = None
            else:
                index += 1
        elif quote is None and (
            value.startswith("'''", index) or value.startswith('\"\"\"', index)
        ):
            quote = value[index : index + 3]
            index += 3
        elif quote is None and character in {"\"", "'"}:
            quote = character
            index += 1
        elif character == quote:
            quote = None
            index += 1
        else:
            index += 1
    return quote


def _shell_group_state(
    value: str,
    parenthesis_depth: int = 0,
    parameter_brace_depth: int = 0,
    square_bracket_depth: int = 0,
    backtick_open: bool = False,
    quote: str | None = None,
) -> tuple[int, int, int, bool, str | None]:
    """Track bounded shell grouping without interpreting commands or expansions."""
    escaped = False
    for index, character in enumerate(value):
        if escaped:
            escaped = False
            continue
        if character == "\\" and quote != "'":
            escaped = True
            continue
        if quote is not None:
            if quote == '"' and character == "(" and index > 0 and value[index - 1] == "$":
                parenthesis_depth += 1
                quote = None
            elif quote == '"' and character == "{" and index > 0 and value[index - 1] == "$":
                parameter_brace_depth += 1
                quote = None
            elif quote == '"' and character == "`":
                backtick_open = not backtick_open
                quote = None
            elif character == quote:
                quote = None
            continue
        if character in {"\"", "'"}:
            quote = character
        elif character == "`":
            backtick_open = not backtick_open
        elif character == "#" and (index == 0 or value[index - 1].isspace()):
            break
        elif character == "(":
            parenthesis_depth += 1
        elif character == ")" and parenthesis_depth:
            parenthesis_depth -= 1
        elif character == "{" and index > 0 and value[index - 1] == "$":
            parameter_brace_depth += 1
        elif character == "}" and parameter_brace_depth:
            parameter_brace_depth -= 1
        elif character == "[":
            square_bracket_depth += 1
        elif character == "]" and square_bracket_depth:
            square_bracket_depth -= 1
    return (
        parenthesis_depth,
        parameter_brace_depth,
        square_bracket_depth,
        backtick_open,
        quote,
    )


def _omit_json_string_credential_documents(text: str) -> tuple[str, int]:
    """Inspect complete JSON string tokens in one pass for encoded documents."""
    ranges: list[tuple[int, int]] = []
    cursor = 0
    while True:
        start = text.find('"', cursor)
        if start < 0:
            break
        token_cursor = start + 1
        while token_cursor < len(text):
            character = text[token_cursor]
            if character == "\\":
                token_cursor += 2
                continue
            token_cursor += 1
            if character != '"':
                continue
            try:
                value, decoded_end = _JSON_DECODER.raw_decode(text, start)
            except (json.JSONDecodeError, RecursionError, ValueError):
                pass
            else:
                if (
                    decoded_end == token_cursor
                    and isinstance(value, str)
                    and _contains_credential_document_key(value)
                ):
                    ranges.append((start, token_cursor))
            cursor = token_cursor
            break
        else:
            break
    return _omit_document_ranges(text, ranges)


def _json_container_end(
    text: str,
    start: int,
    work_limit: int,
) -> tuple[int, int] | None:
    # Charge the start once for later classification, then twice per following
    # character for boundary scanning plus classification of the same span.
    work = 1
    if work > work_limit:
        return None
    stack = [text[start]]
    in_string = False
    escaped = False
    for index in range(start + 1, len(text)):
        if work + 2 > work_limit:
            return None
        work += 2
        character = text[index]
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            continue
        if character == '"':
            in_string = True
        elif character in "[{":
            stack.append(character)
        elif character in "]}":
            expected = "[" if character == "]" else "{"
            if stack[-1] != expected:
                return index + 1, work
            stack.pop()
            if not stack:
                return index + 1, work
    return len(text), work


def _omit_json_credential_documents(text: str) -> tuple[str, int]:
    """Omit complete JSON objects that are recognizable credential records."""
    text, string_documents = _omit_json_string_credential_documents(text)
    ranges: list[tuple[int, int]] = []
    covered_until = 0
    parse_failures = 0
    fallback_scan_characters = 0
    for match in _JSON_CONTAINER_START.finditer(text):
        start = match.start()
        if start > 0 and text[start - 1] == '"':
            start -= 1
        if start < covered_until:
            continue
        try:
            value, end = _JSON_DECODER.raw_decode(text, start)
        except (json.JSONDecodeError, RecursionError, ValueError):
            parse_failures += 1
            if parse_failures >= _MAX_JSON_PARSE_FAILURES:
                return _CREDENTIAL_DOCUMENT_OMITTED, string_documents + 1
            remaining_work = (
                _MAX_JSON_FALLBACK_SCAN_CHARACTERS - fallback_scan_characters
            )
            boundary = _json_container_end(text, match.start(), remaining_work)
            if boundary is None:
                return _CREDENTIAL_DOCUMENT_OMITTED, string_documents + 1
            end, work = boundary
            fallback_scan_characters += work
            container = " ".join(text[match.start() : end].splitlines())
            if _line_has_sensitive_context(container):
                ranges.append((match.start(), end))
                covered_until = end
            continue
        covered_until = end
        if _contains_credential_document_key(value):
            ranges.append((start, end))
    if not ranges:
        return text, string_documents
    parts: list[str] = []
    offset = 0
    for start, end in ranges:
        parts.extend((text[offset:start], _CREDENTIAL_DOCUMENT_OMITTED))
        offset = end
    parts.append(text[offset:])
    return "".join(parts), string_documents + len(ranges)


def _yaml_document_ranges(text: str) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    document_start = 0
    for boundary in _YAML_DOCUMENT_BOUNDARY.finditer(text):
        ranges.append((document_start, boundary.start()))
        document_start = boundary.end()
    ranges.append((document_start, len(text)))
    return ranges


def _has_multiline_explicit_plain_yaml_key(text: str, start: int, end: int) -> bool:
    """Find multiline plain explicit keys with one monotonic bounded line scan."""
    awaiting_delimiter = False
    saw_continuation = False
    for line in text[start:end].splitlines():
        stripped = line.lstrip(" \t")
        if awaiting_delimiter:
            if stripped.startswith(":"):
                if saw_continuation:
                    return True
                awaiting_delimiter = False
            elif not stripped or stripped.startswith("#"):
                continue
            elif line.startswith((" ", "\t")):
                if any(
                    character == ":"
                    and (
                        delimiter + 1 == len(stripped)
                        or stripped[delimiter + 1] in " \t]},"
                    )
                    for delimiter, character in enumerate(stripped)
                ):
                    return True
                saw_continuation = True
                continue
            else:
                awaiting_delimiter = False

        plain_key = _YAML_EXPLICIT_PLAIN_KEY_START.match(line)
        if plain_key is not None and plain_key.group("key").strip():
            awaiting_delimiter = True
            saw_continuation = False
    return False


def _omit_document_ranges(text: str, ranges: list[tuple[int, int]]) -> tuple[str, int]:
    if not ranges:
        return text, 0
    parts: list[str] = []
    offset = 0
    for start, end in ranges:
        ending = "\n" if text[start:end].endswith("\n") else ""
        parts.extend((text[offset:start], f"{_CREDENTIAL_DOCUMENT_OMITTED}{ending}"))
        offset = end
    parts.append(text[offset:])
    return "".join(parts), len(ranges)


def _xml_tag_details(
    text: str,
    start: int,
) -> tuple[bool, str, int, int, bool] | None:
    cursor = start + 1
    closing = cursor < len(text) and text[cursor] == "/"
    if closing:
        cursor += 1
    name_start = cursor
    while cursor < len(text) and text[cursor] in _XML_NAME_CHARACTERS:
        cursor += 1
    if cursor == name_start:
        return None
    if cursor < len(text) and not text[cursor].isspace() and text[cursor] not in "/>":
        return None
    name = text[name_start:cursor]
    name_end = cursor
    quote: str | None = None
    while cursor < len(text):
        character = text[cursor]
        cursor += 1
        if quote is not None:
            if character == quote:
                quote = None
        elif character in {"'", '"'}:
            quote = character
        elif character == ">":
            break
    complete = cursor > start and text[cursor - 1] == ">"
    self_closing = complete and text[start : cursor - 1].rstrip().endswith("/")
    return closing, name, name_end, cursor, self_closing


def _xml_attributes(text: str, start: int, end: int) -> list[tuple[str, str]]:
    limit = end - 1 if end > start and text[end - 1] == ">" else end
    attributes: list[tuple[str, str]] = []
    cursor = start
    while cursor < limit:
        while cursor < limit and (text[cursor].isspace() or text[cursor] == "/"):
            cursor += 1
        name_start = cursor
        while cursor < limit and text[cursor] in _XML_NAME_CHARACTERS:
            cursor += 1
        if cursor == name_start:
            cursor += 1
            continue
        name = text[name_start:cursor]
        while cursor < limit and text[cursor].isspace():
            cursor += 1
        if cursor >= limit or text[cursor] != "=":
            continue
        cursor += 1
        while cursor < limit and text[cursor].isspace():
            cursor += 1
        quote = text[cursor] if cursor < limit and text[cursor] in {"'", '"'} else None
        if quote is not None:
            cursor += 1
            value_start = cursor
            while cursor < limit and text[cursor] != quote:
                cursor += 1
            value = text[value_start:cursor]
            cursor += cursor < limit
        else:
            value_start = cursor
            while cursor < limit and not text[cursor].isspace() and text[cursor] not in "/>":
                cursor += 1
            value = text[value_start:cursor]
        attributes.append((name, value))
    return attributes


def _xml_matching_close_end(
    text: str,
    cursor: int,
    limit: int,
    name: str,
) -> int | None:
    """Find a real same-line close tag while skipping comments and CDATA."""
    depth = 0
    while True:
        start = text.find("<", cursor, limit)
        if start < 0:
            return None
        if text.startswith("<!--", start):
            comment_end = text.find("-->", start + 4, limit)
            if comment_end < 0:
                return None
            cursor = comment_end + 3
            continue
        if text.startswith("<![CDATA[", start):
            cdata_end = text.find("]]>", start + 9, limit)
            if cdata_end < 0:
                return None
            cursor = cdata_end + 3
            continue
        details = _xml_tag_details(text, start)
        if details is None:
            return None
        closing, candidate_name, _name_end, tag_end, self_closing = details
        if tag_end > limit or text[tag_end - 1 : tag_end] != ">":
            return None
        if candidate_name == name:
            if closing:
                if depth == 0:
                    return tag_end
                depth -= 1
            elif not self_closing:
                depth += 1
        cursor = tag_end


def _xml_tag_has_credential_context(name: str, attributes: list[tuple[str, str]]) -> bool:
    if _is_sensitive_key(name.rsplit(":", 1)[-1]):
        return True
    for attribute_name, value in attributes:
        local_name = attribute_name.rsplit(":", 1)[-1]
        decoded_value = unescape(value)
        if _is_sensitive_key(local_name) or (
            local_name.lower() in _XML_CREDENTIAL_SELECTOR_ATTRIBUTES
            and (
                _is_sensitive_key(decoded_value)
                or _XML_UNRESOLVED_NAMED_ENTITY.search(decoded_value) is not None
            )
        ):
            return True
    return False


def _xml_selector_text(value: str) -> str | None:
    """Normalize XML comments and CDATA without interpreting other markup."""
    parts: list[str] = []
    cursor = 0
    while True:
        markup_start = value.find("<", cursor)
        if markup_start < 0:
            parts.append(value[cursor:])
            break
        parts.append(value[cursor:markup_start])
        if value.startswith("<!--", markup_start):
            markup_end = value.find("-->", markup_start + 4)
            if markup_end < 0:
                return None
            cursor = markup_end + 3
        elif value.startswith("<![CDATA[", markup_start):
            markup_end = value.find("]]>", markup_start + 9)
            if markup_end < 0:
                return None
            parts.append(value[markup_start + 9 : markup_end])
            cursor = markup_end + 3
        else:
            return None
    return "".join(parts)


def _xml_scalar_value_end(text: str, start: int) -> int | None:
    """Find a simple scalar value element with one monotonic tag scan."""
    cursor = start
    while cursor < len(text) and text[cursor].isspace():
        cursor += 1
    if cursor >= len(text) or text[cursor] != "<":
        return None
    details = _xml_tag_details(text, cursor)
    if details is None:
        return None
    closing, name, _name_end, tag_end, self_closing = details
    if (
        closing
        or self_closing
        or tag_end > len(text)
        or text[tag_end - 1 : tag_end] != ">"
    ):
        return None

    closing_start = text.find("<", tag_end)
    if closing_start < 0:
        return None
    closing_details = _xml_tag_details(text, closing_start)
    if closing_details is None:
        return None
    is_closing, closing_name, name_end, closing_end, _self_closing = closing_details
    if (
        not is_closing
        or closing_name != name
        or text[closing_end - 1 : closing_end] != ">"
        or text[name_end : closing_end - 1].strip()
    ):
        return None
    return closing_end


def _omit_xml_selector_credential_contexts(text: str) -> tuple[str, int]:
    """Omit plist-style XML values selected by credential key/name elements."""
    ranges: list[tuple[int, int]] = []
    cursor = 0
    while True:
        start = text.find("<", cursor)
        if start < 0:
            break
        details = _xml_tag_details(text, start)
        if details is None:
            cursor = start + 1
            continue
        closing, name, _name_end, tag_end, self_closing = details
        if (
            closing
            or self_closing
            or name.rsplit(":", 1)[-1].lower() not in _XML_CREDENTIAL_SELECTOR_ATTRIBUTES
        ):
            cursor = max(tag_end, start + 1)
            continue

        closing_start: int | None = None
        selector_end = tag_end
        search_cursor = tag_end
        while True:
            candidate = text.find("<", search_cursor)
            if candidate < 0:
                ranges.append((start, len(text)))
                return _omit_document_ranges(text, ranges)
            closing_details = _xml_tag_details(text, candidate)
            if closing_details is None:
                search_cursor = candidate + 1
                continue
            (
                candidate_closing,
                candidate_name,
                _candidate_name_end,
                candidate_end,
                _candidate_self_closing,
            ) = closing_details
            if candidate_closing and candidate_name == name:
                closing_start = candidate
                selector_end = candidate_end
                break
            search_cursor = max(candidate_end, candidate + 1)

        selector_text = _xml_selector_text(text[tag_end:closing_start])
        if selector_text is None:
            ranges.append((start, len(text)))
            break
        decoded_selector = unescape(selector_text)
        if not (
            _is_sensitive_key(decoded_selector)
            or _XML_UNRESOLVED_NAMED_ENTITY.search(decoded_selector) is not None
        ):
            cursor = selector_end
            continue
        value_end = _xml_scalar_value_end(text, selector_end)
        if value_end is None:
            ranges.append((start, len(text)))
            break
        ranges.append((start, value_end))
        cursor = value_end
    return _omit_document_ranges(text, ranges)


def _omit_xml_credential_contexts(text: str) -> tuple[str, int]:
    """Omit XML credential tags without interpreting arbitrary XML documents."""
    doctype = _XML_DOCTYPE.search(text)
    if doctype is not None:
        return _omit_document_ranges(text, [(doctype.start(), len(text))])

    ranges: list[tuple[int, int]] = []
    cursor = 0
    while True:
        start = text.find("<", cursor)
        if start < 0:
            break
        if start > 0 and text[start - 1] == "<":
            cursor = start + 1
            continue
        details = _xml_tag_details(text, start)
        if details is None:
            cursor = start + 1
            continue
        closing, name, name_end, tag_end, self_closing = details
        if closing or not _xml_tag_has_credential_context(
            name,
            _xml_attributes(text, name_end, tag_end),
        ):
            cursor = max(tag_end, start + 1)
            continue

        range_start = text.rfind("\n", 0, start) + 1
        line_end = text.find("\n", tag_end)
        range_end = len(text) if line_end < 0 else line_end + 1
        if not self_closing and _xml_matching_close_end(
            text,
            tag_end,
            range_end,
            name,
        ) is None:
            range_end = len(text)
        ranges.append((range_start, range_end))
        cursor = range_end
    return _omit_document_ranges(text, ranges)


def _omit_ambiguous_yaml_credential_documents(text: str) -> tuple[str, int]:
    """Fail closed for YAML credential values unsafe to redact line by line."""
    ranges: list[tuple[int, int]] = []
    for start, end in _yaml_document_ranges(text):
        explicit_credential_key = any(
            _is_sensitive_key(match.group("key").strip().strip("'\""))
            for match in _YAML_EXPLICIT_MAPPING_KEY.finditer(text, start, end)
        )
        if (
            _YAML_ONLY_ESCAPED_MAPPING_KEY.search(text, start, end)
            or _YAML_BLOCK_EXPLICIT_MAPPING_KEY.search(text, start, end)
            or _YAML_MULTILINE_EXPLICIT_QUOTED_KEY.search(text, start, end)
            or _YAML_MULTILINE_EXPLICIT_SINGLE_QUOTED_KEY.search(text, start, end)
            or _has_multiline_explicit_plain_yaml_key(text, start, end)
            or _YAML_ALIAS_MAPPING_KEY.search(text, start, end)
            or explicit_credential_key
        ):
            ranges.append((start, end))
            continue
        document_has_alias = _YAML_ALIAS.search(text, start, end) is not None
        for line in text[start:end].splitlines():
            value_start = _sensitive_value_start(line)
            if value_start is None:
                continue
            value = line[value_start:].lstrip()
            comment = _YAML_COMMENT.search(value)
            unsafe_comment = comment is not None and (
                _is_pending_yaml_value(value[: comment.start()].strip())
            )
            yaml_key = line[: max(value_start - 1, 0)].strip().strip("'\"")
            unsafe_flow_collection = (
                value.startswith(("[", "{"))
                and value_start > 0
                and line[value_start - 1] == ":"
                and bool(yaml_key)
                and all(
                    character in _SENSITIVE_KEY_CHARACTERS or character.isspace()
                    for character in yaml_key
                )
            )
            if unsafe_comment or unsafe_flow_collection or document_has_alias:
                ranges.append((start, end))
                break
    return _omit_document_ranges(text, ranges)


def _omit_kubernetes_secret_documents(text: str) -> tuple[str, int]:
    """Omit complete YAML documents recognizable or ambiguous as Secrets."""
    ranges = [
        (start, end)
        for start, end in _yaml_document_ranges(text)
        if _KUBERNETES_SECRET_KIND.search(text, start, end)
        or _KUBERNETES_ESCAPED_QUOTED_KIND.search(text, start, end)
        or _KUBERNETES_SECRET_BLOCK_KIND.search(text, start, end)
        or _KUBERNETES_MULTILINE_SECRET_KIND.search(text, start, end)
        or _KUBERNETES_FLOW_SECRET_KIND.search(text, start, end)
        or _KUBERNETES_FLOW_ESCAPED_QUOTED_KIND.search(text, start, end)
        or _KUBERNETES_ALIAS_KIND.search(text, start, end)
        or _KUBERNETES_EXPLICIT_SECRET_KIND.search(text, start, end)
    ]
    return _omit_document_ranges(text, ranges)


def _is_azure_key_vault_secret_value_command(line: str) -> bool:
    expected = ("keyvault", "secret", "show")
    stage = 0
    active = False
    query_pending = False
    for match in _SHELL_COMMAND_WORD.finditer(_normalize_shell_credential_names(line)):
        word = match.group(0)
        if word in ";&|<>()":
            active = False
            stage = 0
            query_pending = False
            continue
        compact = word.strip("'\"").replace("_", "").lower()
        executable = compact.replace("\\", "/").rsplit("/", 1)[-1]
        if executable.removesuffix(".exe") == "az":
            active = True
            stage = 0
            query_pending = False
            continue
        if not active:
            continue
        if stage < len(expected):
            if compact == expected[stage]:
                stage += 1
            continue
        if query_pending:
            return compact == "value"
        if compact == "--query":
            query_pending = True
        elif compact.startswith("--query="):
            return compact[len("--query=") :] == "value"
    return False


def _is_aws_secrets_manager_value_command(line: str) -> bool:
    expected = ("secretsmanager", "get-secret-value")
    stage = 0
    active = False
    query_selected = False
    text_output = False
    pending_option: str | None = None
    for match in _SHELL_COMMAND_WORD.finditer(_normalize_shell_credential_names(line)):
        word = match.group(0)
        if word in ";&|<>()":
            active = False
            stage = 0
            query_selected = False
            text_output = False
            pending_option = None
            continue
        compact = word.strip("'\"").replace("_", "").lower()
        executable = compact.replace("\\", "/").rsplit("/", 1)[-1]
        if executable.removesuffix(".exe") == "aws":
            active = True
            stage = 0
            query_selected = False
            text_output = False
            pending_option = None
            continue
        if not active:
            continue
        if stage < len(expected):
            if compact == expected[stage]:
                stage += 1
            continue
        if pending_option is not None:
            if pending_option == "query":
                query_selected = compact in {"secretbinary", "secretstring"}
            else:
                text_output = compact == "text"
            pending_option = None
        elif compact in {"--query", "--output"}:
            pending_option = compact[2:]
        elif compact.startswith("--query="):
            query_selected = compact[len("--query=") :] in {
                "secretbinary",
                "secretstring",
            }
        elif compact.startswith("--output="):
            text_output = compact[len("--output=") :] == "text"
        if query_selected and text_output:
            return True
    return False


def _omit_secret_scalar_command_output(text: str) -> tuple[str, int]:
    """Omit one-line scalars emitted by exact secret-value CLI queries."""
    lines = text.splitlines(keepends=True)
    sanitized: list[str] = []
    omitted = 0
    index = 0
    while index < len(lines):
        content = lines[index].rstrip("\r\n")
        if not (
            _is_azure_key_vault_secret_value_command(content)
            or _is_aws_secrets_manager_value_command(content)
        ):
            sanitized.append(lines[index])
            index += 1
            continue
        ending = lines[index][len(content) :]
        sanitized.append(f"[credential line omitted]{ending}")
        omitted += 1
        index += 1
        while index < len(lines) and not lines[index].strip():
            sanitized.append(lines[index])
            index += 1
        if index < len(lines):
            output = lines[index].rstrip("\r\n")
            output_ending = lines[index][len(output) :]
            sanitized.append(f"[credential line omitted]{output_ending}")
            omitted += 1
            index += 1
    return "".join(sanitized), omitted


def _omit_sensitive_context_lines(text: str) -> tuple[str, int]:
    lines = text.splitlines(keepends=True)
    sanitized: list[str] = []
    omitted = 0
    index = 0
    while index < len(lines):
        line = lines[index]
        content = line.rstrip("\r\n")
        logical_content = content
        logical_end = index + 1
        while logical_content.rstrip().endswith(("\\", "`", "^")) and logical_end < len(
            lines
        ):
            logical_content = (
                logical_content.rstrip()[:-1] + lines[logical_end].rstrip("\r\n")
            )
            logical_end += 1
        value_start = _sensitive_value_start(logical_content)
        reconstructed_credential = (
            logical_end > index + 1
            and _has_recognizable_inline_credential(logical_content)
        )
        shell_normalized_credential = _has_shell_normalized_credential(
            logical_content
        ) or _has_backslash_ansi_c_word(logical_content)
        ambiguous_shell_substitution = (
            _EMBEDDED_SHELL_SUBSTITUTION.search(logical_content) is not None
            or _SHELL_BRACE_EXPANSION.search(logical_content) is not None
            or _EMBEDDED_CMD_VARIABLE.search(logical_content) is not None
            or _has_word_leading_expansion_credential_option(logical_content)
        )
        if (
            value_start is None
            and not reconstructed_credential
            and not shell_normalized_credential
            and not ambiguous_shell_substitution
        ):
            sanitized.extend(lines[index:logical_end])
            index = logical_end
            continue
        if value_start is None:
            value_start = 0

        final_content = lines[logical_end - 1].rstrip("\r\n")
        ending = lines[logical_end - 1][len(final_content) :]
        sanitized.append(f"[credential line omitted]{ending}")
        omitted += 1
        open_quote = _unterminated_quote(logical_content)
        continued = logical_content.rstrip().endswith(("\\", "`", "^"))
        yaml_block = _is_pending_yaml_value(logical_content[value_start:].strip())
        pending_netrc_value = _NETRC_PENDING_PASSWORD_VALUE.search(logical_content) is not None
        sensitive_value = logical_content[value_start:]
        (
            shell_parenthesis_depth,
            shell_brace_depth,
            shell_bracket_depth,
            shell_backtick_open,
            shell_group_quote,
        ) = _shell_group_state(_INLINE_POWERSHELL_BACKTICK.sub("", logical_content))
        heredocs = list(_HEREDOC_START.finditer(sensitive_value))
        heredoc = heredocs[0] if len(heredocs) == 1 else None
        heredoc_delimiter = heredoc.group("delimiter") if heredoc is not None else None
        heredoc_strips_tabs = heredoc is not None and heredoc.group("strip_tabs") == "-"
        if len(heredocs) > 1 or (
            heredoc_delimiter is None and _HEREDOC_OPERATOR.search(sensitive_value)
        ):
            # An unsupported shell word could expand to an unknown delimiter. Consume
            # the remaining bounded source rather than risk exporting its body. The
            # same fail-closed rule applies to multiple ordered heredoc bodies.
            heredoc_delimiter = "\0"
        indented_block = False
        index = logical_end
        while index < len(lines):
            continuation = lines[index].rstrip("\r\n")
            if pending_netrc_value:
                if not continuation.strip() or continuation.lstrip().startswith("#"):
                    index += 1
                    continue
                index += 1
                break
            if heredoc_delimiter is not None:
                candidate = continuation.lstrip("\t") if heredoc_strips_tabs else continuation
                index += 1
                if candidate == heredoc_delimiter:
                    heredoc_delimiter = None
                continue
            if (
                shell_parenthesis_depth
                or shell_brace_depth
                or shell_bracket_depth
                or shell_backtick_open
            ):
                (
                    shell_parenthesis_depth,
                    shell_brace_depth,
                    shell_bracket_depth,
                    shell_backtick_open,
                    shell_group_quote,
                ) = _shell_group_state(
                    continuation,
                    shell_parenthesis_depth,
                    shell_brace_depth,
                    shell_bracket_depth,
                    shell_backtick_open,
                    shell_group_quote,
                )
                index += 1
                continue
            indented = continuation.startswith((" ", "\t"))
            blank_in_block = (yaml_block or indented_block) and not continuation
            comment_in_block = (yaml_block or indented_block) and (
                continuation.lstrip().startswith("#")
            )
            indentationless_sequence = yaml_block and (
                continuation == "-" or continuation.startswith("- ")
            )
            if not (
                open_quote
                or continued
                or indented
                or blank_in_block
                or comment_in_block
                or indentationless_sequence
            ):
                break
            indented_block = indented_block or indented
            open_quote = _unterminated_quote(continuation, open_quote)
            continued = continuation.rstrip().endswith(("\\", "`", "^"))
            index += 1
    return "".join(sanitized), omitted


def _sanitize_cli_log(text: str) -> tuple[str, int, int]:
    text, terminal_controls = _normalize_terminal_text(text)
    text, aws_csv_documents = _omit_aws_credential_csv_documents(text)
    text, putty_documents = _omit_putty_private_key_documents(text)
    text, xml_selector_contexts = _omit_xml_selector_credential_contexts(text)
    text, xml_contexts = _omit_xml_credential_contexts(text)
    text, json_documents = _omit_json_credential_documents(text)
    text, secret_scalar_values = _omit_secret_scalar_command_output(text)
    text, ambiguous_yaml_documents = _omit_ambiguous_yaml_credential_documents(text)
    text, kubernetes_documents = _omit_kubernetes_secret_documents(text)
    text, pem_documents = _omit_pem_credential_documents(text)
    text, credential_lines = _omit_sensitive_context_lines(text)
    redactions = (
        terminal_controls
        + aws_csv_documents
        + putty_documents
        + xml_selector_contexts
        + xml_contexts
        + ambiguous_yaml_documents
        + kubernetes_documents
        + pem_documents
        + json_documents
        + secret_scalar_values
        + credential_lines
    )

    text, count = _URL_USERINFO.subn(lambda match: f"{match.group('scheme')}{_REDACTED}@", text)
    redactions += count
    text, count = _redact_sensitive_query_values(text)
    redactions += count
    text, count = _PROVIDER_SIGNATURE_VALUE.subn(
        lambda match: f"{match.group('prefix')}{_REDACTED}",
        text,
    )
    redactions += count
    text, count = _AUTHORIZATION_VALUE.subn(
        lambda match: f"{match.group('scheme')} {_REDACTED}",
        text,
    )
    redactions += count
    for pattern in _RECOGNIZABLE_SECRET:
        text, count = pattern.subn(_REDACTED, text)
        redactions += count
    return (
        text,
        redactions,
        aws_csv_documents
        + putty_documents
        + xml_selector_contexts
        + xml_contexts
        + ambiguous_yaml_documents
        + kubernetes_documents
        + pem_documents
        + json_documents,
    )


def _utf8_tail(text: str, maximum_bytes: int) -> tuple[str, int, int, bool]:
    raw = text.encode("utf-8")
    if len(raw) <= maximum_bytes:
        return text, len(raw), 0, False
    tail = raw[-maximum_bytes:].decode("utf-8", errors="ignore")
    returned_bytes = len(tail.encode("utf-8"))
    return tail, returned_bytes, len(raw) - returned_bytes, True


def _cli_log_source(repo_slug: str) -> dict[str, Any]:
    return {
        "kind": "latest_cli_log",
        "repo_slug": repo_slug,
        "task_id": None,
        "invocation_id": None,
        "head_sha": None,
        "producer_timestamp": None,
        "association_status": "unavailable_legacy_record",
    }


def _cli_log_unavailable(
    repo_slug: str,
    observed_at: datetime,
    tail_bytes: int,
    *,
    status: str,
    code: str,
    source_size_bytes: int | None = None,
    ttl: int | None = None,
    producer_truncated: bool | None = None,
) -> dict[str, Any]:
    ttl_fields = (
        _ttl_fields(ttl)
        if ttl is not None
        else {"ttl_seconds_remaining": None, "expiry_status": "unknown"}
    )
    return {
        "schema_version": 1,
        "observed_at": _iso_z(observed_at),
        "repo_slug": repo_slug,
        "source": _cli_log_source(repo_slug),
        "availability": {
            "status": status,
            "code": code,
            "missing_may_mean_expired": status == "missing",
        },
        "text": None,
        "source_size_bytes": source_size_bytes,
        "sanitized_size_bytes": None,
        "read_bound_bytes": _MAX_CLI_LOG_SOURCE_BYTES,
        "requested_tail_bytes": tail_bytes,
        "returned_size_bytes": 0,
        "truncation": {
            "tail_truncated": None,
            "source_oversized": status == "oversized",
            "producer_truncated": producer_truncated,
            "omitted_prefix_bytes": None,
        },
        "redaction": {
            "applied": None,
            "replacement_count": None,
            "credential_documents_omitted": None,
        },
        **ttl_fields,
        "read_only": True,
    }


@mcp.tool()
async def get_latest_cli_log(
    repo_slug: str,
    tail_bytes: int = _DEFAULT_CLI_LOG_TAIL_BYTES,
) -> dict[str, Any]:
    """Return a bounded, sanitized tail of one configured repository's latest CLI log.

    The legacy record has no trustworthy task, invocation, commit, or producer
    timestamp. ``observed_at`` is the read time, not the time the log was made.
    This fixed-key read never refreshes the record's TTL or accesses log files.
    """
    tail_bytes = _validate_limit(tail_bytes, name="tail_bytes", maximum=_MAX_CLI_LOG_TAIL_BYTES)
    observed_at = _utc_now()
    try:
        _, repositories = _configured_repositories()
    except Exception:
        return _cli_log_unavailable(
            repo_slug,
            observed_at,
            tail_bytes,
            status="unavailable",
            code="configuration_invalid",
        )
    repo_slug = _validate_repo_slug(repo_slug, repositories)

    client: Any | None = None
    try:
        try:
            client = _new_redis_client()
        except Exception:
            return _cli_log_unavailable(
                repo_slug,
                observed_at,
                tail_bytes,
                status="unavailable",
                code="redis_connection_failed",
            )
        try:
            async with asyncio.timeout(_REDIS_TIMEOUT_SECONDS):
                raw, source_size_bytes, oversized = await _read_bounded_string(
                    client,
                    cli_log_latest(repo_slug),
                    _MAX_CLI_LOG_SOURCE_BYTES,
                )
                ttl = int(await client.ttl(cli_log_latest(repo_slug)))
        except Exception:
            return _cli_log_unavailable(
                repo_slug,
                observed_at,
                tail_bytes,
                status="unavailable",
                code="cli_log_read_failed",
            )
        if oversized:
            return _cli_log_unavailable(
                repo_slug,
                observed_at,
                tail_bytes,
                status="oversized",
                code="cli_log_source_size_limit",
                source_size_bytes=source_size_bytes,
                ttl=ttl,
            )
        if raw is None:
            return _cli_log_unavailable(
                repo_slug,
                observed_at,
                tail_bytes,
                status="missing",
                code="cli_log_missing_or_expired",
                ttl=ttl,
            )

        decoded = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
        if decoded.startswith(_CLI_LOG_PRODUCER_TRUNCATION_MARKER):
            return _cli_log_unavailable(
                repo_slug,
                observed_at,
                tail_bytes,
                status="unavailable",
                code="cli_log_producer_truncated",
                source_size_bytes=source_size_bytes,
                ttl=ttl,
                producer_truncated=True,
            )
        sanitized, replacement_count, documents_omitted = _sanitize_cli_log(decoded)
        sanitized_size_bytes = len(sanitized.encode("utf-8"))
        text, returned_size_bytes, omitted_prefix_bytes, tail_truncated = _utf8_tail(sanitized, tail_bytes)
        return {
            "schema_version": 1,
            "observed_at": _iso_z(observed_at),
            "repo_slug": repo_slug,
            "source": _cli_log_source(repo_slug),
            "availability": {
                "status": "available",
                "code": None,
                "missing_may_mean_expired": False,
            },
            "text": text,
            "source_size_bytes": source_size_bytes,
            "sanitized_size_bytes": sanitized_size_bytes,
            "read_bound_bytes": _MAX_CLI_LOG_SOURCE_BYTES,
            "requested_tail_bytes": tail_bytes,
            "returned_size_bytes": returned_size_bytes,
            "truncation": {
                "tail_truncated": tail_truncated,
                "source_oversized": False,
                "producer_truncated": False,
                "omitted_prefix_bytes": omitted_prefix_bytes,
            },
            "redaction": {
                "applied": replacement_count > 0,
                "replacement_count": replacement_count,
                "credential_documents_omitted": documents_omitted,
            },
            **_ttl_fields(ttl),
            "read_only": True,
        }
    finally:
        await _close_redis(client)


@mcp.tool()
async def get_orchestrator_status(
    repo_slug: str | None = None,
    retry_limit: int = 5,
    run_limit: int = 5,
) -> dict[str, Any]:
    """Return allowlisted runtime status without mutating orchestrator state.

    Omit ``repo_slug`` for a compact configured-repository overview.  Supply a
    configured ``owner__repo`` slug for bounded Retry, cancellation, inhibitor,
    and run-record metadata.  Snapshot freshness is never coder liveness.
    """
    retry_limit = _validate_limit(retry_limit, name="retry_limit", maximum=_MAX_RETRIES)
    run_limit = _validate_limit(run_limit, name="run_limit", maximum=_MAX_RUNS)
    observed_at = _utc_now()
    try:
        config, repositories = _configured_repositories()
    except Exception:
        return {
            "schema_version": 1,
            "observed_at": _iso_z(observed_at),
            "configuration": {"status": "unavailable", "code": "configuration_invalid"},
            "redis": {"status": "not_checked", "code": None},
            "repositories": [],
            "detail": None,
        }
    if repo_slug is not None:
        repo_slug = _validate_repo_slug(repo_slug, repositories)

    client: Any | None = None
    try:
        try:
            client = _new_redis_client()
        except Exception:
            snapshots = {
                slug: (_snapshot_unavailable("unavailable", "redis_connection_failed"), None) for slug in repositories
            }
            redis_status = "unavailable"
            redis_code = "redis_connection_failed"
        else:
            snapshots = await _read_snapshots(client, repositories, config, observed_at)
            snapshot_statuses = [snapshot[0]["status"] for snapshot in snapshots.values()]
            redis_status = _source_summary(snapshot_statuses) if snapshot_statuses else "available"
            redis_code = "redis_read_failed" if redis_status != "available" else None

        overviews = [_overview(slug, repo, config, *snapshots[slug]) for slug, repo in repositories.items()]
        detail = None
        if repo_slug is not None:
            snapshot, state = snapshots[repo_slug]
            current_task_id = (
                _task_id(state.current_task.pr_id) if state is not None and state.current_task is not None else None
            )
            if client is None:
                retries = {
                    "status": "unavailable",
                    "code": "redis_connection_failed",
                    "records": [],
                    "record_count": None,
                    "scanned_index_entries": 0,
                    "truncated": False,
                }
                runs = {
                    "status": "unavailable",
                    "code": "redis_connection_failed",
                    "task_filter": current_task_id,
                    "records": [],
                    "record_count": None,
                    "scanned_index_entries": 0,
                    "truncated": False,
                }
                cancellation = {
                    "status": "unavailable",
                    "code": "redis_connection_failed",
                    "classification": None,
                }
            else:
                retries = await _pending_retries(client, repo_slug, retry_limit)
                runs = await _run_records(client, repo_slug, current_task_id, run_limit)
                cancellation = await _current_cancellation(client, repo_slug, current_task_id)
                source_statuses = [
                    *(item[0]["status"] for item in snapshots.values()),
                    retries["status"],
                    runs["status"],
                    cancellation["status"],
                ]
                redis_status = _source_summary(source_statuses)
                redis_code = "redis_read_failed" if redis_status != "available" else None
            detail = {
                "repo_slug": repo_slug,
                "pipeline": _pipeline_view(state),
                "snapshot": snapshot,
                "inhibitors": _inhibitors(state, observed_at),
                "current_cancellation": cancellation,
                "pending_retries": retries,
                "run_records": runs,
                "activity_interpretation": {
                    "coder_activity": "unknown",
                    "snapshot_freshness_is_progress": False,
                    "note_code": "no_process_liveness_source",
                },
            }

        return {
            "schema_version": 1,
            "observed_at": _iso_z(observed_at),
            "configuration": {
                "status": "available",
                "code": None,
                "repository_count": len(repositories),
            },
            "redis": {"status": redis_status, "code": redis_code},
            "repositories": overviews,
            "detail": detail,
        }
    finally:
        await _close_redis(client)
