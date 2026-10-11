"""Dormant descriptor and parser for Claude Code browser-code login."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping
from urllib.parse import unquote_to_bytes, urlsplit

from src.coder_registry import (
    CoderBrowserLoginFailure,
    CoderBrowserLoginProgress,
)

_MINIMUM_VERSION = (2, 1, 126)
_MINIMUM_VERSION_TEXT = "2.1.126"
_NUMERIC_VERSION_COMPONENT = r"(?:0|[1-9][0-9]{0,5})"
_STABLE_VERSION_PATTERN = re.compile(
    rf"{_NUMERIC_VERSION_COMPONENT}\."
    rf"{_NUMERIC_VERSION_COMPONENT}\."
    rf"{_NUMERIC_VERSION_COMPONENT}"
)
_PRERELEASE_VERSION_PATTERN = re.compile(
    rf"{_NUMERIC_VERSION_COMPONENT}\."
    rf"{_NUMERIC_VERSION_COMPONENT}\."
    rf"{_NUMERIC_VERSION_COMPONENT}-[0-9A-Za-z.-]+"
)
_ANSI_ESCAPE_PATTERN = re.compile(r"\x1b(?:\][^\x07\x1b]*(?:\x07|\x1b\\)|\[[0-?]*[ -/]*[@-~]|[@-_])")
_URL_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s]+")
_INVALID_PERCENT_ESCAPE = re.compile(r"%(?![0-9A-Fa-f]{2})")
_AUTHORIZATION_PATH = "/cai/oauth/authorize"
_AUTHORIZATION_QUERY_KEYS = frozenset(
    {
        "client_id",
        "code",
        "code_challenge",
        "code_challenge_method",
        "redirect_uri",
        "response_type",
        "scope",
        "state",
    }
)
_PASTE_CODE_PROMPT = "Paste code here if prompted >"
_INVALID_CODE_MESSAGE = "Invalid code. Please make sure the full code was copied."
_MAX_OUTPUT_CHARACTERS = 16 * 1024
_MAX_AUTHORIZATION_URL_CHARACTERS = 4096
_APPLICATION_TIMEOUT_SECONDS = 15 * 60
_MALFORMED_OUTPUT_ERROR = "Claude browser-code login output is malformed"
_REPLACEMENT_WARNING = (
    "Claude Code may replace existing saved authentication; a failed or "
    "cancelled replacement may leave prior authentication unavailable."
)


def _validated_stable_version(observed: object) -> str:
    if observed is None or observed == "":
        raise ValueError("Claude Code CLI version evidence is required")
    if not isinstance(observed, str):
        raise ValueError("Claude Code CLI version evidence is invalid")
    if _PRERELEASE_VERSION_PATTERN.fullmatch(observed) is not None:
        raise ValueError("Claude Code CLI prerelease versions are not verified for browser-code login")
    if _STABLE_VERSION_PATTERN.fullmatch(observed) is None:
        raise ValueError("Claude Code CLI version evidence is invalid")
    numeric_version = tuple(int(component) for component in observed.split("."))
    if numeric_version < _MINIMUM_VERSION:
        raise ValueError(f"Claude Code CLI {_MINIMUM_VERSION_TEXT} or later is required for browser-code login")
    return observed


def supports_browser_login_version(observed: object) -> bool:
    """Report support using the descriptor's sanitized version guard."""
    try:
        _validated_stable_version(observed)
    except ValueError:
        return False
    return True


def _normalize_terminal_output(output: str) -> str:
    if not isinstance(output, str) or len(output) > _MAX_OUTPUT_CHARACTERS:
        raise ValueError(_MALFORMED_OUTPUT_ERROR)
    without_ansi = _ANSI_ESCAPE_PATTERN.sub("", output)
    return without_ansi.replace("\r\n", "\n").replace("\r", "\n")


def _contains_control_character(value: str) -> bool:
    return any(ord(character) < 32 or ord(character) == 127 for character in value)


def _validate_query(query: str) -> None:
    components = query.split("&")
    if len(components) != len(_AUTHORIZATION_QUERY_KEYS):
        raise ValueError(_MALFORMED_OUTPUT_ERROR)
    keys: set[str] = set()
    for component in components:
        key, separator, value = component.partition("=")
        if (
            not separator
            or key not in _AUTHORIZATION_QUERY_KEYS
            or key in keys
            or not value
            or _INVALID_PERCENT_ESCAPE.search(value) is not None
        ):
            raise ValueError(_MALFORMED_OUTPUT_ERROR)
        decoded_value = unquote_to_bytes(value)
        if any(byte < 32 or byte == 127 for byte in decoded_value):
            raise ValueError(_MALFORMED_OUTPUT_ERROR)
        keys.add(key)


def _validate_authorization_url(candidate: str) -> str:
    if (
        len(candidate) > _MAX_AUTHORIZATION_URL_CHARACTERS
        or _contains_control_character(candidate)
        or _INVALID_PERCENT_ESCAPE.search(candidate) is not None
    ):
        raise ValueError(_MALFORMED_OUTPUT_ERROR)
    try:
        parsed = urlsplit(candidate)
        port = parsed.port
    except ValueError:
        raise ValueError(_MALFORMED_OUTPUT_ERROR) from None
    if (
        parsed.scheme != "https"
        or parsed.netloc != "claude.com"
        or parsed.hostname != "claude.com"
        or port is not None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path != _AUTHORIZATION_PATH
        or not parsed.query
        or parsed.fragment
    ):
        raise ValueError(_MALFORMED_OUTPUT_ERROR)
    _validate_query(parsed.query)
    return candidate


def _authorization_url(terminal_output: str) -> str | None:
    complete_candidates: list[str] = []
    has_incomplete_candidate = False
    for match in _URL_PATTERN.finditer(terminal_output):
        candidate = match.group(0)
        if match.end() == len(terminal_output):
            if len(candidate) > _MAX_AUTHORIZATION_URL_CHARACTERS:
                raise ValueError(_MALFORMED_OUTPUT_ERROR)
            has_incomplete_candidate = True
            continue
        complete_candidates.append(_validate_authorization_url(candidate))
    if has_incomplete_candidate:
        return None
    if not complete_candidates:
        return None
    first = complete_candidates[0]
    if any(candidate != first for candidate in complete_candidates[1:]):
        raise ValueError(_MALFORMED_OUTPUT_ERROR)
    return first


@dataclass(frozen=True)
class ClaudeBrowserLoginAdapter:
    """Version-gated Claude login descriptor with a bounded terminal parser."""

    environment: Mapping[str, str] = field(repr=False)
    working_directory: str = field(repr=False)
    credential_location: str = field(repr=False)
    observed_cli_version: str
    command: tuple[str, ...] = field(default=("claude", "auth", "login"), init=False)
    requires_pty: bool = field(default=True, init=False)
    application_timeout_seconds: float = field(default=_APPLICATION_TIMEOUT_SECONDS, init=False)
    replacement_warning: str = field(default=_REPLACEMENT_WARNING, init=False)

    def __post_init__(self) -> None:
        version = _validated_stable_version(self.observed_cli_version)
        if not isinstance(self.environment, Mapping):
            raise ValueError("Claude browser login environment is invalid")
        environment = dict(self.environment)
        if not all(
            isinstance(key, str) and isinstance(value, str) and "\x00" not in key and "\x00" not in value
            for key, value in environment.items()
        ):
            raise ValueError("Claude browser login environment is invalid")
        if (
            not isinstance(self.working_directory, str)
            or not self.working_directory
            or not isinstance(self.credential_location, str)
            or not self.credential_location
        ):
            raise ValueError("Claude browser login context is invalid")
        if environment.get("CLAUDE_CONFIG_DIR") != self.credential_location:
            raise ValueError("Claude browser login credential context is inconsistent")
        object.__setattr__(self, "observed_cli_version", version)
        object.__setattr__(self, "environment", MappingProxyType(environment))

    def parse_progress(self, terminal_output: str) -> CoderBrowserLoginProgress | None:
        normalized = _normalize_terminal_output(terminal_output)
        authorization_url = _authorization_url(normalized)
        if authorization_url is None:
            return None
        return CoderBrowserLoginProgress(
            authorization_url=authorization_url,
            ready_for_code=_PASTE_CODE_PROMPT in normalized,
        )

    def classify_failure(self, terminal_output: str, returncode: int) -> CoderBrowserLoginFailure:
        del returncode
        try:
            normalized = _normalize_terminal_output(terminal_output)
        except ValueError:
            normalized = ""
        if _INVALID_CODE_MESSAGE in normalized:
            return CoderBrowserLoginFailure(
                "invalid_code",
                "Claude Code rejected the authorization code",
            )
        return CoderBrowserLoginFailure(
            "process_failed",
            "Claude browser-code login failed",
        )
