"""Proactive usage-check provider for Claude CLI rate-limit avoidance.

Calls the undocumented OAuth usage endpoint that powers Claude Code's
``/usage`` slash command. Fail-open: any error returns ``None`` and the
daemon falls back to the reactive ``_detect_rate_limit`` on stderr.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import httpx

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class UsageSnapshot:
    session_percent: int
    session_resets_at: int
    weekly_percent: int
    weekly_resets_at: int
    fetched_at: float


class UsageProvider(Protocol):
    def fetch(self) -> UsageSnapshot | None: ...


class OAuthUsageProvider:
    """Primary usage provider backed by the Anthropic OAuth endpoint.

    Fail-open: returns ``None`` on any error so the daemon can fall back
    to the reactive ``_detect_rate_limit`` path.
    """

    ENDPOINT = "https://api.anthropic.com/api/oauth/usage"

    def __init__(
        self,
        credentials_path: str,
        user_agent: str,
        beta_header: str,
        cache_ttl_sec: int = 60,
        request_timeout_sec: float = 5.0,
    ) -> None:
        self._credentials_path = Path(credentials_path)
        self._user_agent = user_agent
        self._beta_header = beta_header
        self._cache_ttl = cache_ttl_sec
        self._timeout = request_timeout_sec
        self._cached: UsageSnapshot | None = None
        self._consecutive_failures = 0

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    def fetch(self) -> UsageSnapshot | None:
        if self._cached is not None:
            age = time.time() - self._cached.fetched_at
            if age < self._cache_ttl:
                return self._cached
        token = self._read_token()
        if token is None:
            self._consecutive_failures += 1
            return None
        try:
            response = httpx.get(
                self.ENDPOINT,
                headers={
                    "Authorization": f"Bearer {token}",
                    "anthropic-beta": self._beta_header,
                    "User-Agent": self._user_agent,
                    "Accept": "application/json",
                },
                timeout=self._timeout,
            )
        except (httpx.HTTPError, OSError) as exc:
            logger.warning("usage endpoint request failed: %s", exc)
            self._consecutive_failures += 1
            return None
        if response.status_code != 200:
            logger.warning(
                "usage endpoint returned %s (check anthropic-beta header)",
                response.status_code,
            )
            self._consecutive_failures += 1
            return None
        try:
            data = response.json()
            snap = UsageSnapshot(
                session_percent=int(data["five_hour"]["used_percentage"]),
                session_resets_at=int(data["five_hour"]["resets_at"]),
                weekly_percent=int(data["seven_day"]["used_percentage"]),
                weekly_resets_at=int(data["seven_day"]["resets_at"]),
                fetched_at=time.time(),
            )
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning("usage endpoint returned unexpected shape: %s", exc)
            self._consecutive_failures += 1
            return None
        self._cached = snap
        self._consecutive_failures = 0
        return snap

    def invalidate_cache(self) -> None:
        self._cached = None

    def _read_token(self) -> str | None:
        if not self._credentials_path.is_file():
            return None
        try:
            raw = self._credentials_path.read_text(encoding="utf-8")
            data = json.loads(raw)
        except (OSError, json.JSONDecodeError):
            return None
        for key in ("accessToken", "access_token"):
            token = data.get(key)
            if isinstance(token, str) and token:
                return token
        claude_ai = data.get("claudeAiOauth") or data.get("claude_ai_oauth")
        if isinstance(claude_ai, dict):
            for key in ("accessToken", "access_token"):
                token = claude_ai.get(key)
                if isinstance(token, str) and token:
                    return token
        return None
