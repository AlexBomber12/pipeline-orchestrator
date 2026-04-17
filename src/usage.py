"""Proactive usage-check providers for rate-limit avoidance.

Supports both Claude (Anthropic OAuth) and Codex (ChatGPT /wham/usage).
Fail-open: any error returns ``None`` and the daemon falls back to the
reactive ``_detect_rate_limit`` on stderr.
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
        self._last_failure_at: float = 0.0

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    def fetch(self) -> UsageSnapshot | None:
        if self._cached is not None:
            age = time.time() - self._cached.fetched_at
            if age < self._cache_ttl:
                return self._cached
        # Backoff on repeated failures: wait cache_ttl * min(failures, 5)
        # before retrying, capping at 5x the normal TTL.  Return None
        # (not stale cached data) to preserve fail-open behavior.
        if self._consecutive_failures > 0:
            backoff = self._cache_ttl * min(self._consecutive_failures, 5)
            if time.time() - self._last_failure_at < backoff:
                return None
        token = self._read_token()
        if token is None:
            self._record_failure()
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
            self._record_failure()
            return None
        if response.status_code != 200:
            logger.warning(
                "usage endpoint returned %s (check anthropic-beta header)",
                response.status_code,
            )
            self._record_failure()
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
            self._record_failure()
            return None
        self._cached = snap
        self._consecutive_failures = 0
        return snap

    def _record_failure(self) -> None:
        self._consecutive_failures += 1
        self._last_failure_at = time.time()

    def invalidate_cache(self) -> None:
        self._cached = None

    def _read_token(self) -> str | None:
        if not self._credentials_path.is_file():
            return None
        try:
            raw = self._credentials_path.read_text(encoding="utf-8")
            data = json.loads(raw)
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
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


class OpenAIUsageProvider:
    """Proactive usage provider for Codex CLI (ChatGPT subscription).

    Calls the same endpoint that Codex CLI polls internally every 60s:
    ``GET https://chatgpt.com/backend-api/wham/usage``

    Source: ``backend-client/src/client.rs::get_rate_limits`` (openai/codex).
    """

    ENDPOINT = "https://chatgpt.com/backend-api/wham/usage"

    def __init__(
        self,
        credentials_path: str,
        cache_ttl_sec: int = 60,
        request_timeout_sec: float = 5.0,
    ) -> None:
        self._credentials_path = Path(credentials_path)
        self._cache_ttl = cache_ttl_sec
        self._timeout = request_timeout_sec
        self._cached: UsageSnapshot | None = None
        self._consecutive_failures = 0
        self._last_failure_at: float = 0.0

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    def fetch(self) -> UsageSnapshot | None:
        if self._cached is not None:
            age = time.time() - self._cached.fetched_at
            if age < self._cache_ttl:
                return self._cached
        if self._consecutive_failures > 0:
            backoff = self._cache_ttl * min(self._consecutive_failures, 5)
            if time.time() - self._last_failure_at < backoff:
                return None
        token = self._read_token()
        if token is None:
            self._record_failure()
            return None
        try:
            response = httpx.get(
                self.ENDPOINT,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                },
                timeout=self._timeout,
            )
        except (httpx.HTTPError, OSError) as exc:
            logger.warning("OpenAI usage endpoint request failed: %s", exc)
            self._record_failure()
            return None
        if response.status_code != 200:
            logger.warning(
                "OpenAI usage endpoint returned %s",
                response.status_code,
            )
            self._record_failure()
            return None
        try:
            data = response.json()
            rl = data.get("rate_limit") or {}
            primary = rl.get("primary_window") or {}
            secondary = rl.get("secondary_window") or {}
            if "used_percent" not in primary or "used_percent" not in secondary:
                logger.warning(
                    "OpenAI usage response missing used_percent fields"
                )
                logger.debug("OpenAI usage response body: %s", response.text[:500])
                self._record_failure()
                return None
            if "reset_at" not in primary or "reset_at" not in secondary:
                logger.warning(
                    "OpenAI usage response missing reset_at fields"
                )
                logger.debug("OpenAI usage response body: %s", response.text[:500])
                self._record_failure()
                return None
            snap = UsageSnapshot(
                session_percent=int(primary["used_percent"]),
                session_resets_at=int(primary["reset_at"]),
                weekly_percent=int(secondary["used_percent"]),
                weekly_resets_at=int(secondary["reset_at"]),
                fetched_at=time.time(),
            )
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            logger.warning("OpenAI usage endpoint returned unexpected shape: %s", exc)
            logger.debug("OpenAI usage response body: %s", response.text[:500])
            self._record_failure()
            return None
        self._cached = snap
        self._consecutive_failures = 0
        return snap

    def _record_failure(self) -> None:
        self._consecutive_failures += 1
        self._last_failure_at = time.time()

    def invalidate_cache(self) -> None:
        self._cached = None

    def _read_token(self) -> str | None:
        if not self._credentials_path.is_file():
            return None
        try:
            raw = self._credentials_path.read_text(encoding="utf-8")
            data = json.loads(raw)
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            return None
        tokens = data.get("tokens")
        if isinstance(tokens, dict):
            token = tokens.get("access_token")
            if isinstance(token, str) and token:
                return token
        return None
