"""Proactive usage-check providers for rate-limit avoidance.

Supports both Claude (Anthropic OAuth) and Codex (ChatGPT /wham/usage).
Fail-open: any error returns ``None`` and the daemon falls back to the
reactive ``_detect_rate_limit`` on stderr.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, Protocol

import httpx
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class UsageSnapshot:
    session_percent: int | None
    session_resets_at: int | None
    weekly_percent: int | None
    weekly_resets_at: int | None
    fetched_at: float


class UsageObservation(BaseModel):
    """Daemon-owned account observation, independent of repository progress."""

    model_config = ConfigDict(allow_inf_nan=False)
    status: Literal["available", "unavailable", "unsupported"]
    checked_at: float = Field(ge=0, le=253402300799)
    last_successful_at: float | None = Field(default=None, ge=0, le=253402300799)
    snapshot: UsageSnapshot | None = None


def _read_window(raw: object, percent_key: str, reset_key: str) -> tuple[int | None, int | None]:
    if raw is None:
        return None, None
    if not isinstance(raw, dict):
        raise ValueError("Invalid usage window")
    percent, reset = raw.get(percent_key), raw.get(reset_key)
    if percent is None or reset is None:
        return None, None
    if isinstance(percent, bool) or isinstance(reset, bool):
        raise ValueError("Invalid usage values")
    percent = float(percent)
    if not 0 <= percent <= 100:
        raise ValueError("Usage percent outside 0..100")
    timestamp = datetime.fromisoformat(reset).timestamp() if reset_key == "resets_at" else int(reset)
    if timestamp <= 0:
        raise ValueError("Invalid usage reset")
    return int(percent), int(timestamp)


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
        # _lock guards cache/failure state mutation only (microsecond hold).
        # Reads from the event-loop thread (consecutive_failures,
        # invalidate_cache) must never block on outbound HTTP.
        self._lock = threading.Lock()
        # _request_lock serializes outbound HTTP across worker threads so a
        # concurrent burst of cache misses still fans out into a single
        # request. It is acquired only by fetch() (worker threads via
        # asyncio.to_thread); the event-loop thread never takes it.
        self._request_lock = threading.Lock()

    @property
    def consecutive_failures(self) -> int:
        with self._lock:
            return self._consecutive_failures

    def _cache_or_backoff(self) -> tuple[bool, UsageSnapshot | None]:
        """Return (handled, value) under _lock for cache/backoff checks.

        ``handled=True`` means fetch() should return ``value`` immediately
        (either a fresh cached snapshot or ``None`` during a backoff window).
        """
        with self._lock:
            if self._cached is not None:
                age = time.time() - self._cached.fetched_at
                if age < self._cache_ttl:
                    return True, self._cached
            # Backoff on repeated failures: wait cache_ttl * min(failures, 5)
            # before retrying, capping at 5x the normal TTL.  Return None
            # (not stale cached data) to preserve fail-open behavior.
            if self._consecutive_failures > 0:
                backoff = self._cache_ttl * min(self._consecutive_failures, 5)
                if time.time() - self._last_failure_at < backoff:
                    return True, None
        return False, None

    def fetch(self) -> UsageSnapshot | None:
        handled, value = self._cache_or_backoff()
        if handled:
            return value
        # Serialize HTTP requests so concurrent cache misses share one
        # outbound call. The state lock is NOT held during HTTP — the
        # event-loop thread can still read consecutive_failures or call
        # invalidate_cache without blocking on a slow request.
        with self._request_lock:
            handled, value = self._cache_or_backoff()
            if handled:
                return value
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
                session = _read_window(data.get("five_hour"), "utilization", "resets_at")
                weekly = _read_window(data.get("seven_day"), "utilization", "resets_at")
                if session[0] is None and weekly[0] is None:
                    raise ValueError("No usage windows available")
                snap = UsageSnapshot(*session, *weekly, fetched_at=time.time())
            except (KeyError, TypeError, ValueError, AttributeError, OverflowError) as exc:
                logger.warning("usage endpoint returned unexpected shape: %s", exc)
                self._record_failure()
                return None
            with self._lock:
                self._cached = snap
                self._consecutive_failures = 0
            return snap

    def _record_failure(self) -> None:
        with self._lock:
            self._consecutive_failures += 1
            self._last_failure_at = time.time()

    def invalidate_cache(self) -> None:
        with self._lock:
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
        # See OAuthUsageProvider for rationale; HTTP must run outside _lock
        # so the event-loop thread is never blocked behind a slow request.
        self._lock = threading.Lock()
        self._request_lock = threading.Lock()

    @property
    def consecutive_failures(self) -> int:
        with self._lock:
            return self._consecutive_failures

    def _cache_or_backoff(self) -> tuple[bool, UsageSnapshot | None]:
        with self._lock:
            if self._cached is not None:
                age = time.time() - self._cached.fetched_at
                if age < self._cache_ttl:
                    return True, self._cached
            if self._consecutive_failures > 0:
                backoff = self._cache_ttl * min(self._consecutive_failures, 5)
                if time.time() - self._last_failure_at < backoff:
                    return True, None
        return False, None

    def fetch(self) -> UsageSnapshot | None:
        handled, value = self._cache_or_backoff()
        if handled:
            return value
        with self._request_lock:
            handled, value = self._cache_or_backoff()
            if handled:
                return value
            token, account_id = self._read_credentials()
            if token is None:
                self._record_failure()
                return None
            try:
                headers: dict[str, str] = {
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                }
                if account_id:
                    headers["ChatGPT-Account-Id"] = account_id
                response = httpx.get(
                    self.ENDPOINT,
                    headers=headers,
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
                primary = _read_window(rl.get("primary_window"), "used_percent", "reset_at")
                secondary = _read_window(rl.get("secondary_window"), "used_percent", "reset_at")
                if primary[0] is None and secondary[0] is None:
                    raise ValueError("No usage windows available")
                snap = UsageSnapshot(*primary, *secondary, fetched_at=time.time())
            except (KeyError, TypeError, ValueError, AttributeError, OverflowError) as exc:
                logger.warning("OpenAI usage endpoint returned unexpected shape: %s", exc)
                logger.debug("OpenAI usage response body: %s", response.text[:500])
                self._record_failure()
                return None
            with self._lock:
                self._cached = snap
                self._consecutive_failures = 0
            return snap

    def _record_failure(self) -> None:
        with self._lock:
            self._consecutive_failures += 1
            self._last_failure_at = time.time()

    def invalidate_cache(self) -> None:
        with self._lock:
            self._cached = None

    def reset_after_credential_change(self) -> None:
        """Discard cached state and retry immediately with new credentials."""
        with self._lock:
            self._cached = None
            self._consecutive_failures = 0
            self._last_failure_at = 0.0

    def _read_credentials(self) -> tuple[str | None, str | None]:
        """Return ``(access_token, account_id)`` from Codex auth file."""
        if not self._credentials_path.is_file():
            return None, None
        try:
            raw = self._credentials_path.read_text(encoding="utf-8")
            data = json.loads(raw)
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            return None, None
        token = None
        tokens = data.get("tokens")
        if isinstance(tokens, dict):
            token = tokens.get("access_token")
            if not isinstance(token, str) or not token:
                token = None
        account_id = data.get("account_id")
        if not isinstance(account_id, str) or not account_id:
            account_id = None
        return token, account_id

    def _read_token(self) -> str | None:
        token, _ = self._read_credentials()
        return token
