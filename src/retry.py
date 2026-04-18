"""Bounded retry with exponential backoff for transient subprocess failures."""

from __future__ import annotations

import logging
import re
import subprocess
import time
from typing import Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

TRANSIENT_MARKERS = (
    "connection reset",
    "connection refused",
    "network unreachable",
    "temporary failure",
    "could not resolve host",
    "operation timed out",
    "timed out",
    "tls handshake timeout",
    "i/o timeout",
    "handshake timeout",
    "context deadline exceeded",
    "502 bad gateway",
    "503 service unavailable",
    "504 gateway timeout",
    "remote end hung up",
)

# Matches 5xx status codes preceded by HTTP/status context words
# (e.g. "error: 503", "HTTP 502", "HTTP/1.1 503", "status code 502")
# Avoids matching bare numbers in URLs like "issues/503/comments".
_NUMERIC_5XX_RE = re.compile(
    r"(?:"
    r"(?:returned\s+)?error\s*:\s*"
    r"|http\s*/?\s*(?:\d\.\d\s+)?"
    r"|status\s*(?:code\s*)?"
    r")\s*5[0-9]{2}\b",
    re.IGNORECASE,
)


def _has_transient_signal(text: str) -> bool:
    """Check text for transient markers or bare 5xx status codes."""
    lower = text.lower()
    if any(marker in lower for marker in TRANSIENT_MARKERS):
        return True
    return bool(_NUMERIC_5XX_RE.search(lower))


def is_transient_error(exc: Exception) -> bool:
    """Return True if exception indicates a likely-transient failure."""
    if isinstance(exc, subprocess.TimeoutExpired):
        return True
    if isinstance(exc, subprocess.CalledProcessError):
        stderr = (getattr(exc, "stderr", "") or "")
        if _has_transient_signal(stderr):
            return True
    if isinstance(exc, RuntimeError):
        if _has_transient_signal(str(exc)):
            return True
    return False


def retry_transient(
    operation: Callable[[], T],
    attempts: int = 3,
    initial_backoff_sec: float = 1.0,
    operation_name: str = "operation",
) -> T:
    """Run operation with bounded exponential backoff on transient errors.

    Returns the operation's result on success. Re-raises the final exception
    on exhaustion, with attempts and operation_name included in the message.
    Non-transient exceptions propagate immediately without retry.
    """
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except Exception as exc:
            if not is_transient_error(exc):
                raise
            last_exc = exc
            if attempt == attempts:
                break
            backoff = initial_backoff_sec * (2 ** (attempt - 1))
            logger.warning(
                "%s: transient failure (attempt %d/%d), retrying in %.1fs: %s",
                operation_name,
                attempt,
                attempts,
                backoff,
                exc,
            )
            time.sleep(backoff)
    raise RuntimeError(
        f"{operation_name} failed after {attempts} attempts: {last_exc}"
    ) from last_exc
