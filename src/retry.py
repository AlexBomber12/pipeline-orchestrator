"""Bounded retry with exponential backoff for transient subprocess failures."""

from __future__ import annotations

import logging
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
    "502 bad gateway",
    "503 service unavailable",
    "504 gateway timeout",
    "remote end hung up",
)


def is_transient_error(exc: Exception) -> bool:
    """Return True if exception indicates a likely-transient failure."""
    if isinstance(exc, subprocess.TimeoutExpired):
        return True
    if isinstance(exc, subprocess.CalledProcessError):
        stderr = (getattr(exc, "stderr", "") or "").lower()
        if any(marker in stderr for marker in TRANSIENT_MARKERS):
            return True
    if isinstance(exc, RuntimeError):
        msg = str(exc).lower()
        if any(marker in msg for marker in TRANSIENT_MARKERS):
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
