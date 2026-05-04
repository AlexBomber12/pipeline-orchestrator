"""Cancellation cause storage substrate (PR-252) and detection helpers (PR-253)."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, Callable

from src.cancellation.blocked_set import (
    TaskNode,
    compute_blocked_set,
    compute_dependents_count,
)
from src.cancellation.storage import (
    CATEGORIES,
    TTL_SECONDS,
    CancellationCause,
    cause_key,
    delete_cancellation_cause,
    get_cancellation_cause,
    index_key,
    list_recent_cancellations,
    record_cancellation_cause,
)

logger = logging.getLogger(__name__)

_RETRY_EXHAUSTION_RE = re.compile(r"failed after (\d+) attempts")

# Tail-truncate the CRASH payload error_message so a multi-megabyte
# stderr blob never lands in Redis with a 30-day TTL. Tail because the
# error tends to be at the end of the captured stream.
CRASH_PAYLOAD_MESSAGE_MAX = 2000


def truncate_for_payload(
    message: str, *, max_chars: int = CRASH_PAYLOAD_MESSAGE_MAX
) -> str:
    """Return ``message`` tail-truncated to fit in a cancellation payload."""
    if len(message) <= max_chars:
        return message
    return f"[truncated]\n{message[-max_chars:]}"


async def safe_record_cancellation_cause(
    redis_client: Any,
    repo_slug: str,
    task_id: str,
    cause: CancellationCause,
    *,
    log: Callable[[str], None] | None = None,
) -> None:
    """Best-effort wrapper around ``record_cancellation_cause``.

    Cause storage is best-effort: a Redis outage during the cancellation
    transition must never block the transition. Swallows any exception
    and forwards a single ``[ERROR] Failed to record cancellation cause``
    line through ``log`` (defaults to module logger). PR-253.
    """
    try:
        await record_cancellation_cause(
            redis_client, repo_slug, task_id, cause
        )
    except Exception as exc:
        msg = (
            f"[ERROR] Failed to record cancellation cause "
            f"({cause.category}): {exc} - continuing without storage."
        )
        if log is not None:
            log(msg)
        else:
            logger.warning(msg)


async def safe_delete_cancellation_cause(
    redis_client: Any,
    repo_slug: str,
    task_id: str,
    *,
    log: Callable[[str], None] | None = None,
) -> None:
    """Best-effort wrapper around ``delete_cancellation_cause``.

    Used when ``handle_error`` decides to retry (ERROR -> IDLE): the
    previously-recorded cause is no longer accurate because the task
    will continue, so the record must be cleared to avoid corrupting
    cancellation reporting if the task later succeeds. A Redis outage
    during cleanup must never block the IDLE transition.
    """
    try:
        await delete_cancellation_cause(redis_client, repo_slug, task_id)
    except Exception as exc:
        msg = (
            f"[ERROR] Failed to clear cancellation cause for {task_id}: "
            f"{exc} - continuing without cleanup."
        )
        if log is not None:
            log(msg)
        else:
            logger.warning(msg)


def classify_infra_exception(
    exc: BaseException, *, subsystem: str = "gh_api"
) -> CancellationCause | None:
    """Return an INFRA cause if ``exc`` looks like retry-exhausted infra.

    ``retry_transient`` raises ``RuntimeError("<op> failed after N attempts:
    <last_exc>")`` after exhausting transient retries. Caller sites that
    catch that exception classify the failure as INFRA so the dashboard
    surfaces the subsystem outage rather than a generic CRASH. Returns
    ``None`` for exceptions that are not retry-exhaustion — those callers
    fall back to the default CRASH cause written by ``_transition_to_error``.
    """
    msg = str(exc)
    match = _RETRY_EXHAUSTION_RE.search(msg)
    if match is None:
        return None
    return CancellationCause(
        category="INFRA",
        payload={
            "subsystem": subsystem,
            "retry_count": int(match.group(1)),
            "last_attempt_iso": datetime.now(timezone.utc).isoformat(),
            "error_class": type(exc).__name__,
            "error_message": msg[:500],
        },
    )


__all__ = [
    "CATEGORIES",
    "CRASH_PAYLOAD_MESSAGE_MAX",
    "TTL_SECONDS",
    "CancellationCause",
    "TaskNode",
    "cause_key",
    "classify_infra_exception",
    "compute_blocked_set",
    "compute_dependents_count",
    "delete_cancellation_cause",
    "get_cancellation_cause",
    "index_key",
    "list_recent_cancellations",
    "record_cancellation_cause",
    "safe_delete_cancellation_cause",
    "safe_record_cancellation_cause",
    "truncate_for_payload",
]
