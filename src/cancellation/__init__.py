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
    current_run_started_at_key,
    delete_cancellation_cause,
    delete_current_run_started_at,
    delete_retry_count,
    delete_task_spec_hash,
    get_cancellation_cause,
    get_current_run_started_at,
    get_task_spec_hash,
    index_key,
    list_recent_cancellations,
    record_cancellation_cause,
    record_current_run_started_at,
    record_task_spec_hash,
    reset_retry_count,
    retry_count_key,
    task_spec_content_hash,
    task_spec_hash_key,
)
from src.daemon import error_rate_tracker

logger = logging.getLogger(__name__)

_RETRY_EXHAUSTION_RE = re.compile(r"failed after (\d+) attempts")

# The PR-315 stable subsource vocabulary, mirrored from
# ``src/cancellation/storage.py``'s module docstring. Kept in sync with the
# DOCUMENTED_SUBSOURCES set asserted by ``tests/cancellation``.
SUBSOURCE_VOCABULARY: frozenset[str] = frozenset(
    {
        "crash",
        "coder_escalate",
        "guardrail",
        "review_timeout",
        "fix_idle_timeout",
        "fix_iteration_cap",
        "no_push_deadlock",
        "infra_failure",
    }
)

# Pre-PR-315 ``category`` values mapped onto the canonical post-migration
# subsource. Used to defensively recover a usable dispatch hint from a
# legacy record that the ``escalate_to_error`` startup migration missed.
_LEGACY_CATEGORY_TO_SUBSOURCE: dict[str, str] = {
    "CRASH": "crash",
    "ESCALATE": "coder_escalate",
    "TIMEOUT": "review_timeout",
    "INFRA": "infra_failure",
    "NO_PUSH_DEADLOCK": "no_push_deadlock",
}


def classify_cancellation_subsource(
    cause: CancellationCause | None,
    *,
    log: Callable[[str], None] | None = None,
) -> str:
    """Return the canonical ``payload.subsource`` for ``cause``, defensively.

    PR-315 collapsed ``CancellationCause.category`` to a single ``ERROR``
    value with detector identity moved to ``payload.subsource``. PR-318
    routes the recovery handler off ``payload.subsource`` instead of the
    now-degenerate ``category`` field. This helper guards three pre-
    migration footguns:

    1. A pre-PR-315 record retaining a legacy ``category`` value (one of
       ``CRASH`` / ``ESCALATE`` / ``TIMEOUT`` / ``INFRA`` /
       ``NO_PUSH_DEADLOCK``) — emit a warning and translate the legacy
       category to its canonical subsource so dispatch still works while
       the operator notices the missed migration.
    2. A migrated record with ``category=ERROR`` but no ``subsource``
       in payload — fall back to ``payload.legacy_category`` (preserved
       by the ``escalate_to_error`` migration) before degrading to ``""``.
    3. ``cause`` is ``None`` (no record found) — return ``""`` so the
       caller routes to the operator-attention path.

    ``payload.subsource`` is also validated against ``SUBSOURCE_VOCABULARY``
    on the ERROR path. A malformed or forward-incompatible value (operator
    typo, detector string introduced by a newer daemon writing to a Redis
    store an older daemon then reads) must not pass through unchecked: an
    unrecognized non-empty subsource would otherwise route to operator-
    attention even when ``legacy_category`` records the original detector
    as ``CRASH``, suppressing the expected crash log signal.

    The empty string ``""`` is the sentinel "no usable subsource"; the
    companion ``recovery_branch_for_subsource`` maps it to operator
    attention so an unknown cause never silently re-enters the crash
    recovery flow.
    """
    if cause is None:
        return ""

    payload = cause.payload or {}
    subsource = payload.get("subsource")
    legacy = payload.get("legacy_category")
    category = cause.category

    if category != "ERROR":
        msg = (
            f"[INFRA] cancellation cause has non-ERROR category "
            f"{category!r}; PR-315 migration should have rewritten it. "
            "Falling back to legacy_category/subsource for dispatch."
        )
        if log is not None:
            log(msg)
        else:
            logger.warning(msg)
        if isinstance(subsource, str) and subsource in SUBSOURCE_VOCABULARY:
            return subsource
        if isinstance(category, str) and category in _LEGACY_CATEGORY_TO_SUBSOURCE:
            return _LEGACY_CATEGORY_TO_SUBSOURCE[category]
        return ""

    if isinstance(subsource, str) and subsource in SUBSOURCE_VOCABULARY:
        return subsource
    if isinstance(legacy, str) and legacy in _LEGACY_CATEGORY_TO_SUBSOURCE:
        return _LEGACY_CATEGORY_TO_SUBSOURCE[legacy]
    return ""


def recovery_branch_for_subsource(subsource: str) -> str:
    """Map a PR-315 subsource onto the recovery dispatch branch.

    PR-318: recovery used to branch on the legacy ``category`` field
    (``CRASH`` vs ``ESCALATE``/``TIMEOUT``/...). After PR-315's collapse
    only ``payload.subsource`` distinguishes the crash recovery flow
    (preserve unpushed commits, mark ERROR) from the operator-attention
    flow (deliberately parked, no auto-retry). Returns one of:

    * ``"crash"`` — the previous run died mid-CODING and any local-only
      commits should be preserved before the task is marked ERROR.
    * ``"operator_attention"`` — the previous run was deliberately parked
      by a detector (review_timeout, fix_iteration_cap, etc.); the
      operator must clear the cause before retry.

    An empty or unrecognized subsource degrades to ``"operator_attention"``
    so an unknown cause never silently re-enters the crash flow.
    """
    if subsource == "crash":
        return "crash"
    return "operator_attention"

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
        return

    try:
        tracker_created_at = datetime.fromisoformat(cause.created_at)
        await error_rate_tracker.record(
            redis_client, repo_slug, tracker_created_at, member_id=task_id
        )
    except Exception as exc:
        msg = (
            f"[ERROR] Failed to record ERROR-rate event "
            f"({cause.category}): {exc} - continuing without tracker."
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
    """Return an ERROR cause with ``infra_failure`` subsource if ``exc`` looks
    like retry-exhausted infra.

    ``retry_transient`` raises ``RuntimeError("<op> failed after N attempts:
    <last_exc>")`` after exhausting transient retries. Caller sites that
    catch that exception classify the failure as ``infra_failure`` so the
    dashboard surfaces the subsystem outage rather than a generic crash.
    Returns ``None`` for exceptions that are not retry-exhaustion — those
    callers fall back to the default ``crash`` cause written by
    ``_transition_to_error``.
    """
    msg = str(exc)
    match = _RETRY_EXHAUSTION_RE.search(msg)
    if match is None:
        return None
    return CancellationCause(
        category="ERROR",
        payload={
            "subsource": "infra_failure",
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
    "SUBSOURCE_VOCABULARY",
    "TTL_SECONDS",
    "CancellationCause",
    "TaskNode",
    "cause_key",
    "classify_cancellation_subsource",
    "classify_infra_exception",
    "compute_blocked_set",
    "compute_dependents_count",
    "current_run_started_at_key",
    "delete_cancellation_cause",
    "delete_current_run_started_at",
    "delete_retry_count",
    "delete_task_spec_hash",
    "get_cancellation_cause",
    "get_current_run_started_at",
    "get_task_spec_hash",
    "index_key",
    "list_recent_cancellations",
    "record_cancellation_cause",
    "record_current_run_started_at",
    "record_task_spec_hash",
    "recovery_branch_for_subsource",
    "reset_retry_count",
    "retry_count_key",
    "safe_delete_cancellation_cause",
    "safe_record_cancellation_cause",
    "task_spec_content_hash",
    "task_spec_hash_key",
    "truncate_for_payload",
]
