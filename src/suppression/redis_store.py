"""Redis-backed ``SuppressionStore`` over existing cancellation storage."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from src.cancellation import (
    CancellationCause,
    classify_cancellation_subsource,
    get_cancellation_cause,
    list_recent_cancellations,
    safe_delete_cancellation_cause,
    safe_record_cancellation_cause,
)
from src.subsource_registry import SuppressionReason, lookup
from src.suppression import SuppressionRecord

logger = logging.getLogger(__name__)


class RedisSuppressionStore:
    """SuppressionStore implementation that reuses cancellation Redis keys."""

    def __init__(self, redis_client: Any) -> None:
        self._redis = redis_client
        self._warned_unknown_subsources: set[str] = set()

    async def suppress(
        self,
        repo: str,
        task_id: str,
        reason: SuppressionReason,
        detail: dict[str, Any],
    ) -> None:
        payload = {**dict(detail), "subsource": reason.value}
        await safe_record_cancellation_cause(
            self._redis,
            repo,
            task_id,
            CancellationCause(category="ERROR", payload=payload),
        )

    async def clear(self, repo: str, task_id: str) -> None:
        await safe_delete_cancellation_cause(self._redis, repo, task_id)

    async def is_suppressed(
        self,
        repo: str,
        task_id: str,
    ) -> SuppressionRecord | None:
        cause = await get_cancellation_cause(self._redis, repo, task_id)
        if cause is None:
            return None
        return self._record_from_cause(cause)

    async def list_suppressed(
        self,
        repo: str,
        *,
        since: datetime | None = None,
        limit: int | None = None,
    ) -> list[SuppressionRecord]:
        since = since or datetime.fromtimestamp(0, tz=timezone.utc)
        causes = await list_recent_cancellations(self._redis, repo, since)
        records = [self._record_from_cause(cause) for cause in causes]
        if limit is None:
            return records
        return records[: max(limit, 0)]

    def _record_from_cause(self, cause: CancellationCause) -> SuppressionRecord:
        payload = cause.payload if isinstance(cause.payload, dict) else {}
        reason = self._reason_from_cause(cause, payload)
        detail = dict(payload)
        detail.pop("subsource", None)
        return SuppressionRecord(
            task_id=cause.task_id,
            reason=reason,
            detail=detail,
            created_at=_parse_created_at(cause.created_at),
            approved_once=bool(payload.get("approved_once", False)),
        )

    def _reason_from_cause(
        self,
        cause: CancellationCause,
        payload: dict[str, Any],
    ) -> SuppressionReason:
        subsource = payload.get("subsource")
        if isinstance(subsource, str) and lookup(subsource) is not None:
            return SuppressionReason(subsource)
        classified = classify_cancellation_subsource(cause, log=lambda _msg: None)
        if lookup(classified) is not None:
            return SuppressionReason(classified)
        self._log_unknown_subsource_once(subsource)
        return SuppressionReason.CRASH

    def _log_unknown_subsource_once(self, subsource: Any) -> None:
        key = repr(subsource)
        if key in self._warned_unknown_subsources:
            return
        self._warned_unknown_subsources.add(key)
        logger.warning(
            "Unknown suppression subsource %r; defaulting suppression reason to crash",
            subsource,
        )


def _parse_created_at(raw: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(raw)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


__all__ = ["RedisSuppressionStore"]
