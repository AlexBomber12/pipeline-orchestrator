"""Suppression store port.

The suppression model has two storage layers:

* Coarse, durable layer: the operator-visible fact that a task is suppressed
  and its canonical reason is intended to live in task frontmatter
  (``status:error`` plus ``blocked_reason``). R1.3/R1.4 wire that layer.
  This port documents the contract, but the Redis implementation in PR-377
  does not write frontmatter yet.
* Rich, lost-tolerant layer: forensic detail, ``approved_once``, counters, and
  related debugging payload live in Redis. The PR-377 implementation reads and
  writes only this rich layer by wrapping the existing cancellation storage.

Until the durable frontmatter layer lands, ``is_suppressed`` answers from the
rich Redis layer only. Production mechanisms are not migrated to this port in
PR-377; it is a scaffold over the existing cancellation cause schema.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

from src.subsource_registry import SuppressionReason


@dataclass(frozen=True)
class SuppressionRecord:
    """Active suppression returned by a ``SuppressionStore``."""

    task_id: str
    reason: SuppressionReason
    detail: dict[str, Any] = field(default_factory=dict)
    created_at: datetime | None = None
    approved_once: bool = False


class SuppressionStore(Protocol):
    """Port for answering whether a task is suppressed and why."""

    async def suppress(
        self,
        repo: str,
        task_id: str,
        reason: SuppressionReason,
        detail: dict[str, Any],
    ) -> None:
        """Record that ``task_id`` is suppressed for ``reason``."""
        ...

    async def clear(self, repo: str, task_id: str) -> None:
        """Remove the active suppression for ``task_id``."""
        ...

    async def is_suppressed(
        self,
        repo: str,
        task_id: str,
    ) -> SuppressionRecord | None:
        """Return the active suppression for ``task_id``, if any."""
        ...

    async def list_suppressed(
        self,
        repo: str,
        *,
        since: datetime | None = None,
        limit: int | None = None,
    ) -> list[SuppressionRecord]:
        """Return active suppressions for ``repo`` newest first."""
        ...


__all__ = ["SuppressionReason", "SuppressionRecord", "SuppressionStore"]
