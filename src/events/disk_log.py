"""Disk-persistent event log mirror.

Mirrors every event from :func:`src.events.publish_repo_event` to a JSONL
file under ``/data/events/<repo>/YYYY-MM-DD.jsonl``. The disk log survives
Redis flushes and provides forensic substrate for incidents older than the
50-entry Redis history cap.

Failures are swallowed (best-effort): the daemon must never block on a
disk write. ``fcntl.flock`` guards concurrent writes from multiple
workers so two appends never interleave bytes within one line.
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_EVENTS_DIR_ENV = "PO_EVENTS_DIR"
_DEFAULT_EVENTS_DIR = "/data/events"

logger = logging.getLogger(__name__)


def _resolve_events_dir() -> Path:
    return Path(os.environ.get(_EVENTS_DIR_ENV) or _DEFAULT_EVENTS_DIR)


def append_event_to_disk(
    repo_slug: str,
    event_type: str,
    payload: dict[str, Any],
    timestamp: datetime | None = None,
) -> None:
    """Best-effort append of one event to today's JSONL partition.

    Failures (filesystem errors, non-serializable payload, etc.) are
    logged at WARNING and swallowed; the daemon never blocks on a disk
    write. ``default=str`` is used so payloads carrying ``datetime``,
    ``set``, ``Path`` etc. fall back to their ``str()`` form rather than
    raising ``TypeError``.
    """
    now = timestamp or datetime.now(timezone.utc)
    record = {
        "timestamp": now.isoformat(),
        "event_type": event_type,
        "repo_slug": repo_slug,
        "payload": payload,
    }
    try:
        line = json.dumps(record, separators=(",", ":"), default=str) + "\n"
        repo_dir = _resolve_events_dir() / repo_slug
        repo_dir.mkdir(parents=True, exist_ok=True)
        filename = repo_dir / f"{now.strftime('%Y-%m-%d')}.jsonl"
        with open(filename, "a", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                handle.write(line)
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except (OSError, TypeError, ValueError):
        logger.warning(
            "Event disk write failed for %s/%s",
            repo_slug,
            event_type,
            exc_info=True,
        )
