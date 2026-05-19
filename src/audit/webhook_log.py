"""Webhook delivery audit log.

Records every webhook POST attempt to disk. URL hashed to avoid leaking
secrets into forensic log. Best-effort write: audit failures swallowed.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

WEBHOOK_AUDIT_DIR = Path("/data/audit/webhooks")

logger = logging.getLogger(__name__)


def webhook_url_hash(webhook_url: str) -> str:
    """Return the stable, non-secret audit identifier for a webhook URL."""
    return "sha256:" + hashlib.sha256(webhook_url.encode("utf-8")).hexdigest()[:16]


def write_webhook_audit(
    event_type: str,
    webhook_url: str,
    payload_size_bytes: int,
    attempt_number: int,
    http_status: int | None,
    response_excerpt: str,
    elapsed_ms: float,
    retry_scheduled_at: datetime | None = None,
) -> None:
    """Append one JSONL record for a webhook delivery attempt."""
    now = datetime.now(timezone.utc)
    record: dict[str, Any] = {
        "timestamp": now.isoformat(),
        "event_type": event_type,
        "url_hash": webhook_url_hash(webhook_url),
        "payload_size_bytes": payload_size_bytes,
        "attempt_number": attempt_number,
        "http_status": http_status,
        "response_excerpt": response_excerpt[:200],
        "elapsed_ms": elapsed_ms,
    }
    if retry_scheduled_at is not None:
        record["retry_scheduled_at"] = retry_scheduled_at.isoformat()

    line = json.dumps(record, separators=(",", ":")) + "\n"
    try:
        type_dir = WEBHOOK_AUDIT_DIR / event_type
        type_dir.mkdir(parents=True, exist_ok=True)
        filename = type_dir / f"{now.strftime('%Y-%m-%d')}.jsonl"
        with open(filename, "a", encoding="utf-8") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                f.write(line)
                f.flush()
                os.fsync(f.fileno())
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    except OSError:
        logger.warning("Webhook audit write failed for %s", event_type, exc_info=True)
