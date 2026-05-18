"""Operator action audit log.

Append-only JSONL log of destructive operator actions for forensic
review. File rotation daily by filename. fcntl.flock guards concurrent
writes from multiple uvicorn workers.
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

AUDIT_DIR = Path("/data/audit/operator-actions")

logger = logging.getLogger(__name__)


def write_audit_record(
    action: str,
    repo_slug: str,
    task_id: str,
    payload: dict[str, Any],
) -> None:
    """Append one JSONL record for an operator action.

    Failures are swallowed: audit is best-effort, not a blocker for the
    operator action. Operator visibility into write failure is via
    docker logs.
    """
    now = datetime.now(timezone.utc)
    record = {
        "timestamp": now.isoformat(),
        "action": action,
        "repo_slug": repo_slug,
        "task_id": task_id,
        "payload": payload,
    }
    line = json.dumps(record, separators=(",", ":")) + "\n"
    try:
        AUDIT_DIR.mkdir(parents=True, exist_ok=True)
        filename = AUDIT_DIR / f"{now.strftime('%Y-%m-%d')}.jsonl"
        with open(filename, "a", encoding="utf-8") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                f.write(line)
                # Hold the lock through flush+fsync so another worker
                # cannot acquire LOCK_EX and interleave its own line
                # while ours is still buffered in userspace or the OS
                # page cache.
                f.flush()
                os.fsync(f.fileno())
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    except OSError:
        logger.warning(
            "Audit write failed for %s/%s", repo_slug, task_id, exc_info=True
        )
