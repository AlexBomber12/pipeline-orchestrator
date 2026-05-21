"""Operator action audit log.

Append-only JSONL log of destructive operator actions for forensic
review. File rotation daily by filename. fcntl.flock guards concurrent
writes from multiple uvicorn workers.

Flat operator events include ``quarantine_apply`` and
``quarantine_release``.
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


def write_operator_action_audit(
    action: str,
    repo: str,
    pr: int | None = None,
    operator_session_id: str | None = None,
    **payload: Any,
) -> None:
    """Append one flat JSONL record for PR-level operator actions."""
    now = datetime.now(timezone.utc)
    record: dict[str, Any] = {
        "ts": now.isoformat(),
        "event": action,
        "repo": repo,
    }
    if pr is not None:
        record["pr"] = pr
    if operator_session_id is not None:
        record["operator_session_id"] = operator_session_id
    record.update(payload)
    line = json.dumps(record, separators=(",", ":")) + "\n"
    try:
        AUDIT_DIR.mkdir(parents=True, exist_ok=True)
        filename = AUDIT_DIR / f"{now.strftime('%Y-%m-%d')}.jsonl"
        with open(filename, "a", encoding="utf-8") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                f.write(line)
                f.flush()
                os.fsync(f.fileno())
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    except OSError:
        logger.warning("Audit write failed for %s", repo, exc_info=True)
