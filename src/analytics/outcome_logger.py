"""Append a merged-PR outcome record to the per-month JSONL log.

The log lives at ``<analytics_dir>/<year>-<month>.jsonl`` where
``analytics_dir`` defaults to ``/data/analytics`` and may be overridden
via the ``PO_ANALYTICS_DIR`` environment variable for tests and
multi-tenant deployments. Writes are guarded by an exclusive
``fcntl.flock`` on a shared lock file to make concurrent appends from
multiple daemon processes safe (low contention in practice — at most one
merge per repo per cycle — but the lock keeps the contract honest if the
daemon ever forks per repo).
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from src.analytics.schema import OUTCOME_FIELDS, validate_outcome_record

_ANALYTICS_DIR_ENV = "PO_ANALYTICS_DIR"
_DEFAULT_ANALYTICS_DIR = "/data/analytics"

logger = logging.getLogger(__name__)


def compute_task_id_hash(pr_id: str, repo_slug: str) -> str:
    """Return the deterministic SHA-256 hex of ``pr_id::repo_slug``.

    The hash is the schema's anonymizable identity for a task. Future
    opt-in telemetry can ship the hash instead of the raw ``pr_id`` and
    ``repo_slug`` pair without changing the local log format.
    """
    payload = f"{pr_id}::{repo_slug}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _resolve_analytics_dir() -> Path:
    return Path(os.environ.get(_ANALYTICS_DIR_ENV) or _DEFAULT_ANALYTICS_DIR)


def _partition_path(when: datetime, analytics_dir: Path) -> Path:
    return analytics_dir / f"{when.year:04d}-{when.month:02d}.jsonl"


def _resolve_partition_when(record: dict) -> datetime:
    """Return the timestamp that decides which monthly partition wins.

    Prefer the record's ``merged_at`` so out-of-order writes always land
    in the partition that matches the merge time. Fall back to ``now()``
    when ``merged_at`` is missing or malformed (defensive — the caller
    should always populate it for a real merge event).
    """
    merged_at = record.get("merged_at")
    if isinstance(merged_at, str) and merged_at:
        try:
            return datetime.fromisoformat(merged_at.replace("Z", "+00:00"))
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def _contains_task_outcome(analytics_dir: Path, task_id_hash: str) -> bool:
    """Read durable completion keys, tolerating damaged or older log rows."""
    for partition in analytics_dir.glob("*.jsonl"):
        with open(partition, "rb") as existing:
            for line in existing:
                try:
                    row = json.loads(line)
                except (ValueError, UnicodeDecodeError):
                    continue
                if isinstance(row, dict) and row.get("task_id_hash") == task_id_hash:
                    return True
    return False


def has_merged_outcome(pr_id: str, repo_slug: str) -> bool:
    """Return whether durable analytics already accounts for this task.

    An absent row permits recovery to replay; the append lock deduplicates
    against concurrent completions. Storage errors propagate to defer recovery.
    """
    return _contains_task_outcome(
        _resolve_analytics_dir(), compute_task_id_hash(pr_id, repo_slug),
    )


def log_merged_pr(pr_data: dict) -> None:
    """Validate and append one outcome per repository/task identity.

    The caller assembles every field defined in ``OUTCOME_FIELDS``; this
    function does not invent values. Fields with no available data must
    be passed as ``None`` (written as JSON ``null``) to keep the schema
    stable across rows. The ``task_id_hash`` field is recomputed here from
    ``pr_id`` + ``repo_slug`` so callers cannot accidentally drift from
    the canonical hash.
    """
    record = dict(pr_data)
    pr_id = record.get("pr_id")
    repo_slug = record.get("repo_slug")
    if isinstance(pr_id, str) and isinstance(repo_slug, str):
        record["task_id_hash"] = compute_task_id_hash(pr_id, repo_slug)

    validate_outcome_record(record)

    when = _resolve_partition_when(record)
    analytics_dir = _resolve_analytics_dir()
    analytics_dir.mkdir(parents=True, exist_ok=True)
    target = _partition_path(when, analytics_dir)

    serialized = json.dumps(
        {field: record[field] for field in OUTCOME_FIELDS},
        sort_keys=True,
        ensure_ascii=False,
    )

    # Recovery may replay after the append but before clearing task ownership.
    # Lock across partitions: a replay can occur in a later month, and must
    # detect the original row before appending. Existing logs supply the
    # durable completion key without a second, crash-sensitive marker write.
    with open(analytics_dir / ".outcomes.lock", "a", encoding="utf-8") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            if _contains_task_outcome(analytics_dir, record["task_id_hash"]):
                return
            with open(target, "a+b") as handle:
                # A crashed writer may have left a truncated final row. Keep
                # the new outcome on its own line so later scans can find it.
                if handle.tell():
                    handle.seek(-1, os.SEEK_END)
                    if handle.read(1) != b"\n":
                        handle.write(b"\n")
                handle.write((serialized + "\n").encode("utf-8"))
                handle.flush()
                os.fsync(handle.fileno())
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
