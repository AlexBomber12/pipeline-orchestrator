"""Cancellation cause storage - Redis schema for OBS-BE expanded.

The cancellation policy substrate. Detection paths (PR-253) write
through these helpers; UI (PR-254) reads through them. Centralizing
the schema here makes the contract bisectable from its consumers.
PR-252.

PR-315 collapsed the five legacy ``category`` values (``CRASH``,
``ESCALATE``, ``TIMEOUT``, ``INFRA``, ``NO_PUSH_DEADLOCK``) into a
single canonical ``ERROR``. Forensic detail moves to
``payload.subsource`` with the stable vocabulary:

* ``crash`` — daemon process died mid-operation
* ``coder_escalate`` — coder stdout contained an explicit ``ESCALATE:``
  marker
* ``guardrail`` — Tier 1/2 guardrail violation
* ``review_timeout`` — WATCH review_timeout exceeded
* ``fix_idle_timeout`` — FIX cycle idle without push
* ``fix_iteration_cap`` — FIX iteration count exceeded
* ``no_push_deadlock`` — coder claimed fix without git push
* ``infra_failure`` — repeated INFRA failures past grace period

The ``escalate_to_error`` startup migration rewrites legacy records
in place and preserves the original detector value as
``payload.legacy_category`` for forensic recall in the UI.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

CATEGORIES = ("ERROR",)

TTL_SECONDS = 30 * 24 * 3600


@dataclass
class CancellationCause:
    """Structured cancellation reason persisted per task."""

    category: str
    payload: dict[str, Any] = field(default_factory=dict)
    created_at: str = ""
    task_id: str = ""
    repo_slug: str = ""

    def to_redis(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))

    @classmethod
    def from_redis(cls, raw: str | bytes) -> "CancellationCause":
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return cls(**json.loads(raw))


@dataclass(frozen=True)
class GuardrailPending:
    """Pending guardrail-flagged cancellation surfaced to operator override UI."""

    repo_slug: str
    task_id: str
    rule: str
    excerpt: str
    recorded_at: int


def cause_key(repo_slug: str, task_id: str) -> str:
    return f"cancellation:{repo_slug}:{task_id}"


def index_key(repo_slug: str) -> str:
    return f"cancellation_index:{repo_slug}"


def task_spec_hash_key(repo_slug: str, task_id: str) -> str:
    return f"task_spec_hash:{repo_slug}:{task_id}"


def retry_count_key(repo_slug: str, task_id: str) -> str:
    return f"metrics:retry_count:{repo_slug}:{task_id}"


def current_run_started_at_key(repo_slug: str, task_id: str) -> str:
    """Key for the per-task dispatch timestamp of the currently active run."""
    return f"current_run_started_at:{repo_slug}:{task_id}"


def task_spec_content_hash(task_text: str) -> str:
    """Hash task spec content excluding daemon-managed frontmatter status."""
    lines = task_text.splitlines(keepends=True)
    first_content_index = next(
        (index for index, raw_line in enumerate(lines) if raw_line.strip()),
        None,
    )
    if first_content_index is None or lines[first_content_index].rstrip() != "---":
        normalized = lines
    else:
        closing_index = next(
            (
                index
                for index, raw_line in enumerate(lines[first_content_index + 1 :], start=first_content_index + 1)
                if raw_line.rstrip() == "---"
            ),
            None,
        )
        if closing_index is None:
            normalized = lines
        else:
            frontmatter = [
                raw_line
                for raw_line in lines[first_content_index + 1 : closing_index]
                if not re.match(r"^status:\s*", raw_line.rstrip())
            ]
            if any(raw_line.strip() for raw_line in frontmatter):
                normalized = (
                    lines[: first_content_index + 1]
                    + frontmatter
                    + lines[closing_index:]
                )
            else:
                body_start = closing_index + 1
                if body_start < len(lines) and not lines[body_start].strip():
                    body_start += 1
                normalized = lines[:first_content_index] + lines[body_start:]
    return hashlib.sha256("".join(normalized).encode("utf-8")).hexdigest()


async def record_cancellation_cause(
    redis_client: Any,
    repo_slug: str,
    task_id: str,
    cause: CancellationCause,
) -> None:
    """Persist a cancellation cause for later UI surfacing."""
    if not cause.created_at:
        cause.created_at = datetime.now(timezone.utc).isoformat()
    cause.task_id = task_id
    cause.repo_slug = repo_slug
    serialized = cause.to_redis()
    score = datetime.fromisoformat(cause.created_at).timestamp()
    expiry_cutoff = datetime.now(timezone.utc).timestamp() - TTL_SECONDS
    pipe = redis_client.pipeline()
    pipe.set(cause_key(repo_slug, task_id), serialized, ex=TTL_SECONDS)
    pipe.zadd(index_key(repo_slug), {task_id: score})
    pipe.zremrangebyscore(index_key(repo_slug), "-inf", f"({expiry_cutoff}")
    pipe.expire(index_key(repo_slug), TTL_SECONDS)
    await pipe.execute()


async def get_cancellation_cause(
    redis_client: Any,
    repo_slug: str,
    task_id: str,
) -> CancellationCause | None:
    raw = await redis_client.get(cause_key(repo_slug, task_id))
    if raw is None:
        return None
    return CancellationCause.from_redis(raw)


async def delete_cancellation_cause(
    redis_client: Any,
    repo_slug: str,
    task_id: str,
) -> None:
    """Drop a previously-recorded cause and its index entry.

    Called when a task transitions out of ERROR back to IDLE for retry,
    so a later success does not leave a stale CRASH/INFRA/TIMEOUT record
    in Redis for the 30-day TTL window.
    """
    await redis_client.delete(cause_key(repo_slug, task_id))
    await redis_client.zrem(index_key(repo_slug), task_id)


async def record_task_spec_hash(
    redis_client: Any,
    repo_slug: str,
    task_id: str,
    spec_hash: str,
) -> None:
    """Persist the last task spec content hash attempted by the daemon."""
    await redis_client.set(
        task_spec_hash_key(repo_slug, task_id),
        spec_hash,
        ex=TTL_SECONDS,
    )


async def get_task_spec_hash(
    redis_client: Any,
    repo_slug: str,
    task_id: str,
) -> str | None:
    raw = await redis_client.get(task_spec_hash_key(repo_slug, task_id))
    if raw is None:
        return None
    if isinstance(raw, bytes):
        return raw.decode("utf-8")
    return str(raw)


async def delete_task_spec_hash(
    redis_client: Any,
    repo_slug: str,
    task_id: str,
) -> None:
    await redis_client.delete(task_spec_hash_key(repo_slug, task_id))


async def record_current_run_started_at(
    redis_client: Any,
    repo_slug: str,
    task_id: str,
    started_at: datetime | None = None,
) -> None:
    """Persist the dispatch timestamp anchoring the current run.

    PR-318 follow-up: ``_dispatch_recovery_branch`` compares
    ``cause.created_at`` against this value to detect stale cancellation
    causes left behind by a prior run when ``safe_delete_cancellation_cause``
    (best-effort) failed to clear them. A cause whose ``created_at`` predates
    the current run's dispatch timestamp does not belong to this run and
    must not be trusted to classify a mid-CODING crash as operator-attention.

    Callers should invoke this from the CODING entry path on every dispatch
    so the marker tracks the latest run. The 30-day TTL matches the
    cancellation cause TTL, and natural overwriting on the next dispatch
    keeps the marker fresh without explicit cleanup.
    """
    if started_at is None:
        started_at = datetime.now(timezone.utc)
    await redis_client.set(
        current_run_started_at_key(repo_slug, task_id),
        started_at.isoformat(),
        ex=TTL_SECONDS,
    )


async def get_current_run_started_at(
    redis_client: Any,
    repo_slug: str,
    task_id: str,
) -> datetime | None:
    """Return the dispatch timestamp recorded for ``task_id``, or ``None``."""
    raw = await redis_client.get(current_run_started_at_key(repo_slug, task_id))
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        parsed = datetime.fromisoformat(raw)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


async def delete_current_run_started_at(
    redis_client: Any,
    repo_slug: str,
    task_id: str,
) -> None:
    """Drop the dispatch timestamp; used when a task definitively ends."""
    await redis_client.delete(current_run_started_at_key(repo_slug, task_id))


async def reset_retry_count(
    redis_client: Any,
    repo_slug: str,
    task_id: str,
) -> None:
    await redis_client.set(retry_count_key(repo_slug, task_id), "0", ex=TTL_SECONDS)


async def delete_retry_count(
    redis_client: Any,
    repo_slug: str,
    task_id: str,
) -> None:
    await redis_client.delete(retry_count_key(repo_slug, task_id))


async def list_recent_cancellations(
    redis_client: Any,
    repo_slug: str,
    since: datetime,
) -> list[CancellationCause]:
    """Return causes recorded for repo at or after ``since``, newest first."""
    since_ts = since.timestamp()
    task_ids = await redis_client.zrangebyscore(
        index_key(repo_slug), since_ts, "+inf"
    )
    causes: list[CancellationCause] = []
    stale: list[str] = []
    for tid in task_ids or []:
        if isinstance(tid, bytes):
            tid = tid.decode("utf-8")
        cause = await get_cancellation_cause(redis_client, repo_slug, tid)
        if cause is None:
            stale.append(tid)
        else:
            causes.append(cause)
    if stale:
        await redis_client.zrem(index_key(repo_slug), *stale)
    causes.sort(key=lambda c: datetime.fromisoformat(c.created_at), reverse=True)
    return causes


async def list_pending_guardrail_decisions(
    redis_client: Any,
    repo_slug: str,
    *,
    limit: int = 100,
) -> list[GuardrailPending]:
    """Return guardrail-flagged pending cancellations for repo, oldest first.

    Reads ``cancellation_index:{repo}``, filters entries whose
    ``payload.subsource == "guardrail"``, returns a list of typed
    ``GuardrailPending`` records sorted ascending by ``recorded_at``
    (oldest first — operator triages in arrival order). Bounded by
    ``limit`` (default 100) so one repo with many pending decisions
    cannot block the dashboard; the helper paginates the underlying
    ZSET in batches of ``limit`` rather than fetching the full set
    upfront, so per-request Redis transfer stays O(limit) when most
    indexed entries are guardrail-flagged.

    Stale index members (where the cause payload has expired or been
    deleted) are removed in-line so subsequent calls do not re-scan
    them, mirroring ``list_recent_cancellations``.
    """
    key = index_key(repo_slug)
    result: list[GuardrailPending] = []
    stale: list[str] = []
    batch_size = max(limit, 1)
    cursor = 0
    while len(result) < limit:
        batch = await redis_client.zrange(
            key, cursor, cursor + batch_size - 1, withscores=False
        )
        if not batch:
            break
        for tid in batch:
            if isinstance(tid, bytes):
                tid = tid.decode("utf-8")
            cause = await get_cancellation_cause(redis_client, repo_slug, tid)
            if cause is None:
                stale.append(tid)
                continue
            if cause.payload.get("subsource") != "guardrail":
                continue
            rule = cause.payload.get("rule", "")
            excerpt = cause.payload.get("excerpt", "")
            recorded_at = int(datetime.fromisoformat(cause.created_at).timestamp())
            result.append(
                GuardrailPending(
                    repo_slug=repo_slug,
                    task_id=tid,
                    rule=rule,
                    excerpt=excerpt,
                    recorded_at=recorded_at,
                )
            )
            if len(result) >= limit:
                break
        if len(batch) < batch_size:
            break
        cursor += batch_size
    if stale:
        await redis_client.zrem(key, *stale)
    return sorted(result, key=lambda p: p.recorded_at)
