"""Permanent accepted-specification receipts, separate from expiring run history.

An attempt is the operator's accepted implementation, possibly spanning many
coder runs and ordinary Retries. Rejection and replacement are fenced by this
identity, never by a reusable task ID or branch alone.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

from src.cancellation.storage import task_spec_content_hash
from src.models import QueueTask


class AttemptChanged(ValueError):
    """An operator command or specification refers to obsolete work."""


class AdmissionRejected(AttemptChanged):
    """This staged input cannot be admitted by retrying the same submission."""


class TaskAttempt(BaseModel):
    attempt_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    repo_url: str
    task: QueueTask
    fingerprint: str
    file_sha256: str
    accepted_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    base_commit: str = ""
    started: bool = False
    # False is explicit new-admission evidence. Mark True before dispatch;
    # missing legacy evidence stays unknown rather than proving non-creation.
    coder_dispatched: bool | None = None
    pr_number: int | None = None
    pr_creation_pending: bool = False
    rejection: str | None = None
    completed: bool = False
    previous_rejection: str | None = None
    admission_pending: bool = False
    branch_prepared: bool = False


def attempt_key(repo: str, task_id: str) -> str:
    return f"task_attempt:{repo}:{task_id}"


async def load_attempt(redis: Any, repo: str, task_id: str) -> TaskAttempt | None:
    raw = await redis.get(attempt_key(repo, task_id))
    return TaskAttempt.model_validate_json(raw) if raw else None


async def clear_failed_pr_creation(redis: Any, repo: str, attempt: TaskAttempt) -> None:
    """Record definite non-creation without overwriting concurrent operator decisions."""
    key = attempt_key(repo, attempt.task.pr_id)

    async def transaction(pipe: Any) -> None:
        raw = await pipe.get(key)
        current = TaskAttempt.model_validate_json(raw) if raw else None
        if current is None or current.attempt_id != attempt.attempt_id:
            raise AttemptChanged("PR creation failure belongs to an obsolete attempt.")
        if not current.pr_creation_pending:
            return
        current.pr_creation_pending = False
        pipe.multi()
        pipe.set(key, current.model_dump_json())

    await redis.transaction(transaction, key)


def new_attempt(repo_url: str, task: QueueTask, content: str, **kwargs: Any) -> TaskAttempt:
    return TaskAttempt(
        repo_url=repo_url,
        task=task.model_copy(deep=True),
        fingerprint=task_spec_content_hash(content),
        file_sha256=hashlib.sha256(content.encode()).hexdigest(),
        **kwargs,
    )


async def save_attempt(
    redis: Any,
    repo: str,
    attempt: TaskAttempt,
    *,
    expected: TaskAttempt | None,
) -> TaskAttempt:
    """CAS a receipt; lost EXEC replies can be replayed without a new identity."""
    key = attempt_key(repo, attempt.task.pr_id)

    async def transaction(pipe: Any) -> TaskAttempt:
        raw = await pipe.get(key)
        current = TaskAttempt.model_validate_json(raw) if raw else None
        if current == attempt:
            return current
        if current != expected:
            raise AttemptChanged("Task attempt changed; refresh before continuing.")
        pipe.multi()
        pipe.set(key, attempt.model_dump_json())
        return attempt

    return await redis.transaction(transaction, key, value_from_callable=True)
