"""Durable operator Retry command schema and Redis storage operations.

The web process only validates and enqueues these records.  A repository
runner claims them at a safe point and records every transition before and
after externally visible side effects.  The pending sorted set is the durable
delivery mechanism; wake-channel messages merely shorten the polling delay.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field

from src.cancellation import retry_count_key
from src.keyspace import (
    retry_command,
    retry_command_dedupe,
    retry_command_latest,
    retry_command_pending,
)

COMMAND_TTL_SECONDS = 90 * 24 * 3600
RETRY_COUNT_TTL_SECONDS = 30 * 24 * 3600
PROCESSING_LEASE_SECONDS = 5 * 60
TRANSITION_HISTORY_LIMIT = 50


class RetryCommandStatus(StrEnum):
    QUEUED = "queued"
    PROCESSING = "processing"
    APPLIED = "applied"
    DEFERRED = "deferred"
    FAILED = "failed"


class RetryExecutionState(StrEnum):
    NOT_STARTED = "not_started"
    PENDING = "pending"
    RUNNING = "running"
    WATCHING = "watching"
    COMPLETED = "completed"
    FAILED = "failed"
    UNCERTAIN = "uncertain"


class RetryEffectStage(StrEnum):
    NONE = "none"
    RETRY_RESERVED = "retry_reserved"
    STATUS_COMMITTED = "status_committed"
    APPLIED = "applied"


class RetryCommandTransition(BaseModel):
    status: RetryCommandStatus
    recorded_at: datetime
    reason: str
    continuation: str | None = None


class RetryCommand(BaseModel):
    command_id: str
    repo_slug: str
    task_id: str
    task_file: str
    task_branch: str
    task_fingerprint: str
    request_binding: str
    failure_id: str
    failure_subsource: str | None = None
    failure_created_at: str | None = None
    bound_pr_number: int | None = None
    bound_pr_branch: str | None = None
    bound_pr_head_sha: str | None = None
    requested_at: datetime
    updated_at: datetime
    status: RetryCommandStatus = RetryCommandStatus.QUEUED
    outcome_reason: str = "Waiting for daemon acknowledgement."
    selected_continuation: str | None = None
    retry_count: int | None = None
    retry_cap: int
    processing_owner: str | None = None
    processing_started_at: datetime | None = None
    processing_attempts: int = 0
    effect_stage: RetryEffectStage = RetryEffectStage.NONE
    reset_counters: list[str] = Field(default_factory=list)
    execution_state: RetryExecutionState = RetryExecutionState.NOT_STARTED
    execution_started_at: datetime | None = None
    history: list[RetryCommandTransition] = Field(default_factory=list)


class RetryCapReached(RuntimeError):
    def __init__(self, current: int, cap: int) -> None:
        super().__init__(f"retry cap reached ({current}/{cap})")
        self.current = current
        self.cap = cap


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def retry_request_binding(
    *,
    repo_slug: str,
    task_id: str,
    task_file: str,
    task_branch: str,
    task_fingerprint: str,
    failure_id: str,
    retry_ordinal: int,
    pr_number: int | None,
    pr_branch: str | None,
    pr_head_sha: str | None,
) -> str:
    """Return the stable idempotency/staleness token rendered by the UI."""
    payload = json.dumps(
        {
            "failure_id": failure_id,
            "pr_branch": pr_branch,
            "pr_head_sha": pr_head_sha,
            "pr_number": pr_number,
            "repo": repo_slug,
            "retry_ordinal": retry_ordinal,
            "task_branch": task_branch,
            "task_file": task_file,
            "task_fingerprint": task_fingerprint,
            "task_id": task_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def new_retry_command(
    *,
    repo_slug: str,
    task_id: str,
    task_file: str,
    task_branch: str,
    task_fingerprint: str,
    request_binding: str,
    failure_id: str,
    retry_cap: int,
    failure_subsource: str | None = None,
    failure_created_at: str | None = None,
    bound_pr_number: int | None = None,
    bound_pr_branch: str | None = None,
    bound_pr_head_sha: str | None = None,
    now: datetime | None = None,
) -> RetryCommand:
    moment = now or utc_now()
    command = RetryCommand(
        command_id=str(uuid.uuid4()),
        repo_slug=repo_slug,
        task_id=task_id,
        task_file=task_file,
        task_branch=task_branch,
        task_fingerprint=task_fingerprint,
        request_binding=request_binding,
        failure_id=failure_id,
        failure_subsource=failure_subsource,
        failure_created_at=failure_created_at,
        bound_pr_number=bound_pr_number,
        bound_pr_branch=bound_pr_branch,
        bound_pr_head_sha=bound_pr_head_sha,
        requested_at=moment,
        updated_at=moment,
        retry_cap=retry_cap,
    )
    command.history.append(
        RetryCommandTransition(
            status=RetryCommandStatus.QUEUED,
            recorded_at=moment,
            reason=command.outcome_reason,
        )
    )
    return command


def _decode_text(raw: object) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, bytes):
        return raw.decode("utf-8")
    return str(raw)


def _decode_count(raw: object) -> int:
    text = _decode_text(raw)
    if text is None:
        return 0
    try:
        return max(0, int(text))
    except ValueError:
        return 0


def _serialize(command: RetryCommand) -> str:
    return command.model_dump_json()


def _parse(raw: object) -> RetryCommand | None:
    text = _decode_text(raw)
    if text is None:
        return None
    try:
        return RetryCommand.model_validate_json(text)
    except Exception:
        return None


def _append_transition(
    command: RetryCommand,
    *,
    status: RetryCommandStatus,
    reason: str,
    continuation: str | None,
    now: datetime,
) -> None:
    transition = RetryCommandTransition(
        status=status,
        recorded_at=now,
        reason=reason,
        continuation=continuation,
    )
    if command.history and command.history[-1].model_dump() == transition.model_dump():
        return
    command.history.append(transition)
    if len(command.history) > TRANSITION_HISTORY_LIMIT:
        command.history = command.history[-TRANSITION_HISTORY_LIMIT:]


async def load_retry_command(
    redis_client: Any, repo_slug: str, command_id: str
) -> RetryCommand | None:
    return _parse(await redis_client.get(retry_command(repo_slug, command_id)))


async def load_retry_command_for_binding(
    redis_client: Any, repo_slug: str, request_binding: str
) -> RetryCommand | None:
    command_id = _decode_text(
        await redis_client.get(retry_command_dedupe(repo_slug, request_binding))
    )
    if command_id is None:
        return None
    return await load_retry_command(redis_client, repo_slug, command_id)


async def load_latest_retry_command(
    redis_client: Any, repo_slug: str, task_id: str
) -> RetryCommand | None:
    command_id = _decode_text(
        await redis_client.get(retry_command_latest(repo_slug, task_id))
    )
    if command_id is None:
        return None
    return await load_retry_command(redis_client, repo_slug, command_id)


async def enqueue_retry_command(
    redis_client: Any, command: RetryCommand
) -> tuple[RetryCommand, bool]:
    """Persist ``command`` atomically or return its idempotent predecessor."""
    command_key = retry_command(command.repo_slug, command.command_id)
    dedupe_key = retry_command_dedupe(command.repo_slug, command.request_binding)
    latest_key = retry_command_latest(command.repo_slug, command.task_id)
    pending_key = retry_command_pending(command.repo_slug)

    async def _transaction(pipe: Any) -> tuple[RetryCommand, bool]:
        existing_id = _decode_text(await pipe.get(dedupe_key))
        if existing_id is not None:
            existing = _parse(
                await pipe.get(retry_command(command.repo_slug, existing_id))
            )
            if existing is not None:
                return existing, False
        pipe.multi()
        pipe.set(command_key, _serialize(command), ex=COMMAND_TTL_SECONDS)
        pipe.set(dedupe_key, command.command_id, ex=COMMAND_TTL_SECONDS)
        pipe.set(latest_key, command.command_id, ex=COMMAND_TTL_SECONDS)
        pipe.zadd(pending_key, {command.command_id: command.requested_at.timestamp()})
        pipe.expire(pending_key, COMMAND_TTL_SECONDS)
        return command, True

    return await redis_client.transaction(
        _transaction,
        dedupe_key,
        value_from_callable=True,
    )


async def list_pending_retry_commands(
    redis_client: Any, repo_slug: str
) -> list[RetryCommand]:
    pending_key = retry_command_pending(repo_slug)
    command_ids = await redis_client.zrangebyscore(pending_key, "-inf", "+inf")
    commands: list[RetryCommand] = []
    stale: list[str] = []
    for raw_id in command_ids or []:
        command_id = _decode_text(raw_id)
        if command_id is None:
            continue
        command = await load_retry_command(redis_client, repo_slug, command_id)
        if command is None:
            stale.append(command_id)
        else:
            commands.append(command)
    if stale:
        await redis_client.zrem(pending_key, *stale)
    return commands


async def claim_retry_command(
    redis_client: Any,
    repo_slug: str,
    command_id: str,
    owner: str,
    *,
    now: datetime | None = None,
) -> RetryCommand | None:
    """Claim a queued/deferred command, recovering an expired processing lease."""
    moment = now or utc_now()
    command_key = retry_command(repo_slug, command_id)
    pending_key = retry_command_pending(repo_slug)

    async def _transaction(pipe: Any) -> RetryCommand | None:
        command = _parse(await pipe.get(command_key))
        if command is None:
            pipe.multi()
            pipe.zrem(pending_key, command_id)
            return None
        if command.status in {RetryCommandStatus.APPLIED, RetryCommandStatus.FAILED}:
            return command if command.status == RetryCommandStatus.APPLIED else None
        if (
            command.status == RetryCommandStatus.PROCESSING
            and command.processing_started_at is not None
            and moment - command.processing_started_at
            < timedelta(seconds=PROCESSING_LEASE_SECONDS)
        ):
            return None
        command.status = RetryCommandStatus.PROCESSING
        command.processing_owner = owner
        command.processing_started_at = moment
        command.processing_attempts += 1
        command.updated_at = moment
        command.outcome_reason = "Daemon acknowledged the Retry command."
        _append_transition(
            command,
            status=command.status,
            reason=command.outcome_reason,
            continuation=command.selected_continuation,
            now=moment,
        )
        pipe.multi()
        pipe.set(command_key, _serialize(command), ex=COMMAND_TTL_SECONDS)
        return command

    return await redis_client.transaction(
        _transaction,
        command_key,
        value_from_callable=True,
    )


async def update_retry_command(
    redis_client: Any,
    repo_slug: str,
    command_id: str,
    mutate: Callable[[RetryCommand], None],
    *,
    keep_pending: bool,
    transition_reason: str | None = None,
    now: datetime | None = None,
) -> RetryCommand | None:
    moment = now or utc_now()
    command_key = retry_command(repo_slug, command_id)
    pending_key = retry_command_pending(repo_slug)

    async def _transaction(pipe: Any) -> RetryCommand | None:
        command = _parse(await pipe.get(command_key))
        if command is None:
            return None
        mutate(command)
        command.updated_at = moment
        if transition_reason is not None:
            command.outcome_reason = transition_reason
            _append_transition(
                command,
                status=command.status,
                reason=transition_reason,
                continuation=command.selected_continuation,
                now=moment,
            )
        pipe.multi()
        pipe.set(command_key, _serialize(command), ex=COMMAND_TTL_SECONDS)
        if keep_pending:
            pipe.zadd(pending_key, {command.command_id: command.requested_at.timestamp()})
            pipe.expire(pending_key, COMMAND_TTL_SECONDS)
        else:
            pipe.zrem(pending_key, command.command_id)
        return command

    return await redis_client.transaction(
        _transaction,
        command_key,
        value_from_callable=True,
    )


async def reserve_retry_attempt(
    redis_client: Any,
    repo_slug: str,
    command_id: str,
    cap: int,
    *,
    now: datetime | None = None,
) -> RetryCommand:
    """Atomically consume one retry allowance exactly once for ``command_id``."""
    moment = now or utc_now()
    command_key = retry_command(repo_slug, command_id)
    command = await load_retry_command(redis_client, repo_slug, command_id)
    if command is None:
        raise RuntimeError("retry command disappeared")
    count_key = retry_count_key(repo_slug, command.task_id)
    pending_key = retry_command_pending(repo_slug)

    async def _transaction(pipe: Any) -> RetryCommand:
        current_command = _parse(await pipe.get(command_key))
        if current_command is None:
            raise RuntimeError("retry command disappeared")
        if current_command.retry_count is not None:
            return current_command
        current_count = _decode_count(await pipe.get(count_key))
        if current_count >= cap:
            raise RetryCapReached(current_count, cap)
        next_count = current_count + 1
        current_command.retry_count = next_count
        current_command.effect_stage = RetryEffectStage.RETRY_RESERVED
        current_command.updated_at = moment
        reason = f"Retry allowance reserved ({next_count}/{cap})."
        current_command.outcome_reason = reason
        _append_transition(
            current_command,
            status=current_command.status,
            reason=reason,
            continuation=current_command.selected_continuation,
            now=moment,
        )
        pipe.multi()
        pipe.set(count_key, str(next_count), ex=RETRY_COUNT_TTL_SECONDS)
        pipe.set(command_key, _serialize(current_command), ex=COMMAND_TTL_SECONDS)
        pipe.zadd(
            pending_key,
            {current_command.command_id: current_command.requested_at.timestamp()},
        )
        pipe.expire(pending_key, COMMAND_TTL_SECONDS)
        return current_command

    return await redis_client.transaction(
        _transaction,
        command_key,
        count_key,
        value_from_callable=True,
    )


__all__ = [
    "COMMAND_TTL_SECONDS",
    "PROCESSING_LEASE_SECONDS",
    "RetryCapReached",
    "RetryCommand",
    "RetryCommandStatus",
    "RetryEffectStage",
    "RetryExecutionState",
    "claim_retry_command",
    "enqueue_retry_command",
    "list_pending_retry_commands",
    "load_latest_retry_command",
    "load_retry_command",
    "load_retry_command_for_binding",
    "new_retry_command",
    "reserve_retry_attempt",
    "retry_request_binding",
    "update_retry_command",
]
