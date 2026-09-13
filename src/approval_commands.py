"""Durable, decision-bound guardrail approvals; pub/sub is only a wake hint.

Records have no TTL: a parked operation and its restart receipt must outlive
an absent daemon. The same binding always names the same logical operation.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from src.cancellation.storage import CancellationCause, cause_key, task_spec_content_hash
from src.keyspace import pipeline_state
from src.models import PRInfo, QueueTask, RepoState
from src.queue_parser import parse_existing_task_header


class ApprovalChanged(ValueError):
    """The displayed decision no longer describes the pending work."""


class ApprovalCommand(BaseModel):
    binding: str
    repo_slug: str
    repo_url: str
    task: QueueTask
    pr: PRInfo
    task_fingerprint: str
    failure: str
    status: Literal["pending", "deferred", "applied", "failed"] = "pending"
    reason: str = "Approval requested; waiting for the daemon to apply it."
    requested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    active: bool = True
    superseded: bool = False


def approval_key(repo: str, binding: str) -> str:
    return f"guardrail_approval:{repo}:{binding}"


def approval_index(repo: str) -> str:
    return f"guardrail_approvals:{repo}"


def approval_recent_index(repo: str) -> str:
    return f"guardrail_approvals_recent:{repo}"


def failure_identity(raw: str | bytes | None) -> str:
    if raw is None:
        return ""
    return json.dumps(json.loads(raw), sort_keys=True, separators=(",", ":"))


def approval_task_path(root: Path, task_file: str) -> Path:
    relative = Path(task_file)
    if relative.is_absolute() or ".." in relative.parts or relative.parts[:1] != ("tasks",):
        raise ApprovalChanged("Task path is outside the repository tasks directory.")
    cursor = root
    for part in relative.parts:
        cursor /= part
        if cursor.is_symlink():
            raise ApprovalChanged("Task path contains a symbolic link.")
    return cursor


def build_approval(
    repo: str,
    state: RepoState,
    raw_cause: str | bytes,
    root: Path,
) -> ApprovalCommand:
    cause = CancellationCause.from_redis(raw_cause)
    if cause.payload.get("subsource") != "guardrail":
        raise ApprovalChanged("There is no pending guardrail decision.")
    task, pr = state.current_task, state.current_pr
    if task is None or pr is None:
        raise ApprovalChanged("The existing PR must be active in daemon state.")
    path = approval_task_path(root, task.task_file or f"tasks/{task.pr_id}.md")
    header = parse_existing_task_header(path)
    if header.pr_id != task.pr_id or header.branch != pr.branch or pr.pr_id not in (None, task.pr_id):
        raise ApprovalChanged("Task and existing PR bindings disagree.")
    if not pr.head_sha:
        raise ApprovalChanged("The existing PR HEAD is unavailable; wait for daemon refresh.")
    fingerprint = task_spec_content_hash(path.read_text(encoding="utf-8"))
    failure = failure_identity(raw_cause)
    binding = hashlib.sha256(
        json.dumps(
            [
                repo,
                state.url,
                task.pr_id,
                path.relative_to(root).as_posix(),
                header.branch,
                fingerprint,
                pr.number,
                pr.head_sha,
                failure,
            ]
        ).encode()
    ).hexdigest()
    return ApprovalCommand(
        binding=binding,
        repo_slug=repo,
        repo_url=state.url,
        task=task.model_copy(update={"task_file": path.relative_to(root).as_posix(), "branch": header.branch}),
        pr=pr.model_copy(deep=True),
        task_fingerprint=fingerprint,
        failure=failure,
    )


async def load_approval(redis: Any, repo: str, binding: str) -> ApprovalCommand | None:
    raw = await redis.get(approval_key(repo, binding))
    return ApprovalCommand.model_validate_json(raw) if raw else None


async def list_approvals(redis: Any, repo: str, *, recent: bool = False) -> list[ApprovalCommand]:
    if recent:
        # Negative timestamps put newest first without reading permanent history.
        bindings = await redis.zrangebyscore(approval_recent_index(repo), "-inf", "+inf", start=0, num=20)
        bindings = list(reversed(bindings))
    else:
        bindings = await redis.zrangebyscore(approval_index(repo), "-inf", "+inf")
    commands = []
    for binding in bindings:
        if isinstance(binding, bytes):
            binding = binding.decode()
        command = await load_approval(redis, repo, binding)
        if command is None:
            raise ApprovalChanged("Approval recovery record is missing; operator investigation required.")
        commands.append(command)
    return commands


async def enqueue_approval(redis: Any, command: ApprovalCommand) -> ApprovalCommand:
    key = approval_key(command.repo_slug, command.binding)
    cancellation = cause_key(command.repo_slug, command.task.pr_id)
    state_key = pipeline_state(command.repo_slug)

    async def transaction(pipe: Any) -> ApprovalCommand:
        existing = await pipe.get(key)
        if existing:
            return ApprovalCommand.model_validate_json(existing)
        if failure_identity(await pipe.get(cancellation)) != command.failure:
            raise ApprovalChanged("The pending failure changed; refresh before approving.")
        state = RepoState.model_validate_json(await pipe.get(state_key))
        if not matches_state(command, state):
            raise ApprovalChanged("The active task or PR changed; refresh before approving.")
        pipe.multi()
        pipe.set(key, command.model_dump_json())
        pipe.zadd(approval_index(command.repo_slug), {command.binding: command.requested_at.timestamp()})
        pipe.zadd(approval_recent_index(command.repo_slug), {command.binding: -command.requested_at.timestamp()})
        return command

    return await redis.transaction(transaction, key, cancellation, state_key, value_from_callable=True)


def matches_state(command: ApprovalCommand, state: RepoState) -> bool:
    task, pr = state.current_task, state.current_pr
    return bool(
        state.url == command.repo_url
        and task
        and pr
        and task.pr_id == command.task.pr_id
        and (task.task_file or f"tasks/{task.pr_id}.md") == command.task.task_file
        and pr.number == command.pr.number
        and pr.branch == command.pr.branch
        and pr.head_sha == command.pr.head_sha
    )
