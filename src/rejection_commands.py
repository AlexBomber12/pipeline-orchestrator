"""Exact, durable Reject requests. Only the daemon performs their effects."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from src.approval_commands import approval_task_path, failure_identity
from src.cancellation.storage import (
    CATEGORIES,
    READ_REFRESH_TTL_SECONDS,
    TTL_SECONDS,
    CancellationCause,
    cause_key,
    index_key,
    task_spec_content_hash,
)
from src.keyspace import pipeline_state
from src.models import PipelineState, PRInfo, QueueTask, RepoState
from src.queue_parser import parse_existing_task_header
from src.task_attempts import AttemptChanged, TaskAttempt, attempt_key, new_attempt

REJECTION_IDENTITY_MANIFEST = "tasks/rejections.json"
LEGACY_REJECTION_SENTINEL = "legacy-missing-identity"
_SAFE_FAILURE_LABEL = re.compile(r"^[A-Za-z0-9_.-]{1,80}$")


class RejectionIdentityManifestUnavailable(AttemptChanged):
    """Durable rejection identity evidence exists but cannot be read safely."""


def recorded_rejection_identity(
    root: Path,
    owner_repo: str,
    base_branch: str,
    task_id: str,
    *,
    fingerprint: str | None = None,
    binding: str | None = None,
) -> dict | None:
    """Return a matching durable rejection identity entry from ``tasks/rejections.json``."""
    records = _load_rejection_identity_records(root, owner_repo, base_branch)
    if records is None:
        return None
    entries = _rejection_identity_entries(records, task_id)
    return _matching_rejection_identity_entry(
        entries,
        fingerprint=fingerprint,
        binding=binding,
    )


def recorded_rejection_identity_by_task_file(
    root: Path,
    owner_repo: str,
    base_branch: str,
    task_file: str,
    *,
    fingerprint: str | None = None,
    binding: str | None = None,
) -> tuple[str, dict] | None:
    """Return a durable rejection identity entry that owns ``task_file``."""
    records = _load_rejection_identity_records(root, owner_repo, base_branch)
    if records is None:
        return None
    matches: list[tuple[tuple[float, int, int], str, dict]] = []
    for task_order, task_id in enumerate(records):
        for entry_order, entry in enumerate(_rejection_identity_entries(records, str(task_id))):
            if not isinstance(entry, dict):
                continue
            if entry.get("task_file") != task_file:
                continue
            if fingerprint is not None and entry.get("fingerprint") != fingerprint:
                continue
            if binding is not None and entry.get("rejection_binding") != binding:
                continue
            matches.append(
                (
                    (_rejection_identity_requested_timestamp(entry), task_order, entry_order),
                    str(task_id),
                    entry,
                )
            )
    if not matches:
        return None
    return max(matches, key=lambda match: match[0])[1:]


def _rejection_identity_requested_timestamp(entry: dict) -> float:
    raw = entry.get("requested_at")
    if not isinstance(raw, str):
        return float("-inf")
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return float("-inf")


def _load_rejection_identity_records(
    root: Path,
    owner_repo: str,
    base_branch: str,
) -> dict | None:
    manifest_path = root / REJECTION_IDENTITY_MANIFEST
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, ValueError, TypeError) as exc:
        raise RejectionIdentityManifestUnavailable("Rejection identity manifest is unavailable.") from exc

    if not isinstance(manifest, dict):
        raise RejectionIdentityManifestUnavailable("Rejection identity manifest is unavailable.")
    if (
        str(manifest.get("repository", "")).casefold() != owner_repo.casefold()
        or manifest.get("base_branch") != base_branch
    ):
        return None
    if manifest.get("schema_version") != 1:
        raise RejectionIdentityManifestUnavailable("Rejection identity manifest is unavailable.")
    records = manifest.get("rejections")
    if not isinstance(records, dict):
        raise RejectionIdentityManifestUnavailable("Rejection identity manifest is unavailable.")
    return records


def _rejection_identity_entries(records: dict, task_id: str) -> list:
    entries = records.get(task_id, [])
    if not isinstance(entries, list):
        raise RejectionIdentityManifestUnavailable("Rejection identity manifest is unavailable.")
    return entries


def _matching_rejection_identity_entry(
    entries: list,
    *,
    fingerprint: str | None = None,
    binding: str | None = None,
) -> dict | None:
    for entry in reversed(entries):
        if not isinstance(entry, dict):
            continue
        if fingerprint is not None and entry.get("fingerprint") != fingerprint:
            continue
        if binding is not None and entry.get("rejection_binding") != binding:
            continue
        return entry
    return None


def _safe_failure_label(value: Any) -> str:
    return value if isinstance(value, str) and _SAFE_FAILURE_LABEL.fullmatch(value) else ""


def redacted_rejection_failure_identity(raw: str | bytes | None) -> str:
    """Return a Git-safe failure identity without guardrail stdout excerpts."""
    if raw is None:
        return ""
    if isinstance(raw, bytes):
        raw_bytes = raw
        raw_text = raw.decode("utf-8", errors="replace")
    else:
        raw_text = str(raw)
        raw_bytes = raw_text.encode("utf-8", errors="surrogatepass")
    summary = {"failure_sha256": hashlib.sha256(raw_bytes).hexdigest()}
    try:
        parsed = json.loads(raw_text)
    except (TypeError, ValueError):
        return json.dumps(summary, sort_keys=True, separators=(",", ":"))
    if not isinstance(parsed, dict):
        return json.dumps(summary, sort_keys=True, separators=(",", ":"))
    category = _safe_failure_label(parsed.get("category"))
    if category in CATEGORIES:
        summary["category"] = category
    payload = parsed.get("payload")
    if isinstance(payload, dict):
        subsource = _safe_failure_label(payload.get("subsource"))
        if subsource:
            summary["subsource"] = subsource
    return json.dumps(summary, sort_keys=True, separators=(",", ":"))


class RejectionCommand(BaseModel):
    binding: str
    repo_slug: str
    repo_url: str
    task: QueueTask
    attempt_id: str
    fingerprint: str
    file_sha256: str
    failure: str
    pr: PRInfo | None
    initial_head_sha: str | None = None
    requested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    status: Literal["accepted", "stopping", "closing", "deferred", "rejected", "merged"] = "accepted"
    reason: str = "Reject accepted; waiting for the daemon to stop this attempt and confirm PR closure."
    stopping_started_at: datetime | None = None
    close_requested_at: datetime | None = None
    absence_confirmed: bool = False
    absence_confirmed_at: datetime | None = None
    branch_head: str | None = None
    base_commit: str = ""
    released: bool = False


def rejection_key(repo: str, binding: str) -> str:
    return f"task_rejection:{repo}:{binding}"


def rejection_index(repo: str) -> str:
    return f"task_rejections:{repo}"


def rejection_pending_index(repo: str) -> str:
    return f"task_rejections_pending:{repo}"


def rejection_pending_backfill_key(repo: str) -> str:
    return f"task_rejections_pending_backfilled:{repo}"


def build_rejection(repo: str, state: RepoState, raw_cause: str | bytes, root: Path) -> RejectionCommand:
    cause = CancellationCause.from_redis(raw_cause)
    task, pr = state.current_task, state.current_pr
    if task is None or cause.task_id != task.pr_id or cause.repo_slug != repo:
        raise AttemptChanged("Failure and active task disagree.")
    if cause.payload.get("subsource") != "guardrail" or state.state != PipelineState.ERROR:
        raise AttemptChanged("The current attempt has no pending guardrail decision.")
    path = approval_task_path(root, task.task_file or f"tasks/{task.pr_id}.md")
    header = parse_existing_task_header(path)
    if header.pr_id != task.pr_id or header.branch != task.branch:
        raise AttemptChanged("Task identity or branch changed; refresh the decision.")
    if pr and (pr.branch != task.branch or pr.pr_id not in (None, task.pr_id) or not pr.head_sha):
        raise AttemptChanged("Exact PR identity or HEAD is unavailable.")
    # A GitHub PR number is an immutable legacy attempt identity. A legacy
    # pre-PR failure without a run/attempt receipt must never be guessed.
    attempt_id = task.attempt_id or (f"legacy-pr-{pr.number}" if pr else None)
    if not attempt_id:
        raise AttemptChanged("Legacy attempt identity is missing; daemon reconciliation is required.")
    content_bytes = path.read_bytes()
    content = content_bytes.decode("utf-8")
    fingerprint = task_spec_content_hash(content)
    failure = failure_identity(raw_cause)
    task = task.model_copy(update={"task_file": path.relative_to(root).as_posix(), "attempt_id": attempt_id})
    binding = hashlib.sha256(
        json.dumps(
            [
                repo,
                state.url,
                task.pr_id,
                task.task_file,
                attempt_id,
                fingerprint,
                task.branch,
                pr.number if pr else None,
                pr.head_sha if pr else None,
                failure,
            ]
        ).encode()
    ).hexdigest()
    return RejectionCommand(
        binding=binding,
        repo_slug=repo,
        repo_url=state.url,
        task=task,
        attempt_id=attempt_id,
        fingerprint=fingerprint,
        file_sha256=hashlib.sha256(content_bytes).hexdigest(),
        failure=failure,
        pr=pr.model_copy(deep=True) if pr else None,
        initial_head_sha=pr.head_sha if pr else None,
    )


async def load_rejection(redis: Any, repo: str, binding: str) -> RejectionCommand | None:
    raw = await redis.get(rejection_key(repo, binding))
    return RejectionCommand.model_validate_json(raw) if raw else None


async def list_rejections(redis: Any, repo: str, *, limit: int | None = None) -> list[RejectionCommand]:
    index = rejection_index(repo)
    if limit is not None and limit <= 0:
        return []
    if limit is None:
        bindings = await redis.zrangebyscore(index, "-inf", "+inf")
    elif zrevrange := getattr(redis, "zrevrange", None):
        bindings = list(reversed(await zrevrange(index, 0, limit - 1)))
    else:
        bindings = (await redis.zrangebyscore(index, "-inf", "+inf"))[-limit:]
    result = []
    for binding in bindings:
        binding = binding.decode() if isinstance(binding, bytes) else binding
        command = await load_rejection(redis, repo, binding)
        if command is None:
            raise AttemptChanged("Rejection receipt missing; operator investigation required.")
        result.append(command)
    return result


async def list_pending_rejections(redis: Any, repo: str) -> list[RejectionCommand]:
    pending_key = rejection_pending_index(repo)
    pending_bindings = await redis.zrangebyscore(pending_key, "-inf", "+inf")
    result = []
    seen = set()
    for binding in pending_bindings:
        binding = binding.decode() if isinstance(binding, bytes) else binding
        seen.add(binding)
        command = await load_rejection(redis, repo, binding)
        if command is None:
            raise AttemptChanged("Rejection receipt missing; operator investigation required.")
        if command.released:
            await redis.zrem(pending_key, binding)
            continue
        result.append(command)
    backfill_key = rejection_pending_backfill_key(repo)
    try:
        backfill_done = await redis.get(backfill_key)
    except Exception:
        return result
    if backfill_done:
        return result
    for binding in await redis.zrangebyscore(rejection_index(repo), "-inf", "+inf"):
        binding = binding.decode() if isinstance(binding, bytes) else binding
        if binding in seen:
            continue
        command = await load_rejection(redis, repo, binding)
        if command is None:
            raise AttemptChanged("Rejection receipt missing; operator investigation required.")
        if command.released:
            continue
        await redis.zadd(pending_key, {binding: command.requested_at.timestamp()})
        result.append(command)
    await redis.set(backfill_key, "1")
    return result


async def enqueue_rejection(redis: Any, command: RejectionCommand) -> RejectionCommand:
    key = rejection_key(command.repo_slug, command.binding)
    task_key = attempt_key(command.repo_slug, command.task.pr_id)
    failure_key = cause_key(command.repo_slug, command.task.pr_id)
    state_key = pipeline_state(command.repo_slug)

    async def transaction(pipe: Any) -> RejectionCommand:
        existing = await pipe.get(key)
        if existing:
            return RejectionCommand.model_validate_json(existing)
        raw = await pipe.get(task_key)
        attempt = TaskAttempt.model_validate_json(raw) if raw else None
        if attempt and (
            attempt.attempt_id != command.attempt_id
            or attempt.fingerprint != command.fingerprint
            or attempt.rejection
            or attempt.admission_pending
            or attempt.completed
        ):
            replay = await pipe.get(key)
            if replay:
                return RejectionCommand.model_validate_json(replay)
            raise AttemptChanged("This decision belongs to an obsolete or completed attempt.")
        state = RepoState.model_validate_json(await pipe.get(state_key))
        task, pr = state.current_task, state.current_pr
        failure = failure_identity(await pipe.get(failure_key))
        replay = await pipe.get(key)
        if replay:
            return RejectionCommand.model_validate_json(replay)
        if (
            state.url != command.repo_url
            or state.state != PipelineState.ERROR
            or task is None
            or task.pr_id != command.task.pr_id
            or task.branch != command.task.branch
            or (task.attempt_id and task.attempt_id != command.attempt_id)
            or (pr.model_dump() if pr else None) != (command.pr.model_dump() if command.pr else None)
            or failure != command.failure
        ):
            raise AttemptChanged("The active decision changed; refresh before rejecting.")
        if attempt is None:
            attempt = new_attempt(command.repo_url, command.task, "", attempt_id=command.attempt_id, started=True)
            attempt.fingerprint = command.fingerprint
            attempt.file_sha256 = command.file_sha256
        attempt.rejection = command.binding
        # A temporarily untracked in-memory PR must not erase durable creation evidence.
        attempt.pr_number = pr.number if pr else attempt.pr_number
        original = CancellationCause.from_redis(command.failure).payload
        original_rule = original.get("rule") or original.get("category") or ""
        original_excerpt = original.get("excerpt") or original.get("reason_text") or ""
        match = re.match(r"^GUARDRAIL:\s*([^:]+):\s*(.+)$", str(original_excerpt))
        if match:
            original_rule = original_rule or match.group(1).strip()
            original_excerpt = match.group(2).strip()
        cause = CancellationCause(
            category="ERROR",
            repo_slug=command.repo_slug,
            task_id=command.task.pr_id,
            created_at=command.requested_at.isoformat(),
            payload={
                "subsource": "operator_reject",
                "original_rule": original_rule,
                "original_excerpt": original_excerpt,
                "reason_text": command.reason,
                "attempt_id": command.attempt_id,
                "rejection_binding": command.binding,
                "pr_number": pr.number if pr else None,
            },
        )
        # Permanent receipt and fence survive history TTLs and lost replies.
        pipe.multi()
        pipe.set(key, command.model_dump_json())
        pipe.zadd(rejection_index(command.repo_slug), {command.binding: command.requested_at.timestamp()})
        pipe.zadd(rejection_pending_index(command.repo_slug), {command.binding: command.requested_at.timestamp()})
        pipe.set(task_key, attempt.model_dump_json())
        pipe.set(failure_key, cause.to_redis(), ex=TTL_SECONDS)
        pipe.zadd(index_key(command.repo_slug), {command.task.pr_id: command.requested_at.timestamp()})
        pipe.expire(index_key(command.repo_slug), READ_REFRESH_TTL_SECONDS)
        return command

    return await redis.transaction(transaction, key, task_key, failure_key, state_key, value_from_callable=True)
