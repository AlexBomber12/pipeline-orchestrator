"""Daemon admission policy for staged uploads and configured-base Git edits.

Validation never removes a dependency or rewrites an old completion record.
The daemon verifies it before changing any files; HTTP acceptance only stages
input. A per-task rejection token records which operator decision an upload
actually followed.
"""

from __future__ import annotations

import json
from pathlib import Path

from src.approval_commands import approval_task_path
from src.cancellation.storage import CancellationCause, cause_key, task_spec_content_hash
from src.completion_evidence import get_recorded_completions
from src.github import gh_pr_get_merged_branches, gh_runner
from src.github import prs as gh_prs
from src.keyspace import pipeline_state
from src.models import PipelineState, QueueTask, RepoState, TaskStatus
from src.queue_parser import UnstructuredLegacyTaskError, parse_existing_task_header, parse_task_header
from src.rejection_commands import load_rejection
from src.task_attempts import AttemptChanged, TaskAttempt, load_attempt, new_attempt
from src.task_status import get_merged_pr_ids


def verify_unfinished(root: Path, base: str, repo_url: str, previous: TaskAttempt) -> None:
    """Check the *prior* accepted bytes, including alternative-PR receipts."""
    task_id = previous.task.pr_id
    if previous.completed or previous.task.status == TaskStatus.DONE:
        raise AttemptChanged(f"{task_id} is completed and cannot be reused.")
    owner = gh_runner.get_repo_full_name(repo_url)
    branches = gh_pr_get_merged_branches(owner, {previous.task.branch})
    merged = get_merged_pr_ids(str(root), base, {task_id})
    recorded = get_recorded_completions(
        str(root),
        base,
        owner,
        {task_id},
        accepted_digests={task_id: previous.file_sha256},
    )
    manifest = root / "tasks/completions.json"
    if manifest.is_file() and task_id in json.loads(manifest.read_text())["completions"] and task_id not in recorded:
        raise AttemptChanged("Completion record has an unresolved prior task identity; reuse is deferred.")
    merged_prs = gh_prs.get_merged_prs(owner, base, refresh=True)
    if (
        previous.task.branch in branches
        or task_id in merged
        or task_id in recorded
        or any(pr.pr_id == task_id or pr.branch == previous.task.branch for pr in merged_prs)
    ):
        raise AttemptChanged(f"{task_id} has authoritative completion evidence and cannot be reused.")


async def admission_candidate(
    redis,
    repo: str,
    repo_url: str,
    base: str,
    root: Path,
    incoming: Path,
    *,
    expected_rejection: str | None = None,
    upload: bool = False,
    available_ids: set[str] | None = None,
) -> tuple[TaskAttempt | None, TaskAttempt | None]:
    """Return prior receipt and proposed replacement, or None for a replay.

    Rejection tokens are checked even for identical inputs, so an upload
    originating before Reject cannot be interpreted as a later rewrite.
    """
    header = parse_task_header(incoming)
    if incoming.name != f"{header.pr_id}.md" or header.branch == base:
        raise AttemptChanged("Task filename/identity must agree and branch must differ from the configured base.")
    # Validate Git ref syntax without shell interpolation or repository writes.
    import subprocess

    if subprocess.run(["git", "check-ref-format", "--branch", header.branch], capture_output=True).returncode:
        raise AttemptChanged("Task branch is not a valid Git branch.")
    previous = await load_attempt(redis, repo, header.pr_id)
    existing = approval_task_path(root, f"tasks/{incoming.name}")
    content = incoming.read_text(encoding="utf-8")
    fingerprint = task_spec_content_hash(content)
    if previous is None and existing.is_file():
        old = parse_existing_task_header(existing)
        previous = new_attempt(
            repo_url,
            QueueTask(
                pr_id=old.pr_id,
                title=old.title,
                task_file=f"tasks/{incoming.name}",
                branch=old.branch,
                status=TaskStatus.DONE if old.frontmatter_status == "done" else TaskStatus.TODO,
            ),
            existing.read_text(encoding="utf-8"),
            started=old.frontmatter_status not in (None, "todo"),
        )
    if previous and previous.admission_pending and fingerprint != previous.fingerprint:
        raise AttemptChanged("A different specification admission is pending; reconcile its Git/Redis result first.")
    raw_cause = await redis.get(cause_key(repo, header.pr_id))
    if raw_cause and CancellationCause.from_redis(raw_cause).payload.get("subsource") == "operator_reject":
        if previous is None or not previous.rejection:
            raise AttemptChanged("Legacy rejection lacks exact attempt/PR ownership; reconcile it before reuse.")
    if previous and previous.rejection:
        if upload and expected_rejection != previous.rejection:
            raise AttemptChanged("Upload predates or belongs to another rejection; submit the rewritten task again.")
        rejection = await load_rejection(redis, repo, previous.rejection)
        if rejection is None or rejection.status != "rejected" or not rejection.released:
            raise AttemptChanged("Rejection is not final; wait for process quiescence and confirmed PR closure.")
        if fingerprint == previous.fingerprint:
            raise AttemptChanged(
                "File unchanged. Reject is final; rewrite or remove the unfinished task. Ordinary Retry is unavailable."
            )
    elif upload and expected_rejection:
        if not previous or previous.previous_rejection != expected_rejection or previous.fingerprint != fingerprint:
            raise AttemptChanged("Upload belongs to an obsolete attempt.")
    if previous and previous.repo_url != repo_url:
        raise AttemptChanged("Task receipt belongs to a different repository.")
    if previous and previous.fingerprint == fingerprint:
        return previous, None
    raw_state = await redis.get(pipeline_state(repo))
    state = RepoState.model_validate_json(raw_state) if raw_state else None
    if (previous is None or not previous.rejection) and state and state.current_task:
        active = state.current_task
        if active.pr_id == header.pr_id and (
            state.current_pr is not None
            or state.state
            in {PipelineState.CODING, PipelineState.WATCH, PipelineState.FIX, PipelineState.MERGE, PipelineState.ERROR}
        ):
            raise AttemptChanged("An active attempt owns this task; Reject before accepting a rewritten specification.")
    if previous:
        verify_unfinished(root, base, repo_url, previous)
        if previous.started and not previous.rejection:
            raise AttemptChanged(
                "An existing attempt owns this specification. Retry unchanged work or Reject before rewriting it."
            )
    available_ids = (
        available_ids if available_ids is not None else {path.stem for path in (root / "tasks").glob("PR-*.md")}
    )
    missing = set(header.depends_on) - available_ids
    if missing:
        merged = get_merged_pr_ids(str(root), base, missing)
        if missing - merged:
            raise AttemptChanged("Missing dependencies: " + ", ".join(sorted(missing - merged)))
    for path in (root / "tasks").glob("PR-*.md"):
        if path.name == incoming.name:
            continue
        try:
            other = parse_existing_task_header(path)
        except UnstructuredLegacyTaskError:
            continue
        if other.branch == header.branch:
            raise AttemptChanged(f"Branch is also assigned to {other.pr_id}.")
    task = QueueTask(
        pr_id=header.pr_id,
        title=header.title,
        task_file=f"tasks/{incoming.name}",
        branch=header.branch,
        status=TaskStatus.TODO,
        depends_on=list(header.depends_on),
        priority=header.priority,
    )
    candidate = new_attempt(
        repo_url,
        task,
        content,
        previous_rejection=(previous.rejection or previous.previous_rejection) if previous else None,
        admission_pending=True,
    )
    candidate.task.attempt_id = candidate.attempt_id
    return previous, candidate
