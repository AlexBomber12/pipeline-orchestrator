"""Daemon admission policy for staged uploads and configured-base Git edits.

Validation never removes a dependency or rewrites an old completion record.
The daemon verifies it before changing any files; HTTP acceptance only stages
input. A per-task rejection token records which operator decision an upload
actually followed.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from collections.abc import Iterable
from pathlib import Path

from src.approval_commands import approval_task_path
from src.cancellation.storage import CancellationCause, cause_key, task_spec_content_hash
from src.completion_evidence import get_recorded_completions
from src.config import normalize_repo_url
from src.dag import detect_cycle
from src.github import gh_pr_get_merged_branches, gh_runner
from src.github import prs as gh_prs
from src.keyspace import pipeline_state
from src.models import PipelineState, QueueTask, RepoState, TaskStatus
from src.queue_parser import (
    QueueValidationError,
    UnstructuredLegacyTaskError,
    parse_existing_task_header,
    parse_task_header,
)
from src.rejection_commands import (
    LEGACY_REJECTION_SENTINEL,
    RejectionIdentityManifestUnavailable,
    load_rejection,
    recorded_rejection_identity,
)
from src.task_attempts import AdmissionRejected, AttemptChanged, TaskAttempt, load_attempt, new_attempt
from src.task_status import get_merged_pr_ids


def _is_missing_existing_task_header_error(exc: QueueValidationError) -> bool:
    return bool(exc.issues) and all(
        "missing task header like" in issue for issue in exc.issues
    )


def _parse_existing_task_header_or_none(path: Path):
    try:
        return parse_existing_task_header(path)
    except UnstructuredLegacyTaskError:
        return None
    except QueueValidationError as exc:
        if _is_missing_existing_task_header_error(exc):
            return None
        raise


def existing_task_header_ids(root: Path) -> set[str]:
    return {
        header.pr_id
        for path in (root / "tasks").glob("PR-*.md")
        if (header := _parse_existing_task_header_or_none(path))
    }


def validate_admission_graph(root: Path, incoming: Iterable[Path] = ()) -> None:
    """Check the proposed task graph before reserving any new attempts.

    Uploaded files replace their filenames in the existing graph; files
    omitted from a batch stay present. Missing/merged dependencies retain
    their separate admission checks and historical parsing stays tolerant.
    """
    paths = {path.name: path for path in (root / "tasks").glob("PR-*.md")}
    replacements = {path.name: path for path in incoming}
    paths.update(replacements)
    graph = {}
    branch_owner = {}
    for name, path in sorted(paths.items()):
        try:
            if name in replacements:
                header = parse_task_header(path)
            else:
                header = _parse_existing_task_header_or_none(path)
                if header is None:
                    continue
        except QueueValidationError as exc:
            raise AdmissionRejected(str(exc)) from exc
        owner = branch_owner.get(header.branch)
        if owner is not None and owner != header.pr_id:
            raise AdmissionRejected(f"Branch {header.branch} is also assigned to {owner}.")
        branch_owner[header.branch] = header.pr_id
        graph[header.pr_id] = header.depends_on
    cycle = detect_cycle(graph)
    if cycle:
        raise AdmissionRejected("Dependency cycle: " + " -> ".join(cycle))


def invalid_upload_graph_members(root: Path, incoming: Iterable[Path]) -> set[str]:
    """Return uploaded task filenames that participate in graph conflicts."""
    replacements = {path.name: path for path in incoming}
    paths = {path.name: path for path in (root / "tasks").glob("PR-*.md")}
    paths.update(replacements)
    graph: dict[str, tuple[str, ...]] = {}
    branch_owners: dict[str, set[str]] = {}
    task_names: dict[str, str] = {}
    incoming_ids: set[str] = set()
    invalid_ids: set[str] = set()
    for name, path in sorted(paths.items()):
        try:
            if name in replacements:
                header = parse_task_header(path)
            else:
                header = _parse_existing_task_header_or_none(path)
                if header is None:
                    continue
        except QueueValidationError as exc:
            if name in replacements:
                return {name}
            raise AdmissionRejected(str(exc)) from exc
        branch_owners.setdefault(header.branch, set()).add(header.pr_id)
        graph[header.pr_id] = tuple(header.depends_on)
        task_names[header.pr_id] = name
        if name in replacements:
            incoming_ids.add(header.pr_id)
    for owners in branch_owners.values():
        if len(owners) > 1:
            invalid_ids.update(owners)
    if cycle := detect_cycle(graph):
        invalid_ids.update(cycle)
    changed = True
    while changed:
        changed = False
        for task_id, dependencies in graph.items():
            if task_id in incoming_ids and task_id not in invalid_ids and invalid_ids.intersection(dependencies):
                invalid_ids.add(task_id)
                changed = True
    return {task_names[task_id] for task_id in invalid_ids.intersection(incoming_ids)}


def _legacy_rejection_branch_ref_exists(root: Path, branch: str) -> bool:
    local = subprocess.run(
        ["git", "-C", str(root), "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"],
        capture_output=True,
    )
    if local.returncode == 0:
        return True
    if local.returncode != 1:
        raise AttemptChanged("Legacy rejection branch ownership is unavailable; reuse is deferred.")
    remote = subprocess.run(
        ["git", "-C", str(root), "ls-remote", "--heads", "origin", f"refs/heads/{branch}"],
        capture_output=True,
        text=True,
    )
    if remote.returncode != 0:
        raise AttemptChanged("Legacy rejection branch ownership is unavailable; reuse is deferred.")
    return bool(remote.stdout.strip())


def verify_unfinished(
    root: Path,
    base: str,
    repo_url: str,
    previous: TaskAttempt,
    *,
    accepted_file_sha256: str | None = None,
) -> None:
    """Check the *prior* accepted bytes, including alternative-PR receipts."""
    task_id = previous.task.pr_id
    if previous.completed or previous.task.status == TaskStatus.DONE:
        raise AdmissionRejected(f"{task_id} is completed and cannot be reused.")
    owner = gh_runner.get_repo_full_name(repo_url)
    branches = gh_pr_get_merged_branches(owner, {previous.task.branch})
    merged = get_merged_pr_ids(str(root), base, {task_id})
    recorded = get_recorded_completions(
        str(root),
        base,
        owner,
        {task_id},
        accepted_digests={task_id: accepted_file_sha256 or previous.file_sha256},
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
        raise AdmissionRejected(f"{task_id} has authoritative completion evidence and cannot be reused.")


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
    try:
        header = parse_task_header(incoming)
    except QueueValidationError as exc:
        raise AdmissionRejected(str(exc)) from exc
    if incoming.name != f"{header.pr_id}.md" or header.branch == base:
        raise AdmissionRejected("Task filename/identity must agree and branch must differ from the configured base.")
    # Validate Git ref syntax without shell interpolation or repository writes.
    if subprocess.run(["git", "check-ref-format", "--branch", header.branch], capture_output=True).returncode:
        raise AdmissionRejected("Task branch is not a valid Git branch.")
    previous = await load_attempt(redis, repo, header.pr_id)
    existing = approval_task_path(root, f"tasks/{incoming.name}")
    incoming_bytes = incoming.read_bytes()
    content = incoming_bytes.decode("utf-8")
    fingerprint = task_spec_content_hash(content)
    owner = gh_runner.get_repo_full_name(repo_url)
    try:
        recorded_incoming_rejection = recorded_rejection_identity(
            root,
            owner,
            base,
            header.pr_id,
            fingerprint=fingerprint,
        )
    except RejectionIdentityManifestUnavailable as exc:
        raise AttemptChanged("Rejection identity manifest is unavailable; reuse is deferred.") from exc
    if previous is None and existing.is_file():
        existing_bytes = existing.read_bytes()
        existing_content = existing_bytes.decode("utf-8")
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
            existing_content,
            started=old.frontmatter_status not in (None, "todo"),
            file_sha256=hashlib.sha256(existing_bytes).hexdigest(),
        )
    if previous and previous.admission_pending and fingerprint != previous.fingerprint:
        raise AttemptChanged("A different specification admission is pending; reconcile its Git/Redis result first.")
    raw_cause = await redis.get(cause_key(repo, header.pr_id))
    if raw_cause and CancellationCause.from_redis(raw_cause).payload.get("subsource") == "operator_reject":
        if previous is None or not previous.rejection:
            raise AttemptChanged("Legacy rejection lacks exact attempt/PR ownership; reconcile it before reuse.")
    rejection_file_sha256 = None
    prior_rejection_binding = previous.previous_rejection if previous else None
    if previous and previous.rejection:
        if previous.completed:
            if fingerprint == previous.fingerprint:
                return previous, None
            raise AdmissionRejected(f"{previous.task.pr_id} is completed and cannot be reused.")
        if upload and expected_rejection != previous.rejection:
            raise AdmissionRejected("Upload predates or belongs to another rejection; submit the rewritten task again.")
        if previous.rejection == LEGACY_REJECTION_SENTINEL:
            rejection_file_sha256 = previous.file_sha256
            if fingerprint != previous.fingerprint and _legacy_rejection_branch_ref_exists(root, previous.task.branch):
                raise AttemptChanged("Legacy rejection branch still exists; remove the abandoned branch before reuse.")
        else:
            rejection = await load_rejection(redis, repo, previous.rejection)
            if rejection is None:
                try:
                    recorded_previous_rejection = recorded_rejection_identity(
                        root,
                        owner,
                        base,
                        previous.task.pr_id,
                        binding=previous.rejection,
                    )
                except RejectionIdentityManifestUnavailable as exc:
                    raise AttemptChanged("Rejection identity manifest is unavailable; reuse is deferred.") from exc
                if recorded_previous_rejection is None:
                    raise AttemptChanged(
                        "Rejection is not final; wait for process quiescence and confirmed PR closure."
                    )
                rejection_file_sha256 = str(recorded_previous_rejection.get("file_sha256") or previous.file_sha256)
            elif rejection.status != "rejected" or not rejection.released:
                raise AttemptChanged("Rejection is not final; wait for process quiescence and confirmed PR closure.")
            else:
                rejection_file_sha256 = rejection.file_sha256
            prior_rejection_binding = previous.rejection
        if fingerprint == previous.fingerprint:
            raise AdmissionRejected(
                "File unchanged. Reject is final; rewrite or remove the unfinished task. Ordinary Retry is unavailable."
            )
    elif previous is None and recorded_incoming_rejection is None:
        try:
            recorded_previous_rejection = recorded_rejection_identity(
                root,
                owner,
                base,
                header.pr_id,
            )
        except RejectionIdentityManifestUnavailable as exc:
            raise AttemptChanged("Rejection identity manifest is unavailable; reuse is deferred.") from exc
        if recorded_previous_rejection and recorded_previous_rejection.get("rejection_binding"):
            prior_rejection_binding = str(recorded_previous_rejection["rejection_binding"])
        if upload and expected_rejection and expected_rejection != prior_rejection_binding:
            raise AdmissionRejected("Upload belongs to an obsolete attempt.")
    elif upload and expected_rejection:
        if not previous or previous.previous_rejection != expected_rejection or previous.fingerprint != fingerprint:
            raise AdmissionRejected("Upload belongs to an obsolete attempt.")
    if previous and normalize_repo_url(previous.repo_url) != normalize_repo_url(
        repo_url
    ):
        raise AdmissionRejected("Task receipt belongs to a different repository.")
    if previous and previous.fingerprint == fingerprint:
        # Replays keep their existing attempt, including completed tasks in a
        # sprint ZIP. Only changed specifications reach verify_unfinished below.
        return previous, None
    if previous is None:
        if recorded_incoming_rejection is not None:
            raise AdmissionRejected(
                "File unchanged. Reject is final; rewrite or remove the unfinished task. Ordinary Retry is unavailable."
            )
        merged = get_merged_pr_ids(str(root), base, {header.pr_id})
        recorded = get_recorded_completions(
            str(root),
            base,
            owner,
            {header.pr_id},
            trust_recorded_digest_if_task_missing=True,
        )
        merged_prs = gh_prs.get_merged_prs(owner, base, refresh=True)
        if (
            header.pr_id in merged
            or header.pr_id in recorded
            or any(pr.pr_id == header.pr_id or pr.branch == header.branch for pr in merged_prs)
        ):
            raise AdmissionRejected(f"{header.pr_id} has authoritative completion evidence and cannot be reused.")
    raw_state = await redis.get(pipeline_state(repo))
    state = RepoState.model_validate_json(raw_state) if raw_state else None
    if (previous is None or not previous.rejection) and state and state.current_task:
        active = state.current_task
        if active.pr_id == header.pr_id and (
            state.current_pr is not None
            or state.state
            in {PipelineState.CODING, PipelineState.WATCH, PipelineState.FIX, PipelineState.MERGE, PipelineState.ERROR}
        ):
            raise AdmissionRejected(
                "An active attempt owns this task; Reject before accepting a rewritten specification."
            )
    if previous:
        verify_unfinished(root, base, repo_url, previous, accepted_file_sha256=rejection_file_sha256)
    available_ids = (
        available_ids if available_ids is not None else existing_task_header_ids(root)
    )
    missing = set(header.depends_on) - available_ids
    if missing:
        merged = get_merged_pr_ids(str(root), base, missing)
        if missing - merged:
            raise AttemptChanged("Missing dependencies: " + ", ".join(sorted(missing - merged)))
    for path in (root / "tasks").glob("PR-*.md"):
        if path.name == incoming.name:
            continue
        other = _parse_existing_task_header_or_none(path)
        if other is None:
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
        previous_rejection=prior_rejection_binding,
        admission_pending=True,
        coder_dispatched=False,
        file_sha256=hashlib.sha256(incoming_bytes).hexdigest(),
    )
    candidate.task.attempt_id = candidate.attempt_id
    return previous, candidate
