"""Derive queue task status from git branch state and open PRs."""

from __future__ import annotations

import re
import subprocess
from collections.abc import Iterable
from pathlib import Path

from src.github_client import extract_queue_pr_id
from src.models import PRInfo, QueueTask, TaskStatus
from src.queue_parser import (
    _PR_ID_RE,
    QueueValidationError,
    TaskHeader,
    parse_task_header,
)

_PR_ID_PATTERN = _PR_ID_RE.pattern.removeprefix("^").removesuffix("$")
_MERGED_SUBJECT_RE = re.compile(rf"^(?P<pr_id>{_PR_ID_PATTERN}):(?:\s|$)")


def derive_task_status(
    task_header: TaskHeader,
    merged_pr_ids: set[str],
    open_prs: Iterable[PRInfo],
) -> TaskStatus:
    """Derive task status from git state."""
    if task_header.pr_id in merged_pr_ids:
        return TaskStatus.DONE
    if find_matching_open_pr(
        task_header.pr_id,
        task_header.branch,
        open_prs,
    ) is not None:
        return TaskStatus.DOING
    return TaskStatus.TODO


def find_matching_open_pr(
    pr_id: str,
    branch: str,
    open_prs: Iterable[PRInfo],
) -> PRInfo | None:
    """Return the matching open PR for a queue task, if one exists."""
    if not branch:
        return None

    for pr in open_prs:
        if pr.branch != branch:
            continue
        open_pr_id = pr.pr_id or extract_queue_pr_id(pr.title)
        if open_pr_id == pr_id:
            return pr
    return None


def get_merged_pr_ids(repo_path: str, base_branch: str) -> set[str]:
    """Return queue PR identifiers already present in ``origin/base_branch`` history."""
    result = _run_merged_pr_probe(repo_path, f"origin/{base_branch}")
    if result.returncode != 0:
        result = _run_merged_pr_probe(repo_path, base_branch)
    if result.returncode != 0:
        raise RuntimeError(
            "git log failed while probing merged PR ids: "
            f"{(result.stderr or '').strip() or result.stdout.strip()}"
        )

    pr_ids: set[str] = set()
    for line in result.stdout.strip().splitlines():
        subject = line.strip()
        if not subject:
            continue
        match = _MERGED_SUBJECT_RE.match(subject)
        if match:
            pr_ids.add(match.group("pr_id"))
    return pr_ids


def _run_merged_pr_probe(
    repo_path: str,
    target_ref: str,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "git",
            "-C",
            repo_path,
            "log",
            target_ref,
            "--format=%s",
        ],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )


def derive_queue_task_statuses(
    tasks: list[QueueTask],
    repo_path: str,
    base_branch: str,
    open_prs: Iterable[PRInfo],
) -> list[QueueTask]:
    """Return queue tasks with status refreshed from git/GitHub state."""
    merged_pr_ids = get_merged_pr_ids(repo_path, base_branch)
    open_prs = list(open_prs)
    derived: list[QueueTask] = []

    for task in tasks:
        header = _load_task_header(task, repo_path)
        status = derive_task_status(header, merged_pr_ids, open_prs)
        derived.append(
            task.model_copy(update={"status": status, "branch": header.branch})
        )

    return derived


def _load_task_header(task: QueueTask, repo_path: str) -> TaskHeader:
    """Load the task header, falling back to queue metadata in tests."""
    if task.task_file:
        task_path = Path(repo_path) / task.task_file
        if task_path.is_file():
            try:
                return parse_task_header(task_path)
            except QueueValidationError:
                pass

    return TaskHeader(
        pr_id=task.pr_id,
        title=task.title,
        branch=task.branch or "",
        task_type="feature",
        complexity="medium",
        depends_on=list(task.depends_on),
        priority=3,
        coder="any",
    )
