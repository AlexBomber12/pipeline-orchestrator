"""Derive queue task status from git branch state and open PRs."""

from __future__ import annotations

import subprocess
from pathlib import Path

from src.models import QueueTask, TaskStatus
from src.queue_parser import (
    QueueValidationError,
    TaskHeader,
    parse_task_header,
)


def derive_task_status(
    task_header: TaskHeader,
    merged_branches: set[str],
    open_pr_branches: set[str],
) -> TaskStatus:
    """Derive task status from git state."""
    if task_header.branch in merged_branches:
        return TaskStatus.DONE
    if task_header.branch in open_pr_branches:
        return TaskStatus.DOING
    return TaskStatus.TODO


def get_merged_branches(repo_path: str, base_branch: str) -> set[str]:
    """Return remote branches already merged into ``origin/base_branch``."""
    result = _run_merged_branch_probe(repo_path, f"origin/{base_branch}")
    if result.returncode != 0:
        result = _run_merged_branch_probe(repo_path, base_branch)
    if result.returncode != 0:
        raise RuntimeError(
            "git branch --merged failed: "
            f"{(result.stderr or '').strip() or result.stdout.strip()}"
        )

    branches: set[str] = set()
    for line in result.stdout.strip().splitlines():
        ref = line.strip()
        if not ref or "->" in ref:
            continue
        if ref.startswith("origin/"):
            ref = ref.removeprefix("origin/")
        if ref and ref != base_branch:
            branches.add(ref)
    return branches


def _run_merged_branch_probe(
    repo_path: str,
    target_ref: str,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "git",
            "-C",
            repo_path,
            "branch",
            "--merged",
            target_ref,
            "-r",
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
    open_pr_branches: set[str],
) -> list[QueueTask]:
    """Return queue tasks with status refreshed from git/GitHub state."""
    merged_branches = get_merged_branches(repo_path, base_branch)
    derived: list[QueueTask] = []

    for task in tasks:
        header = _load_task_header(task, repo_path)
        status = derive_task_status(header, merged_branches, open_pr_branches)
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
