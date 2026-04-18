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
_LEGACY_FALLBACK_SUFFIXES = {
    ": missing Type",
    ": missing Complexity",
    ": missing Depends on",
}


def derive_task_status(
    task_header: TaskHeader,
    merged_pr_ids: set[str],
    open_prs: Iterable[PRInfo],
    merged_prs: Iterable[PRInfo] = (),
) -> TaskStatus:
    """Derive task status from git state."""
    if task_header.pr_id in merged_pr_ids:
        return TaskStatus.DONE
    if (
        find_matching_merged_pr(
            task_header.pr_id,
            task_header.branch,
            merged_prs,
        )
        is not None
    ):
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
        if _branch_matches_task_pr(pr, pr_id, branch):
            return pr
    return None


def find_matching_merged_pr(
    pr_id: str,
    branch: str,
    merged_prs: Iterable[PRInfo],
) -> PRInfo | None:
    """Return the matching merged PR for a queue task, if one exists."""
    for pr in merged_prs:
        if _branch_matches_task_pr(pr, pr_id, branch):
            return pr

    if branch:
        return None

    for pr in merged_prs:
        merged_pr_id = pr.pr_id or extract_queue_pr_id(pr.title)
        if merged_pr_id == pr_id:
            return pr
    return None


def _branch_matches_task_pr(
    pr: PRInfo,
    pr_id: str,
    branch: str,
) -> bool:
    """Return True when a same-repo PR branch can be attributed to a task."""
    if not branch or pr.branch != branch or pr.is_cross_repository:
        return False

    candidate_pr_id = pr.pr_id or extract_queue_pr_id(pr.title)
    if candidate_pr_id is None:
        return True
    return candidate_pr_id == pr_id


def get_merged_pr_ids(
    repo_path: str,
    base_branch: str,
    candidate_pr_ids: Iterable[str] | None = None,
) -> set[str]:
    """Return queue PR identifiers already present in ``origin/base_branch`` history."""
    candidate_set = {pr_id for pr_id in candidate_pr_ids or () if pr_id}
    if candidate_set:
        try:
            result = _scan_candidate_merged_pr_ids(
                repo_path,
                f"origin/{base_branch}",
                candidate_set,
            )
        except subprocess.TimeoutExpired:
            return set()
        if result is None:
            try:
                result = _scan_candidate_merged_pr_ids(
                    repo_path,
                    base_branch,
                    candidate_set,
                )
            except subprocess.TimeoutExpired:
                return set()
        if result is not None:
            return result

    try:
        result = _run_merged_pr_probe(repo_path, f"origin/{base_branch}")
    except subprocess.TimeoutExpired:
        return set()
    if result.returncode != 0:
        try:
            result = _run_merged_pr_probe(repo_path, base_branch)
        except subprocess.TimeoutExpired:
            return set()
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


def _scan_candidate_merged_pr_ids(
    repo_path: str,
    target_ref: str,
    candidate_pr_ids: set[str],
    timeout: int = 10,
) -> set[str] | None:
    """Return matching merged PR ids, or ``None`` when the ref cannot be read."""
    pattern = "|".join(re.escape(pr_id) for pr_id in sorted(candidate_pr_ids))
    result = subprocess.run(
        [
            "git",
            "-C",
            repo_path,
            "log",
            target_ref,
            "--format=%s",
            "--extended-regexp",
            f"--grep=^({pattern}):",
            f"--max-count={len(candidate_pr_ids)}",
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        if stderr.startswith("fatal: ambiguous argument "):
            return None
        raise RuntimeError(
            "git log failed while probing merged PR ids: "
            f"{stderr or target_ref}"
        )

    found: set[str] = set()
    for line in result.stdout.strip().splitlines():
        subject = line.strip()
        if not subject:
            continue
        match = _MERGED_SUBJECT_RE.match(subject)
        if match:
            found.add(match.group("pr_id"))
    return found


def derive_queue_task_statuses(
    tasks: list[QueueTask],
    repo_path: str,
    base_branch: str,
    open_prs: Iterable[PRInfo],
    merged_prs: Iterable[PRInfo] = (),
) -> list[QueueTask]:
    """Return queue tasks with status refreshed from git/GitHub state."""
    open_prs = list(open_prs)
    merged_prs = list(merged_prs)
    merged_pr_ids = get_merged_pr_ids(
        repo_path,
        base_branch,
        (task.pr_id for task in tasks),
    )
    derived: list[QueueTask] = []

    for task in tasks:
        header = _load_task_header(task, repo_path)
        if header.pr_id != task.pr_id:
            task_ref = task.task_file or task.pr_id
            raise QueueValidationError(
                [
                    f"{task_ref}: header PR ID {header.pr_id!r} "
                    f"does not match queue entry {task.pr_id!r}"
                ]
            )
        status = derive_task_status(header, merged_pr_ids, open_prs, merged_prs)
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
            except QueueValidationError as exc:
                legacy_header = _load_legacy_task_header(task, task_path, exc)
                if legacy_header is not None:
                    return legacy_header
                raise

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


def _load_legacy_task_header(
    task: QueueTask,
    task_path: Path,
    exc: QueueValidationError,
) -> TaskHeader | None:
    """Return a narrow fallback only for known legacy task-file headers."""
    if not exc.issues or any(
        not any(issue.endswith(suffix) for suffix in _LEGACY_FALLBACK_SUFFIXES)
        for issue in exc.issues
    ):
        return None

    header_match: re.Match[str] | None = None
    branch: str | None = None
    for raw_line in task_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.rstrip()
        if header_match is None:
            header_match = re.match(r"^#\s+(PR-[A-Za-z0-9_.-]+):\s*(.+?)\s*$", line)
            continue
        if not line.strip():
            continue
        branch_match = re.match(r"^Branch\s*:\s*(.*?)\s*$", line)
        if branch_match:
            branch = branch_match.group(1).strip()
            break
        if line.startswith("#") or line.startswith("- "):
            break

    if header_match is None or not branch:
        return None

    header_pr_id = header_match.group(1)
    if header_pr_id != task.pr_id:
        task_ref = task.task_file or str(task_path)
        raise QueueValidationError(
            [
                f"{task_ref}: header PR ID {header_pr_id!r} "
                f"does not match queue entry {task.pr_id!r}"
            ]
        )

    return TaskHeader(
        pr_id=header_pr_id,
        title=header_match.group(2),
        branch=branch,
        task_type="feature",
        complexity="medium",
        depends_on=list(task.depends_on),
        priority=3,
        coder="any",
    )
