"""Canonical branch state representation for mismatch detection."""

from __future__ import annotations

import logging
import subprocess
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class BranchContext:
    """All branch surfaces relevant to a runner cycle.

    Use ``from_runner`` to construct in handler code; do not construct
    directly. The fields are derived from ``runner.repo_config.branch``
    (base branch), ``runner.state.current_task.branch`` (task branch),
    ``runner.state.current_pr.branch`` (PR branch), ``git rev-parse``
    on the working tree (current git branch), and ``git rev-parse
    --verify`` against ``refs/heads/{branch}`` and
    ``refs/remotes/origin/{branch}`` for the local/remote existence
    flags.
    """

    base_branch: str
    task_branch: str | None
    current_git_branch: str | None
    pr_head_branch: str | None
    remote_branch_exists: bool
    local_branch_exists: bool

    @property
    def mismatch_reason(self) -> str | None:
        """Return a human-readable mismatch reason, or ``None``.

        Conservative detection: only an explicit divergence between two
        known surfaces produces a non-``None`` result. Missing
        information (e.g. no PR yet, or ``git rev-parse`` failed) is
        treated as agreement with whatever other surfaces are known.
        """
        if (
            self.task_branch is not None
            and self.current_git_branch is not None
            and self.task_branch != self.current_git_branch
        ):
            return (
                f"task_branch={self.task_branch} but "
                f"current_git_branch={self.current_git_branch}"
            )
        if (
            self.task_branch is not None
            and self.pr_head_branch is not None
            and self.task_branch != self.pr_head_branch
        ):
            return (
                f"task_branch={self.task_branch} but "
                f"pr_head_branch={self.pr_head_branch}"
            )
        return None

    def log_summary(self) -> str:
        """One-line summary suitable for ``[BRANCH]`` log events.

        Field labels match the regression tests in
        ``tests/runner/test_branch_context.py``: each branch identifier
        is named with its full ``<concept>_branch`` form so the
        diagnostic stays unambiguous when an operator scans the event
        log, and unset surfaces are marked ``<absent>`` rather than
        omitted so ``base / task / git / pr`` cardinality is constant.
        """
        return (
            f"base_branch={self.base_branch} "
            f"task_branch={self.task_branch or '<absent>'} "
            f"current_git_branch={self.current_git_branch or '<absent>'} "
            f"pr_head_branch={self.pr_head_branch or '<absent>'}"
        )

    @classmethod
    def from_runner(cls, runner: object) -> "BranchContext":
        """Build the context from a ``PipelineRunner``-shaped object.

        ``runner`` is typed as ``object`` to avoid an import cycle
        through ``src/daemon/runner.py``. The required attributes are
        ``repo_config.branch``, ``state.current_task``, ``state.current_pr``
        and ``repo_path``.
        """
        base_branch = runner.repo_config.branch or "main"  # type: ignore[attr-defined]
        current_task = runner.state.current_task  # type: ignore[attr-defined]
        task_branch = current_task.branch if current_task is not None else None
        current_pr = runner.state.current_pr  # type: ignore[attr-defined]
        pr_head_branch = current_pr.branch if current_pr is not None else None
        repo_path = runner.repo_path  # type: ignore[attr-defined]
        current_git_branch = _read_current_git_branch(repo_path)
        if task_branch:
            remote_branch_exists = _check_remote_branch_exists(
                repo_path, task_branch
            )
            local_branch_exists = _check_local_branch_exists(
                repo_path, task_branch
            )
        else:
            remote_branch_exists = False
            local_branch_exists = False
        return cls(
            base_branch=base_branch,
            task_branch=task_branch,
            current_git_branch=current_git_branch,
            pr_head_branch=pr_head_branch,
            remote_branch_exists=remote_branch_exists,
            local_branch_exists=local_branch_exists,
        )


def _read_current_git_branch(repo_path: str) -> str | None:
    """Return the currently checked-out branch name or ``None`` on error."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
            cwd=repo_path,
        )
    except (FileNotFoundError, NotADirectoryError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    name = result.stdout.strip()
    return name or None


def _check_remote_branch_exists(repo_path: str, branch: str) -> bool:
    """Return ``True`` if ``refs/remotes/origin/{branch}`` exists locally."""
    try:
        result = subprocess.run(
            [
                "git",
                "rev-parse",
                "--verify",
                "--quiet",
                f"refs/remotes/origin/{branch}",
            ],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
            cwd=repo_path,
        )
    except (FileNotFoundError, NotADirectoryError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0


def _check_local_branch_exists(repo_path: str, branch: str) -> bool:
    """Return ``True`` if local ``refs/heads/{branch}`` exists."""
    try:
        result = subprocess.run(
            [
                "git",
                "rev-parse",
                "--verify",
                "--quiet",
                f"refs/heads/{branch}",
            ],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
            cwd=repo_path,
        )
    except (FileNotFoundError, NotADirectoryError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0
