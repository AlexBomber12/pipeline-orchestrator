"""Push verification helper extracted from ``handlers/fix.py`` (PR-230).

Standalone module so the rev-parse / merge-base ancestry check can be tested
without a full ``handle_fix`` orchestration. ``FixMixin._verify_pushes_since``
is preserved as a thin wrapper for existing call sites.
"""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

from src.daemon import git_ops

if TYPE_CHECKING:
    from src.daemon.runner import PipelineRunner


def verify_pushes_since(
    runner: "PipelineRunner",
    branch: str,
    last_known_sha: str,
    head_after: str,
    *,
    context: str,
) -> bool | None:
    """Verify that ``head_after`` reached ``origin/{branch}``.

    Returns ``True`` when origin contains ``head_after`` (either equal to
    it or fast-forwarded past it), ``False`` when origin is still at
    ``last_known_sha`` (no push happened), and ``None`` when the
    verification itself could not run (fetch / rev-parse / merge-base
    failure). Callers decide whether ``None`` should be treated as a hard
    failure (stop-cancel path: skip bookkeeping) or as fail-open
    (normal-exit path: proceed optimistically) — this helper only reports
    what it observed.

    ``context`` is appended to the log lines emitted on git failures so
    the same helper can serve both call sites without the event log
    losing the distinguishing prefix (``"after FIX stop"`` vs ``"after
    FIX exit"``).
    """
    try:
        git_ops._git(
            runner.repo_path,
            "fetch",
            "--prune",
            "origin",
            f"+refs/heads/{branch}:refs/remotes/origin/{branch}",
            timeout=60,
        )
    except (
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
        OSError,
    ) as exc:
        runner.log_event(f"[FIX] fetch {branch} failed {context}: {exc}.")
        return None
    try:
        remote_head = git_ops._git(
            runner.repo_path,
            "rev-parse",
            f"origin/{branch}",
        ).stdout.strip()
    except (
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
        OSError,
    ) as exc:
        runner.log_event(
            f"[FIX] rev-parse origin/{branch} failed {context}: {exc}."
        )
        return None
    if (
        last_known_sha
        and head_after != last_known_sha
        and remote_head == last_known_sha
    ):
        return False
    if remote_head == head_after:
        return True
    try:
        is_ancestor = git_ops._git(
            runner.repo_path,
            "merge-base",
            "--is-ancestor",
            head_after,
            remote_head,
            check=False,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        runner.log_event(
            f"[FIX] merge-base ancestry check failed {context}: {exc}."
        )
        return None
    return is_ancestor.returncode == 0
