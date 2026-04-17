"""Preflight checks and dirty-tree auto-recovery.

Mixin methods:
    preflight              — check if working tree is clean
    _auto_reset_dirty_tree — hard-reset after N consecutive dirty cycles
"""

from __future__ import annotations

import subprocess

from src.daemon import git_ops
from src.models import PipelineState

# After this many consecutive cycles of a dirty working tree,
# ``preflight`` hard-resets the repo to ``origin/{branch}`` and
# returns IDLE instead of ERROR. Without this safety net a single
# interrupted Claude run can leave the runner stuck in ERROR forever.
_DIRTY_CYCLES_BEFORE_AUTO_RESET = 3


class PreflightMixin:
    """Preflight checks and dirty-tree auto-recovery."""

    def preflight(self) -> bool:
        """Return ``True`` iff the working tree is clean."""
        try:
            result = git_ops._git(self.repo_path, "status", "--porcelain")
        except (
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
            OSError,
        ) as exc:
            # ``OSError`` (missing git binary, missing cwd, permission
            # errors) otherwise escapes to daemon.main's generic
            # handler and leaves the runner state stale.
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"preflight failed: {exc}"
            self.log_event(f"preflight failed: {exc}")
            return False

        dirty = result.stdout.strip()
        if dirty:
            self._consecutive_dirty_cycles += 1
            if self._consecutive_dirty_cycles >= _DIRTY_CYCLES_BEFORE_AUTO_RESET:
                self.log_event(
                    f"Dirty tree persisted {self._consecutive_dirty_cycles} "
                    "cycles, auto-resetting to recover"
                )
                if self._auto_reset_dirty_tree():
                    return True
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"working tree dirty: {dirty}"
            self.log_event("preflight: dirty working tree")
            return False
        self._consecutive_dirty_cycles = 0
        return True

    def _auto_reset_dirty_tree(self) -> bool:
        """Hard-reset the working tree to ``origin/{branch}``.

        Called by ``preflight`` once the consecutive-dirty counter
        crosses ``_DIRTY_CYCLES_BEFORE_AUTO_RESET``. On success the
        runner resumes the state it was in before the dirty-tree
        stall: WATCH when an open PR was being tracked, IDLE
        otherwise. Dropping back to IDLE unconditionally would make
        the next cycle re-pick the still-TODO task from
        ``origin/{base}:tasks/QUEUE.md`` and open a duplicate PR.
        On failure the counter is left untouched and the caller
        falls through to the usual ERROR path.
        """
        branch = self.repo_config.branch
        try:
            # ``--force`` so a dirty PR-branch working tree cannot
            # block the switch back to ``branch``. Without it a failing
            # checkout would leave HEAD on the feature branch while the
            # next ``git reset --hard origin/{branch}`` moves THAT
            # feature branch's tip to ``origin/{branch}``, corrupting
            # the tracked PR branch. Paired with ``check=True`` so any
            # residual checkout failure aborts the reset chain instead
            # of silently proceeding on the wrong ref.
            git_ops._git(self.repo_path, "checkout", "--force", branch)
            git_ops._git(self.repo_path, "reset", "--hard", f"origin/{branch}")
            git_ops._git(self.repo_path, "clean", "-fd")
        except (
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
            OSError,
        ) as exc:
            self.log_event(f"Auto-recovery failed: {exc}")
            return False
        self._consecutive_dirty_cycles = 0
        if self.state.current_pr is not None:
            resumed = PipelineState.WATCH
        else:
            resumed = PipelineState.IDLE
        self.state.state = resumed
        self.state.error_message = None
        self.log_event(f"Auto-recovered from dirty tree -> {resumed.value}")
        return True
