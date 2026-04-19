"""ERROR state handler.

Mixin methods:
    handle_error — ask Claude CLI for diagnosis: FIX, SKIP, or ESCALATE

Module-level:
    ErrorCategory   — enum for error classification
    _classify_error — classify error context string
"""

from __future__ import annotations

import asyncio
import logging
import re
import subprocess
from enum import Enum

from src import claude_cli
from src.daemon import git_ops
from src.models import PipelineState
from src.retry import retry_transient

logger = logging.getLogger(__name__)
_CLAUDE_CLI_COAUTHOR = "Co-authored-by: Claude CLI <noreply@anthropic.com>"


class ErrorCategory(Enum):
    RATE_LIMIT = "rate_limit"
    TIMEOUT = "timeout"
    OTHER = "other"


def _classify_error(context: str) -> ErrorCategory:
    lowered = context.lower()
    if "rate limit" in lowered or re.search(r"\b429\b", lowered):
        return ErrorCategory.RATE_LIMIT
    if "timeout" in lowered:
        return ErrorCategory.TIMEOUT
    return ErrorCategory.OTHER


class ErrorMixin:
    """Ask the claude CLI whether to FIX, SKIP, or ESCALATE the error."""

    async def handle_error(self, error_context: str | None = None) -> None:
        """Ask the claude CLI whether to FIX, SKIP, or ESCALATE the error."""
        context = error_context or self.state.error_message or "Unknown error"
        # Diagnosis still uses claude_cli, so skip it rather than pausing the
        # repo when Claude itself is already rate-limited.
        try:
            snapshot = await asyncio.to_thread(self._claude_usage_provider.fetch)
        except Exception:
            snapshot = None
        if snapshot and (
            snapshot.session_percent
            >= self.app_config.daemon.rate_limit_session_pause_percent
            or snapshot.weekly_percent
            >= self.app_config.daemon.rate_limit_weekly_pause_percent
        ):
            if context == self._error_skip_context:
                self._error_skip_count += 1
            else:
                self._error_skip_context = context
                self._error_skip_count = 1
            self._error_skip_active = True
            if self._error_skip_count > 3:
                self.log_event(
                    "Skipping AI diagnosis: Claude rate limited; "
                    "max soft-skip retries (3) reached, staying ERROR"
                )
                return

            self.log_event("Skipping AI diagnosis: Claude rate limited")
            self.state.state = PipelineState.IDLE
            self.state.error_message = None
            self._error_diagnose_count = 0
            return

        self._error_skip_context = None
        self._error_skip_count = 0
        self._error_skip_active = False
        category = _classify_error(context)
        if category == ErrorCategory.RATE_LIMIT:
            self.log_event("Skipping AI diagnosis for rate-limit error")
            return
        if category == ErrorCategory.TIMEOUT:
            self.log_event(
                "Skipping AI diagnosis for timeout error; "
                "will retry on next cycle"
            )
            return
        self._error_diagnose_count += 1
        if self._error_diagnose_count > 3:
            self.log_event(
                "diagnose_error: max attempts (3) reached, staying ERROR"
            )
            return
        code, stdout, stderr = await claude_cli.diagnose_error_async(
            self.repo_path, context, model=self.app_config.daemon.claude_model
        )
        self._detect_rate_limit(stderr, coder_name="claude")
        if self.state.rate_limited_until is not None:
            self.state.state = PipelineState.PAUSED
            # Preserve error_message so handle_paused resumes to ERROR
            self.log_event(
                f"Rate limit pause active until "
                f"{self.state.rate_limited_until.isoformat()}"
            )
            return
        if code != 0:
            self.log_event(
                f"diagnose_error CLI failed: {stderr.strip() or f'exit {code}'}"
            )
            return

        summary = stdout.strip().splitlines()[-1] if stdout.strip() else ""
        verdict = claude_cli.parse_diagnosis(stdout)
        dirty = ""
        try:
            dirty = git_ops._git(self.repo_path, "status", "--porcelain").stdout.strip()
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
            pass
        if dirty and verdict == "FIX":
            branch = None
            if self.state.current_pr is not None:
                if not self.state.current_pr.is_cross_repository:
                    branch = self.state.current_pr.branch
            elif (
                self.state.current_task is not None
                and self.state.current_task.branch
                and self.state.current_task.branch != self.repo_config.branch
            ):
                branch = self.state.current_task.branch
            if branch is None:
                verdict = "ESCALATE"
                self.log_event(
                    "diagnose_error: dirty tree without active PR/task branch"
                )
            else:
                checked_out_branch = ""
                head_before = ""
                try:
                    head_before = git_ops._git(
                        self.repo_path, "rev-parse", "HEAD"
                    ).stdout.strip()
                    checked_out_branch = git_ops._git(
                        self.repo_path, "rev-parse", "--abbrev-ref", "HEAD"
                    ).stdout.strip()
                    if checked_out_branch != branch:
                        verdict = "ESCALATE"
                        self.log_event(
                            "diagnose_error: active branch mismatch "
                            f"({checked_out_branch!r} != {branch!r})"
                        )
                        raise RuntimeError("diagnose_error branch mismatch")
                    git_ops._git(self.repo_path, "add", "-A")
                    git_ops._git(
                        self.repo_path,
                        "commit",
                        "-m",
                        f"diagnose_error auto-fix: {(summary or 'no summary')[:80]}",
                        "-m",
                        _CLAUDE_CLI_COAUTHOR,
                    )
                    retry_transient(
                        lambda: git_ops._git(
                            self.repo_path,
                            "push",
                            "origin",
                            f"HEAD:{branch}",
                            timeout=60,
                        ),
                        operation_name=f"git push origin HEAD:{branch}",
                    )
                except (
                    subprocess.CalledProcessError,
                    subprocess.TimeoutExpired,
                    OSError,
                    RuntimeError,
                ):
                    if head_before:
                        git_ops._git(
                            self.repo_path,
                            "reset",
                            "--hard",
                            head_before,
                            check=False,
                        )
                        logger.warning("diagnose_error made uncommittable changes, reset")
                    verdict = "ESCALATE"
        elif dirty:
            try:
                head_before = git_ops._git(
                    self.repo_path, "rev-parse", "HEAD"
                ).stdout.strip()
            except (
                subprocess.CalledProcessError,
                subprocess.TimeoutExpired,
                OSError,
            ):
                head_before = ""
            if head_before:
                git_ops._git(
                    self.repo_path,
                    "reset",
                    "--hard",
                    head_before,
                    check=False,
                )
        if verdict == "SKIP":
            self.state.current_task = None
            self.state.current_pr = None
            self.state.error_message = None
            self.state.state = PipelineState.IDLE
            self._error_diagnose_count = 0
            self.log_event("diagnose_error: SKIP -> IDLE")
        elif verdict == "FIX":
            self.state.error_message = None
            self.state.state = PipelineState.IDLE
            self._error_diagnose_count = 0
            self.log_event(f"diagnose_error: FIX -> IDLE ({summary[:80]})")
        else:  # ESCALATE
            self.log_event("diagnose_error: ESCALATE, keeping ERROR state")
