"""ERROR state handler.

Mixin methods:
    handle_error — ask Claude CLI for diagnosis: FIX, SKIP, or ESCALATE

Module-level:
    ErrorCategory   — enum for error classification
    _classify_error — classify error context string
"""

from __future__ import annotations

import asyncio
import re
from enum import Enum

from src import claude_cli
from src.models import PipelineState


class ErrorCategory(Enum):
    RATE_LIMIT = "rate_limit"
    TIMEOUT = "timeout"
    OOM = "oom"
    AUTH_FAILURE = "auth_failure"
    CI_FAILURE = "ci_failure"
    GHOST_PUSH = "ghost_push"
    STALE_BRANCH = "stale_branch"
    CLI_NOT_FOUND = "cli_not_found"
    GIT_ERROR = "git_error"
    OTHER = "other"


def _classify_error(context: str) -> ErrorCategory:
    lowered = context.lower()
    has_ci_token = re.search(r"\bci\b", lowered) is not None
    if "rate limit" in lowered or re.search(r"\b429\b", lowered):
        return ErrorCategory.RATE_LIMIT
    if "timeout" in lowered:
        return ErrorCategory.TIMEOUT
    if (
        re.search(r"\boom\b", lowered)
        or "out of memory" in lowered
        or "killed" in lowered
    ):
        return ErrorCategory.OOM
    if "auth" in lowered or "unauthorized" in lowered or "401" in lowered:
        return ErrorCategory.AUTH_FAILURE
    if has_ci_token and "fail" in lowered:
        return ErrorCategory.CI_FAILURE
    if "ghost push" in lowered or "head sha" in lowered:
        return ErrorCategory.GHOST_PUSH
    if (
        "stale branch" in lowered
        or "non-fast-forward" in lowered
        or "non fast forward" in lowered
        or "branch drift" in lowered
        or "needs rebase" in lowered
        or "need rebase" in lowered
    ):
        return ErrorCategory.STALE_BRANCH
    if "not found" in lowered and "cli" in lowered:
        return ErrorCategory.CLI_NOT_FOUND
    if re.search(r"\bgit\b", lowered) and ("error" in lowered or "fail" in lowered):
        return ErrorCategory.GIT_ERROR
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

        verdict = claude_cli.parse_diagnosis(stdout)
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
            summary = stdout.strip().splitlines()[-1] if stdout.strip() else ""
            self.log_event(f"diagnose_error: FIX -> IDLE ({summary[:80]})")
        else:  # ESCALATE
            self.log_event("diagnose_error: ESCALATE, keeping ERROR state")
