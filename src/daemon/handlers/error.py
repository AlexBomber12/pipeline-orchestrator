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
        coder_name, _ = self._get_coder()
        if coder_name == "claude":
            # Claude-backed repos must preserve the original pause-and-resume
            # behavior so ERROR context survives until Claude recovers.
            if not await self._check_rate_limit(proactive_coder="claude"):
                return
        else:
            # Diagnosis still uses claude_cli, but when another coder is
            # active we should skip diagnosis rather than pausing all work.
            snapshot = await asyncio.to_thread(self._claude_usage_provider.fetch)
            if snapshot and (
                snapshot.session_percent
                >= self.app_config.daemon.rate_limit_session_pause_percent
                or snapshot.weekly_percent
                >= self.app_config.daemon.rate_limit_weekly_pause_percent
            ):
                self.log_event("Skipping AI diagnosis: Claude rate limited")
                self.state.state = PipelineState.IDLE
                self.state.error_message = None
                self._error_diagnose_count = 0
                return

        context = error_context or self.state.error_message or "Unknown error"
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
