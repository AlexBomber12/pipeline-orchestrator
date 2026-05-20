"""ERROR state handler.

Mixin methods:
    handle_error — ask the selected coder for diagnosis: FIX, SKIP, or ESCALATE

Module-level:
    ErrorCategory   — enum for error classification
    _classify_error — classify error context string
"""

from __future__ import annotations

import asyncio
import logging
import re
import subprocess
from datetime import datetime, timezone
from enum import Enum

from src.branch_context import BranchContext
from src.cancellation import safe_delete_cancellation_cause
from src.daemon import git_ops
from src.diagnosis import parse_diagnosis
from src.models import PipelineState
from src.retry import retry_transient

logger = logging.getLogger(__name__)
_CLAUDE_CLI_COAUTHOR = "Co-authored-by: Claude CLI <noreply@anthropic.com>"

INFRA_ERROR_PATTERNS: tuple[str, ...] = (
    "git fetch origin",
    "ensure_repo_cloned",
)

# ``gh: failed to ...`` is too broad to be an unconditional infra signal:
# the same prefix appears in auth failures (``gh: failed to authenticate``)
# and workflow rejections (``gh: failed to run git: not possible to
# fast-forward``). Treat ``gh:`` only as a git/GitHub *context* marker
# (via _GIT_CONTEXT_REGEX) and require a network symptom from
# _INFRA_NETWORK_PATTERNS or a retry wrapper from _INFRA_RETRY_REGEX
# before bypassing diagnose_error.

# Generic network-symptom strings that also commonly appear in app/test
# errors (e.g. "Failed to connect to database", "Connection timed out"
# from a Redis client). Treat them as infra only when accompanied by an
# explicit git/GitHub reference; otherwise let diagnose_error route them
# normally so FIX/ESCALATE guidance is preserved for actionable failures.
_INFRA_NETWORK_PATTERNS: tuple[str, ...] = (
    "could not connect to",
    "connection timed out",
    "network is unreachable",
    "failed to connect",
    "dial tcp",
)

_GIT_CONTEXT_REGEX = re.compile(
    r"\b(?:git|gh|github\.com|ensure_repo_cloned)\b"
)

# retry_transient raises ``"<operation_name> failed after N attempts: <exc>"``;
# only treat the wrapper as infra when the operation_name is a git/gh network
# call. A bare "failed after N attempts" match would also catch unrelated
# retry strings (API/validation/workflow loops), and "git push" alone catches
# actionable rejections (non-fast-forward, branch protection, auth/policy).
_INFRA_RETRY_REGEX = re.compile(
    r"(?:git (?:clone|fetch|push)|gh api)\b[^\n]*?\bfailed after \d+ attempts"
)


def _is_infra_error(context: str) -> bool:
    """True when ``context`` looks like a git/network infra failure."""
    lowered = context.lower()
    if any(pattern in lowered for pattern in INFRA_ERROR_PATTERNS):
        return True
    if _INFRA_RETRY_REGEX.search(lowered) is not None:
        return True
    if any(pattern in lowered for pattern in _INFRA_NETWORK_PATTERNS):
        return _GIT_CONTEXT_REGEX.search(lowered) is not None
    return False


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
    if (
        re.search(r"\bgit\b", lowered)
        and ("error" in lowered or "fail" in lowered)
    ) or lowered.startswith("fatal:"):
        return ErrorCategory.GIT_ERROR
    return ErrorCategory.OTHER


class ErrorMixin:
    """Ask the selected coder whether to FIX, SKIP, or ESCALATE the error."""

    async def handle_error(self, error_context: str | None = None) -> None:
        """Ask the selected coder whether to FIX, SKIP, or ESCALATE the error."""
        context = error_context or self.state.error_message or "Unknown error"
        logger.info(
            "[BRANCH] handle_error: %s",
            BranchContext.from_runner(self).log_summary(),
        )
        # The cancellation cause was written by the prior _transition_to_error
        # call. Any IDLE-for-retry path below means the task continues, so
        # the previously recorded cause must be cleared — otherwise a later
        # success leaves a stale CRASH/INFRA/TIMEOUT record under the same
        # task_id for the 30-day TTL.
        retry_task = self.state.current_task
        retry_task_id = retry_task.pr_id if retry_task is not None else None

        async def _clear_cause_for_retry() -> None:
            if retry_task_id is not None:
                await safe_delete_cancellation_cause(
                    self.redis,
                    self.name,
                    retry_task_id,
                    log=self.log_event,
                )

        if _is_infra_error(context):
            self._error_skip_context = None
            self._error_skip_policy.reset(self)
            self._error_skip_active = False
            truncated = context if len(context) <= 200 else context[:197] + "..."
            self.log_event(
                f"[ERROR] Infra error detected, skipping AI diagnosis and "
                f"transitioning to IDLE for retry: {truncated}."
            )
            await _clear_cause_for_retry()
            self.state.state = PipelineState.IDLE
            await self._clear_error_message_on_recovery(
                log_prefix="[ERROR]",
                reason="infra error soft-skip to IDLE",
            )
            await self.publish_state()
            return
        category = _classify_error(context)
        if category == ErrorCategory.RATE_LIMIT:
            self._error_skip_context = None
            self._error_skip_policy.reset(self)
            self._error_skip_active = False
            self.log_event(
                "[ERROR] Skipping AI diagnosis for rate-limit error, "
                "transitioning to IDLE for retry."
            )
            await _clear_cause_for_retry()
            self.state.state = PipelineState.IDLE
            await self._clear_error_message_on_recovery(
                log_prefix="[ERROR]",
                reason="rate-limit soft-skip to IDLE",
            )
            await self.publish_state()
            return
        if category == ErrorCategory.TIMEOUT:
            self._error_skip_context = None
            self._error_skip_policy.reset(self)
            self._error_skip_active = False
            self.log_event(
                "[ERROR] Skipping AI diagnosis for timeout error, "
                "transitioning to IDLE for retry."
            )
            await _clear_cause_for_retry()
            self.state.state = PipelineState.IDLE
            await self._clear_error_message_on_recovery(
                log_prefix="[ERROR]",
                reason="timeout soft-skip to IDLE",
            )
            await self.publish_state()
            return
        selected = self._get_auxiliary_coder()
        if selected is None:
            self.log_event(
                "[ERROR] No eligible coder available for error diagnosis; "
                "staying ERROR."
            )
            return
        coder_name, plugin = selected
        provider = (
            self._claude_usage_provider
            if coder_name == "claude"
            else self._codex_usage_provider
        )
        # Soft-skip diagnosis rather than pausing the repo when the selected
        # diagnosis coder is already over its usage threshold.
        try:
            snapshot = await asyncio.to_thread(provider.fetch)
        except Exception:
            snapshot = None
        if snapshot and (
            snapshot.session_percent
            >= self.app_config.daemon.rate_limit_session_pause_percent
            or snapshot.weekly_percent
            >= self.app_config.daemon.rate_limit_weekly_pause_percent
        ):
            if context != self._error_skip_context:
                self._error_skip_policy.reset(self)
                self._error_skip_context = context
            self._error_skip_policy.increment(self)
            self._error_skip_active = True
            if await self._error_skip_policy.maybe_escalate(self):
                # Threshold callback already logged "[ERROR] max
                # soft-skip retries (3) reached, staying ERROR."
                return

            self.log_event(
                f"[ERROR] Skipping AI diagnosis: "
                f"{coder_name.capitalize()} rate limited."
            )
            await _clear_cause_for_retry()
            self.state.state = PipelineState.IDLE
            await self._clear_error_message_on_recovery(
                log_prefix="[ERROR]",
                reason="rate-limited diagnosis skipped to IDLE",
            )
            self._error_diagnose_policy.reset(self)
            return

        self._error_skip_context = None
        self._error_skip_policy.reset(self)
        self._error_skip_active = False
        task_id = retry_task.pr_id if retry_task is not None else ""
        if task_id and await self._is_diagnose_exhausted(task_id):
            return
        self._error_diagnose_policy.increment(self)
        if await self._error_diagnose_policy.maybe_escalate(self):
            # Threshold callback already logged the ceiling message.
            await self._mark_diagnose_exhausted(task_id)
            return
        dirty_before = ""
        try:
            dirty_before = git_ops._git(
                self.repo_path, "status", "--porcelain"
            ).stdout.strip()
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
            pass
        model = (
            self.app_config.daemon.claude_model
            if coder_name == "claude"
            else self.app_config.daemon.codex_model
        )
        code, stdout, stderr = await plugin.diagnose_error(
            self.repo_path, context, model=model
        )
        self._detect_rate_limit(stderr, coder_name=coder_name)
        if self.state.rate_limited_until is not None:
            self.state.state = PipelineState.PAUSED
            # Preserve error_message so handle_paused resumes to ERROR
            self.log_event(
                f"[RATE-LIMIT] Rate limit pause active until "
                f"{self.state.rate_limited_until.isoformat()}."
            )
            return
        if code != 0:
            self.log_event(
                f"[ERROR] diagnose_error CLI failed: "
                f"{stderr.strip() or f'exit {code}'}."
            )
            return

        summary = stdout.strip().splitlines()[-1] if stdout.strip() else ""
        verdict = parse_diagnosis(stdout)
        dirty = ""
        try:
            dirty = git_ops._git(self.repo_path, "status", "--porcelain").stdout.strip()
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
            pass
        if dirty_before and dirty:
            verdict = "ESCALATE"
            self.log_event(
                "[ERROR] diagnose_error: pre-existing dirty tree blocks "
                "automatic cleanup/publish."
            )
            dirty = ""
        if dirty and verdict == "FIX":
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
                if head_before:
                    git_ops._git(
                        self.repo_path,
                        "reset",
                        "--hard",
                        head_before,
                        check=False,
                    )
                    git_ops._git(self.repo_path, "clean", "-fd", check=False)
                verdict = "ESCALATE"
                self.log_event(
                    "[ERROR] diagnose_error: dirty tree without active "
                    "PR/task branch."
                )
            else:
                checked_out_branch = ""
                try:
                    checked_out_branch = git_ops._git(
                        self.repo_path, "rev-parse", "--abbrev-ref", "HEAD"
                    ).stdout.strip()
                    if checked_out_branch != branch:
                        verdict = "ESCALATE"
                        self.log_event(
                            f"[ERROR] diagnose_error: active branch "
                            f"mismatch ({checked_out_branch!r} != "
                            f"{branch!r})."
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
                    if self.state.current_pr is not None:
                        push_time = datetime.now(timezone.utc)
                        self._last_push_at = push_time
                        self._last_push_at_pr_number = self.state.current_pr.number
                        head_after = ""
                        try:
                            head_after = git_ops._git(
                                self.repo_path, "rev-parse", "HEAD"
                            ).stdout.strip()
                        except (
                            subprocess.CalledProcessError,
                            subprocess.TimeoutExpired,
                            OSError,
                        ):
                            pass
                        self.state.current_pr.record_observed_head(head_after)
                        self.state.current_pr.last_activity = push_time
                        if not self._post_codex_review(self.state.current_pr.number):
                            await self._transition_to_error(
                                (
                                    f"Failed to post @codex review on PR "
                                    f"#{self.state.current_pr.number} after "
                                    "diagnose_error fix push; manual review "
                                    "trigger required to avoid fix/push loop"
                                ),
                                save_run_record_as=None,
                                publish=False,
                            )
                            return
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
                        git_ops._git(self.repo_path, "clean", "-fd", check=False)
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
                git_ops._git(self.repo_path, "clean", "-fd", check=False)
        if verdict == "SKIP":
            self.state.current_task = None
            self._reset_runner_local_task_counters()
            self.state.state = PipelineState.IDLE
            self.log_event("[ERROR] diagnose_error: SKIP -> IDLE.")
        elif verdict == "FIX":
            await _clear_cause_for_retry()
            await self._clear_error_message_on_recovery(
                log_prefix="[ERROR]",
                reason="diagnose_error FIX retry",
            )
            self.state.state = PipelineState.IDLE
            self._error_diagnose_policy.reset(self)
            self.log_event(
                f"[ERROR] diagnose_error: FIX -> IDLE ({summary[:80]})."
            )
        else:  # ESCALATE
            self.log_event(
                "[ESCALATE] diagnose_error: ESCALATE, keeping ERROR state."
            )
