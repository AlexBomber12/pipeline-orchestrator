"""FIX state handler with idle timeout and in-flight breach monitoring."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import subprocess
from datetime import datetime, timezone

from src.branch_context import BranchContext
from src.cancellation import (
    CancellationCause,
    safe_record_cancellation_cause,
)
from src.daemon import (
    fix_codex_trigger,
    fix_escalation,
    fix_push_verify,
    fix_supervision,
    git_ops,
)
from src.daemon.guardrails import scan_stdout
from src.daemon.handlers.breach import BreachMixin
from src.daemon.quarantine import apply_quarantine_label_for_violation
from src.daemon.recovery_policy import BoundedRecoveryPolicy
from src.github import comments as gh_comments
from src.github import gh_runner
from src.models import CIStatus, PipelineState, PRInfo, ReviewStatus
from src.retry import retry_transient

logger = logging.getLogger(__name__)

_FIX_CI_LOG_TRUNCATE_CHARS = 5000


def _fetch_failed_ci_logs(repo: str, branch: str) -> str | None:
    """Return the last 5000 chars of failed CI logs for ``branch``, or ``None``.

    Resolves the most recent failed run via ``gh run list`` filtered by
    branch, then pulls the failure-only output via ``gh run view
    --log-failed``. Returns ``None`` on any lookup error so the FIX
    prompt simply omits the section instead of blocking on observability.
    """
    try:
        runs = gh_runner.run_gh(
            [
                "run",
                "list",
                "--branch",
                branch,
                "--status",
                "failure",
                "--limit",
                "1",
                "--json",
                "databaseId",
            ],
            repo=repo,
        )
    except (RuntimeError, subprocess.TimeoutExpired, OSError):
        return None
    if not isinstance(runs, list) or not runs:
        return None
    first = runs[0]
    if not isinstance(first, dict):
        return None
    run_id = first.get("databaseId")
    if not run_id:
        return None
    try:
        logs = gh_runner.run_gh(
            ["run", "view", str(run_id), "--log-failed"],
            repo=repo,
            timeout=60,
        )
    except (RuntimeError, subprocess.TimeoutExpired, OSError):
        return None
    if not isinstance(logs, str) or not logs:
        return None
    if len(logs) > _FIX_CI_LOG_TRUNCATE_CHARS:
        return f"[truncated]\n{logs[-_FIX_CI_LOG_TRUNCATE_CHARS:]}"
    return logs


class FixMixin(BreachMixin):
    """FIX FEEDBACK handler with idle timeout and breach monitoring."""

    async def _monitor_fix_idle(
        self,
        pr_number: int,
        idle_limit: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        idle_flag: dict[str, bool],
    ) -> None:
        """Thin wrapper — see ``fix_supervision.monitor_fix_idle``."""
        await fix_supervision.monitor_fix_idle(
            self, pr_number, idle_limit, target, idle_flag
        )

    def _ensure_escalated_label(
        self, pr_number: int, label_create_log_prefix: str
    ) -> bool:
        """Thin wrapper over ``fix_escalation.ensure_escalated_label``.

        Kept as a method because existing tests still invoke
        ``runner._ensure_escalated_label(...)`` directly to assert the
        durable PR-side escalation marker behaves as the FixMixin
        contract describes.
        """
        return fix_escalation.ensure_escalated_label(
            self, pr_number, label_create_log_prefix
        )

    async def _escalate_fix_no_push_deadlock(self, current_pr: PRInfo) -> None:
        """Thin wrapper — see ``fix_escalation.escalate_fix_no_push_deadlock``."""
        await fix_escalation.escalate_fix_no_push_deadlock(self, current_pr)

    async def _escalate_fix_coder_initiated(
        self, current_pr: PRInfo, reason: str
    ) -> None:
        """Thin wrapper — see ``fix_escalation.escalate_fix_coder_initiated``."""
        await fix_escalation.escalate_fix_coder_initiated(
            self, current_pr, reason
        )

    async def _escalate_fix_iteration_cap(self, current_pr: PRInfo) -> None:
        """Thin wrapper — see ``fix_escalation.escalate_fix_iteration_cap``."""
        await fix_escalation.escalate_fix_iteration_cap(self, current_pr)

    async def _poll_github_during_fix(
        self,
        pr_number: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        terminal_flag: dict[str, str | None],
    ) -> None:
        """Thin wrapper — see ``fix_supervision.poll_github_during_fix``."""
        await fix_supervision.poll_github_during_fix(
            self, pr_number, target, terminal_flag
        )

    def _verify_pushes_since(
        self,
        branch: str,
        last_known_sha: str,
        head_after: str,
        *,
        context: str,
    ) -> bool | None:
        """Thin wrapper — see ``fix_push_verify.verify_pushes_since``."""
        return fix_push_verify.verify_pushes_since(
            self,
            branch,
            last_known_sha,
            head_after,
            context=context,
        )

    def _github_api_budget_paused(self) -> bool:
        """Return ``True`` when the cached GH API budget is below pause threshold."""
        budget = self._github_api_budget_cache
        if budget is None:
            return False
        pause_pct = self.app_config.daemon.github_api_pause_threshold_percent
        if budget.remaining_percent >= pause_pct:
            return False
        if datetime.now(timezone.utc) >= budget.reset_at:
            return False
        return True

    def _run_coder_with_polling(
        self,
        pr_number: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        terminal_flag: dict[str, str | None],
    ) -> asyncio.Task[None] | None:
        """Thin wrapper — see ``fix_supervision.run_coder_with_polling``."""
        return fix_supervision.run_coder_with_polling(
            self, pr_number, target, terminal_flag
        )

    async def _handle_external_terminal_pr_state(
        self, terminal_state: str
    ) -> None:
        """Thin wrapper — see ``fix_supervision.handle_external_terminal_pr_state``."""
        await fix_supervision.handle_external_terminal_pr_state(
            self, terminal_state
        )

    async def _build_fix_feedback_context(
        self, current_pr: PRInfo
    ) -> str | None:
        """Compose CI failure logs + latest review feedback for the FIX prompt.

        Each section is added independently so the coder receives whatever
        signals the daemon can resolve. Returns ``None`` when neither
        source is reachable; the prompt then falls back to bare
        ``FIX FEEDBACK``.
        """
        sections: list[str] = []
        if current_pr.ci_status == CIStatus.FAILURE:
            ci_logs = await asyncio.to_thread(
                _fetch_failed_ci_logs, self.owner_repo, current_pr.branch
            )
            if ci_logs:
                sections.append(
                    "CI failure logs (last 5000 chars):\n" + ci_logs
                )
        if current_pr.review_status == ReviewStatus.CHANGES_REQUESTED:
            feedback = await asyncio.to_thread(
                gh_comments.get_latest_codex_feedback,
                self.owner_repo, current_pr.number,
            )
            if feedback:
                sections.append("Latest review feedback:\n" + feedback)
        if not sections:
            return None
        return "\n\n".join(sections)

    async def handle_fix(self) -> None:
        """Run ``FIX FEEDBACK`` via the active coder CLI and return to WATCH."""
        self._stop_requested = False
        # PR-358: a FIX entry begins a new review iteration; clear the
        # single-shot review_timeout repost flag so the next WATCH
        # iteration after this FIX can post one repost again if Codex
        # falls silent on the new push. The companion timestamp (used as
        # the durable elapsed_min floor on the next iteration) clears in
        # lockstep so a stale repost anchor cannot suppress a legitimate
        # timeout in the new window.
        self.state.review_timeout_repost_attempted = False
        self.state.review_timeout_repost_at = None
        logger.info(
            "[BRANCH] handle_fix: %s",
            BranchContext.from_runner(self).log_summary(),
        )
        await self._refresh_auth_status_cache()
        coder_name, plugin = self._get_coder(allow_exploration=False)
        if not await self._check_rate_limit(proactive_coder=coder_name):
            return
        if not await self._check_spend_ceiling(coder_name):
            return

        if (
            self.state.current_pr is not None
            and self.state.current_pr.is_cross_repository
        ):
            self.log_event(
                f"[FIX] Skipping FIX for cross-repo PR "
                f"#{self.state.current_pr.number}."
            )
            self.state.state = PipelineState.WATCH
            return

        self.state.state = PipelineState.FIX
        current_pr = self.state.current_pr
        fix_iteration_cap = self.app_config.daemon.fix_iteration_cap
        if current_pr is not None and current_pr.is_escalated:
            await self._clear_error_message_on_recovery(
                log_prefix="[FIX]",
                reason="escalated PR returned to IDLE",
            )
            self.state.state = PipelineState.IDLE
            self.log_event(
                f"[FIX] FIX blocked for escalated PR #{current_pr.number}, "
                f"moving to IDLE."
            )
            await self.publish_state()
            return
        fix_iteration_policy: BoundedRecoveryPolicy[PRInfo] = BoundedRecoveryPolicy(
            name="fix_iteration_cap",
            max_attempts=fix_iteration_cap,
            counter_getter=lambda pr: pr.fix_iteration_count,
            counter_setter=lambda pr, n: setattr(pr, "fix_iteration_count", n),
            on_threshold=lambda pr: self._escalate_fix_iteration_cap(pr),
        )
        no_push_policy: BoundedRecoveryPolicy[PRInfo] = BoundedRecoveryPolicy(
            name="fix_no_push_cap",
            max_attempts=self.app_config.daemon.fix_no_push_cap,
            counter_getter=lambda pr: pr.no_push_fix_count,
            counter_setter=lambda pr, n: setattr(pr, "no_push_fix_count", n),
            on_threshold=lambda pr: self._escalate_fix_no_push_deadlock(pr),
        )
        if current_pr is not None and await fix_iteration_policy.maybe_escalate(
            current_pr
        ):
            return
        self.log_event(f"[FIX] [{coder_name}] entering FIX.")
        await self.publish_state()
        if self._current_run_record is not None:
            self._current_run_record.fix_iterations += 1
            await self._checkpoint_current_run_record()

        if (
            self.state.current_pr is not None
            and not self.state.current_pr.is_cross_repository
        ):
            branch = self.state.current_pr.branch
            try:
                retry_transient(
                    lambda: git_ops._git(
                        self.repo_path,
                        "fetch", "--prune", "origin",
                        f"+refs/heads/{branch}:refs/remotes/origin/{branch}",
                        timeout=60,
                    ),
                    operation_name=f"git fetch origin {branch}",
                )
                git_ops._git(self.repo_path, "checkout", branch)
                git_ops._git(self.repo_path, "reset", "--hard", f"origin/{branch}")
            except (
                subprocess.CalledProcessError,
                subprocess.TimeoutExpired,
                OSError,
                RuntimeError,
            ) as exc:
                stderr = getattr(exc, "stderr", "") or ""
                await self._transition_to_error(
                    f"git refresh {branch} failed: {stderr.strip() or exc}",
                    save_run_record_as=None,
                    publish=False,
                    log_prefix="[FIX]",
                )
                return

        head_before = ""  # PR-050: detect whether a commit actually happened
        try:
            head_before = git_ops._git(
                self.repo_path, "rev-parse", "HEAD"
            ).stdout.strip()
        except (
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
            OSError,
        ):
            pass

        idle_limit = self.app_config.daemon.fix_idle_timeout_sec
        pr_number = (
            self.state.current_pr.number if self.state.current_pr else 0
        )

        breach_dir, breach_run_id = self._breach_env()
        breach_flag: dict[str, bool] = {"breached": False}
        idle_flag: dict[str, bool] = {"timed_out": False}
        external_state_flag: dict[str, str | None] = {"state": None}

        plugin_run_kwargs = plugin.build_run_kwargs(
            daemon_config=self.app_config.daemon,
            breach_dir=breach_dir,
            breach_run_id=breach_run_id,
        )

        heartbeat = asyncio.create_task(self._publish_while_waiting("FIX"))
        fix_kwargs: dict[str, object] = {
            **plugin_run_kwargs,
            "on_process_start": self._track_current_coder_process,
        }
        if self.state.current_task is not None:
            fix_kwargs["pr_id"] = self.state.current_task.pr_id
            fix_kwargs["task_file"] = self.state.current_task.task_file
        if self.state.current_pr is not None:
            extra_context = await self._build_fix_feedback_context(
                self.state.current_pr
            )
            if extra_context is not None:
                fix_kwargs["extra_context"] = extra_context
        claude_task: asyncio.Task[tuple[int, str, str]] = asyncio.create_task(
            plugin.fix_review(
                self.repo_path,
                **fix_kwargs,
            )
        )
        stop_monitor = asyncio.create_task(self._monitor_stop_request(claude_task))
        idle_monitor = asyncio.create_task(
            self._monitor_fix_idle(pr_number, idle_limit, claude_task, idle_flag)
        )
        breach_monitor: asyncio.Task[None] | None = None
        if plugin.supports_breach_lifecycle:
            breach_monitor = asyncio.create_task(
                self._monitor_inflight_breach(
                    breach_dir, breach_run_id, claude_task, breach_flag,
                )
            )
        external_state_monitor = self._run_coder_with_polling(
            pr_number, claude_task, external_state_flag,
        )
        stop_cancelled = False
        try:
            code, stdout, stderr = await claude_task
        except asyncio.CancelledError:
            if self._stop_requested:
                stop_cancelled = True
                code, stdout, stderr = 1, "", ""
            elif external_state_flag["state"] is not None:
                # External terminal state observed by the polling task;
                # the post-finally external-state branch drives the
                # transition so the same code path also covers the race
                # where the coder finished during the SIGTERM grace
                # window and ``target.cancel()`` became a no-op (Codex
                # P1 on PR #223).
                code, stdout, stderr = 1, "", ""
            elif breach_flag["breached"]:
                if self.state.current_pr is not None:
                    # Breach pause is not a no-push success; reset the
                    # streak so a no-push success → breach → no-push
                    # success sequence is not treated as consecutive
                    # (Codex P2 on PR #222).
                    no_push_policy.reset(self.state.current_pr)
                    self._rehydrate_last_push_at(self.state.current_pr)
                    try:
                        head_now = git_ops._git(
                            self.repo_path, "rev-parse", "HEAD"
                        ).stdout.strip()
                    except Exception:
                        head_now = ""
                    if head_before and head_now and head_before != head_now:
                        if not await fix_codex_trigger.maybe_post_codex_review_after_push(
                            self,
                            self.state.current_pr.number,
                            "after breach-cancel fix push; manual review "
                            "trigger required",
                        ):
                            return
                self.state.state = PipelineState.PAUSED
                await self._clear_error_message_on_recovery(
                    log_prefix="[FIX]",
                    reason="in-flight rate limit breach",
                )
                self.log_event(
                    f"[FIX] FIX aborted: in-flight rate limit breach, "
                    f"paused until {self.state.rate_limited_until}."
                )
                return
            elif not idle_flag["timed_out"]:
                raise
            else:
                code, stdout, stderr = 1, "", ""
        finally:
            stop_monitor.cancel()
            if breach_monitor is not None:
                breach_monitor.cancel()
            idle_monitor.cancel()
            if external_state_monitor is not None:
                external_state_monitor.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await external_state_monitor
            heartbeat.cancel()
            self._current_coder_process = None
            if plugin.supports_breach_lifecycle:
                self._check_late_breach(breach_dir, breach_run_id, breach_flag)
                self._cleanup_breach_marker(breach_dir, breach_run_id)
        if external_state_flag["state"] is not None and not stop_cancelled:
            await self._handle_external_terminal_pr_state(
                external_state_flag["state"]  # type: ignore[arg-type]
            )
            return
        if breach_flag["breached"]:
            if self.state.current_pr is not None:
                # Late-breach pause is not a no-push success; reset the
                # streak (Codex P2 on PR #222) — same rationale as the
                # CancelledError breach path above.
                no_push_policy.reset(self.state.current_pr)
                self._rehydrate_last_push_at(self.state.current_pr)
                try:
                    head_now = git_ops._git(
                        self.repo_path, "rev-parse", "HEAD"
                    ).stdout.strip()
                except Exception:
                    head_now = ""
                if head_before and head_now and head_before != head_now:
                    if not await fix_codex_trigger.maybe_post_codex_review_after_push(
                        self,
                        self.state.current_pr.number,
                        "after late-breach fix push; manual review "
                        "trigger required",
                    ):
                        return
            self.state.state = PipelineState.PAUSED
            await self._clear_error_message_on_recovery(
                log_prefix="[FIX]",
                reason="late in-flight rate limit breach",
            )
            self.log_event(
                f"[FIX] FIX paused: late in-flight rate limit breach, "
                f"paused until {self.state.rate_limited_until}."
            )
            return

        stop_requested_after_exit = False

        async def capture_stop_requested_after_exit() -> bool:
            nonlocal stop_requested_after_exit
            if stop_requested_after_exit:
                return True
            if self._stop_requested:
                stop_requested_after_exit = True
                return True
            requested = await self._pop_stop_request()
            if not requested:
                return False
            self._stop_requested = True
            self.state.user_paused = True
            stop_requested_after_exit = True
            self.log_event(
                "[FIX] User stop requested after FIX exit; deferring "
                "pause until FIX bookkeeping completes."
            )
            return True

        async def pause_for_stop_after_bookkeeping() -> bool:
            if not stop_requested_after_exit:
                return False
            self.state.state = PipelineState.PAUSED
            await self._clear_error_message_on_recovery(
                log_prefix="[FIX]",
                reason="user stop requested after FIX exit",
            )
            self.log_event("[FIX] FIX aborted: user stop requested.")
            return True

        async def read_head_after_fix() -> str | None:
            try:
                return git_ops._git(
                    self.repo_path, "rev-parse", "HEAD"
                ).stdout.strip()
            except (
                subprocess.CalledProcessError,
                subprocess.TimeoutExpired,
                OSError,
            ) as exc:
                await self._transition_to_error(
                    f"rev-parse after fix failed: {exc}",
                    save_run_record_as=None,
                    publish=False,
                    log_prefix="[FIX]",
                )
                return None

        def remote_branch_contains_head(branch: str, head_after: str) -> bool:
            return (
                self._verify_pushes_since(
                    branch,
                    head_before,
                    head_after,
                    context="after FIX stop",
                )
                is True
            )

        async def record_fix_push(head_after: str, failure_detail: str) -> bool:
            if head_before and head_before == head_after:
                return True

            now_time = datetime.now(timezone.utc)
            if self.state.current_pr is not None:
                self._last_push_at = self._canonical_push_timestamp(
                    self.state.current_pr.number
                )
                self._last_push_at_pr_number = self.state.current_pr.number
                self.state.current_pr.record_observed_head(head_after)
                iteration = fix_iteration_policy.increment(self.state.current_pr)
                no_push_policy.reset(self.state.current_pr)
                self.state.current_pr.last_activity = now_time
            else:
                self._last_push_at = now_time
                iteration = 0

            self.log_event(f"[FIX] Fix pushed, iteration #{iteration}.")
            if self.state.current_pr is not None:
                if not await fix_codex_trigger.maybe_post_codex_review_after_push(
                    self,
                    self.state.current_pr.number,
                    failure_detail,
                ):
                    return False
            return True

        await capture_stop_requested_after_exit()
        if idle_flag["timed_out"]:
            # Idle timeout breaks the no-push streak: the coder didn't
            # produce a push and the daemon killed it. Reset the counter
            # so a later "no-push success" cycle starts fresh rather
            # than tripping the cap on a non-consecutive sequence
            # (Codex P2 on PR #222).
            if self.state.current_pr is not None:
                no_push_policy.reset(self.state.current_pr)
            await self._transition_to_error(
                f"FIX idle timeout: no push for {idle_limit}s",
                save_run_record_as=None,
                publish=False,
                log_prefix="[FIX]",
                cancellation_cause=CancellationCause(
                    category="ERROR",
                    payload={
                        "subsource": "fix_idle_timeout",
                        "limit_type": "fix_idle",
                        "duration_elapsed_sec": idle_limit,
                        "active_phase": PipelineState.FIX.value,
                    },
                ),
            )
            await self._save_cli_log("", "", "FIX idle timeout")
            if await pause_for_stop_after_bookkeeping():
                return
            return
        await self._save_cli_log(stdout, stderr, f"FIX FEEDBACK output [{coder_name}]")
        violations = scan_stdout(f"{stdout}\n{stderr}")
        if violations:
            first = violations[0]
            cause = f"GUARDRAIL: {first.category}: {first.excerpt}"
            for violation in violations:
                self.log_event(
                    f"[FIX] [GUARDRAIL] tier={violation.tier} "
                    f"{violation.category}: {violation.excerpt}"
                )
            if await pause_for_stop_after_bookkeeping():
                return
            if self.state.current_pr is not None:
                pr_number = self.state.current_pr.number
                self.state.quarantined_prs.add(pr_number)
                apply_quarantine_label_for_violation(self, pr_number, first)
            await self._transition_to_error(
                cause,
                save_run_record_as=None,
                publish=False,
                log_prefix="[FIX]",
                cancellation_cause=CancellationCause(
                    category="ERROR",
                    payload={"subsource": "guardrail", "reason_text": cause},
                ),
            )
            return
        await capture_stop_requested_after_exit()
        escalate_reason = fix_escalation.parse_escalate_marker(stdout)
        if escalate_reason is not None and self.state.current_pr is not None:
            # Coder semantic circuit breaker (PR-166): an explicit
            # self-report wins over the regular push/return-code flow.
            # ``no_push_fix_count`` is reset because an explicit
            # ESCALATE is not a no-push success — it is a deliberate
            # bail-out and should not feed the deadlock counter.
            no_push_policy.reset(self.state.current_pr)
            if self.state.current_task is not None:
                await safe_record_cancellation_cause(
                    self.redis,
                    self.name,
                    self.state.current_task.pr_id,
                    CancellationCause(
                        category="ERROR",
                        payload={
                            "subsource": "coder_escalate",
                            "reason_text": escalate_reason,
                        },
                    ),
                    log=self.log_event,
                )
            await self._escalate_fix_coder_initiated(
                self.state.current_pr, escalate_reason
            )
            # If a stop arrived while the coder was running, honor the
            # deferred pause: PAUSED takes precedence over the IDLE/HUNG
            # parking state set by ``_escalate_fix_coder_initiated`` so
            # the operator's pause request is not silently dropped on
            # the ESCALATE branch (Codex P1 on PR #228).
            if await pause_for_stop_after_bookkeeping():
                return
            return
        if stop_cancelled:
            head_after = await read_head_after_fix()
            if head_after is None:
                return
            # Stop-cancel breaks the consecutive no-push streak (Codex P2
            # on PR #222). ``record_fix_push`` already resets on a
            # productive push; this covers the no-push case.
            if self.state.current_pr is not None:
                no_push_policy.reset(self.state.current_pr)
            branch = self.state.current_pr.branch if self.state.current_pr is not None else ""
            if branch and remote_branch_contains_head(branch, head_after):
                if not await record_fix_push(
                    head_after,
                    "after stop-cancel fix push; manual review trigger "
                    "required to avoid fix/push loop",
                ):
                    return
            elif head_before and head_before != head_after:
                self.log_event(
                    "[FIX] FIX stop-cancel left local HEAD outside the "
                    "fetched remote branch; skipping push bookkeeping and "
                    "@codex review."
                )
            if await pause_for_stop_after_bookkeeping():
                return
            self.state.state = PipelineState.PAUSED  # pragma: no cover - defensive fallback
            await self._clear_error_message_on_recovery(  # pragma: no cover - defensive fallback
                log_prefix="[FIX]",
                reason="stop-cancel defensive pause fallback",
            )
            return  # pragma: no cover - defensive fallback
        if code != 0:
            # FIX failure breaks the consecutive no-push streak (Codex P2
            # on PR #222): a sequence like no-push success → failed FIX
            # → no-push success would otherwise still trip the deadlock
            # cap even though the no-push cycles were not consecutive.
            if self.state.current_pr is not None:
                no_push_policy.reset(self.state.current_pr)
            self._detect_rate_limit(stderr, coder_name=coder_name)
            if self.state.rate_limited_until is not None:
                self.state.state = PipelineState.PAUSED
                await self._clear_error_message_on_recovery(
                    log_prefix="[FIX]",
                    reason="rate-limit pause after fix failure",
                )
                self.log_event(
                    f"[RATE-LIMIT] Rate limit pause active until "
                    f"{self.state.rate_limited_until.isoformat()}."
                )
                return
            if await pause_for_stop_after_bookkeeping():
                return
            await self._transition_to_error(
                stderr.strip() or f"{coder_name} exit {code}",
                save_run_record_as=None,
                publish=False,
                log_prefix=f"[FIX] [{coder_name}] fix_review failed:",
            )
            return

        head_after = await read_head_after_fix()
        if head_after is None:
            return

        local_no_push = bool(head_before) and head_before == head_after
        remote_no_push = False
        if not local_no_push:
            verify_branch = (
                self.state.current_pr.branch
                if self.state.current_pr is not None
                else ""
            )
            verification = (
                self._verify_pushes_since(
                    verify_branch,
                    head_before,
                    head_after,
                    context="after FIX exit",
                )
                if verify_branch
                else None
            )
            if verification is False:
                remote_no_push = True
            elif verification is None and verify_branch:
                self.log_event(
                    "[FIX] FIX push verification unavailable; "
                    "proceeding optimistically."
                )

        if local_no_push or remote_no_push:
            if self.state.current_pr is not None:
                self._last_push_at = self._canonical_push_timestamp(
                    self.state.current_pr.number
                )
            else:  # pragma: no cover - defensive: current_pr is set throughout handle_fix
                self._last_push_at = datetime.now(timezone.utc)
            if local_no_push:
                self.log_event(
                    "[FIX] FIX FEEDBACK exited 0 but HEAD unchanged; "
                    "no push, skipping @codex review."
                )
            else:
                self.log_event(
                    "[FIX] Coder exited cleanly but no push detected; "
                    "treating as no-push, skipping @codex review."
                )
            if self.state.current_pr is not None:
                no_push_policy.increment(self.state.current_pr)
            if await pause_for_stop_after_bookkeeping():
                return
            if (
                self.state.current_pr is not None
                and await no_push_policy.maybe_escalate(self.state.current_pr)
            ):
                return
            self.state.state = PipelineState.WATCH
            return

        if not await record_fix_push(
            head_after,
            "after fix push; manual review trigger required "
            "to avoid fix/push loop",
        ):
            return
        if await pause_for_stop_after_bookkeeping():
            return
        self.state.state = PipelineState.WATCH
