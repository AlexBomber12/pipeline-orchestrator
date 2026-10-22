"""FIX state handler with idle timeout and in-flight breach monitoring."""

from __future__ import annotations

import asyncio
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
from src.daemon.quarantine import (
    apply_quarantine_label_for_violation,
    complete_size_fix_marker,
    fixing_size_label,
    is_continued_size_label,
    is_fixing_size_label,
)
from src.daemon.recovery_policy import BoundedRecoveryPolicy
from src.daemon.selector import CoderSelectionUnavailable
from src.github import gh_runner
from src.github import prs as gh_prs
from src.github.reviewer_policy import ReviewerPolicy
from src.models import CIStatus, PipelineState, PRInfo, ReviewStatus
from src.retry import retry_transient
from src.subsource_registry import SuppressionReason

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
        idle_flag: dict[str, bool | float],
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
        self,
        current_pr: PRInfo,
        reviewer_policy: ReviewerPolicy | None = None,
        review_feedback: str | None = None,
        review_limitation: str | None = None,
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
            else:
                sections.append(
                    f"CI failed for PR #{current_pr.number} on {current_pr.branch}; "
                    "failed-run logs unavailable."
                )
        if current_pr.review_status == ReviewStatus.CHANGES_REQUESTED:
            if review_feedback:
                sections.append("Latest review feedback:\n" + review_feedback)
            if review_limitation:
                sections.append("Review evidence limitation: " + review_limitation)
        if not sections:
            return None
        return "\n\n".join(sections)

    async def handle_fix(self) -> None:
        """Run ``FIX FEEDBACK`` via the active coder CLI and return to WATCH."""
        self._stop_requested = False
        fix_feedback_reviewer_policy = self._fix_feedback_reviewer_policy
        self._fix_feedback_reviewer_policy = None
        review_feedback = getattr(self, "_fix_review_feedback", None)
        review_head = getattr(self, "_fix_review_head", None)
        review_origin = getattr(self, "_fix_review_origin", None)
        review_limitation = getattr(self, "_fix_review_limitation", None)
        self._fix_review_feedback = None
        self._fix_review_head = None
        self._fix_review_origin = None
        self._fix_review_limitation = None
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
        try:
            coder_name, plugin = self._get_coder(allow_exploration=False)
        except CoderSelectionUnavailable as exc:
            await self._transition_to_error(
                str(exc),
                publish=False,
                log_prefix="[FIX]",
            )
            return
        if not await self.usage_gate(proactive_coder=coder_name):
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
        if review_origin is not None and current_pr is not None:
            from src.github import reviews as gh_reviews

            expected_origin = (
                self.owner_repo, current_pr.number, current_pr.head_sha,
                fix_feedback_reviewer_policy.fingerprint
                if fix_feedback_reviewer_policy is not None else None,
                gh_reviews.review_cache_generation(self.owner_repo),
            )
            if review_origin != expected_origin:
                review_feedback = None
                review_limitation = "status-poll review identity changed before dispatch"
        if (
            current_pr is not None
            and current_pr.ci_status != CIStatus.FAILURE
            and current_pr.review_status == ReviewStatus.CHANGES_REQUESTED
            and (not review_feedback or not review_head or review_head != current_pr.head_sha)
        ):
            self.log_event("[FIX] Current actionable review findings unavailable; returning to WATCH.")
            self.state.state = PipelineState.WATCH
            return
        fix_iteration_cap = self.app_config.daemon.fix_iteration_cap
        task = self.state.current_task
        if task is not None and current_pr is not None:
            if (
                (task.branch and current_pr.branch != task.branch)
                or current_pr.pr_id not in (None, task.pr_id)
                or (not task.branch and current_pr.pr_id != task.pr_id)
            ):
                self.log_event("[FIX] Task/PR binding unavailable; deferring dispatch.")
                self.state.state = PipelineState.WATCH
                return
            if self._current_run_record is None:
                try:
                    recent = await self._metrics_store.recent(
                        task_id=task.pr_id, limit=20, repo_name=self.name
                    )
                except Exception as exc:
                    self.log_event(f"[FIX] Run accounting unavailable: {exc}; deferring dispatch.")
                    self.state.state = PipelineState.WATCH
                    return
                self._current_run_record = next(
                    (item for item in recent if item.task_id == task.pr_id), None
                )
            record = self._current_run_record
            if record is None:
                if (
                    self.state.active_invocation is not None
                    and self.state.active_invocation.phase == "fix"
                ):
                    self.log_event("[FIX] Prior FIX accounting missing; deferring dispatch.")
                    self.state.state = PipelineState.WATCH
                    return
                self._start_current_run_record(coder_name, "")
                record = self._current_run_record
                assert record is not None
                record.run_phase = "fix"
            elif record.task_id != task.pr_id or (
                record.ended_at is not None and record.exit_reason != "coding_complete"
            ):
                self.log_event("[FIX] Completed or unrelated run record; deferring dispatch.")
                self.state.state = PipelineState.WATCH
                return
            elif record.ended_at is not None:
                if record.fix_iterations - record.fix_recovery_baseline >= fix_iteration_cap:
                    await self._escalate_fix_iteration_cap(current_pr)
                    return
                previous_snapshot = self.state.active_invocation
                self._current_run_record = self._new_fix_run_record(
                    record, coder_name=coder_name,
                    model=plugin.resolve_model(self.app_config.daemon),
                )
                try:
                    await self._checkpoint_current_run_record()
                except Exception as exc:
                    self._current_run_record = record
                    self.state.active_invocation = previous_snapshot
                    self.log_event(f"[FIX] Could not persist active FIX run: {exc}.")
                    self.state.state = PipelineState.WATCH
                    return
                self.state.active_invocation = None
        record = self._current_run_record
        if (
            record is not None and current_pr is not None
            and record.fix_iterations - record.fix_recovery_baseline >= fix_iteration_cap
        ):
            await self._escalate_fix_iteration_cap(current_pr)
            return
        prior = self.state.active_invocation
        if prior is not None and prior.phase == "fix" and prior.run_id is not None:
            record = self._current_run_record
            if (
                record is None or record.run_id != prior.run_id
                or record.task_id != getattr(self.state.current_task, "pr_id", None)
                or record.ended_at is not None
            ):
                self.log_event(
                    "[FIX] Persisted FIX invocation count unavailable; "
                    "deferring dispatch for operator recovery."
                )
                self.state.state = PipelineState.WATCH
                return
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
        idle_flag: dict[str, bool | float] = {"timed_out": False}
        external_state_flag: dict[str, str | None] = {"state": None}

        plugin_run_kwargs = plugin.build_run_kwargs(
            daemon_config=self.app_config.daemon,
            breach_dir=breach_dir,
            breach_run_id=breach_run_id,
        )
        fix_kwargs: dict[str, object] = {
            **plugin_run_kwargs,
            "on_process_start": self._track_current_coder_process,
            "on_supervised_process_start": (
                self._track_current_coder_supervised_process
            ),
        }
        if self.state.current_task is not None:
            fix_kwargs["pr_id"] = self.state.current_task.pr_id
            fix_kwargs["task_file"] = self.state.current_task.task_file
        if self.state.current_pr is not None:
            admitted_feedback = (
                review_feedback if review_head and review_head == self.state.current_pr.head_sha else None
            )
            if admitted_feedback and self.state.current_pr.ci_status == CIStatus.FAILURE:
                metadata = await asyncio.to_thread(
                    gh_prs.get_pr_metadata, self.owner_repo, self.state.current_pr.number
                )
                if metadata.get("head_sha") != review_head:
                    admitted_feedback = None
                    review_limitation = "PR HEAD changed before review dispatch"
            extra_context = await self._build_fix_feedback_context(
                self.state.current_pr,
                reviewer_policy=fix_feedback_reviewer_policy,
                review_feedback=admitted_feedback,
                review_limitation=review_limitation,
            )
            if extra_context is not None:
                fix_kwargs["extra_context"] = extra_context
            if review_feedback and self.state.current_pr.ci_status != CIStatus.FAILURE:
                metadata = await asyncio.to_thread(
                    gh_prs.get_pr_metadata, self.owner_repo, self.state.current_pr.number
                )
                if (
                    metadata.get("head_sha") != review_head
                    or self.state.current_pr is not current_pr
                    or self.state.current_pr.head_sha != review_head
                ):
                    self.log_event("[FIX] PR HEAD changed before review dispatch; returning to WATCH.")
                    self.state.state = PipelineState.WATCH
                    return
        if not self._reserve_coder_credentials(
            coder_name,
            invocation_kwargs=fix_kwargs,
        ):
            self.log_event(
                "[FIX] Coder invocation deferred while credential login owns "
                "its credential location."
            )
            return
        size_fix_marker: str | None = None
        if current_pr is not None:
            if any(is_fixing_size_label(label) for label in current_pr.quarantine_labels):
                self.log_event("[FIX] Prior size FIX outcome is unresolved; deferring dispatch.")
                self._release_coder_credentials()
                self.state.state = PipelineState.WATCH
                return
            if any(is_continued_size_label(label) for label in current_pr.quarantine_labels):
                try:
                    size_fix_marker = fixing_size_label(current_pr.head_sha)
                    gh_runner.run_gh(
                        ["label", "create", size_fix_marker, "--color", "B60205",
                         "--description", "Size continuation FIX in progress"],
                        repo=self.owner_repo,
                    )
                except Exception as exc:
                    if not fix_escalation._is_label_already_exists_error(exc):
                        self.log_event(f"[FIX] Size FIX marker creation deferred: {exc}")
                        self._release_coder_credentials()
                        self.state.state = PipelineState.WATCH
                        return
                try:
                    gh_runner.run_gh(
                        ["pr", "edit", str(current_pr.number), "--add-label", size_fix_marker],
                        repo=self.owner_repo,
                    )
                except Exception as exc:
                    self.log_event(f"[FIX] Size FIX marker application deferred: {exc}")
                    self._release_coder_credentials()
                    self.state.state = PipelineState.WATCH
                    return
                current_pr.quarantine_labels.add(size_fix_marker)
        heartbeat: asyncio.Task[None] | None = None
        run_record = self._current_run_record
        previous_profile_id = run_record.profile_id if run_record is not None else None
        previous_active_invocation = self.state.active_invocation
        previous_fix_iterations = (
            run_record.fix_iterations if run_record is not None else None
        )
        captured_snapshot = None
        try:
            if run_record is not None:
                run_record.profile_id = (
                    f"{coder_name}:{plugin_run_kwargs['model']}:container"
                )
            self.log_event(
                f"[{coder_name}] entering FIX.",
                tier="state",
                kind="transition",
            )
            fix_iteration: int | None = None
            if self._current_run_record is not None:
                self._current_run_record.fix_iterations += 1
                fix_iteration = self._current_run_record.fix_iterations
            elif self.state.current_pr is not None:
                fix_iteration = self.state.current_pr.fix_iteration_count + 1
            captured_snapshot = self._capture_invocation_snapshot(
                coder_name,
                fix_kwargs,
                phase="fix",
                fix_iteration=fix_iteration,
            )
            if run_record is not None:
                await self._checkpoint_current_run_record()
            await self.publish_state()
            heartbeat = asyncio.create_task(self._publish_while_waiting("FIX"))
            self._coder_invocation_active = True
            claude_task: asyncio.Task[tuple[int, str, str]] = (
                asyncio.create_task(
                    plugin.fix_review(
                        self.repo_path,
                        **fix_kwargs,
                    )
                )
            )
            idle_flag["started_at"] = fix_supervision.time.monotonic()
        except BaseException:
            if captured_snapshot is not None:
                self.state.active_invocation = previous_active_invocation
                if (
                    run_record is not None
                    and run_record.invocations
                    and run_record.invocations[-1] == captured_snapshot
                ):
                    run_record.invocations.pop()
                    assert previous_fix_iterations is not None
                    run_record.fix_iterations = previous_fix_iterations
                    run_record.profile_id = previous_profile_id
                rollback_writes = [self.publish_state()]
                if run_record is not None:
                    rollback_writes.append(self._checkpoint_current_run_record())
                await asyncio.gather(*rollback_writes, return_exceptions=True)
            if heartbeat is not None:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
            self._coder_invocation_active = False
            self._release_coder_credentials()
            raise
        assert heartbeat is not None
        stop_monitor = asyncio.create_task(self._monitor_stop_request(claude_task))
        idle_monitor = asyncio.create_task(
            self._monitor_fix_idle(pr_number, idle_limit, claude_task, idle_flag)
        )
        breach_monitor: asyncio.Task[None] | None = None
        if plugin.supports_breach_lifecycle:
            breach_monitor = asyncio.create_task(
                self._monitor_inflight_breach(
                    breach_dir,
                    breach_run_id,
                    plugin.name,
                    claude_task,
                    breach_flag,
                )
            )
        external_state_monitor = self._run_coder_with_polling(
            pr_number, claude_task, external_state_flag,
        )
        stop_cancelled = False
        breach_cancelled = False
        pending_cancellation: asyncio.CancelledError | None = None
        coder_result: tuple[int, str, str] | None = None
        cleanup_confirmed = False
        try:
            coder_result = await claude_task
            code, stdout, stderr = coder_result
        except asyncio.CancelledError as exc:
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
                breach_cancelled = True
                code, stdout, stderr = 1, "", ""
            elif not idle_flag["timed_out"]:
                pending_cancellation = exc
                code, stdout, stderr = 1, "", ""
            else:
                code, stdout, stderr = 1, "", ""
        finally:
            stop_monitor.cancel()
            if breach_monitor is not None:
                breach_monitor.cancel()
            idle_monitor.cancel()
            if external_state_monitor is not None:
                external_state_monitor.cancel()
            heartbeat.cancel()
            monitors = [stop_monitor, idle_monitor, heartbeat]
            if breach_monitor is not None:
                monitors.append(breach_monitor)
            if external_state_monitor is not None:
                monitors.append(external_state_monitor)
            await asyncio.gather(*monitors, return_exceptions=True)
            self._coder_invocation_active = False
            if coder_result is not None:
                self._record_unconfirmed_launch_cleanup(coder_result)
            if plugin.supports_breach_lifecycle:
                self._check_late_breach(
                    breach_dir,
                    breach_run_id,
                    plugin.name,
                    breach_flag,
                )
                self._cleanup_breach_marker(breach_dir, breach_run_id)
            cleanup_confirmed = await self._confirm_current_coder_cleanup(
                "FIX completion"
            )
        if pending_cancellation is not None:
            raise pending_cancellation
        if not cleanup_confirmed:
            return
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
                    trigger_context = (
                        "after breach-cancel fix push; manual review "
                        "trigger required"
                        if breach_cancelled
                        else "after late-breach fix push; manual review "
                        "trigger required"
                    )
                    if not await fix_codex_trigger.maybe_post_codex_review_after_push(
                        self,
                        self.state.current_pr.number,
                        trigger_context,
                    ):
                        return
            self.state.state = PipelineState.PAUSED
            await self._clear_error_message_on_recovery(
                log_prefix="[FIX]",
                reason=(
                    "in-flight rate limit breach"
                    if breach_cancelled
                    else "late in-flight rate limit breach"
                ),
            )
            if breach_cancelled:
                self.log_event(
                    f"[FIX] FIX aborted: in-flight rate limit breach, "
                    f"paused until {self.state.rate_limited_until}."
                )
            else:
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

        async def finish_size_fix_marker(head_after: str) -> None:
            if size_fix_marker is None or current_pr is None or self.state.current_task is None:
                return
            committed = await self._commit_task_status_change(
                self.state.current_task, "TODO", "Size FIX completed; continuing published PR",
            )
            if not committed:
                self.log_event("[FIX] Size FIX task status update deferred; retaining recovery marker.")
                return
            current_pr.head_sha = head_after
            complete_size_fix_marker(self, current_pr)

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
                self.state.current_pr.record_observed_head(head_after)
                self.state.current_pr.head_sha = head_after
                self._last_push_at = self._canonical_push_timestamp(
                    self.state.current_pr.number
                )
                self._last_push_at_pr_number = self.state.current_pr.number
                iteration = fix_iteration_policy.increment(self.state.current_pr)
                no_push_policy.reset(self.state.current_pr)
                self.state.current_pr.last_activity = now_time
            else:
                self._last_push_at = None
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
                f"FIX idle timeout: no new push for {idle_limit}s of active FIX time",
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
                        "pr_number": pr_number,
                        "branch": self.state.current_pr.branch if self.state.current_pr else "",
                        "run_id": run_record.run_id if run_record else None,
                    },
                ),
                commit_task_status=True,
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
                    f"tier={violation.tier} "
                    f"{violation.category}: {violation.excerpt}",
                    tier="guardrail",
                    kind=violation.category,
                )
            if await pause_for_stop_after_bookkeeping():
                return
            if self.state.current_pr is not None:
                pr_number = self.state.current_pr.number
                self.state.quarantined_prs.add(pr_number)
                task_id = (
                    self.state.current_pr.pr_id
                    or (self.state.current_task.pr_id if self.state.current_task else "")
                )
                await self._suppress_task(
                    task_id,
                    SuppressionReason.GUARDRAIL,
                    {"pr_number": pr_number, "reason_text": cause},
                )
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
                commit_task_status=True,
            )
            await self._refresh_guardrail_head_after_fix()
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
                self._last_push_at = None
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
            await finish_size_fix_marker(current_pr.head_sha if remote_no_push and current_pr else head_after)
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
        await finish_size_fix_marker(head_after)
        self.state.state = PipelineState.WATCH
