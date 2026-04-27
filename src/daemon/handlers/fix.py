"""FIX state handler with idle timeout and in-flight breach monitoring."""

from __future__ import annotations

import asyncio
import subprocess
import time
from datetime import datetime, timezone

from src import github_client
from src.daemon import git_ops
from src.daemon.handlers.breach import BreachMixin
from src.daemon.recovery_policy import BoundedRecoveryPolicy
from src.models import PipelineState, PRInfo
from src.retry import retry_transient


class FixMixin(BreachMixin):
    """FIX REVIEW handler with idle timeout and breach monitoring."""

    async def _monitor_fix_idle(
        self,
        pr_number: int,
        idle_limit: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        idle_flag: dict[str, bool],
    ) -> None:
        """Cancel *target* if no new push is detected within *idle_limit* seconds."""
        primed = False
        try:
            await asyncio.to_thread(
                github_client.get_branch_last_push_time,
                self.owner_repo, pr_number,
            )
            primed = True
        except github_client.GitHubPollError:
            pass

        poll_interval = min(60, idle_limit)
        now = time.monotonic()
        head_age = await asyncio.to_thread(
            github_client.get_last_push_age_seconds,
            self.owner_repo, pr_number,
        )
        if head_age is not None:
            backdate = min(head_age, idle_limit - poll_interval)
            last_known_push = now - max(0.0, backdate)
        else:
            last_known_push = now
        while True:
            await asyncio.sleep(poll_interval)
            try:
                latest_push_at = await asyncio.to_thread(
                    github_client.get_branch_last_push_time,
                    self.owner_repo, pr_number,
                )
                if not primed:
                    primed = True
                    if latest_push_at is not None:
                        last_known_push = time.monotonic()
            except github_client.GitHubPollError:
                self.log_event("FIX: GitHub API poll failed, preserving deadline")
                latest_push_at = None
            if latest_push_at is not None and latest_push_at > last_known_push:
                last_known_push = latest_push_at
                self.log_event(
                    f"FIX: [{self.state.coder or 'coder'}] pushed, resetting idle timer"
                )
            elapsed = time.monotonic() - last_known_push
            if elapsed >= idle_limit:
                self.log_event(
                    f"FIX: idle timeout ({idle_limit}s since last push), killing"
                )
                idle_flag["timed_out"] = True
                target.cancel()
                return

    async def _escalate_fix_no_push_deadlock(self, current_pr: PRInfo) -> None:
        """Park the PR in HUNG after consecutive no-push FIX cycles.

        Logs the deadlock event with the counter value, posts an
        explanatory comment on the PR, applies the ``escalated`` label so
        ``get_open_prs`` rehydrates ``is_escalated`` after a daemon
        restart (Codex P2 on PR #222), transitions to HUNG, and resets
        the no-push counter so a future cycle out of HUNG starts fresh.
        Comment- and label-post failures are logged but never block the
        HUNG transition: HUNG is the safe parking state regardless, and
        the in-memory ``is_escalated`` flag still holds for the current
        run.

        Marks ``current_pr.is_escalated`` so ``handle_hung`` keeps the
        runner parked even when ``hung_fallback_codex_review`` is on.
        Without this, the default fallback would post ``@codex review``
        and bounce back to WATCH on the very next tick, immediately
        re-entering the FIX loop the deadlock counter was meant to stop.
        """
        count = current_pr.no_push_fix_count
        pr_number = current_pr.number
        message = (
            f"FIX deadlock: {count} consecutive no-push FIX cycles on PR "
            f"#{pr_number}. Coder unable to identify actionable fix. "
            "Manual review required."
        )
        try:
            github_client.post_comment(self.owner_repo, pr_number, message)
        except Exception as exc:
            self.log_event(
                f"Warning: failed to post FIX deadlock comment on PR "
                f"#{pr_number}: {exc}"
            )
        try:
            github_client.run_gh(
                [
                    "label",
                    "create",
                    "escalated",
                    "--color",
                    "B60205",
                    "--description",
                    "Daemon escalated, manual review required",
                ],
                repo=self.owner_repo,
            )
        except Exception as exc:
            self.log_event(f"FIX no-push label create skipped: {exc}")
        try:
            github_client.run_gh(
                ["pr", "edit", str(pr_number), "--add-label", "escalated"],
                repo=self.owner_repo,
            )
        except Exception as exc:
            self.log_event(
                f"Warning: failed to apply escalated label to PR "
                f"#{pr_number}: {exc}"
            )
        current_pr.is_escalated = True
        current_pr.no_push_fix_count = 0
        self.state.state = PipelineState.HUNG
        self.state.error_message = None
        self.log_event(message)
        await self.publish_state()

    async def _escalate_fix_iteration_cap(self, current_pr: PRInfo) -> None:
        """Escalate the PR after the FIX iteration cap is reached.

        Posts a @-mention comment, ensures the ``escalated`` label
        exists, applies it to the PR, marks ``current_pr.is_escalated``
        and transitions the runner to IDLE so subsequent cycles do
        not redrive FIX. Sets ``state.ERROR`` if the GitHub mutation
        fails — see callers for the surrounding control flow.
        """
        count = current_pr.fix_iteration_count
        fix_iteration_cap = self.app_config.daemon.fix_iteration_cap
        pr_number = current_pr.number
        comment = (
            "@AlexBomber12 FIX iteration cap reached "
            f"({count}/{fix_iteration_cap}). Escalating for manual review."
        )
        try:
            github_client.post_comment(self.owner_repo, pr_number, comment)
        except Exception as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"post_comment failed: {exc}"
            self.log_event(self.state.error_message)
            return
        try:
            github_client.run_gh(
                [
                    "label",
                    "create",
                    "escalated",
                    "--color",
                    "B60205",
                    "--description",
                    "Daemon escalated, manual review required",
                ],
                repo=self.owner_repo,
            )
        except Exception as exc:
            self.log_event(f"FIX cap label create skipped: {exc}")
        try:
            github_client.run_gh(
                ["pr", "edit", str(pr_number), "--add-label", "escalated"],
                repo=self.owner_repo,
            )
        except Exception as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"pr edit failed: {exc}"
            self.log_event(self.state.error_message)
            return
        current_pr.is_escalated = True
        self.state.error_message = None
        self.state.state = PipelineState.IDLE
        self.log_event(
            f"FIX cap reached ({count}/{fix_iteration_cap}) on PR "
            f"#{pr_number}: escalated, moving to IDLE."
        )
        await self.publish_state()

    async def handle_fix(self) -> None:
        """Run ``FIX REVIEW`` via the active coder CLI and return to WATCH."""
        self._stop_requested = False
        await self._refresh_auth_status_cache()
        coder_name, plugin = self._get_coder(allow_exploration=False)
        if not await self._check_rate_limit(proactive_coder=coder_name):
            return

        if (
            self.state.current_pr is not None
            and self.state.current_pr.is_cross_repository
        ):
            self.log_event(
                f"Skipping FIX for cross-repo PR #{self.state.current_pr.number}"
            )
            self.state.state = PipelineState.WATCH
            return

        self.state.state = PipelineState.FIX
        current_pr = self.state.current_pr
        fix_iteration_cap = self.app_config.daemon.fix_iteration_cap
        if current_pr is not None and current_pr.is_escalated:
            self.state.error_message = None
            self.state.state = PipelineState.IDLE
            self.log_event(
                f"FIX blocked for escalated PR #{current_pr.number}, moving to IDLE."
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
        self.log_event(f"[{coder_name}] entering FIX")
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
                self.state.state = PipelineState.ERROR
                self.state.error_message = (
                    f"git refresh {branch} failed: {stderr.strip() or exc}"
                )
                self.log_event(self.state.error_message)
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

        model = (
            self.app_config.daemon.codex_model
            if coder_name == "codex"
            else self.app_config.daemon.claude_model
        )

        heartbeat = asyncio.create_task(self._publish_while_waiting("FIX"))
        fix_kwargs: dict[str, object] = {
            "model": model,
            "on_process_start": self._track_current_coder_process,
        }
        if coder_name == "claude":
            fix_kwargs.update(
                breach_dir=breach_dir,
                breach_run_id=breach_run_id,
                session_threshold=self.app_config.daemon.rate_limit_session_pause_percent,
                weekly_threshold=self.app_config.daemon.rate_limit_weekly_pause_percent,
            )
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
        if coder_name == "claude":
            breach_monitor = asyncio.create_task(
                self._monitor_inflight_breach(
                    breach_dir, breach_run_id, claude_task, breach_flag,
                )
            )
        stop_cancelled = False
        try:
            code, stdout, stderr = await claude_task
        except asyncio.CancelledError:
            if self._stop_requested:
                stop_cancelled = True
                code, stdout, stderr = 1, "", ""
            elif breach_flag["breached"]:
                if self.state.current_pr is not None:
                    self._rehydrate_last_push_at(self.state.current_pr)
                    try:
                        head_now = git_ops._git(
                            self.repo_path, "rev-parse", "HEAD"
                        ).stdout.strip()
                    except Exception:
                        head_now = ""
                    if head_before and head_now and head_before != head_now:
                        if not self._post_codex_review(
                            self.state.current_pr.number
                        ):
                            self.state.state = PipelineState.ERROR
                            self.state.error_message = (
                                f"Failed to post @codex review on PR "
                                f"#{self.state.current_pr.number} after "
                                "breach-cancel fix push; manual review "
                                "trigger required"
                            )
                            self.log_event(self.state.error_message)
                            return
                self.state.state = PipelineState.PAUSED
                self.state.error_message = None
                self.log_event(
                    f"FIX aborted: in-flight rate limit breach, "
                    f"paused until {self.state.rate_limited_until}"
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
            heartbeat.cancel()
            self._current_coder_process = None
            if coder_name == "claude":
                self._check_late_breach(breach_dir, breach_run_id, breach_flag)
                self._cleanup_breach_marker(breach_dir, breach_run_id)
        if breach_flag["breached"]:
            if self.state.current_pr is not None:
                self._rehydrate_last_push_at(self.state.current_pr)
                try:
                    head_now = git_ops._git(
                        self.repo_path, "rev-parse", "HEAD"
                    ).stdout.strip()
                except Exception:
                    head_now = ""
                if head_before and head_now and head_before != head_now:
                    if not self._post_codex_review(
                        self.state.current_pr.number
                    ):
                        self.state.state = PipelineState.ERROR
                        self.state.error_message = (
                            f"Failed to post @codex review on PR "
                            f"#{self.state.current_pr.number} after "
                            "late-breach fix push; manual review "
                            "trigger required"
                        )
                        self.log_event(self.state.error_message)
                        return
            self.state.state = PipelineState.PAUSED
            self.state.error_message = None
            self.log_event(
                f"FIX paused: late in-flight rate limit breach, "
                f"paused until {self.state.rate_limited_until}"
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
                "User stop requested after FIX exit; deferring pause "
                "until FIX bookkeeping completes"
            )
            return True

        async def pause_for_stop_after_bookkeeping() -> bool:
            if not stop_requested_after_exit:
                return False
            self.state.state = PipelineState.PAUSED
            self.state.error_message = None
            self.log_event("FIX aborted: user stop requested")
            return True

        def read_head_after_fix() -> str | None:
            try:
                return git_ops._git(
                    self.repo_path, "rev-parse", "HEAD"
                ).stdout.strip()
            except (
                subprocess.CalledProcessError,
                subprocess.TimeoutExpired,
                OSError,
            ) as exc:
                self.state.state = PipelineState.ERROR
                self.state.error_message = f"rev-parse after fix failed: {exc}"
                self.log_event(self.state.error_message)
                return None

        def remote_branch_contains_head(branch: str, head_after: str) -> bool:
            try:
                git_ops._git(
                    self.repo_path,
                    "fetch",
                    "origin",
                    f"+refs/heads/{branch}:refs/remotes/origin/{branch}",
                    timeout=60,
                )
            except (
                subprocess.CalledProcessError,
                subprocess.TimeoutExpired,
                OSError,
            ) as exc:
                self.log_event(f"fetch {branch} failed after FIX stop: {exc}")
                return False
            try:
                remote_head = git_ops._git(
                    self.repo_path,
                    "rev-parse",
                    f"origin/{branch}",
                ).stdout.strip()
            except (
                subprocess.CalledProcessError,
                subprocess.TimeoutExpired,
                OSError,
            ) as exc:
                self.log_event(
                    f"rev-parse origin/{branch} failed after FIX stop: {exc}"
                )
                return False
            if remote_head == head_after:
                return True
            try:
                is_ancestor = git_ops._git(
                    self.repo_path,
                    "merge-base",
                    "--is-ancestor",
                    head_after,
                    remote_head,
                    check=False,
                )
            except (subprocess.TimeoutExpired, OSError) as exc:
                self.log_event(
                    f"merge-base ancestry check failed after FIX stop: {exc}"
                )
                return False
            return is_ancestor.returncode == 0

        def record_fix_push(head_after: str, failure_detail: str) -> bool:
            if head_before and head_before == head_after:
                return True

            push_time = datetime.now(timezone.utc)
            self._last_push_at = push_time
            if self.state.current_pr is not None:
                self._last_push_at_pr_number = self.state.current_pr.number
                self.state.current_pr.push_count += 1
                iteration = fix_iteration_policy.increment(self.state.current_pr)
                no_push_policy.reset(self.state.current_pr)
                self.state.current_pr.last_activity = push_time
            else:
                iteration = 0

            self.log_event(f"Fix pushed, iteration #{iteration}")
            if (
                self.state.current_pr is not None
                and not self._post_codex_review(self.state.current_pr.number)
            ):
                self.state.state = PipelineState.ERROR
                self.state.error_message = (
                    f"Failed to post @codex review on PR "
                    f"#{self.state.current_pr.number} {failure_detail}"
                )
                return False
            return True

        await capture_stop_requested_after_exit()
        if idle_flag["timed_out"]:
            self.state.state = PipelineState.ERROR
            self.state.error_message = (
                f"FIX idle timeout: no push for {idle_limit}s"
            )
            self.log_event(self.state.error_message)
            await self._save_cli_log("", "", "FIX idle timeout")
            if await pause_for_stop_after_bookkeeping():
                return
            return
        await self._save_cli_log(stdout, stderr, f"FIX REVIEW output [{coder_name}]")
        await capture_stop_requested_after_exit()
        if stop_cancelled:
            head_after = read_head_after_fix()
            if head_after is None:
                return
            branch = self.state.current_pr.branch if self.state.current_pr is not None else ""
            if branch and remote_branch_contains_head(branch, head_after):
                if not record_fix_push(
                    head_after,
                    "after stop-cancel fix push; manual review trigger "
                    "required to avoid fix/push loop",
                ):
                    return
            elif head_before and head_before != head_after:
                self.log_event(
                    "FIX stop-cancel left local HEAD outside the fetched remote branch; "
                    "skipping push bookkeeping and @codex review"
                )
            if await pause_for_stop_after_bookkeeping():
                return
            self.state.state = PipelineState.PAUSED  # pragma: no cover - defensive fallback
            self.state.error_message = None  # pragma: no cover - defensive fallback
            return  # pragma: no cover - defensive fallback
        if code != 0:
            self._detect_rate_limit(stderr, coder_name=coder_name)
            if self.state.rate_limited_until is not None:
                self.state.state = PipelineState.PAUSED
                self.state.error_message = None
                self.log_event(
                    f"Rate limit pause active until "
                    f"{self.state.rate_limited_until.isoformat()}"
                )
                return
            if await pause_for_stop_after_bookkeeping():
                return
            self.state.state = PipelineState.ERROR
            self.state.error_message = stderr.strip() or f"{coder_name} exit {code}"
            self.log_event(f"[{coder_name}] fix_review failed: {self.state.error_message}")
            return

        head_after = read_head_after_fix()
        if head_after is None:
            return

        if head_before and head_before == head_after:
            self._last_push_at = datetime.now(timezone.utc)
            self.log_event(
                "FIX REVIEW exited 0 but HEAD unchanged; "
                "no push, skipping @codex review"
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

        if not record_fix_push(
            head_after,
            "after fix push; manual review trigger required "
            "to avoid fix/push loop",
        ):
            return
        if await pause_for_stop_after_bookkeeping():
            return
        self.state.state = PipelineState.WATCH
