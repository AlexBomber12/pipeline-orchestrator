"""FIX state handler with idle timeout and in-flight breach monitoring."""

from __future__ import annotations

import asyncio
import subprocess
import time
from datetime import datetime, timezone

from src import github_client
from src.daemon import git_ops
from src.daemon.handlers.breach import BreachMixin
from src.models import PipelineState
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
                self.log_event("FIX: Claude pushed, resetting idle timer")
            elapsed = time.monotonic() - last_known_push
            if elapsed >= idle_limit:
                self.log_event(
                    f"FIX: idle timeout ({idle_limit}s since last push), killing"
                )
                idle_flag["timed_out"] = True
                target.cancel()
                return

    async def handle_fix(self) -> None:
        """Run ``FIX REVIEW`` via the active coder CLI and return to WATCH."""
        coder_name, plugin = self._get_coder()
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
                        "fetch", "origin",
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
        try:
            code, stdout, stderr = await claude_task
        except asyncio.CancelledError:
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
            if not idle_flag["timed_out"]:
                raise
            code, stdout, stderr = 1, "", ""
        finally:
            if breach_monitor is not None:
                breach_monitor.cancel()
            idle_monitor.cancel()
            heartbeat.cancel()
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
        if idle_flag["timed_out"]:
            self.state.state = PipelineState.ERROR
            self.state.error_message = (
                f"FIX idle timeout: no push for {idle_limit}s"
            )
            self.log_event(self.state.error_message)
            await self._save_cli_log("", "", "FIX idle timeout")
            return
        await self._save_cli_log(stdout, stderr, f"FIX REVIEW output [{coder_name}]")
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
            self.state.state = PipelineState.ERROR
            self.state.error_message = stderr.strip() or f"{coder_name} exit {code}"
            self.log_event(f"[{coder_name}] fix_review failed: {self.state.error_message}")
            return

        head_after = ""  # PR-050: verify HEAD moved
        try:
            head_after = git_ops._git(
                self.repo_path, "rev-parse", "HEAD"
            ).stdout.strip()
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"rev-parse after fix failed: {exc}"
            self.log_event(self.state.error_message)
            return

        if head_before and head_before == head_after:
            self._last_push_at = datetime.now(timezone.utc)
            self.state.state = PipelineState.WATCH
            self.log_event(
                "FIX REVIEW exited 0 but HEAD unchanged; "
                "no push, skipping @codex review"
            )
            return

        push_time = datetime.now(timezone.utc)
        self._last_push_at = push_time
        if self.state.current_pr is not None:
            self._last_push_at_pr_number = self.state.current_pr.number
            self.state.current_pr.push_count += 1
            self.state.current_pr.last_activity = push_time
            iteration = self.state.current_pr.push_count
        else:
            iteration = 0

        self.state.state = PipelineState.WATCH
        self.log_event(f"Fix pushed, iteration #{iteration}")
        if (
            self.state.current_pr is not None
            and not self._post_codex_review(self.state.current_pr.number)
        ):
            self.state.state = PipelineState.ERROR
            self.state.error_message = (
                f"Failed to post @codex review on PR "
                f"#{self.state.current_pr.number} after fix push; "
                "manual review trigger required to avoid fix/push loop"
            )
