"""CODING state handler.

Mixin methods:
    handle_coding — run PLANNED PR via the active coder CLI
"""

from __future__ import annotations

import asyncio

from src import github_client
from src.models import PipelineState

"""In-memory counter of consecutive coder-exit-without-PR failures per task.
Resets on daemon restart; upgrade to Redis persistence in later PR."""
_NO_PR_RETRY_COUNTS: dict[str, int] = {}


def _clear_stale_no_pr_retry_count(runner: object, current_pr_id: str | None) -> None:
    """Drop stale no-PR retry state when CODING starts for another task."""
    if current_pr_id is None:
        return
    last_failed_pr_id = getattr(runner, "_last_no_pr_failed_pr_id", None)
    if last_failed_pr_id == current_pr_id:
        return
    _NO_PR_RETRY_COUNTS.pop(last_failed_pr_id or current_pr_id, None)
    if last_failed_pr_id is not None:
        runner._last_no_pr_failed_pr_id = None


class CodingMixin:
    """Run ``PLANNED PR`` via the active coder CLI and hand off to WATCH."""

    async def handle_coding(self) -> None:
        """Run ``PLANNED PR`` via the active coder CLI and hand off to WATCH.

        The coder owns the full git workflow per AGENTS.md: branch creation,
        commit, push, and PR creation. The daemon must not pre-create the
        task branch — doing so conflicts with AGENTS.md step 4 ("create
        branch from origin/main"). After the CLI returns 0 we poll GitHub
        for the PR; because the list API is eventually consistent, we
        retry a few times before surfacing an ERROR.
        """
        self._stop_requested = False
        current_pr_id = self.state.current_task.pr_id if self.state.current_task is not None else None
        _clear_stale_no_pr_retry_count(self, current_pr_id)
        await self._refresh_auth_status_cache()
        coder_name, plugin = self._get_coder()
        model = (
            self.app_config.daemon.codex_model
            if coder_name == "codex"
            else self.app_config.daemon.claude_model
        )
        self._start_current_run_record(coder_name, model)
        if not await self._check_rate_limit(proactive_coder=coder_name):
            await self._save_current_run_record("rate_limit")
            return

        self.log_event(f"[{coder_name}] Starting PLANNED PR")

        target_branch = (
            self.state.current_task.branch if self.state.current_task else None
        )
        if not target_branch:
            self.state.state = PipelineState.ERROR
            self.state.error_message = (
                "Current task has no branch; cannot identify PR"
            )
            await self._save_current_run_record("error")
            self.log_event(self.state.error_message)
            return

        breach_dir, breach_run_id = self._breach_env()
        breach_flag: dict[str, bool] = {"breached": False}

        heartbeat = asyncio.create_task(self._publish_while_waiting("CODING"))
        coder_kwargs: dict[str, object] = {
            "model": model,
            "timeout": self.app_config.daemon.planned_pr_timeout_sec,
            "on_process_start": self._track_current_coder_process,
        }
        if coder_name == "claude":
            coder_kwargs.update(
                breach_dir=breach_dir,
                breach_run_id=breach_run_id,
                session_threshold=self.app_config.daemon.rate_limit_session_pause_percent,
                weekly_threshold=self.app_config.daemon.rate_limit_weekly_pause_percent,
            )
        cli_task: asyncio.Task[tuple[int, str, str]] = asyncio.create_task(
            plugin.run_planned_pr(
                self.repo_path,
                **coder_kwargs,
            )
        )
        breach_monitor: asyncio.Task[None] | None = None
        if coder_name == "claude":
            breach_monitor = asyncio.create_task(
                self._monitor_inflight_breach(
                    breach_dir, breach_run_id, cli_task, breach_flag,
                )
            )
        stop_monitor = asyncio.create_task(self._monitor_stop_request(cli_task))
        try:
            code, stdout, stderr = await cli_task
        except asyncio.CancelledError:
            if self._stop_requested:
                self.state.state = PipelineState.PAUSED
                self.state.error_message = None
                await self._save_current_run_record("error")
                self.log_event("CODING aborted: user stop requested")
                return
            if not breach_flag["breached"]:
                raise
            # Record the PR if Claude already created one before cancellation,
            # so it enters WATCH/auto-merge flow after pause expiry.
            # Retry up to 3 times — PR list visibility is eventually consistent.
            if target_branch:
                for _attempt in range(3):
                    try:
                        prs = github_client.get_open_prs(
                            self.owner_repo,
                            allow_merge_without_checks=self.repo_config.allow_merge_without_checks,
                        )
                        match = next(
                            (pr for pr in prs if pr.branch == target_branch),
                            None,
                        )
                        if match:
                            self.state.current_pr = match
                            self.log_event(
                                f"Recorded PR #{match.number} before breach-cancel pause"
                            )
                            break
                    except Exception:
                        pass  # best-effort; the pause is still correct
                    if _attempt < 2:
                        await asyncio.sleep(5)
            self.state.state = PipelineState.PAUSED
            self.state.error_message = None
            await self._save_current_run_record("rate_limit")
            self.log_event(
                f"CODING aborted: in-flight rate limit breach, "
                f"paused until {self.state.rate_limited_until}"
            )
            return
        finally:
            stop_monitor.cancel()
            if breach_monitor is not None:
                breach_monitor.cancel()
            heartbeat.cancel()
            self._current_coder_process = None
            if coder_name == "claude":
                self._check_late_breach(breach_dir, breach_run_id, breach_flag)
                self._cleanup_breach_marker(breach_dir, breach_run_id)
        if breach_flag["breached"]:
            # Record the PR if the coder already created one, so it is not
            # orphaned while the runner is paused.
            # Retry up to 3 times — PR list visibility is eventually consistent.
            if target_branch:
                for _attempt in range(3):
                    try:
                        prs = github_client.get_open_prs(
                            self.owner_repo,
                            allow_merge_without_checks=self.repo_config.allow_merge_without_checks,
                        )
                        match = next(
                            (pr for pr in prs if pr.branch == target_branch),
                            None,
                        )
                        if match:
                            self.state.current_pr = match
                            self.log_event(
                                f"Recorded PR #{match.number} before late-breach pause"
                            )
                            break
                    except Exception:
                        pass  # best-effort; the pause is still correct
                    if _attempt < 2:
                        await asyncio.sleep(5)
            self.state.state = PipelineState.PAUSED
            self.state.error_message = None
            await self._save_current_run_record("rate_limit")
            self.log_event(
                f"CODING paused: late in-flight rate limit breach, "
                f"paused until {self.state.rate_limited_until}"
            )
            return

        async def pause_for_stop_if_requested() -> bool:
            if self._stop_requested:
                requested = True
            else:
                requested = await self._pop_stop_request()
                if requested:
                    self._stop_requested = True
                    self.state.user_paused = True
                    self.log_event(
                        "User stop requested after coder exit; honoring persisted stop"
                    )
            if not requested:
                return False
            self.state.state = PipelineState.PAUSED
            self.state.error_message = None
            await self._save_current_run_record("error")
            self.log_event("CODING aborted: user stop requested")
            return True

        await self._save_cli_log(stdout, stderr, f"PLANNED PR output [{coder_name}]")
        if not await pause_for_stop_if_requested():
            await self._refresh_user_paused_from_redis()
            if self.state.user_paused:
                self.log_event(
                    "User pause persisted during coder exit; finishing current run "
                    "before honoring pause"
                )
        if await pause_for_stop_if_requested():
            return
        if code != 0:
            self._detect_rate_limit(stderr, coder_name=coder_name)
            if self.state.rate_limited_until is not None:
                self.state.state = PipelineState.PAUSED
                self.state.error_message = None
                await self._save_current_run_record("rate_limit")
                self.log_event(
                    f"Rate limit pause active until "
                    f"{self.state.rate_limited_until.isoformat()}"
                )
                return
            self.state.state = PipelineState.ERROR
            self.state.error_message = stderr.strip() or f"{coder_name} exit {code}"
            await self._save_current_run_record("error")
            self.log_event(f"[{coder_name}] CLI failed: {self.state.error_message}")
            return

        candidate = None
        for attempt in range(3):
            if await pause_for_stop_if_requested():
                return
            try:
                prs = github_client.get_open_prs(
                    self.owner_repo,
                    allow_merge_without_checks=self.repo_config.allow_merge_without_checks,
                )
            except Exception as exc:
                self.state.state = PipelineState.ERROR
                self.state.error_message = f"get_open_prs failed: {exc}"
                await self._save_current_run_record("error")
                self.log_event(str(exc))
                return
            candidate = next(
                (pr for pr in prs if pr.branch == target_branch), None
            )
            if candidate is not None:
                break
            if attempt < 2:
                self.log_event(
                    f"PR not found for {target_branch!r}, "
                    f"retrying in 5s ({attempt + 1}/3)"
                )
                await asyncio.sleep(5)

        if await pause_for_stop_if_requested():
            return
        if candidate is None:
            no_pr_message = (
                f"[{coder_name}] coder succeeded but no PR found for branch "
                f"{target_branch!r}"
            )
            retry_count = 0
            if current_pr_id is not None:
                retry_count = _NO_PR_RETRY_COUNTS.get(current_pr_id, 0) + 1
                _NO_PR_RETRY_COUNTS[current_pr_id] = retry_count
                self._last_no_pr_failed_pr_id = current_pr_id
            self.log_event(no_pr_message)
            if retry_count >= 2 and current_pr_id is not None:
                blocked_message = (
                    f"Task {current_pr_id} blocked: coder failed to create PR "
                    "2 times in a row. Manual intervention required."
                )
                self.state.state = PipelineState.HUNG
                self.state.error_message = blocked_message
                await self._save_current_run_record("error")
                self.log_event(blocked_message)
                return
            self.state.state = PipelineState.ERROR
            self.state.error_message = no_pr_message
            await self._save_current_run_record("error")
            return

        self.state.current_pr = candidate
        self.state.state = PipelineState.WATCH
        self._rehydrate_last_push_at(candidate)
        await self._save_current_run_record("coding_complete")
        self.log_event(f"Opened PR #{candidate.number} -> WATCH")
        self._post_codex_review(candidate.number)
