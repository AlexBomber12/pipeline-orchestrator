"""CODING state handler.

Mixin methods:
    handle_coding — run PLANNED PR via the active coder CLI
"""

from __future__ import annotations

import asyncio

from src import github_client
from src.models import PipelineState


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
        try:
            code, stdout, stderr = await cli_task
        except asyncio.CancelledError:
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
            if breach_monitor is not None:
                breach_monitor.cancel()
            heartbeat.cancel()
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
        await self._save_cli_log(stdout, stderr, f"PLANNED PR output [{coder_name}]")
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

        if candidate is None:
            self.state.state = PipelineState.ERROR
            self.state.error_message = (
                f"Claude CLI succeeded but no PR found for branch {target_branch!r}"
            )
            await self._save_current_run_record("error")
            self.log_event(self.state.error_message)
            return

        self.state.current_pr = candidate
        self.state.state = PipelineState.WATCH
        self._rehydrate_last_push_at(candidate)
        await self._save_current_run_record("coding_complete")
        self.log_event(f"Opened PR #{candidate.number} -> WATCH")
        self._post_codex_review(candidate.number)
