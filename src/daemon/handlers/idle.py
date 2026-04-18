"""IDLE state handler and PAUSED state handler.

Mixin methods:
    handle_idle   — pick next task and transition to CODING
    handle_paused — wait for rate-limit window, then resume
"""

from __future__ import annotations

import inspect
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from src import github_client
from src.models import PipelineState, TaskStatus
from src.queue_parser import QueueValidationError, get_next_task, parse_queue
from src.task_status import derive_queue_task_statuses, find_matching_open_pr


class IdleMixin:
    """Handle IDLE state: sync, pick next task, dispatch to CODING."""

    async def handle_idle(self) -> None:
        """Hard-sync to ``origin/{branch}``, pick the next task, hand off."""
        self._error_diagnose_count = 0
        if self.state.pending_queue_sync_branch is not None:
            if not self._resolve_pending_queue_sync():
                return

        try:
            self.sync_to_main()
        except (
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
            RuntimeError,
        ) as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"sync_to_main failed: {exc}"
            self.log_event(f"sync_to_main failed: {exc}")
            return

        upload_result = await self.process_pending_uploads()
        if upload_result is None:
            self.log_event("Pending upload failed; skipping task dispatch to retry next cycle")
            return
        if upload_result:
            try:
                self.sync_to_main()
            except (
                subprocess.CalledProcessError,
                subprocess.TimeoutExpired,
                RuntimeError,
            ) as exc:
                self.state.state = PipelineState.ERROR
                self.state.error_message = f"sync_to_main after upload failed: {exc}"
                self.log_event(f"sync_to_main after upload failed: {exc}")
                return

        queue_path = str(Path(self.repo_path) / "tasks" / "QUEUE.md")
        strict = self.app_config.daemon.strict_queue_validation
        try:
            tasks = parse_queue(queue_path, strict=strict)
        except QueueValidationError as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = str(exc)
            self.log_event(f"Queue validation failed: {exc}")
            return
        try:
            prs = github_client.get_open_prs(
                self.owner_repo,
                allow_merge_without_checks=self.repo_config.allow_merge_without_checks,
            )
        except Exception as exc:
            self.log_event(
                f"IDLE: open PR check failed: {exc}; deferring task dispatch"
            )
            self.state.current_pr = None
            return
        try:
            merged_prs = github_client.get_merged_prs(
                self.owner_repo,
                self.repo_config.branch,
            )
        except Exception as exc:
            self.log_event(
                f"IDLE: merged PR check failed: {exc}; deferring task dispatch"
            )
            self.state.current_pr = None
            return
        try:
            derive_args = (
                tasks,
                self.repo_path,
                self.repo_config.branch,
                prs,
            )
            if len(inspect.signature(derive_queue_task_statuses).parameters) >= 5:
                tasks = derive_queue_task_statuses(
                    *derive_args,
                    merged_prs,
                )
            else:
                tasks = derive_queue_task_statuses(*derive_args)
        except (
            OSError,
            RuntimeError,
            QueueValidationError,
            subprocess.TimeoutExpired,
        ) as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"Task status derivation failed: {exc}"
            self.log_event(f"Task status derivation failed: {exc}")
            return
        self.state.queue_done = sum(
            1 for t in tasks if t.status == TaskStatus.DONE
        )
        self.state.queue_total = len(tasks)
        task = get_next_task(tasks)
        if task is None:
            self.log_event("No tasks available")
            if prs:
                done_branches = {
                    t.branch for t in tasks
                    if t.status == TaskStatus.DONE and t.branch
                }
                match = next(
                    (pr for pr in prs if pr.branch in done_branches), None
                )
                self.state.current_pr = match or prs[0]
                self.log_event(
                    f"IDLE: {len(prs)} open PR(s) detected (manual work)"
                )
            else:
                self.state.current_pr = None
            return

        self.state.current_task = task
        task_branch = task.branch

        if task_branch:
            existing = find_matching_open_pr(
                task.pr_id,
                task_branch,
                prs,
            )

            if existing is not None:
                self.state.current_pr = existing
                self.state.state = PipelineState.WATCH
                self._rehydrate_last_push_at(existing)
                self.log_event(
                    f"Task {task.pr_id} has existing open PR #{existing.number} "
                    f"on {task_branch!r} -> WATCH (no duplicate CODING)"
                )
                await self.publish_state()
                return

        self.state.state = PipelineState.CODING
        self.log_event(f"Picked task {task.pr_id}: {task.title}")
        await self.publish_state()
        await self.handle_coding()

    async def handle_paused(self) -> None:
        """Wait for rate limit window to expire, then resume previous flow."""
        if self.state.rate_limited_until is None:
            self.log_event("PAUSED without rate_limited_until -> IDLE")
            self.state.state = PipelineState.IDLE
            return
        coder = self.repo_config.coder or self.app_config.daemon.coder
        pause_coder = self.state.rate_limit_reactive_coder or "claude"
        diagnosis_pause = (
            self.state.error_message is not None
            and pause_coder == "claude"
        )
        other_coder = (
            not diagnosis_pause
            and pause_coder != coder.value
        )
        clearable = other_coder
        if clearable:
            self.state.rate_limited_until = None
            self.state.rate_limit_reactive = False
            self.state.rate_limit_reactive_coder = None
            self._claude_usage_provider.invalidate_cache()
            self._codex_usage_provider.invalidate_cache()
            self._error_diagnose_count = 0
            label = f"{coder.value.capitalize()} active, clearing other-coder pause"
            if self.state.error_message:
                lowered = self.state.error_message.lower()
                is_rate_limit_msg = (
                    "rate limit" in lowered or re.search(r"\b429\b", lowered)
                )
                if is_rate_limit_msg:
                    self.state.error_message = None
                    self.log_event(f"{label}, cleared legacy rate-limit error")
                else:
                    self.state.state = PipelineState.ERROR
                    self.log_event(f"{label} -> ERROR (preserved context)")
                    return
            if (
                self.state.current_pr is not None
                and self.state.current_task is not None
                and self.state.current_pr.branch == self.state.current_task.branch
            ):
                self.state.state = PipelineState.WATCH
                self.log_event(f"{label} -> WATCH")
            else:
                self.state.state = PipelineState.IDLE
                self.log_event(f"{label} -> IDLE")
            return
        if datetime.now(timezone.utc) < self.state.rate_limited_until:
            remaining = (
                self.state.rate_limited_until - datetime.now(timezone.utc)
            ).total_seconds()
            self.log_event(f"Paused, resuming in {int(remaining)}s")
            return
        # Window expired: resume to appropriate state
        self.state.rate_limited_until = None
        self.state.rate_limit_reactive = False
        self.state.rate_limit_reactive_coder = None
        self._error_diagnose_count = 0
        if self.state.error_message:
            lowered = self.state.error_message.lower()
            is_rate_limit_msg = (
                "rate limit" in lowered or re.search(r"\b429\b", lowered)
            )
            if is_rate_limit_msg:
                self.state.error_message = None
                self.log_event(
                    "Rate limit expired, cleared legacy rate-limit error"
                )
            else:
                self.state.state = PipelineState.ERROR
                self.log_event(
                    "Rate limit expired, resuming -> ERROR (preserved context)"
                )
                return
        if (
            self.state.current_pr is not None
            and self.state.current_task is not None
            and self.state.current_pr.branch == self.state.current_task.branch
        ):
            self.state.state = PipelineState.WATCH
            self.log_event("Rate limit expired, resuming -> WATCH")
        else:
            self.state.state = PipelineState.IDLE
            self.log_event("Rate limit expired, resuming -> IDLE")
