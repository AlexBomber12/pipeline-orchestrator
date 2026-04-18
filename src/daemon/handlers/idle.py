"""IDLE state handler and PAUSED state handler.

Mixin methods:
    handle_idle   — pick next task and transition to CODING
    handle_paused — wait for rate-limit window, then resume
"""

from __future__ import annotations

import inspect
import re
import subprocess
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from src import github_client
from src.dag import get_eligible_tasks
from src.models import PipelineState, QueueTask, TaskStatus
from src.queue_parser import (
    QueueValidationError,
    get_next_task,
    parse_queue,
    parse_task_header,
)
from src.task_status import (
    derive_queue_task_statuses,
    derive_task_status,
    find_matching_open_pr,
    get_merged_pr_ids,
)


class IdleMixin:
    """Handle IDLE state: sync, pick next task, dispatch to CODING."""

    @staticmethod
    def _is_missing_task_header_error(exc: QueueValidationError) -> bool:
        return bool(exc.issues) and all(
            "missing task header like '# PR-123: Title'" in issue
            for issue in exc.issues
        )

    @staticmethod
    def _is_legacy_unstructured_task_error(exc: QueueValidationError) -> bool:
        allowed_suffixes = {
            ": missing Branch",
            ": missing Type",
            ": missing Complexity",
            ": missing Depends on",
        }
        return bool(exc.issues) and all(
            any(issue.endswith(suffix) for suffix in allowed_suffixes)
            for issue in exc.issues
        )

    @staticmethod
    def _filter_dag_headers_with_available_dependencies(
        headers: list,
        skipped_legacy_pr_ids: set[str],
        task_dir: Path,
        merged_pr_ids: set[str],
    ) -> list:
        blocked_pr_ids: set[str] = set()
        structured_pr_ids = {header.pr_id for header in headers}

        changed = True
        while changed:
            changed = False
            available_pr_ids = structured_pr_ids - blocked_pr_ids
            for header in headers:
                if header.pr_id in blocked_pr_ids:
                    continue
                for dependency in header.depends_on:
                    if dependency in available_pr_ids:
                        continue
                    if dependency in merged_pr_ids:
                        continue
                    if dependency in blocked_pr_ids:
                        blocked_pr_ids.add(header.pr_id)
                        changed = True
                        break
                    if dependency in skipped_legacy_pr_ids:
                        blocked_pr_ids.add(header.pr_id)
                        changed = True
                        break
                    if not (task_dir / f"{dependency}.md").exists():
                        blocked_pr_ids.add(header.pr_id)
                        changed = True
                        break

        return [header for header in headers if header.pr_id not in blocked_pr_ids]

    async def _select_next_task_from_dag(self) -> QueueTask | None:
        """Pick the next eligible task from structured task headers."""
        self._idle_dag_tasks = None
        task_dir = Path(self.repo_path) / "tasks"
        if not task_dir.is_dir():
            return None

        headers = []
        skipped_legacy_pr_ids: set[str] = set()
        task_files: dict[str, str] = {}
        repo_root = Path(self.repo_path)
        for task_file in sorted(task_dir.glob("PR-*.md")):
            try:
                header = parse_task_header(task_file)
            except QueueValidationError as exc:
                if not (
                    self._is_missing_task_header_error(exc)
                    or self._is_legacy_unstructured_task_error(exc)
                ):
                    raise
                skipped_legacy_pr_ids.add(task_file.stem)
                continue
            headers.append(header)
            task_files[header.pr_id] = task_file.relative_to(repo_root).as_posix()

        merged_pr_ids = get_merged_pr_ids(
            self.repo_path,
            self.repo_config.branch,
            {
                pr_id
                for header in headers
                for pr_id in (header.pr_id, *header.depends_on)
            },
        )
        headers = self._filter_dag_headers_with_available_dependencies(
            headers,
            skipped_legacy_pr_ids,
            task_dir,
            merged_pr_ids,
        )
        if not headers:
            return None

        try:
            dag_headers = [
                replace(
                    header,
                    depends_on=[
                        dependency
                        for dependency in header.depends_on
                        if dependency not in merged_pr_ids
                    ],
                )
                for header in headers
            ]
            merged_pr_ids = {
                pr_id for pr_id in merged_pr_ids if pr_id in {header.pr_id for header in headers}
            }
            open_prs = list(getattr(self, "_idle_open_prs", ()))
            merged_prs = list(getattr(self, "_idle_merged_prs", ()))
            statuses = {
                header.pr_id: derive_task_status(
                    header,
                    merged_pr_ids,
                    open_prs,
                    merged_prs,
                )
                for header in headers
            }
            eligible = get_eligible_tasks(dag_headers, statuses)
        except ValueError as exc:
            raise QueueValidationError([str(exc)]) from exc

        self._idle_dag_tasks = [
            self._queue_task_from_header(header, statuses[header.pr_id], task_files)
            for header in headers
        ]
        doing_tasks = [
            task for task in self._idle_dag_tasks if task.status == TaskStatus.DOING
        ]
        if doing_tasks:
            return doing_tasks[0]
        if not eligible:
            return None

        picked = eligible[0]
        return self._queue_task_from_header(picked, TaskStatus.TODO, task_files)

    def _queue_task_from_header(
        self,
        header,
        status: TaskStatus,
        task_files: dict[str, str],
    ) -> QueueTask:
        return QueueTask(
            pr_id=header.pr_id,
            title=header.title,
            status=status,
            task_file=task_files[header.pr_id],
            depends_on=list(header.depends_on),
            branch=header.branch,
        )

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
            self.state.current_task = None
            return
        try:
            merged_prs = github_client.get_merged_prs(
                self.owner_repo,
                self.repo_config.branch,
                refresh=True,
            )
        except Exception as exc:
            self.log_event(
                f"IDLE: merged PR check failed: {exc}; "
                "continuing with local merged-status heuristics"
            )
            merged_prs = []

        queue_path = str(Path(self.repo_path) / "tasks" / "QUEUE.md")
        strict = self.app_config.daemon.strict_queue_validation
        dag_task = None
        dag_tasks = None
        self._idle_open_prs = prs
        self._idle_merged_prs = merged_prs
        try:
            dag_task = await self._select_next_task_from_dag()
            dag_tasks = self._idle_dag_tasks
        except (
            OSError,
            RuntimeError,
            QueueValidationError,
            subprocess.TimeoutExpired,
        ) as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"Task selection failed: {exc}"
            self.log_event(f"Task selection failed: {exc}")
            return
        finally:
            self._idle_open_prs = []
            self._idle_merged_prs = []

        tasks: list[QueueTask] = []
        queue_task = None
        structured_pr_ids = {queued.pr_id for queued in dag_tasks or []}
        has_legacy_queue_tasks = False
        try:
            tasks = parse_queue(queue_path, strict=strict)
        except QueueValidationError as exc:
            if dag_task is None:
                self.state.state = PipelineState.ERROR
                self.state.error_message = str(exc)
                self.log_event(f"Queue validation failed: {exc}")
                return
            self.log_event(
                "Queue validation failed after DAG selection; "
                f"continuing with DAG task: {exc}"
            )
        else:
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

            queue_task = get_next_task(tasks)
            has_legacy_queue_tasks = any(
                queued.pr_id not in structured_pr_ids for queued in tasks
            )

        task = dag_task
        if queue_task is not None:
            if task is None:
                task = queue_task
            elif (
                task.status != TaskStatus.DOING
                and queue_task.pr_id not in structured_pr_ids
            ):
                task = queue_task

        if has_legacy_queue_tasks or dag_tasks is None:
            queue_tasks = tasks
        else:
            queue_tasks = dag_tasks
        self.state.queue_done = sum(
            1 for t in queue_tasks if t.status == TaskStatus.DONE
        )
        self.state.queue_total = len(queue_tasks)
        if task is None:
            self.log_event("No tasks available")
            if prs:
                done_branches = {
                    t.branch for t in queue_tasks
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
