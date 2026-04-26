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
from src.daemon import git_ops
from src.dag import get_eligible_tasks
from src.models import PipelineState, QueueTask, TaskStatus
from src.queue_parser import (
    QueueValidationError,
    TaskHeader,
    get_next_task,
    parse_queue,
    parse_task_header,
)
from src.retry import is_transient_error, retry_transient
from src.task_status import (
    derive_queue_task_statuses,
    derive_task_status,
    find_matching_open_pr,
    get_merged_pr_ids,
)


class IdleMixin:
    """Handle IDLE state: sync, pick next task, dispatch to CODING."""

    def _preserve_fix_iteration_count(self, pr):
        """Carry the iteration counter forward when reattaching the same PR."""
        current_pr = self.state.current_pr
        if current_pr is None or current_pr.number != pr.number:
            return pr
        return pr.model_copy(
            update={"fix_iteration_count": current_pr.fix_iteration_count}
        )

    @staticmethod
    def _generate_queue_md(
        headers: list[TaskHeader],
        statuses: dict[str, TaskStatus],
    ) -> str:
        """Render a visually compatible QUEUE.md from structured task headers."""
        lines = ["# Task Queue\n"]
        for header in headers:
            lines.append(f"## {header.pr_id}: {header.title}")
            lines.append(f"- Status: {statuses[header.pr_id].value}")
            lines.append(f"- Tasks file: tasks/{header.pr_id}.md")
            lines.append(f"- Branch: {header.branch}")
            if header.depends_on:
                lines.append(f"- Depends on: {', '.join(header.depends_on)}")
            lines.append("")
        return "\n".join(lines)

    def _write_generated_queue_md(
        self,
        headers: list[TaskHeader],
        statuses: dict[str, TaskStatus],
    ) -> bool:
        """Write and publish QUEUE.md only when the generated report changed.

        Returns ``True`` when the generated queue is synchronized locally
        after the call. A rejected push is treated as recoverable: the
        helper drops the local auto-generated queue commit and returns
        ``False`` so IDLE can continue without carrying a stray local
        queue commit.
        """
        queue_path = Path(self.repo_path) / "tasks" / "QUEUE.md"
        self._idle_generated_queue_needs_resync = False
        content = self._generate_queue_md(headers, statuses)
        existing = (
            queue_path.read_text(encoding="utf-8")
            if queue_path.exists()
            else None
        )
        if existing == content:
            return True

        queue_path.parent.mkdir(parents=True, exist_ok=True)
        queue_path.write_text(content, encoding="utf-8")
        git_ops._git(self.repo_path, "add", "tasks/QUEUE.md")
        git_ops._git(
            self.repo_path,
            "commit",
            "-m",
            "AUTO: regenerate QUEUE.md from DAG",
        )

        def _push_queue_md() -> subprocess.CompletedProcess[str]:
            result = git_ops._git(
                self.repo_path,
                "push",
                "origin",
                self.repo_config.branch,
                timeout=60,
                check=False,
            )
            if result.returncode != 0:
                exc = subprocess.CalledProcessError(
                    result.returncode,
                    result.args,
                    output=result.stdout,
                    stderr=result.stderr,
                )
                if is_transient_error(exc):
                    raise exc
            return result

        def _rollback_generated_queue_commit() -> None:
            local_base = "HEAD~1"
            remote_base = f"refs/remotes/origin/{self.repo_config.branch}"
            try:
                fetch = git_ops._git(
                    self.repo_path,
                    "fetch",
                    "origin",
                    self.repo_config.branch,
                    timeout=60,
                    check=False,
                )
                local_base_sha = git_ops._git(
                    self.repo_path,
                    "rev-parse",
                    "--verify",
                    "--quiet",
                    local_base,
                    check=False,
                )
                remote_base_sha = git_ops._git(
                    self.repo_path,
                    "rev-parse",
                    "--verify",
                    "--quiet",
                    remote_base,
                    check=False,
                )
                if (
                    fetch.returncode == 0
                    and local_base_sha.returncode == 0
                    and remote_base_sha.returncode == 0
                    and local_base_sha.stdout.strip() != remote_base_sha.stdout.strip()
                ):
                    self._idle_generated_queue_needs_resync = True
                    git_ops._git(
                        self.repo_path,
                        "reset",
                        "--hard",
                        remote_base,
                    )
                    return
            except (OSError, subprocess.TimeoutExpired):
                self._idle_generated_queue_needs_resync = True
            else:
                if (
                    fetch.returncode != 0
                    or local_base_sha.returncode != 0
                    or remote_base_sha.returncode != 0
                ):
                    self._idle_generated_queue_needs_resync = True
            git_ops._git(
                self.repo_path,
                "reset",
                "--hard",
                local_base,
            )

        try:
            push_result = retry_transient(
                _push_queue_md,
                operation_name=f"git push origin {self.repo_config.branch}",
            )
        except RuntimeError as exc:
            if not is_transient_error(exc) and not is_transient_error(exc.__cause__):
                raise
            _rollback_generated_queue_commit()
            return False
        if push_result.returncode == 0:
            return True

        _rollback_generated_queue_commit()
        return False

    @staticmethod
    def _validate_task_file_header_match(task_file: Path, header_pr_id: str) -> None:
        expected_pr_id = task_file.stem
        if header_pr_id != expected_pr_id:
            raise QueueValidationError(
                [
                    f"{task_file}: header PR ID {header_pr_id!r} "
                    f"does not match task file {expected_pr_id!r}"
                ]
            )

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
    def _queue_md_contains_visible_legacy_entries(
        queue_path: str | Path,
        structured_pr_ids: set[str],
    ) -> bool:
        path = Path(queue_path)
        if not path.is_file():
            return False
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            match = re.match(r"^##\s+(PR-[A-Za-z0-9_.-]+)\b", raw_line.rstrip())
            if match and match.group(1) not in structured_pr_ids:
                return True
        return False

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
        self._idle_dag_headers = None
        self._idle_dag_statuses = None
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
            self._validate_task_file_header_match(task_file, header.pr_id)
            headers.append(header)
            task_files[header.pr_id] = task_file.relative_to(repo_root).as_posix()

        if not headers:
            return None

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

        self._idle_dag_headers = list(dag_headers)
        self._idle_dag_statuses = dict(statuses)
        self._idle_dag_tasks = [
            self._queue_task_from_header(header, statuses[header.pr_id], task_files)
            for header in dag_headers
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
        if self.state.user_paused:
            return
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
        legacy_queue_check_succeeded = False
        visible_legacy_queue_entries = False
        try:
            tasks = parse_queue(queue_path, strict=strict)
        except QueueValidationError as exc:
            if dag_tasks is None:
                self.state.state = PipelineState.ERROR
                self.state.error_message = str(exc)
                self.log_event(f"Queue validation failed: {exc}")
                return
            self.log_event(
                "Queue validation failed after DAG selection; "
                f"continuing with DAG task: {exc}"
            )
            visible_legacy_queue_entries = self._queue_md_contains_visible_legacy_entries(
                queue_path,
                structured_pr_ids,
            )
            legacy_queue_check_succeeded = not visible_legacy_queue_entries
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
                if dag_tasks is None:
                    self.state.state = PipelineState.ERROR
                    self.state.error_message = f"Task status derivation failed: {exc}"
                    self.log_event(f"Task status derivation failed: {exc}")
                    return
                self.log_event(
                    "Task status derivation failed after DAG selection; "
                    f"continuing with DAG tasks: {exc}"
                )
                tasks = []
                queue_task = None
                visible_legacy_queue_entries = self._queue_md_contains_visible_legacy_entries(
                    queue_path,
                    structured_pr_ids,
                )
                legacy_queue_check_succeeded = not visible_legacy_queue_entries
            else:
                queue_task = get_next_task(tasks)
                visible_legacy_queue_entries = self._queue_md_contains_visible_legacy_entries(
                    queue_path,
                    structured_pr_ids,
                )
                has_legacy_queue_tasks = any(
                    queued.pr_id not in structured_pr_ids for queued in tasks
                ) or visible_legacy_queue_entries
                legacy_queue_check_succeeded = not visible_legacy_queue_entries

        task = dag_task
        if queue_task is not None:
            queue_task_is_legacy = queue_task.pr_id not in structured_pr_ids
            if task is None and (dag_tasks is None or queue_task_is_legacy):
                task = queue_task
            elif (
                task is not None
                and task.status != TaskStatus.DOING
                and queue_task_is_legacy
            ):
                task = queue_task

        if has_legacy_queue_tasks or dag_tasks is None:
            queue_tasks = tasks
        else:
            queue_tasks = dag_tasks
        self._set_queue_progress(
            sum(1 for t in queue_tasks if t.status == TaskStatus.DONE),
            len(queue_tasks),
        )
        generated_headers = getattr(self, "_idle_dag_headers", None)
        generated_statuses = getattr(self, "_idle_dag_statuses", None)
        if (
            generated_headers
            and generated_statuses
            and legacy_queue_check_succeeded
            and not has_legacy_queue_tasks
        ):
            try:
                published_queue = self._write_generated_queue_md(
                    generated_headers,
                    generated_statuses,
                )
            except (
                OSError,
                RuntimeError,
                subprocess.CalledProcessError,
                subprocess.TimeoutExpired,
            ) as exc:
                message = f"QUEUE.md auto-generation failed: {exc}"
                self.state.state = PipelineState.ERROR
                self.state.error_message = message
                self.log_event(message)
                return
            if not published_queue:
                self.log_event(
                    "QUEUE.md auto-generation push rejected; "
                    "continuing without publishing"
                )
                if getattr(self, "_idle_generated_queue_needs_resync", False):
                    self.log_event(
                        "QUEUE.md auto-generation refreshed origin state; "
                        "retrying task selection next cycle"
                    )
                    return
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
                selected = match or prs[0]
                self.state.current_pr = self._preserve_fix_iteration_count(selected)
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
                self.state.current_pr = self._preserve_fix_iteration_count(existing)
                self.state.state = PipelineState.WATCH
                self._rehydrate_last_push_at(self.state.current_pr)
                self.log_event(
                    f"Task {task.pr_id} has existing open PR #{existing.number} "
                    f"on {task_branch!r} -> WATCH (no duplicate CODING)"
                )
                await self.publish_state()
                return

        await self._refresh_user_paused_from_redis()
        if self.state.user_paused:
            self.state.current_task = None
            self.log_event(
                f"Pause requested while preparing {task.pr_id}; deferring CODING"
            )
            return

        pin = self._active_task_coder_pin()
        if pin in ("claude", "codex"):
            await self._refresh_auth_status_cache()
            if self._select_coder(allow_exploration=False) is None:
                self.state.state = PipelineState.HUNG
                self.state.error_message = (
                    f"Task {task.pr_id} pinned to {pin} but coder unavailable"
                )
                self.log_event(self.state.error_message)
                await self.publish_state()
                return

        self.state.state = PipelineState.CODING
        self.log_event(f"Picked task {task.pr_id}: {task.title}")
        await self.publish_state()
        await self.handle_coding()

    async def handle_paused(self) -> None:
        """Wait for rate limit window to expire, then resume previous flow."""
        if self.state.user_paused:
            if not getattr(self, "_user_pause_logged", False):
                self.log_event("Paused. Press Play to resume.")
                self._user_pause_logged = True
            return
        if self.state.rate_limited_until is None:
            self.log_event("PAUSED without rate_limited_until -> IDLE")
            self.state.state = PipelineState.IDLE
            return
        pause_coder = self.state.rate_limit_reactive_coder or "claude"
        await self._refresh_auth_status_cache()
        selected = self._select_coder()
        coder_name = selected[0] if selected is not None else pause_coder
        diagnosis_pause = (
            self.state.error_message is not None
            and pause_coder == "claude"
        )
        other_coder = (
            not diagnosis_pause
            and pause_coder != coder_name
        )
        clearable = other_coder
        if clearable:
            self._error_diagnose_count = 0
            self._claude_usage_provider.invalidate_cache()
            self._codex_usage_provider.invalidate_cache()
            label = (
                f"{coder_name.capitalize()} active while {pause_coder} remains "
                f"rate-limited until {self.state.rate_limited_until.isoformat()}"
            )
            if pause_coder not in self.state.rate_limited_coder_until:
                self.state.rate_limited_coders.add(pause_coder)
                self.state.rate_limited_coder_until[pause_coder] = (
                    self.state.rate_limited_until
                )
            self.state.rate_limited_until = None
            self.state.rate_limit_reactive = False
            self.state.rate_limit_reactive_coder = None
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
        self.state.rate_limited_coders.discard(pause_coder)
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
