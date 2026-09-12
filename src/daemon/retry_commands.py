"""Daemon-side application and recovery of durable operator Retry commands."""

from __future__ import annotations

import asyncio
import subprocess
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any

from src.cancellation import (
    get_current_run_started_at,
    task_spec_content_hash,
)
from src.daemon import git_ops
from src.daemon.selector import CoderPurpose, resolve_active_coder
from src.github import prs as gh_prs
from src.inhibitor import InhibitorType, derive_active_inhibitors
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus
from src.queue_parser import QueueValidationError
from src.queue_parser import parse_existing_task_header as parse_task_header
from src.retry_commands import (
    RetryCapReached,
    RetryCommand,
    RetryCommandStatus,
    RetryEffectStage,
    RetryExecutionState,
    claim_retry_command,
    list_pending_retry_commands,
    reserve_retry_attempt,
    update_retry_command,
)
from src.subsource_registry import SuppressionReason
from src.suppression.redis_store import RedisSuppressionStore


class RetryDispatch(StrEnum):
    NONE = "none"
    HANDLED = "handled"
    CODING = "coding"
    WATCH = "watch"


_ACTIVE_TASK_STATES = {
    PipelineState.PREFLIGHT,
    PipelineState.CODING,
    PipelineState.WATCH,
    PipelineState.FIX,
    PipelineState.MERGE,
}
_SPEC_CHANGE_REQUIRED = {
    SuppressionReason.GUARDRAIL,
    SuppressionReason.OPERATOR_REJECT,
}


class RetryCommandMixin:
    """Consume Retry commands without relying on their pub/sub wake message."""

    async def _consume_retry_command(self) -> RetryDispatch:
        try:
            commands = await list_pending_retry_commands(self.redis, self.name)
        except Exception as exc:
            self.log_event(f"[INFRA] Retry command store unavailable: {exc}.")
            return RetryDispatch.NONE
        if not commands:
            self._active_retry_command_id = None
            return RetryDispatch.NONE

        command = commands[0]
        self._active_retry_command_id = command.command_id
        if command.status == RetryCommandStatus.APPLIED:
            return await self._reconcile_applied_retry(command)

        try:
            claimed = await claim_retry_command(
                self.redis,
                self.name,
                command.command_id,
                self._retry_command_owner,
            )
        except Exception as exc:
            self.log_event(
                f"[INFRA] Retry command {command.command_id} claim failed: {exc}."
            )
            return RetryDispatch.HANDLED
        if claimed is None:
            return RetryDispatch.HANDLED
        if claimed.status == RetryCommandStatus.APPLIED:
            return await self._reconcile_applied_retry(claimed)
        return await self._apply_retry_command(claimed)

    async def _defer_retry_command(
        self, command: RetryCommand, reason: str
    ) -> RetryDispatch:
        previous = command.outcome_reason

        def _mutate(current: RetryCommand) -> None:
            current.status = RetryCommandStatus.DEFERRED
            current.processing_owner = None
            current.processing_started_at = None

        await update_retry_command(
            self.redis,
            self.name,
            command.command_id,
            _mutate,
            keep_pending=True,
            transition_reason=reason,
        )
        if previous != reason:
            self.log_event(
                f"[RECOVERY] Retry {command.command_id} deferred for {command.task_id}: "
                f"{reason}"
            )
        return RetryDispatch.HANDLED

    async def _fail_retry_command(
        self, command: RetryCommand, reason: str
    ) -> RetryDispatch:
        def _mutate(current: RetryCommand) -> None:
            current.status = RetryCommandStatus.FAILED
            current.execution_state = RetryExecutionState.FAILED
            current.processing_owner = None
            current.processing_started_at = None

        await update_retry_command(
            self.redis,
            self.name,
            command.command_id,
            _mutate,
            keep_pending=False,
            transition_reason=reason,
        )
        self.log_event(
            f"[RECOVERY] Retry {command.command_id} failed for {command.task_id}: {reason}"
        )
        return RetryDispatch.HANDLED

    def _retry_task_path(self, command: RetryCommand) -> Path | None:
        relative = Path(command.task_file)
        if relative.is_absolute() or ".." in relative.parts:
            return None
        repo_root = Path(self.repo_path).resolve()
        candidate = repo_root / relative
        cursor = repo_root
        for part in relative.parts:
            cursor /= part
            if cursor.is_symlink():
                return None
        try:
            resolved = candidate.resolve()
            resolved.relative_to(repo_root)
        except (OSError, RuntimeError, ValueError):
            return None
        return resolved if resolved.is_file() else None

    def _retry_worktree_dirty(self) -> tuple[bool | None, str]:
        try:
            result = git_ops._git(
                self.repo_path,
                "status",
                "--porcelain",
                timeout=30,
            )
        except (subprocess.SubprocessError, OSError) as exc:
            return None, str(exc)
        detail = result.stdout.strip()
        return bool(detail), detail

    def _origin_retry_task_text(self, command: RetryCommand) -> str | None:
        try:
            result = git_ops._git(
                self.repo_path,
                "show",
                f"origin/{self.repo_config.branch}:{command.task_file}",
                timeout=30,
            )
        except (subprocess.SubprocessError, OSError):
            return None
        return result.stdout

    async def _validate_retry_command(
        self, command: RetryCommand
    ) -> tuple[Any, QueueTask] | RetryDispatch:
        if command.repo_slug != self.name:
            return await self._fail_retry_command(
                command, "Command repository binding does not match this runner."
            )
        if not self.repo_config.active:
            return await self._defer_retry_command(
                command, "Repository is disabled in config.yml."
            )
        if self.state.pending_queue_sync_branch is not None:
            return await self._defer_retry_command(
                command,
                f"Queue synchronization PR {self.state.pending_queue_sync_branch} is pending.",
            )
        active_task = self.state.current_task
        if (
            active_task is not None
            and active_task.pr_id != command.task_id
            and self.state.state in _ACTIVE_TASK_STATES
        ):
            return await self._defer_retry_command(
                command,
                f"Another task ({active_task.pr_id}) is active in {self.state.state.value}.",
            )

        dirty, detail = self._retry_worktree_dirty()
        if dirty is None:
            return await self._defer_retry_command(
                command, f"Could not inspect the worktree: {detail}."
            )
        if dirty:
            return await self._defer_retry_command(
                command,
                f"Worktree is dirty and was preserved ({detail[:160]}).",
            )

        task_path = self._retry_task_path(command)
        if task_path is None:
            return await self._fail_retry_command(
                command, "Bound task file is missing or unsafe."
            )
        try:
            task_text = task_path.read_text(encoding="utf-8")
            header = parse_task_header(task_path)
        except (OSError, UnicodeError, QueueValidationError) as exc:
            return await self._fail_retry_command(
                command, f"Bound task file cannot be parsed: {exc}."
            )
        if header.pr_id != command.task_id or header.branch != command.task_branch:
            return await self._fail_retry_command(
                command, "Task identity or branch changed after Retry was requested."
            )
        if task_spec_content_hash(task_text) != command.task_fingerprint:
            return await self._fail_retry_command(
                command, "Task specification fingerprint changed; refresh before retrying."
            )

        origin_text = self._origin_retry_task_text(command)
        if origin_text is None:
            return await self._defer_retry_command(
                command, "Cannot verify the task specification on the base branch."
            )
        if task_spec_content_hash(origin_text) != command.task_fingerprint:
            return await self._fail_retry_command(
                command,
                "Base-branch task specification differs from the accepted fingerprint.",
            )

        task = QueueTask(
            pr_id=header.pr_id,
            title=header.title,
            status=TaskStatus.ERROR,
            task_file=command.task_file,
            depends_on=list(header.depends_on),
            branch=header.branch,
            priority=header.priority,
        )
        return header, task

    async def _retry_blocker(
        self, command: RetryCommand
    ) -> RetryDispatch | None:
        try:
            record = await self._suppression_record_for_task(command.task_id)
        except Exception as exc:
            return await self._defer_retry_command(
                command, f"Cannot verify the prior failure record: {exc}."
            )
        if record is not None and record.reason in _SPEC_CHANGE_REQUIRED:
            return await self._defer_retry_command(
                command,
                f"{record.reason.value} requires a revised task or its dedicated operator decision.",
            )

        try:
            inhibitors = await derive_active_inhibitors(
                self.state,
                self.redis,
                self.app_config.daemon,
            )
        except Exception as exc:
            return await self._defer_retry_command(
                command, f"Cannot evaluate active inhibitors: {exc}."
            )
        blocking = [
            inhibitor
            for inhibitor in inhibitors
            if inhibitor.inhibitor_type != InhibitorType.GITHUB_BUDGET_SLOWDOWN
        ]
        if blocking:
            details = ", ".join(
                f"{item.inhibitor_type.value}: {item.reason_text}"
                for item in blocking
            )
            return await self._defer_retry_command(
                command, f"Active inhibitor(s): {details}."
            )
        return None

    @staticmethod
    def _matching_prs(command: RetryCommand, prs: list[PRInfo]) -> list[PRInfo]:
        matches: list[PRInfo] = []
        for pr in prs:
            if pr.branch != command.task_branch:
                continue
            if pr.pr_id not in (None, command.task_id):
                continue
            matches.append(pr)
        return matches

    async def _select_retry_continuation(
        self, command: RetryCommand
    ) -> tuple[str, PRInfo | None] | RetryDispatch:
        try:
            open_prs = await asyncio.to_thread(
                gh_prs.get_open_prs,
                self.owner_repo,
                self.repo_config.allow_merge_without_checks,
            )
        except Exception as exc:
            return await self._defer_retry_command(
                command, f"GitHub open-PR state is unavailable: {exc}."
            )

        if command.bound_pr_number is not None:
            bound_open = next(
                (pr for pr in open_prs if pr.number == command.bound_pr_number),
                None,
            )
            if bound_open is not None:
                if (
                    bound_open.branch != command.bound_pr_branch
                    or bound_open.branch != command.task_branch
                    or bound_open.pr_id not in (None, command.task_id)
                ):
                    return await self._fail_retry_command(
                        command, "The bound pull request now belongs to different work."
                    )
                if (
                    command.bound_pr_head_sha
                    and bound_open.head_sha
                    and command.bound_pr_head_sha != bound_open.head_sha
                ):
                    return await self._fail_retry_command(
                        command, "The bound pull request HEAD changed; refresh before retrying."
                    )
                return "watch", bound_open

            state = await asyncio.to_thread(
                gh_prs.get_pr_state,
                self.owner_repo,
                command.bound_pr_number,
            )
            if state is None:
                return await self._defer_retry_command(
                    command,
                    f"State of bound PR #{command.bound_pr_number} is unavailable.",
                )
            if state.upper() == "MERGED":
                return await self._fail_retry_command(
                    command,
                    f"Bound PR #{command.bound_pr_number} is already merged; implementation will not be rerun.",
                )
            if state.upper() == "CLOSED":
                return await self._defer_retry_command(
                    command,
                    f"Bound PR #{command.bound_pr_number} is closed without merge; "
                    "reopen or revise it before retrying.",
                )
            return await self._defer_retry_command(
                command,
                f"Bound PR #{command.bound_pr_number} returned unexpected state {state!r}.",
            )

        matching_open = self._matching_prs(command, open_prs)
        if len(matching_open) > 1:
            numbers = ", ".join(f"#{pr.number}" for pr in matching_open)
            return await self._defer_retry_command(
                command, f"Multiple open pull requests match this task ({numbers})."
            )
        if matching_open:
            return "watch", matching_open[0]

        try:
            merged_prs = await asyncio.to_thread(
                gh_prs.get_merged_prs,
                self.owner_repo,
                self.repo_config.branch,
                refresh=True,
            )
        except Exception as exc:
            return await self._defer_retry_command(
                command, f"GitHub merged-PR state is unavailable: {exc}."
            )
        matching_merged = self._matching_prs(command, merged_prs)
        if matching_merged:
            return await self._fail_retry_command(
                command,
                f"PR #{matching_merged[0].number} is already merged; implementation will not be rerun.",
            )
        return "coding", None

    async def _ensure_retry_coder_available(
        self, command: RetryCommand, header: Any, task: QueueTask
    ) -> tuple[str, Any] | RetryDispatch:
        await self._refresh_auth_status_cache()
        resolution = resolve_active_coder(
            self._selection_context(task_coder_pin=header.coder),
            purpose=CoderPurpose.DISPATCH,
        )
        if resolution is None or resolution.plugin is None:
            return await self._defer_retry_command(
                command, "No authenticated, enabled coder is currently available."
            )
        if not await self.usage_gate(proactive_coder=resolution.name):
            return await self._defer_retry_command(
                command,
                f"Provider or spend limit currently blocks {resolution.name}.",
            )
        return resolution.name, resolution.plugin

    async def _record_retry_stage(
        self,
        command: RetryCommand,
        stage: RetryEffectStage,
        reason: str,
    ) -> RetryCommand:
        def _mutate(current: RetryCommand) -> None:
            current.effect_stage = stage

        updated = await update_retry_command(
            self.redis,
            self.name,
            command.command_id,
            _mutate,
            keep_pending=True,
            transition_reason=reason,
        )
        if updated is None:
            raise RuntimeError("retry command disappeared while recording stage")
        return updated

    async def _clear_retry_failure_evidence(self, command: RetryCommand) -> None:
        await RedisSuppressionStore(self.redis).clear(self.name, command.task_id)
        await self.redis.delete(f"diagnose_exhausted:{self.name}:{command.task_id}")
        self._crashed_task_pr_ids.discard(command.task_id)
        self._user_stopped_task_pr_ids.discard(command.task_id)
        self._status_write_failed_task_pr_ids.discard(command.task_id)
        await self._persist_status_write_failed_task_pr_ids()

    def _reset_retry_local_counters(self, pr: PRInfo | None) -> tuple[PRInfo | None, list[str]]:
        reset = [
            "error_diagnose_attempts",
            "error_soft_skip_attempts",
            "review_timeout_repost",
        ]
        self._error_diagnose_policy.reset(self)
        self._error_skip_policy.reset(self)
        self._error_skip_active = False
        self._error_skip_context = None
        self.state.skip_ai_error_diagnose = False
        self.state.review_timeout_repost_attempted = False
        self.state.review_timeout_repost_at = None
        self._last_error_park_log_key = None
        if pr is None:
            return None, reset
        reset.extend(["fix_iteration_count", "no_push_fix_count", "watch_retrigger_count"])
        return (
            pr.model_copy(
                update={
                    "fix_iteration_count": 0,
                    "no_push_fix_count": 0,
                    "watch_retrigger_count": 0,
                }
            ),
            reset,
        )

    def _set_retry_task_snapshot(self, task: QueueTask) -> None:
        current_queue = list(self.state.current_queue or [])
        replacement = task.model_copy(update={"status": TaskStatus.DOING})
        for index, queued in enumerate(current_queue):
            if queued.pr_id == task.pr_id:
                current_queue[index] = replacement
                break
        else:
            current_queue.append(replacement)
        self.state.current_queue = current_queue
        self.state.current_task = replacement

    async def _mark_retry_applied(
        self,
        command: RetryCommand,
        continuation: str,
        reset_counters: list[str],
    ) -> RetryCommand:
        reason = (
            f"Daemon applied Retry attempt {command.retry_count}/{command.retry_cap}; "
            f"selected {continuation.upper()} continuation."
        )

        def _mutate(current: RetryCommand) -> None:
            current.status = RetryCommandStatus.APPLIED
            current.effect_stage = RetryEffectStage.APPLIED
            current.selected_continuation = continuation
            current.reset_counters = list(reset_counters)
            current.execution_state = RetryExecutionState.PENDING
            current.processing_started_at = None

        updated = await update_retry_command(
            self.redis,
            self.name,
            command.command_id,
            _mutate,
            keep_pending=True,
            transition_reason=reason,
        )
        if updated is None:
            raise RuntimeError("retry command disappeared before acknowledgement")
        self.log_event(
            f"[RECOVERY] Applied Retry {command.command_id} for {command.task_id}; "
            f"fingerprint={command.task_fingerprint[:12]}, continuation={continuation}, "
            f"reset={','.join(reset_counters)}."
        )
        return updated

    async def _apply_retry_command(self, command: RetryCommand) -> RetryDispatch:
        validated = await self._validate_retry_command(command)
        if isinstance(validated, RetryDispatch):
            return validated
        header, task = validated

        blocker = await self._retry_blocker(command)
        if blocker is not None:
            return blocker
        continuation = await self._select_retry_continuation(command)
        if isinstance(continuation, RetryDispatch):
            return continuation
        continuation_name, pr = continuation

        if continuation_name == "coding":
            coder = await self._ensure_retry_coder_available(command, header, task)
            if isinstance(coder, RetryDispatch):
                return coder

        try:
            command = await reserve_retry_attempt(
                self.redis,
                self.name,
                command.command_id,
                command.retry_cap,
            )
        except RetryCapReached as exc:
            return await self._fail_retry_command(
                command, f"Retry cap reached ({exc.current}/{exc.cap})."
            )
        except Exception as exc:
            return await self._defer_retry_command(
                command, f"Could not reserve retry allowance: {exc}."
            )

        if command.effect_stage == RetryEffectStage.RETRY_RESERVED:
            committed = await self._commit_task_status_change(
                task,
                "TODO",
                f"operator Retry {command.command_id}",
            )
            if not committed:
                return await self._defer_retry_command(
                    command,
                    "Retry allowance is reserved, but the daemon could not persist status: TODO.",
                )
            try:
                command = await self._record_retry_stage(
                    command,
                    RetryEffectStage.STATUS_COMMITTED,
                    "Daemon persisted status: TODO on the base branch.",
                )
            except Exception as exc:
                return await self._defer_retry_command(
                    command,
                    f"Status was persisted but acknowledgement failed: {exc}.",
                )

        try:
            await self._clear_retry_failure_evidence(command)
        except Exception as exc:
            return await self._defer_retry_command(
                command,
                f"Status is queued, but prior failure evidence cleanup is incomplete: {exc}.",
            )

        pr, reset_counters = self._reset_retry_local_counters(pr)
        self._set_retry_task_snapshot(task)
        self.state.error_message = None
        if continuation_name == "watch":
            assert pr is not None
            self.state.current_pr = pr
            self.state.state = PipelineState.WATCH
        else:
            self.state.current_pr = None
            self.state.state = PipelineState.CODING
        try:
            await self._mark_retry_applied(
                command, continuation_name, reset_counters
            )
        except Exception as exc:
            return await self._defer_retry_command(
                command, f"Continuation selected but acknowledgement failed: {exc}."
            )
        return (
            RetryDispatch.WATCH
            if continuation_name == "watch"
            else RetryDispatch.CODING
        )

    async def _reconcile_applied_retry(self, command: RetryCommand) -> RetryDispatch:
        validated = await self._validate_retry_command(command)
        if isinstance(validated, RetryDispatch):
            return validated
        header, task = validated
        blocker = await self._retry_blocker(command)
        if blocker is not None:
            return blocker
        continuation = await self._select_retry_continuation(command)
        if isinstance(continuation, RetryDispatch):
            return continuation
        continuation_name, pr = continuation

        if continuation_name == "watch":
            assert pr is not None
            pr, _reset = self._reset_retry_local_counters(pr)
            self._set_retry_task_snapshot(task)
            self.state.current_pr = pr
            self.state.state = PipelineState.WATCH
            return RetryDispatch.WATCH

        if command.execution_state in {
            RetryExecutionState.RUNNING,
            RetryExecutionState.UNCERTAIN,
        }:
            try:
                started = await get_current_run_started_at(
                    self.redis, self.name, command.task_id
                )
            except Exception:
                started = None
            marker = f"; run marker {started.isoformat()}" if started else ""

            def _mutate(current: RetryCommand) -> None:
                current.execution_state = RetryExecutionState.UNCERTAIN

            await update_retry_command(
                self.redis,
                self.name,
                command.command_id,
                _mutate,
                keep_pending=True,
                transition_reason=(
                    "Previous daemon stopped during Retry execution and no PR outcome "
                    f"is visible{marker}; preserved state will not be executed twice."
                ),
            )
            return RetryDispatch.HANDLED

        coder = await self._ensure_retry_coder_available(command, header, task)
        if isinstance(coder, RetryDispatch):
            return coder
        self._set_retry_task_snapshot(task)
        self.state.current_pr = None
        self.state.state = PipelineState.CODING
        return RetryDispatch.CODING

    async def _start_retry_coding_execution(self) -> bool:
        command_id = getattr(self, "_active_retry_command_id", None)
        if command_id is None:
            return False
        moment = datetime.now(timezone.utc)

        def _mutate(current: RetryCommand) -> None:
            current.execution_state = RetryExecutionState.RUNNING
            current.execution_started_at = moment

        try:
            updated = await update_retry_command(
                self.redis,
                self.name,
                command_id,
                _mutate,
                keep_pending=True,
                transition_reason="Retry CODING execution started.",
            )
        except Exception as exc:
            self.log_event(
                f"[RECOVERY] Refusing to start Retry CODING without durable execution marker: {exc}."
            )
            return False
        return updated is not None

    async def _finish_retry_dispatch(self, dispatch: RetryDispatch) -> None:
        command_id = getattr(self, "_active_retry_command_id", None)
        if command_id is None:
            return

        if dispatch == RetryDispatch.WATCH:
            execution_state = RetryExecutionState.WATCHING
            reason = "Retry continuation is now watching the preserved open PR."
            keep_pending = False
        elif self.state.state == PipelineState.WATCH:
            execution_state = RetryExecutionState.WATCHING
            reason = "Retry CODING created or recovered a PR and entered WATCH."
            keep_pending = False
        elif self.state.state == PipelineState.PAUSED:
            execution_state = RetryExecutionState.PENDING
            reason = "Retry was applied; execution is waiting for the active pause to clear."
            keep_pending = True
        elif self.state.state == PipelineState.ERROR:
            execution_state = RetryExecutionState.FAILED
            reason = "Retry was applied and execution produced a new ERROR."
            keep_pending = False
        else:
            execution_state = RetryExecutionState.COMPLETED
            reason = f"Retry execution returned in {self.state.state.value}."
            keep_pending = False

        def _mutate(current: RetryCommand) -> None:
            current.execution_state = execution_state

        try:
            await update_retry_command(
                self.redis,
                self.name,
                command_id,
                _mutate,
                keep_pending=keep_pending,
                transition_reason=reason,
            )
        except Exception as exc:
            self.log_event(
                f"[RECOVERY] Retry execution outcome for {command_id} remains recoverable: {exc}."
            )


__all__ = ["RetryCommandMixin", "RetryDispatch"]
