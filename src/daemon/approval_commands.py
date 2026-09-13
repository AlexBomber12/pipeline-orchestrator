"""Apply approval at the scheduler's boundary without changing existing Git work.

The main loop permits one in-flight cycle per repository. A command is handled
before clone/scaffold, recovery, Retry or dirty-tree preflight. No coder is
started here. Unknown prior execution or a live process using the checkout
parks the command. Redis WATCH fences the decision, controls and published
state; the receipt and WATCH snapshot are committed with the exact cause's
removal, so a lost EXEC reply is recoverable on the next cycle/restart.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
from pathlib import Path
from typing import Any

from src.approval_commands import (
    ApprovalChanged,
    ApprovalCommand,
    approval_key,
    approval_task_path,
    failure_identity,
    list_approvals,
    matches_state,
)
from src.cancellation.storage import cause_key, index_key, task_spec_content_hash
from src.daemon import git_ops
from src.daemon.quarantine import quarantine_label_for_category
from src.github import prs as gh_prs
from src.inhibitor import derive_active_inhibitors
from src.keyspace import control_stop, pipeline_state, upload_pending
from src.models import PipelineState, RepoState, TaskStatus
from src.retry_commands import load_latest_retry_command


def checkout_process_blocker(repo_path: str) -> str | None:
    """Fail closed if Linux cannot establish checkout process quiescence.

    This includes orphaned coder children after a daemon restart. Process
    inspection complements scheduler ownership; preservation does not depend
    on a check-then-mutate window because approval never writes Git state.
    """
    root = Path(repo_path).resolve()
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit() or int(entry.name) == os.getpid():
            continue
        try:
            cwd = (entry / "cwd").resolve(strict=True)
        except FileNotFoundError:
            continue  # exited process or kernel thread
        except PermissionError:
            # Processes belonging to other UIDs cannot be our coder children.
            if entry.stat().st_uid == os.getuid():
                return f"Cannot establish ownership of process {entry.name}."
            continue
        if cwd == root or root in cwd.parents:
            return f"Process {entry.name} still uses the checkout; waiting for it to exit."
    return None


class ApprovalCommandMixin:
    async def _approval_result(self, command: ApprovalCommand, status: str, reason: str) -> None:
        key = approval_key(self.name, command.binding)

        async def transaction(pipe: Any) -> None:
            current = ApprovalCommand.model_validate_json(await pipe.get(key))
            # Never overwrite an acknowledgement from an EXEC whose reply was lost.
            if current.status == "applied":
                if status == "failed":
                    current.active = False
                    current.reason = f"Approval was applied; its continuation has ended: {reason}"
                elif status == "applied":
                    current.reason = reason
                else:
                    current.reason = f"Approval applied; continuation deferred: {reason}"
            else:
                current.status = status
                current.reason = reason
                current.active = status != "failed"
            pipe.multi()
            pipe.set(key, current.model_dump_json())

        await self.redis.transaction(transaction, key)

    async def _consume_approval_command(self) -> bool:
        """Return True when ordinary cycle work must wait or WATCH was restored."""
        try:
            commands = await list_approvals(self.redis, self.name)
            for command in reversed(commands):
                if not command.active:
                    snapshot = self.state
                    if command.status == "failed" and not self._recovered:
                        raw = await self.redis.get(pipeline_state(self.name))
                        if raw:
                            snapshot = RepoState.model_validate_json(raw)
                    if command.status == "failed" and matches_state(command, snapshot):
                        if await self._failed_approval_holds(command):
                            return True
                    continue
                if command.status == "applied" and self._recovered and not self._approval_commit_uncertain:
                    if not matches_state(command, self.state) or self.state.state != PipelineState.WATCH:
                        continue
                    # Protect preserved work from the legacy dirty-tree reset on
                    # subsequent WATCH cycles too, not just the acceptance cycle.
                    if await self.redis.get(cause_key(self.name, command.task.pr_id)):
                        self._approval_receipt = None
                        return True  # a newer failure must never inherit this permission
                    self._approval_receipt = command
                    try:
                        reason = await self._approval_blocker(command)
                    except Exception as exc:
                        reason = f"Continuation safety could not be verified: {exc}"
                    if reason:
                        await self._approval_result(command, "deferred", reason)
                        self.log_event(f"[RECOVERY] Approved PR waiting: {reason}")
                        return True
                    # Keep preserved work out of scaffolding and destructive
                    # preflight on subsequent cycles. WATCH retains all gates.
                    if "continuation deferred:" in command.reason:
                        await self._approval_result(
                            command, "applied", "Approval applied; watching the existing PR with CI and review gates."
                        )
                    if await self._check_github_api_budget():
                        await self.handle_watch()
                    await self.publish_state()
                    return True
                return await self._apply_approval(command)
            return False
        except Exception as exc:
            self.log_event(f"[RECOVERY] Approval store/application unavailable; deferring cycle: {exc}.")
            return True

    async def _failed_approval_holds(self, command: ApprovalCommand) -> bool:
        """Retain preservation until a later operator action safely supersedes it.

        This only releases this approval's hold. Existing Retry/upload/rejection
        consumers keep ownership of their own continuation policies.
        """
        if command.superseded:
            return False
        failure = failure_identity(await self.redis.get(cause_key(self.name, command.task.pr_id)))
        upload = await self.redis.get(upload_pending(self.name))
        retry = await load_latest_retry_command(self.redis, self.name, command.task.pr_id)
        later_retry = retry is not None and retry.requested_at > command.requested_at
        if failure == command.failure and not upload and not later_retry:
            return True
        reason = checkout_process_blocker(self.repo_path)
        if not reason and self._current_coder_process is not None:
            reason = "Coder execution is still active."
        if not reason:
            dirty = git_ops._git(self.repo_path, "--no-optional-locks", "status", "--porcelain")
            local = git_ops._git(self.repo_path, "rev-list", "--branches", "HEAD", "--not", "--remotes")
            if dirty.stdout.strip() or local.stdout.strip():
                reason = "Checkout still contains uncommitted work or local-only commits."
        if reason:
            await self._approval_result(command, "failed", f"Later operator action is waiting: {reason}")
            return True
        key = approval_key(self.name, command.binding)

        async def transaction(pipe: Any) -> None:
            current = ApprovalCommand.model_validate_json(await pipe.get(key))
            current.superseded = True
            current.reason = (
                "Failed approval superseded by a later operator action; normal reconciliation may continue."
            )
            pipe.multi()
            pipe.set(key, current.model_dump_json())

        await self.redis.transaction(transaction, key)
        return False

    async def _approval_blocker(self, command: ApprovalCommand) -> str | None:
        if not self.repo_config.active:
            return "Repository is disabled."
        if (
            self._current_coder_process is not None
            or self._stop_requested
            or command.task.pr_id in self._user_stopped_task_pr_ids
        ):
            return "Coder execution or Stop is still active."
        if self.state.pending_queue_sync_branch:
            return "Queue synchronization is pending."
        if self.state.state not in (PipelineState.ERROR, PipelineState.IDLE, PipelineState.WATCH, PipelineState.PAUSED):
            return "Work is active; waiting for the execution boundary."
        raw = await self.redis.get(pipeline_state(self.name))
        if raw:
            persisted = RepoState.model_validate_json(raw)
            if persisted.state not in (
                PipelineState.ERROR,
                PipelineState.IDLE,
                PipelineState.WATCH,
                PipelineState.PAUSED,
            ):
                return "Prior execution ownership is uncertain; waiting for daemon reconciliation."
            self.state.user_paused = persisted.user_paused
        # Staged uploads do not change the current task yet. Once permission
        # is applied, WATCH must finish so the existing IDLE upload consumer
        # can run. Initial application still defers competing task changes.
        if command.status != "applied" and await self.redis.get(upload_pending(self.name)):
            return "Task upload is pending; approval cannot authorize revised requirements."
        if await self.redis.get(control_stop(self.name)):
            return "Operator Stop is active."
        inhibitors = await derive_active_inhibitors(self.state, self.redis, self.app_config.daemon)
        if inhibitors:
            return "Active inhibitor(s): " + "; ".join(item.reason_text for item in inhibitors)
        reason = checkout_process_blocker(self.repo_path)
        if reason:
            return reason
        if not Path(self.repo_path).exists():
            # Restore an absent checkout without the ordinary clone helper's
            # partial-clone deletion or scaffolding. Git refuses a destination
            # that acquired files meanwhile; failures retain any partial work.
            await asyncio.to_thread(
                subprocess.run,
                ["git", "clone", "--branch", command.pr.branch, "--", self.repo_config.url, self.repo_path],
                capture_output=True, text=True, check=True, timeout=120,
            )
        result = git_ops._git(self.repo_path, "--no-optional-locks", "status", "--porcelain")
        if result.stdout.strip():
            return "Checkout has staged, unstaged or untracked work; preserving it until the operator resolves it."
        path = approval_task_path(Path(self.repo_path), command.task.task_file)
        if task_spec_content_hash(path.read_text(encoding="utf-8")) != command.task_fingerprint:
            raise ApprovalChanged("Task specification changed; this approval cannot authorize revised work.")
        local = git_ops._git(self.repo_path, "rev-list", "--branches", "HEAD", "--not", "--remotes")
        if local.stdout.strip():
            return (
                "Checkout has local-only commits; preserving them until they are published or reviewed by the operator."
            )
        return None

    async def _apply_approval(self, command: ApprovalCommand) -> bool:
        try:
            if command.repo_slug != self.name or command.repo_url != self.repo_config.url:
                raise ApprovalChanged("Approval repository binding changed.")
            if self._recovered and not matches_state(command, self.state):
                raise ApprovalChanged("Active task or PR binding changed.")
            # A later decision invalidates this request even while uploads or
            # other inhibitors block application. The transaction checks again
            # before clearing anything; a stale request must not hold forever.
            failure = failure_identity(await self.redis.get(cause_key(self.name, command.task.pr_id)))
            if failure != command.failure and not (command.status == "applied" and not failure):
                raise ApprovalChanged("Pending failure changed; no unrelated failure was cleared.")
            reason = await self._approval_blocker(command)
            if reason:
                await self._approval_result(command, "deferred", reason)
                return True
            prs = await asyncio.to_thread(
                gh_prs.get_open_prs,
                self.owner_repo,
                self.repo_config.allow_merge_without_checks,
            )
            pr = next((pr for pr in prs if pr.number == command.pr.number), None)
            if pr is None or pr.branch != command.pr.branch or pr.head_sha != command.pr.head_sha:
                raise ApprovalChanged("The bound PR is closed, missing, or its branch/HEAD changed.")
            if pr.pr_id not in (None, command.task.pr_id):
                raise ApprovalChanged("The bound PR now identifies a different task.")
            if self._unapproved_labels(command, pr.quarantine_labels):
                await self._approval_result(
                    command, "deferred", "The PR has another quarantine finding awaiting resolution."
                )
                return True
            # Recheck after GitHub awaits. There are no checkout/index writes on
            # either side of this check, even if an external editor writes later.
            reason = await self._approval_blocker(command)
            if reason:
                await self._approval_result(command, "deferred", reason)
                return True
            await self._commit_approval_state(command)
            return True
        except ApprovalChanged as exc:
            await self._approval_result(command, "failed", str(exc))
            return True
        except Exception as exc:
            await self._approval_result(command, "deferred", f"Application could not be verified: {exc}")
            return True

    async def _commit_approval_state(self, command: ApprovalCommand) -> None:
        key = approval_key(self.name, command.binding)
        cancellation = cause_key(self.name, command.task.pr_id)
        state_key = pipeline_state(self.name)
        stop_key = control_stop(self.name)
        upload_key = upload_pending(self.name)

        async def transaction(pipe: Any) -> RepoState:
            current = ApprovalCommand.model_validate_json(await pipe.get(key))
            raw = await pipe.get(state_key)
            persisted = RepoState.model_validate_json(raw) if raw else self.state
            if not matches_state(command, persisted):
                raise ApprovalChanged("Published task or PR changed while applying approval.")
            if current.status != "applied" and await pipe.get(upload_key):
                raise RuntimeError("Task upload arrived during approval application.")
            path = approval_task_path(Path(self.repo_path), command.task.task_file)
            if task_spec_content_hash(path.read_text(encoding="utf-8")) != command.task_fingerprint:
                raise ApprovalChanged("Task specification changed during approval application.")
            if persisted.user_paused or await pipe.get(stop_key):
                raise RuntimeError("Operator Pause or Stop arrived during application.")
            failure = failure_identity(await pipe.get(cancellation))
            if failure != command.failure and not (current.status == "applied" and not failure):
                raise ApprovalChanged("Pending failure changed; no unrelated failure was cleared.")
            if current.status == "applied" and persisted.state != PipelineState.WATCH:
                raise ApprovalChanged("A later pipeline transition superseded this approval.")
            state = (self.state if self._recovered else persisted).model_copy(deep=True)
            state.current_task = command.task.model_copy(update={"status": TaskStatus.DOING})
            state.current_queue = [
                state.current_task if task.pr_id == command.task.pr_id else task
                for task in (state.current_queue or [state.current_task])
            ]
            # Reconciliation must retain counters accumulated after acceptance.
            state.current_pr = persisted.current_pr.model_copy(deep=True)
            state.current_pr.diff_scanned_at_sha = None
            state.current_pr.is_escalated = False
            state.state = PipelineState.WATCH
            state.error_message = None
            # A concurrently published unrelated quarantine is another inhibitor.
            if self._unapproved_labels(command, state.current_pr.quarantine_labels):
                raise RuntimeError("Another quarantine finding appeared during application.")
            state.quarantined_prs.discard(command.pr.number)
            current.status = "applied"
            current.reason = "Daemon applied approval; watching the existing PR. CI and review gates remain required."
            pipe.multi()
            pipe.set(key, current.model_dump_json())
            pipe.set(state_key, state.model_dump_json())
            if failure:
                # RedisSuppressionStore.is_suppressed reads this same key;
                # clearing it atomically clears the bound MERGE suppression too.
                pipe.delete(cancellation)
                pipe.zrem(index_key(self.name), command.task.pr_id)
            return state

        self._approval_commit_uncertain = True
        self.state = await self.redis.transaction(
            transaction,
            key,
            cancellation,
            state_key,
            stop_key,
            upload_key,
            value_from_callable=True,
        )
        self._approval_commit_uncertain = False
        self._recovered = True
        self._approval_receipt = command.model_copy(update={"status": "applied"})
        logging.getLogger(__name__).info("Approval %s applied to existing PR #%s", command.binding, command.pr.number)

    @staticmethod
    def _approved_finding(command: ApprovalCommand) -> tuple[str, str]:
        payload = json.loads(command.failure)["payload"]
        category = payload.get("category") or payload.get("rule") or ""
        excerpt = payload.get("excerpt") or ""
        # CODING/FIX record the same finding as reason_text; WATCH uses fields.
        reason = payload.get("reason_text", "")
        if isinstance(reason, str) and reason.startswith("GUARDRAIL:"):
            parts = reason.split(":", 2)
            if len(parts) == 3:
                category = category or parts[1].strip()
                excerpt = excerpt or parts[2].strip()
        return category, excerpt

    @classmethod
    def _unapproved_labels(cls, command: ApprovalCommand, labels: set[str]) -> set[str]:
        category, _excerpt = cls._approved_finding(command)
        return labels - {quarantine_label_for_category(category)} if category else labels

    def _current_approval(self, pr: Any) -> ApprovalCommand | None:
        command = self._approval_receipt
        if command is None or pr.number != command.pr.number or pr.head_sha != command.pr.head_sha:
            return None
        path = approval_task_path(Path(self.repo_path), command.task.task_file)
        if task_spec_content_hash(path.read_text(encoding="utf-8")) != command.task_fingerprint:
            return None
        return command

    def _approval_filtered_pr(self, pr: Any) -> Any:
        command = self._current_approval(pr)
        if command is None:
            return pr
        return pr.model_copy(
            update={
                "quarantine_labels": self._unapproved_labels(command, pr.quarantine_labels),
                "is_escalated": False,
            }
        )

    def _approval_allows_violation(self, pr: Any, violation: Any) -> bool:
        command = self._current_approval(pr)
        if command is None:
            return False
        return self._approved_finding(command) == (violation.category, violation.excerpt)
