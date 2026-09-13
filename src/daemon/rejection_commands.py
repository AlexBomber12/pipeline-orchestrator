"""Reconcile final operator rejection before any ordinary scheduler work."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from src.approval_commands import approval_index, approval_key, list_approvals
from src.cancellation.storage import CancellationCause, cause_key
from src.daemon import git_ops
from src.daemon.approval_commands import checkout_process_blocker
from src.daemon.attempt_processes import stop_attempt_children
from src.daemon.attempt_prs import attempt_branch_head, attempt_pr_info, discover_attempt_pr
from src.github import gh_runner
from src.keyspace import pipeline_state
from src.models import PipelineState, RepoState, TaskStatus
from src.rejection_commands import (
    RejectionCommand,
    list_rejections,
    rejection_key,
)
from src.task_attempts import AttemptChanged, load_attempt, save_attempt


def rejection_pr_details(
    owner_repo: str,
    command: RejectionCommand,
    base: str,
    *,
    require_head_match: bool = True,
) -> dict:
    """Uncached exact-PR read, including repository and merge identity."""
    assert command.pr is not None
    data = gh_runner.run_gh(["api", f"repos/{owner_repo}/pulls/{command.pr.number}"])
    if not isinstance(data, dict):
        raise AttemptChanged("PR lookup returned no verifiable identity.")
    head, target = data.get("head", {}), data.get("base", {})
    if (
        data.get("number") != command.pr.number
        or head.get("ref") != command.task.branch
        or head.get("repo", {}).get("full_name", "").casefold() != owner_repo.casefold()
        or target.get("repo", {}).get("full_name", "").casefold() != owner_repo.casefold()
        or target.get("ref") != base
        or not head.get("sha")
        or (require_head_match and head.get("sha") != command.pr.head_sha)
        or data.get("state") not in {"open", "closed"}
        or "merged_at" not in data
    ):
        raise AttemptChanged("Bound PR repository, branch, base, or HEAD changed; closure is deferred.")
    return data


class RejectionCommandMixin:
    async def _save_rejection(self, command: RejectionCommand, status: str, reason: str) -> None:
        command.status = status
        command.reason = reason
        await self.redis.set(rejection_key(self.name, command.binding), command.model_dump_json())
        self.log_event(f"[RECOVERY] {command.task.pr_id}: {reason}")

    async def _attempt_execution_blocked(self) -> bool:
        task = self.state.current_task
        if task is None:
            return False
        try:
            raw_cause = await self.redis.get(cause_key(self.name, task.pr_id))
            if raw_cause and CancellationCause.from_redis(raw_cause).payload.get("subsource") == "operator_reject":
                return True
            attempt = await load_attempt(self.redis, self.name, task.pr_id)
            if attempt and (
                attempt.rejection
                or attempt.admission_pending
                or attempt.completed
                or (task.attempt_id and task.attempt_id != attempt.attempt_id)
            ):
                self.log_event(f"[RECOVERY] Execution fenced for {task.pr_id}; waiting for reconciliation.")
                return True
        except Exception:
            self.log_event("[RECOVERY] Attempt ownership unavailable; execution deferred.")
            return True
        return False

    async def _consume_rejection_commands(self) -> bool:
        """Return True while this checkout is still owned by rejection work.

        Scheduler serialization prevents a second cycle in the same checkout.
        The process monitor sees the permanent attempt fence during a long
        CODING/FIX call. Unknown orphan children hold the checkout visibly.
        """
        try:
            commands = await list_rejections(self.redis, self.name)
            for command in commands:
                attempt = await load_attempt(self.redis, self.name, command.task.pr_id)
                if attempt is None:
                    raise AttemptChanged("Rejected attempt receipt is missing.")
                if attempt.attempt_id != command.attempt_id:
                    continue
                if command.released:
                    continue
                if not self._recovered:
                    raw = await self.redis.get(pipeline_state(self.name))
                    if raw:
                        self.state = RepoState.model_validate_json(raw)
                await self._reconcile_rejection(command)
                return True
            return False
        except Exception as exc:
            self.log_event(f"[RECOVERY] Reconciliation deferred ({type(exc).__name__}); receipt remains pending.")
            return True

    async def _reconcile_rejection(self, command: RejectionCommand) -> None:
        try:
            if command.repo_url != self.repo_config.url:
                raise AttemptChanged("Repository configuration changed; rejection is deferred.")
            task = self.state.current_task
            if task and (task.pr_id != command.task.pr_id or task.attempt_id not in (None, command.attempt_id)):
                raise AttemptChanged("Checkout is owned by another attempt.")
            command.stopping_started_at = command.stopping_started_at or datetime.now(timezone.utc)
            await self._save_rejection(command, "stopping", "Stopping attempt-owned execution.")
            await self._terminate_current_coder()
            elapsed = (datetime.now(timezone.utc) - command.stopping_started_at).total_seconds()
            blocker = stop_attempt_children(
                command.attempt_id,
                force=elapsed >= self.app_config.daemon.coder_terminate_grace_sec,
            ) or checkout_process_blocker(self.repo_path)
            if blocker:
                await self._save_rejection(command, "deferred", blocker)
                return
            # Capture the base before any sync/admission. A Git edit already
            # visible when Reject was accepted cannot count as a later rewrite.
            if not command.base_commit:
                command.base_commit = git_ops._git(
                    self.repo_path,
                    "rev-parse",
                    f"refs/remotes/origin/{self.repo_config.branch}",
                ).stdout.strip()
            if command.pr is None:
                attempt = await load_attempt(self.redis, self.name, command.task.pr_id)
                data = await asyncio.to_thread(
                    discover_attempt_pr,
                    self.repo_path,
                    self.owner_repo,
                    self.repo_config.branch,
                    attempt,
                )
                if data is not None:
                    command.pr = attempt_pr_info(data, self.owner_repo, attempt)
                    command.initial_head_sha = command.pr.head_sha
                    command.branch_head = command.pr.head_sha
                    await self._save_rejection(command, "closing", "Attempt PR identified; verifying exact PR closure.")
                    updated = attempt.model_copy(update={"pr_number": command.pr.number, "pr_creation_pending": False})
                    await save_attempt(self.redis, self.name, updated, expected=attempt)
                elif attempt.pr_creation_pending:
                    raise AttemptChanged("PR creation acknowledgement is unresolved; exact PR identity is required.")
                elif not command.absence_confirmed:
                    command.absence_confirmed = True
                    await self._save_rejection(
                        command, "closing", "Confirming that no PR was created; rechecking next cycle."
                    )
                    return
            if command.pr is not None:
                data = await asyncio.to_thread(
                    rejection_pr_details,
                    self.owner_repo,
                    command,
                    self.repo_config.branch,
                    require_head_match=False,
                )
                if data["merged_at"]:
                    attempt = await load_attempt(self.redis, self.name, command.task.pr_id)
                    updated = attempt.model_copy(update={"completed": True})
                    await save_attempt(self.redis, self.name, updated, expected=attempt)
                    await self._save_rejection(
                        command, "merged", "PR already merged; rejection cannot succeed and task ID cannot be reused."
                    )
                    await self._release_rejected_attempt(command, merged=True)
                    return
                if data["head"]["sha"] != command.pr.head_sha:
                    # The same attempt may finish a push after its decision was
                    # rendered. Execution is now quiescent; require local owner
                    # evidence and matching remote refs before rebinding it.
                    owned_head = attempt_branch_head(self.repo_path, command.task.branch, require_local=True)
                    if owned_head != data["head"]["sha"]:
                        raise AttemptChanged("Updated PR HEAD does not match the attempt-owned branch.")
                    command.initial_head_sha = command.initial_head_sha or command.pr.head_sha
                    command.pr = command.pr.model_copy(update={"head_sha": owned_head})
                    command.branch_head = owned_head
                    await self._save_rejection(
                        command,
                        "closing",
                        "Verified updated HEAD of the same attempt; confirming exact PR closure.",
                    )
                if data["state"] == "open":
                    now = datetime.now(timezone.utc)
                    if command.close_requested_at and now - command.close_requested_at < timedelta(seconds=60):
                        await self._save_rejection(
                            command, "closing", "PR closure awaiting confirmation; next close attempt is deferred."
                        )
                        return
                    command.close_requested_at = now
                    await self._save_rejection(
                        command, "closing", "PR closure requested; awaiting an independent confirmation."
                    )
                    # No comment: an ambiguous response can be reconciled and
                    # retried without duplicating operator comments.
                    await asyncio.to_thread(
                        gh_runner.run_gh,
                        ["pr", "close", str(command.pr.number)],
                        self.owner_repo,
                    )
                    data = await asyncio.to_thread(
                        rejection_pr_details, self.owner_repo, command, self.repo_config.branch
                    )
                    if data["merged_at"] or data["state"] != "closed":
                        raise AttemptChanged("PR closure remains unconfirmed; reconciliation will continue.")
                command.branch_head = data["head"]["sha"]
            if command.pr is None:
                # A pre-PR attempt has an explicit UUID and two negative PR
                # observations. Freeze its now-quiescent branch refs for the
                # same exact-SHA cleanup used for closed-PR attempts.
                command.branch_head = attempt_branch_head(self.repo_path, command.task.branch)
            blocker = checkout_process_blocker(self.repo_path)
            if blocker or self._current_coder_process is not None:
                raise AttemptChanged(blocker or "Coder process remains active.")
            # Release only a clean, proven checkout. Dirty/untracked unrelated
            # work is never swept by the legacy preflight recovery path.
            if git_ops._git(self.repo_path, "status", "--porcelain").stdout.strip():
                raise AttemptChanged(
                    "PR is closed; checkout has uncommitted files. Confirm ownership and clear them before release."
                )
            git_ops._git(self.repo_path, "checkout", self.repo_config.branch)
            await self._save_rejection(
                command,
                "rejected",
                (
                    "Attempt rejected; PR closed without merge. "
                    if command.pr
                    else "Attempt rejected; no PR was created. "
                )
                + "Manually rewrite or remove unfinished tasks and maintain dependencies.",
            )
            await self._release_rejected_attempt(command)
        except Exception as exc:
            reason = (
                str(exc)
                if isinstance(exc, AttemptChanged)
                else f"Closure or checkout verification unavailable ({type(exc).__name__})."
            )
            await self._save_rejection(command, "deferred", reason)

    async def _release_rejected_attempt(self, command: RejectionCommand, *, merged: bool = False) -> None:
        for approval in await list_approvals(self.redis, self.name):
            if approval.task.pr_id == command.task.pr_id:
                approval.active = False
                approval.superseded = True
                await self.redis.set(approval_key(self.name, approval.binding), approval.model_dump_json())
                await self.redis.zrem(approval_index(self.name), approval.binding)
        self._approval_receipt = None
        self._approval_history = []
        self._current_run_record = None
        self._active_retry_command_id = None
        if command.pr:
            self.state.quarantined_prs.discard(command.pr.number)
        self.state.current_queue = [
            task.model_copy(update={"status": TaskStatus.DONE if merged else TaskStatus.ERROR})
            if task.pr_id == command.task.pr_id
            else task
            for task in (self.state.current_queue or [])
        ]
        self.state.current_task = None
        self.state.current_pr = None
        self.state.error_message = None
        self.state.skip_ai_error_diagnose = False
        self.state.state = PipelineState.IDLE
        self._recovered = True
        await self.publish_state()
        command.released = True
        await self.redis.set(rejection_key(self.name, command.binding), command.model_dump_json())
