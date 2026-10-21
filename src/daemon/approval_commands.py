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
    approval_index,
    approval_key,
    approval_task_path,
    build_approval,
    enqueue_approval,
    failure_identity,
    list_approvals,
    matches_state,
)
from src.cancellation.storage import CancellationCause, cause_key, index_key, task_spec_content_hash
from src.daemon import git_ops
from src.daemon.fix_escalation import _is_label_already_exists_error
from src.daemon.quarantine import (
    CONTINUED_SIZE_LABEL,
    OBSOLETE_SIZE_LABEL,
    blocking_quarantine_labels,
    quarantine_label_for_category,
)
from src.github import gh_runner
from src.github import prs as gh_prs
from src.inhibitor import InhibitorType, derive_active_inhibitors
from src.keyspace import control_stop, pipeline_state, upload_pending
from src.models import PipelineState, RepoState, TaskStatus
from src.retry_commands import load_latest_retry_command


def bound_checkout_work_blocker(
    repo_path: str, command: ApprovalCommand, published_head_sha: str | None = None,
) -> str | None:
    """Protect checkout work and the bound PR ref without scanning unrelated refs.

    The PR HEAD is checked against GitHub before approval is committed. A
    matching local branch is therefore published even when its remote-tracking
    ref is stale. A different local branch tip is preserved for inspection.
    """
    dirty = git_ops._git(repo_path, "--no-optional-locks", "status", "--porcelain")
    if dirty.stdout.strip():
        return "Checkout has staged, unstaged or untracked work; preserving it until the operator resolves it."
    branch = command.pr.branch
    if not branch or branch != command.task.branch or branch.startswith("-") or ".." in branch:
        return "Bound task and PR branch are unavailable or disagree."
    bound_ref = git_ops._git(repo_path, "rev-parse", "--verify", f"refs/heads/{branch}", check=False)
    published_heads = {command.pr.head_sha, published_head_sha}
    if bound_ref.returncode == 0 and bound_ref.stdout.strip() not in published_heads:
        unpublished = git_ops._git(repo_path, "rev-list", f"refs/heads/{branch}", "--not", "--remotes")
        if unpublished.stdout.strip():
            return "Bound PR branch has local-only commits; preserving them until published or reviewed."
    if bound_ref.returncode not in (0, 128):
        return "Bound PR branch could not be verified; preserving checkout work."
    head = git_ops._git(repo_path, "rev-parse", "--verify", "HEAD").stdout.strip()
    if head not in published_heads:
        local = git_ops._git(repo_path, "rev-list", "HEAD", "--not", "--remotes")
        if local.stdout.strip():
            return "Checkout HEAD has local-only commits; preserving them until published or reviewed."
    return None


def is_size_only_cause(raw: str | bytes | None, state: RepoState | None = None) -> bool:
    """Recognize legacy size decisions without clearing mixed guardrail causes."""
    if not raw:
        return False
    cause = CancellationCause.from_redis(raw)
    payload = cause.payload
    if payload.get("subsource") != "guardrail":
        return False
    categories = {payload[key] for key in ("category", "rule") if payload.get(key)}
    reason = payload.get("reason_text", "")
    if isinstance(reason, str) and reason.startswith("GUARDRAIL:"):
        parts = reason.split(":", 2)
        if len(parts) != 3:
            return False
        categories.add(parts[1].strip())
    if categories == {"large_diff_threshold"}:
        return True
    return bool(
        not categories
        and payload.get("source") == "github_quarantine_label"
        and state is not None
        and state.current_pr is not None
        and state.error_message
        and "large_diff_threshold" in state.error_message
        and state.current_pr.quarantine_labels == {"quarantine:large_diff"}
    )


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
            if self._approval_head_refresh_uncertain:
                raw = await self.redis.get(pipeline_state(self.name))
                self.state = RepoState.model_validate_json(raw)
                self._approval_head_refresh_uncertain = False
            commands = await list_approvals(self.redis, self.name)
            await self._enqueue_size_only_recovery(commands)
            commands = await list_approvals(self.redis, self.name)
            self._approval_history = [c for c in commands if c.status == "applied" and c.active]
            for command in reversed(commands):
                if command.status == "applied" and not self._recovered:
                    raw = await self.redis.get(pipeline_state(self.name))
                    if raw:
                        persisted = RepoState.model_validate_json(raw)
                        if (
                            persisted.state == PipelineState.WATCH
                            and matches_state(command, persisted, check_head=False)
                        ):
                            # Restore the approved WATCH before header recovery
                            # sees the legacy ERROR task. A later HEAD still
                            # gets no permission from this old receipt.
                            self.state = persisted
                            self._recovered = True
                            if not await self.redis.get(cause_key(self.name, command.task.pr_id)):
                                return True
                if not command.active:
                    snapshot = self.state
                    if not self._recovered:
                        raw = await self.redis.get(pipeline_state(self.name))
                        if raw:
                            snapshot = RepoState.model_validate_json(raw)
                    if matches_state(command, snapshot, check_head=False):
                        if await self._inactive_approval_holds(command):
                            return True
                    elif self._recovered:
                        await self.redis.zrem(approval_index(self.name), command.binding)
                    continue
                if command.status == "applied" and self._recovered and not self._approval_commit_uncertain:
                    if not matches_state(command, self.state, check_head=False):
                        await self.redis.zrem(approval_index(self.name), command.binding)
                        continue
                    if self.state.state != PipelineState.WATCH:
                        continue
                    # Protect preserved work from the legacy dirty-tree reset on
                    # subsequent WATCH cycles too, not just the acceptance cycle.
                    cause = await self.redis.get(cause_key(self.name, command.task.pr_id))
                    if cause:
                        self._approval_receipt = None
                        if CancellationCause.from_redis(cause).payload.get("subsource") == "operator_reject":
                            await self._approval_result(command, "failed", "A later operator rejection ended approval.")
                        return True  # a newer failure must never inherit this permission
                    self._approval_receipt = command
                    try:
                        pr = await self._verified_bound_pr(command)
                        reason = await self._approval_blocker(command, pr.head_sha)
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
                    if pr.head_sha != command.pr.head_sha:
                        # A later push needs its own CI, scan and review result;
                        # retain the old command only for restart recovery.
                        self._approval_receipt = None
                    if await self._check_github_api_budget():
                        await self.handle_watch()
                    await self.publish_state()
                    return True
                return await self._apply_approval(command)
            return False
        except Exception as exc:
            self.log_event(f"[RECOVERY] Approval store/application unavailable; deferring cycle: {exc}.")
            return True

    async def _enqueue_size_only_recovery(self, commands: list[ApprovalCommand]) -> None:
        """Reuse the validated approval path for a persisted obsolete size block."""
        raw_state = await self.redis.get(pipeline_state(self.name))
        if not raw_state:
            return
        state = RepoState.model_validate_json(raw_state)
        if state.state != PipelineState.ERROR or state.current_task is None or state.current_pr is None:
            return
        raw_cause = await self.redis.get(cause_key(self.name, state.current_task.pr_id))
        if not is_size_only_cause(raw_cause, state):
            return
        if any(
            command.active and command.status in ("pending", "deferred")
            and command.task.pr_id == state.current_task.pr_id
            and command.pr.number == state.current_pr.number
            for command in commands
        ):
            return
        if state.url != self.repo_config.url:
            self.log_event("[RECOVERY] Size-only decision retained: repository binding changed.")
            return
        try:
            command = build_approval(self.name, state, raw_cause, Path(self.repo_path))
            await enqueue_approval(self.redis, command)
        except Exception as exc:
            self.log_event(f"[RECOVERY] Size-only decision retained: {exc}")

    async def _inactive_approval_holds(self, command: ApprovalCommand) -> bool:
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
        if not reason and Path(self.repo_path).exists():
            reason = bound_checkout_work_blocker(self.repo_path, command)
        if reason:
            await self._approval_result(command, "failed", f"Later operator action is waiting: {reason}")
            return True
        key = approval_key(self.name, command.binding)

        async def transaction(pipe: Any) -> None:
            current = ApprovalCommand.model_validate_json(await pipe.get(key))
            current.superseded = True
            current.reason = (
                "Approval superseded by a later operator action; normal reconciliation may continue."
            )
            pipe.multi()
            pipe.set(key, current.model_dump_json())
            pipe.zrem(approval_index(self.name), command.binding)

        await self.redis.transaction(transaction, key)
        return False

    async def _verified_bound_pr(self, command: ApprovalCommand) -> Any:
        prs = await asyncio.to_thread(
            gh_prs.get_open_prs, self.owner_repo, self.repo_config.allow_merge_without_checks,
        )
        pr = next((item for item in prs if item.number == command.pr.number), None)
        if pr is None:
            state = await asyncio.to_thread(gh_prs.get_pr_state, self.owner_repo, command.pr.number)
            if state in ("CLOSED", "MERGED"):
                raise ApprovalChanged("The bound PR is closed or merged.")
            raise RuntimeError("The bound PR is absent from the open-PR snapshot; waiting for GitHub evidence.")
        if pr.branch != command.pr.branch:
            raise ApprovalChanged("The bound PR branch changed.")
        if not pr.head_sha:
            raise RuntimeError("The current PR HEAD is unavailable; waiting for a verified GitHub snapshot.")
        if pr.pr_id not in (None, command.task.pr_id):
            raise ApprovalChanged("The bound PR now identifies a different task.")
        return pr

    async def _approval_blocker(
        self, command: ApprovalCommand, published_head_sha: str | None = None,
    ) -> str | None:
        if not self.repo_config.active:
            return "Repository is disabled."
        if (
            self._current_coder_process is not None
            or command.task.pr_id in self._user_stopped_task_pr_ids
        ):
            return "Coder execution or Stop is still active."
        if self.state.pending_queue_sync_branch:
            return "Queue synchronization is pending."
        safe_states = {PipelineState.ERROR, PipelineState.IDLE, PipelineState.WATCH, PipelineState.PAUSED}
        if command.status == "applied":
            safe_states.add(PipelineState.MERGE)
        if self.state.state not in safe_states:
            return "Work is active; waiting for the execution boundary."
        raw = await self.redis.get(pipeline_state(self.name))
        if raw:
            persisted = RepoState.model_validate_json(raw)
            if persisted.state not in safe_states:
                return "Prior execution ownership is uncertain; waiting for daemon reconciliation."
            self.state.user_paused = persisted.user_paused
        # Staged uploads do not change the current task yet. Once permission
        # is applied, WATCH must finish so the existing IDLE upload consumer
        # can run. A competing upload retires an unapplied request; its hold
        # yields to the existing upload consumer once work preservation is safe.
        if command.status != "applied" and await self.redis.get(upload_pending(self.name)):
            raise ApprovalChanged("Task upload superseded this approval; revised requirements need a new decision.")
        if await self.redis.get(control_stop(self.name)):
            return "Operator Stop is active."
        inhibitors = await derive_active_inhibitors(self.state, self.redis, self.app_config.daemon)
        inhibitors = [
            item for item in inhibitors if item.inhibitor_type not in (
                InhibitorType.RATE_LIMIT, InhibitorType.SPEND_CEILING, InhibitorType.GITHUB_BUDGET_SLOWDOWN,
            )
        ]  # WATCH polling keeps its own GitHub budget and coder dispatch gates.
        if inhibitors:
            return "Active inhibitor(s): " + "; ".join(item.reason_text for item in inhibitors)
        reason = checkout_process_blocker(self.repo_path)
        if reason:
            return reason
        if self._stop_requested:
            if not raw:
                return "Prior Stop state is unavailable; waiting for operator reconciliation."
            # Resume clears persisted Pause/Stop, but FIX's local latch survives.
            # Only reconcile it after controls and process quiescence agree.
            self._stop_requested = False
        if not Path(self.repo_path).exists():
            # Restore an absent checkout without the ordinary clone helper's
            # partial-clone deletion or scaffolding. Git refuses a destination
            # that acquired files meanwhile; failures retain any partial work.
            await asyncio.to_thread(
                subprocess.run,
                ["git", "clone", "--branch", command.pr.branch, "--", self.repo_config.url, self.repo_path],
                capture_output=True, text=True, check=True, timeout=120,
            )
        dirty = git_ops._git(self.repo_path, "--no-optional-locks", "status", "--porcelain")
        if dirty.stdout.strip():
            return "Checkout has staged, unstaged or untracked work; preserving it until the operator resolves it."
        if command.status == "applied" and published_head_sha != command.pr.head_sha:
            # This receipt expires on a later push. The new object may not be
            # present locally, and WATCH will validate its own current HEAD.
            return bound_checkout_work_blocker(self.repo_path, command, published_head_sha)
        path = approval_task_path(Path(self.repo_path), command.task.task_file)
        if task_spec_content_hash(path.read_text(encoding="utf-8")) != command.task_fingerprint:
            raise ApprovalChanged("Task specification changed; this approval cannot authorize revised work.")
        published_task = git_ops._git(
            self.repo_path, "show", f"{published_head_sha or command.pr.head_sha}:{command.task.task_file}",
            check=False,
        )
        if published_task.returncode != 0:
            return "Published task generation is unavailable in the checkout; preserving the PR."
        if task_spec_content_hash(published_task.stdout) != command.task_fingerprint:
            raise ApprovalChanged("Published PR and task generation disagree.")
        return bound_checkout_work_blocker(self.repo_path, command, published_head_sha)

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
                if (
                    command.status == "applied" and failure
                    and json.loads(failure)["payload"].get("subsource") == "guardrail"
                ):
                    raw = await self.redis.get(pipeline_state(self.name))
                    if raw and matches_state(command, RepoState.model_validate_json(raw)):
                        self._approval_receipt = None
                        await self._approval_result(
                            command, "deferred", "A newer guardrail decision is awaiting action."
                        )
                        return True
                raise ApprovalChanged("Pending failure changed; no unrelated failure was cleared.")
            pr = await self._verified_bound_pr(command)
            if pr.head_sha != command.pr.head_sha:
                await self._publish_refreshed_guardrail_head(command, pr.head_sha)
                raise ApprovalChanged(
                    "PR HEAD changed; daemon state was refreshed. Review the current PR and approve again."
                )
            reason = await self._approval_blocker(command, pr.head_sha)
            if reason:
                await self._approval_result(command, "deferred", reason)
                return True
            if self._unapproved_labels(command, pr.quarantine_labels):
                await self._approval_result(
                    command, "deferred", "The PR has another quarantine finding awaiting resolution."
                )
                return True
            # Recheck after GitHub awaits. There are no checkout/index writes on
            # either side of this check, even if an external editor writes later.
            reason = await self._approval_blocker(command, pr.head_sha)
            if reason:
                await self._approval_result(command, "deferred", reason)
                return True
            raw_state = await self.redis.get(pipeline_state(self.name))
            persisted = RepoState.model_validate_json(raw_state) if raw_state else None
            await self._commit_approval_state(
                command, pr.quarantine_labels, is_size_only_cause(command.failure, persisted)
            )
            return True
        except ApprovalChanged as exc:
            await self._approval_result(command, "failed", str(exc))
            return True
        except Exception as exc:
            await self._approval_result(command, "deferred", f"Application could not be verified: {exc}")
            return True

    async def _refresh_guardrail_head_after_fix(self) -> None:
        """Publish the pushed implementation/metadata head, not the pre-FIX head."""
        current = self.state.current_pr
        if current is None:
            return
        try:
            prs = await asyncio.to_thread(
                gh_prs.get_open_prs, self.owner_repo, self.repo_config.allow_merge_without_checks,
            )
            fresh = next((p for p in prs if p.number == current.number and p.branch == current.branch), None)
            if fresh is not None and fresh.head_sha:
                current.head_sha = fresh.head_sha
        except Exception as exc:
            self.log_event(f"[RECOVERY] Guardrail PR head refresh deferred: {exc}")

    async def _publish_refreshed_guardrail_head(self, command: ApprovalCommand, head_sha: str) -> None:
        """Refresh a stale error snapshot without rebinding the accepted request."""
        state_key = pipeline_state(self.name)
        cancellation = cause_key(self.name, command.task.pr_id)

        async def transaction(pipe: Any) -> RepoState:
            persisted = RepoState.model_validate_json(await pipe.get(state_key))
            if (
                not matches_state(command, persisted, check_head=False)
                or persisted.current_pr.head_sha not in (command.pr.head_sha, head_sha)
                or failure_identity(await pipe.get(cancellation)) != command.failure
            ):
                raise ApprovalChanged("Pending decision changed while refreshing its PR head.")
            state = (self.state if self._recovered else persisted).model_copy(deep=True)
            state.user_paused = persisted.user_paused
            state.current_pr.head_sha = head_sha
            pipe.multi()
            pipe.set(state_key, state.model_dump_json())
            return state

        self._approval_head_refresh_uncertain = True
        self.state = await self.redis.transaction(transaction, state_key, cancellation, value_from_callable=True)
        self._approval_head_refresh_uncertain = False

    async def _commit_approval_state(
        self, command: ApprovalCommand, live_labels: set[str], size_only: bool,
    ) -> None:
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
            if current.status == "applied" and persisted.state not in (PipelineState.WATCH, PipelineState.MERGE):
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
        if (
            size_only
            and OBSOLETE_SIZE_LABEL in live_labels
        ):
            try:
                gh_runner.run_gh(
                    [
                        "label", "create", CONTINUED_SIZE_LABEL, "--color", "0E8A16",
                        "--description", "Legacy size decision continued; CI and review still required",
                    ],
                    repo=self.owner_repo,
                )
            except Exception as exc:
                if not _is_label_already_exists_error(exc):
                    self.log_event(f"[RECOVERY] Size label migration deferred: {exc}")
                    return
            try:
                gh_runner.run_gh(
                    ["pr", "edit", str(command.pr.number), "--add-label", CONTINUED_SIZE_LABEL],
                    repo=self.owner_repo,
                )
                gh_runner.run_gh(
                    ["pr", "edit", str(command.pr.number), "--remove-label", OBSOLETE_SIZE_LABEL],
                    repo=self.owner_repo,
                )
            except Exception as exc:
                self.log_event(f"[RECOVERY] Obsolete size label removal deferred: {exc}")
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

    @staticmethod
    def _approval_scope(command: ApprovalCommand) -> tuple[Any, ...]:
        return (
            command.repo_slug, command.repo_url, command.task.pr_id, command.task.task_file,
            command.pr.number, command.pr.branch, command.pr.head_sha, command.task_fingerprint,
        )

    def _matching_approvals(self, command: ApprovalCommand) -> list[ApprovalCommand]:
        # Keep independently approved findings at the same task/spec/PR/HEAD.
        # This grants no lifetime across pushes or revised requirements.
        return [command] + [
            previous for previous in self._approval_history
            if previous.binding != command.binding and self._approval_scope(previous) == self._approval_scope(command)
        ]

    def _unapproved_labels(self, command: ApprovalCommand, labels: set[str]) -> set[str]:
        approved = {self._approved_finding(c)[0] for c in self._matching_approvals(command)}
        return blocking_quarantine_labels(labels) - {
            quarantine_label_for_category(category) for category in approved if category
        }

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
            return pr.model_copy(update={"quarantine_labels": blocking_quarantine_labels(pr.quarantine_labels)})
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
        return any(
            self._approved_finding(c) == (violation.category, violation.excerpt)
            for c in self._matching_approvals(command)
        )
