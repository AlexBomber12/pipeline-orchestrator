"""FIX state handler with idle timeout and in-flight breach monitoring."""

from __future__ import annotations

import asyncio
import logging
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

from src.approval_commands import load_approval, matches_state
from src.branch_context import BranchContext
from src.cancellation import (
    CancellationCause,
    safe_record_cancellation_cause,
)
from src.cancellation.storage import cause_key, task_spec_content_hash
from src.daemon import (
    fix_codex_trigger,
    fix_escalation,
    fix_push_verify,
    fix_supervision,
    git_ops,
    publication,
)
from src.daemon.approval_commands import (
    bound_checkout_work_blocker_for_ref,
    checkout_process_blocker,
)
from src.daemon.ci_attempts import (
    actions_job_identity,
    attempt_scope,
    canonical_ci_incident,
    matching_failed_job,
    read_actions_run,
)
from src.daemon.guardrails import scan_stdout
from src.daemon.handlers.breach import BreachMixin
from src.daemon.quarantine import (
    apply_quarantine_label_for_violation,
)
from src.daemon.recovery_policy import BoundedRecoveryPolicy
from src.daemon.selector import CoderSelectionUnavailable
from src.github import cache as gh_cache
from src.github import checks as gh_checks
from src.github import gh_runner
from src.github import prs as gh_prs
from src.github.ci_evidence import _latest, _status_producer, evaluate_ci_evidence
from src.github.reviewer_policy import ReviewerPolicy
from src.keyspace import control_stop, pipeline_state
from src.models import CIStatus, PipelineState, PRInfo, RepoState, ReviewStatus
from src.retry import retry_transient
from src.subsource_registry import SuppressionReason

logger = logging.getLogger(__name__)

_FIX_CI_LOG_TRUNCATE_CHARS = 5000
_GITHUB_ACTIONS_APP_ID = 15368


def _current_ci_failure(
    repo: str, pr_number: int, sha: str, required_checks: tuple[str, ...] | None,
    empty_is_success: bool, trusted_statuses: dict[str, list[str]] | None = None,
    observed_attempts: dict[int, int] | None = None,
) -> dict | None:
    """Return bound, current terminal failure evidence for a live PR head."""
    if len(sha) != 40 or gh_prs.get_pr_metadata(repo, pr_number).get("head_sha") != sha:
        return None
    retrieval = gh_checks._retrieve_ci_status_evidence(repo, sha, fresh=True)
    if not retrieval.evidence.sources_complete or not retrieval.status_source.sha_matches:
        return None
    raw_statuses = retrieval.status_payload.get("statuses", [])
    if not isinstance(raw_statuses, list) or any(not isinstance(s, dict) for s in raw_statuses):
        return None
    trusted_statuses = trusted_statuses or {}
    if any(
        status.get("context") in trusted_statuses
        and isinstance(status.get("id"), int) and status["id"] > 0
        and _status_producer(status) is None
        for status in raw_statuses
    ):
        # The combined-status endpoint can omit both creator and app_id for
        # App-posted statuses. Bind the producer from the status-history API
        # only when that complete response contains the same immutable ID,
        # context, state, and timestamp on this exact commit.
        history = gh_cache._etag_get_paginated_evidence(
            f"repos/{repo}/commits/{sha}/statuses?per_page=100"
        )
        if history.complete:
            bound = []
            for status in raw_statuses:
                if (
                    status.get("context") not in trusted_statuses
                    or not isinstance(status.get("id"), int)
                    or status["id"] <= 0
                    or _status_producer(status) is not None
                ):
                    bound.append(status)
                    continue
                matches = [
                    item for item in history.items
                    if item.get("id") == status.get("id")
                    and item.get("context") == status.get("context")
                    and item.get("state") == status.get("state")
                    and item.get("created_at") == status.get("created_at")
                    and item.get("sha", sha) == sha
                ]
                if len(matches) == 1 and _status_producer(matches[0]) is not None:
                    status = {**status, "app_id": matches[0].get("app_id"),
                              "creator": matches[0].get("creator")}
                bound.append(status)
            raw_statuses = bound
    evidence = evaluate_ci_evidence(
        repo=repo, pr_number=pr_number, sha=sha,
        check_runs=retrieval.check_runs, statuses=raw_statuses,
        check_runs_complete=retrieval.check_runs_source.complete,
        statuses_complete=retrieval.status_source.complete,
        required_contexts=required_checks,
        observed_at=retrieval.evidence.observed_at,
        empty_is_success=empty_is_success,
    )
    if evidence.policy_result != CIStatus.FAILURE:
        return None
    # A failing status or third-party check can name itself arbitrarily. Admit
    # only configured Actions jobs or an exact configured status/producer pair.
    required = set(required_checks or ())
    failures = [
        ctx for name, producers in _latest(evidence.contexts).items()
        for ctx in producers.values()
        if ctx.state == "failure" and ctx.run_id is not None and (
            (not required or name in required)
            and ctx.producer == f"app:{_GITHUB_ACTIONS_APP_ID}"
            and ctx.run_id[0] == "check"
            or ctx.run_id[0] == "status"
            and ctx.producer in trusted_statuses.get(name, ())
        )
    ]
    contexts = []
    for ctx in failures:
        source, identity = ctx.run_id or (None, None)
        run = next((r for r in retrieval.check_runs if source == "check" and r.get("id") == identity), None)
        job_identity = actions_job_identity(repo, run) if source == "check" and run else None
        if source == "check" and job_identity is None:
            continue
        context = {
            "name": ctx.name, "producer": ctx.producer, "source": source,
            "check_id": identity, "attempt": ctx.attempt, "state": ctx.state,
            "workflow_run_id": job_identity[0] if job_identity else None,
            "job_id": job_identity[1] if job_identity else None,
        }
        if source == "check":
            workflow = _current_failed_actions_job(repo, sha, context, observed_attempts)
            if workflow is None:
                continue
            context["attempt"] = workflow["attempt"]
        contexts.append(context)
    if not contexts:
        return None
    return {"repo": repo, "pr_number": pr_number, "head_sha": sha, "contexts": contexts}


def _current_failed_actions_job(
    repo: str, sha: str, context: dict,
    observed_attempts: dict[int, int] | None = None,
) -> dict | None:
    """Bind an Actions check to the latest failed attempt and its exact job."""
    run_id = context.get("workflow_run_id")
    job_id = context.get("job_id")
    if not isinstance(run_id, int) or not isinstance(job_id, int):
        return None
    run = read_actions_run(repo, sha, run_id, observed_attempts)
    if run is None:
        return None
    attempt = run["attempt"]
    if context.get("attempt") is not None and context["attempt"] != attempt:
        return None
    if not matching_failed_job(run, job_id, context.get("name")):
        return None
    return run


def _fetch_failed_ci_logs(
    repo: str, failure: dict, observed_attempts: dict[str, int] | None = None,
) -> str | None:
    """Read logs only for a workflow job matching admitted CI evidence."""
    sha = failure.get("head_sha")
    for context in failure.get("contexts", []):
        run_id = context.get("workflow_run_id")
        job_id = context.get("job_id")
        if not isinstance(run_id, int) or not isinstance(job_id, int):
            continue
        run = _current_failed_actions_job(
            repo, sha, context, observed_attempts,
        )
        if run is None:
            continue
        try:
            logs = gh_runner.run_gh(
                ["run", "view", str(run_id), "--attempt", str(run["attempt"]),
                 "--job", str(job_id), "--log-failed"],
                repo=repo, timeout=60,
            )
        except (RuntimeError, subprocess.TimeoutExpired, OSError):
            continue
        if isinstance(logs, str) and logs:
            return (
                f"[truncated]\n{logs[-_FIX_CI_LOG_TRUNCATE_CHARS:]}"
                if len(logs) > _FIX_CI_LOG_TRUNCATE_CHARS else logs
            )
    return None


class FixMixin(BreachMixin):
    """FIX FEEDBACK handler with idle timeout and breach monitoring."""

    async def _ci_only_trigger_obsolete(self) -> bool:
        """Revalidate only a structured CI-only invocation against its PR."""
        snapshot = self.state.active_invocation
        pr = self.state.current_pr
        trigger = snapshot.fix_trigger if snapshot is not None else None
        if (
            not isinstance(trigger, dict) or trigger.get("review") is not False
            or not isinstance(trigger.get("ci"), dict) or pr is None
        ):
            return False
        admitted = trigger["ci"]
        if (
            admitted.get("repo") != self.owner_repo
            or admitted.get("pr_number") != pr.number
            or admitted.get("head_sha") != pr.head_sha
        ):
            return False
        metadata = await asyncio.to_thread(
            gh_prs.get_pr_metadata, self.owner_repo, pr.number,
        )
        if metadata.get("head_sha") != pr.head_sha:
            return False
        retrieval = await asyncio.to_thread(
            gh_checks._retrieve_ci_status_evidence,
            self.owner_repo, pr.head_sha, fresh=True,
        )
        if not retrieval.evidence.sources_complete or not retrieval.status_source.sha_matches:
            return False
        return gh_checks._classify_ci_retrieval(
            retrieval,
            required_contexts=self.repo_config.required_checks,
            empty_is_success=self.repo_config.allow_merge_without_checks,
        ) in {CIStatus.SUCCESS, CIStatus.PENDING}

    async def _approved_guardrail_fix_attempt(self, record, current_pr):
        """Return the bound approval only for its finalized guardrail run.

        The run's head_sha is checkout telemetry and can predate the published
        PR head. Approval and the live remote check bind the executable head.
        """
        command = self._current_approval(current_pr)
        task = self.state.current_task
        invocation = self.state.active_invocation
        if (
            command is None or command.status != "applied" or not command.active
            or task is None or not matches_state(command, self.state)
            or task.branch != current_pr.branch
            or current_pr.pr_id not in (None, task.pr_id)
            or record.task_id != task.pr_id or record.repo_name != self.name
            or record.ended_at is None or record.cause != "ESCALATE"
            or record.cause_subsource != "guardrail"
            or record.task_spec_hash not in ("", command.task_fingerprint)
            or invocation is None or invocation.run_id != record.run_id
            or invocation.attempt_index != record.attempt_index
            or not record.invocations or record.invocations[-1] != invocation
            or self.state.user_paused or not self.repo_config.active
            or self._current_coder_process is not None
        ):
            return None
        return command if await self._approved_fix_checkout_safe(command, current_pr) else None

    async def _approved_fix_checkout_safe(
        self, command, current_pr, *, require_fetched_head: bool = False,
    ) -> bool:
        """Recheck approval and checkout ownership at destructive/launch boundaries."""
        task = self.state.current_task
        if (
            # This launch was admitted against the old invocation above. Its new
            # snapshot must not inherit violation authority, but still needs the
            # existing checkout preservation checks before starting the coder.
            task is None or self._current_approval(current_pr, check_attempt=False) is None
            or not matches_state(command, self.state)
            or self.state.user_paused or not self.repo_config.active
            or self._current_coder_process is not None
        ):
            return False
        try:
            persisted = await load_approval(self.redis, self.name, command.binding)
            bound = bool(
                persisted is not None and persisted.status == "applied" and persisted.active
                and persisted.binding == command.binding
                and persisted.failure == command.failure
                and self._approval_scope(persisted) == self._approval_scope(command)
                and not await self.redis.get(cause_key(self.name, task.pr_id))
                and not await self.redis.get(control_stop(self.name))
                and not checkout_process_blocker(self.repo_path)
                and not bound_checkout_work_blocker_for_ref(
                    self.repo_path, current_pr.branch, {current_pr.head_sha},
                )
            )
            if not bound:
                return False
            remote_ref = f"refs/heads/{current_pr.branch}"
            live = git_ops._git(
                self.repo_path, "ls-remote", "--exit-code", "origin", remote_ref,
                check=False,
            )
            if live.returncode != 0 or live.stdout.strip() != f"{command.pr.head_sha}\t{remote_ref}":
                return False
            if require_fetched_head:
                fetched = git_ops._git(
                    self.repo_path, "rev-parse", "--verify",
                    f"refs/remotes/origin/{current_pr.branch}", check=False,
                )
                if fetched.returncode != 0 or fetched.stdout.strip() != command.pr.head_sha:
                    return False
            return True
        except (OSError, RuntimeError, ValueError, subprocess.SubprocessError):
            return False

    async def _park_persisted_fix_invocation(self, record) -> bool:
        """Keep uncertain execution ownership binding even after approval expires."""
        if (
            record is not None and record.ended_at is None and record.invocations
            and record.run_id != getattr(self, "_completed_fix_run_id", None)
        ):
            raw_state = await self.redis.get(pipeline_state(self.name))
            if raw_state:
                persisted = RepoState.model_validate_json(raw_state)
                if (
                    persisted.state == PipelineState.FIX
                    and persisted.active_invocation is not None
                    and persisted.active_invocation.run_id == record.run_id
                ):
                    await self._transition_to_error(
                        "Persisted FIX invocation ownership is uncertain; operator Retry required",
                        save_run_record_as=None, log_prefix="[FIX]",
                        commit_task_status=True,
                    )
                    return True
        return False

    async def _recover_fix_prelaunch(self, admitted, previous, approval) -> bool:
        """Reuse a saved invocation only when WATCH still owns the old run."""
        if (
            len(admitted.invocations) != 1
            or admitted.fix_iterations != previous.fix_iterations + 1
            or admitted.invocations[0].phase != "fix"
            or admitted.invocations[0].run_id != admitted.run_id
            or admitted.invocations[0].attempt_index != admitted.attempt_index
            or admitted.invocations[0].fix_iteration != admitted.fix_iterations
        ):
            return False
        raw = await self.redis.get(pipeline_state(self.name))
        if not raw:
            return False
        persisted = RepoState.model_validate_json(raw)
        if approval is not None:
            bound = matches_state(approval, persisted)
        else:
            task, pr = self.state.current_task, self.state.current_pr
            prior_task, prior_pr = persisted.current_task, persisted.current_pr
            bound = bool(
                task is not None and pr is not None
                and prior_task is not None and prior_pr is not None
                and task.pr_id == prior_task.pr_id and task.branch == prior_task.branch
                and task.task_file == prior_task.task_file
                and pr.number == prior_pr.number and pr.branch == prior_pr.branch
                and pr.head_sha == prior_pr.head_sha
            )
        return bool(
            persisted.state == PipelineState.WATCH
            and bound
            and persisted.active_invocation == self.state.active_invocation
            and persisted.active_invocation == previous.invocations[-1]
        )

    async def _monitor_fix_idle(
        self,
        pr_number: int,
        idle_limit: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        idle_flag: dict[str, bool | float],
    ) -> None:
        """Thin wrapper — see ``fix_supervision.monitor_fix_idle``."""
        await fix_supervision.monitor_fix_idle(
            self, pr_number, idle_limit, target, idle_flag
        )

    def _ensure_escalated_label(
        self, pr_number: int, label_create_log_prefix: str
    ) -> bool:
        """Thin wrapper over ``fix_escalation.ensure_escalated_label``.

        Kept as a method because existing tests still invoke
        ``runner._ensure_escalated_label(...)`` directly to assert the
        durable PR-side escalation marker behaves as the FixMixin
        contract describes.
        """
        return fix_escalation.ensure_escalated_label(
            self, pr_number, label_create_log_prefix
        )

    async def _escalate_fix_no_push_deadlock(self, current_pr: PRInfo) -> None:
        """Thin wrapper — see ``fix_escalation.escalate_fix_no_push_deadlock``."""
        await fix_escalation.escalate_fix_no_push_deadlock(self, current_pr)

    async def _escalate_fix_coder_initiated(
        self, current_pr: PRInfo, reason: str, *, ci_trigger: dict | None = None,
    ) -> None:
        """Thin wrapper — see ``fix_escalation.escalate_fix_coder_initiated``."""
        await fix_escalation.escalate_fix_coder_initiated(
            self, current_pr, reason, ci_trigger=ci_trigger
        )

    async def _escalate_fix_iteration_cap(self, current_pr: PRInfo) -> None:
        """Thin wrapper — see ``fix_escalation.escalate_fix_iteration_cap``."""
        await fix_escalation.escalate_fix_iteration_cap(self, current_pr)

    async def _poll_github_during_fix(
        self,
        pr_number: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        terminal_flag: dict[str, str | None],
    ) -> None:
        """Thin wrapper — see ``fix_supervision.poll_github_during_fix``."""
        await fix_supervision.poll_github_during_fix(
            self, pr_number, target, terminal_flag
        )

    def _verify_pushes_since(
        self,
        branch: str,
        last_known_sha: str,
        head_after: str,
        *,
        context: str,
    ) -> bool | None:
        """Thin wrapper — see ``fix_push_verify.verify_pushes_since``."""
        return fix_push_verify.verify_pushes_since(
            self,
            branch,
            last_known_sha,
            head_after,
            context=context,
        )

    def _github_api_budget_paused(self) -> bool:
        """Return ``True`` when the cached GH API budget is below pause threshold."""
        budget = self._github_api_budget_cache
        if budget is None:
            return False
        pause_pct = self.app_config.daemon.github_api_pause_threshold_percent
        if budget.remaining_percent >= pause_pct:
            return False
        if datetime.now(timezone.utc) >= budget.reset_at:
            return False
        return True

    def _run_coder_with_polling(
        self,
        pr_number: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        terminal_flag: dict[str, str | None],
    ) -> asyncio.Task[None] | None:
        """Thin wrapper — see ``fix_supervision.run_coder_with_polling``."""
        return fix_supervision.run_coder_with_polling(
            self, pr_number, target, terminal_flag
        )

    async def _handle_external_terminal_pr_state(
        self, terminal_state: str
    ) -> None:
        """Thin wrapper — see ``fix_supervision.handle_external_terminal_pr_state``."""
        await fix_supervision.handle_external_terminal_pr_state(
            self, terminal_state
        )

    async def _build_fix_feedback_context(
        self,
        current_pr: PRInfo,
        reviewer_policy: ReviewerPolicy | None = None,
        review_feedback: str | None = None,
        review_limitation: str | None = None,
        ci_failure: dict | None = None,
        observed_attempts: dict[str, int] | None = None,
    ) -> str | None:
        """Compose CI failure logs + latest review feedback for the FIX prompt.

        Each section is added independently so the coder receives whatever
        signals the daemon can resolve. Returns ``None`` when neither
        source is reachable; the prompt then falls back to bare
        ``FIX FEEDBACK``.
        """
        sections: list[str] = []
        if ci_failure is not None:
            identities = ", ".join(
                f"{c['source']}={c['check_id']} run={c['workflow_run_id']} "
                f"job={c['job_id']} attempt={c['attempt']} outcome={c['state']}"
                for c in ci_failure["contexts"]
            )
            sections.append(
                f"Confirmed current CI failure: repo={ci_failure['repo']} "
                f"PR #{ci_failure['pr_number']} HEAD={ci_failure['head_sha']}; {identities}."
            )
            if not review_feedback:
                sections.append(
                    "If this CI failure becomes obsolete before any repair is needed, "
                    "make no change and end stdout with exactly "
                    "ESCALATE: OBSOLETE_CI_EVIDENCE."
                )
            ci_logs = await asyncio.to_thread(
                _fetch_failed_ci_logs, self.owner_repo, ci_failure, observed_attempts
            )
            if ci_logs:
                sections.append(
                    "CI failure logs (last 5000 chars):\n" + ci_logs
                )
            else:
                sections.append(
                    "Confirmed CI failure logs unavailable."
                )
        if current_pr.review_status == ReviewStatus.CHANGES_REQUESTED:
            if review_feedback:
                sections.append("Latest review feedback:\n" + review_feedback)
            if review_limitation:
                sections.append("Review evidence limitation: " + review_limitation)
        if not sections:
            return None
        return "\n\n".join(sections)

    async def handle_fix(self) -> None:
        """Run ``FIX FEEDBACK`` via the active coder CLI and return to WATCH."""
        retained = self.state.publication_attempt
        provider_resume = retained is not None and retained.provider_retry_ready
        provider_snapshot = self.state.active_invocation if provider_resume else None
        if (self.state.publication_attempt is not None
            and self.state.publication_attempt.pending and not provider_resume):
            if self.state.publication_attempt.contract_version == 2:
                await self._hold_unresolved_publication()
                return
            resolved = await self._resolve_published_attempt()
            if resolved and self.state.current_task is None:
                return
            if not resolved or self.state.current_pr is None:
                await self._hold_unresolved_publication()
                return
        self._stop_requested = False
        fix_feedback_reviewer_policy = self._fix_feedback_reviewer_policy
        self._fix_feedback_reviewer_policy = None
        review_feedback = getattr(self, "_fix_review_feedback", None)
        review_head = getattr(self, "_fix_review_head", None)
        review_origin = getattr(self, "_fix_review_origin", None)
        review_limitation = getattr(self, "_fix_review_limitation", None)
        self._fix_review_feedback = None
        self._fix_review_head = None
        self._fix_review_origin = None
        self._fix_review_limitation = None
        # PR-358: a FIX entry begins a new review iteration; clear the
        # single-shot review_timeout repost flag so the next WATCH
        # iteration after this FIX can post one repost again if Codex
        # falls silent on the new push. The companion timestamp (used as
        # the durable elapsed_min floor on the next iteration) clears in
        # lockstep so a stale repost anchor cannot suppress a legitimate
        # timeout in the new window.
        self.state.review_timeout_repost_attempted = False
        self.state.review_timeout_repost_at = None
        logger.info(
            "[BRANCH] handle_fix: %s",
            BranchContext.from_runner(self).log_summary(),
        )
        await self._refresh_auth_status_cache()
        try:
            coder_name, plugin = self._get_coder(allow_exploration=False)
        except CoderSelectionUnavailable as exc:
            await self._transition_to_error(
                str(exc),
                publish=False,
                log_prefix="[FIX]",
            )
            return
        if provider_snapshot is not None and provider_snapshot.plugin_id != coder_name:
            await publication._park(self, publication.PROVIDER_RECOVERY_CHANGED)
            return
        if not await self.usage_gate(proactive_coder=coder_name):
            return

        if (
            self.state.current_pr is not None
            and self.state.current_pr.is_cross_repository
        ):
            self.log_event(
                f"[FIX] Skipping FIX for cross-repo PR "
                f"#{self.state.current_pr.number}."
            )
            self.state.state = PipelineState.WATCH
            return

        state_before_fix = self.state.state
        self.state.state = PipelineState.FIX

        async def persist_attempts() -> None:
            # Observation reads precede coder ownership. Keep the persisted
            # phase at its pre-dispatch value until the invocation checkpoint.
            self.state.state = state_before_fix
            try:
                await self.publish_state()
            finally:
                self.state.state = PipelineState.FIX

        current_pr = self.state.current_pr
        if review_origin is not None and current_pr is not None:
            from src.github import reviews as gh_reviews

            expected_origin = (
                self.owner_repo, current_pr.number, current_pr.head_sha,
                fix_feedback_reviewer_policy.fingerprint
                if fix_feedback_reviewer_policy is not None else None,
                gh_reviews.review_cache_generation(self.owner_repo),
            )
            if review_origin != expected_origin:
                review_feedback = None
                review_limitation = "status-poll review identity changed before dispatch"
        if await self._park_persisted_fix_invocation(self._current_run_record):
            return
        ci_failure = None
        observed_attempts = (
            attempt_scope(self.state, self.owner_repo, self.repo_path, current_pr)
            if current_pr is not None else None
        )
        approval_candidate = (
            self._current_approval(current_pr) if current_pr is not None else None
        )
        if (
            not provider_resume and current_pr is not None and current_pr.ci_status == CIStatus.FAILURE
            and approval_candidate is None
        ):
            if self.state.current_task is None or observed_attempts is not None:
                ci_failure = await asyncio.to_thread(
                    _current_ci_failure, self.owner_repo, current_pr.number,
                    current_pr.head_sha, self.repo_config.required_checks,
                    self.repo_config.allow_merge_without_checks,
                    self.repo_config.ci_fix_trusted_statuses,
                    observed_attempts,
                )
            if observed_attempts is not None:
                await persist_attempts()
            if ci_failure is None:
                if not (review_feedback and review_head == current_pr.head_sha):
                    self.log_event("[FIX] CI failure no longer actionable; returning to WATCH.")
                    self.state.state = PipelineState.WATCH
                    return
                current_pr.ci_status = CIStatus.PENDING
        if (
            not provider_resume and current_pr is not None
            and current_pr.ci_status != CIStatus.FAILURE
            and current_pr.review_status == ReviewStatus.CHANGES_REQUESTED
            and (not review_feedback or not review_head or review_head != current_pr.head_sha)
        ):
            self.log_event("[FIX] Current actionable review findings unavailable; returning to WATCH.")
            self.state.state = PipelineState.WATCH
            return
        fix_iteration_cap = self.app_config.daemon.fix_iteration_cap
        task = self.state.current_task
        reused_prelaunch = False
        approved_continuation = None
        pending_fix_record = None
        prelaunch_previous_record = None
        prelaunch_previous_invocation = None
        if task is not None and current_pr is not None:
            if (
                (task.branch and current_pr.branch != task.branch)
                or current_pr.pr_id not in (None, task.pr_id)
                or (not task.branch and current_pr.pr_id != task.pr_id)
            ):
                self.log_event("[FIX] Task/PR binding unavailable; deferring dispatch.")
                self.state.state = PipelineState.WATCH
                return
            if self._current_run_record is None:
                try:
                    recent = await self._metrics_store.recent(
                        task_id=task.pr_id, limit=20, repo_name=self.name
                    )
                except Exception as exc:
                    self.log_event(f"[FIX] Run accounting unavailable: {exc}; deferring dispatch.")
                    self.state.state = PipelineState.WATCH
                    return
                self._current_run_record = next(
                    (item for item in recent if item.task_id == task.pr_id), None
                )
            record = self._current_run_record
            if await self._park_persisted_fix_invocation(record):
                return
            if record is None:
                if approval_candidate is not None:
                    self.log_event("[FIX] Approved guardrail run accounting unavailable; deferring dispatch.")
                    self.state.state = PipelineState.WATCH
                    return
                if (
                    self.state.active_invocation is not None
                    and self.state.active_invocation.phase == "fix"
                ):
                    self.log_event("[FIX] Prior FIX accounting missing; deferring dispatch.")
                    self.state.state = PipelineState.WATCH
                    return
                self._start_current_run_record(coder_name, "")
                record = self._current_run_record
                assert record is not None
                record.run_phase = "fix"
            elif record.task_id != task.pr_id:
                self.log_event("[FIX] Completed or unrelated run record; deferring dispatch.")
                self.state.state = PipelineState.WATCH
                return
            elif record.ended_at is not None:
                approval = None
                if record.exit_reason != "coding_complete" and not provider_resume:
                    approval = await self._approved_guardrail_fix_attempt(record, current_pr)
                    if approval is None:
                        self.log_event("[FIX] Completed or unrelated run record; deferring dispatch.")
                        self.state.state = PipelineState.WATCH
                        return
                    approved_continuation = approval
                if record.fix_iterations - record.fix_recovery_baseline - int(provider_resume) >= fix_iteration_cap:
                    await self._escalate_fix_iteration_cap(current_pr)
                    return
                previous_snapshot = self.state.active_invocation
                admitted_run_id = str(uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"{self.name}:{approval.binding if approval is not None else 'coding_complete'}:"
                    f"{record.run_id}:fix",
                ))
                expected_task_hash = (
                    approval.task_fingerprint if approval is not None else record.task_spec_hash
                )
                try:
                    admitted = await self._metrics_store.get(admitted_run_id)
                    if admitted is None:
                        admitted = self._new_fix_run_record(
                            record, coder_name=coder_name,
                            model=plugin.resolve_model(self.app_config.daemon),
                            task_spec_hash=expected_task_hash,
                        )
                        if provider_resume:
                            admitted.fix_recovery_baseline += 1
                        admitted.run_id = admitted_run_id
                        self._current_run_record = admitted
                        await self._checkpoint_current_run_record()
                    elif (
                        admitted.task_id != task.pr_id or admitted.repo_name != self.name
                        or admitted.ended_at is not None
                        or admitted.attempt_index != record.attempt_index + 1
                        or admitted.fix_recovery_baseline != record.fix_recovery_baseline + int(provider_resume)
                        or admitted.task_spec_hash != expected_task_hash
                    ):
                        raise RuntimeError("admitted FIX accounting changed")
                    elif admitted.invocations:
                        reused_prelaunch = await self._recover_fix_prelaunch(
                            admitted, record, approval,
                        )
                        if not reused_prelaunch:
                            raise RuntimeError("admitted FIX invocation ownership changed")
                        prelaunch_previous_record = record
                        prelaunch_previous_invocation = previous_snapshot
                    elif admitted.fix_iterations != record.fix_iterations:
                        raise RuntimeError("admitted FIX accounting changed")
                    pending_fix_record = admitted
                    prelaunch_previous_record = record
                    prelaunch_previous_invocation = previous_snapshot
                    self._current_run_record = record
                except Exception as exc:
                    self._current_run_record = record
                    self.state.active_invocation = previous_snapshot
                    self.log_event(f"[FIX] Could not persist active FIX run: {exc}.")
                    self.state.state = PipelineState.WATCH
                    return
                self.state.active_invocation = previous_snapshot
        record = self._current_run_record
        if (
            record is not None and current_pr is not None
            and record.fix_iterations - record.fix_recovery_baseline - int(provider_resume) >= fix_iteration_cap
        ):
            await self._escalate_fix_iteration_cap(current_pr)
            return
        prior = self.state.active_invocation
        if pending_fix_record is None and prior is not None and prior.phase == "fix" and prior.run_id is not None:
            record = self._current_run_record
            if (
                record is None or record.run_id != prior.run_id
                or record.task_id != getattr(self.state.current_task, "pr_id", None)
                or record.ended_at is not None
            ):
                self.log_event(
                    "[FIX] Persisted FIX invocation count unavailable; "
                    "deferring dispatch for operator recovery."
                )
                self.state.state = PipelineState.WATCH
                return
        if current_pr is not None and current_pr.is_escalated:
            await self._clear_error_message_on_recovery(
                log_prefix="[FIX]",
                reason="escalated PR returned to IDLE",
            )
            self.state.state = PipelineState.IDLE
            self.log_event(
                f"[FIX] FIX blocked for escalated PR #{current_pr.number}, "
                f"moving to IDLE."
            )
            await self.publish_state()
            return
        fix_iteration_policy: BoundedRecoveryPolicy[PRInfo] = BoundedRecoveryPolicy(
            name="fix_iteration_cap",
            max_attempts=fix_iteration_cap,
            counter_getter=lambda pr: pr.fix_iteration_count,
            counter_setter=lambda pr, n: setattr(pr, "fix_iteration_count", n),
            on_threshold=lambda pr: self._escalate_fix_iteration_cap(pr),
        )
        no_push_policy: BoundedRecoveryPolicy[PRInfo] = BoundedRecoveryPolicy(
            name="fix_no_push_cap",
            max_attempts=self.app_config.daemon.fix_no_push_cap,
            counter_getter=lambda pr: pr.no_push_fix_count,
            counter_setter=lambda pr, n: setattr(pr, "no_push_fix_count", n),
            on_threshold=lambda pr: self._escalate_fix_no_push_deadlock(pr),
        )
        if current_pr is not None and await fix_iteration_policy.maybe_escalate(
            current_pr
        ):
            return
        if (
            self.state.current_pr is not None
            and not self.state.current_pr.is_cross_repository
            and not provider_resume
        ):
            branch = self.state.current_pr.branch
            try:
                retry_transient(
                    lambda: git_ops._git(
                        self.repo_path,
                        "fetch", "--prune", "origin",
                        f"+refs/heads/{branch}:refs/remotes/origin/{branch}",
                        timeout=60,
                    ),
                    operation_name=f"git fetch origin {branch}",
                )
                if approved_continuation is not None and not await self._approved_fix_checkout_safe(
                    approved_continuation, current_pr, require_fetched_head=True,
                ):
                    self.log_event("[FIX] Approved checkout changed before refresh; deferring dispatch.")
                    self.state.state = PipelineState.WATCH
                    return
                git_ops._git(self.repo_path, "checkout", branch)
                if approved_continuation is not None and not await self._approved_fix_checkout_safe(
                    approved_continuation, current_pr, require_fetched_head=True,
                ):
                    self.log_event("[FIX] Approved checkout changed before reset; deferring dispatch.")
                    self.state.state = PipelineState.WATCH
                    return
                git_ops._git(self.repo_path, "reset", "--hard", f"origin/{branch}")
            except (
                subprocess.CalledProcessError,
                subprocess.TimeoutExpired,
                OSError,
                RuntimeError,
            ) as exc:
                stderr = getattr(exc, "stderr", "") or ""
                await self._transition_to_error(
                    f"git refresh {branch} failed: {stderr.strip() or exc}",
                    save_run_record_as=None,
                    publish=False,
                    log_prefix="[FIX]",
                )
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
        idle_flag: dict[str, bool | float] = {"timed_out": False}
        external_state_flag: dict[str, str | None] = {"state": None}

        plugin_run_kwargs = plugin.build_run_kwargs(
            daemon_config=self.app_config.daemon,
            breach_dir=breach_dir,
            breach_run_id=breach_run_id,
        )
        fix_kwargs: dict[str, object] = {
            **plugin_run_kwargs,
            "on_process_start": self._track_current_coder_process,
            "on_supervised_process_start": (
                self._track_current_coder_supervised_process
            ),
        }
        if self.state.current_task is not None:
            fix_kwargs["pr_id"] = self.state.current_task.pr_id
            fix_kwargs["task_file"] = self.state.current_task.task_file
        admitted_feedback = None
        if self.state.current_pr is not None:
            admitted_feedback = (
                review_feedback if review_head and review_head == self.state.current_pr.head_sha else None
            )
            if admitted_feedback and self.state.current_pr.ci_status == CIStatus.FAILURE:
                metadata = await asyncio.to_thread(
                    gh_prs.get_pr_metadata, self.owner_repo, self.state.current_pr.number
                )
                if metadata.get("head_sha") != review_head:
                    admitted_feedback = None
                    review_limitation = "PR HEAD changed before review dispatch"
            if provider_resume:
                extra_context = retained.fix_feedback_context
            else:
                extra_context = await self._build_fix_feedback_context(
                    self.state.current_pr,
                    reviewer_policy=fix_feedback_reviewer_policy,
                    review_feedback=admitted_feedback,
                    review_limitation=review_limitation,
                    ci_failure=ci_failure,
                    observed_attempts=observed_attempts,
                )
            if ci_failure is not None and observed_attempts is not None:
                await persist_attempts()
            if extra_context is not None:
                fix_kwargs["extra_context"] = extra_context
            if review_feedback and self.state.current_pr.ci_status != CIStatus.FAILURE:
                metadata = await asyncio.to_thread(
                    gh_prs.get_pr_metadata, self.owner_repo, self.state.current_pr.number
                )
                if (
                    metadata.get("head_sha") != review_head
                    or self.state.current_pr is not current_pr
                    or self.state.current_pr.head_sha != review_head
                ):
                    self.log_event("[FIX] PR HEAD changed before review dispatch; returning to WATCH.")
                    self.state.state = PipelineState.WATCH
                    return
            if ci_failure is not None:
                latest_failure = await asyncio.to_thread(
                    _current_ci_failure, self.owner_repo, self.state.current_pr.number,
                    self.state.current_pr.head_sha, self.repo_config.required_checks,
                    self.repo_config.allow_merge_without_checks,
                    self.repo_config.ci_fix_trusted_statuses,
                    observed_attempts,
                )
                if observed_attempts is not None:
                    await persist_attempts()
                admitted_failure = canonical_ci_incident(ci_failure)
                if latest_failure != admitted_failure:
                    if admitted_feedback:
                        metadata = await asyncio.to_thread(
                            gh_prs.get_pr_metadata, self.owner_repo, self.state.current_pr.number,
                        )
                        if (
                            metadata.get("head_sha") != review_head
                            or self.state.current_pr is not current_pr
                            or self.state.current_pr.head_sha != review_head
                        ):
                            self.log_event("[FIX] PR HEAD changed before review dispatch; returning to WATCH.")
                            self.state.state = PipelineState.WATCH
                            return
                        ci_failure = None
                        self.state.current_pr.ci_status = CIStatus.PENDING
                        review_context = await self._build_fix_feedback_context(
                            self.state.current_pr,
                            reviewer_policy=fix_feedback_reviewer_policy,
                            review_feedback=admitted_feedback,
                            review_limitation=review_limitation,
                        )
                        if review_context is not None:
                            fix_kwargs["extra_context"] = review_context
                        else:
                            self.log_event("[FIX] Current actionable review findings unavailable; returning to WATCH.")
                            self.state.state = PipelineState.WATCH
                            return
                    else:
                        self.log_event("[FIX] CI failure changed before coder launch; returning to WATCH.")
                        self.state.state = PipelineState.WATCH
                        return
        feedback_context = retained.fix_feedback_context if provider_resume else fix_kwargs.get("extra_context")
        if provider_snapshot is not None:
            fix_kwargs["model"] = provider_snapshot.model_override
            fix_kwargs["reasoning_effort"] = provider_snapshot.reasoning_effort_override
            fix_kwargs["extra_context"] = (
                "Resume the interrupted FIX on this retained checkout. Preserve local commits and dirty changes. "
                "The previous provider invocation failed; its gate is unfinished. Rerun required validation "
                "before reporting success. Reuse the existing PR; do not republish unchanged work.\n"
                + (feedback_context or "")
            )
        if reused_prelaunch:
            snapshot = pending_fix_record.invocations[-1]
            if (
                snapshot.plugin_id != coder_name
                or snapshot.model_override != self._invocation_override(fix_kwargs.get("model"))
                or snapshot.reasoning_effort_override != self._invocation_override(
                    fix_kwargs.get("reasoning_effort")
                )
            ):
                self.log_event("[FIX] Checkpointed invocation settings changed; deferring dispatch.")
                self.state.state = PipelineState.WATCH
                return
        if not self._reserve_coder_credentials(
            coder_name,
            invocation_kwargs=fix_kwargs,
        ):
            self.log_event(
                "[FIX] Coder invocation deferred while credential login owns "
                "its credential location."
            )
            self.state.state = PipelineState.WATCH
            return
        heartbeat: asyncio.Task[None] | None = None
        if pending_fix_record is not None:
            self._current_run_record = pending_fix_record
            self.state.active_invocation = (
                pending_fix_record.invocations[-1] if reused_prelaunch else None
            )
        run_record = self._current_run_record
        previous_profile_id = run_record.profile_id if run_record is not None else None
        previous_active_invocation = self.state.active_invocation
        previous_publication_attempt = self.state.publication_attempt
        previous_fix_iterations = (
            run_record.fix_iterations if run_record is not None else None
        )
        captured_snapshot = None
        try:
            if run_record is not None:
                run_record.profile_id = (
                    f"{coder_name}:{fix_kwargs['model']}:container"
                )
            self.log_event(
                f"[{coder_name}] entering FIX.",
                tier="state",
                kind="transition",
            )
            if reused_prelaunch:
                captured_snapshot = self.state.active_invocation
            else:
                fix_iteration: int | None = None
                if self._current_run_record is not None:
                    self._current_run_record.fix_iterations += 1
                    fix_iteration = self._current_run_record.fix_iterations
                elif self.state.current_pr is not None:
                    fix_iteration = self.state.current_pr.fix_iteration_count + 1
                captured_snapshot = self._capture_invocation_snapshot(
                    coder_name,
                    fix_kwargs,
                    phase="fix",
                    fix_iteration=fix_iteration,
                    fix_trigger={
                        "ci": canonical_ci_incident(ci_failure) if ci_failure else None,
                        "review": bool(admitted_feedback),
                    },
                )
                if run_record is not None:
                    await self._checkpoint_current_run_record()
            if not self._publication_attempt_matches_dispatch():
                self._begin_publication_attempt(started_at=datetime.now(timezone.utc))
                if self.state.publication_attempt is not None:
                    self.state.publication_attempt.fix_feedback_context = feedback_context
                    self.state.publication_attempt.initial_branch_head_sha = head_before or None
                    if provider_resume:
                        self.state.publication_attempt.provider_retries = retained.provider_retries
                        self.state.publication_attempt.provider_retry_command_id = retained.provider_retry_command_id
                        self.state.publication_attempt.initial_branch_head_sha = retained.initial_branch_head_sha
                        self.state.publication_attempt.initial_pr_head_sha = retained.initial_pr_head_sha
                        self.state.publication_attempt.provider_published_head_sha = (
                            retained.provider_published_head_sha
                        )
            await self.publish_state()
            if approved_continuation is not None and not await self._approved_fix_checkout_safe(
                approved_continuation, current_pr, require_fetched_head=True,
            ):
                raise RuntimeError("Approved checkout changed before FIX launch")
            heartbeat = asyncio.create_task(self._publish_while_waiting("FIX"))
            self._coder_invocation_active = True
            claude_task: asyncio.Task[tuple[int, str, str]] = (
                asyncio.create_task(
                    plugin.fix_review(
                        self.repo_path,
                        **fix_kwargs,
                    )
                )
            )
            idle_flag["started_at"] = fix_supervision.time.monotonic()
        except BaseException:
            try:
                if captured_snapshot is not None and not reused_prelaunch:
                    self.state.active_invocation = previous_active_invocation
                    self.state.publication_attempt = previous_publication_attempt
                    if (
                        run_record is not None
                        and run_record.invocations
                        and run_record.invocations[-1] == captured_snapshot
                    ):
                        run_record.invocations.pop()
                        assert previous_fix_iterations is not None
                        run_record.fix_iterations = previous_fix_iterations
                        run_record.profile_id = previous_profile_id
                    if pending_fix_record is not None:
                        self._current_run_record = prelaunch_previous_record
                        self.state.active_invocation = prelaunch_previous_invocation
                        self.state.state = PipelineState.WATCH
                    rollback_writes = [self.publish_state()]
                    if run_record is not None:
                        rollback_writes.append(
                            self._metrics_store.save(run_record) if pending_fix_record is not None
                            else self._checkpoint_current_run_record()
                        )
                    await asyncio.gather(*rollback_writes, return_exceptions=True)
                elif reused_prelaunch:
                    self._current_run_record = prelaunch_previous_record
                    self.state.active_invocation = prelaunch_previous_invocation
                    self.state.publication_attempt = previous_publication_attempt
                    self.state.state = PipelineState.WATCH
                    await self.publish_state()
            finally:
                if heartbeat is not None:
                    heartbeat.cancel()
                    await asyncio.gather(heartbeat, return_exceptions=True)
                self._coder_invocation_active = False
                self._release_coder_credentials()
            raise
        assert heartbeat is not None
        stop_monitor = asyncio.create_task(self._monitor_stop_request(claude_task))
        idle_monitor = asyncio.create_task(
            self._monitor_fix_idle(pr_number, idle_limit, claude_task, idle_flag)
        )
        breach_monitor: asyncio.Task[None] | None = None
        if plugin.supports_breach_lifecycle:
            breach_monitor = asyncio.create_task(
                self._monitor_inflight_breach(
                    breach_dir,
                    breach_run_id,
                    plugin.name,
                    claude_task,
                    breach_flag,
                )
            )
        external_state_monitor = self._run_coder_with_polling(
            pr_number, claude_task, external_state_flag,
        )
        stop_cancelled = False
        breach_cancelled = False
        pending_cancellation: asyncio.CancelledError | None = None
        coder_result: tuple[int, str, str] | None = None
        cleanup_confirmed = False
        try:
            coder_result = await claude_task
            code, stdout, stderr = coder_result
        except asyncio.CancelledError as exc:
            if self._stop_requested:
                stop_cancelled = True
                code, stdout, stderr = 1, "", ""
            elif external_state_flag["state"] is not None:
                # External terminal state observed by the polling task;
                # the post-finally external-state branch drives the
                # transition so the same code path also covers the race
                # where the coder finished during the SIGTERM grace
                # window and ``target.cancel()`` became a no-op (Codex
                # P1 on PR #223).
                code, stdout, stderr = 1, "", ""
            elif breach_flag["breached"]:
                breach_cancelled = True
                code, stdout, stderr = 1, "", ""
            elif not idle_flag["timed_out"]:
                pending_cancellation = exc
                code, stdout, stderr = 1, "", ""
            else:
                code, stdout, stderr = 1, "", ""
        finally:
            stop_monitor.cancel()
            if breach_monitor is not None:
                breach_monitor.cancel()
            idle_monitor.cancel()
            if external_state_monitor is not None:
                external_state_monitor.cancel()
            heartbeat.cancel()
            monitors = [stop_monitor, idle_monitor, heartbeat]
            if breach_monitor is not None:
                monitors.append(breach_monitor)
            if external_state_monitor is not None:
                monitors.append(external_state_monitor)
            await asyncio.gather(*monitors, return_exceptions=True)
            self._coder_invocation_active = False
            if coder_result is not None:
                self._record_unconfirmed_launch_cleanup(coder_result)
            if plugin.supports_breach_lifecycle:
                self._check_late_breach(
                    breach_dir,
                    breach_run_id,
                    plugin.name,
                    breach_flag,
                )
                self._cleanup_breach_marker(breach_dir, breach_run_id)
            try:
                if coder_result is not None and not breach_flag["breached"] and not idle_flag["timed_out"]:
                    await publication.record_exit(
                        self, code, stdout, stderr,
                        terminal_cancelled=(code < 0 and external_state_flag["state"] is not None
                                            and not self._stop_requested),
                    )
                elif (coder_result is None and external_state_flag["state"] is not None
                      and not stop_cancelled and not breach_flag["breached"] and not idle_flag["timed_out"]):
                    await publication.record_exit(self, code, stdout, stderr, terminal_cancelled=True)
                await self._capture_published_attempt()
            finally:
                cleanup_confirmed = await self._confirm_current_coder_cleanup(
                    "FIX completion"
                )
        if pending_cancellation is not None:
            raise pending_cancellation
        if not cleanup_confirmed:
            return
        await publication.record_cleanup(self)
        if run_record is not None:
            # A same-process completed no-push FIX may run another iteration
            # before the cycle publishes WATCH. Restart has no such proof.
            self._completed_fix_run_id = run_record.run_id
        if external_state_flag["state"] is not None and not stop_cancelled:
            attempt = self.state.publication_attempt
            if attempt is not None and attempt.contract_version == 2:
                await self._hold_unresolved_publication()
                return
            if attempt is not None:
                attempt.exit_classification = "failure"
            if attempt is not None and (
                not self._publication_attempt_matches_dispatch()
                or not attempt.verified or attempt.unverified_head_sha is not None
            ):
                await self._hold_unresolved_publication()
                return
            if self.state.publication_attempt is not None:
                self.state.publication_attempt.pending = False
            await self._handle_external_terminal_pr_state(
                external_state_flag["state"]  # type: ignore[arg-type]
            )
            return
        if breach_flag["breached"]:
            if self.state.publication_attempt is not None:
                self.state.publication_attempt.exit_classification = "failure"
            if self.state.current_pr is not None:
                # Late-breach pause is not a no-push success; reset the
                # streak (Codex P2 on PR #222) — same rationale as the
                # CancelledError breach path above.
                no_push_policy.reset(self.state.current_pr)
                self._rehydrate_last_push_at(self.state.current_pr)
                try:
                    head_now = git_ops._git(
                        self.repo_path, "rev-parse", "HEAD"
                    ).stdout.strip()
                except Exception:
                    head_now = ""
                if head_before and head_now and head_before != head_now:
                    trigger_context = (
                        "after breach-cancel fix push; manual review "
                        "trigger required"
                        if breach_cancelled
                        else "after late-breach fix push; manual review "
                        "trigger required"
                    )
                    if not await fix_codex_trigger.maybe_post_codex_review_after_push(
                        self,
                        self.state.current_pr.number,
                        trigger_context,
                    ):
                        return
            self.state.state = PipelineState.PAUSED
            await self._clear_error_message_on_recovery(
                log_prefix="[FIX]",
                reason=(
                    "in-flight rate limit breach"
                    if breach_cancelled
                    else "late in-flight rate limit breach"
                ),
            )
            if breach_cancelled:
                self.log_event(
                    f"[FIX] FIX aborted: in-flight rate limit breach, "
                    f"paused until {self.state.rate_limited_until}."
                )
            else:
                self.log_event(
                    f"[FIX] FIX paused: late in-flight rate limit breach, "
                    f"paused until {self.state.rate_limited_until}."
                )
            return

        stop_requested_after_exit = False

        async def capture_stop_requested_after_exit() -> bool:
            nonlocal stop_requested_after_exit
            if stop_requested_after_exit:
                return True
            if self._stop_requested:
                stop_requested_after_exit = True
                return True
            requested = await self._pop_stop_request()
            if not requested:
                return False
            self._stop_requested = True
            self.state.user_paused = True
            stop_requested_after_exit = True
            self.log_event(
                "[FIX] User stop requested after FIX exit; deferring "
                "pause until FIX bookkeeping completes."
            )
            return True

        async def pause_for_stop_after_bookkeeping() -> bool:
            if not stop_requested_after_exit:
                return False
            self.state.state = PipelineState.PAUSED
            await self._clear_error_message_on_recovery(
                log_prefix="[FIX]",
                reason="user stop requested after FIX exit",
            )
            self.log_event("[FIX] FIX aborted: user stop requested.")
            return True

        async def read_head_after_fix() -> str | None:
            try:
                return git_ops._git(
                    self.repo_path, "rev-parse", "HEAD"
                ).stdout.strip()
            except (
                subprocess.CalledProcessError,
                subprocess.TimeoutExpired,
                OSError,
            ) as exc:
                await self._transition_to_error(
                    f"rev-parse after fix failed: {exc}",
                    save_run_record_as=None,
                    publish=False,
                    log_prefix="[FIX]",
                )
                return None

        def remote_branch_contains_head(branch: str, head_after: str) -> bool:
            return (
                self._verify_pushes_since(
                    branch,
                    head_before,
                    head_after,
                    context="after FIX stop",
                )
                is True
            )

        async def record_fix_push(head_after: str, failure_detail: str) -> bool:
            if head_before and head_before == head_after:
                return True

            now_time = datetime.now(timezone.utc)
            if self.state.current_pr is not None:
                self.state.current_pr.record_observed_head(head_after)
                self.state.current_pr.head_sha = head_after
                self._last_push_at = self._canonical_push_timestamp(
                    self.state.current_pr.number
                )
                self._last_push_at_pr_number = self.state.current_pr.number
                iteration = fix_iteration_policy.increment(self.state.current_pr)
                no_push_policy.reset(self.state.current_pr)
                self.state.current_pr.last_activity = now_time
            else:
                self._last_push_at = None
                iteration = 0

            self.log_event(f"[FIX] Fix pushed, iteration #{iteration}.")
            if self.state.current_pr is not None:
                if not await fix_codex_trigger.maybe_post_codex_review_after_push(
                    self,
                    self.state.current_pr.number,
                    failure_detail,
                ):
                    return False
            return True

        await capture_stop_requested_after_exit()
        if idle_flag["timed_out"]:
            if self.state.publication_attempt is not None:
                self.state.publication_attempt.exit_classification = "failure"
            # Idle timeout breaks the no-push streak: the coder didn't
            # produce a push and the daemon killed it. Reset the counter
            # so a later "no-push success" cycle starts fresh rather
            # than tripping the cap on a non-consecutive sequence
            # (Codex P2 on PR #222).
            if self.state.current_pr is not None:
                no_push_policy.reset(self.state.current_pr)
            await self._transition_to_error(
                f"FIX idle timeout: no new push for {idle_limit}s of active FIX time",
                save_run_record_as=None,
                publish=False,
                log_prefix="[FIX]",
                cancellation_cause=CancellationCause(
                    category="ERROR",
                    payload={
                        "subsource": "fix_idle_timeout",
                        "limit_type": "fix_idle",
                        "duration_elapsed_sec": idle_limit,
                        "active_phase": PipelineState.FIX.value,
                        "pr_number": pr_number,
                        "branch": self.state.current_pr.branch if self.state.current_pr else "",
                        "run_id": run_record.run_id if run_record else None,
                    },
                ),
                commit_task_status=True,
            )
            await self._save_cli_log("", "", "FIX idle timeout")
            if await pause_for_stop_after_bookkeeping():
                return
            return
        await self._save_cli_log(stdout, stderr, f"FIX FEEDBACK output [{coder_name}]")
        violations = scan_stdout(f"{stdout}\n{stderr}")
        if violations:
            if self.state.publication_attempt is not None:
                self.state.publication_attempt.exit_classification = "failure"
            first = violations[0]
            cause = f"GUARDRAIL: {first.category}: {first.excerpt}"
            for violation in violations:
                self.log_event(
                    f"tier={violation.tier} "
                    f"{violation.category}: {violation.excerpt}",
                    tier="guardrail",
                    kind=violation.category,
                )
            if await pause_for_stop_after_bookkeeping():
                return
            if self.state.current_pr is not None:
                pr_number = self.state.current_pr.number
                self.state.quarantined_prs.add(pr_number)
                task_id = (
                    self.state.current_pr.pr_id
                    or (self.state.current_task.pr_id if self.state.current_task else "")
                )
                await self._suppress_task(
                    task_id,
                    SuppressionReason.GUARDRAIL,
                    {"pr_number": pr_number, "reason_text": cause},
                )
                apply_quarantine_label_for_violation(self, pr_number, first)
            await self._transition_to_error(
                cause,
                save_run_record_as=None,
                publish=False,
                log_prefix="[FIX]",
                cancellation_cause=CancellationCause(
                    category="ERROR",
                    payload={"subsource": "guardrail", "reason_text": cause},
                ),
                commit_task_status=True,
            )
            await self._refresh_guardrail_head_after_fix()
            return
        await capture_stop_requested_after_exit()
        escalate_reason = fix_escalation.parse_escalate_marker(stdout)
        if (
            escalate_reason == "OBSOLETE_CI_EVIDENCE"
            and head_before
            and await read_head_after_fix() == head_before
            and await self._ci_only_trigger_obsolete()
        ):
            if await pause_for_stop_after_bookkeeping():
                return
            if self.state.publication_attempt is not None:
                self.state.publication_attempt.pending = False
            self.log_event("[FIX] Bound CI-only failure became obsolete; resuming WATCH.")
            self.state.state = PipelineState.WATCH
            await self.publish_state()
            return
        if escalate_reason is not None and self.state.current_pr is not None:
            snapshot = self.state.active_invocation
            trigger = snapshot.fix_trigger if snapshot is not None else None
            obsolete_ci_claim = (
                {
                    "ci": canonical_ci_incident(trigger["ci"]),
                    "task_id": self.state.current_task.pr_id,
                    "branch": self.state.current_pr.branch,
                    "run_id": snapshot.run_id,
                    "attempt_index": snapshot.attempt_index,
                    "fix_iteration": snapshot.fix_iteration,
                    "task_spec_hash": task_spec_content_hash(
                        (Path(self.repo_path) / self.state.current_task.task_file).read_text(encoding="utf-8")
                    ),
                }
                if escalate_reason == "OBSOLETE_CI_EVIDENCE"
                and isinstance(trigger, dict)
                and trigger.get("review") is False
                and isinstance(trigger.get("ci"), dict)
                and self.state.current_task is not None
                and self.state.current_task.task_file is not None
                and snapshot is not None
                and snapshot.run_id is not None
                and head_before
                and await read_head_after_fix() == head_before
                else None
            )
            if self.state.publication_attempt is not None:
                self.state.publication_attempt.exit_classification = "failure"
            # Coder semantic circuit breaker (PR-166): an explicit
            # self-report wins over the regular push/return-code flow.
            # ``no_push_fix_count`` is reset because an explicit
            # ESCALATE is not a no-push success — it is a deliberate
            # bail-out and should not feed the deadlock counter.
            no_push_policy.reset(self.state.current_pr)
            if self.state.current_task is not None and obsolete_ci_claim is None:
                await safe_record_cancellation_cause(
                    self.redis,
                    self.name,
                    self.state.current_task.pr_id,
                    CancellationCause(
                        category="ERROR",
                        payload={
                            "subsource": "coder_escalate",
                            "reason_text": escalate_reason,
                            **({"obsolete_ci_trigger": obsolete_ci_claim} if obsolete_ci_claim else {}),
                        },
                    ),
                    log=self.log_event,
                )
            await self._escalate_fix_coder_initiated(
                self.state.current_pr, escalate_reason, ci_trigger=obsolete_ci_claim,
            )
            # If a stop arrived while the coder was running, honor the
            # deferred pause: PAUSED takes precedence over the IDLE/HUNG
            # parking state set by ``_escalate_fix_coder_initiated`` so
            # the operator's pause request is not silently dropped on
            # the ESCALATE branch (Codex P1 on PR #228).
            if await pause_for_stop_after_bookkeeping():
                return
            return
        if stop_cancelled:
            if self.state.publication_attempt is not None:
                self.state.publication_attempt.exit_classification = "failure"
            head_after = await read_head_after_fix()
            if head_after is None:
                return
            # Stop-cancel breaks the consecutive no-push streak (Codex P2
            # on PR #222). ``record_fix_push`` already resets on a
            # productive push; this covers the no-push case.
            if self.state.current_pr is not None:
                no_push_policy.reset(self.state.current_pr)
            branch = self.state.current_pr.branch if self.state.current_pr is not None else ""
            if branch and remote_branch_contains_head(branch, head_after):
                if not await record_fix_push(
                    head_after,
                    "after stop-cancel fix push; manual review trigger "
                    "required to avoid fix/push loop",
                ):
                    return
            elif head_before and head_before != head_after:
                self.log_event(
                    "[FIX] FIX stop-cancel left local HEAD outside the "
                    "fetched remote branch; skipping push bookkeeping and "
                    "@codex review."
                )
            if await pause_for_stop_after_bookkeeping():
                return
            self.state.state = PipelineState.PAUSED  # pragma: no cover - defensive fallback
            await self._clear_error_message_on_recovery(  # pragma: no cover - defensive fallback
                log_prefix="[FIX]",
                reason="stop-cancel defensive pause fallback",
            )
            return  # pragma: no cover - defensive fallback
        if code != 0:
            if self.state.publication_attempt is not None:
                self.state.publication_attempt.exit_classification = "failure"
            # FIX failure breaks the consecutive no-push streak (Codex P2
            # on PR #222): a sequence like no-push success → failed FIX
            # → no-push success would otherwise still trip the deadlock
            # cap even though the no-push cycles were not consecutive.
            if self.state.current_pr is not None:
                no_push_policy.reset(self.state.current_pr)
            if await publication.pause_provider_failure(self, stderr, coder_name):
                return
            self._detect_rate_limit(stderr, coder_name=coder_name)
            if (self.state.rate_limited_until is not None
                and self.state.rate_limited_until > datetime.now(timezone.utc)):
                self.state.state = PipelineState.PAUSED
                await self._clear_error_message_on_recovery(
                    log_prefix="[FIX]",
                    reason="rate-limit pause after fix failure",
                )
                self.log_event(
                    f"[RATE-LIMIT] Rate limit pause active until "
                    f"{self.state.rate_limited_until.isoformat()}."
                )
                return
            if await pause_for_stop_after_bookkeeping():
                return
            await self._transition_to_error(
                stderr.strip() or f"{coder_name} exit {code}",
                save_run_record_as=None,
                publish=False,
                log_prefix=f"[FIX] [{coder_name}] fix_review failed:",
            )
            return

        head_after = await read_head_after_fix()
        if head_after is None:
            return

        if self.state.publication_attempt is not None:
            self.state.publication_attempt.exit_classification = "success"
            await self.publish_state()

        attempt = self.state.publication_attempt
        if (attempt is not None and attempt.contract_version == 2
            and (provider_resume or attempt.expected_head_sha != attempt.initial_pr_head_sha or not attempt.verified)):
            await capture_stop_requested_after_exit()
            if await pause_for_stop_after_bookkeeping():
                return
            await self._hold_unresolved_publication()
            return
        if attempt is not None and attempt.unverified_head_sha is not None:
            # Capture retained the candidate commit without exact remote proof.
            # Do not advertise it or clear the hold through optimistic git
            # verification (or through the no-push WATCH path).
            await capture_stop_requested_after_exit()
            if await pause_for_stop_after_bookkeeping():
                return
            await self._hold_unresolved_publication()
            return

        local_no_push = bool(head_before) and head_before == head_after
        remote_no_push = False
        if not local_no_push:
            verify_branch = (
                self.state.current_pr.branch
                if self.state.current_pr is not None
                else ""
            )
            verification = (
                self._verify_pushes_since(
                    verify_branch,
                    head_before,
                    head_after,
                    context="after FIX exit",
                )
                if verify_branch
                else None
            )
            if verification is False:
                remote_no_push = True
            elif verification is None and verify_branch:
                self.log_event(
                    "[FIX] FIX push verification unavailable; "
                    "proceeding optimistically."
                )

        if local_no_push or remote_no_push:
            if await self._ci_only_trigger_obsolete():
                if await pause_for_stop_after_bookkeeping():
                    return
                if self.state.publication_attempt is not None:
                    self.state.publication_attempt.pending = False
                self.log_event("[FIX] Bound CI-only failure became obsolete; resuming WATCH.")
                self.state.state = PipelineState.WATCH
                await self.publish_state()
                return
            if self.state.current_pr is not None:
                self._last_push_at = self._canonical_push_timestamp(
                    self.state.current_pr.number
                )
            else:  # pragma: no cover - defensive: current_pr is set throughout handle_fix
                self._last_push_at = None
            if local_no_push:
                self.log_event(
                    "[FIX] FIX FEEDBACK exited 0 but HEAD unchanged; "
                    "no push, skipping @codex review."
                )
            else:
                self.log_event(
                    "[FIX] Coder exited cleanly but no push detected; "
                    "treating as no-push, skipping @codex review."
                )
            if self.state.current_pr is not None:
                no_push_policy.increment(self.state.current_pr)
            if await pause_for_stop_after_bookkeeping():
                return
            if self.state.publication_attempt is not None:
                self.state.publication_attempt.pending = False
                # Escalation can discard the dispatch binding. Checkpoint the
                # completed attempt first so recovery cannot resurrect its hold.
                await self.publish_state()
            if (
                self.state.current_pr is not None
                and await no_push_policy.maybe_escalate(self.state.current_pr)
            ):
                return
            self.state.state = PipelineState.WATCH
            await self.publish_state()
            return

        if not await record_fix_push(
            head_after,
            "after fix push; manual review trigger required "
            "to avoid fix/push loop",
        ):
            return
        if await pause_for_stop_after_bookkeeping():
            return
        if self.state.publication_attempt is not None:
            self.state.publication_attempt.pending = False
        self.state.state = PipelineState.WATCH
        await self.publish_state()
