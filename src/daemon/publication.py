"""Durable continuation of the existing coder publication attempt.

Exit, remote branch, PR adoption, and process cleanup are independent evidence.
The runner owns polling; a poll never reserves another coder invocation.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from src.cancellation import CancellationCause
from src.daemon import fix_escalation, git_ops
from src.daemon.guardrails import scan_stdout
from src.github import checks as gh_checks
from src.github import comments as gh_comments
from src.github import prs as gh_prs
from src.models import CIStatus, PipelineState, ReviewStatus
from src.subsource_registry import SuppressionReason

if TYPE_CHECKING:
    from src.daemon.runner import PipelineRunner


async def record_exit(
    runner: PipelineRunner, code: int, stdout: str, stderr: str, *, publication_cancelled: bool = False,
) -> None:
    """Checkpoint invocation outcome before cleanup and terminal bookkeeping."""
    attempt = runner.state.publication_attempt
    if attempt is None or attempt.contract_version != 2:
        return
    from src.daemon.handlers.coding import _local_branch_head_sha

    if attempt.expected_head_sha is None:
        attempt.expected_head_sha = await asyncio.to_thread(
            _local_branch_head_sha, runner.repo_path, attempt.branch,
        )
    if attempt.exit_classification is None:
        attempt.exit_classification = (
            "success" if (code == 0 or publication_cancelled) and not scan_stdout(f"{stdout}\n{stderr}")
            and fix_escalation.parse_escalate_marker(stdout) is None else "failure"
        )
    if attempt.reconcile_deadline is None:
        attempt.reconcile_deadline = datetime.now(timezone.utc) + timedelta(
            minutes=runner.repo_config.review_timeout_min or runner.app_config.daemon.review_timeout_min,
        )
    await runner.publish_state()


async def record_cleanup(runner: PipelineRunner) -> None:
    attempt = runner.state.publication_attempt
    if attempt is not None and attempt.contract_version == 2:
        attempt.cleanup_confirmed = True
        await runner.publish_state()


async def resolve(runner: PipelineRunner, *, resolve_terminal: bool) -> bool:
    """Observe the immutable expected head, never the moving local checkout."""
    attempt = runner.state.publication_attempt
    expected = attempt.expected_head_sha
    attempt.head_sha = expected
    attempt.verified = False
    try:
        probe = await asyncio.to_thread(
            git_ops._git, runner.repo_path, "ls-remote", "--heads", "origin",
            f"refs/heads/{attempt.branch}", timeout=30, check=False,
        )
        words = probe.stdout.split()
        attempt.observed_branch_sha = (
            words[0] if probe.returncode == 0 and len(words) == 2
            and words[1] == f"refs/heads/{attempt.branch}" else None
        )
        attempt.incident = "remote branch does not confirm expected SHA"
        if attempt.pr_number is None:
            if attempt.baseline_numbers is None:
                attempt.incident = "pre-launch PR discovery was incomplete"
                return False
            publications = await asyncio.to_thread(
                gh_prs.get_branch_publications, attempt.repo, attempt.base, attempt.branch,
            )
            matches = [item for item in publications if runner._publication_matches_attempt(item)]
            if len(matches) != 1:
                attempt.incident = "exact ready PR discovery is incomplete or ambiguous"
                return False
            attempt.pr_number = matches[0].number
            attempt.candidate = matches[0].to_pr_info()
        outcome = await asyncio.to_thread(
            gh_prs.get_exact_pr_outcome, attempt.repo, attempt.base, attempt.pr_number,
            expected, expected_branch=attempt.branch,
        )
        attempt.observed_pr_sha = outcome.observed_head_sha
        if not outcome.discovery_complete:
            attempt.incident = outcome.reason or "exact PR observation unavailable"
            return False
        attempt.observed_pr_sha = outcome.head_sha
        if attempt.observed_branch_sha != expected:
            # A deleted branch is normal after external terminal resolution;
            # a different extant branch is conflicting evidence, even if newer.
            if attempt.observed_branch_sha is not None or outcome.state == gh_prs.ExactPRState.OPEN:
                return False
            if probe.returncode != 0:
                return False
        current = attempt.candidate
        if (current is None or current.number != attempt.pr_number
            or current.branch != attempt.branch or current.is_cross_repository
            or current.pr_id not in {None, attempt.task_id}):
            attempt.incident = "retained PR identity conflicts with dispatch"
            return False
        attempt.head_sha = expected
        attempt.unverified_head_sha = None
        attempt.verified = True
        current.head_sha = expected
        current.review_status = ReviewStatus.PENDING
        current.ci_status = CIStatus.PENDING
        current.publication_observed = True
        current.publication_at = None
        current.publication_reason = "awaiting current-head publication evidence"
        runner.state.current_pr = current
        if outcome.state in {gh_prs.ExactPRState.MERGED, gh_prs.ExactPRState.CLOSED}:
            if (not resolve_terminal or not attempt.cleanup_confirmed
                or attempt.exit_classification != "success" or attempt.failure
                or runner.state.user_paused):
                return False
            return await runner._handle_external_pr_resolution(current, outcome.state.value)
        if outcome.state != gh_prs.ExactPRState.OPEN:
            return False
        try:
            retrieval = await asyncio.to_thread(
                gh_checks._retrieve_ci_status_evidence, attempt.repo, expected, fresh=True,
            )
            current.ci_status = gh_checks._classify_ci_retrieval(
                retrieval, required_contexts=runner.repo_config.required_checks,
                empty_is_success=runner.repo_config.allow_merge_without_checks,
            )
        except Exception as exc:
            runner.log_event(f"[INFRA] Publication CI hydration deferred: {exc}")
        return True
    except Exception as exc:
        attempt.incident = f"publication observation unavailable: {exc}"
        runner.log_event(f"[INFRA] {attempt.incident}")
        return False


async def hold(runner: PipelineRunner) -> bool:
    """One bounded reconciliation tick, shared by every lifecycle entry point."""
    attempt = runner.state.publication_attempt
    if runner.state.user_paused:
        await runner.publish_state()
        return True
    owned_terminal = attempt.exhausted and attempt.terminal_message is not None
    if (not runner._publication_attempt_matches_dispatch()
        or attempt.failure and not (owned_terminal and attempt.failure == attempt.terminal_message)
        or runner.state.error_message and runner.state.error_message not in {
            attempt.incident, attempt.terminal_message,
        }):
        message = attempt.failure or runner.state.error_message or "publication dispatch identity changed"
        return await _park(runner, message)
    if not attempt.cleanup_confirmed:
        return await _park(runner, "publication cleanup proof is missing; operator recovery required")
    if attempt.exhausted:
        return await _exhaust(runner)
    now = datetime.now(timezone.utc)
    if attempt.reconcile_deadline is None:
        # Never grant a new budget to an incomplete/legacy exit snapshot.
        return await _park(runner, "publication retry deadline is missing; operator recovery required")
    if now >= attempt.reconcile_deadline:
        attempt.exhausted = True
        return await _exhaust(runner)
    if attempt.next_reconcile_at is not None and now < attempt.next_reconcile_at:
        return True
    attempt.next_reconcile_at = now + timedelta(seconds=runner.app_config.daemon.fix_poll_interval_sec)
    attempt.reconcile_polls += 1
    # Charge the poll before IO so restart cannot grant free unlimited retries.
    await runner.publish_state()
    resolved = await runner._resolve_published_attempt()
    if runner.state.current_task is None:
        return True
    if not resolved:
        await runner.publish_state()
        return True
    await runner._refresh_user_paused_from_redis()
    if runner.state.user_paused or await runner._pop_stop_request():
        await runner.publish_state()
        return True
    attempt = runner.state.publication_attempt
    pr = runner.state.current_pr
    if (runner.state.review_request_pr, runner.state.review_request_head) != (pr.number, pr.head_sha):
        runner.state._reset_review_request_history()
        runner.state.review_request_pr = pr.number
        runner.state.review_request_head = pr.head_sha
        runner.state.review_request_task = attempt.task_id
    is_fix = runner.state.active_invocation.phase == "fix"
    changed = attempt.expected_head_sha != (attempt.initial_branch_head_sha or attempt.initial_pr_head_sha)
    needs_review = not is_fix or attempt.expected_head_sha != attempt.initial_pr_head_sha
    if not attempt.push_accounted:
        pr.record_observed_head(attempt.expected_head_sha)
        if is_fix:
            pr.fix_iteration_count = max(pr.fix_iteration_count, attempt.initial_fix_iteration + changed)
            pr.no_push_fix_count = 0 if changed else pr.no_push_fix_count + 1
        attempt.push_accounted = True
        # Both the counter and its receipt are in the same durable snapshot.
        await runner.publish_state()
    if is_fix and not changed and pr.no_push_fix_count >= runner.app_config.daemon.fix_no_push_cap:
        attempt.pending = False
        await runner.publish_state()
        await runner._escalate_fix_no_push_deadlock(pr)
        return True
    runner._rehydrate_last_push_at(pr)
    if not attempt.continuation_saved:
        record = runner._current_run_record
        if record is not None and record.ended_at is None:
            await runner._save_current_run_record("coding_complete")
        attempt.continuation_saved = True
        await runner.publish_state()
    await runner._refresh_user_paused_from_redis()
    if runner.state.user_paused or await runner._pop_stop_request():
        await runner.publish_state()
        return True
    # A crash after POST but before its checkpoint must query provider history
    # before posting again. Unavailable history cannot authorize a duplicate.
    if needs_review and attempt.review_handoff_started:
        try:
            requested_at = gh_comments.latest_codex_review_request_time(
                attempt.repo, pr.number, after=runner._last_push_at or attempt.started_at,
            )
        except Exception as exc:
            attempt.incident = f"review handoff confirmation unavailable: {exc}"
            await runner.publish_state()
            return True
        if requested_at is not None:
            runner.state.review_request_pr = pr.number
            runner.state.review_request_head = pr.head_sha
            runner.state.review_requested_at = requested_at
    elif needs_review:
        attempt.review_handoff_started = True
        await runner.publish_state()
    await runner._refresh_user_paused_from_redis()
    if runner.state.user_paused or await runner._pop_stop_request():
        await runner.publish_state()
        return True
    if (needs_review and runner.state.review_requested_at is None
        and not runner._should_skip_codex_review_post(pr.number)):
        runner._post_codex_review(pr.number)
    attempt.pending = False
    attempt.incident = None
    runner.state.state = PipelineState.WATCH
    runner.log_event("[PUBLICATION] Exact intended PR head confirmed -> WATCH.")
    await runner.publish_state()
    return True


async def _park(runner: PipelineRunner, message: str) -> bool:
    if runner.state.error_message != message or runner.state.state != PipelineState.ERROR:
        await runner._transition_to_error(
            message, save_run_record_as=None, publish=True, log_prefix="[PUBLICATION]",
        )
    return True


async def _exhaust(runner: PipelineRunner) -> bool:
    """Park this task durably, then let the existing picker run independent work."""
    attempt = runner.state.publication_attempt
    message = (
        f"Publication reconciliation exhausted: expected={attempt.expected_head_sha}, "
        f"branch={attempt.observed_branch_sha}, PR={attempt.observed_pr_sha}; "
        f"boundary={attempt.incident or 'unobserved'}"
    )
    attempt.terminal_message = message
    record = runner._current_run_record
    await runner._transition_to_error(
        message, save_run_record_as="error" if record is not None and record.ended_at is None else None,
        publish=False, log_prefix="[PUBLICATION]",
        cancellation_cause=CancellationCause(category="ERROR", payload={
            "subsource": "crash", "reason_text": message,
            "publication_attempt": attempt.model_dump(mode="json"),
        }),
    )
    try:
        written = await runner._commit_task_status_change(
            runner.state.current_task, "ERROR", message, blocked_reason=SuppressionReason.CRASH,
        )
    except Exception as exc:
        runner.log_event(f"[PUBLICATION] Terminal status write deferred: {exc}")
        written = False
    if not written:
        await runner._mark_status_write_failed_task(
            runner.state.current_task, blocked_reason=SuppressionReason.CRASH, ensure_suppression=False,
        )
        return True
    if attempt.task_id in runner._status_write_failed_task_pr_ids:
        runner._status_write_failed_task_pr_ids.discard(attempt.task_id)
        await runner._persist_status_write_failed_task_pr_ids()
    # The task's suppression and ERROR header retain the actionable incident.
    # No DONE write or dependency release occurs. Its branch is never deleted.
    attempt.pending = False
    runner.state.current_task = None
    runner._current_run_record = None
    runner._reset_runner_local_task_counters()
    runner.state.state = PipelineState.IDLE
    await runner.publish_state()
    return True
