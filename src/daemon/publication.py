"""Durable continuation of the existing coder publication attempt.

Exit, remote branch, PR adoption, and process cleanup are independent evidence.
The runner owns polling; a poll never reserves another coder invocation.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from src.cancellation import CancellationCause, get_cancellation_cause
from src.daemon import fix_escalation, git_ops
from src.daemon.guardrails import scan_stdout
from src.github import checks as gh_checks
from src.github import comments as gh_comments
from src.github import prs as gh_prs
from src.keyspace import control_stop, pipeline_state
from src.models import CIStatus, PipelineState, PublicationAttempt, RepoState, ReviewStatus
from src.subsource_registry import SuppressionReason

if TYPE_CHECKING:
    from src.daemon.runner import PipelineRunner
    from src.retry_commands import RetryCommand


async def record_exit(
    runner: PipelineRunner, code: int, stdout: str, stderr: str, *, publication_cancelled: bool = False,
    terminal_cancelled: bool = False,
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
        attempt.terminal_cancelled = terminal_cancelled
        attempt.exit_classification = (
            "success" if (code == 0 or publication_cancelled or terminal_cancelled)
            and not scan_stdout(f"{stdout}\n{stderr}")
            and fix_escalation.parse_escalate_marker(stdout) is None else "failure"
        )
    if attempt.reconcile_deadline is None:
        attempt.reconcile_deadline = datetime.now(timezone.utc) + timedelta(
            minutes=runner.repo_config.review_timeout_min or runner.app_config.daemon.review_timeout_min,
        )
    if (code > 0 and runner.state.active_invocation is not None
        and not terminal_cancelled and not scan_stdout(f"{stdout}\n{stderr}")
        and fix_escalation.parse_escalate_marker(stdout) is None):
        await pause_provider_failure(runner, stderr, runner.state.active_invocation.plugin_id)
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
        attempt.provider_published_head_sha = expected
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
        if attempt.terminal_cancelled:
            attempt.incident = "supervisor-cancelled FIX awaits exact terminal PR evidence"
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


async def _operator_paused(runner: PipelineRunner) -> bool:
    """Latch legacy Stop durably; publish_state acknowledges after cleanup."""
    await runner._refresh_user_paused_from_redis()
    if not runner.state.user_paused:
        state_key, stop_key = pipeline_state(runner.name), control_stop(runner.name)

        async def latch(pipe) -> bool:
            if not await pipe.get(stop_key):
                return False
            raw = await pipe.get(state_key)
            persisted = RepoState.model_validate_json(raw) if raw else runner.state.model_copy(deep=True)
            persisted.user_paused = True
            persisted.state = PipelineState.PAUSED
            pipe.multi()
            pipe.set(state_key, persisted.model_dump_json())
            return True

        # Watch both keys so a concurrent Play cannot be overwritten by a
        # stale Stop read. Persistence errors leave reconciliation fail-closed.
        if await runner.redis.transaction(latch, state_key, stop_key, value_from_callable=True):
            runner._stop_requested = True
            runner.state.user_paused = True
            runner.state.state = PipelineState.PAUSED
            runner.log_event("[INFRA] Stop latched during publication reconciliation -> PAUSED.")
    if runner.state.user_paused:
        await runner.publish_state()
        return True
    return False


async def hold(runner: PipelineRunner) -> bool:
    """One bounded reconciliation tick, shared by every lifecycle entry point."""
    attempt = runner.state.publication_attempt
    if await _operator_paused(runner):
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
    if await _operator_paused(runner):
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
        if not _run_matches(runner):
            await runner._restore_current_run_record()
        if not _run_matches(runner):
            attempt.incident = "bound run accounting is unavailable"
            await runner.publish_state()
            return True
        record = runner._current_run_record
        if record.ended_at is None:
            await runner._save_current_run_record(
                "coding_complete", run_phase=runner.state.active_invocation.phase,
            )
        attempt.continuation_saved = True
        await runner.publish_state()
    if await _operator_paused(runner):
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
    if await _operator_paused(runner):
        return True
    if (needs_review and runner.state.review_requested_at is None
        and not runner._should_skip_codex_review_post(pr.number)):
        runner._post_codex_review(pr.number)
    attempt.pending = False
    attempt.incident = None
    runner.state.state = PipelineState.WATCH
    runner.log_event("[INFRA] Exact intended PR head confirmed -> WATCH.")
    await runner.publish_state()
    return True


async def _park(runner: PipelineRunner, message: str) -> bool:
    if runner.state.error_message != message or runner.state.state != PipelineState.ERROR:
        await runner._transition_to_error(
            message, save_run_record_as=None, publish=True, log_prefix="[INFRA]",
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
        publish=False, log_prefix="[INFRA]",
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
        runner.log_event(f"[INFRA] Terminal status write deferred: {exc}")
        written = False
    if not written:
        await runner._mark_status_write_failed_task(
            runner.state.current_task, blocked_reason=SuppressionReason.CRASH, ensure_suppression=False,
        )
        return True
    if attempt.task_id in runner._status_write_failed_task_pr_ids:
        runner._status_write_failed_task_pr_ids.discard(attempt.task_id)
        await runner._persist_status_write_failed_task_pr_ids()
    # Exhaustion ends polling, but does not prove remote outcome or work safety.
    # Use the same terminal policy as ERROR dispatch before releasing a writer.
    attempt.pending = False
    await runner._release_finalized_failure(SuppressionReason.CRASH)
    await runner.publish_state()
    return True


def _run_matches(runner: PipelineRunner) -> bool:
    attempt = runner.state.publication_attempt
    record = runner._current_run_record
    return bool(record is not None and (
        record.repo_name, record.task_id, record.run_id, record.attempt_index,
    ) == (runner.name, attempt.task_id, attempt.run_id, attempt.attempt_index))


async def retry_floor(runner: PipelineRunner, command: RetryCommand) -> PublicationAttempt | None:
    """Keep exhausted publication evidence binding after independent work runs.

    Operator Retry may authorize continuation, but cannot discard a newer
    expected commit merely because open-PR discovery still reports an old head.
    """
    cause = await get_cancellation_cause(runner.redis, runner.name, command.task_id, refresh_ttl=False)
    raw = cause.payload.get("publication_attempt") if cause is not None else None
    if raw is None:
        return None
    floor = PublicationAttempt.model_validate(raw)
    if floor.terminal_cancelled:
        raise ValueError("Supervisor-cancelled FIX requires terminal bookkeeping, not open-PR continuation")
    if (floor.contract_version != 2 or not floor.exhausted or not floor.cleanup_confirmed
        or not floor.expected_head_sha or floor.pr_number is None
        or (floor.repo, floor.base, floor.task_id, floor.branch) != (
            runner.owner_repo, runner.repo_config.branch or "main", command.task_id, command.task_branch,
        )):
        raise ValueError("Retained publication incident lacks matching identity or cleanup proof")
    from src.daemon.handlers.coding import _remote_branch_exists

    if await asyncio.to_thread(
        _remote_branch_exists, runner.repo_path, floor.branch, floor.expected_head_sha,
    ) is not True:
        raise ValueError("Retained expected commit is not confirmed at the remote branch")
    outcome = await asyncio.to_thread(
        gh_prs.get_exact_pr_outcome, floor.repo, floor.base, floor.pr_number,
        floor.expected_head_sha, expected_branch=floor.branch,
    )
    if not outcome.discovery_complete or outcome.state != gh_prs.ExactPRState.OPEN:
        raise ValueError("Retained expected PR head is unresolved or terminal; implementation will not be rerun")
    return floor


PROVIDER_RECOVERY_EXHAUSTED = (
    "Provider recovery exhausted after 3 interrupted FIX attempts; retained work requires Retry"
)


async def pause_provider_failure(runner: PipelineRunner, stderr: str, coder_name: str) -> bool:
    """Retain interrupted FIX work, independently of quota and publication success."""
    from src.provider_errors import is_capacity_error

    attempt = runner.state.publication_attempt
    invocation = runner.state.active_invocation
    if attempt is None or invocation is None or invocation.phase != "fix":
        return False
    capacity = is_capacity_error(stderr)
    if (not capacity and attempt.provider_error is None
        and not runner._detect_rate_limit(stderr, coder_name=coder_name)):
        return False
    until = runner.state.rate_limited_until
    if not capacity and (until is None or until <= datetime.now(timezone.utc)):
        return False
    first_observation = attempt.provider_error is None
    attempt.provider_error = "capacity" if capacity else "rate_limit"
    attempt.provider_retries += int(first_observation)
    attempt.provider_retry_ready = False
    attempt.provider_retry_at = attempt.provider_retry_at or (
        datetime.now(timezone.utc) + timedelta(
            seconds=runner.app_config.daemon.fix_poll_interval_sec * 2 ** min(attempt.provider_retries - 1, 2),
        ) if capacity else until
    )
    attempt.exit_classification = "failure"
    if runner._current_run_record is not None and runner._current_run_record.ended_at is None:
        await runner._save_current_run_record("paused" if capacity else "rate_limit", run_phase="fix")
    runner.state.error_message = None
    runner.state.state = PipelineState.PAUSED
    runner.log_event(f"[INFRA] {attempt.provider_error}: retained FIX; retry {attempt.provider_retries}/3 scheduled.")
    await runner.publish_state()
    return True


async def resume_provider_failure(runner: PipelineRunner) -> bool:
    """Resume only the bound FIX checkout; never adopt the older remote head."""
    attempt = runner.state.publication_attempt
    invocation = runner.state.active_invocation
    if (attempt is not None and attempt.pending and attempt.provider_error is not None
        and (invocation is None or invocation.phase != "fix")):
        if await _operator_paused(runner):
            return True
        return await _park(runner, "Interrupted FIX invocation binding is missing; inspect retained work and use Retry")
    if (attempt is None or not attempt.pending or invocation is None
        or invocation.phase != "fix" or attempt.exit_classification == "success"
        or (attempt.provider_error is None
            and not (runner.state.state == PipelineState.PAUSED and runner.state.rate_limited_until is not None)
            and not (runner.state.error_message or "").startswith("Interrupted FIX"))):
        return False
    if await _operator_paused(runner):
        return True
    # Legacy pauses may retain an open record despite a checkpointed exit.
    # Exit and cleanup proof can close accounting independently of publication.
    if (attempt.exit_classification == "failure" and attempt.cleanup_confirmed
        and runner._publication_attempt_matches_dispatch()):
        if not _run_matches(runner):
            await runner._restore_current_run_record()
        if _run_matches(runner) and runner._current_run_record.ended_at is None:
            await runner._save_current_run_record(
                "paused" if attempt.provider_error else "error", run_phase="fix",
            )
    now = datetime.now(timezone.utc)
    until = runner.state.rate_limited_until
    if until is not None:
        until = until if until.tzinfo else until.replace(tzinfo=timezone.utc)
        if now < until:
            runner.state.state = PipelineState.PAUSED
            return True
        runner._clear_rate_limit(runner.state.rate_limit_reactive_coder or invocation.plugin_id)
        runner._clear_global_pause_fields()
    if attempt.provider_error is not None and runner.state.error_message:
        # An exhausted or independently blocked attempt remains parked until Retry.
        runner.state.state = PipelineState.ERROR
        return True
    if attempt.provider_retry_at is not None and now < attempt.provider_retry_at:
        runner.state.state = PipelineState.PAUSED
        return True
    if not attempt.provider_error:
        return await _park(
            runner, "Interrupted FIX lacks bound provider failure evidence; inspect retained work and use Retry",
        )
    if attempt.provider_retries >= 3:
        return await _park(
            runner, PROVIDER_RECOVERY_EXHAUSTED,
        )
    try:
        problem = await provider_retry_problem(runner)
        if problem is not None:
            return await _park(runner, problem)
    except Exception as exc:
        return await _defer_provider_observation(runner, f"ownership observation unavailable: {type(exc).__name__}")
    attempt.provider_retry_ready = True
    runner.state.error_message = None
    runner.state.state = PipelineState.FIX
    await runner.publish_state()
    if await _operator_paused(runner):
        return True
    await runner.handle_fix()
    return True


async def _defer_provider_observation(runner: PipelineRunner, boundary: str) -> bool:
    attempt = runner.state.publication_attempt
    now = datetime.now(timezone.utc)
    if attempt.reconcile_deadline is None or now >= attempt.reconcile_deadline:
        message = f"Interrupted FIX {boundary}; observation budget exhausted; inspect and use Retry"
        attempt.provider_observation_failure = message
        return await _park(runner, message)
    attempt.incident = boundary
    attempt.provider_retry_at = now + timedelta(seconds=runner.app_config.daemon.fix_poll_interval_sec)
    runner.state.state = PipelineState.PAUSED
    await runner.publish_state()
    return True


def provider_retry_owns_failure(attempt: PublicationAttempt, message: str | None) -> bool:
    """Only clear provider-owned parks, never independent implementation failures."""
    return message is None or message == PROVIDER_RECOVERY_EXHAUSTED or (
        attempt.provider_observation_failure is not None and message == attempt.provider_observation_failure
    )


async def provider_retry_problem(
    runner: PipelineRunner, *, allow_provider_park: bool = False,
) -> str | None:
    """Observe retained FIX ownership without resetting allowance or dispatching."""
    attempt = runner.state.publication_attempt
    pr = runner.state.current_pr
    if (attempt.contract_version != 2 or attempt.exit_classification != "failure"
        or (attempt.provider_retries == 0 and attempt.provider_retry_command_id is None)
        or not runner._publication_attempt_matches_dispatch() or not attempt.cleanup_confirmed
        or not attempt.expected_head_sha or not attempt.initial_pr_head_sha
        or (attempt.failure is not None and not (
            allow_provider_park and provider_retry_owns_failure(attempt, attempt.failure)
        ))
        or (runner.state.error_message is not None and not (
            allow_provider_park and provider_retry_owns_failure(attempt, runner.state.error_message)
        ))
        or pr is None or pr.number != attempt.pr_number or pr.branch != attempt.branch
        or pr.is_cross_repository or pr.pr_id not in {None, attempt.task_id}
        or pr.head_sha not in {
            attempt.initial_pr_head_sha, attempt.expected_head_sha, attempt.provider_published_head_sha,
        }):
        return "Interrupted FIX lacks matching identity, exit, or cleanup proof; use Retry"
    if not _run_matches(runner):
        await runner._restore_current_run_record()
    if not _run_matches(runner):
        return "Interrupted FIX run accounting is missing; restore bound run before Retry"
    branch = (await asyncio.to_thread(git_ops._git, runner.repo_path, "branch", "--show-current")).stdout.strip()
    head = (await asyncio.to_thread(git_ops._git, runner.repo_path, "rev-parse", "HEAD")).stdout.strip()
    remote = (await asyncio.to_thread(
        git_ops._git, runner.repo_path, "ls-remote", "--heads", "origin", f"refs/heads/{attempt.branch}",
    )).stdout.split()
    if branch != attempt.branch or head != attempt.expected_head_sha:
        return "Interrupted FIX checkout changed; retain both versions and use Retry"
    if len(remote) != 2 or remote[1] != f"refs/heads/{branch}" or remote[0] not in {
        attempt.initial_pr_head_sha, attempt.expected_head_sha, attempt.provider_published_head_sha,
    }:
        return "Interrupted FIX remote head changed independently; preserve both sides and use Retry"
    outcome = await asyncio.to_thread(
        gh_prs.get_exact_pr_outcome, attempt.repo, attempt.base, attempt.pr_number, remote[0],
        expected_branch=attempt.branch,
    )
    if not outcome.discovery_complete:
        raise RuntimeError("exact open PR evidence unavailable")
    if outcome.state != gh_prs.ExactPRState.OPEN:
        return "Interrupted FIX exact PR is terminal; use Retry after inspection"
    attempt.provider_published_head_sha = remote[0]
    return None
