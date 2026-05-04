"""Escalation helpers extracted from ``handlers/fix.py`` (PR-230).

Each function takes the active ``PipelineRunner`` as the first argument
because the operations need access to ``runner.state``, ``runner.owner_repo``,
``runner.app_config``, ``runner.log_event`` and ``runner._escalate_to_hung`` /
``runner._transition_to_error`` primitives. ``handlers/fix.py`` keeps thin
wrapper methods on ``FixMixin`` so existing callers (regression tests in
``tests/runner/`` and the runner itself via ``_escalate_to_hung``) continue
to work unchanged.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.cancellation import (
    CancellationCause,
    safe_record_cancellation_cause,
)
from src.github import comments as gh_comments
from src.github import gh_runner
from src.models import PipelineState, PRInfo

if TYPE_CHECKING:
    from src.daemon.runner import PipelineRunner

_ESCALATE_MARKER_PREFIX = "ESCALATE:"
_ESCALATE_EMPTY_REASON = "(no reason provided)"


def parse_escalate_marker(stdout: str) -> str | None:
    """Return the coder-supplied ESCALATE reason, or ``None``.

    The marker is recognized only when the LAST non-empty line of ``stdout``
    starts with the literal prefix ``"ESCALATE:"`` at column 0 (strict
    case-sensitive match — variants like ``escalate:``, ``ESCALATED:``, or
    an indented ``"  ESCALATE:"`` do not trigger). An empty reason
    (``"ESCALATE:"`` alone, or only trailing whitespace) is returned as the
    empty string so the caller can substitute a placeholder rather than crash.
    """
    for line in reversed(stdout.splitlines()):
        if not line.strip():
            continue
        if line.startswith(_ESCALATE_MARKER_PREFIX):
            return line[len(_ESCALATE_MARKER_PREFIX):].strip()
        return None
    return None


def ensure_escalated_label(
    runner: "PipelineRunner",
    pr_number: int,
    label_create_log_prefix: str,
) -> bool:
    """Create (idempotent) and apply the ``escalated`` label on ``pr_number``.

    Both ``gh`` calls soft-fail with ``log_event``: the in-memory
    ``is_escalated`` flag remains the load-bearing signal, and the label is
    a best-effort hint for ``get_open_prs`` rehydration after a daemon
    restart. ``label_create_log_prefix`` keeps the existing log strings
    stable so callers (no-push deadlock vs. coder-initiated ESCALATE) remain
    distinguishable in history.

    Returns ``True`` when ``pr edit --add-label`` succeeded, ``False``
    otherwise. Callers that park the runner in a state which depends on
    the label for durability check this return to downgrade.
    """
    try:
        gh_runner.run_gh(
            [
                "label",
                "create",
                "escalated",
                "--color",
                "B60205",
                "--description",
                "Daemon escalated, manual review required",
            ],
            repo=runner.owner_repo,
        )
    except Exception as exc:
        runner.log_event(
            f"[FIX] {label_create_log_prefix} label create skipped: {exc}."
        )
    try:
        gh_runner.run_gh(
            ["pr", "edit", str(pr_number), "--add-label", "escalated"],
            repo=runner.owner_repo,
        )
        return True
    except Exception as exc:
        runner.log_event(
            f"[FIX] Warning: failed to apply escalated label to PR "
            f"#{pr_number}: {exc}."
        )
        return False


def apply_canceled_label(runner: "PipelineRunner", pr_number: int) -> None:
    """Best-effort: ensure ``canceled`` label exists and apply it to ``pr_number``.

    Mirrors ``ensure_escalated_label`` but for the PR-258 cancellation
    surface: a ``canceled`` label gives operators a GitHub-side hint
    that the daemon surrendered the task with a structured cause. Both
    ``gh`` calls soft-fail with ``log_event`` so a GitHub outage never
    blocks the IDLE transition.
    """
    try:
        gh_runner.run_gh(
            [
                "label",
                "create",
                "canceled",
                "--color",
                "B60205",
                "--description",
                "Daemon canceled this task; manual recovery required",
            ],
            repo=runner.owner_repo,
        )
    except Exception as exc:
        runner.log_event(
            f"[FIX] FIX no-push canceled label create skipped: {exc}."
        )
    try:
        gh_runner.run_gh(
            ["pr", "edit", str(pr_number), "--add-label", "canceled"],
            repo=runner.owner_repo,
        )
    except Exception as exc:
        runner.log_event(
            f"[FIX] Warning: failed to apply canceled label to PR "
            f"#{pr_number}: {exc}."
        )


async def escalate_fix_no_push_deadlock(
    runner: "PipelineRunner", current_pr: PRInfo
) -> None:
    """Cancel the task via Cancellation policy after a no-push deadlock.

    PR-258 (OBS-BB) replaces the prior HUNG transition with a cancellation
    policy v1 transition: write a ``NO_PUSH_DEADLOCK`` cause to Redis,
    apply the ``canceled`` label to the PR, mark the task CANCELED for the
    next IDLE cycle, and return to IDLE so the daemon picks up the next
    pickable task. Looping retrigger via HUNG/WATCH wastes coder budget
    when the missing artifact is a fix push (not a Codex review), so the
    cleaner signal is a structured surrender of the task.
    """
    attempts = current_pr.no_push_fix_count
    pr_number = current_pr.number
    runner.log_event(
        f"[FIX] PR #{pr_number} no-push deadlock after {attempts} "
        "attempts; canceling task with cause preserved."
    )

    current_task = runner.state.current_task
    task_id = (
        current_task.pr_id
        if current_task is not None
        else f"pr_{pr_number}"
    )
    cause = CancellationCause(
        category="NO_PUSH_DEADLOCK",
        payload={
            "attempts": attempts,
            "pr_number": pr_number,
            "head_sha": current_pr.head_sha or "",
        },
    )
    await safe_record_cancellation_cause(
        runner.redis,
        runner.name,
        task_id,
        cause,
        log=runner.log_event,
    )

    apply_canceled_label(runner, pr_number)

    current_pr.no_push_fix_count = 0
    if current_task is not None:
        runner._recovered_task_pr_ids.add(current_task.pr_id)
        await runner._persist_recovered_task_pr_ids()

    runner.state.current_task = None
    runner._reset_runner_local_task_counters()
    runner.state.state = PipelineState.IDLE
    await runner.publish_state()


async def escalate_fix_coder_initiated(
    runner: "PipelineRunner", current_pr: PRInfo, reason: str
) -> None:
    """Park the PR after the coder emits an ESCALATE marker.

    Posts a fix.py-specific failure-log comment and then routes state via
    ``_escalate_to_hung``. On label-apply success the runner parks in
    ``IDLE`` so the next refresh rehydrates ``is_escalated`` from the
    GitHub label. On label-apply failure ``HUNG`` is used so the in-memory
    flag stays the load-bearing parking signal during a GitHub outage
    (Codex P1 on PR #228).
    """
    pr_number = current_pr.number
    clean_reason = reason.strip() or _ESCALATE_EMPTY_REASON
    comment = (
        f"Coder explicitly escalated this PR. Reason: {clean_reason}. "
        "Manual review required."
    )
    try:
        gh_comments.post_comment(runner.owner_repo, pr_number, comment)
    except Exception as exc:
        runner.log_event(
            f"[FIX] Warning: failed to post FIX coder ESCALATE comment "
            f"on PR #{pr_number}: {exc}."
        )
    label_applied = ensure_escalated_label(
        runner, pr_number, "FIX coder ESCALATE"
    )
    if label_applied:
        await runner._escalate_to_hung(
            f"FIX coder ESCALATE on PR #{pr_number}: {clean_reason}. "
            "Moving to IDLE.",
            target_state=PipelineState.IDLE,
            error_message_override=None,
            apply_escalated_label=False,
        )
        return
    await runner._escalate_to_hung(
        f"FIX coder ESCALATE on PR #{pr_number}: failed to apply "
        f"`escalated` label. Reason: {clean_reason}. Manual "
        "review required.",
        apply_escalated_label=False,
    )


async def escalate_fix_iteration_cap(
    runner: "PipelineRunner", current_pr: PRInfo
) -> None:
    """Escalate the PR after the FIX iteration cap is reached.

    The comment-post and ``pr edit --add-label`` failure paths route to
    ``ERROR`` (durable parking signal for daemon-driven escalation,
    distinct from the coder-initiated ``HUNG`` fallback). The success path
    delegates to ``_escalate_to_hung`` for the IDLE transition +
    ``[ESCALATE]`` log + publish.
    """
    count = current_pr.fix_iteration_count
    fix_iteration_cap = runner.app_config.daemon.fix_iteration_cap
    pr_number = current_pr.number
    comment = (
        "@AlexBomber12 FIX iteration cap reached "
        f"({count}/{fix_iteration_cap}). Escalating for manual review."
    )
    try:
        gh_comments.post_comment(runner.owner_repo, pr_number, comment)
    except Exception as exc:
        await runner._transition_to_error(
            f"post_comment failed: {exc}",
            save_run_record_as=None,
            publish=False,
            log_prefix="[FIX]",
        )
        return
    try:
        gh_runner.run_gh(
            [
                "label",
                "create",
                "escalated",
                "--color",
                "B60205",
                "--description",
                "Daemon escalated, manual review required",
            ],
            repo=runner.owner_repo,
        )
    except Exception as exc:
        runner.log_event(f"[FIX] FIX cap label create skipped: {exc}.")
    try:
        gh_runner.run_gh(
            ["pr", "edit", str(pr_number), "--add-label", "escalated"],
            repo=runner.owner_repo,
        )
    except Exception as exc:
        await runner._transition_to_error(
            f"pr edit failed: {exc}",
            save_run_record_as=None,
            publish=False,
            log_prefix="[FIX]",
        )
        return
    await runner._escalate_to_hung(
        f"FIX cap reached ({count}/{fix_iteration_cap}) on PR "
        f"#{pr_number}: escalated, moving to IDLE.",
        target_state=PipelineState.IDLE,
        error_message_override=None,
        apply_escalated_label=False,
    )
