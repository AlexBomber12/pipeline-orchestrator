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


async def escalate_fix_no_push_deadlock(
    runner: "PipelineRunner", current_pr: PRInfo
) -> None:
    """Park the PR in HUNG after consecutive no-push FIX cycles.

    Thin wrapper over ``_escalate_to_hung``: posts a deadlock comment with
    a fix.py-specific failure-log prefix, resets the no-push counter, then
    delegates state transition / label apply / is_escalated bookkeeping to
    the primitive. ``error_message`` is cleared because HUNG itself is the
    parking signal.
    """
    count = current_pr.no_push_fix_count
    pr_number = current_pr.number
    message = (
        f"FIX deadlock: {count} consecutive no-push FIX cycles on PR "
        f"#{pr_number}. Coder unable to identify actionable fix. "
        "Manual review required."
    )
    try:
        gh_comments.post_comment(runner.owner_repo, pr_number, message)
    except Exception as exc:
        runner.log_event(
            f"[FIX] Warning: failed to post FIX deadlock comment on PR "
            f"#{pr_number}: {exc}."
        )
    current_pr.no_push_fix_count = 0
    await runner._escalate_to_hung(
        message,
        error_message_override=None,
        label_create_log_prefix="FIX no-push",
    )


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
