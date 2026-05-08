"""Background supervision helpers extracted from ``handlers/fix.py`` (PR-230).

The FIX cycle runs the coder under three concurrent supervisors:

* idle-timeout monitor (``monitor_fix_idle``)
* external-merge poller  (``poll_github_during_fix`` + the terminal-state
  handler ``handle_external_terminal_pr_state``)
* the wrapper that wires the poller into the running coder task
  (``run_coder_with_polling``)

Each function takes the ``PipelineRunner`` as its first argument so it can
read ``runner.state``, ``runner.app_config``, and the runner's instance
helpers. ``handlers/fix.py`` keeps thin wrapper methods on ``FixMixin`` so
existing tests that ``monkeypatch.setattr(FixMixin, "_poll_github_during_fix"
, ...)`` continue to win — the wrapper resolves through the class so the
patch is picked up before the implementation function runs.
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

from src.cancellation import CancellationCause, safe_record_cancellation_cause
from src.github import prs as gh_prs
from src.models import PipelineState

if TYPE_CHECKING:
    from src.daemon.runner import PipelineRunner


async def monitor_fix_idle(
    runner: "PipelineRunner",
    pr_number: int,
    idle_limit: int,
    target: asyncio.Task,  # type: ignore[type-arg]
    idle_flag: dict[str, bool],
) -> None:
    """Cancel *target* if no new push is detected within *idle_limit* seconds."""
    primed = False
    try:
        await asyncio.to_thread(
            gh_prs.get_branch_last_push_time,
            runner.owner_repo, pr_number,
        )
        primed = True
    except gh_prs.GitHubPollError:
        pass

    poll_interval = min(60, idle_limit)
    now = time.monotonic()
    head_age = await asyncio.to_thread(
        gh_prs.get_last_push_age_seconds,
        runner.owner_repo, pr_number,
    )
    if head_age is not None:
        backdate = min(head_age, idle_limit - poll_interval)
        last_known_push = now - max(0.0, backdate)
    else:
        last_known_push = now
    while True:
        await asyncio.sleep(poll_interval)
        try:
            latest_push_at = await asyncio.to_thread(
                gh_prs.get_branch_last_push_time,
                runner.owner_repo, pr_number,
            )
            if not primed:
                primed = True
                if latest_push_at is not None:
                    last_known_push = time.monotonic()
        except gh_prs.GitHubPollError as exc:
            runner.log_event(
                f"[FIX] GitHub API poll failed: {exc}, preserving deadline."
            )
            latest_push_at = None
        if latest_push_at is not None and latest_push_at > last_known_push:
            last_known_push = latest_push_at
            runner.log_event(
                f"[FIX] [{runner.state.coder or 'coder'}] pushed, "
                f"resetting idle timer."
            )
        elapsed = time.monotonic() - last_known_push
        if elapsed >= idle_limit:
            runner.log_event(
                f"[FIX] idle timeout ({idle_limit}s since last push), "
                f"killing."
            )
            idle_flag["timed_out"] = True
            target.cancel()
            return


async def poll_github_during_fix(
    runner: "PipelineRunner",
    pr_number: int,
    target: asyncio.Task,  # type: ignore[type-arg]
    terminal_flag: dict[str, str | None],
) -> None:
    """Watch the PR for an external MERGED/CLOSED while FIX is in flight.

    Polls ``gh_prs.pr_state`` every
    ``app_config.daemon.fix_poll_interval_sec`` seconds. When a terminal
    state is observed, records it in ``terminal_flag``, terminates the
    active coder subprocess (SIGTERM with grace before SIGKILL via
    ``_terminate_current_coder``) and cancels ``target`` so the awaiting
    handler observes the cancellation.

    Inherits PR-163 GitHub API budget awareness: when the cached budget
    is below the pause threshold, this iteration is skipped rather than
    spending quota on observability polling.

    Transient ``gh pr view`` failures are logged once per failure and
    the loop continues — observability must never crash the daemon.
    Cancellation (the FIX cycle finished normally) exits cleanly via
    the standard ``CancelledError`` propagation.
    """
    poll_interval = runner.app_config.daemon.fix_poll_interval_sec
    while True:
        await asyncio.sleep(poll_interval)
        if runner._github_api_budget_paused():
            continue
        try:
            state_info = await asyncio.to_thread(
                gh_prs.pr_state, runner.owner_repo, pr_number
            )
        except Exception as exc:
            runner.log_event(
                f"[FIX] GitHub poll for PR #{pr_number} failed: {exc}."
            )
            continue
        if state_info is None:
            runner.log_event(
                f"[FIX] GitHub poll for PR #{pr_number} returned no "
                f"data."
            )
            continue
        state = (state_info.get("state") or "").upper()
        if state not in {"MERGED", "CLOSED"}:
            continue
        terminal_flag["state"] = state
        runner.log_event(
            f"[FIX] PR #{pr_number} reached terminal state {state} "
            f"during FIX, requesting coder termination."
        )
        await runner._terminate_current_coder()
        target.cancel()
        return


def run_coder_with_polling(
    runner: "PipelineRunner",
    pr_number: int,
    target: asyncio.Task,  # type: ignore[type-arg]
    terminal_flag: dict[str, str | None],
) -> "asyncio.Task[None] | None":
    """Spawn the polling task that runs concurrently with the coder.

    Returns the polling task so the caller can cancel it when the coder
    exits normally (try/finally). Returns ``None`` when no live PR
    number is available so the polling logic only engages for real PRs
    (not the initial coding cycle that has not yet produced one).

    The polling coroutine is obtained via
    ``runner._poll_github_during_fix`` (bound method, dispatched through
    the class) rather than calling the module-level function directly.
    Tests that ``monkeypatch.setattr(FixMixin, "_poll_github_during_fix"
    , ...)`` rely on this indirection.
    """
    if pr_number <= 0:
        return None
    return asyncio.create_task(
        runner._poll_github_during_fix(pr_number, target, terminal_flag)
    )


async def handle_external_terminal_pr_state(
    runner: "PipelineRunner", terminal_state: str
) -> None:
    """Transition the runner when an external MERGED/CLOSED is observed.

    On ``MERGED``: reset the FIX recovery counters on the active PR (so
    the next PR does not inherit a stale streak), mark the queue task
    DONE if applicable, drop ``current_pr`` / ``current_task``, and
    return to ``IDLE``.

    On ``CLOSED``: record a crash cause, clear the task, and return to IDLE
    so the queue can continue.
    """
    pr = runner.state.current_pr
    pr_number_str = f"#{pr.number}" if pr is not None else ""
    if terminal_state == "MERGED":
        if pr is not None:
            pr.no_push_fix_count = 0
            pr.fix_iteration_count = 0
        runner.log_event(
            f"[FIX] PR {pr_number_str} merged externally during FIX, "
            f"returning to IDLE."
        )
        await runner._save_current_run_record("success_merged")
        runner._current_run_record = None
        runner._mark_task_done_in_snapshot()
        runner.state.current_task = None
        runner._reset_runner_local_task_counters()
        runner.state.state = PipelineState.IDLE
        await runner.publish_state()
        return
    # CLOSED
    runner.log_event(
        f"[FIX] PR {pr_number_str} closed externally during FIX, "
        f"skipping task and returning to IDLE."
    )
    # Finalize the run record before clearing the task so closed PR metrics
    # retain their explicit exit reason.
    await runner._save_current_run_record("closed_unmerged")
    runner._current_run_record = None
    current_task = runner.state.current_task
    if current_task is not None:
        await safe_record_cancellation_cause(
            runner.redis,
            runner.name,
            current_task.pr_id,
            CancellationCause(
                category="CRASH",
                payload={
                    "closed_externally": True,
                    "pr_number": pr.number if pr is not None else None,
                },
            ),
            log=runner.log_event,
        )
        try:
            status_written = await runner._commit_task_status_change(
                current_task,
                "ERROR",
                "PR closed externally during FIX",
            )
        except Exception as exc:  # pragma: no cover - defensive status-write logging.
            runner.log_event(
                f"[ERROR] Failed to write status:ERROR to "
                f"{current_task.task_file}: {exc}"
            )
            status_written = False
        if not status_written:
            runner._mark_status_write_failed_task(current_task)
    runner.state.error_message = None
    runner.state.current_task = None
    runner._reset_runner_local_task_counters()
    runner.state.state = PipelineState.IDLE
    await runner.publish_state()
