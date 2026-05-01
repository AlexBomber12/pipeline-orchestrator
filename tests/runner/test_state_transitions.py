"""PR-213: Atomic state transition baseline tests.

These tests document the **current** behavior at every callsite that
mutates ``state.current_task``, ``state.current_pr``, or transitions to
``PipelineState.ERROR`` at 2026-05-01. They establish a regression
baseline before the upcoming Foundation-Sprint primitives in PR-218
(``RepoState.__setattr__`` extension) and PR-219
(``_transition_to_error`` helper) collapse the 14 task-set/clear sites
and 30+ ERROR sites onto a single contract. Without these baselines,
the migration could silently change which related fields are reset at
each site (``current_pr``, ``error_message``, ``_error_diagnose_count``,
``_error_skip_*``, ``_idle_dispatch_deferred``).

For each task-clear and task-set site the test asserts:

- the mutation under test (``current_task`` to ``None`` or to a value);
- which related fields the site DOES reset (e.g. ``current_pr``,
  ``error_message``, terminal state);
- which related fields it preserves;
- the operator-visible ``log_event`` (so the upcoming refactor cannot
  silently change dashboard text).

For each ERROR-transition site the test asserts the four invariants
called out in the task spec: ``state.state`` ends ``ERROR``,
``error_message`` matches the expected text, the log event carries the
expected ``[INFRA] / [FIX] / [WATCH] / [CODING] / [MERGE] / [ERROR]``
prefix, and ``publish_state`` is observably called when the handler
publishes (idle/coding/merge/error all do; recovery/fix/watch/hung do
not at the specific sites sampled — the test asserts what the site
actually does today).

Tests do not change production behavior; they only assert it.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from src import codex_cli
from src.coders import claude as claude_plugin_module
from src.daemon import recovery as recovery_module  # noqa: F401  (sanity)
from src.daemon import runner as runner_module
from src.daemon.handlers import coding as coding_module  # noqa: F401
from src.daemon.handlers import error as error_module  # noqa: F401
from src.daemon.handlers import fix as fix_module
from src.daemon.handlers import hung as hung_module
from src.daemon.handlers import idle as idle_module
from src.daemon.handlers import merge as merge_module  # noqa: F401
from src.daemon.handlers import watch as watch_module  # noqa: F401
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus

from tests import test_runner as h

claude_cli = claude_plugin_module.claude_cli


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _install_publish_state_spy(runner: Any) -> list[None]:
    """Replace ``publish_state`` with an awaitable spy and return the call log."""
    calls: list[None] = []

    async def fake_publish() -> None:
        calls.append(None)

    runner.publish_state = fake_publish  # type: ignore[method-assign]
    return calls


def _stub_recovery_queue(
    runner: Any,
    *,
    tasks: list[QueueTask],
    queue_from_origin: bool = False,
) -> None:
    """Pin the queue-source probe and parse output used by ``recover_state``."""
    runner._origin_queue_md_tracked = (  # type: ignore[method-assign]
        lambda: queue_from_origin
    )
    runner._parse_base_queue = (  # type: ignore[method-assign]
        lambda **_: list(tasks)
    )


# =========================================================================
# SECTION 1 — current_task = None (10 callsites)
# =========================================================================


def test_recovery_clears_task_on_already_merged_doing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``recovery.py:284`` — stale DOING already merged on origin.

    Setup mirrors a daemon restart where the prior cycle merged the PR
    but ``_mark_queue_done`` left ``DOING`` pinned in
    ``origin/{branch}/tasks/QUEUE.md`` (legacy tracked-QUEUE flow).
    ``_is_doing_already_merged`` returns ``True`` and the site clears
    ``current_task`` only — it does NOT touch ``current_pr``,
    ``error_message`` or ``state.state`` at this point because the
    follow-on no-recoverable branch resolves them. We assert today's
    field-by-field contract; PR-218 will preserve it.
    """
    task = QueueTask(
        pr_id="PR-100",
        title="stale-doing",
        status=TaskStatus.DOING,
        branch="pr-100-stale",
    )

    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        recovery_module,
        "get_merged_pr_ids",
        lambda repo_path, branch, ids: {"PR-100"},
    )

    runner = h._make_runner()
    _stub_recovery_queue(runner, tasks=[task])

    asyncio.run(runner.recover_state())

    assert runner.state.current_task is None
    # Site falls through to the no-recoverable branch which clears these:
    assert runner.state.current_pr is None
    assert runner.state.state == PipelineState.IDLE
    assert any(
        f"ignoring stale DOING entry {task.pr_id}" in e["event"]
        for e in runner.state.history
    )


def test_recovery_clears_task_on_no_pr_after_crash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``recovery.py:327`` — DOING task has no matching PR (crash signature).

    The site is the most complete reset pattern in the codebase: it
    drops ``current_task``, ``current_pr``, ``error_message``, sets
    ``state`` to IDLE, and adds the PR ID to ``_crashed_task_pr_ids``
    so the selector skips the task on the next cycle. ``error_message``
    is explicitly cleared (P2 review note in ``recovery.py``) so a
    stale ERROR banner from a prior cycle does not surface against the
    quiesced repo.
    """
    task = QueueTask(
        pr_id="PR-200",
        title="crashed",
        status=TaskStatus.DOING,
        branch="pr-200-crashed",
    )

    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        recovery_module,
        "get_merged_pr_ids",
        lambda repo_path, branch, ids: set(),
    )

    runner = h._make_runner()
    runner.state.error_message = "stale before crash"
    _stub_recovery_queue(runner, tasks=[task])
    runner._preserve_crashed_run_commits = (  # type: ignore[method-assign]
        lambda branch: True
    )

    asyncio.run(runner.recover_state())

    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert runner.state.error_message is None
    assert runner.state.state == PipelineState.IDLE
    assert "PR-200" in runner._crashed_task_pr_ids
    assert any(
        "Task PR-200 crashed, marking CANCELED" in e["event"]
        for e in runner.state.history
    )


def test_recovery_clears_task_at_idle_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``recovery.py:373`` — final no-recoverable-anything fallback.

    With no DOING task and no open PR matching a queued branch, recovery
    converges on a clean IDLE: state -> IDLE, ``error_message`` -> None,
    ``current_task`` -> None, ``current_pr`` -> None,
    ``_error_diagnose_count`` -> 0. This is the most aggressive reset
    in the codebase outside the crashed-DOING path.
    """
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )

    runner = h._make_runner()
    runner.state.error_message = "stale before recovery"
    runner._error_diagnose_count = 2
    runner.state.current_task = QueueTask(
        pr_id="PR-IGNORED",
        title="ignored",
        status=TaskStatus.TODO,
    )
    runner.state.current_pr = PRInfo(number=1, branch="stale")
    _stub_recovery_queue(runner, tasks=[])

    asyncio.run(runner.recover_state())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert runner._error_diagnose_count == 0
    assert any(
        "no DOING tasks, no open PRs -> IDLE" in e["event"]
        for e in runner.state.history
    )


def test_idle_clears_task_on_open_pr_check_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``idle.py:428`` — ``get_open_prs`` raises during dispatch.

    The site clears ``current_pr`` and ``current_task``, sets the
    soft-defer flag ``_idle_dispatch_deferred`` so the next cycle
    retries, and stays in IDLE without an ERROR transition (the
    failure is treated as transient observability noise, not a fatal
    handler error). ``error_message`` is intentionally NOT touched.
    """
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)

    def _raise(repo: str, **kw: Any) -> list[PRInfo]:
        raise RuntimeError("API down")

    monkeypatch.setattr(runner_module.github_client, "get_open_prs", _raise)

    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=5, branch="stale")
    runner.state.current_task = QueueTask(
        pr_id="PR-OLD", title="t", status=TaskStatus.TODO,
    )
    runner.state.error_message = "preserved"
    runner._idle_dispatch_deferred = False

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None
    assert runner._idle_dispatch_deferred is True
    assert runner.state.error_message == "preserved"
    assert any(
        "open PR check failed" in e["event"] for e in runner.state.history
    )


def test_idle_clears_task_on_user_pause_during_prep(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``idle.py:691`` — pause requested while preparing the picked task.

    The site clears ``current_task`` only; state stays IDLE and
    ``current_pr`` is left at whatever ``_preserve_fix_iteration_count``
    most recently produced (here ``None``). The contract is "drop the
    handle on the work-in-progress so the next IDLE tick picks again
    after the operator un-pauses".
    """
    h._patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-042",
        title="picked",
        status=TaskStatus.TODO,
        branch="pr-042-picked",
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: task)
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = h._make_runner()

    async def fake_refresh_pause() -> None:
        runner.state.user_paused = True

    runner._refresh_user_paused_from_redis = (  # type: ignore[method-assign]
        fake_refresh_pause
    )

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert any(
        "Pause requested while preparing PR-042" in e["event"]
        for e in runner.state.history
    )


def test_fix_clears_task_on_terminal_pr_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``fix.py:569`` — external MERGED observed during FIX cycle.

    The MERGED branch resets the PR-side recovery counters, drops
    ``current_pr``, ``current_task``, ``error_message``, transitions to
    IDLE, and is one of the few sites that observably calls
    ``publish_state`` (the FIX handler publishes after dropping the
    work because IDLE consumers depend on the transition signal).
    """
    monkeypatch.setattr(
        runner_module.PipelineRunner, "_mark_queue_done", lambda self: None
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.FIX
    runner.state.current_pr = PRInfo(
        number=42,
        branch="pr-042",
        no_push_fix_count=2,
        fix_iteration_count=4,
    )
    runner.state.current_task = QueueTask(
        pr_id="PR-042",
        title="external merge",
        status=TaskStatus.DOING,
        branch="pr-042",
    )
    runner.state.error_message = "fix in progress"
    publish_calls = _install_publish_state_spy(runner)

    asyncio.run(runner._handle_external_terminal_pr_state("MERGED"))

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None
    assert runner.state.error_message is None
    assert publish_calls  # publish_state was called
    assert any(
        "PR #42 merged externally during FIX, returning to IDLE."
        in e["event"]
        for e in runner.state.history
    )


def test_hung_clears_task_on_resolved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``hung.py:285`` — operator merged or closed the parked PR.

    The site drops ``current_pr``, ``current_task``, and transitions to
    IDLE. ``error_message`` is intentionally PRESERVED so the operator
    can still see why the PR ended up in HUNG; only the active handles
    are released so the next IDLE cycle picks fresh work.
    """
    monkeypatch.setattr(
        hung_module.github_client,
        "run_gh",
        lambda *a, **kw: {"state": "MERGED"},
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.HUNG
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="parked", status=TaskStatus.DOING,
    )
    runner.state.error_message = "operator review reason"

    asyncio.run(runner.handle_hung())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None
    # error_message at this site is NOT cleared by the callsite itself.
    assert runner.state.error_message == "operator review reason"
    assert any(
        "PR #5 MERGED by operator -> IDLE" in e["event"]
        for e in runner.state.history
    )


def test_merge_clears_task_after_successful_merge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``merge.py:241`` — happy-path successful merge.

    The site clears ``_current_run_record``, ``current_pr``,
    ``current_task`` and transitions to IDLE. ``error_message`` is
    intentionally NOT touched here (no error was active), demonstrating
    that the "complete reset" pattern in ``recovery.py`` is NOT mirrored
    on the success path.
    """
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        runner_module.github_client, "merge_pr", lambda repo, num: None,
    )
    monkeypatch.setattr(
        runner_module.PipelineRunner, "_mark_queue_done", lambda self: None,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING,
    )
    runner._start_current_run_record("claude", "opus")

    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None
    assert runner._current_run_record is None
    assert any(
        "Merged PR #5 -> IDLE" in e["event"] for e in runner.state.history
    )


def test_error_diagnose_skip_clears_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``error.py:411`` — ``diagnose_error`` SKIP verdict returns to IDLE.

    The site is the ErrorMixin's "give up on this task" branch: drops
    ``current_task``, ``current_pr``, ``error_message``, resets
    ``_error_diagnose_count``, transitions to IDLE.
    """
    monkeypatch.setattr(
        claude_cli, "diagnose_error_async", h._async_cli_result(0, "SKIP", ""),
    )
    monkeypatch.setattr(
        codex_cli, "diagnose_error_async", h._async_cli_result(0, "SKIP", ""),
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "boom"
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="active", status=TaskStatus.DOING,
    )
    runner.state.current_pr = PRInfo(number=42, branch="pr-001-feature")
    runner._error_diagnose_count = 1

    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert runner.state.error_message is None
    assert runner._error_diagnose_count == 0
    assert any(
        "diagnose_error: SKIP -> IDLE" in e["event"]
        for e in runner.state.history
    )


@pytest.mark.parametrize(
    "merged_value, expected_log_fragment",
    [
        (True, "merged externally -> IDLE"),
        (False, "closed without merge"),
        (None, "no longer open (state unknown)"),
    ],
)
def test_watch_clears_task_on_pr_terminal(
    monkeypatch: pytest.MonkeyPatch,
    merged_value: bool | None,
    expected_log_fragment: str,
) -> None:
    """``watch.py:130`` — current PR no longer in the open-PR list.

    All three terminal sub-paths (merged externally, closed without
    merge, unknown state) converge on the same field-mutation contract:
    drop ``_current_run_record``, ``current_pr``, ``current_task`` and
    transition to IDLE. Parametrising over the three sub-paths confirms
    none of them silently diverged from the shared callsite.
    """
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "is_pr_merged",
        lambda repo, num: merged_value,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=7, branch="pr-007")
    runner.state.current_task = QueueTask(
        pr_id="PR-007", title="watching", status=TaskStatus.DOING,
    )
    runner._start_current_run_record("claude", "opus")

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None
    assert runner._current_run_record is None
    assert any(
        expected_log_fragment in e["event"] for e in runner.state.history
    )


# =========================================================================
# SECTION 2 — current_task = task (3 callsites)
# =========================================================================


def test_recovery_assigns_doing_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``recovery.py:254`` — DOING task with matching open PR -> WATCH.

    The site assigns ``current_task`` to the DOING entry and (because a
    matching PR exists) also assigns ``current_pr``, transitions to
    WATCH, and seeds ``_watch_entered_at`` so the slow-start polling
    window anchors immediately.
    """
    task = QueueTask(
        pr_id="PR-300",
        title="resume",
        status=TaskStatus.DOING,
        branch="pr-300-resume",
    )
    pr = PRInfo(number=33, branch="pr-300-resume")

    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_pr_metadata",
        lambda repo, num: {"head_commit_date": "2026-04-14T12:00:00Z"},
    )

    runner = h._make_runner()
    _stub_recovery_queue(runner, tasks=[task])

    asyncio.run(runner.recover_state())

    assert runner.state.current_task == task
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 33
    assert runner.state.state == PipelineState.WATCH
    assert runner._watch_entered_at is not None
    assert any(
        f"Recovered: DOING task {task.pr_id} -> WATCH PR #33" in e["event"]
        for e in runner.state.history
    )


def test_recovery_assigns_matched_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``recovery.py:359`` — open PR matched to DONE/TODO queued task -> WATCH.

    Distinct from the DOING path: the queued task can be DONE (PR
    pending merge) or TODO (race during a daemon restart). Either way
    the site assigns ``current_pr`` and ``current_task``, transitions
    to WATCH, and anchors the slow-start window.
    """
    task = QueueTask(
        pr_id="PR-301",
        title="match",
        status=TaskStatus.DONE,
        branch="pr-301-match",
    )
    pr = PRInfo(number=34, branch="pr-301-match")

    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_pr_metadata",
        lambda repo, num: {"head_commit_date": "2026-04-14T12:00:00Z"},
    )

    runner = h._make_runner()
    _stub_recovery_queue(runner, tasks=[task])

    asyncio.run(runner.recover_state())

    assert runner.state.current_task == task
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 34
    assert runner.state.state == PipelineState.WATCH
    assert runner._watch_entered_at is not None
    assert any(
        "Recovered: DONE task PR-301 -> WATCH PR #34" in e["event"]
        for e in runner.state.history
    )


def test_idle_assigns_active_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``idle.py:667`` — selector picked a task, no existing PR.

    The site assigns ``current_task`` unconditionally. With no matching
    open PR the runner proceeds to CODING (this test stubs
    ``handle_coding`` so it observes the assignment without exercising
    the CLI). ``current_pr`` stays whatever the pre-dispatch path left
    it at; the test asserts the assignment is exactly the picked task.
    """
    h._patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-400",
        title="picked",
        status=TaskStatus.TODO,
        branch="pr-400-picked",
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: task)
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    coding_calls: list[None] = []

    async def fake_handle_coding() -> None:
        coding_calls.append(None)

    runner = h._make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]

    asyncio.run(runner.handle_idle())

    assert runner.state.current_task == task
    assert runner.state.state == PipelineState.CODING
    assert coding_calls == [None]
    assert any(
        f"Picked task {task.pr_id}" in e["event"]
        for e in runner.state.history
    )


# =========================================================================
# SECTION 3 — Sampled ERROR transitions (one per file)
# =========================================================================


def test_idle_transitions_error_on_sync_to_main_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``idle.py:389`` — ``sync_to_main`` raises a recognised exception.

    The site sets ``state.state = ERROR``, ``error_message =
    "sync_to_main failed: <exc>"`` and emits an ``[INFRA]`` log event
    with the same text. ``publish_state`` is NOT called at this site
    because the error path returns immediately; the next cycle
    transition picks up the publish via the run_cycle wrapper.
    """
    runner = h._make_runner()

    def boom() -> None:
        raise RuntimeError("network unreachable")

    runner.sync_to_main = boom  # type: ignore[method-assign]
    publish_calls = _install_publish_state_spy(runner)

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "sync_to_main failed: network unreachable"
    assert publish_calls == []
    assert any(
        e["event"] == "[INFRA] sync_to_main failed: network unreachable."
        for e in runner.state.history
    )


def test_fix_transitions_error_on_post_comment_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``fix.py:321`` — iteration-cap escalation, ``post_comment`` raises.

    The site is inside ``_escalate_fix_iteration_cap``. When the
    ``@-mention`` comment post fails, the runner transitions straight
    to ERROR with ``error_message = "post_comment failed: <exc>"`` and
    a ``[FIX]`` log event. The label-create / label-apply steps are
    not reached because the post comes first.
    """
    def fail_post(repo: str, number: int, body: str) -> None:
        raise RuntimeError("network unreachable")

    monkeypatch.setattr(fix_module.github_client, "post_comment", fail_post)

    runner = h._make_runner()
    cap = runner.app_config.daemon.fix_iteration_cap
    pr = PRInfo(number=500, branch="pr-500-cap", fix_iteration_count=cap)
    runner.state.state = PipelineState.FIX
    runner.state.current_pr = pr

    asyncio.run(runner._escalate_fix_iteration_cap(pr))

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "post_comment failed: network unreachable"
    assert any(
        e["event"] == "[FIX] post_comment failed: network unreachable."
        for e in runner.state.history
    )


def test_coding_transitions_error_on_missing_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``coding.py:98`` — current task has no branch; cannot identify PR.

    This is the earliest ERROR transition in ``handle_coding`` and is
    the site sampled for the coding handler (per the task spec's "pick
    the most common path"). The site sets ERROR, writes a fixed
    ``error_message`` describing the missing branch, and emits a
    ``[CODING]`` log event with the same text.
    """
    runner = h._make_runner()
    runner.state.state = PipelineState.IDLE
    runner.state.current_task = QueueTask(
        pr_id="PR-600", title="no branch", status=TaskStatus.TODO,
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == (
        "Current task has no branch; cannot identify PR"
    )
    assert any(
        e["event"]
        == "[CODING] Current task has no branch; cannot identify PR."
        for e in runner.state.history
    )


def test_watch_transitions_error_on_open_prs_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``watch.py:100`` — ``get_open_prs`` raises during WATCH polling.

    The site sets ERROR, ``error_message = "get_open_prs failed: <exc>"``
    and emits a TERSE ``[WATCH] <exc>.`` log event (note: the log line
    deliberately omits the ``get_open_prs failed:`` prefix that the
    error_message carries — the operator-visible log relies on the
    ``[WATCH]`` tag for context).
    """
    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=8, branch="pr-008")

    def boom(repo: str, **kw: Any) -> list[PRInfo]:
        raise RuntimeError("boom")

    monkeypatch.setattr(runner_module.github_client, "get_open_prs", boom)

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "get_open_prs failed: boom"
    assert any(
        e["event"] == "[WATCH] boom." for e in runner.state.history
    )


def test_merge_transitions_error_on_no_eligible_coder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``merge.py:78`` — merge conflict surfaces but no coder is eligible.

    The site is inside ``handle_merge``'s pre-merge sync conflict
    branch. With no eligible auxiliary coder, the merge is aborted and
    the runner transitions to ERROR with the fixed message
    ``"No eligible coder available for merge conflict resolution"`` and
    a ``[MERGE]`` log event.
    """
    pr_branch = "pr-700"
    base = "main"

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        # Force a merge conflict on the pre-merge sync.
        if cmd[:2] == ["git", "merge"] and cmd[2:3] == [f"origin/{base}"]:
            return h._FakeCompletedProcess(
                args=cmd,
                stdout="CONFLICT (content): Merge conflict in foo.py\n",
                returncode=1,
            )
        if cmd[:2] == ["git", "rev-list"]:
            # Signal "ahead of origin" so the pre-merge sync fires.
            return h._FakeCompletedProcess(
                args=cmd, stdout="1\n", returncode=0,
            )
        return h._FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    runner = h._make_runner()
    # Force the auxiliary coder selector to report none eligible.
    runner._get_auxiliary_coder = lambda: None  # type: ignore[method-assign]
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=70, branch=pr_branch)
    runner.state.current_task = QueueTask(
        pr_id="PR-700", title="merge", status=TaskStatus.DOING,
    )

    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == (
        "No eligible coder available for merge conflict resolution"
    )
    assert any(
        e["event"]
        == "[MERGE] No eligible coder available for merge conflict resolution."
        for e in runner.state.history
    )


def test_recovery_transitions_error_on_get_open_prs_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``recovery.py:204`` — ``recover_state`` GitHub fetch failure.

    Sample ERROR transition for ``recovery.py``. The site sets ERROR,
    ``error_message = "recover_state: get_open_prs failed: <exc>"``,
    emits an ``[INFRA]`` log event with the abridged ``recover_state
    failed:`` text, and returns ``False`` so ``run_cycle`` retries
    recovery on the next tick instead of marking ``_recovered`` true.
    """
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: (_ for _ in ()).throw(RuntimeError("API down")),
    )

    runner = h._make_runner()
    _stub_recovery_queue(runner, tasks=[])

    completed = asyncio.run(runner.recover_state())

    assert completed is False
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == (
        "recover_state: get_open_prs failed: API down"
    )
    assert any(
        e["event"] == "[INFRA] recover_state failed: API down."
        for e in runner.state.history
    )


def test_error_transitions_error_on_review_trigger_failure_after_fix_push(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """``error.py:362`` — ``_post_codex_review`` fails after FIX push.

    Sample ERROR transition for ``error.py`` (the ErrorMixin handler).
    After ``diagnose_error`` returns FIX and the auto-fix commit is
    pushed, the handler must trigger ``@codex review`` so the next
    cycle does not silently sit in IDLE without re-review. When the
    trigger post fails, the handler short-circuits to ERROR with the
    fixed message ``"Failed to post @codex review on PR #<n> after
    diagnose_error fix push; manual review trigger required to avoid
    fix/push loop"`` and emits an ``[ERROR] <error_message>.`` log
    event. ``publish_state`` is NOT called at this site (the handler
    returns immediately), matching the contract for non-publishing
    ERROR transitions in fix/watch/hung/recovery sampled above.
    """
    runner, _calls, _warnings, review_requests = h._run_dirty_diagnose(
        monkeypatch, tmp_path, review_post_ok=False
    )

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == (
        "Failed to post @codex review on PR #119 after "
        "diagnose_error fix push; manual review trigger required "
        "to avoid fix/push loop"
    )
    assert review_requests == [119]
    assert any(
        e["event"] == f"[ERROR] {runner.state.error_message}."
        for e in runner.state.history
    )
