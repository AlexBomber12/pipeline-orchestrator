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

# PR-224a: imports needed by tests moved from tests/test_runner.py
import random  # noqa: F401
import re  # noqa: F401
import time  # noqa: F401
import types  # noqa: F401
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from src import codex_cli
from src.coders import claude as claude_plugin_module
from src.config import AppConfig, CoderType, DaemonConfig
from src.daemon import git_ops as git_ops_module
from src.daemon import recovery as recovery_module  # noqa: F401  (sanity)
from src.daemon import runner as runner_module
from src.daemon.handlers import coding as coding_module  # noqa: F401  # noqa: F401,F811
from src.daemon.handlers import error as error_module  # noqa: F401  # noqa: F401,F811
from src.daemon.handlers import idle as idle_module  # noqa: F811
from src.daemon.handlers import merge as merge_module  # noqa: F401  # noqa: F401,F811
from src.daemon.handlers import watch as watch_module  # noqa: F401  # noqa: F401,F811
from src.daemon.runner import PipelineRunner
from src.keyspace import pipeline_state
from src.models import (
    CIStatus,
    PipelineState,  # noqa: F811
    PRInfo,  # noqa: F811
    QueueTask,  # noqa: F811
    RepoState,
    ReviewStatus,
    TaskStatus,  # noqa: F811
)

from tests.runner import _helpers as h

claude_cli = claude_plugin_module.claude_cli


@pytest.fixture(autouse=True)
def _default_no_merged_branch_api(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.task_status.gh_pr_get_merged_branches",
        lambda repo, branches: set(),
    )


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
    runner._parse_tasks_from_headers = (  # type: ignore[method-assign]
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
        "src.github.prs.get_open_prs",
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
    assert any(f"ignoring stale DOING entry {task.pr_id}" in e["event"] for e in runner.state.history)


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
        "src.github.prs.get_open_prs",
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
    assert any("Task PR-200 crashed, marking CANCELED" in e["event"] for e in runner.state.history)


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
        "src.github.prs.get_open_prs",
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
    assert any("no DOING tasks, no open PRs -> IDLE" in e["event"] for e in runner.state.history)


def test_idle_clears_task_on_open_pr_check_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``idle.py:428`` — ``get_open_prs`` raises during dispatch.

    The site clears ``current_pr``, ``current_task`` and (post-PR-218)
    ``error_message``, sets the soft-defer flag
    ``_idle_dispatch_deferred`` so the next cycle retries, and stays in
    IDLE without an ERROR transition (the failure is treated as
    transient observability noise, not a fatal handler error).

    PR-218 unified the task-clear contract: every callsite that drops
    ``current_task`` now also releases ``current_pr`` and
    ``error_message`` via ``RepoState.__setattr__``. The previous
    "preserve error_message at this site" carve-out is gone, matching
    the recovery.py:371-375 superset that the rest of the codebase
    already followed.
    """
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)

    def _raise(repo: str, **kw: Any) -> list[PRInfo]:
        raise RuntimeError("API down")

    monkeypatch.setattr("src.github.prs.get_open_prs", _raise)

    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=5, branch="stale")
    runner.state.current_task = QueueTask(
        pr_id="PR-OLD",
        title="t",
        status=TaskStatus.TODO,
    )
    runner.state.error_message = "stale before clear"
    runner._idle_dispatch_deferred = False

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None
    assert runner._idle_dispatch_deferred is True
    assert runner.state.error_message is None
    assert any("open PR check failed" in e["event"] for e in runner.state.history)


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
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
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
    assert any("Pause requested while preparing PR-042" in e["event"] for e in runner.state.history)


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
    monkeypatch.setattr(runner_module.PipelineRunner, "_mark_queue_done", lambda self: None)

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
    assert any("PR #42 merged externally during FIX, returning to IDLE." in e["event"] for e in runner.state.history)


def test_hung_clears_task_on_resolved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``hung.py:285`` — operator merged or closed the parked PR.

    The site drops ``current_pr``, ``current_task``, ``error_message``
    and transitions to IDLE. PR-218 unified the task-clear contract:
    once ``current_task`` flips to ``None`` the operator-relevant
    ``error_message`` is released alongside the PR handle so the next
    IDLE cycle starts from the canonical clean shape. Operators who
    need to retain the parked-PR reason consult the event log; the live
    ``error_message`` field is reserved for an active failure.
    """
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *a, **kw: {"state": "MERGED"},
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.HUNG
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="parked",
        status=TaskStatus.DOING,
    )
    runner.state.error_message = "operator review reason"

    asyncio.run(runner.handle_hung())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None
    assert runner.state.error_message is None
    assert any("PR #5 MERGED by operator -> IDLE" in e["event"] for e in runner.state.history)


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
        "src.github.prs.merge_pr",
        lambda repo, num: None,
    )
    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_mark_queue_done",
        lambda self: None,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
    )
    runner._start_current_run_record("claude", "opus")

    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None
    assert runner._current_run_record is None
    assert any("Merged PR #5 -> IDLE" in e["event"] for e in runner.state.history)


def test_error_diagnose_skip_clears_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``error.py:411`` — ``diagnose_error`` SKIP verdict returns to IDLE.

    The site is the ErrorMixin's "give up on this task" branch: drops
    ``current_task``, ``current_pr``, ``error_message``, resets
    ``_error_diagnose_count``, transitions to IDLE.
    """
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        h._async_cli_result(0, "SKIP", ""),
    )
    monkeypatch.setattr(
        codex_cli,
        "diagnose_error_async",
        h._async_cli_result(0, "SKIP", ""),
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "boom"
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="active",
        status=TaskStatus.DOING,
    )
    runner.state.current_pr = PRInfo(number=42, branch="pr-001-feature")
    runner._error_diagnose_count = 1

    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert runner.state.error_message is None
    assert runner._error_diagnose_count == 0
    assert any("diagnose_error: SKIP -> IDLE" in e["event"] for e in runner.state.history)


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
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.is_pr_merged",
        lambda repo, num: merged_value,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=7, branch="pr-007")
    runner.state.current_task = QueueTask(
        pr_id="PR-007",
        title="watching",
        status=TaskStatus.DOING,
    )
    runner._start_current_run_record("claude", "opus")

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None
    assert runner._current_run_record is None
    assert any(expected_log_fragment in e["event"] for e in runner.state.history)


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
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_metadata",
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
    assert any(f"Recovered: DOING task {task.pr_id} -> WATCH PR #33" in e["event"] for e in runner.state.history)


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
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_metadata",
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
    assert any("Recovered: DONE task PR-301 -> WATCH PR #34" in e["event"] for e in runner.state.history)


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
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
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
    assert any(f"Picked task {task.pr_id}" in e["event"] for e in runner.state.history)


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
    assert any(e["event"] == "[INFRA] sync_to_main failed: network unreachable." for e in runner.state.history)


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

    monkeypatch.setattr("src.github.comments.post_comment", fail_post)

    runner = h._make_runner()
    cap = runner.app_config.daemon.fix_iteration_cap
    pr = PRInfo(number=500, branch="pr-500-cap", fix_iteration_count=cap)
    runner.state.state = PipelineState.FIX
    runner.state.current_pr = pr

    asyncio.run(runner._escalate_fix_iteration_cap(pr))

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "post_comment failed: network unreachable"
    assert any(e["event"] == "[FIX] post_comment failed: network unreachable." for e in runner.state.history)


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
        pr_id="PR-600",
        title="no branch",
        status=TaskStatus.TODO,
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == ("Current task has no branch; cannot identify PR")
    assert any(e["event"] == "[CODING] Current task has no branch; cannot identify PR." for e in runner.state.history)


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

    monkeypatch.setattr("src.github.prs.get_open_prs", boom)

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "get_open_prs failed: boom"
    assert any(e["event"] == "[WATCH] boom." for e in runner.state.history)


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
                args=cmd,
                stdout="1\n",
                returncode=0,
            )
        return h._FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    runner = h._make_runner()
    # Force the auxiliary coder selector to report none eligible.
    runner._get_auxiliary_coder = lambda: None  # type: ignore[method-assign]
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=70, branch=pr_branch)
    runner.state.current_task = QueueTask(
        pr_id="PR-700",
        title="merge",
        status=TaskStatus.DOING,
    )

    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == ("No eligible coder available for merge conflict resolution")
    assert any(
        e["event"] == "[MERGE] No eligible coder available for merge conflict resolution." for e in runner.state.history
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
        "src.github.prs.get_open_prs",
        lambda repo, **kw: (_ for _ in ()).throw(RuntimeError("API down")),
    )

    runner = h._make_runner()
    _stub_recovery_queue(runner, tasks=[])

    completed = asyncio.run(runner.recover_state())

    assert completed is False
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == ("recover_state: get_open_prs failed: API down")
    assert any(e["event"] == "[INFRA] recover_state failed: API down." for e in runner.state.history)


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
    runner, _calls, _warnings, review_requests = h._run_dirty_diagnose(monkeypatch, tmp_path, review_post_ok=False)

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == (
        "Failed to post @codex review on PR #119 after "
        "diagnose_error fix push; manual review trigger required "
        "to avoid fix/push loop"
    )
    assert review_requests == [119]
    assert any(e["event"] == f"[ERROR] {runner.state.error_message}." for e in runner.state.history)


# ---------------------------------------------------------------------------
# PR-224a moved from tests/test_runner.py
# ---------------------------------------------------------------------------


def test_handle_coding_no_pr_routes_to_diagnostic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_auto_pr_async",
        h._async_cli_result(0, "ok", ""),
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )

    def fake_git(repo_path: str, *args: str, **kwargs: Any):
        return h._FakeCompletedProcess(args=list(args), returncode=1)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)

    async def instant_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(runner_module.asyncio, "sleep", instant_sleep)

    runner = h._make_runner()
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001")
    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.HUNG
    assert "did nothing" in (runner.state.error_message or "")


def test_handle_coding_creates_run_record(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_auto_pr_async",
        h._async_cli_result(0, "ok", ""),
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [PRInfo(number=42, branch="pr-001")],
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )

    task_file = tmp_path / "tasks" / "PR-001.md"
    task_file.parent.mkdir(parents=True)
    task_file.write_text(
        "# PR-001: Sample\n\nBranch: pr-001\n- Type: feature\n- Complexity: low\n",
        encoding="utf-8",
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )

    asyncio.run(runner.handle_coding())

    record = runner._current_run_record
    assert record is not None
    assert record.task_id == "PR-001"
    assert record.profile_id == "claude:opus:container"
    assert record.task_type == "feature"
    assert record.complexity == "low"


def test_handle_coding_normalizes_task_type_synonym(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_auto_pr_async",
        h._async_cli_result(0, "ok", ""),
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [PRInfo(number=42, branch="pr-001")],
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )

    task_file = tmp_path / "tasks" / "PR-001.md"
    task_file.parent.mkdir(parents=True)
    task_file.write_text(
        "# PR-001: Sample\n\nBranch: pr-001\n- Type: bug\n- Complexity: low\n",
        encoding="utf-8",
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )

    asyncio.run(runner.handle_coding())

    record = runner._current_run_record
    assert record is not None
    assert record.task_type == "bugfix"


def test_handle_coding_saves_record_on_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_auto_pr_async",
        h._async_cli_result(0, "ok", ""),
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [PRInfo(number=42, branch="pr-001")],
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )

    runner = h._make_runner()
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001")

    asyncio.run(runner.handle_coding())

    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=runner.name,
        )
    )

    assert len(recent) == 1
    assert recent[0].run_id == runner._current_run_record.run_id
    assert recent[0].exit_reason == "coding_complete"
    assert recent[0].ended_at is not None
    assert recent[0].duration_ms is not None


def test_handle_coding_saves_record_on_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_auto_pr_async",
        h._async_cli_result(1, "", "build failed"),
    )

    runner = h._make_runner()
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001")

    asyncio.run(runner.handle_coding())

    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=runner.name,
        )
    )

    assert runner.state.state == PipelineState.ERROR
    assert len(recent) == 1
    assert recent[0].exit_reason == "error"
    assert recent[0].ended_at is not None
    assert recent[0].duration_ms is not None


def test_handle_coding_rejects_unmatched_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When no open PR matches current_task.branch, fail fast instead of
    attaching to an unrelated newest open PR. The diagnostic handler then
    routes the no-PR outcome to HUNG via case A/B/C."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_auto_pr_async",
        h._async_cli_result(0, "ok", ""),
    )
    unrelated = PRInfo(number=99, branch="other-branch")
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [unrelated],
    )

    def fake_git(repo_path: str, *args: str, **kwargs: Any):
        return h._FakeCompletedProcess(args=list(args), returncode=1)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)

    async def instant_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(runner_module.asyncio, "sleep", instant_sleep)

    runner = h._make_runner()
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001")
    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.HUNG
    assert runner.state.current_pr is None
    assert "did nothing" in (runner.state.error_message or "")


def test_handle_coding_posts_codex_review_after_pr_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-019: ``handle_coding`` must explicitly post ``@codex review`` on the
    newly-opened PR so Codex kicks off a review for every iteration instead
    of relying on GitHub-side Automatic Reviews (which we want configured
    for PR creation only, to avoid duplicate reviews)."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_auto_pr_async",
        h._async_cli_result(0, "ok", ""),
    )
    opened_pr = PRInfo(number=42, branch="pr-019")
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [opened_pr],
    )
    posted: list[tuple[str, int, str]] = []

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    monkeypatch.setattr("src.github.comments.post_comment", fake_post)

    runner = h._make_runner()
    runner.state.current_task = QueueTask(pr_id="PR-019", title="t", status=TaskStatus.DOING, branch="pr-019")
    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 42
    assert posted == [(runner.owner_repo, 42, "@codex review")]
    assert any("Posted @codex review on PR #42" in e["event"] for e in runner.state.history)


def test_handle_coding_survives_post_comment_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``post_comment`` failure after PR creation must be non-fatal:
    the runner stays in ``WATCH`` and logs a warning. Codex may still
    auto-trigger on push, and a transient ``gh`` hiccup must not flip an
    otherwise healthy pipeline to ``ERROR``."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_auto_pr_async",
        h._async_cli_result(0, "ok", ""),
    )
    opened_pr = PRInfo(number=42, branch="pr-019")
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [opened_pr],
    )

    def boom(repo: str, number: int, body: str) -> None:
        raise RuntimeError("gh transient failure")

    monkeypatch.setattr("src.github.comments.post_comment", boom)

    runner = h._make_runner()
    runner.state.current_task = QueueTask(pr_id="PR-019", title="t", status=TaskStatus.DOING, branch="pr-019")
    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.error_message is None
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 42
    assert any(
        "Warning: failed to post @codex review" in e["event"] and "gh transient failure" in e["event"]
        for e in runner.state.history
    )


def test_handle_coding_stop_request_terminates_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    stop_called = {"terminate": 0, "kill": 0, "wait": 0}

    class _FakeProc:
        def __init__(self) -> None:
            self.returncode: int | None = None
            self._done = asyncio.Event()

        def terminate(self) -> None:
            stop_called["terminate"] += 1
            self.returncode = -15
            self._done.set()

        def kill(self) -> None:
            stop_called["kill"] += 1
            self.returncode = -9
            self._done.set()

        async def wait(self) -> int:
            stop_called["wait"] += 1
            await self._done.wait()
            return self.returncode or 0

    async def fake_run_auto_pr_async(*args: object, **kwargs: object) -> tuple[int, str, str]:
        proc = _FakeProc()
        on_process_start = kwargs["on_process_start"]
        assert callable(on_process_start)
        on_process_start(proc)
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            raise
        return (0, "ok", "")

    monkeypatch.setattr(
        claude_cli,
        "run_auto_pr_async",
        fake_run_auto_pr_async,
    )

    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-127",
        title="Pause controls",
        status=TaskStatus.DOING,
        branch="pr-127-control-endpoints-backend",
    )
    runner.redis.store[f"control:{runner.name}:stop"] = "1"

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.user_paused is True
    assert runner.state.error_message is None
    assert stop_called["terminate"] == 1
    assert stop_called["kill"] == 0
    assert stop_called["wait"] >= 1
    assert any("user stop requested" in entry["event"].lower() for entry in runner.state.history)


def test_handle_coding_honors_persisted_stop_after_fast_cli_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_auto_pr_async",
        h._async_cli_result(1, "", "coder failed fast"),
    )

    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-127",
        title="Pause controls",
        status=TaskStatus.DOING,
        branch="pr-127-control-endpoints-backend",
    )
    runner.redis.store[f"control:{runner.name}:stop"] = "1"

    async def stale_stop_monitor(
        _cli_task: asyncio.Task[tuple[int, str, str]],
    ) -> None:
        return None

    monkeypatch.setattr(runner, "_monitor_stop_request", stale_stop_monitor)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.user_paused is True
    assert runner.state.error_message is None
    assert f"control:{runner.name}:stop" not in runner.redis.store
    assert any("after coder exit" in entry["event"].lower() for entry in runner.state.history)


def test_handle_coding_honors_stop_requested_during_pr_retry_after_cli_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_auto_pr_async",
        h._async_cli_result(0, "ok", ""),
    )

    attempts = {"count": 0}

    def fake_get_open_prs(repo: str, **kw: Any) -> list[PRInfo]:
        attempts["count"] += 1
        if attempts["count"] == 1:
            return []
        return [PRInfo(number=127, branch="pr-127-control-endpoints-backend")]

    async def stale_stop_monitor(
        _cli_task: asyncio.Task[tuple[int, str, str]],
    ) -> None:
        return None

    pop_calls = {"count": 0}

    async def fake_pop_stop_request() -> bool:
        pop_calls["count"] += 1
        return pop_calls["count"] == 4

    async def instant_sleep(_seconds: float) -> None:
        return None

    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-127",
        title="Pause controls",
        status=TaskStatus.DOING,
        branch="pr-127-control-endpoints-backend",
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", fake_get_open_prs)
    monkeypatch.setattr(runner, "_pop_stop_request", fake_pop_stop_request)
    monkeypatch.setattr(runner_module.asyncio, "sleep", instant_sleep)
    monkeypatch.setattr(runner, "_monitor_stop_request", stale_stop_monitor)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.user_paused is True
    assert runner.state.current_pr is None
    assert attempts["count"] == 1
    assert any("after coder exit" in entry["event"].lower() for entry in runner.state.history)


def test_handle_coding_errors_when_task_has_no_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_auto_pr_async",
        h._async_cli_result(0, "ok", ""),
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [PRInfo(number=1, branch="anything")],
    )

    runner = h._make_runner()
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)  # no branch
    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.current_pr is None
    assert "no branch" in (runner.state.error_message or "").lower()
    # Codex P1: the malformed-task error path must bail BEFORE
    # ``_commit_and_push_dirty`` runs, otherwise a dirty tree could be
    # committed + pushed to whatever branch HEAD happens to be on
    # before the runner realises it cannot identify the target PR.
    assert not any(cmd[:2] == ["git", "status"] for cmd in calls)
    assert not any(cmd[:1] == ["scripts/ci.sh"] for cmd in calls)
    assert not any(cmd[:2] == ["git", "add"] for cmd in calls)
    assert not any(cmd[:2] == ["git", "commit"] for cmd in calls)
    assert not any(cmd[:2] == ["git", "push"] for cmd in calls)


def test_handle_coding_retries_pr_detection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GitHub's open-PR list is eventually consistent: a PR opened by
    Claude may not appear on the first poll. ``handle_coding`` must retry
    up to 3 times before giving up."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_auto_pr_async",
        h._async_cli_result(0, "ok", ""),
    )

    opened_pr = PRInfo(number=42, branch="pr-001")
    call_count = {"n": 0}

    def flaky_get_open_prs(repo: str, **kw: Any) -> list[PRInfo]:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return []
        return [opened_pr]

    monkeypatch.setattr("src.github.prs.get_open_prs", flaky_get_open_prs)
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )

    slept: list[float] = []

    async def instant_sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr(runner_module.asyncio, "sleep", instant_sleep)

    runner = h._make_runner()
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001")
    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 42
    assert call_count["n"] == 2
    assert 5 in slept
    assert any("PR not found for 'pr-001'" in e["event"] and "1/3" in e["event"] for e in runner.state.history)


def test_handle_coding_runs_three_retries_before_diagnostic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After 3 consecutive empty get_open_prs results the runner must
    invoke ``_diagnose_exit_zero_no_pr`` rather than flipping straight
    to ERROR. That diagnostic distinguishes A/B/C and routes to HUNG
    when the coder did nothing observable upstream."""
    h._patch_subprocess(monkeypatch)
    call_count = {"n": 0}

    def always_empty(repo: str, **kw: Any) -> list[PRInfo]:
        call_count["n"] += 1
        return []

    monkeypatch.setattr("src.github.prs.get_open_prs", always_empty)

    def fake_git(repo_path: str, *args: str, **kwargs: Any):
        return h._FakeCompletedProcess(args=list(args), returncode=1)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)

    slept: list[float] = []

    async def instant_sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr(runner_module.asyncio, "sleep", instant_sleep)

    class _CodexPlugin:
        supports_breach_lifecycle = False

        async def run_planned_pr(self, path: str, **kwargs: object) -> tuple[int, str, str]:
            return (0, "ok", "")

        async def run_auto_pr(self, path: str, **kwargs: object) -> tuple[int, str, str]:
            return (0, "ok", "")

        def build_run_kwargs(self, *, daemon_config: Any, **_kw: object) -> dict[str, Any]:
            return {"model": daemon_config.codex_model}

    runner = h._make_runner()
    runner._get_coder = lambda allow_exploration=False: (  # type: ignore[method-assign]
        "codex",
        _CodexPlugin(),
    )
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001")
    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.HUNG
    assert "[codex]" in (runner.state.error_message or "")
    assert call_count["n"] == 3
    assert slept.count(5) == 2


def test_handle_hung_posts_codex_review_and_returns_to_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[tuple[str, int, str]] = []

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    monkeypatch.setattr("src.github.comments.post_comment", fake_post)
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *a, **kw: {"state": "OPEN"},
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.HUNG
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_hung())

    assert posted == [(runner.owner_repo, 5, "@codex review")]
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.last_activity is not None


def test_handle_hung_stays_hung_when_pr_is_escalated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Escalated PRs (e.g. parked here by the FIX no-push deadlock circuit
    breaker) must stay HUNG even when ``hung_fallback_codex_review`` is on;
    otherwise the fallback bounces back to WATCH and re-enters the loop
    that triggered escalation in the first place."""
    posted: list[tuple[str, int, str]] = []

    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *a, **kw: {"state": "OPEN"},
    )

    runner = h._make_runner()
    assert runner.app_config.daemon.hung_fallback_codex_review is True
    runner.state.state = PipelineState.HUNG
    runner.state.current_pr = PRInfo(number=217, branch="pr-217", is_escalated=True)
    asyncio.run(runner.handle_hung())

    assert posted == []
    assert runner.state.state == PipelineState.HUNG
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.is_escalated is True
    assert any("PR #217 escalated; staying HUNG" in entry["event"] for entry in runner.state.history)


def test_handle_hung_escalated_pr_resolved_externally_transitions_to_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An escalated PR that the operator manually merges/closes must
    transition out of HUNG. The early-return on ``is_escalated`` used to
    short-circuit the PR state check and trap the runner forever (Codex
    P1 on PR #222)."""
    posted: list[tuple[str, int, str]] = []

    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *a, **kw: {"state": "MERGED"},
    )

    runner = h._make_runner()
    assert runner.app_config.daemon.hung_fallback_codex_review is True
    runner.state.state = PipelineState.HUNG
    runner.state.current_pr = PRInfo(number=217, branch="pr-217", is_escalated=True)
    runner.state.current_task = QueueTask(pr_id="PR-217", title="t", status=TaskStatus.DOING)
    asyncio.run(runner.handle_hung())

    assert posted == []
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None


def test_handle_hung_sets_error_when_fallback_post_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(repo: str, number: int, body: str) -> None:
        raise RuntimeError("gh unavailable")

    monkeypatch.setattr("src.github.comments.post_comment", boom)
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *a, **kw: {"state": "OPEN"},
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.HUNG
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_hung())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "post_comment failed: gh unavailable"
    assert any(entry["event"] == "[ESCALATE] gh unavailable." for entry in runner.state.history)


def test_handle_hung_preserves_context_when_fallback_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When hung_fallback_codex_review=False and PR is still open, runner
    stays in HUNG with current_pr and current_task preserved."""
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *a, **kw: {"state": "OPEN"},
    )
    runner = PipelineRunner(
        h._repo_cfg(),
        AppConfig(
            repositories=[],
            daemon=DaemonConfig(hung_fallback_codex_review=False),
        ),
        h._FakeRedis(),
        *h._usage_providers(),
    )
    runner.state.state = PipelineState.HUNG
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)
    asyncio.run(runner.handle_hung())

    assert runner.state.state == PipelineState.HUNG
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 5
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-001"


def test_handle_hung_stays_hung_when_pr_state_lookup_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(*args: object, **kwargs: object) -> dict[str, str]:
        raise RuntimeError("gh view failed")

    monkeypatch.setattr("src.github.gh_runner.run_gh", boom)
    runner = PipelineRunner(
        h._repo_cfg(),
        AppConfig(
            repositories=[],
            daemon=DaemonConfig(hung_fallback_codex_review=False),
        ),
        h._FakeRedis(),
        *h._usage_providers(),
    )
    runner.state.state = PipelineState.HUNG
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)
    asyncio.run(runner.handle_hung())

    assert runner.state.state == PipelineState.HUNG
    assert runner.state.current_pr is not None
    assert runner.state.current_task is not None
    assert any(
        "hung: failed to check PR state: gh view failed; staying HUNG" in entry["event"]
        for entry in runner.state.history
    )


@pytest.mark.parametrize("pr_state", ["MERGED", "CLOSED"])
def test_handle_hung_transitions_to_idle_when_pr_resolved(
    monkeypatch: pytest.MonkeyPatch,
    pr_state: str,
) -> None:
    """When hung_fallback_codex_review=False and the operator has closed or
    merged the PR, the runner should transition back to IDLE."""
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *a, **kw: {"state": pr_state},
    )
    runner = PipelineRunner(
        h._repo_cfg(),
        AppConfig(
            repositories=[],
            daemon=DaemonConfig(hung_fallback_codex_review=False),
        ),
        h._FakeRedis(),
        *h._usage_providers(),
    )
    runner.state.state = PipelineState.HUNG
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)
    asyncio.run(runner.handle_hung())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None


def test_handle_hung_consumes_operator_recovery_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-247: a one-shot ``control:{name}:recover`` flag transitions the
    runner from HUNG back to IDLE without nudging the reviewer or checking
    the upstream PR state."""
    gh_calls: list[object] = []
    posted: list[tuple[str, int, str]] = []
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *a, **kw: gh_calls.append((a, kw)) or {"state": "OPEN"},
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.HUNG
    runner.state.current_pr = PRInfo(number=42, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="parked", status=TaskStatus.DOING
    )
    runner._error_skip_active = True
    runner._idle_dispatch_deferred = True

    asyncio.run(runner.redis.set(f"control:{runner.name}:recover", "1"))
    asyncio.run(runner.handle_hung())

    # Recovery short-circuits the @codex review and PR-state probe paths.
    assert posted == []
    assert gh_calls == []
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert runner.state.error_message is None
    assert runner._error_skip_active is False
    assert runner._idle_dispatch_deferred is False
    # Flag is consumed (read-and-clear).
    assert f"control:{runner.name}:recover" not in runner.redis.store
    # Trapped task is recorded so the next IDLE cycle does not re-derive
    # it as DOING from the still-open PR and reattach to WATCH.
    assert "PR-001" in runner._recovered_task_pr_ids
    # Operator-visible event records the recovery cause.
    assert any(
        "[RECOVERY] Operator-initiated recovery from HUNG" in entry["event"]
        for entry in runner.state.history
    )


def test_handle_hung_runs_normal_path_when_no_recovery_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without the recover flag, ``handle_hung`` falls through to the
    @codex review fallback exactly as before PR-247."""
    posted: list[tuple[str, int, str]] = []
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *a, **kw: {"state": "OPEN"},
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.HUNG
    runner.state.current_pr = PRInfo(number=99, branch="pr-099")

    asyncio.run(runner.handle_hung())

    assert posted == [(runner.owner_repo, 99, "@codex review")]
    assert runner.state.state == PipelineState.WATCH


def test_handle_hung_recovery_signal_getdel_atomicity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The recover flag's read-and-clear is atomic via Redis GETDEL.

    A non-atomic read-then-delete can leave a stale recover key when
    delete fails, which would trigger an unintended auto-recovery on a
    later, unrelated HUNG state inside the TTL window. Verifies the
    handler invokes ``getdel`` (the atomic primitive) and does not fall
    back to a separate ``delete`` call."""
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *a, **kw: {"state": "OPEN"},
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda *a, **kw: None,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.HUNG
    runner.state.current_pr = PRInfo(number=11, branch="pr-011")

    asyncio.run(runner.redis.set(f"control:{runner.name}:recover", "1"))

    getdel_calls: list[str] = []
    original_getdel = runner.redis.getdel

    async def _tracking_getdel(key: str) -> str | None:
        getdel_calls.append(key)
        return await original_getdel(key)

    async def _forbidden_delete(_key: str) -> int:
        raise AssertionError(
            "handle_hung must use atomic getdel, not a separate delete"
        )

    monkeypatch.setattr(runner.redis, "getdel", _tracking_getdel)
    monkeypatch.setattr(runner.redis, "delete", _forbidden_delete)

    asyncio.run(runner.handle_hung())

    assert runner.state.state == PipelineState.IDLE
    assert getdel_calls == [f"control:{runner.name}:recover"]
    assert f"control:{runner.name}:recover" not in runner.redis.store


def test_handle_hung_persists_recovered_task_pr_ids_to_redis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-247 follow-up: ``_perform_operator_recovery`` must snapshot the
    in-memory ``_recovered_task_pr_ids`` set to Redis. Without this, a
    daemon restart between the recover click and the user's task re-
    upload would lose the marker; ``recover_state`` would rehydrate the
    QUEUE.md ``CANCELED`` row into ``_crashed_task_pr_ids`` instead, and
    the IDLE selector would discard the stricter override on the still-
    open PR re-deriving DOING — reattaching the runner to WATCH on the
    same stuck PR. Persisting the set lets the next recover_state cycle
    hydrate the stronger ``_recovered_task_pr_ids`` override across
    restarts so the abandon contract holds."""
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *a, **kw: {"state": "OPEN"},
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda *a, **kw: None,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.HUNG
    runner.state.current_pr = PRInfo(number=42, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="parked", status=TaskStatus.DOING
    )

    asyncio.run(runner.redis.set(f"control:{runner.name}:recover", "1"))
    asyncio.run(runner.handle_hung())

    assert runner.state.state == PipelineState.IDLE
    assert "PR-001" in runner._recovered_task_pr_ids
    raw = runner.redis.store.get(f"recovered_tasks:{runner.name}")
    assert raw is not None, (
        "operator recovery must snapshot _recovered_task_pr_ids to Redis"
    )
    import json as _json
    assert _json.loads(raw) == ["PR-001"]


def test_redis_round_trip_includes_current_queue() -> None:
    runner = h._make_runner()
    queue = [
        QueueTask(
            pr_id="PR-001",
            title="First",
            status=TaskStatus.DONE,
            task_file="tasks/PR-001.md",
            branch="pr-001-first",
        ),
        QueueTask(
            pr_id="PR-002",
            title="Second",
            status=TaskStatus.TODO,
            task_file="tasks/PR-002.md",
            depends_on=["PR-001"],
            branch="pr-002-second",
        ),
    ]
    runner.state.current_queue = queue

    asyncio.run(runner.publish_state())

    raw = runner.redis.store[pipeline_state(runner.name)]
    restored = RepoState.model_validate_json(raw)
    assert restored.current_queue == queue


def test_handle_hung_recovery_signal_getdel_failure_falls_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``getdel`` Redis error must fail closed: stay HUNG, do not
    invent a recovery transition the operator did not request."""
    posted: list[tuple[str, int, str]] = []
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *a, **kw: {"state": "OPEN"},
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.HUNG
    runner.state.current_pr = PRInfo(number=11, branch="pr-011")

    async def _boom_getdel(_key: str) -> str | None:
        raise RuntimeError("redis down")

    monkeypatch.setattr(runner.redis, "getdel", _boom_getdel)

    asyncio.run(runner.handle_hung())

    # Falls through to the @codex review path -> WATCH (default fallback).
    assert runner.state.state == PipelineState.WATCH
    assert posted == [(runner.owner_repo, 11, "@codex review")]


def test_handle_merge_success_sets_idle(monkeypatch: pytest.MonkeyPatch) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr("src.github.prs.merge_pr", lambda repo, num: None)
    monkeypatch.setattr(runner_module.PipelineRunner, "_mark_queue_done", lambda self: None)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
    )
    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None


def test_handle_merge_without_current_pr_sets_idle() -> None:
    runner = h._make_runner()
    runner.state.state = PipelineState.MERGE

    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None


def test_handle_merge_queue_sync_failure_still_goes_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When _mark_queue_done raises, handle_merge catches the exception
    and still transitions to IDLE. The pending_queue_sync_branch marker
    (set eagerly inside _mark_queue_done) gates handle_idle from
    re-dispatching the same task."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr("src.github.prs.merge_pr", lambda repo, num: None)

    def _failing_mark(self: Any) -> None:
        self.state.pending_queue_sync_branch = "queue-done-pr-001"
        raise RuntimeError("push rejected")

    monkeypatch.setattr(runner_module.PipelineRunner, "_mark_queue_done", _failing_mark)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
    )
    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.pending_queue_sync_branch == "queue-done-pr-001"


def test_escalate_queue_sync_transitions_to_error_when_expired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.state.pending_queue_sync_branch = "queue-done-pr-001"
    runner.state.pending_queue_sync_started_at = datetime.now(timezone.utc) - timedelta(hours=2)
    events: list[str] = []
    monkeypatch.setattr(runner, "log_event", events.append)

    asyncio.run(runner._escalate_queue_sync_if_expired("queue-done-pr-001"))

    assert runner.state.pending_queue_sync_branch is None
    assert runner.state.pending_queue_sync_started_at is None
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message is not None
    assert "queue-sync PR queue-done-pr-001 unresolved after " in (runner.state.error_message)
    assert f"(max {merge_module._QUEUE_SYNC_MAX_WAIT_SEC}s)" in (runner.state.error_message)
    assert events == [f"[MERGE] {runner.state.error_message}."]


def test_handle_merge_failure_sets_error(monkeypatch: pytest.MonkeyPatch) -> None:
    h._patch_subprocess(monkeypatch)

    def boom(repo: str, num: int) -> None:
        raise RuntimeError("merge conflict")

    monkeypatch.setattr("src.github.prs.merge_pr", boom)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.ERROR
    assert "merge conflict" in (runner.state.error_message or "")


def test_handle_merge_syncs_with_main(monkeypatch: pytest.MonkeyPatch) -> None:
    """Before calling merge_pr, handle_merge fetches and merges
    origin/<base> into the PR branch. When the branch is already
    up-to-date, the sync is a no-op and merge_pr runs immediately."""
    git_calls: list[list[str]] = []

    def fake_git(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        git_calls.append(cmd)
        if cmd[:2] == ["git", "merge"] and "origin/main" in cmd:
            return h._FakeCompletedProcess(args=cmd, stdout="Already up to date.\n", returncode=0)
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_git)

    merge_pr_calls: list[tuple[str, int]] = []

    def fake_merge_pr(repo: str, num: int) -> None:
        merge_pr_calls.append((repo, num))

    monkeypatch.setattr("src.github.prs.merge_pr", fake_merge_pr)
    monkeypatch.setattr(runner_module.PipelineRunner, "_mark_queue_done", lambda self: None)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)
    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.IDLE
    assert merge_pr_calls == [(runner.owner_repo, 5)]

    merge_idx = next(i for i, cmd in enumerate(git_calls) if cmd[:2] == ["git", "merge"] and "origin/main" in cmd)
    merge_pr_call_idx = len(git_calls)  # merge_pr ran after all git calls
    assert merge_idx < merge_pr_call_idx
    # No push because the merge was a no-op.
    assert not any(cmd[:2] == ["git", "push"] and "pr-001" in cmd for cmd in git_calls)


def test_handle_merge_marks_pr_ready_before_merge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)

    call_order: list[str] = []

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> None:
        assert cmd == ["pr", "ready", "5"]
        call_order.append("ready")

    def fake_merge_pr(repo: str, num: int) -> None:
        assert (repo, num) == (runner.owner_repo, 5)
        call_order.append("merge")

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    monkeypatch.setattr("src.github.prs.merge_pr", fake_merge_pr)
    monkeypatch.setattr(runner_module.PipelineRunner, "_mark_queue_done", lambda self: None)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)

    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.IDLE
    assert call_order == ["ready", "merge"]


def test_handle_merge_ignores_pr_ready_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)

    call_order: list[str] = []

    def fail_run_gh(cmd: list[str], **kwargs: Any) -> None:
        assert cmd == ["pr", "ready", "5"]
        call_order.append("ready")
        raise RuntimeError("ready failed")

    def fake_merge_pr(repo: str, num: int) -> None:
        assert (repo, num) == (runner.owner_repo, 5)
        call_order.append("merge")

    monkeypatch.setattr("src.github.gh_runner.run_gh", fail_run_gh)
    monkeypatch.setattr("src.github.prs.merge_pr", fake_merge_pr)
    monkeypatch.setattr(runner_module.PipelineRunner, "_mark_queue_done", lambda self: None)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)

    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.IDLE
    assert call_order == ["ready", "merge"]


def test_handle_merge_captures_success_stats_before_queue_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_git(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:2] == ["git", "merge"] and "origin/main" in cmd:
            return h._FakeCompletedProcess(args=cmd, stdout="Already up to date.\n", returncode=0)
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_git)
    monkeypatch.setattr("src.github.prs.merge_pr", lambda repo, num: None)

    call_order: list[str] = []

    def fake_compute(base_branch: str) -> dict[str, object]:
        call_order.append(f"compute:{base_branch}")
        return {
            "files_touched_count": 7,
            "languages_touched": ["python"],
            "diff_lines_added": 11,
            "diff_lines_deleted": 3,
            "test_file_ratio": 0.286,
        }

    def fake_mark(self: runner_module.PipelineRunner) -> None:
        call_order.append("queue")

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)
    runner._start_current_run_record("claude", "opus")
    monkeypatch.setattr(runner, "_compute_diff_stats", fake_compute)
    monkeypatch.setattr(runner_module.PipelineRunner, "_mark_queue_done", fake_mark)

    asyncio.run(runner.handle_merge())

    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=runner.name,
        )
    )

    assert call_order == ["compute:main", "queue"]
    assert len(recent) == 1
    assert recent[0].exit_reason == "success_merged"
    assert recent[0].files_touched_count == 7
    assert recent[0].languages_touched == ["python"]
    assert recent[0].diff_lines_added == 11
    assert recent[0].diff_lines_deleted == 3
    assert recent[0].test_file_ratio == 0.286
    assert recent[0].base_branch == "main"


def test_handle_merge_returns_to_watch_after_sync_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the sync produces a new commit and push succeeds, the
    merged commit invalidates previously observed gate state (branch
    protection may require up-to-date checks or dismiss approvals on
    new commits). Return to WATCH so the next cycle re-verifies gates
    against the refreshed HEAD instead of calling merge_pr with stale
    results."""
    git_calls: list[list[str]] = []

    def fake_git(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        git_calls.append(cmd)
        if cmd[:2] == ["git", "merge"] and "origin/main" in cmd:
            return h._FakeCompletedProcess(
                args=cmd,
                stdout="Merge made by the 'ort' strategy.\n",
                returncode=0,
            )
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_git)

    merge_pr_calls: list[tuple[str, int]] = []
    monkeypatch.setattr(
        "src.github.prs.merge_pr",
        lambda repo, num: merge_pr_calls.append((repo, num)),
    )

    post_calls: list[tuple[str, int, str]] = []
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, num, body: post_calls.append((repo, num, body)),
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_pr = pr
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)
    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is pr
    assert not merge_pr_calls, "merge_pr must not run with stale gate results after sync push"
    assert any(cmd[:2] == ["git", "push"] and "pr-001" in cmd for cmd in git_calls), (
        "sync must push the merged PR branch"
    )
    assert post_calls == [(runner.owner_repo, 5, "@codex review")], "must re-request Codex review on the refreshed HEAD"


def test_handle_merge_errors_when_codex_post_fails_after_sync_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failing to post @codex review after a sync push must flip the
    runner into ERROR: without a fresh review trigger on the new
    HEAD, the prior anchor +1 keeps the PR APPROVED and a subsequent
    handle_watch cycle would merge on stale approval."""

    def fake_git(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:2] == ["git", "merge"] and "origin/main" in cmd:
            return h._FakeCompletedProcess(
                args=cmd,
                stdout="Merge made by the 'ort' strategy.\n",
                returncode=0,
            )
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_git)

    def boom(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("gh api failure")

    monkeypatch.setattr("src.github.comments.post_comment", boom)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)
    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.ERROR
    assert "stale approval" in (runner.state.error_message or "")


def test_handle_merge_resolves_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When git merge origin/<base> reports a conflict, handle_merge
    asks Claude to resolve it. On success the merged HEAD is pushed
    and the runner returns to WATCH so the next cycle re-verifies
    gates — merge_pr is not called in the same cycle because the new
    commit invalidates previously observed CI/review state."""
    git_calls: list[list[str]] = []

    def fake_git(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        git_calls.append(cmd)
        if cmd[:2] == ["git", "merge"] and "origin/main" in cmd:
            return h._FakeCompletedProcess(
                args=cmd,
                returncode=1,
                stdout="CONFLICT (content): merge conflict in foo",
            )
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_git)

    claude_calls: list[tuple[str, str]] = []

    async def fake_claude(
        prompt: str,
        cwd: str,
        timeout: int | None = 600,
        model: str | None = None,
        **kwargs: Any,
    ) -> tuple[int, str, str]:
        claude_calls.append((prompt, cwd))
        return (0, "", "")

    monkeypatch.setattr(claude_cli, "run_claude_async", fake_claude)

    merge_pr_calls: list[tuple[str, int]] = []
    monkeypatch.setattr(
        "src.github.prs.merge_pr",
        lambda repo, num: merge_pr_calls.append((repo, num)),
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, num, body: None,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)
    runner._start_current_run_record("claude", "opus")
    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.WATCH
    assert claude_calls, "Claude must be invoked on merge conflict"
    assert not merge_pr_calls, "merge_pr must not run with stale gate results after sync push"
    assert runner._current_run_record is not None
    assert runner._current_run_record.had_merge_conflict is True
    assert any(cmd[:2] == ["git", "push"] and "pr-001" in cmd for cmd in git_calls), (
        "conflict-resolved HEAD must be pushed to origin"
    )


def test_handle_merge_falls_back_to_codex_for_conflict_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    git_calls: list[tuple[str, ...]] = []

    def fake_git(
        repo_path: str,
        *args: str,
        **kwargs: Any,
    ) -> h._FakeCompletedProcess:
        git_calls.append(args)
        if args[:2] == ("merge", "origin/main"):
            return h._FakeCompletedProcess(
                args=["git", *args],
                returncode=1,
                stdout="CONFLICT (content): merge conflict in foo",
            )
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

    proactive_coders: list[str | None] = []

    async def fake_check_rate_limit(
        self,
        proactive_coder: str | None = None,
    ) -> bool:
        proactive_coders.append(proactive_coder)
        return True

    codex_calls: list[tuple[str, str]] = []

    async def fake_codex(
        prompt: str,
        cwd: str,
        timeout: int | None = 600,
        model: str | None = None,
        **kwargs: Any,
    ) -> tuple[int, str, str]:
        codex_calls.append((prompt, cwd))
        return (0, "", "")

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(runner_module.PipelineRunner, "_check_rate_limit", fake_check_rate_limit)
    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_select_auxiliary_coder",
        lambda self: ("codex", self._registry.get("codex")),
    )
    monkeypatch.setattr(codex_cli, "run_codex_async", fake_codex)
    monkeypatch.setattr(
        claude_cli,
        "run_claude_async",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("Claude should not be used when Codex is selected")
        ),
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, num, body: None,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)

    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.WATCH
    assert proactive_coders == ["codex"]
    assert codex_calls, "Codex must be invoked on merge conflict fallback"
    assert any(cmd[:2] == ("push", "origin") for cmd in git_calls)


def test_handle_merge_sets_error_when_no_auxiliary_coder_is_eligible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    git_calls: list[tuple[str, ...]] = []
    claude_calls: list[object] = []
    codex_calls: list[object] = []

    def fake_git(
        repo_path: str,
        *args: str,
        **kwargs: Any,
    ) -> h._FakeCompletedProcess:
        git_calls.append(args)
        if args[:2] == ("merge", "origin/main"):
            return h._FakeCompletedProcess(
                args=["git", *args],
                returncode=1,
                stdout="CONFLICT (content): merge conflict in foo",
            )
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_select_auxiliary_coder",
        lambda self: None,
    )
    monkeypatch.setattr(
        claude_cli,
        "run_claude_async",
        lambda *args, **kwargs: claude_calls.append(args),
    )
    monkeypatch.setattr(
        codex_cli,
        "run_codex_async",
        lambda *args, **kwargs: codex_calls.append(args),
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )
    runner._start_current_run_record("claude", "opus")

    asyncio.run(runner.handle_merge())

    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=runner.name,
        )
    )

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "No eligible coder available for merge conflict resolution"
    assert ("merge", "--abort") in git_calls
    assert not claude_calls
    assert not codex_calls
    assert len(recent) == 1
    assert recent[0].exit_reason == "error"


def test_handle_merge_skips_sync_for_cross_repo_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fork-based PRs: the head branch is on the contributor's fork,
    not origin. Any local push of the head branch would fail. Skip the
    pre-merge sync entirely and defer to gh pr merge."""
    git_calls: list[list[str]] = []

    def fake_git(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        git_calls.append(cmd)
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_git)

    merge_pr_calls: list[tuple[str, int]] = []
    monkeypatch.setattr(
        "src.github.prs.merge_pr",
        lambda repo, num: merge_pr_calls.append((repo, num)),
    )
    monkeypatch.setattr(runner_module.PipelineRunner, "_mark_queue_done", lambda self: None)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001", is_cross_repository=True)
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)
    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.IDLE
    assert merge_pr_calls == [(runner.owner_repo, 5)]
    assert not any(cmd[:2] == ["git", "push"] and "pr-001" in cmd for cmd in git_calls), (
        "cross-repo PRs must not push the head branch to origin"
    )
    assert not any(cmd[:2] == ["git", "merge"] and "origin/main" in cmd for cmd in git_calls), (
        "cross-repo PRs must not merge base locally"
    )


def test_handle_merge_refreshes_pr_head_before_merge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After a daemon restart, the local PR branch may lag behind
    origin (recover_state resumes WATCH with a stale checkout). The
    sync must fetch origin/<pr_branch> and reset the local branch to
    it, or the subsequent push will be rejected as non-fast-forward."""
    git_calls: list[list[str]] = []

    def fake_git(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        git_calls.append(cmd)
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_git)
    monkeypatch.setattr("src.github.prs.merge_pr", lambda repo, num: None)
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, num, body: None,
    )
    monkeypatch.setattr(runner_module.PipelineRunner, "_mark_queue_done", lambda self: None)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)
    asyncio.run(runner.handle_merge())

    fetch_cmds = [cmd for cmd in git_calls if cmd[:4] == ["git", "fetch", "--prune", "origin"]]
    assert fetch_cmds and any("pr-001" in cmd for cmd in fetch_cmds), (
        "must fetch origin/<pr_branch> with --prune before local merge"
    )
    reset_cmds = [cmd for cmd in git_calls if cmd[:3] == ["git", "reset", "--hard"] and "origin/pr-001" in cmd]
    assert reset_cmds, "must reset local PR branch to origin/<pr_branch>"

    reset_idx = git_calls.index(reset_cmds[0])
    merge_idx = next(i for i, cmd in enumerate(git_calls) if cmd[:2] == ["git", "merge"] and "origin/main" in cmd)
    assert reset_idx < merge_idx, "reset to origin/<pr_branch> must happen before merging base"


def test_handle_merge_sets_error_on_non_conflict_sync_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_git(
        repo_path: str,
        *args: str,
        **kwargs: Any,
    ) -> h._FakeCompletedProcess:
        if args[:2] == ("merge", "origin/main"):
            return h._FakeCompletedProcess(
                args=["git", *args],
                returncode=1,
                stderr="fatal: unrelated histories",
            )
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )
    runner._start_current_run_record("claude", "opus")

    asyncio.run(runner.handle_merge())

    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=runner.name,
        )
    )

    assert runner.state.state == PipelineState.ERROR
    assert "fatal: unrelated histories" in (runner.state.error_message or "")
    assert len(recent) == 1
    assert recent[0].exit_reason == "error"


def test_handle_merge_aborts_on_unresolvable_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When Claude fails to resolve the conflict, handle_merge aborts
    the merge, sets ERROR, and does not call prs.merge_pr."""
    git_calls: list[list[str]] = []

    def fake_git(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        git_calls.append(cmd)
        if cmd[:2] == ["git", "merge"] and "origin/main" in cmd:
            return h._FakeCompletedProcess(
                args=cmd,
                returncode=1,
                stdout="CONFLICT (content): merge conflict in foo",
            )
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_git)

    async def fake_claude_async(
        prompt: str,
        cwd: str,
        timeout: int | None = 600,
        model: str | None = None,
        **kwargs: Any,
    ) -> tuple[int, str, str]:
        return (1, "", "claude failed")

    monkeypatch.setattr(
        claude_cli,
        "run_claude_async",
        fake_claude_async,
    )

    merge_pr_calls: list[tuple[str, int]] = []

    def fake_merge_pr(repo: str, num: int) -> None:
        merge_pr_calls.append((repo, num))

    monkeypatch.setattr("src.github.prs.merge_pr", fake_merge_pr)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)
    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.ERROR
    assert "Merge conflict resolution failed" in (runner.state.error_message or "")
    assert not merge_pr_calls, "merge_pr must not be called on abort"
    abort_cmds = [cmd for cmd in git_calls if cmd[:3] == ["git", "merge", "--abort"]]
    assert abort_cmds, "git merge --abort must be invoked"


def test_handle_merge_sets_error_when_pre_sync_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_retry_transient(op: Any, **kwargs: Any) -> h._FakeCompletedProcess:
        return op()

    def fake_git(
        repo_path: str,
        *args: str,
        **kwargs: Any,
    ) -> h._FakeCompletedProcess:
        if args[:4] == ("fetch", "--prune", "origin", "main"):
            raise OSError("network down")
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

    monkeypatch.setattr(merge_module, "retry_transient", fake_retry_transient)
    monkeypatch.setattr(git_ops_module, "_git", fake_git)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )
    runner._start_current_run_record("claude", "opus")

    asyncio.run(runner.handle_merge())

    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=runner.name,
        )
    )

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "Pre-merge sync failed: network down"
    assert len(recent) == 1
    assert recent[0].exit_reason == "error"


def test_handle_coding_saves_stdout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """handle_coding must save CLI stdout to Redis via _save_cli_log."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_auto_pr_async",
        h._async_cli_result(0, "hello from claude", ""),
    )
    pr = PRInfo(number=42, branch="pr-001")
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda *a, **kw: None,
    )

    runner = h._make_runner()
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001")
    asyncio.run(runner.handle_coding())

    redis_keys = [k for k, _v in runner.redis.writes]
    assert any(k == f"cli_log:{runner.name}:latest" for k in redis_keys)
    stored = runner.redis.store.get(f"cli_log:{runner.name}:latest")
    assert "hello from claude" in (stored or "")
    assert "=== STDOUT ===" in (stored or "")
    assert "=== STDERR ===" in (stored or "")


def test_handle_coding_uses_configured_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """planned_pr_timeout_sec from config must be forwarded to run_planned_pr."""
    h._patch_subprocess(monkeypatch)
    captured: dict[str, Any] = {}

    async def fake_planned(
        path: str, *_args: object, model: str | None = None, timeout: int | None = None, **kwargs: object
    ) -> tuple[int, str, str]:
        captured["timeout"] = timeout
        return (0, "", "")

    monkeypatch.setattr(claude_cli, "run_auto_pr_async", fake_planned)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [PRInfo(number=1, branch="pr-001")],
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda *a, **kw: None,
    )

    runner = PipelineRunner(
        h._repo_cfg(),
        AppConfig(
            repositories=[],
            daemon=DaemonConfig(
                planned_pr_timeout_sec=1234,
                auto_fallback=False,
            ),
        ),
        h._FakeRedis(),
        *h._usage_providers(),
    )
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001")
    asyncio.run(runner.handle_coding())
    assert captured.get("timeout") == 1234


def test_handle_external_terminal_pr_state_closed_transitions_to_hung(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Detected external close must transition runner to HUNG for manual
    review AND finalize the active run record as ``closed_unmerged`` so
    the next ``handle_hung`` -> IDLE tick does not strand the run with
    a missing ``ended_at`` / ``exit_reason`` (Codex P2 follow-up on PR
    #223)."""
    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-055",
        title="external close",
        status=TaskStatus.DOING,
        branch="pr-055",
        task_file="tasks/PR-055.md",
    )
    runner.state.current_pr = PRInfo(number=55, branch="pr-055")
    runner._start_current_run_record("claude", "opus")
    monkeypatch.setattr(runner, "_compute_diff_stats", lambda base_branch: {})

    saved: list[str] = []
    original_save = runner._save_current_run_record

    async def spy_save(exit_reason: str, **kwargs: object) -> None:
        saved.append(exit_reason)
        await original_save(exit_reason, **kwargs)

    runner._save_current_run_record = spy_save  # type: ignore[assignment]

    asyncio.run(runner._handle_external_terminal_pr_state("CLOSED"))

    assert runner.state.state == PipelineState.HUNG
    assert runner.state.error_message is None
    assert saved == ["closed_unmerged"]
    assert runner._current_run_record is None
    assert any(
        "PR #55 closed externally during FIX, transitioning to HUNG." in entry["event"]
        for entry in runner.state.history
    )


def test_handle_coding_uses_async(monkeypatch: pytest.MonkeyPatch) -> None:
    """handle_coding must call run_auto_pr_async, not the sync version."""
    h._patch_subprocess(monkeypatch)
    async_calls: list[str] = []
    sync_calls: list[str] = []

    async def fake_async(
        path: str, *_args: object, **kwargs: object
    ) -> tuple[int, str, str]:
        async_calls.append(path)
        return (0, "ok", "")

    def fake_sync(path: str, model: str | None = None, timeout: int | None = None) -> tuple[int, str, str]:
        sync_calls.append(path)
        return (0, "ok", "")

    monkeypatch.setattr(claude_cli, "run_auto_pr_async", fake_async)
    monkeypatch.setattr(claude_cli, "run_planned_pr", fake_sync)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [PRInfo(number=1, branch="pr-001")],
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda *a, **kw: None,
    )

    runner = h._make_runner()
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001")
    asyncio.run(runner.handle_coding())

    assert async_calls, "run_auto_pr_async must be called"
    assert not sync_calls, "sync run_planned_pr must NOT be called"


def test_handle_coding_publishes_heartbeat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """publish_state is called during Claude CLI run via heartbeat task."""
    h._patch_subprocess(monkeypatch)
    heartbeat_publishes: list[str] = []

    original_publish = PipelineRunner.publish_state

    async def counting_publish(self: Any) -> None:
        await original_publish(self)

    monkeypatch.setattr(PipelineRunner, "publish_state", counting_publish)

    cli_done = None

    async def slow_cli(
        path: str, *_args: object, **kwargs: object
    ) -> tuple[int, str, str]:
        nonlocal cli_done
        cli_done = asyncio.get_event_loop().create_future()
        await cli_done
        return (0, "ok", "")

    monkeypatch.setattr(claude_cli, "run_auto_pr_async", slow_cli)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [PRInfo(number=1, branch="pr-001")],
    )
    monkeypatch.setattr("src.github.comments.post_comment", lambda *a, **kw: None)

    async def fast_heartbeat(self: Any, label: str) -> None:
        while True:
            await asyncio.sleep(0.01)
            self.log_event(f"{label}...")
            heartbeat_publishes.append(label)
            await self.publish_state()

    monkeypatch.setattr(PipelineRunner, "_publish_while_waiting", fast_heartbeat)

    async def run() -> None:
        runner = h._make_runner()
        runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001")
        task = asyncio.create_task(runner.handle_coding())
        await asyncio.sleep(0.05)
        cli_done.set_result(None)
        await task

    asyncio.run(run())

    assert len(heartbeat_publishes) >= 2, (
        f"Expected heartbeat to publish at least twice, got {len(heartbeat_publishes)}"
    )


def test_handle_coding_errors_when_get_open_prs_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GitHub list failures after a successful CLI run surface as ERROR."""
    from src import codex_cli

    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        codex_cli,
        "run_auto_pr_async",
        h._async_cli_result(0, "ok", ""),
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda *args, **kwargs: h._raise_runtime_error("gh unavailable"),
    )

    runner = h._make_runner(coder=CoderType.CODEX)
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
    )
    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "get_open_prs failed: gh unavailable"
    assert any(entry["event"] == "[CODING] get_open_prs failed: gh unavailable." for entry in runner.state.history)


def test_handle_coding_uses_codex_cli_when_coder_is_codex(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src import codex_cli

    h._patch_subprocess(monkeypatch)
    captured_module: list[str] = []

    async def fake_run_planned_pr(path: str, *_args: object, **kwargs: object) -> tuple:
        captured_module.append("codex")
        return (0, "ok", "")

    monkeypatch.setattr(codex_cli, "run_auto_pr_async", fake_run_planned_pr)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda *a, **kw: [
            PRInfo(
                number=42,
                url="https://github.com/octo/demo/pull/42",
                branch="pr-001",
                ci_status=CIStatus.PENDING,
                review_status=ReviewStatus.PENDING,
            )
        ],
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda *a, **kw: True,
    )

    runner = h._make_runner(coder=CoderType.CODEX)
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
    )
    asyncio.run(runner.handle_coding())

    assert captured_module == ["codex"]
    assert runner.state.state == PipelineState.WATCH


def test_handle_coding_honors_stop_requested_after_pr_poll_exhaustion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_auto_pr_async",
        h._async_cli_result(0, "ok", ""),
    )

    async def stale_stop_monitor(
        _cli_task: asyncio.Task[tuple[int, str, str]],
    ) -> None:
        return None

    pop_calls = {"count": 0}
    attempts = {"count": 0}

    async def fake_pop_stop_request() -> bool:
        pop_calls["count"] += 1
        return pop_calls["count"] == 6

    async def instant_sleep(_seconds: float) -> None:
        return None

    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-127",
        title="Pause controls",
        status=TaskStatus.DOING,
        branch="pr-127-control-endpoints-backend",
    )

    def fake_get_open_prs(repo: str, **kw: Any) -> list[PRInfo]:
        attempts["count"] += 1
        return []

    monkeypatch.setattr("src.github.prs.get_open_prs", fake_get_open_prs)
    monkeypatch.setattr(runner, "_pop_stop_request", fake_pop_stop_request)
    monkeypatch.setattr(runner_module.asyncio, "sleep", instant_sleep)
    monkeypatch.setattr(runner, "_monitor_stop_request", stale_stop_monitor)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.error_message is None
    assert runner.state.current_pr is None
    assert attempts["count"] == 3
    assert any(entry["event"] == "[CODING] CODING aborted: user stop requested." for entry in runner.state.history)
