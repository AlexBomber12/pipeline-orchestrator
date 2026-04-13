"""Tests for PipelineRunner.recover_state and the _recovered one-shot gate."""

from __future__ import annotations

import asyncio
import subprocess
from typing import Any

import pytest

from src.config import AppConfig, DaemonConfig, RepoConfig
from src.daemon import runner as runner_module
from src.daemon.runner import PipelineRunner
from src.models import (
    CIStatus,
    PipelineState,
    PRInfo,
    QueueTask,
    ReviewStatus,
    TaskStatus,
)


class _FakeRedis:
    """Minimal async Redis double capturing ``set`` calls."""

    def __init__(self) -> None:
        self.writes: list[tuple[str, str]] = []

    async def set(self, key: str, value: str) -> None:
        self.writes.append((key, value))


def _repo_cfg(**overrides: Any) -> RepoConfig:
    base: dict[str, Any] = {
        "url": "https://github.com/octo/demo.git",
        "branch": "main",
        "auto_merge": True,
        "review_timeout_min": 30,
        "poll_interval_sec": 60,
    }
    base.update(overrides)
    return RepoConfig(**base)


def _make_runner() -> PipelineRunner:
    return PipelineRunner(
        _repo_cfg(),
        AppConfig(repositories=[], daemon=DaemonConfig()),
        _FakeRedis(),
    )


def _doing_task() -> QueueTask:
    return QueueTask(
        pr_id="PR-042",
        title="In-flight",
        status=TaskStatus.DOING,
        branch="pr-042-inflight",
    )


def _done_task() -> QueueTask:
    return QueueTask(
        pr_id="PR-041",
        title="Merged upstream",
        status=TaskStatus.DONE,
        branch="pr-041-done",
    )


def _todo_task() -> QueueTask:
    return QueueTask(
        pr_id="PR-043",
        title="Next up",
        status=TaskStatus.TODO,
        branch="pr-043-next",
    )


def test_recover_doing_task_with_matching_pr_recovers_to_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DOING task + matching open PR on that branch -> WATCH, no CODING run."""
    task = _doing_task()
    matching_pr = PRInfo(
        number=17,
        branch="pr-042-inflight",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: [matching_pr]
    )

    coding_called = False

    async def boom() -> None:  # pragma: no cover - must not fire
        nonlocal coding_called
        coding_called = True

    runner = _make_runner()
    runner._parse_base_queue = lambda: [task]  # type: ignore[method-assign]
    runner.handle_coding = boom  # type: ignore[method-assign]
    asyncio.run(runner.recover_state())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-042"
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 17
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 1
    assert coding_called is False
    assert any(
        "Recovered: DOING task PR-042" in e["event"] and "WATCH PR #17" in e["event"]
        for e in runner.state.history
    )


def test_recover_state_sets_queue_counters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """recover_state must populate queue_done and queue_total."""
    done_task = QueueTask(
        pr_id="PR-001", title="Done", status=TaskStatus.DONE,
        branch="pr-001-done",
    )
    doing_task = _doing_task()
    todo_task = QueueTask(
        pr_id="PR-043", title="Todo", status=TaskStatus.TODO,
        branch="pr-043-todo",
    )
    matching_pr = PRInfo(
        number=17,
        branch="pr-042-inflight",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: [matching_pr]
    )

    runner = _make_runner()
    runner._parse_base_queue = lambda: [done_task, doing_task, todo_task]  # type: ignore[method-assign]
    runner.handle_coding = lambda: None  # type: ignore[method-assign]
    asyncio.run(runner.recover_state())

    assert runner.state.queue_done == 1
    assert runner.state.queue_total == 3


def test_recover_doing_task_without_pr_rerun_coding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DOING task + no matching PR -> CODING + re-run handle_coding()."""
    task = _doing_task()
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: []
    )

    coding_calls: list[PipelineState] = []

    async def fake_coding() -> None:
        # Capture the state at the moment handle_coding was invoked so the
        # test can prove recover_state transitioned to CODING before
        # calling.
        coding_calls.append(runner.state.state)

    runner = _make_runner()
    runner._parse_base_queue = lambda: [task]  # type: ignore[method-assign]
    runner.handle_coding = fake_coding  # type: ignore[method-assign]
    # Stub out the preserve helper so this test focuses on the
    # state-transition contract. A dedicated test below covers its
    # ordering relative to handle_coding.
    runner._preserve_crashed_run_commits = lambda branch: True  # type: ignore[method-assign]
    asyncio.run(runner.recover_state())

    assert coding_calls == [PipelineState.CODING]
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-042"
    assert runner.state.current_pr is None
    assert any(
        "Recovered: DOING task PR-042" in e["event"]
        and "re-running CODING" in e["event"]
        for e in runner.state.history
    )


def test_recover_preserves_crashed_run_commits_before_coding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex P1: Claude's PLANNED PR flow recreates the task branch from
    ``origin/main``, which would orphan unpushed local commits from a
    crashed run. ``recover_state`` must push the local task branch before
    handing off to ``handle_coding`` so the work is durable on origin.
    """
    task = _doing_task()
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: []
    )

    events: list[str] = []

    async def fake_coding() -> None:
        events.append("coding")

    def fake_run(cmd: list[str], **kwargs: Any) -> Any:
        if cmd[:4] == ["git", "rev-parse", "--verify", "--quiet"]:
            events.append("probe")
            # Local branch exists.
            class R:
                returncode = 0
                stdout = "abc\n"
                stderr = ""
            return R()
        if cmd[:2] == ["git", "push"]:
            events.append(f"push:{cmd[-1]}")
            class R:
                returncode = 0
                stdout = ""
                stderr = ""
            return R()
        class R:
            returncode = 0
            stdout = ""
            stderr = ""
        return R()

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    runner = _make_runner()
    runner._parse_base_queue = lambda: [task]  # type: ignore[method-assign]
    runner.handle_coding = fake_coding  # type: ignore[method-assign]
    asyncio.run(runner.recover_state())

    # Preserve push must happen before handle_coding re-runs CODING.
    push_idx = next(i for i, e in enumerate(events) if e.startswith("push:"))
    coding_idx = events.index("coding")
    assert push_idx < coding_idx
    # Must push the exact task branch.
    assert "push:pr-042-inflight:pr-042-inflight" in events
    assert any(
        "Preserved crashed-run commits on pr-042-inflight" in e["event"]
        for e in runner.state.history
    )


def test_recover_preserve_tolerates_missing_local_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the crashed run never created the local branch (crash before
    Claude's first commit), ``_preserve_crashed_run_commits`` must be a
    no-op rather than failing recovery."""
    task = _doing_task()
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: []
    )

    pushes: list[list[str]] = []

    async def fake_coding() -> None:
        return None

    def fake_run(cmd: list[str], **kwargs: Any) -> Any:
        if cmd[:4] == ["git", "rev-parse", "--verify", "--quiet"]:
            class R:
                returncode = 1
                stdout = ""
                stderr = ""
            return R()
        if cmd[:2] == ["git", "push"]:
            pushes.append(cmd)
        class R:
            returncode = 0
            stdout = ""
            stderr = ""
        return R()

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    runner = _make_runner()
    runner._parse_base_queue = lambda: [task]  # type: ignore[method-assign]
    runner.handle_coding = fake_coding  # type: ignore[method-assign]
    asyncio.run(runner.recover_state())

    # No push when local branch doesn't exist.
    assert pushes == []
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-042"


def test_recover_preserve_refuses_base_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex P1: a malformed QUEUE.md entry with ``Branch: main`` must
    not cause recovery to push directly to the base branch, bypassing
    the PR/review gate. ``_preserve_crashed_run_commits`` must refuse
    and recover_state must flip to ERROR rather than running CODING
    against a base-branch task entry."""
    task = QueueTask(
        pr_id="PR-042",
        title="malformed",
        status=TaskStatus.DOING,
        branch="main",  # Same as the repo's base branch.
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: []
    )

    pushes: list[list[str]] = []
    probes: list[list[str]] = []
    coding_ran: list[bool] = []

    async def fake_coding() -> None:
        coding_ran.append(True)

    def fake_run(cmd: list[str], **kwargs: Any) -> Any:
        if cmd[:4] == ["git", "rev-parse", "--verify", "--quiet"]:
            probes.append(cmd)
        if cmd[:2] == ["git", "push"]:
            pushes.append(cmd)
        class R:
            returncode = 0
            stdout = ""
            stderr = ""
        return R()

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    runner = _make_runner()
    runner._parse_base_queue = lambda: [task]  # type: ignore[method-assign]
    runner.handle_coding = fake_coding  # type: ignore[method-assign]
    asyncio.run(runner.recover_state())

    # No push to the base branch, and we never even probed for the
    # local ref — the guard must short-circuit before any subprocess.
    assert pushes == []
    assert probes == []
    assert coding_ran == [], "handle_coding must not run after refusal"
    assert runner.state.state == PipelineState.ERROR
    assert "could not preserve crashed-run commits on 'main'" in (
        runner.state.error_message or ""
    )
    assert any(
        "Refusing to preserve crashed-run commits on base branch 'main'"
        in e["event"]
        for e in runner.state.history
    )


def test_recover_aborts_when_preserve_push_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex P1: a failed preserve push leaves crashed-run commits only
    on local. ``recover_state`` must NOT proceed to handle_coding (which
    would let Claude reset the branch from origin/main and orphan the
    work). Stop in ERROR so an operator can intervene."""
    task = _doing_task()
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: []
    )

    coding_ran: list[bool] = []

    async def fake_coding() -> None:
        coding_ran.append(True)

    def fake_run(cmd: list[str], **kwargs: Any) -> Any:
        if cmd[:4] == ["git", "rev-parse", "--verify", "--quiet"]:
            class R:
                returncode = 0
                stdout = "abc\n"
                stderr = ""
            return R()
        if cmd[:2] == ["git", "push"]:
            raise subprocess.CalledProcessError(
                1, cmd, stderr="auth transient"
            )
        class R:
            returncode = 0
            stdout = ""
            stderr = ""
        return R()

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    runner = _make_runner()
    runner._parse_base_queue = lambda: [task]  # type: ignore[method-assign]
    runner.handle_coding = fake_coding  # type: ignore[method-assign]
    asyncio.run(runner.recover_state())

    assert coding_ran == [], "handle_coding must not run after preserve fail"
    assert runner.state.state == PipelineState.ERROR
    assert "could not preserve crashed-run commits on 'pr-042-inflight'" in (
        runner.state.error_message or ""
    )
    assert any(
        "Failed to preserve unpushed commits on pr-042-inflight"
        in e["event"]
        for e in runner.state.history
    )


def test_recover_no_doing_with_done_matched_pr_recovers_to_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A DONE task whose PR is still open (marked DONE locally but not
    yet merged) -> WATCH. The DONE task is attached as current_task and
    the recovery log line records the matched task id and status."""
    done = _done_task()
    todo = _todo_task()
    done_pr = PRInfo(
        number=88,
        branch="pr-041-done",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.APPROVED,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: [done_pr]
    )

    runner = _make_runner()
    runner._parse_base_queue = lambda: [done, todo]  # type: ignore[method-assign]
    asyncio.run(runner.recover_state())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 88
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-041"
    assert any(
        "Recovered: DONE task PR-041" in e["event"]
        and "WATCH PR #88" in e["event"]
        for e in runner.state.history
    )


def test_recover_no_doing_with_todo_matched_pr_recovers_to_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P1-F regression: tasks flip TODO -> DONE in a single commit as
    part of their own implementation PR, so on a restart from main an
    in-flight task is still TODO until its PR merges. Recovery must
    match open PRs against TODO tasks — otherwise the orphan-PR path
    falls back to clean-slate IDLE and the next cycle's handle_idle
    re-runs PLANNED PR on the already-open PR, running claude_cli a
    second time on active work."""
    todo = QueueTask(
        pr_id="PR-010",
        title="Daemon recovery",
        status=TaskStatus.TODO,
        branch="pr-010-recovery",
    )
    in_flight = PRInfo(
        number=17,
        branch="pr-010-recovery",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: [in_flight]
    )

    runner = _make_runner()
    runner._parse_base_queue = lambda: [todo]  # type: ignore[method-assign]
    result = asyncio.run(runner.recover_state())

    assert result is True
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 17
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-010"
    assert runner.state.current_task.status == TaskStatus.TODO
    assert any(
        "Recovered: TODO task PR-010" in e["event"]
        and "WATCH PR #17" in e["event"]
        for e in runner.state.history
    )


def test_recover_unrelated_open_pr_stays_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An open PR whose branch is not in QUEUE.md (human contributor,
    dependabot, renovate, etc.) must NOT be attached: otherwise the
    runner would later drive merge/fix against a PR outside its queue.
    The queue-match guard stays even after widening the match to TODO
    tasks — only branches that appear in QUEUE.md are eligible."""
    # QUEUE.md has a TODO task on one branch; the open PR is on a
    # different, unrelated branch. The TODO task must NOT be attached
    # to the unrelated PR just because TODO is now eligible for match.
    queued_todo = QueueTask(
        pr_id="PR-050",
        title="Unrelated queued work",
        status=TaskStatus.TODO,
        branch="pr-050-queued",
    )
    unrelated = PRInfo(number=99, branch="dependabot/npm/foo")
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: [unrelated]
    )

    runner = _make_runner()
    runner._parse_base_queue = lambda: [queued_todo]  # type: ignore[method-assign]
    asyncio.run(runner.recover_state())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None
    assert any(
        "not matched to any" in e["event"] for e in runner.state.history
    )


def test_recover_attaches_only_to_done_matched_pr_among_many(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When multiple open PRs exist, recovery must pick the one whose
    branch matches a DONE task and ignore unrelated PRs entirely."""
    done = _done_task()
    unrelated_first = PRInfo(number=200, branch="dependabot/npm/foo")
    matching = PRInfo(
        number=201,
        branch="pr-041-done",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.APPROVED,
    )
    unrelated_last = PRInfo(number=202, branch="user/experiment")
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo: [unrelated_first, matching, unrelated_last],
    )

    runner = _make_runner()
    runner._parse_base_queue = lambda: [done]  # type: ignore[method-assign]
    asyncio.run(runner.recover_state())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 201
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-041"


def test_recover_no_doing_no_prs_stays_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Clean slate: no DOING tasks and no open PRs -> stays IDLE."""
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: []
    )

    runner = _make_runner()
    runner._parse_base_queue = lambda: [_todo_task()]  # type: ignore[method-assign]
    asyncio.run(runner.recover_state())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert any(
        "no DOING tasks, no open PRs" in e["event"]
        for e in runner.state.history
    )


def test_recover_clean_slate_resets_prior_error_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P1-E regression: cycle 1 discovery failed (state=ERROR,
    error_message set, _recovered=False). Cycle 2 discovery succeeds but
    finds no in-flight work. recover_state must explicitly reset state
    to IDLE and clear error_message — otherwise the runner would return
    True, run_cycle would publish the still-ERROR state, and (with
    error_handler_use_ai disabled) the queue would never progress."""
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: []
    )

    runner = _make_runner()
    runner._parse_base_queue = lambda: []  # type: ignore[method-assign]
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = (
        "recover_state: get_open_prs failed: gh api rate limited"
    )

    result = asyncio.run(runner.recover_state())

    assert result is True
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None
    assert runner.state.current_task is None
    assert runner.state.current_pr is None


def test_recover_clean_slate_resets_error_with_unrelated_prs_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The clean-slate reset must also fire when the repository has open
    PRs that don't match any DONE task (e.g. a dependabot PR). That path
    is semantically 'no in-flight work to resume' and must restore IDLE
    from any prior ERROR state, not just the strictly empty-PR case."""
    unrelated = PRInfo(number=77, branch="dependabot/npm/foo")
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: [unrelated]
    )

    runner = _make_runner()
    runner._parse_base_queue = lambda: []  # type: ignore[method-assign]
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "recover_state: get_open_prs failed: boom"

    result = asyncio.run(runner.recover_state())

    assert result is True
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None
    assert runner.state.current_pr is None


def test_recover_get_open_prs_failure_sets_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failing GitHub API call during recovery must transition to ERROR
    AND return False so run_cycle leaves _recovered unset and retries
    discovery on the next cycle (rather than silently going IDLE and
    picking up a new task that might collide with an unknown in-flight
    PR)."""

    def boom(repo: str) -> list[PRInfo]:
        raise RuntimeError("gh auth token expired")

    monkeypatch.setattr(runner_module.github_client, "get_open_prs", boom)

    runner = _make_runner()
    runner._parse_base_queue = lambda: []  # type: ignore[method-assign]
    result = asyncio.run(runner.recover_state())

    assert result is False
    assert runner.state.state == PipelineState.ERROR
    assert "gh auth token expired" in (runner.state.error_message or "")


def test_recover_state_runs_only_once_per_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """run_cycle must call recover_state exactly once; subsequent cycles
    must honor the _recovered flag and skip it."""
    # Make ensure_repo_cloned, preflight, and handle_idle cheap no-ops so the
    # test isolates the recovery gate.
    async def noop_ensure() -> None:
        return None

    def clean_preflight() -> bool:
        return True

    async def noop_idle() -> None:
        return None

    calls: list[str] = []

    async def counting_recover() -> bool:
        calls.append("recover")
        return True

    runner = _make_runner()
    runner.ensure_repo_cloned = noop_ensure  # type: ignore[method-assign]
    runner.preflight = clean_preflight  # type: ignore[method-assign]
    runner.handle_idle = noop_idle  # type: ignore[method-assign]
    runner.recover_state = counting_recover  # type: ignore[method-assign]

    asyncio.run(runner.run_cycle())
    asyncio.run(runner.run_cycle())
    asyncio.run(runner.run_cycle())

    assert calls == ["recover"]
    assert runner._recovered is True


def test_run_cycle_recovered_watch_does_not_dispatch_handle_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The exact P1-D regression: recovery resurrects a PR that happens
    to be CHANGES_REQUESTED / CI-failing. If run_cycle dispatched
    handle_watch on the recovery cycle, handle_watch would immediately
    call handle_fix, which runs claude_cli.fix_review against a working
    tree that preflight did NOT validate. In crash-recovery scenarios
    with leftover local edits that would push unintended files into the
    recovered PR. The recovery cycle must publish recovered state and
    return; the next cycle runs the normal preflight + dispatch path."""
    task = _doing_task()
    # CI FAILURE + CHANGES_REQUESTED is the exact shape that would
    # trigger handle_watch -> handle_fix -> claude_cli.fix_review.
    recovered_pr = PRInfo(
        number=17,
        branch="pr-042-inflight",
        ci_status=CIStatus.FAILURE,
        review_status=ReviewStatus.CHANGES_REQUESTED,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: [recovered_pr]
    )

    async def noop_ensure() -> None:
        return None

    watch_calls: list[int] = []

    async def spy_watch() -> None:
        watch_calls.append(1)

    fix_calls: list[int] = []

    async def spy_fix() -> None:
        fix_calls.append(1)

    preflight_calls: list[int] = []

    def spy_preflight() -> bool:
        preflight_calls.append(1)
        return True

    runner = _make_runner()
    runner._parse_base_queue = lambda: [task]  # type: ignore[method-assign]
    runner.ensure_repo_cloned = noop_ensure  # type: ignore[method-assign]
    runner.handle_watch = spy_watch  # type: ignore[method-assign]
    runner.handle_fix = spy_fix  # type: ignore[method-assign]
    runner.preflight = spy_preflight  # type: ignore[method-assign]

    asyncio.run(runner.run_cycle())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 17
    assert watch_calls == [], "handle_watch must not dispatch on recovery cycle"
    assert fix_calls == [], "handle_fix must not dispatch on recovery cycle"
    assert preflight_calls == [], "preflight must not run on recovery cycle"
    assert runner._recovered is True
    # Verify Redis got the recovered state before the cycle ended.
    assert isinstance(runner.redis, _FakeRedis)
    assert runner.redis.writes, "publish_state should have been called once"


def test_run_cycle_recovered_idle_does_not_dispatch_handle_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Even the clean-slate IDLE recovery path stops after publishing on
    the recovery cycle. handle_idle's sync_to_main could safely clean any
    leftover state, but making IDLE the only dispatched recovery state
    complicates the invariant and obscures the 'recovery cycle only
    discovers' contract. The next cycle's handle_idle will sync_to_main
    and pick up the next task normally."""
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: []
    )

    async def noop_ensure() -> None:
        return None

    idle_calls: list[int] = []

    async def spy_idle() -> None:
        idle_calls.append(1)

    preflight_calls: list[int] = []

    def spy_preflight() -> bool:
        preflight_calls.append(1)
        return True

    runner = _make_runner()
    runner._parse_base_queue = lambda: []  # type: ignore[method-assign]
    runner.ensure_repo_cloned = noop_ensure  # type: ignore[method-assign]
    runner.handle_idle = spy_idle  # type: ignore[method-assign]
    runner.preflight = spy_preflight  # type: ignore[method-assign]

    asyncio.run(runner.run_cycle())

    assert runner.state.state == PipelineState.IDLE
    assert idle_calls == [], "handle_idle must not dispatch on recovery cycle"
    assert preflight_calls == [], "preflight must not run on recovery cycle"
    assert runner._recovered is True


def test_run_cycle_dirty_tree_does_not_clobber_recovered_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The exact crash-recovery case: a DOING task with a matching open PR,
    and the working tree is dirty because the prior cycle crashed. Preflight
    must NOT fire on the recovery cycle — otherwise it would overwrite the
    WATCH state with ERROR and _recovered would already be True, stranding
    the daemon."""
    task = _doing_task()
    matching_pr = PRInfo(
        number=17,
        branch="pr-042-inflight",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: [matching_pr]
    )

    async def noop_ensure() -> None:
        return None

    async def noop_watch() -> None:
        return None

    preflight_calls: list[bool] = []

    def fail_preflight() -> bool:
        # If preflight ever runs on the recovery cycle it will report a
        # dirty tree and flip state to ERROR — exactly the P1 regression.
        preflight_calls.append(True)
        runner.state.state = PipelineState.ERROR
        runner.state.error_message = "working tree dirty: leftover.py"
        return False

    runner = _make_runner()
    runner._parse_base_queue = lambda: [task]  # type: ignore[method-assign]
    runner.ensure_repo_cloned = noop_ensure  # type: ignore[method-assign]
    runner.preflight = fail_preflight  # type: ignore[method-assign]
    runner.handle_watch = noop_watch  # type: ignore[method-assign]

    asyncio.run(runner.run_cycle())

    assert preflight_calls == [], "preflight must not run on recovery cycle"
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 17
    assert runner.state.error_message is None


def test_run_cycle_transient_discovery_failure_stays_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When recover_state's discovery phase fails (e.g. GitHub unreachable),
    _recovered must stay False so the next cycle retries. Otherwise the
    daemon would drift detached from an in-flight PR and a later
    handle_error SKIP/FIX could push it onto new queue work. The second
    cycle, once get_open_prs recovers, must re-run discovery and attach
    to the in-flight DOING task's PR."""
    task = _doing_task()
    matching_pr = PRInfo(
        number=17,
        branch="pr-042-inflight",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )

    probe_calls: list[int] = []

    def probe(repo: str) -> list[PRInfo]:
        probe_calls.append(1)
        if len(probe_calls) == 1:
            raise RuntimeError("gh api rate limited")
        return [matching_pr]

    monkeypatch.setattr(runner_module.github_client, "get_open_prs", probe)

    async def noop_ensure() -> None:
        return None

    def clean_preflight() -> bool:
        return True

    runner = _make_runner()
    runner._parse_base_queue = lambda: [task]  # type: ignore[method-assign]
    runner.ensure_repo_cloned = noop_ensure  # type: ignore[method-assign]
    runner.preflight = clean_preflight  # type: ignore[method-assign]

    # First cycle: discovery fails, cycle bails with ERROR.
    asyncio.run(runner.run_cycle())
    assert runner._recovered is False
    assert runner.state.state == PipelineState.ERROR
    assert "rate limited" in (runner.state.error_message or "")
    assert runner.state.current_pr is None

    # Second cycle: discovery succeeds; runner attaches to the in-flight
    # PR and transitions to WATCH. _recovered is now set and later cycles
    # will skip recovery as normal.
    asyncio.run(runner.run_cycle())
    assert runner._recovered is True
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 17
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-042"


def test_run_cycle_coding_failure_during_recovery_is_not_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If recover_state decides CODING and the re-run of handle_coding
    then fails, discovery has nevertheless completed and _recovered must
    be set. Re-running handle_coding on the next cycle would be unsafe
    (it could create a duplicate PR for the same task); the failure
    belongs to the normal ERROR path, not to a second recovery attempt."""
    task = _doing_task()
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: []
    )

    coding_calls: list[int] = []

    async def failing_coding() -> None:
        coding_calls.append(1)
        runner.state.state = PipelineState.ERROR
        runner.state.error_message = "claude CLI crashed"

    runner = _make_runner()
    runner._parse_base_queue = lambda: [task]  # type: ignore[method-assign]
    runner.handle_coding = failing_coding  # type: ignore[method-assign]
    runner._preserve_crashed_run_commits = lambda branch: True  # type: ignore[method-assign]

    result = asyncio.run(runner.recover_state())

    assert result is True  # discovery completed, do not retry recovery
    assert coding_calls == [1]
    assert runner.state.state == PipelineState.ERROR
    assert "claude CLI crashed" in (runner.state.error_message or "")


def test_run_cycle_subsequent_cycle_still_runs_preflight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After the recovery cycle, preflight must resume gating normal cycles:
    skipping it only on the first cycle, not permanently."""

    async def noop_ensure() -> None:
        return None

    async def noop_recover() -> bool:
        return True

    async def noop_idle() -> None:
        return None

    preflight_calls: list[int] = []

    def ok_preflight() -> bool:
        preflight_calls.append(1)
        return True

    runner = _make_runner()
    runner.ensure_repo_cloned = noop_ensure  # type: ignore[method-assign]
    runner.recover_state = noop_recover  # type: ignore[method-assign]
    runner.preflight = ok_preflight  # type: ignore[method-assign]
    runner.handle_idle = noop_idle  # type: ignore[method-assign]

    asyncio.run(runner.run_cycle())  # recovery cycle: skips preflight
    asyncio.run(runner.run_cycle())  # normal cycle: runs preflight
    asyncio.run(runner.run_cycle())  # normal cycle: runs preflight

    assert preflight_calls == [1, 1]


def test_sync_to_main_runs_fetch_checkout_reset_in_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """sync_to_main must run fetch -> checkout -> reset --hard in that order
    against the configured base branch. Reset --hard is how we guarantee
    QUEUE.md reflects origin, not a stray working-tree edit from a crash."""
    calls: list[list[str]] = []

    class _FakeProc:
        def __init__(self) -> None:
            self.stdout = ""
            self.stderr = ""
            self.returncode = 0

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeProc:
        calls.append(cmd)
        return _FakeProc()

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    runner = _make_runner()
    runner.sync_to_main()

    assert calls == [
        ["git", "fetch", "origin", "main"],
        ["git", "checkout", "main"],
        ["git", "reset", "--hard", "origin/main"],
        ["git", "clean", "-fd"],
    ]


def test_parse_base_queue_reads_from_origin_configured_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P1-G regression: _parse_base_queue must read QUEUE.md from
    origin/{repo_config.branch} via git show, NOT from the working tree.
    On a fresh clone ensure_repo_cloned leaves HEAD on the remote's
    default branch (origin/HEAD), which may not be the configured base
    branch. Reading parse_queue off the working tree in that state
    would return the wrong queue snapshot, miss in-flight PRs, and let
    the next cycle re-run PLANNED PR on active work."""
    captured: list[list[str]] = []

    class _FakeProc:
        stdout = (
            "## PR-010: Daemon recovery and error handling\n"
            "- Status: TODO\n"
            "- Branch: pr-010-recovery\n"
            "- Depends on: PR-009\n"
        )
        stderr = ""
        returncode = 0

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeProc:
        captured.append(cmd)
        return _FakeProc()

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    runner = _make_runner()  # configured branch: "main"
    tasks = runner._parse_base_queue()

    assert captured == [
        ["git", "show", "origin/main:tasks/QUEUE.md"],
    ], "must read from origin/{configured branch}, not HEAD or a file path"
    assert tasks is not None
    assert len(tasks) == 1
    assert tasks[0].pr_id == "PR-010"
    assert tasks[0].status == TaskStatus.TODO
    assert tasks[0].branch == "pr-010-recovery"


def test_parse_base_queue_respects_non_default_configured_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same guarantee when repo_config.branch is not 'main' — the git
    show ref must track the configured branch, not a hardcoded default."""
    captured: list[list[str]] = []

    class _FakeProc:
        stdout = ""
        stderr = ""
        returncode = 0

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeProc:
        captured.append(cmd)
        return _FakeProc()

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    runner = PipelineRunner(
        _repo_cfg(branch="release/2026.04"),
        AppConfig(repositories=[], daemon=DaemonConfig()),
        _FakeRedis(),
    )
    runner._parse_base_queue()

    assert captured == [
        ["git", "show", "origin/release/2026.04:tasks/QUEUE.md"],
    ]


def test_parse_base_queue_returns_none_on_git_show_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """git show failing (ref missing, tasks/QUEUE.md absent on base, or
    subprocess timeout) must surface as None so recover_state can
    translate it into a retryable ERROR rather than silently proceeding
    with an empty queue snapshot and losing track of in-flight work."""

    def fake_run(cmd: list[str], **kwargs: Any) -> None:
        raise subprocess.CalledProcessError(
            128, cmd, stderr="fatal: invalid object name 'origin/main'"
        )

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    runner = _make_runner()
    assert runner._parse_base_queue() is None


def test_parse_base_queue_returns_none_on_git_show_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same surface for TimeoutExpired — recover_state must not
    distinguish the two in its retry handling."""

    def fake_run(cmd: list[str], **kwargs: Any) -> None:
        raise subprocess.TimeoutExpired(cmd, 30)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    runner = _make_runner()
    assert runner._parse_base_queue() is None


def test_recover_state_queue_read_failure_sets_error_and_returns_false(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When _parse_base_queue returns None (git show failed), recovery
    must set ERROR and return False so _recovered stays unset and the
    next cycle retries discovery. Proceeding with an empty list would
    let the runner fall through to clean-slate IDLE and pick new work
    even if an in-flight PR still exists on the configured branch."""
    # get_open_prs is irrelevant here — we must bail before reaching it.
    gh_calls: list[str] = []

    def spy_gh(repo: str) -> list[PRInfo]:
        gh_calls.append(repo)
        return []

    monkeypatch.setattr(runner_module.github_client, "get_open_prs", spy_gh)

    runner = _make_runner()
    runner._parse_base_queue = lambda: None  # type: ignore[method-assign]

    result = asyncio.run(runner.recover_state())

    assert result is False
    assert runner.state.state == PipelineState.ERROR
    assert "read QUEUE.md" in (runner.state.error_message or "")
    assert "origin/main" in (runner.state.error_message or "")
    assert gh_calls == [], "must bail before probing GitHub"
