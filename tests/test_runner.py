"""Tests for src/daemon/runner.py."""

from __future__ import annotations

import asyncio
import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
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
        self.store: dict[str, str] = {}
        self.deleted: list[str] = []

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.writes.append((key, value))
        self.store[key] = value

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def delete(self, key: str) -> int:
        self.deleted.append(key)
        if key in self.store:
            del self.store[key]
            return 1
        return 0

    async def eval(self, script: str, numkeys: int, *args: Any) -> int:
        key = args[0]
        expected = args[1]
        current = self.store.get(key)
        if current == expected:
            del self.store[key]
            return 1
        return 0


class _FakeCompletedProcess:
    def __init__(
        self,
        args: list[str] | None = None,
        stdout: str = "",
        stderr: str = "",
        returncode: int = 0,
    ) -> None:
        self.args = args or []
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode


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


def _app_cfg(**daemon_overrides: Any) -> AppConfig:
    return AppConfig(repositories=[], daemon=DaemonConfig(**daemon_overrides))


def _make_runner(**repo_overrides: Any) -> PipelineRunner:
    return PipelineRunner(_repo_cfg(**repo_overrides), _app_cfg(), _FakeRedis())


def _patch_subprocess(
    monkeypatch: pytest.MonkeyPatch,
    stdout: str = "",
    returncode: int = 0,
) -> list[list[str]]:
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(cmd)
        # ``git rev-list --count`` must return an integer or the
        # scaffold/runner sync probes conservatively interpret empty
        # output as "unverifiable, force scaffold retry". Default to
        # "0\n" (synced) so tests that don't exercise the ahead /
        # stranded path stay green; tests that DO need to simulate
        # an ahead state override subprocess.run directly with a
        # hand-rolled fake_run.
        if cmd[:2] == ["git", "rev-list"]:
            return _FakeCompletedProcess(
                args=cmd, stdout="0\n", returncode=0
            )
        return _FakeCompletedProcess(
            args=cmd, stdout=stdout, returncode=returncode
        )

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    return calls


def test_preflight_returns_true_on_clean_repo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch, stdout="")
    runner = _make_runner()

    assert runner.preflight() is True
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None


def test_preflight_returns_false_on_dirty_repo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch, stdout=" M src/foo.py\n?? artifacts/")
    runner = _make_runner()

    assert runner.preflight() is False
    assert runner.state.state == PipelineState.ERROR
    assert "foo.py" in (runner.state.error_message or "")
    assert runner.state.history, "log_event should append an entry"


def test_preflight_sets_error_when_git_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        raise subprocess.CalledProcessError(128, cmd, stderr="not a git repo")

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    runner = _make_runner()

    assert runner.preflight() is False
    assert runner.state.state == PipelineState.ERROR


def test_log_event_caps_history_at_100(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = _make_runner()

    for i in range(150):
        runner.log_event(f"event {i}")

    assert len(runner.state.history) == 100
    assert runner.state.history[0]["event"] == "event 50"
    assert runner.state.history[-1]["event"] == "event 149"


def test_handle_idle_no_tasks_leaves_state_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _patch_subprocess(monkeypatch)
    monkeypatch.setattr(runner_module, "parse_queue", lambda path: [])
    monkeypatch.setattr(runner_module, "get_next_task", lambda tasks: None)

    runner = _make_runner()
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 0
    assert any("No tasks" in e["event"] for e in runner.state.history)
    # sync_to_main must run fetch -> checkout -> reset --hard in order so
    # that parse_queue reads QUEUE.md from the tip of origin/{branch}, not
    # whatever branch/commit the repo was left on by a prior cycle.
    commands = [cmd[:3] for cmd in calls]
    fetch_idx = commands.index(["git", "fetch", "origin"])
    checkout_idx = next(
        i for i, cmd in enumerate(commands) if cmd[:2] == ["git", "checkout"]
    )
    reset_idx = next(
        i for i, cmd in enumerate(commands) if cmd[:2] == ["git", "reset"]
    )
    assert fetch_idx < checkout_idx < reset_idx
    # No git pull anywhere: sync_to_main replaced it with reset --hard.
    assert not any(cmd[:2] == ["git", "pull"] for cmd in calls)
    # ``git reset --hard`` only removes tracked-file edits; untracked
    # files left by a crashed prior cycle would otherwise survive into
    # the next preflight as a dirty tree. ``git clean -fd`` after the
    # reset guarantees the working copy matches origin/{branch}.
    clean_idx = next(
        i for i, cmd in enumerate(commands) if cmd[:2] == ["git", "clean"]
    )
    assert reset_idx < clean_idx
    assert ["git", "clean", "-fd"] in calls


def test_handle_idle_picks_task_and_drives_coding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-042",
        title="Sample",
        status=TaskStatus.TODO,
        branch="pr-042-sample",
    )
    monkeypatch.setattr(runner_module, "parse_queue", lambda path: [task])
    monkeypatch.setattr(runner_module, "get_next_task", lambda tasks: task)

    claude_calls: list[str] = []

    def fake_run_planned_pr(path: str) -> tuple[int, str, str]:
        claude_calls.append(path)
        return (0, "ok", "")

    monkeypatch.setattr(runner_module.claude_cli, "run_planned_pr", fake_run_planned_pr)

    opened_pr = PRInfo(
        number=17,
        branch="pr-042-sample",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo: [opened_pr],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: None,
    )

    runner = _make_runner()
    asyncio.run(runner.handle_idle())

    assert claude_calls == [runner.repo_path]
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 17
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-042"
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 1


def test_handle_idle_sets_queue_counters_with_mixed_statuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    tasks = [
        QueueTask(pr_id="PR-001", title="Done1", status=TaskStatus.DONE, branch="pr-001"),
        QueueTask(pr_id="PR-002", title="Done2", status=TaskStatus.DONE, branch="pr-002"),
        QueueTask(pr_id="PR-003", title="Todo", status=TaskStatus.TODO, branch="pr-003"),
    ]
    monkeypatch.setattr(runner_module, "parse_queue", lambda path: tasks)
    monkeypatch.setattr(runner_module, "get_next_task", lambda t: tasks[2])
    monkeypatch.setattr(
        runner_module.claude_cli, "run_planned_pr", lambda path: (0, "ok", "")
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo: [PRInfo(number=1, branch="pr-003")],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: None,
    )

    runner = _make_runner()
    asyncio.run(runner.handle_idle())

    assert runner.state.queue_done == 2
    assert runner.state.queue_total == 3


def test_handle_coding_errors_when_no_pr_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        runner_module.claude_cli,
        "run_planned_pr",
        lambda path: (0, "ok", ""),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo: [],
    )

    async def instant_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(runner_module.asyncio, "sleep", instant_sleep)

    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001"
    )
    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    assert "no PR found" in (runner.state.error_message or "")


def test_handle_coding_rejects_unmatched_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When no open PR matches current_task.branch, fail fast instead of
    attaching to an unrelated newest open PR."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        runner_module.claude_cli,
        "run_planned_pr",
        lambda path: (0, "ok", ""),
    )
    unrelated = PRInfo(number=99, branch="other-branch")
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo: [unrelated],
    )

    async def instant_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(runner_module.asyncio, "sleep", instant_sleep)

    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001"
    )
    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.current_pr is None
    assert "pr-001" in (runner.state.error_message or "")


def test_handle_coding_posts_codex_review_after_pr_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-019: ``handle_coding`` must explicitly post ``@codex review`` on the
    newly-opened PR so Codex kicks off a review for every iteration instead
    of relying on GitHub-side Automatic Reviews (which we want configured
    for PR creation only, to avoid duplicate reviews)."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        runner_module.claude_cli,
        "run_planned_pr",
        lambda path: (0, "ok", ""),
    )
    opened_pr = PRInfo(number=42, branch="pr-019")
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo: [opened_pr],
    )
    posted: list[tuple[str, int, str]] = []

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    monkeypatch.setattr(runner_module.github_client, "post_comment", fake_post)

    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-019", title="t", status=TaskStatus.DOING, branch="pr-019"
    )
    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 42
    assert posted == [(runner.owner_repo, 42, "@codex review")]
    assert any(
        "Posted @codex review on PR #42" in e["event"]
        for e in runner.state.history
    )


def test_handle_coding_survives_post_comment_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``post_comment`` failure after PR creation must be non-fatal:
    the runner stays in ``WATCH`` and logs a warning. Codex may still
    auto-trigger on push, and a transient ``gh`` hiccup must not flip an
    otherwise healthy pipeline to ``ERROR``."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        runner_module.claude_cli,
        "run_planned_pr",
        lambda path: (0, "ok", ""),
    )
    opened_pr = PRInfo(number=42, branch="pr-019")
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo: [opened_pr],
    )

    def boom(repo: str, number: int, body: str) -> None:
        raise RuntimeError("gh transient failure")

    monkeypatch.setattr(runner_module.github_client, "post_comment", boom)

    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-019", title="t", status=TaskStatus.DOING, branch="pr-019"
    )
    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.error_message is None
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 42
    assert any(
        "Warning: failed to post @codex review" in e["event"]
        and "gh transient failure" in e["event"]
        for e in runner.state.history
    )


def test_handle_fix_posts_codex_review_after_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-019: after a successful fix push, ``handle_fix`` must post
    ``@codex review`` so Codex reviews the freshly-pushed iteration."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        runner_module.claude_cli, "fix_review", lambda path: (0, "", "")
    )
    posted: list[tuple[str, int, str]] = []

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    monkeypatch.setattr(runner_module.github_client, "post_comment", fake_post)

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=77, branch="pr-019")
    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 1
    assert posted == [(runner.owner_repo, 77, "@codex review")]
    assert any(
        "Posted @codex review on PR #77" in e["event"]
        for e in runner.state.history
    )


def test_handle_fix_errors_when_post_comment_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-019 Codex P1: a ``post_comment`` failure after a fix push must
    flip the runner to ``ERROR``.

    The push itself already succeeded, but the PR is still sitting on the
    prior Codex ``CHANGES_REQUESTED`` signal. If we stayed in ``WATCH``
    after failing to re-request a review, the next ``handle_watch`` cycle
    would see ``CHANGES_REQUESTED`` and immediately loop back into
    ``handle_fix``, pushing a new fix every poll interval without ever
    waiting on Codex. Surfacing ``ERROR`` forces operators to resolve the
    gh failure (e.g. by manually posting ``@codex review``) instead of
    trapping the daemon in a silent fix/push loop.
    """
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        runner_module.claude_cli, "fix_review", lambda path: (0, "", "")
    )

    def boom(repo: str, number: int, body: str) -> None:
        raise RuntimeError("gh rate limited")

    monkeypatch.setattr(runner_module.github_client, "post_comment", boom)

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=77, branch="pr-019")
    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 1
    assert "#77" in (runner.state.error_message or "")
    assert "fix/push loop" in (runner.state.error_message or "")
    assert any(
        "Warning: failed to post @codex review" in e["event"]
        and "gh rate limited" in e["event"]
        for e in runner.state.history
    )


def test_handle_fix_skips_checkout_on_cross_repo_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """For fork-based PRs, the daemon's clone only knows about
    ``origin`` (the base repo) — the PR head lives on the contributor's
    fork. ``git checkout`` against the fork branch would fail and trap
    the runner in ERROR for every fork PR, so ``handle_fix`` must skip
    the checkout entirely for cross-repo PRs. Claude owns commit/push
    inside ``fix_review``."""
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(cmd)
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        runner_module.claude_cli, "fix_review", lambda path: (0, "", "")
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: None,
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=88,
        branch="contributor:feature-x",
        is_cross_repository=True,
    )
    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.WATCH
    assert not any(cmd[:2] == ["git", "checkout"] for cmd in calls)


def test_handle_fix_checks_out_pr_branch_before_fix_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``sync_to_main`` leaves the repo on the base branch. Before invoking
    Claude's fix_review, ``handle_fix`` must check out the PR branch so the
    patch lands on the PR's HEAD instead of the base branch.
    """
    calls = _patch_subprocess(monkeypatch)
    fix_called_at: list[int] = []

    def fake_fix(path: str) -> tuple[int, str, str]:
        fix_called_at.append(len(calls))
        return (0, "", "")

    monkeypatch.setattr(runner_module.claude_cli, "fix_review", fake_fix)
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: None,
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    asyncio.run(runner.handle_fix())

    checkout_calls = [
        i for i, cmd in enumerate(calls)
        if cmd[:2] == ["git", "checkout"] and "pr-042-fix" in cmd
    ]
    assert checkout_calls, "expected git checkout pr-042-fix before fix_review"
    assert fix_called_at, "fix_review must have been invoked"
    assert checkout_calls[0] < fix_called_at[0]
    assert runner.state.state == PipelineState.WATCH


def test_handle_fix_errors_when_pr_branch_checkout_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the PR branch checkout before fix_review fails, the runner must
    transition to ERROR rather than letting Claude patch the base branch.
    """
    fix_calls: list[str] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "checkout"] and "pr-042-fix" in cmd:
            raise subprocess.CalledProcessError(
                1, cmd, stderr="error: pathspec 'pr-042-fix' did not match"
            )
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        runner_module.claude_cli,
        "fix_review",
        lambda path: (fix_calls.append(path), (0, "", ""))[1],
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.ERROR
    assert "pr-042-fix" in (runner.state.error_message or "")
    assert fix_calls == [], "fix_review must not run when checkout fails"


def test_handle_fix_errors_when_pr_branch_checkout_times_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex P2: the checkout has ``timeout=30`` so a stalled git lock or
    slow I/O can raise ``TimeoutExpired``. The except clause must catch it
    too (alongside ``OSError``), otherwise the exception escapes the daemon
    loop without setting ``PipelineState.ERROR`` and the state machine
    drifts on the next cycle.
    """
    fix_calls: list[str] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "checkout"] and "pr-042-fix" in cmd:
            raise subprocess.TimeoutExpired(cmd=cmd, timeout=30)
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        runner_module.claude_cli,
        "fix_review",
        lambda path: (fix_calls.append(path), (0, "", ""))[1],
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.ERROR
    assert "pr-042-fix" in (runner.state.error_message or "")
    assert fix_calls == [], "fix_review must not run when checkout times out"


def test_handle_coding_errors_when_task_has_no_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        runner_module.claude_cli,
        "run_planned_pr",
        lambda path: (0, "ok", ""),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo: [PRInfo(number=1, branch="anything")],
    )

    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )  # no branch
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
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        runner_module.claude_cli,
        "run_planned_pr",
        lambda path: (0, "ok", ""),
    )

    opened_pr = PRInfo(number=42, branch="pr-001")
    call_count = {"n": 0}

    def flaky_get_open_prs(repo: str) -> list[PRInfo]:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return []
        return [opened_pr]

    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", flaky_get_open_prs
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: None,
    )

    slept: list[float] = []

    async def instant_sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr(runner_module.asyncio, "sleep", instant_sleep)

    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001"
    )
    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 42
    assert call_count["n"] == 2
    assert slept == [5]
    assert any(
        "PR not found for 'pr-001'" in e["event"] and "1/3" in e["event"]
        for e in runner.state.history
    )


def test_handle_coding_errors_after_all_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After 3 consecutive empty get_open_prs results the runner must
    flip to ERROR so operators see that Claude exited 0 without opening
    a PR."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        runner_module.claude_cli,
        "run_planned_pr",
        lambda path: (0, "ok", ""),
    )
    call_count = {"n": 0}

    def always_empty(repo: str) -> list[PRInfo]:
        call_count["n"] += 1
        return []

    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", always_empty
    )

    slept: list[float] = []

    async def instant_sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr(runner_module.asyncio, "sleep", instant_sleep)

    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001"
    )
    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    assert "no PR found" in (runner.state.error_message or "")
    assert call_count["n"] == 3
    assert slept == [5, 5]


def test_handle_watch_approved_and_green_merges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.APPROVED,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: [pr]
    )

    merged: list[tuple[str, int]] = []

    def fake_merge(repo: str, number: int) -> None:
        merged.append((repo, number))

    monkeypatch.setattr(runner_module.github_client, "merge_pr", fake_merge)

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )
    asyncio.run(runner.handle_watch())

    assert merged == [(runner.owner_repo, 5)]
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None


def test_handle_watch_green_but_auto_merge_disabled_stays_watching(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.APPROVED,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: [pr]
    )

    merged: list[tuple[str, int]] = []
    monkeypatch.setattr(
        runner_module.github_client,
        "merge_pr",
        lambda repo, number: merged.append((repo, number)),
    )

    runner = _make_runner(auto_merge=False)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )
    asyncio.run(runner.handle_watch())

    assert merged == []
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert any(
        "auto_merge disabled" in e["event"] for e in runner.state.history
    )


def test_handle_watch_changes_requested_triggers_fix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.CHANGES_REQUESTED,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: [pr]
    )
    monkeypatch.setattr(
        runner_module.claude_cli, "fix_review", lambda path: (0, "", "")
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: None,
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 1
    assert any("Fix pushed" in e["event"] for e in runner.state.history)


def test_handle_watch_ci_failure_triggers_fix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.FAILURE,
        review_status=ReviewStatus.PENDING,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: [pr]
    )
    monkeypatch.setattr(
        runner_module.claude_cli, "fix_review", lambda path: (0, "", "")
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: None,
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 1


def test_handle_watch_timeout_sets_hung(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stale = datetime.now(timezone.utc) - timedelta(minutes=90)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
        last_activity=stale,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: [pr]
    )

    runner = _make_runner(review_timeout_min=30)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.HUNG


def test_handle_watch_within_timeout_stays_watching(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fresh = datetime.now(timezone.utc) - timedelta(minutes=2)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
        last_activity=fresh,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: [pr]
    )

    runner = _make_runner(review_timeout_min=30)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.WATCH


def test_handle_watch_approved_but_ci_pending_applies_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """APPROVED + CI PENDING used to fall through the branches in handle_watch,
    leaving the runner stuck in WATCH forever. It should now apply the review
    timeout and transition to HUNG when the PR stays pending for too long."""
    stale = datetime.now(timezone.utc) - timedelta(minutes=90)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.APPROVED,
        last_activity=stale,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: [pr]
    )

    runner = _make_runner(review_timeout_min=30)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.HUNG
    assert any(
        "review=APPROVED" in e["event"] and "ci=PENDING" in e["event"]
        for e in runner.state.history
    )


def test_handle_watch_falls_back_to_daemon_review_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When a repo omits ``review_timeout_min``, hung detection must fall
    back to ``daemon.review_timeout_min``.

    Regression for a P2 Codex finding on PR-016: the runner previously
    only consulted ``self.repo_config.review_timeout_min``, so the new
    "Default review timeout" control in the Settings daemon section was
    persisted to ``config.yml`` but ignored at runtime — users thought
    they'd changed hung behavior while the daemon kept using whatever
    per-repo value the config had.
    """
    stale = datetime.now(timezone.utc) - timedelta(minutes=40)
    pr = PRInfo(
        number=7,
        branch="pr-002",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
        last_activity=stale,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: [pr]
    )

    # ``review_timeout_min=None`` on the repo → the runner must use the
    # daemon's 30-minute default. 40 minutes of inactivity is past that,
    # so the PR flips to HUNG.
    repo_cfg = RepoConfig(
        url="https://github.com/octo/demo.git",
        branch="main",
        auto_merge=True,
        review_timeout_min=None,
        poll_interval_sec=60,
    )
    app_cfg = AppConfig(
        repositories=[], daemon=DaemonConfig(review_timeout_min=30)
    )
    runner = PipelineRunner(repo_cfg, app_cfg, _FakeRedis())
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=7, branch="pr-002")
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.HUNG


def test_handle_watch_repo_timeout_override_wins_over_daemon_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit per-repo ``review_timeout_min`` must override the
    daemon-level default.

    Belt-and-suspenders for the P2 fix: raising the daemon default must
    not silently shorten or lengthen the timeout on repos that pinned
    their own value via the existing per-repo Settings control (PR-015).
    """
    stale = datetime.now(timezone.utc) - timedelta(minutes=90)
    pr = PRInfo(
        number=8,
        branch="pr-003",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
        last_activity=stale,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: [pr]
    )

    # repo pins 120 min, daemon default is 30 min. 90 minutes of
    # inactivity is below the repo override, so the PR stays WATCH.
    repo_cfg = RepoConfig(
        url="https://github.com/octo/demo.git",
        branch="main",
        auto_merge=True,
        review_timeout_min=120,
        poll_interval_sec=60,
    )
    app_cfg = AppConfig(
        repositories=[], daemon=DaemonConfig(review_timeout_min=30)
    )
    runner = PipelineRunner(repo_cfg, app_cfg, _FakeRedis())
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=8, branch="pr-003")
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.WATCH


def test_handle_watch_approved_ci_pending_within_timeout_waits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fresh = datetime.now(timezone.utc) - timedelta(minutes=2)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.APPROVED,
        last_activity=fresh,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: [pr]
    )

    runner = _make_runner(review_timeout_min=30)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.WATCH
    assert any(
        "waiting" in e["event"] and "review=APPROVED" in e["event"]
        for e in runner.state.history
    )


def test_handle_watch_pr_closed_returns_to_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: []
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None


def test_handle_hung_posts_codex_review_and_returns_to_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[tuple[str, int, str]] = []

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    monkeypatch.setattr(runner_module.github_client, "post_comment", fake_post)

    runner = _make_runner()
    runner.state.state = PipelineState.HUNG
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_hung())

    assert posted == [(runner.owner_repo, 5, "@codex review")]
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.last_activity is not None


def test_handle_hung_without_fallback_returns_to_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = PipelineRunner(
        _repo_cfg(),
        AppConfig(
            repositories=[],
            daemon=DaemonConfig(hung_fallback_codex_review=False),
        ),
        _FakeRedis(),
    )
    runner.state.state = PipelineState.HUNG
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )
    asyncio.run(runner.handle_hung())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None


def test_handle_merge_success_sets_idle(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        runner_module.github_client, "merge_pr", lambda repo, num: None
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )
    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None


def test_handle_merge_failure_sets_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(repo: str, num: int) -> None:
        raise RuntimeError("merge conflict")

    monkeypatch.setattr(runner_module.github_client, "merge_pr", boom)

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.ERROR
    assert "merge conflict" in (runner.state.error_message or "")


def test_handle_merge_marks_queue_done(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        runner_module.github_client, "merge_pr", lambda repo, num: None
    )

    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    queue_path.write_text(
        "## PR-001: first\n- Status: DOING\n\n"
        "## PR-002: second\n- Status: TODO\n"
    )

    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(cmd)
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="first", status=TaskStatus.DOING
    )
    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.IDLE
    updated = queue_path.read_text()
    assert "## PR-001: first\n- Status: DONE" in updated
    assert "## PR-002: second\n- Status: TODO" in updated
    assert any(cmd[:3] == ["git", "push", "origin"] for cmd in calls)


def test_handle_merge_tolerates_queue_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        runner_module.github_client, "merge_pr", lambda repo, num: None
    )

    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    queue_path.write_text("## PR-001: first\n- Status: DOING\n")

    def fail_push(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:3] == ["git", "push", "origin"]:
            raise subprocess.CalledProcessError(1, cmd, stderr="push failed")
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fail_push)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="first", status=TaskStatus.DOING
    )
    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None


def test_handle_error_skip_clears_state(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        runner_module.claude_cli,
        "diagnose_error",
        lambda path, ctx: (0, "SKIP", ""),
    )

    runner = _make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "boom"
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )
    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None
    assert runner.state.current_task is None


def test_handle_error_escalate_keeps_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        runner_module.claude_cli,
        "diagnose_error",
        lambda path, ctx: (0, "ESCALATE: human help", ""),
    )

    runner = _make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "boom"
    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "boom"


def test_publish_state_writes_to_redis() -> None:
    runner = _make_runner()
    asyncio.run(runner.publish_state())

    assert isinstance(runner.redis, _FakeRedis)
    assert len(runner.redis.writes) == 1
    key, payload = runner.redis.writes[0]
    assert key == f"pipeline:{runner.name}"
    assert runner.name in payload


def test_run_cycle_resets_stale_transient_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch, stdout="")
    monkeypatch.setattr(runner_module, "parse_queue", lambda path: [])
    monkeypatch.setattr(runner_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: []
    )

    runner = _make_runner()
    # _recovered=True skips recover_state so this test exercises the
    # defensive transient-state reset, not the (separately tested)
    # recovery path that would have caught a mid-coding crash first.
    # _scaffolded=True skips the scaffold retry in ensure_repo_cloned
    # so this test focuses on the transient-state reset rather than
    # scaffolding behavior.
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.CODING  # simulate crash mid-coding
    asyncio.run(runner.run_cycle())

    # The stale CODING state was reset and handle_idle ran to completion.
    assert runner.state.state == PipelineState.IDLE
    assert any("stale transient state" in e["event"] for e in runner.state.history)
    assert isinstance(runner.redis, _FakeRedis)
    assert runner.redis.writes, "publish_state should have been called"


def test_ensure_repo_cloned_retries_scaffold_after_transient_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """A transient scaffold failure (e.g. initial push timeout) must
    not be swallowed and must leave ``_scaffolded`` unset so the next
    cycle retries. Once scaffold_repo finally succeeds,
    ``_scaffolded`` flips to True and scaffold_repo is never called
    again. Without this loop, the first-clone push failure strands
    ``origin/{branch}`` without ``tasks/QUEUE.md`` and the runner sits
    in ERROR forever because ``_parse_base_queue`` keeps reading a
    missing file.
    """
    _patch_subprocess(monkeypatch)

    scaffold_calls: list[str] = []
    attempts = {"n": 0}

    def fake_scaffold(path: str, branch: str) -> list[str]:
        attempts["n"] += 1
        scaffold_calls.append(branch)
        if attempts["n"] == 1:
            raise RuntimeError("simulated push timeout")
        return ["AGENTS.md", "tasks/QUEUE.md"]

    monkeypatch.setattr(
        runner_module.scaffolder, "scaffold_repo", fake_scaffold
    )

    runner = _make_runner()
    # Point repo_path at a non-existent directory so ensure_repo_cloned
    # takes the clone branch on every call (clone is mocked to a no-op
    # via _patch_subprocess).
    runner.repo_path = str(tmp_path / "clone-target")

    # Cycle 1: scaffold raises -> RuntimeError out of
    # ensure_repo_cloned (no longer silently swallowed).
    with pytest.raises(RuntimeError, match="scaffold_repo failed"):
        asyncio.run(runner.ensure_repo_cloned())
    assert runner._scaffolded is False
    assert scaffold_calls == ["main"]

    # Cycle 2: scaffold succeeds -> _scaffolded flips True and the
    # created files are logged.
    asyncio.run(runner.ensure_repo_cloned())
    assert runner._scaffolded is True
    assert scaffold_calls == ["main", "main"]
    assert any(
        "scaffold_repo created" in e["event"] for e in runner.state.history
    )

    # Cycle 3: scaffold_repo is NOT called again — _scaffolded gates
    # the entire retry loop.
    asyncio.run(runner.ensure_repo_cloned())
    assert scaffold_calls == ["main", "main"]


def test_ensure_repo_cloned_tolerates_fetch_failure_before_first_scaffold(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """On a previously-cloned but never-successfully-scaffolded repo,
    ``git fetch origin {branch}`` can fail with "couldn't find remote
    ref" because the prior cycle's scaffolding push never landed.
    ``ensure_repo_cloned`` must tolerate that failure and still call
    scaffold_repo, which is idempotent at the remote level and will
    re-push the stranded commit.
    """
    # Make the path exist so ensure_repo_cloned takes the fetch branch.
    existing = tmp_path / "clone-target"
    existing.mkdir()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "fetch"]:
            raise subprocess.CalledProcessError(
                128,
                cmd,
                stderr="fatal: couldn't find remote ref main",
            )
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    scaffold_calls: list[str] = []

    def fake_scaffold(path: str, branch: str) -> list[str]:
        scaffold_calls.append(branch)
        return ["AGENTS.md"]

    monkeypatch.setattr(
        runner_module.scaffolder, "scaffold_repo", fake_scaffold
    )

    runner = _make_runner()
    runner.repo_path = str(existing)
    # Simulate the pre-scaffold state explicitly — _make_runner's
    # default repo_path doesn't exist so __init__ already seeded
    # _scaffolded=False, but we re-assert here for clarity.
    runner._scaffolded = False

    # fetch failure before first scaffold: must NOT raise, must still
    # call scaffold_repo, and must set _scaffolded True on success.
    asyncio.run(runner.ensure_repo_cloned())

    assert scaffold_calls == ["main"]
    assert runner._scaffolded is True
    # The tolerated fetch failure leaves a breadcrumb in history so
    # the operator can see what happened.
    assert any(
        "will retry scaffold" in e["event"]
        for e in runner.state.history
    )


def test_ensure_repo_cloned_raises_non_missing_ref_fetch_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """``git fetch`` failures that are NOT the missing-remote-ref
    case must raise immediately, regardless of ``_scaffolded`` state.
    The earlier tolerance was too broad: an auth/network blip before
    the first scaffold would silently let ``recover_state`` proceed
    with stale local ``origin/{branch}`` data, even though we have
    no way to refresh it on this cycle.
    """
    existing = tmp_path / "clone-target"
    existing.mkdir()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "fetch"]:
            raise subprocess.CalledProcessError(
                128,
                cmd,
                stderr=(
                    "fatal: Authentication failed for "
                    "'https://github.com/octo/demo.git'"
                ),
            )
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    # Assert both code paths raise on non-missing-ref fetch failures:
    # the pre-scaffold state (was previously tolerated too broadly)
    # AND the post-scaffold state.
    for scaffolded in (False, True):
        runner = _make_runner()
        runner.repo_path = str(existing)
        runner._scaffolded = scaffolded
        with pytest.raises(RuntimeError, match="git fetch failed"):
            asyncio.run(runner.ensure_repo_cloned())


def _populate_fully_scaffolded_repo(repo: Any) -> None:
    """Create every file ``_repo_looks_scaffolded`` checks for.

    Tests that assert the fs probe returns True must provide the full
    set: AGENTS.md (or CLAUDE.md), tasks/QUEUE.md, scripts/ci.sh,
    scripts/make-review-artifacts.sh, and a .gitignore that contains
    ``artifacts/``. Partial coverage is intentionally not accepted
    by the probe — see the comment on ``_repo_looks_scaffolded`` for
    why.
    """
    (repo / "AGENTS.md").write_text("# AGENTS\n")
    (repo / "tasks").mkdir()
    (repo / "tasks" / "QUEUE.md").write_text("# Task Queue\n")
    (repo / "scripts").mkdir()
    (repo / "scripts" / "ci.sh").write_text("#!/usr/bin/env bash\n")
    (repo / "scripts" / "make-review-artifacts.sh").write_text(
        "#!/usr/bin/env bash\n"
    )
    (repo / ".gitignore").write_text("artifacts/\n")


def test_ensure_repo_cloned_skips_scaffold_when_repo_already_looks_scaffolded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """On a daemon restart with an existing clone that already has
    the scaffolding files on disk, ``scaffold_repo`` must NOT run.
    Its upfront ``git checkout {branch}`` would clobber a dirty
    working tree left by an interrupted coding cycle, masking the
    real crash-recovery path handled by ``recover_state``. The
    ``_scaffolded`` gate is seeded from ``_repo_looks_scaffolded``
    at ``__init__`` time so it survives process restarts (the
    in-memory flag itself does not).
    """
    existing = tmp_path / "clone-target"
    existing.mkdir()
    _populate_fully_scaffolded_repo(existing)

    # The helper should recognise this directory as already scaffolded.
    assert runner_module._repo_looks_scaffolded(str(existing)) is True

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        # Both local refs/heads/main and refs/remotes/origin/main
        # exist, and rev-list --count reports 0 commits ahead — the
        # repo is fully in sync, so _base_branch_ahead_of_origin
        # returns False and no scaffold retry is triggered.
        if cmd[:2] == ["git", "rev-list"]:
            return _FakeCompletedProcess(
                args=cmd, returncode=0, stdout="0\n"
            )
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    scaffold_calls: list[str] = []

    def fake_scaffold(path: str, branch: str) -> list[str]:
        scaffold_calls.append(branch)
        return []

    monkeypatch.setattr(
        runner_module.scaffolder, "scaffold_repo", fake_scaffold
    )

    runner = _make_runner()
    runner.repo_path = str(existing)
    # Re-seed the gate using the helper, mirroring what __init__ would
    # have done if ``/data/repos/demo`` were this test-local path.
    runner._scaffolded = runner_module._repo_looks_scaffolded(
        str(existing)
    )
    assert runner._scaffolded is True

    asyncio.run(runner.ensure_repo_cloned())

    # scaffold_repo must not have run: the repo already looks
    # scaffolded, so no git checkout runs against the working tree.
    assert scaffold_calls == []
    assert runner._scaffolded is True


def test_ensure_repo_cloned_defers_scaffold_when_working_tree_dirty(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """A restart on a partially-scaffolded repo (``_repo_looks_
    scaffolded`` returns False) that also has a dirty working tree
    from an interrupted coding cycle must NOT call scaffold_repo:
    scaffold_repo starts with ``git checkout {branch}`` which would
    hit "Your local changes would be overwritten" and raise every
    cycle, masking the real crash-recovery path. ``ensure_repo_
    cloned`` must instead defer scaffolding so ``recover_state`` /
    ``preflight`` can run and either clean up the tree or surface
    the real error; a later cycle with a clean tree will retry.
    """
    existing = tmp_path / "clone-target"
    existing.mkdir()
    # Partial scaffolding: only AGENTS.md. Missing tasks/QUEUE.md,
    # scripts/ci.sh, scripts/make-review-artifacts.sh, and the
    # .gitignore entry — so _repo_looks_scaffolded returns False.
    (existing / "AGENTS.md").write_text("# AGENTS\n")
    assert runner_module._repo_looks_scaffolded(str(existing)) is False

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "status"] and "--porcelain" in cmd:
            # Dirty working tree: interrupted coding left a modified
            # file and an untracked file.
            return _FakeCompletedProcess(
                args=cmd,
                stdout=" M src/foo.py\n?? src/bar.py\n",
                returncode=0,
            )
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    scaffold_calls: list[str] = []

    def fake_scaffold(path: str, branch: str) -> list[str]:
        scaffold_calls.append(branch)
        return ["tasks/QUEUE.md"]

    monkeypatch.setattr(
        runner_module.scaffolder, "scaffold_repo", fake_scaffold
    )

    runner = _make_runner()
    runner.repo_path = str(existing)
    runner._scaffolded = False  # partial fs → __init__ would also set False

    # Must NOT raise: the scaffold is deferred, not executed.
    asyncio.run(runner.ensure_repo_cloned())

    # scaffold_repo must not have run — its git checkout would have
    # clobbered the dirty tree.
    assert scaffold_calls == []
    # _scaffolded stays False so the next cycle (with a clean tree)
    # will retry.
    assert runner._scaffolded is False
    # A defer breadcrumb is logged so the operator can see why
    # scaffold_repo did not run.
    assert any(
        "scaffold_repo deferred" in e["event"]
        for e in runner.state.history
    )


def test_repo_looks_scaffolded_rejects_partial_provisioning(
    tmp_path: Any,
) -> None:
    """The fs probe must require **every** asset scaffold_repo would
    commit — not just the three most visible files. A repo that
    pre-existed with ``AGENTS.md`` + ``tasks/QUEUE.md`` +
    ``scripts/ci.sh`` but no ``scripts/make-review-artifacts.sh``
    (or no ``artifacts/`` entry in ``.gitignore``) must NOT be
    classified as scaffolded: the daemon would otherwise skip
    scaffold_repo permanently, leaving those files uncreated, and
    the first ``make-review-artifacts.sh`` run would dirty the
    working tree until ``preflight`` forces ERROR.
    """
    base = tmp_path / "partial"
    base.mkdir()
    (base / "AGENTS.md").write_text("# AGENTS\n")
    (base / "tasks").mkdir()
    (base / "tasks" / "QUEUE.md").write_text("# Task Queue\n")
    (base / "scripts").mkdir()
    (base / "scripts" / "ci.sh").write_text("#!/usr/bin/env bash\n")
    # Missing: scripts/make-review-artifacts.sh and .gitignore.
    assert runner_module._repo_looks_scaffolded(str(base)) is False

    # Add the missing review-artifacts script — still missing .gitignore.
    (base / "scripts" / "make-review-artifacts.sh").write_text(
        "#!/usr/bin/env bash\n"
    )
    assert runner_module._repo_looks_scaffolded(str(base)) is False

    # Add a .gitignore that does NOT mention artifacts/.
    (base / ".gitignore").write_text("node_modules/\n*.pyc\n")
    assert runner_module._repo_looks_scaffolded(str(base)) is False

    # Finally append artifacts/ — the probe should now return True.
    (base / ".gitignore").write_text(
        "node_modules/\n*.pyc\nartifacts/\n"
    )
    assert runner_module._repo_looks_scaffolded(str(base)) is True


def test_ensure_repo_cloned_resets_scaffolded_when_base_branch_ahead(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """Daemon restart on a repo whose local base branch has commits
    not yet on ``origin/{branch}``: the prior cycle committed
    scaffolding locally but the push failed while ``origin/{branch}``
    still existed (so the missing-ref tolerance did NOT trigger).
    The fs check at ``__init__`` seeds ``_scaffolded=True`` but the
    base-branch-ahead probe must reset it so scaffold_repo runs and
    re-pushes the stranded commit. Without this, ``recover_state``
    keeps reading stale data from ``origin/{branch}:tasks/QUEUE.md``
    with no retry path.
    """
    existing = tmp_path / "clone-target"
    existing.mkdir()
    _populate_fully_scaffolded_repo(existing)

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:3] == ["git", "rev-parse", "--verify"]:
            # Both refs/heads/main and refs/remotes/origin/main exist.
            return _FakeCompletedProcess(args=cmd, returncode=0)
        if cmd[:2] == ["git", "rev-list"]:
            # Local base is 1 commit ahead of origin — the stranded
            # scaffolding commit.
            return _FakeCompletedProcess(
                args=cmd, returncode=0, stdout="1\n"
            )
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    scaffold_calls: list[str] = []

    def fake_scaffold(path: str, branch: str) -> list[str]:
        scaffold_calls.append(branch)
        return []

    monkeypatch.setattr(
        runner_module.scaffolder, "scaffold_repo", fake_scaffold
    )

    runner = _make_runner()
    runner.repo_path = str(existing)
    runner._scaffolded = runner_module._repo_looks_scaffolded(
        str(existing)
    )
    assert runner._scaffolded is True

    asyncio.run(runner.ensure_repo_cloned())

    # Despite the fs check seeding True, the base-branch-ahead probe
    # reset the gate and the retry block ran scaffold_repo.
    assert scaffold_calls == ["main"]
    assert runner._scaffolded is True  # set back to True after retry
    # A breadcrumb records why the retry happened.
    assert any(
        "ahead of origin" in e["event"]
        for e in runner.state.history
    )


def test_ensure_repo_cloned_resets_scaffolded_on_probe_timeout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """A ``TimeoutExpired`` on any of the three
    ``_base_branch_ahead_of_origin`` probes must fall back to
    "ahead" so the scaffold retry still runs. Without this, the
    helper would raise a non-``RuntimeError`` out of
    ``ensure_repo_cloned`` and ``run_cycle`` would skip its normal
    ERROR-state/publish path — most visible during transient git
    stalls (lock contention, slow storage).
    """
    existing = tmp_path / "clone-target"
    existing.mkdir()
    _populate_fully_scaffolded_repo(existing)

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "rev-list"]:
            raise subprocess.TimeoutExpired(cmd, kwargs.get("timeout", 0))
        if cmd[:3] == ["git", "rev-parse", "--verify"]:
            return _FakeCompletedProcess(args=cmd, returncode=0)
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    scaffold_calls: list[str] = []

    def fake_scaffold(path: str, branch: str) -> list[str]:
        scaffold_calls.append(branch)
        return []

    monkeypatch.setattr(
        runner_module.scaffolder, "scaffold_repo", fake_scaffold
    )

    runner = _make_runner()
    runner.repo_path = str(existing)
    runner._scaffolded = runner_module._repo_looks_scaffolded(
        str(existing)
    )
    assert runner._scaffolded is True

    # Must NOT raise TimeoutExpired out of ensure_repo_cloned.
    asyncio.run(runner.ensure_repo_cloned())

    # The timeout was interpreted as "ahead" → scaffold retry ran.
    assert scaffold_calls == ["main"]
    assert runner._scaffolded is True


def test_ensure_repo_cloned_preserves_scaffolded_when_base_branch_synced(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """Daemon restart on a fully-synced, fully-scaffolded repo must
    NOT reset ``_scaffolded`` — doing so would re-run scaffold_repo
    on every normal restart and defeat the round-5 P2 fix that
    protected the crash-recovery path. The base-branch-ahead probe
    should report False (synced), and the retry block should be
    skipped.
    """
    existing = tmp_path / "clone-target"
    existing.mkdir()
    _populate_fully_scaffolded_repo(existing)

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:3] == ["git", "rev-parse", "--verify"]:
            return _FakeCompletedProcess(args=cmd, returncode=0)
        if cmd[:2] == ["git", "rev-list"]:
            # 0 commits ahead — fully synced with origin.
            return _FakeCompletedProcess(
                args=cmd, returncode=0, stdout="0\n"
            )
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    scaffold_calls: list[str] = []

    def fake_scaffold(path: str, branch: str) -> list[str]:
        scaffold_calls.append(branch)
        return []

    monkeypatch.setattr(
        runner_module.scaffolder, "scaffold_repo", fake_scaffold
    )

    runner = _make_runner()
    runner.repo_path = str(existing)
    runner._scaffolded = runner_module._repo_looks_scaffolded(
        str(existing)
    )
    assert runner._scaffolded is True

    asyncio.run(runner.ensure_repo_cloned())

    # scaffold_repo not called, gate preserved.
    assert scaffold_calls == []
    assert runner._scaffolded is True


def test_ensure_repo_cloned_retries_scaffold_on_missing_ref_after_restart(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """Even on a restart where the local fs looks scaffolded, if
    ``git fetch`` reports the missing-remote-ref condition the
    scaffold retry must still run so the stranded commit from a
    prior cycle is re-pushed. Without this, a crashed daemon after
    a transient first-push failure would sit in ERROR forever
    because ``_scaffolded`` seeded True at ``__init__`` would
    otherwise skip the retry.
    """
    existing = tmp_path / "clone-target"
    existing.mkdir()
    # Scaffolding files are on disk (prior cycle committed them)...
    _populate_fully_scaffolded_repo(existing)

    # ...but fetch reports the branch is missing upstream (the prior
    # cycle's initial push failed transiently).
    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "fetch"]:
            raise subprocess.CalledProcessError(
                128,
                cmd,
                stderr="fatal: couldn't find remote ref main",
            )
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    scaffold_calls: list[str] = []

    def fake_scaffold(path: str, branch: str) -> list[str]:
        scaffold_calls.append(branch)
        return []

    monkeypatch.setattr(
        runner_module.scaffolder, "scaffold_repo", fake_scaffold
    )

    runner = _make_runner()
    runner.repo_path = str(existing)
    # Simulate the post-__init__ state: fs check passed so
    # _scaffolded is seeded True, but fetch will report missing ref
    # and force the retry.
    runner._scaffolded = runner_module._repo_looks_scaffolded(
        str(existing)
    )
    assert runner._scaffolded is True

    asyncio.run(runner.ensure_repo_cloned())

    # The missing-ref fetch reset the gate and ran scaffold_repo so
    # the stranded commit gets re-pushed.
    assert scaffold_calls == ["main"]
    assert runner._scaffolded is True


# ------------------------------------------------------------------
# PR-022: IDLE open PR visibility
# ------------------------------------------------------------------


def test_handle_idle_no_tasks_but_open_pr_sets_current_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    done_task = QueueTask(
        pr_id="PR-001",
        title="Done",
        status=TaskStatus.DONE,
        branch="pr-001-done",
    )
    monkeypatch.setattr(runner_module, "parse_queue", lambda path: [done_task])
    monkeypatch.setattr(runner_module, "get_next_task", lambda tasks: None)

    open_pr = PRInfo(
        number=42,
        branch="pr-001-done",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.PENDING,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: [open_pr]
    )

    runner = _make_runner()
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 42
    assert any("open PR(s) detected" in e["event"] for e in runner.state.history)


def test_handle_idle_no_tasks_no_open_prs_clears_current_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(runner_module, "parse_queue", lambda path: [])
    monkeypatch.setattr(runner_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: []
    )

    runner = _make_runner()
    # Set a stale current_pr to verify it gets cleared.
    runner.state.current_pr = PRInfo(number=99, branch="old")
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None


def test_handle_idle_no_tasks_does_not_change_state_from_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(runner_module, "parse_queue", lambda path: [])
    monkeypatch.setattr(runner_module, "get_next_task", lambda tasks: None)

    open_pr = PRInfo(number=7, branch="feature-x")
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo: [open_pr]
    )

    runner = _make_runner()
    assert runner.state.state == PipelineState.IDLE
    asyncio.run(runner.handle_idle())

    # State must remain IDLE — observation only.
    assert runner.state.state == PipelineState.IDLE


def test_handle_idle_open_pr_check_survives_github_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(runner_module, "parse_queue", lambda path: [])
    monkeypatch.setattr(runner_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo: (_ for _ in ()).throw(RuntimeError("API down")),
    )

    runner = _make_runner()
    runner.state.current_pr = PRInfo(number=5, branch="stale")
    asyncio.run(runner.handle_idle())

    # Must not crash, state stays IDLE, and stale current_pr is cleared.
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert any("open PR check failed" in e["event"] for e in runner.state.history)


# ------------------------------------------------------------------
# _commit_and_push_dirty: catch Claude CLI runs that exit 0 while
# leaving uncommitted edits in the working tree. Without this safety
# net, the next cycle's preflight flips the runner to ERROR with
# "working tree dirty" and requires operator intervention.
# ------------------------------------------------------------------


def test_commit_and_push_dirty_commits_when_dirty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(cmd)
        if cmd[:2] == ["git", "status"]:
            return _FakeCompletedProcess(
                args=cmd, stdout=" M src/foo.py\n", returncode=0
            )
        if cmd[:3] == ["git", "rev-parse", "--abbrev-ref"]:
            return _FakeCompletedProcess(
                args=cmd, stdout="pr-001\n", returncode=0
            )
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    runner = _make_runner()
    result = runner._commit_and_push_dirty(
        "PR-001: auto-commit after Claude CLI",
        expected_branch="pr-001",
    )

    assert result is True
    assert runner.state.state != PipelineState.ERROR
    commands = [cmd[:3] for cmd in calls]
    assert ["git", "status", "--porcelain"] in commands
    assert ["git", "rev-parse", "--abbrev-ref"] in commands
    assert ["scripts/ci.sh"] in [cmd[:1] for cmd in calls]
    assert ["git", "add", "-A"] in commands
    assert any(cmd[:2] == ["git", "commit"] for cmd in calls)
    # Push must target the explicit branch, not ``HEAD``, so a
    # pre-push hook that re-points HEAD mid-operation cannot divert
    # the push onto the base branch.
    push_cmd = next(cmd for cmd in calls if cmd[:2] == ["git", "push"])
    assert push_cmd == ["git", "push", "origin", "pr-001:pr-001"]
    # Commit message was threaded through.
    commit_cmd = next(cmd for cmd in calls if cmd[:2] == ["git", "commit"])
    assert "PR-001: auto-commit after Claude CLI" in commit_cmd
    assert any(
        "auto-committed and pushed" in e["event"]
        for e in runner.state.history
    )


def test_commit_and_push_dirty_skips_when_clean(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(cmd)
        # Clean working tree.
        return _FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    runner = _make_runner()
    result = runner._commit_and_push_dirty(
        "should not be committed", expected_branch="pr-001"
    )

    assert result is False
    assert runner.state.state != PipelineState.ERROR
    # Only ``git status --porcelain`` ran; no rev-parse, no ci.sh, no push.
    assert calls == [["git", "status", "--porcelain"]]


def test_commit_and_push_dirty_errors_on_ci_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(cmd)
        if cmd[:2] == ["git", "status"]:
            return _FakeCompletedProcess(
                args=cmd, stdout=" M src/foo.py\n", returncode=0
            )
        if cmd[:3] == ["git", "rev-parse", "--abbrev-ref"]:
            return _FakeCompletedProcess(
                args=cmd, stdout="pr-001\n", returncode=0
            )
        if cmd[:1] == ["scripts/ci.sh"]:
            raise subprocess.CalledProcessError(
                1, cmd, stderr="pytest failed"
            )
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    runner = _make_runner()
    result = runner._commit_and_push_dirty(
        "should not push broken code", expected_branch="pr-001"
    )

    assert result is False
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "CI failed on auto-commit"
    # ci.sh failed -> no git add / commit / push.
    assert not any(cmd[:2] == ["git", "add"] for cmd in calls)
    assert not any(cmd[:2] == ["git", "commit"] for cmd in calls)
    assert not any(cmd[:2] == ["git", "push"] for cmd in calls)
    assert any(
        "CI failed on auto-commit" in e["event"]
        for e in runner.state.history
    )


def test_commit_and_push_dirty_errors_when_ci_script_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex P2: ``scripts/ci.sh`` may be missing or non-executable
    in the dirty tree being auto-committed (Claude CLI deleted it,
    permissions got mangled, etc.). ``subprocess.run`` raises
    ``FileNotFoundError`` / ``PermissionError`` — both subclasses of
    ``OSError`` — rather than ``CalledProcessError``. Without the
    ``OSError`` catch, the exception escapes ``_commit_and_push_dirty``
    and bypasses the structured ERROR-state translation the callers
    rely on.
    """
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(cmd)
        if cmd[:2] == ["git", "status"]:
            return _FakeCompletedProcess(
                args=cmd, stdout=" M src/foo.py\n", returncode=0
            )
        if cmd[:3] == ["git", "rev-parse", "--abbrev-ref"]:
            return _FakeCompletedProcess(
                args=cmd, stdout="pr-001\n", returncode=0
            )
        if cmd[:1] == ["scripts/ci.sh"]:
            raise FileNotFoundError(
                2, "No such file or directory", "scripts/ci.sh"
            )
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    runner = _make_runner()
    result = runner._commit_and_push_dirty(
        "should not push", expected_branch="pr-001"
    )

    assert result is False
    assert runner.state.state == PipelineState.ERROR
    assert "ci.sh could not run" in (runner.state.error_message or "")
    # Missing/broken ci.sh -> no git add/commit/push.
    assert not any(cmd[:2] == ["git", "add"] for cmd in calls)
    assert not any(cmd[:2] == ["git", "commit"] for cmd in calls)
    assert not any(cmd[:2] == ["git", "push"] for cmd in calls)


def test_commit_and_push_dirty_gives_ci_script_generous_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex P2 (round 3): ``scripts/ci.sh`` runs the user repo's full
    test suite and 120s is too tight for moderate-sized projects. A
    real suite exceeding 2 minutes would raise ``TimeoutExpired`` and
    flip the runner to ERROR even when the code is valid, defeating
    the purpose of the auto-commit safety net.

    Git probes (``status``, ``rev-parse``, ``add``, ``commit``,
    ``push``) stay at 120s because they should return in milliseconds
    on a healthy repo; only ``scripts/ci.sh`` gets the large budget.
    """
    ci_timeouts: list[float | int | None] = []
    git_timeouts: list[float | int | None] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        timeout = kwargs.get("timeout")
        if cmd[:1] == ["scripts/ci.sh"]:
            ci_timeouts.append(timeout)
        elif cmd[:1] == ["git"]:
            git_timeouts.append(timeout)
        if cmd[:2] == ["git", "status"]:
            return _FakeCompletedProcess(
                args=cmd, stdout=" M src/foo.py\n", returncode=0
            )
        if cmd[:3] == ["git", "rev-parse", "--abbrev-ref"]:
            return _FakeCompletedProcess(
                args=cmd, stdout="pr-001\n", returncode=0
            )
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    runner = _make_runner()
    result = runner._commit_and_push_dirty(
        "auto-commit", expected_branch="pr-001"
    )

    assert result is True
    assert len(ci_timeouts) == 1
    # Realistic CI suites routinely run longer than 2 minutes. Pin to
    # the module constant so a future regression that drops back to
    # 120s fails loudly here.
    assert ci_timeouts[0] == runner_module._CI_SCRIPT_TIMEOUT_SEC
    assert ci_timeouts[0] >= 600, (
        "ci.sh timeout must be generous enough for moderate test suites"
    )
    # Git probes keep their quick 120s cap — they should return in
    # milliseconds, so there is no reason to bump them.
    assert all(t == 120 for t in git_timeouts)


def test_commit_and_push_dirty_refuses_base_branch_as_expected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex P1 (round 4): a malformed queue entry with ``Branch:``
    set to the configured base branch (``main`` in this repo config)
    would otherwise pass every downstream check — HEAD is on ``main``
    after ``sync_to_main``, ``expected_branch`` equals current branch,
    so the method would happily commit + push directly to the base
    branch, bypassing every PR/review gate. The hard guard refuses
    before any git or ci.sh work runs, so no subprocess at all is
    issued when the caller supplied the base branch.
    """
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(cmd)
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    runner = _make_runner()  # default repo branch is "main"
    result = runner._commit_and_push_dirty(
        "should not touch the base branch", expected_branch="main"
    )

    assert result is False
    assert runner.state.state == PipelineState.ERROR
    assert "base branch" in (runner.state.error_message or "")
    assert "'main'" in (runner.state.error_message or "")
    # Guard fires before any subprocess call — no git status, no
    # rev-parse, no ci.sh, no add/commit/push.
    assert calls == []


def test_commit_and_push_dirty_errors_when_head_on_wrong_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex P1 (round 2): ``_commit_and_push_dirty`` must refuse to
    commit or push when HEAD is not on the expected PR branch.

    The dangerous case is a Claude CLI run that exits 0 without
    creating the feature branch: ``handle_idle.sync_to_main`` has just
    hard-synced the working tree to ``main``, so HEAD is still on the
    base branch. Without the guard, ``_commit_and_push_dirty`` would
    commit uncommitted edits directly onto ``main`` and push them
    upstream, bypassing every PR/review gate in the pipeline.
    """
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(cmd)
        if cmd[:2] == ["git", "status"]:
            return _FakeCompletedProcess(
                args=cmd, stdout=" M src/foo.py\n", returncode=0
            )
        if cmd[:3] == ["git", "rev-parse", "--abbrev-ref"]:
            # HEAD still on the base branch — Claude CLI exited 0 but
            # never switched branches.
            return _FakeCompletedProcess(
                args=cmd, stdout="main\n", returncode=0
            )
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    runner = _make_runner()
    result = runner._commit_and_push_dirty(
        "should not push to main", expected_branch="pr-001"
    )

    assert result is False
    assert runner.state.state == PipelineState.ERROR
    assert "'main'" in (runner.state.error_message or "")
    assert "'pr-001'" in (runner.state.error_message or "")
    # Wrong-branch guard fired before ci.sh, add, commit, or push.
    assert not any(cmd[:1] == ["scripts/ci.sh"] for cmd in calls)
    assert not any(cmd[:2] == ["git", "add"] for cmd in calls)
    assert not any(cmd[:2] == ["git", "commit"] for cmd in calls)
    assert not any(cmd[:2] == ["git", "push"] for cmd in calls)


def test_process_pending_uploads_preserves_upload_on_git_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """On transient git failure, Redis key and staging dir must survive for retry."""

    def failing_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "rev-list"]:
            return _FakeCompletedProcess(args=cmd, stdout="0\n", returncode=0)
        if cmd[:2] == ["git", "add"]:
            raise subprocess.CalledProcessError(1, cmd, stderr="git error")
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", failing_run)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)

    staging = tmp_path.parent / "uploads" / runner.name / "abc123"
    staging.mkdir(parents=True)
    (staging / "QUEUE.md").write_text("- PR-001")

    manifest = json.dumps({"files": ["QUEUE.md"], "staging_dir": str(staging)})
    key = f"upload:{runner.name}:pending"
    asyncio.run(runner.redis.set(key, manifest))

    result = asyncio.run(runner.process_pending_uploads())
    assert result is None
    assert asyncio.run(runner.redis.get(key)) == manifest
    assert staging.is_dir()


def test_process_pending_uploads_cas_delete_skips_newer_manifest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """After a successful push, a newer manifest must not be deleted."""
    _patch_subprocess(monkeypatch)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)

    staging = tmp_path.parent / "uploads" / runner.name / "old123"
    staging.mkdir(parents=True, exist_ok=True)
    (staging / "QUEUE.md").write_text("- PR-001")
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir(exist_ok=True)

    old_manifest = json.dumps({"files": ["QUEUE.md"], "staging_dir": str(staging)})
    new_manifest = json.dumps({"files": ["PR-099.md"]})
    key = f"upload:{runner.name}:pending"
    asyncio.run(runner.redis.set(key, old_manifest))

    # Simulate a new upload arriving after the daemon read the old manifest
    original_eval = runner.redis.eval

    async def inject_new_manifest(script: str, numkeys: int, *args: Any) -> int:
        runner.redis.store[key] = new_manifest
        return await original_eval(script, numkeys, *args)

    runner.redis.eval = inject_new_manifest  # type: ignore[assignment]

    result = asyncio.run(runner.process_pending_uploads())
    assert result is None, "newer upload pending must block dispatch"
    assert asyncio.run(runner.redis.get(key)) == new_manifest
    assert staging.is_dir(), "staging dir must survive when CAS delete skips newer manifest"


def test_process_pending_uploads_redis_error_blocks_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Redis read error must return None so handle_idle skips task dispatch."""
    runner = _make_runner()

    async def broken_get(key: str) -> bytes:
        raise ConnectionError("redis gone")

    runner.redis.get = broken_get  # type: ignore[assignment]

    result = asyncio.run(runner.process_pending_uploads())
    assert result is None


def test_handle_coding_saves_stdout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """handle_coding must save CLI stdout to Redis via _save_cli_log."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        runner_module.claude_cli,
        "run_planned_pr",
        lambda path: (0, "hello from claude", ""),
    )
    pr = PRInfo(number=42, branch="pr-001")
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo: [pr],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda *a, **kw: None,
    )

    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001"
    )
    asyncio.run(runner.handle_coding())

    redis_keys = [k for k, _v in runner.redis.writes]
    assert any(k == f"cli_log:{runner.name}:latest" for k in redis_keys)
    stored = runner.redis.store.get(f"cli_log:{runner.name}:latest")
    assert stored == "hello from claude"


def test_handle_fix_saves_stdout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """handle_fix must save CLI stdout to Redis via _save_cli_log."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        runner_module.claude_cli,
        "fix_review",
        lambda path: (0, "fix output here", ""),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda *a, **kw: None,
    )

    runner = _make_runner()
    runner.state.current_pr = PRInfo(number=10, branch="pr-001")
    asyncio.run(runner.handle_fix())

    redis_keys = [k for k, _v in runner.redis.writes]
    assert any(k == f"cli_log:{runner.name}:latest" for k in redis_keys)
    stored = runner.redis.store.get(f"cli_log:{runner.name}:latest")
    assert stored == "fix output here"
