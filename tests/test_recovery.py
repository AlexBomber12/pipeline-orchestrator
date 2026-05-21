"""Tests for PipelineRunner.recover_state and the _recovered one-shot gate."""

from __future__ import annotations

import asyncio
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from src.config import AppConfig, DaemonConfig, RepoConfig
from src.daemon import recovery as recovery_module
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
from src.queue_parser import QueueValidationError
from src.task_status import MergedState


class _FakeRedis:
    """Minimal async Redis double capturing ``set`` calls."""

    def __init__(self) -> None:
        self.writes: list[tuple[str, str]] = []
        self.lists: dict[str, list[str]] = {}

    async def set(self, key: str, value: str) -> None:
        self.writes.append((key, value))

    async def lpush(self, key: str, value: str) -> int:
        bucket = self.lists.setdefault(key, [])
        bucket.insert(0, value)
        return len(bucket)

    async def ltrim(self, key: str, start: int, stop: int) -> None:
        values = self.lists.get(key, [])
        if stop < 0:
            stop = len(values) + stop
        self.lists[key] = values[start : stop + 1]

    async def publish(self, key: str, value: str) -> int:
        return 1


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
    runner = PipelineRunner(
        _repo_cfg(),
        AppConfig(repositories=[], daemon=DaemonConfig()),
        _FakeRedis(),
        _FakeUsageProvider(),
        _FakeUsageProvider(),
    )
    return runner


class _FakeUsageProvider:
    def fetch(self) -> None:
        return None

    @property
    def consecutive_failures(self) -> int:
        return 0


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


def _write_task_file(
    repo_root: Path,
    pr_id: str,
    status: str,
    branch: str,
) -> None:
    tasks_dir = repo_root / "tasks"
    tasks_dir.mkdir(exist_ok=True)
    (tasks_dir / f"{pr_id}.md").write_text(
        f"---\nstatus: {status}\n---\n\n"
        f"# {pr_id}: Recovered from frontmatter\n\n"
        f"Branch: {branch}\n"
        "- Type: feature\n"
        "- Complexity: medium\n"
        "- Depends on: none\n"
        "- Priority: 2\n"
        "- Coder: codex\n",
        encoding="utf-8",
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
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [matching_pr])

    coding_called = False

    async def boom() -> None:  # pragma: no cover - must not fire
        nonlocal coding_called
        coding_called = True

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
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
        "Recovered: DOING task PR-042" in e["event"] and "WATCH PR #17" in e["event"] for e in runner.state.history
    )


def test_recover_rehydrates_quarantine_from_pr_labels(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Startup recovery must rebuild merge quarantine from GitHub labels."""
    task = _doing_task()
    matching_pr = PRInfo(
        number=17,
        branch="pr-042-inflight",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        quarantine_labels={"quarantine:large_diff"},
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [matching_pr])

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]

    asyncio.run(runner.recover_state())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 17
    assert runner.state.quarantined_prs == {17}
    assert any(
        "Rehydrated quarantine for PR(s) #17 from GitHub labels"
        in e["event"]
        for e in runner.state.history
    )


def test_recover_state_sets_queue_counters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """recover_state must populate queue_done and queue_total."""
    done_task = QueueTask(
        pr_id="PR-001",
        title="Done",
        status=TaskStatus.DONE,
        branch="pr-001-done",
    )
    doing_task = _doing_task()
    todo_task = QueueTask(
        pr_id="PR-043",
        title="Todo",
        status=TaskStatus.TODO,
        branch="pr-043-todo",
    )
    matching_pr = PRInfo(
        number=17,
        branch="pr-042-inflight",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [matching_pr])

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: [done_task, doing_task, todo_task]  # type: ignore[method-assign]
    runner.handle_coding = lambda: None  # type: ignore[method-assign]
    asyncio.run(runner.recover_state())

    assert runner.state.queue_done == 1
    assert runner.state.queue_total == 3


def test_recover_state_publish_state_emits_progress_update(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recovery should emit one progress_updated event after state save."""
    done_task = QueueTask(
        pr_id="PR-001",
        title="Done",
        status=TaskStatus.DONE,
        branch="pr-001-done",
    )
    doing_task = _doing_task()
    todo_task = QueueTask(
        pr_id="PR-043",
        title="Todo",
        status=TaskStatus.TODO,
        branch="pr-043-todo",
    )
    matching_pr = PRInfo(
        number=17,
        branch="pr-042-inflight",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )
    published: list[tuple[str, str, dict[str, int], object | None]] = []

    async def _fake_publish_repo_event(
        repo_name: str,
        event_type: str,
        payload: dict[str, int],
        redis_client: object | None = None,
    ) -> None:
        published.append((repo_name, event_type, payload, redis_client))

    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [matching_pr])
    monkeypatch.setattr(runner_module, "publish_repo_event", _fake_publish_repo_event)

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: [done_task, doing_task, todo_task]  # type: ignore[method-assign]
    runner.handle_coding = lambda: None  # type: ignore[method-assign]

    asyncio.run(runner.recover_state())
    asyncio.run(runner.publish_state())
    asyncio.run(runner.publish_state())

    progress_events = [event for event in published if event[1] == "progress_updated"]
    assert progress_events == [
        (
            runner.name,
            "progress_updated",
            {"queue_done": 1, "queue_total": 3},
            runner.redis,
        )
    ]


def test_recover_state_restores_pending_queue_sync_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recovery should restore pending queue-sync metadata before going IDLE."""
    pending_sync = PRInfo(
        number=301,
        branch="queue-done-20260419",
        last_activity=datetime(2026, 4, 19, 12, 0, tzinfo=timezone.utc),
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pending_sync])

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: []  # type: ignore[method-assign]

    result = asyncio.run(runner.recover_state())

    assert result is True
    assert runner.state.pending_queue_sync_branch == "queue-done-20260419"
    assert runner.state.pending_queue_sync_started_at == pending_sync.last_activity
    assert runner.state.state == PipelineState.IDLE
    assert any("Recovered pending queue-sync branch: queue-done-20260419" in e["event"] for e in runner.state.history)


def test_recover_state_pending_queue_sync_uses_now_when_last_activity_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Queue-sync recovery should fall back to current UTC time when needed."""
    pending_sync = PRInfo(number=302, branch="queue-done-20260419")
    frozen_now = datetime(2026, 4, 19, 13, 30, tzinfo=timezone.utc)

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            assert tz is timezone.utc
            return frozen_now

    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pending_sync])
    monkeypatch.setattr(recovery_module, "datetime", _FrozenDateTime)

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: []  # type: ignore[method-assign]

    asyncio.run(runner.recover_state())

    assert runner.state.pending_queue_sync_started_at == frozen_now


def test_recover_doing_task_skipped_when_already_merged_on_origin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stale DOING entry whose PR is already merged on
    ``origin/{branch}`` must NOT trigger a CODING re-run.

    On legacy tracked-QUEUE repos ``_mark_queue_done`` skips its
    in-place rewrite to keep the working tree clean for preflight, so
    ``origin/{branch}:tasks/QUEUE.md`` keeps the just-merged task pinned
    at DOING. Without this guard, ``recover_state`` would treat the
    stale entry as interrupted work and re-enter CODING for an
    already-merged task on every daemon restart.
    """
    task = _doing_task()
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])

    coding_ran: list[bool] = []

    async def fake_coding() -> None:  # pragma: no cover - must not fire
        coding_ran.append(True)

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
    runner.handle_coding = fake_coding  # type: ignore[method-assign]
    runner._is_doing_already_merged = lambda doing: True  # type: ignore[method-assign]
    # Preserve must NOT run for a merged task — the entry is stale, not
    # interrupted work.
    runner._preserve_crashed_run_commits = (  # type: ignore[method-assign]
        lambda branch: pytest.fail("preserve must not run for merged task")
    )

    result = asyncio.run(runner.recover_state())

    assert result is True
    assert coding_ran == []
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert any(
        "ignoring stale DOING entry PR-042" in e["event"] and "already merged on origin/main" in e["event"]
        for e in runner.state.history
    )
    assert not any("re-running CODING" in e["event"] for e in runner.state.history)


def test_recover_doing_task_without_pr_marks_canceled_and_idles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-186: DOING task + no matching PR is a crash signature; recovery
    must mark it ERROR and stay IDLE rather than re-running CODING in a
    loop."""
    task = _doing_task()
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])

    coding_calls: list[str] = []

    async def fake_coding() -> None:  # pragma: no cover - must not fire
        coding_calls.append("coding")

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
    runner.handle_coding = fake_coding  # type: ignore[method-assign]
    runner._preserve_crashed_run_commits = lambda branch: True  # type: ignore[method-assign]
    asyncio.run(runner.recover_state())

    assert coding_calls == []
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert runner._crashed_task_pr_ids == {"PR-042"}
    assert any(
        e["event"].startswith("[INFRA] Task PR-042 crashed, marking ERROR. Manually re-upload to retry.")
        for e in runner.state.history
    )


def test_recover_clears_stale_expected_branch_marker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """PR-272 follow-up: a SIGKILL/OOM during CODING bypasses the
    ``finally`` cleanup in ``handle_coding`` and leaves
    ``.git/info/expected-branch`` on disk. Recovery must clear the
    marker so the next IDLE cycle's ``process_pending_uploads`` push
    is not rejected by the pre-push hook (HEAD on base != stale
    expected branch). Without this the daemon stalls in IDLE
    indefinitely after every crash recovery against a managed repo
    that has the hook installed."""
    info_dir = tmp_path / ".git" / "info"
    info_dir.mkdir(parents=True)
    marker = info_dir / "expected-branch"
    marker.write_text("pr-old-task-from-killed-cycle\n", encoding="utf-8")

    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner._parse_tasks_from_headers = lambda: []  # type: ignore[method-assign]

    asyncio.run(runner.recover_state())

    assert not marker.exists(), (
        "stale expected-branch marker survived recovery; pre-push hook "
        "would reject the next daemon-driven upload push"
    )


def test_recover_seeds_crashed_set_from_canceled_queue_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-186 Codex P1: After a daemon restart that follows an IDLE cycle
    which already wrote ERROR to QUEUE.md, ``recover_state`` no longer
    sees a DOING entry but must still know the task was crashed. Otherwise
    the next ``_select_next_task_from_dag`` would recompute the task as
    TODO and dispatch it again, defeating the manual-re-upload contract.
    The fix re-seeds ``_crashed_task_pr_ids`` from any ERROR queue
    entry so the cancellation persists across restarts until the user
    re-uploads."""
    error = QueueTask(
        pr_id="PR-042",
        title="Crashed earlier",
        status=TaskStatus.ERROR,
        branch="pr-042-inflight",
    )
    todo = _todo_task()
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: [error, todo]  # type: ignore[method-assign]
    asyncio.run(runner.recover_state())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    # The crashed-task set must include the ERROR entry so the next
    # IDLE cycle's selector overrides its derived status to ERROR
    # rather than re-dispatching it as TODO.
    assert runner._crashed_task_pr_ids == {"PR-042"}
    # The TODO task is unaffected by the rehydrate.
    assert "PR-043" not in runner._crashed_task_pr_ids


def test_recovery_marks_error_tasks_in_crashed_set(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_task_file(tmp_path, "PR-100", "ERROR", "pr-100-error")
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])
    monkeypatch.setattr(
        recovery_module,
        "_resolve_merged_state",
        lambda *args, **kwargs: MergedState(set(), set(), True),
    )

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    asyncio.run(runner.recover_state())

    assert runner._crashed_task_pr_ids == {"PR-100"}
    assert runner.state.current_queue is not None
    assert runner.state.current_queue[0].status == TaskStatus.ERROR


def test_recovery_does_not_mark_todo_or_done(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_task_file(tmp_path, "PR-100", "TODO", "pr-100-todo")
    _write_task_file(tmp_path, "PR-101", "DONE", "pr-101-done")
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])
    monkeypatch.setattr(
        recovery_module,
        "_resolve_merged_state",
        lambda *args, **kwargs: MergedState(set(), set(), True),
    )

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    asyncio.run(runner.recover_state())

    assert runner._crashed_task_pr_ids == set()


def test_recovery_after_redis_flush_excludes_error_tasks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_task_file(tmp_path, "PR-100", "ERROR", "pr-100-error")
    _write_task_file(tmp_path, "PR-101", "TODO", "pr-101-todo")
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])
    monkeypatch.setattr(
        recovery_module,
        "_resolve_merged_state",
        lambda *args, **kwargs: MergedState(set(), set(), True),
    )

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    asyncio.run(runner.recover_state())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner._crashed_task_pr_ids == {"PR-100"}
    statuses = {task.pr_id: task.status for task in runner.state.current_queue or []}
    assert statuses == {"PR-100": TaskStatus.ERROR, "PR-101": TaskStatus.TODO}


def test_recover_paused_doing_task_without_pr_defers_coding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Paused recovery should discover the DOING task without restarting CODING."""
    task = _doing_task()
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])

    coding_calls: list[str] = []

    async def fake_coding() -> None:
        coding_calls.append("coding")

    preserved: list[str] = []

    def fake_preserve(branch: str) -> bool:
        preserved.append(branch)
        return True

    runner = _make_runner()
    runner.state.user_paused = True
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
    runner.handle_coding = fake_coding  # type: ignore[method-assign]
    runner._preserve_crashed_run_commits = fake_preserve  # type: ignore[method-assign]

    result = asyncio.run(runner.recover_state())

    assert result is True
    assert coding_calls == []
    assert preserved == ["pr-042-inflight"]
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-042"
    assert runner.state.current_pr is None
    assert any(
        "[INFRA] Recovered: DOING task PR-042, no PR but user_paused -> defer CODING until resume." == e["event"]
        for e in runner.state.history
    )


def test_recover_paused_doing_task_without_pr_errors_when_preserve_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Paused recovery must still refuse to defer if crashed-run commits are unsafe."""
    task = _doing_task()
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])

    runner = _make_runner()
    runner.state.user_paused = True
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
    runner._preserve_crashed_run_commits = lambda branch: False  # type: ignore[method-assign]

    result = asyncio.run(runner.recover_state())

    assert result is True
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == (
        "recover_state: could not preserve crashed-run commits on "
        "'pr-042-inflight'; refusing to defer CODING while paused"
    )


def test_recover_preserves_crashed_run_commits_before_canceling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-186: Even when recovery now marks the crashed task ERROR
    instead of re-running CODING, any unpushed local commits from the
    crashed run must still be preserved on origin first so the work is
    not lost when the user re-uploads to retry."""
    task = _doing_task()
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])

    events: list[str] = []

    async def fake_coding() -> None:  # pragma: no cover - must not fire
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
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
    runner.handle_coding = fake_coding  # type: ignore[method-assign]
    asyncio.run(runner.recover_state())

    # Preserve push must happen before recovery transitions to IDLE so
    # the work is durable on origin even when the task is ERROR.
    assert "coding" not in events
    assert "push:pr-042-inflight:pr-042-inflight" in events
    assert any("Preserved crashed-run commits on pr-042-inflight" in e["event"] for e in runner.state.history)
    assert runner.state.state == PipelineState.IDLE
    assert "PR-042" in runner._crashed_task_pr_ids


def test_recover_preserve_tolerates_missing_local_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the crashed run never created the local branch (crash before
    Claude's first commit), ``_preserve_crashed_run_commits`` must be a
    no-op rather than failing recovery."""
    task = _doing_task()
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])

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
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
    runner.handle_coding = fake_coding  # type: ignore[method-assign]
    asyncio.run(runner.recover_state())

    # No push when local branch doesn't exist.
    assert pushes == []
    # PR-186: Even with no commits to preserve, the crashed task is
    # still marked ERROR and recovery returns IDLE rather than
    # leaving the DOING task attached to be re-picked.
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner._crashed_task_pr_ids == {"PR-042"}


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
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])

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
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
    runner.handle_coding = fake_coding  # type: ignore[method-assign]
    asyncio.run(runner.recover_state())

    # No push to the base branch, and we never even probed for the
    # local ref — the guard must short-circuit before any subprocess.
    assert pushes == []
    assert probes == []
    assert coding_ran == [], "handle_coding must not run after refusal"
    assert runner.state.state == PipelineState.ERROR
    assert "could not preserve crashed-run commits on 'main'" in (runner.state.error_message or "")
    assert any(
        "Refusing to preserve crashed-run commits on base branch 'main'" in e["event"] for e in runner.state.history
    )


def test_recover_aborts_when_preserve_push_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex P1: a failed preserve push leaves crashed-run commits only
    on local. ``recover_state`` must NOT proceed to handle_coding (which
    would let Claude reset the branch from origin/main and orphan the
    work). Stop in ERROR so an operator can intervene."""
    task = _doing_task()
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])

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
            raise subprocess.CalledProcessError(1, cmd, stderr="auth transient")

        class R:
            returncode = 0
            stdout = ""
            stderr = ""

        return R()

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
    runner.handle_coding = fake_coding  # type: ignore[method-assign]
    asyncio.run(runner.recover_state())

    assert coding_ran == [], "handle_coding must not run after preserve fail"
    assert runner.state.state == PipelineState.ERROR
    assert "could not preserve crashed-run commits on 'pr-042-inflight'" in (runner.state.error_message or "")
    assert any("Failed to preserve unpushed commits on pr-042-inflight" in e["event"] for e in runner.state.history)


@pytest.mark.parametrize(
    ("exc", "message_fragment"),
    [
        (subprocess.TimeoutExpired(["git"], 10), "Command '['git']' timed out"),
        (OSError("probe unavailable"), "probe unavailable"),
    ],
)
def test_recover_aborts_when_branch_probe_fails(
    monkeypatch: pytest.MonkeyPatch,
    exc: Exception,
    message_fragment: str,
) -> None:
    """A failed local-branch probe must stop recovery before CODING reruns."""
    task = _doing_task()
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])

    coding_ran: list[bool] = []

    async def fake_coding() -> None:
        coding_ran.append(True)

    class _Result:
        def __init__(self, returncode: int = 0) -> None:
            self.returncode = returncode
            self.stdout = ""
            self.stderr = ""

    def fake_run(cmd: list[str], **kwargs: Any) -> Any:
        if cmd[:4] == ["git", "rev-parse", "--verify", "--quiet"]:
            raise exc
        # ``_origin_queue_md_tracked`` probes ``git cat-file -e`` to
        # decide whether to read the queue from origin. Default to
        # untracked (returncode != 0) so this test stays focused on
        # the local-branch probe failure under test.
        if cmd[:3] == ["git", "cat-file", "-e"]:
            return _Result(returncode=1)
        # ``_is_doing_already_merged`` probes ``git log origin/{branch}``
        # for a matching merge subject. Report no merge so the test
        # stays focused on the local-branch probe failure under test.
        if cmd[:2] == ["git", "-C"] and len(cmd) > 3 and cmd[3] == "log":
            return _Result(returncode=0)
        # PR-272 follow-up: recovery clears the ``info/expected-branch``
        # marker on startup so a SIGKILL'd CODING dispatch does not
        # leave a stale marker that blocks the next IDLE upload push.
        # Report a non-existent path so ``_cleanup_expected_branch``
        # short-circuits via ``unlink(missing_ok=True)``.
        if cmd[:3] == ["git", "rev-parse", "--git-path"]:
            return _Result(returncode=1)
        raise AssertionError(f"unexpected subprocess call: {cmd}")

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
    runner.handle_coding = fake_coding  # type: ignore[method-assign]

    asyncio.run(runner.recover_state())

    assert coding_ran == []
    assert runner.state.state == PipelineState.ERROR
    assert "could not preserve crashed-run commits on 'pr-042-inflight'" in (runner.state.error_message or "")
    assert any(
        "Could not probe local branch pr-042-inflight" in e["event"] and message_fragment in e["event"]
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
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [done_pr])

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: [done, todo]  # type: ignore[method-assign]
    asyncio.run(runner.recover_state())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 88
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-041"
    assert any(
        "Recovered: DONE task PR-041" in e["event"] and "WATCH PR #88" in e["event"] for e in runner.state.history
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
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [in_flight])

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: [todo]  # type: ignore[method-assign]
    result = asyncio.run(runner.recover_state())

    assert result is True
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 17
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-010"
    assert runner.state.current_task.status == TaskStatus.TODO
    assert any(
        "Recovered: TODO task PR-010" in e["event"] and "WATCH PR #17" in e["event"] for e in runner.state.history
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
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [unrelated])

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: [queued_todo]  # type: ignore[method-assign]
    asyncio.run(runner.recover_state())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None
    assert any("not matched to any" in e["event"] for e in runner.state.history)


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
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [unrelated_first, matching, unrelated_last],
    )

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: [done]  # type: ignore[method-assign]
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
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: [_todo_task()]  # type: ignore[method-assign]
    asyncio.run(runner.recover_state())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    # PR-186: A clean-slate IDLE shutdown must not mark anything ERROR.
    # The crashed-set is only populated when recovery detects a DOING task
    # without a matching PR (the crash signature).
    assert runner._crashed_task_pr_ids == set()
    assert any("no DOING tasks, no open PRs" in e["event"] for e in runner.state.history)
    assert not any("crashed, marking ERROR" in e["event"] for e in runner.state.history)


def test_recover_crashed_preflight_task_marks_canceled_and_idles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-186: A task that crashed during PREFLIGHT (preflight check
    failed mid-task and the daemon restarted) leaves the same crash
    signature as a CODING crash: a DOING task in QUEUE.md with no
    matching PR. Recovery must mark it ERROR and stay IDLE rather
    than re-running the failing task. The crashed-task set must persist
    so the next IDLE cycle's selector skips it instead of dispatching
    another doomed run."""
    task = _doing_task()
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])

    coding_calls: list[str] = []

    async def fake_coding() -> None:  # pragma: no cover - must not fire
        coding_calls.append("coding")

    runner = _make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "preflight failed: working tree dirty"
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
    runner.handle_coding = fake_coding  # type: ignore[method-assign]
    runner._preserve_crashed_run_commits = lambda branch: True  # type: ignore[method-assign]

    asyncio.run(runner.recover_state())

    assert coding_calls == []
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner._crashed_task_pr_ids == {"PR-042"}
    # Codex P2: a prior ERROR transition (preflight failure) leaves
    # ``error_message`` populated. Recovery's ERROR+IDLE landing must
    # clear it so the dashboard does not surface a stale failure banner
    # against a quiesced repo.
    assert runner.state.error_message is None
    assert any(
        e["event"].startswith("[INFRA] Task PR-042 crashed, marking ERROR. Manually re-upload to retry.")
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
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: []  # type: ignore[method-assign]
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "recover_state: get_open_prs failed: gh api rate limited"

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
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [unrelated])

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: []  # type: ignore[method-assign]
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

    def boom(repo: str, **kw: Any) -> list[PRInfo]:
        raise RuntimeError("gh auth token expired")

    monkeypatch.setattr("src.github.prs.get_open_prs", boom)

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: []  # type: ignore[method-assign]
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

    async def clean_preflight() -> bool:
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
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [recovered_pr])

    async def noop_ensure() -> None:
        return None

    watch_calls: list[int] = []

    async def spy_watch() -> None:
        watch_calls.append(1)

    fix_calls: list[int] = []

    async def spy_fix() -> None:
        fix_calls.append(1)

    preflight_calls: list[int] = []

    async def spy_preflight() -> bool:
        preflight_calls.append(1)
        return True

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
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
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])

    async def noop_ensure() -> None:
        return None

    idle_calls: list[int] = []

    async def spy_idle() -> None:
        idle_calls.append(1)

    preflight_calls: list[int] = []

    async def spy_preflight() -> bool:
        preflight_calls.append(1)
        return True

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: []  # type: ignore[method-assign]
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
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [matching_pr])

    async def noop_ensure() -> None:
        return None

    async def noop_watch() -> None:
        return None

    preflight_calls: list[bool] = []

    async def fail_preflight() -> bool:
        # If preflight ever runs on the recovery cycle it will report a
        # dirty tree and flip state to ERROR — exactly the P1 regression.
        preflight_calls.append(True)
        runner.state.state = PipelineState.ERROR
        runner.state.error_message = "working tree dirty: leftover.py"
        return False

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
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

    def probe(repo: str, **kw: Any) -> list[PRInfo]:
        probe_calls.append(1)
        if len(probe_calls) == 1:
            raise RuntimeError("gh api rate limited")
        return [matching_pr]

    monkeypatch.setattr("src.github.prs.get_open_prs", probe)

    async def noop_ensure() -> None:
        return None

    async def clean_preflight() -> bool:
        return True

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
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


def test_run_cycle_recovery_never_invokes_handle_coding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-186 (was: coding-failure-during-recovery): recovery no longer
    re-runs CODING on a crashed DOING task. Discovery still completes
    (``_recovered`` is set), but the crashed task is marked ERROR and
    handle_coding is never called from recover_state, so a CODING crash
    cannot loop on the same task across recovery cycles."""
    task = _doing_task()
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])

    coding_calls: list[int] = []

    async def failing_coding() -> None:  # pragma: no cover - must not fire
        coding_calls.append(1)

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
    runner.handle_coding = failing_coding  # type: ignore[method-assign]
    runner._preserve_crashed_run_commits = lambda branch: True  # type: ignore[method-assign]

    result = asyncio.run(runner.recover_state())

    assert result is True
    assert coding_calls == []
    assert runner.state.state == PipelineState.IDLE
    assert "PR-042" in runner._crashed_task_pr_ids


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

    async def ok_preflight() -> bool:
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
        ["git", "fetch", "--prune", "origin", "main"],
        ["git", "checkout", "main"],
        ["git", "reset", "--hard", "origin/main"],
        ["git", "clean", "-fd"],
    ]


def test_recover_state_local_queue_missing_falls_back_to_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Post-PR-181: a missing local ``tasks/QUEUE.md`` is a normal
    intermediate state (gitignored, regenerated by IDLE). Recovery must
    NOT fail-closed here — returning False would deadlock the runner on
    a dirty worktree, where ``ensure_repo_cloned`` defers scaffolding
    and ``run_cycle`` exits before ``preflight`` can auto-reset. Instead
    recovery completes with an empty queue + IDLE so the next cycle
    runs preflight and self-heals."""
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])

    runner = _make_runner()
    runner._parse_tasks_from_headers = lambda: None  # type: ignore[method-assign]

    result = asyncio.run(runner.recover_state())

    assert result is True
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert any("no tasks/PR-*.md headers parsed" in e["event"] for e in runner.state.history)


def test_recover_validation_error_returns_false(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """QueueValidationError returns False so _recovered stays unset and
    the daemon retries recovery each cycle — allowing self-heal once
    the operator fixes the queue.  run_cycle processes pending uploads
    even on a failed recovery so the upload path is not blocked."""

    def bad_queue() -> None:
        raise QueueValidationError(["duplicate pr_id: PR-001"])

    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])

    runner = _make_runner()
    runner._parse_tasks_from_headers = bad_queue  # type: ignore[method-assign]
    result = asyncio.run(runner.recover_state())

    assert result is False
    assert runner.state.state == PipelineState.ERROR
    assert "queue validation failed" in (runner.state.error_message or "")
    assert "duplicate pr_id" in (runner.state.error_message or "")


def test_rehydrate_last_push_at_ignores_metadata_failure_for_new_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Metadata fetch failures should still bind the PR number without a timestamp."""
    pr = PRInfo(number=17, branch="pr-042-inflight")

    def boom(repo: str, number: int) -> dict[str, str]:
        raise RuntimeError("gh unavailable")

    monkeypatch.setattr("src.github.prs.get_pr_metadata", boom)

    runner = _make_runner()
    runner._last_push_at = datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc)
    runner._last_push_at_pr_number = 3

    runner._rehydrate_last_push_at(pr)

    assert runner._last_push_at is None
    assert runner._last_push_at_pr_number == 17


def test_rehydrate_last_push_at_adds_utc_to_naive_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Naive commit timestamps from metadata should be normalized to UTC."""
    pr = PRInfo(number=17, branch="pr-042-inflight")
    parsed = datetime(2026, 4, 19, 10, 15)

    monkeypatch.setattr(
        "src.github.prs.get_pr_metadata",
        lambda repo, number: {"head_commit_date": "2026-04-19T10:15:00"},
    )
    monkeypatch.setattr("src.github.gh_runner._parse_iso", lambda iso: parsed)

    runner = _make_runner()

    runner._rehydrate_last_push_at(pr)

    assert runner._last_push_at == parsed.replace(tzinfo=timezone.utc)
    assert runner._last_push_at_pr_number == 17


def test_rehydrate_last_push_at_keeps_existing_timestamp_when_metadata_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """For the same PR, a missing commit timestamp must not clear a known baseline."""
    pr = PRInfo(number=17, branch="pr-042-inflight")
    existing = datetime(2026, 4, 19, 9, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(
        "src.github.prs.get_pr_metadata",
        lambda repo, number: {"head_commit_date": ""},
    )

    runner = _make_runner()
    runner._last_push_at = existing
    runner._last_push_at_pr_number = 17

    runner._rehydrate_last_push_at(pr)

    assert runner._last_push_at == existing
    assert runner._last_push_at_pr_number == 17


def test_rehydrate_last_push_at_updates_only_when_newer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """For the same PR, only newer commit timestamps should advance the baseline."""
    pr = PRInfo(number=17, branch="pr-042-inflight")
    older = datetime(2026, 4, 19, 8, 0, tzinfo=timezone.utc)
    newer = datetime(2026, 4, 19, 11, 0, tzinfo=timezone.utc)

    runner = _make_runner()
    runner._last_push_at_pr_number = 17
    runner._last_push_at = newer

    monkeypatch.setattr(
        "src.github.prs.get_pr_metadata",
        lambda repo, number: {"head_commit_date": "2026-04-19T08:00:00+00:00"},
    )
    monkeypatch.setattr("src.github.gh_runner._parse_iso", lambda iso: older)
    runner._rehydrate_last_push_at(pr)
    assert runner._last_push_at == newer

    monkeypatch.setattr(
        "src.github.prs.get_pr_metadata",
        lambda repo, number: {"head_commit_date": "2026-04-19T12:00:00+00:00"},
    )
    monkeypatch.setattr("src.github.gh_runner._parse_iso", lambda iso: newer)
    runner._last_push_at = older
    runner._rehydrate_last_push_at(pr)
    assert runner._last_push_at == newer
