"""Tests for src/daemon/runner.py."""

from __future__ import annotations

import asyncio
import json
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from src.coders import claude as claude_plugin_module
from src.config import AppConfig, DaemonConfig, RepoConfig
from src.daemon import git_ops as git_ops_module
from src.daemon import runner as runner_module
from src.daemon.handlers import breach as breach_module
from src.daemon.handlers import idle as idle_module
from src.daemon.runner import ErrorCategory, PipelineRunner, _classify_error
from src.models import (
    CIStatus,
    FeedbackCheckResult,
    PipelineState,
    PRInfo,
    QueueTask,
    ReviewStatus,
    TaskStatus,
)

claude_cli = claude_plugin_module.claude_cli
_ORIGINAL_SELECT_NEXT_TASK_FROM_DAG = idle_module.IdleMixin._select_next_task_from_dag


def _async_cli_result(*result: object):
    async def _fn(*args: object, **kwargs: object) -> tuple:
        return result
    return _fn


def _async_cli_result_with_side_effect(
    collector: list, label: str, *result: object
):
    async def _fn(*args: object, **kwargs: object) -> tuple:
        collector.append(label)
        return result
    return _fn


def _async_cli_capture_path(collector: list, *result: object):
    async def _fn(path: str, *args: object, **kwargs: object) -> tuple:
        collector.append(path)
        return result
    return _fn


@pytest.fixture(autouse=True)
def _disable_dag_selection_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _no_dag(self) -> None:
        self._idle_dag_tasks = None
        return None

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        _no_dag,
    )


class _FakeRedis:
    """Minimal async Redis double capturing ``set`` calls."""

    def __init__(self) -> None:
        self.writes: list[tuple[str, str]] = []
        self.store: dict[str, str] = {}
        self.deleted: list[str] = []
        self.lists: dict[str, list[str]] = {}

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

    async def lpush(self, key: str, value: str) -> int:
        bucket = self.lists.setdefault(key, [])
        bucket.insert(0, value)
        return len(bucket)

    async def lrem(self, key: str, count: int, value: str) -> int:
        values = self.lists.setdefault(key, [])
        if count != 0:
            raise NotImplementedError("test fake only supports removing all matches")
        kept = [item for item in values if item != value]
        removed = len(values) - len(kept)
        self.lists[key] = kept
        return removed

    async def lrange(self, key: str, start: int, stop: int) -> list[str]:
        values = self.lists.get(key, [])
        if stop < 0:
            stop = len(values) + stop
        return values[start:stop + 1]

    async def ltrim(self, key: str, start: int, stop: int) -> None:
        values = self.lists.get(key, [])
        if stop < 0:
            stop = len(values) + stop
        self.lists[key] = values[start:stop + 1]


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


class _FakeUsageProvider:
    """Minimal stub for OAuthUsageProvider used by _make_runner and tests."""

    def __init__(
        self,
        snapshot: object | None = None,
        failures: int = 0,
    ) -> None:
        self._snapshot = snapshot
        self._consecutive_failures = failures
        self._invalidated = False

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    def fetch(self) -> object | None:
        return self._snapshot

    def invalidate_cache(self) -> None:
        self._invalidated = True


def _app_cfg(**daemon_overrides: Any) -> AppConfig:
    return AppConfig(repositories=[], daemon=DaemonConfig(**daemon_overrides))


def _usage_providers() -> tuple[_FakeUsageProvider, _FakeUsageProvider]:
    return _FakeUsageProvider(), _FakeUsageProvider()


def _make_runner(**repo_overrides: Any) -> PipelineRunner:
    claude_provider, codex_provider = _usage_providers()
    return PipelineRunner(
        _repo_cfg(**repo_overrides),
        _app_cfg(),
        _FakeRedis(),
        claude_provider,
        codex_provider,
    )


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
        # ``git merge origin/<ref>`` defaults to the up-to-date no-op
        # so handle_merge proceeds straight to ``gh pr merge``. Tests
        # that exercise the sync-push / conflict paths install their
        # own fake_run to override this.
        if (
            cmd[:2] == ["git", "merge"]
            and len(cmd) > 2
            and cmd[2].startswith("origin/")
        ):
            return _FakeCompletedProcess(
                args=cmd, stdout="Already up to date.\n", returncode=0
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


def test_preflight_handles_oserror(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing git binary or cwd raises ``OSError`` from subprocess.run.
    Without catching it, the exception escapes to daemon.main's generic
    handler and the runner state stays stale; preflight must translate
    it into ERROR state.
    """
    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        raise FileNotFoundError("git: not found")

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    runner = _make_runner()

    assert runner.preflight() is False
    assert runner.state.state == PipelineState.ERROR
    assert "preflight failed" in (runner.state.error_message or "")


def test_sync_to_main_handles_oserror(monkeypatch: pytest.MonkeyPatch) -> None:
    """``sync_to_main`` translates ``OSError`` to ``RuntimeError`` so
    the caller's structured error-state translation covers missing git
    binary / cwd instead of letting the exception escape unhandled."""
    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        raise FileNotFoundError("git: not found")

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    runner = _make_runner()

    with pytest.raises(RuntimeError, match="sync_to_main OS error"):
        runner.sync_to_main()


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
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr(
        runner_module.github_client, "get_merged_prs", lambda repo, branch, refresh=False: []
    )

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
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: task)

    claude_calls: list[str] = []

    async def fake_run_planned_pr(
        path: str, model: str | None = None, timeout: int | None = None, **kwargs: object
    ) -> tuple[int, str, str]:
        claude_calls.append(path)
        return (0, "ok", "")

    monkeypatch.setattr(claude_cli, "run_planned_pr_async", fake_run_planned_pr)

    opened_pr = PRInfo(
        number=17,
        branch="pr-042-sample",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )
    # First call (guard in handle_idle) returns no matching PR;
    # subsequent calls (handle_coding) return the opened PR.
    call_count = {"n": 0}

    def _get_open_prs(repo: str, **kw: Any) -> list[PRInfo]:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return []  # guard: no existing PR
        return [opened_pr]

    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        _get_open_prs,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_merged_prs", lambda repo, branch, refresh=False: []
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
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: tasks)
    monkeypatch.setattr(idle_module, "get_next_task", lambda t: tasks[2])
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, open_pr_branches: tasks,
    )
    monkeypatch.setattr(
        claude_cli,
        "run_planned_pr_async",
        _async_cli_result(0, "ok", ""),
    )
    # First call (guard) returns no matching PR; subsequent calls return the PR.
    call_count = {"n": 0}

    def _get_open_prs(repo: str, **kw: Any) -> list[PRInfo]:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return []
        return [PRInfo(number=1, branch="pr-003")]

    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        _get_open_prs,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_merged_prs", lambda repo, branch, refresh=False: []
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


def test_handle_idle_uses_dag_when_headers_present(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        _ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: Bootstrap\n\n"
        "Branch: pr-001-bootstrap\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-002.md").write_text(
        "# PR-002: Next task\n\n"
        "Branch: pr-002-next-task\n"
        "- Type: feature\n"
        "- Complexity: medium\n"
        "- Depends on: PR-001\n"
        "- Priority: 2\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: (_ for _ in ()).throw(AssertionError("QUEUE fallback not expected")),
    )
    monkeypatch.setattr(idle_module, "get_merged_pr_ids", lambda *args, **kwargs: {"PR-001"})
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert coding_called["v"] is True
    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-002"
    assert runner.state.current_task.task_file == "tasks/PR-002.md"
    assert runner.state.queue_done == 1
    assert runner.state.queue_total == 2


def test_handle_idle_falls_back_to_queue_md(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        _ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text("No structured header here\n", encoding="utf-8")

    fallback_task = QueueTask(
        pr_id="PR-099",
        title="Fallback queue task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-099.md",
        branch="pr-099-fallback",
    )
    parse_calls: list[str] = []
    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: (parse_calls.append(path) or [fallback_task]),
    )
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: fallback_task)
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert parse_calls == [str(tasks_dir / "QUEUE.md")]
    assert coding_called["v"] is True
    assert runner.state.current_task == fallback_task
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 1


def test_handle_idle_dag_skips_files_without_headers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        _ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: Structured\n\n"
        "Branch: pr-001-structured\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-002.md").write_text(
        "Missing structured metadata\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: (_ for _ in ()).throw(AssertionError("QUEUE fallback not expected")),
    )
    monkeypatch.setattr(idle_module, "get_merged_pr_ids", lambda *args, **kwargs: set())
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert coding_called["v"] is True
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-001"
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 1


def test_handle_idle_dag_surfaces_malformed_task_headers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        _ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: Broken structured task\n\n"
        "Branch: pr-001-broken\n"
        "- Type: definitely-not-valid\n"
        "- Complexity: low\n"
        "- Depends on: none\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message is not None
    assert "invalid Type" in runner.state.error_message


def test_select_next_task_from_dag_prefers_doing_task(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        _ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: In flight task\n\n"
        "Branch: pr-001-in-flight\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-002.md").write_text(
        "# PR-002: Fresh task\n\n"
        "Branch: pr-002-fresh\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "get_merged_pr_ids", lambda *args, **kwargs: set())
    monkeypatch.setattr(
        idle_module,
        "derive_task_status",
        lambda header, merged_pr_ids, open_prs, merged_prs: (
            TaskStatus.DOING
            if header.pr_id == "PR-001"
            else TaskStatus.TODO
        ),
    )

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = []
    runner._idle_merged_prs = []

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is not None
    assert task.pr_id == "PR-001"
    assert task.status == TaskStatus.DOING


def test_handle_idle_attaches_to_existing_pr_instead_of_coding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When a TODO task already has an open PR on its branch, handle_idle
    should attach to that PR and go to WATCH instead of running CODING."""
    _patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-042",
        title="Sample",
        status=TaskStatus.TODO,
        branch="pr-042-sample",
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: task)

    existing_pr = PRInfo(
        number=99,
        branch="pr-042-sample",
        title="PR-042: Sample",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [existing_pr],
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_merged_prs", lambda repo, branch, refresh=False: []
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_pr_metadata",
        lambda repo, number: {"head_commit_date": "2026-04-14T12:00:00Z"},
    )

    coding_called = {"v": False}

    async def spy_handle_coding(self):
        coding_called["v"] = True

    monkeypatch.setattr(runner_module.PipelineRunner, "handle_coding", spy_handle_coding)

    runner = _make_runner()
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 99
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-042"
    assert not coding_called["v"], "handle_coding should NOT be called"


def test_handle_idle_proceeds_to_coding_when_no_matching_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When no open PR matches the task branch, handle_idle proceeds to CODING."""
    _patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-042",
        title="Sample",
        status=TaskStatus.TODO,
        branch="pr-042-sample",
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: task)

    # Guard returns no matching PR; handle_coding's call returns the PR.
    call_count = {"n": 0}

    def _get_open_prs(repo: str, **kw: Any) -> list[PRInfo]:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return []
        return [PRInfo(number=17, branch="pr-042-sample")]

    monkeypatch.setattr(runner_module.github_client, "get_open_prs", _get_open_prs)
    monkeypatch.setattr(
        runner_module.github_client, "get_merged_prs", lambda repo, branch, refresh=False: []
    )
    monkeypatch.setattr(
        claude_cli,
        "run_planned_pr_async",
        _async_cli_result(0, "ok", ""),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: None,
    )

    runner = _make_runner()
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 17


def test_handle_idle_defers_on_gh_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When get_open_prs raises during the guard check, handle_idle defers
    without entering CODING."""
    _patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-042",
        title="Sample",
        status=TaskStatus.TODO,
        branch="pr-042-sample",
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: task)

    def _exploding_get_open_prs(repo: str, **kw: Any) -> list[PRInfo]:
        raise RuntimeError("GitHub API unavailable")

    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", _exploding_get_open_prs
    )

    coding_called = {"v": False}

    async def spy_handle_coding(self):
        coding_called["v"] = True

    monkeypatch.setattr(runner_module.PipelineRunner, "handle_coding", spy_handle_coding)

    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-999",
        title="Stale task",
        status=TaskStatus.DOING,
        branch="pr-999-stale-task",
    )
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert not coding_called["v"], "handle_coding should NOT be called on GH failure"


def test_handle_idle_sets_error_when_task_status_derivation_times_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-042",
        title="Sample",
        status=TaskStatus.TODO,
        branch="pr-042-sample",
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_merged_prs", lambda repo, branch, refresh=False: []
    )

    def _timed_out(*args: Any, **kwargs: Any) -> list[QueueTask]:
        raise subprocess.TimeoutExpired(cmd=["git", "branch", "--merged"], timeout=10)

    monkeypatch.setattr(idle_module, "derive_queue_task_statuses", _timed_out)

    coding_called = {"v": False}

    async def spy_handle_coding(self):
        coding_called["v"] = True

    monkeypatch.setattr(runner_module.PipelineRunner, "handle_coding", spy_handle_coding)

    runner = _make_runner()
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.ERROR
    assert "Task status derivation failed" in (runner.state.error_message or "")
    assert not coding_called["v"], "handle_coding should NOT be called on timeout"


def test_handle_coding_errors_when_no_pr_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_planned_pr_async",
        _async_cli_result(0, "ok", ""),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
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


def test_handle_coding_creates_run_record(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_planned_pr_async",
        _async_cli_result(0, "ok", ""),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [PRInfo(number=42, branch="pr-001")],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: None,
    )

    task_file = tmp_path / "tasks" / "PR-001.md"
    task_file.parent.mkdir(parents=True)
    task_file.write_text(
        "# PR-001: Sample\n\n"
        "Branch: pr-001\n"
        "- Type: feature\n"
        "- Complexity: low\n",
        encoding="utf-8",
    )

    runner = _make_runner()
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


def test_handle_coding_saves_record_on_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_planned_pr_async",
        _async_cli_result(0, "ok", ""),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [PRInfo(number=42, branch="pr-001")],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: None,
    )

    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001"
    )

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
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_planned_pr_async",
        _async_cli_result(1, "", "build failed"),
    )

    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001"
    )

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
    attaching to an unrelated newest open PR."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_planned_pr_async",
        _async_cli_result(0, "ok", ""),
    )
    unrelated = PRInfo(number=99, branch="other-branch")
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [unrelated],
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
        claude_cli,
        "run_planned_pr_async",
        _async_cli_result(0, "ok", ""),
    )
    opened_pr = PRInfo(number=42, branch="pr-019")
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [opened_pr],
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
        claude_cli,
        "run_planned_pr_async",
        _async_cli_result(0, "ok", ""),
    )
    opened_pr = PRInfo(number=42, branch="pr-019")
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [opened_pr],
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
        claude_cli, "fix_review_async", _async_cli_result(0, "", "")
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


def test_fix_increments_iterations(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli, "fix_review_async", _async_cli_result(0, "", "")
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: None,
    )

    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )
    runner._start_current_run_record("claude", "opus")
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=77, branch="pr-001")

    asyncio.run(runner.handle_fix())

    assert runner._current_run_record is not None
    assert runner._current_run_record.fix_iterations == 1


def test_fix_iterations_survive_recovery_until_merge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue_text = (
        "## PR-001: t\n"
        "- Status: DOING\n"
        "- Tasks file: tasks/PR-001.md\n"
        "- Branch: pr-001\n"
    )

    def fake_git(repo_path: str, *args: str, **kw: Any) -> Any:
        if args[0] == "show":
            return _FakeCompletedProcess(
                args=["git", "show"],
                stdout=queue_text,
                returncode=0,
            )
        if args[:2] == ("rev-parse", "HEAD"):
            return _FakeCompletedProcess(
                args=["git", "rev-parse", "HEAD"],
                stdout="abc123\n",
                returncode=0,
            )
        if args[0] == "merge" and len(args) > 1 and args[1].startswith("origin/"):
            return _FakeCompletedProcess(
                args=["git", *args],
                stdout="Already up to date.\n",
                returncode=0,
            )
        return _FakeCompletedProcess(args=["git", *args], returncode=0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(
        claude_cli, "fix_review_async", _async_cli_result(0, "", "")
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [PRInfo(number=77, branch="pr-001")],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_pr_metadata",
        lambda repo, number: {
            "author": "",
            "head_sha": "",
            "head_commit_date": "2026-04-18T12:00:00Z",
        },
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: None,
    )
    monkeypatch.setattr(
        runner_module.github_client, "merge_pr", lambda repo, num: None
    )
    monkeypatch.setattr(
        runner_module.PipelineRunner, "_mark_queue_done", lambda self: None
    )

    redis = _FakeRedis()
    claude_provider, codex_provider = _usage_providers()
    runner = PipelineRunner(
        _repo_cfg(),
        _app_cfg(),
        redis,
        claude_provider,
        codex_provider,
    )
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )
    runner.state.current_pr = PRInfo(number=77, branch="pr-001")
    runner.state.state = PipelineState.WATCH
    runner._start_current_run_record("claude", "opus")
    asyncio.run(runner._save_current_run_record("coding_complete"))

    asyncio.run(runner.handle_fix())

    recovered = PipelineRunner(
        _repo_cfg(),
        _app_cfg(),
        redis,
        *_usage_providers(),
    )
    asyncio.run(recovered.recover_state())

    assert recovered.state.state == PipelineState.WATCH
    assert recovered._current_run_record is not None
    assert recovered._current_run_record.fix_iterations == 1

    asyncio.run(recovered.handle_merge())

    recent = asyncio.run(
        recovered._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=recovered.name,
        )
    )

    assert len(recent) == 1
    assert recent[0].fix_iterations == 1
    assert recent[0].exit_reason == "success_merged"


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
        claude_cli, "fix_review_async", _async_cli_result(0, "", "")
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
        claude_cli, "fix_review_async", _async_cli_result(0, "", "")
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
    assert not any(cmd[:2] == ["git", "fetch"] for cmd in calls)
    assert not any(cmd[:2] == ["git", "checkout"] for cmd in calls)
    assert not any(cmd[:2] == ["git", "reset"] for cmd in calls)


def test_handle_fix_fetches_and_resets_branch_before_fix_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Before invoking fix_review, ``handle_fix`` must fetch the PR branch
    from origin, check it out, and hard-reset to ``origin/<branch>`` so the
    local state matches the remote exactly.
    """
    calls = _patch_subprocess(monkeypatch)
    fix_called_at: list[int] = []

    async def fake_fix(
        path: str, model: str | None = None, timeout: int | None = None, **kwargs: object
    ) -> tuple[int, str, str]:
        fix_called_at.append(len(calls))
        return (0, "", "")

    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: None,
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    asyncio.run(runner.handle_fix())

    fetch_calls = [
        i for i, cmd in enumerate(calls)
        if cmd[:2] == ["git", "fetch"]
        and any("pr-042-fix" in arg for arg in cmd)
    ]
    checkout_calls = [
        i for i, cmd in enumerate(calls)
        if cmd[:2] == ["git", "checkout"] and "pr-042-fix" in cmd
    ]
    reset_calls = [
        i for i, cmd in enumerate(calls)
        if cmd[:2] == ["git", "reset"]
        and "--hard" in cmd
        and "origin/pr-042-fix" in cmd
    ]
    assert fetch_calls, "expected git fetch origin pr-042-fix"
    assert checkout_calls, "expected git checkout pr-042-fix"
    assert reset_calls, "expected git reset --hard origin/pr-042-fix"
    assert fix_called_at, "fix_review must have been invoked"
    # Order: fetch < checkout < reset < fix_review
    assert fetch_calls[0] < checkout_calls[0] < reset_calls[0] < fix_called_at[0]
    assert runner.state.state == PipelineState.WATCH


def test_handle_fix_errors_when_fetch_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the PR branch fetch before fix_review fails, the runner must
    transition to ERROR rather than letting Claude patch stale code.
    """
    fix_calls: list[str] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "fetch"] and any("pr-042-fix" in a for a in cmd):
            raise subprocess.CalledProcessError(
                1, cmd, stderr="fatal: couldn't find remote ref pr-042-fix"
            )
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        claude_cli,
        "fix_review_async",
        _async_cli_capture_path(fix_calls, 0, "", ""),
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.ERROR
    assert "git refresh" in (runner.state.error_message or "")
    assert "pr-042-fix" in (runner.state.error_message or "")
    assert fix_calls == [], "fix_review must not run when fetch fails"


def test_handle_fix_errors_when_reset_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If ``git reset --hard origin/<branch>`` fails after fetch+checkout,
    the runner must transition to ERROR so Claude does not run against a
    diverged local branch.
    """
    fix_calls: list[str] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "reset"] and "origin/pr-042-fix" in cmd:
            raise subprocess.CalledProcessError(
                1, cmd, stderr="fatal: ambiguous argument"
            )
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        claude_cli,
        "fix_review_async",
        _async_cli_capture_path(fix_calls, 0, "", ""),
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.ERROR
    assert "git refresh" in (runner.state.error_message or "")
    assert fix_calls == [], "fix_review must not run when reset fails"


def test_handle_fix_errors_when_checkout_times_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TimeoutExpired during the refresh sequence must be caught and set
    PipelineState.ERROR rather than escaping the daemon loop.
    """
    fix_calls: list[str] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "checkout"] and "pr-042-fix" in cmd:
            raise subprocess.TimeoutExpired(cmd=cmd, timeout=30)
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        claude_cli,
        "fix_review_async",
        _async_cli_capture_path(fix_calls, 0, "", ""),
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.ERROR
    assert "git refresh" in (runner.state.error_message or "")
    assert fix_calls == [], "fix_review must not run when checkout times out"
    assert fix_calls == [], "fix_review must not run when checkout times out"


def test_handle_coding_errors_when_task_has_no_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_planned_pr_async",
        _async_cli_result(0, "ok", ""),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [PRInfo(number=1, branch="anything")],
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
        claude_cli,
        "run_planned_pr_async",
        _async_cli_result(0, "ok", ""),
    )

    opened_pr = PRInfo(number=42, branch="pr-001")
    call_count = {"n": 0}

    def flaky_get_open_prs(repo: str, **kw: Any) -> list[PRInfo]:
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
    assert 5 in slept
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
        claude_cli,
        "run_planned_pr_async",
        _async_cli_result(0, "ok", ""),
    )
    call_count = {"n": 0}

    def always_empty(repo: str, **kw: Any) -> list[PRInfo]:
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
    assert slept.count(5) == 2


def test_handle_watch_approved_and_green_merges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.APPROVED,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo, **kw: [pr]
    )

    merged: list[tuple[str, int]] = []

    def fake_merge(repo: str, number: int) -> None:
        merged.append((repo, number))

    monkeypatch.setattr(runner_module.github_client, "merge_pr", fake_merge)
    monkeypatch.setattr(
        runner_module.PipelineRunner, "_mark_queue_done", lambda self: None
    )

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
        runner_module.github_client, "get_open_prs", lambda repo, **kw: [pr]
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
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo, **kw: [pr]
    )
    monkeypatch.setattr(
        claude_cli, "fix_review_async", _async_cli_result(0, "", "")
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


def test_handle_watch_no_fix_when_ci_pending_and_changes_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=datetime.now(timezone.utc),
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo, **kw: [pr]
    )
    fix_called: list[bool] = []

    async def _no_fix(*_args: object, **_kwargs: object) -> tuple:
        fix_called.append(True)
        return (0, "", "")

    monkeypatch.setattr(claude_cli, "fix_review_async", _no_fix)
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
    assert fix_called == []
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 0


def test_handle_watch_fix_when_ci_success_and_changes_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo, **kw: [pr]
    )
    monkeypatch.setattr(
        claude_cli, "fix_review_async", _async_cli_result(0, "", "")
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
        runner_module.github_client, "get_open_prs", lambda repo, **kw: [pr]
    )
    monkeypatch.setattr(
        claude_cli, "fix_review_async", _async_cli_result(0, "", "")
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
        runner_module.github_client, "get_open_prs", lambda repo, **kw: [pr]
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
        runner_module.github_client, "get_open_prs", lambda repo, **kw: [pr]
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
        runner_module.github_client, "get_open_prs", lambda repo, **kw: [pr]
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
        runner_module.github_client, "get_open_prs", lambda repo, **kw: [pr]
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
    runner = PipelineRunner(
        repo_cfg,
        app_cfg,
        _FakeRedis(),
        *_usage_providers(),
    )
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
        runner_module.github_client, "get_open_prs", lambda repo, **kw: [pr]
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
    runner = PipelineRunner(
        repo_cfg,
        app_cfg,
        _FakeRedis(),
        *_usage_providers(),
    )
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
        runner_module.github_client, "get_open_prs", lambda repo, **kw: [pr]
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
        runner_module.github_client, "get_open_prs", lambda repo, **kw: []
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


def test_handle_hung_preserves_context_when_fallback_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When hung_fallback_codex_review=False and PR is still open, runner
    stays in HUNG with current_pr and current_task preserved."""
    monkeypatch.setattr(
        runner_module.github_client,
        "run_gh",
        lambda *a, **kw: {"state": "OPEN"},
    )
    runner = PipelineRunner(
        _repo_cfg(),
        AppConfig(
            repositories=[],
            daemon=DaemonConfig(hung_fallback_codex_review=False),
        ),
        _FakeRedis(),
        *_usage_providers(),
    )
    runner.state.state = PipelineState.HUNG
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )
    asyncio.run(runner.handle_hung())

    assert runner.state.state == PipelineState.HUNG
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 5
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-001"


@pytest.mark.parametrize("pr_state", ["MERGED", "CLOSED"])
def test_handle_hung_transitions_to_idle_when_pr_resolved(
    monkeypatch: pytest.MonkeyPatch,
    pr_state: str,
) -> None:
    """When hung_fallback_codex_review=False and the operator has closed or
    merged the PR, the runner should transition back to IDLE."""
    monkeypatch.setattr(
        runner_module.github_client,
        "run_gh",
        lambda *a, **kw: {"state": pr_state},
    )
    runner = PipelineRunner(
        _repo_cfg(),
        AppConfig(
            repositories=[],
            daemon=DaemonConfig(hung_fallback_codex_review=False),
        ),
        _FakeRedis(),
        *_usage_providers(),
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
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        runner_module.github_client, "merge_pr", lambda repo, num: None
    )
    monkeypatch.setattr(
        runner_module.PipelineRunner, "_mark_queue_done", lambda self: None
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING,
    )
    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None


def test_merge_finalizes_record(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        runner_module.github_client, "merge_pr", lambda repo, num: None
    )
    monkeypatch.setattr(
        runner_module.PipelineRunner, "_mark_queue_done", lambda self: None
    )

    runner = _make_runner()
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

    assert len(recent) == 1
    assert recent[0].exit_reason == "success_merged"
    assert recent[0].ended_at is not None
    assert recent[0].duration_ms is not None
    assert runner._current_run_record is None


def test_merge_calculates_duration(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        runner_module.github_client, "merge_pr", lambda repo, num: None
    )
    monkeypatch.setattr(
        runner_module.PipelineRunner, "_mark_queue_done", lambda self: None
    )

    fixed_now = datetime(2026, 4, 18, 12, 0, 6, 500000, tzinfo=timezone.utc)

    class _FixedDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            if tz is None:
                return fixed_now.replace(tzinfo=None)
            return fixed_now.astimezone(tz)

    monkeypatch.setattr(runner_module, "datetime", _FixedDateTime)

    runner = _make_runner()
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
    assert runner._current_run_record is not None
    runner._current_run_record.started_at = "2026-04-18T12:00:00+00:00"

    asyncio.run(runner.handle_merge())

    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=runner.name,
        )
    )

    assert len(recent) == 1
    assert recent[0].duration_ms == 6500


def test_handle_merge_queue_sync_failure_still_goes_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When _mark_queue_done raises, handle_merge catches the exception
    and still transitions to IDLE. The pending_queue_sync_branch marker
    (set eagerly inside _mark_queue_done) gates handle_idle from
    re-dispatching the same task."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        runner_module.github_client, "merge_pr", lambda repo, num: None
    )

    def _failing_mark(self: Any) -> None:
        self.state.pending_queue_sync_branch = "queue-done-pr-001"
        raise RuntimeError("push rejected")

    monkeypatch.setattr(
        runner_module.PipelineRunner, "_mark_queue_done", _failing_mark
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING,
    )
    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.pending_queue_sync_branch == "queue-done-pr-001"


def test_mark_queue_done_direct_push(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """After merge, _mark_queue_done pushes the DONE update directly to
    the base branch instead of opening a remediation PR."""
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    queue_path.write_text(
        "## PR-001: first\n- Status: DOING\n\n"
        "## PR-002: second\n- Status: TODO\n"
    )

    git_calls: list[list[str]] = []

    def fake_git(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        git_calls.append(cmd)
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_git)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="first", status=TaskStatus.DOING
    )

    runner._mark_queue_done()

    updated = queue_path.read_text()
    assert "## PR-001: first\n- Status: DONE" in updated
    assert "## PR-002: second\n- Status: TODO" in updated

    push_cmds = [cmd for cmd in git_calls if cmd[:2] == ["git", "push"]]
    assert push_cmds
    assert any("main" in cmd for cmd in push_cmds), (
        "must push directly to the base branch"
    )


def test_mark_queue_done_falls_back_to_pr_on_push_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When direct push to base is rejected, _mark_queue_done falls
    back to a remediation PR and sets pending_queue_sync_branch."""
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    queue_path.write_text("## PR-001: first\n- Status: DOING\n")

    def fail_base_push(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "push"] and "main" in cmd:
            return _FakeCompletedProcess(
                args=cmd, returncode=1, stderr="push rejected"
            )
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fail_base_push)

    gh_calls: list[list[str]] = []
    monkeypatch.setattr(
        runner_module.github_client, "run_gh",
        lambda cmd, **kw: gh_calls.append(cmd),
    )

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="first", status=TaskStatus.DOING
    )

    runner._mark_queue_done()
    assert runner.state.pending_queue_sync_branch == "queue-done-pr-001"
    assert any("pr" in c and "create" in c for c in gh_calls)


def test_handle_merge_failure_sets_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_subprocess(monkeypatch)

    def boom(repo: str, num: int) -> None:
        raise RuntimeError("merge conflict")

    monkeypatch.setattr(runner_module.github_client, "merge_pr", boom)

    runner = _make_runner()
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

    def fake_git(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        git_calls.append(cmd)
        if cmd[:2] == ["git", "merge"] and "origin/main" in cmd:
            return _FakeCompletedProcess(
                args=cmd, stdout="Already up to date.\n", returncode=0
            )
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_git)

    merge_pr_calls: list[tuple[str, int]] = []

    def fake_merge_pr(repo: str, num: int) -> None:
        merge_pr_calls.append((repo, num))

    monkeypatch.setattr(runner_module.github_client, "merge_pr", fake_merge_pr)
    monkeypatch.setattr(
        runner_module.PipelineRunner, "_mark_queue_done", lambda self: None
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )
    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.IDLE
    assert merge_pr_calls == [(runner.owner_repo, 5)]

    merge_idx = next(
        i for i, cmd in enumerate(git_calls)
        if cmd[:2] == ["git", "merge"] and "origin/main" in cmd
    )
    merge_pr_call_idx = len(git_calls)  # merge_pr ran after all git calls
    assert merge_idx < merge_pr_call_idx
    # No push because the merge was a no-op.
    assert not any(
        cmd[:2] == ["git", "push"] and "pr-001" in cmd for cmd in git_calls
    )


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

    def fake_git(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        git_calls.append(cmd)
        if cmd[:2] == ["git", "merge"] and "origin/main" in cmd:
            return _FakeCompletedProcess(
                args=cmd,
                stdout="Merge made by the 'ort' strategy.\n",
                returncode=0,
            )
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_git)

    merge_pr_calls: list[tuple[str, int]] = []
    monkeypatch.setattr(
        runner_module.github_client,
        "merge_pr",
        lambda repo, num: merge_pr_calls.append((repo, num)),
    )

    post_calls: list[tuple[str, int, str]] = []
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, num, body: post_calls.append((repo, num, body)),
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_pr = pr
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )
    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is pr
    assert not merge_pr_calls, (
        "merge_pr must not run with stale gate results after sync push"
    )
    assert any(
        cmd[:2] == ["git", "push"] and "pr-001" in cmd for cmd in git_calls
    ), "sync must push the merged PR branch"
    assert post_calls == [(runner.owner_repo, 5, "@codex review")], (
        "must re-request Codex review on the refreshed HEAD"
    )


def test_handle_merge_errors_when_codex_post_fails_after_sync_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failing to post @codex review after a sync push must flip the
    runner into ERROR: without a fresh review trigger on the new
    HEAD, the prior anchor +1 keeps the PR APPROVED and a subsequent
    handle_watch cycle would merge on stale approval."""
    def fake_git(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "merge"] and "origin/main" in cmd:
            return _FakeCompletedProcess(
                args=cmd,
                stdout="Merge made by the 'ort' strategy.\n",
                returncode=0,
            )
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_git)

    def boom(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("gh api failure")

    monkeypatch.setattr(runner_module.github_client, "post_comment", boom)

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )
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

    def fake_git(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        git_calls.append(cmd)
        if cmd[:2] == ["git", "merge"] and "origin/main" in cmd:
            return _FakeCompletedProcess(
                args=cmd,
                returncode=1,
                stdout="CONFLICT (content): merge conflict in foo",
            )
        return _FakeCompletedProcess(args=cmd, returncode=0)

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
        runner_module.github_client,
        "merge_pr",
        lambda repo, num: merge_pr_calls.append((repo, num)),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, num, body: None,
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )
    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.WATCH
    assert claude_calls, "Claude must be invoked on merge conflict"
    assert not merge_pr_calls, (
        "merge_pr must not run with stale gate results after sync push"
    )
    assert any(
        cmd[:2] == ["git", "push"] and "pr-001" in cmd for cmd in git_calls
    ), "conflict-resolved HEAD must be pushed to origin"


def test_handle_merge_skips_sync_for_cross_repo_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fork-based PRs: the head branch is on the contributor's fork,
    not origin. Any local push of the head branch would fail. Skip the
    pre-merge sync entirely and defer to gh pr merge."""
    git_calls: list[list[str]] = []

    def fake_git(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        git_calls.append(cmd)
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_git)

    merge_pr_calls: list[tuple[str, int]] = []
    monkeypatch.setattr(
        runner_module.github_client,
        "merge_pr",
        lambda repo, num: merge_pr_calls.append((repo, num)),
    )
    monkeypatch.setattr(
        runner_module.PipelineRunner, "_mark_queue_done", lambda self: None
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=5, branch="pr-001", is_cross_repository=True
    )
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )
    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.IDLE
    assert merge_pr_calls == [(runner.owner_repo, 5)]
    assert not any(
        cmd[:2] == ["git", "push"] and "pr-001" in cmd for cmd in git_calls
    ), "cross-repo PRs must not push the head branch to origin"
    assert not any(
        cmd[:2] == ["git", "merge"] and "origin/main" in cmd
        for cmd in git_calls
    ), "cross-repo PRs must not merge base locally"


def test_handle_merge_refreshes_pr_head_before_merge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After a daemon restart, the local PR branch may lag behind
    origin (recover_state resumes WATCH with a stale checkout). The
    sync must fetch origin/<pr_branch> and reset the local branch to
    it, or the subsequent push will be rejected as non-fast-forward."""
    git_calls: list[list[str]] = []

    def fake_git(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        git_calls.append(cmd)
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_git)
    monkeypatch.setattr(
        runner_module.github_client, "merge_pr", lambda repo, num: None
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, num, body: None,
    )
    monkeypatch.setattr(
        runner_module.PipelineRunner, "_mark_queue_done", lambda self: None
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )
    asyncio.run(runner.handle_merge())

    fetch_cmds = [cmd for cmd in git_calls if cmd[:3] == ["git", "fetch",
                                                          "origin"]]
    assert fetch_cmds and any("pr-001" in cmd for cmd in fetch_cmds), (
        "must fetch origin/<pr_branch> before local merge"
    )
    reset_cmds = [
        cmd for cmd in git_calls
        if cmd[:3] == ["git", "reset", "--hard"] and "origin/pr-001" in cmd
    ]
    assert reset_cmds, "must reset local PR branch to origin/<pr_branch>"

    reset_idx = git_calls.index(reset_cmds[0])
    merge_idx = next(
        i for i, cmd in enumerate(git_calls)
        if cmd[:2] == ["git", "merge"] and "origin/main" in cmd
    )
    assert reset_idx < merge_idx, (
        "reset to origin/<pr_branch> must happen before merging base"
    )


def test_handle_merge_aborts_on_unresolvable_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When Claude fails to resolve the conflict, handle_merge aborts
    the merge, sets ERROR, and does not call github_client.merge_pr."""
    git_calls: list[list[str]] = []

    def fake_git(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        git_calls.append(cmd)
        if cmd[:2] == ["git", "merge"] and "origin/main" in cmd:
            return _FakeCompletedProcess(
                args=cmd,
                returncode=1,
                stdout="CONFLICT (content): merge conflict in foo",
            )
        return _FakeCompletedProcess(args=cmd, returncode=0)

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

    monkeypatch.setattr(runner_module.github_client, "merge_pr", fake_merge_pr)

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )
    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.ERROR
    assert "Merge conflict resolution failed" in (
        runner.state.error_message or ""
    )
    assert not merge_pr_calls, "merge_pr must not be called on abort"
    abort_cmds = [
        cmd for cmd in git_calls
        if cmd[:3] == ["git", "merge", "--abort"]
    ]
    assert abort_cmds, "git merge --abort must be invoked"


def test_handle_error_skip_clears_state(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        _async_cli_result(0, "SKIP", ""),
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
        claude_cli,
        "diagnose_error_async",
        _async_cli_result(0, "ESCALATE: human help", ""),
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
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo, **kw: []
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
    (repo / "CLAUDE.md").write_text("Read and follow AGENTS.md in this repository.\n")
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

    # Finally append artifacts/ — now fully scaffolded.
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
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [done_task])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)

    open_pr = PRInfo(
        number=42,
        branch="pr-001-done",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.PENDING,
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo, **kw: [open_pr]
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_merged_prs", lambda repo, branch, refresh=False: []
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
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo, **kw: []
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_merged_prs", lambda repo, branch, refresh=False: []
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
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)

    open_pr = PRInfo(number=7, branch="feature-x")
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo, **kw: [open_pr]
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
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: (_ for _ in ()).throw(RuntimeError("API down")),
    )

    runner = _make_runner()
    runner.state.current_pr = PRInfo(number=5, branch="stale")
    asyncio.run(runner.handle_idle())

    # Must not crash, state stays IDLE, and stale current_pr is cleared.
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert any("open PR check failed" in e["event"] for e in runner.state.history)


def test_handle_idle_falls_back_when_merged_pr_check_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-123",
        title="Keep dispatching",
        branch="pr-123-keep-dispatching",
        status=TaskStatus.TODO,
        task_file="tasks/PR-123.md",
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
        lambda repo, branch, refresh=False: (_ for _ in ()).throw(RuntimeError("API down")),
    )
    derived_calls: list[list[PRInfo]] = []
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): (
            derived_calls.append(list(merged_prs)) or tasks
        ),
    )
    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True
        return None

    runner = _make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    runner.state.current_pr = PRInfo(number=5, branch="stale")
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task == task
    assert runner.state.current_pr.number == 5
    assert coding_called["v"] is True
    assert derived_calls == [[]]
    assert any("merged PR check failed" in e["event"] for e in runner.state.history)


def test_handle_idle_uses_fallback_queue_counters_when_dag_picks_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    dag_tasks = [
        QueueTask(
            pr_id="PR-001",
            title="Done DAG task",
            status=TaskStatus.DONE,
            task_file="tasks/PR-001.md",
            branch="pr-001-done",
        ),
        QueueTask(
            pr_id="PR-002",
            title="Blocked DAG task",
            status=TaskStatus.TODO,
            task_file="tasks/PR-002.md",
            branch="pr-002-blocked",
        ),
    ]
    fallback_task = QueueTask(
        pr_id="PR-099",
        title="Fallback queue task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-099.md",
        branch="pr-099-fallback",
    )
    fallback_tasks = [fallback_task]

    async def fake_select(self):
        self._idle_dag_tasks = dag_tasks
        return None

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: fallback_tasks)
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: fallback_task)
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    async def fake_handle_coding() -> None:
        return None

    runner = _make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert runner.state.current_task == fallback_task
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 1


def test_handle_idle_requests_fresh_merged_prs_for_status_derivation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-123",
        title="Refresh merged PRs",
        branch="pr-123-refresh-merged-prs",
        status=TaskStatus.TODO,
        task_file="tasks/PR-123.md",
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    refresh_calls: list[bool] = []

    def fake_get_merged_prs(
        repo: str,
        branch: str,
        refresh: bool = False,
    ) -> list[PRInfo]:
        refresh_calls.append(refresh)
        return []

    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
        fake_get_merged_prs,
    )
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )

    async def fake_handle_coding() -> None:
        return None

    runner = _make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert refresh_calls == [True]


# ------------------------------------------------------------------
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
        claude_cli,
        "run_planned_pr_async",
        _async_cli_result(0, "hello from claude", ""),
    )
    pr = PRInfo(number=42, branch="pr-001")
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [pr],
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
    assert "hello from claude" in (stored or "")
    assert "=== STDOUT ===" in (stored or "")
    assert "=== STDERR ===" in (stored or "")


def test_handle_fix_saves_stdout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """handle_fix must save CLI stdout to Redis via _save_cli_log."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "fix_review_async",
        _async_cli_result(0, "fix output here", ""),
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
    assert "fix output here" in (stored or "")
    assert "=== STDOUT ===" in (stored or "")
    assert "=== STDERR ===" in (stored or "")


def test_dirty_tree_auto_recovery_after_3_cycles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After three consecutive dirty preflights the runner hard-resets
    to ``origin/{branch}`` and returns to IDLE instead of staying stuck
    in ERROR requiring manual intervention."""
    commands: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        commands.append(cmd)
        if cmd[:3] == ["git", "status", "--porcelain"]:
            return _FakeCompletedProcess(
                args=cmd, stdout=" M src/foo.py\n", returncode=0
            )
        return _FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    runner = _make_runner()

    assert runner.preflight() is False
    assert runner._consecutive_dirty_cycles == 1
    assert runner.state.state == PipelineState.ERROR

    assert runner.preflight() is False
    assert runner._consecutive_dirty_cycles == 2
    assert runner.state.state == PipelineState.ERROR

    assert runner.preflight() is True
    assert runner._consecutive_dirty_cycles == 0
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None
    assert any(
        cmd[:2] == ["git", "reset"] and "--hard" in cmd
        for cmd in commands
    )
    assert any(cmd[:3] == ["git", "clean", "-fd"] for cmd in commands)
    assert any(
        "Auto-recovered from dirty tree" in e["event"]
        for e in runner.state.history
    )


def test_dirty_tree_auto_recovery_preserves_watch_with_open_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When auto-recovery fires while a PR is being tracked, the runner
    must resume WATCH, not IDLE. Dropping to IDLE lets the next cycle
    re-pick the still-TODO task from origin/main's QUEUE.md and open a
    duplicate PR — exactly the churn this safety net is meant to
    avoid."""
    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:3] == ["git", "status", "--porcelain"]:
            return _FakeCompletedProcess(
                args=cmd, stdout=" M src/foo.py\n", returncode=0
            )
        return _FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    runner = _make_runner()
    runner.state.current_pr = PRInfo(number=99, branch="pr-099-wip")
    runner.state.current_task = QueueTask(
        pr_id="PR-099", title="wip", status=TaskStatus.DOING, branch="pr-099-wip"
    )

    runner.preflight()
    runner.preflight()
    assert runner.preflight() is True
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 99
    assert runner.state.current_task is not None
    assert any(
        "auto-recovered from dirty tree -> watch" in e["event"].lower()
        for e in runner.state.history
    )


def test_dirty_tree_counter_resets_on_clean(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The dirty-cycle counter must return to zero after any clean
    preflight so a transient glitch does not push a later cycle over
    the auto-reset threshold."""
    _patch_subprocess(monkeypatch, stdout=" M foo.py")
    runner = _make_runner()
    assert runner.preflight() is False
    assert runner._consecutive_dirty_cycles == 1

    _patch_subprocess(monkeypatch, stdout="")
    assert runner.preflight() is True
    assert runner._consecutive_dirty_cycles == 0


def test_dirty_tree_auto_recovery_failure_stays_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the auto-reset git commands themselves fail, preflight must
    leave the runner in ERROR so the operator still sees the issue
    rather than silently declaring the tree clean."""
    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:3] == ["git", "status", "--porcelain"]:
            return _FakeCompletedProcess(
                args=cmd, stdout=" M src/foo.py\n", returncode=0
            )
        if cmd[:2] == ["git", "reset"]:
            raise subprocess.CalledProcessError(
                1, cmd, stderr="reset refused"
            )
        return _FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    runner = _make_runner()

    runner.preflight()
    runner.preflight()
    assert runner.preflight() is False
    assert runner.state.state == PipelineState.ERROR
    assert any(
        "Auto-recovery failed" in e["event"]
        for e in runner.state.history
    )


def test_codex_review_not_reposted_same_pr_same_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The runner must not repost ``@codex review`` for the same PR
    when no new push has happened since the last post."""
    posted: list[tuple[str, int, str]] = []

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    monkeypatch.setattr(runner_module.github_client, "post_comment", fake_post)
    monkeypatch.setattr(
        git_ops_module,
        "_git",
        lambda *args, **kwargs: _FakeCompletedProcess(
            args=list(args), stdout="head-1\n", returncode=0
        ),
    )
    runner = _make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-42", push_count=1)

    assert runner._post_codex_review(42) is True
    assert runner._post_codex_review(42) is True
    assert posted == [(runner.owner_repo, 42, "@codex review")]
    assert runner._last_codex_review_pr == 42
    assert runner._last_codex_review_head_sha == "head-1"
    assert any(
        "Skipping duplicate @codex review for PR #42" in e["event"]
        for e in runner.state.history
    )


def test_codex_review_reposted_after_new_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A new push on the same PR must allow a fresh review trigger."""
    posted: list[tuple[str, int, str]] = []

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    monkeypatch.setattr(runner_module.github_client, "post_comment", fake_post)
    head_shas = iter(["head-1\n", "head-2\n"])
    monkeypatch.setattr(
        git_ops_module,
        "_git",
        lambda *args, **kwargs: _FakeCompletedProcess(
            args=list(args), stdout=next(head_shas), returncode=0
        ),
    )
    runner = _make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-42", push_count=1)

    assert runner._post_codex_review(42) is True
    runner.state.current_pr.push_count += 1

    assert runner._post_codex_review(42) is True
    assert posted == [
        (runner.owner_repo, 42, "@codex review"),
        (runner.owner_repo, 42, "@codex review"),
    ]
    assert runner._last_codex_review_pr == 42
    assert runner._last_codex_review_head_sha == "head-2"


def test_codex_review_not_reposted_when_author_already_requested_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A recent PR-author trigger on the current head should suppress
    the daemon's first duplicate comment for that same head."""
    posted: list[tuple[str, int, str]] = []

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    runner = _make_runner()

    monkeypatch.setattr(runner_module.github_client, "post_comment", fake_post)
    monkeypatch.setattr(
        runner_module.github_client,
        "get_pr_metadata",
        lambda repo, number: {
            "author": "alice",
            "head_sha": "head-1",
            "head_commit_date": "2026-04-17T23:14:11Z",
        },
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "has_recent_codex_review_request",
        lambda repo, number, pr_author, within_minutes=5, after_iso=None: (
            repo == runner.owner_repo
            and number == 42
            and pr_author == "alice"
            and after_iso == "2026-04-17T23:14:11Z"
        ),
    )
    monkeypatch.setattr(
        git_ops_module,
        "_git",
        lambda *args, **kwargs: _FakeCompletedProcess(
            args=list(args), stdout="head-1\n", returncode=0
        ),
    )
    runner.state.current_pr = PRInfo(number=42, branch="pr-42", push_count=1)

    assert runner._post_codex_review(42) is True
    assert posted == []
    assert runner._last_codex_review_pr == 42
    assert runner._last_codex_review_head_sha == "head-1"
    assert any(
        "PR author already requested review for this head" in e["event"]
        for e in runner.state.history
    )


def test_codex_review_git_head_lookup_failure_does_not_dedup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient HEAD lookup failure must not suppress review requests."""
    posted: list[tuple[str, int, str]] = []

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    def fake_git(*args: object, **kwargs: object) -> _FakeCompletedProcess:
        raise RuntimeError("git rev-parse failed")

    monkeypatch.setattr(runner_module.github_client, "post_comment", fake_post)
    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    runner = _make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-42", push_count=1)

    assert runner._post_codex_review(42) is True
    assert runner._post_codex_review(42) is True
    assert posted == [
        (runner.owner_repo, 42, "@codex review"),
        (runner.owner_repo, 42, "@codex review"),
    ]
    assert runner._last_codex_review_pr is None
    assert runner._last_codex_review_head_sha is None
    assert any(
        "posting @codex review without dedup" in e["event"]
        for e in runner.state.history
    )


def test_codex_review_metadata_failure_posts_without_pr_author_dedup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Metadata lookup failures must not escape or suppress review posts."""
    posted: list[tuple[str, int, str]] = []

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    monkeypatch.setattr(runner_module.github_client, "post_comment", fake_post)
    monkeypatch.setattr(
        runner_module.github_client,
        "get_pr_metadata",
        lambda repo, number: (_ for _ in ()).throw(OSError("gh timed out")),
    )
    monkeypatch.setattr(
        git_ops_module,
        "_git",
        lambda *args, **kwargs: _FakeCompletedProcess(
            args=list(args), stdout="head-1\n", returncode=0
        ),
    )
    runner = _make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-42", push_count=1)

    assert runner._post_codex_review(42) is True
    assert posted == [(runner.owner_repo, 42, "@codex review")]
    assert runner._last_codex_review_pr == 42
    assert runner._last_codex_review_head_sha == "head-1"
    assert any(
        "failed to load PR metadata for @codex review dedup" in e["event"]
        for e in runner.state.history
    )


def test_save_cli_log_includes_stderr() -> None:
    """Both stdout and stderr must be saved to the CLI log."""
    runner = _make_runner()
    asyncio.run(
        runner._save_cli_log("out text", "err text", "LABEL")
    )
    stored = runner.redis.store.get(f"cli_log:{runner.name}:latest")
    assert stored is not None
    assert "out text" in stored
    assert "err text" in stored
    assert "=== STDOUT ===" in stored
    assert "=== STDERR ===" in stored


def test_handle_watch_skips_fix_no_new_feedback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHANGES_REQUESTED with no new Codex P1/P2 after last push must not
    trigger handle_fix — the stale review is waiting for a fresh pass."""
    last_push = datetime.now(timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=last_push,
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [pr],
    )
    # No Codex comments at all -> no new feedback.
    monkeypatch.setattr(
        runner_module.github_client,
        "_gh_api_paginated",
        lambda path: [],
    )
    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = _make_runner()
    runner._last_push_at = last_push
    runner._last_push_at_pr_number = pr.number
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    asyncio.run(runner.handle_watch())

    assert fix_called == []
    assert any(
        "no new Codex feedback" in e["event"]
        for e in runner.state.history
    )


def test_handle_watch_triggers_fix_new_feedback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHANGES_REQUESTED with fresh P1 feedback after last push triggers fix."""
    last_push = datetime.now(timezone.utc) - timedelta(minutes=10)
    recent = datetime.now(timezone.utc) - timedelta(minutes=1)
    pr = PRInfo(
        number=42,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=last_push,
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [pr],
    )

    comments = [
        {
            "user": {"login": "chatgpt-codex-connector"},
            "body": "P1: missing null check",
            "created_at": recent.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
    ]

    def fake_paginated(path: str) -> list[dict]:
        if "issues" in path:
            return comments
        return []

    monkeypatch.setattr(
        runner_module.github_client,
        "_gh_api_paginated",
        fake_paginated,
    )
    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = _make_runner()
    runner._last_push_at = last_push
    runner._last_push_at_pr_number = pr.number
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    asyncio.run(runner.handle_watch())

    assert fix_called == [True]


def test_handle_watch_still_fixes_ci_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CI failure must still trigger fix regardless of Codex feedback state."""
    pr = PRInfo(
        number=42,
        branch="pr-001",
        ci_status=CIStatus.FAILURE,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [pr],
    )
    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = _make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    asyncio.run(runner.handle_watch())

    assert fix_called == [True]


def test_handle_error_caps_at_3(monkeypatch: pytest.MonkeyPatch) -> None:
    """handle_error must stop invoking diagnose_error after 3 attempts."""
    calls: list[str] = []

    async def fake_diag(path: str, ctx: str, model: str | None = None) -> tuple[int, str, str]:
        calls.append(ctx)
        return (0, "ESCALATE", "")

    monkeypatch.setattr(claude_cli, "diagnose_error_async", fake_diag)
    runner = _make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "generic failure"
    for _ in range(5):
        asyncio.run(runner.handle_error())
    assert len(calls) == 3
    assert any(
        "max attempts" in e["event"] for e in runner.state.history
    )


def test_handle_error_skips_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """A timeout-marked error must skip the AI-diagnosis call entirely."""
    called: list[bool] = []

    async def fake_diag(*a: Any, **kw: Any) -> tuple[int, str, str]:
        called.append(True)
        return (0, "SKIP", "")

    monkeypatch.setattr(claude_cli, "diagnose_error_async", fake_diag)
    runner = _make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "claude CLI timeout after 900s"
    asyncio.run(runner.handle_error())
    assert called == []
    assert runner.state.state == PipelineState.ERROR


def test_handle_error_skips_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A rate-limit error must skip the AI-diagnosis call entirely."""
    called: list[bool] = []

    async def fake_diag(*a: Any, **kw: Any) -> tuple[int, str, str]:
        called.append(True)
        return (0, "SKIP", "")

    monkeypatch.setattr(claude_cli, "diagnose_error_async", fake_diag)
    runner = _make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "API rate limit exceeded"
    asyncio.run(runner.handle_error())
    assert called == []


def test_handle_fix_skips_fork(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cross-repo (fork) PRs must return to WATCH without running fix_review."""
    fix_called: list[bool] = []

    async def fake_fix(
        path: str, model: str | None = None, timeout: int | None = None, **kwargs: object
    ) -> tuple[int, str, str]:
        fix_called.append(True)
        return (0, "", "")

    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)
    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=10,
        branch="fork:feature",
        is_cross_repository=True,
    )
    asyncio.run(runner.handle_fix())
    assert fix_called == []
    assert runner.state.state == PipelineState.WATCH
    assert any(
        "Skipping FIX for cross-repo" in e["event"]
        for e in runner.state.history
    )


def test_handle_coding_uses_configured_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """planned_pr_timeout_sec from config must be forwarded to run_planned_pr."""
    _patch_subprocess(monkeypatch)
    captured: dict[str, Any] = {}

    async def fake_planned(
        path: str, model: str | None = None, timeout: int | None = None, **kwargs: object
    ) -> tuple[int, str, str]:
        captured["timeout"] = timeout
        return (0, "", "")

    monkeypatch.setattr(claude_cli, "run_planned_pr_async", fake_planned)
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [PRInfo(number=1, branch="pr-001")],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda *a, **kw: None,
    )

    runner = PipelineRunner(
        _repo_cfg(),
        AppConfig(
            repositories=[],
            daemon=DaemonConfig(planned_pr_timeout_sec=1234),
        ),
        _FakeRedis(),
        *_usage_providers(),
    )
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001"
    )
    asyncio.run(runner.handle_coding())
    assert captured.get("timeout") == 1234


def test_handle_fix_does_not_forward_idle_timeout_as_cli_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FIX REVIEW should stay uncapped while the idle monitor enforces no-push timeouts."""
    _patch_subprocess(monkeypatch)
    captured: dict[str, Any] = {}

    async def fake_fix(
        path: str, model: str | None = None, timeout: int | None = None, **kwargs: object
    ) -> tuple[int, str, str]:
        captured["timeout"] = timeout
        return (0, "", "")

    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda *a, **kw: None,
    )

    runner = PipelineRunner(
        _repo_cfg(),
        AppConfig(
            repositories=[],
            daemon=DaemonConfig(fix_idle_timeout_sec=5),
        ),
        _FakeRedis(),
        *_usage_providers(),
    )
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_fix())
    assert captured.get("timeout") is None


def test_fix_idle_timeout_kills_on_no_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Claude task must be cancelled when no push is detected within idle limit."""
    _patch_subprocess(monkeypatch)

    async def fake_fix_hangs(
        path: str, model: str | None = None, timeout: int | None = None, **kwargs: object
    ) -> tuple[int, str, str]:
        await asyncio.Future()
        return (0, "", "")

    async def immediate_cancel_monitor(
        self: object,
        pr_number: int,
        idle_limit: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        idle_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)
        idle_flag["timed_out"] = True
        target.cancel()

    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix_hangs)
    monkeypatch.setattr(
        PipelineRunner, "_monitor_fix_idle", immediate_cancel_monitor
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda *a, **kw: None,
    )

    runner = PipelineRunner(
        _repo_cfg(),
        AppConfig(
            repositories=[],
            daemon=DaemonConfig(fix_idle_timeout_sec=5),
        ),
        _FakeRedis(),
        *_usage_providers(),
    )
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_fix())
    assert runner.state.state == PipelineState.ERROR
    assert "idle timeout" in (runner.state.error_message or "")


def test_fix_idle_timeout_resets_on_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Timer must reset when a push is detected, allowing Claude to finish."""
    _patch_subprocess(monkeypatch)

    async def fake_fix_quick(
        path: str, model: str | None = None, timeout: int | None = None, **kwargs: object
    ) -> tuple[int, str, str]:
        await asyncio.sleep(0)
        return (0, "", "")

    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix_quick)
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda *a, **kw: None,
    )

    runner = PipelineRunner(
        _repo_cfg(),
        AppConfig(
            repositories=[],
            daemon=DaemonConfig(fix_idle_timeout_sec=1800),
        ),
        _FakeRedis(),
        *_usage_providers(),
    )
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_fix())
    assert runner.state.state == PipelineState.WATCH


def test_fix_idle_timeout_monitor_resets_on_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Push detection resets the idle timer so a productive session is not killed."""
    _patch_subprocess(monkeypatch)

    push_detected = [False]

    async def monitor_with_push_then_finish(
        self: object,
        pr_number: int,
        idle_limit: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        idle_flag: dict[str, bool],
    ) -> None:
        push_detected[0] = True
        await asyncio.sleep(0)

    async def fake_fix_quick(
        path: str, model: str | None = None, timeout: int | None = None, **kwargs: object
    ) -> tuple[int, str, str]:
        await asyncio.sleep(0)
        return (0, "", "")

    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix_quick)
    monkeypatch.setattr(
        PipelineRunner, "_monitor_fix_idle", monitor_with_push_then_finish
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda *a, **kw: None,
    )

    runner = PipelineRunner(
        _repo_cfg(),
        AppConfig(
            repositories=[],
            daemon=DaemonConfig(fix_idle_timeout_sec=1800),
        ),
        _FakeRedis(),
        *_usage_providers(),
    )
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_fix())
    assert runner.state.state == PipelineState.WATCH
    assert push_detected[0]


def test_handle_watch_stale_feedback_still_times_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHANGES_REQUESTED + no new feedback must still escalate to HUNG when
    the review timeout has elapsed. Early-returning here would pin the
    runner in WATCH forever for a sticky historical CHANGES_REQUESTED."""
    last_push = datetime.now(timezone.utc) - timedelta(hours=2)
    pr = PRInfo(
        number=42,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=last_push,
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "_gh_api_paginated",
        lambda path: [],
    )
    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = _make_runner(review_timeout_min=30)
    runner._last_push_at = last_push
    runner._last_push_at_pr_number = pr.number
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    asyncio.run(runner.handle_watch())

    assert fix_called == []
    assert runner.state.state == PipelineState.HUNG


def test_handle_fix_records_last_push_at(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """handle_fix must set ``_last_push_at`` so the next handle_watch can
    compare Codex feedback against our actual push time, not GitHub's
    ``updatedAt`` (which advances every time Codex posts)."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "fix_review_async",
        _async_cli_result(0, "", ""),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda *a, **kw: None,
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=7, branch="pr-001")
    before = datetime.now(timezone.utc)
    asyncio.run(runner.handle_fix())
    after = datetime.now(timezone.utc)

    assert runner._last_push_at is not None
    assert before <= runner._last_push_at <= after


def test_rehydrate_last_push_at_from_head_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Restart recovery must seed ``_last_push_at`` from the head commit
    committer date so the stale-feedback guard does not immediately
    trigger FIX on the first post-restart cycle."""
    head_iso = "2026-04-14T20:00:00Z"
    monkeypatch.setattr(
        runner_module.github_client,
        "get_pr_metadata",
        lambda repo, number: {"author": "", "head_sha": "", "head_commit_date": head_iso},
    )
    runner = _make_runner()
    assert runner._last_push_at is None
    pr = PRInfo(number=99, branch="pr-001")
    runner._rehydrate_last_push_at(pr)
    assert runner._last_push_at is not None
    assert runner._last_push_at.isoformat() == "2026-04-14T20:00:00+00:00"


def test_rehydrate_last_push_at_no_fallback_to_last_activity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the head commit time can't be fetched, DO NOT fall back to
    pr.last_activity. That value is GitHub's ``updatedAt`` which advances
    on Codex comments, so using it could seed _last_push_at to AFTER a
    pending P1/P2 comment and silently skip the fix. Leaving it None
    lets handle_watch retry the rehydrate next cycle."""
    monkeypatch.setattr(
        runner_module.github_client,
        "get_pr_metadata",
        lambda repo, number: {"author": "", "head_sha": "", "head_commit_date": ""},
    )
    fallback = datetime(2026, 4, 1, 10, 0, tzinfo=timezone.utc)
    pr = PRInfo(number=99, branch="pr-001", last_activity=fallback)
    runner = _make_runner()
    runner._rehydrate_last_push_at(pr)
    assert runner._last_push_at is None


def test_handle_watch_retries_rehydrate_last_push_at(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If recover_state's rehydrate failed (e.g. transient API hiccup),
    handle_watch must retry so a stuck-None last_push_at doesn't stale-fix
    loop forever."""
    pr = PRInfo(
        number=42,
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_pr_metadata",
        lambda repo, number: {"author": "", "head_sha": "", "head_commit_date": "2026-04-14T18:00:00Z"},
    )

    runner = _make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    assert runner._last_push_at is None
    asyncio.run(runner.handle_watch())
    assert runner._last_push_at is not None
    assert runner._last_push_at.isoformat() == "2026-04-14T18:00:00+00:00"


def test_recover_state_rehydrates_last_push_at(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """recover_state must rehydrate _last_push_at when matching to an
    in-flight DOING task's open PR so the first post-restart handle_watch
    does not falsely fire handle_fix on pre-restart Codex feedback."""
    queue_text = (
        "## PR-001: t\n"
        "- Status: DOING\n"
        "- Tasks file: tasks/PR-001.md\n"
        "- Branch: pr-001\n"
    )

    def fake_git(repo_path: str, *args: str, **kw: Any) -> Any:
        if args[0] == "show":
            return _FakeCompletedProcess(
                args=["git", "show"], stdout=queue_text, returncode=0
            )
        return _FakeCompletedProcess(args=list(args), returncode=0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    head_iso = "2026-04-10T12:00:00Z"
    monkeypatch.setattr(
        runner_module.github_client,
        "get_pr_metadata",
        lambda repo, number: {"author": "", "head_sha": "", "head_commit_date": head_iso},
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [PRInfo(number=7, branch="pr-001")],
    )

    runner = _make_runner()
    assert runner._last_push_at is None
    asyncio.run(runner.recover_state())
    assert runner.state.state == PipelineState.WATCH
    assert runner._last_push_at is not None
    assert runner._last_push_at.isoformat() == "2026-04-10T12:00:00+00:00"


def test_rehydrate_replaces_last_push_at_on_different_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Switching from PR A's timestamp to PR B must unconditionally
    replace — the 'only update if newer' gate is safe only within one
    PR. A newer-timestamp leak from the previous PR would make legit
    feedback on the new PR look stale."""
    head_iso = "2026-04-10T12:00:00Z"
    monkeypatch.setattr(
        runner_module.github_client,
        "get_pr_metadata",
        lambda repo, number: {"author": "", "head_sha": "", "head_commit_date": head_iso},
    )
    runner = _make_runner()
    # Simulate a stale last_push_at from a previously-tracked PR (newer
    # timestamp than the new PR's head commit).
    runner._last_push_at = datetime(2026, 4, 20, tzinfo=timezone.utc)
    runner._last_push_at_pr_number = 999

    runner._rehydrate_last_push_at(PRInfo(number=42, branch="pr-new"))

    assert runner._last_push_at_pr_number == 42
    assert runner._last_push_at is not None
    assert runner._last_push_at.isoformat() == "2026-04-10T12:00:00+00:00"


def test_handle_watch_falls_through_for_fork_with_ci_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CI failure on a fork PR must NOT call handle_fix (which would no-op
    and create a skip loop). It must fall through to the waiting/timeout
    logic so the PR can escalate to HUNG."""
    past = datetime.now(timezone.utc) - timedelta(hours=2)
    pr = PRInfo(
        number=88,
        branch="fork:feature",
        ci_status=CIStatus.FAILURE,
        review_status=ReviewStatus.PENDING,
        last_activity=past,
        is_cross_repository=True,
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_pr_metadata",
        lambda repo, number: {"author": "", "head_sha": "", "head_commit_date": ""},
    )
    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = _make_runner(review_timeout_min=30)
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    asyncio.run(runner.handle_watch())

    assert fix_called == []
    assert runner.state.state == PipelineState.HUNG


def test_handle_watch_falls_through_for_fork_with_changes_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHANGES_REQUESTED on a fork PR must also fall through to timeout
    instead of being routed into handle_fix (even with fresh feedback)."""
    past = datetime.now(timezone.utc) - timedelta(hours=2)
    pr = PRInfo(
        number=88,
        branch="fork:feature",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=past,
        is_cross_repository=True,
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_pr_metadata",
        lambda repo, number: {"author": "", "head_sha": "", "head_commit_date": ""},
    )
    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = _make_runner(review_timeout_min=30)
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    asyncio.run(runner.handle_watch())

    assert fix_called == []
    assert runner.state.state == PipelineState.HUNG


def test_handle_watch_rehydrates_on_pr_number_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If handle_watch is tracking a different PR than the last-push
    number, rehydrate must fire even when _last_push_at is non-None —
    otherwise a transient rehydrate failure on the prior PR switch
    would keep the stale previous-PR timestamp forever."""
    head_iso = "2026-04-14T18:00:00Z"
    monkeypatch.setattr(
        runner_module.github_client,
        "get_pr_metadata",
        lambda repo, number: {"author": "", "head_sha": "", "head_commit_date": head_iso},
    )
    pr = PRInfo(
        number=55,
        branch="pr-new",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [pr],
    )

    runner = _make_runner()
    # Stale last_push_at from a previously-tracked PR (different number).
    runner._last_push_at = datetime(2026, 4, 20, tzinfo=timezone.utc)
    runner._last_push_at_pr_number = 999
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    asyncio.run(runner.handle_watch())

    assert runner._last_push_at_pr_number == 55
    assert runner._last_push_at is not None
    assert runner._last_push_at.isoformat() == "2026-04-14T18:00:00+00:00"


def test_rehydrate_clears_stale_on_mismatch_when_fetch_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On a PR-number mismatch with a failing commit-time fetch, the
    previous PR's stale timestamp must be cleared rather than carried
    over. Next cycle's handle_watch retries the rehydrate; in the
    meantime the None baseline lets _has_new_codex_feedback_since_last_push
    return True so one fix attempt can run."""
    monkeypatch.setattr(
        runner_module.github_client,
        "get_pr_metadata",
        lambda repo, number: {"author": "", "head_sha": "", "head_commit_date": ""},
    )
    runner = _make_runner()
    runner._last_push_at = datetime(2026, 4, 20, tzinfo=timezone.utc)
    runner._last_push_at_pr_number = 999

    runner._rehydrate_last_push_at(PRInfo(number=42, branch="pr-new"))

    assert runner._last_push_at is None
    assert runner._last_push_at_pr_number == 42


def test_check_rate_limit_blocks_when_limited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_check_rate_limit returns False when _rate_limited_until is in future."""
    _patch_subprocess(monkeypatch)
    runner = _make_runner()
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=10)

    assert asyncio.run(runner._check_rate_limit()) is False
    assert runner.state.rate_limited_until is not None


def test_check_rate_limit_allows_when_expired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_check_rate_limit returns True and clears when _rate_limited_until is past."""
    _patch_subprocess(monkeypatch)
    runner = _make_runner()
    runner.state.rate_limited_until = datetime.now(timezone.utc) - timedelta(minutes=1)

    assert asyncio.run(runner._check_rate_limit()) is True
    assert runner.state.rate_limited_until is None


def test_handle_coding_skips_when_rate_limited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """handle_coding returns early without calling claude_cli when rate-limited."""
    _patch_subprocess(monkeypatch)
    cli_calls: list[str] = []
    monkeypatch.setattr(
        claude_cli,
        "run_planned_pr_async",
        _async_cli_result_with_side_effect(cli_calls, "run_planned_pr_async", 0, "", ""),
    )
    runner = _make_runner()
    runner.state.state = PipelineState.CODING
    runner.state.current_task = QueueTask(
        pr_id="PR-099", title="test", branch="pr-099-test", status=TaskStatus.TODO
    )
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=10)

    asyncio.run(runner.handle_coding())

    assert cli_calls == []


def test_handle_error_skips_diagnose_for_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """handle_error skips diagnose_error when error contains 'rate limit'."""
    _patch_subprocess(monkeypatch)
    cli_calls: list[str] = []
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        _async_cli_result_with_side_effect(cli_calls, "diagnose", 0, "SKIP", ""),
    )
    runner = _make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "Claude rate limit exceeded"

    asyncio.run(runner.handle_error())

    assert cli_calls == []


def test_handle_error_skips_diagnose_for_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """handle_error skips diagnose_error when error contains 'timeout'."""
    _patch_subprocess(monkeypatch)
    cli_calls: list[str] = []
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        _async_cli_result_with_side_effect(cli_calls, "diagnose", 0, "SKIP", ""),
    )
    runner = _make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "Timeout waiting for response"

    asyncio.run(runner.handle_error())

    assert cli_calls == []


def test_handle_error_preserves_error_message_on_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When diagnose_error_async is rate-limited, error_message is preserved."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        _async_cli_result_with_side_effect([], "diagnose", 1, "", "Error: 429 Too Many Requests"),
    )
    runner = _make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "Build failed: missing dependency X"

    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.error_message == "Build failed: missing dependency X"


def test_handle_error_skips_ai_diagnosis_when_claude_session_is_limited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.config import CoderType
    from src.usage import UsageSnapshot

    cli_calls: list[str] = []
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        _async_cli_result_with_side_effect(cli_calls, "diagnose", 0, "SKIP", ""),
    )
    runner = _make_runner(coder=CoderType.CODEX)
    runner.app_config.daemon.rate_limit_session_pause_percent = 80
    runner._claude_usage_provider = _FakeUsageProvider(
        snapshot=UsageSnapshot(
            session_percent=90,
            session_resets_at=9999999999,
            weekly_percent=10,
            weekly_resets_at=9999999999,
            fetched_at=time.time(),
        )
    )
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "Build failed: missing dependency X"
    runner._error_diagnose_count = 2

    asyncio.run(runner.handle_error())

    assert cli_calls == []
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None
    assert runner.state.rate_limited_until is None
    assert runner._error_diagnose_count == 0
    assert any(
        e["event"] == "Skipping AI diagnosis: Claude rate limited"
        for e in runner.state.history
    )


def test_handle_error_honors_claude_rate_limit_when_active_coder_is_claude(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.usage import UsageSnapshot

    cli_calls: list[str] = []
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        _async_cli_result_with_side_effect(cli_calls, "diagnose", 0, "SKIP", ""),
    )
    runner = _make_runner()
    runner.app_config.daemon.rate_limit_session_pause_percent = 80
    runner._claude_usage_provider = _FakeUsageProvider(
        snapshot=UsageSnapshot(
            session_percent=90,
            session_resets_at=int(time.time()) + 3600,
            weekly_percent=10,
            weekly_resets_at=int(time.time()) + 86400,
            fetched_at=time.time(),
        )
    )
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "Build failed: missing dependency X"

    asyncio.run(runner.handle_error())

    assert cli_calls == []
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None
    assert runner.state.rate_limited_until is None
    assert runner._error_diagnose_count == 0
    assert any(
        e["event"] == "Skipping AI diagnosis: Claude rate limited"
        for e in runner.state.history
    )


def test_handle_error_skips_ai_diagnosis_when_claude_weekly_is_limited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.config import CoderType
    from src.usage import UsageSnapshot

    cli_calls: list[str] = []
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        _async_cli_result_with_side_effect(cli_calls, "diagnose", 0, "SKIP", ""),
    )
    runner = _make_runner(coder=CoderType.CODEX)
    runner.app_config.daemon.rate_limit_session_pause_percent = 80
    runner.app_config.daemon.rate_limit_weekly_pause_percent = 90
    runner._claude_usage_provider = _FakeUsageProvider(
        snapshot=UsageSnapshot(
            session_percent=20,
            session_resets_at=int(time.time()) + 3600,
            weekly_percent=95,
            weekly_resets_at=int(time.time()) + 86400,
            fetched_at=time.time(),
        )
    )
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "Build failed: missing dependency X"
    runner._error_diagnose_count = 2

    asyncio.run(runner.handle_error())

    assert cli_calls == []
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None
    assert runner.state.rate_limited_until is None
    assert runner._error_diagnose_count == 0
    assert any(
        e["event"] == "Skipping AI diagnosis: Claude rate limited"
        for e in runner.state.history
    )


def test_handle_error_soft_skip_caps_repeated_codex_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.config import CoderType
    from src.usage import UsageSnapshot

    cli_calls: list[str] = []
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        _async_cli_result_with_side_effect(cli_calls, "diagnose", 0, "SKIP", ""),
    )
    runner = _make_runner(coder=CoderType.CODEX)
    runner.app_config.daemon.rate_limit_session_pause_percent = 80
    runner._claude_usage_provider = _FakeUsageProvider(
        snapshot=UsageSnapshot(
            session_percent=90,
            session_resets_at=int(time.time()) + 3600,
            weekly_percent=10,
            weekly_resets_at=int(time.time()) + 86400,
            fetched_at=time.time(),
        )
    )

    for _ in range(3):
        runner.state.state = PipelineState.ERROR
        runner.state.error_message = "sync_to_main failed: auth denied"
        asyncio.run(runner.handle_error())
        assert runner.state.state == PipelineState.IDLE
        assert runner.state.error_message is None

    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "sync_to_main failed: auth denied"
    asyncio.run(runner.handle_error())

    assert cli_calls == []
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "sync_to_main failed: auth denied"
    assert any(
        "max soft-skip retries (3) reached" in e["event"]
        for e in runner.state.history
    )


def test_run_cycle_clears_soft_skip_budget_after_successful_non_error_cycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.config import CoderType
    from src.usage import UsageSnapshot

    cli_calls: list[str] = []
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        _async_cli_result_with_side_effect(cli_calls, "diagnose", 0, "SKIP", ""),
    )

    runner = _make_runner(coder=CoderType.CODEX)
    runner._recovered = True
    runner.app_config.daemon.rate_limit_session_pause_percent = 80
    runner._claude_usage_provider = _FakeUsageProvider(
        snapshot=UsageSnapshot(
            session_percent=90,
            session_resets_at=int(time.time()) + 3600,
            weekly_percent=10,
            weekly_resets_at=int(time.time()) + 86400,
            fetched_at=time.time(),
        )
    )

    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "sync_to_main failed: auth denied"
    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.IDLE
    assert runner._error_skip_count == 1
    assert runner._error_skip_active is True

    async def fake_handle_idle() -> None:
        runner.log_event("successful idle cycle")
        runner.state.state = PipelineState.IDLE

    async def fake_ensure_repo_cloned() -> None:
        return None

    monkeypatch.setattr(runner, "handle_idle", fake_handle_idle)
    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "preflight", lambda: True)

    asyncio.run(runner.run_cycle())

    assert runner._error_skip_count == 0
    assert runner._error_skip_context is None
    assert runner._error_skip_active is False

    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "sync_to_main failed: auth denied"
    asyncio.run(runner.handle_error())

    assert cli_calls == []
    assert runner.state.state == PipelineState.IDLE
    assert runner._error_skip_count == 1


def test_handle_paused_resumes_to_error_when_error_message_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Window expired with error_message set -> ERROR so fault is retried."""
    _patch_subprocess(monkeypatch)

    runner = _make_runner()
    runner.state.state = PipelineState.PAUSED
    runner.state.rate_limited_until = datetime.now(timezone.utc) - timedelta(minutes=1)
    runner.state.error_message = "Build failed: missing dependency X"
    runner.state.current_pr = PRInfo(number=50, branch="pr-050")
    runner.state.current_task = QueueTask(
        pr_id="PR-050", title="test", branch="pr-050", status=TaskStatus.DOING
    )

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "Build failed: missing dependency X"
    assert runner.state.rate_limited_until is None


def test_handle_paused_clears_legacy_rate_limit_error_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Window expired with rate-limit error_message -> clear msg, resume WATCH/IDLE (no deadlock)."""
    _patch_subprocess(monkeypatch)

    runner = _make_runner()
    runner.state.state = PipelineState.PAUSED
    runner.state.rate_limited_until = datetime.now(timezone.utc) - timedelta(minutes=1)
    runner.state.error_message = "API rate limit exceeded (429)"
    runner.state.current_pr = PRInfo(number=50, branch="pr-050")
    runner.state.current_task = QueueTask(
        pr_id="PR-050", title="test", branch="pr-050", status=TaskStatus.DOING
    )

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.error_message is None
    assert any("cleared legacy rate-limit" in e["event"] for e in runner.state.history)


def test_detect_rate_limit_sets_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_detect_rate_limit sets _rate_limited_until on rate limit signal."""
    _patch_subprocess(monkeypatch)
    runner = _make_runner()
    assert runner.state.rate_limited_until is None

    runner._detect_rate_limit("Error: 429 Too Many Requests")

    assert runner.state.rate_limited_until is not None
    assert runner.state.rate_limited_until > datetime.now(timezone.utc)
    expected_pause = timedelta(minutes=27)
    actual_pause = runner.state.rate_limited_until - datetime.now(timezone.utc)
    assert actual_pause > expected_pause - timedelta(seconds=5)


def test_detect_rate_limit_respects_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_detect_rate_limit triggers on usage percentage above threshold."""
    _patch_subprocess(monkeypatch)
    runner = _make_runner()
    runner.app_config.daemon.rate_limit_session_pause_percent = 80

    runner._detect_rate_limit("Warning: 75% of rate limit capacity used")
    assert runner.state.rate_limited_until is None

    runner._detect_rate_limit("Warning: 85% of rate limit capacity used")
    assert runner.state.rate_limited_until is not None


def test_detect_rate_limit_fixed_pause_duration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_detect_rate_limit always uses a fixed 30-minute cooldown."""
    _patch_subprocess(monkeypatch)
    runner = _make_runner()
    runner.app_config.daemon.rate_limit_session_pause_percent = 50

    runner._detect_rate_limit("Error: 429 Too Many Requests")

    assert runner.state.rate_limited_until is not None
    expected_pause = timedelta(minutes=30)
    actual_pause = runner.state.rate_limited_until - datetime.now(timezone.utc)
    assert actual_pause > expected_pause - timedelta(seconds=5)
    assert actual_pause < expected_pause + timedelta(seconds=5)


def test_detect_rate_limit_weekly_respects_weekly_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Weekly limit at 95% with weekly_threshold=100 should NOT trigger pause."""
    _patch_subprocess(monkeypatch)
    runner = _make_runner()
    runner.app_config.daemon.rate_limit_weekly_pause_percent = 100

    runner._detect_rate_limit("Warning: 95% of your weekly rate limit reached")
    assert runner.state.rate_limited_until is None


def test_detect_rate_limit_weekly_triggers_at_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Weekly limit at 95% with weekly_threshold=90 should trigger pause."""
    _patch_subprocess(monkeypatch)
    runner = _make_runner()
    runner.app_config.daemon.rate_limit_weekly_pause_percent = 90

    runner._detect_rate_limit("Warning: 95% of your weekly rate limit reached")
    assert runner.state.rate_limited_until is not None


def test_detect_rate_limit_session_respects_session_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Session limit at 95% with session_threshold=95 should trigger pause."""
    _patch_subprocess(monkeypatch)
    runner = _make_runner()
    runner.app_config.daemon.rate_limit_session_pause_percent = 95

    runner._detect_rate_limit("Warning: 95% of your session rate limit reached")
    assert runner.state.rate_limited_until is not None


def test_detect_rate_limit_429_always_pauses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """HTTP 429 triggers pause regardless of thresholds."""
    _patch_subprocess(monkeypatch)
    runner = _make_runner()
    runner.app_config.daemon.rate_limit_session_pause_percent = 100
    runner.app_config.daemon.rate_limit_weekly_pause_percent = 100

    runner._detect_rate_limit("Error: HTTP 429 Too Many Requests")
    assert runner.state.rate_limited_until is not None


def test_detect_rate_limit_log_identifies_limit_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Event log must distinguish session vs weekly rate limits."""
    _patch_subprocess(monkeypatch)

    runner1 = _make_runner()
    runner1.app_config.daemon.rate_limit_session_pause_percent = 80
    runner1._detect_rate_limit("Warning: 90% of your session rate limit reached")
    assert any("(session)" in e["event"] for e in runner1.state.history)

    runner2 = _make_runner()
    runner2.app_config.daemon.rate_limit_weekly_pause_percent = 80
    runner2._detect_rate_limit("Warning: 90% of your weekly rate limit reached")
    assert any("(weekly)" in e["event"] for e in runner2.state.history)


def test_handle_coding_uses_async(monkeypatch: pytest.MonkeyPatch) -> None:
    """handle_coding must call run_planned_pr_async, not the sync version."""
    _patch_subprocess(monkeypatch)
    async_calls: list[str] = []
    sync_calls: list[str] = []

    async def fake_async(
        path: str, model: str | None = None, timeout: int | None = None, **kwargs: object
    ) -> tuple[int, str, str]:
        async_calls.append(path)
        return (0, "ok", "")

    def fake_sync(
        path: str, model: str | None = None, timeout: int | None = None
    ) -> tuple[int, str, str]:
        sync_calls.append(path)
        return (0, "ok", "")

    monkeypatch.setattr(claude_cli, "run_planned_pr_async", fake_async)
    monkeypatch.setattr(claude_cli, "run_planned_pr", fake_sync)
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [PRInfo(number=1, branch="pr-001")],
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

    assert async_calls, "run_planned_pr_async must be called"
    assert not sync_calls, "sync run_planned_pr must NOT be called"


def test_handle_fix_uses_async(monkeypatch: pytest.MonkeyPatch) -> None:
    """handle_fix must call fix_review_async, not the sync version."""
    _patch_subprocess(monkeypatch)
    async_calls: list[str] = []
    sync_calls: list[str] = []

    async def fake_async(
        path: str, model: str | None = None, timeout: int | None = None, **kwargs: object
    ) -> tuple[int, str, str]:
        async_calls.append(path)
        return (0, "", "")

    def fake_sync(
        path: str, model: str | None = None, timeout: int | None = None
    ) -> tuple[int, str, str]:
        sync_calls.append(path)
        return (0, "", "")

    monkeypatch.setattr(claude_cli, "fix_review_async", fake_async)
    monkeypatch.setattr(claude_cli, "fix_review", fake_sync)
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda *a, **kw: None,
    )

    runner = _make_runner()
    runner.state.current_pr = PRInfo(number=10, branch="pr-001")
    asyncio.run(runner.handle_fix())

    assert async_calls, "fix_review_async must be called"
    assert not sync_calls, "sync fix_review must NOT be called"


def test_handle_coding_publishes_heartbeat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """publish_state is called during Claude CLI run via heartbeat task."""
    _patch_subprocess(monkeypatch)
    heartbeat_publishes: list[str] = []

    original_publish = PipelineRunner.publish_state

    async def counting_publish(self: Any) -> None:
        await original_publish(self)

    monkeypatch.setattr(PipelineRunner, "publish_state", counting_publish)

    cli_done = None

    async def slow_cli(
        path: str, model: str | None = None, timeout: int | None = None, **kwargs: object
    ) -> tuple[int, str, str]:
        nonlocal cli_done
        cli_done = asyncio.get_event_loop().create_future()
        await cli_done
        return (0, "ok", "")

    monkeypatch.setattr(claude_cli, "run_planned_pr_async", slow_cli)
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [PRInfo(number=1, branch="pr-001")],
    )
    monkeypatch.setattr(
        runner_module.github_client, "post_comment", lambda *a, **kw: None
    )

    async def fast_heartbeat(self: Any, label: str) -> None:
        while True:
            await asyncio.sleep(0.01)
            self.log_event(f"{label}...")
            heartbeat_publishes.append(label)
            await self.publish_state()

    monkeypatch.setattr(
        PipelineRunner, "_publish_while_waiting", fast_heartbeat
    )

    async def run() -> None:
        runner = _make_runner()
        runner.state.current_task = QueueTask(
            pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001"
        )
        task = asyncio.create_task(runner.handle_coding())
        await asyncio.sleep(0.05)
        cli_done.set_result(None)
        await task

    asyncio.run(run())

    assert len(heartbeat_publishes) >= 2, (
        f"Expected heartbeat to publish at least twice, got {len(heartbeat_publishes)}"
    )


def test_handle_fix_publishes_heartbeat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """publish_state is called during FIX REVIEW via heartbeat task."""
    _patch_subprocess(monkeypatch)
    heartbeat_publishes: list[str] = []

    original_publish = PipelineRunner.publish_state

    async def counting_publish(self: Any) -> None:
        await original_publish(self)

    monkeypatch.setattr(PipelineRunner, "publish_state", counting_publish)

    cli_done = None

    async def slow_cli(
        path: str, model: str | None = None, timeout: int | None = None, **kwargs: object
    ) -> tuple[int, str, str]:
        nonlocal cli_done
        cli_done = asyncio.get_event_loop().create_future()
        await cli_done
        return (0, "", "")

    monkeypatch.setattr(claude_cli, "fix_review_async", slow_cli)
    monkeypatch.setattr(
        runner_module.github_client, "post_comment", lambda *a, **kw: None
    )

    async def fast_heartbeat(self: Any, label: str) -> None:
        while True:
            await asyncio.sleep(0.01)
            self.log_event(f"{label}...")
            heartbeat_publishes.append(label)
            await self.publish_state()

    monkeypatch.setattr(
        PipelineRunner, "_publish_while_waiting", fast_heartbeat
    )

    async def run() -> None:
        runner = _make_runner()
        runner.state.current_pr = PRInfo(number=10, branch="pr-001")
        task = asyncio.create_task(runner.handle_fix())
        await asyncio.sleep(0.05)
        cli_done.set_result(None)
        await task

    asyncio.run(run())

    assert len(heartbeat_publishes) >= 2, (
        f"Expected heartbeat to publish at least twice, got {len(heartbeat_publishes)}"
    )


# --- _has_new_codex_feedback_since_last_push tests ---


def test_has_new_feedback_returns_true_for_any_codex_comment_after_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Codex comment without P1/P2 posted after _last_push_at -> True."""
    runner = _make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-fix")
    runner._last_push_at = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(
        runner_module.github_client,
        "_gh_api_paginated",
        lambda path: [
            {
                "user": {"login": "chatgpt-codex-bot"},
                "body": "Consider renaming this variable",
                "created_at": "2026-01-01T00:05:00Z",
            },
        ],
    )

    assert runner._has_new_codex_feedback_since_last_push() == FeedbackCheckResult.NEW


def test_has_new_feedback_returns_false_for_old_comments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Codex comment posted before _last_push_at -> False."""
    runner = _make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-fix")
    runner._last_push_at = datetime(2026, 1, 1, 1, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(
        runner_module.github_client,
        "_gh_api_paginated",
        lambda path: [
            {
                "user": {"login": "chatgpt-codex-bot"},
                "body": "Old feedback",
                "created_at": "2026-01-01T00:30:00Z",
            },
        ],
    )

    assert runner._has_new_codex_feedback_since_last_push() == FeedbackCheckResult.NONE


def test_has_new_feedback_ignores_non_codex_users(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A comment from a regular user after _last_push_at -> False."""
    runner = _make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-fix")
    runner._last_push_at = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(
        runner_module.github_client,
        "_gh_api_paginated",
        lambda path: [
            {
                "user": {"login": "some-reviewer"},
                "body": "Please fix this",
                "created_at": "2026-01-01T00:05:00Z",
            },
        ],
    )

    assert runner._has_new_codex_feedback_since_last_push() == FeedbackCheckResult.NONE


def test_feedback_check_returns_unknown_on_api_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GitHub API failure during feedback check returns UNKNOWN, not NEW."""
    runner = _make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-fix")
    runner._last_push_at = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    def _raise(path: str) -> list:
        raise RuntimeError("GitHub API unavailable")

    monkeypatch.setattr(
        runner_module.github_client,
        "_gh_api_paginated",
        _raise,
    )

    assert runner._has_new_codex_feedback_since_last_push() == FeedbackCheckResult.UNKNOWN


def test_handle_watch_stays_in_watch_on_unknown_feedback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHANGES_REQUESTED + UNKNOWN feedback check -> stay in WATCH, no FIX."""
    last_push = datetime.now(timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=last_push,
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [pr],
    )

    def _raise(path: str) -> list:
        raise RuntimeError("GitHub API unavailable")

    monkeypatch.setattr(
        runner_module.github_client,
        "_gh_api_paginated",
        _raise,
    )
    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = _make_runner()
    runner._last_push_at = last_push
    runner._last_push_at_pr_number = pr.number
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    asyncio.run(runner.handle_watch())

    assert fix_called == []
    assert runner.state.state == PipelineState.WATCH
    assert any(
        "feedback check failed" in e["event"]
        for e in runner.state.history
    )


def test_handle_watch_skips_hung_timeout_on_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHANGES_REQUESTED + UNKNOWN + elapsed > timeout_min -> stay WATCH, not HUNG.

    When the observation itself is unreliable we cannot trust the elapsed
    time either. The runner must stay in WATCH and retry next cycle.
    """
    last_push = datetime.now(timezone.utc) - timedelta(hours=2)
    pr = PRInfo(
        number=42,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=last_push,
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [pr],
    )

    def _raise(path: str) -> list:
        raise RuntimeError("GitHub API unavailable")

    monkeypatch.setattr(
        runner_module.github_client,
        "_gh_api_paginated",
        _raise,
    )
    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = _make_runner(review_timeout_min=30)
    runner._last_push_at = last_push
    runner._last_push_at_pr_number = pr.number
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    asyncio.run(runner.handle_watch())

    assert fix_called == []
    assert runner.state.state == PipelineState.WATCH
    assert any(
        "feedback check failed" in e["event"]
        for e in runner.state.history
    )


# ---------------------------------------------------------------------------
# PR-050: HEAD SHA verification after FIX
# ---------------------------------------------------------------------------


def test_handle_fix_skips_review_post_when_head_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-050: when FIX exits 0 but HEAD hasn't moved, handle_fix must
    skip push accounting and @codex review, returning to WATCH."""
    same_sha = "abc123"

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "rev-parse"] and "HEAD" in cmd:
            return _FakeCompletedProcess(
                args=cmd, stdout=f"{same_sha}\n", returncode=0
            )
        if cmd[:2] == ["git", "rev-list"]:
            return _FakeCompletedProcess(
                args=cmd, stdout="0\n", returncode=0
            )
        return _FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        claude_cli,
        "fix_review_async",
        _async_cli_result(0, "", ""),
    )
    posted: list[str] = []
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda *a, **kw: posted.append("posted"),
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=50, branch="pr-050")
    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 0
    assert posted == []
    assert any(
        "HEAD unchanged" in e["event"]
        for e in runner.state.history
    )


def test_handle_fix_counts_push_when_head_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-050: when HEAD moves after FIX, handle_fix must increment
    push_count, update _last_push_at, and post @codex review."""
    sha_before = "aaa111"
    sha_after = "bbb222"
    call_count = {"n": 0}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "rev-parse"] and "HEAD" in cmd:
            call_count["n"] += 1
            sha = sha_before if call_count["n"] == 1 else sha_after
            return _FakeCompletedProcess(
                args=cmd, stdout=f"{sha}\n", returncode=0
            )
        if cmd[:2] == ["git", "rev-list"]:
            return _FakeCompletedProcess(
                args=cmd, stdout="0\n", returncode=0
            )
        return _FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        claude_cli,
        "fix_review_async",
        _async_cli_result(0, "", ""),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda *a, **kw: None,
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=50, branch="pr-050")
    before = datetime.now(timezone.utc)
    asyncio.run(runner.handle_fix())
    after = datetime.now(timezone.utc)

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 1
    assert runner._last_push_at is not None
    assert before <= runner._last_push_at <= after


def test_handle_fix_error_on_rev_parse_after_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-050: if rev-parse fails after FIX, handle_fix must go to ERROR."""
    call_count = {"n": 0}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "rev-parse"] and "HEAD" in cmd:
            call_count["n"] += 1
            if call_count["n"] == 1:
                return _FakeCompletedProcess(
                    args=cmd, stdout="aaa111\n", returncode=0
                )
            # Second call: simulate failure
            raise subprocess.CalledProcessError(
                128, cmd, stderr="fatal: bad object HEAD"
            )
        if cmd[:2] == ["git", "rev-list"]:
            return _FakeCompletedProcess(
                args=cmd, stdout="0\n", returncode=0
            )
        return _FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        claude_cli,
        "fix_review_async",
        _async_cli_result(0, "", ""),
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=50, branch="pr-050")
    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.ERROR
    assert "rev-parse after fix" in (runner.state.error_message or "")


# ── PAUSED state tests ──────────────────────────────────────────────


def test_handle_coding_sets_paused_on_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CLI returns non-zero with rate limit stderr -> state = PAUSED, error_message = None."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_planned_pr_async",
        _async_cli_result(1, "", "Error: 429 Too Many Requests"),
    )

    runner = _make_runner()
    runner.state.state = PipelineState.CODING
    runner.state.current_task = QueueTask(
        pr_id="PR-099", title="test", branch="pr-099-test", status=TaskStatus.TODO
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.error_message is None
    assert runner.state.rate_limited_until is not None
    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-099",
            limit=1,
            repo_name=runner.name,
        )
    )
    assert len(recent) == 1
    assert recent[0].exit_reason == "rate_limit"


def test_handle_coding_saves_record_on_proactive_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    runner.state.state = PipelineState.CODING
    runner.state.current_task = QueueTask(
        pr_id="PR-099", title="test", branch="pr-099-test", status=TaskStatus.TODO
    )

    async def fake_check_rate_limit() -> bool:
        runner.state.state = PipelineState.PAUSED
        runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=10)
        return False

    monkeypatch.setattr(runner, "_check_rate_limit", fake_check_rate_limit)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.rate_limited_until is not None
    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-099",
            limit=1,
            repo_name=runner.name,
        )
    )
    assert len(recent) == 1
    assert recent[0].exit_reason == "rate_limit"
    assert recent[0].ended_at is not None
    assert recent[0].duration_ms is not None


def test_handle_fix_sets_paused_on_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CLI returns non-zero with rate limit stderr in fix path -> PAUSED."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "fix_review_async",
        _async_cli_result(1, "", "Error: 429 Too Many Requests"),
    )

    runner = _make_runner()
    runner.state.state = PipelineState.FIX
    runner.state.current_pr = PRInfo(number=50, branch="pr-050")

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.error_message is None
    assert runner.state.rate_limited_until is not None


def test_handle_coding_success_ignores_rate_limit_text_in_stderr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Successful coding runs must not convert informational stderr into PAUSED."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_planned_pr_async",
        _async_cli_result(0, "ok", "Error: 429 Too Many Requests"),
    )
    pr = PRInfo(number=42, branch="pr-001")
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [pr],
    )

    runner = _make_runner()
    runner._post_codex_review = lambda _pr_number: True  # type: ignore[method-assign]
    runner.state.state = PipelineState.CODING
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="test", branch="pr-001", status=TaskStatus.DOING
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.error_message is None
    assert runner.state.rate_limited_until is None


def test_handle_fix_success_ignores_rate_limit_text_in_stderr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Successful fix runs must not convert informational stderr into PAUSED."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "fix_review_async",
        _async_cli_result(0, "ok", "Error: 429 Too Many Requests"),
    )

    runner = _make_runner()
    runner._post_codex_review = lambda _pr_number: True  # type: ignore[method-assign]
    runner.state.state = PipelineState.FIX
    runner.state.current_pr = PRInfo(number=50, branch="pr-050")

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.error_message is None
    assert runner.state.rate_limited_until is None


def test_handle_paused_waits_when_window_active(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """rate_limited_until in future -> state stays PAUSED."""
    _patch_subprocess(monkeypatch)

    runner = _make_runner()
    runner.state.state = PipelineState.PAUSED
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=20)

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.rate_limited_until is not None


def test_handle_paused_resumes_to_watch_when_window_expires(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Window expired, current_pr and current_task match -> WATCH."""
    _patch_subprocess(monkeypatch)

    runner = _make_runner()
    runner.state.state = PipelineState.PAUSED
    runner.state.rate_limited_until = datetime.now(timezone.utc) - timedelta(minutes=1)
    runner.state.current_pr = PRInfo(number=50, branch="pr-050")
    runner.state.current_task = QueueTask(
        pr_id="PR-050", title="test", branch="pr-050", status=TaskStatus.DOING
    )

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.rate_limited_until is None


def test_handle_paused_resumes_to_idle_when_no_active_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Window expired, no current_pr -> IDLE."""
    _patch_subprocess(monkeypatch)

    runner = _make_runner()
    runner.state.state = PipelineState.PAUSED
    runner.state.rate_limited_until = datetime.now(timezone.utc) - timedelta(minutes=1)

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.rate_limited_until is None


def test_handle_paused_handles_missing_rate_limited_until(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """State PAUSED but rate_limited_until None -> IDLE with log."""
    _patch_subprocess(monkeypatch)

    runner = _make_runner()
    runner.state.state = PipelineState.PAUSED
    runner.state.rate_limited_until = None

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.IDLE
    assert any("PAUSED without rate_limited_until" in e["event"] for e in runner.state.history)


def test_paused_not_reset_by_transient_states(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """run_cycle with state=PAUSED does not reset to IDLE via transient check."""
    _patch_subprocess(monkeypatch)

    runner = _make_runner()
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.PAUSED
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=20)

    asyncio.run(runner.run_cycle())

    assert runner.state.state == PipelineState.PAUSED


def test_check_rate_limit_transitions_to_paused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """rate_limited_until set, state was CODING -> transitions to PAUSED on check."""
    _patch_subprocess(monkeypatch)

    runner = _make_runner()
    runner.state.state = PipelineState.CODING
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=10)

    result = asyncio.run(runner._check_rate_limit())

    assert result is False
    assert runner.state.state == PipelineState.PAUSED


def test_legacy_error_with_rate_limited_until_converts_to_paused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Legacy state=ERROR + rate_limited_until -> PAUSED during run_cycle dispatch."""
    _patch_subprocess(monkeypatch)

    runner = _make_runner()
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.ERROR
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=15)
    runner.state.error_message = "some real error"

    asyncio.run(runner.run_cycle())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.error_message == "some real error"
    assert any("Legacy ERROR" in e["event"] for e in runner.state.history)


# ── ErrorCategory / _classify_error ──────────────────────────────────


@pytest.mark.parametrize(
    "msg",
    ["rate limit exceeded", "429 Too Many Requests", "API rate limit hit"],
)
def test_classify_error_rate_limit(msg: str) -> None:
    assert _classify_error(msg) == ErrorCategory.RATE_LIMIT


@pytest.mark.parametrize(
    "msg",
    ["Timeout after 3600s", "network timeout", "claude CLI timeout after 900s"],
)
def test_classify_error_timeout(msg: str) -> None:
    assert _classify_error(msg) == ErrorCategory.TIMEOUT


@pytest.mark.parametrize(
    "msg",
    ["git push failed", "file not found", "Unknown error"],
)
def test_classify_error_other(msg: str) -> None:
    assert _classify_error(msg) == ErrorCategory.OTHER


def test_handle_error_timeout_has_distinct_log(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Timeout errors must produce a log mentioning 'timeout error', not 'rate-limit'."""
    called: list[bool] = []

    async def fake_diag(*a: Any, **kw: Any) -> tuple[int, str, str]:
        called.append(True)
        return (0, "SKIP", "")

    monkeypatch.setattr(claude_cli, "diagnose_error_async", fake_diag)
    runner = _make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "Timeout after 600s"
    asyncio.run(runner.handle_error())

    assert called == []
    log_msgs = [e["event"] for e in runner.state.history]
    assert any("timeout error" in m for m in log_msgs)
    assert not any("rate-limit" in m for m in log_msgs)


# --- retry integration tests (PR-054) ---


def test_sync_to_main_retries_fetch_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """sync_to_main retries git fetch on transient TimeoutExpired."""
    calls: list[tuple] = []

    def fake_git(repo_path: str, *args: str, **kw: Any) -> _FakeCompletedProcess:
        calls.append(args)
        if args[0] == "fetch" and len(calls) == 1:
            raise subprocess.TimeoutExpired(cmd=["git", "fetch"], timeout=60)
        return _FakeCompletedProcess(args=list(args), returncode=0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr("src.retry.time.sleep", lambda _: None)

    runner = _make_runner()
    runner.sync_to_main()

    fetch_calls = [c for c in calls if c[0] == "fetch"]
    assert len(fetch_calls) == 2


def test_sync_to_main_fails_after_retries_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """sync_to_main propagates RuntimeError after retries exhausted."""

    def fake_git(repo_path: str, *args: str, **kw: Any) -> _FakeCompletedProcess:
        if args[0] == "fetch":
            raise subprocess.TimeoutExpired(cmd=["git", "fetch"], timeout=60)
        return _FakeCompletedProcess(args=list(args), returncode=0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr("src.retry.time.sleep", lambda _: None)

    runner = _make_runner()
    with pytest.raises(RuntimeError, match="failed after 3 attempts"):
        runner.sync_to_main()


def test_git_checkout_does_not_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Local git operations (checkout) are NOT wrapped in retry."""
    calls: list[tuple] = []

    def fake_git(repo_path: str, *args: str, **kw: Any) -> _FakeCompletedProcess:
        calls.append(args)
        if args[0] == "checkout":
            raise subprocess.CalledProcessError(
                1, ["git", "checkout"], stderr="error: pathspec 'foo' did not match"
            )
        return _FakeCompletedProcess(args=list(args), returncode=0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr("src.retry.time.sleep", lambda _: None)

    runner = _make_runner()
    with pytest.raises(subprocess.CalledProcessError):
        runner.sync_to_main()

    checkout_calls = [c for c in calls if c[0] == "checkout"]
    assert len(checkout_calls) == 1


# ---------------------------------------------------------------------------
# Proactive usage check (PR-063)
# ---------------------------------------------------------------------------


def test_check_rate_limit_triggers_paused_when_session_over_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.usage import UsageSnapshot

    _patch_subprocess(monkeypatch)
    runner = _make_runner()
    snap = UsageSnapshot(
        session_percent=96,
        session_resets_at=9999999999,
        weekly_percent=50,
        weekly_resets_at=9999999999,
        fetched_at=0,
    )
    runner._claude_usage_provider = _FakeUsageProvider(snapshot=snap)
    runner.app_config.daemon.rate_limit_session_pause_percent = 95

    assert asyncio.run(runner._check_rate_limit()) is False
    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.rate_limited_until is not None


def test_check_rate_limit_triggers_paused_when_weekly_over_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.usage import UsageSnapshot

    _patch_subprocess(monkeypatch)
    runner = _make_runner()
    snap = UsageSnapshot(
        session_percent=50,
        session_resets_at=9999999999,
        weekly_percent=100,
        weekly_resets_at=9999999999,
        fetched_at=0,
    )
    runner._claude_usage_provider = _FakeUsageProvider(snapshot=snap)
    runner.app_config.daemon.rate_limit_weekly_pause_percent = 100

    assert asyncio.run(runner._check_rate_limit()) is False
    assert runner.state.state == PipelineState.PAUSED


def test_check_rate_limit_allows_cli_when_under_thresholds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.usage import UsageSnapshot

    _patch_subprocess(monkeypatch)
    runner = _make_runner()
    snap = UsageSnapshot(
        session_percent=50,
        session_resets_at=9999999999,
        weekly_percent=60,
        weekly_resets_at=9999999999,
        fetched_at=0,
    )
    runner._claude_usage_provider = _FakeUsageProvider(snapshot=snap)

    assert asyncio.run(runner._check_rate_limit()) is True


def test_check_rate_limit_fail_open_when_provider_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    runner = _make_runner()
    runner._claude_usage_provider = _FakeUsageProvider(snapshot=None)

    assert asyncio.run(runner._check_rate_limit()) is True


def test_check_rate_limit_invalidates_cache_after_pause_expires(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    runner = _make_runner()
    fake = _FakeUsageProvider(snapshot=None)
    runner._claude_usage_provider = fake
    runner.state.rate_limited_until = datetime.now(timezone.utc) - timedelta(minutes=1)

    asyncio.run(runner._check_rate_limit())
    assert fake._invalidated is True


def test_proactive_check_logs_degradation_at_10_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    runner = _make_runner()
    runner._claude_usage_provider = _FakeUsageProvider(snapshot=None, failures=10)

    result = asyncio.run(runner._proactive_usage_check())
    assert result is True
    assert any("degraded" in e.get("event", "").lower() for e in runner.state.history)


def test_rate_limited_until_uses_resets_at_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.usage import UsageSnapshot

    _patch_subprocess(monkeypatch)
    runner = _make_runner()
    resets_at = 1744824000
    snap = UsageSnapshot(
        session_percent=99,
        session_resets_at=resets_at,
        weekly_percent=50,
        weekly_resets_at=9999999999,
        fetched_at=0,
    )
    runner._claude_usage_provider = _FakeUsageProvider(snapshot=snap)
    runner.app_config.daemon.rate_limit_session_pause_percent = 95

    asyncio.run(runner._check_rate_limit())
    assert runner.state.rate_limited_until is not None
    assert int(runner.state.rate_limited_until.timestamp()) == resets_at


# ---- In-flight breach monitor tests ----


def test_monitor_inflight_breach_cancels_claude_task_on_marker(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Breach marker file triggers task cancellation and PAUSED state."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(breach_module, "_BREACH_DIR", str(tmp_path))
    monkeypatch.setattr(breach_module, "_BREACH_POLL_SEC", 0.05)

    breach_run_id = "test-breach-001"

    async def _run() -> None:
        runner = _make_runner()
        runner._claude_usage_provider = _FakeUsageProvider()

        cancelled = asyncio.Event()

        async def fake_cli_forever() -> tuple[int, str, str]:
            try:
                await asyncio.sleep(999)
            except asyncio.CancelledError:
                cancelled.set()
                raise
            return (0, "", "")

        task = asyncio.create_task(fake_cli_forever())
        breach_flag: dict[str, bool] = {"breached": False}
        monitor = asyncio.create_task(
            runner._monitor_inflight_breach(
                str(tmp_path), breach_run_id, task, breach_flag,
            )
        )

        # Write breach marker after a short delay
        await asyncio.sleep(0.1)
        marker = tmp_path / f"{breach_run_id}.breach"
        marker.write_text(json.dumps({
            "type": "session",
            "resets_at": 1700000000,
            "session_pct": 97,
            "weekly_pct": 30,
            "detected_at": 1234567890.0,
        }))

        # Wait for monitor to detect and cancel
        await asyncio.sleep(0.3)
        assert breach_flag["breached"] is True
        assert cancelled.is_set()
        assert runner.state.rate_limited_until is not None
        assert runner.state.rate_limit_reactive_coder == "claude"

        monitor.cancel()

    asyncio.run(_run())


def test_monitor_inflight_breach_sets_paused_state_with_resets_at(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Breach monitor sets rate_limited_until from breach marker resets_at."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(breach_module, "_BREACH_DIR", str(tmp_path))
    monkeypatch.setattr(breach_module, "_BREACH_POLL_SEC", 0.05)

    breach_run_id = "test-resets-at"

    async def _run() -> None:
        runner = _make_runner()
        runner._claude_usage_provider = _FakeUsageProvider()

        async def fake_cli() -> tuple[int, str, str]:
            await asyncio.sleep(999)
            return (0, "", "")

        task = asyncio.create_task(fake_cli())
        breach_flag: dict[str, bool] = {"breached": False}
        monitor = asyncio.create_task(
            runner._monitor_inflight_breach(
                str(tmp_path), breach_run_id, task, breach_flag,
            )
        )

        marker = tmp_path / f"{breach_run_id}.breach"
        marker.write_text(json.dumps({
            "type": "weekly",
            "resets_at": 1800000000,
            "session_pct": 50,
            "weekly_pct": 105,
            "detected_at": 1234567890.0,
        }))

        await asyncio.sleep(0.3)
        assert runner.state.rate_limited_until is not None
        assert int(runner.state.rate_limited_until.timestamp()) == 1800000000

        monitor.cancel()

    asyncio.run(_run())


def test_monitor_inflight_breach_exits_when_claude_task_completes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Monitor exits cleanly when the CLI task completes without a breach."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(breach_module, "_BREACH_DIR", str(tmp_path))
    monkeypatch.setattr(breach_module, "_BREACH_POLL_SEC", 0.05)

    breach_run_id = "test-no-breach"

    async def _run() -> None:
        runner = _make_runner()
        runner._claude_usage_provider = _FakeUsageProvider()

        async def fake_cli_quick() -> tuple[int, str, str]:
            await asyncio.sleep(0.05)
            return (0, "done", "")

        task = asyncio.create_task(fake_cli_quick())
        breach_flag: dict[str, bool] = {"breached": False}
        monitor = asyncio.create_task(
            runner._monitor_inflight_breach(
                str(tmp_path), breach_run_id, task, breach_flag,
            )
        )

        await task
        # Give monitor a moment to notice the task is done
        await asyncio.sleep(0.2)

        assert breach_flag["breached"] is False
        assert runner.state.rate_limited_until is None

        monitor.cancel()

    asyncio.run(_run())


def test_handle_coding_cleans_up_breach_marker_after_run(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Late breach marker is detected, causes PAUSED, and is cleaned up."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(breach_module, "_BREACH_DIR", str(tmp_path))

    captured_run_id: list[str] = []

    async def fake_planned(
        path: str, model: str | None = None, timeout: int | None = None, **kwargs: object
    ) -> tuple[int, str, str]:
        run_id = kwargs.get("breach_run_id", "")
        if run_id:
            captured_run_id.append(run_id)
            # Simulate breach marker written by hook near end of CLI run
            marker = tmp_path / f"{run_id}.breach"
            marker.write_text('{"type":"session","resets_at":0}')
        return (0, "ok", "")

    monkeypatch.setattr(claude_cli, "run_planned_pr_async", fake_planned)
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [PRInfo(number=1, branch="pr-001")],
    )
    monkeypatch.setattr(
        runner_module.github_client, "post_comment", lambda *a, **kw: None,
    )

    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001",
    )
    asyncio.run(runner.handle_coding())

    # The marker should have been cleaned up
    assert captured_run_id
    marker = tmp_path / f"{captured_run_id[0]}.breach"
    assert not marker.exists()
    # Late breach detection should have paused the runner
    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.rate_limited_until is not None


def test_handle_coding_pauses_on_inflight_breach(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """handle_coding transitions to PAUSED when in-flight breach is detected."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(breach_module, "_BREACH_DIR", str(tmp_path))
    monkeypatch.setattr(breach_module, "_BREACH_POLL_SEC", 0.05)

    async def fake_planned_hangs(
        path: str, model: str | None = None, timeout: int | None = None, **kwargs: object
    ) -> tuple[int, str, str]:
        run_id = kwargs.get("breach_run_id", "")
        # Write breach marker immediately to simulate mid-flight breach
        if run_id:
            marker = tmp_path / f"{run_id}.breach"
            marker.write_text(json.dumps({
                "type": "session",
                "resets_at": 1700000000,
                "session_pct": 98,
                "weekly_pct": 30,
                "detected_at": 1234567890.0,
            }))
        await asyncio.sleep(999)  # Block until cancelled
        return (0, "", "")

    monkeypatch.setattr(claude_cli, "run_planned_pr_async", fake_planned_hangs)

    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001",
    )
    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.rate_limited_until is not None
    assert runner.state.error_message is None


# -----------------------------------------------------------------------
# PR-065: Coder selection tests
# -----------------------------------------------------------------------


def test_get_coder_returns_claude_by_default() -> None:
    runner = _make_runner()
    name, plugin = runner._get_coder()
    assert name == "claude"
    assert plugin.name == "claude"


def test_get_coder_returns_codex_when_configured() -> None:
    from src.config import CoderType
    runner = _make_runner()
    runner._app_config = _app_cfg(coder=CoderType.CODEX)
    name, plugin = runner._get_coder()
    assert name == "codex"
    assert plugin.name == "codex"


def test_get_coder_repo_override_takes_precedence() -> None:
    from src.config import CoderType
    runner = _make_runner(coder=CoderType.CODEX)
    # Daemon default is claude, repo override is codex
    name, plugin = runner._get_coder()
    assert name == "codex"
    assert plugin.name == "codex"


def test_handle_coding_uses_codex_cli_when_coder_is_codex(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src import codex_cli
    from src.config import CoderType

    _patch_subprocess(monkeypatch)
    captured_module: list[str] = []

    async def fake_run_planned_pr(path: str, **kwargs: object) -> tuple:
        captured_module.append("codex")
        return (0, "ok", "")

    monkeypatch.setattr(codex_cli, "run_planned_pr_async", fake_run_planned_pr)
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda *a, **kw: [PRInfo(
            number=42,
            url="https://github.com/octo/demo/pull/42",
            branch="pr-001",
            ci_status=CIStatus.PENDING,
            review_status=ReviewStatus.PENDING,
        )],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda *a, **kw: True,
    )

    runner = _make_runner(coder=CoderType.CODEX)
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001",
    )
    asyncio.run(runner.handle_coding())

    assert captured_module == ["codex"]
    assert runner.state.state == PipelineState.WATCH


def test_handle_fix_uses_codex_cli_when_coder_is_codex(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src import codex_cli
    from src.config import CoderType

    _patch_subprocess(monkeypatch)
    captured_module: list[str] = []

    async def fake_fix_review(path: str, **kwargs: object) -> tuple:
        captured_module.append("codex")
        return (0, "ok", "")

    monkeypatch.setattr(codex_cli, "fix_review_async", fake_fix_review)
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda *a, **kw: True,
    )

    runner = _make_runner(coder=CoderType.CODEX)
    runner.state.current_pr = PRInfo(
        number=42,
        url="https://github.com/octo/demo/pull/42",
        branch="fix-branch",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.CHANGES_REQUESTED,
    )
    asyncio.run(runner.handle_fix())

    assert captured_module == ["codex"]


def test_event_log_includes_coder_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src import codex_cli
    from src.config import CoderType

    _patch_subprocess(monkeypatch)

    async def fake_run_planned_pr(path: str, **kwargs: object) -> tuple:
        return (0, "ok", "")

    monkeypatch.setattr(codex_cli, "run_planned_pr_async", fake_run_planned_pr)
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda *a, **kw: [PRInfo(
            number=42,
            url="https://github.com/octo/demo/pull/42",
            branch="pr-001",
            ci_status=CIStatus.PENDING,
            review_status=ReviewStatus.PENDING,
        )],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda *a, **kw: True,
    )

    runner = _make_runner(coder=CoderType.CODEX)
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001",
    )
    asyncio.run(runner.handle_coding())

    events = [h["event"] for h in runner.state.history]
    assert any("[codex]" in e for e in events)


def test_check_rate_limit_runs_proactive_for_codex(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Proactive usage check now runs for Codex too (OpenAI provider)."""
    from src.config import CoderType

    runner = _make_runner(coder=CoderType.CODEX)

    proactive_called = []

    async def fake_proactive(*a: object, **kw: object) -> bool:
        proactive_called.append(True)
        return True

    monkeypatch.setattr(runner, "_proactive_usage_check", fake_proactive)

    result = asyncio.run(runner._check_rate_limit())
    assert result is True
    assert proactive_called == [True]


def test_check_rate_limit_codex_clears_proactive_claude_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex clears a proactive Claude pause for general work."""
    from src.config import CoderType

    _patch_subprocess(monkeypatch)
    runner = _make_runner(coder=CoderType.CODEX)
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=10)
    runner.state.rate_limit_reactive = False
    runner.state.rate_limit_reactive_coder = "claude"
    runner.state.state = PipelineState.PAUSED

    result = asyncio.run(runner._check_rate_limit())
    assert result is True
    assert runner.state.rate_limited_until is None
    assert runner.state.state == PipelineState.IDLE


def test_check_rate_limit_honors_claude_pause_with_proactive_coder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """proactive_coder='claude' honors a Claude pause (merge/diagnosis)."""
    from src.config import CoderType

    _patch_subprocess(monkeypatch)
    runner = _make_runner(coder=CoderType.CODEX)
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=10)
    runner.state.rate_limit_reactive = False
    runner.state.rate_limit_reactive_coder = "claude"
    runner.state.state = PipelineState.PAUSED

    result = asyncio.run(runner._check_rate_limit(proactive_coder="claude"))
    assert result is False
    assert runner.state.rate_limited_until is not None
    assert runner.state.state == PipelineState.PAUSED


def test_check_rate_limit_codex_clears_reactive_claude_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex clears a reactive (stderr-detected) Claude pause."""
    from src.config import CoderType

    _patch_subprocess(monkeypatch)
    runner = _make_runner(coder=CoderType.CODEX)
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=10)
    runner.state.rate_limit_reactive = True
    runner.state.rate_limit_reactive_coder = "claude"
    runner.state.state = PipelineState.PAUSED

    result = asyncio.run(runner._check_rate_limit())
    assert result is True
    assert runner.state.rate_limited_until is None
    assert runner.state.rate_limit_reactive is False
    assert runner.state.state == PipelineState.IDLE


def test_check_rate_limit_codex_honors_reactive_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex respects a reactive (stderr-detected) rate-limit pause."""
    from src.config import CoderType

    _patch_subprocess(monkeypatch)
    runner = _make_runner(coder=CoderType.CODEX)
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=10)
    runner.state.rate_limit_reactive = True
    runner.state.rate_limit_reactive_coder = CoderType.CODEX.value

    result = asyncio.run(runner._check_rate_limit())
    assert result is False
    assert runner.state.rate_limited_until is not None


# ---------- Codex-specific rate limit detection tests ----------


def test_detect_rate_limit_codex_try_again_pattern(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex 'try again in X days Y hours Z minutes' -> exact pause, weekly."""
    from src.config import CoderType

    _patch_subprocess(monkeypatch)
    runner = _make_runner(coder=CoderType.CODEX)
    runner._detect_rate_limit(
        "You've hit your usage limit. Upgrade to Pro or try again in 3 days 13 hours 6 minutes.",
        coder_name="codex",
    )
    assert runner.state.rate_limited_until is not None
    assert runner.state.rate_limit_reactive_coder == "codex"
    expected_pause = timedelta(minutes=3 * 1440 + 13 * 60 + 6)
    actual_pause = runner.state.rate_limited_until - datetime.now(timezone.utc)
    assert actual_pause > expected_pause - timedelta(seconds=10)
    assert any("(weekly)" in e["event"] for e in runner.state.history)


def test_detect_rate_limit_codex_session_pattern(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex 'try again in 4 hours 32 minutes' -> session, exact pause."""
    from src.config import CoderType

    _patch_subprocess(monkeypatch)
    runner = _make_runner(coder=CoderType.CODEX)
    runner._detect_rate_limit(
        "You've hit your usage limit. Try again in 4 hours 32 minutes.",
        coder_name="codex",
    )
    assert runner.state.rate_limited_until is not None
    assert runner.state.rate_limit_reactive_coder == "codex"
    expected_pause = timedelta(minutes=4 * 60 + 32)
    actual_pause = runner.state.rate_limited_until - datetime.now(timezone.utc)
    assert actual_pause > expected_pause - timedelta(seconds=10)
    assert any("(session)" in e["event"] for e in runner.state.history)


def test_detect_rate_limit_codex_hit_limit_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex 'You've hit your usage limit' (no retry info) triggers pause."""
    from src.config import CoderType

    _patch_subprocess(monkeypatch)
    runner = _make_runner(coder=CoderType.CODEX)
    runner._detect_rate_limit(
        "You've hit your usage limit.",
        coder_name="codex",
    )
    assert runner.state.rate_limited_until is not None
    assert runner.state.rate_limit_reactive_coder == "codex"


def test_detect_rate_limit_codex_429(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Generic 429 triggers rate limit for Codex coder."""
    from src.config import CoderType

    _patch_subprocess(monkeypatch)
    runner = _make_runner(coder=CoderType.CODEX)
    runner._detect_rate_limit("Error: 429 Too Many Requests", coder_name="codex")
    assert runner.state.rate_limited_until is not None
    assert runner.state.rate_limit_reactive_coder == "codex"


def test_detect_rate_limit_codex_retry_seconds_pattern(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex retry text with seconds should trigger a minimum one-minute pause."""
    from src.config import CoderType

    _patch_subprocess(monkeypatch)
    runner = _make_runner(coder=CoderType.CODEX)
    runner._detect_rate_limit(
        "Rate limit reached. Please try again in 6.379s",
        coder_name="codex",
    )
    assert runner.state.rate_limited_until is not None
    assert runner.state.rate_limit_reactive_coder == "codex"
    expected_pause = timedelta(minutes=1)
    actual_pause = runner.state.rate_limited_until - datetime.now(timezone.utc)
    assert actual_pause > expected_pause - timedelta(seconds=5)
    assert actual_pause < expected_pause + timedelta(seconds=5)


def test_detect_rate_limit_anthropic_regex_does_not_fallback_for_codex(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Anthropic-style percentage text must not pause when stderr came from Codex."""
    from src.config import CoderType

    _patch_subprocess(monkeypatch)
    runner = _make_runner(coder=CoderType.CODEX)
    runner.app_config.daemon.rate_limit_session_pause_percent = 80
    runner._detect_rate_limit(
        "Warning: 95% of session rate limit reached",
        coder_name="codex",
    )
    assert runner.state.rate_limited_until is None
    assert runner.state.rate_limit_reactive_coder is None


def test_detect_rate_limit_codex_progress_output_does_not_trigger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex progress output mentioning rate limit must not pause."""
    from src.config import CoderType

    _patch_subprocess(monkeypatch)
    runner = _make_runner(coder=CoderType.CODEX)
    runner._detect_rate_limit(
        "Progress update: 87% remaining for weekly rate limit",
        coder_name="codex",
    )
    assert runner.state.rate_limited_until is None
    assert runner.state.rate_limit_reactive_coder is None


def test_detect_rate_limit_codex_progress_output_zero_remaining_does_not_trigger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex progress output still does not pause without a confirmed error pattern."""
    from src.config import CoderType

    _patch_subprocess(monkeypatch)
    runner = _make_runner(coder=CoderType.CODEX)
    runner._detect_rate_limit(
        "Progress update: 0% remaining for weekly rate limit",
        coder_name="codex",
    )
    assert runner.state.rate_limited_until is None
    assert runner.state.rate_limit_reactive_coder is None


def test_detect_rate_limit_codex_error_fallback_without_parseable_duration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex unmatched retry text should still pause on concrete rate-limit failures."""
    from src.config import CoderType

    _patch_subprocess(monkeypatch)
    runner = _make_runner(coder=CoderType.CODEX)
    runner._detect_rate_limit(
        "Rate limit reached. Please try again later.",
        coder_name="codex",
    )
    assert runner.state.rate_limited_until is not None
    assert runner.state.rate_limit_reactive_coder == "codex"


def test_detect_rate_limit_codex_progress_output_with_seconds_retry_triggers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex progress mixed with a confirmed seconds retry should still pause."""
    from src.config import CoderType

    _patch_subprocess(monkeypatch)
    runner = _make_runner(coder=CoderType.CODEX)
    runner._detect_rate_limit(
        "Progress update: 87% remaining for weekly rate limit\n"
        "Rate limit reached. Please try again in 6.379s",
        coder_name="codex",
    )
    assert runner.state.rate_limited_until is not None
    assert runner.state.rate_limit_reactive_coder == "codex"


def test_detect_rate_limit_generic_fallback_still_applies_to_claude(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Claude keeps the generic rate-limit fallback when no specific regex matches."""
    _patch_subprocess(monkeypatch)
    runner = _make_runner()
    runner._detect_rate_limit("Warning: API rate limit reached", coder_name="claude")
    assert runner.state.rate_limited_until is not None
    assert runner.state.rate_limit_reactive_coder == "claude"


def test_proactive_check_uses_codex_provider_when_coder_is_codex(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_proactive_usage_check should use codex provider for codex coder."""
    from src.config import CoderType
    from src.usage import UsageSnapshot

    _patch_subprocess(monkeypatch)
    runner = _make_runner(coder=CoderType.CODEX)
    runner.app_config.daemon.rate_limit_session_pause_percent = 80
    snap = UsageSnapshot(
        session_percent=90,
        session_resets_at=int(time.time()) + 3600,
        weekly_percent=10,
        weekly_resets_at=int(time.time()) + 86400,
        fetched_at=time.time(),
    )
    runner._codex_usage_provider = _FakeUsageProvider(snapshot=snap)
    runner._claude_usage_provider = _FakeUsageProvider(snapshot=None)

    result = asyncio.run(runner._proactive_usage_check())
    assert result is False
    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.rate_limit_reactive_coder == "codex"


def test_proactive_check_uses_claude_provider_when_coder_is_claude(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_proactive_usage_check should use claude provider for claude coder."""
    from src.usage import UsageSnapshot

    _patch_subprocess(monkeypatch)
    runner = _make_runner()
    runner.app_config.daemon.rate_limit_session_pause_percent = 80
    snap = UsageSnapshot(
        session_percent=90,
        session_resets_at=int(time.time()) + 3600,
        weekly_percent=10,
        weekly_resets_at=int(time.time()) + 86400,
        fetched_at=time.time(),
    )
    runner._claude_usage_provider = _FakeUsageProvider(snapshot=snap)
    runner._codex_usage_provider = _FakeUsageProvider(snapshot=None)

    result = asyncio.run(runner._proactive_usage_check())
    assert result is False
    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.rate_limit_reactive_coder == "claude"
