"""Tests for src/daemon/runner.py."""

from __future__ import annotations

import asyncio
import contextlib
import json
import random
import re
import subprocess
import time
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from src import codex_cli
from src import retry as retry_module
from src.coder_registry import CoderRegistry
from src.coders import claude as claude_plugin_module
from src.config import AppConfig, CoderType, DaemonConfig, RepoConfig
from src.daemon import git_ops as git_ops_module
from src.daemon import recovery_policy as recovery_policy_module
from src.daemon import runner as runner_module
from src.daemon import selector as selector_module
from src.daemon.handlers import breach as breach_module
from src.daemon.handlers import coding as coding_module
from src.daemon.handlers import error as error_module
from src.daemon.handlers import fix as fix_module
from src.daemon.handlers import hung as hung_module
from src.daemon.handlers import idle as idle_module
from src.daemon.handlers import merge as merge_module
from src.daemon.handlers import watch as watch_module
from src.daemon.runner import ErrorCategory, PipelineRunner, _classify_error
from src.models import (
    CIStatus,
    FeedbackCheckResult,
    PipelineState,
    PRInfo,
    QueueTask,
    RepoState,
    ReviewStatus,
    TaskStatus,
)
from src.queue_parser import QueueValidationError, TaskHeader

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


def _raise_runtime_error(message: str):
    raise RuntimeError(message)


def _raise_cycle_detected(headers: object, statuses: object):
    raise ValueError("cycle detected")


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

    async def exists(self, key: str) -> int:
        return int(key in self.store)

    async def delete(self, key: str) -> int:
        self.deleted.append(key)
        if key in self.store:
            del self.store[key]
            return 1
        return 0

    async def renamenx(self, old: str, new: str) -> int:
        if old not in self.store or new in self.store:
            return 0
        self.store[new] = self.store.pop(old)
        return 1

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

    async def publish(self, key: str, value: str) -> int:
        return 1

    async def transaction(
        self,
        func,
        *watches: str,
        value_from_callable: bool = False,
        **_kwargs: object,
    ):
        pipe = _FakePipeline(self)
        func_value = func(pipe)
        if asyncio.iscoroutine(func_value):
            func_value = await func_value
        exec_value = await pipe.execute()
        if value_from_callable:
            return func_value
        return exec_value


class _FakePipeline:
    def __init__(self, redis: _FakeRedis) -> None:
        self.redis = redis
        self.commands: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    async def get(self, key: str) -> str | None:
        return self.redis.store.get(key)

    def multi(self) -> None:
        return None

    def set(self, key: str, value: str, **kwargs: object) -> "_FakePipeline":
        self.commands.append(("set", (key, value), kwargs))
        return self

    async def execute(self) -> list[object]:
        results: list[object] = []
        for command, args, kwargs in self.commands:
            if command == "set":
                await self.redis.set(args[0], args[1], **kwargs)
                results.append(True)
        return results


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
    runner = PipelineRunner(
        _repo_cfg(**repo_overrides),
        _app_cfg(),
        _FakeRedis(),
        claude_provider,
        codex_provider,
    )
    runner._selector_rng.seed(0)
    runner._auth_status_cache = {
        "claude": {"status": "ok"},
        "codex": {"status": "ok"},
    }
    runner._auth_status_cache_expires_at = (
        datetime.now(timezone.utc) + timedelta(minutes=5)
    )
    return runner


def test_reload_repo_config_if_dirty_updates_coder_at_idle_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    runner.redis.store["control:octo__demo:config_dirty"] = "1"
    runner.repo_config = RepoConfig.model_validate(
        {**runner.repo_config.model_dump(), "coder": None}
    )

    reloaded = AppConfig(
        repositories=[
            RepoConfig.model_validate(
                {**runner.repo_config.model_dump(), "coder": "codex"}
            )
        ],
        daemon=runner.app_config.daemon,
    )
    monkeypatch.setattr(runner_module, "load_config", lambda path="config.yml": reloaded)

    asyncio.run(runner.reload_repo_config_if_dirty())

    assert runner.repo_config.coder == CoderType.CODEX
    assert "control:octo__demo:config_dirty" not in runner.redis.store
    assert runner.state.history[-1]["event"] == "Reloaded repo config from config.yml"


def test_staged_config_reload_waits_until_idle_boundary() -> None:
    runner = _make_runner()
    original_coder = runner.repo_config.coder
    next_repo_config = RepoConfig.model_validate(
        {**runner.repo_config.model_dump(), "coder": "codex"}
    )
    next_app_config = AppConfig(
        repositories=[next_repo_config],
        daemon=runner.app_config.daemon,
    )
    next_claude = _FakeUsageProvider(snapshot="new-claude")
    next_codex = _FakeUsageProvider(snapshot="new-codex")

    runner.state.state = PipelineState.WATCH
    runner.stage_config_reload(
        next_repo_config,
        next_app_config,
        next_claude,
        next_codex,
    )

    assert runner.repo_config.coder == original_coder
    assert runner._pending_repo_config is not None

    runner.state.state = PipelineState.IDLE
    asyncio.run(runner.reload_repo_config_if_dirty())

    assert runner.repo_config.coder == CoderType.CODEX
    assert runner.app_config is next_app_config
    assert runner._pending_repo_config is None
    assert runner._claude_usage_provider is next_claude
    assert runner._codex_usage_provider is next_codex


def test_reload_repo_config_if_dirty_clears_missing_repo_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    runner.redis.store["control:octo__demo:config_dirty"] = "1"
    next_repo_config = RepoConfig.model_validate(
        {**runner.repo_config.model_dump(), "coder": "codex"}
    )
    next_app_config = AppConfig(
        repositories=[next_repo_config],
        daemon=runner.app_config.daemon,
    )
    runner.stage_config_reload(
        next_repo_config,
        next_app_config,
        _FakeUsageProvider(snapshot="new-claude"),
        _FakeUsageProvider(snapshot="new-codex"),
    )
    monkeypatch.setattr(
        runner_module,
        "load_config",
        lambda path="config.yml": AppConfig(
            repositories=[],
            daemon=runner.app_config.daemon,
        ),
    )

    asyncio.run(runner.reload_repo_config_if_dirty())

    assert "control:octo__demo:config_dirty" not in runner.redis.store
    assert runner.repo_config.coder == CoderType.CODEX


def test_reload_repo_config_if_dirty_supports_redis_without_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()

    class _GetOnlyRedis:
        def __init__(self, store: dict[str, str]) -> None:
            self.store = store

        async def get(self, key: str) -> str | None:
            return self.store.get(key)

        async def delete(self, key: str) -> int:
            existed = key in self.store
            self.store.pop(key, None)
            return int(existed)

    runner.redis = _GetOnlyRedis({"control:octo__demo:config_dirty": "1"})  # type: ignore[assignment]
    reloaded = AppConfig(
        repositories=[
            RepoConfig.model_validate(
                {**runner.repo_config.model_dump(), "coder": "codex"}
            )
        ],
        daemon=runner.app_config.daemon,
    )
    monkeypatch.setattr(runner_module, "load_config", lambda path="config.yml": reloaded)

    asyncio.run(runner.reload_repo_config_if_dirty())

    assert runner.repo_config.coder == CoderType.CODEX


def test_reload_repo_config_if_dirty_applies_staged_reload_when_redis_unavailable() -> None:
    runner = _make_runner()
    next_repo_config = RepoConfig.model_validate(
        {**runner.repo_config.model_dump(), "coder": "codex"}
    )
    runner.stage_config_reload(
        next_repo_config,
        AppConfig(repositories=[next_repo_config], daemon=runner.app_config.daemon),
        None,
        None,
    )

    async def broken_exists(key: str) -> int:
        raise RedisConnectionError("redis down")

    runner.redis.exists = broken_exists  # type: ignore[method-assign]

    asyncio.run(runner.reload_repo_config_if_dirty())

    assert runner.repo_config.coder == CoderType.CODEX
    assert runner._pending_repo_config is None


def test_reload_repo_config_if_dirty_clears_staged_reload_after_disk_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    runner.redis.store["control:octo__demo:config_dirty"] = "1"
    staged_repo_config = RepoConfig.model_validate(
        {**runner.repo_config.model_dump(), "coder": "codex"}
    )
    staged_claude = _FakeUsageProvider(snapshot="new-claude")
    staged_codex = _FakeUsageProvider(snapshot="new-codex")
    runner.stage_config_reload(
        staged_repo_config,
        AppConfig(
            repositories=[staged_repo_config],
            daemon=runner.app_config.daemon,
        ),
        staged_claude,
        staged_codex,
    )
    disk_repo_config = RepoConfig.model_validate(
        {**runner.repo_config.model_dump(), "coder": "claude"}
    )
    refreshed_app_config = AppConfig(
        repositories=[disk_repo_config],
        daemon=DaemonConfig(
            **{
                **runner.app_config.daemon.model_dump(),
                "usage_api_cache_ttl_sec": 321,
                "usage_api_beta_header": "oauth-test-header",
            }
        ),
    )
    monkeypatch.setattr(
        runner_module,
        "load_config",
        lambda path="config.yml": refreshed_app_config,
    )

    asyncio.run(runner.reload_repo_config_if_dirty())
    asyncio.run(runner.reload_repo_config_if_dirty())

    assert runner.repo_config.coder == CoderType.CLAUDE
    assert runner._pending_repo_config is None
    assert runner._pending_app_config is None
    assert runner._pending_usage_providers is None
    assert runner._claude_usage_provider is not staged_claude
    assert runner._codex_usage_provider is not staged_codex
    assert getattr(runner._claude_usage_provider, "_cache_ttl") == 321
    assert getattr(runner._claude_usage_provider, "_beta_header") == "oauth-test-header"
    assert getattr(runner._codex_usage_provider, "_cache_ttl") == 321


def _allow_all_coder_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(selector_module, "_auth_failed", lambda *args, **kwargs: False)


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


async def _preflight_true_stub() -> bool:
    return True


async def _preflight_false_stub() -> bool:
    return False


def _preflight_recording_stub(
    sink: list[str], result: bool = True
) -> Callable[[], Awaitable[bool]]:
    async def _stub() -> bool:
        sink.append("preflight")
        return result
    return _stub


def _run_dirty_diagnose(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    push_exc: Exception | None = None,
    with_pr: bool = True,
    head_branch: str = "fix/diagnose-error-commits-fixes",
    diagnosis_stdout: str = "FIX\nrepair broken config",
    review_post_ok: bool = True,
    preexisting_dirty: str = "",
):
    repo = tmp_path / "repo"
    repo.mkdir()
    changed = repo / "fix.txt"
    calls = []
    warnings = []
    review_requests = []
    head_before = "abc123\n"

    async def fake_diag(*args: object, **kwargs: object):
        changed.write_text("fixed\n")
        return (0, diagnosis_stdout, "")

    def fake_git(repo_path: str, *args: str, **kwargs: Any):
        calls.append(args)
        if args[:2] == ("status", "--porcelain"):
            status = preexisting_dirty
            if changed.exists() and "fix.txt" not in preexisting_dirty:
                status += " M fix.txt\n"
            return _FakeCompletedProcess(stdout=status)
        if args[:3] == ("rev-parse", "--abbrev-ref", "HEAD"):
            return _FakeCompletedProcess(stdout=f"{head_branch}\n")
        if args[:2] == ("rev-parse", "HEAD"):
            return _FakeCompletedProcess(stdout=head_before)
        if push_exc and args[:2] == ("push", "origin"):
            raise push_exc
        return _FakeCompletedProcess()

    monkeypatch.setattr(claude_cli, "diagnose_error_async", fake_diag)
    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(error_module, "retry_transient", lambda op, **_: op())
    monkeypatch.setattr(error_module.logger, "warning", lambda msg: warnings.append(msg))
    runner = _make_runner()
    monkeypatch.setattr(
        runner,
        "_post_codex_review",
        lambda pr_number: review_requests.append(pr_number) or review_post_ok,
    )
    runner.repo_path = str(repo)
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "boom"
    if with_pr:
        runner.state.current_pr = PRInfo(
            number=119, branch="fix/diagnose-error-commits-fixes"
        )
    asyncio.run(runner.handle_error())
    return runner, calls, warnings, review_requests


def test_preflight_returns_true_on_clean_repo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch, stdout="")
    runner = _make_runner()

    assert asyncio.run(runner.preflight()) is True
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None


def test_preflight_returns_false_on_dirty_repo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch, stdout=" M src/foo.py\n?? artifacts/")
    runner = _make_runner()

    assert asyncio.run(runner.preflight()) is False
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

    assert asyncio.run(runner.preflight()) is False
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

    assert asyncio.run(runner.preflight()) is False
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


def test_log_event_deduplicates_consecutive_identical_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeDateTime:
        _values = iter(
            [
                datetime(2026, 4, 20, 11, 55, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 20, 12, 5, 0, tzinfo=timezone.utc),
            ]
        )

        @classmethod
        def now(cls, tz: timezone) -> datetime:
            value = next(cls._values)
            return value.astimezone(tz)

    monkeypatch.setattr(runner_module, "datetime", _FakeDateTime)
    runner = _make_runner()

    runner.log_event("No tasks available")
    runner.log_event("No tasks available")

    assert len(runner.state.history) == 1
    assert runner.state.history[0]["count"] == 2
    assert runner.state.history[0]["time"] == "2026-04-20T12:00:00+00:00"
    assert runner.state.history[0]["last_seen_at"] == "2026-04-20T12:05:00+00:00"


def test_log_event_does_not_deduplicate_when_state_changes() -> None:
    runner = _make_runner()

    runner.state.state = PipelineState.IDLE
    runner.log_event("No tasks available")
    runner.state.state = PipelineState.WATCH
    runner.log_event("No tasks available")

    assert len(runner.state.history) == 2
    assert [entry["state"] for entry in runner.state.history] == ["IDLE", "WATCH"]
    assert all(entry.get("count", 1) == 1 for entry in runner.state.history)


def test_log_event_starts_new_counter_after_different_event() -> None:
    runner = _make_runner()

    runner.log_event("No tasks available")
    runner.log_event("No tasks available")
    runner.log_event("Picked task PR-130")
    runner.log_event("No tasks available")

    assert len(runner.state.history) == 3
    assert runner.state.history[0]["event"] == "No tasks available"
    assert runner.state.history[0]["count"] == 2
    assert runner.state.history[1]["event"] == "Picked task PR-130"
    assert runner.state.history[1]["count"] == 1
    assert runner.state.history[2]["event"] == "No tasks available"
    assert runner.state.history[2]["count"] == 1


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
    commands = [cmd[:4] for cmd in calls]
    fetch_idx = commands.index(["git", "fetch", "--prune", "origin"])
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


def test_handle_idle_publishes_progress_update_only_for_new_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    tasks = [
        QueueTask(pr_id="PR-001", title="Done", status=TaskStatus.DONE, branch="pr-001"),
        QueueTask(pr_id="PR-002", title="Todo", status=TaskStatus.TODO, branch="pr-002"),
    ]
    published: list[tuple[str, str, dict[str, int], object | None]] = []

    async def _fake_publish_repo_event(
        repo_name: str,
        event_type: str,
        payload: dict[str, int],
        redis_client: object | None = None,
    ) -> None:
        published.append((repo_name, event_type, payload, redis_client))

    async def _fake_handle_coding() -> None:
        return None

    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: tasks)
    monkeypatch.setattr(idle_module, "get_next_task", lambda t: tasks[1])
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, open_pr_branches: tasks,
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_merged_prs", lambda repo, branch, refresh=False: []
    )
    monkeypatch.setattr(runner_module, "publish_repo_event", _fake_publish_repo_event)

    runner = _make_runner()
    runner.handle_coding = _fake_handle_coding  # type: ignore[method-assign]

    asyncio.run(runner.handle_idle())
    asyncio.run(runner.publish_state())
    runner._set_queue_progress(1, 2)
    asyncio.run(runner.publish_state())

    assert published == [
        (
            runner.name,
            "progress_updated",
            {"queue_done": 1, "queue_total": 2},
            runner.redis,
        )
    ]


def test_publish_state_skips_progress_update_when_value_was_already_published(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published: list[tuple[str, str, dict[str, int], object | None]] = []

    async def _fake_publish_repo_event(
        repo_name: str,
        event_type: str,
        payload: dict[str, int],
        redis_client: object | None = None,
    ) -> None:
        published.append((repo_name, event_type, payload, redis_client))

    monkeypatch.setattr(runner_module, "publish_repo_event", _fake_publish_repo_event)

    runner = _make_runner()
    runner._last_published_queue_progress = (1, 2)
    runner._set_queue_progress(1, 2)

    asyncio.run(runner.publish_state())

    assert published == []
    assert runner._queue_progress_dirty is False


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

    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
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

    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
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


def test_handle_idle_dag_falls_back_for_legacy_task_files(
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
        "# PR-001: Legacy task\n\n"
        "Branch: pr-001-legacy\n",
        encoding="utf-8",
    )

    fallback_task = QueueTask(
        pr_id="PR-001",
        title="Legacy task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-001.md",
        branch="pr-001-legacy",
    )
    parse_calls: list[str] = []

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
    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: parse_calls.append(path) or [fallback_task],
    )
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda *args, **kwargs: [fallback_task],
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: fallback_task)

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert parse_calls == [str(tasks_dir / "QUEUE.md")]
    assert coding_called["v"] is True
    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task == fallback_task
    assert runner.state.error_message is None


def test_handle_idle_dag_falls_back_when_structured_task_depends_on_legacy_file(
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
        "# PR-001: Legacy task\n\n"
        "Branch: pr-001-legacy\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-002.md").write_text(
        "# PR-002: Structured task\n\n"
        "Branch: pr-002-structured\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: PR-001\n",
        encoding="utf-8",
    )

    fallback_task = QueueTask(
        pr_id="PR-001",
        title="Legacy task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-001.md",
        branch="pr-001-legacy",
    )
    parse_calls: list[str] = []

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
    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: parse_calls.append(path) or [fallback_task],
    )
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda *args, **kwargs: [fallback_task],
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: fallback_task)

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert parse_calls == [str(tasks_dir / "QUEUE.md")]
    assert coding_called["v"] is True
    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task == fallback_task
    assert runner.state.error_message is None


def test_handle_idle_dag_falls_back_when_structured_task_depends_on_missing_file(
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
    (tasks_dir / "QUEUE.md").write_text(
        "## PR-001: Queue-only dependency\n"
        "- Status: TODO\n"
        "- Tasks file: tasks/PR-001.md\n"
        "- Branch: pr-001-queue-only\n\n"
        "## PR-002: Structured task\n"
        "- Status: TODO\n"
        "- Tasks file: tasks/PR-002.md\n"
        "- Branch: pr-002-structured\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-002.md").write_text(
        "# PR-002: Structured task\n\n"
        "Branch: pr-002-structured\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: PR-001\n",
        encoding="utf-8",
    )

    fallback_task = QueueTask(
        pr_id="PR-001",
        title="Queue-only dependency",
        status=TaskStatus.TODO,
        task_file="tasks/PR-001.md",
        branch="pr-001-queue-only",
    )
    parse_calls: list[str] = []

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
    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: parse_calls.append(path) or [fallback_task],
    )
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda *args, **kwargs: [fallback_task],
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: fallback_task)

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert parse_calls == [str(tasks_dir / "QUEUE.md")]
    assert coding_called["v"] is True
    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task == fallback_task
    assert runner.state.error_message is None


def test_handle_idle_keeps_independent_dag_task_when_other_dependency_file_missing(
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
    (tasks_dir / "PR-002.md").write_text(
        "# PR-002: Blocked structured task\n\n"
        "Branch: pr-002-blocked\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: PR-001\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-003.md").write_text(
        "# PR-003: Independent structured task\n\n"
        "Branch: pr-003-independent\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n",
        encoding="utf-8",
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
    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-003"
    assert runner.state.current_task.branch == "pr-003-independent"
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 1
    assert runner.state.error_message is None


def test_handle_idle_keeps_structured_task_when_legacy_dependency_is_already_done(
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
    (tasks_dir / "PR-002.md").write_text(
        "# PR-002: Structured task\n\n"
        "Branch: pr-002-structured\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: PR-001\n",
        encoding="utf-8",
    )

    def fake_get_merged_pr_ids(repo_path: str, base_branch: str, candidate_pr_ids=None) -> set[str]:
        assert repo_path == str(tmp_path)
        assert base_branch == "main"
        assert set(candidate_pr_ids or ()) == {"PR-001", "PR-002"}
        return {"PR-001"}

    monkeypatch.setattr(idle_module, "get_merged_pr_ids", fake_get_merged_pr_ids)
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
    assert runner.state.current_task.branch == "pr-002-structured"
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 1
    assert runner.state.error_message is None
    assert runner._idle_dag_headers[0].depends_on == []
    assert runner._idle_dag_tasks[0].depends_on == []


def test_handle_idle_prefers_legacy_queue_task_over_dag_task(
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
    (tasks_dir / "QUEUE.md").write_text(
        "## PR-001: Legacy task\n"
        "- Status: TODO\n"
        "- Tasks file: tasks/PR-001.md\n"
        "- Branch: pr-001-legacy\n\n"
        "## PR-002: Structured task\n"
        "- Status: TODO\n"
        "- Tasks file: tasks/PR-002.md\n"
        "- Branch: pr-002-structured\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: Legacy task\n\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-002.md").write_text(
        "# PR-002: Structured task\n\n"
        "Branch: pr-002-structured\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n",
        encoding="utf-8",
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
    assert runner.state.current_task.branch == "pr-001-legacy"
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 2


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
        lambda header, merged_pr_ids, open_prs, merged_prs, **kwargs: (
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


def test_select_next_task_from_dag_marks_current_task_doing_without_open_pr(
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
        "# PR-001: Active task\n\n"
        "Branch: pr-001-active\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "get_merged_pr_ids", lambda *args, **kwargs: set())

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = []
    runner._idle_merged_prs = []
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="Active task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-001.md",
        branch="pr-001-active",
    )

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is not None
    assert task.pr_id == "PR-001"
    assert task.status == TaskStatus.DOING
    assert runner._idle_dag_statuses == {"PR-001": TaskStatus.DOING}
    assert all(t.status == TaskStatus.DOING for t in runner._idle_dag_tasks)
    queue_md = runner._generate_queue_md(
        runner._idle_dag_headers,
        runner._idle_dag_statuses,
    )
    assert "## PR-001" in queue_md
    assert "- Status: DOING" in queue_md


def test_select_next_task_from_dag_skips_user_stopped_current_task(
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
        "# PR-001: Stopped task\n\n"
        "Branch: pr-001-stopped\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-002.md").write_text(
        "# PR-002: Follow-up task\n\n"
        "Branch: pr-002-follow-up\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "get_merged_pr_ids", lambda *args, **kwargs: set())

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = []
    runner._idle_merged_prs = []
    runner._user_stopped_task_pr_ids.add("PR-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="Stopped task",
        status=TaskStatus.DOING,
        task_file="tasks/PR-001.md",
        branch="pr-001-stopped",
    )

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is not None
    assert task.pr_id == "PR-002"
    assert task.status == TaskStatus.TODO
    assert runner._idle_dag_statuses == {
        "PR-001": TaskStatus.TODO,
        "PR-002": TaskStatus.TODO,
    }
    assert runner._user_stopped_task_pr_ids == set()


def test_select_next_task_from_dag_retries_user_stopped_task_when_only_choice(
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
        "# PR-001: Stopped task\n\n"
        "Branch: pr-001-stopped\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "get_merged_pr_ids", lambda *args, **kwargs: set())

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = []
    runner._idle_merged_prs = []
    runner._user_stopped_task_pr_ids.add("PR-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="Stopped task",
        status=TaskStatus.DOING,
        task_file="tasks/PR-001.md",
        branch="pr-001-stopped",
    )

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is not None
    assert task.pr_id == "PR-001"
    assert task.status == TaskStatus.TODO
    assert runner._user_stopped_task_pr_ids == set()


def test_select_next_task_from_dag_watches_user_stopped_task_with_open_pr(
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
        "# PR-001: Open PR task\n\n"
        "Branch: pr-001-open\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "get_merged_pr_ids", lambda *args, **kwargs: set())

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = [PRInfo(number=11, branch="pr-001-open", pr_id="PR-001")]
    runner._idle_merged_prs = []
    runner._user_stopped_task_pr_ids.add("PR-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="Open PR task",
        status=TaskStatus.DOING,
        task_file="tasks/PR-001.md",
        branch="pr-001-open",
    )

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is not None
    assert task.pr_id == "PR-001"
    assert task.status == TaskStatus.DOING
    assert runner._user_stopped_task_pr_ids == set()


def test_select_next_task_from_dag_rejects_header_filename_mismatch(
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
        "# PR-999: Wrong task\n\n"
        "Branch: pr-999-wrong-task\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "get_merged_pr_ids", lambda *args, **kwargs: set())

    runner = _make_runner()
    runner.repo_path = str(tmp_path)

    with pytest.raises(QueueValidationError) as excinfo:
        asyncio.run(runner._select_next_task_from_dag())

    assert excinfo.value.issues == [
        f"{tasks_dir / 'PR-001.md'}: header PR ID 'PR-999' does not match task file 'PR-001'"
    ]


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


def test_handle_idle_rereads_pause_flag_before_coding_transition(
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
    refresh_calls: list[str] = []
    coding_calls: list[str] = []

    async def fake_refresh_user_paused_from_redis() -> None:
        refresh_calls.append("refresh")
        runner.state.user_paused = True

    async def fake_publish_state() -> None:
        return None

    async def fake_handle_coding() -> None:
        coding_calls.append("coding")

    monkeypatch.setattr(
        runner,
        "_refresh_user_paused_from_redis",
        fake_refresh_user_paused_from_redis,
    )
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)
    monkeypatch.setattr(runner, "handle_coding", fake_handle_coding)

    asyncio.run(runner.handle_idle())

    assert refresh_calls == ["refresh"]
    assert coding_calls == []
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert any(
        entry["event"] == "Pause requested while preparing PR-042; deferring CODING"
        for entry in runner.state.history
    )


def test_handle_idle_transitions_to_hung_when_pinned_coder_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A task pinned to ``codex`` whose coder is unavailable must transition
    to HUNG with a clear message instead of silently falling back."""
    _patch_subprocess(monkeypatch)
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-200.md").write_text(
        "# PR-200: Pinned to codex\n\n"
        "Branch: pr-200-pinned\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: codex\n",
        encoding="utf-8",
    )
    task = QueueTask(
        pr_id="PR-200",
        title="Pinned to codex",
        status=TaskStatus.TODO,
        task_file="tasks/PR-200.md",
        branch="pr-200-pinned",
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: task)
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

    async def spy_handle_coding(self):
        coding_called["v"] = True

    monkeypatch.setattr(runner_module.PipelineRunner, "handle_coding", spy_handle_coding)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner._auth_status_cache = {
        "claude": {"status": "ok"},
        "codex": {"status": "failed"},
    }
    runner._auth_status_cache_expires_at = (
        datetime.now(timezone.utc) + timedelta(minutes=5)
    )
    runner.state.current_pr = PRInfo(
        number=1234,
        branch="some-other-branch",
        title="Unrelated manual PR from prior cycle",
    )
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.HUNG
    assert runner.state.error_message == (
        "Task PR-200 pinned to codex but coder unavailable"
    )
    assert runner.state.current_pr is None
    assert not coding_called["v"]


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


def test_handle_idle_waits_for_pending_queue_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    sync_calls: list[str] = []

    def fake_resolve() -> bool:
        sync_calls.append("pending")
        return False

    runner = _make_runner()
    runner.state.pending_queue_sync_branch = "queue-sync/pr-120"
    runner._resolve_pending_queue_sync = fake_resolve  # type: ignore[method-assign]

    asyncio.run(runner.handle_idle())

    assert sync_calls == ["pending"]
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None


def test_handle_idle_sets_error_when_initial_sync_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    runner = _make_runner()
    runner.sync_to_main = lambda: (_ for _ in ()).throw(RuntimeError("sync broke"))  # type: ignore[method-assign]

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "sync_to_main failed: sync broke"
    assert any("sync_to_main failed: sync broke" in e["event"] for e in runner.state.history)


def test_handle_idle_stops_when_pending_upload_processing_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    runner = _make_runner()

    async def fake_uploads() -> None:
        return None

    runner.process_pending_uploads = fake_uploads  # type: ignore[method-assign]

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert any(
        "Pending upload failed; skipping task dispatch to retry next cycle" in e["event"]
        for e in runner.state.history
    )


def test_handle_idle_sets_error_when_sync_after_upload_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    runner = _make_runner()
    sync_calls = {"count": 0}

    def fake_sync() -> None:
        sync_calls["count"] += 1
        if sync_calls["count"] == 2:
            raise RuntimeError("resync broke")

    async def fake_uploads() -> bool:
        return True

    runner.sync_to_main = fake_sync  # type: ignore[method-assign]
    runner.process_pending_uploads = fake_uploads  # type: ignore[method-assign]

    asyncio.run(runner.handle_idle())

    assert sync_calls["count"] == 2
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "sync_to_main after upload failed: resync broke"
    assert any(
        "sync_to_main after upload failed: resync broke" in e["event"]
        for e in runner.state.history
    )


def test_handle_idle_sets_error_when_queue_validation_fails_without_dag_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    runner = _make_runner()
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
    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: (_ for _ in ()).throw(
            QueueValidationError(["tasks/QUEUE.md: malformed status"])
        ),
    )

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == (
        "Queue validation failed:\n"
        "  - tasks/QUEUE.md: malformed status"
    )
    assert any("Queue validation failed:" in e["event"] for e in runner.state.history)
    assert any("tasks/QUEUE.md: malformed status" in e["event"] for e in runner.state.history)


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


def test_handle_coding_stop_request_terminates_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
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

    async def fake_run_planned_pr_async(*args: object, **kwargs: object) -> tuple[int, str, str]:
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
        "run_planned_pr_async",
        fake_run_planned_pr_async,
    )

    runner = _make_runner()
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
    assert any(
        "user stop requested" in entry["event"].lower()
        for entry in runner.state.history
    )


def test_handle_coding_honors_persisted_stop_after_fast_cli_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_planned_pr_async",
        _async_cli_result(1, "", "coder failed fast"),
    )

    runner = _make_runner()
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
    assert any(
        "after coder exit" in entry["event"].lower()
        for entry in runner.state.history
    )


def test_handle_coding_honors_persisted_pause_after_fast_cli_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_planned_pr_async",
        _async_cli_result(1, "", "coder failed fast"),
    )

    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-127",
        title="Pause controls",
        status=TaskStatus.DOING,
        branch="pr-127-control-endpoints-backend",
    )
    runner.redis.store[f"pipeline:{runner.name}"] = RepoState(
        url=runner.repo_config.url,
        name=runner.name,
        state=PipelineState.CODING,
        user_paused=True,
    ).model_dump_json()

    async def stale_stop_monitor(
        _cli_task: asyncio.Task[tuple[int, str, str]],
    ) -> None:
        return None

    monkeypatch.setattr(runner, "_monitor_stop_request", stale_stop_monitor)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.user_paused is True
    assert runner.state.error_message == "coder failed fast"
    assert any(
        "finishing current run before honoring pause" in entry["event"].lower()
        for entry in runner.state.history
    )


def test_handle_coding_finishes_success_path_when_pause_persists_during_exit(
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
        lambda repo, **kw: [
            PRInfo(number=127, branch="pr-127-control-endpoints-backend")
        ],
    )
    posted: list[tuple[str, int, str]] = []

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    monkeypatch.setattr(runner_module.github_client, "post_comment", fake_post)

    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-127",
        title="Pause controls",
        status=TaskStatus.DOING,
        branch="pr-127-control-endpoints-backend",
    )
    runner.redis.store[f"pipeline:{runner.name}"] = RepoState(
        url=runner.repo_config.url,
        name=runner.name,
        state=PipelineState.CODING,
        user_paused=True,
    ).model_dump_json()

    async def stale_stop_monitor(
        _cli_task: asyncio.Task[tuple[int, str, str]],
    ) -> None:
        return None

    monkeypatch.setattr(runner, "_monitor_stop_request", stale_stop_monitor)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.user_paused is True
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 127
    assert posted == [(runner.owner_repo, 127, "@codex review")]
    assert any(
        "finishing current run before honoring pause" in entry["event"].lower()
        for entry in runner.state.history
    )


def test_handle_coding_honors_stop_requested_during_pr_retry_after_cli_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_planned_pr_async",
        _async_cli_result(0, "ok", ""),
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

    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-127",
        title="Pause controls",
        status=TaskStatus.DOING,
        branch="pr-127-control-endpoints-backend",
    )
    monkeypatch.setattr(runner_module.github_client, "get_open_prs", fake_get_open_prs)
    monkeypatch.setattr(runner, "_pop_stop_request", fake_pop_stop_request)
    monkeypatch.setattr(runner_module.asyncio, "sleep", instant_sleep)
    monkeypatch.setattr(runner, "_monitor_stop_request", stale_stop_monitor)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.user_paused is True
    assert runner.state.current_pr is None
    assert attempts["count"] == 1
    assert any(
        "after coder exit" in entry["event"].lower()
        for entry in runner.state.history
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
    assert runner.state.current_pr.fix_iteration_count == 1
    assert posted == [(runner.owner_repo, 77, "@codex review")]
    assert any(
        "Posted @codex review on PR #77" in e["event"]
        for e in runner.state.history
    )


def test_handle_fix_finishes_push_bookkeeping_before_post_exit_stop_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    runner.redis.store[f"control:{runner.name}:stop"] = "1"

    async def stale_stop_monitor(
        _cli_task: asyncio.Task[tuple[int, str, str]],
    ) -> None:
        return None

    monkeypatch.setattr(runner, "_monitor_stop_request", stale_stop_monitor)

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.user_paused is True
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 1
    assert runner.state.current_pr.fix_iteration_count == 1
    assert posted == [(runner.owner_repo, 77, "@codex review")]
    assert any(
        "deferring pause until fix bookkeeping completes" in entry["event"].lower()
        for entry in runner.state.history
    )
    assert any("Fix pushed, iteration #1" in e["event"] for e in runner.state.history)


def test_handle_fix_honors_stop_requested_during_fix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
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

    async def fake_fix_review_async(*args: object, **kwargs: object) -> tuple[int, str, str]:
        proc = _FakeProc()
        on_process_start = kwargs["on_process_start"]
        assert callable(on_process_start)
        on_process_start(proc)
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            raise
        return (0, "ok", "")

    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix_review_async)

    runner = _make_runner()
    runner.state.current_pr = PRInfo(number=77, branch="pr-019")
    runner.redis.store[f"control:{runner.name}:stop"] = "1"

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.user_paused is True
    assert runner.state.error_message is None
    assert stop_called["terminate"] == 1
    assert stop_called["kill"] == 0
    assert stop_called["wait"] >= 1
    assert any(
        "user stop requested" in entry["event"].lower()
        for entry in runner.state.history
    )


def test_handle_fix_escalates_at_iteration_cap_before_next_spawn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[tuple[str, int, str]] = []
    gh_calls: list[list[str]] = []
    fix_called: list[bool] = []

    class _UnexpectedPlugin:
        async def fix_review(
            self, path: str, **kwargs: object
        ) -> tuple[int, str, str]:
            fix_called.append(True)
            return (0, "", "")

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        gh_calls.append(cmd)
        return ""

    monkeypatch.setattr(runner_module.github_client, "post_comment", fake_post)
    monkeypatch.setattr(runner_module.github_client, "run_gh", fake_run_gh)

    runner = _make_runner()
    runner._get_coder = lambda allow_exploration=False: (  # type: ignore[method-assign]
        "claude",
        _UnexpectedPlugin(),
    )
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=77,
        branch="pr-077",
        fix_iteration_count=15,
    )
    runner._app_config = _app_cfg(fix_iteration_cap=15)

    asyncio.run(runner.handle_fix())

    assert fix_called == []
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.user_paused is False
    assert posted == [
        (
            runner.owner_repo,
            77,
            "@AlexBomber12 FIX iteration cap reached (15/15). "
            "Escalating for manual review.",
        )
    ]
    assert gh_calls == [
        [
            "label",
            "create",
            "escalated",
            "--color",
            "B60205",
            "--description",
            "Daemon escalated, manual review required",
        ],
        ["pr", "edit", "77", "--add-label", "escalated"],
    ]
    assert any(
        entry["event"]
        == "FIX cap reached (15/15) on PR #77: escalated, moving to IDLE."
        for entry in runner.state.history
    )


def test_handle_fix_cap_ignores_existing_label_create_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[tuple[str, int, str]] = []
    gh_calls: list[list[str]] = []

    class _UnexpectedPlugin:
        async def fix_review(
            self, path: str, **kwargs: object
        ) -> tuple[int, str, str]:
            raise AssertionError("fix_review should not run at cap boundary")

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        gh_calls.append(cmd)
        if cmd[:3] == ["label", "create", "escalated"]:
            raise RuntimeError("label already exists")
        return ""

    monkeypatch.setattr(runner_module.github_client, "post_comment", fake_post)
    monkeypatch.setattr(runner_module.github_client, "run_gh", fake_run_gh)

    runner = _make_runner()
    runner._get_coder = lambda allow_exploration=False: (  # type: ignore[method-assign]
        "claude",
        _UnexpectedPlugin(),
    )
    runner.state.current_pr = PRInfo(
        number=88,
        branch="pr-088",
        fix_iteration_count=2,
    )
    runner.state.user_paused = True
    runner._app_config = _app_cfg(fix_iteration_cap=2)

    asyncio.run(runner.handle_fix())

    assert posted == [
        (
            runner.owner_repo,
            88,
            "@AlexBomber12 FIX iteration cap reached (2/2). "
            "Escalating for manual review.",
        )
    ]
    assert gh_calls == [
        [
            "label",
            "create",
            "escalated",
            "--color",
            "B60205",
            "--description",
            "Daemon escalated, manual review required",
        ],
        ["pr", "edit", "88", "--add-label", "escalated"],
    ]
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.user_paused is True
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.is_escalated is True


def test_handle_fix_cap_skips_repeat_escalation_when_pr_already_escalated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[tuple[str, int, str]] = []
    gh_calls: list[list[str]] = []

    class _UnexpectedPlugin:
        async def fix_review(
            self, path: str, **kwargs: object
        ) -> tuple[int, str, str]:
            raise AssertionError("fix_review should not run at cap boundary")

    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "run_gh",
        lambda cmd, **kwargs: gh_calls.append(cmd) or "",
    )

    runner = _make_runner()
    runner._get_coder = lambda allow_exploration=False: (  # type: ignore[method-assign]
        "claude",
        _UnexpectedPlugin(),
    )
    runner.state.current_pr = PRInfo(
        number=91,
        branch="pr-091",
        fix_iteration_count=2,
        is_escalated=True,
    )
    runner.state.user_paused = True
    runner._app_config = _app_cfg(fix_iteration_cap=2)

    asyncio.run(runner.handle_fix())

    assert posted == []
    assert gh_calls == []
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.user_paused is True
    assert any(
        entry["event"]
        == "FIX blocked for escalated PR #91, moving to IDLE."
        for entry in runner.state.history
    )


def test_handle_fix_blocks_escalated_pr_even_when_counter_resets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[tuple[str, int, str]] = []
    gh_calls: list[list[str]] = []

    class _UnexpectedPlugin:
        async def fix_review(
            self, path: str, **kwargs: object
        ) -> tuple[int, str, str]:
            raise AssertionError("fix_review should not run for escalated PRs")

    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "run_gh",
        lambda cmd, **kwargs: gh_calls.append(cmd) or "",
    )

    runner = _make_runner()
    runner._get_coder = lambda allow_exploration=False: (  # type: ignore[method-assign]
        "claude",
        _UnexpectedPlugin(),
    )
    runner.state.current_pr = PRInfo(
        number=92,
        branch="pr-092",
        fix_iteration_count=0,
        is_escalated=True,
    )
    runner._app_config = _app_cfg(fix_iteration_cap=2)

    asyncio.run(runner.handle_fix())

    assert posted == []
    assert gh_calls == []
    assert runner.state.state == PipelineState.IDLE
    assert any(
        entry["event"] == "FIX blocked for escalated PR #92, moving to IDLE."
        for entry in runner.state.history
    )


def test_handle_idle_preserves_fix_iteration_count_when_reattaching_same_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-145",
        title="Fix iteration cap",
        status=TaskStatus.TODO,
        branch="pr-145-fix-iteration-cap",
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [task])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: task)

    reattached_pr = PRInfo(
        number=145,
        branch="pr-145-fix-iteration-cap",
        title="PR-145: Fix iteration cap",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.CHANGES_REQUESTED,
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [reattached_pr],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_pr_metadata",
        lambda repo, number: {"head_commit_date": "2026-04-21T00:48:54Z"},
    )

    runner = _make_runner()
    runner.state.current_task = task
    runner.state.current_pr = PRInfo(
        number=145,
        branch="pr-145-fix-iteration-cap",
        fix_iteration_count=15,
    )

    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 145
    assert runner.state.current_pr.fix_iteration_count == 15


def test_handle_fix_cap_sets_error_when_comment_post_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _UnexpectedPlugin:
        async def fix_review(
            self, path: str, **kwargs: object
        ) -> tuple[int, str, str]:
            raise AssertionError("fix_review should not run at cap boundary")

    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: (_ for _ in ()).throw(
            RuntimeError("gh unavailable")
        ),
    )

    runner = _make_runner()
    runner._get_coder = lambda allow_exploration=False: (  # type: ignore[method-assign]
        "claude",
        _UnexpectedPlugin(),
    )
    runner.state.current_pr = PRInfo(
        number=89,
        branch="pr-089",
        fix_iteration_count=2,
    )
    runner._app_config = _app_cfg(fix_iteration_cap=2)

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "post_comment failed: gh unavailable"


def test_handle_fix_cap_sets_error_when_add_label_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[tuple[str, int, str]] = []

    class _UnexpectedPlugin:
        async def fix_review(
            self, path: str, **kwargs: object
        ) -> tuple[int, str, str]:
            raise AssertionError("fix_review should not run at cap boundary")

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        if cmd[:2] == ["pr", "edit"]:
            raise RuntimeError("add label failed")
        return ""

    monkeypatch.setattr(runner_module.github_client, "post_comment", fake_post)
    monkeypatch.setattr(runner_module.github_client, "run_gh", fake_run_gh)

    runner = _make_runner()
    runner._get_coder = lambda allow_exploration=False: (  # type: ignore[method-assign]
        "claude",
        _UnexpectedPlugin(),
    )
    runner.state.current_pr = PRInfo(
        number=90,
        branch="pr-090",
        fix_iteration_count=2,
    )
    runner._app_config = _app_cfg(fix_iteration_cap=2)

    asyncio.run(runner.handle_fix())

    assert posted == [
        (
            runner.owner_repo,
            90,
            "@AlexBomber12 FIX iteration cap reached (2/2). "
            "Escalating for manual review.",
        )
    ]
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "pr edit failed: add label failed"


def test_handle_fix_routes_iteration_cap_through_bounded_recovery_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Guard: the FIX iteration-cap site must call
    ``BoundedRecoveryPolicy.maybe_escalate`` rather than rebuilding
    the threshold check inline. Future maintainers must not silently
    bypass the framework."""
    posted: list[tuple[str, int, str]] = []
    gh_calls: list[list[str]] = []

    class _UnexpectedPlugin:
        async def fix_review(
            self, path: str, **kwargs: object
        ) -> tuple[int, str, str]:
            raise AssertionError("fix_review must not run at cap boundary")

    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "run_gh",
        lambda cmd, **kwargs: gh_calls.append(cmd) or "",
    )

    maybe_escalate_calls: list[str] = []
    orig_maybe_escalate = recovery_policy_module.BoundedRecoveryPolicy.maybe_escalate

    async def spy_maybe_escalate(self: Any, ctx: Any) -> bool:
        maybe_escalate_calls.append(self.name)
        return await orig_maybe_escalate(self, ctx)

    monkeypatch.setattr(
        recovery_policy_module.BoundedRecoveryPolicy,
        "maybe_escalate",
        spy_maybe_escalate,
    )

    runner = _make_runner()
    runner._get_coder = lambda allow_exploration=False: (  # type: ignore[method-assign]
        "claude",
        _UnexpectedPlugin(),
    )
    runner.state.current_pr = PRInfo(
        number=93,
        branch="pr-093",
        fix_iteration_count=2,
    )
    runner._app_config = _app_cfg(fix_iteration_cap=2)

    asyncio.run(runner.handle_fix())

    assert maybe_escalate_calls == ["fix_iteration_cap"]
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.is_escalated is True


def test_handle_fix_finishes_push_bookkeeping_before_stop_cancel_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stop-cancelled FIX must still record a completed push before pausing."""
    rev_parse_calls = {"count": 0}
    posted: list[int] = []
    saved_logs: list[tuple[str, str, str]] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            sha = "aaa111\n" if rev_parse_calls["count"] == 1 else "bbb222\n"
            return _FakeCompletedProcess(args=["git", *args], stdout=sha, returncode=0)
        if args[:2] == ("rev-parse", "origin/pr-042-fix"):
            return _FakeCompletedProcess(
                args=["git", *args],
                stdout="bbb222\n",
                returncode=0,
            )
        if args[:2] == ("merge-base", "--is-ancestor"):
            return _FakeCompletedProcess(args=["git", *args], returncode=0)
        return _FakeCompletedProcess(args=["git", *args], returncode=0)

    async def fake_fix(*args: object, **kwargs: object) -> tuple[int, str, str]:
        await asyncio.Future()
        return (0, "", "")

    async def no_idle_monitor(
        self: object,
        pr_number: int,
        idle_limit: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        idle_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    async def no_breach_monitor(
        self: object,
        breach_dir: str,
        run_id: str,
        claude_task: asyncio.Task,  # type: ignore[type-arg]
        breach_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", no_idle_monitor)
    monkeypatch.setattr(
        PipelineRunner, "_monitor_inflight_breach", no_breach_monitor
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")

    async def save_log(stdout: str, stderr: str, label: str) -> None:
        saved_logs.append((stdout, stderr, label))

    async def stop_monitor(
        cli_task: asyncio.Task[tuple[int, str, str]],
    ) -> None:
        runner._stop_requested = True
        runner.state.user_paused = True
        await asyncio.sleep(0)
        cli_task.cancel()

    monkeypatch.setattr(runner, "_save_cli_log", save_log)
    monkeypatch.setattr(runner, "_monitor_stop_request", stop_monitor)
    monkeypatch.setattr(
        runner, "_post_codex_review", lambda pr_number: posted.append(pr_number) or True
    )

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.user_paused is True
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 1
    assert runner.state.current_pr.fix_iteration_count == 1
    assert runner.state.current_pr.last_activity is not None
    assert runner._last_push_at is not None
    assert runner._last_push_at_pr_number == 42
    assert posted == [42]
    assert saved_logs == [("", "", "FIX REVIEW output [claude]")]
    assert any("Fix pushed, iteration #1" in e["event"] for e in runner.state.history)


def test_handle_fix_stop_cancel_skips_push_bookkeeping_when_remote_head_is_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stop-cancelled FIX must not count an unpushed local commit as a push."""
    rev_parse_calls = {"count": 0}
    posted: list[int] = []
    saved_logs: list[tuple[str, str, str]] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            sha = "aaa111\n" if rev_parse_calls["count"] == 1 else "bbb222\n"
            return _FakeCompletedProcess(args=["git", *args], stdout=sha, returncode=0)
        if args[:2] == ("rev-parse", "origin/pr-042-fix"):
            return _FakeCompletedProcess(
                args=["git", *args],
                stdout="aaa111\n",
                returncode=0,
            )
        if args[:2] == ("merge-base", "--is-ancestor"):
            return _FakeCompletedProcess(args=["git", *args], returncode=1)
        return _FakeCompletedProcess(args=["git", *args], returncode=0)

    async def fake_fix(*args: object, **kwargs: object) -> tuple[int, str, str]:
        await asyncio.Future()
        return (0, "", "")

    async def no_idle_monitor(
        self: object,
        pr_number: int,
        idle_limit: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        idle_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    async def no_breach_monitor(
        self: object,
        breach_dir: str,
        run_id: str,
        claude_task: asyncio.Task,  # type: ignore[type-arg]
        breach_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", no_idle_monitor)
    monkeypatch.setattr(
        PipelineRunner, "_monitor_inflight_breach", no_breach_monitor
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")

    async def save_log(stdout: str, stderr: str, label: str) -> None:
        saved_logs.append((stdout, stderr, label))

    async def stop_monitor(
        cli_task: asyncio.Task[tuple[int, str, str]],
    ) -> None:
        runner._stop_requested = True
        runner.state.user_paused = True
        await asyncio.sleep(0)
        cli_task.cancel()

    monkeypatch.setattr(runner, "_save_cli_log", save_log)
    monkeypatch.setattr(runner, "_monitor_stop_request", stop_monitor)
    monkeypatch.setattr(
        runner, "_post_codex_review", lambda pr_number: posted.append(pr_number) or True
    )

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.user_paused is True
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 0
    assert runner.state.current_pr.fix_iteration_count == 0
    assert runner.state.current_pr.last_activity is None
    assert runner._last_push_at is None
    assert runner._last_push_at_pr_number is None
    assert posted == []
    assert saved_logs == [("", "", "FIX REVIEW output [claude]")]
    assert any(
        "outside the fetched remote branch" in e["event"].lower()
        for e in runner.state.history
    )


def test_handle_fix_stop_cancel_records_push_when_remote_advanced_past_local_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stop-cancelled FIX must still count a push when remote moved past it."""
    rev_parse_calls = {"count": 0}
    posted: list[int] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            sha = "aaa111\n" if rev_parse_calls["count"] == 1 else "bbb222\n"
            return _FakeCompletedProcess(args=["git", *args], stdout=sha, returncode=0)
        if args[:2] == ("rev-parse", "origin/pr-042-fix"):
            return _FakeCompletedProcess(
                args=["git", *args],
                stdout="ccc333\n",
                returncode=0,
            )
        if args[:2] == ("merge-base", "--is-ancestor"):
            assert args[2:] == ("bbb222", "ccc333")
            return _FakeCompletedProcess(args=["git", *args], returncode=0)
        return _FakeCompletedProcess(args=["git", *args], returncode=0)

    async def fake_fix(*args: object, **kwargs: object) -> tuple[int, str, str]:
        await asyncio.Future()
        return (0, "", "")

    async def no_idle_monitor(
        self: object,
        pr_number: int,
        idle_limit: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        idle_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    async def no_breach_monitor(
        self: object,
        breach_dir: str,
        run_id: str,
        claude_task: asyncio.Task,  # type: ignore[type-arg]
        breach_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", no_idle_monitor)
    monkeypatch.setattr(
        PipelineRunner, "_monitor_inflight_breach", no_breach_monitor
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")

    async def stop_monitor(
        cli_task: asyncio.Task[tuple[int, str, str]],
    ) -> None:
        runner._stop_requested = True
        runner.state.user_paused = True
        await asyncio.sleep(0)
        cli_task.cancel()

    monkeypatch.setattr(runner, "_monitor_stop_request", stop_monitor)
    monkeypatch.setattr(
        runner, "_post_codex_review", lambda pr_number: posted.append(pr_number) or True
    )

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 1
    assert posted == [42]
    assert any("Fix pushed, iteration #1" in e["event"] for e in runner.state.history)


def test_handle_fix_honors_persisted_stop_after_fast_fix_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "fix_review_async",
        _async_cli_result(1, "", "fix failed fast"),
    )

    runner = _make_runner()
    runner.state.current_pr = PRInfo(number=77, branch="pr-019")
    runner.redis.store[f"control:{runner.name}:stop"] = "1"

    async def stale_stop_monitor(
        _cli_task: asyncio.Task[tuple[int, str, str]],
    ) -> None:
        return None

    monkeypatch.setattr(runner, "_monitor_stop_request", stale_stop_monitor)

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.user_paused is True
    assert runner.state.error_message is None
    assert f"control:{runner.name}:stop" not in runner.redis.store
    assert any(
        "deferring pause until fix bookkeeping completes" in entry["event"].lower()
        for entry in runner.state.history
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


def test_start_current_run_record_sets_stage_coder() -> None:
    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )

    runner._start_current_run_record("claude", "opus")

    assert runner._current_run_record is not None
    assert runner._current_run_record.stage == "coder"


def test_start_current_run_record_clears_record_without_task() -> None:
    runner = _make_runner()
    runner._current_run_record = object()  # type: ignore[assignment]

    runner._start_current_run_record("claude", "opus")

    assert runner._current_run_record is None


def test_app_config_setter_and_usage_provider_swap() -> None:
    runner = _make_runner()
    new_config = _app_cfg(poll_interval_sec=120)
    claude_provider = _FakeUsageProvider(snapshot={"name": "claude"})
    codex_provider = _FakeUsageProvider(snapshot={"name": "codex"})

    runner.app_config = new_config
    runner.set_usage_providers(claude_provider, codex_provider)

    assert runner.app_config is new_config
    assert runner._claude_usage_provider is claude_provider
    assert runner._codex_usage_provider is codex_provider


def test_refresh_auth_status_cache_returns_early_when_cache_is_fresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    runner._auth_status_cache = {"claude": {"status": "ok"}}
    runner._auth_status_cache_expires_at = datetime.now(timezone.utc) + timedelta(minutes=1)

    async def fail_to_thread(*args: object, **kwargs: object) -> object:
        raise AssertionError("asyncio.to_thread should not be called")

    monkeypatch.setattr(runner_module.asyncio, "to_thread", fail_to_thread)

    asyncio.run(runner._refresh_auth_status_cache())

    assert runner._auth_status_cache == {"claude": {"status": "ok"}}


def test_refresh_auth_status_cache_marks_plugin_probe_errors() -> None:
    class _Plugin:
        display_name = "fake"
        models: list[str] = []

        def __init__(self, name: str, status: dict[str, str] | Exception) -> None:
            self.name = name
            self._status = status

        async def run_planned_pr(self, *args: object, **kwargs: object) -> tuple[int, str, str]:
            return (0, "", "")

        async def fix_review(self, *args: object, **kwargs: object) -> tuple[int, str, str]:
            return (0, "", "")

        def check_auth(self) -> dict[str, str]:
            if isinstance(self._status, Exception):
                raise self._status
            return self._status

        def create_usage_provider(self, **kwargs: object) -> None:
            return None

        def rate_limit_patterns(self) -> list[re.Pattern[str]]:
            return []

    registry = CoderRegistry()
    registry.register(_Plugin("claude", {"status": "ok", "detail": "ready"}))
    registry.register(_Plugin("codex", RuntimeError("boom")))
    claude_provider, codex_provider = _usage_providers()
    runner = PipelineRunner(
        _repo_cfg(),
        _app_cfg(),
        _FakeRedis(),
        claude_provider,
        codex_provider,
        registry=registry,
    )

    asyncio.run(runner._refresh_auth_status_cache())

    assert runner._auth_status_cache == {
        "claude": {"status": "ok", "detail": "ready"},
        "codex": {"status": "error"},
    }
    assert runner._auth_status_cache_expires_at is not None
    assert runner._auth_status_cache_expires_at > datetime.now(timezone.utc)


def test_init_migrates_legacy_clone_when_origin_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_name = f"runner-init-migrate-{time.time_ns()}"
    old_path = Path("/data/repos") / repo_name
    new_path = Path("/data/repos") / f"octo__{repo_name}"
    old_path.mkdir(parents=True)
    (old_path / ".git").mkdir()
    info_logs: list[tuple[object, ...]] = []
    run_calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        run_calls.append(cmd)
        assert cmd in (
            ["git", "-C", str(old_path), "remote", "get-url", "origin"],
            ["git", "-C", str(new_path), "remote", "get-url", "origin"],
        )
        return _FakeCompletedProcess(args=cmd, stdout=f"https://github.com/octo/{repo_name}.git\n")

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        runner_module.logger,
        "info",
        lambda *args: info_logs.append(args),
    )

    try:
        runner = _make_runner(url=f"https://github.com/octo/{repo_name}.git")
        assert runner.repo_path == str(new_path)
        assert new_path.exists()
        assert not old_path.exists()
        assert run_calls == [
            ["git", "-C", str(old_path), "remote", "get-url", "origin"],
            ["git", "-C", str(new_path), "remote", "get-url", "origin"],
        ]
        assert info_logs
    finally:
        with contextlib.suppress(FileNotFoundError):
            import shutil
            shutil.rmtree(new_path)
        with contextlib.suppress(FileNotFoundError):
            import shutil
            shutil.rmtree(old_path)


def test_init_skips_legacy_clone_migration_when_origin_mismatches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_name = f"runner-init-skip-{time.time_ns()}"
    old_path = Path("/data/repos") / repo_name
    new_path = Path("/data/repos") / f"octo__{repo_name}"
    old_path.mkdir(parents=True)
    warnings: list[tuple[object, ...]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(args=cmd, stdout="https://github.com/octo/other.git\n")

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        runner_module.logger,
        "warning",
        lambda *args: warnings.append(args),
    )

    try:
        _make_runner(url=f"https://github.com/octo/{repo_name}.git")
        assert old_path.exists()
        assert not new_path.exists()
        assert any("Legacy clone" in str(args[0]) for args in warnings)
    finally:
        with contextlib.suppress(FileNotFoundError):
            import shutil
            shutil.rmtree(new_path)
        with contextlib.suppress(FileNotFoundError):
            import shutil
            shutil.rmtree(old_path)


def test_init_skips_legacy_clone_migration_when_origin_probe_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_name = f"runner-init-error-{time.time_ns()}"
    old_path = Path("/data/repos") / repo_name
    new_path = Path("/data/repos") / f"octo__{repo_name}"
    old_path.mkdir(parents=True)
    warnings: list[tuple[object, ...]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        raise RuntimeError("git unavailable")

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        runner_module.logger,
        "warning",
        lambda *args: warnings.append(args),
    )

    try:
        _make_runner(url=f"https://github.com/octo/{repo_name}.git")
        assert old_path.exists()
        assert not new_path.exists()
        assert any("Could not verify origin for %s — skipping migration" in str(args[0]) for args in warnings)
    finally:
        with contextlib.suppress(FileNotFoundError):
            import shutil
            shutil.rmtree(new_path)
        with contextlib.suppress(FileNotFoundError):
            import shutil
            shutil.rmtree(old_path)


def test_init_removes_non_git_directory_at_new_clone_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_name = f"runner-init-nongit-{time.time_ns()}"
    new_path = Path("/data/repos") / f"octo__{repo_name}"
    new_path.mkdir(parents=True)
    warnings: list[tuple[object, ...]] = []

    monkeypatch.setattr(
        runner_module.logger,
        "warning",
        lambda *args: warnings.append(args),
    )

    try:
        _make_runner(url=f"https://github.com/octo/{repo_name}.git")
        assert not new_path.exists()
        assert any("Removing non-git directory %s" in str(args[0]) for args in warnings)
    finally:
        with contextlib.suppress(FileNotFoundError):
            import shutil
            shutil.rmtree(new_path)


def test_init_removes_stale_clone_when_origin_mismatches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_name = f"runner-init-stale-{time.time_ns()}"
    new_path = Path("/data/repos") / f"octo__{repo_name}"
    (new_path / ".git").mkdir(parents=True)
    warnings: list[tuple[object, ...]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(args=cmd, stdout="https://github.com/octo/other.git\n")

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        runner_module.logger,
        "warning",
        lambda *args: warnings.append(args),
    )

    try:
        _make_runner(url=f"https://github.com/octo/{repo_name}.git")
        assert not new_path.exists()
        assert any("removing stale clone" in str(args[0]).lower() for args in warnings)
    finally:
        with contextlib.suppress(FileNotFoundError):
            import shutil
            shutil.rmtree(new_path)


def test_init_logs_when_new_clone_origin_probe_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_name = f"runner-init-new-error-{time.time_ns()}"
    new_path = Path("/data/repos") / f"octo__{repo_name}"
    (new_path / ".git").mkdir(parents=True)
    warnings: list[tuple[object, ...]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        raise RuntimeError("git unavailable")

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        runner_module.logger,
        "warning",
        lambda *args: warnings.append(args),
    )

    try:
        _make_runner(url=f"https://github.com/octo/{repo_name}.git")
        assert new_path.exists()
        assert any("Could not verify origin for %s" == str(args[0]) for args in warnings)
    finally:
        with contextlib.suppress(FileNotFoundError):
            import shutil
            shutil.rmtree(new_path)


def test_compute_diff_stats_returns_populated_fields_on_clean_diff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        assert cmd == ["git", "diff", "--numstat", "origin/main...HEAD"]
        return _FakeCompletedProcess(
            args=cmd,
            stdout="10\t2\tsrc/app.py\n3\t1\ttests/test_app.py\n-\t-\tassets/logo.png\n",
            returncode=0,
        )

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    runner = _make_runner()

    stats = runner._compute_diff_stats("main")

    assert stats == {
        "files_touched_count": 3,
        "languages_touched": ["python"],
        "diff_lines_added": 13,
        "diff_lines_deleted": 3,
        "test_file_ratio": 0.333,
    }


def test_compute_diff_stats_returns_empty_dict_on_git_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(args=cmd, stderr="boom", returncode=1)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    runner = _make_runner()

    assert runner._compute_diff_stats("main") == {}


def test_compute_diff_stats_skips_malformed_and_invalid_numstat_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(
            args=cmd,
            stdout="bad row\nbogus\t1\tsrc/broken.py\n2\t4\tsrc/app.py\n",
            returncode=0,
        )

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    runner = _make_runner()

    stats = runner._compute_diff_stats("main")

    assert stats == {
        "files_touched_count": 1,
        "languages_touched": ["python"],
        "diff_lines_added": 2,
        "diff_lines_deleted": 4,
        "test_file_ratio": 0.0,
    }


def test_ext_to_language_returns_none_for_unknown_extension() -> None:
    assert PipelineRunner._ext_to_language("notes.unknown") is None


def test_save_populates_enriched_fields_on_success_merged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )
    runner._start_current_run_record("claude", "opus")
    monkeypatch.setattr(
        runner,
        "_compute_diff_stats",
        lambda base_branch: {
            "files_touched_count": 4,
            "languages_touched": ["python", "yaml"],
            "diff_lines_added": 20,
            "diff_lines_deleted": 5,
            "test_file_ratio": 0.25,
        },
    )

    asyncio.run(runner._save_current_run_record("success_merged"))

    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=runner.name,
        )
    )

    assert len(recent) == 1
    assert recent[0].files_touched_count == 4
    assert recent[0].languages_touched == ["python", "yaml"]
    assert recent[0].diff_lines_added == 20
    assert recent[0].diff_lines_deleted == 5
    assert recent[0].test_file_ratio == 0.25
    assert recent[0].base_branch == "main"


def test_save_skips_enriched_fields_on_error_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )
    runner._start_current_run_record("claude", "opus")
    calls: list[str] = []

    def fake_compute(base_branch: str) -> dict[str, object]:
        calls.append(base_branch)
        return {"files_touched_count": 99}

    monkeypatch.setattr(runner, "_compute_diff_stats", fake_compute)

    asyncio.run(runner._save_current_run_record("error"))

    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=runner.name,
        )
    )

    assert calls == []
    assert len(recent) == 1
    assert recent[0].files_touched_count == 0
    assert recent[0].languages_touched == []
    assert recent[0].base_branch == ""


def test_checkpoint_current_run_record_skips_save_without_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    save_calls: list[object] = []

    async def fake_save(record: object) -> None:
        save_calls.append(record)

    monkeypatch.setattr(runner._metrics_store, "save", fake_save)

    asyncio.run(runner._checkpoint_current_run_record())

    assert save_calls == []


def test_restore_current_run_record_clears_state_without_task() -> None:
    runner = _make_runner()
    runner._current_run_record = object()  # type: ignore[assignment]

    asyncio.run(runner._restore_current_run_record())

    assert runner._current_run_record is None


def test_restore_current_run_record_logs_metrics_lookup_failure() -> None:
    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )

    async def fake_recent(**kwargs: Any) -> list[object]:
        raise RuntimeError("metrics unavailable")

    runner._metrics_store.recent = fake_recent  # type: ignore[method-assign]

    asyncio.run(runner._restore_current_run_record())

    assert runner._current_run_record is None
    assert any(
        "restore_current_run_record failed for PR-001: metrics unavailable"
        in entry["event"]
        for entry in runner.state.history
    )


def test_save_current_run_record_sets_duration_none_for_invalid_started_at() -> None:
    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )
    runner._start_current_run_record("claude", "opus")
    assert runner._current_run_record is not None
    runner._current_run_record.started_at = "not-an-iso-timestamp"

    asyncio.run(
        runner._save_current_run_record(
            "coding_complete",
            diff_stats={},
            base_branch="main",
        )
    )

    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=runner.name,
        )
    )

    assert len(recent) == 1
    assert recent[0].duration_ms is None


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
    assert all("--prune" in calls[i] for i in fetch_calls), (
        "git fetch in handle_fix must pass --prune to drop stale "
        "remote-tracking refs (PR-161)"
    )
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

    class _CodexPlugin:
        async def run_planned_pr(
            self, path: str, **kwargs: object
        ) -> tuple[int, str, str]:
            return (0, "ok", "")

    runner = _make_runner()
    runner._get_coder = lambda allow_exploration=False: (  # type: ignore[method-assign]
        "codex",
        _CodexPlugin(),
    )
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001"
    )
    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == (
        "[codex] coder succeeded but no PR found for branch 'pr-001'"
    )
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
    runner._start_current_run_record("claude", "opus")
    asyncio.run(runner.handle_watch())

    assert merged == [(runner.owner_repo, 5)]
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None


def test_handle_watch_without_current_pr_returns_to_idle() -> None:
    runner = _make_runner()
    runner.state.state = PipelineState.WATCH

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.history[-1]["event"] == "WATCH without current_pr -> IDLE"


def test_handle_watch_open_pr_lookup_failure_sets_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")

    def _raise(repo: str, **kwargs: Any) -> list[PRInfo]:
        raise RuntimeError("boom")

    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        _raise,
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "get_open_prs failed: boom"
    assert runner.state.history[-1]["event"] == "boom"


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
    runner._start_current_run_record("claude", "opus")
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


def test_handle_watch_preserves_fix_iteration_count_for_same_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo, **kw: [pr]
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=5,
        branch="pr-001",
        fix_iteration_count=7,
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.current_pr is not None
    assert runner.state.current_pr.fix_iteration_count == 7


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


def test_handle_watch_naive_last_activity_is_treated_as_utc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recent = datetime.now(timezone.utc) - timedelta(minutes=2)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=recent.replace(tzinfo=None),
    )
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo, **kw: [pr]
    )

    runner = _make_runner(review_timeout_min=30)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.WATCH
    assert any("2/30m" in e["event"] for e in runner.state.history)


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


def test_handle_watch_captures_success_merged_on_external_merge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo, **kw: []
    )
    monkeypatch.setattr(
        runner_module.github_client, "is_pr_merged", lambda repo, number: True
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )
    runner._start_current_run_record("claude", "opus")
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None
    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=runner.name,
        )
    )
    assert len(recent) == 1
    assert recent[0].exit_reason == "success_merged"


def test_handle_watch_captures_closed_unmerged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo, **kw: []
    )
    monkeypatch.setattr(
        runner_module.github_client, "is_pr_merged", lambda repo, number: False
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )
    runner._start_current_run_record("claude", "opus")

    asyncio.run(runner.handle_watch())

    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=runner.name,
        )
    )

    assert runner.state.state == PipelineState.IDLE
    assert len(recent) == 1
    assert recent[0].exit_reason == "closed_unmerged"


def test_handle_watch_unknown_merge_state_logs_but_does_not_save(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runner_module.github_client, "get_open_prs", lambda repo, **kw: []
    )
    monkeypatch.setattr(
        runner_module.github_client, "is_pr_merged", lambda repo, number: None
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )
    runner._start_current_run_record("claude", "opus")

    asyncio.run(runner.handle_watch())

    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=runner.name,
        )
    )

    assert runner.state.state == PipelineState.IDLE
    assert recent == []
    assert any(
        "state unknown" in entry["event"] for entry in runner.state.history
    )


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


def test_handle_hung_sets_error_when_fallback_post_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(repo: str, number: int, body: str) -> None:
        raise RuntimeError("gh unavailable")

    monkeypatch.setattr(runner_module.github_client, "post_comment", boom)

    runner = _make_runner()
    runner.state.state = PipelineState.HUNG
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_hung())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "post_comment failed: gh unavailable"
    assert any(
        entry["event"] == "gh unavailable" for entry in runner.state.history
    )


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


def test_handle_hung_stays_hung_when_pr_state_lookup_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(*args: object, **kwargs: object) -> dict[str, str]:
        raise RuntimeError("gh view failed")

    monkeypatch.setattr(runner_module.github_client, "run_gh", boom)
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
    assert runner.state.current_task is not None
    assert any(
        "hung: failed to check PR state: gh view failed; staying HUNG"
        in entry["event"]
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


def test_handle_merge_without_current_pr_sets_idle() -> None:
    runner = _make_runner()
    runner.state.state = PipelineState.MERGE

    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None


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


def test_mark_queue_done_returns_without_current_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_git(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("git should not run without a current task")

    monkeypatch.setattr(git_ops_module, "_git", fail_git)

    runner = _make_runner()
    runner._mark_queue_done()

    assert runner.state.pending_queue_sync_branch is None
    assert runner.state.pending_queue_sync_started_at is None


def test_mark_queue_done_clears_pending_when_queue_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    git_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def fake_git(
        repo_path: str, *args: str, **kwargs: Any
    ) -> _FakeCompletedProcess:
        git_calls.append(((repo_path, *args), kwargs))
        return _FakeCompletedProcess(args=["git", *args], returncode=0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="first", status=TaskStatus.DOING
    )

    runner._mark_queue_done()

    assert runner.state.pending_queue_sync_branch is None
    assert runner.state.pending_queue_sync_started_at is None
    assert [
        call_args[1]
        for call_args, _ in git_calls
    ] == ["fetch", "checkout", "reset"]


def test_mark_queue_done_clears_pending_when_queue_update_is_noop(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    (queue_dir / "QUEUE.md").write_text("## PR-999: first\n- Status: TODO\n")

    git_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def fake_git(
        repo_path: str, *args: str, **kwargs: Any
    ) -> _FakeCompletedProcess:
        git_calls.append(((repo_path, *args), kwargs))
        return _FakeCompletedProcess(args=["git", *args], returncode=0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="first", status=TaskStatus.DOING
    )

    runner._mark_queue_done()

    assert runner.state.pending_queue_sync_branch is None
    assert runner.state.pending_queue_sync_started_at is None
    assert [
        call_args[1]
        for call_args, _ in git_calls
    ] == ["fetch", "checkout", "reset"]


def test_mark_queue_done_falls_back_to_immediate_merge_when_auto_rejected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    (queue_dir / "QUEUE.md").write_text("## PR-001: first\n- Status: DOING\n")

    def fail_base_push(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "push"] and "main" in cmd:
            return _FakeCompletedProcess(
                args=cmd, returncode=1, stderr="push rejected"
            )
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fail_base_push)

    gh_calls: list[list[str]] = []

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> None:
        gh_calls.append(cmd)
        if cmd[:4] == ["pr", "merge", "queue-done-pr-001", "--squash"] and (
            "--auto" in cmd
        ):
            raise RuntimeError("auto disabled")

    monkeypatch.setattr(runner_module.github_client, "run_gh", fake_run_gh)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="first", status=TaskStatus.DOING
    )
    events: list[str] = []
    monkeypatch.setattr(runner, "log_event", events.append)

    runner._mark_queue_done()

    assert gh_calls == [
        [
            "pr", "create",
            "--base", "main",
            "--head", "queue-done-pr-001",
            "--title", "PR-001: mark DONE in QUEUE.md",
            "--body",
            "Post-merge queue sync for PR-001 (auto-generated by the daemon).",
        ],
        ["pr", "merge", "queue-done-pr-001", "--squash", "--delete-branch", "--auto"],
        ["pr", "merge", "queue-done-pr-001", "--squash", "--delete-branch"],
    ]
    assert any(
        "queue-sync --auto rejected (auto disabled); attempting immediate merge"
        in event
        for event in events
    )


def test_mark_queue_done_leaves_pr_open_when_immediate_merge_also_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    (queue_dir / "QUEUE.md").write_text("## PR-001: first\n- Status: DOING\n")

    def fail_base_push(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "push"] and "main" in cmd:
            return _FakeCompletedProcess(
                args=cmd, returncode=1, stderr="push rejected"
            )
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fail_base_push)

    merge_attempts = 0

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> None:
        nonlocal merge_attempts
        if cmd[:2] == ["pr", "merge"]:
            merge_attempts += 1
            raise RuntimeError(f"merge failure {merge_attempts}")

    monkeypatch.setattr(runner_module.github_client, "run_gh", fake_run_gh)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="first", status=TaskStatus.DOING
    )
    events: list[str] = []
    monkeypatch.setattr(runner, "log_event", events.append)

    runner._mark_queue_done()

    assert merge_attempts == 2
    assert any(
        "queue-sync immediate merge also failed; PR left open for later resolution"
        in event
        for event in events
    )


def test_mark_queue_done_resets_repo_before_reraising(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    (queue_dir / "QUEUE.md").write_text("## PR-001: first\n- Status: DOING\n")

    git_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def fake_git(
        repo_path: str, *args: str, **kwargs: Any
    ) -> _FakeCompletedProcess:
        git_calls.append(((repo_path, *args), kwargs))
        if args[:3] == ("push", "origin", "main"):
            return _FakeCompletedProcess(
                args=["git", *args], returncode=1, stderr="push rejected"
            )
        return _FakeCompletedProcess(args=["git", *args], returncode=0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> None:
        if cmd[:2] == ["pr", "create"]:
            raise RuntimeError("gh create failed")

    monkeypatch.setattr(runner_module.github_client, "run_gh", fake_run_gh)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="first", status=TaskStatus.DOING
    )

    with pytest.raises(RuntimeError, match="gh create failed"):
        runner._mark_queue_done()

    assert git_calls[-2:] == [
        (
            (str(tmp_path), "reset", "--hard", "origin/main"),
            {"check": False},
        ),
        (
            (str(tmp_path), "checkout", "main"),
            {"check": False},
        ),
    ]


def test_resolve_pending_queue_sync_returns_true_without_branch() -> None:
    runner = _make_runner()

    assert runner._resolve_pending_queue_sync() is True


def test_resolve_pending_queue_sync_continues_when_pr_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    escalations: list[str] = []
    monkeypatch.setattr(
        runner_module.github_client,
        "run_gh",
        lambda cmd, **kwargs: {"state": "open", "mergedAt": None},
    )

    runner = _make_runner()
    runner.state.pending_queue_sync_branch = "queue-done-pr-001"
    runner.state.pending_queue_sync_started_at = datetime.now(timezone.utc)
    monkeypatch.setattr(
        runner,
        "_escalate_queue_sync_if_expired",
        lambda branch: escalations.append(branch),
    )

    assert runner._resolve_pending_queue_sync() is False
    assert runner.state.pending_queue_sync_branch == "queue-done-pr-001"
    assert escalations == ["queue-done-pr-001"]


def test_resolve_pending_queue_sync_clears_state_when_pr_merged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runner_module.github_client,
        "run_gh",
        lambda cmd, **kwargs: {
            "state": "merged",
            "mergedAt": "2026-04-19T18:00:00Z",
        },
    )

    runner = _make_runner()
    runner.state.pending_queue_sync_branch = "queue-done-pr-001"
    runner.state.pending_queue_sync_started_at = datetime.now(timezone.utc)
    events: list[str] = []
    monkeypatch.setattr(runner, "log_event", events.append)

    assert runner._resolve_pending_queue_sync() is True
    assert runner.state.pending_queue_sync_branch is None
    assert runner.state.pending_queue_sync_started_at is None
    assert events == ["Queue-sync PR merged (queue-done-pr-001)"]


def test_resolve_pending_queue_sync_clears_state_when_pr_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runner_module.github_client,
        "run_gh",
        lambda cmd, **kwargs: {"state": "closed", "mergedAt": None},
    )

    runner = _make_runner()
    runner.state.pending_queue_sync_branch = "queue-done-pr-001"
    runner.state.pending_queue_sync_started_at = datetime.now(timezone.utc)
    events: list[str] = []
    monkeypatch.setattr(runner, "log_event", events.append)

    assert runner._resolve_pending_queue_sync() is False
    assert runner.state.pending_queue_sync_branch is None
    assert runner.state.pending_queue_sync_started_at is None
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == (
        "queue-sync PR queue-done-pr-001 closed without merging"
    )
    assert events == [runner.state.error_message]


def test_resolve_pending_queue_sync_handles_missing_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    escalations: list[str] = []
    monkeypatch.setattr(
        runner_module.github_client,
        "run_gh",
        lambda cmd, **kwargs: None,
    )

    runner = _make_runner()
    runner.state.pending_queue_sync_branch = "queue-done-pr-001"
    monkeypatch.setattr(
        runner,
        "_escalate_queue_sync_if_expired",
        lambda branch: escalations.append(branch),
    )

    assert runner._resolve_pending_queue_sync() is False
    assert runner.state.pending_queue_sync_branch == "queue-done-pr-001"
    assert escalations == ["queue-done-pr-001"]


def test_resolve_pending_queue_sync_logs_and_escalates_on_view_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    escalations: list[str] = []

    def fail_run_gh(cmd: list[str], **kwargs: Any) -> None:
        raise RuntimeError("gh unavailable")

    monkeypatch.setattr(runner_module.github_client, "run_gh", fail_run_gh)

    runner = _make_runner()
    runner.state.pending_queue_sync_branch = "queue-done-pr-001"
    events: list[str] = []
    monkeypatch.setattr(runner, "log_event", events.append)
    monkeypatch.setattr(
        runner,
        "_escalate_queue_sync_if_expired",
        lambda branch: escalations.append(branch),
    )

    assert runner._resolve_pending_queue_sync() is False
    assert events == ["queue-sync PR queue-done-pr-001 view failed: gh unavailable"]
    assert escalations == ["queue-done-pr-001"]


def test_escalate_queue_sync_no_op_when_started_at_missing() -> None:
    runner = _make_runner()
    runner.state.pending_queue_sync_branch = "queue-done-pr-001"

    runner._escalate_queue_sync_if_expired("queue-done-pr-001")

    assert runner.state.pending_queue_sync_branch == "queue-done-pr-001"
    assert runner.state.pending_queue_sync_started_at is None
    assert runner.state.state == PipelineState.IDLE


def test_escalate_queue_sync_no_op_when_not_expired() -> None:
    runner = _make_runner()
    runner.state.pending_queue_sync_branch = "queue-done-pr-001"
    runner.state.pending_queue_sync_started_at = (
        datetime.now(timezone.utc) - timedelta(minutes=5)
    )

    runner._escalate_queue_sync_if_expired("queue-done-pr-001")

    assert runner.state.pending_queue_sync_branch == "queue-done-pr-001"
    assert runner.state.pending_queue_sync_started_at is not None
    assert runner.state.state == PipelineState.IDLE


def test_escalate_queue_sync_transitions_to_error_when_expired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    runner.state.pending_queue_sync_branch = "queue-done-pr-001"
    runner.state.pending_queue_sync_started_at = (
        datetime.now(timezone.utc) - timedelta(hours=2)
    )
    events: list[str] = []
    monkeypatch.setattr(runner, "log_event", events.append)

    runner._escalate_queue_sync_if_expired("queue-done-pr-001")

    assert runner.state.pending_queue_sync_branch is None
    assert runner.state.pending_queue_sync_started_at is None
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message is not None
    assert "queue-sync PR queue-done-pr-001 unresolved after " in (
        runner.state.error_message
    )
    assert f"(max {merge_module._QUEUE_SYNC_MAX_WAIT_SEC}s)" in (
        runner.state.error_message
    )
    assert events == [runner.state.error_message]


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


def test_handle_merge_marks_pr_ready_before_merge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)

    call_order: list[str] = []

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> None:
        assert cmd == ["pr", "ready", "5"]
        call_order.append("ready")

    def fake_merge_pr(repo: str, num: int) -> None:
        assert (repo, num) == (runner.owner_repo, 5)
        call_order.append("merge")

    monkeypatch.setattr(runner_module.github_client, "run_gh", fake_run_gh)
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
    assert call_order == ["ready", "merge"]


def test_handle_merge_ignores_pr_ready_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)

    call_order: list[str] = []

    def fail_run_gh(cmd: list[str], **kwargs: Any) -> None:
        assert cmd == ["pr", "ready", "5"]
        call_order.append("ready")
        raise RuntimeError("ready failed")

    def fake_merge_pr(repo: str, num: int) -> None:
        assert (repo, num) == (runner.owner_repo, 5)
        call_order.append("merge")

    monkeypatch.setattr(runner_module.github_client, "run_gh", fail_run_gh)
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
    assert call_order == ["ready", "merge"]


def test_handle_merge_captures_success_stats_before_queue_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_git(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "merge"] and "origin/main" in cmd:
            return _FakeCompletedProcess(
                args=cmd, stdout="Already up to date.\n", returncode=0
            )
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_git)
    monkeypatch.setattr(
        runner_module.github_client, "merge_pr", lambda repo, num: None
    )

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

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )
    runner._start_current_run_record("claude", "opus")
    monkeypatch.setattr(runner, "_compute_diff_stats", fake_compute)
    monkeypatch.setattr(
        runner_module.PipelineRunner, "_mark_queue_done", fake_mark
    )

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


def test_handle_merge_aborts_when_conflict_resolution_is_rate_limited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    git_calls: list[tuple[str, ...]] = []

    def fake_git(
        repo_path: str,
        *args: str,
        **kwargs: Any,
    ) -> _FakeCompletedProcess:
        git_calls.append(args)
        if args[:2] == ("merge", "origin/main"):
            return _FakeCompletedProcess(
                args=["git", *args],
                returncode=1,
                stdout="CONFLICT (content): merge conflict in foo",
            )
        return _FakeCompletedProcess(args=["git", *args], returncode=0)

    async def fake_check_rate_limit(*args: Any, **kwargs: Any) -> bool:
        return False

    claude_calls: list[tuple[Any, ...]] = []
    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_check_rate_limit",
        fake_check_rate_limit,
    )
    monkeypatch.setattr(
        claude_cli,
        "run_claude_async",
        lambda *args, **kwargs: claude_calls.append(args),
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )

    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.WATCH
    assert ("merge", "--abort") in git_calls
    assert not claude_calls


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
    runner._start_current_run_record("claude", "opus")
    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.WATCH
    assert claude_calls, "Claude must be invoked on merge conflict"
    assert not merge_pr_calls, (
        "merge_pr must not run with stale gate results after sync push"
    )
    assert runner._current_run_record is not None
    assert runner._current_run_record.had_merge_conflict is True
    assert any(
        cmd[:2] == ["git", "push"] and "pr-001" in cmd for cmd in git_calls
    ), "conflict-resolved HEAD must be pushed to origin"


def test_handle_merge_falls_back_to_codex_for_conflict_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    git_calls: list[tuple[str, ...]] = []

    def fake_git(
        repo_path: str,
        *args: str,
        **kwargs: Any,
    ) -> _FakeCompletedProcess:
        git_calls.append(args)
        if args[:2] == ("merge", "origin/main"):
            return _FakeCompletedProcess(
                args=["git", *args],
                returncode=1,
                stdout="CONFLICT (content): merge conflict in foo",
            )
        return _FakeCompletedProcess(args=["git", *args], returncode=0)

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
    ) -> _FakeCompletedProcess:
        git_calls.append(args)
        if args[:2] == ("merge", "origin/main"):
            return _FakeCompletedProcess(
                args=["git", *args],
                returncode=1,
                stdout="CONFLICT (content): merge conflict in foo",
            )
        return _FakeCompletedProcess(args=["git", *args], returncode=0)

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

    assert runner.state.state == PipelineState.ERROR
    assert (
        runner.state.error_message
        == "No eligible coder available for merge conflict resolution"
    )
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

    fetch_cmds = [
        cmd for cmd in git_calls
        if cmd[:4] == ["git", "fetch", "--prune", "origin"]
    ]
    assert fetch_cmds and any("pr-001" in cmd for cmd in fetch_cmds), (
        "must fetch origin/<pr_branch> with --prune before local merge"
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


def test_handle_merge_sets_error_on_non_conflict_sync_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_git(
        repo_path: str,
        *args: str,
        **kwargs: Any,
    ) -> _FakeCompletedProcess:
        if args[:2] == ("merge", "origin/main"):
            return _FakeCompletedProcess(
                args=["git", *args],
                returncode=1,
                stderr="fatal: unrelated histories",
            )
        return _FakeCompletedProcess(args=["git", *args], returncode=0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)

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

    assert runner.state.state == PipelineState.ERROR
    assert "fatal: unrelated histories" in (runner.state.error_message or "")
    assert len(recent) == 1
    assert recent[0].exit_reason == "error"


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


def test_handle_merge_pauses_when_conflict_resolution_hits_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
        return (1, "", "Error: 429 Too Many Requests")

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

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.error_message is None
    assert runner.state.rate_limited_until is not None
    assert not merge_pr_calls, "merge_pr must not be called while paused"
    abort_cmds = [
        cmd for cmd in git_calls
        if cmd[:3] == ["git", "merge", "--abort"]
    ]
    assert abort_cmds, "git merge --abort must be invoked"
    assert any(
        "Rate limit pause active until" in e["event"] for e in runner.state.history
    )


def test_handle_merge_sets_error_when_pre_sync_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_retry_transient(op: Any, **kwargs: Any) -> _FakeCompletedProcess:
        return op()

    def fake_git(
        repo_path: str,
        *args: str,
        **kwargs: Any,
    ) -> _FakeCompletedProcess:
        if args[:4] == ("fetch", "--prune", "origin", "main"):
            raise OSError("network down")
        return _FakeCompletedProcess(args=["git", *args], returncode=0)

    monkeypatch.setattr(merge_module, "retry_transient", fake_retry_transient)
    monkeypatch.setattr(git_ops_module, "_git", fake_git)

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

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "Pre-merge sync failed: network down"
    assert len(recent) == 1
    assert recent[0].exit_reason == "error"


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


def test_handle_error_falls_back_to_codex_for_diagnosis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    codex_calls: list[tuple[str, str, str | None]] = []

    async def fake_codex_diag(
        repo_path: str,
        context: str,
        model: str | None = None,
    ) -> tuple[int, str, str]:
        codex_calls.append((repo_path, context, model))
        return (0, "ESCALATE", "")

    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_select_auxiliary_coder",
        lambda self: ("codex", self._registry.get("codex")),
    )
    monkeypatch.setattr(codex_cli, "diagnose_error_async", fake_codex_diag)
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("Claude should not be used when Codex is selected")
        ),
    )

    runner = _make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "boom"

    asyncio.run(runner.handle_error())

    assert codex_calls == [(runner.repo_path, "boom", runner.app_config.daemon.codex_model)]
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "boom"


def test_handle_error_logs_when_no_auxiliary_coder_is_eligible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claude_calls: list[object] = []
    codex_calls: list[object] = []

    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_select_auxiliary_coder",
        lambda self: None,
    )
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        lambda *args, **kwargs: claude_calls.append(args),
    )
    monkeypatch.setattr(
        codex_cli,
        "diagnose_error_async",
        lambda *args, **kwargs: codex_calls.append(args),
    )

    runner = _make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "boom"

    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "boom"
    assert not claude_calls
    assert not codex_calls
    assert any(
        e["event"] == "No eligible coder available for error diagnosis; staying ERROR"
        for e in runner.state.history
    )


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


def test_handle_error_commits_and_pushes_diagnose_fixes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runner, calls, _, review_requests = _run_dirty_diagnose(monkeypatch, tmp_path)

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 1
    assert runner.state.current_pr.last_activity is not None
    assert runner._last_push_at is not None
    assert runner._last_push_at_pr_number == 119
    assert review_requests == [119]
    assert [cmd[0] for cmd in calls] == [
        "status",
        "status",
        "rev-parse",
        "rev-parse",
        "add",
        "commit",
        "push",
    ]
    assert calls[-1] == (
        "push",
        "origin",
        "HEAD:fix/diagnose-error-commits-fixes",
    )


def test_handle_error_resets_when_push_fails_and_escalates(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runner, calls, warnings, _ = _run_dirty_diagnose(
        monkeypatch, tmp_path, push_exc=RuntimeError("push failed")
    )

    assert runner.state.state == PipelineState.ERROR
    assert [cmd[0] for cmd in calls] == [
        "status",
        "status",
        "rev-parse",
        "rev-parse",
        "add",
        "commit",
        "push",
        "reset",
        "clean",
    ]
    assert any(cmd[:3] == ("reset", "--hard", "abc123") for cmd in calls)
    assert any(cmd[:2] == ("clean", "-fd") for cmd in calls)
    assert warnings == ["diagnose_error made uncommittable changes, reset"]


def test_handle_error_errors_when_review_trigger_fails_after_diagnose_push(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runner, calls, _, review_requests = _run_dirty_diagnose(
        monkeypatch, tmp_path, review_post_ok=False
    )

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 1
    assert runner._last_push_at is not None
    assert runner._last_push_at_pr_number == 119
    assert review_requests == [119]
    assert [cmd[0] for cmd in calls] == [
        "status",
        "status",
        "rev-parse",
        "rev-parse",
        "add",
        "commit",
        "push",
    ]
    assert (
        runner.state.error_message
        == "Failed to post @codex review on PR #119 after "
        "diagnose_error fix push; manual review trigger required "
        "to avoid fix/push loop"
    )
    assert any(
        e["event"] == runner.state.error_message for e in runner.state.history
    )


def test_handle_error_escalates_dirty_tree_without_active_pr_branch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runner, calls, _warnings, _ = _run_dirty_diagnose(
        monkeypatch, tmp_path, with_pr=False
    )

    assert runner.state.state == PipelineState.ERROR
    assert [cmd[0] for cmd in calls] == [
        "status",
        "status",
        "rev-parse",
        "reset",
        "clean",
    ]
    assert any(
        e["event"] == "diagnose_error: dirty tree without active PR/task branch"
        for e in runner.state.history
    )


def test_handle_error_head_before_defaults_empty_when_rev_parse_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    changed = repo / "fix.txt"
    calls: list[tuple[str, ...]] = []

    async def fake_diag(*args: object, **kwargs: object) -> tuple[int, str, str]:
        changed.write_text("fixed\n")
        return (0, "FIX\nrepair broken config", "")

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(args)
        if args[:2] == ("status", "--porcelain"):
            status = ""
            if changed.exists():
                status = " M fix.txt\n"
            return _FakeCompletedProcess(stdout=status)
        if args[:2] == ("rev-parse", "HEAD"):
            raise subprocess.CalledProcessError(128, ["git", *args], "boom")
        if args[:3] == ("rev-parse", "--abbrev-ref", "HEAD"):
            return _FakeCompletedProcess(stdout="fix/diagnose-error-commits-fixes\n")
        return _FakeCompletedProcess()

    monkeypatch.setattr(claude_cli, "diagnose_error_async", fake_diag)
    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(error_module, "retry_transient", lambda op, **_: op())
    runner = _make_runner()
    monkeypatch.setattr(runner, "_post_codex_review", lambda pr_number: True)
    runner.repo_path = str(repo)
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "boom"
    runner.state.current_pr = PRInfo(
        number=119, branch="fix/diagnose-error-commits-fixes"
    )

    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.IDLE
    assert [cmd[0] for cmd in calls] == [
        "status",
        "status",
        "rev-parse",
        "rev-parse",
        "add",
        "commit",
        "push",
    ]
    assert not any(cmd[:1] == ("reset",) for cmd in calls)
    assert not any(cmd[:1] == ("clean",) for cmd in calls)


def test_handle_error_uses_current_task_branch_when_no_current_pr_and_task_branch_differs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    changed = repo / "fix.txt"
    calls: list[tuple[str, ...]] = []
    review_requests: list[int] = []

    async def fake_diag(*args: object, **kwargs: object) -> tuple[int, str, str]:
        changed.write_text("fixed\n")
        return (0, "FIX\nrepair broken config", "")

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(args)
        if args[:2] == ("status", "--porcelain"):
            status = ""
            if changed.exists():
                status = " M fix.txt\n"
            return _FakeCompletedProcess(stdout=status)
        if args[:3] == ("rev-parse", "--abbrev-ref", "HEAD"):
            return _FakeCompletedProcess(stdout="feature-x\n")
        if args[:2] == ("rev-parse", "HEAD"):
            return _FakeCompletedProcess(stdout="abc123\n")
        return _FakeCompletedProcess()

    monkeypatch.setattr(claude_cli, "diagnose_error_async", fake_diag)
    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(error_module, "retry_transient", lambda op, **_: op())
    runner = _make_runner()
    monkeypatch.setattr(
        runner,
        "_post_codex_review",
        lambda pr_number: review_requests.append(pr_number) or True,
    )
    runner.repo_path = str(repo)
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "boom"
    runner.state.current_task = QueueTask(
        pr_id="PR-101",
        title="t",
        branch="feature-x",
        status=TaskStatus.DOING,
    )

    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.IDLE
    assert review_requests == []
    assert calls[-1] == ("push", "origin", "HEAD:feature-x")


def test_handle_error_escalates_dirty_tree_when_branch_mismatches_pr(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runner, calls, warnings, _ = _run_dirty_diagnose(
        monkeypatch, tmp_path, head_branch="main"
    )

    assert runner.state.state == PipelineState.ERROR
    assert [cmd[0] for cmd in calls] == [
        "status",
        "status",
        "rev-parse",
        "rev-parse",
        "reset",
        "clean",
    ]
    assert warnings == ["diagnose_error made uncommittable changes, reset"]
    assert any(
        "diagnose_error: active branch mismatch ('main' != "
        "'fix/diagnose-error-commits-fixes')"
        == e["event"]
        for e in runner.state.history
    )


def test_handle_error_discards_dirty_tree_for_non_fix_verdict(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runner, calls, warnings, _ = _run_dirty_diagnose(
        monkeypatch, tmp_path, diagnosis_stdout="ESCALATE\nhuman help"
    )

    assert runner.state.state == PipelineState.ERROR
    assert [cmd[0] for cmd in calls] == [
        "status",
        "status",
        "rev-parse",
        "reset",
        "clean",
    ]
    assert warnings == []


def test_handle_error_non_fix_dirty_tree_skips_cleanup_when_head_lookup_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    changed = repo / "fix.txt"
    calls: list[tuple[str, ...]] = []

    async def fake_diag(*args: object, **kwargs: object) -> tuple[int, str, str]:
        changed.write_text("dirty\n")
        return (0, "SKIP\nskip this task", "")

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(args)
        if args[:2] == ("status", "--porcelain"):
            status = ""
            if changed.exists():
                status = " M fix.txt\n"
            return _FakeCompletedProcess(stdout=status)
        if args[:2] == ("rev-parse", "HEAD"):
            raise subprocess.CalledProcessError(128, ["git", *args], "boom")
        return _FakeCompletedProcess()

    monkeypatch.setattr(claude_cli, "diagnose_error_async", fake_diag)
    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    runner = _make_runner()
    runner.repo_path = str(repo)
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "boom"
    runner.state.current_task = QueueTask(
        pr_id="PR-101", title="t", branch="feature-x", status=TaskStatus.DOING
    )
    runner.state.current_pr = PRInfo(number=119, branch="feature-x")

    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert [cmd[0] for cmd in calls] == ["status", "status", "rev-parse"]
    assert not any(cmd[:1] == ("reset",) for cmd in calls)
    assert not any(cmd[:1] == ("clean",) for cmd in calls)


def test_handle_error_escalates_without_publishing_preexisting_dirty_tree(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runner, calls, warnings, review_requests = _run_dirty_diagnose(
        monkeypatch,
        tmp_path,
        preexisting_dirty=" M unrelated.txt\n",
    )

    assert runner.state.state == PipelineState.ERROR
    assert [cmd[0] for cmd in calls] == ["status", "status"]
    assert warnings == []
    assert review_requests == []
    assert any(
        e["event"]
        == "diagnose_error: pre-existing dirty tree blocks automatic cleanup/publish"
        for e in runner.state.history
    )


def test_publish_state_writes_to_redis() -> None:
    runner = _make_runner()
    asyncio.run(runner.publish_state())

    assert isinstance(runner.redis, _FakeRedis)
    assert len(runner.redis.writes) == 1
    key, payload = runner.redis.writes[0]
    assert key == f"pipeline:{runner.name}"
    assert runner.name in payload


def test_publish_state_preserves_concurrent_pause_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    runner.redis.store[f"pipeline:{runner.name}"] = RepoState(
        url=runner.repo_config.url,
        name=runner.name,
        state=PipelineState.IDLE,
        user_paused=False,
    ).model_dump_json()

    def fake_fetch() -> None:
        runner.redis.store[f"pipeline:{runner.name}"] = RepoState(
            url=runner.repo_config.url,
            name=runner.name,
            state=PipelineState.IDLE,
            user_paused=True,
        ).model_dump_json()
        return None

    monkeypatch.setattr(runner._claude_usage_provider, "fetch", fake_fetch)

    asyncio.run(runner.publish_state())

    stored = RepoState.model_validate_json(runner.redis.store[f"pipeline:{runner.name}"])
    assert stored.user_paused is True


def test_publish_state_keeps_selected_fallback_coder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _allow_all_coder_auth(monkeypatch)
    runner = _make_runner()
    runner.state.rate_limited_coders.add("claude")

    name, _plugin = runner._get_coder()
    asyncio.run(runner.publish_state())

    assert name == "codex"
    assert runner.state.coder == "codex"


def test_publish_state_copies_usage_snapshot_to_state() -> None:
    from src.usage import UsageSnapshot

    runner = _make_runner()
    runner._claude_usage_provider = _FakeUsageProvider(
        snapshot=UsageSnapshot(
            session_percent=90,
            session_resets_at=123,
            weekly_percent=10,
            weekly_resets_at=456,
            fetched_at=0,
        )
    )

    asyncio.run(runner.publish_state())

    assert runner.state.usage_session_percent == 90
    assert runner.state.usage_session_resets_at == 123
    assert runner.state.usage_weekly_percent == 10
    assert runner.state.usage_weekly_resets_at == 456


def test_publish_state_for_inactive_repo_forces_idle_payload() -> None:
    runner = _make_runner(active=False)
    runner.state.state = PipelineState.ERROR

    asyncio.run(runner.publish_state())

    assert runner.redis.writes
    _key, payload = runner.redis.writes[-1]
    state = json.loads(payload)
    assert state["state"] == PipelineState.IDLE.value
    assert runner.state.state == PipelineState.ERROR


def test_publish_state_migrates_owned_legacy_upload_key() -> None:
    runner = _make_runner(url="https://github.com/octo/demo-renamed.git")
    assert isinstance(runner.redis, _FakeRedis)
    runner._old_basename = "demo"
    runner.redis.store["pipeline:demo"] = json.dumps(
        {"url": "https://github.com/octo/demo-renamed.git"}
    )
    runner.redis.store["upload:demo:pending"] = "pending"

    asyncio.run(runner.publish_state())

    assert "pipeline:demo" not in runner.redis.store
    assert f"pipeline:{runner.name}" in runner.redis.store
    assert "upload:demo:pending" not in runner.redis.store
    assert runner.redis.store[f"upload:{runner.name}:pending"] == "pending"


def test_publish_state_ignores_legacy_upload_migration_error() -> None:
    class _BrokenRenameRedis(_FakeRedis):
        async def exists(self, key: str) -> int:
            raise RuntimeError("rename failed")

    runner = _make_runner(url="https://github.com/octo/demo-renamed.git")
    runner.redis = _BrokenRenameRedis()
    runner._old_basename = "demo"
    runner.redis.store["pipeline:demo"] = json.dumps(
        {"url": "https://github.com/octo/demo-renamed.git"}
    )
    runner.redis.store["upload:demo:pending"] = "pending"

    asyncio.run(runner.publish_state())

    assert runner.redis.store[f"pipeline:{runner.name}"]
    assert runner.redis.store["upload:demo:pending"] == "pending"


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


def test_handle_idle_new_open_pr_resets_fix_iteration_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [PRInfo(number=42, branch="fresh-branch")],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = _make_runner()
    runner.state.current_pr = PRInfo(
        number=99,
        branch="old-branch",
        fix_iteration_count=7,
    )

    asyncio.run(runner.handle_idle())

    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 42
    assert runner.state.current_pr.fix_iteration_count == 0


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


def test_handle_idle_keeps_dag_task_when_queue_validation_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    dag_task = QueueTask(
        pr_id="PR-123",
        title="Structured task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-123.md",
        branch="pr-123-structured",
    )
    dag_tasks = [dag_task]

    async def fake_select(self):
        self._idle_dag_tasks = dag_tasks
        return dag_task

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: (_ for _ in ()).throw(
            idle_module.QueueValidationError(
                ["Queue validation failed:\n- malformed queue"]
            )
        ),
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

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True
        return None

    runner = _make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert coding_called["v"] is True
    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task == dag_task
    assert runner.state.error_message is None
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 1
    assert any(
        "Queue validation failed after DAG selection" in entry["event"]
        for entry in runner.state.history
    )


def test_handle_idle_keeps_dag_state_when_queue_status_derivation_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    dag_task = QueueTask(
        pr_id="PR-123",
        title="Structured task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-123.md",
        branch="pr-123-structured",
    )
    dag_tasks = [dag_task]
    queue_tasks = [
        QueueTask(
            pr_id="PR-123",
            title="Structured task",
            status=TaskStatus.TODO,
            task_file="tasks/PR-123.md",
            branch="pr-123-structured",
        )
    ]

    async def fake_select(self):
        self._idle_dag_tasks = dag_tasks
        return dag_task

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: queue_tasks)
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            idle_module.QueueValidationError(
                ["tasks/QUEUE.md: PR-123 does not match task file"]
            )
        ),
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

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True
        return None

    runner = _make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert coding_called["v"] is True
    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task == dag_task
    assert runner.state.error_message is None
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 1
    assert any(
        "Task status derivation failed after DAG selection" in entry["event"]
        for entry in runner.state.history
    )


def test_handle_idle_keeps_dag_metrics_when_derivation_fails_with_visible_legacy_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_subprocess(monkeypatch)
    dag_task = QueueTask(
        pr_id="PR-123",
        title="Structured task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-123.md",
        branch="pr-123-structured",
    )
    dag_tasks = [dag_task]
    queue_tasks = [
        QueueTask(
            pr_id="PR-123",
            title="Structured task",
            status=TaskStatus.TODO,
            task_file="tasks/PR-123.md",
            branch="pr-123-structured",
        )
    ]

    async def fake_select(self):
        self._idle_dag_tasks = dag_tasks
        return dag_task

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: queue_tasks)
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            idle_module.QueueValidationError(
                ["tasks/QUEUE.md: PR-123 does not match task file"]
            )
        ),
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

    async def fake_handle_coding() -> None:
        return None

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    (queue_dir / "QUEUE.md").write_text(
        "# Task Queue\n\n## PR-001: Legacy queue task\n- Status: TODO\n",
        encoding="utf-8",
    )
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task == dag_task
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 1


def test_handle_idle_keeps_dag_state_when_queue_validation_fails_without_dag_pick(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    dag_tasks = [
        QueueTask(
            pr_id="PR-123",
            title="Structured task",
            status=TaskStatus.DONE,
            task_file="tasks/PR-123.md",
            branch="pr-123-structured",
        )
    ]

    async def fake_select(self):
        self._idle_dag_tasks = dag_tasks
        return None

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: (_ for _ in ()).throw(
            idle_module.QueueValidationError(
                ["Queue validation failed:\n- malformed queue"]
            )
        ),
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
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None
    assert runner.state.current_task is None
    assert runner.state.queue_done == 1
    assert runner.state.queue_total == 1
    assert any(
        "Queue validation failed after DAG selection" in entry["event"]
        for entry in runner.state.history
    )


def test_handle_idle_keeps_doing_dag_task_over_legacy_queue_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    dag_task = QueueTask(
        pr_id="PR-123",
        title="Structured in-flight task",
        status=TaskStatus.DOING,
        task_file="tasks/PR-123.md",
        branch="pr-123-structured",
    )
    dag_tasks = [dag_task]
    legacy_queue_task = QueueTask(
        pr_id="PR-001",
        title="Legacy queue task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-001.md",
        branch="pr-001-legacy",
    )

    async def fake_select(self):
        self._idle_dag_tasks = dag_tasks
        return dag_task

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [legacy_queue_task])
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: legacy_queue_task)
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
        return None

    runner = _make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert coding_called["v"] is True
    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task == dag_task
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 1


def test_handle_idle_marks_freshly_picked_dag_task_as_doing_in_regenerated_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A freshly picked structured TODO task must be regenerated as DOING in
    QUEUE.md before handle_coding runs, so the shim sees a DOING entry."""
    _patch_subprocess(monkeypatch)
    dag_task = QueueTask(
        pr_id="PR-123",
        title="Fresh structured task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-123.md",
        branch="pr-123-fresh",
    )
    header = TaskHeader(
        pr_id=dag_task.pr_id,
        title=dag_task.title,
        branch=dag_task.branch or "",
        task_type="feature",
        complexity="low",
        depends_on=[],
        priority=1,
        coder="any",
    )

    async def fake_select(self):
        self._idle_dag_tasks = [dag_task]
        self._idle_dag_headers = [header]
        self._idle_dag_statuses = {dag_task.pr_id: TaskStatus.TODO}
        return dag_task

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [dag_task])
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(), **kwargs: tasks,
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: dag_task)
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

    write_calls: list[tuple[list[TaskHeader], dict[str, TaskStatus]]] = []

    def fake_write_generated_queue_md(
        self,
        headers: list[TaskHeader],
        statuses: dict[str, TaskStatus],
    ) -> bool:
        write_calls.append((list(headers), dict(statuses)))
        return True

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_write_generated_queue_md",
        fake_write_generated_queue_md,
    )

    coding_called = {"v": False}

    async def fake_handle_coding() -> None:
        coding_called["v"] = True

    runner = _make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert coding_called["v"] is True
    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == dag_task.pr_id
    assert runner.state.current_task.status == TaskStatus.DOING
    assert len(write_calls) == 1
    written_headers, written_statuses = write_calls[0]
    assert written_headers == [header]
    assert written_statuses == {dag_task.pr_id: TaskStatus.DOING}


def test_handle_idle_skips_queue_regeneration_when_legacy_tasks_exist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    dag_task = QueueTask(
        pr_id="PR-123",
        title="Structured in-flight task",
        status=TaskStatus.DOING,
        task_file="tasks/PR-123.md",
        branch="pr-123-structured",
    )
    legacy_queue_task = QueueTask(
        pr_id="PR-001",
        title="Legacy queue task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-001.md",
        branch="pr-001-legacy",
    )

    async def fake_select(self):
        self._idle_dag_tasks = [dag_task]
        self._idle_dag_headers = [
            TaskHeader(
                pr_id=dag_task.pr_id,
                title=dag_task.title,
                branch=dag_task.branch or "",
                task_type="feature",
                complexity="low",
                depends_on=[],
                priority=1,
                coder="any",
            )
        ]
        self._idle_dag_statuses = {dag_task.pr_id: dag_task.status}
        return dag_task

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [legacy_queue_task])
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: legacy_queue_task)
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

    write_calls: list[tuple[list[TaskHeader], dict[str, TaskStatus]]] = []

    def fake_write_generated_queue_md(
        self,
        headers: list[TaskHeader],
        statuses: dict[str, TaskStatus],
    ) -> None:
        write_calls.append((headers, statuses))

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_write_generated_queue_md",
        fake_write_generated_queue_md,
    )

    async def fake_handle_coding() -> None:
        return None

    runner = _make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert write_calls == []


def test_handle_idle_skips_queue_regeneration_when_legacy_check_fails_with_visible_legacy_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_subprocess(monkeypatch)
    dag_task = QueueTask(
        pr_id="PR-123",
        title="Structured in-flight task",
        status=TaskStatus.DOING,
        task_file="tasks/PR-123.md",
        branch="pr-123-structured",
    )

    async def fake_select(self):
        self._idle_dag_tasks = [dag_task]
        self._idle_dag_headers = [
            TaskHeader(
                pr_id=dag_task.pr_id,
                title=dag_task.title,
                branch=dag_task.branch or "",
                task_type="feature",
                complexity="low",
                depends_on=[],
                priority=1,
                coder="any",
            )
        ]
        self._idle_dag_statuses = {dag_task.pr_id: dag_task.status}
        return dag_task

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: (_ for _ in ()).throw(
            idle_module.QueueValidationError(["Queue validation failed"])
        ),
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

    write_calls: list[tuple[list[TaskHeader], dict[str, TaskStatus]]] = []

    def fake_write_generated_queue_md(
        self,
        headers: list[TaskHeader],
        statuses: dict[str, TaskStatus],
    ) -> None:
        write_calls.append((headers, statuses))

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_write_generated_queue_md",
        fake_write_generated_queue_md,
    )

    async def fake_handle_coding() -> None:
        return None

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    (queue_dir / "QUEUE.md").write_text(
        "# Task Queue\n\n## PR-001: Legacy queue task\n- Status: TODO\n",
        encoding="utf-8",
    )
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert write_calls == []


def test_handle_idle_skips_queue_regeneration_when_visible_legacy_row_is_malformed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_subprocess(monkeypatch)
    dag_task = QueueTask(
        pr_id="PR-123",
        title="Structured in-flight task",
        status=TaskStatus.DOING,
        task_file="tasks/PR-123.md",
        branch="pr-123-structured",
    )

    async def fake_select(self):
        self._idle_dag_tasks = [dag_task]
        self._idle_dag_headers = [
            TaskHeader(
                pr_id=dag_task.pr_id,
                title=dag_task.title,
                branch=dag_task.branch or "",
                task_type="feature",
                complexity="low",
                depends_on=[],
                priority=1,
                coder="any",
            )
        ]
        self._idle_dag_statuses = {dag_task.pr_id: dag_task.status}
        return dag_task

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: (_ for _ in ()).throw(
            idle_module.QueueValidationError(["Queue validation failed"])
        ),
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

    write_calls: list[tuple[list[TaskHeader], dict[str, TaskStatus]]] = []

    def fake_write_generated_queue_md(
        self,
        headers: list[TaskHeader],
        statuses: dict[str, TaskStatus],
    ) -> bool:
        write_calls.append((headers, statuses))
        return True

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_write_generated_queue_md",
        fake_write_generated_queue_md,
    )

    async def fake_handle_coding() -> None:
        return None

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    (queue_dir / "QUEUE.md").write_text(
        "# Task Queue\n\n## PR-001 Legacy queue task\n- Status: TODO\n",
        encoding="utf-8",
    )
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert write_calls == []


def test_handle_idle_skips_queue_regeneration_when_successful_parse_still_has_visible_legacy_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_subprocess(monkeypatch)
    dag_task = QueueTask(
        pr_id="PR-123",
        title="Structured in-flight task",
        status=TaskStatus.DOING,
        task_file="tasks/PR-123.md",
        branch="pr-123-structured",
    )

    async def fake_select(self):
        self._idle_dag_tasks = [dag_task]
        self._idle_dag_headers = [
            TaskHeader(
                pr_id=dag_task.pr_id,
                title=dag_task.title,
                branch=dag_task.branch or "",
                task_type="feature",
                complexity="low",
                depends_on=[],
                priority=1,
                coder="any",
            )
        ]
        self._idle_dag_statuses = {dag_task.pr_id: dag_task.status}
        return dag_task

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: [dag_task],
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
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda *args, **kwargs: [dag_task],
    )
    monkeypatch.setattr(
        idle_module,
        "get_next_task",
        lambda tasks: dag_task,
    )

    write_calls: list[tuple[list[TaskHeader], dict[str, TaskStatus]]] = []

    def fake_write_generated_queue_md(
        self,
        headers: list[TaskHeader],
        statuses: dict[str, TaskStatus],
    ) -> bool:
        write_calls.append((headers, statuses))
        return True

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_write_generated_queue_md",
        fake_write_generated_queue_md,
    )

    async def fake_handle_coding() -> None:
        return None

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    (queue_dir / "QUEUE.md").write_text(
        "# Task Queue\n\n"
        "## PR-123: Structured in-flight task\n"
        "- Status: DOING\n"
        "- Tasks file: tasks/PR-123.md\n"
        "- Branch: pr-123-structured\n\n"
        "## PR-001 Legacy queue task\n"
        "- Status: TODO\n",
        encoding="utf-8",
    )
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert write_calls == []


def test_handle_idle_regenerates_queue_when_validation_fails_without_visible_legacy_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_subprocess(monkeypatch)
    dag_task = QueueTask(
        pr_id="PR-123",
        title="Structured in-flight task",
        status=TaskStatus.DOING,
        task_file="tasks/PR-123.md",
        branch="pr-123-structured",
    )

    async def fake_select(self):
        self._idle_dag_tasks = [dag_task]
        self._idle_dag_headers = [
            TaskHeader(
                pr_id=dag_task.pr_id,
                title=dag_task.title,
                branch=dag_task.branch or "",
                task_type="feature",
                complexity="low",
                depends_on=[],
                priority=1,
                coder="any",
            )
        ]
        self._idle_dag_statuses = {dag_task.pr_id: dag_task.status}
        return dag_task

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(
        idle_module,
        "parse_queue",
        lambda path, **kw: (_ for _ in ()).throw(
            idle_module.QueueValidationError(["Queue validation failed"])
        ),
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

    write_calls: list[tuple[list[TaskHeader], dict[str, TaskStatus]]] = []

    def fake_write_generated_queue_md(
        self,
        headers: list[TaskHeader],
        statuses: dict[str, TaskStatus],
    ) -> bool:
        write_calls.append((headers, statuses))
        return True

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_write_generated_queue_md",
        fake_write_generated_queue_md,
    )

    async def fake_handle_coding() -> None:
        return None

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    (queue_dir / "QUEUE.md").write_text(
        "# Task Queue\n\n## PR-123: Structured in-flight task\n- Status: TODO,\n",
        encoding="utf-8",
    )
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert write_calls == [([runner._idle_dag_headers[0]], {dag_task.pr_id: dag_task.status})]


def test_handle_idle_stops_when_queue_regeneration_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    dag_task = QueueTask(
        pr_id="PR-123",
        title="Structured in-flight task",
        status=TaskStatus.DOING,
        task_file="tasks/PR-123.md",
        branch="pr-123-structured",
    )

    async def fake_select(self):
        self._idle_dag_tasks = [dag_task]
        self._idle_dag_headers = [
            TaskHeader(
                pr_id=dag_task.pr_id,
                title=dag_task.title,
                branch=dag_task.branch or "",
                task_type="feature",
                complexity="low",
                depends_on=[],
                priority=1,
                coder="any",
            )
        ]
        self._idle_dag_statuses = {dag_task.pr_id: dag_task.status}
        return dag_task

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
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
        return None

    def fake_write_generated_queue_md(
        self,
        headers: list[TaskHeader],
        statuses: dict[str, TaskStatus],
    ) -> bool:
        raise RuntimeError("commit failed")

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_write_generated_queue_md",
        fake_write_generated_queue_md,
    )

    runner = _make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert coding_called["v"] is False
    assert runner.state.state == PipelineState.ERROR
    assert "QUEUE.md auto-generation failed" in (runner.state.error_message or "")


def test_handle_idle_continues_when_queue_regeneration_push_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    dag_task = QueueTask(
        pr_id="PR-123",
        title="Structured in-flight task",
        status=TaskStatus.DOING,
        task_file="tasks/PR-123.md",
        branch="pr-123-structured",
    )

    async def fake_select(self):
        self._idle_dag_tasks = [dag_task]
        self._idle_dag_headers = [
            TaskHeader(
                pr_id=dag_task.pr_id,
                title=dag_task.title,
                branch=dag_task.branch or "",
                task_type="feature",
                complexity="low",
                depends_on=[],
                priority=1,
                coder="any",
            )
        ]
        self._idle_dag_statuses = {dag_task.pr_id: dag_task.status}
        return dag_task

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
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
        return None

    def fake_write_generated_queue_md(
        self,
        headers: list[TaskHeader],
        statuses: dict[str, TaskStatus],
    ) -> bool:
        return False

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_write_generated_queue_md",
        fake_write_generated_queue_md,
    )

    runner = _make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert coding_called["v"] is True
    assert runner.state.state == PipelineState.CODING
    assert runner.state.current_task == dag_task
    assert any(
        "QUEUE.md auto-generation push rejected" in entry["event"]
        for entry in runner.state.history
    )


def test_handle_idle_stops_after_queue_regeneration_push_rejection_that_needs_resync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    dag_task = QueueTask(
        pr_id="PR-123",
        title="Structured in-flight task",
        status=TaskStatus.DOING,
        task_file="tasks/PR-123.md",
        branch="pr-123-structured",
    )

    async def fake_select(self):
        self._idle_dag_tasks = [dag_task]
        self._idle_dag_headers = [
            TaskHeader(
                pr_id=dag_task.pr_id,
                title=dag_task.title,
                branch=dag_task.branch or "",
                task_type="feature",
                complexity="low",
                depends_on=[],
                priority=1,
                coder="any",
            )
        ]
        self._idle_dag_statuses = {dag_task.pr_id: dag_task.status}
        return dag_task

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
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
        return None

    def fake_write_generated_queue_md(
        self,
        headers: list[TaskHeader],
        statuses: dict[str, TaskStatus],
    ) -> bool:
        self._idle_generated_queue_needs_resync = True
        return False

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_write_generated_queue_md",
        fake_write_generated_queue_md,
    )

    runner = _make_runner()
    runner.handle_coding = fake_handle_coding  # type: ignore[method-assign]
    asyncio.run(runner.handle_idle())

    assert coding_called["v"] is False
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert any(
        "QUEUE.md auto-generation refreshed origin state" in entry["event"]
        for entry in runner.state.history
    )


def test_handle_idle_does_not_promote_structured_queue_task_when_dag_blocks_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    dag_tasks = [
        QueueTask(
            pr_id="PR-123",
            title="Blocked structured task",
            status=TaskStatus.TODO,
            task_file="tasks/PR-123.md",
            branch="pr-123-structured",
            depends_on=["PR-001"],
        )
    ]
    queue_task = QueueTask(
        pr_id="PR-123",
        title="Blocked structured task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-123.md",
        branch="pr-123-structured",
        depends_on=["PR-001"],
    )

    async def fake_select(self):
        self._idle_dag_tasks = dag_tasks
        return None

    monkeypatch.setattr(idle_module.IdleMixin, "_select_next_task_from_dag", fake_select)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [queue_task])
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda tasks, repo_path, base_branch, prs, merged_prs=(): tasks,
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: queue_task)
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
    asyncio.run(runner.handle_idle())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.error_message is None
    assert runner.state.queue_done == 0
    assert runner.state.queue_total == 1
    assert any("No tasks available" in entry["event"] for entry in runner.state.history)


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


def test_generate_queue_md_format() -> None:
    runner = _make_runner()
    headers = [
        TaskHeader(
            pr_id="PR-001",
            title="Project bootstrap",
            branch="pr-001-bootstrap",
            task_type="feature",
            complexity="low",
            depends_on=[],
            priority=1,
            coder="any",
        ),
        TaskHeader(
            pr_id="PR-002",
            title="Config loader",
            branch="pr-002-models",
            task_type="feature",
            complexity="low",
            depends_on=["PR-001"],
            priority=2,
            coder="any",
        ),
    ]

    rendered = runner._generate_queue_md(
        headers,
        {
            "PR-001": TaskStatus.DONE,
            "PR-002": TaskStatus.TODO,
        },
    )

    assert rendered == (
        "# Task Queue\n\n"
        "## PR-001: Project bootstrap\n"
        "- Status: DONE\n"
        "- Tasks file: tasks/PR-001.md\n"
        "- Branch: pr-001-bootstrap\n\n"
        "## PR-002: Config loader\n"
        "- Status: TODO\n"
        "- Tasks file: tasks/PR-002.md\n"
        "- Branch: pr-002-models\n"
        "- Depends on: PR-001\n"
    )


def test_queue_md_not_committed_when_unchanged(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    headers = [
        TaskHeader(
            pr_id="PR-001",
            title="Project bootstrap",
            branch="pr-001-bootstrap",
            task_type="feature",
            complexity="low",
            depends_on=[],
            priority=1,
            coder="any",
        )
    ]
    statuses = {"PR-001": TaskStatus.DONE}
    queue_path.write_text(
        _make_runner()._generate_queue_md(headers, statuses),
        encoding="utf-8",
    )

    git_calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        git_calls.append(cmd)
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(git_ops_module.subprocess, "run", fake_run)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner._write_generated_queue_md(headers, statuses)

    assert git_calls == []
    assert queue_path.read_text(encoding="utf-8") == runner._generate_queue_md(
        headers,
        statuses,
    )


def test_write_generated_queue_md_resets_on_push_rejection(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    original = "# Task Queue\n\n## PR-000: Existing\n"
    queue_path.write_text(original, encoding="utf-8")
    headers = [
        TaskHeader(
            pr_id="PR-001",
            title="Project bootstrap",
            branch="pr-001-bootstrap",
            task_type="feature",
            complexity="low",
            depends_on=[],
            priority=1,
            coder="any",
        )
    ]
    statuses = {"PR-001": TaskStatus.DONE}

    git_calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        git_calls.append(cmd)
        if cmd[:2] == ["git", "push"]:
            queue_path.write_text("generated queue", encoding="utf-8")
            return _FakeCompletedProcess(args=cmd, returncode=1, stderr="rejected")
        if cmd[:2] == ["git", "reset"]:
            queue_path.write_text(original, encoding="utf-8")
            return _FakeCompletedProcess(args=cmd, returncode=0)
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(git_ops_module.subprocess, "run", fake_run)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)

    published = runner._write_generated_queue_md(headers, statuses)

    assert published is False
    assert queue_path.read_text(encoding="utf-8") == original
    assert any(cmd[:2] == ["git", "reset"] for cmd in git_calls)
    assert ["git", "reset", "--hard", "HEAD~1"] in git_calls


def test_write_generated_queue_md_retries_transient_push_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    headers = [
        TaskHeader(
            pr_id="PR-001",
            title="Project bootstrap",
            branch="pr-001-bootstrap",
            task_type="feature",
            complexity="low",
            depends_on=[],
            priority=1,
            coder="any",
        )
    ]
    statuses = {"PR-001": TaskStatus.DONE}

    git_calls: list[list[str]] = []
    push_attempts = {"count": 0}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        git_calls.append(cmd)
        if cmd[:2] == ["git", "push"]:
            push_attempts["count"] += 1
            if push_attempts["count"] < 3:
                return _FakeCompletedProcess(
                    args=cmd,
                    returncode=1,
                    stderr="operation timed out",
                )
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(git_ops_module.subprocess, "run", fake_run)
    monkeypatch.setattr(retry_module.time, "sleep", lambda _: None)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)

    published = runner._write_generated_queue_md(headers, statuses)

    assert published is True
    assert push_attempts["count"] == 3
    assert not any(cmd[:2] == ["git", "reset"] for cmd in git_calls)


def test_write_generated_queue_md_retries_numeric_503_push_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    headers = [
        TaskHeader(
            pr_id="PR-001",
            title="Project bootstrap",
            branch="pr-001-bootstrap",
            task_type="feature",
            complexity="low",
            depends_on=[],
            priority=1,
            coder="any",
        )
    ]
    statuses = {"PR-001": TaskStatus.DONE}

    git_calls: list[list[str]] = []
    push_attempts = {"count": 0}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        git_calls.append(cmd)
        if cmd[:2] == ["git", "push"]:
            push_attempts["count"] += 1
            if push_attempts["count"] < 3:
                return _FakeCompletedProcess(
                    args=cmd,
                    returncode=1,
                    stderr="fatal: The requested URL returned error: 503",
                )
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(git_ops_module.subprocess, "run", fake_run)
    monkeypatch.setattr(retry_module.time, "sleep", lambda _: None)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)

    published = runner._write_generated_queue_md(headers, statuses)

    assert published is True
    assert push_attempts["count"] == 3
    assert not any(cmd[:2] == ["git", "reset"] for cmd in git_calls)


def test_write_generated_queue_md_recovers_after_exhausted_transient_push_retries(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    original = "# Task Queue\n\n## PR-000: Existing\n"
    queue_path.write_text(original, encoding="utf-8")
    headers = [
        TaskHeader(
            pr_id="PR-001",
            title="Project bootstrap",
            branch="pr-001-bootstrap",
            task_type="feature",
            complexity="low",
            depends_on=[],
            priority=1,
            coder="any",
        )
    ]
    statuses = {"PR-001": TaskStatus.DONE}

    git_calls: list[list[str]] = []
    push_attempts = {"count": 0}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        git_calls.append(cmd)
        if cmd[:2] == ["git", "push"]:
            push_attempts["count"] += 1
            queue_path.write_text("generated queue", encoding="utf-8")
            return _FakeCompletedProcess(
                args=cmd,
                returncode=1,
                stderr="fatal: The requested URL returned error: 503",
            )
        if cmd[:2] == ["git", "reset"]:
            queue_path.write_text(original, encoding="utf-8")
            return _FakeCompletedProcess(args=cmd, returncode=0)
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(git_ops_module.subprocess, "run", fake_run)
    monkeypatch.setattr(retry_module.time, "sleep", lambda _: None)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)

    published = runner._write_generated_queue_md(headers, statuses)

    assert published is False
    assert push_attempts["count"] == 3
    assert queue_path.read_text(encoding="utf-8") == original
    assert ["git", "reset", "--hard", "HEAD~1"] in git_calls


def test_write_generated_queue_md_marks_resync_when_origin_moved_after_push_rejection(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    original = "# Task Queue\n\n## PR-000: Existing\n"
    queue_path.write_text(original, encoding="utf-8")
    headers = [
        TaskHeader(
            pr_id="PR-001",
            title="Project bootstrap",
            branch="pr-001-bootstrap",
            task_type="feature",
            complexity="low",
            depends_on=[],
            priority=1,
            coder="any",
        )
    ]
    statuses = {"PR-001": TaskStatus.DONE}

    git_calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        git_calls.append(cmd)
        if cmd[:2] == ["git", "push"]:
            queue_path.write_text("generated queue", encoding="utf-8")
            return _FakeCompletedProcess(args=cmd, returncode=1, stderr="rejected")
        if cmd[:2] == ["git", "fetch"]:
            return _FakeCompletedProcess(args=cmd, returncode=0)
        if cmd[:2] == ["git", "rev-parse"] and cmd[-1] == "HEAD~1":
            return _FakeCompletedProcess(args=cmd, returncode=0, stdout="local-base\n")
        if cmd[:2] == ["git", "rev-parse"] and cmd[-1] == "refs/remotes/origin/main":
            return _FakeCompletedProcess(args=cmd, returncode=0, stdout="remote-base\n")
        if cmd[:2] == ["git", "reset"]:
            queue_path.write_text(original, encoding="utf-8")
            return _FakeCompletedProcess(args=cmd, returncode=0)
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(git_ops_module.subprocess, "run", fake_run)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)

    published = runner._write_generated_queue_md(headers, statuses)

    assert published is False
    assert runner._idle_generated_queue_needs_resync is True
    assert queue_path.read_text(encoding="utf-8") == original
    assert ["git", "reset", "--hard", "refs/remotes/origin/main"] in git_calls


def test_write_generated_queue_md_marks_resync_when_probe_cannot_verify_remote_tip(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    original = "# Task Queue\n\n## PR-000: Existing\n"
    queue_path.write_text(original, encoding="utf-8")
    headers = [
        TaskHeader(
            pr_id="PR-001",
            title="Project bootstrap",
            branch="pr-001-bootstrap",
            task_type="feature",
            complexity="low",
            depends_on=[],
            priority=1,
            coder="any",
        )
    ]
    statuses = {"PR-001": TaskStatus.DONE}

    git_calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        git_calls.append(cmd)
        if cmd[:2] == ["git", "push"]:
            queue_path.write_text("generated queue", encoding="utf-8")
            return _FakeCompletedProcess(args=cmd, returncode=1, stderr="rejected")
        if cmd[:2] == ["git", "fetch"]:
            return _FakeCompletedProcess(args=cmd, returncode=1, stderr="temporary failure")
        if cmd[:2] == ["git", "reset"]:
            queue_path.write_text(original, encoding="utf-8")
            return _FakeCompletedProcess(args=cmd, returncode=0)
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(git_ops_module.subprocess, "run", fake_run)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)

    published = runner._write_generated_queue_md(headers, statuses)

    assert published is False
    assert runner._idle_generated_queue_needs_resync is True
    assert queue_path.read_text(encoding="utf-8") == original
    assert ["git", "reset", "--hard", "HEAD~1"] in git_calls


def test_write_generated_queue_md_marks_resync_when_probe_raises_oserror(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    original = "# Task Queue\n\n## PR-000: Existing\n"
    queue_path.write_text(original, encoding="utf-8")
    headers = [
        TaskHeader(
            pr_id="PR-001",
            title="Project bootstrap",
            branch="pr-001-bootstrap",
            task_type="feature",
            complexity="low",
            depends_on=[],
            priority=1,
            coder="any",
        )
    ]
    statuses = {"PR-001": TaskStatus.DONE}

    git_calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        git_calls.append(cmd)
        if cmd[:2] == ["git", "push"]:
            queue_path.write_text("generated queue", encoding="utf-8")
            return _FakeCompletedProcess(args=cmd, returncode=1, stderr="rejected")
        if cmd[:2] == ["git", "fetch"]:
            raise OSError("fetch unavailable")
        if cmd[:2] == ["git", "reset"]:
            queue_path.write_text(original, encoding="utf-8")
            return _FakeCompletedProcess(args=cmd, returncode=0)
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(git_ops_module.subprocess, "run", fake_run)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)

    published = runner._write_generated_queue_md(headers, statuses)

    assert published is False
    assert runner._idle_generated_queue_needs_resync is True
    assert queue_path.read_text(encoding="utf-8") == original
    assert ["git", "reset", "--hard", "HEAD~1"] in git_calls


def test_write_generated_queue_md_reraises_non_transient_retry_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    headers = [
        TaskHeader(
            pr_id="PR-001",
            title="Project bootstrap",
            branch="pr-001-bootstrap",
            task_type="feature",
            complexity="low",
            depends_on=[],
            priority=1,
            coder="any",
        )
    ]
    statuses = {"PR-001": TaskStatus.DONE}

    runner = _make_runner()
    runner.repo_path = str(tmp_path)

    monkeypatch.setattr(
        git_ops_module.subprocess,
        "run",
        lambda cmd, **kwargs: _FakeCompletedProcess(args=cmd, returncode=0),
    )
    monkeypatch.setattr(
        idle_module,
        "retry_transient",
        lambda operation, operation_name=None: _raise_runtime_error(
            "permanent push failure"
        ),
    )

    with pytest.raises(RuntimeError, match="permanent push failure"):
        runner._write_generated_queue_md(headers, statuses)


def test_select_next_task_from_dag_returns_none_when_tasks_dir_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        _ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )
    runner = _make_runner()
    runner.repo_path = str(tmp_path)

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is None


def test_filter_dag_headers_blocks_tasks_with_transitively_blocked_dependencies(
    tmp_path: Path,
) -> None:
    headers = [
        TaskHeader(
            pr_id="PR-001",
            title="Base",
            branch="pr-001-base",
            task_type="feature",
            complexity="low",
            depends_on=["PR-LEGACY"],
            priority=1,
            coder="any",
        ),
        TaskHeader(
            pr_id="PR-002",
            title="Blocked by blocked task",
            branch="pr-002-blocked",
            task_type="feature",
            complexity="low",
            depends_on=["PR-001"],
            priority=2,
            coder="any",
        ),
    ]

    filtered = idle_module.IdleMixin._filter_dag_headers_with_available_dependencies(
        headers,
        {"PR-LEGACY"},
        tmp_path,
        set(),
    )

    assert filtered == []


def test_select_next_task_from_dag_wraps_dag_cycle_errors(
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
        "# PR-001: Cyclic\n\n"
        "Branch: pr-001-cyclic\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = []
    runner._idle_merged_prs = []

    monkeypatch.setattr(idle_module, "get_merged_pr_ids", lambda *args, **kwargs: set())
    monkeypatch.setattr(idle_module, "get_eligible_tasks", _raise_cycle_detected)

    with pytest.raises(QueueValidationError, match="cycle detected"):
        asyncio.run(runner._select_next_task_from_dag())


def test_select_next_task_from_dag_returns_none_when_nothing_is_eligible(
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
        "# PR-001: Waiting\n\n"
        "Branch: pr-001-waiting\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = []
    runner._idle_merged_prs = []

    monkeypatch.setattr(idle_module, "get_merged_pr_ids", lambda *args, **kwargs: set())
    monkeypatch.setattr(
        idle_module,
        "derive_task_status",
        lambda header, merged_pr_ids, open_prs, merged_prs, **kwargs: TaskStatus.DONE,
    )
    monkeypatch.setattr(idle_module, "get_eligible_tasks", lambda headers, statuses: [])

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is None


def test_select_next_task_from_dag_skips_merged_probe_without_structured_headers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_subprocess(monkeypatch)
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# Legacy task without structured metadata\n\n"
        "Some older task body.\n",
        encoding="utf-8",
    )

    def fail_get_merged_pr_ids(*args, **kwargs):
        raise AssertionError("get_merged_pr_ids should not be called")

    monkeypatch.setattr(idle_module, "get_merged_pr_ids", fail_get_merged_pr_ids)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is None
    assert runner._idle_dag_tasks is None


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


def test_process_pending_uploads_routes_root_instruction_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_subprocess(monkeypatch)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)

    staging = tmp_path.parent / "uploads" / runner.name / "rootfiles"
    staging.mkdir(parents=True, exist_ok=True)
    (staging / "QUEUE.md").write_text("# Task Queue\n", encoding="utf-8")
    (staging / "AGENTS.md").write_text("# AGENTS\n", encoding="utf-8")
    (staging / "CLAUDE.md").write_text("Read AGENTS.md\n", encoding="utf-8")
    (tmp_path / "tasks").mkdir(exist_ok=True)

    manifest = json.dumps(
        {
            "files": ["QUEUE.md", "AGENTS.md", "CLAUDE.md"],
            "staging_dir": str(staging),
        }
    )
    key = f"upload:{runner.name}:pending"
    asyncio.run(runner.redis.set(key, manifest))

    result = asyncio.run(runner.process_pending_uploads())

    assert result is True
    assert (tmp_path / "tasks" / "QUEUE.md").read_text(encoding="utf-8") == "# Task Queue\n"
    assert (tmp_path / "AGENTS.md").read_text(encoding="utf-8") == "# AGENTS\n"
    assert (tmp_path / "CLAUDE.md").read_text(encoding="utf-8") == "Read AGENTS.md\n"
    assert not (tmp_path / "tasks" / "AGENTS.md").exists()
    assert not (tmp_path / "tasks" / "CLAUDE.md").exists()


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

    assert asyncio.run(runner.preflight()) is False
    assert runner._consecutive_dirty_cycles == 1
    assert runner.state.state == PipelineState.ERROR

    assert asyncio.run(runner.preflight()) is False
    assert runner._consecutive_dirty_cycles == 2
    assert runner.state.state == PipelineState.ERROR

    assert asyncio.run(runner.preflight()) is True
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

    asyncio.run(runner.preflight())
    asyncio.run(runner.preflight())
    assert asyncio.run(runner.preflight()) is True
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
    assert asyncio.run(runner.preflight()) is False
    assert runner._consecutive_dirty_cycles == 1

    _patch_subprocess(monkeypatch, stdout="")
    assert asyncio.run(runner.preflight()) is True
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

    asyncio.run(runner.preflight())
    asyncio.run(runner.preflight())
    assert asyncio.run(runner.preflight()) is False
    assert runner.state.state == PipelineState.ERROR
    assert any(
        "Auto-recovery failed" in e["event"]
        for e in runner.state.history
    )


def test_preflight_routes_through_bounded_recovery_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Guard: the dirty-tree site must call BoundedRecoveryPolicy
    rather than rebuilding the increment/threshold dance inline."""
    _patch_subprocess(monkeypatch, stdout=" M src/foo.py\n")
    runner = _make_runner()

    increment_calls: list[str] = []
    maybe_escalate_calls: list[str] = []
    orig_increment = recovery_policy_module.BoundedRecoveryPolicy.increment
    orig_maybe_escalate = recovery_policy_module.BoundedRecoveryPolicy.maybe_escalate

    def spy_increment(self: Any, ctx: Any) -> int:
        increment_calls.append(self.name)
        return orig_increment(self, ctx)

    async def spy_maybe_escalate(self: Any, ctx: Any) -> bool:
        maybe_escalate_calls.append(self.name)
        return await orig_maybe_escalate(self, ctx)

    monkeypatch.setattr(
        recovery_policy_module.BoundedRecoveryPolicy, "increment", spy_increment
    )
    monkeypatch.setattr(
        recovery_policy_module.BoundedRecoveryPolicy,
        "maybe_escalate",
        spy_maybe_escalate,
    )

    asyncio.run(runner.preflight())
    assert increment_calls == ["dirty_tree_auto_reset"]
    assert maybe_escalate_calls == ["dirty_tree_auto_reset"]


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


def test_codex_review_reposted_same_head_when_bypass_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A forced stale-review retrigger must bypass same-head cache dedup."""
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
    assert runner._post_codex_review(
        42,
        bypass_same_head_dedup=True,
    ) is True

    assert posted == [
        (runner.owner_repo, 42, "@codex review"),
        (runner.owner_repo, 42, "@codex review"),
    ]
    assert runner._last_codex_review_pr == 42
    assert runner._last_codex_review_head_sha == "head-1"


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


def test_codex_review_result_returns_retry_at_for_author_dedup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requested_at = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
    runner = _make_runner()

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
        hung_module,
        "_author_already_requested_review",
        lambda *args, **kwargs: True,
    )
    monkeypatch.setattr(
        hung_module,
        "_author_recent_review_requested_at",
        lambda *args, **kwargs: requested_at,
    )
    monkeypatch.setattr(
        git_ops_module,
        "_git",
        lambda *args, **kwargs: _FakeCompletedProcess(
            args=list(args), stdout="head-1\n", returncode=0
        ),
    )
    runner.state.current_pr = PRInfo(number=42, branch="pr-42", push_count=1)

    success, posted, retry_at = runner._post_codex_review_result(42)

    assert success is True
    assert posted is False
    assert retry_at == requested_at + timedelta(minutes=5)


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


def test_author_already_requested_review_fails_open_on_lookup_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        hung_module.github_client,
        "has_recent_codex_review_request",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    assert (
        hung_module._author_already_requested_review(
            "octo/demo",
            42,
            "alice",
            "2026-04-17T23:14:11Z",
        )
        is False
    )


def test_author_recent_review_requested_at_fails_open_on_lookup_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        hung_module.github_client,
        "get_recent_codex_review_request_time",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    assert (
        hung_module._author_recent_review_requested_at(
            "octo/demo",
            42,
            "alice",
            "2026-04-17T23:14:11Z",
        )
        is None
    )


def test_codex_review_post_failure_clears_cached_dedup_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(repo: str, number: int, body: str) -> None:
        raise RuntimeError("gh rate limited")

    monkeypatch.setattr(runner_module.github_client, "post_comment", boom)
    monkeypatch.setattr(
        git_ops_module,
        "_git",
        lambda *args, **kwargs: _FakeCompletedProcess(
            args=list(args), stdout="head-1\n", returncode=0
        ),
    )

    runner = _make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-42", push_count=1)

    assert runner._post_codex_review(42) is False
    assert runner._last_codex_review_pr is None
    assert runner._last_codex_review_head_sha is None
    assert any(
        "Warning: failed to post @codex review on PR #42: gh rate limited"
        in entry["event"]
        for entry in runner.state.history
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


def test_save_cli_log_truncates_and_warns_on_redis_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FailingRedis(_FakeRedis):
        async def set(self, key: str, value: str, ex: int | None = None) -> None:
            raise OSError("disk full")

    warnings: list[str] = []
    events: list[str] = []
    runner = _make_runner()
    runner.redis = _FailingRedis()
    runner.log_event = events.append  # type: ignore[method-assign]
    monkeypatch.setattr(
        runner_module.logger,
        "warning",
        lambda msg, *args: warnings.append(msg % args),
    )

    asyncio.run(runner._save_cli_log("x" * (70 * 1024), "err text", "LABEL"))

    assert warnings == [f"Failed to save CLI log for {runner.name}"]
    assert events and events[0].startswith("LABEL: [truncated]")
    assert len(events[0]) <= 207


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
            daemon=DaemonConfig(
                planned_pr_timeout_sec=1234,
                auto_fallback=False,
            ),
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


def test_fix_idle_timeout_defers_to_user_stop_after_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stop consumed alongside idle timeout should pause instead of erroring."""
    _patch_subprocess(monkeypatch)
    saved_logs: list[tuple[str, str, str]] = []

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

    async def stale_stop_monitor(
        _cli_task: asyncio.Task[tuple[int, str, str]],
    ) -> None:
        return None

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
    runner.redis.store[f"control:{runner.name}:stop"] = "1"

    async def save_log(stdout: str, stderr: str, label: str) -> None:
        saved_logs.append((stdout, stderr, label))

    monkeypatch.setattr(runner, "_monitor_stop_request", stale_stop_monitor)
    monkeypatch.setattr(runner, "_save_cli_log", save_log)

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.user_paused is True
    assert runner.state.error_message is None
    assert f"control:{runner.name}:stop" not in runner.redis.store
    assert saved_logs == [("", "", "FIX idle timeout")]
    assert any("user stop requested" in e["event"].lower() for e in runner.state.history)


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


def test_monitor_fix_idle_times_out_without_push_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_monitor_fix_idle should tolerate missing push history and still time out."""
    runner = _make_runner()
    events: list[str] = []
    monkeypatch.setattr(runner, "log_event", events.append)

    branch_results: list[object] = [
        fix_module.github_client.GitHubPollError("bootstrap failed"),
        None,
    ]

    def fake_branch_last_push(repo: str, pr_number: int) -> float | None:
        result = branch_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    async def fake_to_thread(func: Any, *args: object, **kwargs: object) -> Any:
        return func(*args, **kwargs)

    async def fake_sleep(delay: float) -> None:
        return None

    monotonic_values = iter([100.0, 105.0])
    monkeypatch.setattr(
        fix_module.github_client,
        "get_branch_last_push_time",
        fake_branch_last_push,
    )
    monkeypatch.setattr(
        fix_module.github_client,
        "get_last_push_age_seconds",
        lambda repo, pr: None,
    )
    monkeypatch.setattr(fix_module.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(fix_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(
        fix_module,
        "time",
        types.SimpleNamespace(monotonic=lambda: next(monotonic_values)),
    )

    async def run_monitor() -> tuple[dict[str, bool], asyncio.Task[None]]:
        idle_flag = {"timed_out": False}
        blocker: asyncio.Future[None] = asyncio.get_running_loop().create_future()

        async def wait_forever() -> None:
            await blocker

        target = asyncio.create_task(wait_forever())
        await runner._monitor_fix_idle(5, 5, target, idle_flag)
        return idle_flag, target

    idle_flag, target = asyncio.run(run_monitor())
    assert idle_flag["timed_out"] is True
    assert target.cancelled() is True
    assert events == ["FIX: idle timeout (5s since last push), killing"]


def test_monitor_fix_idle_backdates_elapsed_time_from_head_age(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Head age should backdate the idle deadline before the first poll."""
    runner = _make_runner()

    async def fake_to_thread(func: Any, *args: object, **kwargs: object) -> Any:
        return func(*args, **kwargs)

    async def fake_sleep(delay: float) -> None:
        return None

    monotonic_values = iter([100.0, 250.0])
    monkeypatch.setattr(
        fix_module.github_client,
        "get_branch_last_push_time",
        lambda repo, pr: None,
    )
    monkeypatch.setattr(
        fix_module.github_client,
        "get_last_push_age_seconds",
        lambda repo, pr: 30.0,
    )
    monkeypatch.setattr(fix_module.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(fix_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(
        fix_module,
        "time",
        types.SimpleNamespace(monotonic=lambda: next(monotonic_values)),
    )

    async def run_monitor() -> tuple[dict[str, bool], asyncio.Task[None]]:
        idle_flag = {"timed_out": False}
        blocker: asyncio.Future[None] = asyncio.get_running_loop().create_future()

        async def wait_forever() -> None:
            await blocker

        target = asyncio.create_task(wait_forever())
        await runner._monitor_fix_idle(5, 120, target, idle_flag)
        return idle_flag, target

    idle_flag, target = asyncio.run(run_monitor())
    assert idle_flag["timed_out"] is True
    assert target.cancelled() is True


def test_monitor_fix_idle_resets_timer_on_detected_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A newly detected push should reset the timer without cancelling the target."""
    runner = _make_runner()
    runner.state.coder = "codex"
    events: list[str] = []
    monkeypatch.setattr(runner, "log_event", events.append)

    class _StopMonitor(Exception):
        pass

    branch_results: list[object] = [
        fix_module.github_client.GitHubPollError("bootstrap failed"),
        50.0,
        220.0,
    ]

    def fake_branch_last_push(repo: str, pr_number: int) -> float | None:
        result = branch_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    sleep_calls = 0

    async def fake_sleep(delay: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls >= 3:
            raise _StopMonitor

    async def fake_to_thread(func: Any, *args: object, **kwargs: object) -> Any:
        return func(*args, **kwargs)

    monotonic_values = iter([100.0, 150.0, 230.0, 250.0])
    monkeypatch.setattr(
        fix_module.github_client,
        "get_branch_last_push_time",
        fake_branch_last_push,
    )
    monkeypatch.setattr(
        fix_module.github_client,
        "get_last_push_age_seconds",
        lambda repo, pr: None,
    )
    monkeypatch.setattr(fix_module.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(fix_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(
        fix_module,
        "time",
        types.SimpleNamespace(monotonic=lambda: next(monotonic_values)),
    )
    idle_flag = {"timed_out": False}
    target_holder: dict[str, asyncio.Task[None]] = {}

    async def run_monitor() -> None:
        blocker: asyncio.Future[None] = asyncio.get_running_loop().create_future()

        async def wait_forever() -> None:
            await blocker

        target = asyncio.create_task(wait_forever())
        target_holder["task"] = target
        try:
            await runner._monitor_fix_idle(5, 100, target, idle_flag)
        finally:
            target.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await target

    with pytest.raises(_StopMonitor):
        asyncio.run(run_monitor())

    assert idle_flag["timed_out"] is False
    assert target_holder["task"].cancelled() is True
    assert "FIX: [codex] pushed, resetting idle timer" in events


def test_monitor_fix_idle_logs_poll_failures_before_timing_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GitHub poll failures should preserve the deadline and be logged."""
    runner = _make_runner()
    events: list[str] = []
    monkeypatch.setattr(runner, "log_event", events.append)

    def fake_branch_last_push(repo: str, pr_number: int) -> float | None:
        if fake_branch_last_push.calls == 0:
            fake_branch_last_push.calls += 1
            return None
        raise fix_module.github_client.GitHubPollError("poll failed")

    fake_branch_last_push.calls = 0

    async def fake_to_thread(func: Any, *args: object, **kwargs: object) -> Any:
        return func(*args, **kwargs)

    async def fake_sleep(delay: float) -> None:
        return None

    monotonic_values = iter([100.0, 130.0])
    monkeypatch.setattr(
        fix_module.github_client,
        "get_branch_last_push_time",
        fake_branch_last_push,
    )
    monkeypatch.setattr(
        fix_module.github_client,
        "get_last_push_age_seconds",
        lambda repo, pr: None,
    )
    monkeypatch.setattr(fix_module.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(fix_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(
        fix_module,
        "time",
        types.SimpleNamespace(monotonic=lambda: next(monotonic_values)),
    )

    async def run_monitor() -> tuple[dict[str, bool], asyncio.Task[None]]:
        idle_flag = {"timed_out": False}
        blocker: asyncio.Future[None] = asyncio.get_running_loop().create_future()

        async def wait_forever() -> None:
            await blocker

        target = asyncio.create_task(wait_forever())
        await runner._monitor_fix_idle(5, 30, target, idle_flag)
        return idle_flag, target

    idle_flag, target = asyncio.run(run_monitor())
    assert idle_flag["timed_out"] is True
    assert target.cancelled() is True
    assert events == [
        "FIX: GitHub API poll failed, preserving deadline",
        "FIX: idle timeout (30s since last push), killing",
    ]


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


def test_maybe_retrigger_stale_review_returns_without_current_pr() -> None:
    runner = _make_runner()

    runner._maybe_retrigger_stale_review(42)

    assert runner.state.last_stale_retrigger_at is None


def test_maybe_retrigger_stale_review_returns_for_non_changes_requested() -> None:
    runner = _make_runner()
    runner.state.current_pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        review_status=ReviewStatus.APPROVED,
    )

    runner._maybe_retrigger_stale_review(42)

    assert runner.state.last_stale_retrigger_at is None


def test_maybe_retrigger_stale_review_returns_for_missing_push_age(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    runner.state.current_pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        review_status=ReviewStatus.CHANGES_REQUESTED,
    )

    monkeypatch.setattr(
        runner_module.github_client,
        "get_last_push_age_seconds",
        lambda repo, number: None,
    )

    runner._maybe_retrigger_stale_review(42)

    assert runner.state.last_stale_retrigger_at is None


def test_handle_watch_retriggers_stale_changes_requested_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=now,
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
    monkeypatch.setattr(
        runner_module.github_client,
        "get_last_push_age_seconds",
        lambda repo, number: 11 * 60,
    )
    retriggers: list[int] = []
    bypass_flags: list[bool] = []

    runner = _make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = now - timedelta(minutes=11)
    runner._last_push_at_pr_number = pr.number

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        retriggers.append(number)
        bypass_flags.append(bypass_same_head_dedup)
        return True, True, None

    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert retriggers == [42]
    assert bypass_flags == [True]
    assert runner.state.last_stale_retrigger_at is not None
    assert any(
        entry["event"]
        == "Stale CHANGES_REQUESTED on PR #42; re-triggering @codex review."
        for entry in runner.state.history
    )


def test_handle_watch_retries_after_author_dedup_window_expires(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=now,
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
    monkeypatch.setattr(
        runner_module.github_client,
        "get_last_push_age_seconds",
        lambda repo, number: 11 * 60,
    )
    bypass_flags: list[bool] = []

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    runner = _make_runner(stale_review_threshold_min=1)
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = now - timedelta(minutes=11)
    runner._last_push_at_pr_number = pr.number
    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)

    requested_at = now - timedelta(minutes=1)

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        bypass_flags.append(bypass_same_head_dedup)
        return True, False, requested_at + timedelta(minutes=5)

    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert bypass_flags == [True]
    assert runner.state.last_stale_retrigger_at == now


def test_handle_watch_debounces_failed_stale_review_retrigger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=now,
    )

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)
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
    monkeypatch.setattr(
        runner_module.github_client,
        "get_last_push_age_seconds",
        lambda repo, number: 11 * 60,
    )
    bypass_flags: list[bool] = []

    runner = _make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = now - timedelta(minutes=11)
    runner._last_push_at_pr_number = pr.number

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        bypass_flags.append(bypass_same_head_dedup)
        return False, False, None

    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert bypass_flags == [True]
    assert runner.state.last_stale_retrigger_at == now


def test_handle_watch_does_not_retrigger_recent_changes_requested_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=now,
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
    monkeypatch.setattr(
        runner_module.github_client,
        "get_last_push_age_seconds",
        lambda repo, number: 5 * 60,
    )
    retriggers: list[int] = []

    runner = _make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = now - timedelta(minutes=5)
    runner._last_push_at_pr_number = pr.number
    runner._post_codex_review = retriggers.append  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert retriggers == []
    assert runner.state.last_stale_retrigger_at is None


def test_handle_watch_does_not_retrigger_within_debounce_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=now,
    )

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)
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
    monkeypatch.setattr(
        runner_module.github_client,
        "get_last_push_age_seconds",
        lambda repo, number: 60 * 60,
    )
    retriggers: list[int] = []

    runner = _make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.state.last_stale_retrigger_at = now - timedelta(minutes=30)
    runner._last_push_at = now - timedelta(hours=1)
    runner._last_push_at_pr_number = pr.number
    runner._post_codex_review = retriggers.append  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert retriggers == []
    assert runner.state.last_stale_retrigger_at == now - timedelta(minutes=30)


def test_handle_watch_normalizes_naive_stale_retrigger_timestamps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=now,
    )

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)
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
    monkeypatch.setattr(
        runner_module.github_client,
        "get_last_push_age_seconds",
        lambda repo, number: 60 * 60,
    )
    retriggers: list[int] = []

    runner = _make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.state.last_stale_retrigger_at = datetime(2026, 4, 21, 11, 30)
    runner._last_push_at = now - timedelta(hours=1)
    runner._last_push_at_pr_number = pr.number
    runner._post_codex_review = retriggers.append  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert retriggers == []


@pytest.mark.parametrize(
    "review_status",
    [ReviewStatus.APPROVED, ReviewStatus.PENDING],
)
def test_handle_watch_does_not_retrigger_for_non_changes_requested_status(
    monkeypatch: pytest.MonkeyPatch,
    review_status: ReviewStatus,
) -> None:
    now = datetime.now(timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.SUCCESS,
        review_status=review_status,
        last_activity=now,
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [pr],
    )
    retriggers: list[int] = []

    runner = _make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._post_codex_review = retriggers.append  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert retriggers == []


def test_handle_watch_allows_pending_review_only_when_repo_bypasses_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = PRInfo(
        number=42,
        branch="pr-042-test",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [pr],
    )
    merged: list[int] = []

    async def fake_handle_merge() -> None:
        merged.append(42)

    runner = _make_runner(allow_merge_without_review=True)
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_merge = fake_handle_merge  # type: ignore[method-assign]

    asyncio.run(runner.handle_watch())

    assert merged == [42]


def test_handle_watch_does_not_bypass_changes_requested_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = PRInfo(
        number=42,
        branch="pr-042-test",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=datetime.now(timezone.utc),
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
    merged: list[int] = []

    async def fake_handle_merge() -> None:
        merged.append(42)

    runner = _make_runner(allow_merge_without_review=True)
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = datetime.now(timezone.utc) - timedelta(minutes=1)
    runner._last_push_at_pr_number = pr.number
    runner.handle_merge = fake_handle_merge  # type: ignore[method-assign]

    asyncio.run(runner.handle_watch())

    assert merged == []
    assert runner.state.state == PipelineState.WATCH


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
    """_check_rate_limit blocks when the proactive coder is still paused."""
    _patch_subprocess(monkeypatch)
    runner = _make_runner()
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=10)
    runner.state.rate_limit_reactive_coder = "claude"

    assert asyncio.run(runner._check_rate_limit(proactive_coder="claude")) is False
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
    runner._error_skip_context = "stale context"
    runner._error_skip_count = 3
    runner._error_skip_active = True

    asyncio.run(runner.handle_error())

    assert cli_calls == []
    assert runner._error_skip_context is None
    assert runner._error_skip_count == 0
    assert runner._error_skip_active is False


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
    runner._error_skip_context = "stale context"
    runner._error_skip_count = 2
    runner._error_skip_active = True

    asyncio.run(runner.handle_error())

    assert cli_calls == []
    assert runner._error_skip_context is None
    assert runner._error_skip_count == 0
    assert runner._error_skip_active is False


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


def test_handle_error_proceeds_when_usage_snapshot_fetch_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cli_calls: list[str] = []
    _patch_subprocess(monkeypatch)

    def raise_fetch() -> object:
        raise RuntimeError("usage fetch failed")

    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        _async_cli_result_with_side_effect(
            cli_calls, "diagnose", 0, "ESCALATE", ""
        ),
    )
    runner = _make_runner()
    runner._claude_usage_provider.fetch = raise_fetch  # type: ignore[method-assign]
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "Build failed: missing dependency X"

    asyncio.run(runner.handle_error())

    assert cli_calls == ["diagnose"]
    assert runner.state.state == PipelineState.ERROR
    assert not any(
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


def test_handle_error_logs_and_returns_when_diagnose_cli_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        _async_cli_result(1, "", "boom"),
    )
    runner = _make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "Build failed: missing dependency X"

    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "Build failed: missing dependency X"
    assert any(
        e["event"] == "diagnose_error CLI failed: boom"
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
    monkeypatch.setattr(runner, "preflight", _preflight_true_stub)

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


def test_handle_paused_resumes_with_other_coder_while_preserving_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.config import CoderType

    _patch_subprocess(monkeypatch)

    runner = _make_runner(coder=CoderType.CODEX)
    runner.state.state = PipelineState.PAUSED
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=20)
    runner.state.rate_limit_reactive_coder = "claude"
    runner.state.rate_limited_coders.add("claude")

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.rate_limited_until is None
    assert runner.state.rate_limit_reactive_coder is None
    assert "claude" in runner.state.rate_limited_coders


def test_handle_paused_uses_selector_for_pinned_repo_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.config import CoderType

    _patch_subprocess(monkeypatch)

    runner = _make_runner(coder=CoderType.CODEX)
    runner.state.state = PipelineState.PAUSED
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=20)
    runner.state.rate_limit_reactive_coder = "codex"
    runner.state.rate_limited_coders.add("codex")
    runner.state.rate_limited_coder_until["codex"] = runner.state.rate_limited_until

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.rate_limited_until is None
    assert runner.state.rate_limit_reactive_coder is None
    assert "codex" in runner.state.rate_limited_coders


def test_handle_paused_clearable_error_drops_top_level_pause_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)

    runner = _make_runner()
    runner.state.state = PipelineState.PAUSED
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=20)
    runner.state.rate_limit_reactive_coder = "codex"
    runner.state.rate_limited_coders.add("codex")
    runner.state.rate_limited_coder_until["codex"] = runner.state.rate_limited_until
    runner.state.error_message = "Build failed: missing dependency X"

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.rate_limited_until is None
    assert runner.state.rate_limit_reactive_coder is None


def test_handle_paused_invalidates_usage_caches_when_switching_coders(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)

    runner = _make_runner()
    claude_provider = _FakeUsageProvider(snapshot=None)
    codex_provider = _FakeUsageProvider(snapshot=None)
    runner._claude_usage_provider = claude_provider
    runner._codex_usage_provider = codex_provider
    runner.state.state = PipelineState.PAUSED
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=20)
    runner.state.rate_limit_reactive_coder = "claude"
    runner.state.rate_limited_coders.add("claude")
    runner.state.rate_limited_coder_until["claude"] = runner.state.rate_limited_until

    asyncio.run(runner.handle_paused())

    assert claude_provider._invalidated is True
    assert codex_provider._invalidated is True


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
    two_heartbeats = asyncio.Event()

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
            if len(heartbeat_publishes) >= 2:
                two_heartbeats.set()
            await self.publish_state()

    monkeypatch.setattr(
        PipelineRunner, "_publish_while_waiting", fast_heartbeat
    )

    async def run() -> None:
        runner = _make_runner()
        runner.state.current_pr = PRInfo(number=10, branch="pr-001")
        task = asyncio.create_task(runner.handle_fix())
        await asyncio.wait_for(two_heartbeats.wait(), timeout=1)
        cli_done.set_result(None)
        await task

    asyncio.run(run())

    assert len(heartbeat_publishes) >= 2, (
        f"Expected heartbeat to publish at least twice, got {len(heartbeat_publishes)}"
    )


# --- _has_new_codex_feedback_since_last_push tests ---


def test_has_new_feedback_returns_none_without_current_pr() -> None:
    runner = _make_runner()

    assert runner._has_new_codex_feedback_since_last_push() == FeedbackCheckResult.NONE


def test_has_new_feedback_returns_true_without_last_push_timestamp() -> None:
    runner = _make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-fix")

    assert runner._has_new_codex_feedback_since_last_push() == FeedbackCheckResult.NEW


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


def test_has_new_feedback_normalizes_naive_timestamps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-fix")
    runner._last_push_at = datetime(2026, 1, 1, 0, 0, 0)

    monkeypatch.setattr(
        runner_module.github_client,
        "_gh_api_paginated",
        lambda path: [
            {
                "user": {"login": "chatgpt-codex-bot"},
                "created_at": "2026-01-01T00:05:00",
            },
        ],
    )

    assert runner._has_new_codex_feedback_since_last_push() == FeedbackCheckResult.NEW


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


def test_has_new_feedback_skips_unparseable_codex_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-fix")
    runner._last_push_at = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(
        runner_module.github_client,
        "_gh_api_paginated",
        lambda path: [
            {
                "user": {"login": "chatgpt-codex-bot"},
                "created_at": "not-a-date",
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
    assert runner.state.current_pr.commits_count == 0
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


def test_handle_fix_ignores_initial_rev_parse_failure_and_logs_iteration_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pre-fix rev-parse failure must not block a successful FIX run."""
    rev_parse_calls = {"count": 0}

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            if rev_parse_calls["count"] == 1:
                raise subprocess.CalledProcessError(
                    128, ["git", *args], stderr="fatal: bad object HEAD"
                )
            return _FakeCompletedProcess(
                args=["git", *args], stdout="bbb222\n", returncode=0
            )
        return _FakeCompletedProcess(args=["git", *args], returncode=0)

    async def no_idle_monitor(
        self: object,
        pr_number: int,
        idle_limit: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        idle_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    async def no_breach_monitor(
        self: object,
        breach_dir: str,
        run_id: str,
        claude_task: asyncio.Task,  # type: ignore[type-arg]
        breach_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(
        claude_cli, "fix_review_async", _async_cli_result(0, "ok", "")
    )
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", no_idle_monitor)
    monkeypatch.setattr(
        PipelineRunner, "_monitor_inflight_breach", no_breach_monitor
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is None
    assert any("Fix pushed, iteration #0" in e["event"] for e in runner.state.history)


def test_handle_fix_sets_error_when_breach_cancel_review_post_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A breach-cancelled fix with a real HEAD change must surface review-post failure."""
    rev_parse_calls = {"count": 0}
    rehydrated: list[int] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            sha = "aaa111\n" if rev_parse_calls["count"] == 1 else "bbb222\n"
            return _FakeCompletedProcess(args=["git", *args], stdout=sha, returncode=0)
        return _FakeCompletedProcess(args=["git", *args], returncode=0)

    async def fake_fix(*args: object, **kwargs: object) -> tuple[int, str, str]:
        await asyncio.Future()
        return (0, "", "")

    async def no_idle_monitor(
        self: object,
        pr_number: int,
        idle_limit: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        idle_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    async def breach_cancel_monitor(
        self: PipelineRunner,
        breach_dir: str,
        run_id: str,
        claude_task: asyncio.Task,  # type: ignore[type-arg]
        breach_flag: dict[str, bool],
    ) -> None:
        self.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=5)
        breach_flag["breached"] = True
        await asyncio.sleep(0)
        claude_task.cancel()

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", no_idle_monitor)
    monkeypatch.setattr(
        PipelineRunner, "_monitor_inflight_breach", breach_cancel_monitor
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    monkeypatch.setattr(
        runner, "_rehydrate_last_push_at", lambda pr: rehydrated.append(pr.number)
    )
    monkeypatch.setattr(runner, "_post_codex_review", lambda pr_number: False)

    asyncio.run(runner.handle_fix())

    assert rehydrated == [42]
    assert runner.state.state == PipelineState.ERROR
    assert "breach-cancel fix push" in (runner.state.error_message or "")


def test_handle_fix_pauses_when_breach_cancel_rev_parse_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A breach-cancelled fix still pauses cleanly if HEAD cannot be re-read."""
    rev_parse_calls = {"count": 0}
    rehydrated: list[int] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            if rev_parse_calls["count"] == 1:
                return _FakeCompletedProcess(
                    args=["git", *args], stdout="aaa111\n", returncode=0
                )
            raise RuntimeError("rev-parse lost HEAD")
        return _FakeCompletedProcess(args=["git", *args], returncode=0)

    async def fake_fix(*args: object, **kwargs: object) -> tuple[int, str, str]:
        await asyncio.Future()
        return (0, "", "")

    async def no_idle_monitor(
        self: object,
        pr_number: int,
        idle_limit: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        idle_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    async def breach_cancel_monitor(
        self: PipelineRunner,
        breach_dir: str,
        run_id: str,
        claude_task: asyncio.Task,  # type: ignore[type-arg]
        breach_flag: dict[str, bool],
    ) -> None:
        self.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=5)
        breach_flag["breached"] = True
        await asyncio.sleep(0)
        claude_task.cancel()

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", no_idle_monitor)
    monkeypatch.setattr(
        PipelineRunner, "_monitor_inflight_breach", breach_cancel_monitor
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    monkeypatch.setattr(
        runner, "_rehydrate_last_push_at", lambda pr: rehydrated.append(pr.number)
    )
    monkeypatch.setattr(
        runner,
        "_post_codex_review",
        lambda pr_number: pytest.fail("review post should not run when HEAD read fails"),
    )

    asyncio.run(runner.handle_fix())

    assert rehydrated == [42]
    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.error_message is None
    assert any("FIX aborted: in-flight rate limit breach" in e["event"] for e in runner.state.history)


def test_handle_fix_reraises_unexpected_cancelled_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only breach and idle cancellations are swallowed by handle_fix."""

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(args=["git", *args], stdout="aaa111\n", returncode=0)

    async def fake_fix(*args: object, **kwargs: object) -> tuple[int, str, str]:
        raise asyncio.CancelledError

    async def no_idle_monitor(
        self: object,
        pr_number: int,
        idle_limit: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        idle_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    async def no_breach_monitor(
        self: object,
        breach_dir: str,
        run_id: str,
        claude_task: asyncio.Task,  # type: ignore[type-arg]
        breach_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", no_idle_monitor)
    monkeypatch.setattr(
        PipelineRunner, "_monitor_inflight_breach", no_breach_monitor
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(runner.handle_fix())


def test_handle_fix_sets_error_when_late_breach_review_post_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A late breach with a changed HEAD must surface review-post failure."""
    rev_parse_calls = {"count": 0}
    rehydrated: list[int] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            sha = "aaa111\n" if rev_parse_calls["count"] == 1 else "bbb222\n"
            return _FakeCompletedProcess(args=["git", *args], stdout=sha, returncode=0)
        return _FakeCompletedProcess(args=["git", *args], returncode=0)

    async def no_idle_monitor(
        self: object,
        pr_number: int,
        idle_limit: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        idle_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    async def no_breach_monitor(
        self: object,
        breach_dir: str,
        run_id: str,
        claude_task: asyncio.Task,  # type: ignore[type-arg]
        breach_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    def fake_late_breach(
        self: PipelineRunner,
        breach_dir: str,
        run_id: str,
        breach_flag: dict[str, bool],
    ) -> None:
        self.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=5)
        breach_flag["breached"] = True

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(
        claude_cli, "fix_review_async", _async_cli_result(0, "ok", "")
    )
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", no_idle_monitor)
    monkeypatch.setattr(
        PipelineRunner, "_monitor_inflight_breach", no_breach_monitor
    )
    monkeypatch.setattr(PipelineRunner, "_check_late_breach", fake_late_breach)

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    monkeypatch.setattr(
        runner, "_rehydrate_last_push_at", lambda pr: rehydrated.append(pr.number)
    )
    monkeypatch.setattr(runner, "_post_codex_review", lambda pr_number: False)

    asyncio.run(runner.handle_fix())

    assert rehydrated == [42]
    assert runner.state.state == PipelineState.ERROR
    assert "late-breach fix push" in (runner.state.error_message or "")


def test_handle_fix_pauses_when_late_breach_rev_parse_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A late breach still pauses cleanly if HEAD cannot be re-read."""
    rev_parse_calls = {"count": 0}
    rehydrated: list[int] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            if rev_parse_calls["count"] == 1:
                return _FakeCompletedProcess(
                    args=["git", *args], stdout="aaa111\n", returncode=0
                )
            raise RuntimeError("rev-parse lost HEAD")
        return _FakeCompletedProcess(args=["git", *args], returncode=0)

    async def no_idle_monitor(
        self: object,
        pr_number: int,
        idle_limit: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        idle_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    async def no_breach_monitor(
        self: object,
        breach_dir: str,
        run_id: str,
        claude_task: asyncio.Task,  # type: ignore[type-arg]
        breach_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    def fake_late_breach(
        self: PipelineRunner,
        breach_dir: str,
        run_id: str,
        breach_flag: dict[str, bool],
    ) -> None:
        self.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=5)
        breach_flag["breached"] = True

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(
        claude_cli, "fix_review_async", _async_cli_result(0, "ok", "")
    )
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", no_idle_monitor)
    monkeypatch.setattr(
        PipelineRunner, "_monitor_inflight_breach", no_breach_monitor
    )
    monkeypatch.setattr(PipelineRunner, "_check_late_breach", fake_late_breach)

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    monkeypatch.setattr(
        runner, "_rehydrate_last_push_at", lambda pr: rehydrated.append(pr.number)
    )
    monkeypatch.setattr(
        runner,
        "_post_codex_review",
        lambda pr_number: pytest.fail("review post should not run when HEAD read fails"),
    )

    asyncio.run(runner.handle_fix())

    assert rehydrated == [42]
    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.error_message is None
    assert any("FIX paused: late in-flight rate limit breach" in e["event"] for e in runner.state.history)


def test_handle_fix_sets_error_on_non_rate_limit_cli_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-rate-limit CLI failures must surface as ERROR with stderr text."""

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(args=["git", *args], stdout="aaa111\n", returncode=0)

    async def no_idle_monitor(
        self: object,
        pr_number: int,
        idle_limit: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        idle_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    async def no_breach_monitor(
        self: object,
        breach_dir: str,
        run_id: str,
        claude_task: asyncio.Task,  # type: ignore[type-arg]
        breach_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(
        claude_cli, "fix_review_async", _async_cli_result(7, "", "plain failure")
    )
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", no_idle_monitor)
    monkeypatch.setattr(
        PipelineRunner, "_monitor_inflight_breach", no_breach_monitor
    )

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "plain failure"
    assert any("[claude] fix_review failed: plain failure" in e["event"] for e in runner.state.history)


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

    async def fake_check_rate_limit(
        proactive_coder: str | None = None,
    ) -> bool:
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


def test_handle_coding_reuses_selected_coder_for_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    runner.state.state = PipelineState.CODING
    runner.state.current_task = QueueTask(
        pr_id="PR-099", title="test", branch="pr-099-test", status=TaskStatus.TODO
    )
    runner._get_coder = (  # type: ignore[method-assign]
        lambda **kwargs: ("codex", runner._registry.get("codex"))
    )
    seen: list[str | None] = []

    async def fake_check_rate_limit(
        proactive_coder: str | None = None,
    ) -> bool:
        seen.append(proactive_coder)
        runner.state.state = PipelineState.PAUSED
        runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=10)
        return False

    monkeypatch.setattr(runner, "_check_rate_limit", fake_check_rate_limit)

    asyncio.run(runner.handle_coding())

    assert seen == ["codex"]


def test_handle_fix_reuses_selected_coder_for_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    runner.state.state = PipelineState.FIX
    runner.state.current_pr = PRInfo(number=50, branch="pr-050")
    runner._get_coder = (  # type: ignore[method-assign]
        lambda **kwargs: ("codex", runner._registry.get("codex"))
    )
    seen: list[str | None] = []

    async def fake_check_rate_limit(
        proactive_coder: str | None = None,
    ) -> bool:
        seen.append(proactive_coder)
        runner.state.state = PipelineState.PAUSED
        runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=10)
        return False

    monkeypatch.setattr(runner, "_check_rate_limit", fake_check_rate_limit)

    asyncio.run(runner.handle_fix())

    assert seen == ["codex"]


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
    """Legacy unattributed pause yields to an available fallback coder."""
    _patch_subprocess(monkeypatch)

    runner = _make_runner()
    runner.state.state = PipelineState.PAUSED
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=20)

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.rate_limited_until is None
    assert "claude" in runner.state.rate_limited_coders


def test_handle_paused_clearable_rate_limit_error_resumes_to_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)

    runner = _make_runner()
    runner.state.state = PipelineState.PAUSED
    pause_until = datetime.now(timezone.utc) + timedelta(minutes=20)
    runner.state.rate_limited_until = pause_until
    runner.state.rate_limit_reactive_coder = "codex"
    runner.state.rate_limited_coders.add("codex")
    runner.state.error_message = "Codex hit rate limit 429"
    runner.state.current_pr = PRInfo(number=50, branch="pr-050")
    runner.state.current_task = QueueTask(
        pr_id="PR-050",
        title="test",
        branch="pr-050",
        status=TaskStatus.DOING,
    )
    runner._select_coder = (  # type: ignore[method-assign]
        lambda allow_exploration=True: ("claude", runner._registry.get("claude"))
    )

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.error_message is None
    assert any("cleared legacy rate-limit error" in e["event"] for e in runner.state.history)
    assert any("-> WATCH" in e["event"] for e in runner.state.history)


def test_handle_paused_clears_other_coder_from_rate_limited_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.config import CoderType

    _patch_subprocess(monkeypatch)

    runner = _make_runner(coder=CoderType.CODEX)
    runner.state.state = PipelineState.PAUSED
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=20)
    runner.state.rate_limit_reactive_coder = "claude"
    runner.state.rate_limited_coders.add("claude")

    asyncio.run(runner.handle_paused())

    assert "claude" in runner.state.rate_limited_coders
    assert runner.state.rate_limited_until is None
    assert runner.state.state == PipelineState.IDLE


def test_handle_paused_preserves_legacy_pause_for_other_coder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.config import CoderType

    _patch_subprocess(monkeypatch)

    runner = _make_runner(coder=CoderType.CODEX)
    pause_until = datetime.now(timezone.utc) + timedelta(minutes=20)
    runner.state.state = PipelineState.PAUSED
    runner.state.rate_limited_until = pause_until
    runner.state.rate_limit_reactive_coder = "claude"

    asyncio.run(runner.handle_paused())

    assert runner.state.rate_limited_until is None
    assert runner.state.rate_limit_reactive_coder is None
    assert "claude" in runner.state.rate_limited_coders
    assert runner.state.rate_limited_coder_until["claude"] == pause_until
    assert runner.state.state == PipelineState.IDLE


def test_handle_paused_stays_paused_when_no_alternate_coder_is_runnable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)

    runner = _make_runner()
    runner.state.state = PipelineState.PAUSED
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=20)
    runner.state.rate_limit_reactive_coder = "claude"
    runner.state.rate_limited_coders.update({"claude", "codex"})
    runner.state.rate_limited_coder_until = {
        "claude": datetime.now(timezone.utc) + timedelta(minutes=20),
        "codex": datetime.now(timezone.utc) + timedelta(minutes=10),
    }

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
    """run_cycle preserves PAUSED handling instead of transient-reset logic."""
    _patch_subprocess(monkeypatch)

    runner = _make_runner()
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.PAUSED
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=20)

    asyncio.run(runner.run_cycle())

    assert runner.state.state == PipelineState.IDLE
    assert "claude" in runner.state.rate_limited_coders


def test_check_rate_limit_transitions_to_paused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Current-coder pauses still transition CODING -> PAUSED on check."""
    _patch_subprocess(monkeypatch)

    runner = _make_runner()
    runner.state.state = PipelineState.CODING
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=10)
    runner.state.rate_limit_reactive_coder = "claude"

    result = asyncio.run(runner._check_rate_limit(proactive_coder="claude"))

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


def test_publish_while_waiting_handles_publish_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    warnings: list[str] = []
    sleep_calls = {"count": 0}
    runner = _make_runner()

    async def fake_sleep(seconds: float) -> None:
        sleep_calls["count"] += 1
        if sleep_calls["count"] > 1:
            raise asyncio.CancelledError()

    async def fake_publish_state() -> None:
        raise RuntimeError("redis offline")

    monkeypatch.setattr(runner_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)
    monkeypatch.setattr(
        runner_module.logger,
        "warning",
        lambda msg, *args: warnings.append(msg % args),
    )

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(runner._publish_while_waiting("heartbeat"))

    assert warnings == [f"[{runner.name}] heartbeat publish failed, will retry"]


def test_run_cycle_handles_ensure_repo_cloned_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publishes: list[str] = []
    runner = _make_runner()

    async def fake_ensure_repo_cloned() -> None:
        raise RuntimeError("clone failed")

    async def fake_publish_state() -> None:
        publishes.append("published")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "clone failed"
    assert publishes == ["published"]
    assert any(
        "ensure_repo_cloned failed: clone failed" in entry["event"]
        for entry in runner.state.history
    )


@pytest.mark.parametrize(
    ("head_ref", "expect_checkout"),
    [
        ("main", False),
        ("feature/work", True),
    ],
)
def test_run_cycle_processes_pending_uploads_from_recovery_when_on_or_off_base(
    monkeypatch: pytest.MonkeyPatch,
    head_ref: str,
    expect_checkout: bool,
) -> None:
    publishes: list[str] = []
    upload_calls: list[bool] = []
    git_calls: list[tuple[str, ...]] = []
    runner = _make_runner()

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_recover_state() -> bool:
        return False

    async def fake_publish_state() -> None:
        publishes.append("published")

    async def fake_process_pending_uploads(*, _safe: bool) -> None:
        upload_calls.append(_safe)

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        git_calls.append(args)
        if args == ("rev-parse", "--abbrev-ref", "HEAD"):
            return _FakeCompletedProcess(stdout=f"{head_ref}\n")
        if args == ("checkout", runner.repo_config.branch):
            return _FakeCompletedProcess(stdout="")
        raise AssertionError(f"unexpected git call: {args}")

    runner.redis.store[f"upload:{runner.name}:pending"] = "pending"
    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "recover_state", fake_recover_state)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)
    monkeypatch.setattr(runner, "process_pending_uploads", fake_process_pending_uploads)
    monkeypatch.setattr(git_ops_module, "_git", fake_git)

    asyncio.run(runner.run_cycle())

    assert publishes == ["published"]
    assert upload_calls == [True]
    if expect_checkout:
        assert ("checkout", runner.repo_config.branch) in git_calls
    else:
        assert ("checkout", runner.repo_config.branch) not in git_calls


def test_run_cycle_skips_pending_uploads_when_git_probe_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publishes: list[str] = []
    upload_calls: list[bool] = []
    runner = _make_runner()

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_recover_state() -> bool:
        return False

    async def fake_publish_state() -> None:
        publishes.append("published")

    async def fake_process_pending_uploads(*, _safe: bool) -> None:
        upload_calls.append(_safe)

    runner.redis.store[f"upload:{runner.name}:pending"] = "pending"
    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "recover_state", fake_recover_state)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)
    monkeypatch.setattr(runner, "process_pending_uploads", fake_process_pending_uploads)
    monkeypatch.setattr(
        git_ops_module,
        "_git",
        lambda repo_path, *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("rev-parse failed")
        ),
    )

    asyncio.run(runner.run_cycle())

    assert publishes == ["published"]
    assert upload_calls == []


def test_run_cycle_ignores_pending_upload_probe_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publishes: list[str] = []
    runner = _make_runner()

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_recover_state() -> bool:
        return False

    async def fake_publish_state() -> None:
        publishes.append("published")

    async def fake_get(key: str) -> str | None:
        raise RuntimeError("redis unavailable")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "recover_state", fake_recover_state)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)
    monkeypatch.setattr(runner.redis, "get", fake_get)

    asyncio.run(runner.run_cycle())

    assert publishes == ["published"]


def test_run_cycle_marks_recovery_complete_and_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publishes: list[str] = []
    runner = _make_runner()

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_recover_state() -> bool:
        return True

    async def fake_publish_state() -> None:
        publishes.append("published")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "recover_state", fake_recover_state)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert runner._recovered is True
    assert publishes == ["published"]


def test_run_cycle_runs_recovery_before_honoring_user_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publishes: list[str] = []
    recovery_calls: list[str] = []
    preflight_calls: list[str] = []
    runner = _make_runner()
    runner.state.state = PipelineState.IDLE

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_refresh_user_paused_from_redis() -> None:
        runner.state.user_paused = True

    async def fake_recover_state() -> bool:
        recovery_calls.append("recover")
        return True

    async def fake_publish_state() -> None:
        publishes.append("published")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(
        runner,
        "_refresh_user_paused_from_redis",
        fake_refresh_user_paused_from_redis,
    )
    monkeypatch.setattr(runner, "recover_state", fake_recover_state)
    monkeypatch.setattr(
        runner,
        "preflight",
        _preflight_recording_stub(preflight_calls),
    )
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert recovery_calls == ["recover"]
    assert preflight_calls == []
    assert publishes == ["published"]
    assert runner._recovered is True
    assert not any(
        entry["event"] == "Paused. Press Play to resume."
        for entry in runner.state.history
    )


def test_run_cycle_returns_after_preflight_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publishes: list[str] = []
    runner = _make_runner()
    runner._recovered = True
    runner._scaffolded = True

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_publish_state() -> None:
        publishes.append("published")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "preflight", _preflight_false_stub)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert publishes == ["published"]


def test_run_cycle_short_circuits_idle_when_user_paused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publishes: list[str] = []
    idle_calls: list[str] = []
    preflight_calls: list[str] = []
    runner = _make_runner()
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.IDLE
    runner.state.user_paused = True

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_handle_idle() -> None:
        idle_calls.append("idle")

    async def fake_publish_state() -> None:
        publishes.append("published")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(
        runner,
        "preflight",
        _preflight_recording_stub(preflight_calls),
    )
    monkeypatch.setattr(runner, "handle_idle", fake_handle_idle)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())
    asyncio.run(runner.run_cycle())

    assert idle_calls == []
    assert preflight_calls == []
    assert publishes == ["published", "published"]
    assert sum(
        1
        for entry in runner.state.history
        if entry["event"] == "Paused. Press Play to resume."
    ) == 1


def test_run_cycle_short_circuits_paused_when_user_paused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publishes: list[str] = []
    paused_calls: list[str] = []
    preflight_calls: list[str] = []
    runner = _make_runner()
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.PAUSED
    runner.state.user_paused = True

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_handle_paused() -> None:
        paused_calls.append("paused")

    async def fake_publish_state() -> None:
        publishes.append("published")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(
        runner,
        "preflight",
        _preflight_recording_stub(preflight_calls),
    )
    monkeypatch.setattr(runner, "handle_paused", fake_handle_paused)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())
    asyncio.run(runner.run_cycle())

    assert paused_calls == []
    assert preflight_calls == []
    assert publishes == ["published", "published"]
    assert sum(
        1
        for entry in runner.state.history
        if entry["event"] == "Paused. Press Play to resume."
    ) == 1


@pytest.mark.parametrize(
    ("state", "handler_name"),
    [
        (PipelineState.WATCH, "handle_watch"),
        (PipelineState.MERGE, "handle_merge"),
    ],
)
def test_run_cycle_short_circuits_active_watch_and_merge_when_user_paused(
    monkeypatch: pytest.MonkeyPatch,
    state: PipelineState,
    handler_name: str,
) -> None:
    publishes: list[str] = []
    handler_calls: list[str] = []
    preflight_calls: list[str] = []
    runner = _make_runner()
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = state
    runner.state.user_paused = True

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_handler() -> None:
        handler_calls.append(handler_name)

    async def fake_publish_state() -> None:
        publishes.append("published")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(
        runner,
        "preflight",
        _preflight_recording_stub(preflight_calls),
    )
    monkeypatch.setattr(runner, handler_name, fake_handler)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())
    asyncio.run(runner.run_cycle())

    assert handler_calls == []
    assert preflight_calls == []
    assert publishes == ["published", "published"]
    assert sum(
        1
        for entry in runner.state.history
        if entry["event"] == "Paused. Press Play to resume."
    ) == 1


def test_run_cycle_skips_preflight_after_pause_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publishes: list[str] = []
    preflight_calls: list[str] = []
    runner = _make_runner()
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.IDLE
    runner.state.user_paused = False

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_refresh_user_paused_from_redis() -> None:
        runner.state.user_paused = True

    async def fake_publish_state() -> None:
        publishes.append("published")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(
        runner,
        "_refresh_user_paused_from_redis",
        fake_refresh_user_paused_from_redis,
    )
    monkeypatch.setattr(
        runner,
        "preflight",
        _preflight_recording_stub(preflight_calls),
    )
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert preflight_calls == []
    assert publishes == ["published"]


def test_run_cycle_rereads_pause_flag_before_idle_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publishes: list[str] = []
    idle_calls: list[str] = []
    preflight_calls: list[str] = []
    refresh_states = iter([False, True])
    runner = _make_runner()
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.IDLE
    runner.state.user_paused = False

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_refresh_user_paused_from_redis() -> None:
        runner.state.user_paused = next(refresh_states)

    async def fake_handle_idle() -> None:
        idle_calls.append("idle")

    async def fake_publish_state() -> None:
        publishes.append("published")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(
        runner,
        "_refresh_user_paused_from_redis",
        fake_refresh_user_paused_from_redis,
    )
    monkeypatch.setattr(
        runner,
        "preflight",
        _preflight_recording_stub(preflight_calls),
    )
    monkeypatch.setattr(runner, "handle_idle", fake_handle_idle)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert preflight_calls == ["preflight"]
    assert idle_calls == []
    assert publishes == ["published"]
    assert sum(
        1
        for entry in runner.state.history
        if entry["event"] == "Paused. Press Play to resume."
    ) == 1


@pytest.mark.parametrize(
    ("state", "handler_name"),
    [
        (PipelineState.WATCH, "handle_watch"),
        (PipelineState.HUNG, "handle_hung"),
    ],
)
def test_run_cycle_dispatches_watch_and_hung_handlers(
    monkeypatch: pytest.MonkeyPatch,
    state: PipelineState,
    handler_name: str,
) -> None:
    calls: list[str] = []
    publishes: list[str] = []
    runner = _make_runner()
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = state

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_handler() -> None:
        calls.append(handler_name)

    async def fake_publish_state() -> None:
        publishes.append("published")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "preflight", _preflight_true_stub)
    monkeypatch.setattr(runner, handler_name, fake_handler)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert calls == [handler_name]
    assert publishes == ["published"]


def test_run_cycle_dispatches_error_handler_when_ai_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    publishes: list[str] = []
    runner = _make_runner()
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.ERROR
    runner.state.rate_limited_until = None

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_handle_error() -> None:
        calls.append("handle_error")

    async def fake_publish_state() -> None:
        publishes.append("published")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "preflight", _preflight_true_stub)
    monkeypatch.setattr(runner, "handle_error", fake_handle_error)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert calls == ["handle_error"]
    assert publishes == ["published"]


def test_run_cycle_does_not_reload_dirty_config_before_error_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    runner = _make_runner()
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.ERROR
    runner.state.rate_limited_until = None

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_reload_repo_config_if_dirty() -> None:
        calls.append("reload")

    async def fake_handle_error() -> None:
        calls.append("handle_error")

    async def fake_publish_state() -> None:
        calls.append("publish")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "preflight", _preflight_true_stub)
    monkeypatch.setattr(
        runner, "reload_repo_config_if_dirty", fake_reload_repo_config_if_dirty
    )
    monkeypatch.setattr(runner, "handle_error", fake_handle_error)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert calls == ["handle_error", "publish"]


def test_run_cycle_stops_idle_dispatch_when_dirty_reload_disables_repo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    runner = _make_runner()
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.IDLE

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_refresh_user_paused_from_redis() -> None:
        calls.append("refresh")

    async def fake_reload_repo_config_if_dirty() -> None:
        calls.append("reload")
        runner.repo_config.active = False

    async def fake_handle_idle() -> None:
        calls.append("handle_idle")

    async def fake_publish_state() -> None:
        calls.append("publish")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "preflight", _preflight_true_stub)
    monkeypatch.setattr(
        runner,
        "_refresh_user_paused_from_redis",
        fake_refresh_user_paused_from_redis,
    )
    monkeypatch.setattr(
        runner, "reload_repo_config_if_dirty", fake_reload_repo_config_if_dirty
    )
    monkeypatch.setattr(runner, "handle_idle", fake_handle_idle)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert "reload" in calls
    assert "handle_idle" not in calls
    assert calls[-1] == "publish"


def test_run_cycle_error_state_ignores_dirty_reload_until_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    runner = _make_runner()
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.ERROR
    runner.state.rate_limited_until = None

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_reload_repo_config_if_dirty() -> None:
        calls.append("reload")
        runner.repo_config.active = False

    async def fake_handle_error() -> None:
        calls.append("handle_error")

    async def fake_publish_state() -> None:
        calls.append("publish")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "preflight", _preflight_true_stub)
    monkeypatch.setattr(
        runner, "reload_repo_config_if_dirty", fake_reload_repo_config_if_dirty
    )
    monkeypatch.setattr(runner, "handle_error", fake_handle_error)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert calls == ["handle_error", "publish"]


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
    ["file not found", "Unknown error"],
)
def test_classify_error_other(msg: str) -> None:
    assert _classify_error(msg) == ErrorCategory.OTHER


@pytest.mark.parametrize(
    "msg",
    ["OOM killer invoked", "process killed: out of memory", "worker oom"],
)
def test_classify_oom(msg: str) -> None:
    assert _classify_error(msg) == ErrorCategory.OOM


def test_classify_oom_requires_token_boundary() -> None:
    assert _classify_error("No room left on device") == ErrorCategory.OTHER


@pytest.mark.parametrize(
    "msg",
    ["auth failed", "401 Unauthorized", "unauthorized request"],
)
def test_classify_auth_failure(msg: str) -> None:
    assert _classify_error(msg) == ErrorCategory.AUTH_FAILURE


@pytest.mark.parametrize(
    "msg",
    ["CI failed on main", "ci job fail", "CI checks failing"],
)
def test_classify_ci_failure(msg: str) -> None:
    assert _classify_error(msg) == ErrorCategory.CI_FAILURE


@pytest.mark.parametrize(
    "msg",
    [
        "Push rejected: non-fast-forward update required",
        "Branch drift detected; needs rebase before retry",
        "stale branch state blocks merge",
    ],
)
def test_classify_stale_branch(msg: str) -> None:
    assert _classify_error(msg) == ErrorCategory.STALE_BRANCH


def test_classify_ci_failure_requires_ci_word_boundary() -> None:
    assert _classify_error("decision failed during merge") == ErrorCategory.OTHER


@pytest.mark.parametrize(
    "msg",
    ["ghost push detected", "HEAD SHA changed unexpectedly"],
)
def test_classify_ghost_push(msg: str) -> None:
    assert _classify_error(msg) == ErrorCategory.GHOST_PUSH


@pytest.mark.parametrize(
    "msg",
    ["codex cli not found", "CLI executable not found"],
)
def test_classify_cli_not_found(msg: str) -> None:
    assert _classify_error(msg) == ErrorCategory.CLI_NOT_FOUND


@pytest.mark.parametrize(
    "msg",
    ["git push failed", "git error: detached head"],
)
def test_classify_git_error(msg: str) -> None:
    assert _classify_error(msg) == ErrorCategory.GIT_ERROR


def test_classify_git_error_requires_git_token() -> None:
    assert _classify_error("GitHub API request failed") == ErrorCategory.OTHER


def test_classify_git_error_for_fatal_stderr() -> None:
    assert (
        _classify_error("fatal: could not resolve host: github.com")
        == ErrorCategory.GIT_ERROR
    )


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


def test_handle_coding_reraises_cancelled_error_without_breach(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancelled CLI runs re-raise when no breach marker was detected."""
    from src import codex_cli
    from src.config import CoderType

    _patch_subprocess(monkeypatch)

    async def fake_run_planned_pr(path: str, **kwargs: object) -> tuple[int, str, str]:
        raise asyncio.CancelledError

    monkeypatch.setattr(codex_cli, "run_planned_pr_async", fake_run_planned_pr)

    runner = _make_runner(coder=CoderType.CODEX)
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001",
    )

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(runner.handle_coding())


def test_handle_coding_records_pr_before_breach_cancel_pause(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """In-flight breach cancellation still records the just-opened PR."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(breach_module, "_BREACH_DIR", str(tmp_path))
    monkeypatch.setattr(breach_module, "_BREACH_POLL_SEC", 0.05)

    async def fake_planned_hangs(
        path: str, model: str | None = None, timeout: int | None = None, **kwargs: object
    ) -> tuple[int, str, str]:
        run_id = kwargs.get("breach_run_id", "")
        if run_id:
            marker = tmp_path / f"{run_id}.breach"
            marker.write_text(json.dumps({
                "type": "session",
                "resets_at": 1700000000,
                "session_pct": 98,
                "weekly_pct": 30,
                "detected_at": 1234567890.0,
            }))
        await asyncio.sleep(999)
        return (0, "", "")

    pr = PRInfo(
        number=42,
        url="https://github.com/octo/demo/pull/42",
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )
    gh_calls = {"count": 0}
    original_sleep = asyncio.sleep

    def fake_get_open_prs(*args: object, **kwargs: object) -> list[PRInfo]:
        gh_calls["count"] += 1
        if gh_calls["count"] == 1:
            raise RuntimeError("temporary gh failure")
        return [pr]

    async def fast_sleep(seconds: float) -> None:
        await original_sleep(0)

    monkeypatch.setattr(claude_cli, "run_planned_pr_async", fake_planned_hangs)
    monkeypatch.setattr(runner_module.github_client, "get_open_prs", fake_get_open_prs)
    monkeypatch.setattr(coding_module.asyncio, "sleep", fast_sleep)

    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001",
    )
    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.current_pr == pr
    assert any(
        "Recorded PR #42 before breach-cancel pause" in entry["event"]
        for entry in runner.state.history
    )


def test_handle_coding_records_pr_before_late_breach_pause(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Late breach pauses still attach the matching PR before returning."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(breach_module, "_BREACH_DIR", str(tmp_path))

    async def fake_planned(
        path: str, model: str | None = None, timeout: int | None = None, **kwargs: object
    ) -> tuple[int, str, str]:
        return (0, "ok", "")

    pr = PRInfo(
        number=42,
        url="https://github.com/octo/demo/pull/42",
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )
    gh_calls = {"count": 0}
    original_sleep = asyncio.sleep

    def fake_get_open_prs(*args: object, **kwargs: object) -> list[PRInfo]:
        gh_calls["count"] += 1
        if gh_calls["count"] == 1:
            raise RuntimeError("temporary gh failure")
        return [pr]

    def fake_check_late_breach(
        breach_dir: str, breach_run_id: str, breach_flag: dict[str, bool],
    ) -> None:
        breach_flag["breached"] = True
        runner.state.rate_limited_until = datetime.fromtimestamp(
            1700000000, tz=timezone.utc,
        )

    async def fast_sleep(seconds: float) -> None:
        await original_sleep(0)

    monkeypatch.setattr(claude_cli, "run_planned_pr_async", fake_planned)
    monkeypatch.setattr(runner_module.github_client, "get_open_prs", fake_get_open_prs)
    monkeypatch.setattr(coding_module.asyncio, "sleep", fast_sleep)

    runner = _make_runner()
    monkeypatch.setattr(runner, "_check_late_breach", fake_check_late_breach)
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001",
    )
    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.current_pr == pr
    assert any(
        "Recorded PR #42 before late-breach pause" in entry["event"]
        for entry in runner.state.history
    )


def test_handle_coding_errors_when_get_open_prs_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GitHub list failures after a successful CLI run surface as ERROR."""
    from src import codex_cli
    from src.config import CoderType

    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        codex_cli, "run_planned_pr_async", _async_cli_result(0, "ok", ""),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda *args, **kwargs: _raise_runtime_error("gh unavailable"),
    )

    runner = _make_runner(coder=CoderType.CODEX)
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001",
    )
    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "get_open_prs failed: gh unavailable"
    assert any("gh unavailable" == entry["event"] for entry in runner.state.history)


# -----------------------------------------------------------------------
# PR-065: Coder selection tests
# -----------------------------------------------------------------------


def test_get_coder_returns_claude_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _allow_all_coder_auth(monkeypatch)
    runner = _make_runner()
    name, plugin = runner._get_coder()
    assert name == "claude"
    assert plugin.name == "claude"


def test_get_coder_returns_codex_when_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.config import CoderType

    _allow_all_coder_auth(monkeypatch)
    runner = _make_runner()
    runner._app_config = _app_cfg(coder=CoderType.CODEX)
    name, plugin = runner._get_coder()
    assert name == "codex"
    assert plugin.name == "codex"


def test_get_coder_repo_override_takes_precedence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.config import CoderType

    _allow_all_coder_auth(monkeypatch)
    runner = _make_runner(coder=CoderType.CODEX)
    # Daemon default is claude, repo override is codex
    name, plugin = runner._get_coder()
    assert name == "codex"
    assert plugin.name == "codex"


def test_get_coder_uses_selector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    codex = runner._registry.get("codex")
    seen = []

    def fake_select(ctx: object) -> tuple[str, object]:
        seen.append(ctx)
        return ("codex", codex)

    monkeypatch.setattr(runner_module, "select_coder", fake_select)

    name, plugin = runner._get_coder()

    assert seen
    assert name == "codex"
    assert plugin is codex


def test_get_coder_uses_cached_auth_statuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _allow_all_coder_auth(monkeypatch)
    runner = _make_runner()
    runner._auth_status_cache = {
        "claude": {"status": "ok"},
        "codex": {"status": "error"},
    }
    seen: list[object] = []

    def fake_select(ctx: object) -> tuple[str, object]:
        seen.append(ctx)
        return ("claude", runner._registry.get("claude"))

    monkeypatch.setattr(runner_module, "select_coder", fake_select)

    runner._get_coder()

    assert seen
    assert getattr(seen[0], "auth_statuses") == runner._auth_status_cache


def test_get_coder_falls_through_to_default_when_selector_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _allow_all_coder_auth(monkeypatch)
    runner = _make_runner()
    monkeypatch.setattr(runner_module, "select_coder", lambda ctx: None)

    name, plugin = runner._get_coder()

    assert name == "claude"
    assert plugin.name == "claude"


def test_get_coder_hard_pin_overrides_default_when_selector_returns_none(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """When the active task pins a specific coder, ``_get_coder`` must not
    silently fall back to the repo/global default if the selector rejects
    the pin. Otherwise FIX iterations can run on the wrong coder."""
    from src.config import CoderType

    _allow_all_coder_auth(monkeypatch)
    runner = _make_runner(coder=CoderType.CLAUDE)
    runner.repo_path = str(tmp_path)
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-201.md").write_text(
        "# PR-201: Pinned to codex\n\n"
        "Branch: pr-201-pinned\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: codex\n",
        encoding="utf-8",
    )
    runner.state.current_task = QueueTask(
        pr_id="PR-201",
        title="Pinned to codex",
        status=TaskStatus.TODO,
        task_file="tasks/PR-201.md",
        branch="pr-201-pinned",
    )
    monkeypatch.setattr(runner_module, "select_coder", lambda ctx: None)

    name, plugin = runner._get_coder()

    assert name == "codex"
    assert plugin.name == "codex"


def test_get_coder_auto_fallback_switches_on_rate_limit_via_selector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _allow_all_coder_auth(monkeypatch)
    runner = _make_runner()

    runner.state.rate_limited_coders.add("claude")

    name, plugin = runner._get_coder()

    assert name == "codex"
    assert plugin.name == "codex"


def test_get_coder_repo_override_uses_selector_for_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.config import CoderType

    _allow_all_coder_auth(monkeypatch)
    runner = _make_runner(coder=CoderType.CODEX)
    runner.state.rate_limited_coders.add("codex")

    name, plugin = runner._get_coder()

    assert name == "claude"
    assert plugin.name == "claude"


def test_get_coder_exploration_occasionally_picks_non_greedy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _allow_all_coder_auth(monkeypatch)
    registry = CoderRegistry()
    registry.register(runner_module.build_coder_registry().get("claude"))
    registry.register(runner_module.build_coder_registry().get("codex"))
    runner = PipelineRunner(
        _repo_cfg(),
        _app_cfg(
            auto_fallback=True,
            coder_priority={"claude": 10, "codex": 20},
            exploration_epsilon=0.15,
        ),
        _FakeRedis(),
        _FakeUsageProvider(),
        _FakeUsageProvider(),
        registry=registry,
    )
    runner._selector_rng.seed(9)

    picks = [runner._get_coder()[0] for _ in range(200)]
    non_greedy = sum(1 for pick in picks if pick != "claude")

    assert 15 <= non_greedy <= 45


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
    """Codex can proceed while Claude remains paused until the window expires."""
    from src.config import CoderType

    _patch_subprocess(monkeypatch)
    runner = _make_runner(coder=CoderType.CODEX)
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=10)
    runner.state.rate_limit_reactive = False
    runner.state.rate_limit_reactive_coder = "claude"
    runner.state.rate_limited_coders.add("claude")
    runner.state.rate_limited_coder_until["claude"] = runner.state.rate_limited_until
    runner.state.state = PipelineState.PAUSED

    result = asyncio.run(runner._check_rate_limit())
    assert result is True
    assert runner.state.rate_limited_until is None
    assert runner.state.rate_limit_reactive_coder is None
    assert "claude" in runner.state.rate_limited_coders
    assert runner.state.rate_limited_coder_until.get("claude") is not None
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
    """Codex can proceed while a reactive Claude pause remains active."""
    from src.config import CoderType

    _patch_subprocess(monkeypatch)
    runner = _make_runner(coder=CoderType.CODEX)
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=10)
    runner.state.rate_limit_reactive = True
    runner.state.rate_limit_reactive_coder = "claude"
    runner.state.rate_limited_coders.add("claude")
    runner.state.rate_limited_coder_until["claude"] = runner.state.rate_limited_until
    runner.state.state = PipelineState.PAUSED

    result = asyncio.run(runner._check_rate_limit())
    assert result is True
    assert runner.state.rate_limited_until is None
    assert runner.state.rate_limit_reactive is False
    assert runner.state.rate_limit_reactive_coder is None
    assert "claude" in runner.state.rate_limited_coders
    assert runner.state.rate_limited_coder_until.get("claude") is not None
    assert runner.state.state == PipelineState.IDLE


def test_check_rate_limit_invalidates_cache_before_fallback_proactive_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fallback resumes with fresh usage snapshots instead of cached values."""
    from src.config import CoderType

    _patch_subprocess(monkeypatch)
    runner = _make_runner(coder=CoderType.CODEX)
    claude_provider = _FakeUsageProvider(snapshot=None)
    codex_provider = _FakeUsageProvider(snapshot=None)
    runner._claude_usage_provider = claude_provider
    runner._codex_usage_provider = codex_provider
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=10)
    runner.state.rate_limit_reactive = True
    runner.state.rate_limit_reactive_coder = "claude"
    runner.state.rate_limited_coders.add("claude")
    runner.state.rate_limited_coder_until["claude"] = runner.state.rate_limited_until
    runner.state.state = PipelineState.PAUSED

    result = asyncio.run(runner._check_rate_limit())

    assert result is True
    assert claude_provider._invalidated is True
    assert codex_provider._invalidated is True


def test_check_rate_limit_honors_effective_coder_pause_before_proactive_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.config import CoderType

    _patch_subprocess(monkeypatch)
    runner = _make_runner(coder=CoderType.CODEX)
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=10)
    runner.state.rate_limit_reactive = True
    runner.state.rate_limit_reactive_coder = "claude"
    runner.state.rate_limited_coders.update({"claude", "codex"})
    runner.state.rate_limited_coder_until = {
        "claude": datetime.now(timezone.utc) + timedelta(minutes=10),
        "codex": datetime.now(timezone.utc) + timedelta(minutes=5),
    }
    runner.state.state = PipelineState.PAUSED

    result = asyncio.run(runner._check_rate_limit(proactive_coder="codex"))

    assert result is False
    assert runner.state.rate_limited_until is not None
    assert runner.state.rate_limit_reactive_coder == "codex"
    assert runner.state.state == PipelineState.PAUSED


def test_check_rate_limit_expires_other_coder_pause_before_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Expired pause metadata is cleared even when another coder is active."""
    from src.config import CoderType

    _patch_subprocess(monkeypatch)
    runner = _make_runner(coder=CoderType.CODEX)
    runner.state.rate_limited_until = datetime.now(timezone.utc) - timedelta(minutes=1)
    runner.state.rate_limit_reactive = True
    runner.state.rate_limit_reactive_coder = "claude"
    runner.state.rate_limited_coders.add("claude")
    runner.state.state = PipelineState.PAUSED

    result = asyncio.run(runner._check_rate_limit())

    assert result is True
    assert runner.state.rate_limited_until is None
    assert runner.state.rate_limit_reactive is False
    assert runner.state.rate_limit_reactive_coder is None
    assert "claude" not in runner.state.rate_limited_coders


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


def test_check_rate_limit_preserves_per_coder_expiry_windows() -> None:
    runner = _make_runner()
    now = datetime.now(timezone.utc)
    claude_until = now + timedelta(minutes=20)
    codex_until = now + timedelta(minutes=5)
    runner.state.rate_limited_coders.update({"claude", "codex"})
    runner.state.rate_limited_coder_until = {
        "claude": claude_until,
        "codex": codex_until,
    }
    runner.state.rate_limited_until = codex_until
    runner.state.rate_limit_reactive_coder = "codex"
    runner.state.rate_limit_reactive = False

    assert selector_module._is_rate_limited("claude", runner.state) is True
    assert selector_module._is_rate_limited("codex", runner.state) is True

    runner.state.rate_limited_coder_until["codex"] = now - timedelta(minutes=1)
    result = asyncio.run(runner._check_rate_limit(proactive_coder="codex"))

    assert result is True
    assert "codex" not in runner.state.rate_limited_coders
    assert runner.state.rate_limited_coder_until.get("claude") == claude_until
    assert selector_module._is_rate_limited("claude", runner.state) is True


def test_check_rate_limit_reapplies_effective_coder_pause_after_other_expires(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.config import CoderType

    _patch_subprocess(monkeypatch)
    runner = _make_runner(coder=CoderType.CODEX)
    now = datetime.now(timezone.utc)
    codex_until = now + timedelta(minutes=5)
    runner.state.rate_limited_until = now - timedelta(minutes=1)
    runner.state.rate_limit_reactive = True
    runner.state.rate_limit_reactive_coder = "claude"
    runner.state.rate_limited_coders.update({"claude", "codex"})
    runner.state.rate_limited_coder_until = {
        "claude": now - timedelta(minutes=1),
        "codex": codex_until,
    }
    runner.state.state = PipelineState.PAUSED

    result = asyncio.run(runner._check_rate_limit(proactive_coder="codex"))

    assert result is False
    assert "claude" not in runner.state.rate_limited_coders
    assert runner.state.rate_limited_until == codex_until
    assert runner.state.rate_limit_reactive_coder == "codex"
    assert runner.state.state == PipelineState.PAUSED


def test_check_rate_limit_preserves_legacy_pause_for_other_coder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    runner = _make_runner()
    pause_until = datetime.now(timezone.utc) + timedelta(minutes=10)
    runner.state.rate_limited_until = pause_until
    runner.state.rate_limit_reactive = True
    runner.state.state = PipelineState.PAUSED

    result = asyncio.run(runner._check_rate_limit(proactive_coder="codex"))

    assert result is True
    assert runner.state.rate_limited_until is None
    assert runner.state.rate_limit_reactive is False
    assert runner.state.rate_limit_reactive_coder is None
    assert "claude" in runner.state.rate_limited_coders
    assert runner.state.rate_limited_coder_until["claude"] == pause_until


def test_runner_initializes_selector_rng_without_fixed_seed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_args: list[tuple[object, ...]] = []
    real_random = runner_module.random.Random

    def fake_random(*args: object, **kwargs: object) -> random.Random:
        assert not kwargs
        captured_args.append(args)
        return real_random(*args)

    monkeypatch.setattr(runner_module.random, "Random", fake_random)

    PipelineRunner(
        _repo_cfg(),
        _app_cfg(),
        _FakeRedis(),
        _FakeUsageProvider(),
        _FakeUsageProvider(),
    )

    assert captured_args == [()]


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


def test_monitor_inflight_breach_retries_bad_marker_then_uses_default_reset(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Bad marker JSON should be retried before defaulting to a 30-minute pause."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(breach_module, "_BREACH_POLL_SEC", 0.05)

    breach_run_id = "test-breach-bad-json"

    async def _run() -> None:
        runner = _make_runner()

        async def fake_cli_forever() -> tuple[int, str, str]:
            await asyncio.sleep(999)
            return (0, "", "")

        task = asyncio.create_task(fake_cli_forever())
        breach_flag: dict[str, bool] = {"breached": False}
        monitor = asyncio.create_task(
            runner._monitor_inflight_breach(
                str(tmp_path), breach_run_id, task, breach_flag,
            )
        )

        marker = tmp_path / f"{breach_run_id}.breach"
        marker.write_text("{not-json")
        await asyncio.sleep(0.1)
        marker.write_text(json.dumps({
            "type": "session",
            "resets_at": 0,
            "session_pct": 99,
        }))

        await asyncio.sleep(0.2)

        assert breach_flag["breached"] is True
        assert runner.state.rate_limited_until is not None
        remaining = runner.state.rate_limited_until - datetime.now(timezone.utc)
        assert timedelta(minutes=29) <= remaining <= timedelta(minutes=31)

        monitor.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    asyncio.run(_run())


def test_check_late_breach_returns_after_retry_exhaustion(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Late breach detection should stop after repeated marker read failures."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(breach_module.time, "sleep", lambda _seconds: None)

    runner = _make_runner()
    breach_flag = {"breached": False}
    marker = tmp_path / "retry-exhausted.breach"
    marker.write_text("{not-json")

    runner._check_late_breach(str(tmp_path), "retry-exhausted", breach_flag)

    assert breach_flag["breached"] is False
    assert runner.state.rate_limited_until is None


def test_check_late_breach_uses_resets_at_timestamp(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Late breach detection should use the marker reset timestamp when present."""
    _patch_subprocess(monkeypatch)

    runner = _make_runner()
    breach_flag = {"breached": False}
    resets_at = 1800000000
    marker = tmp_path / "late-reset.breach"
    marker.write_text(json.dumps({
        "type": "weekly",
        "resets_at": resets_at,
        "weekly_pct": 101,
    }))

    runner._check_late_breach(str(tmp_path), "late-reset", breach_flag)

    assert breach_flag["breached"] is True
    assert runner.state.rate_limited_until is not None
    assert int(runner.state.rate_limited_until.timestamp()) == resets_at


def test_cleanup_breach_marker_ignores_unlink_oserror(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Cleanup should swallow filesystem unlink failures."""
    _patch_subprocess(monkeypatch)
    runner = _make_runner()

    def fake_unlink(self: Path, missing_ok: bool = False) -> None:
        raise OSError("simulated unlink failure")

    monkeypatch.setattr(Path, "unlink", fake_unlink)

    runner._cleanup_breach_marker(str(tmp_path), "cleanup-oserror")


def test_publish_state_ignores_invalid_persisted_state_during_transaction() -> None:
    runner = _make_runner()
    runner.redis.store[f"pipeline:{runner.name}"] = "{not-json"

    asyncio.run(runner.publish_state())

    stored = RepoState.model_validate_json(runner.redis.store[f"pipeline:{runner.name}"])
    assert stored.name == runner.name


def test_handle_idle_returns_immediately_when_user_paused() -> None:
    runner = _make_runner()
    runner.state.user_paused = True

    asyncio.run(runner.handle_idle())

    assert runner.state.history == []


def test_handle_paused_logs_user_pause_only_once() -> None:
    runner = _make_runner()
    runner.state.user_paused = True

    asyncio.run(runner.handle_paused())
    asyncio.run(runner.handle_paused())

    assert sum(
        1
        for entry in runner.state.history
        if entry["event"] == "Paused. Press Play to resume."
    ) == 1


def test_handle_fix_head_unchanged_honors_stop_requested_after_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "rev-parse"] and "HEAD" in cmd:
            return _FakeCompletedProcess(args=cmd, stdout="abc123\n", returncode=0)
        if cmd[:2] == ["git", "rev-list"]:
            return _FakeCompletedProcess(args=cmd, stdout="0\n", returncode=0)
        return _FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    async def fake_pop_stop_request() -> bool:
        return True

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(claude_cli, "fix_review_async", _async_cli_result(0, "", ""))

    runner = _make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=50, branch="pr-050")
    monkeypatch.setattr(runner, "_pop_stop_request", fake_pop_stop_request)

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.error_message is None
    assert any(
        entry["event"] == "FIX aborted: user stop requested"
        for entry in runner.state.history
    )


def test_handle_fix_stop_cancel_returns_when_rev_parse_after_fix_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rev_parse_calls = {"count": 0}

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            if rev_parse_calls["count"] == 1:
                return _FakeCompletedProcess(
                    args=["git", *args], stdout="aaa111\n", returncode=0
                )
            raise subprocess.CalledProcessError(128, ["git", *args], stderr="boom")
        return _FakeCompletedProcess(args=["git", *args], returncode=0)

    async def fake_fix(*args: object, **kwargs: object) -> tuple[int, str, str]:
        await asyncio.Future()
        return (0, "", "")

    async def no_idle_monitor(
        self: object,
        pr_number: int,
        idle_limit: int,
        target: asyncio.Task,
        idle_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    async def no_breach_monitor(
        self: object,
        breach_dir: str,
        run_id: str,
        claude_task: asyncio.Task,
        breach_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    runner = _make_runner()

    async def stop_monitor(cli_task: asyncio.Task[tuple[int, str, str]]) -> None:
        runner._stop_requested = True
        runner.state.user_paused = True
        await asyncio.sleep(0)
        cli_task.cancel()

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", no_idle_monitor)
    monkeypatch.setattr(PipelineRunner, "_monitor_inflight_breach", no_breach_monitor)

    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    monkeypatch.setattr(runner, "_monitor_stop_request", stop_monitor)

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.ERROR
    assert "rev-parse after fix failed" in (runner.state.error_message or "")


def test_handle_fix_stop_cancel_logs_fetch_failure_after_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rev_parse_calls = {"count": 0}
    fetch_calls = {"count": 0}

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        if args and args[0] == "fetch":
            fetch_calls["count"] += 1
            if fetch_calls["count"] == 2:
                raise subprocess.CalledProcessError(1, ["git", *args], stderr="fetch fail")
            return _FakeCompletedProcess(args=["git", *args], returncode=0)
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            sha = "aaa111\n" if rev_parse_calls["count"] == 1 else "bbb222\n"
            return _FakeCompletedProcess(args=["git", *args], stdout=sha, returncode=0)
        return _FakeCompletedProcess(args=["git", *args], returncode=0)

    async def fake_fix(*args: object, **kwargs: object) -> tuple[int, str, str]:
        await asyncio.Future()
        return (0, "", "")

    async def no_idle_monitor(
        self: object,
        pr_number: int,
        idle_limit: int,
        target: asyncio.Task,
        idle_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    async def no_breach_monitor(
        self: object,
        breach_dir: str,
        run_id: str,
        claude_task: asyncio.Task,
        breach_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    runner = _make_runner()

    async def stop_monitor(cli_task: asyncio.Task[tuple[int, str, str]]) -> None:
        runner._stop_requested = True
        runner.state.user_paused = True
        await asyncio.sleep(0)
        cli_task.cancel()

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", no_idle_monitor)
    monkeypatch.setattr(PipelineRunner, "_monitor_inflight_breach", no_breach_monitor)

    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    monkeypatch.setattr(runner, "_monitor_stop_request", stop_monitor)

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.PAUSED
    assert any(
        "fetch pr-042-fix failed after FIX stop" in entry["event"]
        for entry in runner.state.history
    )


def test_handle_fix_stop_cancel_logs_remote_rev_parse_failure_after_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rev_parse_head_calls = {"count": 0}

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_head_calls["count"] += 1
            sha = "aaa111\n" if rev_parse_head_calls["count"] == 1 else "bbb222\n"
            return _FakeCompletedProcess(args=["git", *args], stdout=sha, returncode=0)
        if args[:2] == ("fetch", "origin"):
            return _FakeCompletedProcess(args=["git", *args], returncode=0)
        if args[:2] == ("rev-parse", "origin/pr-042-fix"):
            raise subprocess.CalledProcessError(128, ["git", *args], stderr="bad ref")
        return _FakeCompletedProcess(args=["git", *args], returncode=0)

    async def fake_fix(*args: object, **kwargs: object) -> tuple[int, str, str]:
        await asyncio.Future()
        return (0, "", "")

    async def no_idle_monitor(
        self: object,
        pr_number: int,
        idle_limit: int,
        target: asyncio.Task,
        idle_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    async def no_breach_monitor(
        self: object,
        breach_dir: str,
        run_id: str,
        claude_task: asyncio.Task,
        breach_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    runner = _make_runner()

    async def stop_monitor(cli_task: asyncio.Task[tuple[int, str, str]]) -> None:
        runner._stop_requested = True
        runner.state.user_paused = True
        await asyncio.sleep(0)
        cli_task.cancel()

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", no_idle_monitor)
    monkeypatch.setattr(PipelineRunner, "_monitor_inflight_breach", no_breach_monitor)

    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    monkeypatch.setattr(runner, "_monitor_stop_request", stop_monitor)

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.PAUSED
    assert any(
        "rev-parse origin/pr-042-fix failed after FIX stop" in entry["event"]
        for entry in runner.state.history
    )


def test_handle_fix_stop_cancel_logs_merge_base_failure_after_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rev_parse_head_calls = {"count": 0}

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_head_calls["count"] += 1
            sha = "aaa111\n" if rev_parse_head_calls["count"] == 1 else "bbb222\n"
            return _FakeCompletedProcess(args=["git", *args], stdout=sha, returncode=0)
        if args[:2] == ("fetch", "origin"):
            return _FakeCompletedProcess(args=["git", *args], returncode=0)
        if args[:2] == ("rev-parse", "origin/pr-042-fix"):
            return _FakeCompletedProcess(args=["git", *args], stdout="ccc333\n", returncode=0)
        if args[:2] == ("merge-base", "--is-ancestor"):
            raise OSError("merge-base fail")
        return _FakeCompletedProcess(args=["git", *args], returncode=0)

    async def fake_fix(*args: object, **kwargs: object) -> tuple[int, str, str]:
        await asyncio.Future()
        return (0, "", "")

    async def no_idle_monitor(
        self: object,
        pr_number: int,
        idle_limit: int,
        target: asyncio.Task,
        idle_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    async def no_breach_monitor(
        self: object,
        breach_dir: str,
        run_id: str,
        claude_task: asyncio.Task,
        breach_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    runner = _make_runner()

    async def stop_monitor(cli_task: asyncio.Task[tuple[int, str, str]]) -> None:
        runner._stop_requested = True
        runner.state.user_paused = True
        await asyncio.sleep(0)
        cli_task.cancel()

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", no_idle_monitor)
    monkeypatch.setattr(PipelineRunner, "_monitor_inflight_breach", no_breach_monitor)

    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    monkeypatch.setattr(runner, "_monitor_stop_request", stop_monitor)

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.PAUSED
    assert any(
        "merge-base ancestry check failed after FIX stop" in entry["event"]
        for entry in runner.state.history
    )


def test_handle_fix_stop_cancel_short_circuits_when_head_matches_before(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            return _FakeCompletedProcess(
                args=["git", *args], stdout="aaa111\n", returncode=0
            )
        if args[:2] == ("fetch", "origin"):
            return _FakeCompletedProcess(args=["git", *args], returncode=0)
        if args[:2] == ("rev-parse", "origin/pr-042-fix"):
            return _FakeCompletedProcess(args=["git", *args], stdout="aaa111\n", returncode=0)
        return _FakeCompletedProcess(args=["git", *args], returncode=0)

    async def fake_fix(*args: object, **kwargs: object) -> tuple[int, str, str]:
        await asyncio.Future()
        return (0, "", "")

    async def no_idle_monitor(
        self: object,
        pr_number: int,
        idle_limit: int,
        target: asyncio.Task,
        idle_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    async def no_breach_monitor(
        self: object,
        breach_dir: str,
        run_id: str,
        claude_task: asyncio.Task,
        breach_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    runner = _make_runner()

    async def stop_monitor(cli_task: asyncio.Task[tuple[int, str, str]]) -> None:
        runner._stop_requested = True
        runner.state.user_paused = True
        await asyncio.sleep(0)
        cli_task.cancel()

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", no_idle_monitor)
    monkeypatch.setattr(PipelineRunner, "_monitor_inflight_breach", no_breach_monitor)

    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    monkeypatch.setattr(runner, "_monitor_stop_request", stop_monitor)

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 0


def test_handle_fix_stop_cancel_errors_when_review_post_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rev_parse_calls = {"count": 0}

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            sha = "aaa111\n" if rev_parse_calls["count"] == 1 else "bbb222\n"
            return _FakeCompletedProcess(args=["git", *args], stdout=sha, returncode=0)
        if args[:2] == ("fetch", "origin"):
            return _FakeCompletedProcess(args=["git", *args], returncode=0)
        if args[:2] == ("rev-parse", "origin/pr-042-fix"):
            return _FakeCompletedProcess(args=["git", *args], stdout="bbb222\n", returncode=0)
        return _FakeCompletedProcess(args=["git", *args], returncode=0)

    async def fake_fix(*args: object, **kwargs: object) -> tuple[int, str, str]:
        await asyncio.Future()
        return (0, "", "")

    async def no_idle_monitor(
        self: object,
        pr_number: int,
        idle_limit: int,
        target: asyncio.Task,
        idle_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    async def no_breach_monitor(
        self: object,
        breach_dir: str,
        run_id: str,
        claude_task: asyncio.Task,
        breach_flag: dict[str, bool],
    ) -> None:
        await asyncio.sleep(0)

    runner = _make_runner()

    async def stop_monitor(cli_task: asyncio.Task[tuple[int, str, str]]) -> None:
        runner._stop_requested = True
        runner.state.user_paused = True
        await asyncio.sleep(0)
        cli_task.cancel()

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", no_idle_monitor)
    monkeypatch.setattr(PipelineRunner, "_monitor_inflight_breach", no_breach_monitor)

    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    monkeypatch.setattr(runner, "_monitor_stop_request", stop_monitor)
    monkeypatch.setattr(runner, "_post_codex_review", lambda pr_number: False)

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.ERROR
    assert "Failed to post @codex review on PR #42 after stop-cancel fix push" in (
        runner.state.error_message or ""
    )


def test_refresh_user_paused_from_redis_ignores_invalid_json() -> None:
    runner = _make_runner()
    runner.redis.store[f"pipeline:{runner.name}"] = "{not-json"
    runner.state.user_paused = False

    asyncio.run(runner._refresh_user_paused_from_redis())

    assert runner.state.user_paused is False


def test_pop_stop_request_returns_false_when_redis_get_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()

    async def boom_get(key: str) -> str | None:
        raise RuntimeError("redis down")

    monkeypatch.setattr(runner.redis, "get", boom_get)

    assert asyncio.run(runner._pop_stop_request()) is False


def test_pop_stop_request_returns_true_when_delete_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    runner.redis.store[f"control:{runner.name}:stop"] = "1"

    async def boom_delete(key: str) -> int:
        raise RuntimeError("delete failed")

    monkeypatch.setattr(runner.redis, "delete", boom_delete)

    assert asyncio.run(runner._pop_stop_request()) is True


def test_terminate_current_coder_clears_exited_process() -> None:
    runner = _make_runner()

    class _Proc:
        returncode = 0

    runner._current_coder_process = _Proc()

    asyncio.run(runner._terminate_current_coder())

    assert runner._current_coder_process is None


def test_terminate_current_coder_handles_missing_process() -> None:
    runner = _make_runner()

    class _Proc:
        returncode = None

        def terminate(self) -> None:
            raise ProcessLookupError

    runner._current_coder_process = _Proc()

    asyncio.run(runner._terminate_current_coder())

    assert runner._current_coder_process is None


def test_terminate_current_coder_kills_after_timeout() -> None:
    runner = _make_runner()
    calls: list[str] = []

    class _Proc:
        returncode = None

        def terminate(self) -> None:
            calls.append("terminate")

        def kill(self) -> None:
            calls.append("kill")

        async def wait(self) -> None:
            calls.append("wait")
            if calls.count("wait") == 1:
                raise asyncio.TimeoutError
            return None

    runner._current_coder_process = _Proc()

    asyncio.run(runner._terminate_current_coder())

    assert calls == ["terminate", "wait", "kill", "wait"]
    assert runner._current_coder_process is None


def test_terminate_current_coder_ignores_missing_process_on_kill() -> None:
    runner = _make_runner()
    calls: list[str] = []

    class _Proc:
        returncode = None

        def terminate(self) -> None:
            calls.append("terminate")

        def kill(self) -> None:
            calls.append("kill")
            raise ProcessLookupError

        async def wait(self) -> None:
            calls.append("wait")
            if calls.count("wait") == 1:
                raise asyncio.TimeoutError
            return None

    runner._current_coder_process = _Proc()

    asyncio.run(runner._terminate_current_coder())

    assert calls == ["terminate", "wait", "kill", "wait"]
    assert runner._current_coder_process is None


def test_handle_coding_honors_stop_requested_after_pr_poll_exhaustion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_planned_pr_async",
        _async_cli_result(0, "ok", ""),
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

    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-127",
        title="Pause controls",
        status=TaskStatus.DOING,
        branch="pr-127-control-endpoints-backend",
    )

    def fake_get_open_prs(repo: str, **kw: Any) -> list[PRInfo]:
        attempts["count"] += 1
        return []

    monkeypatch.setattr(runner_module.github_client, "get_open_prs", fake_get_open_prs)
    monkeypatch.setattr(runner, "_pop_stop_request", fake_pop_stop_request)
    monkeypatch.setattr(runner_module.asyncio, "sleep", instant_sleep)
    monkeypatch.setattr(runner, "_monitor_stop_request", stale_stop_monitor)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.error_message is None
    assert runner.state.current_pr is None
    assert attempts["count"] == 3
    assert any(
        entry["event"] == "CODING aborted: user stop requested"
        for entry in runner.state.history
    )
