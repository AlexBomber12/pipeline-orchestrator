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
from src.coder_registry import CoderRegistry
from src.coders import claude as claude_plugin_module
from src.config import AppConfig, CoderType, DaemonConfig, RepoConfig
from src.daemon import git_ops as git_ops_module
from src.daemon import recovery_policy as recovery_policy_module
from src.daemon import runner as runner_module
from src.daemon import selector as selector_module
from src.daemon.handlers import error as error_module
from src.daemon.handlers import fix as fix_module
from src.daemon.handlers import hung as hung_module
from src.daemon.handlers import idle as idle_module
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


@pytest.fixture(autouse=True)
def _disable_github_rate_limit_fetch_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tests that don't pin a budget shouldn't actually call the gh CLI."""
    monkeypatch.setattr(
        runner_module.github_client,
        "fetch_rate_limit_buckets",
        lambda: (None, None),
    )


class _FakeRedis:
    """Minimal async Redis double capturing ``set`` calls."""

    def __init__(self) -> None:
        self.writes: list[tuple[str, str]] = []
        self.store: dict[str, str] = {}
        self.deleted: list[str] = []
        self.lists: dict[str, list[str]] = {}

    async def set(
        self,
        key: str,
        value: str,
        ex: int | None = None,
        nx: bool = False,
    ) -> bool | None:
        if nx and key in self.store:
            return None
        self.writes.append((key, value))
        self.store[key] = value
        return True

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
    assert (
        runner.state.history[-1]["event"]
        == "[INFRA] Reloaded repo config from config.yml."
    )


def test_stage_config_reload_tracks_idle_boundary_flag() -> None:
    """``stage_config_reload`` records whether the staged change must wait
    for an IDLE boundary so the daemon's post-cycle drain can decide
    whether to apply immediately or keep deferring."""

    runner = _make_runner()
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
        _FakeUsageProvider(snapshot="claude"),
        _FakeUsageProvider(snapshot="codex"),
    )
    assert runner._pending_requires_idle_boundary is False

    runner.stage_config_reload(
        next_repo_config,
        next_app_config,
        _FakeUsageProvider(snapshot="claude"),
        _FakeUsageProvider(snapshot="codex"),
        requires_idle_boundary=True,
    )
    assert runner._pending_requires_idle_boundary is True

    runner.clear_staged_config_reload()
    assert runner._pending_requires_idle_boundary is False


def test_stage_config_reload_preserves_idle_boundary_across_updates() -> None:
    """A later ``requires_idle_boundary=False`` reload must not downgrade
    a pending coder-swap deferral that was staged earlier in the same
    in-flight window."""

    runner = _make_runner()
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
        _FakeUsageProvider(snapshot="claude"),
        _FakeUsageProvider(snapshot="codex"),
        requires_idle_boundary=True,
    )
    assert runner._pending_requires_idle_boundary is True

    runner.stage_config_reload(
        next_repo_config,
        next_app_config,
        _FakeUsageProvider(snapshot="claude-2"),
        _FakeUsageProvider(snapshot="codex-2"),
    )
    assert runner._pending_requires_idle_boundary is True

    runner.clear_staged_config_reload()
    assert runner._pending_requires_idle_boundary is False


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
    rev_parse_head_calls = {"n": 0}

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
        # ``git cat-file -e origin/<branch>:tasks/QUEUE.md`` is the
        # tracked-QUEUE probe used by ``_origin_queue_md_tracked``.
        # Default to non-zero (untracked, post-PR-181) so tests don't
        # accidentally take the legacy-skip branch; tests that DO need
        # to simulate the legacy tracked-QUEUE state install their own
        # fake_run.
        if cmd[:3] == ["git", "cat-file", "-e"]:
            return _FakeCompletedProcess(args=cmd, returncode=1)
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
        # ``git rev-parse HEAD`` must return a real-looking SHA: the
        # FIX path's ``record_observed_head`` adds the SHA to a set
        # and the empty string is now a deliberate no-op (a rev-parse
        # failure that polling reconciles on the next refresh, see
        # ``PRInfo.record_observed_head``). Returning a deterministic
        # head_before / head_after pair keeps the default FIX mock
        # exercising the productive-push path, the same way it
        # implicitly did before via the empty-SHA fallback.
        if cmd[:3] == ["git", "rev-parse", "HEAD"]:
            rev_parse_head_calls["n"] += 1
            sha = (
                "head-before-abc"
                if rev_parse_head_calls["n"] == 1
                else "head-after-def"
            )
            return _FakeCompletedProcess(
                args=cmd, stdout=f"{sha}\n", returncode=0
            )
        # ``git rev-parse origin/<branch>`` answers
        # ``_verify_pushes_since`` after the FIX push. Match
        # head_after so verification short-circuits to ``True`` —
        # otherwise the merge-base fallback fires unnecessarily and
        # tests that capture the call sequence have to reason about
        # extra git plumbing.
        if (
            cmd[:2] == ["git", "rev-parse"]
            and len(cmd) >= 3
            and cmd[2].startswith("origin/")
        ):
            return _FakeCompletedProcess(
                args=cmd, stdout="head-after-def\n", returncode=0
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

    def _label(idx: int) -> str:
        # Distinct non-numeric labels so fuzzy dedup never collapses them.
        return f"event-{chr(ord('a') + idx % 10)}-{chr(ord('a') + idx // 10)}"

    for i in range(150):
        runner.log_event(_label(i))

    assert len(runner.state.history) == 100
    assert runner.state.history[0]["event"] == _label(50)
    assert runner.state.history[-1]["event"] == _label(149)


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


def test_normalize_for_dedup_replaces_numeric_runs() -> None:
    assert (
        runner_module._normalize_for_dedup("PR #221 waiting 1/20m")
        == "PR #221 waiting #/#m"
    )
    assert runner_module._normalize_for_dedup("no numbers here") == "no numbers here"
    assert runner_module._normalize_for_dedup("123") == "#"
    assert runner_module._normalize_for_dedup("1a2b3") == "#a#b#"


def test_normalize_for_dedup_preserves_pr_identifier() -> None:
    """``PR #<n>`` tokens are NOT normalized so different PRs stay distinct."""
    five = runner_module._normalize_for_dedup(
        "PR #5 waiting (review=APPROVED, ci=SUCCESS, 1/20m)"
    )
    six = runner_module._normalize_for_dedup(
        "PR #6 waiting (review=APPROVED, ci=SUCCESS, 1/20m)"
    )
    assert five != six
    assert "PR #5" in five
    assert "PR #6" in six


def test_log_event_fuzzy_dedupes_messages_differing_only_in_numbers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeDateTime:
        _values = iter(
            [
                datetime(2026, 4, 28, 11, 59, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 28, 12, 0, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 28, 12, 1, 0, tzinfo=timezone.utc),
            ]
        )

        @classmethod
        def now(cls, tz: timezone) -> datetime:
            return next(cls._values).astimezone(tz)

    monkeypatch.setattr(runner_module, "datetime", _FakeDateTime)
    runner = _make_runner()

    runner.log_event("PR #221 waiting (review=EYES, ci=PENDING, 1/20m)")
    runner.log_event("PR #221 waiting (review=EYES, ci=PENDING, 2/20m)")

    assert len(runner.state.history) == 1
    entry = runner.state.history[0]
    assert entry["count"] == 2
    assert entry["event"] == "PR #221 waiting (review=EYES, ci=PENDING, 2/20m)"
    assert entry["time"] == "2026-04-28T12:00:00+00:00"
    assert entry["last_seen_at"] == "2026-04-28T12:01:00+00:00"


def test_log_event_fuzzy_dedupes_three_in_a_row() -> None:
    runner = _make_runner()

    for i in range(1, 4):
        runner.log_event(f"PR #221 waiting ({i}/20m)")

    assert len(runner.state.history) == 1
    assert runner.state.history[0]["count"] == 3
    assert runner.state.history[0]["event"] == "PR #221 waiting (3/20m)"


def test_log_event_does_not_fuzzy_dedupe_when_non_numeric_content_differs() -> None:
    runner = _make_runner()

    runner.log_event("PR #221 merged")
    runner.log_event("PR #221 closed")

    assert len(runner.state.history) == 2
    assert runner.state.history[0]["event"] == "PR #221 merged"
    assert runner.state.history[1]["event"] == "PR #221 closed"
    assert runner.state.history[0]["count"] == 1
    assert runner.state.history[1]["count"] == 1


def test_log_event_does_not_fuzzy_dedupe_across_pr_numbers() -> None:
    """Switching PR numbers must produce two history rows, not one merged row.

    Regression: the previous regex normalized every digit run, so consecutive
    WATCH cycles for two different PRs (e.g., PR #5 -> PR #6) with the same
    review/CI text would collapse and the earlier PR transition would be lost.
    """
    runner = _make_runner()

    runner.log_event("PR #5 waiting (review=APPROVED, ci=SUCCESS, 1/20m)")
    runner.log_event("PR #6 waiting (review=APPROVED, ci=SUCCESS, 1/20m)")

    assert len(runner.state.history) == 2
    assert "PR #5" in runner.state.history[0]["event"]
    assert "PR #6" in runner.state.history[1]["event"]
    assert runner.state.history[0]["count"] == 1
    assert runner.state.history[1]["count"] == 1


def test_log_event_resets_count_after_fuzzy_streak_breaks() -> None:
    runner = _make_runner()

    runner.log_event("PR #5 waiting (1/20m)")
    runner.log_event("PR #5 waiting (2/20m)")
    runner.log_event("PR #5 merged")
    runner.log_event("PR #5 waiting (3/20m)")

    assert len(runner.state.history) == 3
    assert runner.state.history[0]["count"] == 2
    assert runner.state.history[0]["event"] == "PR #5 waiting (2/20m)"
    assert runner.state.history[1]["count"] == 1
    assert runner.state.history[1]["event"] == "PR #5 merged"
    assert runner.state.history[2]["count"] == 1
    assert runner.state.history[2]["event"] == "PR #5 waiting (3/20m)"


def test_idle_uses_cached_merged_prs(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )

    merged_pr_calls: list[dict[str, object]] = []

    def fake_get_merged_prs(*args: object, **kwargs: object) -> list[PRInfo]:
        merged_pr_calls.append(dict(kwargs))
        return []

    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
        fake_get_merged_prs,
    )

    runner = _make_runner()
    started_at = time.monotonic()
    asyncio.run(runner.handle_idle())
    asyncio.run(runner.handle_idle())

    assert time.monotonic() - started_at < 60
    assert len(merged_pr_calls) == 2
    assert all(call.get("refresh", False) is False for call in merged_pr_calls)


def test_idle_refreshes_merged_prs_when_open_pr_snapshot_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    open_pr_cycles = [[PRInfo(number=42, branch="pr-042-sample")], []]

    def fake_get_open_prs(repo: str, **kwargs: object) -> list[PRInfo]:
        del repo, kwargs
        return open_pr_cycles.pop(0)

    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        fake_get_open_prs,
    )
    refresh_calls: list[bool] = []

    def fake_get_merged_prs(
        repo: str,
        branch: str,
        refresh: bool = False,
    ) -> list[PRInfo]:
        del repo, branch
        refresh_calls.append(refresh)
        return []

    monkeypatch.setattr(
        runner_module.github_client,
        "get_merged_prs",
        fake_get_merged_prs,
    )

    runner = _make_runner()
    asyncio.run(runner.handle_idle())
    asyncio.run(runner.handle_idle())

    assert refresh_calls == [False, True]


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


def _capture_fix_kwargs(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, Any]:
    """Patch ``claude_cli.fix_review_async`` to record kwargs and exit 0."""
    captured: dict[str, Any] = {}

    async def fake_fix(*args: object, **kwargs: object) -> tuple[int, str, str]:
        captured["kwargs"] = dict(kwargs)
        return (0, "", "")

    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)
    return captured


def test_fetch_failed_ci_logs_truncates_to_last_5000_chars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_fetch_failed_ci_logs returns last 5000 chars prefixed with [truncated]."""
    long_log = "X" * 6000

    def fake_run_gh(args: list[str], **kwargs: Any) -> object:
        if args[:2] == ["run", "list"]:
            return [{"databaseId": 12345}]
        if args[:2] == ["run", "view"]:
            return long_log
        raise AssertionError(f"unexpected gh call: {args}")

    monkeypatch.setattr(fix_module.github_client, "run_gh", fake_run_gh)

    out = fix_module._fetch_failed_ci_logs("octo/demo", "pr-019")

    assert out is not None
    assert out.startswith("[truncated]\n")
    assert out.endswith("X" * 5000)
    assert len(out) == len("[truncated]\n") + 5000


def test_fetch_failed_ci_logs_short_log_returned_as_is(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    short_log = "tiny failure trace"

    def fake_run_gh(args: list[str], **kwargs: Any) -> object:
        if args[:2] == ["run", "list"]:
            return [{"databaseId": 12345}]
        if args[:2] == ["run", "view"]:
            return short_log
        raise AssertionError(f"unexpected gh call: {args}")

    monkeypatch.setattr(fix_module.github_client, "run_gh", fake_run_gh)

    assert fix_module._fetch_failed_ci_logs("octo/demo", "pr-019") == short_log


def test_fetch_failed_ci_logs_returns_none_when_no_failed_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        fix_module.github_client, "run_gh", lambda args, **kwargs: []
    )
    assert fix_module._fetch_failed_ci_logs("octo/demo", "pr-019") is None


def test_fetch_failed_ci_logs_returns_none_on_non_list_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        fix_module.github_client,
        "run_gh",
        lambda args, **kwargs: "not a list",
    )
    assert fix_module._fetch_failed_ci_logs("octo/demo", "pr-019") is None


def test_fetch_failed_ci_logs_returns_none_when_first_entry_not_dict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        fix_module.github_client,
        "run_gh",
        lambda args, **kwargs: ["unexpected-string"],
    )
    assert fix_module._fetch_failed_ci_logs("octo/demo", "pr-019") is None


def test_fetch_failed_ci_logs_returns_none_when_database_id_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        fix_module.github_client,
        "run_gh",
        lambda args, **kwargs: [{"databaseId": None}],
    )
    assert fix_module._fetch_failed_ci_logs("octo/demo", "pr-019") is None


def test_fetch_failed_ci_logs_returns_none_when_run_view_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str], **kwargs: Any) -> object:
        if args[:2] == ["run", "list"]:
            return [{"databaseId": 9}]
        return ""

    monkeypatch.setattr(fix_module.github_client, "run_gh", fake_run_gh)
    assert fix_module._fetch_failed_ci_logs("octo/demo", "pr-019") is None


def test_fetch_failed_ci_logs_returns_none_on_run_list_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str], **kwargs: Any) -> object:
        raise RuntimeError("api blew up")

    monkeypatch.setattr(fix_module.github_client, "run_gh", fake_run_gh)
    assert fix_module._fetch_failed_ci_logs("octo/demo", "pr-019") is None


def test_fetch_failed_ci_logs_returns_none_on_run_view_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str], **kwargs: Any) -> object:
        if args[:2] == ["run", "list"]:
            return [{"databaseId": 9}]
        raise RuntimeError("log fetch blew up")

    monkeypatch.setattr(fix_module.github_client, "run_gh", fake_run_gh)
    assert fix_module._fetch_failed_ci_logs("octo/demo", "pr-019") is None


# ---------------------------------------------------------------------------
# PR-164: FIX no-push deadlock circuit breaker
# ---------------------------------------------------------------------------


def _patch_no_push_fix(
    monkeypatch: pytest.MonkeyPatch,
    head_seq: Callable[[], str],
) -> list[tuple[str, int, str]]:
    """Wire fake git/CLI/comment hooks for no-push FIX cycle tests.

    ``head_seq`` is a zero-arg callable that returns the SHA each time
    ``git rev-parse HEAD`` is invoked, letting individual tests stitch
    together arbitrary head_before/head_after sequences across many
    consecutive ``handle_fix`` calls.
    """
    posted: list[tuple[str, int, str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        # ``git rev-parse HEAD`` is the no-push detection probe whose
        # sequencing the test stitches together via ``head_seq``. The
        # ``--abbrev-ref HEAD`` form is the BranchContext probe and
        # must not consume from ``head_seq`` — return a stable branch
        # name so the no-push counter sequence stays deterministic.
        if cmd[:3] == ["git", "rev-parse", "HEAD"]:
            return _FakeCompletedProcess(
                args=cmd, stdout=f"{head_seq()}\n", returncode=0
            )
        if cmd[:2] == ["git", "rev-parse"] and "--abbrev-ref" in cmd:
            return _FakeCompletedProcess(
                args=cmd, stdout="pr-218\n", returncode=0
            )
        if cmd[:2] == ["git", "rev-list"]:
            return _FakeCompletedProcess(args=cmd, stdout="0\n", returncode=0)
        return _FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        claude_cli, "fix_review_async", _async_cli_result(0, "", "")
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "run_gh",
        lambda *a, **kw: "",
    )
    return posted


# ---------------------------------------------------------------------------
# PR-166: Coder ESCALATE protocol in FIX cycle
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "stdout, expected",
    [
        # Last non-empty line is ESCALATE: <reason>
        ("doing things\nESCALATE: rate limit exceeded\n", "rate limit exceeded"),
        # Trailing blank lines are ignored
        ("ESCALATE: foo\n\n\n", "foo"),
        # ESCALATE in the middle, last line is something else → no trigger
        ("ESCALATE: stale\nfollow-up unrelated\n", None),
        # Empty stdout
        ("", None),
        # No marker at all
        ("ran tests\nall good\n", None),
        # Empty reason → empty string (caller substitutes placeholder)
        ("ESCALATE:\n", ""),
        ("ESCALATE: \n", ""),
        # Strict case-sensitive: lowercase variant rejected
        ("escalate: typo\n", None),
        # Past-tense variant rejected
        ("ESCALATED: misnamed\n", None),
        # Strict start-of-line: indented marker must NOT trigger
        ("  ESCALATE: indented\n", None),
        ("\tESCALATE: tabbed\n", None),
    ],
)
def test_parse_escalate_marker(stdout: str, expected: str | None) -> None:
    assert fix_module._parse_escalate_marker(stdout) == expected


def _patch_fix_with_stdout(
    monkeypatch: pytest.MonkeyPatch,
    *,
    stdout: str,
    code: int = 0,
    head_seq: Callable[[], str] | None = None,
) -> tuple[list[tuple[str, int, str]], list[list[str]]]:
    """Wire git/CLI/comment fakes for an ESCALATE-marker FIX cycle test.

    ``head_seq`` defaults to a constant ``"abc123"`` (no-push) so tests
    that only care about the ESCALATE behavior do not have to spell
    out the head sequence. Returns ``(posted, gh_calls)``.
    """
    seq = head_seq or (lambda: "abc123")
    posted: list[tuple[str, int, str]] = []
    gh_calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "rev-parse"] and "HEAD" in cmd:
            return _FakeCompletedProcess(
                args=cmd, stdout=f"{seq()}\n", returncode=0
            )
        if cmd[:2] == ["git", "rev-list"]:
            return _FakeCompletedProcess(args=cmd, stdout="0\n", returncode=0)
        return _FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    async def fake_fix(*args: object, **kwargs: object) -> tuple[int, str, str]:
        return (code, stdout, "")

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)
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
    return posted, gh_calls


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
    tmp_path: Path,
) -> None:
    parsed_tasks = [
        QueueTask(
            pr_id="PR-001",
            title="t",
            status=TaskStatus.DOING,
            branch="pr-001",
            task_file="tasks/PR-001.md",
        )
    ]
    (tmp_path / "tasks").mkdir(parents=True)
    (tmp_path / "tasks" / "PR-001.md").write_text("# PR-001\n")

    def fake_git(repo_path: str, *args: str, **kw: Any) -> Any:
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
        runner_module.PipelineRunner,
        "_parse_base_queue",
        lambda self, **_: parsed_tasks,
    )
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
    runner.repo_path = str(tmp_path)
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
    recovered.repo_path = str(tmp_path)
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


def test_preserve_fix_iteration_count_counts_new_head_sha_after_upgrade() -> None:
    """``_preserve_fix_iteration_count`` is the IDLE-side mirror of the
    WATCH merge. A pre-PR-195 ``current_pr`` carrying a legacy
    ``push_count`` with empty ``observed_head_shas`` must bump the
    counter when IDLE rehydrates a freshly fetched PRInfo with a new
    head SHA — same regression, different polling path.
    """
    runner = _make_runner()
    runner.state.current_pr = PRInfo(
        number=21,
        branch="pr-021",
        push_count=7,
        observed_head_shas=set(),
    )
    polled = PRInfo(
        number=21,
        branch="pr-021",
        push_count=1,
        observed_head_shas={"new-polled-sha"},
    )

    rehydrated = runner._preserve_fix_iteration_count(polled)

    assert rehydrated.observed_head_shas == {"new-polled-sha"}
    assert rehydrated.push_count == 8


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


def test_mark_queue_done_writes_updated_queue_to_disk(tmp_path: Path) -> None:
    """PR-181: ``_mark_queue_done`` updates the local QUEUE.md only —
    no commit, no push, no remediation PR. The next IDLE cycle
    regenerates the file deterministically from task headers anyway,
    so the disk write is just a best-effort tweak for read consumers
    between merge and the next IDLE tick."""
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    queue_path.write_text(
        "## PR-001: first\n- Status: DOING\n\n"
        "## PR-002: second\n- Status: TODO\n"
    )

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="first", status=TaskStatus.DOING
    )

    runner._mark_queue_done()

    updated = queue_path.read_text()
    assert "## PR-001: first\n- Status: DONE" in updated
    assert "## PR-002: second\n- Status: TODO" in updated
    # The pending queue-sync infrastructure is no longer engaged.
    assert runner.state.pending_queue_sync_branch is None
    assert runner.state.pending_queue_sync_started_at is None


def test_mark_queue_done_skips_when_origin_queue_md_tracked(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Legacy repos that still track ``tasks/QUEUE.md`` on origin must
    not have the local file rewritten — an unstaged rewrite would
    dirty the working tree, push the next-cycle preflight to ERROR,
    and block normal IDLE dispatch. Mirrors the
    ``_write_generated_queue_md`` skip in IDLE."""
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    original = (
        "## PR-001: first\n- Status: DOING\n\n"
        "## PR-002: second\n- Status: TODO\n"
    )
    queue_path.write_text(original)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="first", status=TaskStatus.DOING
    )
    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_origin_queue_md_tracked",
        lambda self: True,
    )

    runner._mark_queue_done()

    assert queue_path.read_text() == original


def test_mark_queue_done_skips_when_tracking_probe_indeterminate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """If the tracked-QUEUE probe itself failed (``None``), ``_mark_queue_done``
    must skip the in-place rewrite. Conservatively treating ``None`` as
    "tracked" protects legacy repos with a transiently flaky probe from
    a dirtied working tree on every merge; post-PR-181 repos lose only
    one in-place tweak and the next IDLE cycle regenerates QUEUE.md."""
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    original = (
        "## PR-001: first\n- Status: DOING\n\n"
        "## PR-002: second\n- Status: TODO\n"
    )
    queue_path.write_text(original)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="first", status=TaskStatus.DOING
    )
    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_origin_queue_md_tracked",
        lambda self: None,
    )

    runner._mark_queue_done()

    assert queue_path.read_text() == original


def test_mark_queue_done_returns_without_current_task() -> None:
    runner = _make_runner()
    runner._mark_queue_done()

    assert runner.state.pending_queue_sync_branch is None
    assert runner.state.pending_queue_sync_started_at is None


def test_mark_queue_done_no_op_when_queue_missing(tmp_path: Path) -> None:
    """A missing local QUEUE.md is a no-op — nothing to update."""
    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="first", status=TaskStatus.DOING
    )

    runner._mark_queue_done()

    assert runner.state.pending_queue_sync_branch is None


def test_mark_queue_done_no_op_when_pr_id_not_in_queue(tmp_path: Path) -> None:
    """If the merged ``pr_id`` is absent from the local QUEUE.md the
    file is left untouched — ``mark_task_done`` returns ``None`` and
    the helper exits without writing."""
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    original = "## PR-999: other\n- Status: TODO\n"
    queue_path.write_text(original)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="first", status=TaskStatus.DOING
    )

    runner._mark_queue_done()

    assert queue_path.read_text() == original


def test_mark_queue_done_logs_warning_on_read_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    queue_path.write_text("## PR-001: first\n- Status: DOING\n")

    def boom(self: Path, *args: Any, **kwargs: Any) -> str:
        raise OSError("read denied")

    monkeypatch.setattr(Path, "read_text", boom)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="first", status=TaskStatus.DOING
    )
    events: list[str] = []
    monkeypatch.setattr(runner, "log_event", events.append)

    runner._mark_queue_done()

    assert any("read QUEUE.md to mark PR-001 DONE failed" in e for e in events)


def test_mark_queue_done_logs_warning_on_write_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    queue_path.write_text("## PR-001: first\n- Status: DOING\n")

    def boom(self: Path, *args: Any, **kwargs: Any) -> int:
        raise OSError("write denied")

    monkeypatch.setattr(Path, "write_text", boom)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="first", status=TaskStatus.DOING
    )
    events: list[str] = []
    monkeypatch.setattr(runner, "log_event", events.append)

    runner._mark_queue_done()

    assert any("write QUEUE.md to mark PR-001 DONE failed" in e for e in events)


def test_resolve_pending_queue_sync_returns_true_without_branch() -> None:
    runner = _make_runner()

    assert asyncio.run(runner._resolve_pending_queue_sync()) is True


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

    async def fake_escalate(branch: str) -> None:
        escalations.append(branch)

    monkeypatch.setattr(
        runner,
        "_escalate_queue_sync_if_expired",
        fake_escalate,
    )

    assert asyncio.run(runner._resolve_pending_queue_sync()) is False
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

    assert asyncio.run(runner._resolve_pending_queue_sync()) is True
    assert runner.state.pending_queue_sync_branch is None
    assert runner.state.pending_queue_sync_started_at is None
    assert events == ["[MERGE] Queue-sync PR merged (queue-done-pr-001)."]


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

    assert asyncio.run(runner._resolve_pending_queue_sync()) is False
    assert runner.state.pending_queue_sync_branch is None
    assert runner.state.pending_queue_sync_started_at is None
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == (
        "queue-sync PR queue-done-pr-001 closed without merging"
    )
    assert events == [f"[MERGE] {runner.state.error_message}."]


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

    async def fake_escalate(branch: str) -> None:
        escalations.append(branch)

    monkeypatch.setattr(
        runner,
        "_escalate_queue_sync_if_expired",
        fake_escalate,
    )

    assert asyncio.run(runner._resolve_pending_queue_sync()) is False
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

    async def fake_escalate(branch: str) -> None:
        escalations.append(branch)

    monkeypatch.setattr(
        runner,
        "_escalate_queue_sync_if_expired",
        fake_escalate,
    )

    assert asyncio.run(runner._resolve_pending_queue_sync()) is False
    assert events == [
        "[MERGE] queue-sync PR queue-done-pr-001 view failed: gh unavailable."
    ]
    assert escalations == ["queue-done-pr-001"]


def test_escalate_queue_sync_no_op_when_started_at_missing() -> None:
    runner = _make_runner()
    runner.state.pending_queue_sync_branch = "queue-done-pr-001"

    asyncio.run(runner._escalate_queue_sync_if_expired("queue-done-pr-001"))

    assert runner.state.pending_queue_sync_branch == "queue-done-pr-001"
    assert runner.state.pending_queue_sync_started_at is None
    assert runner.state.state == PipelineState.IDLE


def test_escalate_queue_sync_no_op_when_not_expired() -> None:
    runner = _make_runner()
    runner.state.pending_queue_sync_branch = "queue-done-pr-001"
    runner.state.pending_queue_sync_started_at = (
        datetime.now(timezone.utc) - timedelta(minutes=5)
    )

    asyncio.run(runner._escalate_queue_sync_if_expired("queue-done-pr-001"))

    assert runner.state.pending_queue_sync_branch == "queue-done-pr-001"
    assert runner.state.pending_queue_sync_started_at is not None
    assert runner.state.state == PipelineState.IDLE


def test_publish_state_writes_to_redis() -> None:
    runner = _make_runner()
    asyncio.run(runner.publish_state())

    assert isinstance(runner.redis, _FakeRedis)
    assert len(runner.redis.writes) == 1
    key, payload = runner.redis.writes[0]
    assert key == f"pipeline:{runner.name}"
    assert runner.name in payload


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
        # Simulate the post-PR-181 untracked state: ``git cat-file -e
        # origin/main:tasks/QUEUE.md`` reports the file is missing from
        # origin so the helper takes the local-write branch.
        if cmd[1:3] == ["cat-file", "-e"]:
            return _FakeCompletedProcess(args=cmd, returncode=1)
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(git_ops_module.subprocess, "run", fake_run)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)
    runner._write_generated_queue_md(headers, statuses)

    assert all(call[1] == "cat-file" for call in git_calls), git_calls
    assert queue_path.read_text(encoding="utf-8") == runner._generate_queue_md(
        headers,
        statuses,
    )


def test_write_generated_queue_md_writes_disk_only(tmp_path: Path) -> None:
    """PR-181: ``_write_generated_queue_md`` writes the regenerated
    QUEUE.md to disk for read-side consumers and never commits or
    pushes it (the file is gitignored)."""
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    queue_path.write_text(
        "# Task Queue\n\n## PR-000: Existing\n", encoding="utf-8"
    )
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

    published = runner._write_generated_queue_md(headers, statuses)

    assert published is True
    expected = runner._generate_queue_md(headers, statuses)
    assert queue_path.read_text(encoding="utf-8") == expected
    assert runner._idle_generated_queue_needs_resync is False


def test_write_generated_queue_md_no_op_when_content_unchanged(
    tmp_path: Path,
) -> None:
    """If the regenerated queue matches the on-disk content, the
    helper short-circuits without rewriting the file."""
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

    runner = _make_runner()
    runner.repo_path = str(tmp_path)

    rendered = runner._generate_queue_md(headers, statuses)
    queue_path.write_text(rendered, encoding="utf-8")
    mtime_before = queue_path.stat().st_mtime_ns

    published = runner._write_generated_queue_md(headers, statuses)

    assert published is True
    assert queue_path.stat().st_mtime_ns == mtime_before


def test_write_generated_queue_md_creates_tasks_dir_if_missing(
    tmp_path: Path,
) -> None:
    """Fresh repos may not have ``tasks/`` yet — the helper must create
    the parent directory before writing the queue file."""
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

    published = runner._write_generated_queue_md(headers, statuses)

    assert published is True
    assert (tmp_path / "tasks" / "QUEUE.md").exists()


def test_write_generated_queue_md_skips_when_tracked_on_origin(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Legacy repos (pre-PR-181) keep ``tasks/QUEUE.md`` tracked on
    ``origin/{branch}``. ``.gitignore`` does not retroactively untrack
    files, so a write here would dirty the working tree on every IDLE
    cycle, push preflight into ERROR, and block dispatch. The helper
    must detect the tracked snapshot and skip the write entirely.
    """
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    legacy_text = "# legacy on disk\n"
    queue_path.write_text(legacy_text, encoding="utf-8")
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
        if cmd[1:3] == ["cat-file", "-e"]:
            return _FakeCompletedProcess(args=cmd, returncode=0)
        return _FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(git_ops_module.subprocess, "run", fake_run)

    runner = _make_runner()
    runner.repo_path = str(tmp_path)

    published = runner._write_generated_queue_md(headers, statuses)

    assert published is True
    # The tracked file on disk is left alone — no dirty modification.
    assert queue_path.read_text(encoding="utf-8") == legacy_text
    # Only the cat-file probe runs; no further git plumbing is invoked.
    assert git_calls == [
        ["git", "cat-file", "-e", "origin/main:tasks/QUEUE.md"]
    ]
    assert any(
        "Skipping QUEUE.md regeneration" in entry["event"]
        and "tracked on origin/main" in entry["event"]
        for entry in runner.state.history
    )

    # Re-running on the same legacy repo must not log the warning a
    # second time (one-shot guard via ``_legacy_tracked_queue_md_logged``).
    history_count_before = len(runner.state.history)
    runner._write_generated_queue_md(headers, statuses)
    new_logs = [
        entry
        for entry in runner.state.history[history_count_before:]
        if "Skipping QUEUE.md regeneration" in entry["event"]
    ]
    assert new_logs == []


def test_write_generated_queue_md_skips_when_probe_indeterminate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """When the tracked-QUEUE probe itself is indeterminate (timeout /
    OSError reports ``None``), ``_write_generated_queue_md`` skips the
    write conservatively. Treating ``None`` as "not tracked" would
    let a legacy repo's working tree be dirtied on every IDLE tick
    while the probe was flaky; treating it as "tracked" only loses one
    cycle of regeneration on post-PR-181 repos and self-heals next
    tick. The legacy-tracked log line must NOT fire — it would mislead
    operators into untracking a file that's actually fine."""
    queue_dir = tmp_path / "tasks"
    queue_dir.mkdir()
    queue_path = queue_dir / "QUEUE.md"
    existing = "# existing on disk\n"
    queue_path.write_text(existing, encoding="utf-8")
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
        runner_module.PipelineRunner,
        "_origin_queue_md_tracked",
        lambda self: None,
    )

    published = runner._write_generated_queue_md(headers, statuses)

    assert published is True
    # The on-disk file is left alone — no rewrite, no dirty tree.
    assert queue_path.read_text(encoding="utf-8") == existing
    # The "tracked on origin" log line must NOT fire under indeterminate
    # probe results — that message tells operators to untrack the file.
    assert not any(
        "Skipping QUEUE.md regeneration" in entry["event"]
        for entry in runner.state.history
    )


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
    (staging / "PR-001.md").write_text("- PR-001")

    manifest = json.dumps({"files": ["PR-001.md"], "staging_dir": str(staging)})
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
    (staging / "PR-001.md").write_text("- PR-001")
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir(exist_ok=True)

    old_manifest = json.dumps({"files": ["PR-001.md"], "staging_dir": str(staging)})
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
    # QUEUE.md is gitignored (PR-181) and must NOT be staged or copied
    # to the working tree from an upload, otherwise ``git add`` would
    # abort the whole batch and block subsequent dispatches.
    assert not (tmp_path / "tasks" / "QUEUE.md").exists()
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
    assert events and events[0].startswith("[INFRA] LABEL: [truncated]")
    # `[INFRA] ` (8 chars) is added on top of the existing 207 bound, plus
    # a trailing period.
    assert len(events[0]) <= 207 + len("[INFRA] ") + 1


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
    assert events == ["[FIX] idle timeout (5s since last push), killing."]


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
    assert "[FIX] [codex] pushed, resetting idle timer." in events


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
        "[FIX] GitHub API poll failed, preserving deadline.",
        "[FIX] idle timeout (30s since last push), killing.",
    ]


def test_run_coder_with_polling_returns_none_when_pr_number_is_zero() -> None:
    """No PR is in flight yet, so the polling task must not be spawned."""
    runner = _make_runner()
    target = asyncio.new_event_loop().create_future()
    try:
        result = runner._run_coder_with_polling(0, target, {"state": None})
    finally:
        target.cancel()
    assert result is None


def test_poll_github_during_fix_logs_and_continues_on_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``gh pr view`` failures must be logged and not crash the loop."""
    runner = _make_runner()
    runner._app_config = _app_cfg(fix_poll_interval_sec=1)
    events: list[str] = []
    monkeypatch.setattr(runner, "log_event", events.append)

    sequence: list[Any] = [RuntimeError("rate-limit"), None]

    def fake_pr_state(repo: str, number: int) -> dict[str, str | None] | None:
        result = sequence.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)
        if len(sleeps) >= len(sequence) + 2:
            raise asyncio.CancelledError

    async def fake_to_thread(func: Any, *args: object, **kwargs: object) -> Any:
        return func(*args, **kwargs)

    monkeypatch.setattr(fix_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(fix_module.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(fix_module.github_client, "pr_state", fake_pr_state)

    async def run_loop() -> None:
        loop = asyncio.get_running_loop()
        target_fut: asyncio.Future[None] = loop.create_future()

        async def hold() -> None:
            await target_fut

        target = asyncio.create_task(hold())
        with contextlib.suppress(asyncio.CancelledError):
            await runner._poll_github_during_fix(11, target, {"state": None})
        target.cancel()
        with contextlib.suppress(BaseException):
            await target

    asyncio.run(run_loop())

    assert any("GitHub poll for PR #11 failed: rate-limit" in e for e in events)
    assert any("returned no data" in e for e in events)


def test_poll_github_during_fix_continues_when_pr_remains_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An ``OPEN`` payload must not trigger termination; the loop continues."""
    runner = _make_runner()
    runner._app_config = _app_cfg(fix_poll_interval_sec=1)

    sequence: list[dict[str, str | None]] = [
        {"state": "OPEN", "mergedAt": None, "closedAt": None},
        {"state": "MERGED", "mergedAt": "now", "closedAt": None},
    ]

    def fake_pr_state(repo: str, number: int) -> dict[str, str | None]:
        return sequence.pop(0)

    terminated: list[bool] = []

    async def fake_terminate() -> None:
        terminated.append(True)

    monkeypatch.setattr(runner, "_terminate_current_coder", fake_terminate)

    async def fake_sleep(delay: float) -> None:
        return None

    async def fake_to_thread(func: Any, *args: object, **kwargs: object) -> Any:
        return func(*args, **kwargs)

    monkeypatch.setattr(fix_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(fix_module.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(fix_module.github_client, "pr_state", fake_pr_state)

    async def run_loop() -> dict[str, str | None]:
        loop = asyncio.get_running_loop()
        target_fut: asyncio.Future[None] = loop.create_future()

        async def hold() -> None:
            await target_fut

        target = asyncio.create_task(hold())
        flag: dict[str, str | None] = {"state": None}
        await runner._poll_github_during_fix(7, target, flag)
        with contextlib.suppress(BaseException):
            await target
        return flag

    flag = asyncio.run(run_loop())
    assert flag == {"state": "MERGED"}
    assert terminated == [True]


def test_poll_github_during_fix_terminates_coder_on_external_merge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On detected MERGED, the loop must terminate the coder and cancel target."""
    runner = _make_runner()
    runner._app_config = _app_cfg(fix_poll_interval_sec=1)
    events: list[str] = []
    monkeypatch.setattr(runner, "log_event", events.append)
    terminated = []

    async def fake_terminate() -> None:
        terminated.append(True)

    monkeypatch.setattr(runner, "_terminate_current_coder", fake_terminate)

    def fake_pr_state(repo: str, number: int) -> dict[str, str | None]:
        return {"state": "merged", "mergedAt": "now", "closedAt": None}

    async def fake_sleep(delay: float) -> None:
        return None

    async def fake_to_thread(func: Any, *args: object, **kwargs: object) -> Any:
        return func(*args, **kwargs)

    monkeypatch.setattr(fix_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(fix_module.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(fix_module.github_client, "pr_state", fake_pr_state)

    async def run_loop() -> tuple[dict[str, str | None], asyncio.Task[None]]:
        loop = asyncio.get_running_loop()
        target_fut: asyncio.Future[None] = loop.create_future()

        async def hold() -> None:
            await target_fut

        target = asyncio.create_task(hold())
        flag: dict[str, str | None] = {"state": None}
        await runner._poll_github_during_fix(99, target, flag)
        with contextlib.suppress(BaseException):
            await target
        return flag, target

    flag, target = asyncio.run(run_loop())

    assert flag == {"state": "MERGED"}
    assert terminated == [True]
    assert target.cancelled() is True
    assert any(
        "PR #99 reached terminal state MERGED during FIX" in e
        for e in events
    )


def test_handle_external_terminal_pr_state_merged_resets_counters_and_marks_done(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Detected external merge must reset counters and clean up bookkeeping."""
    runner = _make_runner()
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

    queue_done_calls = {"count": 0}

    def fake_mark_done(self: object) -> None:
        queue_done_calls["count"] += 1

    monkeypatch.setattr(runner_module.PipelineRunner, "_mark_queue_done", fake_mark_done)

    asyncio.run(runner._handle_external_terminal_pr_state("MERGED"))

    assert queue_done_calls["count"] == 1
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None
    assert runner.state.error_message is None
    assert any(
        "PR #42 merged externally during FIX, returning to IDLE." in entry["event"]
        for entry in runner.state.history
    )


def test_handle_external_terminal_pr_state_swallows_mark_queue_done_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A queue-sync failure on external merge must not block the IDLE
    transition and must log a warning, but the
    ``pending_queue_sync_branch`` guard MUST be preserved so a stale
    ``DOING`` task is not redispatched on the next idle cycle —
    ``_resolve_pending_queue_sync`` owns the retry/timeout from
    ``handle_idle`` (Codex P2 round-2 + P1 round-3 on PR #223)."""
    runner = _make_runner()
    runner.state.current_pr = PRInfo(number=44, branch="pr-044")
    sync_started_at = datetime.now(timezone.utc)

    def fake_mark_done(self: object) -> None:
        # Simulate _mark_queue_done's eager marker-then-fragile-op shape:
        # set the marker, then raise to model a fetch/checkout failure
        # mid-way through the sync.
        self.state.pending_queue_sync_branch = "queue-done-pr-044"
        self.state.pending_queue_sync_started_at = sync_started_at
        raise RuntimeError("queue mutation failed")

    monkeypatch.setattr(runner_module.PipelineRunner, "_mark_queue_done", fake_mark_done)

    asyncio.run(runner._handle_external_terminal_pr_state("MERGED"))

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    # Marker preserved so handle_idle gates dispatch via
    # _resolve_pending_queue_sync rather than redispatching the stale
    # DOING task as new work.
    assert runner.state.pending_queue_sync_branch == "queue-done-pr-044"
    assert runner.state.pending_queue_sync_started_at == sync_started_at
    assert any(
        "_mark_queue_done failed during external-merge cleanup" in entry["event"]
        for entry in runner.state.history
    )


def test_handle_external_terminal_pr_state_logs_without_pr_number(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Even with no current_pr (race with cleanup), MERGED still moves to IDLE."""
    runner = _make_runner()
    runner.state.current_pr = None

    monkeypatch.setattr(
        runner_module.PipelineRunner, "_mark_queue_done", lambda self: None
    )
    asyncio.run(runner._handle_external_terminal_pr_state("MERGED"))
    assert runner.state.state == PipelineState.IDLE
    assert any(
        "merged externally during FIX" in entry["event"]
        for entry in runner.state.history
    )


def test_handle_external_terminal_pr_state_closed_logs_without_pr_number() -> None:
    """No-pr CLOSED race must still transition to HUNG with a generic log."""
    runner = _make_runner()
    runner.state.current_pr = None
    asyncio.run(runner._handle_external_terminal_pr_state("CLOSED"))
    assert runner.state.state == PipelineState.HUNG
    assert any(
        "closed externally during FIX" in entry["event"]
        for entry in runner.state.history
    )


def test_handle_external_terminal_pr_state_merged_saves_success_merged_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """External MERGED in FIX must finalize the run record as ``success_merged``
    so dashboard / metrics views don't see it stuck on ``coding_complete``
    (Codex P2 on PR #223)."""
    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-091",
        title="external merge",
        status=TaskStatus.DOING,
        branch="pr-091",
        task_file="tasks/PR-091.md",
    )
    runner.state.current_pr = PRInfo(number=91, branch="pr-091")
    runner._start_current_run_record("claude", "opus")

    monkeypatch.setattr(
        runner_module.PipelineRunner, "_mark_queue_done", lambda self: None
    )
    monkeypatch.setattr(
        runner, "_compute_diff_stats", lambda base_branch: {}
    )

    saved: list[str] = []
    original_save = runner._save_current_run_record

    async def spy_save(exit_reason: str, **kwargs: object) -> None:
        saved.append(exit_reason)
        await original_save(exit_reason, **kwargs)

    runner._save_current_run_record = spy_save  # type: ignore[assignment]

    asyncio.run(runner._handle_external_terminal_pr_state("MERGED"))

    assert saved == ["success_merged"]
    assert runner._current_run_record is None
    assert runner.state.state == PipelineState.IDLE


def test_terminate_current_coder_uses_configured_grace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``daemon.coder_terminate_grace_sec`` must drive the SIGTERM-to-SIGKILL
    grace (Codex P3 on PR #223). Operators must be able to tune the grace
    via config rather than the value being hard-coded."""
    runner = _make_runner()
    runner._app_config = _app_cfg(coder_terminate_grace_sec=42)

    captured_timeouts: list[float] = []

    class _Proc:
        returncode = None

        def terminate(self) -> None:
            return None

        async def wait(self) -> None:
            return None

    runner._current_coder_process = _Proc()

    original_wait_for = asyncio.wait_for

    async def fake_wait_for(coro: object, timeout: float) -> object:
        captured_timeouts.append(timeout)
        return await original_wait_for(coro, timeout)

    monkeypatch.setattr(
        runner_module.asyncio, "wait_for", fake_wait_for
    )

    asyncio.run(runner._terminate_current_coder())

    assert captured_timeouts == [42]


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


@pytest.mark.parametrize(
    "msg,expected",
    [
        ("git fetch origin main failed after 3 attempts", True),
        ("Could not connect to github.com", True),
        ("Failed to connect to api.github.com", True),
        ("ensure_repo_cloned failed", True),
        (
            "git push origin HEAD:foo failed after 3 attempts: connection reset",
            True,
        ),
        ("git clone failed after 2 attempts: i/o timeout", True),
        ("gh api repos/x/y/commits/z/check-runs failed after 3 attempts", True),
        # Network-symptom strings count as infra only when paired with an
        # explicit git/GitHub reference in the surrounding context.
        (
            "fatal: unable to access 'https://github.com/o/r': Connection timed out",
            True,
        ),
        ("git fetch origin: dial tcp 1.2.3.4:443: i/o timeout", True),
        ("gh: network is unreachable while contacting api.github.com", True),
        ("gh: failed to run git: dial tcp 140.82.112.4:443: i/o timeout", True),
        # ``gh: failed to ...`` without a network symptom must NOT short-circuit
        # diagnose_error: the same prefix is emitted for auth failures and
        # workflow rejections that need real FIX/ESCALATE routing.
        ("gh: failed to run git", False),
        ("get_open_prs failed: gh: failed to authenticate to github.com", False),
        (
            "merge_pr failed: gh: failed to run git: "
            "not possible to fast-forward, you may want to integrate first",
            False,
        ),
        # Push rejections, branch-protection denials, auth/policy errors must
        # NOT be classified as infra so diagnose_error can route FIX/ESCALATE.
        ("git push origin HEAD:foo rejected (non-fast-forward)", False),
        ("remote: Branch protection rule prevents push", False),
        ("git push: 403 forbidden", False),
        # Generic retry strings without a git/gh operation prefix must not
        # trigger infra bypass — they may come from API/validation/workflow
        # retry loops that need real diagnosis.
        ("failed after 7 attempts", False),
        ("pipeline step failed after 5 attempts", False),
        # Bare network-symptom strings without git/GitHub context are NOT
        # infra: they can come from app or test failures (e.g. database,
        # Redis, third-party API clients) that need real diagnosis.
        ("Connection timed out", False),
        ("network is unreachable", False),
        ("dial tcp 1.2.3.4:443: i/o timeout", False),
        ("Failed to connect to database", False),
        ("Could not connect to redis at localhost:6379", False),
        ("pytest: 3 failed in test_x.py", False),
        ("ImportError: cannot import name 'foo'", False),
        ("API rate limit exceeded", False),
        ("", False),
    ],
)
def test_is_infra_error_classifies_messages(msg: str, expected: bool) -> None:
    """_is_infra_error classifies known infra strings, ignores everything else."""
    from src.daemon.runner import _is_infra_error

    assert _is_infra_error(msg) is expected


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


# ---------------------------------------------------------------------------
# PR-050: HEAD SHA verification after FIX
# ---------------------------------------------------------------------------


def _patch_eyes_reaction_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stub the EYES-skip pre-push gate to fire (fresh EYES after push)."""
    monkeypatch.setattr(
        runner_module.github_client,
        "_get_codex_issue_reactions",
        lambda repo, number: [
            {
                "content": "eyes",
                "user": {"login": "chatgpt-codex-connector[bot]"},
                "created_at": "2026-04-30T12:30:00Z",
            }
        ],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_pr_last_push_time",
        lambda repo, number: runner_module.github_client._parse_iso(
            "2026-04-30T12:00:00Z"
        ),
    )


def _patch_eyes_reaction_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stub a stale EYES reaction (predates push) — gate must NOT skip."""
    monkeypatch.setattr(
        runner_module.github_client,
        "_get_codex_issue_reactions",
        lambda repo, number: [
            {
                "content": "eyes",
                "user": {"login": "chatgpt-codex-connector[bot]"},
                "created_at": "2026-04-30T11:00:00Z",
            }
        ],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_pr_last_push_time",
        lambda repo, number: runner_module.github_client._parse_iso(
            "2026-04-30T12:00:00Z"
        ),
    )


# ── PAUSED state tests ──────────────────────────────────────────────


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
        entry["event"] == "[INFRA] Paused. Press Play to resume."
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


def test_proactive_check_logs_degradation_at_10_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch)
    runner = _make_runner()
    runner._claude_usage_provider = _FakeUsageProvider(snapshot=None, failures=10)

    result = asyncio.run(runner._proactive_usage_check())
    assert result is True
    assert any("degraded" in e.get("event", "").lower() for e in runner.state.history)


# ---- In-flight breach monitor tests ----


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


def test_event_log_includes_coder_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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


def test_publish_state_ignores_invalid_persisted_state_during_transaction() -> None:
    runner = _make_runner()
    runner.redis.store[f"pipeline:{runner.name}"] = "{not-json"

    asyncio.run(runner.publish_state())

    stored = RepoState.model_validate_json(runner.redis.store[f"pipeline:{runner.name}"])
    assert stored.name == runner.name


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


# ---- GitHub API rate-limit-aware polling (PR-163) -----------------------


def _far_future_reset(seconds: int = 1800) -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=seconds)


def _budget(remaining: int, limit: int = 5000, reset_at: datetime | None = None):
    from src.daemon.github_rate_limit import RateLimitBudget

    return RateLimitBudget(
        installation_id=None,
        remaining=remaining,
        limit=limit,
        reset_at=reset_at or _far_future_reset(),
    )


def _set_budget(runner: PipelineRunner, budget: object) -> None:
    """Pre-populate the runner's budget cache so refresh becomes a no-op."""
    runner._github_api_budget_cache = budget  # type: ignore[assignment]
    runner._github_api_budget_last_fetched = datetime.now(timezone.utc)


def test_check_budget_returns_true_when_no_observation() -> None:
    runner = _make_runner()
    assert asyncio.run(runner._check_github_api_budget()) is True


def test_check_budget_slowdown_window_passed_means_proceed() -> None:
    runner = _make_runner()
    runner.app_config.daemon.github_api_pause_threshold_percent = 5
    runner.app_config.daemon.github_api_slowdown_threshold_percent = 20
    runner.app_config.daemon.github_api_slowdown_multiplier = 5
    # Above the pause threshold but below slowdown, with reset already elapsed:
    # the snapshot is stale so neither throttle branch should fire.
    _set_budget(
        runner,
        _budget(
            remaining=500,
            limit=5000,
            reset_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        ),
    )
    runner._github_api_slowdown_attempts = 3
    runner._github_api_slowdown_cycle = 2

    proceed = asyncio.run(runner._check_github_api_budget())

    assert proceed is True
    assert runner._github_api_slowdown_attempts == 0
    assert runner._github_api_slowdown_cycle == 0


def test_check_budget_slowdown_runs_one_in_n_cycles() -> None:
    runner = _make_runner()
    runner.app_config.daemon.github_api_pause_threshold_percent = 5
    runner.app_config.daemon.github_api_slowdown_threshold_percent = 20
    runner.app_config.daemon.github_api_slowdown_multiplier = 5
    _set_budget(runner, _budget(remaining=500, limit=5000))  # 10%

    decisions = [
        asyncio.run(runner._check_github_api_budget()) for _ in range(11)
    ]

    # cycles 0, 5, 10 proceed; everything else skipped
    assert decisions[0] is True
    assert decisions[5] is True
    assert decisions[10] is True
    assert decisions[1:5] == [False, False, False, False]
    slowdown_logs = [
        e for e in runner.state.history if "GitHub API budget low" in e["event"]
    ]
    assert len(slowdown_logs) == 1


def test_check_budget_no_skip_when_extended_idle_active() -> None:
    """Extended-idle cadence already absorbs the slowdown; do not skip cycles.

    With both slowdowns active, real cycles must space at
    ``max(extended, base * multiplier)`` — not their product. The
    extended-idle interval already handles the spacing, so the
    budget check proceeds every cycle in this branch.
    """
    runner = _make_runner(poll_interval_sec=60)
    runner.app_config.daemon.github_api_pause_threshold_percent = 5
    runner.app_config.daemon.github_api_slowdown_threshold_percent = 20
    runner.app_config.daemon.github_api_slowdown_multiplier = 5
    runner.app_config.daemon.idle_extended_after_cycles = 3
    runner.app_config.daemon.idle_extended_poll_interval_sec = 300
    _set_budget(runner, _budget(remaining=500, limit=5000))  # 10%

    runner._idle_streak = 5  # past idle_extended_after_cycles

    decisions = [
        asyncio.run(runner._check_github_api_budget()) for _ in range(6)
    ]

    assert decisions == [True] * 6
    assert runner._github_api_slowdown_cycle == 0
    assert runner._github_api_slowdown_attempts == 6


def test_check_budget_slowdown_resets_on_recovery() -> None:
    runner = _make_runner()
    runner.app_config.daemon.github_api_slowdown_threshold_percent = 20

    _set_budget(runner, _budget(remaining=500, limit=5000))
    asyncio.run(runner._check_github_api_budget())
    asyncio.run(runner._check_github_api_budget())
    assert runner._github_api_slowdown_attempts == 2

    _set_budget(runner, _budget(remaining=4500, limit=5000))
    proceed = asyncio.run(runner._check_github_api_budget())

    assert proceed is True
    assert runner._github_api_slowdown_attempts == 0
    assert runner._github_api_slowdown_cycle == 0


def test_check_budget_normal_proceeds_without_changes() -> None:
    runner = _make_runner()
    _set_budget(runner, _budget(remaining=4500, limit=5000))

    proceed = asyncio.run(runner._check_github_api_budget())

    assert proceed is True
    assert runner._github_api_pause_attempts == 0
    assert runner._github_api_slowdown_attempts == 0


def test_check_budget_zero_multiplier_falls_back_to_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    runner.app_config.daemon.github_api_slowdown_threshold_percent = 20
    # bypass pydantic validator to exercise the max(1, ...) guard
    object.__setattr__(
        runner.app_config.daemon, "github_api_slowdown_multiplier", 0
    )
    _set_budget(runner, _budget(remaining=500, limit=5000))

    proceed = asyncio.run(runner._check_github_api_budget())

    # multiplier coerced to 1 means every cycle in slowdown still proceeds.
    assert proceed is True


def test_refresh_github_api_budget_fetches_and_persists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    fetched = _budget(remaining=4321, limit=5000)
    monkeypatch.setattr(
        runner_module.github_client,
        "fetch_rate_limit_buckets",
        lambda: (fetched, None),
    )

    result = asyncio.run(runner._refresh_github_api_budget())

    assert result is fetched
    assert runner._github_api_budget_cache is fetched
    from src.daemon.github_rate_limit import BUDGET_REDIS_KEY

    assert BUDGET_REDIS_KEY in runner.redis.store


def test_refresh_github_api_budget_persists_per_bucket_snapshots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """REST and GraphQL buckets land in their own Redis keys for the dashboard."""
    from src.daemon.github_rate_limit import (
        BUDGET_GRAPHQL_REDIS_KEY,
        BUDGET_REDIS_KEY,
        BUDGET_REST_REDIS_KEY,
        RateLimitBudget,
    )

    runner = _make_runner()
    rest = _budget(remaining=4321, limit=5000)
    graphql = _budget(remaining=120, limit=5000)
    monkeypatch.setattr(
        runner_module.github_client,
        "fetch_rate_limit_buckets",
        lambda: (rest, graphql),
    )

    result = asyncio.run(runner._refresh_github_api_budget())

    # Constrained min surfaces as the cached/legacy budget, but each bucket
    # is also persisted under its own key so the dashboard can render them
    # individually rather than collapsing both into a single bar.
    assert result is not None and result.remaining == graphql.remaining
    stored_rest = RateLimitBudget.from_redis_payload(
        runner.redis.store[BUDGET_REST_REDIS_KEY]
    )
    stored_graphql = RateLimitBudget.from_redis_payload(
        runner.redis.store[BUDGET_GRAPHQL_REDIS_KEY]
    )
    assert stored_rest is not None and stored_rest.remaining == rest.remaining
    assert stored_graphql is not None and stored_graphql.remaining == graphql.remaining
    assert BUDGET_REDIS_KEY in runner.redis.store


def test_refresh_github_api_budget_clears_missing_bucket_snapshots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A partial probe (one bucket ``None``) drops the prior bucket snapshot.

    Otherwise the dashboard keeps reading a stale per-bucket key and renders
    the missing surface as healthy/warn/critical instead of the intended
    neutral "no data" state during transient ``gh api rate_limit`` failures.
    """
    from src.daemon.github_rate_limit import (
        BUDGET_GRAPHQL_REDIS_KEY,
        BUDGET_REST_REDIS_KEY,
    )

    runner = _make_runner()
    # Pre-populate both buckets to simulate a prior healthy snapshot.
    runner.redis.store[BUDGET_REST_REDIS_KEY] = _budget(
        remaining=4500
    ).to_redis_payload()
    runner.redis.store[BUDGET_GRAPHQL_REDIS_KEY] = _budget(
        remaining=4500
    ).to_redis_payload()

    rest = _budget(remaining=4321, limit=5000)
    monkeypatch.setattr(
        runner_module.github_client,
        "fetch_rate_limit_buckets",
        lambda: (rest, None),
    )

    asyncio.run(runner._refresh_github_api_budget())

    # REST snapshot is refreshed; GraphQL is dropped so the dashboard renders
    # neutral instead of the stale value.
    assert BUDGET_REST_REDIS_KEY in runner.redis.store
    assert BUDGET_GRAPHQL_REDIS_KEY not in runner.redis.store


def test_refresh_github_api_budget_uses_cache_within_ttl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    cached = _budget(remaining=999)
    runner._github_api_budget_cache = cached
    runner._github_api_budget_last_fetched = datetime.now(timezone.utc)

    calls = {"count": 0}

    def _fetch() -> object:
        calls["count"] += 1
        return _budget(remaining=1), None

    monkeypatch.setattr(
        runner_module.github_client, "fetch_rate_limit_buckets", _fetch
    )

    result = asyncio.run(runner._refresh_github_api_budget())

    assert result is cached
    assert calls["count"] == 0


def test_run_cycle_short_circuits_when_budget_critical(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_subprocess(monkeypatch, stdout="")
    runner = _make_runner()
    runner._scaffolded = True
    runner._recovered = True

    monkeypatch.setattr(
        runner_module.github_client,
        "fetch_rate_limit_buckets",
        lambda: (_budget(remaining=1, limit=5000), None),
    )

    asyncio.run(runner.run_cycle())

    # Critical-budget path skips preflight/state machine and just publishes.
    assert runner.redis.writes, "publish_state should still run on early-return"
    assert any(
        "GitHub API budget critical" in e["event"] for e in runner.state.history
    )


def test_refresh_github_api_budget_picks_up_sibling_update_within_ttl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Within the local TTL, a sibling's freshly published snapshot must win.

    The local TTL only guards repeated ``gh api rate_limit`` probes from the
    same runner; otherwise a multi-repo deployment can keep using a stale
    "healthy" local cache for up to 60s after a sibling has already published
    a "critical" update, exactly when the protection should engage.
    """
    from src.daemon.github_rate_limit import BUDGET_REDIS_KEY

    runner = _make_runner()
    stale_local = _budget(remaining=4500, limit=5000)
    runner._github_api_budget_cache = stale_local
    runner._github_api_budget_last_fetched = datetime.now(timezone.utc)

    fresh_shared = _budget(remaining=120, limit=5000)
    runner.redis.store[BUDGET_REDIS_KEY] = fresh_shared.to_redis_payload()

    fetch_calls = {"count": 0}

    def _fetch() -> object:
        fetch_calls["count"] += 1  # pragma: no cover - probe must not be invoked
        return _budget(remaining=1), None

    monkeypatch.setattr(
        runner_module.github_client, "fetch_rate_limit_buckets", _fetch
    )

    result = asyncio.run(runner._refresh_github_api_budget())

    assert fetch_calls["count"] == 0
    assert result is not None
    assert result.remaining == fresh_shared.remaining
    assert runner._github_api_budget_cache is not None
    assert runner._github_api_budget_cache.remaining == fresh_shared.remaining


def test_refresh_github_api_budget_keeps_cache_when_fetch_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    cached = _budget(remaining=999)
    runner._github_api_budget_cache = cached
    runner._github_api_budget_last_fetched = (
        datetime.now(timezone.utc) - timedelta(seconds=120)
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "fetch_rate_limit_buckets",
        lambda: (None, None),
    )

    result = asyncio.run(runner._refresh_github_api_budget())

    assert result is cached


def test_refresh_github_api_budget_releases_lock_when_probe_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lock holder whose ``gh api`` call fails must free the lock for a sibling.

    Otherwise every runner is blocked from probing for the full TTL window
    while ``read_budget`` returns ``None``, silently disabling rate-limit
    protection during exactly the conditions it was added to cover.
    """
    from src.daemon.github_rate_limit import REFRESH_LOCK_REDIS_KEY

    runner = _make_runner()
    monkeypatch.setattr(
        runner_module.github_client,
        "fetch_rate_limit_buckets",
        lambda: (None, None),
    )

    result = asyncio.run(runner._refresh_github_api_budget())

    assert result is None
    assert REFRESH_LOCK_REDIS_KEY not in runner.redis.store
    assert REFRESH_LOCK_REDIS_KEY in runner.redis.deleted


def test_refresh_github_api_budget_skips_fetch_when_lock_held(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Other runners reuse the persisted observation instead of re-probing."""
    from src.daemon.github_rate_limit import (
        BUDGET_REDIS_KEY,
        REFRESH_LOCK_REDIS_KEY,
    )

    runner = _make_runner()
    # Simulate another runner holding the lock and having published a budget.
    persisted = _budget(remaining=2500, limit=5000)
    runner.redis.store[REFRESH_LOCK_REDIS_KEY] = "1"
    runner.redis.store[BUDGET_REDIS_KEY] = persisted.to_redis_payload()

    fetch_calls = {"count": 0}

    def _fetch() -> object:
        fetch_calls["count"] += 1
        return _budget(remaining=1), None

    monkeypatch.setattr(
        runner_module.github_client, "fetch_rate_limit_buckets", _fetch
    )

    result = asyncio.run(runner._refresh_github_api_budget())

    assert fetch_calls["count"] == 0
    assert result is not None
    assert result.remaining == persisted.remaining
    # Cache mirrors the shared observation so subsequent local-TTL hits work.
    assert runner._github_api_budget_cache is not None
    assert runner._github_api_budget_cache.remaining == persisted.remaining


def test_refresh_github_api_budget_lock_serializes_concurrent_runners(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two runners sharing one Redis trigger exactly one ``gh api`` probe."""
    shared_redis = _FakeRedis()
    runner_a = _make_runner()
    runner_b = _make_runner()
    runner_a.redis = shared_redis  # type: ignore[assignment]
    runner_b.redis = shared_redis  # type: ignore[assignment]

    fetched = _budget(remaining=4000, limit=5000)
    fetch_calls = {"count": 0}

    def _fetch() -> object:
        fetch_calls["count"] += 1
        return fetched, None

    monkeypatch.setattr(
        runner_module.github_client, "fetch_rate_limit_buckets", _fetch
    )

    result_a = asyncio.run(runner_a._refresh_github_api_budget())
    result_b = asyncio.run(runner_b._refresh_github_api_budget())

    assert fetch_calls["count"] == 1
    assert result_a is fetched
    assert result_b is not None
    assert result_b.remaining == fetched.remaining


def test_refresh_github_api_budget_falls_back_to_local_cache_when_shared_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lock held but no shared observation yet — keep the previous local value."""
    from src.daemon.github_rate_limit import REFRESH_LOCK_REDIS_KEY

    runner = _make_runner()
    cached = _budget(remaining=777)
    runner._github_api_budget_cache = cached
    runner.redis.store[REFRESH_LOCK_REDIS_KEY] = "1"

    monkeypatch.setattr(
        runner_module.github_client,
        "fetch_rate_limit_buckets",
        lambda: (_budget(remaining=1), None),
    )

    result = asyncio.run(runner._refresh_github_api_budget())

    assert result is cached


def test_refresh_github_api_budget_keeps_ttl_unset_when_no_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent-startup race: another runner holds the lock but has not yet
    written a snapshot, and this runner has no local cache. The TTL must not
    advance — otherwise budget protection silently disengages for 60s while
    ``_check_github_api_budget()`` keeps short-circuiting to ``True``.
    """
    from src.daemon.github_rate_limit import REFRESH_LOCK_REDIS_KEY

    runner = _make_runner()
    runner.redis.store[REFRESH_LOCK_REDIS_KEY] = "1"

    monkeypatch.setattr(
        runner_module.github_client,
        "fetch_rate_limit_buckets",
        lambda: (None, None),  # pragma: no cover - lock held, fetch never invoked
    )

    result = asyncio.run(runner._refresh_github_api_budget())

    assert result is None
    assert runner._github_api_budget_last_fetched is None


def test_refresh_github_api_budget_advances_ttl_on_shared_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Once another runner publishes a snapshot, the next refresh picks it up
    and advances the TTL so the local guard kicks in normally.
    """
    from src.daemon.github_rate_limit import (
        BUDGET_REDIS_KEY,
        REFRESH_LOCK_REDIS_KEY,
    )

    runner = _make_runner()
    persisted = _budget(remaining=2500, limit=5000)
    runner.redis.store[REFRESH_LOCK_REDIS_KEY] = "1"
    runner.redis.store[BUDGET_REDIS_KEY] = persisted.to_redis_payload()

    result = asyncio.run(runner._refresh_github_api_budget())

    assert result is not None
    assert result.remaining == persisted.remaining
    assert runner._github_api_budget_last_fetched is not None


def test_run_cycle_records_graphql_burn_when_budget_drops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A drop in remaining between before/after observations is captured.

    The burn-record wrapper observes the shared snapshot before the cycle
    body runs and again after, so a sibling-published refresh that lowers
    the remaining count surfaces as a non-zero per-cycle delta.
    """
    from src.daemon.github_rate_limit import (
        BUDGET_REDIS_KEY,
        BURNS_REDIS_KEY_PREFIX,
    )

    runner = _make_runner()
    runner._scaffolded = True
    runner._recovered = True
    runner.redis.store[BUDGET_REDIS_KEY] = _budget(remaining=4500).to_redis_payload()

    async def fake_run_cycle_body() -> None:
        # Simulate API consumption observed via a sibling refresh mid-cycle.
        runner.redis.store[BUDGET_REDIS_KEY] = _budget(
            remaining=4480
        ).to_redis_payload()

    monkeypatch.setattr(runner, "_run_cycle_body", fake_run_cycle_body)

    asyncio.run(runner.run_cycle())

    bucket = runner.redis.lists.get(f"{BURNS_REDIS_KEY_PREFIX}{runner.name}")
    assert bucket == ["20"]


def test_run_cycle_records_zero_burn_when_window_resets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A negative delta (rate-limit window reset) clamps to zero, never -N."""
    from src.daemon.github_rate_limit import (
        BUDGET_REDIS_KEY,
        BURNS_REDIS_KEY_PREFIX,
    )

    runner = _make_runner()
    runner._scaffolded = True
    runner._recovered = True
    runner.redis.store[BUDGET_REDIS_KEY] = _budget(remaining=120).to_redis_payload()

    async def fake_run_cycle_body() -> None:
        runner.redis.store[BUDGET_REDIS_KEY] = _budget(
            remaining=5000
        ).to_redis_payload()

    monkeypatch.setattr(runner, "_run_cycle_body", fake_run_cycle_body)

    asyncio.run(runner.run_cycle())

    bucket = runner.redis.lists.get(f"{BURNS_REDIS_KEY_PREFIX}{runner.name}")
    assert bucket == ["0"]


def test_run_cycle_records_zero_burn_when_no_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No budget snapshot anywhere means we record zero rather than crashing."""
    from src.daemon.github_rate_limit import BURNS_REDIS_KEY_PREFIX

    runner = _make_runner()
    runner._scaffolded = True
    runner._recovered = True

    async def fake_run_cycle_body() -> None:
        return None

    monkeypatch.setattr(runner, "_run_cycle_body", fake_run_cycle_body)

    asyncio.run(runner.run_cycle())

    bucket = runner.redis.lists.get(f"{BURNS_REDIS_KEY_PREFIX}{runner.name}")
    assert bucket == ["0"]


def test_run_cycle_records_burn_even_when_body_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Burn recording lives in a finally block so transient body errors do not
    silently suppress observability for the whole next polling cycle."""
    from src.daemon.github_rate_limit import (
        BUDGET_REDIS_KEY,
        BURNS_REDIS_KEY_PREFIX,
    )

    runner = _make_runner()
    runner._scaffolded = True
    runner._recovered = True
    runner.redis.store[BUDGET_REDIS_KEY] = _budget(remaining=4500).to_redis_payload()

    async def fake_run_cycle_body() -> None:
        raise RuntimeError("body failed mid-cycle")

    monkeypatch.setattr(runner, "_run_cycle_body", fake_run_cycle_body)

    with pytest.raises(RuntimeError):
        asyncio.run(runner.run_cycle())

    bucket = runner.redis.lists.get(f"{BURNS_REDIS_KEY_PREFIX}{runner.name}")
    assert bucket == ["0"]


def test_run_cycle_swallows_burn_recording_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recording failures must never propagate; observability is best-effort."""
    runner = _make_runner()
    runner._scaffolded = True
    runner._recovered = True

    async def fake_run_cycle_body() -> None:
        return None

    async def boom_recorder(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("redis exploded")

    monkeypatch.setattr(runner, "_run_cycle_body", fake_run_cycle_body)
    monkeypatch.setattr(runner_module, "record_cycle_burn", boom_recorder)

    # Must not raise even though the recorder itself is broken.
    asyncio.run(runner.run_cycle())


# ---------------------------------------------------------------------------
# PR-184: adaptive IDLE polling
# ---------------------------------------------------------------------------


def test_effective_idle_poll_interval_uses_base_below_threshold() -> None:
    """First two consecutive IDLE cycles still poll at the base interval."""
    runner = _make_runner(poll_interval_sec=60)
    runner.app_config.daemon.idle_extended_after_cycles = 3
    runner.app_config.daemon.idle_extended_poll_interval_sec = 300

    runner._idle_streak = 1
    assert runner.effective_idle_poll_interval == 60
    runner._idle_streak = 2
    assert runner.effective_idle_poll_interval == 60


def test_effective_idle_poll_interval_uses_extended_at_threshold() -> None:
    """Third+ consecutive IDLE cycle drops to the extended interval."""
    runner = _make_runner(poll_interval_sec=60)
    runner.app_config.daemon.idle_extended_after_cycles = 3
    runner.app_config.daemon.idle_extended_poll_interval_sec = 300

    runner._idle_streak = 3
    assert runner.effective_idle_poll_interval == 300
    runner._idle_streak = 50
    assert runner.effective_idle_poll_interval == 300


def test_effective_idle_poll_interval_takes_larger_of_two_slowdowns() -> None:
    """Rate-limit slowdown is folded in as ``max(extended, base*multiplier)``.

    Below the IDLE-streak threshold the slowdown does not affect the
    interval — the budget-check skip logic still throttles work to one
    in ``multiplier`` cycles. At/above the threshold the property
    returns the larger of the extended interval and ``base*multiplier``
    so the two slowdowns do not compound (the budget check then
    proceeds every cycle on the extended cadence, see
    ``test_check_budget_no_skip_when_extended_idle_active``).
    """
    runner = _make_runner(poll_interval_sec=60)
    runner.app_config.daemon.idle_extended_after_cycles = 3
    runner.app_config.daemon.idle_extended_poll_interval_sec = 200
    runner.app_config.daemon.github_api_slowdown_multiplier = 5

    runner._idle_streak = 0
    runner._github_api_slowdown_attempts = 2
    assert runner.effective_idle_poll_interval == 60

    # extended=200 < base*multiplier=300 → take the larger (300).
    runner._idle_streak = 3
    runner._github_api_slowdown_attempts = 2
    assert runner.effective_idle_poll_interval == 300

    # extended=400 > base*multiplier=300 → stay at extended.
    runner.app_config.daemon.idle_extended_poll_interval_sec = 400
    assert runner.effective_idle_poll_interval == 400

    # No slowdown active: extended interval applies as-is.
    runner._github_api_slowdown_attempts = 0
    runner.app_config.daemon.idle_extended_poll_interval_sec = 200
    assert runner.effective_idle_poll_interval == 200


def test_update_idle_streak_increments_on_idle_with_no_pr() -> None:
    """The streak grows by one each cycle that ends in IDLE with no PR."""
    runner = _make_runner()
    runner.state.state = PipelineState.IDLE
    runner.state.current_pr = None

    for expected in range(1, 6):
        runner._update_idle_streak_after_cycle(PipelineState.IDLE)
        assert runner._idle_streak == expected


def test_update_idle_streak_resets_when_state_leaves_idle() -> None:
    """Transitioning out of IDLE clears the streak so polling stays fast."""
    runner = _make_runner()
    runner._idle_streak = 5
    runner.state.state = PipelineState.WATCH

    runner._update_idle_streak_after_cycle(PipelineState.IDLE)
    assert runner._idle_streak == 0


def test_update_idle_streak_resets_when_cycle_started_outside_idle() -> None:
    """A cycle that began in an active state and ended in IDLE resets the streak."""
    runner = _make_runner()
    runner._idle_streak = 5
    runner.state.state = PipelineState.IDLE
    runner.state.current_pr = None

    for active in (
        PipelineState.WATCH,
        PipelineState.FIX,
        PipelineState.MERGE,
        PipelineState.CODING,
    ):
        runner._idle_streak = 5
        runner._update_idle_streak_after_cycle(active)
        assert runner._idle_streak == 0


def test_update_idle_streak_resets_when_idle_attaches_open_pr() -> None:
    """An IDLE-with-open-PR cycle is real work: reset the streak too."""
    runner = _make_runner()
    runner._idle_streak = 4
    runner.state.state = PipelineState.IDLE
    runner.state.current_pr = PRInfo(number=42, branch="pr-042")

    runner._update_idle_streak_after_cycle(PipelineState.IDLE)
    assert runner._idle_streak == 0


def test_update_idle_streak_caps_at_sane_ceiling() -> None:
    """``_idle_streak`` does not grow without bound across long uptimes."""
    runner = _make_runner()
    runner.state.state = PipelineState.IDLE
    runner.state.current_pr = None

    runner._idle_streak = runner_module._IDLE_STREAK_CAP
    runner._update_idle_streak_after_cycle(PipelineState.IDLE)
    assert runner._idle_streak == runner_module._IDLE_STREAK_CAP


def test_update_idle_streak_cap_respects_high_configured_threshold() -> None:
    """A configured threshold above the static cap must still be reachable."""
    runner = _make_runner()
    runner.app_config.daemon.idle_extended_after_cycles = 250
    runner.state.state = PipelineState.IDLE
    runner.state.current_pr = None

    runner._idle_streak = runner_module._IDLE_STREAK_CAP
    runner._update_idle_streak_after_cycle(PipelineState.IDLE)
    assert runner._idle_streak == runner_module._IDLE_STREAK_CAP + 1

    runner._idle_streak = 250
    runner._update_idle_streak_after_cycle(PipelineState.IDLE)
    assert runner._idle_streak == 250


def test_reset_idle_streak_clears_counter() -> None:
    """``reset_idle_streak`` is the wake-event entry point."""
    runner = _make_runner()
    runner._idle_streak = 7
    runner.reset_idle_streak()
    assert runner._idle_streak == 0


def test_run_cycle_grows_idle_streak_across_consecutive_idle_cycles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end: three IDLE-ending cycles flip to extended polling."""
    _patch_subprocess(monkeypatch)

    runner = _make_runner(poll_interval_sec=60)
    runner.app_config.daemon.idle_extended_after_cycles = 3
    runner.app_config.daemon.idle_extended_poll_interval_sec = 300
    runner._recovered = True
    runner._scaffolded = True

    async def fake_handle_idle() -> None:
        runner.state.state = PipelineState.IDLE
        runner.state.current_pr = None

    async def fake_ensure_repo_cloned() -> None:
        return None

    monkeypatch.setattr(runner, "handle_idle", fake_handle_idle)
    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "preflight", _preflight_true_stub)

    runner.state.state = PipelineState.IDLE
    intervals: list[int] = []
    for _ in range(4):
        asyncio.run(runner.run_cycle())
        intervals.append(runner.effective_idle_poll_interval)

    assert intervals == [60, 60, 300, 300]
    assert runner._idle_streak == 4


def test_run_cycle_resets_idle_streak_on_transition_into_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A WATCH→IDLE cycle must reset the streak, not increment it."""
    _patch_subprocess(monkeypatch)

    runner = _make_runner(poll_interval_sec=60)
    runner.app_config.daemon.idle_extended_after_cycles = 3
    runner.app_config.daemon.idle_extended_poll_interval_sec = 300
    runner._recovered = True
    runner._scaffolded = True
    runner._idle_streak = 2

    async def fake_handle_watch() -> None:
        runner.state.state = PipelineState.IDLE
        runner.state.current_pr = None

    async def fake_ensure_repo_cloned() -> None:
        return None

    monkeypatch.setattr(runner, "handle_watch", fake_handle_watch)
    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "preflight", _preflight_true_stub)

    runner.state.state = PipelineState.WATCH
    asyncio.run(runner.run_cycle())

    assert runner.state.state == PipelineState.IDLE
    assert runner._idle_streak == 0


def test_update_idle_streak_resets_when_pending_upload_deferred() -> None:
    """A cycle that deferred a pending upload must not grow the streak."""
    runner = _make_runner()
    runner.state.state = PipelineState.IDLE
    runner.state.current_pr = None
    runner._idle_streak = 2
    runner._idle_dispatch_deferred = True

    runner._update_idle_streak_after_cycle(PipelineState.IDLE)

    assert runner._idle_streak == 0
    assert runner._idle_dispatch_deferred is False


def test_update_idle_streak_clears_deferred_flag_after_consuming() -> None:
    """The deferred flag is one-shot: cleared regardless of streak path."""
    runner = _make_runner()
    runner.state.state = PipelineState.IDLE
    runner.state.current_pr = None
    runner._idle_dispatch_deferred = True
    runner._update_idle_streak_after_cycle(PipelineState.IDLE)
    assert runner._idle_dispatch_deferred is False

    runner._update_idle_streak_after_cycle(PipelineState.IDLE)
    assert runner._idle_streak == 1


def test_run_cycle_open_prs_failures_keep_polling_fast(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated GitHub read failures must not flip the runner to extended IDLE."""
    _patch_subprocess(monkeypatch)

    runner = _make_runner(poll_interval_sec=60)
    runner.app_config.daemon.idle_extended_after_cycles = 3
    runner.app_config.daemon.idle_extended_poll_interval_sec = 300
    runner._recovered = True
    runner._scaffolded = True

    async def fake_handle_idle() -> None:
        runner.state.state = PipelineState.IDLE
        runner.state.current_pr = None
        runner._idle_dispatch_deferred = True

    async def fake_ensure_repo_cloned() -> None:
        return None

    monkeypatch.setattr(runner, "handle_idle", fake_handle_idle)
    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "preflight", _preflight_true_stub)

    runner.state.state = PipelineState.IDLE
    intervals: list[int] = []
    for _ in range(5):
        asyncio.run(runner.run_cycle())
        intervals.append(runner.effective_idle_poll_interval)

    assert intervals == [60, 60, 60, 60, 60]
    assert runner._idle_streak == 0


def test_run_cycle_pending_upload_retries_keep_polling_fast(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pending-upload retries do not slow the runner into extended IDLE."""
    _patch_subprocess(monkeypatch)

    runner = _make_runner(poll_interval_sec=60)
    runner.app_config.daemon.idle_extended_after_cycles = 3
    runner.app_config.daemon.idle_extended_poll_interval_sec = 300
    runner._recovered = True
    runner._scaffolded = True

    async def fake_handle_idle() -> None:
        runner.state.state = PipelineState.IDLE
        runner.state.current_pr = None
        runner._idle_dispatch_deferred = True

    async def fake_ensure_repo_cloned() -> None:
        return None

    monkeypatch.setattr(runner, "handle_idle", fake_handle_idle)
    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "preflight", _preflight_true_stub)

    runner.state.state = PipelineState.IDLE
    intervals: list[int] = []
    for _ in range(5):
        asyncio.run(runner.run_cycle())
        intervals.append(runner.effective_idle_poll_interval)

    assert intervals == [60, 60, 60, 60, 60]
    assert runner._idle_streak == 0


def _codex_bot_pr(review: ReviewStatus = ReviewStatus.EYES) -> PRInfo:
    """PRInfo in a state where the WATCH cycle would otherwise hit the
    review-timeout branch (review is EYES/PENDING, not CHANGES_REQUESTED)."""
    return PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.PENDING,
        review_status=review,
        last_activity=datetime.now(timezone.utc),
    )


def _codex_bot_error_comment(
    body: str = "Something went wrong while reviewing this PR. Please try again.",
    *,
    user: str = "chatgpt-codex-connector[bot]",
    created_at: str | None = None,
) -> dict[str, Any]:
    return {
        "user": {"login": user},
        "body": body,
        "created_at": created_at or "2026-04-30T12:00:00Z",
    }


def test_repo_state_resets_codex_retrigger_on_pr_transition() -> None:
    state = RepoState(
        url="https://github.com/octo/demo",
        name="octo__demo",
        last_updated=datetime.now(timezone.utc),
    )
    state.current_pr = PRInfo(number=1, branch="pr-001")
    state.last_codex_retrigger_at = datetime.now(timezone.utc)

    state.current_pr = PRInfo(number=2, branch="pr-002")

    assert state.last_codex_retrigger_at is None


# ---------------------------------------------------------------------------
# PR-202: WATCH adaptive polling (slow-start, fast-tail)
# ---------------------------------------------------------------------------


def _configure_watch_adaptive_defaults(runner: PipelineRunner) -> None:
    runner.app_config.daemon.watch_slow_window_sec = 300
    runner.app_config.daemon.watch_slow_poll_interval_sec = 300
    runner.app_config.daemon.watch_fast_poll_interval_sec = 45


def test_effective_watch_poll_interval_is_slow_immediately_after_entry() -> None:
    """First WATCH cycle returns the slow interval (default 300s)."""
    runner = _make_runner(poll_interval_sec=60)
    _configure_watch_adaptive_defaults(runner)

    runner._watch_entered_at = datetime.now(timezone.utc)

    assert runner.effective_watch_poll_interval == 300


def test_effective_watch_poll_interval_still_slow_inside_window() -> None:
    """Four minutes after WATCH entry still uses the slow interval."""
    runner = _make_runner(poll_interval_sec=60)
    _configure_watch_adaptive_defaults(runner)

    runner._watch_entered_at = datetime.now(timezone.utc) - timedelta(minutes=4)

    assert runner.effective_watch_poll_interval == 300


def test_effective_watch_poll_interval_becomes_fast_past_window() -> None:
    """Past 5 min the slow window closes and polling drops to fast (45s)."""
    runner = _make_runner(poll_interval_sec=60)
    _configure_watch_adaptive_defaults(runner)

    runner._watch_entered_at = (
        datetime.now(timezone.utc) - timedelta(minutes=5, seconds=1)
    )

    assert runner.effective_watch_poll_interval == 45


def test_effective_watch_poll_interval_event_resets_slow_window() -> None:
    """A detected GitHub event re-anchors the slow window from event time."""
    runner = _make_runner(poll_interval_sec=60)
    _configure_watch_adaptive_defaults(runner)

    # WATCH entered 6 minutes ago — past the slow window.
    runner._watch_entered_at = datetime.now(timezone.utc) - timedelta(minutes=6)
    assert runner.effective_watch_poll_interval == 45

    # Prime a baseline signature, then observe a different signature.
    baseline = PRInfo(number=42, branch="pr-042", ci_status=CIStatus.PENDING)
    runner._observe_watch_event_signature(baseline)
    changed = PRInfo(
        number=42,
        branch="pr-042",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
    )
    runner._observe_watch_event_signature(changed)

    assert runner._watch_last_event_at is not None
    # Event reset → new anchor is recent → back inside slow window.
    assert runner.effective_watch_poll_interval == 300


def test_watch_polling_anchors_cleared_on_transition_out_of_watch() -> None:
    """Leaving WATCH wipes the adaptive polling anchors for the next session."""
    runner = _make_runner(poll_interval_sec=60)
    _configure_watch_adaptive_defaults(runner)

    runner._watch_entered_at = datetime.now(timezone.utc)
    runner._watch_last_event_at = datetime.now(timezone.utc)
    runner._watch_last_event_signature = (42, CIStatus.PENDING, ReviewStatus.PENDING, None)

    runner._reset_watch_polling()

    assert runner._watch_entered_at is None
    assert runner._watch_last_event_at is None
    assert runner._watch_last_event_signature is None
    # No anchor → falls back to the static base interval.
    assert runner.effective_watch_poll_interval == 60


def test_run_cycle_resets_watch_anchors_when_state_leaves_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_run_cycle_body`` clears watch anchors when the cycle exits WATCH."""
    runner = _make_runner(poll_interval_sec=60)
    _configure_watch_adaptive_defaults(runner)
    runner._recovered = True
    runner._watch_entered_at = datetime.now(timezone.utc) - timedelta(minutes=2)
    runner._watch_last_event_at = datetime.now(timezone.utc) - timedelta(minutes=1)
    runner._watch_last_event_signature = (42, CIStatus.PENDING, ReviewStatus.PENDING, None)
    runner.state.state = PipelineState.WATCH

    async def _noop_cloned(self: PipelineRunner) -> None:
        return None

    async def _budget_ok(self: PipelineRunner) -> bool:
        return True

    async def _no_user_paused(self: PipelineRunner) -> None:
        self.state.user_paused = False

    async def _preflight_ok(self: PipelineRunner) -> bool:
        return True

    async def _watch_to_idle(self: PipelineRunner) -> None:
        self.state.state = PipelineState.IDLE

    async def _publish_state(self: PipelineRunner) -> None:
        return None

    monkeypatch.setattr(PipelineRunner, "ensure_repo_cloned", _noop_cloned)
    monkeypatch.setattr(PipelineRunner, "_check_github_api_budget", _budget_ok)
    monkeypatch.setattr(
        PipelineRunner, "_refresh_user_paused_from_redis", _no_user_paused
    )
    monkeypatch.setattr(PipelineRunner, "preflight", _preflight_ok)
    monkeypatch.setattr(PipelineRunner, "handle_watch", _watch_to_idle)
    monkeypatch.setattr(PipelineRunner, "publish_state", _publish_state)

    asyncio.run(runner._run_cycle_body())

    assert runner.state.state == PipelineState.IDLE
    assert runner._watch_entered_at is None
    assert runner._watch_last_event_at is None
    assert runner._watch_last_event_signature is None


def test_run_cycle_anchors_watch_entered_at_on_transition_into_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_run_cycle_body`` anchors ``_watch_entered_at`` at the transition.

    A handler that pushes the runner from a non-WATCH state into WATCH
    must leave the anchor set before ``_run_cycle_body`` returns so the
    daemon's *next* call to ``_runner_poll_interval`` already sees the
    slow cadence — otherwise the first interval after entry uses the
    fast base poll and the slow-start window is wasted.
    """
    runner = _make_runner(poll_interval_sec=60)
    _configure_watch_adaptive_defaults(runner)
    runner._recovered = True
    runner.state.state = PipelineState.IDLE
    assert runner._watch_entered_at is None

    async def _noop_cloned(self: PipelineRunner) -> None:
        return None

    async def _budget_ok(self: PipelineRunner) -> bool:
        return True

    async def _no_user_paused(self: PipelineRunner) -> None:
        self.state.user_paused = False

    async def _preflight_ok(self: PipelineRunner) -> bool:
        return True

    async def _idle_to_watch(self: PipelineRunner) -> None:
        self.state.state = PipelineState.WATCH

    async def _publish_state(self: PipelineRunner) -> None:
        return None

    monkeypatch.setattr(PipelineRunner, "ensure_repo_cloned", _noop_cloned)
    monkeypatch.setattr(PipelineRunner, "_check_github_api_budget", _budget_ok)
    monkeypatch.setattr(
        PipelineRunner, "_refresh_user_paused_from_redis", _no_user_paused
    )
    monkeypatch.setattr(PipelineRunner, "preflight", _preflight_ok)
    monkeypatch.setattr(PipelineRunner, "handle_idle", _idle_to_watch)
    monkeypatch.setattr(PipelineRunner, "publish_state", _publish_state)

    asyncio.run(runner._run_cycle_body())

    assert runner.state.state == PipelineState.WATCH
    assert runner._watch_entered_at is not None
    # The next poll interval — computed by the daemon main loop *after*
    # this cycle — already sees the slow cadence, not the base fallback.
    assert runner.effective_watch_poll_interval == 300


def test_check_budget_no_skip_when_state_is_watch() -> None:
    """WATCH cadence already absorbs the slowdown; do not skip cycles.

    ``effective_watch_poll_interval`` already takes
    ``max(target, base * multiplier)`` when the rate-limit slowdown is
    active. If ``_check_github_api_budget`` also skipped (multiplier-1)
    out of every multiplier cycles, the effective poll spacing would
    become ``effective_watch_poll_interval * multiplier`` and could
    delay merge/fix transitions by an hour or more when quota is low.
    """
    runner = _make_runner(poll_interval_sec=60)
    runner.app_config.daemon.github_api_pause_threshold_percent = 5
    runner.app_config.daemon.github_api_slowdown_threshold_percent = 20
    runner.app_config.daemon.github_api_slowdown_multiplier = 10
    _set_budget(runner, _budget(remaining=500, limit=5000))  # 10%

    runner.state.state = PipelineState.WATCH

    decisions = [
        asyncio.run(runner._check_github_api_budget()) for _ in range(5)
    ]

    assert decisions == [True] * 5
    assert runner._github_api_slowdown_cycle == 0
    assert runner._github_api_slowdown_attempts == 5


# ---------------------------------------------------------------------------
# PR-190: Asymmetric push verification on the normal FIX exit path
# ---------------------------------------------------------------------------


async def _pr190_no_idle_monitor_async(
    self: object,
    pr_number: int,
    idle_limit: int,
    target: asyncio.Task,  # type: ignore[type-arg]
    idle_flag: dict[str, bool],
) -> None:
    await asyncio.sleep(0)


async def _pr190_no_breach_monitor_async(
    self: object,
    breach_dir: str,
    run_id: str,
    claude_task: asyncio.Task,  # type: ignore[type-arg]
    breach_flag: dict[str, bool],
) -> None:
    await asyncio.sleep(0)


def test_verify_pushes_since_returns_false_when_remote_diverged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_verify_pushes_since`` returns ``False`` when the remote moved
    to a SHA that does not contain ``head_after`` (e.g. force-pushed
    over the FIX commit). This exercises the merge-base branch where
    the early-out shortcut against ``last_known_sha`` does not apply."""
    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        if args and args[0] == "fetch":
            return _FakeCompletedProcess(args=["git", *args], returncode=0)
        if args[:2] == ("rev-parse", "origin/pr-190"):
            return _FakeCompletedProcess(
                args=["git", *args], stdout="ddd444\n", returncode=0,
            )
        if args[:2] == ("merge-base", "--is-ancestor"):
            return _FakeCompletedProcess(args=["git", *args], returncode=1)
        return _FakeCompletedProcess(args=["git", *args], returncode=0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)

    runner = _make_runner()
    result = runner._verify_pushes_since(
        "pr-190", "aaa111", "bbb222", context="after FIX exit",
    )

    assert result is False


