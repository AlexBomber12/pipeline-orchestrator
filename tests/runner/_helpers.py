"""Shared helpers for tests/runner/ — extracted from tests/test_runner.py.

PR-224b: when ``tests/test_runner.py`` was split apart, the helpers used
across multiple test modules were promoted here so each thematic file in
``tests/runner/`` can import them without the package depending on the
old monolithic test file.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

import pytest
from src.coders import claude as claude_plugin_module
from src.config import AppConfig, DaemonConfig, RepoConfig
from src.daemon import git_ops as git_ops_module
from src.daemon import runner as runner_module
from src.daemon import selector as selector_module
from src.daemon.handlers import coding as coding_module
from src.daemon.handlers import error as error_module
from src.daemon.handlers import idle as idle_module
from src.daemon.runner import PipelineRunner
from src.github import gh_runner
from src.models import (
    CIStatus,
    PipelineState,
    PRInfo,
    ReviewStatus,
)

claude_cli = claude_plugin_module.claude_cli
_ORIGINAL_SELECT_NEXT_TASK_FROM_DAG = idle_module.IdleMixin._select_next_task_from_dag


def _stub_dag_select(
    monkeypatch: pytest.MonkeyPatch,
    *,
    task: object = None,
    tasks: list[object] | None = None,
) -> None:
    """Replace ``_select_next_task_from_dag`` with a stub that returns ``task``.

    Sets ``_idle_dag_tasks`` and ``_idle_dag_statuses`` on the runner so the
    surrounding handler can compute queue counters and snapshot the queue
    just like the real selector does. ``tasks`` defaults to ``[task]`` when
    omitted.
    """
    queue_tasks = list(tasks) if tasks is not None else ([task] if task else [])
    statuses = {t.pr_id: t.status for t in queue_tasks}

    async def fake_select(self):  # type: ignore[no-untyped-def]
        self._idle_dag_tasks = list(queue_tasks)
        self._idle_dag_statuses = dict(statuses)
        return task

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        fake_select,
    )


def _async_cli_result(*result: object):
    async def _fn(*args: object, **kwargs: object) -> tuple:
        return result

    return _fn


def _async_cli_result_with_side_effect(collector: list, label: str, *result: object):
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


class _FakeRedis:
    """Minimal async Redis double capturing ``set`` calls."""

    def __init__(self) -> None:
        self.writes: list[tuple[str, str]] = []
        self.store: dict[str, str] = {}
        self.ttls: dict[str, int] = {}
        self.deleted: list[str] = []
        self.lists: dict[str, list[str]] = {}
        self.sets: dict[str, set[str]] = {}
        self.zsets: dict[str, dict[str, float]] = {}

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
        if ex is not None:
            self.ttls[key] = ex
        return True

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def mget(self, keys: list[str]) -> list[str | None]:
        return [self.store.get(key) for key in keys]

    async def exists(self, key: str) -> int:
        return int(key in self.store)

    async def delete(self, key: str) -> int:
        self.deleted.append(key)
        if key in self.store:
            del self.store[key]
            return 1
        return 0

    async def zrem(self, key: str, *members: str) -> int:
        bucket = self.zsets.setdefault(key, {})
        removed = 0
        for member in members:
            if member in bucket:
                del bucket[member]
                removed += 1
        return removed

    async def zadd(self, key: str, mapping: dict[str, float]) -> int:
        bucket = self.zsets.setdefault(key, {})
        added = 0
        for member, score in mapping.items():
            if member not in bucket:
                added += 1
            bucket[member] = float(score)
        return added

    async def zcount(self, key: str, min_score: object, max_score: object) -> int:
        def _bound(value: object) -> tuple[float, bool]:
            if value == "-inf":
                return float("-inf"), False
            if value == "+inf":
                return float("inf"), False
            if isinstance(value, str) and value.startswith("("):
                return float(value[1:]), True
            return float(value), False

        lower, lower_exclusive = _bound(min_score)
        upper, upper_exclusive = _bound(max_score)
        return sum(
            1
            for score in self.zsets.get(key, {}).values()
            if (score > lower if lower_exclusive else score >= lower)
            and (score < upper if upper_exclusive else score <= upper)
        )

    async def zremrangebyscore(
        self, key: str, min_score: object, max_score: object
    ) -> int:
        def _bound(value: object) -> tuple[float, bool]:
            if value == "-inf":
                return float("-inf"), False
            if value == "+inf":
                return float("inf"), False
            if isinstance(value, str) and value.startswith("("):
                return float(value[1:]), True
            return float(value), False

        lower, lower_exclusive = _bound(min_score)
        upper, upper_exclusive = _bound(max_score)
        bucket = self.zsets.setdefault(key, {})
        doomed = [
            member
            for member, score in bucket.items()
            if (score > lower if lower_exclusive else score >= lower)
            and (score < upper if upper_exclusive else score <= upper)
        ]
        for member in doomed:
            del bucket[member]
        return len(doomed)

    async def getdel(self, key: str) -> str | None:
        self.deleted.append(key)
        return self.store.pop(key, None)

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
        return values[start : stop + 1]

    async def ltrim(self, key: str, start: int, stop: int) -> None:
        values = self.lists.get(key, [])
        if stop < 0:
            stop = len(values) + stop
        self.lists[key] = values[start : stop + 1]

    async def sadd(self, key: str, value: str) -> int:
        bucket = self.sets.setdefault(key, set())
        before = len(bucket)
        bucket.add(value)
        return len(bucket) - before

    async def smembers(self, key: str) -> set[str]:
        return set(self.sets.get(key, set()))

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
    runner._auth_status_cache_expires_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    return runner


def _allow_all_coder_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(selector_module, "_auth_failed", lambda *args, **kwargs: False)


def _patch_subprocess(
    monkeypatch: pytest.MonkeyPatch,
    stdout: str = "",
    returncode: int = 0,
    *,
    stub_auto_pr_read: bool = True,
) -> list[list[str]]:
    calls: list[list[str]] = []
    rev_parse_head_calls = {"n": 0}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(cmd)
        if cmd[:2] == ["git", "rev-list"]:
            return _FakeCompletedProcess(args=cmd, stdout="0\n", returncode=0)
        if cmd[:3] == ["git", "cat-file", "-e"]:
            return _FakeCompletedProcess(args=cmd, returncode=1)
        if cmd[:2] == ["git", "merge"] and len(cmd) > 2 and cmd[2].startswith("origin/"):
            return _FakeCompletedProcess(args=cmd, stdout="Already up to date.\n", returncode=0)
        if cmd[:3] == ["git", "rev-parse", "HEAD"]:
            rev_parse_head_calls["n"] += 1
            sha = "head-before-abc" if rev_parse_head_calls["n"] == 1 else "head-after-def"
            return _FakeCompletedProcess(args=cmd, stdout=f"{sha}\n", returncode=0)
        if cmd[:2] == ["git", "rev-parse"] and len(cmd) >= 3 and cmd[2].startswith("origin/"):
            return _FakeCompletedProcess(args=cmd, stdout="head-after-def\n", returncode=0)
        return _FakeCompletedProcess(args=cmd, stdout=stdout, returncode=returncode)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    if stub_auto_pr_read:
        _stub_auto_pr_task_body_read(monkeypatch)
    return calls


def _stub_auto_pr_task_body_read(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub the AUTO PR ``Path(repo_path)/tasks/PR-*.md`` read in coding.py.

    The PR-271 dispatch switch reads the inline task body from disk before
    invoking the coder plugin. Tests in this suite point ``runner.repo_path``
    at a synthetic location that does not contain a real ``tasks/`` tree, so
    the read would always raise ``OSError`` and short-circuit the handler
    into the ERROR transition. Intercept ``Path.read_text`` for the AUTO PR
    file path only — and only when the file does not actually exist — so
    dispatch tests get a stable canned body while idle/recovery tests that
    populate real ``tasks/PR-*.md`` files in a ``tmp_path`` continue to
    read the on-disk content.
    """
    real_read_text = coding_module.Path.read_text
    real_read_bytes = coding_module.Path.read_bytes

    def fake_read_text(self: Any, *args: Any, **kwargs: Any) -> str:
        s = str(self)
        if (
            "/tasks/PR-" in s
            and s.endswith(".md")
            and not self.exists()
        ):
            return f"# {self.stem}\n\nBranch: pr-001\n"
        return real_read_text(self, *args, **kwargs)

    def fake_read_bytes(self: Any, *args: Any, **kwargs: Any) -> bytes:
        s = str(self)
        if (
            "/tasks/PR-" in s
            and s.endswith(".md")
            and not self.exists()
        ):
            return f"# {self.stem}\n\nBranch: pr-001\n".encode("utf-8")
        return real_read_bytes(self, *args, **kwargs)

    monkeypatch.setattr(coding_module.Path, "read_text", fake_read_text)
    monkeypatch.setattr(coding_module.Path, "read_bytes", fake_read_bytes)


async def _preflight_true_stub() -> bool:
    return True


async def _preflight_false_stub() -> bool:
    return False


def _preflight_recording_stub(sink: list[str], result: bool = True) -> Callable[[], Awaitable[bool]]:
    async def _stub() -> bool:
        sink.append("preflight")
        return result

    return _stub


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


def _patch_no_push_fix(
    monkeypatch: pytest.MonkeyPatch,
    head_seq: Callable[[], str],
) -> list[tuple[str, int, str]]:
    """Wire fake git/CLI/comment hooks for no-push FIX cycle tests."""
    posted: list[tuple[str, int, str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:3] == ["git", "rev-parse", "HEAD"]:
            return _FakeCompletedProcess(args=cmd, stdout=f"{head_seq()}\n", returncode=0)
        if cmd[:2] == ["git", "rev-parse"] and "--abbrev-ref" in cmd:
            return _FakeCompletedProcess(args=cmd, stdout="pr-218\n", returncode=0)
        if cmd[:2] == ["git", "rev-list"]:
            return _FakeCompletedProcess(args=cmd, stdout="0\n", returncode=0)
        return _FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(claude_cli, "fix_review_async", _async_cli_result(0, "", ""))
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *a, **kw: "",
    )
    return posted


def _patch_fix_with_stdout(
    monkeypatch: pytest.MonkeyPatch,
    *,
    stdout: str,
    code: int = 0,
    head_seq: Callable[[], str] | None = None,
) -> tuple[list[tuple[str, int, str]], list[list[str]]]:
    """Wire git/CLI/comment fakes for an ESCALATE-marker FIX cycle test."""
    seq = head_seq or (lambda: "abc123")
    posted: list[tuple[str, int, str]] = []
    gh_calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "rev-parse"] and "HEAD" in cmd:
            return _FakeCompletedProcess(args=cmd, stdout=f"{seq()}\n", returncode=0)
        if cmd[:2] == ["git", "rev-list"]:
            return _FakeCompletedProcess(args=cmd, stdout="0\n", returncode=0)
        return _FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    async def fake_fix(*args: object, **kwargs: object) -> tuple[int, str, str]:
        return (code, stdout, "")

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda cmd, **kwargs: gh_calls.append(cmd) or "",
    )
    return posted, gh_calls


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
        runner.state.current_pr = PRInfo(number=119, branch="fix/diagnose-error-commits-fixes")
    asyncio.run(runner.handle_error())
    return runner, calls, warnings, review_requests


def _populate_fully_scaffolded_repo(repo: Any) -> None:
    """Create every file ``_repo_looks_scaffolded`` checks for."""
    (repo / "AGENTS.md").write_text("# AGENTS\n")
    (repo / "CLAUDE.md").write_text("Read and follow AGENTS.md in this repository.\n")
    (repo / "tasks").mkdir()
    (repo / "tasks" / "QUEUE.md").write_text("# Task Queue\n")
    (repo / "scripts").mkdir()
    (repo / "scripts" / "ci.sh").write_text("#!/usr/bin/env bash\n")
    (repo / "scripts" / "make-review-artifacts.sh").write_text("#!/usr/bin/env bash\n")
    (repo / ".gitignore").write_text("artifacts/\ntasks/QUEUE.md\n")


def _patch_eyes_reaction_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stub the EYES-skip pre-push gate to fire (fresh EYES after push)."""
    monkeypatch.setattr(
        "src.github.reactions._get_codex_issue_reactions",
        lambda repo, number: [
            {
                "content": "eyes",
                "user": {"login": "chatgpt-codex-connector[bot]"},
                "created_at": "2026-04-30T12:30:00Z",
            }
        ],
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_last_push_time",
        lambda repo, number: gh_runner._parse_iso("2026-04-30T12:00:00Z"),
    )


def _patch_eyes_reaction_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stub a stale EYES reaction (predates push) — gate must NOT skip."""
    monkeypatch.setattr(
        "src.github.reactions._get_codex_issue_reactions",
        lambda repo, number: [
            {
                "content": "eyes",
                "user": {"login": "chatgpt-codex-connector[bot]"},
                "created_at": "2026-04-30T11:00:00Z",
            }
        ],
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_last_push_time",
        lambda repo, number: gh_runner._parse_iso("2026-04-30T12:00:00Z"),
    )


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


def _configure_watch_adaptive_defaults(runner: PipelineRunner) -> None:
    runner.app_config.daemon.watch_slow_window_sec = 300
    runner.app_config.daemon.watch_slow_poll_interval_sec = 300
    runner.app_config.daemon.watch_fast_poll_interval_sec = 45


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
