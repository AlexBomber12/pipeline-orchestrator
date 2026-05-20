"""PR-224a: Rate-limit, usage, breach, and pause-flow tests for src/daemon/runner.py

Mechanical move from tests/test_runner.py. Helpers and fixtures still live in
tests/test_runner.py and are referenced via the ``h`` alias.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from src import codex_cli
from src.coders import claude as claude_plugin_module
from src.config import CoderType, FeatureFlags
from src.daemon import fix_supervision as fix_supervision_module
from src.daemon import git_ops as git_ops_module
from src.daemon import runner as runner_module
from src.daemon import selector as selector_module
from src.daemon.handlers import breach as breach_module
from src.daemon.handlers import coding as coding_module
from src.daemon.handlers import idle as idle_module
from src.daemon.runner import ErrorCategory, PipelineRunner, _classify_error
from src.models import (
    CIStatus,
    PipelineState,
    PRInfo,
    QueueTask,
    RepoState,
    ReviewStatus,
    TaskStatus,
)

from tests.runner import _helpers as h

claude_cli = claude_plugin_module.claude_cli


@pytest.fixture(autouse=True)
def _default_no_merged_branch_api(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.task_status.gh_pr_get_merged_branches",
        lambda repo, branches: set(),
    )


def test_handle_idle_rereads_pause_flag_before_coding_transition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    task = QueueTask(
        pr_id="PR-042",
        title="Sample",
        status=TaskStatus.TODO,
        branch="pr-042-sample",
    )
    h._stub_dag_select(monkeypatch, task=task)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_merged_prs",
        lambda repo, branch, refresh=False: [],
    )

    runner = h._make_runner()
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
        entry["event"] == "[INFRA] Pause requested while preparing PR-042; deferring CODING."
        for entry in runner.state.history
    )


def test_handle_coding_honors_persisted_pause_after_fast_cli_exit(
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
        "finishing current run before honoring pause" in entry["event"].lower() for entry in runner.state.history
    )


def test_handle_coding_finishes_success_path_when_pause_persists_during_exit(
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
        lambda repo, **kw: [PRInfo(number=127, branch="pr-127-control-endpoints-backend")],
    )
    posted: list[tuple[str, int, str]] = []

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    monkeypatch.setattr("src.github.comments.post_comment", fake_post)

    runner = h._make_runner()
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
        "finishing current run before honoring pause" in entry["event"].lower() for entry in runner.state.history
    )


def test_handle_fix_breach_cancel_resets_no_push_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A breach pause via CancelledError breaks the no-push streak: a
    rate-limit interruption is not a no-push success, so the counter
    must reset (Codex P2 round 3 on PR #222)."""
    rev_parse_calls = {"count": 0}

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            sha = "aaa111\n" if rev_parse_calls["count"] == 1 else "bbb222\n"
            return h._FakeCompletedProcess(args=["git", *args], stdout=sha, returncode=0)
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

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
    monkeypatch.setattr(PipelineRunner, "_monitor_inflight_breach", breach_cancel_monitor)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=227, branch="pr-227", no_push_fix_count=2)
    monkeypatch.setattr(runner, "_rehydrate_last_push_at", lambda pr: None)
    monkeypatch.setattr(runner, "_post_codex_review", lambda pr_number: True)

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.no_push_fix_count == 0


def test_handle_fix_late_breach_resets_no_push_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A late breach (detected after FIX completed) also breaks the
    no-push streak — same rationale as the CancelledError breach path
    (Codex P2 round 3 on PR #222)."""
    rev_parse_calls = {"count": 0}

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            sha = "aaa111\n" if rev_parse_calls["count"] == 1 else "bbb222\n"
            return h._FakeCompletedProcess(args=["git", *args], stdout=sha, returncode=0)
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

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
    monkeypatch.setattr(claude_cli, "fix_review_async", h._async_cli_result(0, "ok", ""))
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", no_idle_monitor)
    monkeypatch.setattr(PipelineRunner, "_monitor_inflight_breach", no_breach_monitor)
    monkeypatch.setattr(PipelineRunner, "_check_late_breach", fake_late_breach)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=228, branch="pr-228", no_push_fix_count=2)
    monkeypatch.setattr(runner, "_rehydrate_last_push_at", lambda pr: None)
    monkeypatch.setattr(runner, "_post_codex_review", lambda pr_number: True)

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.no_push_fix_count == 0


def test_app_config_setter_and_usage_provider_swap() -> None:
    runner = h._make_runner()
    new_config = h._app_cfg(poll_interval_sec=120)
    claude_provider = h._FakeUsageProvider(snapshot={"name": "claude"})
    codex_provider = h._FakeUsageProvider(snapshot={"name": "codex"})

    runner.app_config = new_config
    runner.set_usage_providers(claude_provider, codex_provider)

    assert runner.app_config is new_config
    assert runner._claude_usage_provider is claude_provider
    assert runner._codex_usage_provider is codex_provider


def test_handle_merge_aborts_when_conflict_resolution_is_rate_limited(
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

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)

    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.WATCH
    assert ("merge", "--abort") in git_calls
    assert not claude_calls


def test_handle_merge_pauses_when_conflict_resolution_hits_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
        return (1, "", "Error: 429 Too Many Requests")

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

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.error_message is None
    assert runner.state.rate_limited_until is not None
    assert not merge_pr_calls, "merge_pr must not be called while paused"
    abort_cmds = [cmd for cmd in git_calls if cmd[:3] == ["git", "merge", "--abort"]]
    assert abort_cmds, "git merge --abort must be invoked"
    assert any("Rate limit pause active until" in e["event"] for e in runner.state.history)


def test_publish_state_preserves_concurrent_pause_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
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


def test_publish_state_copies_usage_snapshot_to_state() -> None:
    from src.usage import UsageSnapshot

    runner = h._make_runner()
    runner._claude_usage_provider = h._FakeUsageProvider(
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


def test_github_api_budget_paused_handles_no_cache() -> None:
    """A missing budget snapshot must not throttle the polling loop."""
    runner = h._make_runner()
    runner._github_api_budget_cache = None
    assert runner._github_api_budget_paused() is False


def test_github_api_budget_paused_when_remaining_below_threshold() -> None:
    """A critical cached budget must pause the polling iteration."""
    runner = h._make_runner()
    runner._github_api_budget_cache = runner_module.RateLimitBudget(
        installation_id=None,
        remaining=10,
        limit=5000,
        reset_at=datetime.now(timezone.utc) + timedelta(minutes=30),
    )
    runner._app_config = h._app_cfg(github_api_pause_threshold_percent=5)
    assert runner._github_api_budget_paused() is True


def test_github_api_budget_paused_returns_false_when_reset_elapsed() -> None:
    """A stale snapshot whose reset has passed must not block polling."""
    runner = h._make_runner()
    runner._github_api_budget_cache = runner_module.RateLimitBudget(
        installation_id=None,
        remaining=10,
        limit=5000,
        reset_at=datetime.now(timezone.utc) - timedelta(minutes=1),
    )
    runner._app_config = h._app_cfg(github_api_pause_threshold_percent=5)
    assert runner._github_api_budget_paused() is False


def test_github_api_budget_paused_returns_false_when_above_threshold() -> None:
    """A healthy budget must not throttle observability polling."""
    runner = h._make_runner()
    runner._github_api_budget_cache = runner_module.RateLimitBudget(
        installation_id=None,
        remaining=4_500,
        limit=5_000,
        reset_at=datetime.now(timezone.utc) + timedelta(minutes=30),
    )
    runner._app_config = h._app_cfg(github_api_pause_threshold_percent=5)
    assert runner._github_api_budget_paused() is False


def test_poll_github_during_fix_skips_iteration_when_budget_paused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the cached GH API budget is critical the loop must not call gh."""
    runner = h._make_runner()
    runner._app_config = h._app_cfg(fix_poll_interval_sec=1)
    runner._github_api_budget_cache = runner_module.RateLimitBudget(
        installation_id=None,
        remaining=1,
        limit=5_000,
        reset_at=datetime.now(timezone.utc) + timedelta(minutes=30),
    )

    pr_state_calls: list[tuple[str, int]] = []

    def fake_pr_state(repo: str, number: int) -> dict[str, str | None] | None:
        pr_state_calls.append((repo, number))
        return {"state": "MERGED", "mergedAt": "now", "closedAt": None}

    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)
        if len(sleeps) >= 2:
            raise asyncio.CancelledError

    monkeypatch.setattr(fix_supervision_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr("src.github.prs.pr_state", fake_pr_state)

    async def run_loop() -> None:
        loop = asyncio.get_running_loop()
        target_fut: asyncio.Future[None] = loop.create_future()

        async def hold() -> None:
            await target_fut

        target = asyncio.create_task(hold())
        with contextlib.suppress(asyncio.CancelledError):
            await runner._poll_github_during_fix(7, target, {"state": None})
        target.cancel()
        with contextlib.suppress(BaseException):
            await target

    asyncio.run(run_loop())

    assert pr_state_calls == []
    assert sleeps == [1, 1]


def test_check_rate_limit_blocks_when_limited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_check_rate_limit blocks when the proactive coder is still paused."""
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=10)
    runner.state.rate_limit_reactive_coder = "claude"

    assert asyncio.run(runner._check_rate_limit(proactive_coder="claude")) is False
    assert runner.state.rate_limited_until is not None


def test_check_rate_limit_allows_when_expired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_check_rate_limit returns True and clears when _rate_limited_until is past."""
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    runner.state.rate_limited_until = datetime.now(timezone.utc) - timedelta(minutes=1)

    assert asyncio.run(runner._check_rate_limit()) is True
    assert runner.state.rate_limited_until is None


def test_handle_coding_skips_when_rate_limited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """handle_coding returns early without calling claude_cli when rate-limited."""
    h._patch_subprocess(monkeypatch)
    cli_calls: list[str] = []
    monkeypatch.setattr(
        claude_cli,
        "run_auto_pr_async",
        h._async_cli_result_with_side_effect(cli_calls, "run_auto_pr_async", 0, "", ""),
    )
    runner = h._make_runner()
    runner.state.state = PipelineState.CODING
    runner.state.current_task = QueueTask(pr_id="PR-099", title="test", branch="pr-099-test", status=TaskStatus.TODO)
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=10)

    asyncio.run(runner.handle_coding())

    assert cli_calls == []


def test_handle_paused_resumes_to_error_when_error_message_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Window expired with error_message set -> ERROR so fault is retried."""
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner()
    runner.state.state = PipelineState.PAUSED
    runner.state.rate_limited_until = datetime.now(timezone.utc) - timedelta(minutes=1)
    runner.state.error_message = "Build failed: missing dependency X"
    runner.state.current_pr = PRInfo(number=50, branch="pr-050")
    runner.state.current_task = QueueTask(pr_id="PR-050", title="test", branch="pr-050", status=TaskStatus.DOING)

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "Build failed: missing dependency X"
    assert runner.state.rate_limited_until is None


def test_handle_paused_clears_legacy_rate_limit_error_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Window expired with rate-limit error_message -> clear msg, resume WATCH/IDLE (no deadlock)."""
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner()
    runner.state.state = PipelineState.PAUSED
    runner.state.rate_limited_until = datetime.now(timezone.utc) - timedelta(minutes=1)
    runner.state.error_message = "API rate limit exceeded (429)"
    runner.state.current_pr = PRInfo(number=50, branch="pr-050")
    runner.state.current_task = QueueTask(pr_id="PR-050", title="test", branch="pr-050", status=TaskStatus.DOING)

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.error_message is None
    assert any("cleared legacy rate-limit" in e["event"] for e in runner.state.history)


def test_handle_paused_resumes_with_other_coder_while_preserving_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner(coder=CoderType.CODEX)
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
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner(coder=CoderType.CODEX)
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
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner()
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
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner()
    runner.repo_config.feature_flags = FeatureFlags(
        use_unified_inhibitor_check=False
    )
    claude_provider = h._FakeUsageProvider(snapshot=None)
    codex_provider = h._FakeUsageProvider(snapshot=None)
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
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
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
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    runner.app_config.daemon.rate_limit_session_pause_percent = 80

    runner._detect_rate_limit("Warning: 75% of rate limit capacity used")
    assert runner.state.rate_limited_until is None

    runner._detect_rate_limit("Warning: 85% of rate limit capacity used")
    assert runner.state.rate_limited_until is not None


def test_detect_rate_limit_fixed_pause_duration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_detect_rate_limit always uses a fixed 30-minute cooldown."""
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
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
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    runner.app_config.daemon.rate_limit_weekly_pause_percent = 100

    runner._detect_rate_limit("Warning: 95% of your weekly rate limit reached")
    assert runner.state.rate_limited_until is None


def test_detect_rate_limit_weekly_triggers_at_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Weekly limit at 95% with weekly_threshold=90 should trigger pause."""
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    runner.app_config.daemon.rate_limit_weekly_pause_percent = 90

    runner._detect_rate_limit("Warning: 95% of your weekly rate limit reached")
    assert runner.state.rate_limited_until is not None


def test_detect_rate_limit_session_respects_session_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Session limit at 95% with session_threshold=95 should trigger pause."""
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    runner.app_config.daemon.rate_limit_session_pause_percent = 95

    runner._detect_rate_limit("Warning: 95% of your session rate limit reached")
    assert runner.state.rate_limited_until is not None


def test_detect_rate_limit_429_always_pauses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """HTTP 429 triggers pause regardless of thresholds."""
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    runner.app_config.daemon.rate_limit_session_pause_percent = 100
    runner.app_config.daemon.rate_limit_weekly_pause_percent = 100

    runner._detect_rate_limit("Error: HTTP 429 Too Many Requests")
    assert runner.state.rate_limited_until is not None


def test_detect_rate_limit_log_identifies_limit_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Event log must distinguish session vs weekly rate limits."""
    h._patch_subprocess(monkeypatch)

    runner1 = h._make_runner()
    runner1.app_config.daemon.rate_limit_session_pause_percent = 80
    runner1._detect_rate_limit("Warning: 90% of your session rate limit reached")
    assert any("(session)" in e["event"] for e in runner1.state.history)

    runner2 = h._make_runner()
    runner2.app_config.daemon.rate_limit_weekly_pause_percent = 80
    runner2._detect_rate_limit("Warning: 90% of your weekly rate limit reached")
    assert any("(weekly)" in e["event"] for e in runner2.state.history)


def test_handle_fix_sets_error_when_breach_cancel_review_post_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A breach-cancelled fix with a real HEAD change must surface review-post failure."""
    rev_parse_calls = {"count": 0}
    rehydrated: list[int] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            sha = "aaa111\n" if rev_parse_calls["count"] == 1 else "bbb222\n"
            return h._FakeCompletedProcess(args=["git", *args], stdout=sha, returncode=0)
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

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
    monkeypatch.setattr(PipelineRunner, "_monitor_inflight_breach", breach_cancel_monitor)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    monkeypatch.setattr(runner, "_rehydrate_last_push_at", lambda pr: rehydrated.append(pr.number))
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

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            if rev_parse_calls["count"] == 1:
                return h._FakeCompletedProcess(args=["git", *args], stdout="aaa111\n", returncode=0)
            raise RuntimeError("rev-parse lost HEAD")
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

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
    monkeypatch.setattr(PipelineRunner, "_monitor_inflight_breach", breach_cancel_monitor)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    monkeypatch.setattr(runner, "_rehydrate_last_push_at", lambda pr: rehydrated.append(pr.number))
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


def test_handle_fix_sets_error_when_late_breach_review_post_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A late breach with a changed HEAD must surface review-post failure."""
    rev_parse_calls = {"count": 0}
    rehydrated: list[int] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            sha = "aaa111\n" if rev_parse_calls["count"] == 1 else "bbb222\n"
            return h._FakeCompletedProcess(args=["git", *args], stdout=sha, returncode=0)
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

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
    monkeypatch.setattr(claude_cli, "fix_review_async", h._async_cli_result(0, "ok", ""))
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", no_idle_monitor)
    monkeypatch.setattr(PipelineRunner, "_monitor_inflight_breach", no_breach_monitor)
    monkeypatch.setattr(PipelineRunner, "_check_late_breach", fake_late_breach)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    monkeypatch.setattr(runner, "_rehydrate_last_push_at", lambda pr: rehydrated.append(pr.number))
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

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            if rev_parse_calls["count"] == 1:
                return h._FakeCompletedProcess(args=["git", *args], stdout="aaa111\n", returncode=0)
            raise RuntimeError("rev-parse lost HEAD")
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

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
    monkeypatch.setattr(claude_cli, "fix_review_async", h._async_cli_result(0, "ok", ""))
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", no_idle_monitor)
    monkeypatch.setattr(PipelineRunner, "_monitor_inflight_breach", no_breach_monitor)
    monkeypatch.setattr(PipelineRunner, "_check_late_breach", fake_late_breach)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    monkeypatch.setattr(runner, "_rehydrate_last_push_at", lambda pr: rehydrated.append(pr.number))
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


def test_handle_fix_breach_cancel_skips_codex_review_when_eyes_already_reacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OBS-Z: breach-cancel push path honors the EYES race-window dedup."""
    rev_parse_calls = {"count": 0}

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            sha = "aaa111\n" if rev_parse_calls["count"] == 1 else "bbb222\n"
            return h._FakeCompletedProcess(args=["git", *args], stdout=sha, returncode=0)
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

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
    monkeypatch.setattr(PipelineRunner, "_monitor_inflight_breach", breach_cancel_monitor)
    h._patch_eyes_reaction_present(monkeypatch)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    monkeypatch.setattr(runner, "_rehydrate_last_push_at", lambda pr: None)
    monkeypatch.setattr(
        runner,
        "_post_codex_review",
        lambda pr_number: pytest.fail("post must be skipped when EYES already present"),
    )

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.PAUSED
    assert any(
        "Codex auto-trigger detected, skipping duplicate @codex review post" in e["event"] for e in runner.state.history
    )


def test_handle_fix_late_breach_skips_codex_review_when_eyes_already_reacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OBS-Z: late-breach push path honors the EYES race-window dedup."""
    rev_parse_calls = {"count": 0}

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            sha = "aaa111\n" if rev_parse_calls["count"] == 1 else "bbb222\n"
            return h._FakeCompletedProcess(args=["git", *args], stdout=sha, returncode=0)
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

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
    monkeypatch.setattr(claude_cli, "fix_review_async", h._async_cli_result(0, "ok", ""))
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", no_idle_monitor)
    monkeypatch.setattr(PipelineRunner, "_monitor_inflight_breach", no_breach_monitor)
    monkeypatch.setattr(PipelineRunner, "_check_late_breach", fake_late_breach)
    h._patch_eyes_reaction_present(monkeypatch)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    monkeypatch.setattr(runner, "_rehydrate_last_push_at", lambda pr: None)
    monkeypatch.setattr(
        runner,
        "_post_codex_review",
        lambda pr_number: pytest.fail("post must be skipped when EYES already present"),
    )

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.PAUSED
    assert any(
        "Codex auto-trigger detected, skipping duplicate @codex review post" in e["event"] for e in runner.state.history
    )


def test_handle_fix_sets_error_on_non_rate_limit_cli_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-rate-limit CLI failures must surface as ERROR with stderr text."""

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        return h._FakeCompletedProcess(args=["git", *args], stdout="aaa111\n", returncode=0)

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
    monkeypatch.setattr(claude_cli, "fix_review_async", h._async_cli_result(7, "", "plain failure"))
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", no_idle_monitor)
    monkeypatch.setattr(PipelineRunner, "_monitor_inflight_breach", no_breach_monitor)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "plain failure"
    assert any("[claude] fix_review failed: plain failure" in e["event"] for e in runner.state.history)


def test_handle_coding_sets_paused_on_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CLI returns non-zero with rate limit stderr -> state = PAUSED, error_message = None."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_auto_pr_async",
        h._async_cli_result(1, "", "Error: 429 Too Many Requests"),
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.CODING
    runner.state.current_task = QueueTask(pr_id="PR-099", title="test", branch="pr-099-test", status=TaskStatus.TODO)

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
    runner = h._make_runner()
    runner.state.state = PipelineState.CODING
    runner.state.current_task = QueueTask(pr_id="PR-099", title="test", branch="pr-099-test", status=TaskStatus.TODO)

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
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "fix_review_async",
        h._async_cli_result(1, "", "Error: 429 Too Many Requests"),
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.FIX
    runner.state.current_pr = PRInfo(number=50, branch="pr-050")

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.error_message is None
    assert runner.state.rate_limited_until is not None


def test_handle_coding_reuses_selected_coder_for_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.state.state = PipelineState.CODING
    runner.state.current_task = QueueTask(pr_id="PR-099", title="test", branch="pr-099-test", status=TaskStatus.TODO)
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
    runner = h._make_runner()
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
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "run_auto_pr_async",
        h._async_cli_result(0, "ok", "Error: 429 Too Many Requests"),
    )
    pr = PRInfo(number=42, branch="pr-001")
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )

    runner = h._make_runner()
    runner._post_codex_review = lambda _pr_number: True  # type: ignore[method-assign]
    runner.state.state = PipelineState.CODING
    runner.state.current_task = QueueTask(pr_id="PR-001", title="test", branch="pr-001", status=TaskStatus.DOING)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.error_message is None
    assert runner.state.rate_limited_until is None


def test_handle_fix_success_ignores_rate_limit_text_in_stderr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Successful fix runs must not convert informational stderr into PAUSED."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "fix_review_async",
        h._async_cli_result(0, "ok", "Error: 429 Too Many Requests"),
    )

    runner = h._make_runner()
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
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner()
    runner.repo_config.feature_flags = FeatureFlags(
        use_unified_inhibitor_check=False
    )
    runner.state.state = PipelineState.PAUSED
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=20)

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.rate_limited_until is None
    assert "claude" in runner.state.rate_limited_coders


def test_handle_paused_clearable_rate_limit_error_resumes_to_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner()
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
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner(coder=CoderType.CODEX)
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
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner(coder=CoderType.CODEX)
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
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner()
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
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner()
    runner.state.state = PipelineState.PAUSED
    runner.state.rate_limited_until = datetime.now(timezone.utc) - timedelta(minutes=1)
    runner.state.current_pr = PRInfo(number=50, branch="pr-050")
    runner.state.current_task = QueueTask(pr_id="PR-050", title="test", branch="pr-050", status=TaskStatus.DOING)

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.rate_limited_until is None


def test_handle_paused_resumes_to_idle_when_no_active_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Window expired, no current_pr -> IDLE."""
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner()
    runner.state.state = PipelineState.PAUSED
    runner.state.rate_limited_until = datetime.now(timezone.utc) - timedelta(minutes=1)

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.rate_limited_until is None


def test_handle_paused_handles_missing_rate_limited_until(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """State PAUSED but rate_limited_until None -> IDLE with log."""
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner()
    runner.repo_config.feature_flags = FeatureFlags(
        use_unified_inhibitor_check=False
    )
    runner.state.state = PipelineState.PAUSED
    runner.state.rate_limited_until = None

    asyncio.run(runner.handle_paused())

    assert runner.state.state == PipelineState.IDLE
    assert any("PAUSED without rate_limited_until" in e["event"] for e in runner.state.history)


def test_paused_not_reset_by_transient_states(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """run_cycle preserves PAUSED handling instead of transient-reset logic."""
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner()
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
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner()
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
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner()
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.ERROR
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=15)
    runner.state.error_message = "some real error"

    asyncio.run(runner.run_cycle())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.error_message == "some real error"
    assert any("Legacy ERROR" in e["event"] for e in runner.state.history)


def test_run_cycle_short_circuits_idle_when_user_paused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publishes: list[str] = []
    idle_calls: list[str] = []
    preflight_calls: list[str] = []
    runner = h._make_runner()
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
        h._preflight_recording_stub(preflight_calls),
    )
    monkeypatch.setattr(runner, "handle_idle", fake_handle_idle)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())
    asyncio.run(runner.run_cycle())

    assert idle_calls == []
    assert preflight_calls == []
    assert publishes == ["published", "published"]
    assert sum(1 for entry in runner.state.history if entry["event"] == "[INFRA] Paused. Press Play to resume.") == 1


def test_run_cycle_short_circuits_paused_when_user_paused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publishes: list[str] = []
    paused_calls: list[str] = []
    preflight_calls: list[str] = []
    runner = h._make_runner()
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
        h._preflight_recording_stub(preflight_calls),
    )
    monkeypatch.setattr(runner, "handle_paused", fake_handle_paused)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())
    asyncio.run(runner.run_cycle())

    assert paused_calls == []
    assert preflight_calls == []
    assert publishes == ["published", "published"]
    assert sum(1 for entry in runner.state.history if entry["event"] == "[INFRA] Paused. Press Play to resume.") == 1


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
    runner = h._make_runner()
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
        h._preflight_recording_stub(preflight_calls),
    )
    monkeypatch.setattr(runner, handler_name, fake_handler)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())
    asyncio.run(runner.run_cycle())

    assert handler_calls == []
    assert preflight_calls == []
    assert publishes == ["published", "published"]
    assert sum(1 for entry in runner.state.history if entry["event"] == "[INFRA] Paused. Press Play to resume.") == 1


def test_run_cycle_skips_preflight_after_pause_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publishes: list[str] = []
    preflight_calls: list[str] = []
    runner = h._make_runner()
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
        h._preflight_recording_stub(preflight_calls),
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
    runner = h._make_runner()
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
        h._preflight_recording_stub(preflight_calls),
    )
    monkeypatch.setattr(runner, "handle_idle", fake_handle_idle)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert preflight_calls == ["preflight"]
    assert idle_calls == []
    assert publishes == ["published"]
    assert sum(1 for entry in runner.state.history if entry["event"] == "[INFRA] Paused. Press Play to resume.") == 1


@pytest.mark.parametrize(
    "msg",
    ["rate limit exceeded", "429 Too Many Requests", "API rate limit hit"],
)
def test_classify_error_rate_limit(msg: str) -> None:
    assert _classify_error(msg) == ErrorCategory.RATE_LIMIT


def test_check_rate_limit_triggers_paused_when_session_over_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.usage import UsageSnapshot

    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    snap = UsageSnapshot(
        session_percent=96,
        session_resets_at=9999999999,
        weekly_percent=50,
        weekly_resets_at=9999999999,
        fetched_at=0,
    )
    runner._claude_usage_provider = h._FakeUsageProvider(snapshot=snap)
    runner.app_config.daemon.rate_limit_session_pause_percent = 95

    assert asyncio.run(runner._check_rate_limit()) is False
    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.rate_limited_until is not None


def test_check_rate_limit_triggers_paused_when_weekly_over_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.usage import UsageSnapshot

    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    snap = UsageSnapshot(
        session_percent=50,
        session_resets_at=9999999999,
        weekly_percent=100,
        weekly_resets_at=9999999999,
        fetched_at=0,
    )
    runner._claude_usage_provider = h._FakeUsageProvider(snapshot=snap)
    runner.app_config.daemon.rate_limit_weekly_pause_percent = 100

    assert asyncio.run(runner._check_rate_limit()) is False
    assert runner.state.state == PipelineState.PAUSED


def test_check_rate_limit_allows_cli_when_under_thresholds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.usage import UsageSnapshot

    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    snap = UsageSnapshot(
        session_percent=50,
        session_resets_at=9999999999,
        weekly_percent=60,
        weekly_resets_at=9999999999,
        fetched_at=0,
    )
    runner._claude_usage_provider = h._FakeUsageProvider(snapshot=snap)

    assert asyncio.run(runner._check_rate_limit()) is True


def test_check_rate_limit_fail_open_when_provider_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    runner._claude_usage_provider = h._FakeUsageProvider(snapshot=None)

    assert asyncio.run(runner._check_rate_limit()) is True


def test_check_rate_limit_invalidates_cache_after_pause_expires(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    fake = h._FakeUsageProvider(snapshot=None)
    runner._claude_usage_provider = fake
    runner.state.rate_limited_until = datetime.now(timezone.utc) - timedelta(minutes=1)

    asyncio.run(runner._check_rate_limit())
    assert fake._invalidated is True


def test_rate_limited_until_uses_resets_at_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.usage import UsageSnapshot

    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    resets_at = 1744824000
    snap = UsageSnapshot(
        session_percent=99,
        session_resets_at=resets_at,
        weekly_percent=50,
        weekly_resets_at=9999999999,
        fetched_at=0,
    )
    runner._claude_usage_provider = h._FakeUsageProvider(snapshot=snap)
    runner.app_config.daemon.rate_limit_session_pause_percent = 95

    asyncio.run(runner._check_rate_limit())
    assert runner.state.rate_limited_until is not None
    assert int(runner.state.rate_limited_until.timestamp()) == resets_at


def test_monitor_inflight_breach_cancels_claude_task_on_marker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Breach marker file triggers task cancellation and PAUSED state."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(breach_module, "_BREACH_DIR", str(tmp_path))
    monkeypatch.setattr(breach_module, "_BREACH_POLL_SEC", 0.05)

    breach_run_id = "test-breach-001"

    async def _run() -> None:
        runner = h._make_runner()
        runner._claude_usage_provider = h._FakeUsageProvider()

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
                str(tmp_path),
                breach_run_id,
                task,
                breach_flag,
            )
        )

        # Write breach marker after a short delay
        await asyncio.sleep(0.1)
        marker = tmp_path / f"{breach_run_id}.breach"
        marker.write_text(
            json.dumps(
                {
                    "type": "session",
                    "resets_at": 1700000000,
                    "session_pct": 97,
                    "weekly_pct": 30,
                    "detected_at": 1234567890.0,
                }
            )
        )

        # Wait for monitor to detect and cancel
        await asyncio.sleep(0.3)
        assert breach_flag["breached"] is True
        assert cancelled.is_set()
        assert runner.state.rate_limited_until is not None
        assert runner.state.rate_limit_reactive_coder == "claude"

        monitor.cancel()

    asyncio.run(_run())


def test_monitor_inflight_breach_sets_paused_state_with_resets_at(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Breach monitor sets rate_limited_until from breach marker resets_at."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(breach_module, "_BREACH_DIR", str(tmp_path))
    monkeypatch.setattr(breach_module, "_BREACH_POLL_SEC", 0.05)

    breach_run_id = "test-resets-at"

    async def _run() -> None:
        runner = h._make_runner()
        runner._claude_usage_provider = h._FakeUsageProvider()

        async def fake_cli() -> tuple[int, str, str]:
            await asyncio.sleep(999)
            return (0, "", "")

        task = asyncio.create_task(fake_cli())
        breach_flag: dict[str, bool] = {"breached": False}
        monitor = asyncio.create_task(
            runner._monitor_inflight_breach(
                str(tmp_path),
                breach_run_id,
                task,
                breach_flag,
            )
        )

        marker = tmp_path / f"{breach_run_id}.breach"
        marker.write_text(
            json.dumps(
                {
                    "type": "weekly",
                    "resets_at": 1800000000,
                    "session_pct": 50,
                    "weekly_pct": 105,
                    "detected_at": 1234567890.0,
                }
            )
        )

        await asyncio.sleep(0.3)
        assert runner.state.rate_limited_until is not None
        assert int(runner.state.rate_limited_until.timestamp()) == 1800000000

        monitor.cancel()

    asyncio.run(_run())


def test_monitor_inflight_breach_exits_when_claude_task_completes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Monitor exits cleanly when the CLI task completes without a breach."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(breach_module, "_BREACH_DIR", str(tmp_path))
    monkeypatch.setattr(breach_module, "_BREACH_POLL_SEC", 0.05)

    breach_run_id = "test-no-breach"

    async def _run() -> None:
        runner = h._make_runner()
        runner._claude_usage_provider = h._FakeUsageProvider()

        async def fake_cli_quick() -> tuple[int, str, str]:
            await asyncio.sleep(0.05)
            return (0, "done", "")

        task = asyncio.create_task(fake_cli_quick())
        breach_flag: dict[str, bool] = {"breached": False}
        monitor = asyncio.create_task(
            runner._monitor_inflight_breach(
                str(tmp_path),
                breach_run_id,
                task,
                breach_flag,
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
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Late breach marker is detected, causes PAUSED, and is cleaned up."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(breach_module, "_BREACH_DIR", str(tmp_path))

    captured_run_id: list[str] = []

    async def fake_planned(
        path: str, *_args: object, model: str | None = None, timeout: int | None = None, **kwargs: object
    ) -> tuple[int, str, str]:
        run_id = kwargs.get("breach_run_id", "")
        if run_id:
            captured_run_id.append(run_id)
            # Simulate breach marker written by hook near end of CLI run
            marker = tmp_path / f"{run_id}.breach"
            marker.write_text('{"type":"session","resets_at":0}')
        return (0, "ok", "")

    monkeypatch.setattr(claude_cli, "run_auto_pr_async", fake_planned)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [PRInfo(number=1, branch="pr-001")],
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda *a, **kw: None,
    )

    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
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
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """handle_coding transitions to PAUSED when in-flight breach is detected."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(breach_module, "_BREACH_DIR", str(tmp_path))
    monkeypatch.setattr(breach_module, "_BREACH_POLL_SEC", 0.05)

    async def fake_planned_hangs(
        path: str, *_args: object, model: str | None = None, timeout: int | None = None, **kwargs: object
    ) -> tuple[int, str, str]:
        run_id = kwargs.get("breach_run_id", "")
        # Write breach marker immediately to simulate mid-flight breach
        if run_id:
            marker = tmp_path / f"{run_id}.breach"
            marker.write_text(
                json.dumps(
                    {
                        "type": "session",
                        "resets_at": 1700000000,
                        "session_pct": 98,
                        "weekly_pct": 30,
                        "detected_at": 1234567890.0,
                    }
                )
            )
        await asyncio.sleep(999)  # Block until cancelled
        return (0, "", "")

    monkeypatch.setattr(claude_cli, "run_auto_pr_async", fake_planned_hangs)

    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
    )
    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.rate_limited_until is not None
    assert runner.state.error_message is None


def test_handle_coding_reraises_cancelled_error_without_breach(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancelled CLI runs re-raise when no breach marker was detected."""
    from src.config import CoderType

    h._patch_subprocess(monkeypatch)

    async def fake_run_planned_pr(path: str, *_args: object, **kwargs: object) -> tuple[int, str, str]:
        raise asyncio.CancelledError

    monkeypatch.setattr(codex_cli, "run_auto_pr_async", fake_run_planned_pr)

    runner = h._make_runner(coder=CoderType.CODEX)
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
    )

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(runner.handle_coding())


def test_handle_coding_records_pr_before_breach_cancel_pause(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """In-flight breach cancellation still records the just-opened PR."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(breach_module, "_BREACH_DIR", str(tmp_path))
    monkeypatch.setattr(breach_module, "_BREACH_POLL_SEC", 0.05)

    async def fake_planned_hangs(
        path: str, *_args: object, model: str | None = None, timeout: int | None = None, **kwargs: object
    ) -> tuple[int, str, str]:
        run_id = kwargs.get("breach_run_id", "")
        if run_id:
            marker = tmp_path / f"{run_id}.breach"
            marker.write_text(
                json.dumps(
                    {
                        "type": "session",
                        "resets_at": 1700000000,
                        "session_pct": 98,
                        "weekly_pct": 30,
                        "detected_at": 1234567890.0,
                    }
                )
            )
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

    monkeypatch.setattr(claude_cli, "run_auto_pr_async", fake_planned_hangs)
    monkeypatch.setattr("src.github.prs.get_open_prs", fake_get_open_prs)
    monkeypatch.setattr(coding_module.asyncio, "sleep", fast_sleep)

    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
    )
    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.current_pr == pr
    assert any("Recorded PR #42 before breach-cancel pause" in entry["event"] for entry in runner.state.history)


def test_handle_coding_records_pr_before_late_breach_pause(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Late breach pauses still attach the matching PR before returning."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(breach_module, "_BREACH_DIR", str(tmp_path))

    async def fake_planned(
        path: str, *_args: object, model: str | None = None, timeout: int | None = None, **kwargs: object
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
        breach_dir: str,
        breach_run_id: str,
        breach_flag: dict[str, bool],
    ) -> None:
        breach_flag["breached"] = True
        runner.state.rate_limited_until = datetime.fromtimestamp(
            1700000000,
            tz=timezone.utc,
        )

    async def fast_sleep(seconds: float) -> None:
        await original_sleep(0)

    monkeypatch.setattr(claude_cli, "run_auto_pr_async", fake_planned)
    monkeypatch.setattr("src.github.prs.get_open_prs", fake_get_open_prs)
    monkeypatch.setattr(coding_module.asyncio, "sleep", fast_sleep)

    runner = h._make_runner()
    monkeypatch.setattr(runner, "_check_late_breach", fake_check_late_breach)
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
    )
    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.current_pr == pr
    assert any("Recorded PR #42 before late-breach pause" in entry["event"] for entry in runner.state.history)


def test_get_coder_auto_fallback_switches_on_rate_limit_via_selector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._allow_all_coder_auth(monkeypatch)
    runner = h._make_runner()

    runner.state.rate_limited_coders.add("claude")

    name, plugin = runner._get_coder()

    assert name == "codex"
    assert plugin.name == "codex"


def test_check_rate_limit_runs_proactive_for_codex(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Proactive usage check now runs for Codex too (OpenAI provider)."""

    runner = h._make_runner(coder=CoderType.CODEX)

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

    h._patch_subprocess(monkeypatch)
    runner = h._make_runner(coder=CoderType.CODEX)
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

    h._patch_subprocess(monkeypatch)
    runner = h._make_runner(coder=CoderType.CODEX)
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

    h._patch_subprocess(monkeypatch)
    runner = h._make_runner(coder=CoderType.CODEX)
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

    h._patch_subprocess(monkeypatch)
    runner = h._make_runner(coder=CoderType.CODEX)
    claude_provider = h._FakeUsageProvider(snapshot=None)
    codex_provider = h._FakeUsageProvider(snapshot=None)
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
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner(coder=CoderType.CODEX)
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

    h._patch_subprocess(monkeypatch)
    runner = h._make_runner(coder=CoderType.CODEX)
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

    h._patch_subprocess(monkeypatch)
    runner = h._make_runner(coder=CoderType.CODEX)
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=10)
    runner.state.rate_limit_reactive = True
    runner.state.rate_limit_reactive_coder = CoderType.CODEX.value

    result = asyncio.run(runner._check_rate_limit())
    assert result is False
    assert runner.state.rate_limited_until is not None


def test_check_rate_limit_preserves_per_coder_expiry_windows() -> None:
    runner = h._make_runner()
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
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner(coder=CoderType.CODEX)
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
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
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


def test_detect_rate_limit_codex_try_again_pattern(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex 'try again in X days Y hours Z minutes' -> exact pause, weekly."""

    h._patch_subprocess(monkeypatch)
    runner = h._make_runner(coder=CoderType.CODEX)
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

    h._patch_subprocess(monkeypatch)
    runner = h._make_runner(coder=CoderType.CODEX)
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

    h._patch_subprocess(monkeypatch)
    runner = h._make_runner(coder=CoderType.CODEX)
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

    h._patch_subprocess(monkeypatch)
    runner = h._make_runner(coder=CoderType.CODEX)
    runner._detect_rate_limit("Error: 429 Too Many Requests", coder_name="codex")
    assert runner.state.rate_limited_until is not None
    assert runner.state.rate_limit_reactive_coder == "codex"


def test_detect_rate_limit_codex_retry_seconds_pattern(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex retry text with seconds should trigger a minimum one-minute pause."""

    h._patch_subprocess(monkeypatch)
    runner = h._make_runner(coder=CoderType.CODEX)
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

    h._patch_subprocess(monkeypatch)
    runner = h._make_runner(coder=CoderType.CODEX)
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

    h._patch_subprocess(monkeypatch)
    runner = h._make_runner(coder=CoderType.CODEX)
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

    h._patch_subprocess(monkeypatch)
    runner = h._make_runner(coder=CoderType.CODEX)
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

    h._patch_subprocess(monkeypatch)
    runner = h._make_runner(coder=CoderType.CODEX)
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

    h._patch_subprocess(monkeypatch)
    runner = h._make_runner(coder=CoderType.CODEX)
    runner._detect_rate_limit(
        "Progress update: 87% remaining for weekly rate limit\nRate limit reached. Please try again in 6.379s",
        coder_name="codex",
    )
    assert runner.state.rate_limited_until is not None
    assert runner.state.rate_limit_reactive_coder == "codex"


def test_detect_rate_limit_generic_fallback_still_applies_to_claude(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Claude keeps the generic rate-limit fallback when no specific regex matches."""
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    runner._detect_rate_limit("Warning: API rate limit reached", coder_name="claude")
    assert runner.state.rate_limited_until is not None
    assert runner.state.rate_limit_reactive_coder == "claude"


def test_monitor_inflight_breach_retries_bad_marker_then_uses_default_reset(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Bad marker JSON should be retried before defaulting to a 30-minute pause."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(breach_module, "_BREACH_POLL_SEC", 0.05)

    breach_run_id = "test-breach-bad-json"

    async def _run() -> None:
        runner = h._make_runner()

        async def fake_cli_forever() -> tuple[int, str, str]:
            await asyncio.sleep(999)
            return (0, "", "")

        task = asyncio.create_task(fake_cli_forever())
        breach_flag: dict[str, bool] = {"breached": False}
        monitor = asyncio.create_task(
            runner._monitor_inflight_breach(
                str(tmp_path),
                breach_run_id,
                task,
                breach_flag,
            )
        )

        marker = tmp_path / f"{breach_run_id}.breach"
        marker.write_text("{not-json")
        await asyncio.sleep(0.1)
        marker.write_text(
            json.dumps(
                {
                    "type": "session",
                    "resets_at": 0,
                    "session_pct": 99,
                }
            )
        )

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
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Late breach detection should stop after repeated marker read failures."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(breach_module.time, "sleep", lambda _seconds: None)

    runner = h._make_runner()
    breach_flag = {"breached": False}
    marker = tmp_path / "retry-exhausted.breach"
    marker.write_text("{not-json")

    runner._check_late_breach(str(tmp_path), "retry-exhausted", breach_flag)

    assert breach_flag["breached"] is False
    assert runner.state.rate_limited_until is None


def test_check_late_breach_uses_resets_at_timestamp(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Late breach detection should use the marker reset timestamp when present."""
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner()
    breach_flag = {"breached": False}
    resets_at = 1800000000
    marker = tmp_path / "late-reset.breach"
    marker.write_text(
        json.dumps(
            {
                "type": "weekly",
                "resets_at": resets_at,
                "weekly_pct": 101,
            }
        )
    )

    runner._check_late_breach(str(tmp_path), "late-reset", breach_flag)

    assert breach_flag["breached"] is True
    assert runner.state.rate_limited_until is not None
    assert int(runner.state.rate_limited_until.timestamp()) == resets_at


def test_cleanup_breach_marker_ignores_unlink_oserror(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Cleanup should swallow filesystem unlink failures."""
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()

    def fake_unlink(self: Path, missing_ok: bool = False) -> None:
        raise OSError("simulated unlink failure")

    monkeypatch.setattr(Path, "unlink", fake_unlink)

    runner._cleanup_breach_marker(str(tmp_path), "cleanup-oserror")


def test_handle_idle_returns_immediately_when_user_paused() -> None:
    runner = h._make_runner()
    runner.repo_config.feature_flags = FeatureFlags(
        use_unified_inhibitor_check=False
    )
    runner.state.user_paused = True

    asyncio.run(runner.handle_idle())

    assert runner.state.history == []


def test_handle_paused_logs_user_pause_only_once() -> None:
    runner = h._make_runner()
    runner.repo_config.feature_flags = FeatureFlags(
        use_unified_inhibitor_check=False
    )
    runner.state.user_paused = True

    asyncio.run(runner.handle_paused())
    asyncio.run(runner.handle_paused())

    assert sum(1 for entry in runner.state.history if entry["event"] == "[INFRA] Paused. Press Play to resume.") == 1


def test_refresh_user_paused_from_redis_ignores_invalid_json() -> None:
    runner = h._make_runner()
    runner.redis.store[f"pipeline:{runner.name}"] = "{not-json"
    runner.state.user_paused = False

    asyncio.run(runner._refresh_user_paused_from_redis())

    assert runner.state.user_paused is False


def test_check_budget_returns_false_below_pause_threshold() -> None:
    runner = h._make_runner()
    runner.app_config.daemon.github_api_pause_threshold_percent = 5
    runner.app_config.daemon.github_api_slowdown_threshold_percent = 20
    h._set_budget(runner, h._budget(remaining=10, limit=5000))  # 0.2%

    proceed = asyncio.run(runner._check_github_api_budget())

    assert proceed is False
    assert runner._github_api_pause_attempts == 1
    assert any("GitHub API budget critical" in e["event"] for e in runner.state.history)


def test_check_budget_pause_log_only_fires_once_per_window() -> None:
    runner = h._make_runner()
    runner.app_config.daemon.github_api_pause_threshold_percent = 5
    runner.app_config.daemon.github_api_slowdown_threshold_percent = 20
    h._set_budget(runner, h._budget(remaining=10, limit=5000))

    asyncio.run(runner._check_github_api_budget())
    asyncio.run(runner._check_github_api_budget())

    pause_logs = [e for e in runner.state.history if "GitHub API budget critical" in e["event"]]
    assert len(pause_logs) == 1


def test_check_budget_resets_pause_counter_on_recovery() -> None:
    runner = h._make_runner()
    runner.app_config.daemon.github_api_pause_threshold_percent = 5
    runner.app_config.daemon.github_api_slowdown_threshold_percent = 20

    h._set_budget(runner, h._budget(remaining=10, limit=5000))
    asyncio.run(runner._check_github_api_budget())
    assert runner._github_api_pause_attempts == 1

    h._set_budget(runner, h._budget(remaining=4500, limit=5000))
    proceed = asyncio.run(runner._check_github_api_budget())

    assert proceed is True
    assert runner._github_api_pause_attempts == 0


def test_check_budget_pause_window_passed_means_proceed() -> None:
    runner = h._make_runner()
    runner.app_config.daemon.github_api_pause_threshold_percent = 5
    runner.app_config.daemon.github_api_slowdown_threshold_percent = 20
    # reset_at already in the past — the pause window has elapsed.
    h._set_budget(
        runner,
        h._budget(
            remaining=10,
            limit=5000,
            reset_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        ),
    )

    proceed = asyncio.run(runner._check_github_api_budget())

    assert proceed is True


def test_effective_watch_poll_interval_stacks_with_rate_limit_slowdown() -> None:
    """Stacking with rate-limit slowdown takes the larger of the two."""
    runner = h._make_runner(poll_interval_sec=60)
    h._configure_watch_adaptive_defaults(runner)
    runner.app_config.daemon.github_api_slowdown_multiplier = 10  # 60*10 = 600

    runner._watch_entered_at = datetime.now(timezone.utc)

    # No slowdown active: slow watch window applies as-is.
    assert runner.effective_watch_poll_interval == 300

    # Slowdown active in slow window → max(300, 600) = 600.
    runner._github_api_slowdown_attempts = 2
    assert runner.effective_watch_poll_interval == 600

    # Slowdown active past window (fast=45) → max(45, 600) = 600.
    runner._watch_entered_at = datetime.now(timezone.utc) - timedelta(minutes=10)
    assert runner.effective_watch_poll_interval == 600


# ---------------------------------------------------------------------------
# PR-224b moved from tests/test_runner.py — rate_limit group
# ---------------------------------------------------------------------------


def test_proactive_check_logs_degradation_at_10_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    runner._claude_usage_provider = h._FakeUsageProvider(snapshot=None, failures=10)

    result = asyncio.run(runner._proactive_usage_check())
    assert result is True
    assert any("degraded" in e.get("event", "").lower() for e in runner.state.history)


def test_check_budget_returns_true_when_no_observation() -> None:
    runner = h._make_runner()
    assert asyncio.run(runner._check_github_api_budget()) is True


def test_check_budget_slowdown_window_passed_means_proceed() -> None:
    runner = h._make_runner()
    runner.app_config.daemon.github_api_pause_threshold_percent = 5
    runner.app_config.daemon.github_api_slowdown_threshold_percent = 20
    runner.app_config.daemon.github_api_slowdown_multiplier = 5
    # Above the pause threshold but below slowdown, with reset already elapsed:
    # the snapshot is stale so neither throttle branch should fire.
    h._set_budget(
        runner,
        h._budget(
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
    runner = h._make_runner()
    runner.app_config.daemon.github_api_pause_threshold_percent = 5
    runner.app_config.daemon.github_api_slowdown_threshold_percent = 20
    runner.app_config.daemon.github_api_slowdown_multiplier = 5
    h._set_budget(runner, h._budget(remaining=500, limit=5000))  # 10%

    decisions = [asyncio.run(runner._check_github_api_budget()) for _ in range(11)]

    # cycles 0, 5, 10 proceed; everything else skipped
    assert decisions[0] is True
    assert decisions[5] is True
    assert decisions[10] is True
    assert decisions[1:5] == [False, False, False, False]
    slowdown_logs = [e for e in runner.state.history if "GitHub API budget low" in e["event"]]
    assert len(slowdown_logs) == 1


def test_check_budget_no_skip_when_extended_idle_active() -> None:
    """Extended-idle cadence already absorbs the slowdown; do not skip cycles.

    With both slowdowns active, real cycles must space at
    ``max(extended, base * multiplier)`` — not their product. The
    extended-idle interval already handles the spacing, so the
    budget check proceeds every cycle in this branch.
    """
    runner = h._make_runner(poll_interval_sec=60)
    runner.app_config.daemon.github_api_pause_threshold_percent = 5
    runner.app_config.daemon.github_api_slowdown_threshold_percent = 20
    runner.app_config.daemon.github_api_slowdown_multiplier = 5
    runner.app_config.daemon.idle_extended_after_cycles = 3
    runner.app_config.daemon.idle_extended_poll_interval_sec = 300
    h._set_budget(runner, h._budget(remaining=500, limit=5000))  # 10%

    runner._idle_streak = 5  # past idle_extended_after_cycles

    decisions = [asyncio.run(runner._check_github_api_budget()) for _ in range(6)]

    assert decisions == [True] * 6
    assert runner._github_api_slowdown_cycle == 0
    assert runner._github_api_slowdown_attempts == 6


def test_check_budget_slowdown_resets_on_recovery() -> None:
    runner = h._make_runner()
    runner.app_config.daemon.github_api_slowdown_threshold_percent = 20

    h._set_budget(runner, h._budget(remaining=500, limit=5000))
    asyncio.run(runner._check_github_api_budget())
    asyncio.run(runner._check_github_api_budget())
    assert runner._github_api_slowdown_attempts == 2

    h._set_budget(runner, h._budget(remaining=4500, limit=5000))
    proceed = asyncio.run(runner._check_github_api_budget())

    assert proceed is True
    assert runner._github_api_slowdown_attempts == 0
    assert runner._github_api_slowdown_cycle == 0


def test_check_budget_normal_proceeds_without_changes() -> None:
    runner = h._make_runner()
    h._set_budget(runner, h._budget(remaining=4500, limit=5000))

    proceed = asyncio.run(runner._check_github_api_budget())

    assert proceed is True
    assert runner._github_api_pause_attempts == 0
    assert runner._github_api_slowdown_attempts == 0


def test_check_budget_zero_multiplier_falls_back_to_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.app_config.daemon.github_api_slowdown_threshold_percent = 20
    # bypass pydantic validator to exercise the max(1, ...) guard
    object.__setattr__(runner.app_config.daemon, "github_api_slowdown_multiplier", 0)
    h._set_budget(runner, h._budget(remaining=500, limit=5000))

    proceed = asyncio.run(runner._check_github_api_budget())

    # multiplier coerced to 1 means every cycle in slowdown still proceeds.
    assert proceed is True


def test_refresh_github_api_budget_fetches_and_persists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    fetched = h._budget(remaining=4321, limit=5000)
    monkeypatch.setattr(
        "src.github.rate_limit.fetch_rate_limit_buckets",
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

    runner = h._make_runner()
    rest = h._budget(remaining=4321, limit=5000)
    graphql = h._budget(remaining=120, limit=5000)
    monkeypatch.setattr(
        "src.github.rate_limit.fetch_rate_limit_buckets",
        lambda: (rest, graphql),
    )

    result = asyncio.run(runner._refresh_github_api_budget())

    # Constrained min surfaces as the cached/legacy budget, but each bucket
    # is also persisted under its own key so the dashboard can render them
    # individually rather than collapsing both into a single bar.
    assert result is not None and result.remaining == graphql.remaining
    stored_rest = RateLimitBudget.from_redis_payload(runner.redis.store[BUDGET_REST_REDIS_KEY])
    stored_graphql = RateLimitBudget.from_redis_payload(runner.redis.store[BUDGET_GRAPHQL_REDIS_KEY])
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

    runner = h._make_runner()
    # Pre-populate both buckets to simulate a prior healthy snapshot.
    runner.redis.store[BUDGET_REST_REDIS_KEY] = h._budget(remaining=4500).to_redis_payload()
    runner.redis.store[BUDGET_GRAPHQL_REDIS_KEY] = h._budget(remaining=4500).to_redis_payload()

    rest = h._budget(remaining=4321, limit=5000)
    monkeypatch.setattr(
        "src.github.rate_limit.fetch_rate_limit_buckets",
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
    runner = h._make_runner()
    cached = h._budget(remaining=999)
    runner._github_api_budget_cache = cached
    runner._github_api_budget_last_fetched = datetime.now(timezone.utc)

    calls = {"count": 0}

    def _fetch() -> object:
        calls["count"] += 1
        return h._budget(remaining=1), None

    monkeypatch.setattr("src.github.rate_limit.fetch_rate_limit_buckets", _fetch)

    result = asyncio.run(runner._refresh_github_api_budget())

    assert result is cached
    assert calls["count"] == 0


def test_run_cycle_short_circuits_when_budget_critical(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch, stdout="")
    runner = h._make_runner()
    runner._scaffolded = True
    runner._recovered = True

    monkeypatch.setattr(
        "src.github.rate_limit.fetch_rate_limit_buckets",
        lambda: (h._budget(remaining=1, limit=5000), None),
    )

    asyncio.run(runner.run_cycle())

    # Critical-budget path skips preflight/state machine and just publishes.
    assert runner.redis.writes, "publish_state should still run on early-return"
    assert any("GitHub API budget critical" in e["event"] for e in runner.state.history)


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

    runner = h._make_runner()
    stale_local = h._budget(remaining=4500, limit=5000)
    runner._github_api_budget_cache = stale_local
    runner._github_api_budget_last_fetched = datetime.now(timezone.utc)

    fresh_shared = h._budget(remaining=120, limit=5000)
    runner.redis.store[BUDGET_REDIS_KEY] = fresh_shared.to_redis_payload()

    fetch_calls = {"count": 0}

    def _fetch() -> object:
        fetch_calls["count"] += 1  # pragma: no cover - probe must not be invoked
        return h._budget(remaining=1), None

    monkeypatch.setattr("src.github.rate_limit.fetch_rate_limit_buckets", _fetch)

    result = asyncio.run(runner._refresh_github_api_budget())

    assert fetch_calls["count"] == 0
    assert result is not None
    assert result.remaining == fresh_shared.remaining
    assert runner._github_api_budget_cache is not None
    assert runner._github_api_budget_cache.remaining == fresh_shared.remaining


def test_refresh_github_api_budget_keeps_cache_when_fetch_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    cached = h._budget(remaining=999)
    runner._github_api_budget_cache = cached
    runner._github_api_budget_last_fetched = datetime.now(timezone.utc) - timedelta(seconds=120)
    monkeypatch.setattr(
        "src.github.rate_limit.fetch_rate_limit_buckets",
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

    runner = h._make_runner()
    monkeypatch.setattr(
        "src.github.rate_limit.fetch_rate_limit_buckets",
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

    runner = h._make_runner()
    # Simulate another runner holding the lock and having published a budget.
    persisted = h._budget(remaining=2500, limit=5000)
    runner.redis.store[REFRESH_LOCK_REDIS_KEY] = "1"
    runner.redis.store[BUDGET_REDIS_KEY] = persisted.to_redis_payload()

    fetch_calls = {"count": 0}

    def _fetch() -> object:
        fetch_calls["count"] += 1
        return h._budget(remaining=1), None

    monkeypatch.setattr("src.github.rate_limit.fetch_rate_limit_buckets", _fetch)

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
    shared_redis = h._FakeRedis()
    runner_a = h._make_runner()
    runner_b = h._make_runner()
    runner_a.redis = shared_redis  # type: ignore[assignment]
    runner_b.redis = shared_redis  # type: ignore[assignment]

    fetched = h._budget(remaining=4000, limit=5000)
    fetch_calls = {"count": 0}

    def _fetch() -> object:
        fetch_calls["count"] += 1
        return fetched, None

    monkeypatch.setattr("src.github.rate_limit.fetch_rate_limit_buckets", _fetch)

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

    runner = h._make_runner()
    cached = h._budget(remaining=777)
    runner._github_api_budget_cache = cached
    runner.redis.store[REFRESH_LOCK_REDIS_KEY] = "1"

    monkeypatch.setattr(
        "src.github.rate_limit.fetch_rate_limit_buckets",
        lambda: (h._budget(remaining=1), None),
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

    runner = h._make_runner()
    runner.redis.store[REFRESH_LOCK_REDIS_KEY] = "1"

    monkeypatch.setattr(
        "src.github.rate_limit.fetch_rate_limit_buckets",
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

    runner = h._make_runner()
    persisted = h._budget(remaining=2500, limit=5000)
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

    runner = h._make_runner()
    runner._scaffolded = True
    runner._recovered = True
    runner.redis.store[BUDGET_REDIS_KEY] = h._budget(remaining=4500).to_redis_payload()

    async def fake_run_cycle_body() -> None:
        # Simulate API consumption observed via a sibling refresh mid-cycle.
        runner.redis.store[BUDGET_REDIS_KEY] = h._budget(remaining=4480).to_redis_payload()

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

    runner = h._make_runner()
    runner._scaffolded = True
    runner._recovered = True
    runner.redis.store[BUDGET_REDIS_KEY] = h._budget(remaining=120).to_redis_payload()

    async def fake_run_cycle_body() -> None:
        runner.redis.store[BUDGET_REDIS_KEY] = h._budget(remaining=5000).to_redis_payload()

    monkeypatch.setattr(runner, "_run_cycle_body", fake_run_cycle_body)

    asyncio.run(runner.run_cycle())

    bucket = runner.redis.lists.get(f"{BURNS_REDIS_KEY_PREFIX}{runner.name}")
    assert bucket == ["0"]


def test_run_cycle_records_zero_burn_when_no_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No budget snapshot anywhere means we record zero rather than crashing."""
    from src.daemon.github_rate_limit import BURNS_REDIS_KEY_PREFIX

    runner = h._make_runner()
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

    runner = h._make_runner()
    runner._scaffolded = True
    runner._recovered = True
    runner.redis.store[BUDGET_REDIS_KEY] = h._budget(remaining=4500).to_redis_payload()

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
    runner = h._make_runner()
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
