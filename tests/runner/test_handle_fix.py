"""PR-224a: handle_fix handler tests for src/daemon/runner.py

Mechanical move from tests/test_runner.py. Helpers and fixtures still live in
tests/test_runner.py and are referenced via the ``h`` alias.
"""

from __future__ import annotations

import asyncio
import subprocess
from datetime import datetime, timezone
from typing import Any

import pytest
from src import codex_cli
from src.coders import claude as claude_plugin_module
from src.config import AppConfig, CoderType, DaemonConfig
from src.daemon import git_ops as git_ops_module
from src.daemon import recovery_policy as recovery_policy_module
from src.daemon import runner as runner_module
from src.daemon.handlers import fix as fix_module
from src.daemon.runner import PipelineRunner
from src.models import (
    CIStatus,
    PipelineState,
    PRInfo,
    ReviewStatus,
)

from tests import test_runner as h

claude_cli = claude_plugin_module.claude_cli


def test_handle_fix_posts_codex_review_after_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-019: after a successful fix push, ``handle_fix`` must post
    ``@codex review`` so Codex reviews the freshly-pushed iteration."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli, "fix_review_async", h._async_cli_result(0, "", "")
    )
    posted: list[tuple[str, int, str]] = []

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    monkeypatch.setattr(runner_module.github_client, "post_comment", fake_post)

    runner = h._make_runner()
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


def test_handle_fix_injects_ci_logs_when_ci_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ci_status=FAILURE: handle_fix passes CI logs into the FIX prompt."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        fix_module, "_fetch_failed_ci_logs",
        lambda repo, branch: "pytest assertion error: boom",
    )
    monkeypatch.setattr(
        fix_module.github_client, "get_latest_codex_feedback",
        lambda repo, pr_number: None,
    )
    captured = h._capture_fix_kwargs(monkeypatch)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=77, branch="pr-019", ci_status=CIStatus.FAILURE
    )
    asyncio.run(runner.handle_fix())

    extra_context = captured["kwargs"]["extra_context"]
    assert "CI failure logs (last 5000 chars):" in extra_context
    assert "pytest assertion error: boom" in extra_context
    assert "Latest review feedback:" not in extra_context


def test_handle_fix_injects_review_feedback_when_changes_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """review_status=CHANGES_REQUESTED: handle_fix passes review body into prompt."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        fix_module, "_fetch_failed_ci_logs",
        lambda repo, branch: None,
    )
    monkeypatch.setattr(
        fix_module.github_client, "get_latest_codex_feedback",
        lambda repo, pr_number: "P1: please rename foo to bar",
    )
    captured = h._capture_fix_kwargs(monkeypatch)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=77,
        branch="pr-019",
        review_status=ReviewStatus.CHANGES_REQUESTED,
    )
    asyncio.run(runner.handle_fix())

    extra_context = captured["kwargs"]["extra_context"]
    assert "Latest review feedback:" in extra_context
    assert "P1: please rename foo to bar" in extra_context
    assert "CI failure logs" not in extra_context


def test_handle_fix_injects_both_ci_logs_and_review_feedback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        fix_module, "_fetch_failed_ci_logs",
        lambda repo, branch: "ci-boom",
    )
    monkeypatch.setattr(
        fix_module.github_client, "get_latest_codex_feedback",
        lambda repo, pr_number: "review-feedback-text",
    )
    captured = h._capture_fix_kwargs(monkeypatch)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=77,
        branch="pr-019",
        ci_status=CIStatus.FAILURE,
        review_status=ReviewStatus.CHANGES_REQUESTED,
    )
    asyncio.run(runner.handle_fix())

    extra_context = captured["kwargs"]["extra_context"]
    assert "CI failure logs (last 5000 chars):" in extra_context
    assert "ci-boom" in extra_context
    assert "Latest review feedback:" in extra_context
    assert "review-feedback-text" in extra_context


def test_handle_fix_omits_extra_context_when_no_signals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ci_status=PENDING + review_status=PENDING: no extra_context kwarg."""
    h._patch_subprocess(monkeypatch)
    captured = h._capture_fix_kwargs(monkeypatch)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=77, branch="pr-019")
    asyncio.run(runner.handle_fix())

    assert "extra_context" not in captured["kwargs"]


def test_handle_fix_finishes_push_bookkeeping_before_post_exit_stop_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli, "fix_review_async", h._async_cli_result(0, "", "")
    )
    posted: list[tuple[str, int, str]] = []

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    monkeypatch.setattr(runner_module.github_client, "post_comment", fake_post)

    runner = h._make_runner()
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

    runner = h._make_runner()
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

    runner = h._make_runner()
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
    runner._app_config = h._app_cfg(fix_iteration_cap=15)

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
        == "[ESCALATE] FIX cap reached (15/15) on PR #77: escalated, "
        "moving to IDLE."
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

    runner = h._make_runner()
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
    runner._app_config = h._app_cfg(fix_iteration_cap=2)

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

    runner = h._make_runner()
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
    runner._app_config = h._app_cfg(fix_iteration_cap=2)

    asyncio.run(runner.handle_fix())

    assert posted == []
    assert gh_calls == []
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.user_paused is True
    assert any(
        entry["event"]
        == "[FIX] FIX blocked for escalated PR #91, moving to IDLE."
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

    runner = h._make_runner()
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
    runner._app_config = h._app_cfg(fix_iteration_cap=2)

    asyncio.run(runner.handle_fix())

    assert posted == []
    assert gh_calls == []
    assert runner.state.state == PipelineState.IDLE
    assert any(
        entry["event"]
        == "[FIX] FIX blocked for escalated PR #92, moving to IDLE."
        for entry in runner.state.history
    )


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

    runner = h._make_runner()
    runner._get_coder = lambda allow_exploration=False: (  # type: ignore[method-assign]
        "claude",
        _UnexpectedPlugin(),
    )
    runner.state.current_pr = PRInfo(
        number=89,
        branch="pr-089",
        fix_iteration_count=2,
    )
    runner._app_config = h._app_cfg(fix_iteration_cap=2)

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

    runner = h._make_runner()
    runner._get_coder = lambda allow_exploration=False: (  # type: ignore[method-assign]
        "claude",
        _UnexpectedPlugin(),
    )
    runner.state.current_pr = PRInfo(
        number=90,
        branch="pr-090",
        fix_iteration_count=2,
    )
    runner._app_config = h._app_cfg(fix_iteration_cap=2)

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

    runner = h._make_runner()
    runner._get_coder = lambda allow_exploration=False: (  # type: ignore[method-assign]
        "claude",
        _UnexpectedPlugin(),
    )
    runner.state.current_pr = PRInfo(
        number=93,
        branch="pr-093",
        fix_iteration_count=2,
    )
    runner._app_config = h._app_cfg(fix_iteration_cap=2)

    asyncio.run(runner.handle_fix())

    assert maybe_escalate_calls == ["fix_iteration_cap"]
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.is_escalated is True


def test_handle_fix_three_no_push_cycles_transition_to_hung(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Three consecutive no-push FIX cycles must park the PR in HUNG."""
    posted = h._patch_no_push_fix(monkeypatch, head_seq=lambda: "abc123")

    runner = h._make_runner()
    runner._app_config = h._app_cfg(fix_no_push_cap=3)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=217, branch="pr-217")

    asyncio.run(runner.handle_fix())
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.no_push_fix_count == 1

    asyncio.run(runner.handle_fix())
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr.no_push_fix_count == 2

    asyncio.run(runner.handle_fix())
    expected_msg = (
        "FIX deadlock: 3 consecutive no-push FIX cycles on PR #217. "
        "Coder unable to identify actionable fix. Manual review required."
    )
    assert runner.state.state == PipelineState.HUNG
    assert runner.state.current_pr.no_push_fix_count == 0
    # is_escalated must be set so handle_hung's hung_fallback_codex_review
    # path stays parked instead of bouncing back to WATCH.
    assert runner.state.current_pr.is_escalated is True
    assert posted[-1] == (runner.owner_repo, 217, expected_msg)
    assert any(
        entry["event"] == f"[ESCALATE] {expected_msg}"
        for entry in runner.state.history
    )


def test_handle_fix_productive_push_resets_no_push_counter_before_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two no-push cycles followed by a productive push reset the counter."""
    seq = iter([
        "aaa000",  # call 1 head_before
        "aaa000",  # call 1 head_after  → no-push
        "aaa000",  # call 2 head_before
        "aaa000",  # call 2 head_after  → no-push
        "aaa000",  # call 3 head_before
        "bbb111",  # call 3 head_after  → productive push
    ])
    posted = h._patch_no_push_fix(monkeypatch, head_seq=lambda: next(seq))

    runner = h._make_runner()
    runner._app_config = h._app_cfg(fix_no_push_cap=3)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=218, branch="pr-218")
    monkeypatch.setattr(runner, "_post_codex_review", lambda pr_number: True)

    asyncio.run(runner.handle_fix())
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.no_push_fix_count == 1

    asyncio.run(runner.handle_fix())
    assert runner.state.current_pr.no_push_fix_count == 2

    asyncio.run(runner.handle_fix())
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr.no_push_fix_count == 0
    assert runner.state.current_pr.push_count == 1
    assert all(
        "FIX deadlock" not in entry["event"] for entry in runner.state.history
    )
    assert all("FIX deadlock" not in body for _repo, _num, body in posted)


def test_handle_fix_no_push_counter_resets_between_productive_pushes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No-push, productive push, then no-push again must not trigger HUNG."""
    seq = iter([
        "aaa000",  # call 1 head_before
        "aaa000",  # call 1 head_after  → no-push  (counter 0→1)
        "aaa000",  # call 2 head_before
        "bbb111",  # call 2 head_after  → productive push (counter 1→0)
        "bbb111",  # call 3 head_before
        "bbb111",  # call 3 head_after  → no-push  (counter 0→1)
    ])
    posted = h._patch_no_push_fix(monkeypatch, head_seq=lambda: next(seq))

    runner = h._make_runner()
    runner._app_config = h._app_cfg(fix_no_push_cap=3)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=219, branch="pr-219")
    monkeypatch.setattr(runner, "_post_codex_review", lambda pr_number: True)

    asyncio.run(runner.handle_fix())
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.no_push_fix_count == 1

    asyncio.run(runner.handle_fix())
    assert runner.state.current_pr.no_push_fix_count == 0

    asyncio.run(runner.handle_fix())
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr.no_push_fix_count == 1
    assert all(
        "FIX deadlock" not in entry["event"] for entry in runner.state.history
    )
    assert all("FIX deadlock" not in body for _repo, _num, body in posted)


def test_handle_fix_no_push_counter_independent_of_fix_iteration_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A PR can carry fix_iteration_count=2 and trip no_push_fix_count first."""
    posted = h._patch_no_push_fix(monkeypatch, head_seq=lambda: "abc123")

    runner = h._make_runner()
    runner._app_config = h._app_cfg(fix_iteration_cap=15, fix_no_push_cap=3)
    runner.state.state = PipelineState.WATCH
    # fix_iteration_count=2 reflects two prior productive cycles; the no-push
    # counter advances orthogonally and trips HUNG without touching the cap.
    runner.state.current_pr = PRInfo(
        number=220,
        branch="pr-220",
        fix_iteration_count=2,
    )

    asyncio.run(runner.handle_fix())
    asyncio.run(runner.handle_fix())
    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.HUNG
    assert runner.state.current_pr is not None
    # fix_iteration_count untouched: only productive pushes increment it.
    assert runner.state.current_pr.fix_iteration_count == 2
    assert runner.state.current_pr.no_push_fix_count == 0
    assert runner.state.current_pr.is_escalated is True
    expected_msg = (
        "FIX deadlock: 3 consecutive no-push FIX cycles on PR #220. "
        "Coder unable to identify actionable fix. Manual review required."
    )
    assert posted[-1] == (runner.owner_repo, 220, expected_msg)


def test_handle_fix_routes_no_push_cap_through_bounded_recovery_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Guard: the no-push escalation site must call
    ``BoundedRecoveryPolicy.maybe_escalate`` rather than rebuilding
    the threshold check inline."""
    h._patch_no_push_fix(monkeypatch, head_seq=lambda: "abc123")

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

    runner = h._make_runner()
    runner._app_config = h._app_cfg(fix_no_push_cap=2)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=221, branch="pr-221")

    asyncio.run(runner.handle_fix())
    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.HUNG
    assert "fix_no_push_cap" in maybe_escalate_calls


def test_handle_fix_no_push_deadlock_post_failure_still_transitions_to_hung(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed deadlock-comment post must not block the HUNG transition."""

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:2] == ["git", "rev-parse"] and "HEAD" in cmd:
            return h._FakeCompletedProcess(args=cmd, stdout="abc123\n", returncode=0)
        if cmd[:2] == ["git", "rev-list"]:
            return h._FakeCompletedProcess(args=cmd, stdout="0\n", returncode=0)
        return h._FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        claude_cli, "fix_review_async", h._async_cli_result(0, "", "")
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("gh down")),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "run_gh",
        lambda *a, **kw: "",
    )

    runner = h._make_runner()
    runner._app_config = h._app_cfg(fix_no_push_cap=2)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=222, branch="pr-222")

    asyncio.run(runner.handle_fix())
    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.HUNG
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.no_push_fix_count == 0
    assert runner.state.current_pr.is_escalated is True
    assert any(
        "failed to post FIX deadlock comment" in entry["event"]
        for entry in runner.state.history
    )
    assert any(
        "FIX deadlock: 2 consecutive no-push FIX cycles on PR #222"
        in entry["event"]
        for entry in runner.state.history
    )


def test_handle_fix_no_push_deadlock_applies_escalated_label(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The no-push escalation must add the ``escalated`` label so
    ``get_open_prs`` rehydrates ``is_escalated`` after a daemon restart
    (Codex P2 on PR #222)."""
    posted = h._patch_no_push_fix(monkeypatch, head_seq=lambda: "abc123")
    gh_calls: list[list[str]] = []

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        gh_calls.append(cmd)
        return ""

    monkeypatch.setattr(runner_module.github_client, "run_gh", fake_run_gh)

    runner = h._make_runner()
    runner._app_config = h._app_cfg(fix_no_push_cap=2)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=223, branch="pr-223")

    asyncio.run(runner.handle_fix())
    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.HUNG
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.is_escalated is True
    escalation_calls = [
        cmd
        for cmd in gh_calls
        if cmd[:1] == ["label"] or cmd[:2] == ["pr", "edit"]
    ]
    assert escalation_calls == [
        [
            "label",
            "create",
            "escalated",
            "--color",
            "B60205",
            "--description",
            "Daemon escalated, manual review required",
        ],
        ["pr", "edit", "223", "--add-label", "escalated"],
    ]
    assert posted[-1][1] == 223


def test_handle_fix_no_push_deadlock_label_failures_do_not_block_hung(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Label create/add failures must not block the HUNG transition; the
    in-memory ``is_escalated`` flag is sufficient for the current run."""
    h._patch_no_push_fix(monkeypatch, head_seq=lambda: "abc123")

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        if cmd[:3] == ["label", "create", "escalated"]:
            raise RuntimeError("label already exists")
        if cmd[:2] == ["pr", "edit"]:
            raise RuntimeError("gh down")
        return ""

    monkeypatch.setattr(runner_module.github_client, "run_gh", fake_run_gh)

    runner = h._make_runner()
    runner._app_config = h._app_cfg(fix_no_push_cap=2)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=224, branch="pr-224")

    asyncio.run(runner.handle_fix())
    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.HUNG
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.is_escalated is True
    assert any(
        "FIX no-push label create skipped: label already exists"
        in entry["event"]
        for entry in runner.state.history
    )
    assert any(
        "failed to apply escalated label to PR #224: gh down"
        in entry["event"]
        for entry in runner.state.history
    )


def test_handle_fix_coder_escalate_transitions_to_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted, gh_calls = h._patch_fix_with_stdout(
        monkeypatch, stdout="working...\nESCALATE: rate limit exceeded\n"
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=300, branch="pr-300")

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.is_escalated is True
    expected_message = (
        "Coder explicitly escalated this PR. Reason: rate limit exceeded. "
        "Manual review required."
    )
    assert posted == [(runner.owner_repo, 300, expected_message)]
    assert [
        cmd for cmd in gh_calls
        if cmd[:1] == ["label"] or cmd[:2] == ["pr", "edit"]
    ] == [
        [
            "label", "create", "escalated", "--color", "B60205",
            "--description", "Daemon escalated, manual review required",
        ],
        ["pr", "edit", "300", "--add-label", "escalated"],
    ]
    assert any(
        entry["event"]
        == "[ESCALATE] FIX coder ESCALATE on PR #300: rate limit "
        "exceeded. Moving to IDLE."
        for entry in runner.state.history
    )


def test_handle_fix_coder_escalate_only_last_non_empty_line_triggers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ESCALATE in the middle of stdout must not trigger the protocol."""
    posted, _ = h._patch_fix_with_stdout(
        monkeypatch,
        stdout="ESCALATE: ignored\nfinal line is something else\n",
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=301, branch="pr-301")

    asyncio.run(runner.handle_fix())

    # No-push exit path: WATCH (counter incremented but cap not hit).
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.is_escalated is False
    assert runner.state.current_pr.no_push_fix_count == 1
    assert posted == []


def test_handle_fix_coder_escalate_no_marker_keeps_normal_flow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted, gh_calls = h._patch_fix_with_stdout(
        monkeypatch, stdout="ran tests\nall good\n"
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=302, branch="pr-302")

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.is_escalated is False
    assert posted == []
    assert all(
        cmd[:1] != ["label"] and cmd[:2] != ["pr", "edit"]
        for cmd in gh_calls
    )


def test_handle_fix_coder_escalate_empty_reason_uses_placeholder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted, _ = h._patch_fix_with_stdout(monkeypatch, stdout="ESCALATE:\n")

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=303, branch="pr-303")

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.is_escalated is True
    expected_message = (
        "Coder explicitly escalated this PR. Reason: (no reason provided). "
        "Manual review required."
    )
    assert posted == [(runner.owner_repo, 303, expected_message)]
    assert any(
        "(no reason provided)" in entry["event"]
        for entry in runner.state.history
    )


def test_handle_fix_coder_escalate_wins_over_productive_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An ESCALATE marker preempts the regular push → @codex review path."""
    head_calls = iter(["aaa000", "bbb111"])
    review_posts: list[int] = []
    posted, _ = h._patch_fix_with_stdout(
        monkeypatch,
        stdout="pushed a fix\nESCALATE: cannot resolve\n",
        head_seq=lambda: next(head_calls),
    )
    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=304, branch="pr-304")
    monkeypatch.setattr(
        runner,
        "_post_codex_review",
        lambda pr_number: review_posts.append(pr_number) or True,
    )

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.is_escalated is True
    # ESCALATE preempts record_fix_push → no iteration increment, no
    # @codex review post, no_push counter reset.
    assert runner.state.current_pr.fix_iteration_count == 0
    assert runner.state.current_pr.push_count == 0
    assert runner.state.current_pr.no_push_fix_count == 0
    assert review_posts == []
    assert posted == [
        (
            runner.owner_repo,
            304,
            "Coder explicitly escalated this PR. Reason: cannot resolve. "
            "Manual review required.",
        )
    ]


def test_handle_fix_coder_escalate_post_failure_still_parks_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Comment-post failure must not block the IDLE transition."""
    h._patch_fix_with_stdout(
        monkeypatch, stdout="ESCALATE: cannot recover\n"
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: (_ for _ in ()).throw(
            RuntimeError("gh down")
        ),
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=305, branch="pr-305")

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.is_escalated is True
    assert any(
        "failed to post FIX coder ESCALATE comment on PR #305" in entry["event"]
        for entry in runner.state.history
    )


def test_handle_fix_coder_escalate_label_apply_failure_parks_in_hung(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Label apply failure must park in HUNG, not IDLE.

    IDLE refreshes rehydrate ``is_escalated`` from GitHub labels via
    ``_preserve_fix_iteration_count``; if the label apply soft-failed,
    transitioning to IDLE would silently drop the parking signal on
    the next refresh (Codex P1 on PR #228). HUNG honors the in-memory
    flag, so the runner stays parked until the operator resolves the
    PR. The comment is still posted (descriptive record) and the
    ``label create`` soft-fail is unchanged.
    """
    posted, _ = h._patch_fix_with_stdout(
        monkeypatch, stdout="ESCALATE: infra error\n"
    )

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        if cmd[:3] == ["label", "create", "escalated"]:
            raise RuntimeError("label already exists")
        if cmd[:2] == ["pr", "edit"]:
            raise RuntimeError("gh down")
        return ""

    monkeypatch.setattr(runner_module.github_client, "run_gh", fake_run_gh)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=306, branch="pr-306")

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.HUNG
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.is_escalated is True
    assert runner.state.error_message is not None
    assert "failed to apply `escalated` label" in runner.state.error_message
    assert "infra error" in runner.state.error_message
    assert posted and posted[0][1] == 306
    assert any(
        "FIX coder ESCALATE label create skipped: label already exists"
        in entry["event"]
        for entry in runner.state.history
    )
    assert any(
        "failed to apply escalated label to PR #306: gh down"
        in entry["event"]
        for entry in runner.state.history
    )


def test_handle_fix_coder_escalate_honors_deferred_stop_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A user stop arriving alongside an ESCALATE marker must win.

    The ESCALATE branch parks in IDLE (or HUNG on label-apply failure);
    if a stop request was deferred until after FIX bookkeeping, PAUSED
    must override so the operator's pause is not silently dropped
    (Codex P1 on PR #228).
    """
    posted, _ = h._patch_fix_with_stdout(
        monkeypatch, stdout="ESCALATE: handing off\n"
    )

    async def fake_pop_stop_request() -> bool:
        return True

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=308, branch="pr-308")
    monkeypatch.setattr(runner, "_pop_stop_request", fake_pop_stop_request)

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.error_message is None
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.is_escalated is True
    # ESCALATE bookkeeping (comment + label) still ran before PAUSED.
    assert posted and posted[0][1] == 308
    assert any(
        entry["event"] == "[FIX] FIX aborted: user stop requested."
        for entry in runner.state.history
    )


def test_handle_fix_coder_escalate_resets_no_push_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Coder ESCALATE breaks the no-push streak: a deliberate bail-out
    must not feed the deadlock counter."""
    h._patch_fix_with_stdout(monkeypatch, stdout="ESCALATE: bail out\n")

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=307, branch="pr-307", no_push_fix_count=2
    )

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.no_push_fix_count == 0
    assert runner.state.current_pr.is_escalated is True


def test_handle_fix_failure_resets_no_push_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-zero FIX exit between two no-push successes must reset the
    no-push counter so a non-consecutive sequence does not trip the
    deadlock cap (Codex P2 on PR #222)."""
    posted: list[tuple[str, int, str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:2] == ["git", "rev-parse"] and "HEAD" in cmd:
            return h._FakeCompletedProcess(args=cmd, stdout="abc123\n", returncode=0)
        if cmd[:2] == ["git", "rev-list"]:
            return h._FakeCompletedProcess(args=cmd, stdout="0\n", returncode=0)
        return h._FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    monkeypatch.setattr(
        runner_module.github_client, "run_gh", lambda *a, **kw: ""
    )

    cycles = iter([(0, "", ""), (1, "", "boom"), (0, "", "")])

    async def fake_fix(*args: object, **kwargs: object) -> tuple[int, str, str]:
        return next(cycles)

    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)

    runner = h._make_runner()
    runner._app_config = h._app_cfg(fix_no_push_cap=2)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=225, branch="pr-225")

    # Cycle 1: no-push success → counter 0→1
    asyncio.run(runner.handle_fix())
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.no_push_fix_count == 1

    # Cycle 2: non-zero exit → counter resets to 0, state ERROR
    runner.state.state = PipelineState.WATCH
    asyncio.run(runner.handle_fix())
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.current_pr.no_push_fix_count == 0

    # Cycle 3: no-push success → counter 0→1; cap=2 so still no escalation.
    runner.state.state = PipelineState.WATCH
    asyncio.run(runner.handle_fix())
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr.no_push_fix_count == 1
    assert all("FIX deadlock" not in body for _r, _n, body in posted)


def test_handle_fix_stop_cancel_resets_no_push_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """User stop-cancel during FIX also breaks the no-push streak
    (Codex P2 round 3 on PR #222). When the operator pauses mid-FIX the
    cycle isn't a coder no-push outcome, so the counter resets."""

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            return h._FakeCompletedProcess(
                args=["git", *args], stdout="aaa111\n", returncode=0
            )
        if args[:2] == ("rev-parse", "origin/pr-229"):
            return h._FakeCompletedProcess(
                args=["git", *args], stdout="aaa111\n", returncode=0
            )
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

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=229, branch="pr-229", no_push_fix_count=2
    )

    async def stop_monitor(
        cli_task: asyncio.Task[tuple[int, str, str]],
    ) -> None:
        runner._stop_requested = True
        runner.state.user_paused = True
        await asyncio.sleep(0)
        cli_task.cancel()

    async def save_log(stdout: str, stderr: str, label: str) -> None:
        pass

    monkeypatch.setattr(runner, "_save_cli_log", save_log)
    monkeypatch.setattr(runner, "_monitor_stop_request", stop_monitor)

    asyncio.run(runner.handle_fix())

    assert runner.state.current_pr is not None
    assert runner.state.current_pr.no_push_fix_count == 0


def test_handle_fix_idle_timeout_resets_no_push_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FIX idle timeout breaks the no-push streak: the daemon killed the
    coder for not progressing, which is not a consecutive no-push exit
    and must reset the counter (Codex P2 on PR #222)."""
    posted: list[tuple[str, int, str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:2] == ["git", "rev-parse"] and "HEAD" in cmd:
            return h._FakeCompletedProcess(args=cmd, stdout="abc123\n", returncode=0)
        if cmd[:2] == ["git", "rev-list"]:
            return h._FakeCompletedProcess(args=cmd, stdout="0\n", returncode=0)
        return h._FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    monkeypatch.setattr(
        runner_module.github_client, "run_gh", lambda *a, **kw: ""
    )

    async def idle_timeout_monitor(
        self: object,
        pr_number: int,
        idle_limit: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        idle_flag: dict[str, bool],
    ) -> None:
        idle_flag["timed_out"] = True
        target.cancel()

    monkeypatch.setattr(
        PipelineRunner, "_monitor_fix_idle", idle_timeout_monitor
    )

    async def slow_fix(*args: object, **kwargs: object) -> tuple[int, str, str]:
        await asyncio.Future()
        return (0, "", "")

    monkeypatch.setattr(claude_cli, "fix_review_async", slow_fix)

    runner = h._make_runner()
    runner._app_config = h._app_cfg(fix_no_push_cap=2)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=226, branch="pr-226", no_push_fix_count=1
    )

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.no_push_fix_count == 0
    assert all("FIX deadlock" not in body for _r, _n, body in posted)


def test_handle_fix_finishes_push_bookkeeping_before_stop_cancel_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stop-cancelled FIX must still record a completed push before pausing."""
    rev_parse_calls = {"count": 0}
    posted: list[int] = []
    saved_logs: list[tuple[str, str, str]] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            sha = "aaa111\n" if rev_parse_calls["count"] == 1 else "bbb222\n"
            return h._FakeCompletedProcess(args=["git", *args], stdout=sha, returncode=0)
        if args[:2] == ("rev-parse", "origin/pr-042-fix"):
            return h._FakeCompletedProcess(
                args=["git", *args],
                stdout="bbb222\n",
                returncode=0,
            )
        if args[:2] == ("merge-base", "--is-ancestor"):
            return h._FakeCompletedProcess(args=["git", *args], returncode=0)
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

    runner = h._make_runner()
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
    assert saved_logs == [("", "", "FIX FEEDBACK output [claude]")]
    assert any("Fix pushed, iteration #1" in e["event"] for e in runner.state.history)


def test_handle_fix_stop_cancel_skips_push_bookkeeping_when_remote_head_is_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stop-cancelled FIX must not count an unpushed local commit as a push."""
    rev_parse_calls = {"count": 0}
    posted: list[int] = []
    saved_logs: list[tuple[str, str, str]] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            sha = "aaa111\n" if rev_parse_calls["count"] == 1 else "bbb222\n"
            return h._FakeCompletedProcess(args=["git", *args], stdout=sha, returncode=0)
        if args[:2] == ("rev-parse", "origin/pr-042-fix"):
            return h._FakeCompletedProcess(
                args=["git", *args],
                stdout="aaa111\n",
                returncode=0,
            )
        if args[:2] == ("merge-base", "--is-ancestor"):
            return h._FakeCompletedProcess(args=["git", *args], returncode=1)
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

    runner = h._make_runner()
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
    assert saved_logs == [("", "", "FIX FEEDBACK output [claude]")]
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

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            sha = "aaa111\n" if rev_parse_calls["count"] == 1 else "bbb222\n"
            return h._FakeCompletedProcess(args=["git", *args], stdout=sha, returncode=0)
        if args[:2] == ("rev-parse", "origin/pr-042-fix"):
            return h._FakeCompletedProcess(
                args=["git", *args],
                stdout="ccc333\n",
                returncode=0,
            )
        if args[:2] == ("merge-base", "--is-ancestor"):
            assert args[2:] == ("bbb222", "ccc333")
            return h._FakeCompletedProcess(args=["git", *args], returncode=0)
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

    runner = h._make_runner()
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
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "fix_review_async",
        h._async_cli_result(1, "", "fix failed fast"),
    )

    runner = h._make_runner()
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
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli, "fix_review_async", h._async_cli_result(0, "", "")
    )

    def boom(repo: str, number: int, body: str) -> None:
        raise RuntimeError("gh rate limited")

    monkeypatch.setattr(runner_module.github_client, "post_comment", boom)

    runner = h._make_runner()
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

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        calls.append(cmd)
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        claude_cli, "fix_review_async", h._async_cli_result(0, "", "")
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: None,
    )

    runner = h._make_runner()
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
    calls = h._patch_subprocess(monkeypatch)
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

    runner = h._make_runner()
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

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:2] == ["git", "fetch"] and any("pr-042-fix" in a for a in cmd):
            raise subprocess.CalledProcessError(
                1, cmd, stderr="fatal: couldn't find remote ref pr-042-fix"
            )
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        claude_cli,
        "fix_review_async",
        h._async_cli_capture_path(fix_calls, 0, "", ""),
    )

    runner = h._make_runner()
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

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:2] == ["git", "reset"] and "origin/pr-042-fix" in cmd:
            raise subprocess.CalledProcessError(
                1, cmd, stderr="fatal: ambiguous argument"
            )
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        claude_cli,
        "fix_review_async",
        h._async_cli_capture_path(fix_calls, 0, "", ""),
    )

    runner = h._make_runner()
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

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:2] == ["git", "checkout"] and "pr-042-fix" in cmd:
            raise subprocess.TimeoutExpired(cmd=cmd, timeout=30)
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        claude_cli,
        "fix_review_async",
        h._async_cli_capture_path(fix_calls, 0, "", ""),
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.ERROR
    assert "git refresh" in (runner.state.error_message or "")
    assert fix_calls == [], "fix_review must not run when checkout times out"
    assert fix_calls == [], "fix_review must not run when checkout times out"


def test_handle_fix_saves_stdout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """handle_fix must save CLI stdout to Redis via _save_cli_log."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "fix_review_async",
        h._async_cli_result(0, "fix output here", ""),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda *a, **kw: None,
    )

    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=10, branch="pr-001")
    asyncio.run(runner.handle_fix())

    redis_keys = [k for k, _v in runner.redis.writes]
    assert any(k == f"cli_log:{runner.name}:latest" for k in redis_keys)
    stored = runner.redis.store.get(f"cli_log:{runner.name}:latest")
    assert "fix output here" in (stored or "")
    assert "=== STDOUT ===" in (stored or "")
    assert "=== STDERR ===" in (stored or "")


def test_handle_fix_skips_fork(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cross-repo (fork) PRs must return to WATCH without running fix_review."""
    fix_called: list[bool] = []

    async def fake_fix(
        path: str, model: str | None = None, timeout: int | None = None, **kwargs: object
    ) -> tuple[int, str, str]:
        fix_called.append(True)
        return (0, "", "")

    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)
    runner = h._make_runner()
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


def test_handle_fix_does_not_forward_idle_timeout_as_cli_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FIX FEEDBACK should stay uncapped while the idle monitor enforces no-push timeouts."""
    h._patch_subprocess(monkeypatch)
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
        h._repo_cfg(),
        AppConfig(
            repositories=[],
            daemon=DaemonConfig(fix_idle_timeout_sec=5),
        ),
        h._FakeRedis(),
        *h._usage_providers(),
    )
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_fix())
    assert captured.get("timeout") is None


def test_handle_fix_external_merge_during_coder_transitions_to_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A polling task that detects MERGED while the coder runs must drive IDLE."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        runner_module.PipelineRunner, "_mark_queue_done", lambda self: None
    )

    runner = h._make_runner()
    runner._app_config = h._app_cfg(fix_poll_interval_sec=1)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=77, branch="pr-077", no_push_fix_count=1, fix_iteration_count=3,
    )

    poll_started: asyncio.Event | None = None

    async def fake_poll(
        self: object,
        pr_number: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        terminal_flag: dict[str, str | None],
    ) -> None:
        nonlocal poll_started
        poll_started = asyncio.Event()
        terminal_flag["state"] = "MERGED"
        target.cancel()

    monkeypatch.setattr(
        fix_module.FixMixin, "_poll_github_during_fix", fake_poll
    )

    async def fake_fix_review_async(
        *args: object, **kwargs: object
    ) -> tuple[int, str, str]:
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            raise
        return (0, "", "")

    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix_review_async)
    monkeypatch.setattr(
        runner_module.github_client, "post_comment", lambda *a, **kw: None
    )

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.error_message is None
    assert any(
        "merged externally during FIX" in e["event"]
        for e in runner.state.history
    )


def test_handle_fix_external_close_during_coder_transitions_to_hung(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A polling task that detects CLOSED while the coder runs must park HUNG."""
    h._patch_subprocess(monkeypatch)

    runner = h._make_runner()
    runner._app_config = h._app_cfg(fix_poll_interval_sec=1)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=78, branch="pr-078")

    async def fake_poll(
        self: object,
        pr_number: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        terminal_flag: dict[str, str | None],
    ) -> None:
        terminal_flag["state"] = "CLOSED"
        target.cancel()

    monkeypatch.setattr(
        fix_module.FixMixin, "_poll_github_during_fix", fake_poll
    )

    async def fake_fix_review_async(
        *args: object, **kwargs: object
    ) -> tuple[int, str, str]:
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            raise
        return (0, "", "")

    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix_review_async)
    monkeypatch.setattr(
        runner_module.github_client, "post_comment", lambda *a, **kw: None
    )

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.HUNG
    assert any(
        "closed externally during FIX" in e["event"]
        for e in runner.state.history
    )


def test_handle_fix_normal_completion_cancels_polling_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the coder exits normally, the polling task must be cancelled cleanly."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli, "fix_review_async", h._async_cli_result(0, "", "")
    )
    monkeypatch.setattr(
        runner_module.github_client, "post_comment", lambda *a, **kw: None
    )

    cancellations: list[bool] = []

    async def fake_poll(
        self: object,
        pr_number: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        terminal_flag: dict[str, str | None],
    ) -> None:
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancellations.append(True)
            raise

    monkeypatch.setattr(
        fix_module.FixMixin, "_poll_github_during_fix", fake_poll
    )

    runner = h._make_runner()
    runner._app_config = h._app_cfg(fix_poll_interval_sec=1)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=79, branch="pr-079")

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.WATCH
    assert cancellations == [True]


def test_handle_fix_external_merge_when_coder_exits_during_grace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the polling task sets the terminal flag but the coder finishes
    naturally before the cancellation lands (so ``target.cancel()`` is a
    no-op and ``await claude_task`` returns normally), the post-finally
    branch must still drive IDLE on MERGED (Codex P1 on PR #223)."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        runner_module.PipelineRunner, "_mark_queue_done", lambda self: None
    )
    monkeypatch.setattr(
        runner_module.github_client, "post_comment", lambda *a, **kw: None
    )

    async def fake_poll(
        self: object,
        pr_number: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        terminal_flag: dict[str, str | None],
    ) -> None:
        # Set the flag but do not cancel ``target``: simulate the race
        # where the coder finishes during the SIGTERM grace.
        terminal_flag["state"] = "MERGED"

    monkeypatch.setattr(
        fix_module.FixMixin, "_poll_github_during_fix", fake_poll
    )
    monkeypatch.setattr(
        claude_cli, "fix_review_async", h._async_cli_result(0, "", "")
    )

    runner = h._make_runner()
    runner._app_config = h._app_cfg(fix_poll_interval_sec=1)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=81, branch="pr-081", no_push_fix_count=1, fix_iteration_count=2,
    )

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.error_message is None
    assert any(
        "merged externally during FIX" in e["event"]
        for e in runner.state.history
    )


def test_handle_fix_records_last_push_at(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """handle_fix must set ``_last_push_at`` so the next handle_watch can
    compare Codex feedback against our actual push time, not GitHub's
    ``updatedAt`` (which advances every time Codex posts)."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "fix_review_async",
        h._async_cli_result(0, "", ""),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda *a, **kw: None,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=7, branch="pr-001")
    before = datetime.now(timezone.utc)
    asyncio.run(runner.handle_fix())
    after = datetime.now(timezone.utc)

    assert runner._last_push_at is not None
    assert before <= runner._last_push_at <= after


def test_handle_fix_uses_async(monkeypatch: pytest.MonkeyPatch) -> None:
    """handle_fix must call fix_review_async, not the sync version."""
    h._patch_subprocess(monkeypatch)
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

    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=10, branch="pr-001")
    asyncio.run(runner.handle_fix())

    assert async_calls, "fix_review_async must be called"
    assert not sync_calls, "sync fix_review must NOT be called"


def test_handle_fix_publishes_heartbeat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """publish_state is called during FIX FEEDBACK via heartbeat task."""
    h._patch_subprocess(monkeypatch)
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
        runner = h._make_runner()
        runner.state.current_pr = PRInfo(number=10, branch="pr-001")
        task = asyncio.create_task(runner.handle_fix())
        await asyncio.wait_for(two_heartbeats.wait(), timeout=1)
        cli_done.set_result(None)
        await task

    asyncio.run(run())

    assert len(heartbeat_publishes) >= 2, (
        f"Expected heartbeat to publish at least twice, got {len(heartbeat_publishes)}"
    )


def test_handle_fix_skips_review_post_when_head_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-050: when FIX exits 0 but HEAD hasn't moved, handle_fix must
    skip push accounting and @codex review, returning to WATCH."""
    same_sha = "abc123"

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:2] == ["git", "rev-parse"] and "HEAD" in cmd:
            return h._FakeCompletedProcess(
                args=cmd, stdout=f"{same_sha}\n", returncode=0
            )
        if cmd[:2] == ["git", "rev-list"]:
            return h._FakeCompletedProcess(
                args=cmd, stdout="0\n", returncode=0
            )
        return h._FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        claude_cli,
        "fix_review_async",
        h._async_cli_result(0, "", ""),
    )
    posted: list[str] = []
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda *a, **kw: posted.append("posted"),
    )

    runner = h._make_runner()
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

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:3] == ["git", "rev-parse", "HEAD"]:
            call_count["n"] += 1
            sha = sha_before if call_count["n"] == 1 else sha_after
            return h._FakeCompletedProcess(
                args=cmd, stdout=f"{sha}\n", returncode=0
            )
        if cmd[:2] == ["git", "rev-parse"] and "--abbrev-ref" in cmd:
            return h._FakeCompletedProcess(
                args=cmd, stdout="pr-050\n", returncode=0
            )
        if cmd[:2] == ["git", "rev-list"]:
            return h._FakeCompletedProcess(
                args=cmd, stdout="0\n", returncode=0
            )
        return h._FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        claude_cli,
        "fix_review_async",
        h._async_cli_result(0, "", ""),
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda *a, **kw: None,
    )

    runner = h._make_runner()
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

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:2] == ["git", "rev-parse"] and "HEAD" in cmd:
            call_count["n"] += 1
            if call_count["n"] == 1:
                return h._FakeCompletedProcess(
                    args=cmd, stdout="aaa111\n", returncode=0
                )
            # Second call: simulate failure
            raise subprocess.CalledProcessError(
                128, cmd, stderr="fatal: bad object HEAD"
            )
        if cmd[:2] == ["git", "rev-list"]:
            return h._FakeCompletedProcess(
                args=cmd, stdout="0\n", returncode=0
            )
        return h._FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        claude_cli,
        "fix_review_async",
        h._async_cli_result(0, "", ""),
    )

    runner = h._make_runner()
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

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            if rev_parse_calls["count"] == 1:
                raise subprocess.CalledProcessError(
                    128, ["git", *args], stderr="fatal: bad object HEAD"
                )
            return h._FakeCompletedProcess(
                args=["git", *args], stdout="bbb222\n", returncode=0
            )
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

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(
        claude_cli, "fix_review_async", h._async_cli_result(0, "ok", "")
    )
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", no_idle_monitor)
    monkeypatch.setattr(
        PipelineRunner, "_monitor_inflight_breach", no_breach_monitor
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is None
    assert any("Fix pushed, iteration #0" in e["event"] for e in runner.state.history)


def test_handle_fix_reraises_unexpected_cancelled_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only breach and idle cancellations are swallowed by handle_fix."""

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        return h._FakeCompletedProcess(args=["git", *args], stdout="aaa111\n", returncode=0)

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

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(runner.handle_fix())


def test_handle_fix_normal_push_skips_codex_review_when_eyes_already_reacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OBS-Z: normal fix-push path honors the EYES race-window dedup."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli, "fix_review_async", h._async_cli_result(0, "", "")
    )
    posted: list[tuple[str, int, str]] = []

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    monkeypatch.setattr(runner_module.github_client, "post_comment", fake_post)
    h._patch_eyes_reaction_present(monkeypatch)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=77, branch="pr-019")

    asyncio.run(runner.handle_fix())

    assert posted == []
    assert any(
        "Codex auto-trigger detected, skipping duplicate "
        "@codex review post" in e["event"]
        for e in runner.state.history
    )


def test_handle_fix_normal_push_posts_codex_review_when_eyes_predates_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OBS-Z: a stale EYES (predates new head) must NOT suppress the post.

    Without head-freshness gating, a prior EYES reaction would silently
    suppress ``_post_codex_review`` after every FIX push, leaving the
    new commit without a review trigger until the 1-hour stale-retrigger
    debounce in ``watch.py`` recovered.
    """
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli, "fix_review_async", h._async_cli_result(0, "", "")
    )
    posted: list[tuple[str, int, str]] = []

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    monkeypatch.setattr(runner_module.github_client, "post_comment", fake_post)
    h._patch_eyes_reaction_stale(monkeypatch)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=77, branch="pr-019")

    asyncio.run(runner.handle_fix())

    assert posted == [(runner.owner_repo, 77, "@codex review")]
    assert not any(
        "Codex auto-trigger detected, skipping duplicate "
        "@codex review post" in e["event"]
        for e in runner.state.history
    )


def test_handle_fix_uses_codex_cli_when_coder_is_codex(
    monkeypatch: pytest.MonkeyPatch,
) -> None:

    h._patch_subprocess(monkeypatch)
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

    runner = h._make_runner(coder=CoderType.CODEX)
    runner.state.current_pr = PRInfo(
        number=42,
        url="https://github.com/octo/demo/pull/42",
        branch="fix-branch",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.CHANGES_REQUESTED,
    )
    asyncio.run(runner.handle_fix())

    assert captured_module == ["codex"]


def test_handle_fix_head_unchanged_honors_stop_requested_after_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:2] == ["git", "rev-parse"] and "HEAD" in cmd:
            return h._FakeCompletedProcess(args=cmd, stdout="abc123\n", returncode=0)
        if cmd[:2] == ["git", "rev-list"]:
            return h._FakeCompletedProcess(args=cmd, stdout="0\n", returncode=0)
        return h._FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    async def fake_pop_stop_request() -> bool:
        return True

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(claude_cli, "fix_review_async", h._async_cli_result(0, "", ""))

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=50, branch="pr-050")
    monkeypatch.setattr(runner, "_pop_stop_request", fake_pop_stop_request)

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.error_message is None
    assert any(
        entry["event"] == "[FIX] FIX aborted: user stop requested."
        for entry in runner.state.history
    )


def test_handle_fix_stop_cancel_returns_when_rev_parse_after_fix_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rev_parse_calls = {"count": 0}

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            if rev_parse_calls["count"] == 1:
                return h._FakeCompletedProcess(
                    args=["git", *args], stdout="aaa111\n", returncode=0
                )
            raise subprocess.CalledProcessError(128, ["git", *args], stderr="boom")
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

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

    runner = h._make_runner()

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

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args and args[0] == "fetch":
            fetch_calls["count"] += 1
            if fetch_calls["count"] == 2:
                raise subprocess.CalledProcessError(1, ["git", *args], stderr="fetch fail")
            return h._FakeCompletedProcess(args=["git", *args], returncode=0)
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

    runner = h._make_runner()

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

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_head_calls["count"] += 1
            sha = "aaa111\n" if rev_parse_head_calls["count"] == 1 else "bbb222\n"
            return h._FakeCompletedProcess(args=["git", *args], stdout=sha, returncode=0)
        if args[:2] == ("fetch", "origin"):
            return h._FakeCompletedProcess(args=["git", *args], returncode=0)
        if args[:2] == ("rev-parse", "origin/pr-042-fix"):
            raise subprocess.CalledProcessError(128, ["git", *args], stderr="bad ref")
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

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

    runner = h._make_runner()

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

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_head_calls["count"] += 1
            sha = "aaa111\n" if rev_parse_head_calls["count"] == 1 else "bbb222\n"
            return h._FakeCompletedProcess(args=["git", *args], stdout=sha, returncode=0)
        if args[:2] == ("fetch", "origin"):
            return h._FakeCompletedProcess(args=["git", *args], returncode=0)
        if args[:2] == ("rev-parse", "origin/pr-042-fix"):
            return h._FakeCompletedProcess(args=["git", *args], stdout="ccc333\n", returncode=0)
        if args[:2] == ("merge-base", "--is-ancestor"):
            raise OSError("merge-base fail")
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

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

    runner = h._make_runner()

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
    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            return h._FakeCompletedProcess(
                args=["git", *args], stdout="aaa111\n", returncode=0
            )
        if args[:2] == ("fetch", "origin"):
            return h._FakeCompletedProcess(args=["git", *args], returncode=0)
        if args[:2] == ("rev-parse", "origin/pr-042-fix"):
            return h._FakeCompletedProcess(args=["git", *args], stdout="aaa111\n", returncode=0)
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

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

    runner = h._make_runner()

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

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            sha = "aaa111\n" if rev_parse_calls["count"] == 1 else "bbb222\n"
            return h._FakeCompletedProcess(args=["git", *args], stdout=sha, returncode=0)
        if args[:2] == ("fetch", "origin"):
            return h._FakeCompletedProcess(args=["git", *args], returncode=0)
        if args[:2] == ("rev-parse", "origin/pr-042-fix"):
            return h._FakeCompletedProcess(args=["git", *args], stdout="bbb222\n", returncode=0)
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

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

    runner = h._make_runner()

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


def test_handle_fix_normal_exit_records_push_when_remote_contains_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Normal FIX exit + ``_verify_pushes_since`` confirms origin has the
    new commit: the runner records the push, posts ``@codex review``, and
    transitions to WATCH (the existing happy path is unchanged)."""
    rev_parse_calls = {"count": 0}
    posted: list[int] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            sha = "aaa111\n" if rev_parse_calls["count"] == 1 else "bbb222\n"
            return h._FakeCompletedProcess(args=["git", *args], stdout=sha, returncode=0)
        if args[:2] == ("rev-parse", "origin/pr-190"):
            return h._FakeCompletedProcess(
                args=["git", *args], stdout="bbb222\n", returncode=0,
            )
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

    async def fake_fix(*args: object, **kwargs: object) -> tuple[int, str, str]:
        return (0, "", "")

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)
    monkeypatch.setattr(
        PipelineRunner, "_monitor_fix_idle", h._pr190_no_idle_monitor_async
    )
    monkeypatch.setattr(
        PipelineRunner, "_monitor_inflight_breach", h._pr190_no_breach_monitor_async
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=190, branch="pr-190")
    monkeypatch.setattr(
        runner,
        "_post_codex_review",
        lambda pr_number: posted.append(pr_number) or True,
    )

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 1
    assert runner.state.current_pr.fix_iteration_count == 1
    assert runner.state.current_pr.no_push_fix_count == 0
    assert posted == [190]


def test_handle_fix_normal_exit_treats_unverified_push_as_no_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Normal FIX exit + the local HEAD moved but ``origin/<branch>`` is
    still at the pre-FIX SHA: the daemon must log the no-push event,
    increment ``no_push_fix_count``, skip ``@codex review``, and return
    to WATCH (no inadvertent fix/push loop)."""
    rev_parse_calls = {"count": 0}
    posted: list[int] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            sha = "aaa111\n" if rev_parse_calls["count"] == 1 else "bbb222\n"
            return h._FakeCompletedProcess(args=["git", *args], stdout=sha, returncode=0)
        if args[:2] == ("rev-parse", "origin/pr-190"):
            return h._FakeCompletedProcess(
                args=["git", *args], stdout="aaa111\n", returncode=0,
            )
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

    async def fake_fix(*args: object, **kwargs: object) -> tuple[int, str, str]:
        return (0, "", "")

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)
    monkeypatch.setattr(
        PipelineRunner, "_monitor_fix_idle", h._pr190_no_idle_monitor_async
    )
    monkeypatch.setattr(
        PipelineRunner, "_monitor_inflight_breach", h._pr190_no_breach_monitor_async
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=190, branch="pr-190")
    monkeypatch.setattr(
        runner,
        "_post_codex_review",
        lambda pr_number: posted.append(pr_number) or True,
    )

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 0
    assert runner.state.current_pr.fix_iteration_count == 0
    assert runner.state.current_pr.no_push_fix_count == 1
    assert posted == []
    assert any(
        "Coder exited cleanly but no push detected" in e["event"]
        for e in runner.state.history
    )


def test_handle_fix_normal_exit_fails_open_when_verification_fetch_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Normal FIX exit + verification fetch fails: per the fail-open
    rule shared with PR-189, ``handle_fix`` logs a warning and proceeds
    optimistically, treating the FIX as a successful push."""
    rev_parse_calls = {"count": 0}
    fetch_calls = {"count": 0}
    posted: list[int] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args and args[0] == "fetch":
            fetch_calls["count"] += 1
            # The first fetch is the pre-FIX checkout/reset prep; the
            # second fetch is the verification fetch we want to fail.
            if fetch_calls["count"] == 2:
                raise subprocess.CalledProcessError(
                    1, ["git", *args], stderr="fetch fail",
                )
            return h._FakeCompletedProcess(args=["git", *args], returncode=0)
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            sha = "aaa111\n" if rev_parse_calls["count"] == 1 else "bbb222\n"
            return h._FakeCompletedProcess(args=["git", *args], stdout=sha, returncode=0)
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

    async def fake_fix(*args: object, **kwargs: object) -> tuple[int, str, str]:
        return (0, "", "")

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)
    monkeypatch.setattr(
        PipelineRunner, "_monitor_fix_idle", h._pr190_no_idle_monitor_async
    )
    monkeypatch.setattr(
        PipelineRunner, "_monitor_inflight_breach", h._pr190_no_breach_monitor_async
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=190, branch="pr-190")
    monkeypatch.setattr(
        runner,
        "_post_codex_review",
        lambda pr_number: posted.append(pr_number) or True,
    )

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 1
    assert runner.state.current_pr.fix_iteration_count == 1
    assert posted == [190]
    assert any(
        "FIX push verification unavailable; proceeding optimistically"
        in e["event"]
        for e in runner.state.history
    )
    assert any(
        "fetch pr-190 failed after FIX exit" in e["event"]
        for e in runner.state.history
    )
