"""PR-224a: handle_fix handler tests for src/daemon/runner.py

Mechanical move from tests/test_runner.py. Helpers and fixtures still live in
tests/test_runner.py and are referenced via the ``h`` alias.
"""

from __future__ import annotations

import asyncio
import contextlib
import subprocess
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from src import codex_cli
from src.coders import claude as claude_plugin_module
from src.config import AppConfig, CoderType, DaemonConfig
from src.daemon import git_ops as git_ops_module
from src.daemon import recovery_policy as recovery_policy_module
from src.daemon import runner as runner_module
from src.daemon import fix_escalation as fix_escalation_module
from src.daemon import fix_supervision as fix_supervision_module
from src.daemon.handlers import fix as fix_module
from src.daemon.handlers import hung as hung_module
from src.daemon.runner import PipelineRunner
from src.models import (
    CIStatus,
    FeedbackCheckResult,
    PipelineState,
    PRInfo,
    QueueTask,
    ReviewStatus,
    TaskStatus,
)

from tests.runner import _helpers as h

claude_cli = claude_plugin_module.claude_cli


def test_handle_fix_posts_codex_review_after_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-019: after a successful fix push, ``handle_fix`` must post
    ``@codex review`` so Codex reviews the freshly-pushed iteration."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(claude_cli, "fix_review_async", h._async_cli_result(0, "", ""))
    posted: list[tuple[str, int, str]] = []

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    monkeypatch.setattr("src.github.comments.post_comment", fake_post)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=77, branch="pr-019")
    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 1
    assert runner.state.current_pr.fix_iteration_count == 1
    assert posted == [(runner.owner_repo, 77, "@codex review")]
    assert any("Posted @codex review on PR #77" in e["event"] for e in runner.state.history)


def test_handle_fix_injects_ci_logs_when_ci_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ci_status=FAILURE: handle_fix passes CI logs into the FIX prompt."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        fix_module,
        "_fetch_failed_ci_logs",
        lambda repo, branch: "pytest assertion error: boom",
    )
    monkeypatch.setattr(
        "src.github.comments.get_latest_codex_feedback",
        lambda repo, pr_number: None,
    )
    captured = h._capture_fix_kwargs(monkeypatch)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=77, branch="pr-019", ci_status=CIStatus.FAILURE)
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
        fix_module,
        "_fetch_failed_ci_logs",
        lambda repo, branch: None,
    )
    monkeypatch.setattr(
        "src.github.comments.get_latest_codex_feedback",
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
        fix_module,
        "_fetch_failed_ci_logs",
        lambda repo, branch: "ci-boom",
    )
    monkeypatch.setattr(
        "src.github.comments.get_latest_codex_feedback",
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
    monkeypatch.setattr(claude_cli, "fix_review_async", h._async_cli_result(0, "", ""))
    posted: list[tuple[str, int, str]] = []

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    monkeypatch.setattr("src.github.comments.post_comment", fake_post)

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
        "deferring pause until fix bookkeeping completes" in entry["event"].lower() for entry in runner.state.history
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
    assert any("user stop requested" in entry["event"].lower() for entry in runner.state.history)


def test_handle_fix_escalates_at_iteration_cap_before_next_spawn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[tuple[str, int, str]] = []
    gh_calls: list[list[str]] = []
    fix_called: list[bool] = []

    class _UnexpectedPlugin:
        async def fix_review(self, path: str, **kwargs: object) -> tuple[int, str, str]:
            fix_called.append(True)
            return (0, "", "")

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        gh_calls.append(cmd)
        return ""

    monkeypatch.setattr("src.github.comments.post_comment", fake_post)
    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

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
            "@AlexBomber12 FIX iteration cap reached (15/15). Escalating for manual review.",
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
        entry["event"] == "[ESCALATE] FIX cap reached (15/15) on PR #77: escalated, moving to IDLE."
        for entry in runner.state.history
    )


def test_handle_fix_cap_ignores_existing_label_create_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[tuple[str, int, str]] = []
    gh_calls: list[list[str]] = []

    class _UnexpectedPlugin:
        async def fix_review(self, path: str, **kwargs: object) -> tuple[int, str, str]:
            raise AssertionError("fix_review should not run at cap boundary")

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        gh_calls.append(cmd)
        if cmd[:3] == ["label", "create", "escalated"]:
            raise RuntimeError("label already exists")
        return ""

    monkeypatch.setattr("src.github.comments.post_comment", fake_post)
    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

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
            "@AlexBomber12 FIX iteration cap reached (2/2). Escalating for manual review.",
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
        async def fix_review(self, path: str, **kwargs: object) -> tuple[int, str, str]:
            raise AssertionError("fix_review should not run at cap boundary")

    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
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
        entry["event"] == "[FIX] FIX blocked for escalated PR #91, moving to IDLE." for entry in runner.state.history
    )


def test_handle_fix_blocks_escalated_pr_even_when_counter_resets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[tuple[str, int, str]] = []
    gh_calls: list[list[str]] = []

    class _UnexpectedPlugin:
        async def fix_review(self, path: str, **kwargs: object) -> tuple[int, str, str]:
            raise AssertionError("fix_review should not run for escalated PRs")

    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
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
        entry["event"] == "[FIX] FIX blocked for escalated PR #92, moving to IDLE." for entry in runner.state.history
    )


def test_handle_fix_cap_sets_error_when_comment_post_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _UnexpectedPlugin:
        async def fix_review(self, path: str, **kwargs: object) -> tuple[int, str, str]:
            raise AssertionError("fix_review should not run at cap boundary")

    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: (_ for _ in ()).throw(RuntimeError("gh unavailable")),
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
        async def fix_review(self, path: str, **kwargs: object) -> tuple[int, str, str]:
            raise AssertionError("fix_review should not run at cap boundary")

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        if cmd[:2] == ["pr", "edit"]:
            raise RuntimeError("add label failed")
        return ""

    monkeypatch.setattr("src.github.comments.post_comment", fake_post)
    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

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
            "@AlexBomber12 FIX iteration cap reached (2/2). Escalating for manual review.",
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
        async def fix_review(self, path: str, **kwargs: object) -> tuple[int, str, str]:
            raise AssertionError("fix_review must not run at cap boundary")

    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
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


def test_handle_fix_three_no_push_cycles_cancel_to_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-258: three consecutive no-push FIX cycles cancel the task and
    park the runner in IDLE rather than HUNG."""
    posted = h._patch_no_push_fix(monkeypatch, head_seq=lambda: "abc123")

    runner = h._make_runner()
    runner._app_config = h._app_cfg(fix_no_push_cap=3)
    runner.state.state = PipelineState.WATCH
    runner.state.current_task = QueueTask(
        pr_id="PR-217",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-217",
    )
    runner.state.current_pr = PRInfo(number=217, branch="pr-217")

    asyncio.run(runner.handle_fix())
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.no_push_fix_count == 1

    asyncio.run(runner.handle_fix())
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr.no_push_fix_count == 2

    asyncio.run(runner.handle_fix())
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert "PR-217" in runner._recovered_task_pr_ids
    assert posted == []
    assert any(
        "PR #217 no-push deadlock after 3 attempts; canceling task"
        in entry["event"]
        for entry in runner.state.history
    )


def test_handle_fix_productive_push_resets_no_push_counter_before_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two no-push cycles followed by a productive push reset the counter."""
    seq = iter(
        [
            "aaa000",  # call 1 head_before
            "aaa000",  # call 1 head_after  → no-push
            "aaa000",  # call 2 head_before
            "aaa000",  # call 2 head_after  → no-push
            "aaa000",  # call 3 head_before
            "bbb111",  # call 3 head_after  → productive push
        ]
    )
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
    assert all("FIX deadlock" not in entry["event"] for entry in runner.state.history)
    assert all("FIX deadlock" not in body for _repo, _num, body in posted)


def test_handle_fix_no_push_counter_resets_between_productive_pushes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No-push, productive push, then no-push again must not trigger HUNG."""
    seq = iter(
        [
            "aaa000",  # call 1 head_before
            "aaa000",  # call 1 head_after  → no-push  (counter 0→1)
            "aaa000",  # call 2 head_before
            "bbb111",  # call 2 head_after  → productive push (counter 1→0)
            "bbb111",  # call 3 head_before
            "bbb111",  # call 3 head_after  → no-push  (counter 0→1)
        ]
    )
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
    assert all("FIX deadlock" not in entry["event"] for entry in runner.state.history)
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
    # counter advances orthogonally and trips cancellation without touching
    # the iteration cap.
    runner.state.current_task = QueueTask(
        pr_id="PR-220",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-220",
    )
    runner.state.current_pr = PRInfo(
        number=220,
        branch="pr-220",
        fix_iteration_count=2,
    )

    asyncio.run(runner.handle_fix())
    asyncio.run(runner.handle_fix())
    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert posted == []
    assert any(
        "PR #220 no-push deadlock after 3 attempts; canceling task"
        in entry["event"]
        for entry in runner.state.history
    )


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

    assert runner.state.state == PipelineState.IDLE
    assert "fix_no_push_cap" in maybe_escalate_calls


def test_handle_fix_no_push_deadlock_applies_canceled_label(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-258: the no-push cancellation surface adds a ``canceled`` label
    so the dashboard / reviewers can see the surrender state at a glance."""
    h._patch_no_push_fix(monkeypatch, head_seq=lambda: "abc123")
    gh_calls: list[list[str]] = []

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        gh_calls.append(cmd)
        return ""

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    runner = h._make_runner()
    runner._app_config = h._app_cfg(fix_no_push_cap=2)
    runner.state.state = PipelineState.WATCH
    runner.state.current_task = QueueTask(
        pr_id="PR-223",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-223",
    )
    runner.state.current_pr = PRInfo(number=223, branch="pr-223")

    asyncio.run(runner.handle_fix())
    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.IDLE
    canceled_calls = [
        cmd for cmd in gh_calls
        if cmd[:1] == ["label"] or cmd[:2] == ["pr", "edit"]
    ]
    assert canceled_calls == [
        [
            "label",
            "create",
            "canceled",
            "--color",
            "B60205",
            "--description",
            "Daemon canceled this task; manual recovery required",
        ],
        ["pr", "edit", "223", "--add-label", "canceled"],
    ]


def test_handle_fix_no_push_deadlock_label_failures_do_not_block_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-258: label create/add failures during cancellation must not
    block the IDLE transition; the cancellation cause in Redis is the
    durable signal."""
    h._patch_no_push_fix(monkeypatch, head_seq=lambda: "abc123")

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        if cmd[:3] == ["label", "create", "canceled"]:
            raise RuntimeError("label already exists")
        if cmd[:2] == ["pr", "edit"]:
            raise RuntimeError("gh down")
        return ""

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    runner = h._make_runner()
    runner._app_config = h._app_cfg(fix_no_push_cap=2)
    runner.state.state = PipelineState.WATCH
    runner.state.current_task = QueueTask(
        pr_id="PR-224",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-224",
    )
    runner.state.current_pr = PRInfo(number=224, branch="pr-224")

    asyncio.run(runner.handle_fix())
    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert any(
        "FIX no-push canceled label create skipped: label already exists"
        in entry["event"]
        for entry in runner.state.history
    )
    assert any(
        "failed to apply canceled label to PR #224: gh down"
        in entry["event"]
        for entry in runner.state.history
    )


def test_handle_fix_coder_escalate_transitions_to_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted, gh_calls = h._patch_fix_with_stdout(monkeypatch, stdout="working...\nESCALATE: rate limit exceeded\n")

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=300, branch="pr-300")

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.is_escalated is True
    expected_message = "Coder explicitly escalated this PR. Reason: rate limit exceeded. Manual review required."
    assert posted == [(runner.owner_repo, 300, expected_message)]
    assert [cmd for cmd in gh_calls if cmd[:1] == ["label"] or cmd[:2] == ["pr", "edit"]] == [
        [
            "label",
            "create",
            "escalated",
            "--color",
            "B60205",
            "--description",
            "Daemon escalated, manual review required",
        ],
        ["pr", "edit", "300", "--add-label", "escalated"],
    ]
    assert any(
        entry["event"] == "[ESCALATE] FIX coder ESCALATE on PR #300: rate limit exceeded. Moving to IDLE."
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
    posted, gh_calls = h._patch_fix_with_stdout(monkeypatch, stdout="ran tests\nall good\n")

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=302, branch="pr-302")

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.is_escalated is False
    assert posted == []
    assert all(cmd[:1] != ["label"] and cmd[:2] != ["pr", "edit"] for cmd in gh_calls)


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
    expected_message = "Coder explicitly escalated this PR. Reason: (no reason provided). Manual review required."
    assert posted == [(runner.owner_repo, 303, expected_message)]
    assert any("(no reason provided)" in entry["event"] for entry in runner.state.history)


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
            "Coder explicitly escalated this PR. Reason: cannot resolve. Manual review required.",
        )
    ]


def test_handle_fix_coder_escalate_post_failure_still_parks_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Comment-post failure must not block the IDLE transition."""
    h._patch_fix_with_stdout(monkeypatch, stdout="ESCALATE: cannot recover\n")
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: (_ for _ in ()).throw(RuntimeError("gh down")),
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=305, branch="pr-305")

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.is_escalated is True
    assert any(
        "failed to post FIX coder ESCALATE comment on PR #305" in entry["event"] for entry in runner.state.history
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
    posted, _ = h._patch_fix_with_stdout(monkeypatch, stdout="ESCALATE: infra error\n")

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        if cmd[:3] == ["label", "create", "escalated"]:
            raise RuntimeError("label already exists")
        if cmd[:2] == ["pr", "edit"]:
            raise RuntimeError("gh down")
        return ""

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

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
        "FIX coder ESCALATE label create skipped: label already exists" in entry["event"]
        for entry in runner.state.history
    )
    assert any(
        "failed to apply escalated label to PR #306: gh down" in entry["event"] for entry in runner.state.history
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
    posted, _ = h._patch_fix_with_stdout(monkeypatch, stdout="ESCALATE: handing off\n")

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
    assert any(entry["event"] == "[FIX] FIX aborted: user stop requested." for entry in runner.state.history)


def test_handle_fix_coder_escalate_resets_no_push_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Coder ESCALATE breaks the no-push streak: a deliberate bail-out
    must not feed the deadlock counter."""
    h._patch_fix_with_stdout(monkeypatch, stdout="ESCALATE: bail out\n")

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=307, branch="pr-307", no_push_fix_count=2)

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
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *a, **kw: "")

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
            return h._FakeCompletedProcess(args=["git", *args], stdout="aaa111\n", returncode=0)
        if args[:2] == ("rev-parse", "origin/pr-229"):
            return h._FakeCompletedProcess(args=["git", *args], stdout="aaa111\n", returncode=0)
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
    monkeypatch.setattr(PipelineRunner, "_monitor_inflight_breach", no_breach_monitor)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=229, branch="pr-229", no_push_fix_count=2)

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
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *a, **kw: "")

    async def idle_timeout_monitor(
        self: object,
        pr_number: int,
        idle_limit: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        idle_flag: dict[str, bool],
    ) -> None:
        idle_flag["timed_out"] = True
        target.cancel()

    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", idle_timeout_monitor)

    async def slow_fix(*args: object, **kwargs: object) -> tuple[int, str, str]:
        await asyncio.Future()
        return (0, "", "")

    monkeypatch.setattr(claude_cli, "fix_review_async", slow_fix)

    runner = h._make_runner()
    runner._app_config = h._app_cfg(fix_no_push_cap=2)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=226, branch="pr-226", no_push_fix_count=1)

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
    monkeypatch.setattr(PipelineRunner, "_monitor_inflight_breach", no_breach_monitor)

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
    monkeypatch.setattr(runner, "_post_codex_review", lambda pr_number: posted.append(pr_number) or True)

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
    monkeypatch.setattr(PipelineRunner, "_monitor_inflight_breach", no_breach_monitor)

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
    monkeypatch.setattr(runner, "_post_codex_review", lambda pr_number: posted.append(pr_number) or True)

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
    assert any("outside the fetched remote branch" in e["event"].lower() for e in runner.state.history)


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
    monkeypatch.setattr(PipelineRunner, "_monitor_inflight_breach", no_breach_monitor)

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
    monkeypatch.setattr(runner, "_post_codex_review", lambda pr_number: posted.append(pr_number) or True)

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
        "deferring pause until fix bookkeeping completes" in entry["event"].lower() for entry in runner.state.history
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
    monkeypatch.setattr(claude_cli, "fix_review_async", h._async_cli_result(0, "", ""))

    def boom(repo: str, number: int, body: str) -> None:
        raise RuntimeError("gh rate limited")

    monkeypatch.setattr("src.github.comments.post_comment", boom)

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
        "Warning: failed to post @codex review" in e["event"] and "gh rate limited" in e["event"]
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
    monkeypatch.setattr(claude_cli, "fix_review_async", h._async_cli_result(0, "", ""))
    monkeypatch.setattr(
        "src.github.comments.post_comment",
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
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042-fix")
    asyncio.run(runner.handle_fix())

    fetch_calls = [
        i for i, cmd in enumerate(calls) if cmd[:2] == ["git", "fetch"] and any("pr-042-fix" in arg for arg in cmd)
    ]
    checkout_calls = [i for i, cmd in enumerate(calls) if cmd[:2] == ["git", "checkout"] and "pr-042-fix" in cmd]
    reset_calls = [
        i
        for i, cmd in enumerate(calls)
        if cmd[:2] == ["git", "reset"] and "--hard" in cmd and "origin/pr-042-fix" in cmd
    ]
    assert fetch_calls, "expected git fetch origin pr-042-fix"
    assert all("--prune" in calls[i] for i in fetch_calls), (
        "git fetch in handle_fix must pass --prune to drop stale remote-tracking refs (PR-161)"
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
            raise subprocess.CalledProcessError(1, cmd, stderr="fatal: couldn't find remote ref pr-042-fix")
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
            raise subprocess.CalledProcessError(1, cmd, stderr="fatal: ambiguous argument")
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
        "src.github.comments.post_comment",
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
    assert any("Skipping FIX for cross-repo" in e["event"] for e in runner.state.history)


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
        "src.github.comments.post_comment",
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
    monkeypatch.setattr(runner_module.PipelineRunner, "_mark_queue_done", lambda self: None)

    runner = h._make_runner()
    runner._app_config = h._app_cfg(fix_poll_interval_sec=1)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=77,
        branch="pr-077",
        no_push_fix_count=1,
        fix_iteration_count=3,
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

    monkeypatch.setattr(fix_module.FixMixin, "_poll_github_during_fix", fake_poll)

    async def fake_fix_review_async(*args: object, **kwargs: object) -> tuple[int, str, str]:
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            raise
        return (0, "", "")

    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix_review_async)
    monkeypatch.setattr("src.github.comments.post_comment", lambda *a, **kw: None)

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.error_message is None
    assert any("merged externally during FIX" in e["event"] for e in runner.state.history)


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

    monkeypatch.setattr(fix_module.FixMixin, "_poll_github_during_fix", fake_poll)

    async def fake_fix_review_async(*args: object, **kwargs: object) -> tuple[int, str, str]:
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            raise
        return (0, "", "")

    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix_review_async)
    monkeypatch.setattr("src.github.comments.post_comment", lambda *a, **kw: None)

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.HUNG
    assert any("closed externally during FIX" in e["event"] for e in runner.state.history)


def test_handle_fix_normal_completion_cancels_polling_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the coder exits normally, the polling task must be cancelled cleanly."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(claude_cli, "fix_review_async", h._async_cli_result(0, "", ""))
    monkeypatch.setattr("src.github.comments.post_comment", lambda *a, **kw: None)

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

    monkeypatch.setattr(fix_module.FixMixin, "_poll_github_during_fix", fake_poll)

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
    monkeypatch.setattr(runner_module.PipelineRunner, "_mark_queue_done", lambda self: None)
    monkeypatch.setattr("src.github.comments.post_comment", lambda *a, **kw: None)

    async def fake_poll(
        self: object,
        pr_number: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        terminal_flag: dict[str, str | None],
    ) -> None:
        # Set the flag but do not cancel ``target``: simulate the race
        # where the coder finishes during the SIGTERM grace.
        terminal_flag["state"] = "MERGED"

    monkeypatch.setattr(fix_module.FixMixin, "_poll_github_during_fix", fake_poll)
    monkeypatch.setattr(claude_cli, "fix_review_async", h._async_cli_result(0, "", ""))

    runner = h._make_runner()
    runner._app_config = h._app_cfg(fix_poll_interval_sec=1)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=81,
        branch="pr-081",
        no_push_fix_count=1,
        fix_iteration_count=2,
    )

    asyncio.run(runner.handle_fix())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.error_message is None
    assert any("merged externally during FIX" in e["event"] for e in runner.state.history)


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
        "src.github.comments.post_comment",
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

    def fake_sync(path: str, model: str | None = None, timeout: int | None = None) -> tuple[int, str, str]:
        sync_calls.append(path)
        return (0, "", "")

    monkeypatch.setattr(claude_cli, "fix_review_async", fake_async)
    monkeypatch.setattr(claude_cli, "fix_review", fake_sync)
    monkeypatch.setattr(
        "src.github.comments.post_comment",
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
    monkeypatch.setattr("src.github.comments.post_comment", lambda *a, **kw: None)

    async def fast_heartbeat(self: Any, label: str) -> None:
        while True:
            await asyncio.sleep(0.01)
            self.log_event(f"{label}...")
            heartbeat_publishes.append(label)
            if len(heartbeat_publishes) >= 2:
                two_heartbeats.set()
            await self.publish_state()

    monkeypatch.setattr(PipelineRunner, "_publish_while_waiting", fast_heartbeat)

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
            return h._FakeCompletedProcess(args=cmd, stdout=f"{same_sha}\n", returncode=0)
        if cmd[:2] == ["git", "rev-list"]:
            return h._FakeCompletedProcess(args=cmd, stdout="0\n", returncode=0)
        return h._FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        claude_cli,
        "fix_review_async",
        h._async_cli_result(0, "", ""),
    )
    posted: list[str] = []
    monkeypatch.setattr(
        "src.github.comments.post_comment",
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
    assert any("HEAD unchanged" in e["event"] for e in runner.state.history)


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
            return h._FakeCompletedProcess(args=cmd, stdout=f"{sha}\n", returncode=0)
        if cmd[:2] == ["git", "rev-parse"] and "--abbrev-ref" in cmd:
            return h._FakeCompletedProcess(args=cmd, stdout="pr-050\n", returncode=0)
        if cmd[:2] == ["git", "rev-list"]:
            return h._FakeCompletedProcess(args=cmd, stdout="0\n", returncode=0)
        return h._FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        claude_cli,
        "fix_review_async",
        h._async_cli_result(0, "", ""),
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
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
                return h._FakeCompletedProcess(args=cmd, stdout="aaa111\n", returncode=0)
            # Second call: simulate failure
            raise subprocess.CalledProcessError(128, cmd, stderr="fatal: bad object HEAD")
        if cmd[:2] == ["git", "rev-list"]:
            return h._FakeCompletedProcess(args=cmd, stdout="0\n", returncode=0)
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
                raise subprocess.CalledProcessError(128, ["git", *args], stderr="fatal: bad object HEAD")
            return h._FakeCompletedProcess(args=["git", *args], stdout="bbb222\n", returncode=0)
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
    monkeypatch.setattr(claude_cli, "fix_review_async", h._async_cli_result(0, "ok", ""))
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", no_idle_monitor)
    monkeypatch.setattr(PipelineRunner, "_monitor_inflight_breach", no_breach_monitor)

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
    monkeypatch.setattr(PipelineRunner, "_monitor_inflight_breach", no_breach_monitor)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(runner.handle_fix())


def test_handle_fix_normal_push_skips_codex_review_when_eyes_already_reacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OBS-Z: normal fix-push path honors the EYES race-window dedup."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(claude_cli, "fix_review_async", h._async_cli_result(0, "", ""))
    posted: list[tuple[str, int, str]] = []

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    monkeypatch.setattr("src.github.comments.post_comment", fake_post)
    h._patch_eyes_reaction_present(monkeypatch)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=77, branch="pr-019")

    asyncio.run(runner.handle_fix())

    assert posted == []
    assert any(
        "Codex auto-trigger detected, skipping duplicate @codex review post" in e["event"] for e in runner.state.history
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
    monkeypatch.setattr(claude_cli, "fix_review_async", h._async_cli_result(0, "", ""))
    posted: list[tuple[str, int, str]] = []

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    monkeypatch.setattr("src.github.comments.post_comment", fake_post)
    h._patch_eyes_reaction_stale(monkeypatch)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=77, branch="pr-019")

    asyncio.run(runner.handle_fix())

    assert posted == [(runner.owner_repo, 77, "@codex review")]
    assert not any(
        "Codex auto-trigger detected, skipping duplicate @codex review post" in e["event"] for e in runner.state.history
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
        "src.github.comments.post_comment",
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
    assert any(entry["event"] == "[FIX] FIX aborted: user stop requested." for entry in runner.state.history)


def test_handle_fix_stop_cancel_returns_when_rev_parse_after_fix_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rev_parse_calls = {"count": 0}

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_calls["count"] += 1
            if rev_parse_calls["count"] == 1:
                return h._FakeCompletedProcess(args=["git", *args], stdout="aaa111\n", returncode=0)
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
    assert any("fetch pr-042-fix failed after FIX stop" in entry["event"] for entry in runner.state.history)


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
    assert any("rev-parse origin/pr-042-fix failed after FIX stop" in entry["event"] for entry in runner.state.history)


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
    assert any("merge-base ancestry check failed after FIX stop" in entry["event"] for entry in runner.state.history)


def test_handle_fix_stop_cancel_short_circuits_when_head_matches_before(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args[:2] == ("rev-parse", "HEAD"):
            return h._FakeCompletedProcess(args=["git", *args], stdout="aaa111\n", returncode=0)
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
    assert "Failed to post @codex review on PR #42 after stop-cancel fix push" in (runner.state.error_message or "")


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
                args=["git", *args],
                stdout="bbb222\n",
                returncode=0,
            )
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

    async def fake_fix(*args: object, **kwargs: object) -> tuple[int, str, str]:
        return (0, "", "")

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", h._pr190_no_idle_monitor_async)
    monkeypatch.setattr(PipelineRunner, "_monitor_inflight_breach", h._pr190_no_breach_monitor_async)

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
                args=["git", *args],
                stdout="aaa111\n",
                returncode=0,
            )
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

    async def fake_fix(*args: object, **kwargs: object) -> tuple[int, str, str]:
        return (0, "", "")

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix)
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", h._pr190_no_idle_monitor_async)
    monkeypatch.setattr(PipelineRunner, "_monitor_inflight_breach", h._pr190_no_breach_monitor_async)

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
    assert any("Coder exited cleanly but no push detected" in e["event"] for e in runner.state.history)


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
                    1,
                    ["git", *args],
                    stderr="fetch fail",
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
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", h._pr190_no_idle_monitor_async)
    monkeypatch.setattr(PipelineRunner, "_monitor_inflight_breach", h._pr190_no_breach_monitor_async)

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
        "FIX push verification unavailable; proceeding optimistically" in e["event"] for e in runner.state.history
    )
    assert any("fetch pr-190 failed after FIX exit" in e["event"] for e in runner.state.history)


# ---------------------------------------------------------------------------
# PR-224b moved from tests/test_runner.py — handle_fix group
# ---------------------------------------------------------------------------


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

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

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

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    assert fix_module._fetch_failed_ci_logs("octo/demo", "pr-019") == short_log


def test_fetch_failed_ci_logs_returns_none_when_no_failed_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda args, **kwargs: [])
    assert fix_module._fetch_failed_ci_logs("octo/demo", "pr-019") is None


def test_fetch_failed_ci_logs_returns_none_on_non_list_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda args, **kwargs: "not a list",
    )
    assert fix_module._fetch_failed_ci_logs("octo/demo", "pr-019") is None


def test_fetch_failed_ci_logs_returns_none_when_first_entry_not_dict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda args, **kwargs: ["unexpected-string"],
    )
    assert fix_module._fetch_failed_ci_logs("octo/demo", "pr-019") is None


def test_fetch_failed_ci_logs_returns_none_when_database_id_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
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

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    assert fix_module._fetch_failed_ci_logs("octo/demo", "pr-019") is None


def test_fetch_failed_ci_logs_returns_none_on_run_list_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str], **kwargs: Any) -> object:
        raise RuntimeError("api blew up")

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    assert fix_module._fetch_failed_ci_logs("octo/demo", "pr-019") is None


def test_fetch_failed_ci_logs_returns_none_on_run_view_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str], **kwargs: Any) -> object:
        if args[:2] == ["run", "list"]:
            return [{"databaseId": 9}]
        raise RuntimeError("log fetch blew up")

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    assert fix_module._fetch_failed_ci_logs("octo/demo", "pr-019") is None


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
    assert fix_escalation_module.parse_escalate_marker(stdout) == expected


def test_fix_increments_iterations(monkeypatch: pytest.MonkeyPatch) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(claude_cli, "fix_review_async", h._async_cli_result(0, "", ""))
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )

    runner = h._make_runner()
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
            return h._FakeCompletedProcess(
                args=["git", "rev-parse", "HEAD"],
                stdout="abc123\n",
                returncode=0,
            )
        if args[0] == "merge" and len(args) > 1 and args[1].startswith("origin/"):
            return h._FakeCompletedProcess(
                args=["git", *args],
                stdout="Already up to date.\n",
                returncode=0,
            )
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_parse_tasks_from_headers",
        lambda self: parsed_tasks,
    )
    monkeypatch.setattr(claude_cli, "fix_review_async", h._async_cli_result(0, "", ""))
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [PRInfo(number=77, branch="pr-001")],
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_metadata",
        lambda repo, number: {
            "author": "",
            "head_sha": "",
            "head_commit_date": "2026-04-18T12:00:00Z",
        },
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )
    monkeypatch.setattr("src.github.prs.merge_pr", lambda repo, num: None)
    monkeypatch.setattr(runner_module.PipelineRunner, "_mark_queue_done", lambda self: None)

    redis = h._FakeRedis()
    claude_provider, codex_provider = h._usage_providers()
    runner = PipelineRunner(
        h._repo_cfg(),
        h._app_cfg(),
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
        h._repo_cfg(),
        h._app_cfg(),
        redis,
        *h._usage_providers(),
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


def test_preserve_fix_iteration_count_counts_new_head_sha_after_upgrade() -> None:
    """``_preserve_fix_iteration_count`` is the IDLE-side mirror of the
    WATCH merge. A pre-PR-195 ``current_pr`` carrying a legacy
    ``push_count`` with empty ``observed_head_shas`` must bump the
    counter when IDLE rehydrates a freshly fetched PRInfo with a new
    head SHA — same regression, different polling path.
    """
    runner = h._make_runner()
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


def test_codex_review_not_reposted_same_pr_same_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The runner must not repost ``@codex review`` for the same PR
    when no new push has happened since the last post."""
    posted: list[tuple[str, int, str]] = []

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    monkeypatch.setattr("src.github.comments.post_comment", fake_post)
    monkeypatch.setattr(
        git_ops_module,
        "_git",
        lambda *args, **kwargs: h._FakeCompletedProcess(args=list(args), stdout="head-1\n", returncode=0),
    )
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-42", push_count=1)

    assert runner._post_codex_review(42) is True
    assert runner._post_codex_review(42) is True
    assert posted == [(runner.owner_repo, 42, "@codex review")]
    assert runner._last_codex_review_pr == 42
    assert runner._last_codex_review_head_sha == "head-1"
    assert any("Skipping duplicate @codex review for PR #42" in e["event"] for e in runner.state.history)


def test_codex_review_reposted_after_new_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A new push on the same PR must allow a fresh review trigger."""
    posted: list[tuple[str, int, str]] = []

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    monkeypatch.setattr("src.github.comments.post_comment", fake_post)
    head_shas = iter(["head-1\n", "head-2\n"])
    monkeypatch.setattr(
        git_ops_module,
        "_git",
        lambda *args, **kwargs: h._FakeCompletedProcess(args=list(args), stdout=next(head_shas), returncode=0),
    )
    runner = h._make_runner()
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

    monkeypatch.setattr("src.github.comments.post_comment", fake_post)
    monkeypatch.setattr(
        git_ops_module,
        "_git",
        lambda *args, **kwargs: h._FakeCompletedProcess(args=list(args), stdout="head-1\n", returncode=0),
    )
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-42", push_count=1)

    assert runner._post_codex_review(42) is True
    assert (
        runner._post_codex_review(
            42,
            bypass_same_head_dedup=True,
        )
        is True
    )

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

    runner = h._make_runner()

    monkeypatch.setattr("src.github.comments.post_comment", fake_post)
    monkeypatch.setattr(
        "src.github.prs.get_pr_metadata",
        lambda repo, number: {
            "author": "alice",
            "head_sha": "head-1",
            "head_commit_date": "2026-04-17T23:14:11Z",
        },
    )
    monkeypatch.setattr(
        "src.github.comments.has_recent_codex_review_request",
        lambda repo, number, pr_author, within_minutes=5, after_iso=None: (
            repo == runner.owner_repo and number == 42 and pr_author == "alice" and after_iso == "2026-04-17T23:14:11Z"
        ),
    )
    monkeypatch.setattr(
        git_ops_module,
        "_git",
        lambda *args, **kwargs: h._FakeCompletedProcess(args=list(args), stdout="head-1\n", returncode=0),
    )
    runner.state.current_pr = PRInfo(number=42, branch="pr-42", push_count=1)

    assert runner._post_codex_review(42) is True
    assert posted == []
    assert runner._last_codex_review_pr == 42
    assert runner._last_codex_review_head_sha == "head-1"
    assert any("PR author already requested review for this head" in e["event"] for e in runner.state.history)


def test_codex_review_result_returns_retry_at_for_author_dedup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requested_at = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
    runner = h._make_runner()

    monkeypatch.setattr(
        "src.github.prs.get_pr_metadata",
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
        lambda *args, **kwargs: h._FakeCompletedProcess(args=list(args), stdout="head-1\n", returncode=0),
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

    def fake_git(*args: object, **kwargs: object) -> h._FakeCompletedProcess:
        raise RuntimeError("git rev-parse failed")

    monkeypatch.setattr("src.github.comments.post_comment", fake_post)
    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-42", push_count=1)

    assert runner._post_codex_review(42) is True
    assert runner._post_codex_review(42) is True
    assert posted == [
        (runner.owner_repo, 42, "@codex review"),
        (runner.owner_repo, 42, "@codex review"),
    ]
    assert runner._last_codex_review_pr is None
    assert runner._last_codex_review_head_sha is None
    assert any("posting @codex review without dedup" in e["event"] for e in runner.state.history)


def test_codex_review_metadata_failure_posts_without_pr_author_dedup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Metadata lookup failures must not escape or suppress review posts."""
    posted: list[tuple[str, int, str]] = []

    def fake_post(repo: str, number: int, body: str) -> None:
        posted.append((repo, number, body))

    monkeypatch.setattr("src.github.comments.post_comment", fake_post)
    monkeypatch.setattr(
        "src.github.prs.get_pr_metadata",
        lambda repo, number: (_ for _ in ()).throw(OSError("gh timed out")),
    )
    monkeypatch.setattr(
        git_ops_module,
        "_git",
        lambda *args, **kwargs: h._FakeCompletedProcess(args=list(args), stdout="head-1\n", returncode=0),
    )
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-42", push_count=1)

    assert runner._post_codex_review(42) is True
    assert posted == [(runner.owner_repo, 42, "@codex review")]
    assert runner._last_codex_review_pr == 42
    assert runner._last_codex_review_head_sha == "head-1"
    assert any("failed to load PR metadata for @codex review dedup" in e["event"] for e in runner.state.history)


def test_author_already_requested_review_fails_open_on_lookup_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github.comments.has_recent_codex_review_request",
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
        "src.github.comments.get_recent_codex_review_request_time",
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

    monkeypatch.setattr("src.github.comments.post_comment", boom)
    monkeypatch.setattr(
        git_ops_module,
        "_git",
        lambda *args, **kwargs: h._FakeCompletedProcess(args=list(args), stdout="head-1\n", returncode=0),
    )

    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-42", push_count=1)

    assert runner._post_codex_review(42) is False
    assert runner._last_codex_review_pr is None
    assert runner._last_codex_review_head_sha is None
    assert any(
        "Warning: failed to post @codex review on PR #42: gh rate limited" in entry["event"]
        for entry in runner.state.history
    )


def test_fix_idle_timeout_kills_on_no_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Claude task must be cancelled when no push is detected within idle limit."""
    h._patch_subprocess(monkeypatch)

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
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", immediate_cancel_monitor)
    monkeypatch.setattr(
        "src.github.comments.post_comment",
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
    assert runner.state.state == PipelineState.ERROR
    assert "idle timeout" in (runner.state.error_message or "")


def test_fix_idle_timeout_defers_to_user_stop_after_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stop consumed alongside idle timeout should pause instead of erroring."""
    h._patch_subprocess(monkeypatch)
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
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", immediate_cancel_monitor)
    monkeypatch.setattr(
        "src.github.comments.post_comment",
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
    h._patch_subprocess(monkeypatch)

    async def fake_fix_quick(
        path: str, model: str | None = None, timeout: int | None = None, **kwargs: object
    ) -> tuple[int, str, str]:
        await asyncio.sleep(0)
        return (0, "", "")

    monkeypatch.setattr(claude_cli, "fix_review_async", fake_fix_quick)
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda *a, **kw: None,
    )

    runner = PipelineRunner(
        h._repo_cfg(),
        AppConfig(
            repositories=[],
            daemon=DaemonConfig(fix_idle_timeout_sec=1800),
        ),
        h._FakeRedis(),
        *h._usage_providers(),
    )
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_fix())
    assert runner.state.state == PipelineState.WATCH


def test_fix_idle_timeout_monitor_resets_on_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Push detection resets the idle timer so a productive session is not killed."""
    h._patch_subprocess(monkeypatch)

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
    monkeypatch.setattr(PipelineRunner, "_monitor_fix_idle", monitor_with_push_then_finish)
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda *a, **kw: None,
    )

    runner = PipelineRunner(
        h._repo_cfg(),
        AppConfig(
            repositories=[],
            daemon=DaemonConfig(fix_idle_timeout_sec=1800),
        ),
        h._FakeRedis(),
        *h._usage_providers(),
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
    runner = h._make_runner()
    events: list[str] = []
    monkeypatch.setattr(runner, "log_event", events.append)

    branch_results: list[object] = [
        fix_supervision_module.gh_prs.GitHubPollError("bootstrap failed"),
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
        "src.github.prs.get_branch_last_push_time",
        fake_branch_last_push,
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, pr: None,
    )
    monkeypatch.setattr(fix_supervision_module.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(fix_supervision_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(
        fix_supervision_module,
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
    runner = h._make_runner()

    async def fake_to_thread(func: Any, *args: object, **kwargs: object) -> Any:
        return func(*args, **kwargs)

    async def fake_sleep(delay: float) -> None:
        return None

    monotonic_values = iter([100.0, 250.0])
    monkeypatch.setattr(
        "src.github.prs.get_branch_last_push_time",
        lambda repo, pr: None,
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, pr: 30.0,
    )
    monkeypatch.setattr(fix_supervision_module.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(fix_supervision_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(
        fix_supervision_module,
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
    runner = h._make_runner()
    runner.state.coder = "codex"
    events: list[str] = []
    monkeypatch.setattr(runner, "log_event", events.append)

    class _StopMonitor(Exception):
        pass

    branch_results: list[object] = [
        fix_supervision_module.gh_prs.GitHubPollError("bootstrap failed"),
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
        "src.github.prs.get_branch_last_push_time",
        fake_branch_last_push,
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, pr: None,
    )
    monkeypatch.setattr(fix_supervision_module.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(fix_supervision_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(
        fix_supervision_module,
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
    runner = h._make_runner()
    events: list[str] = []
    monkeypatch.setattr(runner, "log_event", events.append)

    def fake_branch_last_push(repo: str, pr_number: int) -> float | None:
        if fake_branch_last_push.calls == 0:
            fake_branch_last_push.calls += 1
            return None
        raise fix_supervision_module.gh_prs.GitHubPollError("poll failed")

    fake_branch_last_push.calls = 0

    async def fake_to_thread(func: Any, *args: object, **kwargs: object) -> Any:
        return func(*args, **kwargs)

    async def fake_sleep(delay: float) -> None:
        return None

    monotonic_values = iter([100.0, 130.0])
    monkeypatch.setattr(
        "src.github.prs.get_branch_last_push_time",
        fake_branch_last_push,
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, pr: None,
    )
    monkeypatch.setattr(fix_supervision_module.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(fix_supervision_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(
        fix_supervision_module,
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
    runner = h._make_runner()
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
    runner = h._make_runner()
    runner._app_config = h._app_cfg(fix_poll_interval_sec=1)
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

    monkeypatch.setattr(fix_supervision_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(fix_supervision_module.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr("src.github.prs.pr_state", fake_pr_state)

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
    runner = h._make_runner()
    runner._app_config = h._app_cfg(fix_poll_interval_sec=1)

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

    monkeypatch.setattr(fix_supervision_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(fix_supervision_module.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr("src.github.prs.pr_state", fake_pr_state)

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
    runner = h._make_runner()
    runner._app_config = h._app_cfg(fix_poll_interval_sec=1)
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

    monkeypatch.setattr(fix_supervision_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(fix_supervision_module.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr("src.github.prs.pr_state", fake_pr_state)

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
    assert any("PR #99 reached terminal state MERGED during FIX" in e for e in events)


def test_handle_external_terminal_pr_state_merged_resets_counters_and_marks_done(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Detected external merge must reset counters and clean up bookkeeping."""
    runner = h._make_runner()
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
        "PR #42 merged externally during FIX, returning to IDLE." in entry["event"] for entry in runner.state.history
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
    runner = h._make_runner()
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
        "_mark_queue_done failed during external-merge cleanup" in entry["event"] for entry in runner.state.history
    )


def test_handle_external_terminal_pr_state_logs_without_pr_number(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Even with no current_pr (race with cleanup), MERGED still moves to IDLE."""
    runner = h._make_runner()
    runner.state.current_pr = None

    monkeypatch.setattr(runner_module.PipelineRunner, "_mark_queue_done", lambda self: None)
    asyncio.run(runner._handle_external_terminal_pr_state("MERGED"))
    assert runner.state.state == PipelineState.IDLE
    assert any("merged externally during FIX" in entry["event"] for entry in runner.state.history)


def test_handle_external_terminal_pr_state_closed_logs_without_pr_number() -> None:
    """No-pr CLOSED race must still transition to HUNG with a generic log."""
    runner = h._make_runner()
    runner.state.current_pr = None
    asyncio.run(runner._handle_external_terminal_pr_state("CLOSED"))
    assert runner.state.state == PipelineState.HUNG
    assert any("closed externally during FIX" in entry["event"] for entry in runner.state.history)


def test_handle_external_terminal_pr_state_merged_saves_success_merged_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """External MERGED in FIX must finalize the run record as ``success_merged``
    so dashboard / metrics views don't see it stuck on ``coding_complete``
    (Codex P2 on PR #223)."""
    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-091",
        title="external merge",
        status=TaskStatus.DOING,
        branch="pr-091",
        task_file="tasks/PR-091.md",
    )
    runner.state.current_pr = PRInfo(number=91, branch="pr-091")
    runner._start_current_run_record("claude", "opus")

    monkeypatch.setattr(runner_module.PipelineRunner, "_mark_queue_done", lambda self: None)
    monkeypatch.setattr(runner, "_compute_diff_stats", lambda base_branch: {})

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
    runner = h._make_runner()
    runner._app_config = h._app_cfg(coder_terminate_grace_sec=42)

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

    monkeypatch.setattr(runner_module.asyncio, "wait_for", fake_wait_for)

    asyncio.run(runner._terminate_current_coder())

    assert captured_timeouts == [42]


def test_maybe_retrigger_stale_review_returns_without_current_pr() -> None:
    runner = h._make_runner()

    runner._maybe_retrigger_stale_review(42)

    assert runner.state.last_stale_retrigger_at is None


def test_maybe_retrigger_stale_review_returns_for_non_changes_requested() -> None:
    runner = h._make_runner()
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
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        review_status=ReviewStatus.CHANGES_REQUESTED,
    )

    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
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
        "src.github.prs.get_pr_metadata",
        lambda repo, number: {"author": "", "head_sha": "", "head_commit_date": head_iso},
    )
    runner = h._make_runner()
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
        "src.github.prs.get_pr_metadata",
        lambda repo, number: {"author": "", "head_sha": "", "head_commit_date": ""},
    )
    fallback = datetime(2026, 4, 1, 10, 0, tzinfo=timezone.utc)
    pr = PRInfo(number=99, branch="pr-001", last_activity=fallback)
    runner = h._make_runner()
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
        "src.github.prs.get_pr_metadata",
        lambda repo, number: {"author": "", "head_sha": "", "head_commit_date": head_iso},
    )
    runner = h._make_runner()
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
        "src.github.prs.get_pr_metadata",
        lambda repo, number: {"author": "", "head_sha": "", "head_commit_date": ""},
    )
    runner = h._make_runner()
    runner._last_push_at = datetime(2026, 4, 20, tzinfo=timezone.utc)
    runner._last_push_at_pr_number = 999

    runner._rehydrate_last_push_at(PRInfo(number=42, branch="pr-new"))

    assert runner._last_push_at is None
    assert runner._last_push_at_pr_number == 42


def test_has_new_feedback_returns_none_without_current_pr() -> None:
    runner = h._make_runner()

    assert runner._has_new_codex_feedback_since_last_push() == FeedbackCheckResult.NONE


def test_has_new_feedback_returns_true_without_last_push_timestamp() -> None:
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-fix")

    assert runner._has_new_codex_feedback_since_last_push() == FeedbackCheckResult.NEW


def test_has_new_feedback_returns_true_for_any_codex_comment_after_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Codex comment without P1/P2 posted after _last_push_at -> True."""
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-fix")
    runner._last_push_at = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
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
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-fix")
    runner._last_push_at = datetime(2026, 1, 1, 1, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
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
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-fix")
    runner._last_push_at = datetime(2026, 1, 1, 0, 0, 0)

    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
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
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-fix")
    runner._last_push_at = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
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
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-fix")
    runner._last_push_at = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
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
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-fix")
    runner._last_push_at = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    def _raise(path: str) -> list:
        raise RuntimeError("GitHub API unavailable")

    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        _raise,
    )

    assert runner._has_new_codex_feedback_since_last_push() == FeedbackCheckResult.UNKNOWN


def test_terminate_current_coder_clears_exited_process() -> None:
    runner = h._make_runner()

    class _Proc:
        returncode = 0

    runner._current_coder_process = _Proc()

    asyncio.run(runner._terminate_current_coder())

    assert runner._current_coder_process is None


def test_terminate_current_coder_handles_missing_process() -> None:
    runner = h._make_runner()

    class _Proc:
        returncode = None

        def terminate(self) -> None:
            raise ProcessLookupError

    runner._current_coder_process = _Proc()

    asyncio.run(runner._terminate_current_coder())

    assert runner._current_coder_process is None


def test_terminate_current_coder_kills_after_timeout() -> None:
    runner = h._make_runner()
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
    runner = h._make_runner()
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


def test_verify_pushes_since_returns_false_when_remote_diverged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_verify_pushes_since`` returns ``False`` when the remote moved
    to a SHA that does not contain ``head_after`` (e.g. force-pushed
    over the FIX commit). This exercises the merge-base branch where
    the early-out shortcut against ``last_known_sha`` does not apply."""

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args and args[0] == "fetch":
            return h._FakeCompletedProcess(args=["git", *args], returncode=0)
        if args[:2] == ("rev-parse", "origin/pr-190"):
            return h._FakeCompletedProcess(
                args=["git", *args],
                stdout="ddd444\n",
                returncode=0,
            )
        if args[:2] == ("merge-base", "--is-ancestor"):
            return h._FakeCompletedProcess(args=["git", *args], returncode=1)
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)

    runner = h._make_runner()
    result = runner._verify_pushes_since(
        "pr-190",
        "aaa111",
        "bbb222",
        context="after FIX exit",
    )

    assert result is False
