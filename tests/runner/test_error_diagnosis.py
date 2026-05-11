"""PR-210: ``_error_skip_*`` regression tests.

These tests document the **current** behavior of ``handle_error`` and the
runner's post-cycle reset block at 2026-05-01 — not the desired behavior.
They establish a regression baseline before the BoundedRecoveryPolicy
migration in C7 (PR-219, PR-223) so that those refactors can preserve
each branch verbatim with explicit confidence about what changes.

The six tests cover every branch of the ``_error_skip_*`` triplet
(``_error_skip_context``, ``_error_skip_count``, ``_error_skip_active``):

- Tests 1 and 2 — soft-skip path under same-context rate-limit pressure
  (1, 2, 3 -> IDLE; 4 stays ERROR per the ``error.py`` ``> 3`` ceiling).
- Test 3 — context change resets the soft-skip counter to 1.
- Tests 4 and 5 — SKIP and FIX verdicts from ``diagnose_error`` clear
  the triplet via the explicit ``self._error_skip_context = None`` /
  ``self._error_skip_count = 0`` reset.
- Test 6 — runner-level cross-cycle reset when a non-ERROR cycle
  completes while ``_error_skip_active`` was left set.

The tests isolate ``ErrorMixin`` and runner behavior from real CLI calls
by stubbing the diagnosis async helpers and the usage-provider snapshot.
"""

from __future__ import annotations

import asyncio

# PR-224a: imports needed by tests moved from tests/test_runner.py
import random  # noqa: F401
import re  # noqa: F401
import subprocess
import time  # noqa: F401
import types  # noqa: F401
from pathlib import Path
from typing import Any

import pytest
from src import codex_cli
from src.coders import claude as claude_plugin_module
from src.config import CoderType
from src.daemon import git_ops as git_ops_module
from src.daemon import runner as runner_module
from src.daemon.handlers import coding as coding_module  # noqa: F401,F811
from src.daemon.handlers import error as error_module  # noqa: F401,F811
from src.daemon.handlers import idle as idle_module  # noqa: F811
from src.daemon.handlers import merge as merge_module  # noqa: F401,F811
from src.daemon.handlers import watch as watch_module  # noqa: F401,F811
from src.daemon.runner import ErrorCategory, _classify_error
from src.models import (
    PipelineState,  # noqa: F811
    PRInfo,  # noqa: F811
    QueueTask,  # noqa: F811
    TaskStatus,  # noqa: F811
)
from src.usage import UsageSnapshot

from tests.runner import _helpers as h

claude_cli = claude_plugin_module.claude_cli


def _patch_plugin_diagnose(
    monkeypatch: pytest.MonkeyPatch,
    runner: Any,
    coder_name: str,
    result: tuple[int, str, str],
) -> None:
    """Replace plugin.diagnose_error on the runner's registry."""
    plugin = runner._registry.get(coder_name)

    async def _fn(*args: object, **kwargs: object) -> tuple[int, str, str]:
        return result

    monkeypatch.setattr(plugin, "diagnose_error", _fn)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


# Spec divergence note (PR-210):
#
# The PR-210 task file uses the literal string ``"Anthropic rate limit
# at 95%"`` as the soft-skip ``error_message``. That string is not a
# valid trigger for the soft-skip path: ``_classify_error`` matches
# ``"rate limit"`` and ``handle_error`` early-returns to IDLE via the
# RATE_LIMIT shortcut at ``error.py:150-160`` *before* the auxiliary
# coder / usage-provider check that drives the soft-skip counter ever
# runs. Per the PR-210 instruction "document the divergence in the
# test docstring but do not change production code in this PR", these
# tests use neutral error contexts that classify as ``OTHER`` so the
# code reaches the soft-skip branch under test. The rate-limit *signal*
# that the task spec describes is the **usage provider snapshot**, not
# the error message itself; that signal is supplied by
# ``_force_claude_rate_limited_provider`` below.
_SOFT_SKIP_CONTEXT = "Coder subprocess crashed before completing diagnosis"
_SOFT_SKIP_CONTEXT_OTHER = "Coder subprocess crashed reading task header"


def _force_claude_rate_limited_provider(runner: Any) -> None:
    """Pin the claude usage provider to a snapshot above the pause threshold.

    ``handle_error`` reads from whichever provider matches the auxiliary
    coder. ``_FakeUsageProvider.fetch`` returns the cached snapshot, so
    swapping the snapshot for one at 99% session usage triggers the
    soft-skip branch deterministically.
    """
    snapshot = UsageSnapshot(
        session_percent=99,
        session_resets_at=0,
        weekly_percent=10,
        weekly_resets_at=0,
        fetched_at=0.0,
    )
    runner._claude_usage_provider = h._FakeUsageProvider(snapshot=snapshot)
    runner._codex_usage_provider = h._FakeUsageProvider(snapshot=snapshot)


def _block_diagnose_calls(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Fail the test if any diagnose CLI is invoked.

    The soft-skip branches must short-circuit before either CLI is
    called, so an unexpected call signals a behavior regression.
    """
    calls: list[str] = []

    async def _fail_claude(*args: object, **kwargs: object) -> tuple:
        calls.append("claude")
        raise AssertionError("claude_cli.diagnose_error_async must not be called on soft-skip")

    async def _fail_codex(*args: object, **kwargs: object) -> tuple:
        calls.append("codex")
        raise AssertionError("codex_cli.diagnose_error_async must not be called on soft-skip")

    monkeypatch.setattr(claude_cli, "diagnose_error_async", _fail_claude)
    monkeypatch.setattr(codex_cli, "diagnose_error_async", _fail_codex)
    return calls


# ---------------------------------------------------------------------------
# Test 1 — same-context soft-skip 1, 2, 3 returns IDLE
# ---------------------------------------------------------------------------


def test_same_context_soft_skip_3_cycles_return_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Three consecutive same-context soft-skips each transition to IDLE.

    Branch covered: ``error.py`` rate-limited soft-skip path where the
    auxiliary coder's usage provider reports a snapshot at or above the
    configured pause threshold and the incoming ``error_message`` matches
    the cached ``_error_skip_context``. Each cycle increments
    ``_error_skip_count`` and routes the daemon back to IDLE so the
    outer loop can retry without paging the operator.
    """
    diag_calls = _block_diagnose_calls(monkeypatch)

    runner = h._make_runner()
    _force_claude_rate_limited_provider(runner)

    runner.state.state = PipelineState.ERROR
    runner.state.error_message = _SOFT_SKIP_CONTEXT

    # Cycle 1 — first observation of the context; counter starts at 1.
    asyncio.run(runner.handle_error())
    assert runner.state.state == PipelineState.IDLE
    assert runner._error_skip_count == 1
    assert runner._error_skip_active is True
    assert runner._error_skip_context == _SOFT_SKIP_CONTEXT
    assert runner.state.error_message is None

    # Cycle 2 — outer loop re-enters ERROR with the same context.
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = _SOFT_SKIP_CONTEXT
    asyncio.run(runner.handle_error())
    assert runner.state.state == PipelineState.IDLE
    assert runner._error_skip_count == 2
    assert runner._error_skip_active is True

    # Cycle 3 — final IDLE-bound increment before the > 3 ceiling fires.
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = _SOFT_SKIP_CONTEXT
    asyncio.run(runner.handle_error())
    assert runner.state.state == PipelineState.IDLE
    assert runner._error_skip_count == 3
    assert runner._error_skip_active is True
    assert diag_calls == []


# ---------------------------------------------------------------------------
# Test 2 — 4th same-context skip stays ERROR
# ---------------------------------------------------------------------------


def test_fourth_same_context_skip_stays_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The 4th same-context skip stays ERROR per the ``> 3`` ceiling.

    Branch covered: ``error.py:203-209``. After three IDLE-bound
    soft-skips the runner is parked in ERROR so a human can intervene
    rather than letting the daemon spin indefinitely under sustained
    rate-limit pressure for the same root cause.
    """
    diag_calls = _block_diagnose_calls(monkeypatch)

    runner = h._make_runner()
    _force_claude_rate_limited_provider(runner)

    runner.state.state = PipelineState.ERROR
    runner.state.error_message = _SOFT_SKIP_CONTEXT
    runner._error_skip_context = _SOFT_SKIP_CONTEXT
    runner._error_skip_count = 3
    runner._error_skip_active = True

    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.ERROR
    assert runner._error_skip_count == 4
    assert runner._error_skip_active is True
    assert runner.state.error_message == _SOFT_SKIP_CONTEXT
    assert diag_calls == []
    assert any("max soft-skip retries (3) reached, staying ERROR" in entry["event"] for entry in runner.state.history)


# ---------------------------------------------------------------------------
# Test 3 — different context resets the counter to 1
# ---------------------------------------------------------------------------


def test_different_context_resets_counter_to_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A new error context restarts the soft-skip counter at 1.

    Branch covered: ``error.py:197-201`` ``else`` branch. The
    soft-skip ceiling is per-context, so a different root cause must
    not inherit accumulated count from an unrelated rate-limit episode.
    """
    diag_calls = _block_diagnose_calls(monkeypatch)

    runner = h._make_runner()
    _force_claude_rate_limited_provider(runner)

    runner.state.state = PipelineState.ERROR
    runner.state.error_message = _SOFT_SKIP_CONTEXT_OTHER
    runner._error_skip_context = _SOFT_SKIP_CONTEXT
    runner._error_skip_count = 2
    runner._error_skip_active = True

    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.IDLE
    assert runner._error_skip_count == 1
    assert runner._error_skip_active is True
    assert runner._error_skip_context == _SOFT_SKIP_CONTEXT_OTHER
    assert diag_calls == []


# ---------------------------------------------------------------------------
# Test 4 — diagnose_error SKIP verdict clears task / PR / error -> IDLE
# ---------------------------------------------------------------------------


def test_diagnose_skip_clears_task_pr_error_and_returns_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A SKIP verdict drops the task, the PR, and the error_message.

    Branch covered: ``error.py:410-416``. SKIP signals that the active
    task cannot be safely resumed; the daemon must drop its handle on
    both ``current_task`` and ``current_pr`` before returning to IDLE so
    the next cycle re-selects a task from scratch.
    """
    runner = h._make_runner()
    _patch_plugin_diagnose(monkeypatch, runner, "claude", (0, "SKIP", ""))
    _patch_plugin_diagnose(monkeypatch, runner, "codex", (0, "SKIP", ""))
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "boom"
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="active task",
        status=TaskStatus.DOING,
    )
    runner.state.current_pr = PRInfo(number=42, branch="pr-001-feature")

    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert runner.state.error_message is None


# ---------------------------------------------------------------------------
# Test 5 — diagnose_error FIX verdict clears error and returns IDLE
# ---------------------------------------------------------------------------


def test_diagnose_fix_clears_error_and_returns_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A FIX verdict clears ``error_message`` but preserves task and PR.

    Branch covered: ``error.py:417-423``. FIX hands off to the next
    cycle's FIX handler, which still needs ``current_task`` and
    ``current_pr`` to know which PR branch to operate on. Only
    ``error_message`` is cleared because the diagnosis itself is the
    resolution attempt for the original error.
    """
    task = QueueTask(
        pr_id="PR-002",
        title="active task",
        status=TaskStatus.DOING,
    )
    pr = PRInfo(number=99, branch="pr-002-feature")

    runner = h._make_runner()
    _patch_plugin_diagnose(monkeypatch, runner, "claude", (0, "FIX\nrepair config", ""))
    _patch_plugin_diagnose(monkeypatch, runner, "codex", (0, "FIX\nrepair config", ""))
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "boom"
    runner.state.current_task = task
    runner.state.current_pr = pr

    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None
    # FIX preserves both so the next cycle's FIX handler has context.
    assert runner.state.current_task is task
    assert runner.state.current_pr is pr


# ---------------------------------------------------------------------------
# Test 6 — non-ERROR cycle resets ``_error_skip_*`` after recovery
# ---------------------------------------------------------------------------


def test_non_error_cycle_resets_error_skip_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A normal IDLE cycle clears any leftover ``_error_skip_*`` state.

    Branch covered: ``runner.py:1379-1386`` post-handler reset block.
    The triplet sticks across the soft-skip IDLE transition so that the
    next ERROR cycle observes the prior context — but once a cycle ends
    in any non-ERROR state without re-entering ``handle_error``, the
    runner clears the triplet. This prevents stale counters from
    biasing a future, unrelated error.
    """
    h._patch_subprocess(monkeypatch, stdout="")
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )

    runner = h._make_runner()
    # Skip recover_state: this test exercises the post-handler reset
    # block, not recovery. Skip scaffolding for the same reason.
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.IDLE
    runner._error_skip_context = "Anthropic rate limit at 95%"
    runner._error_skip_count = 2
    runner._error_skip_active = True

    asyncio.run(runner.run_cycle())

    assert runner._error_skip_context is None
    assert runner._error_skip_count == 0
    assert runner._error_skip_active is False


# ---------------------------------------------------------------------------
# PR-223 — BoundedRecoveryPolicy semantics for the error-skip / diagnose
# counters. These tests bind the policy migration so the counter shape
# stays interchangeable with the legacy fields the PR-210 baseline asserts.
# ---------------------------------------------------------------------------


def test_error_skip_policy_counter_tracks_legacy_field() -> None:
    """The policy's counter accessor reads the same backing field
    that PR-210 baseline tests assert on, so the two must always
    agree."""
    runner = h._make_runner()
    runner._error_skip_count = 5
    assert runner._error_skip_policy.counter_getter(runner) == 5
    runner._error_skip_policy.counter_setter(runner, 7)
    assert runner._error_skip_count == 7


def test_error_skip_policy_increment_and_reset_round_trip() -> None:
    """``policy.increment`` advances the legacy counter; ``policy.reset``
    zeros it back to the post-init state."""
    runner = h._make_runner()
    assert runner._error_skip_count == 0
    runner._error_skip_policy.increment(runner)
    runner._error_skip_policy.increment(runner)
    assert runner._error_skip_count == 2
    runner._error_skip_policy.reset(runner)
    assert runner._error_skip_count == 0


def test_error_skip_policy_threshold_fires_after_count_exceeds_three() -> None:
    """The threshold callback fires once the counter passes the legacy
    ``count > 3`` gate. ``max_attempts=4`` translates the original
    ``> 3`` semantic into the policy's ``counter >= max_attempts``
    check, so increments 1-3 stay below threshold and increment 4
    triggers the callback."""
    runner = h._make_runner()
    for _ in range(3):
        runner._error_skip_policy.increment(runner)
    fired_at_three = asyncio.run(runner._error_skip_policy.maybe_escalate(runner))
    assert fired_at_three is False

    runner._error_skip_policy.increment(runner)
    fired_at_four = asyncio.run(runner._error_skip_policy.maybe_escalate(runner))
    assert fired_at_four is True
    assert any("max soft-skip retries (3) reached, staying ERROR" in entry["event"] for entry in runner.state.history)


def test_error_diagnose_policy_counter_tracks_legacy_field() -> None:
    runner = h._make_runner()
    runner._error_diagnose_count = 2
    assert runner._error_diagnose_policy.counter_getter(runner) == 2
    runner._error_diagnose_policy.reset(runner)
    assert runner._error_diagnose_count == 0


def test_error_diagnose_policy_threshold_fires_after_count_exceeds_three() -> None:
    runner = h._make_runner()
    for _ in range(3):
        runner._error_diagnose_policy.increment(runner)
    assert asyncio.run(runner._error_diagnose_policy.maybe_escalate(runner)) is False

    runner._error_diagnose_policy.increment(runner)
    assert asyncio.run(runner._error_diagnose_policy.maybe_escalate(runner)) is True
    assert any(
        "diagnose_error: max attempts (3) reached, staying ERROR" in entry["event"] for entry in runner.state.history
    )


# ---------------------------------------------------------------------------
# PR-221 — handle_error dispatches to plugin.diagnose_error via the registry
# ---------------------------------------------------------------------------


def test_handle_error_dispatches_to_codex_plugin_when_codex_active(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the auxiliary coder is Codex, ``plugin.diagnose_error`` of the
    Codex plugin is invoked — Claude's plugin is not.

    Establishes that ``error.py`` no longer branches on ``coder_name`` to
    pick the diagnosis CLI: dispatch flows through the plugin contract,
    so the active coder's plugin alone is consulted.
    """
    runner = h._make_runner()
    claude_plugin = runner._registry.get("claude")
    codex_plugin = runner._registry.get("codex")

    claude_calls: list[tuple[str, str, str]] = []
    codex_calls: list[tuple[str, str, str]] = []

    async def claude_diag(repo_path: str, context: str, model: str) -> tuple[int, str, str]:
        claude_calls.append((repo_path, context, model))
        return (0, "SKIP", "")

    async def codex_diag(repo_path: str, context: str, model: str) -> tuple[int, str, str]:
        codex_calls.append((repo_path, context, model))
        return (0, "SKIP", "")

    monkeypatch.setattr(claude_plugin, "diagnose_error", claude_diag)
    monkeypatch.setattr(codex_plugin, "diagnose_error", codex_diag)
    runner._get_auxiliary_coder = lambda: ("codex", codex_plugin)
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "boom"

    asyncio.run(runner.handle_error())

    assert codex_calls and codex_calls[0][1] == "boom"
    assert codex_calls[0][2] == runner.app_config.daemon.codex_model
    assert claude_calls == []


def test_handle_error_dispatches_to_third_coder_plugin_without_handler_edits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A hypothetical third coder plugin is dispatched correctly without
    any edits to ``error.py``.

    Verifies the Protocol contract: ``handle_error`` takes whatever
    plugin ``_get_auxiliary_coder`` returns and calls
    ``plugin.diagnose_error`` on it. Adding a new coder requires
    implementing the method on its plugin and registering it; no
    handler change is needed.
    """

    class _ThirdCoderPlugin:
        name = "third"
        display_name = "Third Coder"
        models = ["m1"]

        def __init__(self) -> None:
            self.calls: list[tuple[str, str, str]] = []

        async def diagnose_error(self, repo_path: str, context: str, model: str) -> tuple[int, str, str]:
            self.calls.append((repo_path, context, model))
            return (0, "FIX\nsynthetic", "")

    third = _ThirdCoderPlugin()

    runner = h._make_runner()
    runner._registry.register(third)  # type: ignore[arg-type]
    runner._get_auxiliary_coder = lambda: ("third", third)
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "boom from third"

    asyncio.run(runner.handle_error())

    assert third.calls and third.calls[0][1] == "boom from third"
    # FIX verdict transitions back to IDLE.
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None


# ---------------------------------------------------------------------------
# PR-224a moved from tests/test_runner.py
# ---------------------------------------------------------------------------


def test_handle_error_skip_clears_state(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        h._async_cli_result(0, "SKIP", ""),
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "boom"
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)
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

    runner = h._make_runner()
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

    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "boom"

    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "boom"
    assert not claude_calls
    assert not codex_calls
    assert any(
        e["event"] == "[ERROR] No eligible coder available for error diagnosis; staying ERROR."
        for e in runner.state.history
    )


def test_handle_error_escalate_keeps_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        h._async_cli_result(0, "ESCALATE: human help", ""),
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "boom"
    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "boom"


def test_handle_error_commits_and_pushes_diagnose_fixes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    runner, calls, _, review_requests = h._run_dirty_diagnose(monkeypatch, tmp_path)

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 1
    assert runner.state.current_pr.observed_head_shas == {"abc123"}
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
        "rev-parse",
    ]
    assert calls[-2] == (
        "push",
        "origin",
        "HEAD:fix/diagnose-error-commits-fixes",
    )


def test_handle_error_resets_when_push_fails_and_escalates(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    runner, calls, warnings, _ = h._run_dirty_diagnose(monkeypatch, tmp_path, push_exc=RuntimeError("push failed"))

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
    runner, calls, _, review_requests = h._run_dirty_diagnose(monkeypatch, tmp_path, review_post_ok=False)

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 1
    assert runner.state.current_pr.observed_head_shas == {"abc123"}
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
        "rev-parse",
    ]
    assert (
        runner.state.error_message == "Failed to post @codex review on PR #119 after "
        "diagnose_error fix push; manual review trigger required "
        "to avoid fix/push loop"
    )
    assert any(e["event"] == f"[ERROR] {runner.state.error_message}." for e in runner.state.history)


def test_handle_error_escalates_dirty_tree_without_active_pr_branch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runner, calls, _warnings, _ = h._run_dirty_diagnose(monkeypatch, tmp_path, with_pr=False)

    assert runner.state.state == PipelineState.ERROR
    assert [cmd[0] for cmd in calls] == [
        "status",
        "status",
        "rev-parse",
        "reset",
        "clean",
    ]
    assert any(
        e["event"] == "[ERROR] diagnose_error: dirty tree without active PR/task branch." for e in runner.state.history
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

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        calls.append(args)
        if args[:2] == ("status", "--porcelain"):
            status = ""
            if changed.exists():
                status = " M fix.txt\n"
            return h._FakeCompletedProcess(stdout=status)
        if args[:2] == ("rev-parse", "HEAD"):
            raise subprocess.CalledProcessError(128, ["git", *args], "boom")
        if args[:3] == ("rev-parse", "--abbrev-ref", "HEAD"):
            return h._FakeCompletedProcess(stdout="fix/diagnose-error-commits-fixes\n")
        return h._FakeCompletedProcess()

    monkeypatch.setattr(claude_cli, "diagnose_error_async", fake_diag)
    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(error_module, "retry_transient", lambda op, **_: op())
    runner = h._make_runner()
    monkeypatch.setattr(runner, "_post_codex_review", lambda pr_number: True)
    runner.repo_path = str(repo)
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "boom"
    runner.state.current_pr = PRInfo(number=119, branch="fix/diagnose-error-commits-fixes")

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
        "rev-parse",
    ]
    assert not any(cmd[:1] == ("reset",) for cmd in calls)
    assert not any(cmd[:1] == ("clean",) for cmd in calls)


def test_handle_error_post_push_rev_parse_failure_defers_count_to_polling(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Empty-SHA after a diagnose-error push must not bump ``push_count``.

    Regression: the previous fix bumped ``push_count`` directly inside
    ``record_observed_head("")``. On the next WATCH/IDLE refresh the
    polling merge would see the real head SHA as a new observation
    and increment ``push_count`` a second time for the same real push.
    The corrected behavior leaves ``push_count`` unchanged on the
    empty-SHA path; the next poll cycle resolves the real SHA and
    counts the push exactly once via ``merge_observed_pushes``.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    changed = repo / "fix.txt"
    calls: list[tuple[str, ...]] = []
    rev_parse_head_calls = {"n": 0}
    review_requests: list[int] = []

    async def fake_diag(*args: object, **kwargs: object) -> tuple[int, str, str]:
        changed.write_text("fixed\n")
        return (0, "FIX\nrepair broken config", "")

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        calls.append(args)
        if args[:2] == ("status", "--porcelain"):
            status = ""
            if changed.exists():
                status = " M fix.txt\n"
            return h._FakeCompletedProcess(stdout=status)
        if args[:3] == ("rev-parse", "--abbrev-ref", "HEAD"):
            return h._FakeCompletedProcess(stdout="fix/diagnose-error-commits-fixes\n")
        if args[:2] == ("rev-parse", "HEAD"):
            rev_parse_head_calls["n"] += 1
            if rev_parse_head_calls["n"] == 1:
                return h._FakeCompletedProcess(stdout="abc123\n")
            raise subprocess.CalledProcessError(128, ["git", *args], stderr="fatal: rev-parse intermittent")
        return h._FakeCompletedProcess()

    monkeypatch.setattr(claude_cli, "diagnose_error_async", fake_diag)
    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(error_module, "retry_transient", lambda op, **_: op())
    runner = h._make_runner()
    monkeypatch.setattr(
        runner,
        "_post_codex_review",
        lambda pr_number: review_requests.append(pr_number) or True,
    )
    runner.repo_path = str(repo)
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "boom"
    runner.state.current_pr = PRInfo(
        number=119,
        branch="fix/diagnose-error-commits-fixes",
        observed_head_shas={"earlier-sha"},
        push_count=1,
    )

    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.observed_head_shas == {"earlier-sha"}
    assert runner.state.current_pr.push_count == 1
    assert review_requests == [119]

    polled = PRInfo(
        number=119,
        branch="fix/diagnose-error-commits-fixes",
        observed_head_shas={"post-rev-parse-failure-sha"},
        push_count=1,
    )
    merged_shas, merged_push_count = runner.state.current_pr.merge_observed_pushes(polled)
    assert merged_shas == {"earlier-sha", "post-rev-parse-failure-sha"}
    assert merged_push_count == 2


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

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        calls.append(args)
        if args[:2] == ("status", "--porcelain"):
            status = ""
            if changed.exists():
                status = " M fix.txt\n"
            return h._FakeCompletedProcess(stdout=status)
        if args[:3] == ("rev-parse", "--abbrev-ref", "HEAD"):
            return h._FakeCompletedProcess(stdout="feature-x\n")
        if args[:2] == ("rev-parse", "HEAD"):
            return h._FakeCompletedProcess(stdout="abc123\n")
        return h._FakeCompletedProcess()

    monkeypatch.setattr(claude_cli, "diagnose_error_async", fake_diag)
    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(error_module, "retry_transient", lambda op, **_: op())
    runner = h._make_runner()
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
    runner, calls, warnings, _ = h._run_dirty_diagnose(monkeypatch, tmp_path, head_branch="main")

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
        "[ERROR] diagnose_error: active branch mismatch ('main' != 'fix/diagnose-error-commits-fixes')." == e["event"]
        for e in runner.state.history
    )


def test_handle_error_discards_dirty_tree_for_non_fix_verdict(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    runner, calls, warnings, _ = h._run_dirty_diagnose(monkeypatch, tmp_path, diagnosis_stdout="ESCALATE\nhuman help")

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

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        calls.append(args)
        if args[:2] == ("status", "--porcelain"):
            status = ""
            if changed.exists():
                status = " M fix.txt\n"
            return h._FakeCompletedProcess(stdout=status)
        if args[:2] == ("rev-parse", "HEAD"):
            raise subprocess.CalledProcessError(128, ["git", *args], "boom")
        return h._FakeCompletedProcess()

    monkeypatch.setattr(claude_cli, "diagnose_error_async", fake_diag)
    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    runner = h._make_runner()
    runner.repo_path = str(repo)
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "boom"
    runner.state.current_task = QueueTask(pr_id="PR-101", title="t", branch="feature-x", status=TaskStatus.DOING)
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
    runner, calls, warnings, review_requests = h._run_dirty_diagnose(
        monkeypatch,
        tmp_path,
        preexisting_dirty=" M unrelated.txt\n",
    )

    assert runner.state.state == PipelineState.ERROR
    assert [cmd[0] for cmd in calls] == ["status", "status"]
    assert warnings == []
    assert review_requests == []
    assert any(
        e["event"] == "[ERROR] diagnose_error: pre-existing dirty tree blocks automatic cleanup/publish."
        for e in runner.state.history
    )


def test_handle_error_caps_at_3(monkeypatch: pytest.MonkeyPatch) -> None:
    """handle_error must stop invoking diagnose_error after 3 attempts."""
    calls: list[str] = []

    async def fake_diag(path: str, ctx: str, model: str | None = None) -> tuple[int, str, str]:
        calls.append(ctx)
        return (0, "ESCALATE", "")

    monkeypatch.setattr(claude_cli, "diagnose_error_async", fake_diag)
    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "generic failure"
    for _ in range(5):
        asyncio.run(runner.handle_error())
    assert len(calls) == 3
    assert any("max attempts" in e["event"] for e in runner.state.history)


def test_handle_error_skips_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """A timeout-marked error must skip the AI-diagnosis call entirely."""
    called: list[bool] = []

    async def fake_diag(*a: Any, **kw: Any) -> tuple[int, str, str]:
        called.append(True)
        return (0, "SKIP", "")

    monkeypatch.setattr(claude_cli, "diagnose_error_async", fake_diag)
    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "claude CLI timeout after 900s"
    asyncio.run(runner.handle_error())
    assert called == []
    assert runner.state.state == PipelineState.IDLE


def test_handle_error_skips_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A rate-limit error must skip the AI-diagnosis call entirely."""
    called: list[bool] = []

    async def fake_diag(*a: Any, **kw: Any) -> tuple[int, str, str]:
        called.append(True)
        return (0, "SKIP", "")

    monkeypatch.setattr(claude_cli, "diagnose_error_async", fake_diag)
    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "API rate limit exceeded"
    asyncio.run(runner.handle_error())
    assert called == []


def test_handle_error_skips_diagnose_for_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """handle_error skips diagnose_error when error contains 'rate limit'."""
    h._patch_subprocess(monkeypatch)
    cli_calls: list[str] = []
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        h._async_cli_result_with_side_effect(cli_calls, "diagnose", 0, "SKIP", ""),
    )
    runner = h._make_runner()
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
    h._patch_subprocess(monkeypatch)
    cli_calls: list[str] = []
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        h._async_cli_result_with_side_effect(cli_calls, "diagnose", 0, "SKIP", ""),
    )
    runner = h._make_runner()
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


@pytest.mark.parametrize(
    "msg",
    [
        "ensure_repo_cloned failed: git fetch origin main failed after 3 attempts",
        "git push origin HEAD:foo failed: Could not connect to github.com",
        "Connection timed out reaching api.github.com",
        "git fetch origin: network is unreachable",
        "Failed to connect to github.com port 443",
        "gh: failed to run git: dial tcp 140.82.112.4:443: i/o timeout",
        "ensure_repo_cloned: dial tcp 1.2.3.4:22: connect: network is unreachable",
        "git fetch origin main failed after 5 attempts",
    ],
)
def test_handle_error_skips_diagnose_for_infra_error(msg: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Infra/network failure messages must bypass the AI diagnose CLI."""
    h._patch_subprocess(monkeypatch)
    cli_calls: list[str] = []
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        h._async_cli_result_with_side_effect(cli_calls, "diagnose", 0, "SKIP", ""),
    )
    monkeypatch.setattr(
        codex_cli,
        "diagnose_error_async",
        h._async_cli_result_with_side_effect(cli_calls, "diagnose", 0, "SKIP", ""),
    )
    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = msg
    runner._error_skip_context = "stale context"
    runner._error_skip_count = 2
    runner._error_skip_active = True
    runner._error_diagnose_count = 1

    asyncio.run(runner.handle_error())

    assert cli_calls == []
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message == msg
    assert runner._error_skip_context is None
    assert runner._error_skip_count == 0
    assert runner._error_skip_active is False
    # Counter is preserved (not poisoned): a later non-infra error must still
    # be eligible for diagnosis.
    assert runner._error_diagnose_count == 1
    assert any(
        e["event"].startswith(
            "[ERROR] Infra error detected, skipping AI diagnosis and transitioning to IDLE for retry:"
        )
        for e in runner.state.history
    )


def test_handle_error_infra_bypass_truncates_long_messages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Very long infra error messages are truncated to 200 chars in the log."""
    h._patch_subprocess(monkeypatch)
    cli_calls: list[str] = []
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        h._async_cli_result_with_side_effect(cli_calls, "diagnose", 0, "SKIP", ""),
    )
    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "git fetch origin main: " + ("x" * 500)

    asyncio.run(runner.handle_error())

    assert cli_calls == []
    prefix = "[ERROR] Infra error detected, skipping AI diagnosis and transitioning to IDLE for retry: "
    log_entry = next(e["event"] for e in runner.state.history if e["event"].startswith(prefix))
    # Trim the trailing ".".
    payload = log_entry[len(prefix) : -1]
    assert len(payload) == 200
    assert payload.endswith("...")


def test_handle_error_infra_bypass_repeats_without_invoking_cli(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated invocations with an infra error never invoke the diagnose CLI."""
    h._patch_subprocess(monkeypatch)
    cli_calls: list[str] = []
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        h._async_cli_result_with_side_effect(cli_calls, "diagnose", 0, "SKIP", ""),
    )
    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "ensure_repo_cloned failed: git fetch origin main failed after 3 attempts"

    for _ in range(5):
        asyncio.run(runner.handle_error())

    assert cli_calls == []
    assert runner.state.state == PipelineState.IDLE


def test_handle_error_runs_diagnose_for_non_infra_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-infra errors still go through the AI diagnose path."""
    h._patch_subprocess(monkeypatch)
    cli_calls: list[str] = []
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        h._async_cli_result_with_side_effect(cli_calls, "diagnose", 0, "SKIP", ""),
    )
    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "pytest: 3 tests failed in test_models.py"

    asyncio.run(runner.handle_error())

    assert cli_calls == ["diagnose"]
    assert not any(e["event"].startswith("Infra error detected") for e in runner.state.history)


def test_handle_error_infra_bypass_does_not_lock_out_subsequent_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Infra bypass must not poison the diagnose counter for later non-infra errors."""
    h._patch_subprocess(monkeypatch)
    cli_calls: list[str] = []
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        h._async_cli_result_with_side_effect(cli_calls, "diagnose", 0, "SKIP", ""),
    )
    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "ensure_repo_cloned failed: git fetch origin main failed after 3 attempts"

    asyncio.run(runner.handle_error())
    assert cli_calls == []

    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "working tree dirty: M src/foo.py"

    asyncio.run(runner.handle_error())

    assert cli_calls == ["diagnose"]


def test_handle_error_preserves_error_message_on_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When diagnose_error_async is rate-limited, error_message is preserved."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        h._async_cli_result_with_side_effect([], "diagnose", 1, "", "Error: 429 Too Many Requests"),
    )
    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "Build failed: missing dependency X"

    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.error_message == "Build failed: missing dependency X"


def test_handle_error_skips_ai_diagnosis_when_claude_session_is_limited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.usage import UsageSnapshot

    cli_calls: list[str] = []
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        h._async_cli_result_with_side_effect(cli_calls, "diagnose", 0, "SKIP", ""),
    )
    runner = h._make_runner(coder=CoderType.CODEX)
    runner.app_config.daemon.rate_limit_session_pause_percent = 80
    runner._claude_usage_provider = h._FakeUsageProvider(
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
    assert any(e["event"] == "[ERROR] Skipping AI diagnosis: Claude rate limited." for e in runner.state.history)


def test_handle_error_honors_claude_rate_limit_when_active_coder_is_claude(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.usage import UsageSnapshot

    cli_calls: list[str] = []
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        h._async_cli_result_with_side_effect(cli_calls, "diagnose", 0, "SKIP", ""),
    )
    runner = h._make_runner()
    runner.app_config.daemon.rate_limit_session_pause_percent = 80
    runner._claude_usage_provider = h._FakeUsageProvider(
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
    assert any(e["event"] == "[ERROR] Skipping AI diagnosis: Claude rate limited." for e in runner.state.history)


def test_handle_error_skips_ai_diagnosis_when_claude_weekly_is_limited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.usage import UsageSnapshot

    cli_calls: list[str] = []
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        h._async_cli_result_with_side_effect(cli_calls, "diagnose", 0, "SKIP", ""),
    )
    runner = h._make_runner(coder=CoderType.CODEX)
    runner.app_config.daemon.rate_limit_session_pause_percent = 80
    runner.app_config.daemon.rate_limit_weekly_pause_percent = 90
    runner._claude_usage_provider = h._FakeUsageProvider(
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
    assert any(e["event"] == "[ERROR] Skipping AI diagnosis: Claude rate limited." for e in runner.state.history)


def test_handle_error_proceeds_when_usage_snapshot_fetch_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cli_calls: list[str] = []
    h._patch_subprocess(monkeypatch)

    def raise_fetch() -> object:
        raise RuntimeError("usage fetch failed")

    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        h._async_cli_result_with_side_effect(cli_calls, "diagnose", 0, "ESCALATE", ""),
    )
    runner = h._make_runner()
    runner._claude_usage_provider.fetch = raise_fetch  # type: ignore[method-assign]
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "Build failed: missing dependency X"

    asyncio.run(runner.handle_error())

    assert cli_calls == ["diagnose"]
    assert runner.state.state == PipelineState.ERROR
    assert not any(e["event"] == "[ERROR] Skipping AI diagnosis: Claude rate limited." for e in runner.state.history)


def test_handle_error_soft_skip_caps_repeated_codex_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.usage import UsageSnapshot

    cli_calls: list[str] = []
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        h._async_cli_result_with_side_effect(cli_calls, "diagnose", 0, "SKIP", ""),
    )
    runner = h._make_runner(coder=CoderType.CODEX)
    runner.app_config.daemon.rate_limit_session_pause_percent = 80
    runner._claude_usage_provider = h._FakeUsageProvider(
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
    assert any("max soft-skip retries (3) reached" in e["event"] for e in runner.state.history)


def test_handle_error_logs_and_returns_when_diagnose_cli_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        h._async_cli_result(1, "", "boom"),
    )
    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "Build failed: missing dependency X"

    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "Build failed: missing dependency X"
    assert any(e["event"] == "[ERROR] diagnose_error CLI failed: boom." for e in runner.state.history)


def test_handle_error_timeout_has_distinct_log(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Timeout errors must produce a log mentioning 'timeout error', not 'rate-limit'."""
    called: list[bool] = []

    async def fake_diag(*a: Any, **kw: Any) -> tuple[int, str, str]:
        called.append(True)
        return (0, "SKIP", "")

    monkeypatch.setattr(claude_cli, "diagnose_error_async", fake_diag)
    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "Timeout after 600s"
    asyncio.run(runner.handle_error())

    assert called == []
    log_msgs = [e["event"] for e in runner.state.history]
    assert any("timeout error" in m for m in log_msgs)
    assert not any("rate-limit" in m for m in log_msgs)


def test_handle_error_infra_bypass_resets_state_to_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Infra-error bypass must transition state to IDLE for the next cycle to
    pick the failed task back up, while preserving error_message for the
    operator dashboard until the next successful cycle clears it."""
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "ensure_repo_cloned failed: git fetch origin main failed after 3 attempts"

    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message == ("ensure_repo_cloned failed: git fetch origin main failed after 3 attempts")


def test_handle_error_rate_limit_bypass_resets_state_to_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rate-limit bypass must transition state to IDLE so the next cycle
    retries the failing operation instead of trapping the daemon in ERROR."""
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "API rate limit exceeded"

    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message == "API rate limit exceeded"


def test_handle_error_timeout_bypass_resets_state_to_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Timeout bypass must transition state to IDLE so the next cycle
    retries the failing operation instead of looping in ERROR forever."""
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "claude CLI timeout after 900s"

    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message == "claude CLI timeout after 900s"


def test_handle_error_recovers_within_two_cycles_after_tls_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression guard for the 2026-04-30 morning incident: a single TLS
    handshake timeout produced 16 consecutive ``handle_error`` invocations
    over 15 minutes because the bypass branches never reset state out of
    ERROR. After the fix, the first cycle's bypass transitions to IDLE so
    the second cycle dispatches to ``handle_idle`` (not ``handle_error``)
    and is free to retry the failing call."""
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = (
        "git fetch origin main failed after 3 attempts: TLS handshake timeout reaching github.com"
    )

    # Cycle 1: ERROR -> handle_error bypass -> IDLE (state transition that
    # the daemon's main dispatcher uses to route the next cycle).
    asyncio.run(runner.handle_error())
    assert runner.state.state == PipelineState.IDLE

    # Cycle 2 simulation: a successful retry on the second attempt clears
    # the error_message naturally. Before the fix, the daemon would still be
    # in ERROR here and would re-enter handle_error for at least 14 more
    # cycles before manual intervention; we assert directly that the state
    # is IDLE so the dispatcher cannot loop in the ERROR branch.
    runner.state.error_message = None
    assert runner.state.state == PipelineState.IDLE


# ---------------------------------------------------------------------------
# PR-224b moved from tests/test_runner.py — error_diagnosis group
# ---------------------------------------------------------------------------


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
            "merge_pr failed: gh: failed to run git: not possible to fast-forward, you may want to integrate first",
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
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        claude_cli,
        "diagnose_error_async",
        h._async_cli_result_with_side_effect(cli_calls, "diagnose", 0, "SKIP", ""),
    )

    runner = h._make_runner(coder=CoderType.CODEX)
    runner._recovered = True
    runner.app_config.daemon.rate_limit_session_pause_percent = 80
    runner._claude_usage_provider = h._FakeUsageProvider(
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
    monkeypatch.setattr(runner, "preflight", h._preflight_true_stub)

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


def test_run_cycle_dispatches_error_handler_when_ai_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    publishes: list[str] = []
    runner = h._make_runner()
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
    monkeypatch.setattr(runner, "preflight", h._preflight_true_stub)
    monkeypatch.setattr(runner, "handle_error", fake_handle_error)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert calls == ["handle_error"]
    assert publishes == ["published"]


def test_run_cycle_in_error_with_review_timeout_park_skips_handle_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-316 follow-up: WATCH parks ERROR with ``skip_ai_error_diagnose``.

    While the cancellation cause for the active task is still present,
    ``run_cycle`` must NOT invoke ``handle_error`` — otherwise the AI
    diagnose loop burns budget on a non-fixable review timeout and may
    auto-leave ERROR through a FIX/SKIP verdict, undermining the
    operator-controlled park.
    """
    from src.cancellation import CancellationCause
    from src.cancellation.storage import cause_key

    calls: list[str] = []
    runner = h._make_runner()
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "PR #5 hung after 90m (review=EYES, ci=PENDING)"
    runner.state.skip_ai_error_diagnose = True
    runner.state.current_task = QueueTask(
        pr_id="PR-005",
        title="t",
        status=TaskStatus.ERROR,
        branch="pr-001",
    )
    cause = CancellationCause(
        category="ERROR",
        payload={"subsource": "review_timeout"},
        created_at="2026-05-11T00:00:00+00:00",
        task_id="PR-005",
        repo_slug=runner.name,
    )
    asyncio.run(
        runner.redis.set(cause_key(runner.name, "PR-005"), cause.to_redis())
    )

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_handle_error() -> None:
        calls.append("handle_error")

    async def fake_publish_state() -> None:
        return None

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "preflight", h._preflight_true_stub)
    monkeypatch.setattr(runner, "handle_error", fake_handle_error)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert calls == []
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.skip_ai_error_diagnose is True


def test_review_timeout_park_cleared_returns_true_when_no_current_task() -> None:
    """The helper reports cleared when there is no task to read a cause for.

    Defensive: a runner that already dropped ``current_task`` cannot have
    a cancellation cause to wait on, so ``run_cycle`` should release the
    park rather than spin forever in ERROR.
    """
    runner = h._make_runner()
    runner.state.current_task = None
    assert asyncio.run(runner._review_timeout_park_cleared()) is True


def test_review_timeout_park_cleared_returns_false_on_redis_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Redis read failure keeps the runner parked.

    Without this guard a transient Redis blip would race the operator's
    Retry button and falsely transition the runner to IDLE while the
    cause record is still authoritative on the server.
    """
    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-099",
        title="t",
        status=TaskStatus.ERROR,
    )

    async def boom(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("redis down")

    monkeypatch.setattr(
        "src.daemon.runner.get_cancellation_cause", boom
    )
    assert asyncio.run(runner._review_timeout_park_cleared()) is False


def test_run_cycle_in_error_with_park_flag_releases_when_cause_deleted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Operator Retry deletes the cause; the park flag releases to IDLE.

    PR-316 follow-up: ``repo_control.retry_repo_task`` clears the
    cancellation cause record. The next ERROR cycle observes the empty
    slot, clears ``skip_ai_error_diagnose`` and ``error_message``, and
    transitions to IDLE so ``handle_idle``'s picker can pick up the
    now-status:TODO task without an AI round-trip.
    """
    calls: list[str] = []
    runner = h._make_runner()
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "PR #7 hung after 90m (review=EYES, ci=PENDING)"
    runner.state.skip_ai_error_diagnose = True
    runner.state.current_task = QueueTask(
        pr_id="PR-007",
        title="t",
        status=TaskStatus.TODO,
        branch="pr-007",
    )
    # No cause stored in Redis — operator's Retry just deleted it.

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_handle_error() -> None:
        calls.append("handle_error")

    async def fake_publish_state() -> None:
        return None

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "preflight", h._preflight_true_stub)
    monkeypatch.setattr(runner, "handle_error", fake_handle_error)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)
    # The cycle ends in IDLE; the subsequent handle_idle dispatch is
    # outside this test's scope.
    async def fake_handle_idle() -> None:
        return None

    monkeypatch.setattr(runner, "handle_idle", fake_handle_idle)

    asyncio.run(runner.run_cycle())

    assert calls == []
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.skip_ai_error_diagnose is False
    assert runner.state.error_message is None
    assert any(
        "review_timeout park cleared by operator" in entry.get("event", "")
        for entry in runner.state.history
    )


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
    assert _classify_error("fatal: could not resolve host: github.com") == ErrorCategory.GIT_ERROR
