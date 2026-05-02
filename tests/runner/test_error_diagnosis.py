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
from typing import Any

import pytest
from src import codex_cli
from src.coders import claude as claude_plugin_module
from src.daemon import runner as runner_module
from src.daemon.handlers import idle as idle_module
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus
from src.usage import UsageSnapshot

from tests import test_runner as h

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
        raise AssertionError(
            "claude_cli.diagnose_error_async must not be called on soft-skip"
        )

    async def _fail_codex(*args: object, **kwargs: object) -> tuple:
        calls.append("codex")
        raise AssertionError(
            "codex_cli.diagnose_error_async must not be called on soft-skip"
        )

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
    assert any(
        "max soft-skip retries (3) reached, staying ERROR"
        in entry["event"]
        for entry in runner.state.history
    )


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
    _patch_plugin_diagnose(
        monkeypatch, runner, "claude", (0, "FIX\nrepair config", "")
    )
    _patch_plugin_diagnose(
        monkeypatch, runner, "codex", (0, "FIX\nrepair config", "")
    )
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
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
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
    fired_at_three = asyncio.run(
        runner._error_skip_policy.maybe_escalate(runner)
    )
    assert fired_at_three is False

    runner._error_skip_policy.increment(runner)
    fired_at_four = asyncio.run(
        runner._error_skip_policy.maybe_escalate(runner)
    )
    assert fired_at_four is True
    assert any(
        "max soft-skip retries (3) reached, staying ERROR" in entry["event"]
        for entry in runner.state.history
    )


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
        "diagnose_error: max attempts (3) reached, staying ERROR"
        in entry["event"]
        for entry in runner.state.history
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

    async def claude_diag(
        repo_path: str, context: str, model: str
    ) -> tuple[int, str, str]:
        claude_calls.append((repo_path, context, model))
        return (0, "SKIP", "")

    async def codex_diag(
        repo_path: str, context: str, model: str
    ) -> tuple[int, str, str]:
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

        async def diagnose_error(
            self, repo_path: str, context: str, model: str
        ) -> tuple[int, str, str]:
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
