"""Unit tests for BoundedRecoveryPolicy."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace

import pytest
from src.daemon.recovery_policy import (
    BoundedRecoveryPolicy,
    TerminalReleaseDecision,
    TerminalReleaseFacts,
    terminal_release_decision,
)
from src.subsource_registry import all_subsources


@dataclass
class _Ctx:
    counter: int = 0
    actions: list[str] = field(default_factory=list)


def _make_policy(
    *,
    on_threshold: object,
    max_attempts: int = 3,
) -> BoundedRecoveryPolicy[_Ctx]:
    return BoundedRecoveryPolicy(
        name="t",
        max_attempts=max_attempts,
        counter_getter=lambda c: c.counter,
        counter_setter=lambda c, n: setattr(c, "counter", n),
        on_threshold=on_threshold,  # type: ignore[arg-type]
    )


def test_increment_increases_counter_monotonically() -> None:
    ctx = _Ctx()
    policy = _make_policy(on_threshold=lambda _c: None)

    assert policy.increment(ctx) == 1
    assert policy.increment(ctx) == 2
    assert policy.increment(ctx) == 3
    assert ctx.counter == 3


def test_reset_zeroes_counter() -> None:
    ctx = _Ctx(counter=7)
    policy = _make_policy(on_threshold=lambda _c: None)

    policy.reset(ctx)
    assert ctx.counter == 0


def test_maybe_escalate_below_threshold_returns_false_and_skips_action() -> None:
    ctx = _Ctx(counter=2)
    invoked: list[str] = []

    def action(c: _Ctx) -> None:
        invoked.append("ran")
        c.actions.append("ran")

    policy = _make_policy(on_threshold=action, max_attempts=3)
    triggered = asyncio.run(policy.maybe_escalate(ctx))

    assert triggered is False
    assert invoked == []
    assert ctx.actions == []


def test_maybe_escalate_at_threshold_triggers_action_once() -> None:
    ctx = _Ctx(counter=3)
    invoked: list[str] = []

    def action(c: _Ctx) -> None:
        invoked.append("ran")

    policy = _make_policy(on_threshold=action, max_attempts=3)
    triggered = asyncio.run(policy.maybe_escalate(ctx))

    assert triggered is True
    assert invoked == ["ran"]


def test_maybe_escalate_above_threshold_still_triggers() -> None:
    ctx = _Ctx(counter=10)
    invoked: list[str] = []

    def action(c: _Ctx) -> None:
        invoked.append("ran")

    policy = _make_policy(on_threshold=action, max_attempts=3)
    triggered = asyncio.run(policy.maybe_escalate(ctx))

    assert triggered is True
    assert invoked == ["ran"]


def test_maybe_escalate_awaits_async_callback() -> None:
    ctx = _Ctx(counter=3)
    invoked: list[str] = []

    async def action(c: _Ctx) -> None:
        await asyncio.sleep(0)
        invoked.append("async-ran")

    policy = _make_policy(on_threshold=action, max_attempts=3)
    triggered = asyncio.run(policy.maybe_escalate(ctx))

    assert triggered is True
    assert invoked == ["async-ran"]


def test_maybe_escalate_runs_action_each_call_caller_resets() -> None:
    """The framework does not reset the counter — it only triggers
    the action when the threshold is met. Repeat calls re-trigger
    until the caller resets, matching the documented contract."""
    ctx = _Ctx(counter=3)
    invoked: list[str] = []

    def action(c: _Ctx) -> None:
        invoked.append("ran")

    policy = _make_policy(on_threshold=action, max_attempts=3)

    asyncio.run(policy.maybe_escalate(ctx))
    asyncio.run(policy.maybe_escalate(ctx))
    assert invoked == ["ran", "ran"]

    policy.reset(ctx)
    triggered_after_reset = asyncio.run(policy.maybe_escalate(ctx))
    assert triggered_after_reset is False


def test_increment_uses_counter_getter_and_setter_not_attribute_access() -> None:
    """The framework must not bypass the supplied accessors. This
    guards a future maintainer from sneaking direct attribute access
    in that would break callers whose counters live elsewhere."""
    storage: dict[str, int] = {"c": 0}
    get_calls: list[str] = []
    set_calls: list[tuple[str, int]] = []

    def getter(_ctx: object) -> int:
        get_calls.append("get")
        return storage["c"]

    def setter(_ctx: object, n: int) -> None:
        set_calls.append(("set", n))
        storage["c"] = n

    policy: BoundedRecoveryPolicy[object] = BoundedRecoveryPolicy(
        name="t",
        max_attempts=2,
        counter_getter=getter,
        counter_setter=setter,
        on_threshold=lambda _c: None,
    )
    sentinel = object()

    assert policy.increment(sentinel) == 1
    assert policy.increment(sentinel) == 2
    policy.reset(sentinel)
    assert storage["c"] == 0
    assert get_calls == ["get", "get"]
    assert set_calls == [("set", 1), ("set", 2), ("set", 0)]


def test_dataclass_is_frozen() -> None:
    policy = _make_policy(on_threshold=lambda _c: None)
    with pytest.raises(Exception):
        policy.max_attempts = 99  # type: ignore[misc]


# Explicit expectations ensure a newly registered source requires policy review.
_TERMINAL_RELEASE_CASES = [
    ("crash", "release"),
    ("coder_escalate", "release"),
    ("guardrail", "operator_decision"),
    ("review_timeout", "release"),
    ("fix_idle_timeout", "release"),
    ("fix_iteration_cap", "release"),
    ("no_push_deadlock", "release"),
    ("infra_failure", "release"),
    ("daemon", "release"),
    ("watch_retrigger_cap", "release"),
    ("operator_reject", "release"),
    ("diagnose_exhausted", "release"),
    ("operator_stopped", "operator_decision"),
    ("rate_limit", "release"),
]
_CLEAN_FAILURE = TerminalReleaseFacts(
    failure_persisted=True,
    remote_ownership_known=True,
    remote_outcome_resolved=True,
    cleanup_confirmed=True,
    worktree_stable=True,
    no_loss_confirmed=True,
    preservation_verified=False,
    user_paused=False,
    cost_hold=False,
    security_hold=False,
)


def test_terminal_release_table_covers_entire_registry() -> None:
    assert {source for source, _ in _TERMINAL_RELEASE_CASES} == all_subsources()


@pytest.mark.parametrize("source,expected", _TERMINAL_RELEASE_CASES)
def test_terminal_release_for_every_registered_source(source, expected) -> None:
    assert terminal_release_decision(source, _CLEAN_FAILURE) == expected


@pytest.mark.parametrize("source,_expected", _TERMINAL_RELEASE_CASES)
def test_missing_evidence_never_releases_registered_source(source, _expected) -> None:
    assert terminal_release_decision(source, TerminalReleaseFacts()) != "release"


@pytest.mark.parametrize("source", [None, "", "unknown", "CRASH"])
def test_unknown_subsource_requires_operator_decision(source) -> None:
    assert terminal_release_decision(source, _CLEAN_FAILURE) == "operator_decision"


@pytest.mark.parametrize("source", [source for source, _ in _TERMINAL_RELEASE_CASES])
@pytest.mark.parametrize(
    "fact",
    [
        "failure_persisted", "remote_ownership_known", "remote_outcome_resolved",
        "cleanup_confirmed", "worktree_stable",
    ],
)
@pytest.mark.parametrize("evidence", [None, False])
def test_missing_or_negative_required_fact_never_releases(source, fact, evidence) -> None:
    facts = replace(_CLEAN_FAILURE, **{fact: evidence})
    assert terminal_release_decision(source, facts) != "release"


@pytest.mark.parametrize("hold", ["user_paused", "cost_hold", "security_hold"])
@pytest.mark.parametrize("evidence", [None, True])
def test_active_or_unknown_hold_blocks_even_confirmed_clean_failure(hold, evidence) -> None:
    facts = replace(_CLEAN_FAILURE, **{hold: evidence})
    expected = "operator_decision" if evidence is True and hold != "cost_hold" else "wait"
    assert terminal_release_decision("crash", facts) == expected


@pytest.mark.parametrize("no_loss", [None, False])
@pytest.mark.parametrize("preserved", [None, False])
def test_unverified_preservation_requires_substantive_decision(no_loss, preserved) -> None:
    facts = replace(
        _CLEAN_FAILURE, no_loss_confirmed=no_loss, preservation_verified=preserved,
    )
    assert terminal_release_decision("crash", facts) == "operator_decision"


def test_verified_preservation_can_prove_safety_but_cannot_replace_cleanup() -> None:
    facts = replace(_CLEAN_FAILURE, no_loss_confirmed=False, preservation_verified=True)
    assert terminal_release_decision("crash", facts) == "release"
    assert terminal_release_decision(
        "crash", replace(facts, cleanup_confirmed=None),
    ) == "wait"
    assert terminal_release_decision(
        "crash", replace(facts, worktree_stable=False),
    ) == "wait"


def test_quota_wait_requires_existing_wake_and_positive_safety_facts() -> None:
    facts = replace(_CLEAN_FAILURE, cost_hold=True)
    assert terminal_release_decision("rate_limit", facts) == "wait"
    assert terminal_release_decision("rate_limit", replace(facts, cost_hold=False)) == "release"


def test_release_is_read_only_and_cannot_retry_or_clear_the_failed_task() -> None:
    # No runner/counter/cancellation mutation is exposed by this policy.
    before = replace(_CLEAN_FAILURE)
    decision = terminal_release_decision("coder_escalate", _CLEAN_FAILURE)
    assert decision is TerminalReleaseDecision.RELEASE
    assert _CLEAN_FAILURE == before
    assert set(TerminalReleaseDecision) == {
        TerminalReleaseDecision.RELEASE,
        TerminalReleaseDecision.WAIT,
        TerminalReleaseDecision.OPERATOR_DECISION,
    }
