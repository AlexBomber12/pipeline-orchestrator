"""Unit tests for BoundedRecoveryPolicy."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import pytest
from src.daemon.recovery_policy import BoundedRecoveryPolicy


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
