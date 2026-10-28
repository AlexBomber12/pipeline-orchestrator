"""BoundedRecoveryPolicy: declarative threshold/escalate framework.

Encodes the shape used by ad-hoc dirty-tree auto-recovery and FIX
iteration-cap escalation: count consecutive failure cycles, run an
action when the count reaches the threshold, reset on success.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Awaitable, Callable, Generic, TypeVar, Union

from src.subsource_registry import SuppressionReason, lookup

T = TypeVar("T")

OnThresholdCallback = Union[
    Callable[[T], Awaitable[None]],
    Callable[[T], None],
]


@dataclass(frozen=True)
class BoundedRecoveryPolicy(Generic[T]):
    """Threshold/escalate counter framework.

    The policy is agnostic to where the counter lives — accessors
    ``counter_getter`` and ``counter_setter`` operate on the supplied
    context object ``ctx``. ``on_threshold`` is the recovery action
    and may be sync or async; ``maybe_escalate`` adapts both.
    """

    name: str
    max_attempts: int
    counter_getter: Callable[[T], int]
    counter_setter: Callable[[T, int], None]
    on_threshold: OnThresholdCallback[T]

    def increment(self, ctx: T) -> int:
        new_value = self.counter_getter(ctx) + 1
        self.counter_setter(ctx, new_value)
        return new_value

    def reset(self, ctx: T) -> None:
        self.counter_setter(ctx, 0)

    async def maybe_escalate(self, ctx: T) -> bool:
        """Run ``on_threshold`` if the counter has reached ``max_attempts``.

        Returns ``True`` iff the action ran. Communication of action
        success/failure is the action's responsibility — typically
        via mutation of ``ctx`` — and the caller is expected to call
        ``reset`` when appropriate.
        """
        if self.counter_getter(ctx) < self.max_attempts:
            return False
        result: Any = self.on_threshold(ctx)
        if inspect.isawaitable(result):
            await result
        return True


class TerminalReleaseDecision(StrEnum):
    """Permission for independent work only; never permission to retry the task.

    Every result retains failed-task suppression and dependency blocking.
    WAIT uses existing bounded recovery/wake conditions; OPERATOR_DECISION
    requires a substantive decision rather than another technical retry.
    """

    RELEASE = "release"
    WAIT = "wait"
    OPERATOR_DECISION = "operator_decision"


@dataclass(frozen=True)
class TerminalReleaseFacts:
    """Observed safety facts; absent evidence is never a positive observation.

    ``cleanup_confirmed`` means positively confirmed process cleanup, not a
    missing process handle. Remote facts cover ownership and the outcome of
    any creation/merge attempt (including confirmed absence of an attempt).
    Preservation must be verified before it can substitute for no-loss proof.
    """

    failure_persisted: bool | None = None
    remote_ownership_known: bool | None = None
    remote_outcome_resolved: bool | None = None
    cleanup_confirmed: bool | None = None
    worktree_stable: bool | None = None
    no_loss_confirmed: bool | None = None
    preservation_verified: bool | None = None
    user_paused: bool | None = None
    cost_hold: bool | None = None
    security_hold: bool | None = None


def terminal_release_decision(
    subsource: SuppressionReason | str | None,
    facts: TerminalReleaseFacts,
) -> TerminalReleaseDecision:
    """Decide whether a terminal failure may release an independent writer.

    This read-only policy neither clears suppression/status nor selects work.
    Dependents remain blocked and the failed task remains suppressed even on
    RELEASE. It grants no recovery attempts beyond existing bounded policies
    and does not change quota wake conditions or user pause semantics.
    """
    if subsource is None or lookup(subsource) is None:
        return TerminalReleaseDecision.OPERATOR_DECISION
    if (
        subsource in (SuppressionReason.GUARDRAIL, SuppressionReason.OPERATOR_STOPPED)
        or facts.user_paused is True
        or facts.security_hold is True
    ):
        return TerminalReleaseDecision.OPERATOR_DECISION
    if (
        facts.user_paused is not False
        or facts.security_hold is not False
        or facts.cost_hold is not False
        or facts.failure_persisted is not True
        or facts.remote_ownership_known is not True
        or facts.remote_outcome_resolved is not True
        or facts.cleanup_confirmed is not True
        or facts.worktree_stable is not True
    ):
        return TerminalReleaseDecision.WAIT
    if facts.no_loss_confirmed is not True and facts.preservation_verified is not True:
        return TerminalReleaseDecision.OPERATOR_DECISION
    return TerminalReleaseDecision.RELEASE
