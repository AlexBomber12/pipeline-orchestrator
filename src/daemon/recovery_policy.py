"""BoundedRecoveryPolicy: declarative threshold/escalate framework.

Encodes the shape used by ad-hoc dirty-tree auto-recovery and FIX
iteration-cap escalation: count consecutive failure cycles, run an
action when the count reaches the threshold, reset on success.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Generic, TypeVar, Union

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
