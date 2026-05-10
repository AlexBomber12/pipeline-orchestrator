"""Tests for error_message recovery lifecycle helper."""

from __future__ import annotations

import asyncio

from tests.runner import _helpers as h


def test_clear_error_message_on_recovery_no_op_when_already_none() -> None:
    runner = h._make_runner()
    runner.state.error_message = None
    events: list[str] = []
    runner.log_event = events.append  # type: ignore[method-assign]

    asyncio.run(
        runner._clear_error_message_on_recovery(
            log_prefix="[CODING]",
            reason="coder pushed",
        )
    )

    assert runner.state.error_message is None
    assert events == []


def test_clear_error_message_on_recovery_clears_and_logs() -> None:
    runner = h._make_runner()
    runner.state.error_message = "something failed"
    events: list[str] = []
    runner.log_event = events.append  # type: ignore[method-assign]

    asyncio.run(
        runner._clear_error_message_on_recovery(
            log_prefix="[CODING]",
            reason="coder pushed",
        )
    )

    assert runner.state.error_message is None
    assert events == [
        "[CODING] cleared error_message (coder pushed): something failed"
    ]


def test_clear_error_message_on_recovery_truncates_long_previous() -> None:
    runner = h._make_runner()
    previous = "0123456789" * 20
    runner.state.error_message = previous
    events: list[str] = []
    runner.log_event = events.append  # type: ignore[method-assign]

    asyncio.run(
        runner._clear_error_message_on_recovery(
            log_prefix="[FIX]",
            reason="retry succeeded",
        )
    )

    assert runner.state.error_message is None
    assert events == [
        f"[FIX] cleared error_message (retry succeeded): {previous[:120]}"
    ]
    assert events[0].endswith(previous[:120])
    assert len(events[0]) == len("[FIX] cleared error_message (retry succeeded): ") + 120


def test_clear_error_message_on_recovery_publish_flag() -> None:
    runner = h._make_runner()
    runner.state.error_message = "recoverable failure"
    published = {"count": 0}

    async def fake_publish_state_for_repo() -> None:
        published["count"] += 1

    runner._publish_state_for_repo = fake_publish_state_for_repo  # type: ignore[method-assign]

    asyncio.run(
        runner._clear_error_message_on_recovery(
            log_prefix="[ERROR]",
            reason="retry succeeded",
            publish=True,
        )
    )

    assert runner.state.error_message is None
    assert published["count"] == 1


def test_publish_state_for_repo_delegates_to_publish_state() -> None:
    runner = h._make_runner()
    published = {"count": 0}

    async def fake_publish_state() -> None:
        published["count"] += 1

    runner.publish_state = fake_publish_state  # type: ignore[method-assign]

    asyncio.run(runner._publish_state_for_repo())

    assert published["count"] == 1
