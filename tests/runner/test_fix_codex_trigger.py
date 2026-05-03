"""Direct unit tests for ``src/daemon/fix_codex_trigger.py`` (PR-230)."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from src.daemon import fix_codex_trigger
from src.models import PipelineState
from tests.runner import _helpers as h


def _stub_skip(runner: Any, *, returns: bool) -> None:
    runner._should_skip_codex_review_post = lambda pr_number: returns


def _stub_post(runner: Any, *, returns: bool, capture: list[int] | None = None) -> None:
    def _post(pr_number: int, **kwargs: Any) -> bool:
        if capture is not None:
            capture.append(pr_number)
        return returns

    runner._post_codex_review = _post


def test_maybe_post_skips_and_returns_true_when_codex_already_triggered() -> None:
    """Fresh EYES skip path logs and reports success without posting."""
    runner = h._make_runner()
    _stub_skip(runner, returns=True)
    posts: list[int] = []
    _stub_post(runner, returns=True, capture=posts)

    result = asyncio.run(
        fix_codex_trigger.maybe_post_codex_review_after_push(
            runner, 600, "after fix push"
        )
    )

    assert result is True
    assert posts == []  # _post_codex_review was NOT called
    assert any(
        "Codex auto-trigger detected, skipping duplicate @codex review post."
        in event["event"]
        for event in runner.state.history
    )


def test_maybe_post_returns_true_after_successful_post() -> None:
    """No fresh EYES + successful post: True without ERROR transition."""
    runner = h._make_runner()
    _stub_skip(runner, returns=False)
    posts: list[int] = []
    _stub_post(runner, returns=True, capture=posts)

    result = asyncio.run(
        fix_codex_trigger.maybe_post_codex_review_after_push(
            runner, 601, "after fix push"
        )
    )

    assert result is True
    assert posts == [601]
    assert runner.state.state != PipelineState.ERROR


def test_maybe_post_transitions_to_error_when_post_fails() -> None:
    """Failed ``_post_codex_review`` returns False AND transitions to ERROR."""
    runner = h._make_runner()
    _stub_skip(runner, returns=False)
    _stub_post(runner, returns=False)

    result = asyncio.run(
        fix_codex_trigger.maybe_post_codex_review_after_push(
            runner,
            602,
            "after stop-cancel fix push; manual review trigger required",
        )
    )

    assert result is False
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message is not None
    assert "Failed to post @codex review on PR #602" in runner.state.error_message
    assert (
        "after stop-cancel fix push" in runner.state.error_message
    ), "failure_detail must propagate into error_message"


def test_maybe_post_failure_emits_fix_prefixed_log(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Error transition uses the ``[FIX]`` log prefix for grouping."""
    runner = h._make_runner()
    _stub_skip(runner, returns=False)
    _stub_post(runner, returns=False)

    asyncio.run(
        fix_codex_trigger.maybe_post_codex_review_after_push(
            runner, 603, "after fix push"
        )
    )

    assert any(
        event["event"].startswith("[FIX] Failed to post @codex review on PR #603")
        for event in runner.state.history
    )
