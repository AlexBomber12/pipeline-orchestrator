from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from src.daemon.handlers import watch as watch_module
from src.models import CIStatus, PipelineState, PRInfo, ReviewStatus

from tests.runner import _helpers as h


def _capture_events(runner: object) -> list[str]:
    events: list[str] = []
    runner.log_event = events.append  # type: ignore[attr-defined]
    return events


def _watching_runner(review_status: ReviewStatus) -> tuple[object, list[str]]:
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.PENDING,
        review_status=review_status,
    )
    runner.state.state = PipelineState.WATCH
    return runner, _capture_events(runner)


def test_stale_retrigger_logs_missing_current_pr() -> None:
    runner = h._make_runner()
    events = _capture_events(runner)

    assert asyncio.run(runner._maybe_retrigger_stale_review(42)) is False
    assert "[WATCH] Stale retrigger skipped: no current_pr." in events


def test_stale_retrigger_logs_ineligible_review_status() -> None:
    runner, events = _watching_runner(ReviewStatus.APPROVED)

    assert asyncio.run(runner._maybe_retrigger_stale_review(42)) is False
    assert any("review_status APPROVED not eligible" in event for event in events)


def test_stale_retrigger_logs_unavailable_last_push_age(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, events = _watching_runner(ReviewStatus.CHANGES_REQUESTED)
    monkeypatch.setattr("src.github.prs.get_last_push_age_seconds", lambda repo, number: None)

    assert asyncio.run(runner._maybe_retrigger_stale_review(42)) is False
    assert any("last_push_age unavailable" in event for event in events)


def test_stale_retrigger_logs_below_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, events = _watching_runner(ReviewStatus.CHANGES_REQUESTED)
    runner.app_config.daemon.stale_review_threshold_min = 10
    monkeypatch.setattr("src.github.prs.get_last_push_age_seconds", lambda repo, number: 300)

    assert asyncio.run(runner._maybe_retrigger_stale_review(42)) is False
    assert any("push age 300s below threshold 600s" in event for event in events)


def test_stale_retrigger_logs_active_debounce(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc)

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)
    runner, events = _watching_runner(ReviewStatus.CHANGES_REQUESTED)
    runner.app_config.daemon.stale_review_threshold_min = 10
    runner.state.last_stale_retrigger_at = now - timedelta(minutes=30)
    monkeypatch.setattr("src.github.prs.get_last_push_age_seconds", lambda repo, number: 900)

    assert asyncio.run(runner._maybe_retrigger_stale_review(42)) is False
    assert any("debounce window active" in event for event in events)


def test_stale_retrigger_skip_logs_are_deduped_per_pr_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, events = _watching_runner(ReviewStatus.CHANGES_REQUESTED)
    runner.app_config.daemon.stale_review_threshold_min = 10
    monkeypatch.setattr("src.github.prs.get_last_push_age_seconds", lambda repo, number: 300)

    assert asyncio.run(runner._maybe_retrigger_stale_review(42)) is False
    assert asyncio.run(runner._maybe_retrigger_stale_review(42)) is False
    assert sum("push age 300s below threshold 600s" in event for event in events) == 1
