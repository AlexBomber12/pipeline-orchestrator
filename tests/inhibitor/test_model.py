"""Unit tests for the WorkInhibitor typed model."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from src.inhibitor import InhibitorType, WorkInhibitor


@pytest.mark.parametrize("inhibitor_type", list(InhibitorType))
def test_all_8_inhibitor_types_instantiable(inhibitor_type: InhibitorType) -> None:
    inhibitor = WorkInhibitor(
        inhibitor_type=inhibitor_type,
        source_key=f"key:{inhibitor_type.value}",
    )
    assert inhibitor.inhibitor_type is inhibitor_type


def test_is_blocking_now_returns_true_when_no_expiry() -> None:
    inhibitor = WorkInhibitor(
        inhibitor_type=InhibitorType.USER_PAUSE,
        source_key="control:user_paused",
    )
    assert inhibitor.is_blocking_now() is True


def test_is_blocking_now_returns_false_after_expiry() -> None:
    past = datetime.now(timezone.utc) - timedelta(seconds=60)
    inhibitor = WorkInhibitor(
        inhibitor_type=InhibitorType.USER_STOP,
        expires_at=past,
        source_key="control:stop",
    )
    assert inhibitor.is_blocking_now() is False


def test_time_remaining_seconds_returns_none_when_no_expiry() -> None:
    inhibitor = WorkInhibitor(
        inhibitor_type=InhibitorType.CASCADE_PANIC,
        source_key="cascade:panic",
    )
    assert inhibitor.time_remaining_seconds() is None


def test_time_remaining_seconds_returns_positive_for_future_expiry() -> None:
    future = datetime.now(timezone.utc) + timedelta(seconds=120)
    inhibitor = WorkInhibitor(
        inhibitor_type=InhibitorType.RATE_LIMIT,
        coder_affected="claude",
        expires_at=future,
        source_key="rate_limited_coder_until:claude",
    )
    remaining = inhibitor.time_remaining_seconds()
    assert remaining is not None
    assert remaining > 0


def test_time_remaining_seconds_clamps_to_zero_for_past_expiry() -> None:
    past = datetime.now(timezone.utc) - timedelta(seconds=30)
    inhibitor = WorkInhibitor(
        inhibitor_type=InhibitorType.RATE_LIMIT,
        coder_affected="claude",
        expires_at=past,
        source_key="rate_limited_coder_until:claude",
    )
    assert inhibitor.time_remaining_seconds() == 0.0


def test_is_per_coder_true_when_coder_affected_set() -> None:
    inhibitor = WorkInhibitor(
        inhibitor_type=InhibitorType.RATE_LIMIT,
        coder_affected="claude",
        source_key="rate_limited_coder_until:claude",
    )
    assert inhibitor.is_per_coder() is True


def test_is_per_coder_false_when_coder_affected_none() -> None:
    inhibitor = WorkInhibitor(
        inhibitor_type=InhibitorType.USER_PAUSE,
        source_key="control:user_paused",
    )
    assert inhibitor.is_per_coder() is False


def test_model_is_frozen() -> None:
    inhibitor = WorkInhibitor(
        inhibitor_type=InhibitorType.USER_PAUSE,
        source_key="control:user_paused",
    )
    with pytest.raises(ValidationError):
        inhibitor.reason_text = "mutated"


def test_naive_expires_at_is_normalized_to_utc_for_is_blocking_now() -> None:
    naive_future = (datetime.now(timezone.utc) + timedelta(seconds=120)).replace(
        tzinfo=None
    )
    assert naive_future.tzinfo is None
    inhibitor = WorkInhibitor(
        inhibitor_type=InhibitorType.USER_STOP,
        expires_at=naive_future,
        source_key="control:stop",
    )
    assert inhibitor.expires_at is not None
    assert inhibitor.expires_at.tzinfo is timezone.utc
    assert inhibitor.is_blocking_now() is True


def test_naive_expires_at_is_normalized_to_utc_for_time_remaining_seconds() -> None:
    naive_past = (datetime.now(timezone.utc) - timedelta(seconds=30)).replace(
        tzinfo=None
    )
    assert naive_past.tzinfo is None
    inhibitor = WorkInhibitor(
        inhibitor_type=InhibitorType.RATE_LIMIT,
        coder_affected="claude",
        expires_at=naive_past,
        source_key="rate_limited_coder_until:claude",
    )
    assert inhibitor.time_remaining_seconds() == 0.0


def test_explicit_none_expires_at_passes_through_validator() -> None:
    inhibitor = WorkInhibitor(
        inhibitor_type=InhibitorType.USER_PAUSE,
        expires_at=None,
        source_key="control:user_paused",
    )
    assert inhibitor.expires_at is None


def test_aware_non_utc_expires_at_is_converted_to_utc() -> None:
    pacific = timezone(timedelta(hours=-8))
    aware_pacific = datetime(2030, 6, 15, 4, 0, tzinfo=pacific)
    inhibitor = WorkInhibitor(
        inhibitor_type=InhibitorType.GITHUB_BUDGET_PAUSE,
        expires_at=aware_pacific,
        source_key="github_rate_limit_budget_pause",
    )
    assert inhibitor.expires_at == datetime(2030, 6, 15, 12, 0, tzinfo=timezone.utc)


def test_json_round_trip_preserves_fields() -> None:
    original = WorkInhibitor(
        inhibitor_type=InhibitorType.GITHUB_BUDGET_SLOWDOWN,
        coder_affected=None,
        expires_at=datetime(2030, 6, 15, 12, 0, tzinfo=timezone.utc),
        reason_text="GitHub REST budget below slowdown threshold",
        source_key="github_rate_limit_budget_slowdown",
    )
    payload = original.model_dump_json()
    restored = WorkInhibitor.model_validate_json(payload)
    assert restored == original
