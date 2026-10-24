"""Review request history is based on explicit current-publication evidence."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from src.github import comments


def test_latest_request_rejects_old_head_and_non_trigger_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda _path: [
            {"body": "@codex review", "created_at": "2026-10-24T11:59:00Z"},
            {"body": "review looks good", "created_at": "2026-10-24T12:05:00Z"},
            {"body": "@codex review", "created_at": "2026-10-24T12:03:00Z"},
        ],
    )
    assert comments.latest_codex_review_request_time(
        "octo/demo", 5, after=datetime(2026, 10, 24, 12, tzinfo=timezone.utc)
    ) == datetime(2026, 10, 24, 12, 3, tzinfo=timezone.utc)


def test_request_history_transport_failure_is_not_absence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(_path: str):
        raise RuntimeError("transport unavailable")

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fail)
    with pytest.raises(RuntimeError, match="transport unavailable"):
        comments.latest_codex_review_request_time(
            "octo/demo", 5, after=datetime(2026, 10, 24, tzinfo=timezone.utc)
        )


def test_request_history_ignores_malformed_time_and_normalizes_naive_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda _path: [
            {"body": "@codex review", "created_at": "bad timestamp"},
            {"body": "@codex review", "created_at": "2026-10-24T12:03:00"},
        ],
    )
    assert comments.latest_codex_review_request_time(
        "octo/demo", 5, after=datetime(2026, 10, 24, 12, tzinfo=timezone.utc)
    ) == datetime(2026, 10, 24, 12, 3, tzinfo=timezone.utc)
