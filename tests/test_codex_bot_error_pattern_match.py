from __future__ import annotations

from datetime import datetime

import pytest
from src.daemon.handlers.watch import CODEX_BOT_ERROR_PATTERNS

from tests.runner import _helpers as h

OBSERVED_ERROR_BODY = (
    "Codex Review: Something went wrong. Try again later by commenting "
    "@codex review. We were unable to download your code in a timely manner."
)


@pytest.mark.parametrize(
    "pattern",
    [
        "Something went wrong. Try again",
        "Try again later by commenting",
        "unable to download your code",
    ],
)
def test_new_codex_bot_error_patterns_match_observed_body(pattern: str) -> None:
    assert pattern in CODEX_BOT_ERROR_PATTERNS
    assert pattern in OBSERVED_ERROR_BODY


def test_original_codex_bot_error_patterns_still_match_reference_bodies() -> None:
    reference_bodies = {
        "Something went wrong while reviewing": (
            "Codex Review: Something went wrong while reviewing this PR."
        ),
        "error reviewing this PR": "Codex Review: error reviewing this PR.",
        "Please try again": "Codex Review: Please try again later.",
        "unable to complete review": (
            "Codex Review: unable to complete review for this PR."
        ),
    }

    for pattern, body in reference_bodies.items():
        assert pattern in CODEX_BOT_ERROR_PATTERNS
        assert pattern in body


def test_codex_bot_download_error_retriggers_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    posted: list[tuple[int, bool]] = []
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [h._codex_bot_error_comment(body=OBSERVED_ERROR_BODY)],
    )

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        posted.append((number, bypass_same_head_dedup))
        return True, True, None

    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    assert runner._maybe_retrigger_on_codex_bot_error(42) is True
    assert posted == [(42, True)]
