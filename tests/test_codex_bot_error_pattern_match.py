from __future__ import annotations

import pytest
from src.daemon.handlers.watch import CODEX_BOT_ERROR_PATTERNS

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
