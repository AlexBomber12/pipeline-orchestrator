from __future__ import annotations

import pytest
from src.config import AppConfig, DaemonConfig, TrustedReviewerIdentity
from src.github.reviewer_policy import reviewer_policy_from_config

VERIFIED_ID = 199175422


def _policy(*identities: TrustedReviewerIdentity):
    config = AppConfig(
        daemon=DaemonConfig(
            trusted_reviewer_identities=list(identities)
            or [
                TrustedReviewerIdentity(
                    user_id=VERIFIED_ID,
                    login="chatgpt-codex-connector[bot]",
                )
            ]
        )
    )
    return reviewer_policy_from_config(config)


def test_impersonating_codex_login_with_different_id_is_untrusted() -> None:
    policy = _policy()

    assert (
        policy.is_trusted_user(
            {
                "id": 404,
                "login": "chatgpt-codex-connector[bot]",
                "type": "Bot",
            }
        )
        is False
    )


def test_stable_id_rename_remains_trusted_without_bot_type() -> None:
    policy = _policy()

    assert (
        policy.is_trusted_user(
            {
                "id": VERIFIED_ID,
                "login": "renamed-reviewer",
                "type": "User",
            }
        )
        is True
    )


def test_explicit_second_actor_is_trusted_by_id() -> None:
    policy = _policy(
        TrustedReviewerIdentity(
            user_id=VERIFIED_ID,
            login="chatgpt-codex-connector[bot]",
        ),
        TrustedReviewerIdentity(user_id=200, login="second-reviewer"),
    )

    assert policy.is_trusted_user({"id": 200, "login": "second-reviewer"})
    assert policy.identities[-1].login == "second-reviewer"
    assert policy.diagnostic_login(200) == "second-reviewer"
    assert policy.diagnostic_login(999) is None


def test_fingerprint_is_stable_for_same_identity_set() -> None:
    policy_a = _policy(
        TrustedReviewerIdentity(user_id=200, login="second-reviewer"),
        TrustedReviewerIdentity(user_id=VERIFIED_ID, login="chatgpt-codex-connector[bot]"),
    )
    policy_b = _policy(
        TrustedReviewerIdentity(user_id=VERIFIED_ID, login="chatgpt-codex-connector[bot]"),
        TrustedReviewerIdentity(user_id=200, login="second-reviewer"),
    )

    assert policy_a.fingerprint == policy_b.fingerprint


@pytest.mark.parametrize(
    "user",
    [
        None,
        {},
        {"login": "chatgpt-codex-connector[bot]"},
        {"id": "199175422", "login": "chatgpt-codex-connector[bot]"},
        {"id": True, "login": "chatgpt-codex-connector[bot]"},
    ],
)
def test_missing_malformed_and_boolean_user_ids_are_untrusted(
    user: dict | None,
) -> None:
    policy = _policy()

    assert policy.is_trusted_user(user) is False
