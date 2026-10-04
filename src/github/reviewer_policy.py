"""Stable GitHub reviewer identity policy."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping

from src.config import AppConfig, TrustedReviewerIdentity


@dataclass(frozen=True)
class TrustedReviewer:
    user_id: int
    login: str | None = None


class ReviewerPolicy:
    """Immutable trust policy keyed by stable GitHub user IDs."""

    def __init__(self, identities: list[TrustedReviewerIdentity]) -> None:
        by_id: dict[int, TrustedReviewer] = {}
        for identity in identities:
            by_id[identity.user_id] = TrustedReviewer(
                user_id=identity.user_id,
                login=identity.login,
            )
        self._by_id: Mapping[int, TrustedReviewer] = MappingProxyType(by_id)

    @classmethod
    def from_config(cls, config: AppConfig) -> "ReviewerPolicy":
        return cls(list(config.daemon.trusted_reviewer_identities))

    @property
    def identities(self) -> tuple[TrustedReviewer, ...]:
        return tuple(self._by_id.values())

    def is_trusted_user(self, user: dict | None) -> bool:
        if not isinstance(user, dict):
            return False
        user_id = user.get("id")
        if not isinstance(user_id, int) or isinstance(user_id, bool):
            return False
        return user_id in self._by_id

    def diagnostic_login(self, user_id: int) -> str | None:
        identity = self._by_id.get(user_id)
        if identity is None:
            return None
        return identity.login


def reviewer_policy_from_config(config: AppConfig) -> ReviewerPolicy:
    return ReviewerPolicy.from_config(config)
