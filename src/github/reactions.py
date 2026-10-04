"""Codex-bot reaction tracking helpers.

Owns the Codex-bot identification helpers (login pattern, user check,
onboarding-comment detection) and the reaction-content tests that the
review-status logic and the comments helpers both rely on. Splitting
these out of ``reviews.py`` keeps the review-status decision tree free
of low-level "is this user a codex bot" plumbing.
"""

from __future__ import annotations

import logging

from src.config import load_config
from src.github import cache, gh_runner
from src.github.reviewer_policy import ReviewerPolicy, reviewer_policy_from_config
from src.retry import is_transient_error

logger = logging.getLogger(__name__)

_CODEX_ONBOARDING_TEXT = "create a Codex account and connect to github"


def _default_reviewer_policy() -> ReviewerPolicy:
    return reviewer_policy_from_config(load_config())


def _is_codex_user(
    user_dict: dict | None,
    policy: ReviewerPolicy | None = None,
) -> bool:
    """Return True if the GitHub user object is a trusted reviewer actor."""
    selected = policy if policy is not None else _default_reviewer_policy()
    return selected.is_trusted_user(user_dict)


def _is_codex_onboarding_comment(comment: dict) -> bool:
    """Return True for Codex connector setup guidance, not review feedback."""
    body = comment.get("body") or ""
    return _CODEX_ONBOARDING_TEXT.lower() in body.lower()


def _is_reaction_content(
    reaction: dict,
    content: str,
    policy: ReviewerPolicy | None = None,
) -> bool:
    """Return True when a reaction matches an exact content from Codex."""
    if not isinstance(reaction, dict):
        return False
    if reaction.get("content") != content:
        return False
    return _is_codex_user(reaction.get("user"), policy=policy)


def _is_plus_one(
    reaction: dict,
    policy: ReviewerPolicy | None = None,
) -> bool:
    """Return True if the reaction is exactly +1 from a Codex user."""
    return _is_reaction_content(reaction, "+1", policy=policy)


def _should_degrade_reactions_error(exc: RuntimeError) -> bool:
    return gh_runner._is_http_404_error(exc) or is_transient_error(exc)


def _find_codex_plus_one_reaction(
    reactions: list[dict],
    policy: ReviewerPolicy | None = None,
) -> dict | None:
    """Return the most recent +1 reaction from a Codex user, or None."""
    best: dict | None = None
    for r in reactions:
        if not _is_plus_one(r, policy=policy):
            continue
        if best is None or (r.get("created_at") or "") > (best.get("created_at") or ""):
            best = r
    return best


def _get_codex_issue_reactions(
    repo: str,
    pr_number: int,
    policy: ReviewerPolicy | None = None,
) -> list[dict]:
    """Fetch Codex reactions on a PR body."""
    try:
        reactions = cache._gh_api_paginated(f"repos/{repo}/issues/{pr_number}/reactions")
    except RuntimeError as exc:
        if gh_runner._is_http_404_error(exc):
            return []
        if not is_transient_error(exc):
            raise
        logger.warning(
            "Reactions fetch degraded for PR %s in %s: %s",
            pr_number,
            repo,
            exc,
        )
        return []
    if not reactions:
        return []
    selected = policy if policy is not None else _default_reviewer_policy()
    return [
        r
        for r in reactions
        if isinstance(r, dict) and _is_codex_user(r.get("user"), policy=selected)
    ]
