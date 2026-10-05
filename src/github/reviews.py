"""Codex review status, freshness, and dedup helpers.

Owns the per-PR review status computation that powers the Codex Review
gate (``ReviewStatus.APPROVED`` / ``EYES`` / ``CHANGES_REQUESTED`` /
``PENDING``). Codex-bot identification and reaction helpers live in
:mod:`src.github.reactions`.
"""

from __future__ import annotations

import logging
from datetime import datetime

from src.config import load_config
from src.github import cache
from src.github import reactions as _reactions
from src.github.gh_runner import (
    _extract_commit_date,
    _is_http_404_error,
    _parse_iso,
)
from src.github.reviewer_policy import ReviewerPolicy, reviewer_policy_from_config
from src.models import ReviewStatus

logger = logging.getLogger(__name__)

_review_status_cache: dict[str, "ReviewStatus"] = {}
_review_status_cache_cycle: int | None = None


def _cache_key(
    repo: str, pr_number: int, head_sha: str, policy: ReviewerPolicy
) -> str:
    return f"{repo}#{pr_number}#{head_sha}#{policy.fingerprint!r}"


def clear_review_status_cache() -> None:
    """Clear the review status cache (used in tests)."""
    global _review_status_cache_cycle
    _review_status_cache.clear()
    _review_status_cache_cycle = None


def _begin_review_cache_cycle() -> None:
    """Start a new cache cycle, invalidating all previous entries."""
    global _review_status_cache_cycle
    _review_status_cache.clear()
    if _review_status_cache_cycle is None:
        _review_status_cache_cycle = 0
    _review_status_cache_cycle += 1


def _get_commit_time(repo: str, sha: str) -> datetime | None:
    """Return the committer date of a commit, or None on failure."""
    try:
        payload = cache._etag_get(f"repos/{repo}/commits/{sha}")
    except RuntimeError:
        return None
    return _parse_iso(_extract_commit_date(payload))


def get_pr_review_status(
    repo: str,
    pr_number: int,
    pr_author: str = "",
    head_sha: str = "",
    policy: ReviewerPolicy | None = None,
) -> ReviewStatus:
    """Derive a Codex review status from PR issue comments, review comments, and reactions.

    Logic:
    1. Find the latest ``@codex review`` trigger comment by the PR author.
    2. Check reactions on that comment from Codex: +1 → APPROVED, eyes → EYES.
    3. If neither reaction, scan Codex comments (issue + review) posted after
       the anchor for P1/P2 → CHANGES_REQUESTED.
    4. Otherwise → PENDING.
    """
    reviewer_policy = (
        policy if policy is not None else reviewer_policy_from_config(load_config())
    )

    if head_sha:
        ck = _cache_key(repo, pr_number, head_sha, reviewer_policy)
        cached = _review_status_cache.get(ck)
        if cached is not None:
            return cached

    result = _compute_review_status(
        repo, pr_number, pr_author, head_sha, reviewer_policy
    )

    if head_sha:
        _review_status_cache[_cache_key(repo, pr_number, head_sha, reviewer_policy)] = result
    return result


def _compute_review_status(
    repo: str,
    pr_number: int,
    pr_author: str,
    head_sha: str,
    policy: ReviewerPolicy | None = None,
) -> ReviewStatus:
    """Core review status logic, separated for caching."""
    body_eyes = False
    body_approved = False
    head_commit_time: datetime | None = None
    head_commit_time_loaded = False
    reviewer_policy = (
        policy if policy is not None else reviewer_policy_from_config(load_config())
    )

    try:
        codex_reactions = _reactions._get_codex_issue_reactions(
            repo, pr_number, policy=reviewer_policy
        )
        if codex_reactions:
            plus_one = _reactions._find_codex_plus_one_reaction(
                codex_reactions, policy=reviewer_policy
            )
            if plus_one is not None:
                if head_sha:
                    try:
                        review_info = _get_codex_review_signals(
                            repo,
                            pr_number,
                            policy=reviewer_policy,
                            actor=plus_one.get("user"),
                        )
                    except RuntimeError:
                        review_info = {
                            "latest_sha": "",
                            "latest_time": None,
                            "latest_state": "",
                        }
                    latest_review_time = review_info["latest_time"]
                    latest_review_sha = review_info["latest_sha"]
                    reaction_time = _parse_iso(plus_one.get("created_at"))
                    head_commit_time = _get_commit_time(repo, head_sha)
                    head_commit_time_loaded = True
                    if head_commit_time is not None:
                        threshold = head_commit_time
                        if latest_review_sha != head_sha and (
                            latest_review_time is not None
                            and latest_review_time > threshold
                        ):
                            threshold = latest_review_time
                        if (
                            reaction_time
                            and reaction_time >= threshold
                        ):
                            body_approved = True
            if not body_approved and any(
                _reactions._is_reaction_content(
                    reaction, "eyes", policy=reviewer_policy
                )
                for reaction in codex_reactions
            ):
                body_eyes = True
                return ReviewStatus.EYES
    except RuntimeError as exc:
        if not _is_http_404_error(exc):
            raise

    try:
        issue_comments = cache._gh_api_paginated(f"repos/{repo}/issues/{pr_number}/comments") or []
    except RuntimeError as exc:
        if not _is_http_404_error(exc):
            raise
        issue_comments = []
    try:
        review_comments = cache._gh_api_paginated(f"repos/{repo}/pulls/{pr_number}/comments") or []
    except RuntimeError as exc:
        if not _is_http_404_error(exc):
            raise
        review_comments = []

    anchor = None
    for c in reversed(issue_comments):
        author = (c.get("user") or {}).get("login", "")
        if pr_author and author != pr_author:
            continue
        if "@codex review" in (c.get("body") or "").lower():
            anchor = c
            break

    anchor_approved = False
    anchor_eyes = False
    if anchor is not None:
        cid = anchor.get("id")
        if cid is not None:
            try:
                anchor_reactions = cache._gh_api_paginated(
                    f"repos/{repo}/issues/comments/{cid}/reactions"
                )
                if anchor_reactions:
                    anchor_plus_one = _reactions._find_codex_plus_one_reaction(
                        anchor_reactions,
                        policy=reviewer_policy,
                    )
                    if anchor_plus_one is not None:
                        if head_sha:
                            if not head_commit_time_loaded:
                                head_commit_time = _get_commit_time(repo, head_sha)
                                head_commit_time_loaded = True
                            reaction_time = _parse_iso(
                                anchor_plus_one.get("created_at")
                            )
                            anchor_approved = bool(
                                reaction_time
                                and head_commit_time
                                and reaction_time >= head_commit_time
                            )
                    if not anchor_approved and any(
                        _reactions._is_reaction_content(
                            reaction, "eyes", policy=reviewer_policy
                        )
                        for reaction in anchor_reactions
                    ):
                        anchor_eyes = True
            except RuntimeError as exc:
                if _is_http_404_error(exc):
                    pass
                elif _reactions._should_degrade_reactions_error(exc):
                    logger.warning(
                        "Anchor comment reactions fetch degraded for comment %s in %s: %s",
                        cid,
                        repo,
                        exc,
                    )
                else:
                    raise

    if body_eyes or anchor_eyes:
        return ReviewStatus.EYES
    if body_approved:
        return ReviewStatus.APPROVED
    if anchor_approved:
        return ReviewStatus.APPROVED

    anchor_ts = (anchor.get("created_at") or "") if anchor else ""
    for comment in issue_comments + review_comments:
        if not _reactions._is_codex_user(
            comment.get("user"), policy=reviewer_policy
        ):
            continue
        if _reactions._is_codex_onboarding_comment(comment):
            continue
        if anchor_ts and (comment.get("created_at") or "") <= anchor_ts:
            continue
        return ReviewStatus.CHANGES_REQUESTED

    return ReviewStatus.PENDING


def _get_codex_review_signals(
    repo: str,
    pr_number: int,
    policy: _reactions.ReviewerPolicy | None = None,
    actor: dict | None = None,
) -> dict[str, str | datetime | None]:
    """Return the latest Codex review timestamp, sha, and state."""
    try:
        reviews = cache._gh_api_paginated(f"repos/{repo}/pulls/{pr_number}/reviews")
    except RuntimeError as exc:
        if not _is_http_404_error(exc):
            raise
        return {
            "latest_sha": "",
            "latest_time": None,
            "latest_state": "",
        }
    if not reviews:
        return {
            "latest_sha": "",
            "latest_time": None,
            "latest_state": "",
        }

    best_sha = ""
    best_time: datetime | None = None
    best_raw = ""
    best_state = ""
    actor_id = actor.get("id") if isinstance(actor, dict) else None
    for review in reviews:
        user = review.get("user")
        if not _reactions._is_codex_user(user, policy=policy):
            continue
        if actor_id is not None and (
            not isinstance(user, dict) or user.get("id") != actor_id
        ):
            continue
        submitted_raw = review.get("submitted_at") or ""
        parsed = _parse_iso(submitted_raw)
        if parsed is None:
            continue
        if best_time is None or submitted_raw > best_raw:
            best_sha = review.get("commit_id") or ""
            best_time = parsed
            best_raw = submitted_raw
            best_state = (review.get("state") or "").upper()
    return {
        "latest_sha": best_sha,
        "latest_time": best_time,
        "latest_state": best_state,
    }


def _get_latest_codex_review_info(
    repo: str, pr_number: int
) -> tuple[str, datetime | None]:
    """Return ``(commit_id, submitted_at)`` of the most recent Codex review."""
    reviewer_policy = reviewer_policy_from_config(load_config())
    signals = _get_codex_review_signals(
        repo, pr_number, policy=reviewer_policy
    )
    latest_sha = signals["latest_sha"]
    latest_time = signals["latest_time"]
    return str(latest_sha or ""), latest_time if isinstance(latest_time, datetime) else None
