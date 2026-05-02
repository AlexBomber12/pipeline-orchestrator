"""Codex review status, freshness, and dedup helpers.

Owns the per-PR review status computation that powers the Codex Review
gate (``ReviewStatus.APPROVED`` / ``EYES`` / ``CHANGES_REQUESTED`` /
``PENDING``), plus the codex-bot identification helpers shared with the
not-yet-moved reactions/comments code in ``src.github_client``. Cache and
paginated GET primitives live in ``src.github_client`` until PR-226b;
this module accesses them via ``from src import github_client``.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime

from src.github.gh_runner import (
    _extract_commit_date,
    _is_http_404_error,
    _parse_iso,
)
from src.models import ReviewStatus
from src.retry import is_transient_error

logger = logging.getLogger(__name__)

CODEX_BOT_LOGIN_PATTERN = re.compile(r"codex", re.IGNORECASE)
_CODEX_ONBOARDING_TEXT = "create a Codex account and connect to github"

_review_status_cache: dict[str, "ReviewStatus"] = {}
_review_status_cache_cycle: int | None = None


def _cache_key(repo: str, pr_number: int, head_sha: str) -> str:
    return f"{repo}#{pr_number}#{head_sha}"


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


def _is_codex_user(user_dict: dict | None) -> bool:
    """Return True if the GitHub user object represents a Codex bot."""
    if not isinstance(user_dict, dict):
        return False
    login = user_dict.get("login", "") or ""
    return bool(CODEX_BOT_LOGIN_PATTERN.search(login))


def _is_codex_onboarding_comment(comment: dict) -> bool:
    """Return True for Codex connector setup guidance, not review feedback."""
    body = comment.get("body") or ""
    return _CODEX_ONBOARDING_TEXT.lower() in body.lower()


def _is_reaction_content(reaction: dict, content: str) -> bool:
    """Return True when a reaction matches an exact content from Codex."""
    if not isinstance(reaction, dict):
        return False
    if reaction.get("content") != content:
        return False
    return _is_codex_user(reaction.get("user"))


def _is_plus_one(reaction: dict) -> bool:
    """Return True if the reaction is exactly +1 from a Codex user."""
    return _is_reaction_content(reaction, "+1")


def _should_degrade_reactions_error(exc: RuntimeError) -> bool:
    return _is_http_404_error(exc) or is_transient_error(exc)


def _find_codex_plus_one_reaction(reactions: list[dict]) -> dict | None:
    """Return the most recent +1 reaction from a Codex user, or None."""
    best: dict | None = None
    for r in reactions:
        if not _is_plus_one(r):
            continue
        if best is None or (r.get("created_at") or "") > (best.get("created_at") or ""):
            best = r
    return best


def _get_codex_issue_reactions(repo: str, pr_number: int) -> list[dict]:
    """Fetch Codex reactions on a PR body."""
    from src import github_client as _ghc

    try:
        reactions = _ghc._gh_api_paginated(
            f"repos/{repo}/issues/{pr_number}/reactions"
        )
    except RuntimeError as exc:
        if _is_http_404_error(exc):
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
    return [
        r for r in reactions
        if isinstance(r, dict)
        and _is_codex_user(r.get("user"))
    ]


def _get_commit_time(repo: str, sha: str) -> datetime | None:
    """Return the committer date of a commit, or None on failure."""
    from src import github_client as _ghc

    try:
        payload = _ghc._etag_get(f"repos/{repo}/commits/{sha}")
    except RuntimeError:
        return None
    return _parse_iso(_extract_commit_date(payload))


def get_pr_review_status(
    repo: str,
    pr_number: int,
    pr_author: str = "",
    head_sha: str = "",
) -> ReviewStatus:
    """Derive a Codex review status from PR issue comments, review comments, and reactions.

    Logic:
    1. Find the latest ``@codex review`` trigger comment by the PR author.
    2. Check reactions on that comment from Codex: +1 → APPROVED, eyes → EYES.
    3. If neither reaction, scan Codex comments (issue + review) posted after
       the anchor for P1/P2 → CHANGES_REQUESTED.
    4. Otherwise → PENDING.
    """
    if head_sha:
        ck = _cache_key(repo, pr_number, head_sha)
        cached = _review_status_cache.get(ck)
        if cached is not None:
            return cached

    result = _compute_review_status(repo, pr_number, pr_author, head_sha)

    if head_sha:
        _review_status_cache[_cache_key(repo, pr_number, head_sha)] = result
    return result


def _compute_review_status(
    repo: str,
    pr_number: int,
    pr_author: str,
    head_sha: str,
) -> ReviewStatus:
    """Core review status logic, separated for caching."""
    from src import github_client as _ghc

    body_eyes = False
    body_approved = False
    head_commit_time: datetime | None = None

    try:
        codex_reactions = _get_codex_issue_reactions(repo, pr_number)
        if codex_reactions:
            plus_one = _find_codex_plus_one_reaction(codex_reactions)
            if plus_one is not None:
                if head_sha:
                    try:
                        review_info = _get_codex_review_signals(repo, pr_number)
                    except RuntimeError:
                        review_info = {
                            "latest_sha": "",
                            "latest_time": None,
                            "latest_state": "",
                        }
                    latest_review_time = review_info["latest_time"]
                    latest_review_sha = review_info["latest_sha"]
                    reaction_time = _parse_iso(plus_one.get("created_at"))
                    if latest_review_sha and latest_review_sha == head_sha:
                        body_approved = True
                    else:
                        head_commit_time = _get_commit_time(repo, head_sha)
                        threshold = head_commit_time
                        if (
                            latest_review_time is not None
                            and (
                                threshold is None
                                or latest_review_time > threshold
                            )
                        ):
                            threshold = latest_review_time
                        if (
                            reaction_time
                            and threshold
                            and reaction_time >= threshold
                        ):
                            body_approved = True
                        elif not threshold:
                            body_approved = True
                else:
                    body_approved = True
            if not body_approved and any(
                _is_reaction_content(reaction, "eyes")
                for reaction in codex_reactions
            ):
                body_eyes = True
                return ReviewStatus.EYES
    except RuntimeError as exc:
        if not _is_http_404_error(exc):
            raise

    try:
        issue_comments = _ghc._gh_api_paginated(f"repos/{repo}/issues/{pr_number}/comments") or []
    except RuntimeError as exc:
        if not _is_http_404_error(exc):
            raise
        issue_comments = []
    try:
        review_comments = _ghc._gh_api_paginated(f"repos/{repo}/pulls/{pr_number}/comments") or []
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
                reactions = _ghc._gh_api_paginated(
                    f"repos/{repo}/issues/comments/{cid}/reactions"
                )
                if reactions:
                    if any(_is_plus_one(reaction) for reaction in reactions):
                        anchor_approved = True
                    elif any(
                        _is_reaction_content(reaction, "eyes")
                        for reaction in reactions
                    ):
                        anchor_eyes = True
            except RuntimeError as exc:
                if _is_http_404_error(exc):
                    pass
                elif _should_degrade_reactions_error(exc):
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
        if not _is_codex_user(comment.get("user")):
            continue
        if _is_codex_onboarding_comment(comment):
            continue
        if anchor_ts and (comment.get("created_at") or "") <= anchor_ts:
            continue
        return ReviewStatus.CHANGES_REQUESTED

    return ReviewStatus.PENDING


def _get_codex_review_signals(
    repo: str, pr_number: int
) -> dict[str, str | datetime | None]:
    """Return the latest Codex review timestamp, sha, and state."""
    from src import github_client as _ghc

    try:
        reviews = _ghc._gh_api_paginated(f"repos/{repo}/pulls/{pr_number}/reviews")
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
    for review in reviews:
        if not _is_codex_user(review.get("user")):
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
    from src import github_client as _ghc

    signals = _ghc._get_codex_review_signals(repo, pr_number)
    latest_sha = signals["latest_sha"]
    latest_time = signals["latest_time"]
    return str(latest_sha or ""), latest_time if isinstance(latest_time, datetime) else None
