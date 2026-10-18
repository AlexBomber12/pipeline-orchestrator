"""Codex review status, freshness, and dedup helpers.

Owns the per-PR review status computation that powers the Codex Review
gate (``ReviewStatus.APPROVED`` / ``EYES`` / ``CHANGES_REQUESTED`` /
``PENDING``). Codex-bot identification and reaction helpers live in
:mod:`src.github.reactions`.
"""

from __future__ import annotations

import logging
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone

from src.config import load_config
from src.github import cache
from src.github import reactions as _reactions
from src.github.gh_runner import (
    _is_http_404_error,
    _parse_iso,
)
from src.github.reviewer_policy import ReviewerPolicy, reviewer_policy_from_config
from src.models import ReviewStatus

logger = logging.getLogger(__name__)
REVIEW_FEEDBACK_TRUNCATE_CHARS = 5000

_PRIORITY_FINDING = re.compile(r"(?:^|\n)\s*(?:\*\*)?\[?P[0-3]\b", re.I)
_RECEIPT = re.compile(
    r"^<!-- codex-review-receipt:v1 -->|"
    r"^(?:codex review:\s*)?(?:didn't find any major issues(?:\. delightful!)?|"
    r"no (?:major )?(?:issues|findings))[.!\s]*$|"
    r"^(?:lgtm|looks good overall)[.!\s]*$", re.I,
)
_PROGRESS = re.compile(
    r"^(?:review (?:is )?(?:in progress|started|complete)|"
    r"reviewing (?:this|your|the) pr|reviewing now)[.!\s]*$", re.I,
)
_PROVIDER_ERROR = re.compile(
    r"^(?:unable to (?:review|complete)\b|encountered an error\b|"
    r"something went wrong while reviewing\b)", re.I,
)
_SUMMARY = re.compile(r"^(?:\*\*)?(?:review )?(?:summary|status update)(?:\*\*)?:", re.I)


@dataclass(frozen=True)
class ReviewFeedback:
    """Actionable text and source identity from one reviewer-policy snapshot."""

    findings: tuple[str, ...]
    source_ids: tuple[int, ...]
    unknown_reason: str | None = None
    sources: tuple[tuple[str, int, int], ...] = ()
    latest_blocking_at: datetime | None = None
    undated_blocking: bool = False
    latest_finding_at: datetime | None = None


def format_review_feedback(findings: tuple[str, ...]) -> str | None:
    """Bound the exact admitted findings for a coder CLI argument."""
    if not findings:
        return None
    joined = "\n\n".join(findings)
    if len(joined) > REVIEW_FEEDBACK_TRUNCATE_CHARS:
        return "[truncated]\n" + joined[-REVIEW_FEEDBACK_TRUNCATE_CHARS:]
    return joined


def classify_review_comment(comment: dict, *, inline: bool = False) -> str:
    """Classify trusted reviewer text without treating receipts as findings."""
    body = (comment.get("body") or "").strip()
    if not body:
        return "unknown"
    if _RECEIPT.search(body):
        return "receipt"
    if _reactions._is_codex_onboarding_comment(comment):
        return "onboarding"
    if inline or _PRIORITY_FINDING.search(body):
        return "finding"
    if _SUMMARY.search(body):
        return "summary"
    if _PROVIDER_ERROR.search(body):
        return "provider_error"
    if _PROGRESS.search(body):
        return "progress"
    return "finding"


def feedback_from_comments(
    issue_comments: list[dict],
    review_comments: list[dict],
    pr_author: str,
    policy: ReviewerPolicy,
    after: datetime | None = None,
) -> ReviewFeedback:
    """Use the latest author anchor and optional push floor for both verdict and FIX."""
    anchor = next(
        (
            c
            for c in reversed(issue_comments)
            if (not pr_author or (c.get("user") or {}).get("login") == pr_author)
            and "@codex review" in (c.get("body") or "").lower()
        ),
        None,
    )
    anchor_time = _parse_iso(anchor.get("created_at")) if anchor else None
    if anchor_time is not None and anchor_time.tzinfo is None:
        anchor_time = anchor_time.replace(tzinfo=timezone.utc)
    if after is not None and after.tzinfo is None:
        after = after.replace(tzinfo=timezone.utc)
    findings: list[str] = []
    ids: list[int] = []
    sources: list[tuple[str, int, int]] = []
    unknown = False
    latest_blocking_at: datetime | None = None
    undated_blocking = False
    latest_finding_at: datetime | None = None
    for inline, comments in ((False, issue_comments), (True, review_comments)):
        for comment in comments:
            if not _reactions._is_codex_user(comment.get("user"), policy=policy):
                continue
            created = _parse_iso(comment.get("created_at"))
            if created is not None and created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
            if (anchor_time and (created is None or created <= anchor_time)) or (
                after and (created is None or created <= after)
            ):
                continue
            kind = classify_review_comment(comment, inline=inline)
            if kind in ("finding", "unknown", "provider_error"):
                if created is None:
                    undated_blocking = True
                elif latest_blocking_at is None or created > latest_blocking_at:
                    latest_blocking_at = created
            if kind == "finding":
                if created is not None and (
                    latest_finding_at is None or created > latest_finding_at
                ):
                    latest_finding_at = created
                findings.append((comment.get("body") or "").strip())
                comment_id = comment.get("id") or 0
                ids.append(comment_id)
                sources.append((
                    "review" if inline else "issue", comment_id,
                    (comment.get("user") or {}).get("id") or 0,
                ))
            elif kind in ("unknown", "provider_error"):
                unknown = True
    return ReviewFeedback(
        tuple(findings), tuple(ids),
        (
            "trusted review text has no supported finding shape" if unknown and not findings
            else None
        ),
        tuple(sources),
        latest_blocking_at,
        undated_blocking,
        latest_finding_at,
    )


def read_review_feedback(
    repo: str,
    pr_number: int,
    pr_author: str,
    policy: ReviewerPolicy,
    after: datetime | None = None,
) -> ReviewFeedback:
    """Read current actionable evidence; transport failures remain pending."""
    try:
        issues = cache._gh_api_paginated(f"repos/{repo}/issues/{pr_number}/comments") or []
        inline = cache._gh_api_paginated(f"repos/{repo}/pulls/{pr_number}/comments") or []
    except (RuntimeError, subprocess.TimeoutExpired, OSError) as exc:
        return ReviewFeedback((), (), f"review feedback unavailable: {exc}")
    return feedback_from_comments(issues, inline, pr_author, policy, after)


_review_status_cache: dict[str, "ReviewStatus"] = {}
_review_author_cache: dict[str, str] = {}
_review_status_cache_cycle: int | None = None


def _cache_key(
    repo: str, pr_number: int, head_sha: str, policy: ReviewerPolicy
) -> str:
    return f"{repo}#{pr_number}#{head_sha}#{policy.fingerprint!r}"


def clear_review_status_cache() -> None:
    """Clear the review status cache (used in tests)."""
    global _review_status_cache_cycle
    _review_status_cache.clear()
    _review_author_cache.clear()
    _review_status_cache_cycle = None


def _begin_review_cache_cycle() -> None:
    """Start a new cache cycle, invalidating all previous entries."""
    global _review_status_cache_cycle
    _review_status_cache.clear()
    _review_author_cache.clear()
    if _review_status_cache_cycle is None:
        _review_status_cache_cycle = 0
    _review_status_cache_cycle += 1


def _get_pr_push_time(repo: str, pr_number: int) -> datetime | None:
    """Return the actual latest push time without creating an import cycle."""
    from src.github import prs

    return prs.get_pr_last_push_time(repo, pr_number)


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
        _review_author_cache[ck] = pr_author
        cached = _review_status_cache.get(ck)
        if cached is not None:
            return cached

    result = _compute_review_status(
        repo, pr_number, pr_author, head_sha, reviewer_policy
    )

    if head_sha:
        _review_status_cache[_cache_key(repo, pr_number, head_sha, reviewer_policy)] = result
    return result


def cached_review_author(
    repo: str, pr_number: int, head_sha: str, policy: ReviewerPolicy
) -> str:
    """Use the author and policy snapshot from the PR status poll."""
    return _review_author_cache.get(_cache_key(repo, pr_number, head_sha, policy), "")


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
    body_approval_at: datetime | None = None
    saw_plus_one = False
    last_push_time: datetime | None = None
    last_push_time_loaded = False
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
                saw_plus_one = True
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
                    last_push_time = _get_pr_push_time(repo, pr_number)
                    last_push_time_loaded = True
                    if last_push_time is not None:
                        threshold = last_push_time
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
                            body_approval_at = reaction_time
            if not body_approved and any(
                _reactions._is_reaction_content(
                    reaction, "eyes", policy=reviewer_policy
                )
                for reaction in codex_reactions
            ):
                body_eyes = True
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
    anchor_approval_at: datetime | None = None
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
                        saw_plus_one = True
                        if head_sha:
                            if not last_push_time_loaded:
                                last_push_time = _get_pr_push_time(repo, pr_number)
                                last_push_time_loaded = True
                            reaction_time = _parse_iso(
                                anchor_plus_one.get("created_at")
                            )
                            anchor_approved = bool(
                                reaction_time
                                and last_push_time
                                and reaction_time >= last_push_time
                            )
                            if anchor_approved:
                                anchor_approval_at = reaction_time
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

    feedback = feedback_from_comments(
        issue_comments, review_comments, pr_author, reviewer_policy
    )
    approved_at = max(
        (t for t in (body_approval_at, anchor_approval_at) if t is not None),
        default=None,
    )
    if approved_at and (feedback.findings or feedback.unknown_reason):
        if (
            feedback.latest_blocking_at is not None
            and not feedback.undated_blocking
            and approved_at > feedback.latest_blocking_at
        ):
            return ReviewStatus.APPROVED
        if feedback.latest_finding_at is not None and feedback.latest_finding_at > approved_at:
            return ReviewStatus.CHANGES_REQUESTED
        logger.info("PR #%s review pending: +1 does not supersede current review text", pr_number)
        return ReviewStatus.PENDING
    if body_approved:
        return ReviewStatus.APPROVED
    if anchor_approved:
        return ReviewStatus.APPROVED
    if body_eyes or anchor_eyes:
        return ReviewStatus.EYES
    if feedback.unknown_reason:
        logger.info("PR #%s review pending: %s", pr_number, feedback.unknown_reason)
        return ReviewStatus.PENDING

    if feedback.findings:
        return ReviewStatus.CHANGES_REQUESTED
    if saw_plus_one:
        logger.info(
            "PR #%s approval pending: current HEAD push or reaction provenance unavailable or stale",
            pr_number,
        )

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
