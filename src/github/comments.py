"""PR comment posting, fetching, and Codex feedback aggregation.

Owns the ``post_comment`` write path plus the Codex feedback readers
that the FIX prompt and the dedup gate rely on. Reactions are handled
in :mod:`src.github.reactions`; this module imports the codex-bot
identification helpers from there.
"""

from __future__ import annotations

import subprocess
from datetime import datetime, timezone

from src.config import load_config
from src.github import cache, gh_runner, reviews
from src.github.reviewer_policy import ReviewerPolicy, reviewer_policy_from_config

_REVIEW_FEEDBACK_TRUNCATE_CHARS = reviews.REVIEW_FEEDBACK_TRUNCATE_CHARS

def post_comment(repo: str, pr_number: int, body: str) -> None:
    """Post a comment on a PR via ``gh pr comment``."""
    gh_runner.run_gh(["pr", "comment", str(pr_number), "--body", body], repo=repo)


def get_latest_codex_feedback(
    repo: str,
    pr_number: int,
    policy: ReviewerPolicy | None = None,
) -> str | None:
    """Return concatenated Codex feedback comments after the latest review anchor.

    Pulls from the same sources as ``_compute_review_status`` — Codex-authored
    issue and pull review comments posted after the most recent
    ``@codex review`` trigger by the PR author, with onboarding messages
    filtered out — so the FIX prompt sees exactly the feedback that drove
    ``ReviewStatus.CHANGES_REQUESTED``. Returns ``None`` when no qualifying
    feedback exists or both comment endpoints are unreachable; the FIX
    prompt then omits the section instead of blocking on observability.
    """
    from src.github.prs import get_pr_author

    pr_author = get_pr_author(repo, pr_number)
    reviewer_policy = policy or reviewer_policy_from_config(load_config())
    try:
        issue_comments = cache._gh_api_paginated(f"repos/{repo}/issues/{pr_number}/comments") or []
    except (RuntimeError, subprocess.TimeoutExpired, OSError):
        issue_comments = []
    try:
        review_comments = cache._gh_api_paginated(f"repos/{repo}/pulls/{pr_number}/comments") or []
    except (RuntimeError, subprocess.TimeoutExpired, OSError):
        review_comments = []

    evidence = reviews.feedback_from_comments(
        issue_comments, review_comments, pr_author, reviewer_policy
    )
    return None if evidence.unknown_reason else reviews.format_review_feedback(evidence.findings)


def has_recent_codex_review_request(
    repo: str,
    pr_number: int,
    pr_author: str,
    within_minutes: int = 5,
    after_iso: str | None = None,
) -> bool:
    """Return ``True`` iff ``pr_author`` recently posted ``@codex review``.

    The daemon posts ``@codex review`` after every coding/fix cycle, but
    Claude may also post one itself from the AGENTS.md runbook. Without
    this guard both trigger comments land back-to-back and Codex starts
    two redundant reviews. The caller checks this before posting and
    skips when a qualifying trigger already exists within
    ``within_minutes``.

    ``after_iso`` optionally restricts matches to comments created
    strictly after the given ISO-8601 timestamp. Callers pass the
    PR's current head-commit time so a trigger posted for an earlier
    commit does not suppress the fresh anchor the new commit needs —
    this is what keeps the dedup safe when the daemon and PR author
    share a gh identity.
    """
    return (
        get_recent_codex_review_request_time(
            repo,
            pr_number,
            pr_author,
            within_minutes=within_minutes,
            after_iso=after_iso,
        )
        is not None
    )


def get_recent_codex_review_request_time(
    repo: str,
    pr_number: int,
    pr_author: str,
    within_minutes: int = 5,
    after_iso: str | None = None,
) -> datetime | None:
    """Return the latest qualifying PR-author ``@codex review`` timestamp."""
    try:
        comments = cache._gh_api_paginated(f"repos/{repo}/issues/{pr_number}/comments") or []
    except RuntimeError as exc:
        if gh_runner._is_http_404_error(exc):
            return None
        raise
    now = datetime.now(timezone.utc)
    cutoff = within_minutes * 60
    for c in reversed(comments):
        author = (c.get("user") or {}).get("login", "")
        if author != pr_author:
            continue
        if "@codex review" not in (c.get("body") or "").lower():
            continue
        created_raw = c.get("created_at") or ""
        # Strict-less-than (not <=) lets same-second comments count. GitHub
        # ISO-8601 timestamps are second-granularity; a coder posting
        # ``@codex review`` within one second of pushing the commit produces
        # equal stringified timestamps. PR-239 fixed the regression where
        # same-second comments were skipped from dedup and daemon posted a
        # duplicate trigger.
        if after_iso and (not created_raw or created_raw < after_iso):
            continue
        created = gh_runner._parse_iso(created_raw)
        if created is None:
            continue
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        if (now - created).total_seconds() < cutoff:
            return created
    return None
