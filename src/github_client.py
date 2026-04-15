"""Wrapper around the `gh` CLI for reading PR state and performing PR actions."""

from __future__ import annotations

import json
import re
import subprocess
import time
from datetime import datetime, timezone

from src.models import CIStatus, PRInfo, ReviewStatus

_REPO_URL_RE = re.compile(
    r"github\.com[:/]+(?P<owner>[^/]+)/(?P<repo>[^/]+?)(?:\.git)?/?$"
)

_review_status_cache: dict[str, "ReviewStatus"] = {}
_review_status_cache_cycle: int | None = None

_last_known_sha: dict[str, str] = {}


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


def run_gh(
    args: list[str],
    repo: str | None = None,
    timeout: int = 30,
) -> dict | list | str:
    """Run `gh` with the given arguments and return parsed output.

    If ``repo`` is provided, ``-R <repo>`` is appended. The command's stdout is
    parsed as JSON when possible; otherwise the raw stripped string is returned.
    A non-zero exit raises ``RuntimeError`` with stderr.
    """
    cmd: list[str] = ["gh", *args]
    if repo:
        cmd.extend(["-R", repo])

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"gh {' '.join(args)} failed (exit {result.returncode}): "
            f"{result.stderr.strip()}"
        )

    stdout = result.stdout.strip()
    if not stdout:
        return ""

    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        return stdout


def get_repo_full_name(url: str) -> str:
    """Extract ``owner/repo`` from a GitHub URL.

    Accepts ``https://github.com/owner/repo``, ``...repo.git``, ``...repo/``,
    and ``git@github.com:owner/repo.git``.
    """
    match = _REPO_URL_RE.search(url.strip())
    if not match:
        raise ValueError(f"Not a recognizable GitHub URL: {url!r}")
    return f"{match.group('owner')}/{match.group('repo')}"


def get_open_prs(repo: str) -> list[PRInfo]:
    """Return open PRs for ``repo`` (``owner/repo``) with CI and review status."""
    _begin_review_cache_cycle()
    raw = run_gh(
        [
            "pr",
            "list",
            "--state",
            "open",
            "--json",
            "number,headRefName,headRefOid,statusCheckRollup,url,updatedAt,commits,author,isCrossRepository",
        ],
        repo=repo,
    )
    if not isinstance(raw, list):
        return []

    prs: list[PRInfo] = []
    for entry in raw:
        number = int(entry.get("number", 0))
        if not number:
            continue
        commits = entry.get("commits") or []
        head_sha = entry.get("headRefOid", "")
        prs.append(
            PRInfo(
                number=number,
                branch=entry.get("headRefName", ""),
                ci_status=_ci_status_from_rollup(entry.get("statusCheckRollup")),
                review_status=get_pr_review_status(
                    repo,
                    number,
                    pr_author=(entry.get("author") or {}).get("login", ""),
                    head_sha=head_sha,
                ),
                push_count=len(commits),
                url=entry.get("url", ""),
                last_activity=_parse_iso(entry.get("updatedAt")),
                is_cross_repository=bool(entry.get("isCrossRepository", False)),
            )
        )
    return prs


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


def _get_codex_issue_reactions(
    repo: str, pr_number: int
) -> list[dict]:
    """Fetch Codex reactions on a PR body."""
    try:
        reactions = _gh_api_paginated(
            f"repos/{repo}/issues/{pr_number}/reactions"
        )
    except RuntimeError as exc:
        if "HTTP 404" not in str(exc):
            raise
        return []
    if not reactions:
        return []
    return [
        r for r in reactions
        if isinstance(r, dict)
        and isinstance(r.get("user"), dict)
        and re.search(r"codex", r["user"].get("login", ""), re.IGNORECASE)
    ]


def _compute_review_status(
    repo: str,
    pr_number: int,
    pr_author: str,
    head_sha: str,
) -> ReviewStatus:
    """Core review status logic, separated for caching."""
    body_approved = False
    try:
        codex_reactions = _get_codex_issue_reactions(repo, pr_number)
        if codex_reactions:
            codex_contents = {r.get("content") for r in codex_reactions}
            if "+1" in codex_contents:
                if head_sha:
                    plus_one = _find_codex_plus_one_reaction(codex_reactions)
                    if plus_one:
                        reaction_time = _parse_iso(plus_one.get("created_at"))
                        head_commit_time = _get_commit_time(repo, head_sha)
                        latest_sha, latest_review_time = (
                            _get_latest_codex_review_info(repo, pr_number)
                        )
                        if latest_sha and latest_sha == head_sha:
                            body_approved = True
                        else:
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
                else:
                    body_approved = True
            if not body_approved and "eyes" in codex_contents:
                return ReviewStatus.EYES
    except RuntimeError as exc:
        if "HTTP 404" not in str(exc):
            raise

    try:
        issue_comments = _gh_api_paginated(f"repos/{repo}/issues/{pr_number}/comments") or []
    except RuntimeError as exc:
        if "HTTP 404" not in str(exc):
            raise
        issue_comments = []
    try:
        review_comments = _gh_api_paginated(f"repos/{repo}/pulls/{pr_number}/comments") or []
    except RuntimeError as exc:
        if "HTTP 404" not in str(exc):
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
    if anchor is not None:
        cid = anchor.get("id")
        if cid is not None:
            try:
                reactions = _gh_api_paginated(
                    f"repos/{repo}/issues/comments/{cid}/reactions"
                )
                if reactions:
                    codex_contents = {
                        r.get("content")
                        for r in reactions
                        if isinstance(r, dict)
                        and "codex"
                        in ((r.get("user") or {}).get("login", "")).lower()
                    }
                    if "+1" in codex_contents:
                        anchor_approved = True
                    elif "eyes" in codex_contents:
                        return ReviewStatus.EYES
            except RuntimeError as exc:
                if "HTTP 404" not in str(exc):
                    raise

    anchor_ts = (anchor.get("created_at") or "") if anchor else ""
    for comment in issue_comments + review_comments:
        user = (comment.get("user") or {}).get("login", "") or ""
        if "codex" not in user.lower():
            continue
        if anchor_ts and (comment.get("created_at") or "") <= anchor_ts:
            continue
        body = comment.get("body") or ""
        if "P1" in body or "P2" in body:
            return ReviewStatus.CHANGES_REQUESTED

    if anchor_approved or body_approved:
        return ReviewStatus.APPROVED

    return ReviewStatus.PENDING


def merge_pr(repo: str, pr_number: int) -> None:
    """Merge a PR using ``gh pr merge --squash --delete-branch``."""
    run_gh(["pr", "merge", str(pr_number), "--squash", "--delete-branch"], repo=repo)


def post_comment(repo: str, pr_number: int, body: str) -> None:
    """Post a comment on a PR via ``gh pr comment``."""
    run_gh(["pr", "comment", str(pr_number), "--body", body], repo=repo)


def get_pr_author(repo: str, pr_number: int) -> str:
    """Return the GitHub login of the PR's author, or "" on failure.

    Read directly from PR metadata rather than the daemon's ``gh``
    identity: Claude CLI may run under a different authentication
    context than the daemon, so ``gh api user`` is not a reliable
    proxy for "who opened this PR" and using it would cause
    ``has_recent_codex_review_request`` to miss the trigger that the
    real author already posted.
    """
    try:
        raw = run_gh(
            [
                "api",
                f"repos/{repo}/pulls/{pr_number}",
                "--jq",
                ".user.login",
            ]
        )
    except RuntimeError:
        return ""
    if isinstance(raw, str):
        return raw.strip()
    return ""


def get_pr_head_commit_iso(repo: str, pr_number: int) -> str:
    """Return the ISO-8601 committer date of the PR's head commit, or "".

    Used by the dedup gate on ``_post_codex_review`` to tell
    "Claude already triggered a review for THIS commit" apart from
    "the daemon posted a trigger for an earlier commit". Without a
    commit-time threshold, the daemon's own post from a prior cycle
    would be seen as a duplicate on the next fix push when the PR
    author and daemon share a gh identity, suppressing the fresh
    review anchor that the new commit needs.
    """
    try:
        raw_sha = run_gh(
            [
                "api",
                f"repos/{repo}/pulls/{pr_number}",
                "--jq",
                ".head.sha",
            ]
        )
    except RuntimeError:
        return ""
    sha = raw_sha.strip() if isinstance(raw_sha, str) else ""
    if not sha:
        return ""
    try:
        raw_date = run_gh(
            [
                "api",
                f"repos/{repo}/commits/{sha}",
                "--jq",
                ".commit.committer.date",
            ]
        )
    except RuntimeError:
        return ""
    return raw_date.strip() if isinstance(raw_date, str) else ""


def get_pr_metadata(repo: str, pr_number: int) -> dict:
    """Fetch PR author, head SHA, and head commit date in 1-2 API calls.

    Replaces separate ``get_pr_author`` + ``get_pr_head_commit_iso`` calls.
    Returns ``{"author": str, "head_sha": str, "head_commit_date": str}``.
    """
    try:
        raw = run_gh([
            "api",
            f"repos/{repo}/pulls/{pr_number}",
            "--jq",
            "{author: .user.login, head_sha: .head.sha}",
        ])
    except RuntimeError:
        return {"author": "", "head_sha": "", "head_commit_date": ""}
    if isinstance(raw, dict):
        author = raw.get("author") or ""
        head_sha = raw.get("head_sha") or ""
    elif isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            author = parsed.get("author") or ""
            head_sha = parsed.get("head_sha") or ""
        except (json.JSONDecodeError, AttributeError):
            return {"author": "", "head_sha": "", "head_commit_date": ""}
    else:
        return {"author": "", "head_sha": "", "head_commit_date": ""}

    head_commit_date = ""
    if head_sha:
        try:
            raw_date = run_gh([
                "api",
                f"repos/{repo}/commits/{head_sha}",
                "--jq",
                ".commit.committer.date",
            ])
            head_commit_date = raw_date.strip() if isinstance(raw_date, str) else ""
        except RuntimeError:
            pass

    return {
        "author": author,
        "head_sha": head_sha,
        "head_commit_date": head_commit_date,
    }


class GitHubPollError(Exception):
    """Raised when a GitHub API poll fails (transient)."""


def get_branch_last_push_time(
    repo: str, pr_number: int
) -> float | None:
    """Return ``time.monotonic()`` if the PR's head SHA changed since last call.

    Compares the current head SHA from the GitHub API against the
    previously observed SHA for this ``(repo, pr_number)`` pair.
    Returns the current monotonic time when a new SHA is detected,
    or ``None`` when the SHA is unchanged (or on first call).

    Raises ``GitHubPollError`` when the API call fails so callers can
    distinguish "no push" from "could not check."
    """
    key = f"{repo}#{pr_number}"
    try:
        raw = run_gh([
            "api",
            f"repos/{repo}/pulls/{pr_number}",
            "--jq",
            ".head.sha",
        ])
    except (RuntimeError, subprocess.TimeoutExpired) as exc:
        raise GitHubPollError(str(exc)) from exc
    sha = raw.strip() if isinstance(raw, str) else ""
    if not sha:
        return None

    prev = _last_known_sha.get(key)
    _last_known_sha[key] = sha
    if prev is None:
        return None
    if sha != prev:
        return time.monotonic()
    return None


def clear_last_known_sha() -> None:
    """Reset SHA tracking state (used in tests)."""
    _last_known_sha.clear()


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
    try:
        comments = _gh_api_paginated(
            f"repos/{repo}/issues/{pr_number}/comments"
        ) or []
    except RuntimeError as exc:
        if "HTTP 404" in str(exc):
            return False
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
        if after_iso and (not created_raw or created_raw <= after_iso):
            continue
        created = _parse_iso(created_raw)
        if created is None:
            continue
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        if (now - created).total_seconds() < cutoff:
            return True
    return False


def _gh_api_paginated(path: str) -> list[dict] | None:
    """Fetch every page of a GitHub REST endpoint that returns a JSON array.

    Uses ``gh api --paginate --slurp``: ``--paginate`` walks all pages and
    ``--slurp`` wraps them into a single outer JSON array (one entry per page).
    Without ``--slurp`` ``gh`` writes one JSON document per page back-to-back,
    which is not parseable as a single document. The pages are then flattened
    into a single list of items. Returns ``None`` if the response is not a
    list (which would indicate an unexpected ``gh`` output).
    """
    raw = run_gh(["api", "--paginate", "--slurp", path])
    if not isinstance(raw, list):
        return None

    items: list[dict] = []
    for page in raw:
        if isinstance(page, list):
            items.extend(item for item in page if isinstance(item, dict))
        elif isinstance(page, dict):
            items.append(page)
    return items


def _find_codex_plus_one_reaction(reactions: list[dict]) -> dict | None:
    """Return the most recent +1 reaction from a Codex user, or None."""
    best: dict | None = None
    for r in reactions:
        if not isinstance(r, dict):
            continue
        login = ((r.get("user") or {}).get("login", "")).lower()
        if "codex" not in login:
            continue
        if r.get("content") != "+1":
            continue
        if best is None or (r.get("created_at") or "") > (best.get("created_at") or ""):
            best = r
    return best


def _get_commit_time(repo: str, sha: str) -> datetime | None:
    """Return the committer date of a commit, or None on failure."""
    try:
        raw = run_gh(
            ["api", f"repos/{repo}/commits/{sha}", "--jq", ".commit.committer.date"]
        )
    except RuntimeError:
        return None
    return _parse_iso(raw.strip() if isinstance(raw, str) else "")


def _get_latest_codex_review_info(
    repo: str, pr_number: int
) -> tuple[str, datetime | None]:
    """Return ``(commit_id, submitted_at)`` of the most recent Codex
    formal review, or ``("", None)`` if no such review exists.

    Used alongside the head-commit-time check so a force-push that moves
    HEAD to an older commit cannot silently reinstate a stale ``+1``:
    the latest review submission time is always at least as new as the
    moment the previous head was current, so requiring the reaction to
    beat that threshold too catches the force-push case.
    """
    try:
        reviews = _gh_api_paginated(f"repos/{repo}/pulls/{pr_number}/reviews")
    except RuntimeError as exc:
        if "HTTP 404" not in str(exc):
            raise
        return "", None
    if not reviews:
        return "", None
    best_sha = ""
    best_time: datetime | None = None
    best_raw = ""
    for review in reviews:
        user = (review.get("user") or {}).get("login", "") or ""
        if "codex" not in user.lower():
            continue
        submitted_raw = review.get("submitted_at") or ""
        if best_time is not None and submitted_raw <= best_raw:
            continue
        parsed = _parse_iso(submitted_raw)
        if parsed is None:
            continue
        best_sha = review.get("commit_id") or ""
        best_time = parsed
        best_raw = submitted_raw
    return best_sha, best_time


def _ci_status_from_rollup(rollup: object) -> CIStatus:
    """Map a ``statusCheckRollup`` payload to a single ``CIStatus`` value."""
    if not isinstance(rollup, list):
        return CIStatus.PENDING
    if not rollup:
        return CIStatus.SUCCESS

    states: list[str] = []
    for check in rollup:
        if not isinstance(check, dict):
            continue
        value = check.get("conclusion") or check.get("state") or check.get("status")
        if value:
            states.append(str(value).upper())

    if not states:
        return CIStatus.PENDING
    if any(s in {"FAILURE", "FAILED", "ERROR", "CANCELLED", "TIMED_OUT"} for s in states):
        return CIStatus.FAILURE
    if all(s in {"SUCCESS", "COMPLETED", "NEUTRAL", "SKIPPED"} for s in states):
        return CIStatus.SUCCESS
    return CIStatus.PENDING


def _parse_iso(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
