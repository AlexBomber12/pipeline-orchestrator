"""Pull-request list, metadata, and action helpers.

Owns the ``gh pr`` reads (open, merged, single PR), per-PR metadata
extraction (author, head sha, head commit date), branch push tracking,
and the ``merge_pr`` action. Cache and paginated GET primitives live in
``src.github.cache``; CI-status helpers live in ``src.github.checks``;
review status lives in ``src.github.reviews``.
"""

from __future__ import annotations

import json
import logging
import re
import subprocess
import time
from collections.abc import Iterable
from datetime import datetime, timezone
from urllib.parse import quote

from src.github import cache, checks, gh_runner, reviews
from src.github.gh_runner import (
    _extract_commit_date,
    _extract_head_sha,
    _parse_iso,
)
from src.github.reviews import _begin_review_cache_cycle
from src.models import CIStatus, PRInfo

logger = logging.getLogger(__name__)

_QUEUE_PR_ID_RE = re.compile(r"^(PR-[A-Za-z0-9_.-]+):(?:\s|$)")
_GH_HEAD_QUERY_CHUNK = 20
_BRANCH_NAME_RE = re.compile(r"^[^\x00-\x20\x7f~^:?*\\[]+$")

_last_known_sha: dict[str, str] = {}
_merged_prs_cache: dict[tuple[str, str], tuple[float, list["PRInfo"]]] = {}
_MERGED_PRS_CACHE_TTL_SECONDS = 60.0


class GitHubPollError(Exception):
    """Raised when a GitHub API poll fails (transient)."""


class GhPrMergedBranchesUnavailable(Exception):
    """Raised when GitHub cannot confirm merged PR branches."""


def extract_queue_pr_id(subject: str) -> str | None:
    """Return the canonical queue PR id from a title/subject prefix."""
    match = _QUEUE_PR_ID_RE.match(subject.strip())
    if match is None:
        return None
    return match.group(1)


def clear_merged_prs_cache() -> None:
    """Clear merged PR lookup cache (used in tests)."""
    _merged_prs_cache.clear()


def clear_last_known_sha() -> None:
    """Reset SHA tracking state (used in tests)."""
    _last_known_sha.clear()


def gh_pr_get_merged_branches(repo: str, branches: Iterable[str]) -> set[str]:
    """Return the input branch names that GitHub reports as merged PR heads."""
    branch_names = list(branches)
    for branch in branch_names:
        if not _is_valid_branch_name(branch):
            raise ValueError(f"Invalid branch name: {branch!r}")
    if not branch_names:
        return set()

    repo_parts = repo.split("/")
    owner = repo_parts[-2] if len(repo_parts) >= 2 else ""
    repo_name = repo_parts[-1]
    merged_branches: set[str] = set()
    for offset in range(0, len(branch_names), _GH_HEAD_QUERY_CHUNK):
        chunk = branch_names[offset : offset + _GH_HEAD_QUERY_CHUNK]
        branch_variables = ", ".join(
            f"$branch{index}: String!" for index in range(len(chunk))
        )
        branch_queries = " ".join(
            (
                f"b{index}: pullRequests("
                f"first: 1, states: MERGED, headRefName: $branch{index}"
                ") { nodes { headRefName mergedAt } }"
            )
            for index in range(len(chunk))
        )
        query = (
            f"query($owner: String!, $repo: String!, {branch_variables}) "
            f"{{ repository(owner: $owner, name: $repo) {{ {branch_queries} }} }}"
        )
        args = [
            "api",
            "graphql",
            "-f",
            f"query={query}",
            "-f",
            f"owner={owner}",
            "-f",
            f"repo={repo_name}",
        ]
        for index, branch in enumerate(chunk):
            args.extend(["-f", f"branch{index}={branch}"])
        try:
            raw = gh_runner.run_gh(args)
        except RuntimeError as exc:
            raise GhPrMergedBranchesUnavailable(
                f"gh pr merged branch lookup failed: {exc}"
            ) from exc
        repository = raw.get("data", {}).get("repository", {})
        for index, branch in enumerate(chunk):
            nodes = repository.get(f"b{index}", {}).get("nodes") or []
            if any(
                entry.get("headRefName") == branch and entry.get("mergedAt")
                for entry in nodes
            ):
                merged_branches.add(branch)
    return merged_branches


def _is_valid_branch_name(branch: str) -> bool:
    if _BRANCH_NAME_RE.fullmatch(branch) is None:
        return False
    if branch.startswith(("-", "/")) or branch.endswith(("/", ".")):
        return False
    if ".." in branch or "//" in branch or "@{" in branch:
        return False
    return all(
        part not in {"", ".", ".."}
        and not part.startswith(".")
        and not part.endswith(".lock")
        for part in branch.split("/")
    )


def get_open_prs(
    repo: str,
    allow_merge_without_checks: bool = False,
) -> list[PRInfo]:
    """Return open PRs for ``repo`` (``owner/repo``) with CI and review status."""

    _begin_review_cache_cycle()
    try:
        raw = gh_runner.run_gh(
            [
                "pr",
                "list",
                "--state",
                "open",
                "--json",
                "number,title,headRefName,headRefOid,url,updatedAt,commits,author,isCrossRepository,labels",
            ],
            repo=repo,
        )
    except RuntimeError as exc:
        if "GraphQL: API rate limit" not in str(exc):
            raise
        logger.warning(
            "gh pr list GraphQL rate-limited for %s; falling back to REST", repo
        )
        return _get_open_prs_rest(
            repo,
            allow_merge_without_checks=allow_merge_without_checks,
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
        title = entry.get("title", "")
        check_runs, status_payload, fetch_ok = checks._fetch_ci_status_rest(
            repo, head_sha
        )
        prs.append(
            PRInfo(
                number=number,
                branch=entry.get("headRefName", ""),
                title=title,
                pr_id=extract_queue_pr_id(title),
                ci_status=checks._map_rest_ci_status_to_enum(
                    check_runs,
                    status_payload,
                    empty_is_success=allow_merge_without_checks,
                    fetch_ok=fetch_ok,
                ),
                review_status=reviews.get_pr_review_status(
                    repo,
                    number,
                    pr_author=(entry.get("author") or {}).get("login", ""),
                    head_sha=head_sha,
                ),
                commits_count=len(commits),
                push_count=1 if head_sha else 0,
                observed_head_shas={head_sha} if head_sha else set(),
                head_sha=head_sha,
                url=entry.get("url", ""),
                last_activity=_parse_iso(entry.get("updatedAt")),
                is_escalated=any(
                    isinstance(label, dict)
                    and (label.get("name") or "").lower() == "escalated"
                    for label in (entry.get("labels") or [])
                ),
                is_cross_repository=bool(entry.get("isCrossRepository", False)),
            )
        )
    return prs


def _get_open_prs_rest(
    repo: str,
    *,
    allow_merge_without_checks: bool,
) -> list[PRInfo]:
    """Return open PRs via REST when GraphQL status rollup is unavailable."""

    raw = cache._gh_api_paginated(f"repos/{repo}/pulls?state=open&per_page=100")
    if raw is None:
        return []

    prs: list[PRInfo] = []
    for entry in raw:
        number = int(entry.get("number", 0))
        if not number:
            continue
        head = entry.get("head") or {}
        user = entry.get("user") or {}
        title = entry.get("title", "")
        head_sha = head.get("sha", "")
        labels = entry.get("labels") or []
        prs.append(
            PRInfo(
                number=number,
                branch=head.get("ref", ""),
                title=title,
                pr_id=extract_queue_pr_id(title),
                ci_status=(
                    CIStatus.SUCCESS
                    if allow_merge_without_checks
                    else CIStatus.PENDING
                ),
                review_status=reviews.get_pr_review_status(
                    repo,
                    number,
                    pr_author=user.get("login", ""),
                    head_sha=head_sha,
                ),
                commits_count=1 if head_sha else 0,
                push_count=1 if head_sha else 0,
                observed_head_shas={head_sha} if head_sha else set(),
                head_sha=head_sha,
                url=entry.get("html_url", ""),
                last_activity=_parse_iso(entry.get("updated_at")),
                is_escalated=any(
                    isinstance(label, dict)
                    and (label.get("name") or "").lower() == "escalated"
                    for label in labels
                ),
                is_cross_repository=bool(head.get("repo", {}).get("fork", False)),
            )
        )
    return prs


def get_merged_prs(
    repo: str,
    base_branch: str | None = None,
    *,
    refresh: bool = False,
) -> list[PRInfo]:
    """Return merged PRs for ``repo``.

    This is a best-effort fallback used by queue status derivation when
    merged work can no longer be inferred from local git history alone
    (for example after squash-merging with a custom title). If GitHub
    cannot be queried, return an empty list and let callers fall back to
    their local heuristics.
    """

    cache_key = (repo, base_branch or "")
    cached = _merged_prs_cache.get(cache_key)
    now = time.monotonic()
    if (
        not refresh
        and cached is not None
        and (now - cached[0]) < _MERGED_PRS_CACHE_TTL_SECONDS
    ):
        return list(cached[1])

    path = f"repos/{repo}/pulls?state=closed&per_page=100"
    if base_branch:
        path = (
            f"repos/{repo}/pulls?state=closed"
            f"&base={quote(base_branch, safe='')}&per_page=100"
        )

    raw = cache._gh_api_paginated(path)
    if raw is None:
        raise RuntimeError(
            f"gh api repos/{repo}/pulls returned unexpected payload"
        )

    prs: list[PRInfo] = []
    for entry in raw:
        if entry.get("merged_at") in (None, ""):
            continue
        base = entry.get("base") or {}
        if base_branch and base.get("ref") != base_branch:
            continue
        number = int(entry.get("number", 0))
        if not number:
            continue
        title = entry.get("title", "")
        head = entry.get("head") or {}
        head_repo = head.get("repo")
        if not isinstance(head_repo, dict):
            head_repo = {}
        prs.append(
            PRInfo(
                number=number,
                branch=head.get("ref", ""),
                title=title,
                pr_id=extract_queue_pr_id(title),
                url="",
                is_cross_repository=bool(head_repo.get("fork", False)),
                last_activity=_parse_iso(entry.get("merged_at")),
            )
        )
    _merged_prs_cache[cache_key] = (now, prs)
    return list(prs)


def pr_state(repo: str, pr_number: int) -> dict[str, str | None] | None:
    """Return the PR's terminal state markers, or ``None`` on lookup failure.

    Returned dict shape: ``{"state": str, "mergedAt": str|None, "closedAt": str|None}``
    where ``state`` is the GitHub-normalized ``"OPEN"``, ``"CLOSED"``, or
    ``"MERGED"``. Used by the FIX-cycle polling task to detect external
    merge or close events while a coder process is running.
    """

    try:
        raw = gh_runner.run_gh(
            [
                "pr",
                "view",
                str(pr_number),
                "--json",
                "state,mergedAt,closedAt",
            ],
            repo=repo,
        )
    except (RuntimeError, subprocess.TimeoutExpired, OSError):
        return None
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return None
    if not isinstance(raw, dict):
        return None
    state = raw.get("state")
    if not isinstance(state, str):
        return None
    merged_at = raw.get("mergedAt") if isinstance(raw.get("mergedAt"), str) else None
    closed_at = raw.get("closedAt") if isinstance(raw.get("closedAt"), str) else None
    return {
        "state": state.upper(),
        "mergedAt": merged_at,
        "closedAt": closed_at,
    }


def is_pr_merged(repo: str, pr_number: int) -> bool | None:
    """Return True if PR is merged, False if closed without merge, None on lookup failure."""

    try:
        payload = cache._etag_get(f"repos/{repo}/pulls/{pr_number}")
    except (RuntimeError, subprocess.TimeoutExpired, OSError):
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("merged") is True:
        return True
    if payload.get("state") == "closed":
        return False
    return None


def get_pr_diff(owner_repo: str, pr_number: int) -> str:
    """Return unified diff text for a PR via gh CLI."""
    result = gh_runner.run_gh(
        ["pr", "diff", str(pr_number), "--repo", owner_repo],
    )
    if isinstance(result, str):
        return result
    return ""


def merge_pr(repo: str, pr_number: int) -> None:
    """Merge a PR using ``gh pr merge --squash --delete-branch``.

    Invalidates the cached ``repos/{repo}/pulls`` ETag entries on success
    so the next REST list fetch sees the merged PR drop out of
    ``state=open`` instead of returning a stale 304-cached page that
    still contains it.
    """

    gh_runner.run_gh(
        ["pr", "merge", str(pr_number), "--squash", "--delete-branch"], repo=repo
    )
    cache._invalidate_etag_cache(f"repos/{repo}/pulls")


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
        payload = cache._etag_get(f"repos/{repo}/pulls/{pr_number}")
    except (RuntimeError, subprocess.TimeoutExpired, OSError):
        return ""
    if isinstance(payload, dict):
        user = payload.get("user")
        if isinstance(user, dict):
            login = user.get("login")
            if isinstance(login, str):
                return login.strip()
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
        pr_payload = cache._etag_get(f"repos/{repo}/pulls/{pr_number}")
    except RuntimeError:
        return ""
    sha = _extract_head_sha(pr_payload)
    if not sha:
        return ""
    try:
        commit_payload = cache._etag_get(f"repos/{repo}/commits/{sha}")
    except RuntimeError:
        return ""
    return _extract_commit_date(commit_payload)


def get_pr_metadata(repo: str, pr_number: int) -> dict:
    """Fetch PR author, head SHA, and head commit date in 1-2 API calls.

    Replaces separate ``get_pr_author`` + ``get_pr_head_commit_iso`` calls.
    Returns ``{"author": str, "head_sha": str, "head_commit_date": str}``.
    """

    empty = {"author": "", "head_sha": "", "head_commit_date": ""}
    try:
        pr_payload = cache._etag_get(f"repos/{repo}/pulls/{pr_number}")
    except RuntimeError:
        return dict(empty)
    author, head_sha = _extract_author_and_head_sha(pr_payload)
    if author == "" and head_sha == "" and not isinstance(pr_payload, (dict, str)):
        return dict(empty)

    head_commit_date = ""
    if head_sha:
        try:
            commit_payload = cache._etag_get(f"repos/{repo}/commits/{head_sha}")
            head_commit_date = _extract_commit_date(commit_payload)
        except RuntimeError:
            pass

    return {
        "author": author,
        "head_sha": head_sha,
        "head_commit_date": head_commit_date,
    }


def _extract_author_and_head_sha(payload: object) -> tuple[str, str]:
    """Pull author + head sha out of a PR payload, accepting dict or jq string."""
    if isinstance(payload, dict):
        user = payload.get("user")
        if isinstance(user, dict):
            login = user.get("login")
            author = login if isinstance(login, str) else ""
        else:
            top_author = payload.get("author")
            author = top_author if isinstance(top_author, str) else ""
        head = payload.get("head")
        if isinstance(head, dict):
            sha = head.get("sha")
            head_sha = sha if isinstance(sha, str) else ""
        else:
            top_sha = payload.get("head_sha")
            head_sha = top_sha if isinstance(top_sha, str) else ""
        return author, head_sha
    if isinstance(payload, str):
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            return "", ""
        if isinstance(parsed, dict):
            return _extract_author_and_head_sha(parsed)
    return "", ""


def get_branch_last_push_time(repo: str, pr_number: int) -> float | None:
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
        payload = cache._etag_get(f"repos/{repo}/pulls/{pr_number}")
    except (RuntimeError, subprocess.TimeoutExpired, OSError) as exc:
        raise GitHubPollError(str(exc)) from exc
    sha = _extract_head_sha(payload)
    if not sha:
        return None

    prev = _last_known_sha.get(key)
    _last_known_sha[key] = sha
    if prev is None:
        return None
    if sha != prev:
        return time.monotonic()
    return None


def get_pr_last_push_time(repo: str, pr_number: int) -> datetime | None:
    """Return the timestamp of the most recent push to the PR's head ref.

    Uses the GitHub repository activity API, which records the actual
    push event time. This is distinct from the head commit's committer
    date: a cherry-picked, amended, or rebased commit can carry a
    committer date that predates the push that put it on the branch.
    Anywhere we want "did X happen after this branch's latest push?",
    push time is the correct anchor.

    Returns ``None`` on any API or parse failure (callers must fail
    open).
    """

    try:
        branch_raw = gh_runner.run_gh([
            "api",
            f"repos/{repo}/pulls/{pr_number}",
            "--jq",
            ".head.ref",
        ])
        branch = branch_raw.strip() if isinstance(branch_raw, str) else ""
        if not branch:
            return None
        date_raw = gh_runner.run_gh([
            "api",
            f"repos/{repo}/activity",
            "-f", f"ref=refs/heads/{branch}",
            "-f", "activity_type=push",
            "-f", "per_page=1",
            "-f", "direction=desc",
            "--jq",
            ".[0].pushed_at",
        ])
        date_str = date_raw.strip() if isinstance(date_raw, str) else ""
        if not date_str:
            return None
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except Exception:
        return None


def get_last_push_age_seconds(repo: str, pr_number: int) -> float | None:
    """Return seconds since the last push to the PR branch.

    Returns ``None`` on any API or parse failure.
    """

    push_dt = get_pr_last_push_time(repo, pr_number)
    if push_dt is None:
        return None
    return max(0.0, (datetime.now(timezone.utc) - push_dt).total_seconds())
