"""Wrapper around the `gh` CLI for reading PR state and performing PR actions."""

from __future__ import annotations

import json
import re
import subprocess
from datetime import datetime

from src.models import CIStatus, PRInfo, ReviewStatus

_REPO_URL_RE = re.compile(
    r"github\.com[:/]+(?P<owner>[^/]+)/(?P<repo>[^/]+?)(?:\.git)?/?$"
)


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
    raw = run_gh(
        [
            "pr",
            "list",
            "--state",
            "open",
            "--json",
            "number,headRefName,statusCheckRollup,url,updatedAt,commits,author,isCrossRepository",
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
        last_push_at = None
        if commits:
            last_commit = commits[-1]
            committed = last_commit.get("committedDate") or last_commit.get("authoredDate")
            if committed:
                last_push_at = _parse_iso(committed)
        prs.append(
            PRInfo(
                number=number,
                branch=entry.get("headRefName", ""),
                ci_status=_ci_status_from_rollup(entry.get("statusCheckRollup")),
                review_status=get_pr_review_status(
                    repo,
                    number,
                    pr_author=(entry.get("author") or {}).get("login", ""),
                    last_push_at=last_push_at,
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
    last_push_at: datetime | None = None,
) -> ReviewStatus:
    """Derive a Codex review status from PR issue comments, review comments, and reactions.

    Logic:
    1. Find the latest ``@codex review`` trigger comment by the PR author.
    2. Check reactions on that comment from Codex: +1 → APPROVED, eyes → EYES.
    3. If neither reaction, scan Codex comments (issue + review) posted after
       the anchor for P1/P2 → CHANGES_REQUESTED.
    4. Otherwise → PENDING.
    """
    # Check for Codex reactions on the PR body (issue-level reactions).
    # Codex bot (chatgpt-codex-connector) puts +1 on the PR body itself,
    # not on comments, so check this first.
    try:
        issue_reactions = _gh_api_paginated(
            f"repos/{repo}/issues/{pr_number}/reactions"
        )
        if issue_reactions:
            codex_reactions = [
                r
                for r in issue_reactions
                if isinstance(r, dict)
                and "codex" in ((r.get("user") or {}).get("login", "")).lower()
            ]
            codex_contents = {r.get("content") for r in codex_reactions}
            if "+1" in codex_contents:
                if last_push_at is not None:
                    plus_one = next(
                        (r for r in codex_reactions if r.get("content") == "+1"),
                        None,
                    )
                    if plus_one:
                        reaction_time = _parse_iso(plus_one.get("created_at"))
                        if reaction_time and reaction_time < last_push_at:
                            pass  # stale, fall through
                        else:
                            return ReviewStatus.APPROVED
                    else:
                        return ReviewStatus.APPROVED
                else:
                    return ReviewStatus.APPROVED
            if "eyes" in codex_contents:
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

    # Step 1: latest "@codex review" trigger comment by the PR author.
    anchor = None
    for c in reversed(issue_comments):
        author = (c.get("user") or {}).get("login", "")
        if pr_author and author != pr_author:
            continue
        if "@codex review" in (c.get("body") or "").lower():
            anchor = c
            break

    # Step 2: check Codex reactions on the anchor comment.
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
                        return ReviewStatus.APPROVED
                    if "eyes" in codex_contents:
                        return ReviewStatus.EYES
            except RuntimeError as exc:
                if "HTTP 404" not in str(exc):
                    raise

    # Step 3: P1/P2 in Codex comments after the anchor → CHANGES_REQUESTED.
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

    return ReviewStatus.PENDING


def merge_pr(repo: str, pr_number: int) -> None:
    """Merge a PR using ``gh pr merge --merge --delete-branch``."""
    run_gh(["pr", "merge", str(pr_number), "--merge", "--delete-branch"], repo=repo)


def post_comment(repo: str, pr_number: int, body: str) -> None:
    """Post a comment on a PR via ``gh pr comment``."""
    run_gh(["pr", "comment", str(pr_number), "--body", body], repo=repo)


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
