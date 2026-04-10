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
            "number,headRefName,statusCheckRollup,url,updatedAt,commits",
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
        prs.append(
            PRInfo(
                number=number,
                branch=entry.get("headRefName", ""),
                ci_status=_ci_status_from_rollup(entry.get("statusCheckRollup")),
                review_status=get_pr_review_status(repo, number),
                push_count=len(entry.get("commits") or []),
                url=entry.get("url", ""),
                last_activity=_parse_iso(entry.get("updatedAt")),
            )
        )
    return prs


def get_pr_review_status(repo: str, pr_number: int) -> ReviewStatus:
    """Derive a Codex review status from PR issue comments and their reactions."""
    comments = run_gh(
        ["api", f"repos/{repo}/issues/{pr_number}/comments"],
    )
    if not isinstance(comments, list):
        return ReviewStatus.PENDING

    for comment in reversed(comments):
        user = (comment.get("user") or {}).get("login", "") or ""
        if "codex" not in user.lower():
            continue

        comment_id = comment.get("id")
        if comment_id is not None:
            reactions = run_gh(
                ["api", f"repos/{repo}/issues/comments/{comment_id}/reactions"],
            )
            if isinstance(reactions, list):
                contents = {r.get("content") for r in reactions}
                if "+1" in contents:
                    return ReviewStatus.APPROVED
                if "eyes" in contents:
                    return ReviewStatus.EYES

        body = comment.get("body") or ""
        if "P1" in body or "P2" in body:
            return ReviewStatus.CHANGES_REQUESTED

        return ReviewStatus.PENDING

    return ReviewStatus.PENDING


def merge_pr(repo: str, pr_number: int) -> None:
    """Merge a PR using ``gh pr merge --merge --delete-branch``."""
    run_gh(["pr", "merge", str(pr_number), "--merge", "--delete-branch"], repo=repo)


def post_comment(repo: str, pr_number: int, body: str) -> None:
    """Post a comment on a PR via ``gh pr comment``."""
    run_gh(["pr", "comment", str(pr_number), "--body", body], repo=repo)


def _ci_status_from_rollup(rollup: object) -> CIStatus:
    """Map a ``statusCheckRollup`` payload to a single ``CIStatus`` value."""
    if not isinstance(rollup, list) or not rollup:
        return CIStatus.PENDING

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
