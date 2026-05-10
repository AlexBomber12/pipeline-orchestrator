"""Best-effort audit for commits that landed on main without verified CI.

GitHub squash and rebase merges can produce single-parent linear commits on
main. The audit queries GitHub's associated-pulls endpoint before treating a
single-parent commit as a direct commit, then verifies CI against the PR head
SHA when a merged PR is available.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass
from typing import Any

from src.github import cache, checks
from src.github.gh_runner import run_gh
from src.models import CIStatus

logger = logging.getLogger(__name__)

_AUDIT_TTL_SECONDS = 30 * 24 * 60 * 60
_FINDINGS_MAX_LENGTH = 100
_MERGE_PR_PATTERNS = (
    re.compile(r"^Merge pull request #(\d+)"),
    re.compile(r"\(#(\d+)\)$"),
)


@dataclass(frozen=True)
class MainCommitAuditFinding:
    sha: str
    short_sha: str
    message_first_line: str
    parent_count: int
    pr_number: int | None
    violation_category: str
    rule: str


def _audited_key(repo: str) -> str:
    return f"audit:main_commits:{repo}:audited"


def _findings_key(repo: str) -> str:
    return f"audit:main_commits:{repo}:findings"


def _message_first_line(message: object) -> str:
    line = str(message or "").splitlines()[0] if message else ""
    return line[:120]


def _extract_pr_number(message: str) -> int | None:
    first_line = _message_first_line(message)
    for pattern in _MERGE_PR_PATTERNS:
        match = pattern.search(first_line)
        if match:
            return int(match.group(1))
    return None


def _finding(
    *,
    sha: str,
    message: object,
    parent_count: int,
    pr_number: int | None,
    violation_category: str,
    rule: str,
) -> MainCommitAuditFinding:
    return MainCommitAuditFinding(
        sha=sha,
        short_sha=sha[:7],
        message_first_line=_message_first_line(message),
        parent_count=parent_count,
        pr_number=pr_number,
        violation_category=violation_category,
        rule=rule,
    )


def _as_dict(payload: object) -> dict[str, Any]:
    return payload if isinstance(payload, dict) else {}


def _commit_shas(payload: object) -> list[str]:
    if not isinstance(payload, list):
        return []
    shas: list[str] = []
    for item in payload:
        if isinstance(item, dict) and isinstance(item.get("sha"), str):
            shas.append(item["sha"])
    return shas


def list_recent_main_commit_shas(owner_repo: str, lookback_n: int = 10) -> list[str]:
    """Return the newest ``lookback_n`` main commit SHAs via GitHub CLI."""
    if lookback_n <= 0:
        return []
    payload = run_gh(
        [
            "api",
            f"repos/{owner_repo}/commits?sha=main&per_page={lookback_n}",
        ]
    )
    return _commit_shas(payload)


def _commit_message(payload: dict[str, Any]) -> str:
    commit = _as_dict(payload.get("commit"))
    message = commit.get("message")
    return message if isinstance(message, str) else ""


def _parents(payload: dict[str, Any]) -> list[dict[str, Any]] | None:
    parents = payload.get("parents")
    if not isinstance(parents, list):
        return None
    return [parent for parent in parents if isinstance(parent, dict)]


def _merged_associated_pr(payload: object) -> dict[str, Any] | None:
    if not isinstance(payload, list):
        return None
    for pr in payload:
        if not isinstance(pr, dict):
            continue
        if pr.get("merged_at"):
            return pr
    return None


def _pr_number(payload: dict[str, Any]) -> int | None:
    number = payload.get("number")
    return number if isinstance(number, int) else None


def _pr_head_sha(payload: dict[str, Any]) -> str | None:
    head = payload.get("head")
    if not isinstance(head, dict):
        return None
    sha = head.get("sha")
    return sha if isinstance(sha, str) and sha else None


def _check_runs(payload: object) -> list[dict]:
    check_runs = payload.get("check_runs") if isinstance(payload, dict) else payload
    if not isinstance(check_runs, list):
        return []
    return [run for run in check_runs if isinstance(run, dict)]


def _check_run_pages(owner_repo: str, sha: str) -> list[dict] | None:
    return cache._gh_api_paginated(
        f"repos/{owner_repo}/commits/{sha}/check-runs?per_page=100"
    )


def _has_successful_ci(owner_repo: str, sha: str) -> bool:
    check_runs = []
    check_pages = _check_run_pages(owner_repo, sha)
    if isinstance(check_pages, list):
        for page in check_pages:
            check_runs.extend(_check_runs(page))
    status_payload = run_gh(
        [
            "api",
            f"repos/{owner_repo}/commits/{sha}/status",
        ]
    )
    return (
        checks._map_rest_ci_status_to_enum(
            check_runs,
            status_payload if isinstance(status_payload, dict) else {},
        )
        == CIStatus.SUCCESS
    )


def audit_main_commits(
    owner_repo: str,
    lookback_n: int = 10,
    audited_shas: set[str] | None = None,
) -> list[MainCommitAuditFinding]:
    """Audit recent main commits for CI bypass violations.

    ``audited_shas`` are skipped so stable history is not rechecked on every
    daemon IDLE pass. GitHub API errors are logged and swallowed because the
    daemon audit is advisory and must not crash the runner.
    """
    if lookback_n <= 0:
        return []

    audited = audited_shas or set()
    try:
        shas = list_recent_main_commit_shas(owner_repo, lookback_n)
    except Exception:
        logger.exception("Failed to list recent main commits for %s", owner_repo)
        return []

    findings, _checked_shas = audit_main_commit_shas(owner_repo, shas, audited)
    return findings


def audit_main_commit_shas(
    owner_repo: str,
    shas: list[str],
    audited_shas: set[str] | None = None,
) -> tuple[list[MainCommitAuditFinding], list[str]]:
    """Audit explicit main commit SHAs and return findings plus checked SHAs."""
    audited = audited_shas or set()
    findings: list[MainCommitAuditFinding] = []
    checked_shas: list[str] = []
    for sha in shas:
        if sha in audited:
            continue
        try:
            commit_payload = _as_dict(
                run_gh(["api", f"repos/{owner_repo}/commits/{sha}"])
            )
            parents = _parents(commit_payload)
            message = _commit_message(commit_payload)
            if parents is None:
                findings.append(
                    _finding(
                        sha=sha,
                        message=message,
                        parent_count=0,
                        pr_number=None,
                        violation_category="merge_commit_pr_unverified",
                        rule="Commit parent metadata is unavailable; verify main history manually.",
                    )
                )
                checked_shas.append(sha)
                continue

            parent_count = len(parents)
            if parent_count == 0:
                findings.append(
                    _finding(
                        sha=sha,
                        message=message,
                        parent_count=parent_count,
                        pr_number=None,
                        violation_category="direct_commit_no_pr",
                        rule=(
                            "Root commit is not associated with a merged PR; "
                            "investigate and revert if unauthorized."
                        ),
                    )
                )
                checked_shas.append(sha)
                continue

            if parent_count == 1:
                associated_pr = _merged_associated_pr(
                    run_gh(["api", f"repos/{owner_repo}/commits/{sha}/pulls"])
                )
                if associated_pr is None:
                    findings.append(
                        _finding(
                            sha=sha,
                            message=message,
                            parent_count=parent_count,
                            pr_number=None,
                            violation_category="direct_commit_no_pr",
                            rule=(
                                "Commit landed on main without an associated "
                                "merged PR; investigate and revert if unauthorized."
                            ),
                        )
                    )
                    checked_shas.append(sha)
                    continue

                pr_number = _pr_number(associated_pr)
                ci_sha = _pr_head_sha(associated_pr) or sha
                if not _has_successful_ci(owner_repo, ci_sha):
                    findings.append(
                        _finding(
                            sha=sha,
                            message=message,
                            parent_count=parent_count,
                            pr_number=pr_number,
                            violation_category="linear_pr_failed_ci",
                            rule=(
                                "Linear-history PR commit has no successful "
                                "PR-head check run; investigate branch-protection bypass."
                            ),
                        )
                    )
                checked_shas.append(sha)
                continue
            if parent_count > 2:
                findings.append(
                    _finding(
                        sha=sha,
                        message=message,
                        parent_count=parent_count,
                        pr_number=None,
                        violation_category="merge_commit_pr_unverified",
                        rule="Octopus merge on main cannot be traced to one verified PR; investigate manually.",
                    )
                )
                checked_shas.append(sha)
                continue

            pr_number = _extract_pr_number(message)
            if pr_number is None:
                findings.append(
                    _finding(
                        sha=sha,
                        message=message,
                        parent_count=parent_count,
                        pr_number=None,
                        violation_category="merge_commit_pr_unverified",
                        rule="Merge commit on main does not identify a PR; verify operator action and CI manually.",
                    )
                )
                checked_shas.append(sha)
                continue

            pr_head_sha = parents[1].get("sha")
            if not isinstance(pr_head_sha, str) or not pr_head_sha:
                findings.append(
                    _finding(
                        sha=sha,
                        message=message,
                        parent_count=parent_count,
                        pr_number=pr_number,
                        violation_category="merge_commit_pr_unverified",
                        rule="Merge commit PR head SHA is unavailable; verify CI manually.",
                    )
                )
                checked_shas.append(sha)
                continue

            if not _has_successful_ci(owner_repo, pr_head_sha):
                findings.append(
                    _finding(
                        sha=sha,
                        message=message,
                        parent_count=parent_count,
                        pr_number=pr_number,
                        violation_category="merge_commit_pr_failed_ci",
                        rule="Merged PR head has no successful check run; investigate branch-protection bypass.",
                    )
                )
            checked_shas.append(sha)
        except Exception:
            logger.exception("Failed to audit main commit %s for %s", sha, owner_repo)
            continue

    return findings, checked_shas


async def load_audited_shas_from_redis(redis: Any, repo: str) -> set[str]:
    try:
        values = await redis.smembers(_audited_key(repo))
    except Exception:
        logger.exception("Failed to load main commit audit cache for %s", repo)
        return set()
    return {
        value.decode("utf-8") if isinstance(value, bytes) else str(value)
        for value in values
    }


async def record_audit_findings_in_redis(
    redis: Any,
    repo: str,
    findings: list[MainCommitAuditFinding],
) -> None:
    if not findings:
        return
    key = _findings_key(repo)
    try:
        for finding in findings:
            await redis.lpush(key, json.dumps(asdict(finding), sort_keys=True))
        await redis.ltrim(key, 0, _FINDINGS_MAX_LENGTH - 1)
        await redis.expire(key, _AUDIT_TTL_SECONDS)
    except Exception:
        logger.exception("Failed to record main commit audit findings for %s", repo)


async def mark_shas_audited_in_redis(
    redis: Any,
    repo: str,
    shas: list[str],
) -> None:
    if not shas:
        return
    key = _audited_key(repo)
    try:
        for sha in shas:
            await redis.sadd(key, sha)
        await redis.expire(key, _AUDIT_TTL_SECONDS)
    except Exception:
        logger.exception("Failed to update main commit audit cache for %s", repo)
