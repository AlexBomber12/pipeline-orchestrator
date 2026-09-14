"""Discover an attempt's PR across open and terminal states without adopting forks."""

from __future__ import annotations

from datetime import datetime
from urllib.parse import urlencode

from src.daemon import git_ops
from src.github import gh_runner
from src.models import PRInfo
from src.task_attempts import AttemptChanged, TaskAttempt

_DAEMON_CREATED_PR_PREFIX = (
    "Auto-created by pipeline-orchestrator after coder exit=0 with no PR."
)
_ATTEMPT_MARKER_LABEL = "Pipeline-Attempt-ID"


def daemon_created_pr_body(attempt: TaskAttempt) -> str:
    body = _DAEMON_CREATED_PR_PREFIX
    if attempt.task.task_file:
        body = f"{body} See `{attempt.task.task_file}` for the planned scope."
    return f"{body}\n\n{_ATTEMPT_MARKER_LABEL}: `{attempt.attempt_id}`"


def _has_durable_unnumbered_pr_ownership(data: dict, attempt: TaskAttempt) -> bool:
    body = data.get("body")
    if not isinstance(body, str):
        return False
    marker = f"{_ATTEMPT_MARKER_LABEL}: `{attempt.attempt_id}`"
    return marker in {line.strip() for line in body.splitlines()}


def attempt_branch_head(repo_path: str, branch: str, *, require_local: bool = False) -> str | None:
    remote = git_ops._git(repo_path, "ls-remote", "--heads", "origin", f"refs/heads/{branch}").stdout.strip()
    remote_sha = remote.split()[0] if remote else None
    local = git_ops._git(repo_path, "rev-parse", "--verify", f"refs/heads/{branch}", check=False)
    local_sha = local.stdout.strip() if local.returncode == 0 else None
    if require_local and not local_sha:
        raise AttemptChanged("Local attempt branch is missing; an updated PR HEAD cannot be attributed safely.")
    if remote_sha and local_sha and remote_sha != local_sha:
        raise AttemptChanged("Attempt branch refs disagree; PR ownership must be reconciled.")
    return remote_sha or local_sha


def _belongs_to_attempt(data: dict, owner_repo: str, base: str, attempt: TaskAttempt) -> bool:
    head, target = data.get("head", {}), data.get("base", {})
    if head.get("repo", {}).get("full_name", "").casefold() != owner_repo.casefold():
        return False  # A fork is never the daemon's implementation branch.
    if head.get("ref") != attempt.task.branch:
        return False
    if attempt.pr_number is not None and data.get("number") != attempt.pr_number:
        return False
    if attempt.pr_number is None:
        # GitHub branch names can be reused. Historical PRs from an earlier
        # accepted attempt may target a different base and must be discarded
        # before current-attempt safety checks validate the PR target.
        created = datetime.fromisoformat(data["created_at"])
        if created < attempt.accepted_at.replace(microsecond=0):
            return False
    if target.get("repo", {}).get("full_name", "").casefold() != owner_repo.casefold() or target.get("ref") != base:
        raise AttemptChanged("Attempt PR targets an unexpected repository or base.")
    if attempt.pr_number is not None:
        return True
    return True


def discover_attempt_pr(
    repo_path: str,
    owner_repo: str,
    base: str,
    attempt: TaskAttempt,
) -> dict | None:
    if gh_runner.get_repo_full_name(attempt.repo_url).casefold() != owner_repo.casefold():
        raise AttemptChanged("Attempt belongs to a different configured repository.")
    query = urlencode(
        {
            "state": "all",
            "head": f"{owner_repo.split('/')[0]}:{attempt.task.branch}",
            "per_page": 100,
        }
    )
    pages = gh_runner.run_gh(["api", "--paginate", "--slurp", f"repos/{owner_repo}/pulls?{query}"])
    if not isinstance(pages, list) or any(not isinstance(page, list) for page in pages):
        raise AttemptChanged("PR discovery returned an unverifiable response.")
    matches = [row for page in pages for row in page if _belongs_to_attempt(row, owner_repo, base, attempt)]
    if not matches:
        return None
    if len(matches) != 1:
        raise AttemptChanged("Multiple PRs match this attempt; ownership is ambiguous.")
    number = matches[0]["number"]
    data = gh_runner.run_gh(["api", f"repos/{owner_repo}/pulls/{number}"])
    if (
        data.get("number") != number
        or not _belongs_to_attempt(data, owner_repo, base, attempt)
        or data.get("state") not in {"open", "closed"}
        or "merged_at" not in data
    ):
        raise AttemptChanged("Discovered PR identity or state changed; reconciliation is deferred.")
    if (
        attempt.pr_number is None
        and not attempt.pr_discovery_pending
        and not _has_durable_unnumbered_pr_ownership(data, attempt)
    ):
        raise AttemptChanged("Unnumbered attempt PR lacks durable ownership evidence.")
    expected = attempt_branch_head(repo_path, attempt.task.branch)
    if expected and data.get("head", {}).get("sha") != expected:
        raise AttemptChanged("Discovered PR HEAD differs from the attempt-owned branch.")
    if not expected and data.get("state") != "closed":
        raise AttemptChanged("Discovered PR HEAD differs from the attempt-owned branch.")
    return data


def attempt_pr_info(data: dict, owner_repo: str, attempt: TaskAttempt) -> PRInfo:
    return PRInfo(
        number=data["number"],
        branch=attempt.task.branch,
        pr_id=attempt.task.pr_id,
        head_sha=data["head"]["sha"],
        url=f"https://github.com/{owner_repo}/pull/{data['number']}",
        is_cross_repository=False,
    )
