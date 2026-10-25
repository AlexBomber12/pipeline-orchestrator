"""Bound, durable observations of GitHub Actions workflow attempts."""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

from src.cancellation.storage import task_spec_content_hash
from src.github import gh_runner
from src.models import PRInfo, QueueTask, RepoState

_JOB_URL = re.compile(r"https://github\.com/([^/]+/[^/]+)/actions/runs/(\d+)/job/(\d+)")
FAILED_OUTCOMES = {
    "failure", "timed_out", "action_required", "cancelled", "stale", "startup_failure",
}


def attempt_scope(
    state: RepoState, repo: str, repo_path: str, pr: PRInfo,
) -> dict[str, int] | None:
    """Return the active task/PR/HEAD observations, resetting only that scope."""
    task: QueueTask | None = state.current_task
    generation = ""
    if task is not None:
        try:
            generation = task_spec_content_hash(
                (Path(repo_path) / task.task_file).read_text(encoding="utf-8")
            )
        except (OSError, TypeError, ValueError):
            return None
    scope = json.dumps([
        repo, task.pr_id if task else None, generation,
        task.branch if task else None, pr.number, pr.head_sha,
    ], separators=(",", ":"))
    if state.ci_attempt_scope != scope:
        state.ci_attempt_scope = scope
        state.ci_attempts = {}
    return state.ci_attempts


def actions_job_identity(repo: str, check: dict) -> tuple[int, int] | None:
    """Accept only a GitHub Actions URL for this repository and exact job ID."""
    match = _JOB_URL.fullmatch(str(check.get("details_url") or ""))
    if match is None or match.group(1) != repo or check.get("id") != int(match.group(3)):
        return None
    return int(match.group(2)), int(match.group(3))


def read_actions_run(
    repo: str, sha: str, run_id: int, observations: dict[str, int] | None = None,
) -> dict | None:
    """Read current run metadata and retain the highest verified attempt."""
    try:
        run = gh_runner.run_gh(
            ["run", "view", str(run_id), "--json", "databaseId,headSha,attempt,status,conclusion,jobs"],
            repo=repo,
        )
    except (RuntimeError, subprocess.TimeoutExpired, OSError):
        return None
    if not isinstance(run, dict) or run.get("databaseId") != run_id or run.get("headSha") != sha:
        return None
    attempt = run.get("attempt")
    if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt < 1:
        return None
    if observations is not None:
        key = str(run_id)
        observations[key] = max(attempt, observations.get(key, 0))
        if observations[key] != attempt:
            return None
    return run


def matching_failed_job(run: dict, job_id: int, name: str) -> bool:
    """Require an exact failed job in the current completed attempt."""
    jobs = run.get("jobs")
    return bool(
        run.get("status") == "completed"
        and run.get("conclusion") in FAILED_OUTCOMES
        and isinstance(jobs, list)
        and any(
            isinstance(job, dict) and job.get("databaseId") == job_id
            and job.get("name") == name and job.get("conclusion") in FAILED_OUTCOMES
            for job in jobs
        )
    )


def canonical_ci_incident(ci: dict) -> dict:
    """Freeze invocation provenance; discard only legacy mutable observations."""
    stable = {key: value for key, value in ci.items() if key != "_observed_attempts"}
    return json.loads(json.dumps(stable, sort_keys=True))
