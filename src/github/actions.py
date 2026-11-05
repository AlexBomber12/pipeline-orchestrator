"""Exact GitHub Actions run/job provenance shared by CI readers and recovery."""
from __future__ import annotations

import re
import subprocess

from src.github import gh_runner

_JOB_URL = re.compile(r"https://github\.com/([^/]+/[^/]+)/actions/runs/(\d+)/job/(\d+)")
FAILED_OUTCOMES = {
    "failure", "timed_out", "action_required", "cancelled", "stale", "startup_failure",
}


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
