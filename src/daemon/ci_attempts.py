"""Bound, durable observations of GitHub Actions workflow attempts."""

from __future__ import annotations

import json
from pathlib import Path

from src.cancellation.storage import task_spec_content_hash
from src.github import gh_runner as gh_runner
from src.github.actions import (
    FAILED_OUTCOMES as FAILED_OUTCOMES,
)
from src.github.actions import (
    actions_job_identity as actions_job_identity,
)
from src.github.actions import (
    matching_failed_job as matching_failed_job,
)
from src.github.actions import (
    read_actions_run as read_actions_run,
)
from src.models import PRInfo, QueueTask, RepoState


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


def canonical_ci_incident(ci: dict) -> dict:
    """Freeze invocation provenance; discard only legacy mutable observations."""
    stable = {key: value for key, value in ci.items() if key != "_observed_attempts"}
    return json.loads(json.dumps(stable, sort_keys=True))
