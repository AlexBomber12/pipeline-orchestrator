"""Canonical Redis key namespaces for pipeline-orchestrator.

This module is the single source of truth for every Redis key the app
constructs, except the wake-channel namespace owned by ``src.events.wake``.
Centralizing the names makes it easy to grep for who reads or writes a
given key family, and prevents silent drift such as a feature accidentally
introducing ``pipeline_state:`` while the rest of the code uses
``pipeline:``.

The module intentionally has zero runtime dependencies. Helpers return
plain strings so callers can use them anywhere a key literal was used
before, byte-identical to the previous f-strings.
"""

from __future__ import annotations


def pipeline_state(repo_name: str) -> str:
    """Key for the published RepoState snapshot consumed by the dashboard."""
    return f"pipeline:{repo_name}"


def control_stop(repo_name: str) -> str:
    """Key for the stop-request control flag set by the web layer."""
    return f"control:{repo_name}:stop"


def control_config_dirty(repo_name: str) -> str:
    """Key for the config-reload trigger consumed by the daemon."""
    return f"control:{repo_name}:config_dirty"


def control_recover(repo_name: str) -> str:
    """One-shot recovery signal written by the web layer for HUNG repos.

    The HUNG handler reads-and-clears this key to exit HUNG and return to
    IDLE without waiting for the operator to merge or close the parked PR
    (PR-247).
    """
    return f"control:{repo_name}:recover"


def upload_pending(repo_name: str) -> str:
    """Key for the pending-upload manifest consumed by the daemon."""
    return f"upload:{repo_name}:pending"


def upload_pending_count(repo_name: str) -> str:
    """Count of files waiting in a pending dashboard upload."""
    return f"upload_pending_count:{repo_name}"


def status_write_failed_tasks(repo_name: str) -> str:
    """PR IDs parked after status:ERROR could not be committed."""
    return f"status_write_failed_tasks:{repo_name}"


def legacy_recovered_tasks(repo_name: str) -> str:
    """Legacy PR IDs parked by pre-PR-281 recovery fallback state."""
    return f"recovered_tasks:{repo_name}"


def recovery_backup_branch(repo_name: str, task_id: str) -> str:
    """Backup-branch pointer recorded when the PR-351 recovery push fallback
    succeeded for a task whose primary feature-branch push was rejected by
    GitHub branch protection or a non-fast-forward state. Surfaces in the
    dashboard cancellation card so operators can recover work via
    ``git fetch && git checkout <branch>`` even when the feature-branch
    push was rejected.
    """
    return f"recovery:backup_branch:{repo_name}:{task_id}"


def upload_pending_pattern() -> str:
    """Glob pattern matching all ``upload:*:pending`` keys."""
    return "upload:*:pending"


def cli_log_latest(repo_name: str) -> str:
    """Key for the most recent CLI log payload published for the dashboard."""
    return f"cli_log:{repo_name}:latest"


def cli_log_history(repo_name: str, timestamp: str) -> str:
    """Key for a timestamped CLI log history entry."""
    return f"cli_log:{repo_name}:{timestamp}"


def ci_infra_retried(repo_name: str, pr_number: int, head_sha: str) -> str:
    """Per-(repo, pr, head_sha) marker that the WATCH handler has already
    re-run the failed workflow once after CI was classified
    ``CIStatus.INFRA_FAILURE``. Subsequent INFRA_FAILURE classifications
    on the same SHA route to ``handle_fix`` instead of triggering another
    rerun, so a persistent infra-class failure does not loop forever
    (PR-251 / OBS-BC).
    """
    return f"ci_infra_retried:{repo_name}:{pr_number}:{head_sha}"


def repo_events_channel(repo_name: str) -> str:
    """PubSub channel for repo-scoped operator-visible events."""
    return f"repo-events:{repo_name}"


def repo_events_history(repo_name: str) -> str:
    """List key holding the recent history of operator-visible events."""
    return f"repo-events-history:{repo_name}"


def daemon_panic_state() -> str:
    """Daemon-global panic state record (not per-repo)."""
    return "daemon:panic_state"
