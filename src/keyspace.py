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


def recovered_tasks(repo_name: str) -> str:
    """Persisted PR-IDs the operator abandoned via the HUNG recover button.

    JSON-encoded sorted list of PR-IDs. The daemon hydrates the
    in-memory ``_recovered_task_pr_ids`` set from this key in
    ``recover_state`` and adds to it from
    ``_perform_operator_recovery``; ``process_pending_uploads`` removes
    any uploaded PR-IDs and rewrites the snapshot. Without this
    persistence a daemon restart between the recover click and the
    user's task re-upload would lose the marker, ``recover_state`` would
    rehydrate the CANCELED row into ``_crashed_task_pr_ids`` instead,
    and the IDLE selector would discard the override on the still-open
    PR deriving back to ``DOING`` — defeating the recover button's
    "abandon until re-upload" contract (PR-247 follow-up).
    """
    return f"recovered_tasks:{repo_name}"


def upload_pending(repo_name: str) -> str:
    """Key for the pending-upload manifest consumed by the daemon."""
    return f"upload:{repo_name}:pending"


def upload_pending_pattern() -> str:
    """Glob pattern matching all ``upload:*:pending`` keys."""
    return "upload:*:pending"


def cli_log_latest(repo_name: str) -> str:
    """Key for the most recent CLI log payload published for the dashboard."""
    return f"cli_log:{repo_name}:latest"


def cli_log_history(repo_name: str, timestamp: str) -> str:
    """Key for a timestamped CLI log history entry."""
    return f"cli_log:{repo_name}:{timestamp}"


def repo_events_channel(repo_name: str) -> str:
    """PubSub channel for repo-scoped operator-visible events."""
    return f"repo-events:{repo_name}"


def repo_events_history(repo_name: str) -> str:
    """List key holding the recent history of operator-visible events."""
    return f"repo-events-history:{repo_name}"
