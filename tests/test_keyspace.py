"""Tests for the canonical Redis key namespace helpers."""

from __future__ import annotations

import pytest
from src import keyspace


@pytest.mark.parametrize(
    "repo",
    [
        "alpha",
        "example__alpha",
        "Owner__Repo.with-dots",
        "owner__repo_with_underscores",
    ],
)
def test_pipeline_state_format(repo: str) -> None:
    assert keyspace.pipeline_state(repo) == f"pipeline:{repo}"


def test_control_stop_format() -> None:
    assert keyspace.control_stop("alpha") == "control:alpha:stop"
    assert keyspace.control_stop("owner__repo") == "control:owner__repo:stop"


def test_control_config_dirty_format() -> None:
    assert keyspace.control_config_dirty("alpha") == "control:alpha:config_dirty"
    assert (
        keyspace.control_config_dirty("owner__repo")
        == "control:owner__repo:config_dirty"
    )


def test_control_recover_format() -> None:
    assert keyspace.control_recover("alpha") == "control:alpha:recover"
    assert (
        keyspace.control_recover("owner__repo")
        == "control:owner__repo:recover"
    )


def test_upload_pending_format() -> None:
    assert keyspace.upload_pending("alpha") == "upload:alpha:pending"
    assert keyspace.upload_pending("owner__repo") == "upload:owner__repo:pending"


def test_status_write_failed_tasks_format() -> None:
    assert (
        keyspace.status_write_failed_tasks("alpha")
        == "status_write_failed_tasks:alpha"
    )
    assert (
        keyspace.status_write_failed_tasks("owner__repo")
        == "status_write_failed_tasks:owner__repo"
    )


def test_legacy_recovered_tasks_format() -> None:
    assert keyspace.legacy_recovered_tasks("alpha") == "recovered_tasks:alpha"
    assert (
        keyspace.legacy_recovered_tasks("owner__repo")
        == "recovered_tasks:owner__repo"
    )


def test_upload_pending_pattern_is_glob() -> None:
    assert keyspace.upload_pending_pattern() == "upload:*:pending"


def test_cli_log_latest_format() -> None:
    assert keyspace.cli_log_latest("alpha") == "cli_log:alpha:latest"


def test_cli_log_history_format() -> None:
    assert (
        keyspace.cli_log_history("alpha", "2026-05-02T12:00:00Z")
        == "cli_log:alpha:2026-05-02T12:00:00Z"
    )


def test_repo_events_channel_format() -> None:
    assert keyspace.repo_events_channel("alpha") == "repo-events:alpha"
    assert (
        keyspace.repo_events_channel("owner__repo") == "repo-events:owner__repo"
    )


def test_repo_events_history_format() -> None:
    assert keyspace.repo_events_history("alpha") == "repo-events-history:alpha"
    assert (
        keyspace.repo_events_history("owner__repo")
        == "repo-events-history:owner__repo"
    )


def test_helpers_handle_owner_repo_slug_form() -> None:
    """Internally repo names follow ``owner__repo``; helpers must round-trip."""
    name = "AlexBomber12__pipeline-orchestrator"
    assert keyspace.pipeline_state(name).startswith("pipeline:")
    assert keyspace.pipeline_state(name).endswith(name)
    assert keyspace.control_stop(name).split(":")[1] == name
    assert keyspace.upload_pending(name).split(":")[1] == name
    assert keyspace.repo_events_channel(name).split(":")[1] == name
