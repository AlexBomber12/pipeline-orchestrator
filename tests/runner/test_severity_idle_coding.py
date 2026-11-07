"""Outcome severity contracts for task dispatch, coding, and preflight."""

import ast
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from src.daemon import preflight
from src.daemon.handlers import coding, idle
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus

from tests.runner import _helpers as h


def _events(module):
    source = Path(module.__file__).read_text()
    return source, [node for node in ast.walk(ast.parse(source))
                    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "log_event"]


@pytest.mark.parametrize("module", [idle, coding, preflight])
def test_every_event_has_explicit_valid_severity(module):
    _, events = _events(module)
    assert events
    for event in events:
        severity = next((kw.value for kw in event.keywords if kw.arg == "severity"), None)
        assert isinstance(severity, ast.Constant), f"Missing severity at {module.__name__}:{event.lineno}"
        assert severity.value in {"error", "warn", "ok", "info"}


@pytest.mark.parametrize("module,fragment,expected", [
    (idle, "Picked task", "info"),
    (idle, "No tasks available.", "info"),
    (idle, "bundle creation crashed", "error"),
    (idle, "git bundle failed", "error"),
    (idle, "prune failed", "error"),
    (idle, "admission requires action", "error"),
    (idle, "coder configuration requires action", "error"),
    (idle, "git bundle created", "ok"),
    (idle, "old bundles", "ok"),
    (idle, "PAUSED inhibitors cleared", "ok"),
    (idle, "waiting for coder resource", "warn"),
    (idle, "Retained publication upload deferred", "warn"),
    (idle, "open PR check failed", "warn"),
    (coding, "Starting PLANNED PR.", "info"),
    (coding, "user stop requested.", "info"),
    (coding, "Reconciliation deferred", "warn"),
    (coding, "FIX head verification deferred", "warn"),
    (coding, "Publication baseline unavailable", "warn"),
    (coding, "Publication recheck failed before", "warn"),
    (coding, "current_run_started_at write failed", "error"),
    (coding, "expected-branch write failed", "error"),
    (coding, "active-pr-id runtime write failed", "error"),
    (coding, "mismatch detected", "error"),
    (coding, "Recovered retained PR", "ok"),
    (coding, "Verified publication", "ok"),
    (preflight, "Dirty tree persisted", "warn"),
    (preflight, "Auto-recovery failed", "error"),
    (preflight, "Auto-recovered from dirty tree", "ok"),
])
def test_representative_event_outcomes(module, fragment, expected):
    source, events = _events(module)
    matches = [event for event in events if fragment in ast.get_source_segment(source, event)]
    assert matches
    for event in matches:
        assert next(kw.value.value for kw in event.keywords if kw.arg == "severity") == expected


async def test_backup_crash_is_error_and_remains_retryable(monkeypatch):
    runner = h._make_runner()
    runner.app_config.daemon.git_bundle_backup_enabled = True
    runner.app_config.daemon.git_bundle_backup_dir = "/tmp/test-severity-backup"
    runner._git_bundle_backup_last_run_at = 0.0
    monkeypatch.setattr(idle.time, "monotonic", lambda: 10**9)
    monkeypatch.setattr(idle, "create_repo_bundle", AsyncMock(side_effect=OSError("disk offline")))
    await runner._run_git_bundle_backup_if_due()
    event = runner.state.history[-1]
    assert event["event"] == "[BACKUP] bundle creation crashed: disk offline"
    assert event["severity"] == "error"
    assert runner._git_bundle_backup_last_run_at == 0.0


async def test_coding_recorded_publication_is_ok(monkeypatch):
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-001")
    runner.state.current_task = QueueTask(pr_id="PR-001", title="test", status=TaskStatus.TODO, branch="pr-001")
    runner._start_current_run_record("claude", "opus")
    runner._capture_invocation_snapshot("claude", {}, phase="coding")
    runner._begin_publication_attempt(started_at=datetime.now(timezone.utc), baseline_numbers=set())
    monkeypatch.setattr(runner, "_resolve_published_attempt", AsyncMock(return_value=True))
    await runner._record_pre_pause_pr("pr-001", "before pause")
    event = runner.state.history[-1]
    assert event["event"] == "[CODING] Recorded PR #42 before pause."
    assert event["severity"] == "ok"


@pytest.mark.parametrize("failed,expected", [(False, "ok"), (True, "error")])
def test_preflight_recovery_outcome_severity(monkeypatch, failed, expected):
    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR

    def fake_git(*args, **kwargs):
        if failed:
            raise OSError("disk offline")

    monkeypatch.setattr(preflight.git_ops, "_git", fake_git)
    runner._auto_reset_dirty_tree()
    event = runner.state.history[-1]
    assert event["severity"] == expected
    assert ("Auto-recovery failed" if failed else "Auto-recovered") in event["event"]
    assert runner.state.state == (PipelineState.ERROR if failed else PipelineState.IDLE)
