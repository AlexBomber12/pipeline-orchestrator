"""Explicit severity contracts for merge and watch events."""

import ast
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from src.daemon.handlers import merge, watch
from src.models import CIStatus, PipelineState, PRInfo, QueueTask, ReviewStatus, TaskStatus

from tests.runner import _helpers as h


@pytest.mark.parametrize("module", [merge, watch])
def test_every_event_has_explicit_valid_severity(module):
    tree = ast.parse(Path(module.__file__).read_text())
    events = [node for node in ast.walk(tree)
              if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
              and node.func.attr == "log_event"]
    assert events
    for event in events:
        severity = next((kw.value for kw in event.keywords if kw.arg == "severity"), None)
        assert isinstance(severity, ast.Constant), f"Missing severity at {module.__name__}:{event.lineno}"
        assert severity.value in {"error", "warn", "ok", "info"}


@pytest.mark.parametrize("module,fragment,expected", [
    (merge, "Merging PR #", "info"),
    (merge, "Merged PR #", "ok"),
    (merge, "Pre-merge sync pushed new commits", "ok"),
    (merge, "Run checkpoint deferred", "warn"),
    (merge, "command failed without admission", "error"),
    (merge, "Remote merge confirmed but status:DONE was not written", "error"),
    (watch, "review approved; waiting for CI", "info"),
    (watch, "quarantine released externally", "ok"),
    (watch, "will retry next cycle.", "warn"),
    (watch, "awaiting operator reconciliation", "error"),
    (watch, "Remote merge confirmed but status:DONE was not written", "error"),
])
def test_representative_event_outcomes(module, fragment, expected):
    source = Path(module.__file__).read_text()
    matches = [node for node in ast.walk(ast.parse(source))
               if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
               and node.func.attr == "log_event"
               and fragment in ast.get_source_segment(source, node)]
    assert matches
    for event in matches:
        assert next(kw.value.value for kw in event.keywords if kw.arg == "severity") == expected


@pytest.mark.parametrize("handler", ["merge", "watch"])
@pytest.mark.parametrize("raises", [False, True])
async def test_confirmed_merge_status_write_failure_is_error(monkeypatch, handler, raises):
    runner = h._make_runner()
    runner.state.state = PipelineState.MERGE if handler == "merge" else PipelineState.WATCH
    task = QueueTask(pr_id="PR-480", title="Severity", branch="pr-480",
                     task_file="tasks/PR-480.md", status=TaskStatus.DOING)
    pr = PRInfo(number=480, branch=task.branch, pr_id=task.pr_id, head_sha="a" * 40)
    runner.state.current_task = task
    runner.state.current_pr = pr
    monkeypatch.setattr(runner, "_restore_current_run_record", AsyncMock())
    monkeypatch.setattr(runner, "_checkpoint_current_run_record", AsyncMock())
    writer = AsyncMock(side_effect=RuntimeError("write failed")) if raises else AsyncMock(return_value=False)
    monkeypatch.setattr(runner, "_commit_task_status_change", writer)
    if handler == "merge":
        await runner._finish_confirmed_merge({})
    else:
        assert await runner._handle_external_pr_resolution(pr, "MERGED") is False
    event = runner.state.history[-1]
    assert event["severity"] == "error"
    assert ("Failed to write status:DONE" if raises else
            "Remote merge confirmed but status:DONE was not written") in event["event"]
    assert runner.state.current_task is task
    assert runner.state.current_pr is pr
    assert task.status == TaskStatus.DOING


async def test_review_approved_waiting_for_pending_ci_is_info(monkeypatch):
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    pr = PRInfo(number=480, branch="pr-480", head_sha="a" * 40,
                ci_status=CIStatus.PENDING, review_status=ReviewStatus.APPROVED,
                diff_scanned_at_sha="a" * 40)
    runner.state.current_pr = pr
    monkeypatch.setattr(watch.gh_prs, "get_open_prs", lambda *args, **kwargs: [pr])
    monkeypatch.setattr(watch.gh_prs, "get_pr_state", lambda *args: "OPEN")
    await runner.handle_watch()
    event = runner.state.history[-1]
    assert event["event"] == "[WATCH] PR #480 review approved; waiting for CI (PENDING)."
    assert event["severity"] == "info"
    assert runner.state.state == PipelineState.WATCH
