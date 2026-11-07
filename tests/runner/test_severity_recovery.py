"""Explicit outcome severity for recovery and approval events."""

import ast
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from src.daemon import approval_commands, recovery

from tests.runner import _helpers as h


def _events(module):
    source = Path(module.__file__).read_text()
    return source, [node for node in ast.walk(ast.parse(source))
                    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "log_event"]


@pytest.mark.parametrize("module", [recovery, approval_commands])
def test_every_event_has_explicit_valid_severity(module):
    _, events = _events(module)
    assert events
    for event in events:
        severity = next((kw.value for kw in event.keywords if kw.arg == "severity"), None)
        assert isinstance(severity, ast.Constant), f"Missing severity at {module.__name__}:{event.lineno}"
        assert severity.value in {"error", "warn", "ok", "info"}


@pytest.mark.parametrize("module,fragment,expected", [
    (recovery, "operator investigation required.", "error"),
    (recovery, "Publication migration rejected:", "error"),
    (recovery, "failed to persist status:ERROR", "error"),
    (recovery, "Failed to preserve unpushed commits", "error"),
    (recovery, "Merge bookkeeping discovery deferred:", "warn"),
    (recovery, "Publication snapshot read unavailable:", "warn"),
    (recovery, "Obsolete CI-only condition cleared", "ok"),
    (recovery, "Preserved crashed-run commits", "ok"),
    (recovery, "Restored MERGE attempt", "ok"),
    (recovery, "Restored retained merge failure", "error"),
    (approval_commands, "parking for operator Retry.", "error"),
    (approval_commands, "Size approval task status write deferred", "error"),
    (approval_commands, "Guardrail PR head refresh deferred:", "warn"),
    (approval_commands, "Approval store/application unavailable", "warn"),
    (approval_commands, "Approved PR waiting:", "info"),
])
def test_representative_event_outcomes(module, fragment, expected):
    source, events = _events(module)
    matches = [event for event in events if fragment in ast.get_source_segment(source, event)]
    assert matches
    for event in matches:
        assert next(kw.value.value for kw in event.keywords if kw.arg == "severity") == expected


def test_preserve_on_base_branch_is_error():
    runner = h._make_runner()
    assert runner._preserve_crashed_run_commits(runner.repo_config.branch) is False
    event = runner.state.history[-1]
    assert "Refusing to preserve crashed-run commits" in event["event"]
    assert event["severity"] == "error"


async def test_approval_store_unavailable_is_warn(monkeypatch):
    runner = h._make_runner()
    monkeypatch.setattr(approval_commands, "list_approvals", AsyncMock(side_effect=OSError("store offline")))
    assert await runner._consume_approval_command() is True
    event = runner.state.history[-1]
    assert "Approval store/application unavailable; deferring cycle: store offline." in event["event"]
    assert event["severity"] == "warn"
