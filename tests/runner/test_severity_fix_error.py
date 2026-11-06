"""Outcome severity contracts for FIX, error recovery, and operator Retry."""

import ast
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from src.daemon import retry_commands
from src.daemon.handlers import error, fix
from src.models import PipelineState, PRInfo

from tests.runner import _helpers as h


def _events(module):
    source = Path(module.__file__).read_text()
    return source, [node for node in ast.walk(ast.parse(source))
                    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "log_event"]


@pytest.mark.parametrize("module", [fix, error, retry_commands])
def test_every_event_has_explicit_valid_severity(module):
    _, events = _events(module)
    assert events
    for event in events:
        severity = next((kw.value for kw in event.keywords if kw.arg == "severity"), None)
        assert isinstance(severity, ast.Constant), f"Missing severity at {module.__name__}:{event.lineno}"
        assert severity.value in {"error", "warn", "ok", "info"}


@pytest.mark.parametrize("module,fragment,expected", [
    (fix, "Fix pushed, iteration #", "ok"),
    (fix, "returning to WATCH.", "info"),
    (fix, "entering FIX.", "info"),
    (fix, "deferring dispatch.", "warn"),
    (fix, "for operator recovery.", "error"),
    (fix, "Could not persist active FIX run", "error"),
    (fix, "no push detected", "error"),
    (error, "operator Retry", "error"),
    (error, "write still", "error"),
    (error, "diagnose_error CLI failed", "error"),
    (error, "re-observing safety evidence next cycle", "warn"),
    (error, "Diagnosis deferred", "warn"),
    (error, "independent work released", "ok"),
    (error, "diagnose_error: FIX -> IDLE", "ok"),
    (error, "SKIP -> terminal failure reconciliation", "info"),
    (retry_commands, "operator Retry required", "error"),
    (retry_commands, "status:TODO write failed", "error"),
    (retry_commands, "admission failed", "error"),
    (retry_commands, "admission deferred", "warn"),
    (retry_commands, "deferred for", "warn"),
    (retry_commands, "Applied Retry", "ok"),
    (retry_commands, "retry admitted", "ok"),
])
def test_representative_event_outcomes(module, fragment, expected):
    source, events = _events(module)
    matches = [event for event in events if fragment in ast.get_source_segment(source, event)]
    assert matches
    for event in matches:
        assert next(kw.value.value for kw in event.keywords if kw.arg == "severity") == expected


async def test_fix_cross_repo_rejection_is_error(monkeypatch):
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=481, branch="pr-481", is_cross_repository=True)
    monkeypatch.setattr(runner, "_refresh_auth_status_cache", AsyncMock())
    monkeypatch.setattr(runner, "usage_gate", AsyncMock(return_value=True))
    await runner.handle_fix()
    assert runner.state.history[-1]["severity"] == "error"
    assert "Skipping FIX for cross-repo PR #481" in runner.state.history[-1]["event"]
    assert runner.state.state == PipelineState.WATCH


async def test_error_operator_retry_is_error():
    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    await runner.handle_error("Size FIX outcome unresolved; operator Retry required.")
    assert runner.state.history[-1]["severity"] == "error"
    assert "parked for operator Retry" in runner.state.history[-1]["event"]
    assert runner.state.state == PipelineState.ERROR


async def test_retry_missing_binding_is_error():
    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    await runner._admit_fix_idle_timeout()
    assert runner.state.history[-1]["severity"] == "error"
    assert "no bound task/PR; operator Retry required" in runner.state.history[-1]["event"]
    assert runner.state.state == PipelineState.ERROR
