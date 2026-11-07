"""Outcome severity contracts for rate limits, review posting, and breaches."""

import ast
import json
from pathlib import Path

import pytest
from src.daemon import rate_limit
from src.daemon.handlers import breach, hung

from tests.runner import _helpers as h


def _events(module):
    source = Path(module.__file__).read_text()
    return source, [node for node in ast.walk(ast.parse(source))
                    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "log_event"]


@pytest.mark.parametrize("module", [rate_limit, hung, breach])
def test_every_event_has_explicit_valid_severity(module):
    _, events = _events(module)
    assert events
    for event in events:
        severity = next((kw.value for kw in event.keywords if kw.arg == "severity"), None)
        assert isinstance(severity, ast.Constant), f"Missing severity at {module.__name__}:{event.lineno}"
        assert severity.value in {"error", "warn", "ok", "info"}


@pytest.mark.parametrize("module,fragment,expected", [
    (rate_limit, "paused until reset.", "warn"),
    (rate_limit, "Proactive pause:", "warn"),
    (rate_limit, "warn dedup cleanup failed:", "error"),
    (rate_limit, "warn notification failed:", "error"),
    (rate_limit, "Rate limited, resuming in", "warn"),
    (rate_limit, "Usage API degraded", "warn"),
    (rate_limit, "Rate limit window expired, resuming.", "ok"),
    (rate_limit, "remains rate-limited until", "ok"),
    (rate_limit, "Rate limit detected", "warn"),
    (hung, "failed to load PR metadata", "warn"),
    (hung, "failed to resolve HEAD", "warn"),
    (hung, "review request history", "warn"),
    (hung, "Skipping duplicate @codex review", "info"),
    (hung, "Posted @codex review", "ok"),
    (hung, "failed to post @codex review", "error"),
    (breach, "In-flight breach:", "warn"),
    (breach, "Late in-flight breach detected:", "warn"),
])
def test_event_outcome_severity(module, fragment, expected):
    source, events = _events(module)
    matches = [event for event in events if fragment in ast.get_source_segment(source, event)]
    assert matches
    for event in matches:
        assert next(kw.value.value for kw in event.keywords if kw.arg == "severity") == expected


def test_late_breach_records_warning_and_pause(tmp_path):
    runner = h._make_runner()
    (tmp_path / "run.breach").write_text(json.dumps({"type": "session", "session_pct": 99}))
    flag = {"breached": False}
    runner._check_late_breach(str(tmp_path), "run", "claude", flag)
    assert flag["breached"] is True
    assert "claude" in runner.state.rate_limited_coders
    assert runner.state.history[-1]["severity"] == "warn"
