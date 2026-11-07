"""Outcome severity contracts for quarantine and FIX escalation/supervision."""

import ast
from pathlib import Path

import pytest
from src.daemon import fix_escalation, fix_supervision, quarantine
from src.daemon.guardrails import GuardrailViolation

from tests.runner import _helpers as h


def _events(module):
    source = Path(module.__file__).read_text()
    return source, [node for node in ast.walk(ast.parse(source))
                    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "log_event"]


@pytest.mark.parametrize("module", [quarantine, fix_escalation, fix_supervision])
def test_every_event_has_explicit_valid_severity(module):
    _, events = _events(module)
    assert events
    for event in events:
        severity = next((kw.value for kw in event.keywords if kw.arg == "severity"), None)
        assert isinstance(severity, ast.Constant), f"Missing severity at {module.__name__}:{event.lineno}"
        assert severity.value in {"error", "warn", "ok", "info"}


@pytest.mark.parametrize("module,fragment,expected", [
    (quarantine, "cleanup deferred", "warn"),
    (quarantine, "Informational diff size", "info"),
    (quarantine, "label create failed", "error"),
    (quarantine, "label apply on PR", "error"),
    (quarantine, "comment post on PR", "error"),
    (fix_escalation, "label create failed", "error"),
    (fix_escalation, "failed to apply", "error"),
    (fix_escalation, "no-push deadlock after", "error"),
    (fix_escalation, "status:ERROR", "error"),
    (fix_escalation, "failed to post", "error"),
    (fix_supervision, "GitHub API poll failed", "warn"),
    (fix_supervision, "GitHub poll for PR", "warn"),
    (fix_supervision, "resetting idle timer", "ok"),
    (fix_supervision, "idle timeout", "error"),
    (fix_supervision, "reached terminal state", "info"),
    (fix_supervision, "merged externally", "ok"),
    (fix_supervision, "closed externally", "error"),
    (fix_supervision, "status:ERROR", "error"),
])
def test_representative_event_outcomes(module, fragment, expected):
    source, events = _events(module)
    matches = [event for event in events if fragment in ast.get_source_segment(source, event)]
    assert matches
    for event in matches:
        assert next(kw.value.value for kw in event.keywords if kw.arg == "severity") == expected


def test_informational_size_event_records_info():
    runner = h._make_runner()
    violation = GuardrailViolation(tier=2, category="large_diff_threshold", excerpt="+900 LOC", rule="size")
    assert quarantine.apply_quarantine_label_for_violation(runner, 42, violation)
    assert runner.state.history[-1]["severity"] == "info"
