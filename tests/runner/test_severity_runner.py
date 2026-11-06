"""Runner and repository event severity follows outcomes, not message prefixes."""

import ast
from pathlib import Path

import pytest
from src.daemon import repo_ops, runner

from tests.runner import _helpers as h


def _events(module):
    source = Path(module.__file__).read_text()
    return source, [node for node in ast.walk(ast.parse(source))
                    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "log_event"]


@pytest.mark.parametrize("module", [runner, repo_ops])
def test_every_event_has_explicit_severity(module):
    _, events = _events(module)
    assert events
    for event in events:
        severity = next((kw.value for kw in event.keywords if kw.arg == "severity"), None)
        assert severity is not None, f"Missing severity at {module.__name__}:{event.lineno}"
        if isinstance(severity, ast.Constant):
            assert severity.value in {"error", "warn", "ok", "info"}
        else:
            # The ERROR primitive forwards an explicitly supplied caller outcome.
            assert module is runner
            assert isinstance(severity, ast.Name) and severity.id == "log_severity"


@pytest.mark.parametrize("module,fragment,expected", [
    (runner, "cleared error_message", "ok"),
    (runner, "Applied staged repo config", "ok"),
    (runner, "late review admitted", "ok"),
    (runner, "operator-cleared ERROR task frontmatter", "ok"),
    (runner, "Paused. Press Play to resume.", "info"),
    (runner, "User stop requested", "info"),
    (runner, "No task status change to commit", "info"),
    (runner, "GitHub API budget critical", "warn"),
    (runner, "Coder invocation deferred", "warn"),
    (runner, "Obsolete CI observation deferred", "warn"),
    (runner, "status:ERROR write failed", "error"),
    (runner, "failed to commit", "error"),
    (runner, "max soft-skip retries", "error"),
    (runner, "failure remains ERROR until", "error"),
    (repo_ops, "Upload push failed", "error"),
    (repo_ops, "Upload admission blocked", "error"),
    (repo_ops, "Task upload blocked", "error"),
    (repo_ops, "Upload metadata update failed", "error"),
    (repo_ops, "will retry", "warn"),
    (repo_ops, "scaffold_repo deferred", "warn"),
    (repo_ops, "Newer upload pending", "warn"),
    (repo_ops, "and pushed to", "ok"),
    (repo_ops, "scaffold_repo created", "ok"),
    (repo_ops, "Skipping QUEUE.md", "info"),
])
def test_representative_event_outcomes(module, fragment, expected):
    source, events = _events(module)
    matches = [event for event in events if fragment in ast.get_source_segment(source, event)]
    assert matches
    for event in matches:
        assert next(kw.value.value for kw in event.keywords if kw.arg == "severity") == expected


async def test_clearing_error_message_is_success():
    pipeline = h._make_runner()
    pipeline.state.error_message = "Upload push failed"
    await pipeline._clear_error_message_on_recovery(log_prefix="[ERROR]", reason="Retry")
    assert pipeline.state.error_message is None
    event = pipeline.state.history[-1]
    assert event["event"] == "[ERROR] cleared error_message (Retry): Upload push failed"
    assert event["severity"] == "ok"


@pytest.mark.parametrize("severity,expected", [(None, "error"), ("warn", "warn"), ("info", "info")])
async def test_error_transition_severity_default_and_override(severity, expected):
    pipeline = h._make_runner()
    await pipeline._transition_to_error(
        "step failed", log_severity=severity, save_run_record_as=None, publish=False,
    )
    event = next(event for event in pipeline.state.history if "step failed" in event["event"])
    assert event["severity"] == expected
