"""Scheduled concrete Runner emitters; outcome fixtures own severity semantics.

PR-479 writes web history directly and has no concrete Runner emitter.
task_status.py's bare log_event is a Callable[[str], None] adapter, not this API.
New/unselected daemon modules are reported separately, not rollout prerequisites.
"""

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCHEDULED = (
    "handlers/merge.py", "handlers/watch.py",  # PR-480
    "handlers/fix.py", "handlers/error.py", "retry_commands.py",  # PR-481
    "runner.py", "repo_ops.py",  # PR-482
    "recovery.py", "approval_commands.py",  # PR-483
    "handlers/idle.py", "handlers/coding.py", "preflight.py",  # PR-484
    "rate_limit.py", "handlers/hung.py", "handlers/breach.py",  # PR-485
    "quarantine.py", "fix_escalation.py", "fix_supervision.py",  # PR-486
    "publication.py",  # PR-513
    "fix_push_verify.py", "fix_codex_trigger.py",  # PR-487
)


def _emitters(tree):
    return [node for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr == "log_event"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id in {"self", "runner"}]


def _missing(tree):
    return [node.lineno for node in _emitters(tree)
            if not any(kw.arg == "severity" for kw in node.keywords)]


@pytest.mark.parametrize("module", SCHEDULED)
def test_scheduled_emitters_have_explicit_severity(module):
    tree = ast.parse((ROOT / "src/daemon" / module).read_text())
    assert _emitters(tree), f"Stale scheduled inventory: {module}"
    assert not _missing(tree), f"Missing severity in {module} at lines {_missing(tree)}"


@pytest.mark.parametrize("module", SCHEDULED)
def test_removing_scheduled_severity_is_detected(module):
    tree = ast.parse((ROOT / "src/daemon" / module).read_text())
    for event in _emitters(tree):
        original = event.keywords
        event.keywords = [kw for kw in original if kw.arg != "severity"]
        assert event.lineno in _missing(tree)
        event.keywords = original


def test_callback_adapter_is_excluded():
    tree = ast.parse((ROOT / "src/task_status.py").read_text())
    assert any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
               and node.func.id == "log_event" for node in ast.walk(tree))
    assert not _emitters(tree)
    assert not _missing(ast.parse('log_event("callback"); other.log_event("unrelated")'))


def test_report_unselected_daemon_emitters():
    inventory = {}
    for path in sorted((ROOT / "src/daemon").rglob("*.py")):
        module = path.relative_to(ROOT / "src/daemon").as_posix()
        if module not in SCHEDULED:
            tree = ast.parse(path.read_text())
            if _emitters(tree):
                inventory[module] = {"emitters": [n.lineno for n in _emitters(tree)],
                                     "missing_severity": _missing(tree)}
    print(f"Unselected daemon Runner emitters (informational): {inventory}")
