"""Validate that every ``log_event(...)`` call in ``src/daemon/`` starts
with one of the approved category prefixes (PR-199).

The check walks each module's AST and inspects ``log_event`` call sites
whose first argument is a literal-string expression (``str`` constant,
``f"..."`` JoinedStr, or implicit string concatenation). Calls that
forward a runtime value (``log_event(message)``,
``log_event(self.state.error_message)``) are skipped because their
value is constructed elsewhere — typically by wrapping
``self.state.error_message`` with a category prefix at the call site.
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from src.daemon.runner import PipelineRunner
from src.models import RepoState, validate_event_severity

ALLOWED_CATEGORIES = (
    "CODING",
    "WATCH",
    "FIX",
    "MERGE",
    "ERROR",
    "INFRA",
    "RATE-LIMIT",
    "ESCALATE",
    "ANALYTICS",
    "BRANCH",
    "RECOVERY",
    "AGENTS-SCAN",
    "AUDIT",
    "AUTO-PAUSE",
    "GUARDRAIL",
    "BACKUP",
)
_PREFIX_RE = re.compile(
    r"^\[(?:" + "|".join(re.escape(c) for c in ALLOWED_CATEGORIES) + r")\] "
)
DAEMON_DIR = Path(__file__).resolve().parent.parent / "src" / "daemon"


def _leading_literal(node: ast.expr) -> str | None:
    """Return the leading literal-string text of *node*, or ``None``.

    Handles plain string constants and f-strings (``ast.JoinedStr``).
    For f-strings, the leading run of literal text before the first
    interpolation is returned. Returns ``None`` when the expression is
    not a string literal at all (e.g. a variable reference).
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        if not node.values:
            return ""
        first = node.values[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, str):
            return first.value
        return None
    return None


def _iter_log_event_calls():
    for path in sorted(DAEMON_DIR.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        tree = ast.parse(text, filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not (isinstance(func, ast.Attribute) and func.attr == "log_event"):
                continue
            if not node.args:
                continue
            yield path, node


def _has_structured_tier(node: ast.Call) -> bool:
    return any(keyword.arg == "tier" for keyword in node.keywords)


def test_every_log_event_call_starts_with_known_category() -> None:
    offenders: list[str] = []
    for path, node in _iter_log_event_calls():
        if _has_structured_tier(node):
            continue
        leading = _leading_literal(node.args[0])
        if leading is None:
            # Runtime value (e.g. forwarded error_message). The call site
            # is responsible for wrapping with the category prefix.
            continue
        if not _PREFIX_RE.match(leading):
            offenders.append(f"{path}:{node.lineno}: {leading!r}")
    assert not offenders, (
        "log_event calls missing a `[CATEGORY] ` prefix:\n"
        + "\n".join(offenders)
    )


@pytest.fixture
def event_runner() -> PipelineRunner:
    runner = object.__new__(PipelineRunner)
    runner.name = "owner__repo"
    runner.state = RepoState(url="https://github.com/owner/repo.git", name=runner.name)
    runner._pending_event_log_entries = []
    runner.redis = AsyncMock()
    return runner


@pytest.mark.parametrize("severity", ["error", "warn", "ok", "info", None])
def test_log_event_severity(event_runner: PipelineRunner, severity: str | None) -> None:
    validate_event_severity(severity)
    event_runner.log_event("message", severity=severity)
    event_runner.log_event("message", severity=severity)
    entry = event_runner.state.history[0]
    assert len(event_runner.state.history) == 1
    assert entry["count"] == 2
    assert entry.get("severity") == severity
    assert ("severity" in entry) == (severity is not None)


@pytest.mark.parametrize("severity", ["fatal", "ERROR", "", "unknown"])
def test_log_event_rejects_invalid_severity(event_runner: PipelineRunner, severity: str) -> None:
    with pytest.raises(ValueError, match="Unknown event severity"):
        event_runner.log_event("message", severity=severity)
    assert event_runner.state.history == []
    assert event_runner._pending_event_log_entries == []


def test_severity_is_part_of_dedup_key(event_runner: PipelineRunner) -> None:
    for severity in (None, "error", "warn", "ok", "info", None):
        event_runner.log_event("message", severity=severity)
    assert len(event_runner.state.history) == 6
    assert all(entry["count"] == 1 for entry in event_runner.state.history)


async def test_severity_in_published_event_and_disk(
    event_runner: PipelineRunner, isolate_events_dir: Path,
) -> None:
    event_runner.log_event("message", severity="error")
    event_runner.log_event("message", severity="error")
    await event_runner._publish_pending_event_log_entries()
    messages = [json.loads(call.args[1]) for call in event_runner.redis.publish.call_args_list]
    assert [message["type"] for message in messages] == ["event_log_append"] * 2
    assert [message["data"]["entry"]["severity"] for message in messages] == ["error"] * 2
    assert [message["data"]["entry"]["count"] for message in messages] == [1, 2]
    records = [
        json.loads(line)
        for path in (isolate_events_dir / event_runner.name).glob("*.jsonl")
        for line in path.read_text().splitlines()
    ]
    assert [record["severity"] for record in records] == ["error"] * 2
    assert [record["payload"] for record in records] == [message["data"] for message in messages]
    assert event_runner._pending_event_log_entries == []
