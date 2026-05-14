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
import re
from pathlib import Path

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


def test_every_log_event_call_starts_with_known_category() -> None:
    offenders: list[str] = []
    for path, node in _iter_log_event_calls():
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
