"""Provider diagnostics, excluding prompt and tool transcript content."""
from __future__ import annotations

import json
import re

RATE_LIMIT_STATUS = re.compile(
    r"^(?:(?:error|warning|api error)\s*:\s*)?(?:(?:claude|codex)\s+)?"
    r"(?:http(?:/\d\.\d)?\s+429\b|status(?: code)?[: =]+429\b|429\s+too many requests)",
    re.IGNORECASE | re.MULTILINE,
)


def diagnostic_text(text: str, *, custom: bool = False) -> str:
    lines = []
    fenced = False
    tool = False
    for line in text.splitlines():
        value = line.strip()
        if value.startswith("```"):
            fenced = not fenced
            continue
        if value == "exec":
            tool = True
            continue
        if value in {"thinking", "codex", "assistant", "tokens used"}:
            tool = False
            continue
        if fenced or tool or value.startswith(('"', "'", "`", ">", "$")):
            continue
        if value.startswith("{"):
            try:
                event = json.loads(value)
            except ValueError:
                continue
            if event.get("type") not in {"error", "turn.failed"}:
                continue
            error = event.get("error", event)
            if not isinstance(error, dict):
                continue
            value = str(error.get("message", ""))
        if custom or re.match(
            r"(?i)^(?:(?:error|warning|api error)\s*:\s*)?(?:(?:claude|codex)\s+)?"
            r"(?:http(?:/\d\.\d)?\s+\d{3}\b|status(?: code)?[: =]+429\b|"
            r"429\s+too many requests|503\s+service unavailable|"
            r"selected model is at capacity|service unavailable|"
            r"(?:you've hit your |you have exceeded your |)usage limit|"
            r"rate limit\b|api rate limit\b|\d{1,3}%\s|"
            r"(?:weekly|week|session|5-hour)\s+rate limit\b)", value,
        ):
            lines.append(value)
    return "\n".join(lines)


def is_capacity_error(text: str) -> bool:
    return bool(re.search(
        r"(?i)(selected model is at capacity|\b503\s+service unavailable|"
        r"\bservice unavailable\b)", diagnostic_text(text),
    ))
