"""Stable identifier validation shared by coder configuration and tasks."""

from __future__ import annotations

import re
from typing import Any

CODER_PLUGIN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")


def validate_coder_plugin_id(value: Any, *, allow_any: bool = False) -> str:
    """Return a validated plugin ID without importing plugin implementations."""
    if not isinstance(value, str) or not CODER_PLUGIN_ID_PATTERN.fullmatch(value):
        suffix = " or 'any'" if allow_any else ""
        raise ValueError(
            "coder must be a stable plugin ID using only letters, digits, "
            f"underscores, and hyphens{suffix}"
        )
    if value == "any" and not allow_any:
        raise ValueError("coder plugin ID 'any' is reserved for task inheritance")
    return value
