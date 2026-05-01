"""Detect installed coder CLI versions via ``npm list -g``.

The detected version goes into the ``coder_extension_version`` field of
the merged-PR outcome log. A value change between rows means the CLI
tool that produced the run was upgraded — useful when a coder tool
update correlates with an outcome distribution shift.

Detection is deliberately resilient: any failure (no node, missing
package, malformed JSON, timeout) returns ``None`` so the analytics
record can still be written. The caller decides how to surface the
absence (the schema accepts ``null``).
"""

from __future__ import annotations

import json
import logging
import subprocess

logger = logging.getLogger(__name__)

_PACKAGE_BY_CODER: dict[str, str] = {
    "claude": "@anthropic-ai/claude-code",
    "codex": "@openai/codex",
}

_NPM_TIMEOUT_SEC = 5


def _npm_list_global(package: str) -> dict | None:
    try:
        result = subprocess.run(
            ["npm", "list", "-g", "--depth=0", "--json", package],
            capture_output=True,
            text=True,
            timeout=_NPM_TIMEOUT_SEC,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as exc:
        logger.debug("npm list -g %s failed: %s", package, exc)
        return None
    stdout = result.stdout or ""
    if not stdout.strip():
        return None
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def detect_coder_extension_version(coder: str) -> str | None:
    """Return the installed CLI version for ``coder`` or ``None``.

    ``coder`` is the configured coder name (``claude`` or ``codex``).
    Returns ``None`` for unknown coders, tool absence, lookup failure,
    or any malformed response — the caller always treats ``None`` as
    "version unknown" and writes JSON ``null``.
    """
    package = _PACKAGE_BY_CODER.get(coder)
    if package is None:
        return None
    payload = _npm_list_global(package)
    if payload is None:
        return None
    dependencies = payload.get("dependencies")
    if not isinstance(dependencies, dict):
        return None
    entry = dependencies.get(package)
    if not isinstance(entry, dict):
        return None
    version = entry.get("version")
    if isinstance(version, str) and version:
        return version
    return None
