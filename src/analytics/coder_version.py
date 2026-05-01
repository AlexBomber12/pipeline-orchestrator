"""Detect installed coder CLI versions from their npm package.json.

The detected version goes into the ``coder_extension_version`` field of
the merged-PR outcome log. A value change between rows means the CLI
tool that produced the run was upgraded — useful when a coder tool
update correlates with an outcome distribution shift.

Detection prefers reading ``package.json`` directly from the npm install
prefix derived from the binary on ``PATH``. This is the only reliable
strategy when a coder is installed under a non-default global prefix
(e.g. codex in this repo's Dockerfile uses
``npm i -g --prefix /home/runner/.npm-global``); a plain ``npm list -g``
queries only the default prefix and would miss those installs. The
``npm list -g`` path is kept as a fallback for environments where the
binary is not yet on ``PATH`` at detection time.

Detection is deliberately resilient: any failure (no node, missing
package, malformed JSON, timeout, unreadable file) returns ``None`` so
the analytics record can still be written. The caller always treats
``None`` as "version unknown" and writes JSON ``null``.
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)

_PACKAGE_BY_CODER: dict[str, str] = {
    "claude": "@anthropic-ai/claude-code",
    "codex": "@openai/codex",
}

_BINARY_BY_CODER: dict[str, str] = {
    "claude": "claude",
    "codex": "codex",
}

_NPM_TIMEOUT_SEC = 5


def _resolve_install_prefix(binary: str) -> str | None:
    """Return the npm install prefix that hosts ``binary`` on PATH.

    npm installs CLIs as ``<prefix>/bin/<binary>`` regardless of whether
    the prefix is the default global one or a custom one passed to
    ``npm i -g --prefix``. Resolving the binary on ``PATH`` and walking
    one level up from its parent ``bin`` directory recovers the prefix
    used at install time without needing to ask npm. Returns ``None``
    when the binary is missing or its parent is not a ``bin`` directory.
    """
    path = shutil.which(binary)
    if path is None:
        return None
    try:
        bin_dir = Path(path).resolve().parent
    except OSError:
        return None
    if bin_dir.name != "bin":
        return None
    return str(bin_dir.parent)


def _read_package_version(prefix: str, package: str) -> str | None:
    """Return ``version`` from the package.json at ``prefix``, or None."""
    pkg_json = (
        Path(prefix) / "lib" / "node_modules" / package / "package.json"
    )
    try:
        text = pkg_json.read_text(encoding="utf-8")
    except (OSError, ValueError) as exc:
        logger.debug("read %s failed: %s", pkg_json, exc)
        return None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    version = payload.get("version")
    if isinstance(version, str) and version:
        return version
    return None


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

    binary = _BINARY_BY_CODER.get(coder)
    if binary is not None:
        prefix = _resolve_install_prefix(binary)
        if prefix is not None:
            version = _read_package_version(prefix, package)
            if version is not None:
                return version

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
