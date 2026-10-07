"""Auth status probes for the dashboard's coder/gh credential indicators.

Each probe spawns a CLI subprocess (``claude --version``, ``codex login
status``, ``gh auth status``) with a short timeout, so the dashboard can
surface a green/red dot per coder without blocking the event loop. Probes
read ``CONFIG_PATH`` lazily from :mod:`src.web.app` so test overrides
``monkeypatch.setattr(web_app, "CONFIG_PATH", ...)`` continue to apply.
"""

from __future__ import annotations

import asyncio
import inspect
import os
import subprocess
from typing import Any

from src.coder_registry import (
    CoderAuthCapabilities,
    CoderAuthStatus,
    CoderRegistry,
    coder_auth_payload,
)
from src.coders import build_coder_registry
from src.config import DEFAULT_CODER_PLUGINS, load_config

_AUTH_CHECK_TIMEOUT_SEC = 5

_AUTH_STATUS_CACHE: dict[str, dict[str, Any]] | None = None


def _auth_error(detail: str, failure_reason: str) -> dict[str, Any]:
    return coder_auth_payload(
        CoderAuthStatus(
            status="error",
            detail=detail,
            failure_reason=failure_reason,
        )
    )


def _default_auth_status() -> dict[str, dict[str, Any]]:
    """Return placeholder auth status entries when no cached probe exists."""
    unavailable = _auth_error("Status unavailable", "probe_unavailable")
    return {
        "claude": dict(unavailable),
        "codex": dict(unavailable),
        "gh": dict(unavailable),
    }


def _get_cached_auth_status() -> dict[str, dict[str, Any]]:
    """Return the last collected auth status, if available."""
    source = _AUTH_STATUS_CACHE or _default_auth_status()
    return {key: dict(value) for key, value in source.items()}


def _run_auth_command(
    cmd: list[str], env: dict[str, str] | None = None
) -> tuple[int, str, str]:
    """Run ``cmd`` for an auth status probe and return (rc, stdout, stderr).

    Any failure to spawn (``FileNotFoundError``, ``PermissionError``) or
    the subprocess exceeding ``_AUTH_CHECK_TIMEOUT_SEC`` is reported as a
    non-zero return code so the caller can render a red status dot without
    crashing the request.
    """
    try:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=_AUTH_CHECK_TIMEOUT_SEC,
            check=False,
            env=env,
        )
    except FileNotFoundError:
        return 127, "", f"{cmd[0]} not found"
    except PermissionError as exc:
        return 126, "", str(exc)
    except subprocess.TimeoutExpired:
        return 124, "", f"{cmd[0]} timed out after {_AUTH_CHECK_TIMEOUT_SEC}s"
    return completed.returncode, completed.stdout or "", completed.stderr or ""


def _auth_probe_env(**overrides: str) -> dict[str, str]:
    """Return the environment block used for an auth CLI probe.

    ``docker-compose.yml`` only sets ``CLAUDE_CONFIG_DIR`` / ``GH_CONFIG_DIR``
    on the ``daemon`` service; the ``web`` service inherits none of them and
    would otherwise probe the wrong credential location (the web container's
    home directory, not ``/data/auth``). Reading the paths from ``config.yml``
    and injecting them into the subprocess environment keeps the dashboard
    in lock-step with whatever auth context the daemon was built to use, so
    "Authorized" on the dashboard matches "the daemon can actually run".
    """
    env = os.environ.copy()
    env.update(overrides)
    return env


def _first_probe_line(text: str) -> str:
    """Return the first meaningful line from CLI probe output."""
    for line in text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.lower().startswith("warning:"):
            return stripped
    return ""


def _config_path() -> str:
    """Return the active web app config path, resolved lazily.

    Imports :mod:`src.web.app` on demand so the auth probe service can be
    imported during app startup without participating in the circular
    dependency between ``app.py`` and the route submodules.
    """
    from src.web import app as _app

    return _app.CONFIG_PATH


def _check_coder_auth(
    registry: CoderRegistry,
    plugin_id: str,
) -> dict[str, Any]:
    """Probe one startup-registered coder in the active config context."""
    plugin = registry.get(plugin_id)
    try:
        check_auth = plugin.check_auth
        kwargs = (
            {"config_path": _config_path()}
            if "config_path" in inspect.signature(check_auth).parameters
            else {}
        )
        result = check_auth(**kwargs)
        capabilities = getattr(plugin, "auth_capabilities", None)
        if capabilities is not None and not isinstance(
            capabilities, CoderAuthCapabilities
        ):
            raise TypeError("invalid auth capabilities")
        return coder_auth_payload(result, capabilities=capabilities)
    except Exception as exc:
        return _auth_error(
            (
                f"{plugin.display_name} auth check failed "
                f"({type(exc).__name__})"
            ),
            "probe_failed",
        )


def _check_claude_auth(
    registry: CoderRegistry | None = None,
) -> dict[str, Any]:
    """Probe the ``claude`` CLI and report its authorization status."""
    return _check_coder_auth(registry or build_coder_registry(), "claude")


def _check_codex_auth(
    registry: CoderRegistry | None = None,
) -> dict[str, Any]:
    """Probe the ``codex`` CLI and report its authorization status."""
    return _check_coder_auth(registry or build_coder_registry(), "codex")


async def _bounded_coder_auth_probe(
    registry: CoderRegistry,
    plugin_id: str,
) -> dict[str, Any]:
    """Run one plugin probe with a deadline that also ends its worker."""
    plugin = registry.get(plugin_id)
    reference = registry.reference_for(plugin_id)
    if reference is None:
        return _auth_error(
            f"{plugin.display_name} auth check is unavailable",
            "probe_unavailable",
        )
    if reference != DEFAULT_CODER_PLUGINS.get(plugin_id):
        return await _daemon_coder_auth_probe(
            plugin_id,
            reference,
            plugin.display_name,
        )
    probe = _check_claude_auth if plugin_id == "claude" else _check_codex_auth
    return await asyncio.to_thread(probe, registry)


async def _daemon_coder_auth_probe(
    plugin_id: str,
    reference: str,
    display_name: str,
) -> dict[str, Any]:
    """Ask the daemon to own a configured plugin auth probe."""
    from src.web import app as _app

    bridge = getattr(_app.app.state, "plugin_bridge", None)
    if bridge is None:
        return _auth_error(
            f"{display_name} auth check is unavailable",
            "daemon_unavailable",
        )
    try:
        return await bridge.load_auth_status(
            plugin_id,
            expected_reference=reference,
        )
    except Exception:
        return _auth_error(
            f"{display_name} auth check is unavailable from daemon",
            "daemon_unavailable",
        )


def _check_gh_auth() -> dict[str, str]:
    """Probe the ``gh`` CLI and report its authorization status."""
    cfg = load_config(_config_path())
    env = _auth_probe_env(GH_CONFIG_DIR=cfg.auth.gh_config_dir)
    rc, stdout, stderr = _run_auth_command(
        ["gh", "auth", "status"], env=env
    )
    # ``gh auth status`` prints its report to stderr on recent versions and
    # to stdout on older ones, so merge both streams before scanning.
    combined = f"{stdout}\n{stderr}".strip()
    if rc == 0 and "Logged in" in combined:
        detail = ""
        for line in combined.splitlines():
            stripped = line.strip()
            if "Logged in" in stripped:
                detail = stripped
                break
        return {"status": "ok", "detail": detail or "Logged in"}
    if combined:
        detail = combined.splitlines()[0].strip()
    else:
        detail = "gh CLI not configured"
    return {"status": "error", "detail": detail}


async def _collect_auth_status(
    registry: CoderRegistry | None = None,
) -> dict[str, dict[str, Any]]:
    """Return auth status for every registered coder plus GitHub CLI.

    Each probe invokes a blocking ``subprocess.run`` call with a 5s
    timeout, so they would block the event loop if awaited directly from
    an async handler. Dispatching them through ``asyncio.to_thread`` and
    ``asyncio.gather`` moves the blocking work onto the default thread
    pool and runs probes concurrently, so one slow or missing CLI does not
    serially delay the remaining registered plugins and infrastructure probe.
    """
    active_registry = registry or build_coder_registry()
    plugin_ids = active_registry.coder_names()
    probes = [
        _bounded_coder_auth_probe(active_registry, plugin_id)
        for plugin_id in plugin_ids
    ]
    results = await asyncio.gather(*probes, asyncio.to_thread(_check_gh_auth))
    global _AUTH_STATUS_CACHE
    _AUTH_STATUS_CACHE = dict(zip((*plugin_ids, "gh"), results, strict=True))
    return _get_cached_auth_status()


def auth_status_view(entry: dict[str, Any]) -> dict[str, str]:
    """Return generic presentation metadata for one auth contract."""
    if entry.get("service_access_verified") is True:
        return {"label": "Access verified", "tone": "ok"}
    if entry.get("service_access_verified") is False:
        return {"label": "Access failed", "tone": "fail"}
    if entry.get("saved_credentials_present") is False:
        return {"label": "Credentials missing", "tone": "fail"}
    if entry.get("cli_available") is False:
        return {"label": "CLI unavailable", "tone": "fail"}
    if entry.get("failure_reason") in {
        "daemon_unavailable",
        "probe_timeout",
        "probe_unavailable",
    }:
        return {"label": "Status unavailable", "tone": "warn"}
    if entry.get("status") == "error":
        return {"label": "Error", "tone": "fail"}
    if entry.get("saved_credentials_present") is True:
        return {"label": "Credentials saved", "tone": "warn"}
    if entry.get("status") == "ok":
        return {"label": "Available", "tone": "ok"}
    return {"label": "Error", "tone": "fail"}
