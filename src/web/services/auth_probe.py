"""Auth status probes for the dashboard's coder/gh credential indicators.

Each probe spawns a CLI subprocess (``claude --version``, ``codex auth
status``, ``gh auth status``) with a short timeout, so the dashboard can
surface a green/red dot per coder without blocking the event loop. Probes
read ``CONFIG_PATH`` lazily from :mod:`src.web.app` so test overrides
``monkeypatch.setattr(web_app, "CONFIG_PATH", ...)`` continue to apply.
"""

from __future__ import annotations

import asyncio
import os
import subprocess

from src.coders import build_coder_registry
from src.config import load_config

_AUTH_CHECK_TIMEOUT_SEC = 5

_AUTH_STATUS_CACHE: dict[str, dict[str, str]] | None = None


def _default_auth_status() -> dict[str, dict[str, str]]:
    """Return placeholder auth status entries when no cached probe exists."""
    unavailable = {"status": "error", "detail": "Status unavailable"}
    return {
        "claude": dict(unavailable),
        "codex": dict(unavailable),
        "gh": dict(unavailable),
    }


def _get_cached_auth_status() -> dict[str, dict[str, str]]:
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


def _check_claude_auth() -> dict[str, str]:
    """Probe the ``claude`` CLI and report its authorization status."""
    return build_coder_registry().get("claude").check_auth(
        config_path=_config_path()
    )


def _check_codex_auth() -> dict[str, str]:
    """Probe the ``codex`` CLI and report its authorization status."""
    return build_coder_registry().get("codex").check_auth(
        config_path=_config_path()
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


async def _collect_auth_status() -> dict[str, dict[str, str]]:
    """Return ``{"claude": ..., "codex": ..., "gh": ...}`` auth status dicts.

    Each probe invokes a blocking ``subprocess.run`` call with a 5s
    timeout, so they would block the event loop if awaited directly from
    an async handler. Dispatching them through ``asyncio.to_thread`` and
    ``asyncio.gather`` moves the blocking work onto the default thread
    pool and runs probes concurrently, so the dashboard's 30s HTMX
    auth-status poll cannot stall the worker for up to ~15s (three serial
    5s timeouts) whenever a CLI is missing or slow.
    """
    claude, codex, gh = await asyncio.gather(
        asyncio.to_thread(_check_claude_auth),
        asyncio.to_thread(_check_codex_auth),
        asyncio.to_thread(_check_gh_auth),
    )
    global _AUTH_STATUS_CACHE
    _AUTH_STATUS_CACHE = {"claude": claude, "codex": codex, "gh": gh}
    return _get_cached_auth_status()
