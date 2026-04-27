"""Polling watcher for ``config.yml`` edits made outside the web UI.

The web Settings endpoint already sets a ``control:<repo>:config_dirty`` key
in Redis on save, which the runner consumes at its next IDLE boundary via
:meth:`PipelineRunner.reload_repo_config_if_dirty`. Manual edits to
``config.yml`` (vim, ``scp``, ``docker cp``) bypass that endpoint, so the
daemon would otherwise keep using its in-memory config until the process
restarts.

This module restores the natural mental model — *"I edited the file, so
the config is updated"* — by polling ``config.yml`` for changes and
flagging the same dirty key for every active runner when a content change
is detected. The signature mixes ``mtime`` and size with a sha256 of the
file body so a no-op ``touch`` (or atomic-write rename) does not produce
spurious reloads.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

CONFIG_WATCH_INTERVAL_SEC = 5.0


def _resolve_config_path() -> Path:
    return Path(os.environ.get("PO_CONFIG_PATH", "config.yml"))


def _compute_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _config_signature(path: Path) -> str:
    """Return the sha256 hex digest of the file body.

    A pure content hash protects against false positives from atomic-write
    tools (and ``touch``) that bump ``mtime`` without changing the body.
    SHA on a small config file is sub-millisecond, so ``mtime``/size
    short-circuiting buys very little; clarity wins.
    """
    return _compute_sha256(path)


def _safe_signature(path: Path) -> str | None:
    """Return the signature or ``None`` if the file is missing/unreadable."""
    try:
        if not path.exists():
            return None
        return _config_signature(path)
    except OSError:
        return None


async def _set_config_dirty_flags(
    redis_client: Any, repo_names: Iterable[str]
) -> None:
    """Mark every named runner's ``config_dirty`` flag in Redis."""
    for name in repo_names:
        key = f"control:{name}:config_dirty"
        try:
            await redis_client.set(key, "1")
        except Exception:
            logger.warning(
                "Failed to set %s; runner will not auto-reload this cycle",
                key,
                exc_info=True,
            )


async def watch_config_file_changes(
    redis_client: Any,
    get_repo_names: Callable[[], Iterable[str]],
    *,
    config_path: Path | None = None,
    interval_sec: float = CONFIG_WATCH_INTERVAL_SEC,
) -> None:
    """Poll ``config.yml`` for changes and flag runners on every edit.

    Returns silently if the configured path does not exist on startup
    (e.g. test environments without a config file). Otherwise loops
    until cancelled, sleeping ``interval_sec`` between checks.
    """
    path = config_path if config_path is not None else _resolve_config_path()
    last_signature = _safe_signature(path)
    if last_signature is None:
        return
    while True:
        await asyncio.sleep(interval_sec)
        current = _safe_signature(path)
        if current is None or current == last_signature:
            continue
        last_signature = current
        names = list(get_repo_names())
        if not names:
            continue
        logger.info(
            "Detected config.yml change; flagging %d runner(s) for reload",
            len(names),
        )
        await _set_config_dirty_flags(redis_client, names)
