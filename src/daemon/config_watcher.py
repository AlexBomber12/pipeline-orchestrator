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

from src.keyspace import control_config_dirty

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
) -> bool:
    """Mark every named runner's ``config_dirty`` flag in Redis.

    Returns ``True`` when every write succeeded, ``False`` when any write
    raised. Callers use the return value to decide whether the watcher's
    baseline signature can be advanced; on a partial/total failure the
    signature stays put so the next poll retries the same change event.
    """
    all_ok = True
    for name in repo_names:
        key = control_config_dirty(name)
        try:
            await redis_client.set(key, "1")
        except Exception:
            all_ok = False
            logger.warning(
                "Failed to set %s; will retry on next poll",
                key,
                exc_info=True,
            )
    return all_ok


def _normalize_path(path: Path | str) -> str:
    """Return an absolute, resolved string form of ``path``.

    Used to compare watchfiles event paths against the configured target
    set without false negatives from relative-vs-absolute or symlink
    differences. Falls back to the unresolved string when ``resolve()``
    raises (e.g. the file vanished between event delivery and lookup),
    so a transient resolve error never blocks the event filter.
    """
    try:
        return str(Path(path).resolve())
    except OSError:
        return str(Path(path))


async def watch_config_changes(
    config_paths: list[Path],
    on_change_callback: Callable[[], None],
) -> None:
    """Fire ``on_change_callback`` on inotify events for any of ``config_paths``.

    Reduces config-edit-to-active latency from the 5-cycle poll worst case
    (~25s) to under 2 seconds: the kernel wakes the watcher the moment the
    file body lands. The callback is invoked at most once per inotify
    batch (a single rename + write atomic-replace fires many ``Change``
    entries; only the first relevant one matters here). Falls back to the
    existing poll-based reload by returning silently when:

    * ``watchfiles`` is not importable (CI environment without the dep, a
      platform without inotify support, etc.); or
    * none of the requested paths exist at startup (the daemon booted
      before ``config.yml`` was mounted).

    Callback exceptions are logged and swallowed so the watcher loop
    keeps monitoring — a broken reload path must not silently disable
    every future config edit detection.

    The watcher subscribes to each target file's *parent directory* and
    filters events back to the target paths instead of watching the
    files directly. Many editors and config writers use atomic save
    (write tmp + rename over the target), which replaces the original
    inode; a file-level inotify watch goes deaf after the first such
    save because the kernel watch is bound to the inode it opened, not
    the path. Watching the parent directory keeps subsequent edits
    visible.
    """
    try:
        from watchfiles import Change, awatch
    except ImportError:
        logger.info(
            "watchfiles unavailable; relying on poll-based config reload"
        )
        return

    existing_paths = [p for p in config_paths if p.exists()]
    if not existing_paths:
        logger.info(
            "No existing config paths to watch; inotify reload disabled"
        )
        return

    target_paths = {_normalize_path(p) for p in existing_paths}
    parent_dirs = sorted({_normalize_path(p.parent) for p in existing_paths})

    async for changes in awatch(*parent_dirs, recursive=False):
        for change_type, changed_path in changes:
            if change_type not in (Change.modified, Change.added):
                continue
            if _normalize_path(changed_path) not in target_paths:
                continue
            logger.info("Inotify config change detected: %s", changed_path)
            try:
                on_change_callback()
            except Exception:
                logger.exception("Config reload callback raised")
            break


async def watch_config_file_changes(
    redis_client: Any,
    get_repo_names: Callable[[], Iterable[str]],
    *,
    config_path: Path | None = None,
    interval_sec: float = CONFIG_WATCH_INTERVAL_SEC,
) -> None:
    """Poll ``config.yml`` for changes and flag runners on every edit.

    Loops until cancelled, sleeping ``interval_sec`` between checks. The
    watcher tolerates a missing or unreadable file at startup: it keeps
    polling and silently establishes the baseline as soon as the file
    becomes readable, so a daemon that boots before ``config.yml`` is
    present (or during a transient unreadable state) still picks up
    later edits at the polling cadence.
    """
    path = config_path if config_path is not None else _resolve_config_path()
    last_signature = _safe_signature(path)
    while True:
        await asyncio.sleep(interval_sec)
        current = _safe_signature(path)
        if current is None:
            continue
        if last_signature is None:
            # First successful read after a missing/unreadable startup —
            # silently baseline. Treating it as a "change" would spuriously
            # flag every runner on the first poll.
            last_signature = current
            continue
        if current == last_signature:
            continue
        names = list(get_repo_names())
        if not names:
            last_signature = current
            continue
        logger.info(
            "Detected config.yml change; flagging %d runner(s) for reload",
            len(names),
        )
        if await _set_config_dirty_flags(redis_client, names):
            last_signature = current
