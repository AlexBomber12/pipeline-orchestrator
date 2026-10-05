"""Application-scoped cache for Codex CLI model discovery."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Awaitable, Callable

from src.coders.codex_models import CodexModel, discover_codex_models
from src.config import AppConfig

logger = logging.getLogger(__name__)

_CATALOG_TTL_SECONDS = 30.0
_NOT_LOADED_MESSAGE = "Codex model catalog has not been loaded yet."
_EMPTY_MESSAGE = "Codex CLI advertised no usable models."
_UNAVAILABLE_MESSAGE = "Codex model discovery is unavailable."
_STALE_MESSAGE = "Refresh failed; showing last-known Codex models."
_EXPIRED_MESSAGE = "Showing last-known Codex models while a refresh is due."


@dataclass(frozen=True)
class CodexModelDiscoveryContext:
    """Cache key and subprocess context for one effective Codex setup."""

    home_dir: str
    working_directory: str

    @classmethod
    def from_config(
        cls, config: AppConfig, *, config_path: str
    ) -> CodexModelDiscoveryContext:
        return cls(
            home_dir=config.auth.codex_home_dir,
            working_directory=str(Path(config_path).absolute().parent),
        )

    def subprocess_env(self) -> dict[str, str]:
        env = dict(os.environ)
        env["HOME"] = self.home_dir
        # Discovery must use the installed CLI's authenticated session, not an
        # API billing credential inherited by the web process.
        env.pop("OPENAI_API_KEY", None)
        return env


@dataclass(frozen=True)
class CodexModelCatalogSnapshot:
    models: tuple[CodexModel, ...] = ()
    status: str = "not_loaded"
    message: str = _NOT_LOADED_MESSAGE
    refreshed_at: str | None = None
    attempted_at: str | None = None

    @property
    def has_usable_models(self) -> bool:
        return bool(self.models)


@dataclass(frozen=True)
class _CacheEntry:
    snapshot: CodexModelCatalogSnapshot
    expires_at: float


DiscoveryCallable = Callable[..., Awaitable[tuple[CodexModel, ...]]]


class CodexModelCatalogCache:
    """Short-lived catalog cache with one discovery task per context."""

    def __init__(
        self,
        *,
        ttl_seconds: float = _CATALOG_TTL_SECONDS,
        discover: DiscoveryCallable | None = None,
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError("catalog TTL must be positive")
        self._ttl_seconds = ttl_seconds
        self._discover = discover or discover_codex_models
        self._entries: dict[CodexModelDiscoveryContext, _CacheEntry] = {}
        self._in_flight: dict[
            CodexModelDiscoveryContext, asyncio.Task[CodexModelCatalogSnapshot]
        ] = {}
        self._lock = asyncio.Lock()

    async def get(
        self,
        context: CodexModelDiscoveryContext,
        *,
        refresh: bool = False,
    ) -> CodexModelCatalogSnapshot:
        """Return a fresh snapshot, coalescing concurrent discovery calls."""
        async with self._lock:
            entry = self._entries.get(context)
            if not refresh and entry is not None and entry.expires_at > time.monotonic():
                return entry.snapshot
            task = self._in_flight.get(context)
            if task is None:
                task = asyncio.create_task(self._refresh(context))
                self._in_flight[context] = task
        return await asyncio.shield(task)

    def peek(
        self, context: CodexModelDiscoveryContext
    ) -> CodexModelCatalogSnapshot:
        """Return cached metadata without starting a subprocess."""
        entry = self._entries.get(context)
        if entry is None:
            return CodexModelCatalogSnapshot()
        if entry.expires_at > time.monotonic() or entry.snapshot.status not in {
            "available",
            "empty",
        }:
            return entry.snapshot
        return replace(
            entry.snapshot,
            status="stale" if entry.snapshot.models else "empty",
            message=_EXPIRED_MESSAGE if entry.snapshot.models else _EMPTY_MESSAGE,
        )

    async def _refresh(
        self, context: CodexModelDiscoveryContext
    ) -> CodexModelCatalogSnapshot:
        attempted_at = datetime.now(timezone.utc).isoformat()
        try:
            models = await self._discover(
                env=context.subprocess_env(),
                cwd=context.working_directory,
            )
        except Exception:
            logger.warning("Codex model discovery failed", exc_info=True)
            previous = self._entries.get(context)
            if previous is not None and previous.snapshot.models:
                snapshot = replace(
                    previous.snapshot,
                    status="stale",
                    message=_STALE_MESSAGE,
                    attempted_at=attempted_at,
                )
            else:
                snapshot = CodexModelCatalogSnapshot(
                    status="unavailable",
                    message=_UNAVAILABLE_MESSAGE,
                    attempted_at=attempted_at,
                )
        else:
            snapshot = CodexModelCatalogSnapshot(
                models=models,
                status="available" if models else "empty",
                message=(
                    f"{len(models)} Codex model{'s' if len(models) != 1 else ''} loaded."
                    if models
                    else _EMPTY_MESSAGE
                ),
                refreshed_at=attempted_at,
                attempted_at=attempted_at,
            )
        async with self._lock:
            self._entries[context] = _CacheEntry(
                snapshot=snapshot,
                expires_at=time.monotonic() + self._ttl_seconds,
            )
            current_task = asyncio.current_task()
            if self._in_flight.get(context) is current_task:
                self._in_flight.pop(context, None)
        return snapshot

    async def close(self) -> None:
        """Cancel discovery owned by an application that is shutting down."""
        async with self._lock:
            tasks = tuple(self._in_flight.values())
            self._in_flight.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
