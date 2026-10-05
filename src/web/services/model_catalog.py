"""Application-scoped cache for plugin-owned model catalogs."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Hashable

from src.coder_registry import CoderPlugin, ModelCatalog, ModelMetadata
from src.config import AppConfig

logger = logging.getLogger(__name__)

_CATALOG_TTL_SECONDS = 30.0
_NOT_LOADED_MESSAGE = "Model catalog has not been loaded yet."
_EMPTY_MESSAGE = "The plugin supplied no usable models."
_UNAVAILABLE_MESSAGE = "Model catalog is unavailable."
_STALE_MESSAGE = "Refresh failed; showing last-known models."
_EXPIRED_MESSAGE = "Showing last-known models while a refresh is due."


@dataclass(frozen=True)
class ModelCatalogSnapshot:
    models: tuple[ModelMetadata, ...] = ()
    status: str = "not_loaded"
    message: str = _NOT_LOADED_MESSAGE
    source: str | None = None
    refreshable: bool = False
    refreshed_at: str | None = None
    attempted_at: str | None = None

    @property
    def has_usable_models(self) -> bool:
        return bool(self.models)


@dataclass(frozen=True)
class _CatalogKey:
    plugin_name: str
    auth_context: Hashable


@dataclass(frozen=True)
class _CacheEntry:
    snapshot: ModelCatalogSnapshot
    expires_at: float


class ModelCatalogCache:
    """Short-lived catalogs coalesced per plugin and auth context."""

    def __init__(
        self,
        *,
        ttl_seconds: float = _CATALOG_TTL_SECONDS,
        loader: Callable[..., Awaitable[ModelCatalog]] | None = None,
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError("catalog TTL must be positive")
        self._ttl_seconds = ttl_seconds
        self._loader = loader
        self._entries: dict[_CatalogKey, _CacheEntry] = {}
        self._in_flight: dict[
            _CatalogKey, asyncio.Task[ModelCatalogSnapshot]
        ] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _key(
        plugin: CoderPlugin, *, config: AppConfig, config_path: str
    ) -> _CatalogKey:
        return _CatalogKey(
            plugin.name,
            plugin.model_catalog_cache_key(
                config=config,
                config_path=config_path,
            ),
        )

    async def get(
        self,
        plugin: CoderPlugin,
        *,
        config: AppConfig,
        config_path: str,
        refresh: bool = False,
    ) -> ModelCatalogSnapshot:
        """Return a fresh snapshot, coalescing concurrent plugin calls."""
        key = self._key(plugin, config=config, config_path=config_path)
        async with self._lock:
            entry = self._entries.get(key)
            if (
                not refresh
                and entry is not None
                and entry.expires_at > time.monotonic()
            ):
                return entry.snapshot
            task = self._in_flight.get(key)
            if task is None:
                task = asyncio.create_task(
                    self._refresh(
                        key,
                        plugin,
                        config=config,
                        config_path=config_path,
                    )
                )
                self._in_flight[key] = task
        return await asyncio.shield(task)

    def peek(
        self,
        plugin: CoderPlugin,
        *,
        config: AppConfig,
        config_path: str,
    ) -> ModelCatalogSnapshot:
        """Return cached metadata without starting a plugin operation."""
        key = self._key(plugin, config=config, config_path=config_path)
        entry = self._entries.get(key)
        if entry is None:
            return ModelCatalogSnapshot(
                refreshable=plugin.model_catalog_refreshable
            )
        if (
            entry.expires_at > time.monotonic()
            or entry.snapshot.status not in {"available", "empty"}
        ):
            return entry.snapshot
        return replace(
            entry.snapshot,
            status="stale" if entry.snapshot.models else "empty",
            message=(
                _EXPIRED_MESSAGE if entry.snapshot.models else _EMPTY_MESSAGE
            ),
        )

    async def _refresh(
        self,
        key: _CatalogKey,
        plugin: CoderPlugin,
        *,
        config: AppConfig,
        config_path: str,
    ) -> ModelCatalogSnapshot:
        attempted_at = datetime.now(timezone.utc).isoformat()
        try:
            try:
                # Non-refreshable catalogs are explicit plugin metadata, so
                # keep them available in the web control plane even while the
                # daemon is offline.  Only refreshable discovery crosses the
                # daemon bridge, where coder subprocess ownership belongs.
                if self._loader is None or not plugin.model_catalog_refreshable:
                    catalog = await plugin.get_model_catalog(
                        config=config,
                        config_path=config_path,
                    )
                else:
                    catalog = await self._loader(
                        plugin,
                        config=config,
                        config_path=config_path,
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "%s model catalog failed", plugin.name, exc_info=True
                )
                previous = self._entries.get(key)
                if previous is not None and previous.snapshot.models:
                    snapshot = replace(
                        previous.snapshot,
                        status="stale",
                        message=_STALE_MESSAGE,
                        attempted_at=attempted_at,
                    )
                else:
                    snapshot = ModelCatalogSnapshot(
                        status="unavailable",
                        message=_UNAVAILABLE_MESSAGE,
                        refreshable=plugin.model_catalog_refreshable,
                        attempted_at=attempted_at,
                    )
            else:
                snapshot = ModelCatalogSnapshot(
                    models=catalog.models,
                    status="available" if catalog.models else "empty",
                    message=catalog.description,
                    source=catalog.source,
                    refreshable=plugin.model_catalog_refreshable,
                    refreshed_at=attempted_at,
                    attempted_at=attempted_at,
                )
            async with self._lock:
                self._entries[key] = _CacheEntry(
                    snapshot=snapshot,
                    expires_at=time.monotonic() + self._ttl_seconds,
                )
            return snapshot
        finally:
            async with self._lock:
                current_task = asyncio.current_task()
                if self._in_flight.get(key) is current_task:
                    self._in_flight.pop(key, None)

    async def close(self) -> None:
        """Cancel catalog operations owned by a shutting-down application."""
        async with self._lock:
            tasks = tuple(self._in_flight.values())
            self._in_flight.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
