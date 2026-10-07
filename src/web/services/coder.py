"""Coder selection helpers shared across web route modules.

Lives in ``services`` so route modules can depend on it without importing
each other: ``dashboard`` and ``repo_control`` both need to know which
coder a repo currently resolves to, and routing the helper through
``services`` keeps those router modules independent.
"""

from __future__ import annotations

from collections.abc import Collection

from src.coder_ids import validate_coder_plugin_id
from src.coder_registry import CoderPlugin, CoderRegistry
from src.config import AppConfig, RepoConfig


def _selectable_coder_plugins(registry: CoderRegistry) -> list[CoderPlugin]:
    """Return startup-loaded plugins that expose usable metadata."""
    return [
        plugin
        for plugin in registry.list_coders()
        if getattr(plugin, "metadata_available", True)
    ]


def _coder_display_name(coder: str, registry: CoderRegistry) -> str:
    """Return a registry display name or an explicit unavailable label."""
    plugin = registry.get_optional(coder)
    if plugin is None:
        return f"{coder} (unavailable)"
    if not getattr(plugin, "metadata_available", True):
        return f"{plugin.name} (unavailable)"
    return plugin.display_name


def _validate_coder_selection(
    value: str,
    registry: CoderRegistry,
    *,
    inherit_values: Collection[str] = (),
) -> str | None:
    """Resolve inheritance or require a usable startup-loaded plugin ID."""
    if value in inherit_values:
        return None
    plugin_id = validate_coder_plugin_id(value)
    plugin = registry.get_optional(plugin_id)
    if plugin is None:
        raise ValueError(f"coder plugin is not registered: {plugin_id}")
    if not getattr(plugin, "metadata_available", True):
        raise ValueError(f"coder plugin is unavailable: {plugin_id}")
    return plugin_id


def _effective_coder_name(
    repo_config: RepoConfig | None, config: AppConfig
) -> str:
    """Return the effective coder name for a repo."""
    if repo_config is not None and repo_config.coder is not None:
        return repo_config.coder
    return config.daemon.coder
