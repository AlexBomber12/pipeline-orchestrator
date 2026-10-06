"""Configuration-driven coder plugin registry construction."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Mapping
from typing import Any

from src.coder_ids import CODER_PLUGIN_ID_PATTERN
from src.coder_registry import CoderPlugin, CoderRegistry, ModelSetting
from src.config import DEFAULT_CODER_PLUGINS, AppConfig, DaemonConfig

_RESERVED_PLUGIN_IDS = frozenset({"gh"})
_RESERVED_MODEL_SETTING_KEYS = frozenset({"reasoning_effort"})
_LEGACY_MODEL_FIELDS = {
    "claude": "claude_model",
    "codex": "codex_model",
}


class CoderPluginConfigurationError(ValueError):
    """A configured coder plugin could not be loaded safely."""


def _configuration_error(
    plugin_id: str,
    reference: object,
    stage: str,
    detail: str,
) -> CoderPluginConfigurationError:
    return CoderPluginConfigurationError(
        f"Coder plugin {plugin_id!r} reference {reference!r} failed at "
        f"{stage}: {detail}"
    )


def _parse_reference(plugin_id: str, reference: object) -> tuple[str, str]:
    if not isinstance(reference, str):
        raise _configuration_error(
            plugin_id,
            reference,
            "reference",
            "expected a module:factory string",
        )
    if reference.count(":") != 1:
        raise _configuration_error(
            plugin_id,
            reference,
            "reference",
            "expected exactly one ':' separator",
        )
    module_name, factory_name = reference.split(":", 1)
    if not module_name or not factory_name:
        raise _configuration_error(
            plugin_id,
            reference,
            "reference",
            "module and factory names must be non-empty",
        )
    return module_name, factory_name


def _load_factory(
    plugin_id: str,
    reference: str,
    module_name: str,
    factory_name: str,
) -> Callable[[], Any]:
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        raise _configuration_error(
            plugin_id,
            reference,
            "module import",
            type(exc).__name__,
        ) from None

    try:
        factory = getattr(module, factory_name)
    except Exception as exc:
        detail = (
            "factory was not found"
            if isinstance(exc, AttributeError)
            else type(exc).__name__
        )
        raise _configuration_error(
            plugin_id,
            reference,
            "factory lookup",
            detail,
        ) from None
    if not callable(factory):
        raise _configuration_error(
            plugin_id,
            reference,
            "factory validation",
            "resolved object is not callable",
        )
    return factory


def _validate_plugin_metadata(
    plugin_id: str,
    reference: str,
    plugin: CoderPlugin,
) -> None:
    try:
        name = plugin.name
        display_name = plugin.display_name
        models = plugin.models
        model_setting = plugin.model_setting
        refreshable = plugin.model_catalog_refreshable
    except Exception as exc:
        raise _configuration_error(
            plugin_id,
            reference,
            "metadata validation",
            f"metadata access raised {type(exc).__name__}",
        ) from None

    if not isinstance(name, str) or not name:
        raise _configuration_error(
            plugin_id,
            reference,
            "metadata validation",
            "plugin.name must be a non-empty string",
        )
    if name != plugin_id:
        raise _configuration_error(
            plugin_id,
            reference,
            "identity validation",
            f"factory declared plugin.name {name!r}",
        )
    if not isinstance(display_name, str) or not display_name:
        raise _configuration_error(
            plugin_id,
            reference,
            "metadata validation",
            "plugin.display_name must be a non-empty string",
        )
    if not isinstance(models, list) or not all(
        isinstance(model, str) for model in models
    ):
        raise _configuration_error(
            plugin_id,
            reference,
            "metadata validation",
            "plugin.models must be a list of strings",
        )
    if not isinstance(model_setting, ModelSetting):
        raise _configuration_error(
            plugin_id,
            reference,
            "metadata validation",
            "plugin.model_setting must be ModelSetting metadata",
        )
    legacy_field = model_setting.config_field
    if legacy_field is not None and legacy_field != _LEGACY_MODEL_FIELDS.get(
        plugin_id
    ):
        raise _configuration_error(
            plugin_id,
            reference,
            "metadata validation",
            "plugin.model_setting.config_field must be None or the legacy "
            "model field owned by this built-in plugin ID",
        )
    if not isinstance(model_setting.default_value, str):
        raise _configuration_error(
            plugin_id,
            reference,
            "metadata validation",
            "plugin.model_setting.default_value must be a string",
        )
    if (
        not isinstance(model_setting.default_label, str)
        or not model_setting.default_label
    ):
        raise _configuration_error(
            plugin_id,
            reference,
            "metadata validation",
            "plugin.model_setting.default_label must be a non-empty string",
        )
    if (
        not isinstance(model_setting.setting_key, str)
        or not model_setting.setting_key
        or "." in model_setting.setting_key
    ):
        raise _configuration_error(
            plugin_id,
            reference,
            "metadata validation",
            "plugin.model_setting.setting_key must be a non-empty, dot-free string",
        )
    if model_setting.setting_key in _RESERVED_MODEL_SETTING_KEYS:
        raise _configuration_error(
            plugin_id,
            reference,
            "metadata validation",
            f"plugin.model_setting.setting_key {model_setting.setting_key!r} "
            "is reserved for plugin options",
        )
    if not isinstance(refreshable, bool):
        raise _configuration_error(
            plugin_id,
            reference,
            "metadata validation",
            "plugin.model_catalog_refreshable must be a boolean",
        )


def _load_plugin(plugin_id: str, reference: object) -> CoderPlugin:
    if not isinstance(plugin_id, str) or not CODER_PLUGIN_ID_PATTERN.fullmatch(plugin_id):
        raise _configuration_error(
            str(plugin_id),
            reference,
            "plugin ID validation",
            "expected an ASCII letter/digit slug using only letters, digits, "
            "underscores, and hyphens",
        )
    if plugin_id in _RESERVED_PLUGIN_IDS:
        raise _configuration_error(
            plugin_id,
            reference,
            "plugin ID validation",
            "ID is reserved for infrastructure status",
        )
    module_name, factory_name = _parse_reference(plugin_id, reference)
    assert isinstance(reference, str)
    factory = _load_factory(plugin_id, reference, module_name, factory_name)
    try:
        plugin = factory()
    except Exception as exc:
        raise _configuration_error(
            plugin_id,
            reference,
            "factory invocation",
            f"factory raised {type(exc).__name__}",
        ) from None
    try:
        compatible = isinstance(plugin, CoderPlugin)
    except Exception as exc:
        raise _configuration_error(
            plugin_id,
            reference,
            "contract validation",
            f"contract inspection raised {type(exc).__name__}",
        ) from None
    if not compatible:
        raise _configuration_error(
            plugin_id,
            reference,
            "contract validation",
            "factory result does not implement CoderPlugin",
        )
    _validate_plugin_metadata(plugin_id, reference, plugin)
    return plugin


def _validate_configured_model_setting(
    plugin_id: str,
    reference: str,
    plugin: CoderPlugin,
    daemon_config: DaemonConfig,
) -> None:
    """Reject values the plugin's shared model resolver cannot consume."""
    plugin_settings = daemon_config.coder_settings.get(plugin_id)
    if plugin_settings is None:
        return
    setting_key = plugin.model_setting.setting_key
    if setting_key in plugin_settings and not isinstance(
        plugin_settings[setting_key], str
    ):
        raise _configuration_error(
            plugin_id,
            reference,
            "model setting validation",
            f"daemon.coder_settings.{plugin_id}.{setting_key} must be a string",
        )


def build_coder_registry(config: AppConfig | None = None) -> CoderRegistry:
    """Build a registry from trusted ``module:factory`` references.

    With no configuration, the compatibility defaults load Claude and Codex.
    ``AppConfig`` merges explicit definitions over those defaults, so built-ins
    and operator-configured plugins always use this same import path.
    """
    references: Mapping[str, str] = (
        config.coder_plugins if config is not None else DEFAULT_CODER_PLUGINS
    )
    registry = CoderRegistry()
    for plugin_id, reference in references.items():
        plugin = _load_plugin(plugin_id, reference)
        if config is not None:
            _validate_configured_model_setting(
                plugin_id,
                reference,
                plugin,
                config.daemon,
            )
        registry.register(plugin, reference=reference)
    return registry
