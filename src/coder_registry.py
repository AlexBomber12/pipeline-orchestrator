"""Coder plugin protocol and registry."""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Hashable, Protocol, runtime_checkable

from src.process_supervisor import SupervisedProcess
from src.usage import UsageProvider

if TYPE_CHECKING:
    from src.config import AppConfig, DaemonConfig


@dataclass(frozen=True)
class ModelReasoningEffort:
    """Reasoning-effort metadata advertised for one model."""

    name: str
    description: str | None = None


@dataclass(frozen=True)
class ModelMetadata:
    """Provider-neutral metadata for one invokable model."""

    invocation_id: str
    display_name: str
    is_default: bool = False
    default_reasoning_effort: str | None = None
    reasoning_efforts: tuple[ModelReasoningEffort, ...] = ()


@dataclass(frozen=True)
class ModelCatalog:
    """A plugin-owned model catalog normalized for shared consumers."""

    models: tuple[ModelMetadata, ...]
    source: str
    description: str


@dataclass(frozen=True)
class ModelSetting:
    """Plugin-owned binding for a model setting.

    New values live under ``daemon.coder_settings.<plugin_id>.<setting_key>``.
    ``config_field`` is an optional legacy fallback/input name; keeping that
    mapping in plugin metadata lets shared consumers stay provider-neutral.
    """

    config_field: str | None
    default_value: str
    default_label: str
    setting_key: str = "model"

    def control_name(self, plugin_id: str) -> str:
        """Return the generic Settings form field for ``plugin_id``."""
        return f"coder_settings.{plugin_id}.{self.setting_key}"

    def resolve(self, plugin_id: str, daemon_config: "DaemonConfig") -> str:
        """Resolve generic value, legacy fallback, then plugin default."""
        plugin_settings = daemon_config.coder_settings.get(plugin_id)
        if plugin_settings is not None and self.setting_key in plugin_settings:
            value = plugin_settings[self.setting_key]
            if not isinstance(value, str):
                raise ValueError(
                    f"daemon.coder_settings.{plugin_id}.{self.setting_key} "
                    "must be a string"
                )
            return value
        if self.config_field is not None:
            return str(getattr(daemon_config, self.config_field))
        return self.default_value


class ModelCatalogUnavailable(RuntimeError):
    """A plugin could not provide a usable model catalog."""


@runtime_checkable
class CoderPlugin(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def display_name(self) -> str: ...

    @property
    def models(self) -> list[str]: ...

    @property
    def model_setting(self) -> ModelSetting: ...

    def resolve_model(self, daemon_config: "DaemonConfig") -> str:
        """Return the effective model invocation ID for this plugin."""
        ...

    @property
    def model_catalog_refreshable(self) -> bool: ...

    def model_catalog_cache_key(
        self, *, config: "AppConfig", config_path: str
    ) -> Hashable:
        """Return the plugin/authentication context used to scope caching."""
        ...

    async def get_model_catalog(
        self, *, config: "AppConfig", config_path: str
    ) -> ModelCatalog:
        """Return normalized model metadata without starting inference."""
        ...

    async def run_planned_pr(
        self,
        repo_path: str,
        model: str | None,
        timeout: int,
        **kwargs: Any,
    ) -> tuple[int, str, str]: ...

    async def run_auto_pr(
        self,
        repo_path: str,
        *,
        pr_id: str,
        task_file: str,
        task_body: str,
        model: str | None,
        timeout: int,
        **kwargs: Any,
    ) -> tuple[int, str, str]: ...

    async def fix_review(
        self,
        repo_path: str,
        model: str | None,
        timeout: int | None = None,
        **kwargs: Any,
    ) -> tuple[int, str, str]: ...

    async def run_prompt(
        self,
        prompt: str,
        repo_path: str,
        model: str | None,
        timeout: int | None,
        on_process_start: Callable[[asyncio.subprocess.Process], None] | None = None,
        on_supervised_process_start: Callable[[SupervisedProcess], None]
        | None = None,
        **kwargs: Any,
    ) -> tuple[int, str, str]:
        """Run provider-neutral auxiliary work under process supervision."""
        ...

    def check_auth(self) -> dict[str, str]: ...

    def create_usage_provider(self, **kwargs: Any) -> UsageProvider | None: ...

    def rate_limit_patterns(self) -> list[re.Pattern[str]]: ...

    @property
    def supports_breach_lifecycle(self) -> bool:
        """True if the plugin honors breach detection.

        Anthropic CLI emits breach signals on stderr when usage hits
        configured thresholds. Other coders may not have this concept
        and return False here. Handlers check this property before
        wiring breach monitors.
        """
        ...

    @property
    def default_session_pause_percent(self) -> int:
        """Session-tier rate-limit pause threshold for this plugin."""
        ...

    @property
    def default_weekly_pause_percent(self) -> int:
        """Weekly-tier rate-limit pause threshold for this plugin."""
        ...

    async def diagnose_error(
        self,
        repo_path: str,
        context: str,
        model: str | None,
        on_process_start: Callable[[asyncio.subprocess.Process], None] | None = None,
        on_supervised_process_start: Callable[[SupervisedProcess], None]
        | None = None,
        **kwargs: Any,
    ) -> tuple[int, str, str]: ...

    def build_run_kwargs(
        self,
        *,
        daemon_config: "DaemonConfig",
        breach_dir: str | None = None,
        breach_run_id: str | None = None,
    ) -> dict[str, Any]:
        """Construct plugin-specific kwargs for primary and auxiliary runs.

        Returns the model selection plus any plugin-specific extras
        (e.g. breach monitoring inputs for plugins that support the
        breach lifecycle). Handlers compose handler-specific keys
        (timeout, on_process_start, extra_context) on top of the
        returned dict and pass the merged mapping via ``**kwargs`` to
        primary or auxiliary plugin method. Plugins that ignore the breach
        inputs (``supports_breach_lifecycle`` False) silently drop them so
        callers can pass them unconditionally.
        """
        ...


class CoderRegistry:
    def __init__(self) -> None:
        self._plugins: dict[str, CoderPlugin] = {}
        self._references: dict[str, str] = {}

    def register(
        self,
        plugin: CoderPlugin,
        *,
        reference: str | None = None,
    ) -> None:
        self._plugins[plugin.name] = plugin
        if reference is None:
            self._references.pop(plugin.name, None)
        else:
            self._references[plugin.name] = reference

    def get(self, name: str) -> CoderPlugin:
        if name not in self._plugins:
            raise KeyError(f"Unknown coder: {name}")
        return self._plugins[name]

    def list_coders(self) -> list[CoderPlugin]:
        return list(self._plugins.values())

    def coder_names(self) -> list[str]:
        return list(self._plugins.keys())

    def reference_for(self, name: str) -> str | None:
        """Return the startup factory reference for a configured plugin."""
        self.get(name)
        return self._references.get(name)
