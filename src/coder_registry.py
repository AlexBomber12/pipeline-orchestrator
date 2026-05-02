"""Coder plugin protocol and registry."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from src.usage import UsageProvider

if TYPE_CHECKING:
    from src.config import DaemonConfig


@runtime_checkable
class CoderPlugin(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def display_name(self) -> str: ...

    @property
    def models(self) -> list[str]: ...

    async def run_planned_pr(
        self,
        repo_path: str,
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
        model: str,
    ) -> tuple[int, str, str]: ...

    def build_run_kwargs(
        self,
        *,
        daemon_config: "DaemonConfig",
        breach_dir: str | None = None,
        breach_run_id: str | None = None,
    ) -> dict[str, Any]:
        """Construct plugin-specific kwargs for run_planned_pr / fix_review.

        Returns the model selection plus any plugin-specific extras
        (e.g. breach monitoring inputs for plugins that support the
        breach lifecycle). Handlers compose handler-specific keys
        (timeout, on_process_start, extra_context) on top of the
        returned dict and pass the merged mapping via ``**kwargs`` to
        ``run_planned_pr`` / ``fix_review``. Plugins that ignore the
        breach inputs (``supports_breach_lifecycle`` False) silently
        drop them so callers can pass them unconditionally.
        """
        ...


class CoderRegistry:
    def __init__(self) -> None:
        self._plugins: dict[str, CoderPlugin] = {}

    def register(self, plugin: CoderPlugin) -> None:
        self._plugins[plugin.name] = plugin

    def get(self, name: str) -> CoderPlugin:
        if name not in self._plugins:
            raise KeyError(f"Unknown coder: {name}")
        return self._plugins[name]

    def list_coders(self) -> list[CoderPlugin]:
        return list(self._plugins.values())

    def coder_names(self) -> list[str]:
        return list(self._plugins.keys())
