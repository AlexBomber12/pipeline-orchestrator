"""Coder plugin protocol and registry."""

from __future__ import annotations

import re
from typing import Any, Protocol

from src.usage import UsageProvider


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
        timeout: int | None,
        **kwargs: Any,
    ) -> tuple[int, str, str]: ...

    def check_auth(self) -> dict[str, str]: ...

    def create_usage_provider(self, **kwargs: Any) -> UsageProvider | None: ...

    def rate_limit_patterns(self) -> list[re.Pattern[str]]: ...


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
