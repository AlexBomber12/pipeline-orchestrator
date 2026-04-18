from __future__ import annotations

import re

import pytest

from src.coder_registry import CoderRegistry


class DummyCoderPlugin:
    def __init__(self, name: str, display_name: str) -> None:
        self.name = name
        self.display_name = display_name
        self.models = ["model-a", "model-b"]

    async def run_planned_pr(
        self, repo_path: str, model: str | None, timeout: int
    ) -> tuple[int, str, str]:
        return (0, repo_path, model or str(timeout))

    async def fix_review(
        self, repo_path: str, model: str | None, timeout: int | None
    ) -> tuple[int, str, str]:
        return (0, repo_path, model or str(timeout))

    def check_auth(self) -> dict[str, str]:
        return {"status": "ok"}

    def create_usage_provider(self, **kwargs: object) -> None:
        return None

    def rate_limit_patterns(self) -> list[re.Pattern[str]]:
        return [re.compile("limit")]


def test_register_and_get() -> None:
    registry = CoderRegistry()
    plugin = DummyCoderPlugin(name="claude", display_name="Claude")

    registry.register(plugin)

    assert registry.get("claude") is plugin


def test_get_unknown_raises() -> None:
    registry = CoderRegistry()

    with pytest.raises(KeyError, match="Unknown coder: missing"):
        registry.get("missing")


def test_list_coders() -> None:
    registry = CoderRegistry()
    claude = DummyCoderPlugin(name="claude", display_name="Claude")
    codex = DummyCoderPlugin(name="codex", display_name="Codex")

    registry.register(claude)
    registry.register(codex)

    assert registry.list_coders() == [claude, codex]


def test_coder_names() -> None:
    registry = CoderRegistry()
    registry.register(DummyCoderPlugin(name="claude", display_name="Claude"))
    registry.register(DummyCoderPlugin(name="codex", display_name="Codex"))

    assert registry.coder_names() == ["claude", "codex"]
