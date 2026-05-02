from __future__ import annotations

import asyncio
import re

import pytest
from src import claude_cli, codex_cli
from src.coder_registry import CoderPlugin, CoderRegistry
from src.coders.claude import ClaudePlugin
from src.coders.codex import CodexPlugin


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

    @property
    def supports_breach_lifecycle(self) -> bool:
        return True

    @property
    def default_session_pause_percent(self) -> int:
        return 95

    @property
    def default_weekly_pause_percent(self) -> int:
        return 80

    async def diagnose_error(
        self, repo_path: str, context: str, model: str
    ) -> tuple[int, str, str]:
        return (0, f"{repo_path}|{context}|{model}", "")


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


def test_protocol_includes_diagnose_error() -> None:
    """``CoderPlugin`` declares ``diagnose_error`` and ``isinstance`` checks
    succeed for the bundled plugins."""
    plugin = DummyCoderPlugin(name="claude", display_name="Claude")
    assert isinstance(plugin, CoderPlugin)
    assert isinstance(ClaudePlugin(), CoderPlugin)
    assert isinstance(CodexPlugin(), CoderPlugin)


def test_protocol_includes_supports_breach_lifecycle() -> None:
    """``CoderPlugin`` declares ``supports_breach_lifecycle`` so handlers
    can gate breach monitoring without hardcoding coder names."""
    assert "supports_breach_lifecycle" in dir(CoderPlugin)
    assert isinstance(ClaudePlugin(), CoderPlugin)
    assert isinstance(CodexPlugin(), CoderPlugin)


def test_claude_plugin_supports_breach_lifecycle_true() -> None:
    assert ClaudePlugin().supports_breach_lifecycle is True


def test_codex_plugin_supports_breach_lifecycle_false() -> None:
    assert CodexPlugin().supports_breach_lifecycle is False


def test_protocol_includes_default_pause_percent_properties() -> None:
    """``CoderPlugin`` declares ``default_session_pause_percent`` and
    ``default_weekly_pause_percent`` so handlers can read per-plugin
    rate-limit thresholds without hardcoding coder names."""
    assert "default_session_pause_percent" in dir(CoderPlugin)
    assert "default_weekly_pause_percent" in dir(CoderPlugin)
    assert isinstance(ClaudePlugin(), CoderPlugin)
    assert isinstance(CodexPlugin(), CoderPlugin)


def test_claude_plugin_default_session_pause_percent() -> None:
    assert ClaudePlugin().default_session_pause_percent == 95


def test_claude_plugin_default_weekly_pause_percent() -> None:
    assert ClaudePlugin().default_weekly_pause_percent == 80


def test_codex_plugin_default_session_pause_percent() -> None:
    assert CodexPlugin().default_session_pause_percent == 100


def test_codex_plugin_default_weekly_pause_percent() -> None:
    assert CodexPlugin().default_weekly_pause_percent == 100


def test_claude_plugin_diagnose_error_delegates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    async def fake(
        repo_path: str, context: str, *, model: str | None = None
    ) -> tuple[int, str, str]:
        captured["repo_path"] = repo_path
        captured["context"] = context
        captured["model"] = model
        return (0, "FIX", "")

    monkeypatch.setattr(claude_cli, "diagnose_error_async", fake)

    code, stdout, stderr = asyncio.run(
        ClaudePlugin().diagnose_error(
            "/tmp/repo", "ci red", model="opus"
        )
    )

    assert (code, stdout, stderr) == (0, "FIX", "")
    assert captured == {
        "repo_path": "/tmp/repo",
        "context": "ci red",
        "model": "opus",
    }


def test_codex_plugin_diagnose_error_delegates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    async def fake(
        repo_path: str, context: str, *, model: str | None = None
    ) -> tuple[int, str, str]:
        captured["repo_path"] = repo_path
        captured["context"] = context
        captured["model"] = model
        return (0, "SKIP", "")

    monkeypatch.setattr(codex_cli, "diagnose_error_async", fake)

    code, stdout, stderr = asyncio.run(
        CodexPlugin().diagnose_error(
            "/tmp/repo", "ci red", model="gpt-5.4"
        )
    )

    assert (code, stdout, stderr) == (0, "SKIP", "")
    assert captured == {
        "repo_path": "/tmp/repo",
        "context": "ci red",
        "model": "gpt-5.4",
    }
