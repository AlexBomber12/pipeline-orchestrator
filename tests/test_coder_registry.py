from __future__ import annotations

import asyncio
import re
from typing import Any

import pytest
from src import claude_cli, codex_cli
from src.coder_registry import CoderPlugin, CoderRegistry
from src.coders.claude import ClaudePlugin
from src.coders.codex import CodexPlugin
from src.config import DaemonConfig


class DummyCoderPlugin:
    def __init__(self, name: str, display_name: str) -> None:
        self.name = name
        self.display_name = display_name
        self.models = ["model-a", "model-b"]

    async def run_planned_pr(
        self, repo_path: str, model: str | None, timeout: int
    ) -> tuple[int, str, str]:
        return (0, repo_path, model or str(timeout))

    async def run_auto_pr(
        self,
        repo_path: str,
        *,
        pr_id: str,
        task_file: str,
        task_body: str,
        model: str | None,
        timeout: int,
    ) -> tuple[int, str, str]:
        return (0, repo_path, f"{pr_id}|{task_file}|{task_body}|{model}|{timeout}")

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

    def build_run_kwargs(
        self,
        *,
        daemon_config: DaemonConfig,
        breach_dir: str | None = None,
        breach_run_id: str | None = None,
    ) -> dict[str, Any]:
        return {"model": daemon_config.claude_model}


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


def test_protocol_includes_run_auto_pr() -> None:
    """``CoderPlugin`` declares ``run_auto_pr`` so the daemon can dispatch
    AUTO PR runs without depending on QUEUE.md/AGENTS.md indirection."""
    assert "run_auto_pr" in dir(CoderPlugin)
    assert isinstance(ClaudePlugin(), CoderPlugin)
    assert isinstance(CodexPlugin(), CoderPlugin)


def test_protocol_includes_build_run_kwargs() -> None:
    """``CoderPlugin`` declares ``build_run_kwargs`` so handlers can
    delegate plugin-specific kwargs construction without hardcoding
    coder names."""
    assert "build_run_kwargs" in dir(CoderPlugin)
    assert isinstance(ClaudePlugin(), CoderPlugin)
    assert isinstance(CodexPlugin(), CoderPlugin)


def test_claude_plugin_build_run_kwargs_with_breach() -> None:
    daemon = DaemonConfig(
        claude_model="opus",
        rate_limit_session_pause_percent=90,
        rate_limit_weekly_pause_percent=70,
    )
    kwargs = ClaudePlugin().build_run_kwargs(
        daemon_config=daemon,
        breach_dir="/tmp/breach",
        breach_run_id="abc123",
    )
    assert kwargs == {
        "model": "opus",
        "breach_dir": "/tmp/breach",
        "breach_run_id": "abc123",
        "session_threshold": 90,
        "weekly_threshold": 70,
    }


def test_claude_plugin_build_run_kwargs_without_breach() -> None:
    daemon = DaemonConfig(claude_model="sonnet")
    kwargs = ClaudePlugin().build_run_kwargs(daemon_config=daemon)
    assert kwargs == {"model": "sonnet"}


def test_claude_plugin_build_run_kwargs_partial_breach_input_omits_breach() -> None:
    """A single breach input without the other yields no breach kwargs.

    Both ``breach_dir`` and ``breach_run_id`` must be supplied together
    for the plugin to emit breach-monitoring kwargs.
    """
    daemon = DaemonConfig(claude_model="opus")
    only_dir = ClaudePlugin().build_run_kwargs(
        daemon_config=daemon, breach_dir="/tmp/breach"
    )
    only_id = ClaudePlugin().build_run_kwargs(
        daemon_config=daemon, breach_run_id="abc"
    )
    assert only_dir == {"model": "opus"}
    assert only_id == {"model": "opus"}


def test_codex_plugin_build_run_kwargs_no_breach_keys() -> None:
    """Codex returns only ``model`` even when breach inputs are passed.

    ``supports_breach_lifecycle`` is False so the plugin silently
    ignores breach inputs, letting callers pass them unconditionally.
    """
    daemon = DaemonConfig(codex_model="gpt-5.4")
    kwargs = CodexPlugin().build_run_kwargs(
        daemon_config=daemon,
        breach_dir="/tmp/breach",
        breach_run_id="abc123",
    )
    assert kwargs == {"model": "gpt-5.4"}


def test_codex_plugin_build_run_kwargs_default_codex_model_empty() -> None:
    daemon = DaemonConfig()
    kwargs = CodexPlugin().build_run_kwargs(daemon_config=daemon)
    assert kwargs == {"model": ""}
