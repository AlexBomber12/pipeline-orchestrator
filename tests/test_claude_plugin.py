from __future__ import annotations

from pathlib import Path

import pytest
from src.coders import claude as claude_module
from src.coders.claude import ClaudePlugin
from src.config import AppConfig


def test_claude_plugin_name() -> None:
    plugin = ClaudePlugin()

    assert plugin.name == "claude"
    assert plugin.display_name == "Claude Code"


def test_claude_plugin_models() -> None:
    plugin = ClaudePlugin()

    assert plugin.models == ["opus", "sonnet"]


@pytest.mark.asyncio
async def test_claude_plugin_run_planned_pr_delegates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    async def fake_run_planned_pr_async(
        repo_path: str,
        model: str | None = None,
        timeout: int = 900,
        **_: object,
    ) -> tuple[int, str, str]:
        captured["repo_path"] = repo_path
        captured["model"] = model
        captured["timeout"] = timeout
        return (0, "ok", "")

    monkeypatch.setattr(
        "src.coders.claude.claude_cli.run_planned_pr_async",
        fake_run_planned_pr_async,
    )

    result = await ClaudePlugin().run_planned_pr(
        "/data/repos/demo",
        model="opus",
        timeout=321,
    )

    assert result == (0, "ok", "")
    assert captured == {
        "repo_path": "/data/repos/demo",
        "model": "opus",
        "timeout": 321,
    }


@pytest.mark.asyncio
async def test_claude_plugin_fix_review_delegates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    async def fake_fix_review_async(
        repo_path: str,
        model: str | None = None,
        timeout: int | None = None,
        **_: object,
    ) -> tuple[int, str, str]:
        captured["repo_path"] = repo_path
        captured["model"] = model
        captured["timeout"] = timeout
        return (0, "fixed", "")

    monkeypatch.setattr(
        "src.coders.claude.claude_cli.fix_review_async",
        fake_fix_review_async,
    )

    result = await ClaudePlugin().fix_review(
        "/data/repos/demo",
        model="sonnet",
        timeout=123,
    )

    assert result == (0, "fixed", "")
    assert captured == {
        "repo_path": "/data/repos/demo",
        "model": "sonnet",
        "timeout": 123,
    }


def test_claude_plugin_check_auth(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "auth:\n"
        f"  claude_config_dir: {tmp_path / 'claude-auth'}\n",
        encoding="utf-8",
    )

    captured: dict[str, object] = {}

    def fake_run_auth_command(
        cmd: list[str], *, env: dict[str, str] | None = None
    ) -> tuple[int, str, str]:
        captured["cmd"] = cmd
        captured["env"] = env or {}
        return (0, "claude 1.2.3\n", "")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "src.coders.claude._run_auth_command",
        fake_run_auth_command,
    )

    result = ClaudePlugin().check_auth()

    assert result == {"status": "ok", "detail": "claude 1.2.3"}
    assert captured["cmd"] == ["claude", "--version"]
    assert captured["env"]["CLAUDE_CONFIG_DIR"] == str(tmp_path / "claude-auth")


def test_claude_plugin_check_auth_returns_error_detail(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "auth:\n"
        f"  claude_config_dir: {tmp_path / 'claude-auth'}\n",
        encoding="utf-8",
    )

    def fake_run_auth_command(
        cmd: list[str], *, env: dict[str, str] | None = None
    ) -> tuple[int, str, str]:
        del cmd, env
        return (1, "", "broken auth")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "src.coders.claude._run_auth_command",
        fake_run_auth_command,
    )

    result = ClaudePlugin().check_auth()

    assert result == {"status": "error", "detail": "broken auth"}


def test_run_auth_command_returns_completed_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class CompletedProcess:
        returncode = 3
        stdout = "stdout text"
        stderr = "stderr text"

    def fake_run(*args: object, **kwargs: object) -> object:
        return CompletedProcess()

    monkeypatch.setattr("src.coders.claude.subprocess.run", fake_run)

    result = claude_module._run_auth_command(["claude", "--version"])

    assert result == (3, "stdout text", "stderr text")


def test_run_auth_command_returns_127_on_missing_binary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*args: object, **kwargs: object) -> object:
        raise FileNotFoundError

    monkeypatch.setattr("src.coders.claude.subprocess.run", fake_run)

    result = claude_module._run_auth_command(["claude", "--version"])

    assert result == (127, "", "claude not found")


def test_auth_check_returns_126_on_permission_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*args: object, **kwargs: object) -> object:
        raise PermissionError("denied")

    monkeypatch.setattr("src.coders.claude.subprocess.run", fake_run)

    result = claude_module._run_auth_command(["claude", "--version"])

    assert result == (126, "", "denied")


def test_run_auth_command_returns_124_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*args: object, **kwargs: object) -> object:
        raise claude_module.subprocess.TimeoutExpired(
            cmd=["claude", "--version"],
            timeout=claude_module._AUTH_CHECK_TIMEOUT_SEC,
        )

    monkeypatch.setattr("src.coders.claude.subprocess.run", fake_run)

    result = claude_module._run_auth_command(["claude", "--version"])

    assert result == (
        124,
        "",
        "claude timed out after 5s",
    )


def test_create_usage_provider_loads_config_from_default_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_load_config(path: str) -> AppConfig:
        captured["config_path"] = path
        return AppConfig()

    def fake_oauth_usage_provider(**kwargs: object) -> object:
        captured["provider_kwargs"] = kwargs
        return {"provider_kwargs": kwargs}

    monkeypatch.setattr("src.coders.claude.load_config", fake_load_config)
    monkeypatch.setattr(
        "src.coders.claude.OAuthUsageProvider",
        fake_oauth_usage_provider,
    )

    result = ClaudePlugin().create_usage_provider()

    assert captured["config_path"] == claude_module.CONFIG_PATH
    assert result == {"provider_kwargs": captured["provider_kwargs"]}


def test_rate_limit_patterns_returns_anthropic_pattern() -> None:
    patterns = ClaudePlugin().rate_limit_patterns()

    assert patterns == [claude_module._ANTHROPIC_RATE_LIMIT_PATTERN]
