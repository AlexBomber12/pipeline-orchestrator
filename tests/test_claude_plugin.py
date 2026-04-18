from __future__ import annotations

from pathlib import Path

import pytest
from src.coders.claude import ClaudePlugin


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
