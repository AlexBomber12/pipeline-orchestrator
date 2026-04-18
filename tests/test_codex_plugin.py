from __future__ import annotations

from pathlib import Path

import pytest
from src.coders.codex import CodexPlugin
from src.usage import OpenAIUsageProvider


def test_codex_plugin_name() -> None:
    plugin = CodexPlugin()

    assert plugin.name == "codex"
    assert plugin.display_name == "Codex CLI"


def test_codex_plugin_models_includes_default() -> None:
    plugin = CodexPlugin()

    assert plugin.models == [
        "",
        "gpt-5.4",
        "gpt-5.3-codex",
        "gpt-5.3-codex-spark",
        "gpt-5.2-codex",
        "gpt-5.4-mini",
        "gpt-5.1-codex-max",
        "gpt-5.1-codex-mini",
        "gpt-5.2",
    ]


@pytest.mark.asyncio
async def test_codex_plugin_run_planned_pr_delegates(
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
        "src.coders.codex.codex_cli.run_planned_pr_async",
        fake_run_planned_pr_async,
    )

    result = await CodexPlugin().run_planned_pr(
        "/data/repos/demo",
        model="",
        timeout=321,
    )

    assert result == (0, "ok", "")
    assert captured == {
        "repo_path": "/data/repos/demo",
        "model": None,
        "timeout": 321,
    }


def test_codex_plugin_check_auth(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "auth:\n"
        f"  codex_home_dir: {tmp_path / 'codex-home'}\n",
        encoding="utf-8",
    )

    calls: list[tuple[list[str], dict[str, str] | None]] = []

    def fake_run_auth_command(
        cmd: list[str], *, env: dict[str, str] | None = None
    ) -> tuple[int, str, str]:
        calls.append((cmd, env))
        if cmd == ["codex", "--version"]:
            return (0, "codex 0.99.0\n", "")
        if cmd == ["codex", "login", "status"]:
            return (0, "Logged in as test-user\n", "")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "src.coders.codex._run_auth_command",
        fake_run_auth_command,
    )

    result = CodexPlugin().check_auth()

    assert result == {
        "status": "ok",
        "detail": "codex 0.99.0 (installed); Logged in as test-user",
    }
    assert [cmd for cmd, _env in calls] == [
        ["codex", "--version"],
        ["codex", "login", "status"],
    ]
    assert all(env is not None for _cmd, env in calls)
    assert calls[0][1]["HOME"] == str(tmp_path / "codex-home")
    assert calls[1][1]["HOME"] == str(tmp_path / "codex-home")


def test_codex_plugin_create_usage_provider(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "auth:\n"
        f"  codex_home_dir: {tmp_path / 'codex-home'}\n"
        "daemon:\n"
        "  usage_api_cache_ttl_sec: 123\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    provider = CodexPlugin().create_usage_provider()

    assert isinstance(provider, OpenAIUsageProvider)
    assert provider._credentials_path == tmp_path / "codex-home" / ".codex" / "auth.json"
    assert provider._cache_ttl == 123
