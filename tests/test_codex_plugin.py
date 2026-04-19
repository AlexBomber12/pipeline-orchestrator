from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
from src.coders import codex as codex_module
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


def test_auth_command_returns_126_on_permission_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*args: object, **kwargs: object) -> object:
        raise PermissionError("permission denied")

    monkeypatch.setattr(codex_module.subprocess, "run", fake_run)

    assert codex_module._run_auth_command(["codex", "--version"]) == (
        126,
        "",
        "permission denied",
    )


def test_auth_command_returns_127_on_file_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*args: object, **kwargs: object) -> object:
        raise FileNotFoundError()

    monkeypatch.setattr(codex_module.subprocess, "run", fake_run)

    assert codex_module._run_auth_command(["codex", "--version"]) == (
        127,
        "",
        "codex not found",
    )


def test_auth_command_returns_124_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*args: object, **kwargs: object) -> object:
        raise subprocess.TimeoutExpired(cmd=["codex", "--version"], timeout=5)

    monkeypatch.setattr(codex_module.subprocess, "run", fake_run)

    assert codex_module._run_auth_command(["codex", "--version"]) == (
        124,
        "",
        "codex timed out after 5s",
    )


def test_auth_command_returns_completed_process_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    completed = subprocess.CompletedProcess(
        args=["codex", "--version"],
        returncode=3,
        stdout="",
        stderr="warn",
    )

    def fake_run(*args: object, **kwargs: object) -> object:
        return completed

    monkeypatch.setattr(codex_module.subprocess, "run", fake_run)

    assert codex_module._run_auth_command(["codex", "--version"]) == (
        3,
        "",
        "warn",
    )


def test_first_probe_line_returns_empty_when_only_warnings_or_blank() -> None:
    assert (
        codex_module._first_probe_line("warning: heads up\n\n  \nwarning: again")
        == ""
    )


def test_check_auth_detail_fallback_for_unknown_version_stderr(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "auth:\n"
        f"  codex_home_dir: {tmp_path / 'codex-home'}\n",
        encoding="utf-8",
    )

    def fake_run_auth_command(
        cmd: list[str], *, env: dict[str, str] | None = None
    ) -> tuple[int, str, str]:
        assert env is not None
        if cmd == ["codex", "--version"]:
            return (1, "", "mysterious failure\n")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(codex_module, "_run_auth_command", fake_run_auth_command)

    assert CodexPlugin().check_auth() == {
        "status": "error",
        "detail": "mysterious failure",
    }


def test_check_auth_version_not_found_returns_cli_not_installed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "auth:\n"
        f"  codex_home_dir: {tmp_path / 'codex-home'}\n",
        encoding="utf-8",
    )

    def fake_run_auth_command(
        cmd: list[str], *, env: dict[str, str] | None = None
    ) -> tuple[int, str, str]:
        assert env is not None
        if cmd == ["codex", "--version"]:
            return (1, "", "codex: not found")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(codex_module, "_run_auth_command", fake_run_auth_command)

    assert CodexPlugin().check_auth() == {
        "status": "error",
        "detail": "codex CLI not installed",
    }


def test_check_auth_login_status_not_found_returns_cli_not_installed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "auth:\n"
        f"  codex_home_dir: {tmp_path / 'codex-home'}\n",
        encoding="utf-8",
    )

    def fake_run_auth_command(
        cmd: list[str], *, env: dict[str, str] | None = None
    ) -> tuple[int, str, str]:
        assert env is not None
        if cmd == ["codex", "--version"]:
            return (0, "codex 0.99.0\n", "")
        if cmd == ["codex", "login", "status"]:
            return (1, "", "codex: not found")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(codex_module, "_run_auth_command", fake_run_auth_command)

    assert CodexPlugin().check_auth() == {
        "status": "error",
        "detail": "codex CLI not installed",
    }


@pytest.mark.asyncio
async def test_codex_plugin_fix_review_delegates(
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
        "src.coders.codex.codex_cli.fix_review_async",
        fake_fix_review_async,
    )

    result = await CodexPlugin().fix_review(
        "/data/repos/demo",
        model="",
        timeout=654,
    )

    assert result == (0, "fixed", "")
    assert captured == {
        "repo_path": "/data/repos/demo",
        "model": None,
        "timeout": 654,
    }


def test_check_auth_detail_mentions_api_key_when_set_but_unverified(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "auth:\n"
        f"  codex_home_dir: {tmp_path / 'codex-home'}\n",
        encoding="utf-8",
    )

    def fake_run_auth_command(
        cmd: list[str], *, env: dict[str, str] | None = None
    ) -> tuple[int, str, str]:
        assert env is not None
        if cmd == ["codex", "--version"]:
            return (0, "codex 0.99.0\n", "")
        if cmd == ["codex", "login", "status"]:
            return (1, "", "login required")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(codex_module, "_run_auth_command", fake_run_auth_command)

    assert CodexPlugin().check_auth() == {
        "status": "error",
        "detail": (
            "codex 0.99.0 (installed); "
            "login required (OPENAI_API_KEY set but unverified)"
        ),
    }


def test_rate_limit_patterns_returns_both_codex_patterns() -> None:
    assert CodexPlugin().rate_limit_patterns() == [
        codex_module._CODEX_RETRY_PATTERN,
        codex_module._CODEX_USAGE_LIMIT_PATTERN,
    ]
