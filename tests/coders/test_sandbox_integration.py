"""Integration tests for bubblewrap sandbox in coder dispatch path."""

from __future__ import annotations

import asyncio
import logging
import subprocess
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from src import claude_cli, codex_cli
from src.config import AppConfig, AuthConfig, DaemonConfig
from src.daemon import sandbox as sandbox_mod


def _config(*, isolation: bool) -> AppConfig:
    return AppConfig(
        daemon=DaemonConfig(coder_filesystem_isolation=isolation),
        auth=AuthConfig(
            claude_config_dir="/data/auth/claude",
            gh_config_dir="/data/auth/gh",
            codex_home_dir="/data/auth",
        ),
    )


def _patch_bwrap(monkeypatch: pytest.MonkeyPatch, *, available: bool) -> None:
    """Force both module-local and sandbox-module bwrap checks to ``available``."""
    monkeypatch.setattr(claude_cli, "is_bubblewrap_available", lambda: available)
    monkeypatch.setattr(codex_cli, "is_bubblewrap_available", lambda: available)
    monkeypatch.setattr(sandbox_mod, "is_bubblewrap_available", lambda: available)


def _make_fake_proc(returncode: int = 0) -> MagicMock:
    proc = MagicMock()
    proc.communicate = AsyncMock(return_value=(b"", b""))
    proc.returncode = returncode
    proc.kill = MagicMock()
    proc.wait = AsyncMock()
    return proc


def test_coder_spawn_uses_bwrap_when_isolation_enabled_and_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> MagicMock:
        captured["cmd"] = cmd
        result = MagicMock()
        result.stdout = ""
        result.stderr = ""
        result.returncode = 0
        return result

    monkeypatch.setattr(claude_cli, "load_config", lambda: _config(isolation=True))
    _patch_bwrap(monkeypatch, available=True)
    monkeypatch.setattr(subprocess, "run", fake_run)

    claude_cli.run_claude("prompt", "/data/repos/demo")

    assert captured["cmd"][0] == "bwrap"
    assert "claude" in captured["cmd"]


def test_coder_spawn_no_bwrap_when_isolation_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> MagicMock:
        captured["cmd"] = cmd
        result = MagicMock()
        result.stdout = ""
        result.stderr = ""
        result.returncode = 0
        return result

    monkeypatch.setattr(claude_cli, "load_config", lambda: _config(isolation=False))
    _patch_bwrap(monkeypatch, available=True)
    monkeypatch.setattr(subprocess, "run", fake_run)

    claude_cli.run_claude("prompt", "/data/repos/demo")

    assert captured["cmd"][0] == "claude"


def test_coder_spawn_logs_warning_when_isolation_enabled_but_bwrap_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> MagicMock:
        captured["cmd"] = cmd
        result = MagicMock()
        result.stdout = ""
        result.stderr = ""
        result.returncode = 0
        return result

    monkeypatch.setattr(claude_cli, "load_config", lambda: _config(isolation=True))
    _patch_bwrap(monkeypatch, available=False)
    monkeypatch.setattr(subprocess, "run", fake_run)

    with caplog.at_level(logging.WARNING, logger=claude_cli.logger.name):
        claude_cli.run_claude("prompt", "/data/repos/demo")

    assert captured["cmd"][0] == "claude"
    assert any("[SANDBOX]" in rec.message for rec in caplog.records)


def test_coder_spawn_passes_coder_config_dir_to_bwrap_when_provided(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> MagicMock:
        captured["cmd"] = cmd
        result = MagicMock()
        result.stdout = ""
        result.stderr = ""
        result.returncode = 0
        return result

    monkeypatch.setattr(claude_cli, "load_config", lambda: _config(isolation=True))
    _patch_bwrap(monkeypatch, available=True)
    monkeypatch.setattr(subprocess, "run", fake_run)

    claude_cli.run_claude("prompt", "/data/repos/demo")

    cmd = captured["cmd"]
    assert "/data/auth/claude" in cmd
    assert "/data/auth/gh" in cmd
    assert "/data/repos/demo" in cmd


@pytest.mark.asyncio
async def test_codex_async_spawn_uses_bwrap_when_isolation_enabled_and_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    fake_proc = _make_fake_proc()

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        captured["cmd"] = list(args)
        return fake_proc

    monkeypatch.setattr(codex_cli, "load_config", lambda: _config(isolation=True))
    _patch_bwrap(monkeypatch, available=True)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    await codex_cli.run_codex_async("prompt", "/data/repos/demo")

    assert captured["cmd"][0] == "bwrap"
    assert "codex" in captured["cmd"]
    assert "/data/repos/demo" in captured["cmd"]
    assert "/data/auth" in captured["cmd"]


@pytest.mark.asyncio
async def test_codex_async_spawn_no_bwrap_when_isolation_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    fake_proc = _make_fake_proc()

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        captured["cmd"] = list(args)
        return fake_proc

    monkeypatch.setattr(codex_cli, "load_config", lambda: _config(isolation=False))
    _patch_bwrap(monkeypatch, available=True)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    await codex_cli.run_codex_async("prompt", "/data/repos/demo")

    assert captured["cmd"][0] == "codex"


@pytest.mark.asyncio
async def test_codex_async_spawn_warns_when_bwrap_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    captured: dict[str, Any] = {}
    fake_proc = _make_fake_proc()

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        captured["cmd"] = list(args)
        return fake_proc

    monkeypatch.setattr(codex_cli, "load_config", lambda: _config(isolation=True))
    _patch_bwrap(monkeypatch, available=False)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    with caplog.at_level(logging.WARNING, logger=codex_cli.logger.name):
        await codex_cli.run_codex_async("prompt", "/data/repos/demo")

    assert captured["cmd"][0] == "codex"
    assert any("[SANDBOX]" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_claude_async_spawn_uses_bwrap_when_isolation_enabled_and_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    fake_proc = _make_fake_proc()

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        captured["cmd"] = list(args)
        return fake_proc

    monkeypatch.setattr(claude_cli, "load_config", lambda: _config(isolation=True))
    _patch_bwrap(monkeypatch, available=True)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    await claude_cli.run_claude_async(
        "prompt", "/data/repos/demo", system_prompt_file=None
    )

    assert captured["cmd"][0] == "bwrap"
    assert "claude" in captured["cmd"]
