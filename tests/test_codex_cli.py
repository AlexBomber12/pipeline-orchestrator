"""Tests for src/codex_cli.py."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from src.codex_cli import (
    fix_review_async,
    run_codex_async,
    run_planned_pr_async,
)


def _make_fake_proc(
    stdout: bytes = b"", stderr: bytes = b"", returncode: int = 0
) -> MagicMock:
    proc = MagicMock()
    proc.communicate = AsyncMock(return_value=(stdout, stderr))
    proc.returncode = returncode
    proc.kill = MagicMock()
    proc.wait = AsyncMock()
    return proc


@pytest.mark.asyncio
async def test_run_codex_async_success(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    fake_proc = _make_fake_proc(stdout=b"done", stderr=b"info", returncode=0)

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        captured["cmd"] = list(args)
        captured["kwargs"] = kwargs
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    result = await run_codex_async("do a thing", "/data/repos/demo", timeout=42)

    assert result == (0, "done", "info")
    cmd = captured["cmd"]
    assert cmd[0] == "codex"
    assert "exec" in cmd
    assert "--full-auto" in cmd
    assert cmd[-1] == "do a thing"
    assert captured["kwargs"]["cwd"] == "/data/repos/demo"


@pytest.mark.asyncio
async def test_run_codex_async_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_proc = _make_fake_proc()
    fake_proc.communicate = AsyncMock(side_effect=asyncio.TimeoutError)

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    result = await run_codex_async("prompt", "/tmp", timeout=5)

    assert result == (-1, "", "Timeout after 5s")
    fake_proc.kill.assert_called_once()


@pytest.mark.asyncio
async def test_run_codex_async_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        raise FileNotFoundError(2, "No such file or directory", "codex")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    result = await run_codex_async("prompt", "/tmp")

    assert result == (-1, "", "codex CLI not found")


@pytest.mark.asyncio
async def test_run_planned_pr_async_calls_exec_full_auto(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    fake_proc = _make_fake_proc(returncode=0)

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        captured["cmd"] = list(args)
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    await run_planned_pr_async("/data/repos/demo", model="o3")

    cmd = captured["cmd"]
    assert "exec" in cmd
    assert "--full-auto" in cmd
    assert "--model" in cmd
    assert cmd[cmd.index("--model") + 1] == "o3"
    assert cmd[-1] == "PLANNED PR"


@pytest.mark.asyncio
async def test_fix_review_async_passes_prompt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    fake_proc = _make_fake_proc(returncode=0)

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        captured["cmd"] = list(args)
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    await fix_review_async("/data/repos/demo")

    cmd = captured["cmd"]
    assert cmd[-1] == "FIX REVIEW"
    assert "exec" in cmd
    assert "--full-auto" in cmd


@pytest.mark.asyncio
async def test_run_planned_pr_async_ignores_extra_kwargs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Extra kwargs (breach_dir etc.) from the runner are accepted via **_kwargs."""
    fake_proc = _make_fake_proc(returncode=0)

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    # Should not raise even with extra kwargs
    result = await run_planned_pr_async(
        "/tmp",
        model="o3",
        breach_dir="/tmp/breach",
        breach_run_id="abc",
        session_threshold=95,
        weekly_threshold=100,
    )
    assert result[0] == 0


@pytest.mark.asyncio
async def test_run_codex_async_cwd_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        raise FileNotFoundError(
            2, "No such file or directory", "/data/repos/missing"
        )

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    result = await run_codex_async("prompt", "/data/repos/missing")
    assert result == (-1, "", "cwd not found: /data/repos/missing")
