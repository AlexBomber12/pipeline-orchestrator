"""Tests for src/claude_cli.py."""

from __future__ import annotations

import asyncio
import subprocess
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from src.claude_cli import (
    diagnose_error,
    diagnose_error_async,
    fix_review,
    fix_review_async,
    parse_diagnosis,
    run_claude,
    run_claude_async,
    run_planned_pr,
    run_planned_pr_async,
)


class _FakeCompletedProcess:
    def __init__(self, stdout: str = "", stderr: str = "", returncode: int = 0) -> None:
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode


def test_run_claude_success(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        return _FakeCompletedProcess(stdout="hello", stderr="warn", returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.delenv("NODE_OPTIONS", raising=False)

    result = run_claude("do a thing", "/data/repos/demo", timeout=42)

    assert result == (0, "hello", "warn")
    assert captured["cmd"] == [
        "claude",
        "--print",
        "--dangerously-skip-permissions",
        "do a thing",
    ]
    assert captured["kwargs"]["cwd"] == "/data/repos/demo"
    assert captured["kwargs"]["timeout"] == 42
    assert captured["kwargs"]["capture_output"] is True
    assert captured["kwargs"]["text"] is True
    assert captured["kwargs"]["stdin"] is subprocess.DEVNULL
    assert captured["kwargs"]["env"]["NODE_OPTIONS"] == "--max-old-space-size=4096"


def test_run_claude_appends_to_existing_node_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured["kwargs"] = kwargs
        return _FakeCompletedProcess(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setenv("NODE_OPTIONS", "--use-openssl-ca")

    run_claude("prompt", "/tmp")

    assert (
        captured["kwargs"]["env"]["NODE_OPTIONS"]
        == "--use-openssl-ca --max-old-space-size=4096"
    )


def test_run_claude_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        raise subprocess.TimeoutExpired(cmd=cmd, timeout=5)

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert run_claude("prompt", "/tmp", timeout=5) == (-1, "", "Timeout after 5s")


def test_run_claude_file_not_found_missing_binary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """subprocess raises FileNotFoundError(filename=<executable>) when the
    binary is not on PATH."""

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        raise FileNotFoundError(2, "No such file or directory", "claude")

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert run_claude("prompt", "/tmp") == (-1, "", "claude CLI not found")


def test_run_claude_file_not_found_missing_cwd(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """subprocess raises FileNotFoundError(filename=<cwd>) when the working
    directory does not exist. It must not be reported as a missing CLI."""

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        raise FileNotFoundError(
            2, "No such file or directory", "/data/repos/missing"
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert run_claude("prompt", "/data/repos/missing") == (
        -1,
        "",
        "cwd not found: /data/repos/missing",
    )


def test_run_claude_file_not_found_without_filename(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the exception carries no filename we cannot tell which path was
    missing; fall back to reporting the CLI as the likely cause."""

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        raise FileNotFoundError()

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert run_claude("prompt", "/tmp") == (-1, "", "claude CLI not found")


def test_run_claude_with_model(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured["cmd"] = cmd
        return _FakeCompletedProcess(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    run_claude("do a thing", "/tmp", model="opus")

    assert captured["cmd"] == [
        "claude",
        "--print",
        "--dangerously-skip-permissions",
        "--model",
        "opus",
        "do a thing",
    ]


def test_run_claude_without_model_has_no_model_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured["cmd"] = cmd
        return _FakeCompletedProcess(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    run_claude("do a thing", "/tmp")

    assert "--model" not in captured["cmd"]


def test_run_planned_pr_uses_planned_pr_prompt(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        return _FakeCompletedProcess(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    run_planned_pr("/data/repos/demo")

    assert captured["cmd"][-1] == "PLANNED PR"
    assert captured["kwargs"]["cwd"] == "/data/repos/demo"
    assert captured["kwargs"]["timeout"] == 900


def test_run_planned_pr_forwards_model(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured["cmd"] = cmd
        return _FakeCompletedProcess(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    run_planned_pr("/data/repos/demo", model="sonnet")

    assert "--model" in captured["cmd"]
    assert captured["cmd"][captured["cmd"].index("--model") + 1] == "sonnet"
    assert captured["cmd"][-1] == "PLANNED PR"


def test_fix_review_forwards_model(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured["cmd"] = cmd
        return _FakeCompletedProcess(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    fix_review("/data/repos/demo", model="opus")

    assert "--model" in captured["cmd"]
    assert captured["cmd"][captured["cmd"].index("--model") + 1] == "opus"


def test_diagnose_error_forwards_model(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured["cmd"] = cmd
        return _FakeCompletedProcess(stdout="FIX", returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    diagnose_error("/data/repos/demo", "boom", model="opus")

    assert "--model" in captured["cmd"]
    assert captured["cmd"][captured["cmd"].index("--model") + 1] == "opus"


def test_fix_review_uses_fix_review_prompt(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        return _FakeCompletedProcess(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    fix_review("/data/repos/demo")

    assert captured["cmd"][-1] == "FIX REVIEW"
    assert captured["kwargs"]["cwd"] == "/data/repos/demo"
    assert captured["kwargs"]["timeout"] == 3600


def test_diagnose_error_builds_prompt(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        return _FakeCompletedProcess(stdout="FIX\nretry", returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    code, stdout, _ = diagnose_error("/data/repos/demo", "git push failed: 403")

    assert code == 0
    assert stdout == "FIX\nretry"
    prompt = captured["cmd"][-1]
    assert "git push failed: 403" in prompt
    assert "FIX, SKIP, or ESCALATE" in prompt
    assert captured["kwargs"]["timeout"] == 120


def test_parse_diagnosis_fix_with_plan() -> None:
    assert parse_diagnosis("FIX\ndo something") == "FIX"


def test_parse_diagnosis_skip() -> None:
    assert parse_diagnosis("SKIP") == "SKIP"


def test_parse_diagnosis_escalate_with_suffix() -> None:
    assert parse_diagnosis("ESCALATE: too complex") == "ESCALATE"


def test_parse_diagnosis_unknown_defaults_to_escalate() -> None:
    assert parse_diagnosis("I don't know") == "ESCALATE"


def test_parse_diagnosis_empty_defaults_to_escalate() -> None:
    assert parse_diagnosis("") == "ESCALATE"


def test_fix_review_accepts_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured["timeout"] = kwargs.get("timeout")
        return _FakeCompletedProcess(stdout="", stderr="", returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)
    fix_review("/tmp", timeout=4242)
    assert captured["timeout"] == 4242


def test_run_planned_pr_accepts_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured["timeout"] = kwargs.get("timeout")
        return _FakeCompletedProcess(stdout="", stderr="", returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)
    run_planned_pr("/tmp", timeout=777)
    assert captured["timeout"] == 777


# --- Async tests ---


def _make_fake_proc(stdout: bytes = b"", stderr: bytes = b"", returncode: int = 0) -> MagicMock:
    proc = MagicMock()
    proc.communicate = AsyncMock(return_value=(stdout, stderr))
    proc.returncode = returncode
    proc.kill = MagicMock()
    proc.wait = AsyncMock()
    return proc


@pytest.mark.asyncio
async def test_run_claude_async_success(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    fake_proc = _make_fake_proc(stdout=b"hello", stderr=b"warn", returncode=0)

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        captured["cmd"] = args
        captured["kwargs"] = kwargs
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)
    monkeypatch.delenv("NODE_OPTIONS", raising=False)

    result = await run_claude_async("do a thing", "/data/repos/demo", timeout=42)

    assert result == (0, "hello", "warn")
    cmd = list(captured["cmd"])
    assert cmd[0] == "claude"
    assert "--print" in cmd
    assert "--dangerously-skip-permissions" in cmd
    assert cmd[-1] == "do a thing"
    assert captured["kwargs"]["cwd"] == "/data/repos/demo"


@pytest.mark.asyncio
async def test_run_claude_async_forwards_model_and_breach_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    fake_proc = _make_fake_proc(returncode=0)

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        captured["cmd"] = list(args)
        captured["kwargs"] = kwargs
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)
    monkeypatch.setenv("NODE_OPTIONS", "--use-openssl-ca")

    await run_claude_async(
        "do a thing",
        "/data/repos/demo",
        model="sonnet",
        breach_dir="/tmp/breach",
        breach_run_id="run-123",
        session_threshold=12,
        weekly_threshold=34,
    )

    cmd = captured["cmd"]
    assert "--model" in cmd
    assert cmd[cmd.index("--model") + 1] == "sonnet"
    assert "--append-system-prompt-file" in cmd
    assert cmd[cmd.index("--append-system-prompt-file") + 1] == "CLAUDE.md"
    assert captured["kwargs"]["env"]["NODE_OPTIONS"] == (
        "--use-openssl-ca --max-old-space-size=4096"
    )
    assert captured["kwargs"]["env"]["PIPELINE_BREACH_DIR"] == "/tmp/breach"
    assert captured["kwargs"]["env"]["PIPELINE_RUN_ID"] == "run-123"
    assert captured["kwargs"]["env"]["PIPELINE_SESSION_THRESHOLD"] == "12"
    assert captured["kwargs"]["env"]["PIPELINE_WEEKLY_THRESHOLD"] == "34"


@pytest.mark.asyncio
async def test_run_claude_async_generates_breach_run_id_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    fake_proc = _make_fake_proc(returncode=0)

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        captured["kwargs"] = kwargs
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)
    monkeypatch.setattr("src.claude_cli.uuid.uuid4", lambda: MagicMock(hex="abcdef1234567890"))

    await run_claude_async("do a thing", "/data/repos/demo", breach_dir="/tmp/breach")

    assert captured["kwargs"]["env"]["PIPELINE_RUN_ID"] == "abcdef123456"
    assert "PIPELINE_SESSION_THRESHOLD" not in captured["kwargs"]["env"]
    assert "PIPELINE_WEEKLY_THRESHOLD" not in captured["kwargs"]["env"]


@pytest.mark.asyncio
async def test_run_claude_async_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_proc = _make_fake_proc()
    fake_proc.communicate = AsyncMock(side_effect=asyncio.TimeoutError)

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    result = await run_claude_async("prompt", "/tmp", timeout=5)

    assert result == (-1, "", "Timeout after 5s")
    fake_proc.kill.assert_called_once()


@pytest.mark.asyncio
async def test_run_claude_async_timeout_ignores_missing_process_on_kill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_proc = _make_fake_proc()
    fake_proc.communicate = AsyncMock(side_effect=asyncio.TimeoutError)
    fake_proc.kill.side_effect = ProcessLookupError

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    result = await run_claude_async("prompt", "/tmp", timeout=5)

    assert result == (-1, "", "Timeout after 5s")
    fake_proc.wait.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_claude_async_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        raise FileNotFoundError(2, "No such file or directory", "claude")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    result = await run_claude_async("prompt", "/tmp")

    assert result == (-1, "", "claude CLI not found")


@pytest.mark.asyncio
async def test_run_claude_async_not_found_missing_cwd(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        raise FileNotFoundError(2, "No such file or directory", "/tmp/missing")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    result = await run_claude_async("prompt", "/tmp/missing")

    assert result == (-1, "", "cwd not found: /tmp/missing")


@pytest.mark.asyncio
async def test_run_claude_async_bare_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    fake_proc = _make_fake_proc(returncode=0)

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        captured["cmd"] = list(args)
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    await run_claude_async("test", "/tmp")

    cmd = captured["cmd"]
    assert "--print" in cmd
    assert "--dangerously-skip-permissions" in cmd


@pytest.mark.asyncio
async def test_run_claude_async_cancelled_kills_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_proc = _make_fake_proc()
    fake_proc.communicate = AsyncMock(side_effect=asyncio.CancelledError)

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    with pytest.raises(asyncio.CancelledError):
        await run_claude_async("prompt", "/tmp")

    fake_proc.kill.assert_called_once()
    fake_proc.wait.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_claude_async_cancelled_ignores_missing_process_on_kill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_proc = _make_fake_proc()
    fake_proc.communicate = AsyncMock(side_effect=asyncio.CancelledError)
    fake_proc.kill.side_effect = ProcessLookupError

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    with pytest.raises(asyncio.CancelledError):
        await run_claude_async("prompt", "/tmp")

    fake_proc.wait.assert_awaited_once()


@pytest.mark.asyncio
async def test_diagnose_error_async_skips_system_prompt(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    fake_proc = _make_fake_proc(stdout=b"FIX\nretry", returncode=0)

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        captured["cmd"] = list(args)
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    code, stdout, _ = await diagnose_error_async("/data/repos/demo", "git push failed")

    assert code == 0
    assert stdout == "FIX\nretry"
    cmd = captured["cmd"]
    assert "--append-system-prompt-file" not in cmd
    assert "CLAUDE.md" not in cmd


@pytest.mark.asyncio
async def test_run_claude_async_calls_on_process_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_proc = _make_fake_proc(returncode=0)
    started: list[MagicMock] = []

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    result = await run_claude_async(
        "prompt",
        "/tmp",
        on_process_start=lambda proc: started.append(proc),
    )

    assert result == (0, "", "")
    assert started == [fake_proc]


@pytest.mark.asyncio
async def test_run_planned_pr_async_forwards_to_run_claude_async(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    async def fake_run_claude_async(*args: Any, **kwargs: Any) -> tuple[int, str, str]:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return (0, "ok", "")

    monkeypatch.setattr("src.claude_cli.run_claude_async", fake_run_claude_async)

    result = await run_planned_pr_async(
        "/data/repos/demo",
        model="sonnet",
        timeout=111,
        breach_dir="/tmp/breach",
        breach_run_id="run-123",
        session_threshold=12,
        weekly_threshold=34,
    )

    assert result == (0, "ok", "")
    assert captured["args"] == ("PLANNED PR", "/data/repos/demo")
    assert captured["kwargs"] == {
        "timeout": 111,
        "model": "sonnet",
        "breach_dir": "/tmp/breach",
        "breach_run_id": "run-123",
        "session_threshold": 12,
        "weekly_threshold": 34,
    }


@pytest.mark.asyncio
async def test_run_planned_pr_async_forwards_on_process_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    callback = object()

    async def fake_run_claude_async(*args: Any, **kwargs: Any) -> tuple[int, str, str]:
        captured["kwargs"] = kwargs
        return (0, "ok", "")

    monkeypatch.setattr("src.claude_cli.run_claude_async", fake_run_claude_async)

    await run_planned_pr_async("/data/repos/demo", on_process_start=callback)  # type: ignore[arg-type]

    assert captured["kwargs"]["on_process_start"] is callback


@pytest.mark.asyncio
async def test_fix_review_async_forwards_to_run_claude_async(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    async def fake_run_claude_async(*args: Any, **kwargs: Any) -> tuple[int, str, str]:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return (0, "ok", "")

    monkeypatch.setattr("src.claude_cli.run_claude_async", fake_run_claude_async)

    result = await fix_review_async(
        "/data/repos/demo",
        model="opus",
        timeout=None,
        breach_dir="/tmp/breach",
        breach_run_id="run-456",
        session_threshold=56,
        weekly_threshold=78,
    )

    assert result == (0, "ok", "")
    assert captured["args"] == ("FIX REVIEW", "/data/repos/demo")
    assert captured["kwargs"] == {
        "timeout": None,
        "model": "opus",
        "breach_dir": "/tmp/breach",
        "breach_run_id": "run-456",
        "session_threshold": 56,
        "weekly_threshold": 78,
    }


@pytest.mark.asyncio
async def test_fix_review_async_forwards_on_process_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    callback = object()

    async def fake_run_claude_async(*args: Any, **kwargs: Any) -> tuple[int, str, str]:
        captured["kwargs"] = kwargs
        return (0, "ok", "")

    monkeypatch.setattr("src.claude_cli.run_claude_async", fake_run_claude_async)

    await fix_review_async("/data/repos/demo", on_process_start=callback)  # type: ignore[arg-type]

    assert captured["kwargs"]["on_process_start"] is callback
