"""Tests for src/claude_cli.py."""

from __future__ import annotations

import subprocess
from typing import Any

import pytest

from src.claude_cli import (
    diagnose_error,
    fix_review,
    parse_diagnosis,
    run_claude,
    run_planned_pr,
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
        "--bare",
        "--strict-mcp-config",
        "--no-session-persistence",
        "--system-prompt-file",
        "CLAUDE.md",
        "--max-turns",
        "30",
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
        "--bare",
        "--strict-mcp-config",
        "--no-session-persistence",
        "--model",
        "opus",
        "--system-prompt-file",
        "CLAUDE.md",
        "--max-turns",
        "30",
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
