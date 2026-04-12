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
    assert captured["kwargs"]["timeout"] == 600


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
