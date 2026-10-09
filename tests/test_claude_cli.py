"""Tests for src/claude_cli.py."""

from __future__ import annotations

import asyncio
import subprocess
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from src.claude_cli import (
    _build_fix_feedback_prompt,
    diagnose_error,
    diagnose_error_async,
    fix_review,
    fix_review_async,
    run_auto_pr,
    run_auto_pr_async,
    run_claude,
    run_claude_async,
    run_planned_pr,
    run_planned_pr_async,
)
from src.process_supervisor import CleanupResult, CleanupStatus


class _FakeSupervisedProcess:
    def __init__(self, process: MagicMock) -> None:
        self.process = process

    async def cleanup(
        self, *, term_grace: float, kill_grace: float
    ) -> CleanupResult:
        del term_grace, kill_grace
        try:
            self.process.kill()
        except ProcessLookupError:
            pass
        if "_cleanup_returncode" in self.process.__dict__:
            self.process.returncode = self.process.__dict__["_cleanup_returncode"]
        error = self.process.__dict__.get("_cleanup_error")
        if error is not None:
            raise error
        return self.process.__dict__.get(
            "_cleanup_result",
            CleanupResult(
                CleanupStatus.QUIESCENT,
                self.process.returncode,
                True,
                False,
            ),
        )


@pytest.fixture(autouse=True)
def _adapt_async_launch(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_launch(*args: Any, **kwargs: Any) -> _FakeSupervisedProcess:
        process = await asyncio.create_subprocess_exec(*args, **kwargs)
        return _FakeSupervisedProcess(process)

    monkeypatch.setattr("src.claude_cli.launch_process", fake_launch)


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


def test_run_claude_with_model_and_effort(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured["cmd"] = cmd
        return _FakeCompletedProcess(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    # Keep the historical positional arguments valid while accepting effort.
    run_claude("do a thing", "/tmp", 600, "opus", "high")

    assert captured["cmd"] == [
        "claude",
        "--print",
        "--dangerously-skip-permissions",
        "--model",
        "opus",
        "--effort",
        "high",
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
    assert "--effort" not in captured["cmd"]


def test_run_planned_pr_uses_planned_pr_prompt(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        return _FakeCompletedProcess(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    run_planned_pr("/data/repos/demo")

    assert captured["cmd"][-1] == "PLANNED PR"
    assert "DAEMON INVOCATION" not in captured["cmd"][-1]
    assert captured["kwargs"]["cwd"] == "/data/repos/demo"
    assert captured["kwargs"]["timeout"] == 900


def test_run_planned_pr_forwards_model(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured["cmd"] = cmd
        return _FakeCompletedProcess(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    run_planned_pr(
        "/data/repos/demo",
        model="sonnet",
        reasoning_effort="medium",
    )

    assert "--model" in captured["cmd"]
    assert captured["cmd"][captured["cmd"].index("--model") + 1] == "sonnet"
    assert captured["cmd"][captured["cmd"].index("--effort") + 1] == "medium"
    assert captured["cmd"][-1] == "PLANNED PR"


def test_fix_review_forwards_model(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured["cmd"] = cmd
        return _FakeCompletedProcess(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    fix_review("/data/repos/demo", model="opus", reasoning_effort="low")

    assert "--model" in captured["cmd"]
    assert captured["cmd"][captured["cmd"].index("--model") + 1] == "opus"
    assert captured["cmd"][captured["cmd"].index("--effort") + 1] == "low"


def test_diagnose_error_forwards_model(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured["cmd"] = cmd
        return _FakeCompletedProcess(stdout="FIX", returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    diagnose_error(
        "/data/repos/demo",
        "boom",
        model="opus",
        reasoning_effort="high",
    )

    assert "--model" in captured["cmd"]
    assert captured["cmd"][captured["cmd"].index("--model") + 1] == "opus"
    assert captured["cmd"][captured["cmd"].index("--effort") + 1] == "high"


def test_fix_review_uses_fix_review_prompt(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        return _FakeCompletedProcess(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    fix_review("/data/repos/demo")

    prompt = captured["cmd"][-1]
    assert prompt.startswith("FIX FEEDBACK\n\n")
    assert "This FIX FEEDBACK run was dispatched" in prompt
    assert "OPERATOR-AUTHORIZED EXECUTION POLICY" in prompt
    assert "one iteration" in prompt
    assert "scripts/make-review-artifacts.sh" in prompt
    assert "remote PR HEAD is the pushed local HEAD" in prompt
    assert "daemon owns review triggering" in prompt
    assert "Do not wait for a new review" in prompt
    assert captured["kwargs"]["cwd"] == "/data/repos/demo"
    assert captured["kwargs"]["timeout"] == 3600


def test_fix_review_appends_extra_context(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured["cmd"] = cmd
        return _FakeCompletedProcess(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    fix_review(
        "/data/repos/demo",
        extra_context="CI failure logs (last 5000 chars):\nboom",
    )

    prompt = captured["cmd"][-1]
    assert prompt.startswith("FIX FEEDBACK\n\n")
    assert "CI failure logs (last 5000 chars):" in prompt
    assert "boom" in prompt


def test_build_fix_feedback_prompt_with_task_anchor() -> None:
    prompt = _build_fix_feedback_prompt(
        "some logs",
        pr_id="PR-100",
        task_file="tasks/PR-100.md",
    )

    assert prompt.startswith("Task: PR-100\n\nFile: tasks/PR-100.md\n\n")
    assert (
        "Stay in the scope of this task. Do not address any "
        "other PR or task in this run."
    ) in prompt
    assert "\n\nFIX FEEDBACK\n\nDAEMON INVOCATION" in prompt
    assert prompt.endswith("some logs")


def test_build_fix_feedback_prompt_legacy_fallbacks() -> None:
    prompt = _build_fix_feedback_prompt(
        "ci logs",
        pr_id=None,
        task_file=None,
    )
    assert prompt.startswith("FIX FEEDBACK\n\nDAEMON INVOCATION")
    assert prompt.endswith("\n\nci logs")
    partial_prompt = _build_fix_feedback_prompt(
        "ci logs",
        pr_id="PR-100",
        task_file=None,
    )
    assert partial_prompt.startswith("FIX FEEDBACK\n\nDAEMON INVOCATION")
    assert "Task: PR-100" not in partial_prompt
    assert partial_prompt.endswith("\n\nci logs")
    assert _build_fix_feedback_prompt(None).startswith(
        "FIX FEEDBACK\n\nDAEMON INVOCATION"
    )


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
    assert "DAEMON INVOCATION" not in prompt
    assert captured["kwargs"]["timeout"] == 120


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


def _make_fake_proc(
    stdout: bytes = b"", stderr: bytes = b"", returncode: int = 0
) -> MagicMock:
    proc = MagicMock()
    stdout_reader = asyncio.StreamReader()
    stdout_reader.feed_data(stdout)
    stdout_reader.feed_eof()
    stderr_reader = asyncio.StreamReader()
    stderr_reader.feed_data(stderr)
    stderr_reader.feed_eof()
    proc.stdout = stdout_reader
    proc.stderr = stderr_reader
    proc.returncode = returncode
    proc.kill = MagicMock()
    proc.wait = AsyncMock(return_value=returncode)
    return proc


def _block_until_cleanup(proc: MagicMock) -> None:
    proc.returncode = None
    proc.__dict__["_cleanup_returncode"] = 0


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
    assert "--effort" not in cmd
    assert cmd[-1] == "do a thing"
    assert captured["kwargs"]["cwd"] == "/data/repos/demo"


@pytest.mark.asyncio
async def test_run_claude_async_preserves_provider_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_proc = _make_fake_proc(
        stdout=b"provider output",
        stderr=b"unsupported effort",
        returncode=2,
    )

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    result = await run_claude_async(
        "do a thing",
        "/data/repos/demo",
        reasoning_effort="provider-specific-value",
    )

    assert result == (2, "provider output", "unsupported effort")


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
        reasoning_effort="high",
        breach_dir="/tmp/breach",
        breach_run_id="run-123",
        session_threshold=12,
        weekly_threshold=34,
    )

    cmd = captured["cmd"]
    assert "--model" in cmd
    assert cmd[cmd.index("--model") + 1] == "sonnet"
    assert cmd.count("--effort") == 1
    assert cmd[cmd.index("--effort") + 1] == "high"
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

    # When the daemon itself runs the test suite, its own breach env vars
    # leak into os.environ and thus into the spawned subprocess; clear them
    # so the assertions reflect what run_claude_async injects, not what was
    # inherited.
    monkeypatch.delenv("PIPELINE_SESSION_THRESHOLD", raising=False)
    monkeypatch.delenv("PIPELINE_WEEKLY_THRESHOLD", raising=False)
    monkeypatch.delenv("PIPELINE_RUN_ID", raising=False)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)
    monkeypatch.setattr("src.claude_cli.uuid.uuid4", lambda: MagicMock(hex="abcdef1234567890"))

    await run_claude_async("do a thing", "/data/repos/demo", breach_dir="/tmp/breach")

    assert captured["kwargs"]["env"]["PIPELINE_RUN_ID"] == "abcdef123456"
    assert "PIPELINE_SESSION_THRESHOLD" not in captured["kwargs"]["env"]
    assert "PIPELINE_WEEKLY_THRESHOLD" not in captured["kwargs"]["env"]


@pytest.mark.asyncio
async def test_run_claude_async_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_proc = _make_fake_proc()
    _block_until_cleanup(fake_proc)

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    result = await run_claude_async("prompt", "/tmp", timeout=0.01)

    assert result == (-1, "", "Timeout after 0.01s")
    fake_proc.kill.assert_called_once()


@pytest.mark.asyncio
async def test_run_claude_async_timeout_ignores_missing_process_on_kill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_proc = _make_fake_proc()
    _block_until_cleanup(fake_proc)
    fake_proc.kill.side_effect = ProcessLookupError

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    result = await run_claude_async("prompt", "/tmp", timeout=0.01)

    assert result == (-1, "", "Timeout after 0.01s")


@pytest.mark.asyncio
async def test_run_claude_async_cleanup_failure_is_explicit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_proc = _make_fake_proc(stdout=b"partial", stderr=b"rate limit exceeded")
    fake_proc.__dict__["_cleanup_result"] = CleanupResult(
        CleanupStatus.FAILED,
        0,
        True,
        True,
        "ownership proof lost",
    )

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    result = await run_claude_async("prompt", "/tmp", timeout=5)

    assert result == (
        -1,
        "partial\n[captured provider stderr]\nrate limit exceeded",
        "Process supervision failed: ownership proof lost",
    )
    fake_proc.kill.assert_called_once()


@pytest.mark.asyncio
async def test_run_claude_async_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        raise FileNotFoundError(2, "No such file or directory", "claude")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    result = await run_claude_async("prompt", "/tmp")

    assert result == (-1, "", "claude CLI not found")


@pytest.mark.asyncio
async def test_run_claude_async_supervised_launch_failure_is_explicit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def failed_launch(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("ownership proof unavailable")

    monkeypatch.setattr("src.claude_cli.launch_process", failed_launch)

    result = await run_claude_async("prompt", "/tmp")

    assert result == (
        -1,
        "",
        "Process supervision launch failed: RuntimeError: "
        "ownership proof unavailable",
    )
    assert "rate limit" not in result[2].lower()


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
    started = asyncio.Event()
    fake_proc = _make_fake_proc()
    _block_until_cleanup(fake_proc)

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    task = asyncio.create_task(
        run_claude_async(
            "prompt",
            "/tmp",
            on_process_start=lambda _proc: started.set(),
        )
    )
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    fake_proc.kill.assert_called_once()


@pytest.mark.asyncio
async def test_run_claude_async_cancelled_ignores_missing_process_on_kill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = asyncio.Event()
    fake_proc = _make_fake_proc()
    _block_until_cleanup(fake_proc)
    fake_proc.kill.side_effect = ProcessLookupError

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    task = asyncio.create_task(
        run_claude_async(
            "prompt",
            "/tmp",
            on_process_start=lambda _proc: started.set(),
        )
    )
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task



@pytest.mark.asyncio
async def test_run_claude_async_cancellation_records_cleanup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = asyncio.Event()
    fake_proc = _make_fake_proc()
    _block_until_cleanup(fake_proc)
    fake_proc.__dict__["_cleanup_result"] = CleanupResult(
        CleanupStatus.FAILED,
        None,
        True,
        True,
        "cleanup could not prove quiescence",
    )

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    task = asyncio.create_task(
        run_claude_async(
            "prompt",
            "/tmp",
            on_process_start=lambda _proc: started.set(),
        )
    )
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError) as caught:
        await task

    fake_proc.kill.assert_called_once()
    assert any(
        "cleanup could not prove quiescence" in note
        for note in getattr(caught.value, "__notes__", [])
    )


@pytest.mark.asyncio
async def test_diagnose_error_async_skips_system_prompt(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    fake_proc = _make_fake_proc(stdout=b"FIX\nretry", returncode=0)
    started: list[MagicMock] = []
    supervised: list[_FakeSupervisedProcess] = []

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        captured["cmd"] = list(args)
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    code, stdout, _ = await diagnose_error_async(
        "/data/repos/demo",
        "git push failed",
        reasoning_effort="low",
        on_process_start=started.append,
        on_supervised_process_start=supervised.append,
    )

    assert code == 0
    assert stdout == "FIX\nretry"
    cmd = captured["cmd"]
    assert "--append-system-prompt-file" not in cmd
    assert "CLAUDE.md" not in cmd
    assert cmd[cmd.index("--effort") + 1] == "low"
    assert started == [fake_proc]
    assert len(supervised) == 1
    assert supervised[0].process is fake_proc


@pytest.mark.asyncio
async def test_run_claude_async_calls_on_process_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_proc = _make_fake_proc(returncode=0)
    started: list[MagicMock] = []
    supervised: list[_FakeSupervisedProcess] = []

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    result = await run_claude_async(
        "prompt",
        "/tmp",
        on_process_start=lambda proc: started.append(proc),
        on_supervised_process_start=lambda managed: supervised.append(managed),
    )

    assert result == (0, "", "")
    assert started == [fake_proc]
    assert [managed.process for managed in supervised] == [fake_proc]


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
        reasoning_effort="medium",
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
        "reasoning_effort": "medium",
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
    supervised_callback = object()

    async def fake_run_claude_async(*args: Any, **kwargs: Any) -> tuple[int, str, str]:
        captured["kwargs"] = kwargs
        return (0, "ok", "")

    monkeypatch.setattr("src.claude_cli.run_claude_async", fake_run_claude_async)

    await run_planned_pr_async(
        "/data/repos/demo",
        on_process_start=callback,  # type: ignore[arg-type]
        on_supervised_process_start=supervised_callback,  # type: ignore[arg-type]
    )

    assert captured["kwargs"]["on_process_start"] is callback
    assert (
        captured["kwargs"]["on_supervised_process_start"]
        is supervised_callback
    )


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
        reasoning_effort="low",
        timeout=None,
        breach_dir="/tmp/breach",
        breach_run_id="run-456",
        session_threshold=56,
        weekly_threshold=78,
    )

    assert result == (0, "ok", "")
    prompt, repo = captured["args"]
    assert repo == "/data/repos/demo"
    assert prompt.startswith("FIX FEEDBACK\n\nDAEMON INVOCATION")
    assert captured["kwargs"] == {
        "timeout": None,
        "model": "opus",
        "reasoning_effort": "low",
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
    supervised_callback = object()

    async def fake_run_claude_async(*args: Any, **kwargs: Any) -> tuple[int, str, str]:
        captured["kwargs"] = kwargs
        return (0, "ok", "")

    monkeypatch.setattr("src.claude_cli.run_claude_async", fake_run_claude_async)

    await fix_review_async(
        "/data/repos/demo",
        on_process_start=callback,  # type: ignore[arg-type]
        on_supervised_process_start=supervised_callback,  # type: ignore[arg-type]
    )

    assert captured["kwargs"]["on_process_start"] is callback
    assert (
        captured["kwargs"]["on_supervised_process_start"]
        is supervised_callback
    )


@pytest.mark.asyncio
async def test_fix_review_async_appends_extra_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    async def fake_run_claude_async(*args: Any, **kwargs: Any) -> tuple[int, str, str]:
        captured["args"] = args
        return (0, "ok", "")

    monkeypatch.setattr("src.claude_cli.run_claude_async", fake_run_claude_async)

    await fix_review_async(
        "/data/repos/demo",
        extra_context="Latest review feedback:\nP1: fix this",
        pr_id="PR-270",
        task_file="tasks/PR-270.md",
    )

    prompt, repo = captured["args"]
    assert repo == "/data/repos/demo"
    assert prompt.startswith("Task: PR-270\n\nFile: tasks/PR-270.md\n\n")
    assert "\n\nFIX FEEDBACK\n\nDAEMON INVOCATION" in prompt
    assert "one iteration" in prompt
    assert "daemon owns review triggering" in prompt
    assert "Latest review feedback:" in prompt
    assert "P1: fix this" in prompt
    assert prompt.endswith("Latest review feedback:\nP1: fix this")


# --- AUTO PR helpers ---


@pytest.mark.asyncio
async def test_run_auto_pr_async_formats_prompt_with_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    async def fake_run_claude_async(*args: Any, **kwargs: Any) -> tuple[int, str, str]:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return (0, "ok", "")

    monkeypatch.setattr("src.claude_cli.run_claude_async", fake_run_claude_async)

    await run_auto_pr_async(
        "/data/repos/demo",
        "PR-270",
        "tasks/PR-270.md",
        "<body>",
    )

    prompt, repo = captured["args"]
    assert repo == "/data/repos/demo"
    assert prompt.startswith(
        "AUTO PR\nTask: PR-270\nFile: tasks/PR-270.md\n\n<body>\n\n"
    )
    assert "This AUTO PR run was dispatched" in prompt
    assert "OPERATOR-AUTHORIZED EXECUTION POLICY" in prompt
    assert "ready (not draft) PR" in prompt
    assert "scripts/make-review-artifacts.sh" in prompt
    assert "artifacts/pr.patch must be nonempty" in prompt
    assert "repository, base branch, head branch, and HEAD SHA" in prompt
    assert "daemon owns review triggering" in prompt
    assert "Do not trigger or poll review" in prompt
    assert "Never fabricate a PR, push, gate, or approval" in prompt


@pytest.mark.asyncio
async def test_run_auto_pr_async_propagates_model_and_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    async def fake_run_claude_async(*args: Any, **kwargs: Any) -> tuple[int, str, str]:
        captured["kwargs"] = kwargs
        return (0, "ok", "")

    monkeypatch.setattr("src.claude_cli.run_claude_async", fake_run_claude_async)

    await run_auto_pr_async(
        "/data/repos/demo",
        "PR-270",
        "tasks/PR-270.md",
        "<body>",
        model="opus",
        reasoning_effort="high",
        timeout=321,
        breach_dir="/tmp/breach",
        breach_run_id="run-9",
        session_threshold=42,
        weekly_threshold=84,
    )

    assert captured["kwargs"] == {
        "timeout": 321,
        "model": "opus",
        "reasoning_effort": "high",
        "breach_dir": "/tmp/breach",
        "breach_run_id": "run-9",
        "session_threshold": 42,
        "weekly_threshold": 84,
    }


@pytest.mark.asyncio
async def test_run_auto_pr_async_forwards_on_process_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    callback = object()
    supervised_callback = object()

    async def fake_run_claude_async(*args: Any, **kwargs: Any) -> tuple[int, str, str]:
        captured["kwargs"] = kwargs
        return (0, "ok", "")

    monkeypatch.setattr("src.claude_cli.run_claude_async", fake_run_claude_async)

    await run_auto_pr_async(
        "/data/repos/demo",
        "PR-270",
        "tasks/PR-270.md",
        "<body>",
        on_process_start=callback,  # type: ignore[arg-type]
        on_supervised_process_start=supervised_callback,  # type: ignore[arg-type]
    )

    assert captured["kwargs"]["on_process_start"] is callback
    assert (
        captured["kwargs"]["on_supervised_process_start"]
        is supervised_callback
    )


def test_run_auto_pr_sync_formats_prompt_with_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        return _FakeCompletedProcess(stdout="ok", returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = run_auto_pr(
        "/data/repos/demo",
        "PR-270",
        "tasks/PR-270.md",
        "<body>",
        model="opus",
        timeout=321,
        reasoning_effort="high",
    )

    assert result == (0, "ok", "")
    assert captured["cmd"][-1].startswith(
        "AUTO PR\nTask: PR-270\nFile: tasks/PR-270.md\n\n<body>\n\n"
    )
    assert "DAEMON INVOCATION -- PUBLICATION HANDOFF" in captured["cmd"][-1]
    assert "--model" in captured["cmd"]
    assert captured["cmd"][captured["cmd"].index("--model") + 1] == "opus"
    assert captured["cmd"][captured["cmd"].index("--effort") + 1] == "high"
    assert captured["kwargs"]["cwd"] == "/data/repos/demo"
    assert captured["kwargs"]["timeout"] == 321
