"""Tests for src/codex_cli.py."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from src.claude_cli import _CODER_EXECUTION_POLICY as _CLAUDE_EXECUTION_POLICY
from src.codex_cli import (
    _CODER_EXECUTION_POLICY as _CODEX_EXECUTION_POLICY,
)
from src.codex_cli import (
    _build_fix_feedback_prompt,
    diagnose_error_async,
    fix_review_async,
    run_auto_pr_async,
    run_codex_async,
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

    monkeypatch.setattr("src.codex_cli.launch_process", fake_launch)


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
    assert cmd[:6] == [
        "codex",
        "--ask-for-approval",
        "never",
        "exec",
        "--sandbox",
        "danger-full-access",
    ]
    assert cmd[-1] == "do a thing"
    assert captured["kwargs"]["cwd"] == "/data/repos/demo"


@pytest.mark.asyncio
async def test_run_codex_async_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_proc = _make_fake_proc()
    _block_until_cleanup(fake_proc)

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    result = await run_codex_async("prompt", "/tmp", timeout=0.01)

    assert result == (-1, "", "Timeout after 0.01s")
    fake_proc.kill.assert_called_once()


@pytest.mark.asyncio
async def test_run_codex_async_timeout_ignores_missing_process_on_kill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_proc = _make_fake_proc()
    _block_until_cleanup(fake_proc)
    fake_proc.kill.side_effect = ProcessLookupError

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    result = await run_codex_async("prompt", "/tmp", timeout=0.01)

    assert result == (-1, "", "Timeout after 0.01s")
    fake_proc.kill.assert_called_once()


@pytest.mark.asyncio
async def test_run_codex_async_cleanup_failure_is_explicit(
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

    result = await run_codex_async("prompt", "/tmp", timeout=5)

    assert result == (
        -1,
        "partial\n[captured provider stderr]\nrate limit exceeded",
        "Process supervision failed: ownership proof lost",
    )
    fake_proc.kill.assert_called_once()


@pytest.mark.asyncio
async def test_run_codex_async_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        raise FileNotFoundError(2, "No such file or directory", "codex")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    result = await run_codex_async("prompt", "/tmp")

    assert result == (-1, "", "codex CLI not found")


@pytest.mark.asyncio
async def test_run_codex_async_supervised_launch_failure_is_explicit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def failed_launch(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("ownership proof unavailable")

    monkeypatch.setattr("src.codex_cli.launch_process", failed_launch)

    result = await run_codex_async("prompt", "/tmp")

    assert result == (
        -1,
        "",
        "Process supervision launch failed: RuntimeError: "
        "ownership proof unavailable",
    )
    assert "rate limit" not in result[2].lower()


@pytest.mark.asyncio
async def test_run_codex_async_calls_on_process_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_proc = _make_fake_proc(returncode=0)
    started: list[MagicMock] = []
    supervised: list[_FakeSupervisedProcess] = []

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    result = await run_codex_async(
        "prompt",
        "/tmp",
        on_process_start=lambda proc: started.append(proc),
        on_supervised_process_start=lambda managed: supervised.append(managed),
    )

    assert result == (0, "", "")
    assert started == [fake_proc]
    assert [managed.process for managed in supervised] == [fake_proc]


@pytest.mark.asyncio
async def test_run_planned_pr_async_calls_exec_with_docker_sandbox(
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
    assert cmd[:6] == [
        "codex",
        "--ask-for-approval",
        "never",
        "exec",
        "--sandbox",
        "danger-full-access",
    ]
    assert "--model" in cmd
    assert cmd[cmd.index("--model") + 1] == "o3"
    assert cmd[-1] == "PLANNED PR"
    assert "DAEMON INVOCATION" not in cmd[-1]


@pytest.mark.asyncio
async def test_run_planned_pr_async_forwards_on_process_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_proc = _make_fake_proc(returncode=0)
    started: list[MagicMock] = []
    supervised: list[_FakeSupervisedProcess] = []

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    await run_planned_pr_async(
        "/data/repos/demo",
        on_process_start=lambda proc: started.append(proc),
        on_supervised_process_start=lambda managed: supervised.append(managed),
    )

    assert started == [fake_proc]
    assert [managed.process for managed in supervised] == [fake_proc]


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

    await fix_review_async(
        "/data/repos/demo",
        extra_context="Latest review feedback:\nP1: fix this",
        pr_id="PR-270",
        task_file="tasks/PR-270.md",
    )

    cmd = captured["cmd"]
    assert cmd[:6] == [
        "codex",
        "--ask-for-approval",
        "never",
        "exec",
        "--sandbox",
        "danger-full-access",
    ]
    prompt = cmd[-1]
    assert prompt.startswith("Task: PR-270\n\nFile: tasks/PR-270.md\n\n")
    assert "\n\nFIX FEEDBACK\n\nDAEMON INVOCATION" in prompt
    assert "This FIX FEEDBACK run was dispatched" in prompt
    assert "OPERATOR-AUTHORIZED EXECUTION POLICY" in prompt
    assert "one iteration" in prompt
    assert "scripts/make-review-artifacts.sh" in prompt
    assert "remote PR HEAD is the pushed local HEAD" in prompt
    assert "daemon owns review triggering" in prompt
    assert "Do not wait for a new review" in prompt
    assert prompt.endswith("Latest review feedback:\nP1: fix this")


@pytest.mark.asyncio
async def test_fix_review_async_forwards_on_process_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_proc = _make_fake_proc(returncode=0)
    started: list[MagicMock] = []
    supervised: list[_FakeSupervisedProcess] = []

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    await fix_review_async(
        "/data/repos/demo",
        on_process_start=lambda proc: started.append(proc),
        on_supervised_process_start=lambda managed: supervised.append(managed),
    )

    assert started == [fake_proc]
    assert [managed.process for managed in supervised] == [fake_proc]


@pytest.mark.asyncio
async def test_fix_review_async_appends_extra_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    fake_proc = _make_fake_proc(returncode=0)

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        captured["cmd"] = list(args)
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    await fix_review_async(
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


def test_daemon_execution_policy_matches_claude() -> None:
    """Both coder providers receive one execution contract."""
    assert _CODEX_EXECUTION_POLICY == _CLAUDE_EXECUTION_POLICY


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


@pytest.mark.asyncio
async def test_diagnose_error_async_uses_expected_prompt_and_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    fake_proc = _make_fake_proc(returncode=0)
    started: list[MagicMock] = []
    supervised: list[_FakeSupervisedProcess] = []

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        captured["cmd"] = list(args)
        captured["kwargs"] = kwargs
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    await diagnose_error_async(
        "/data/repos/demo",
        "broken CI",
        model="gpt-5.4",
        on_process_start=started.append,
        on_supervised_process_start=supervised.append,
    )

    cmd = captured["cmd"]
    assert cmd[:6] == [
        "codex",
        "--ask-for-approval",
        "never",
        "exec",
        "--sandbox",
        "danger-full-access",
    ]
    assert "--model" in cmd
    assert cmd[cmd.index("--model") + 1] == "gpt-5.4"
    assert "Error context: broken CI" in cmd[-1]
    assert "FIX, SKIP, or ESCALATE" in cmd[-1]
    assert "DAEMON INVOCATION" not in cmd[-1]
    assert started == [fake_proc]
    assert len(supervised) == 1
    assert supervised[0].process is fake_proc


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


@pytest.mark.asyncio
async def test_run_codex_async_cancellation_kills_process(
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
        run_codex_async(
            "prompt",
            "/tmp",
            timeout=5,
            on_process_start=lambda _proc: started.set(),
        )
    )
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    fake_proc.kill.assert_called_once()


@pytest.mark.asyncio
async def test_run_auto_pr_async_formats_prompt_with_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    fake_proc = _make_fake_proc(returncode=0)

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        captured["cmd"] = list(args)
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    await run_auto_pr_async(
        "/data/repos/demo",
        "PR-270",
        "tasks/PR-270.md",
        "<body>",
        model="gpt-5.4",
        timeout=321,
    )

    cmd = captured["cmd"]
    assert cmd[:6] == [
        "codex",
        "--ask-for-approval",
        "never",
        "exec",
        "--sandbox",
        "danger-full-access",
    ]
    assert "--model" in cmd
    assert cmd[cmd.index("--model") + 1] == "gpt-5.4"
    assert cmd[-1].startswith(
        "AUTO PR\nTask: PR-270\nFile: tasks/PR-270.md\n\n<body>\n\n"
    )
    assert "This AUTO PR run was dispatched" in cmd[-1]
    assert "OPERATOR-AUTHORIZED EXECUTION POLICY" in cmd[-1]
    assert "ready (not draft) PR" in cmd[-1]
    assert "scripts/make-review-artifacts.sh" in cmd[-1]
    assert "artifacts/pr.patch must be nonempty" in cmd[-1]
    assert "repository, base branch, head branch, and HEAD SHA" in cmd[-1]
    assert "daemon owns review triggering" in cmd[-1]
    assert "Do not trigger or poll review" in cmd[-1]
    assert "Never fabricate a PR, push, gate, or approval" in cmd[-1]


@pytest.mark.asyncio
async def test_run_auto_pr_async_forwards_on_process_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_proc = _make_fake_proc(returncode=0)
    started: list[MagicMock] = []
    supervised: list[_FakeSupervisedProcess] = []

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    await run_auto_pr_async(
        "/data/repos/demo",
        "PR-270",
        "tasks/PR-270.md",
        "<body>",
        on_process_start=lambda proc: started.append(proc),
        on_supervised_process_start=lambda managed: supervised.append(managed),
    )

    assert started == [fake_proc]
    assert [managed.process for managed in supervised] == [fake_proc]


@pytest.mark.asyncio
async def test_run_auto_pr_async_ignores_extra_kwargs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Extra kwargs (breach_dir etc.) from the runner are accepted via **_kwargs."""
    fake_proc = _make_fake_proc(returncode=0)

    async def fake_create(*args: Any, **kwargs: Any) -> MagicMock:
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)

    result = await run_auto_pr_async(
        "/tmp",
        "PR-270",
        "tasks/PR-270.md",
        "<body>",
        breach_dir="/tmp/breach",
        breach_run_id="abc",
        session_threshold=95,
        weekly_threshold=100,
    )
    assert result[0] == 0


@pytest.mark.asyncio
async def test_run_codex_async_cancellation_records_cleanup_failure(
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
        run_codex_async(
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
