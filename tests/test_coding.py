from __future__ import annotations

import asyncio
import subprocess
from typing import Any

import pytest
from src import github_client
from src.daemon import git_ops
from src.daemon.handlers import coding as coding_module
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus

from tests import test_runner as h


def _runner(
    monkeypatch: pytest.MonkeyPatch,
    *,
    open_prs_after_create: list[PRInfo] | None = None,
    open_prs_initial: list[PRInfo] | None = None,
    raise_on_post_create_list: bool = False,
):
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        h.claude_cli, "run_planned_pr_async", h._async_cli_result(0, "ok", "")
    )
    pr_list_calls = {"n": 0}

    def _fake_open_prs(repo: str, **kw: Any) -> list[PRInfo]:
        pr_list_calls["n"] += 1
        if pr_list_calls["n"] <= 3:
            return open_prs_initial or []
        if raise_on_post_create_list:
            raise RuntimeError("list-post-create boom")
        return open_prs_after_create or []

    monkeypatch.setattr(
        h.runner_module.github_client, "get_open_prs", _fake_open_prs
    )

    async def _sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(h.runner_module.asyncio, "sleep", _sleep)
    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="Sample task",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )
    runner._post_codex_review = lambda pr_number: True  # type: ignore[method-assign]
    return runner


def _patch_branch_state(
    monkeypatch: pytest.MonkeyPatch,
    *,
    local_exists: bool,
    remote_exists: bool,
) -> None:
    def fake_git(repo_path: str, *args: str, **kwargs: Any):
        if args[:3] == ("rev-parse", "--verify", "--quiet"):
            rc = 0 if local_exists else 1
            return subprocess.CompletedProcess(
                args=list(args), returncode=rc, stdout="", stderr=""
            )
        if args[:1] == ("ls-remote",):
            if remote_exists:
                return subprocess.CompletedProcess(
                    args=list(args),
                    returncode=0,
                    stdout="abcdef refs/heads/pr-001\n",
                    stderr="",
                )
            return subprocess.CompletedProcess(
                args=list(args), returncode=2, stdout="", stderr=""
            )
        return subprocess.CompletedProcess(
            args=list(args), returncode=0, stdout="", stderr=""
        )

    monkeypatch.setattr(git_ops, "_git", fake_git)


def test_happy_path_pr_exists_transitions_to_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = PRInfo(number=42, branch="pr-001")
    runner = _runner(monkeypatch, open_prs_initial=[pr])
    asyncio.run(runner.handle_coding())
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 42


def test_case_a_no_branch_no_remote_marks_hung(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(monkeypatch)
    _patch_branch_state(monkeypatch, local_exists=False, remote_exists=False)
    asyncio.run(runner.handle_coding())
    assert runner.state.state == PipelineState.HUNG
    assert "did nothing" in (runner.state.error_message or "")
    assert any(
        "did nothing" in entry["event"] for entry in runner.state.history
    )


def test_case_b_local_branch_only_marks_hung(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(monkeypatch)
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=False)
    asyncio.run(runner.handle_coding())
    assert runner.state.state == PipelineState.HUNG
    assert "no push" in (runner.state.error_message or "")


def test_case_c_remote_branch_no_pr_daemon_creates_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = PRInfo(number=99, branch="pr-001")
    runner = _runner(monkeypatch, open_prs_after_create=[created])
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=True)

    create_calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **_kw: Any):
        create_calls.append(args)
        return ""

    monkeypatch.setattr(github_client, "run_gh", fake_run_gh)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 99
    assert create_calls and create_calls[0][:2] == ["pr", "create"]
    assert "--head" in create_calls[0]
    assert "pr-001" in create_calls[0]
    assert any(
        "daemon creating PR" in entry["event"] for entry in runner.state.history
    )


def test_case_c_create_pr_failure_marks_hung(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(monkeypatch)
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=True)

    def fake_run_gh(args: list[str], repo: str | None = None, **_kw: Any):
        raise RuntimeError("gh boom")

    monkeypatch.setattr(github_client, "run_gh", fake_run_gh)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.HUNG
    assert "Daemon PR creation failed" in (runner.state.error_message or "")


def test_case_c_post_create_list_failure_marks_hung(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(monkeypatch, raise_on_post_create_list=True)
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=True)
    monkeypatch.setattr(
        github_client, "run_gh", lambda *a, **kw: ""
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.HUNG
    assert "Daemon-created PR not visible" in (runner.state.error_message or "")


def test_case_c_pr_not_found_after_create_marks_hung(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(monkeypatch, open_prs_after_create=[])
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=True)
    monkeypatch.setattr(
        github_client, "run_gh", lambda *a, **kw: ""
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.HUNG
    assert "Daemon-created PR not found" in (runner.state.error_message or "")


def test_local_branch_exists_handles_subprocess_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_git(*args: Any, **kwargs: Any):
        raise OSError("git not found")

    monkeypatch.setattr(git_ops, "_git", fake_git)
    assert coding_module._local_branch_exists("/tmp/nope", "any") is False


def test_remote_branch_exists_handles_subprocess_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_git(*args: Any, **kwargs: Any):
        raise subprocess.TimeoutExpired(cmd=["git"], timeout=30)

    monkeypatch.setattr(git_ops, "_git", fake_git)
    assert coding_module._remote_branch_exists("/tmp/nope", "any") is False


def test_remote_branch_exists_returns_false_on_empty_stdout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_git(*args: Any, **kwargs: Any):
        return subprocess.CompletedProcess(
            args=["git"], returncode=0, stdout="", stderr=""
        )

    monkeypatch.setattr(git_ops, "_git", fake_git)
    assert coding_module._remote_branch_exists("/tmp/nope", "any") is False


def test_daemon_create_pr_uses_pr_id_when_title_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = PRInfo(number=7, branch="pr-001")
    runner = _runner(monkeypatch, open_prs_after_create=[created])
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="",
        status=TaskStatus.DOING,
        branch="pr-001",
    )
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=True)

    captured: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **_kw: Any):
        captured.append(args)
        return ""

    monkeypatch.setattr(github_client, "run_gh", fake_run_gh)

    asyncio.run(runner.handle_coding())
    assert runner.state.state == PipelineState.WATCH
    create_args = next(
        args for args in captured if args[:2] == ["pr", "create"]
    )
    title_idx = create_args.index("--title") + 1
    assert create_args[title_idx] == "PR-001"
    body_idx = create_args.index("--body") + 1
    assert "with no PR" in create_args[body_idx]
