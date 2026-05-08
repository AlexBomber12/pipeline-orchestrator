"""Tests for CODING dispatch task metadata."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from src.cancellation import task_spec_content_hash, task_spec_hash_key
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus

from tests.runner import _helpers as h


def test_dispatch_persists_task_spec_hash(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch, stub_auto_pr_read=False)

    async def fake_auto_pr(*args: object, **kwargs: object) -> tuple[int, str, str]:
        return (0, "ok", "")

    monkeypatch.setattr(h.claude_cli, "run_auto_pr_async", fake_auto_pr)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [PRInfo(number=42, branch="pr-001")],
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )

    task_content = (
        "# PR-001: Sample\n\n"
        "Branch: pr-001\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n"
    )
    task_file = tmp_path / "tasks" / "PR-001.md"
    task_file.parent.mkdir(parents=True)
    task_file.write_text(task_content, encoding="utf-8")

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.state = PipelineState.CODING
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="Sample",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )

    asyncio.run(runner.handle_coding())

    assert runner.redis.store[task_spec_hash_key("octo__demo", "PR-001")] == (
        task_spec_content_hash(task_content)
    )


def test_dispatch_task_spec_hash_ignores_frontmatter_status(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch, stub_auto_pr_read=False)

    async def fake_auto_pr(*args: object, **kwargs: object) -> tuple[int, str, str]:
        return (0, "ok", "")

    monkeypatch.setattr(h.claude_cli, "run_auto_pr_async", fake_auto_pr)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [PRInfo(number=42, branch="pr-001")],
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )

    task_content = (
        "---\n"
        "status: ERROR\n"
        "---\n\n"
        "# PR-001: Sample\n\n"
        "Branch: pr-001\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n"
    )
    task_file = tmp_path / "tasks" / "PR-001.md"
    task_file.parent.mkdir(parents=True)
    task_file.write_text(task_content, encoding="utf-8")

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.state = PipelineState.CODING
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="Sample",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )

    asyncio.run(runner.handle_coding())

    todo_content = task_content.replace("status: ERROR", "status: TODO")
    assert (
        runner.redis.store[task_spec_hash_key("octo__demo", "PR-001")]
        == task_spec_content_hash(todo_content)
    )


def test_handle_coding_transitions_error_when_task_hash_persist_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    h._patch_subprocess(monkeypatch, stub_auto_pr_read=False)
    coder_called = False

    async def fake_auto_pr(*args: object, **kwargs: object) -> tuple[int, str, str]:
        nonlocal coder_called
        coder_called = True
        return (0, "ok", "")

    monkeypatch.setattr(h.claude_cli, "run_auto_pr_async", fake_auto_pr)

    task_file = tmp_path / "tasks" / "PR-001.md"
    task_file.parent.mkdir(parents=True)
    task_file.write_text(
        "# PR-001: Sample\n\n"
        "Branch: pr-001\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.state = PipelineState.CODING
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="Sample",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )

    async def fail_set(
        key: str,
        value: str,
        ex: int | None = None,
        nx: bool = False,
    ) -> bool | None:
        if key == task_spec_hash_key("octo__demo", "PR-001"):
            raise RuntimeError("redis down")
        return await original_set(key, value, ex=ex, nx=nx)

    original_set = runner.redis.set
    runner.redis.set = fail_set  # type: ignore[method-assign]

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == (
        "Cannot persist task spec hash for PR-001: redis down"
    )
    assert not coder_called
