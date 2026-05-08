"""Tests for CODING dispatch task metadata."""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

import pytest
from src.cancellation import task_spec_hash_key
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
        hashlib.sha256(task_content.encode("utf-8")).hexdigest()
    )
