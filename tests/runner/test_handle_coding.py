"""Tests for CODING dispatch task metadata."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from src.cancellation import task_spec_content_hash, task_spec_hash_key
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus
from src.usage import UsageSnapshot

from tests.runner import _helpers as h


def test_handle_coding_skipped_when_spend_ceiling_exceeded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    called: list[str] = []

    async def fake_auto_pr(*args: object, **kwargs: object) -> tuple[int, str, str]:
        called.append("run_auto_pr")
        return (0, "ok", "")

    monkeypatch.setattr(h.claude_cli, "run_auto_pr_async", fake_auto_pr)

    runner = h._make_runner()
    runner.app_config.daemon.spend_ceiling_session_percent = 70
    runner._claude_usage_provider = h._FakeUsageProvider(
        snapshot=UsageSnapshot(
            session_percent=75,
            session_resets_at=1_900_000_000,
            weekly_percent=10,
            weekly_resets_at=1_900_000_600,
            fetched_at=0,
        )
    )
    runner.state.state = PipelineState.CODING
    runner.state.current_task = QueueTask(
        pr_id="PR-309",
        title="Spend ceiling",
        status=TaskStatus.DOING,
        branch="pr-309-token-spend-ceiling",
        task_file="tasks/PR-309.md",
    )

    asyncio.run(runner.handle_coding())

    assert called == []
    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.error_message is None


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


def test_dispatch_continues_when_previous_task_hash_read_fails(
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

    task_content = "# PR-001: Sample\n\nBranch: pr-001\n"
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
    original_get = runner.redis.get

    async def fail_hash_get(key: str) -> str | None:
        if key == task_spec_hash_key("octo__demo", "PR-001"):
            raise RuntimeError("read failed")
        return await original_get(key)

    runner.redis.get = fail_hash_get  # type: ignore[method-assign]

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert runner._current_run_record is not None
    assert runner._current_run_record.attempt_index == 1


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


def test_coding_post_coder_guardrail_violation_transitions_to_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.state.state = PipelineState.CODING
    transition_calls: list[tuple[str, str]] = []

    async def fake_transition_to_error(
        message: str,
        **kwargs: object,
    ) -> None:
        transition_calls.append((message, str(kwargs["log_prefix"])))

    async def fake_save_cli_log(*args: object, **kwargs: object) -> None:
        return None

    def fail_get_open_prs(*args: object, **kwargs: object) -> list[PRInfo]:
        raise AssertionError("PR lookup should not run after guardrail violation")

    monkeypatch.setattr(runner, "_transition_to_error", fake_transition_to_error)
    monkeypatch.setattr(runner, "_save_cli_log", fake_save_cli_log)
    monkeypatch.setattr("src.daemon.handlers.coding.gh_prs.get_open_prs", fail_get_open_prs)

    asyncio.run(
        runner._post_coder_resolution(
            "claude",
            0,
            "gh repo create octo/demo\n",
            "",
            target_branch="pr-289a",
            current_pr_id="PR-289a",
        )
    )

    assert transition_calls
    assert transition_calls[0][0].startswith("GUARDRAIL: repo_create:")
    assert transition_calls[0][1] == "[CODING]"


def test_coding_post_coder_guardrail_violation_scans_stderr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.state.state = PipelineState.CODING
    transition_calls: list[str] = []

    async def fake_transition_to_error(
        message: str,
        **kwargs: object,
    ) -> None:
        transition_calls.append(message)

    async def fake_save_cli_log(*args: object, **kwargs: object) -> None:
        return None

    def fail_get_open_prs(*args: object, **kwargs: object) -> list[PRInfo]:
        raise AssertionError("PR lookup should not run after guardrail violation")

    monkeypatch.setattr(runner, "_transition_to_error", fake_transition_to_error)
    monkeypatch.setattr(runner, "_save_cli_log", fake_save_cli_log)
    monkeypatch.setattr("src.daemon.handlers.coding.gh_prs.get_open_prs", fail_get_open_prs)

    asyncio.run(
        runner._post_coder_resolution(
            "claude",
            0,
            "ordinary stdout\n",
            "++ gh repo create octo/demo\n",
            target_branch="pr-289a",
            current_pr_id="PR-289a",
        )
    )

    assert transition_calls
    assert transition_calls[0].startswith("GUARDRAIL: repo_create:")


def test_coding_post_coder_guardrail_violation_honors_deferred_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.state.state = PipelineState.CODING
    runner.redis.store[f"control:{runner.name}:stop"] = "1"
    transition_calls: list[str] = []

    async def fake_transition_to_error(
        message: str,
        **kwargs: object,
    ) -> None:
        transition_calls.append(message)

    async def fake_save_cli_log(*args: object, **kwargs: object) -> None:
        return None

    def fail_get_open_prs(*args: object, **kwargs: object) -> list[PRInfo]:
        raise AssertionError("PR lookup should not run after deferred stop")

    monkeypatch.setattr(runner, "_transition_to_error", fake_transition_to_error)
    monkeypatch.setattr(runner, "_save_cli_log", fake_save_cli_log)
    monkeypatch.setattr("src.daemon.handlers.coding.gh_prs.get_open_prs", fail_get_open_prs)

    asyncio.run(
        runner._post_coder_resolution(
            "claude",
            0,
            "gh repo create octo/demo\n",
            "",
            target_branch="pr-289a",
            current_pr_id="PR-289a",
        )
    )

    assert transition_calls == []
    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.user_paused is True
    assert runner.state.error_message is None
    assert "PR-289a" in runner._user_stopped_task_pr_ids


def test_coding_post_coder_clean_stdout_proceeds_normally(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.state.state = PipelineState.CODING
    posted: list[int] = []

    async def fake_save_cli_log(*args: object, **kwargs: object) -> None:
        return None

    monkeypatch.setattr(runner, "_save_cli_log", fake_save_cli_log)
    monkeypatch.setattr(
        "src.daemon.handlers.coding.gh_prs.get_open_prs",
        lambda *args, **kwargs: [PRInfo(number=42, branch="pr-289a")],
    )
    monkeypatch.setattr(runner, "_post_codex_review", lambda number: posted.append(number))

    asyncio.run(
        runner._post_coder_resolution(
            "claude",
            0,
            "python -m pytest -q\nscripts/ci.sh exited 0\n",
            "",
            target_branch="pr-289a",
            current_pr_id="PR-289a",
        )
    )

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr == PRInfo(number=42, branch="pr-289a")
    assert posted == [42]
