"""``_commit_and_park_in_error`` primitive + status-write fallback tests.

PR-317 deleted the prior ``_escalate_and_skip`` primitive. The remaining
tests in this file cover (a) the status-write fallback bookkeeping that
``_commit_and_park_in_error`` relies on, (b) the merge-side
``_commit_task_status_change`` durability path, and (c) the few
filesystem-level commit guarantees that span multiple call sites.
The primitive-direct behavior is now exercised in
``test_commit_and_park_in_error.py``.
"""

from __future__ import annotations

import asyncio
import subprocess
from pathlib import Path
from typing import Any

import pytest
from src.keyspace import status_write_failed_tasks
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus

from tests.runner import _helpers as h


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def _make_repo_with_task(tmp_path: Path, pr_id: str) -> Path:
    remote = tmp_path / "remote.git"
    work = tmp_path / "work"
    subprocess.run(["git", "init", "--bare", remote], check=True)
    subprocess.run(["git", "clone", str(remote), work], check=True)
    _git(work, "config", "user.email", "test@example.com")
    _git(work, "config", "user.name", "Test User")
    _git(work, "checkout", "-b", "main")
    task_dir = work / "tasks"
    task_dir.mkdir()
    (task_dir / f"{pr_id}.md").write_text(
        "---\nstatus: TODO\n---\n\n"
        f"# {pr_id}: Test\n\n"
        "Branch: pr-test\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: codex\n",
        encoding="utf-8",
    )
    _git(work, "add", "tasks")
    _git(work, "commit", "-m", "seed")
    _git(work, "push", "-u", "origin", "main")
    _git(work, "checkout", "-b", "pr-test")
    _git(work, "push", "-u", "origin", "pr-test")
    _git(work, "checkout", "main")
    return work


def _commit_pr_branch_task_status(repo: Path, pr_id: str, status: str) -> None:
    _git(repo, "checkout", "pr-test")
    task_file = repo / "tasks" / f"{pr_id}.md"
    task_file.write_text(
        task_file.read_text(encoding="utf-8").replace(
            "status: TODO", f"status: {status}", 1
        ),
        encoding="utf-8",
    )
    _git(repo, "add", str(task_file.relative_to(repo)))
    _git(repo, "commit", "-m", f"mark {pr_id} {status} on pr branch")
    _git(repo, "push", "origin", "pr-test")


def _install_publish_state_spy(runner: Any) -> list[None]:
    """Replace ``publish_state`` with an awaitable spy and return the call log."""
    calls: list[None] = []

    async def fake_publish() -> None:
        calls.append(None)

    runner.publish_state = fake_publish  # type: ignore[method-assign]
    return calls


def _patch_label_calls(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    """Capture every ``run_gh`` invocation made by ``_ensure_escalated_label``."""
    gh_calls: list[list[str]] = []

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        gh_calls.append(cmd)
        return ""

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    return gh_calls


def test_commit_and_park_in_error_sets_status_write_fallback_when_commit_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed task status write must still park the task until re-upload."""
    _patch_label_calls(monkeypatch)
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=501, branch="pr-501")
    runner.state.current_task = QueueTask(
        pr_id="PR-501",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-501",
        task_file="tasks/PR-501.md",
    )
    _install_publish_state_spy(runner)

    async def fake_commit(*args: Any, **kwargs: Any) -> bool:
        return False

    monkeypatch.setattr(runner, "_commit_task_status_change", fake_commit)

    asyncio.run(
        runner._commit_and_park_in_error(
            "park after write failure",
            subsource="infra_failure",
        )
    )

    assert runner.state.state == PipelineState.ERROR
    assert runner._status_write_failed_task_pr_ids == {"PR-501"}
    assert runner.redis.store[status_write_failed_tasks(runner.name)] == (
        '["PR-501"]'
    )
    assert any(
        "using in-memory ERROR fallback for PR-501" in entry["event"]
        for entry in runner.state.history
    )


def test_mark_status_write_failed_task_ignores_missing_pr_id() -> None:
    runner = h._make_runner()

    asyncio.run(runner._mark_status_write_failed_task(object()))

    assert runner._status_write_failed_task_pr_ids == set()
    assert runner.state.history == []


def test_persist_status_write_failed_task_ids_deletes_empty_set() -> None:
    runner = h._make_runner()
    key = status_write_failed_tasks(runner.name)
    runner.redis.store[key] = '["PR-001"]'

    asyncio.run(runner._persist_status_write_failed_task_pr_ids())

    assert key not in runner.redis.store
    assert key in runner.redis.deleted


def test_persist_status_write_failed_task_ids_logs_redis_failure() -> None:
    runner = h._make_runner()

    async def fail_delete(key: str) -> int:
        raise RuntimeError("redis down")

    runner.redis.delete = fail_delete  # type: ignore[method-assign]

    asyncio.run(runner._persist_status_write_failed_task_pr_ids())

    assert runner._status_write_failed_task_pr_ids_persist_failed is True
    assert any(
        "failed to persist status-write fallback markers: redis down"
        in entry["event"]
        for entry in runner.state.history
    )


@pytest.mark.parametrize(
    "raw",
    [
        b'["PR-001", "", 12, "PR-002"]',
        "",
        "{not json",
        '{"not":"a list"}',
    ],
)
def test_hydrate_status_write_failed_task_ids_handles_stored_shapes(
    raw: bytes | str,
) -> None:
    runner = h._make_runner()
    runner.redis.store[status_write_failed_tasks(runner.name)] = raw  # type: ignore[assignment]

    asyncio.run(runner._hydrate_status_write_failed_task_pr_ids())

    if isinstance(raw, bytes):
        assert runner._status_write_failed_task_pr_ids == {"PR-001", "PR-002"}
    else:
        assert runner._status_write_failed_task_pr_ids == set()


def test_hydrate_status_write_failed_task_ids_replaces_stale_memory() -> None:
    runner = h._make_runner()
    runner._status_write_failed_task_pr_ids = {"PR-OLD"}
    runner.redis.store[status_write_failed_tasks(runner.name)] = '["PR-NEW"]'

    asyncio.run(runner._hydrate_status_write_failed_task_pr_ids())

    assert runner._status_write_failed_task_pr_ids == {"PR-NEW"}


def test_hydrate_status_write_failed_task_ids_keeps_memory_on_missing_keys() -> None:
    runner = h._make_runner()
    runner._status_write_failed_task_pr_ids = {"PR-OLD"}
    runner._status_write_failed_task_pr_ids_persist_failed = True

    asyncio.run(runner._hydrate_status_write_failed_task_pr_ids())

    assert runner._status_write_failed_task_pr_ids == {"PR-OLD"}


def test_hydrate_status_write_failed_task_ids_keeps_memory_on_redis_failure() -> None:
    runner = h._make_runner()
    runner._status_write_failed_task_pr_ids = {"PR-OLD"}

    async def fail_get(key: str) -> str | None:
        raise RuntimeError("redis down")

    runner.redis.get = fail_get  # type: ignore[method-assign]

    asyncio.run(runner._hydrate_status_write_failed_task_pr_ids())

    assert runner._status_write_failed_task_pr_ids == {"PR-OLD"}


def test_clear_status_write_failed_task_ids_logs_legacy_delete_failure() -> None:
    runner = h._make_runner()
    runner._status_write_failed_task_pr_ids.add("PR-001")
    legacy_key = status_write_failed_tasks(runner.name).replace(
        "status_write_failed_tasks:",
        "recovered_tasks:",
    )

    async def fail_delete(key: str) -> int:
        raise RuntimeError("redis down")

    runner.redis.delete = fail_delete  # type: ignore[method-assign]

    asyncio.run(runner._clear_status_write_failed_task_ids({"PR-001"}))

    assert runner._status_write_failed_task_pr_ids == set()
    assert runner.redis.store[legacy_key] == "[]"
    assert any(
        "failed to clear legacy status-write fallback markers: redis down"
        in entry["event"]
        for entry in runner.state.history
    )


def test_merge_writes_status_done_to_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo = _make_repo_with_task(tmp_path, "PR-701")
    monkeypatch.setattr("src.github.prs.merge_pr", lambda repo, num: None)
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *args, **kwargs: "")

    runner = h._make_runner()
    runner.repo_path = str(repo)
    runner.state.state = PipelineState.MERGE
    runner.state.current_pr = PRInfo(number=701, branch="pr-test")
    runner.state.current_task = QueueTask(
        pr_id="PR-701",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-test",
        task_file="tasks/PR-701.md",
    )

    asyncio.run(runner.handle_merge())

    task_text = (repo / "tasks" / "PR-701.md").read_text(encoding="utf-8")
    assert task_text.startswith("---\nstatus: DONE\n---\n")
    assert "[STATUS] PR-701 marked DONE: PR merged" in _git(
        repo,
        "log",
        "--oneline",
        "-1",
    )


def test_merge_logs_status_commit_exception(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo = _make_repo_with_task(tmp_path, "PR-701")
    monkeypatch.setattr("src.github.prs.merge_pr", lambda repo, num: None)
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *args, **kwargs: "")

    runner = h._make_runner()
    runner.repo_path = str(repo)
    runner.state.state = PipelineState.MERGE
    runner.state.current_pr = PRInfo(number=701, branch="pr-test")
    runner.state.current_task = QueueTask(
        pr_id="PR-701",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-test",
        task_file="tasks/PR-701.md",
    )

    async def fail_commit(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("checkout refused")

    runner._commit_task_status_change = fail_commit  # type: ignore[method-assign]

    asyncio.run(runner.handle_merge())

    assert any(
        "[ERROR] Failed to write status:DONE to tasks/PR-701.md: checkout refused"
        in event["event"]
        for event in runner.state.history
    )


def test_merge_status_commit_ignores_divergent_pr_branch_task_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo = _make_repo_with_task(tmp_path, "PR-701")
    _commit_pr_branch_task_status(repo, "PR-701", "DOING")
    monkeypatch.setattr("src.github.prs.merge_pr", lambda repo, num: None)
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *args, **kwargs: "")

    runner = h._make_runner()
    runner.repo_path = str(repo)
    runner.state.state = PipelineState.MERGE
    runner.state.current_pr = PRInfo(number=701, branch="pr-test")
    runner.state.current_task = QueueTask(
        pr_id="PR-701",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-test",
        task_file="tasks/PR-701.md",
    )

    asyncio.run(runner.handle_merge())

    task_text = (repo / "tasks" / "PR-701.md").read_text(encoding="utf-8")
    assert task_text.startswith("---\nstatus: DONE\n---\n")
    assert "[STATUS] PR-701 marked DONE: PR merged" in _git(
        repo,
        "log",
        "--oneline",
        "-1",
    )


def test_commit_task_status_change_push_failure_logs_warning(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    task_dir = tmp_path / "tasks"
    task_dir.mkdir()
    (task_dir / "PR-702.md").write_text(
        "---\nstatus: TODO\n---\n\nBody\n",
        encoding="utf-8",
    )
    calls: list[tuple[str, ...]] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        calls.append(args)
        if args[:1] == ("push",):
            raise RuntimeError("push denied")
        if args[:3] == ("diff", "--cached", "--quiet"):
            return h._FakeCompletedProcess(returncode=1)
        return h._FakeCompletedProcess(returncode=0)

    monkeypatch.setattr("src.daemon.git_ops._git", fake_git)
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    task = QueueTask(
        pr_id="PR-702",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-test",
        task_file="tasks/PR-702.md",
    )

    asyncio.run(runner._commit_task_status_change(task, "ERROR", "failed hard"))

    assert ("push", "origin", "main") in calls
    assert any(
        "failed to commit ERROR status for tasks/PR-702.md: push denied"
        in event["event"]
        for event in runner.state.history
    )


def test_commit_task_status_change_skips_missing_task_file() -> None:
    runner = h._make_runner()
    task = QueueTask(
        pr_id="PR-703",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-test",
    )

    asyncio.run(runner._commit_task_status_change(task, "ERROR", "no file"))

    assert runner.state.history == []


def test_commit_task_status_change_rejects_unsafe_task_path() -> None:
    runner = h._make_runner()
    task = QueueTask(
        pr_id="PR-704",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-test",
        task_file="../PR-704.md",
    )

    asyncio.run(runner._commit_task_status_change(task, "ERROR", "bad path"))

    assert any(
        "refusing to commit unsafe task path '../PR-704.md'"
        in event["event"]
        for event in runner.state.history
    )


def test_commit_task_status_change_rejects_symlink_escape(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "PR-706.md").write_text(
        "---\nstatus: TODO\n---\n\nBody\n",
        encoding="utf-8",
    )
    (repo / "tasks").symlink_to(outside, target_is_directory=True)
    calls: list[tuple[str, ...]] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        calls.append(args)
        return h._FakeCompletedProcess(returncode=0)

    monkeypatch.setattr("src.daemon.git_ops._git", fake_git)
    runner = h._make_runner()
    runner.repo_path = str(repo)
    task = QueueTask(
        pr_id="PR-706",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-test",
        task_file="tasks/PR-706.md",
    )

    asyncio.run(runner._commit_task_status_change(task, "ERROR", "bad path"))

    assert calls == []
    assert (outside / "PR-706.md").read_text(encoding="utf-8").startswith(
        "---\nstatus: TODO\n---\n"
    )
    assert any(
        "refusing to commit task path outside repo 'tasks/PR-706.md'"
        in event["event"]
        for event in runner.state.history
    )


def test_commit_task_status_change_force_checkouts_base_with_dirty_pr_branch(
    tmp_path: Path,
) -> None:
    repo = _make_repo_with_task(tmp_path, "PR-707")
    _git(repo, "checkout", "pr-test")
    task_file = repo / "tasks" / "PR-707.md"
    task_file.write_text(
        task_file.read_text(encoding="utf-8").replace(
            "status: TODO", "status: DOING", 1
        ),
        encoding="utf-8",
    )
    runner = h._make_runner()
    runner.repo_path = str(repo)
    task = QueueTask(
        pr_id="PR-707",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-test",
        task_file="tasks/PR-707.md",
    )

    asyncio.run(runner._commit_task_status_change(task, "ERROR", "failed hard"))

    assert (repo / "tasks" / "PR-707.md").read_text(encoding="utf-8").startswith(
        "---\nstatus: ERROR\n---\n"
    )
    assert "[STATUS] PR-707 marked ERROR: failed hard" in _git(
        repo,
        "log",
        "--oneline",
        "-1",
    )


def test_commit_task_status_change_truncates_long_reason(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    task_dir = tmp_path / "tasks"
    task_dir.mkdir()
    (task_dir / "PR-705.md").write_text(
        "---\nstatus: TODO\n---\n\nBody\n",
        encoding="utf-8",
    )
    commit_messages: list[str] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args[:3] == ("diff", "--cached", "--quiet"):
            return h._FakeCompletedProcess(returncode=1)
        if args[:1] == ("commit",):
            commit_messages.append(args[2])
        return h._FakeCompletedProcess(returncode=0)

    monkeypatch.setattr("src.daemon.git_ops._git", fake_git)
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    task = QueueTask(
        pr_id="705",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-test",
        task_file="tasks/PR-705.md",
    )

    asyncio.run(
        runner._commit_task_status_change(task, "ERROR", "x" * 90)
    )

    assert commit_messages == [
        f"[STATUS] PR-705 marked ERROR: {'x' * 77}...\n\n[skip ci]"
    ]
