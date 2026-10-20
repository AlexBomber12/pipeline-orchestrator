"""Merge-side task status durability and filesystem commit guarantees."""

from __future__ import annotations

import asyncio
import hashlib
import subprocess
import types
from pathlib import Path
from typing import Any

import pytest
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus
from src.subsource_registry import SuppressionReason

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


def _status_runner(repo: Path) -> Any:
    runner = h._make_runner()
    runner.repo_path = str(repo)
    runner.repo_config.url = _git(repo, "remote", "get-url", "origin").strip()
    return runner


def _remote_task(repo: Path, pr_id: str) -> str:
    _git(repo, "fetch", "origin", "main")
    return _git(repo, "show", f"FETCH_HEAD:tasks/{pr_id}.md")


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


def test_merge_writes_status_done_to_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo = _make_repo_with_task(tmp_path, "PR-701")
    monkeypatch.setattr("src.github.prs.merge_pr", lambda repo, num, expected_head_sha: None)
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *args, **kwargs: "")

    runner = _status_runner(repo)
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

    task_text = _remote_task(repo, "PR-701")
    assert task_text.startswith("---\nstatus: DONE\n---\n")
    assert "[STATUS] PR-701 marked DONE: PR merged" in _git(
        repo, "log", "--oneline", "-1", "FETCH_HEAD"
    )


def test_merge_logs_status_commit_exception(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo = _make_repo_with_task(tmp_path, "PR-701")
    monkeypatch.setattr("src.github.prs.merge_pr", lambda repo, num, expected_head_sha: None)
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
    monkeypatch.setattr("src.github.prs.merge_pr", lambda repo, num, expected_head_sha: None)
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *args, **kwargs: "")

    runner = _status_runner(repo)
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

    task_text = _remote_task(repo, "PR-701")
    assert task_text.startswith("---\nstatus: DONE\n---\n")
    assert "[STATUS] PR-701 marked DONE: PR merged" in _git(
        repo,
        "log",
        "--oneline",
        "-1",
        "FETCH_HEAD",
    )


def test_commit_task_status_change_push_failure_logs_warning(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo = _make_repo_with_task(tmp_path, "PR-702")
    calls: list[tuple[str, ...]] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        calls.append(args)
        if args[:1] == ("push",):
            raise RuntimeError("push denied")
        return real_git(repo_path, *args, **kwargs)

    from src.daemon import git_ops
    real_git = git_ops._git
    monkeypatch.setattr("src.daemon.git_ops._git", fake_git)
    runner = _status_runner(repo)
    task = QueueTask(
        pr_id="PR-702",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-test",
        task_file="tasks/PR-702.md",
    )

    asyncio.run(runner._commit_task_status_change(task, "ERROR", "failed hard"))

    assert any(call[:2] == ("push", "origin") for call in calls)
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


def test_commit_task_status_change_rejects_tracked_task_symlink(
    tmp_path: Path,
) -> None:
    repo = _make_repo_with_task(tmp_path, "PR-706")
    _git(repo, "mv", "tasks/PR-706.md", "tasks/target.md")
    (repo / "tasks" / "PR-706.md").symlink_to("target.md")
    _git(repo, "add", "tasks/PR-706.md")
    _git(repo, "commit", "-m", "link task to another tracked file")
    _git(repo, "push", "origin", "main")
    runner = _status_runner(repo)
    task = QueueTask(
        pr_id="PR-706",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-test",
        task_file="tasks/PR-706.md",
    )

    result = asyncio.run(
        runner._commit_task_status_change(task, "ERROR", "failed hard")
    )

    assert result is False
    assert _remote_task(repo, "PR-706") == "target.md"
    assert _git(repo, "show", "FETCH_HEAD:tasks/target.md").startswith(
        "---\nstatus: TODO\n---\n"
    )
    assert any(
        "task path is a symlink in isolated checkout" in event["event"]
        for event in runner.state.history
    )


def test_commit_task_status_change_preserves_dirty_pr_branch(
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
    runner = _status_runner(repo)
    task = QueueTask(
        pr_id="PR-707",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-test",
        task_file="tasks/PR-707.md",
    )

    asyncio.run(runner._commit_task_status_change(task, "ERROR", "failed hard"))

    assert _remote_task(repo, "PR-707").startswith(
        "---\nstatus: ERROR\nblocked_reason: crash\n---\n"
    )
    assert task_file.read_text(encoding="utf-8").startswith("---\nstatus: DOING\n")
    assert _git(repo, "branch", "--show-current").strip() == "pr-test"
    assert "[STATUS] PR-707 marked ERROR: failed hard" in _git(
        repo,
        "log",
        "--oneline",
        "-1",
        "FETCH_HEAD",
    )


def test_commit_task_status_change_uses_explicit_blocked_reason(
    tmp_path: Path,
) -> None:
    repo = _make_repo_with_task(tmp_path, "PR-708")
    runner = _status_runner(repo)
    task = QueueTask(
        pr_id="PR-708",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-test",
        task_file="tasks/PR-708.md",
    )

    asyncio.run(
        runner._commit_task_status_change(
            task,
            "ERROR",
            "failed hard",
            blocked_reason=SuppressionReason.GUARDRAIL,
        )
    )

    assert _remote_task(repo, "PR-708").startswith(
        "---\nstatus: ERROR\nblocked_reason: guardrail\n---\n"
    )


@pytest.mark.parametrize(
    ("status", "reason", "expected"),
    [
        ("DONE", "merged", None),
        ("ERROR", "FIX no-push deadlock", SuppressionReason.NO_PUSH_DEADLOCK.value),
        ("ERROR", "PR #1 hung after 20m (review=EYES, ci=PENDING)", "review_timeout"),
        ("ERROR", "PR closed externally during FIX", SuppressionReason.CRASH.value),
    ],
)
def test_blocked_reason_for_status_change_falls_back_from_reason_text(
    status: str,
    reason: str,
    expected: str | None,
) -> None:
    runner = h._make_runner()
    actual = asyncio.run(
        runner._blocked_reason_for_status_change(status, reason)
    )

    assert actual == expected


def test_blocked_reason_for_status_change_rejects_invalid_explicit_reason() -> None:
    runner = h._make_runner()

    actual = asyncio.run(
        runner._blocked_reason_for_status_change(
            "ERROR",
            "failed hard",
            blocked_reason="not_a_real_reason",
        )
    )

    assert actual == SuppressionReason.CRASH.value


def test_commit_task_status_change_truncates_long_reason(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo = _make_repo_with_task(tmp_path, "PR-705")
    runner = _status_runner(repo)
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

    _remote_task(repo, "PR-705")
    assert _git(repo, "log", "-1", "--format=%B", "FETCH_HEAD").strip() == (
        f"[STATUS] PR-705 marked ERROR: {'x' * 77}...\n\n[skip ci]"
    )


def test_status_write_adds_frontmatter_to_legacy_task(tmp_path: Path) -> None:
    repo = _make_repo_with_task(tmp_path, "PR-706")
    task_path = repo / "tasks" / "PR-706.md"
    task_path.write_text(task_path.read_text().split("---\n\n", 1)[1])
    _git(repo, "add", "tasks/PR-706.md")
    _git(repo, "commit", "-m", "legacy task without frontmatter")
    _git(repo, "push", "origin", "main")
    runner = _status_runner(repo)
    task = QueueTask(
        pr_id="PR-706", title="t", status=TaskStatus.DOING,
        branch="pr-test", task_file="tasks/PR-706.md",
    )

    assert asyncio.run(runner._commit_task_status_change(task, "DONE", "merged"))
    assert _remote_task(repo, "PR-706").startswith("---\nstatus: DONE\n---\n\n# PR-706")


@pytest.mark.parametrize("detached", [False, True])
def test_status_write_preserves_coder_checkout_and_index(
    tmp_path: Path, detached: bool
) -> None:
    repo = _make_repo_with_task(tmp_path, "PR-709")
    tracked = repo / "coder.txt"
    tracked.write_text("committed\n", encoding="utf-8")
    _git(repo, "add", "coder.txt")
    _git(repo, "commit", "-m", "local coder commit")
    if detached:
        _git(repo, "checkout", "--detach")
    tracked.write_text("staged\n", encoding="utf-8")
    _git(repo, "add", "coder.txt")
    tracked.write_text("unstaged\n", encoding="utf-8")
    binary = repo / "draft.bin"
    binary.write_bytes(b"\x00\xff\x01")
    index = (repo / ".git" / "index").read_bytes()
    before = (
        _git(repo, "rev-parse", "HEAD"),
        _git(repo, "symbolic-ref", "-q", "HEAD") if not detached else "detached",
        _git(repo, "status", "--porcelain"),
    )
    runner = _status_runner(repo)
    task = QueueTask(pr_id="PR-709", title="t", status=TaskStatus.DOING,
                     branch="pr-test", task_file="tasks/PR-709.md")

    assert asyncio.run(runner._commit_task_status_change(task, "ERROR", "crash"))

    after = (
        _git(repo, "rev-parse", "HEAD"),
        _git(repo, "symbolic-ref", "-q", "HEAD") if not detached else "detached",
        _git(repo, "status", "--porcelain"),
    )
    assert after == before
    assert (repo / ".git" / "index").read_bytes() == index
    assert tracked.read_text(encoding="utf-8") == "unstaged\n"
    assert binary.read_bytes() == b"\x00\xff\x01"
    assert _remote_task(repo, "PR-709").startswith("---\nstatus: ERROR\n")


def test_status_write_retries_concurrent_base_advance(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    repo = _make_repo_with_task(tmp_path, "PR-710")
    runner = _status_runner(repo)
    from src.daemon import git_ops

    real_git = git_ops._git
    advanced = False

    def racing_git(path: str, *args: str, **kwargs: Any) -> Any:
        nonlocal advanced
        if args[:2] == ("push", "origin") and not advanced:
            advanced = True
            other = tmp_path / "other"
            _git(tmp_path, "clone", str(tmp_path / "remote.git"), str(other))
            _git(other, "checkout", "main")
            _git(other, "config", "user.email", "test@example.com")
            _git(other, "config", "user.name", "Test User")
            (other / "concurrent.txt").write_text("another writer\n")
            _git(other, "add", "concurrent.txt")
            _git(other, "commit", "-m", "concurrent")
            _git(other, "push", "origin", "main")
        return real_git(path, *args, **kwargs)

    monkeypatch.setattr(git_ops, "_git", racing_git)
    task = QueueTask(pr_id="PR-710", title="t", status=TaskStatus.DOING,
                     branch="pr-test", task_file="tasks/PR-710.md")

    assert asyncio.run(runner._commit_task_status_change(task, "DONE", "merged"))
    assert advanced
    assert _remote_task(repo, "PR-710").startswith("---\nstatus: DONE\n")
    assert _git(repo, "show", "FETCH_HEAD:concurrent.txt") == "another writer\n"


@pytest.mark.parametrize("replacement", ["body", "branch"])
def test_status_write_stops_when_reuploaded_during_push(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, replacement: str
) -> None:
    repo = _make_repo_with_task(tmp_path, "PR-719")
    runner = _status_runner(repo)
    from src.daemon import git_ops

    real_git = git_ops._git
    advanced = False

    def racing_git(path: str, *args: str, **kwargs: Any) -> Any:
        nonlocal advanced
        if args[:2] == ("push", "origin") and not advanced:
            advanced = True
            other = tmp_path / "replacement"
            _git(tmp_path, "clone", str(tmp_path / "remote.git"), str(other))
            _git(other, "checkout", "main")
            _git(other, "config", "user.email", "test@example.com")
            _git(other, "config", "user.name", "Test User")
            task_file = other / "tasks" / "PR-719.md"
            source = task_file.read_text(encoding="utf-8")
            if replacement == "branch":
                source = source.replace("Branch: pr-test", "Branch: replacement")
            else:
                source += "\nReuploaded instructions\n"
            task_file.write_text(source, encoding="utf-8")
            _git(other, "add", "tasks/PR-719.md")
            _git(other, "commit", "-m", "reupload task")
            _git(other, "push", "origin", "main")
        return real_git(path, *args, **kwargs)

    monkeypatch.setattr(git_ops, "_git", racing_git)
    task = QueueTask(pr_id="PR-719", title="t", status=TaskStatus.DOING,
                     branch="pr-test", task_file="tasks/PR-719.md")

    assert not asyncio.run(runner._commit_task_status_change(task, "DONE", "merged"))
    remote = _remote_task(repo, "PR-719")
    assert remote.startswith("---\nstatus: TODO\n")
    assert ("Reuploaded instructions" in remote) == (replacement == "body")


def test_status_write_fetches_bounded_base_without_cloning(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    repo = _make_repo_with_task(tmp_path, "PR-720")
    runner = _status_runner(repo)
    from src.daemon import git_ops

    real_git = git_ops._git
    calls: list[tuple[str, ...]] = []

    def recorded_git(path: str, *args: str, **kwargs: Any) -> Any:
        calls.append(args)
        return real_git(path, *args, **kwargs)

    monkeypatch.setattr(git_ops, "_git", recorded_git)
    task = QueueTask(pr_id="PR-720", title="t", status=TaskStatus.DOING,
                     branch="pr-test", task_file="tasks/PR-720.md")

    assert asyncio.run(runner._commit_task_status_change(task, "DONE", "merged"))
    assert not any(args[0] == "clone" for args in calls)
    assert any(args[:3] == ("fetch", "--depth=16", "--filter=blob:none")
               for args in calls)


def test_status_write_confirms_ambiguous_push(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    repo = _make_repo_with_task(tmp_path, "PR-711")
    runner = _status_runner(repo)
    from src.daemon import git_ops

    real_git = git_ops._git

    def uncertain_git(path: str, *args: str, **kwargs: Any) -> Any:
        result = real_git(path, *args, **kwargs)
        if args[:2] == ("push", "origin"):
            return types.SimpleNamespace(returncode=1, stderr="response lost")
        return result

    monkeypatch.setattr(git_ops, "_git", uncertain_git)
    task = QueueTask(pr_id="PR-711", title="t", status=TaskStatus.DOING,
                     branch="pr-test", task_file="tasks/PR-711.md")

    assert asyncio.run(runner._commit_task_status_change(task, "DONE", "merged"))
    assert _remote_task(repo, "PR-711").startswith("---\nstatus: DONE\n")


def test_failed_commit_does_not_report_status_or_touch_coder_work(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    repo = _make_repo_with_task(tmp_path, "PR-712")
    runner = _status_runner(repo)
    task = QueueTask(pr_id="PR-712", title="t", status=TaskStatus.DOING,
                     branch="pr-test", task_file="tasks/PR-712.md")
    coder_file = repo / "coder.bin"
    coder_file.write_bytes(b"\x00\xff")
    head = _git(repo, "rev-parse", "HEAD")
    from src.daemon import git_ops

    real_git = git_ops._git

    def failed_commit(path: str, *args: str, **kwargs: Any) -> Any:
        if "commit" in args:
            raise RuntimeError("commit denied")
        return real_git(path, *args, **kwargs)

    monkeypatch.setattr(git_ops, "_git", failed_commit)

    assert not asyncio.run(runner._commit_task_status_change(task, "DONE", "merged"))
    assert _git(repo, "rev-parse", "HEAD") == head
    assert coder_file.read_bytes() == b"\x00\xff"
    assert _remote_task(repo, "PR-712").startswith("---\nstatus: TODO\n")


def test_status_write_rejects_wrong_origin_and_task_identity(tmp_path: Path) -> None:
    repo = _make_repo_with_task(tmp_path, "PR-713")
    runner = _status_runner(repo)
    task = QueueTask(pr_id="PR-713", title="t", status=TaskStatus.DOING,
                     branch="pr-test", task_file="tasks/PR-713.md")
    runner.repo_config.url = str(tmp_path / "different.git")
    assert not asyncio.run(runner._commit_task_status_change(task, "DONE", "merged"))
    runner.repo_config.url = _git(repo, "remote", "get-url", "origin").strip()
    task.task_file = "tasks/PR-714.md"
    assert not asyncio.run(runner._commit_task_status_change(task, "DONE", "merged"))
    assert _remote_task(repo, "PR-713").startswith("---\nstatus: TODO\n")


@pytest.mark.parametrize("status", ["DONE", "ERROR"])
def test_status_write_accepts_equivalent_origin_url(tmp_path: Path, status: str) -> None:
    repo = _make_repo_with_task(tmp_path, "PR-718")
    runner = _status_runner(repo)
    # Runner sync retains this checkout when the configured URL loses .git.
    # A symlink makes both spellings usable by the real Git clone.
    (tmp_path / "remote").symlink_to(tmp_path / "remote.git", target_is_directory=True)
    runner.repo_config.url = str(tmp_path / "remote")
    task = QueueTask(pr_id="PR-718", title="t", status=TaskStatus.DOING,
                     branch="pr-test", task_file="tasks/PR-718.md")

    assert asyncio.run(runner._commit_task_status_change(task, status, "completed"))
    assert _remote_task(repo, "PR-718").startswith(f"---\nstatus: {status}\n")


def test_status_write_rejects_invalid_base_and_missing_remote_task(tmp_path: Path) -> None:
    repo = _make_repo_with_task(tmp_path, "PR-714")
    runner = _status_runner(repo)
    task = QueueTask(pr_id="PR-714", title="t", status=TaskStatus.DOING,
                     branch="pr-test", task_file="tasks/PR-714.md")
    runner.repo_config.branch = "bad..branch"
    assert not asyncio.run(runner._commit_task_status_change(task, "DONE", "merged"))
    runner.repo_config.branch = "main"
    # The path has the right identity but is absent from the remote base.
    task.pr_id = "PR-715"
    task.task_file = "tasks/PR-715.md"
    assert not asyncio.run(runner._commit_task_status_change(task, "DONE", "merged"))
    assert _remote_task(repo, "PR-714").startswith("---\nstatus: TODO\n")


def test_status_write_no_change_keeps_remote_head(tmp_path: Path) -> None:
    repo = _make_repo_with_task(tmp_path, "PR-716")
    runner = _status_runner(repo)
    task = QueueTask(pr_id="PR-716", title="t", status=TaskStatus.DOING,
                     branch="pr-test", task_file="tasks/PR-716.md")
    assert asyncio.run(runner._commit_task_status_change(task, "DONE", "merged"))
    head = _git(tmp_path / "remote.git", "rev-parse", "refs/heads/main")
    assert asyncio.run(runner._commit_task_status_change(task, "DONE", "merged"))
    assert _git(tmp_path / "remote.git", "rev-parse", "refs/heads/main") == head


@pytest.mark.parametrize("replacement", ["body", "todo"])
def test_status_write_rejects_reupload_since_admission(
    tmp_path: Path, replacement: str,
) -> None:
    repo = _make_repo_with_task(tmp_path, "PR-716")
    runner = _status_runner(repo)
    task = QueueTask(pr_id="PR-716", title="t", status=TaskStatus.ERROR,
                     branch="pr-test", task_file="tasks/PR-716.md")
    original = _remote_task(repo, "PR-716")
    admitted_revision = hashlib.sha256(original.encode()).hexdigest()
    task_file = repo / task.task_file
    if replacement == "body":
        task_file.write_text(original + "\nReuploaded instructions\n")
    else:
        task_file.write_text(original.replace("status: TODO", "status: ERROR"))
    _git(repo, "add", task.task_file)
    _git(repo, "commit", "-m", "reupload")
    _git(repo, "push", "origin", "main")
    changed = _remote_task(repo, "PR-716")

    assert not asyncio.run(runner._commit_task_status_change(
        task, "TODO", "timeout recovery", expected_task_sha256=admitted_revision,
    ))
    assert _remote_task(repo, "PR-716") == changed


@pytest.mark.parametrize("failure", ["diff", "confirm", "push"])
def test_status_write_does_not_claim_unconfirmed_commit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failure: str
) -> None:
    repo = _make_repo_with_task(tmp_path, "PR-717")
    runner = _status_runner(repo)
    task = QueueTask(pr_id="PR-717", title="t", status=TaskStatus.DOING,
                     branch="pr-test", task_file="tasks/PR-717.md")
    from src.daemon import git_ops

    real_git = git_ops._git

    def failing_git(path: str, *args: str, **kwargs: Any) -> Any:
        if failure == "diff" and args[:3] == ("diff", "--cached", "--quiet"):
            return types.SimpleNamespace(returncode=2)
        if failure == "confirm" and args[:2] == ("merge-base", "--is-ancestor"):
            return types.SimpleNamespace(returncode=2)
        if failure == "push" and args[:2] == ("push", "origin"):
            return types.SimpleNamespace(returncode=1, stderr="rejected")
        return real_git(path, *args, **kwargs)

    monkeypatch.setattr(git_ops, "_git", failing_git)
    assert not asyncio.run(runner._commit_task_status_change(task, "DONE", "merged"))
    if failure != "confirm":
        assert _remote_task(repo, "PR-717").startswith("---\nstatus: TODO\n")
