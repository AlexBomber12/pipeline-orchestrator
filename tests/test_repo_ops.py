"""Tests for src.daemon.repo_ops."""

from __future__ import annotations

import asyncio
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from src.daemon import repo_ops


class _FakeCompletedProcess:
    def __init__(
        self,
        *,
        stdout: str = "",
        stderr: str = "",
        returncode: int = 0,
    ) -> None:
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode


class _FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, Any] = {}
        self.eval_result: Any = 1
        self.eval_error: Exception | None = None
        self.get_error: Exception | None = None
        self.delete_error: Exception | None = None

    async def get(self, key: str) -> Any:
        if self.get_error is not None:
            raise self.get_error
        return self.store.get(key)

    async def delete(self, key: str) -> None:
        if self.delete_error is not None:
            raise self.delete_error
        self.store.pop(key, None)

    async def eval(self, script: str, numkeys: int, *args: Any) -> Any:
        del script, numkeys
        if self.eval_error is not None:
            raise self.eval_error
        if args and len(args) >= 2 and self.eval_result:
            key, expected = args[0], args[1]
            if self.store.get(key) == expected:
                self.store.pop(key, None)
        return self.eval_result


class _Runner(repo_ops.RepoOpsMixin):
    def __init__(self, tmp_path: Path) -> None:
        self.repo_path = str(tmp_path / "repo")
        self.repo_config = SimpleNamespace(
            url="https://github.com/example/project.git",
            branch="main",
        )
        self._scaffolded = False
        self.redis = _FakeRedis()
        self.name = "demo"
        self.events: list[str] = []

    def log_event(self, message: str) -> None:
        self.events.append(message)


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


def _patch_retry_passthrough(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        repo_ops,
        "retry_transient",
        lambda func, operation_name=None: func(),
    )


def test_uploaded_repo_path_routes_root_instruction_files() -> None:
    assert repo_ops._uploaded_repo_path("AGENTS.md") == Path("AGENTS.md")
    assert repo_ops._uploaded_repo_path("CLAUDE.md") == Path("CLAUDE.md")
    assert repo_ops._uploaded_repo_path("PR-112.md") == Path("tasks/PR-112.md")


def test_ensure_repo_cloned_clones_scaffolds_and_backfills(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    clone_root = Path(runner.repo_path)
    removed: list[Path] = []
    clone_calls: list[list[str]] = []

    def fake_retry(func: Any, operation_name: str | None = None) -> None:
        del operation_name
        clone_root.mkdir(parents=True)
        func()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        clone_calls.append(cmd)
        return _FakeCompletedProcess()

    monkeypatch.setattr(repo_ops, "retry_transient", fake_retry)
    monkeypatch.setattr(repo_ops.subprocess, "run", fake_run)
    monkeypatch.setattr(repo_ops.shutil, "rmtree", lambda path: removed.append(Path(path)))
    monkeypatch.setattr(repo_ops.scaffolder, "scaffold_repo", lambda path, branch: ["AGENTS.md"])
    monkeypatch.setattr(repo_ops, "_working_tree_dirty", lambda path: False)
    monkeypatch.setattr(repo_ops.scaffolder, "ensure_claude_md", lambda path, branch: True)

    _run(runner.ensure_repo_cloned())

    assert removed == [clone_root]
    assert clone_calls == [["git", "clone", runner.repo_config.url, runner.repo_path]]
    assert runner._scaffolded is True
    assert "scaffold_repo created: AGENTS.md" in runner.events
    assert "backfilled CLAUDE.md for legacy repo" in runner.events


@pytest.mark.parametrize(
    ("exc", "message"),
    [
        (
            subprocess.CalledProcessError(
                1,
                ["git", "clone"],
                stderr="fatal clone error",
            ),
            "git clone failed: fatal clone error",
        ),
        (
            subprocess.TimeoutExpired(["git", "clone"], timeout=120),
            "git clone timed out",
        ),
    ],
)
def test_ensure_repo_cloned_reports_clone_errors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    exc: Exception,
    message: str,
) -> None:
    runner = _Runner(tmp_path)

    def raising_retry(func: Any, operation_name: str | None = None) -> None:
        del func, operation_name
        raise exc

    monkeypatch.setattr(repo_ops, "retry_transient", raising_retry)

    with pytest.raises(RuntimeError, match=message):
        _run(runner.ensure_repo_cloned())


def test_ensure_repo_cloned_reports_fetch_failures_and_timeout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    Path(runner.repo_path).mkdir(parents=True)

    def failing_retry(func: Any, operation_name: str | None = None) -> None:
        del func, operation_name
        raise subprocess.CalledProcessError(1, ["git", "fetch"], stderr="fatal auth")

    monkeypatch.setattr(repo_ops, "retry_transient", failing_retry)

    with pytest.raises(RuntimeError, match="git fetch failed: fatal auth"):
        _run(runner.ensure_repo_cloned())

    def timeout_retry(func: Any, operation_name: str | None = None) -> None:
        del func, operation_name
        raise subprocess.TimeoutExpired(["git", "fetch"], timeout=60)

    monkeypatch.setattr(repo_ops, "retry_transient", timeout_retry)

    with pytest.raises(RuntimeError, match="git fetch timed out"):
        _run(runner.ensure_repo_cloned())


def test_ensure_repo_cloned_retries_scaffold_after_missing_ref(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    runner._scaffolded = True
    Path(runner.repo_path).mkdir(parents=True)
    scaffold_calls: list[tuple[str, str]] = []

    def missing_ref_retry(func: Any, operation_name: str | None = None) -> None:
        del func, operation_name
        raise subprocess.CalledProcessError(
            1,
            ["git", "fetch"],
            stderr="fatal: couldn't find remote ref main",
        )

    monkeypatch.setattr(repo_ops, "retry_transient", missing_ref_retry)
    monkeypatch.setattr(
        repo_ops.scaffolder,
        "scaffold_repo",
        lambda path, branch: scaffold_calls.append((path, branch)) or ["tasks/QUEUE.md"],
    )
    monkeypatch.setattr(repo_ops, "_working_tree_dirty", lambda path: False)
    monkeypatch.setattr(repo_ops.scaffolder, "ensure_claude_md", lambda path, branch: False)

    _run(runner.ensure_repo_cloned())

    assert scaffold_calls == [(runner.repo_path, "main")]
    assert any("will retry scaffold" in event for event in runner.events)


def test_ensure_repo_cloned_resets_scaffold_when_base_branch_ahead(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    runner._scaffolded = True
    Path(runner.repo_path).mkdir(parents=True)
    scaffold_calls: list[str] = []
    _patch_retry_passthrough(monkeypatch)
    monkeypatch.setattr(repo_ops, "_base_branch_ahead_of_origin", lambda path, branch: True)
    monkeypatch.setattr(
        repo_ops.scaffolder,
        "scaffold_repo",
        lambda path, branch: scaffold_calls.append(branch) or [],
    )
    monkeypatch.setattr(repo_ops, "_working_tree_dirty", lambda path: False)
    monkeypatch.setattr(repo_ops.scaffolder, "ensure_claude_md", lambda path, branch: False)
    monkeypatch.setattr(repo_ops.git_ops, "_git", lambda *args, **kwargs: _FakeCompletedProcess())

    _run(runner.ensure_repo_cloned())

    assert scaffold_calls == ["main"]
    assert any("ahead of origin" in event for event in runner.events)


def test_ensure_repo_cloned_defers_dirty_tree_and_wraps_scaffold_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    Path(runner.repo_path).mkdir(parents=True)
    _patch_retry_passthrough(monkeypatch)
    monkeypatch.setattr(repo_ops.git_ops, "_git", lambda *args, **kwargs: _FakeCompletedProcess())
    monkeypatch.setattr(repo_ops, "_working_tree_dirty", lambda path: True)

    _run(runner.ensure_repo_cloned())
    assert any("scaffold_repo deferred" in event for event in runner.events)

    monkeypatch.setattr(repo_ops, "_working_tree_dirty", lambda path: False)
    monkeypatch.setattr(
        repo_ops.scaffolder,
        "scaffold_repo",
        lambda path, branch: (_ for _ in ()).throw(ValueError("boom")),
    )

    with pytest.raises(RuntimeError, match="scaffold_repo failed: boom"):
        _run(runner.ensure_repo_cloned())


def test_ensure_repo_cloned_wraps_claude_backfill_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    runner._scaffolded = True
    Path(runner.repo_path).mkdir(parents=True)
    _patch_retry_passthrough(monkeypatch)
    monkeypatch.setattr(repo_ops.git_ops, "_git", lambda *args, **kwargs: _FakeCompletedProcess())
    monkeypatch.setattr(repo_ops, "_base_branch_ahead_of_origin", lambda path, branch: False)
    monkeypatch.setattr(repo_ops, "_working_tree_dirty", lambda path: False)
    monkeypatch.setattr(
        repo_ops.scaffolder,
        "ensure_claude_md",
        lambda path, branch: (_ for _ in ()).throw(OSError("disk full")),
    )

    with pytest.raises(RuntimeError, match="CLAUDE.md backfill failed: disk full"):
        _run(runner.ensure_repo_cloned())


def test_sync_to_main_runs_git_sequence_and_wraps_oserror(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    calls: list[tuple[Any, ...]] = []
    _patch_retry_passthrough(monkeypatch)

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        del kwargs
        calls.append((repo_path, *args))
        return _FakeCompletedProcess()

    monkeypatch.setattr(repo_ops.git_ops, "_git", fake_git)
    runner.sync_to_main()

    assert calls == [
        (runner.repo_path, "fetch", "origin", "main"),
        (runner.repo_path, "checkout", "main"),
        (runner.repo_path, "reset", "--hard", "origin/main"),
        (runner.repo_path, "clean", "-fd"),
    ]

    monkeypatch.setattr(
        repo_ops.git_ops,
        "_git",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("no git")),
    )

    with pytest.raises(RuntimeError, match="sync_to_main OS error: no git"):
        runner.sync_to_main()


def test_parse_base_queue_returns_parsed_queue_and_none_on_git_errors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    queue_text = (
        "# Task Queue\n\n"
        "## PR-112: Example\n"
        "- Status: TODO\n"
        "- Tasks file: tasks/PR-112.md\n"
        "- Branch: pr-112-coverage-repo-ops\n"
    )

    def good_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        del repo_path, args, kwargs
        return _FakeCompletedProcess(stdout=queue_text)

    monkeypatch.setattr(repo_ops.git_ops, "_git", good_git)
    parsed = runner._parse_base_queue(strict=True)

    assert parsed is not None
    assert parsed[0].pr_id == "PR-112"
    assert parsed[0].branch == "pr-112-coverage-repo-ops"

    monkeypatch.setattr(
        repo_ops.git_ops,
        "_git",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            subprocess.CalledProcessError(1, ["git", "show"])
        ),
    )
    assert runner._parse_base_queue() is None

    monkeypatch.setattr(
        repo_ops.git_ops,
        "_git",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            subprocess.TimeoutExpired(["git", "show"], timeout=30)
        ),
    )
    assert runner._parse_base_queue() is None


def test_delete_upload_if_unchanged_uses_eval_and_fallback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    key = "upload:demo:pending"
    expected = '{"files":["QUEUE.md"]}'

    runner.redis.eval_result = 1
    assert _run(runner._delete_upload_if_unchanged(key, expected)) is True

    runner.redis.eval_result = 0
    assert _run(runner._delete_upload_if_unchanged(key, expected)) is False

    runner.redis.eval_error = RuntimeError("eval failed")
    runner.redis.store[key] = expected
    assert _run(runner._delete_upload_if_unchanged(key, expected)) is True
    assert key not in runner.redis.store

    runner.redis.store[key] = "newer"
    assert _run(runner._delete_upload_if_unchanged(key, expected)) is False

    runner.redis.get_error = RuntimeError("still broken")
    assert _run(runner._delete_upload_if_unchanged(key, expected)) is False


def test_process_pending_uploads_handles_empty_bad_and_missing_manifests(
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    key = f"upload:{runner.name}:pending"

    assert _run(runner.process_pending_uploads()) is False

    runner.redis.store[key] = "{bad json"
    assert _run(runner.process_pending_uploads()) is False
    assert key not in runner.redis.store

    runner.redis.store[key] = json.dumps({"files": []})
    assert _run(runner.process_pending_uploads()) is False
    assert key not in runner.redis.store


def test_process_pending_uploads_handles_redis_error_and_type_error_manifest(
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    key = f"upload:{runner.name}:pending"
    runner.redis.get_error = ConnectionError("redis down")
    assert _run(runner.process_pending_uploads()) is None

    runner.redis.get_error = None
    runner.redis.store[key] = b"{}"
    assert _run(runner.process_pending_uploads()) is False
    assert key not in runner.redis.store


def test_process_pending_uploads_success_and_nothing_to_commit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    repo_dir = Path(runner.repo_path)
    repo_dir.mkdir(parents=True)
    staging = tmp_path / "uploads" / "demo"
    staging.mkdir(parents=True)
    (staging / "QUEUE.md").write_text("# Queue\n", encoding="utf-8")
    (staging / "AGENTS.md").write_text("# AGENTS\n", encoding="utf-8")
    manifest = json.dumps(
        {
            "files": ["QUEUE.md", "AGENTS.md"],
            "staging_dir": str(staging),
        }
    )
    key = f"upload:{runner.name}:pending"
    runner.redis.store[key] = manifest
    git_calls: list[tuple[Any, ...]] = []
    removed: list[Path] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        git_calls.append((repo_path, *args))
        if args[:1] == ("commit",):
            return _FakeCompletedProcess(
                returncode=1,
                stdout="nothing to commit",
            )
        return _FakeCompletedProcess()

    monkeypatch.setattr(repo_ops.git_ops, "_git", fake_git)
    monkeypatch.setattr(repo_ops, "retry_transient", lambda func, operation_name=None: func())
    monkeypatch.setattr(repo_ops.shutil, "rmtree", lambda path, ignore_errors=True: removed.append(Path(path)))

    assert _run(runner.process_pending_uploads()) is True

    assert (repo_dir / "tasks" / "QUEUE.md").read_text(encoding="utf-8") == "# Queue\n"
    assert (repo_dir / "AGENTS.md").read_text(encoding="utf-8") == "# AGENTS\n"
    assert git_calls == [
        (runner.repo_path, "add", "tasks/QUEUE.md", "AGENTS.md"),
        (
            runner.repo_path,
            "commit",
            "-m",
            "chore: upload sprint tasks via dashboard",
        ),
        (runner.repo_path, "push", "origin", "main"),
    ]
    assert removed == [staging]
    assert key not in runner.redis.store
    assert any(
        "Uploaded 0 task files to tasks/ and pushed to main" in event
        for event in runner.events
    )


def test_process_pending_uploads_returns_none_when_newer_manifest_exists(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    Path(runner.repo_path).mkdir(parents=True)
    staging = tmp_path / "uploads" / "demo"
    staging.mkdir(parents=True)
    (staging / "QUEUE.md").write_text("# Queue\n", encoding="utf-8")
    key = f"upload:{runner.name}:pending"
    old_manifest = json.dumps({"files": ["QUEUE.md"], "staging_dir": str(staging)})
    runner.redis.store[key] = old_manifest

    monkeypatch.setattr(repo_ops.git_ops, "_git", lambda *args, **kwargs: _FakeCompletedProcess())
    monkeypatch.setattr(repo_ops, "retry_transient", lambda func, operation_name=None: func())
    monkeypatch.setattr(
        runner,
        "_delete_upload_if_unchanged",
        lambda upload_key, expected: asyncio.sleep(0, result=False),
    )

    assert _run(runner.process_pending_uploads()) is None
    assert staging.is_dir()
    assert any("Newer upload pending" in event for event in runner.events)


def test_process_pending_uploads_handles_failures_and_safe_mode(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    Path(runner.repo_path).mkdir(parents=True)
    staging = tmp_path / "uploads" / "demo"
    staging.mkdir(parents=True)
    (staging / "QUEUE.md").write_text("# Queue\n", encoding="utf-8")
    manifest = json.dumps({"files": ["QUEUE.md"], "staging_dir": str(staging)})
    key = f"upload:{runner.name}:pending"
    runner.redis.store[key] = manifest
    git_calls: list[tuple[Any, ...]] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        git_calls.append((repo_path, *args))
        if args[:1] == ("commit",):
            return _FakeCompletedProcess(returncode=1, stderr="commit exploded")
        if args[:1] == ("reset",):
            raise OSError("reset failed")
        return _FakeCompletedProcess()

    monkeypatch.setattr(repo_ops.git_ops, "_git", fake_git)
    monkeypatch.setattr(repo_ops, "retry_transient", lambda func, operation_name=None: func())

    assert _run(runner.process_pending_uploads()) is None
    assert git_calls[-1] == (runner.repo_path, "reset", "--hard", "origin/main")
    assert runner.redis.store[key] == manifest
    assert any("Upload push failed: commit exploded" in event for event in runner.events)

    git_calls.clear()
    runner.redis.store[key] = manifest
    assert _run(runner.process_pending_uploads(_safe=True)) is None
    assert all(call[1] != "reset" for call in git_calls)
