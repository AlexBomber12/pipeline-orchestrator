"""Tests for src.daemon.repo_ops."""

from __future__ import annotations

import asyncio
import hashlib
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from src.cancellation import retry_count_key, task_spec_content_hash, task_spec_hash_key
from src.daemon import repo_ops


@pytest.fixture(autouse=True)
def _merged_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    """Graph admission uses production evaluators with controlled GitHub reads."""
    monkeypatch.setattr(repo_ops, "get_merged_pr_ids", lambda *args: set())
    monkeypatch.setattr(repo_ops, "get_merged_branch_pr_ids", lambda *args: set())


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

    async def set(self, key: str, value: str, **kwargs: object) -> None:
        self.store[key] = value

    async def delete(self, key: str) -> None:
        if self.delete_error is not None:
            raise self.delete_error
        self.store.pop(key, None)

    async def zrem(self, key: str, *members: str) -> int:
        return 0

    async def eval(self, script: str, numkeys: int, *args: Any) -> Any:
        if self.eval_error is not None:
            raise self.eval_error
        if "redis.call('set', KEYS[1], ARGV[2])" in script:
            key, expected, replacement = args
            if self.store.get(key) != expected:
                return 0
            self.store[key] = replacement
            return 1
        if args and len(args) >= 2 and self.eval_result:
            key = args[0]
            if numkeys >= 2:
                delete_key, expected = args[1], args[numkeys]
            else:
                delete_key, expected = key, args[1]
            if self.store.get(key) == expected:
                if "return redis.call(\"del\", KEYS[1])" in script:
                    self.store.pop(key, None)
                self.store.pop(delete_key, None)
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
        self._crashed_task_pr_ids: set[str] = set()
        self._status_write_failed_task_pr_ids: set[str] = set()
        self.cleared_status_write_failed_calls = 0
        self.state = SimpleNamespace(current_queue=None)

    def log_event(self, message: str) -> None:
        self.events.append(message)

    async def _clear_status_write_failed_task_ids(
        self,
        uploaded_pr_ids: set[str],
    ) -> None:
        self.cleared_status_write_failed_calls += 1
        for key in ("status_write_failed_tasks:demo", "recovered_tasks:demo"):
            raw = self.redis.store.get(key)
            if raw:
                self._status_write_failed_task_pr_ids.update(json.loads(raw))
        self._status_write_failed_task_pr_ids.difference_update(uploaded_pr_ids)
        if self._status_write_failed_task_pr_ids:
            self.redis.store["status_write_failed_tasks:demo"] = json.dumps(
                sorted(self._status_write_failed_task_pr_ids)
            )
        else:
            self.redis.store.pop("status_write_failed_tasks:demo", None)
        try:
            await self.redis.delete("recovered_tasks:demo")
        except Exception:
            pass

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
    assert "[INFRA] scaffold_repo created: AGENTS.md." in runner.events
    assert "[INFRA] backfilled CLAUDE.md for legacy repo." in runner.events


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


def test_ensure_repo_cloned_fetch_uses_prune(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    runner._scaffolded = True
    Path(runner.repo_path).mkdir(parents=True)
    calls: list[tuple[Any, ...]] = []
    _patch_retry_passthrough(monkeypatch)

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        del kwargs
        calls.append((repo_path, *args))
        return _FakeCompletedProcess()

    monkeypatch.setattr(repo_ops.git_ops, "_git", fake_git)
    monkeypatch.setattr(repo_ops, "_base_branch_ahead_of_origin", lambda path, branch: False)
    monkeypatch.setattr(repo_ops, "_working_tree_dirty", lambda path: False)
    monkeypatch.setattr(repo_ops.scaffolder, "ensure_claude_md", lambda path, branch: False)

    _run(runner.ensure_repo_cloned())

    fetch_calls = [c for c in calls if c[1] == "fetch"]
    assert fetch_calls, "ensure_repo_cloned must invoke git fetch"
    assert all("--prune" in c for c in fetch_calls), (
        f"ensure_repo_cloned fetch must include --prune; got {fetch_calls}"
    )


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
        (runner.repo_path, "fetch", "--prune", "origin", "main"),
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


def test_delete_upload_if_unchanged_uses_eval_and_fallback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    key = "upload:demo:pending"
    count_key = "upload_pending_count:demo"
    expected = '{"files":["QUEUE.md"]}'

    runner.redis.store[key] = expected
    runner.redis.store[count_key] = "1"
    runner.redis.eval_result = 1
    assert (
        _run(
            runner._delete_upload_if_unchanged(
                key,
                expected,
                also_delete_key=count_key,
            )
        )
        is True
    )
    assert key not in runner.redis.store
    assert count_key not in runner.redis.store

    runner.redis.eval_result = 0
    assert _run(runner._delete_upload_if_unchanged(key, expected)) is False

    runner.redis.eval_error = RuntimeError("eval failed")
    runner.redis.store[key] = expected
    runner.redis.store[count_key] = "1"
    assert _run(runner._delete_upload_if_unchanged(key, expected)) is True
    assert key not in runner.redis.store
    assert count_key in runner.redis.store

    runner.redis.store[key] = expected
    assert (
        _run(
            runner._delete_upload_if_unchanged(
                key,
                expected,
                also_delete_key=count_key,
            )
        )
        is True
    )
    assert key not in runner.redis.store
    assert count_key not in runner.redis.store

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

    # QUEUE.md is gitignored; it must NOT be copied to the working
    # tree or staged, otherwise ``git add`` would abort the upload.
    assert not (repo_dir / "tasks" / "QUEUE.md").exists()
    assert (repo_dir / "AGENTS.md").read_text(encoding="utf-8") == "# AGENTS\n"
    assert git_calls == [
        (runner.repo_path, "add", "AGENTS.md"),
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
        "Skipping QUEUE.md from upload" in event for event in runner.events
    )
    assert any(
        "Uploaded 0 task files to tasks/ and pushed to main" in event
        for event in runner.events
    )


def test_process_pending_uploads_persists_task_hash_after_push(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    repo_dir = Path(runner.repo_path)
    repo_dir.mkdir(parents=True)
    staging = tmp_path / "uploads" / "demo"
    staging.mkdir(parents=True)
    (staging / "PR-001.md").write_text("# PR-001\n", encoding="utf-8")
    key = f"upload:{runner.name}:pending"
    manifest = json.dumps(
        {
            "files": ["PR-001.md"],
            "staging_dir": str(staging),
            "task_hashes": {"PR-001": "new-hash"},
        }
    )
    runner.redis.store[key] = manifest
    runner.redis.store[task_spec_hash_key("demo", "PR-001")] = "old-hash"
    runner.redis.store[retry_count_key("demo", "PR-001")] = "3"

    monkeypatch.setattr(
        repo_ops.git_ops,
        "_git",
        lambda *args, **kwargs: _FakeCompletedProcess(),
    )
    monkeypatch.setattr(repo_ops, "retry_transient", lambda func, operation_name=None: func())
    monkeypatch.setattr(repo_ops.shutil, "rmtree", lambda path, ignore_errors=True: None)

    assert _run(runner.process_pending_uploads()) is True
    assert runner.redis.store[task_spec_hash_key("demo", "PR-001")] == task_spec_content_hash("# PR-001\n")
    assert runner.redis.store[retry_count_key("demo", "PR-001")] == "0"
    assert key not in runner.redis.store


def test_process_pending_uploads_uses_manifest_commit_subject_and_body(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    repo_dir = Path(runner.repo_path)
    repo_dir.mkdir(parents=True)
    staging = tmp_path / "uploads" / "demo"
    staging.mkdir(parents=True)
    (staging / "PR-001.md").write_text("# PR-001\n", encoding="utf-8")
    (staging / "PR-002.md").write_text("# PR-002\n", encoding="utf-8")
    runner.redis.store[f"upload:{runner.name}:pending"] = json.dumps(
        {
            "files": ["PR-002.md", "PR-001.md"],
            "staging_dir": str(staging),
            "commit_subject": "My batch",
        }
    )
    git_calls: list[tuple[Any, ...]] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        git_calls.append((repo_path, *args))
        return _FakeCompletedProcess()

    monkeypatch.setattr(repo_ops.git_ops, "_git", fake_git)
    monkeypatch.setattr(repo_ops, "retry_transient", lambda func, operation_name=None: func())
    monkeypatch.setattr(repo_ops.shutil, "rmtree", lambda path, ignore_errors=True: None)

    assert _run(runner.process_pending_uploads()) is True
    assert (
        runner.repo_path,
        "commit",
        "-m",
        "My batch",
        "-m",
        "tasks/PR-001.md\ntasks/PR-002.md",
    ) in git_calls


def test_process_pending_uploads_ignores_malformed_task_hashes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    Path(runner.repo_path).mkdir(parents=True)
    staging = tmp_path / "uploads" / "demo"
    staging.mkdir(parents=True)
    (staging / "PR-001.md").write_text("# PR-001\n", encoding="utf-8")
    key = f"upload:{runner.name}:pending"
    runner.redis.store[key] = json.dumps(
        {
            "files": ["PR-001.md"],
            "staging_dir": str(staging),
            "task_hashes": ["not", "a", "mapping"],
        }
    )

    monkeypatch.setattr(
        repo_ops.git_ops,
        "_git",
        lambda *args, **kwargs: _FakeCompletedProcess(),
    )
    monkeypatch.setattr(repo_ops, "retry_transient", lambda func, operation_name=None: func())
    monkeypatch.setattr(repo_ops.shutil, "rmtree", lambda path, ignore_errors=True: None)

    assert _run(runner.process_pending_uploads()) is True
    assert runner.redis.store[task_spec_hash_key("demo", "PR-001")] == task_spec_content_hash("# PR-001\n")


def test_process_pending_uploads_keeps_manifest_when_metadata_update_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    Path(runner.repo_path).mkdir(parents=True)
    staging = tmp_path / "uploads" / "demo"
    staging.mkdir(parents=True)
    (staging / "PR-001.md").write_text("# PR-001\n", encoding="utf-8")
    key = f"upload:{runner.name}:pending"
    manifest = json.dumps(
        {
            "files": ["PR-001.md"],
            "staging_dir": str(staging),
            "task_hashes": {"PR-001": "new-hash"},
        }
    )
    runner.redis.store[key] = manifest

    async def fail_set(key: str, value: str, **kwargs: object) -> None:
        raise RuntimeError("redis down")

    runner.redis.set = fail_set  # type: ignore[method-assign]
    monkeypatch.setattr(
        repo_ops.git_ops,
        "_git",
        lambda *args, **kwargs: _FakeCompletedProcess(),
    )
    monkeypatch.setattr(repo_ops, "retry_transient", lambda func, operation_name=None: func())

    assert _run(runner.process_pending_uploads()) is None
    assert runner.redis.store[key] == manifest
    assert any("Upload metadata update failed" in event for event in runner.events)


def test_process_pending_uploads_leaves_task_hash_on_git_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    repo_dir = Path(runner.repo_path)
    repo_dir.mkdir(parents=True)
    staging = tmp_path / "uploads" / "demo"
    staging.mkdir(parents=True)
    (staging / "PR-001.md").write_text("# PR-001\n", encoding="utf-8")
    key = f"upload:{runner.name}:pending"
    manifest = json.dumps(
        {
            "files": ["PR-001.md"],
            "staging_dir": str(staging),
            "task_hashes": {"PR-001": "new-hash"},
        }
    )
    runner.redis.store[key] = manifest
    runner.redis.store[task_spec_hash_key("demo", "PR-001")] = "old-hash"
    runner.redis.store[retry_count_key("demo", "PR-001")] = "3"

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        if args[:1] == ("push",):
            raise subprocess.CalledProcessError(1, ["git", "push"], stderr="nope")
        return _FakeCompletedProcess()

    monkeypatch.setattr(repo_ops.git_ops, "_git", fake_git)
    monkeypatch.setattr(repo_ops, "retry_transient", lambda func, operation_name=None: func())

    assert _run(runner.process_pending_uploads()) is None
    assert runner.redis.store[task_spec_hash_key("demo", "PR-001")] == "old-hash"
    assert runner.redis.store[retry_count_key("demo", "PR-001")] == "3"
    assert runner.redis.store[key] == manifest


def test_process_pending_uploads_returns_none_when_newer_manifest_exists(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    Path(runner.repo_path).mkdir(parents=True)
    staging = tmp_path / "uploads" / "demo"
    staging.mkdir(parents=True)
    (staging / "PR-001.md").write_text("# PR-001\n", encoding="utf-8")
    key = f"upload:{runner.name}:pending"
    old_manifest = json.dumps({"files": ["PR-001.md"], "staging_dir": str(staging)})
    runner.redis.store[key] = old_manifest

    monkeypatch.setattr(repo_ops.git_ops, "_git", lambda *args, **kwargs: _FakeCompletedProcess())
    monkeypatch.setattr(repo_ops, "retry_transient", lambda func, operation_name=None: func())
    monkeypatch.setattr(
        runner,
        "_delete_upload_if_unchanged",
        lambda upload_key, expected, **kwargs: asyncio.sleep(0, result=False),
    )

    assert _run(runner.process_pending_uploads()) is None
    assert staging.is_dir()
    assert any("Newer upload pending" in event for event in runner.events)


def test_process_pending_uploads_reports_configured_push_branch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    runner.repo_config.branch = "develop"
    Path(runner.repo_path).mkdir(parents=True)
    staging = tmp_path / "uploads" / "demo"
    staging.mkdir(parents=True)
    (staging / "AGENTS.md").write_text("# AGENTS\n", encoding="utf-8")
    key = f"upload:{runner.name}:pending"
    manifest = json.dumps({"files": ["AGENTS.md"], "staging_dir": str(staging)})
    runner.redis.store[key] = manifest

    monkeypatch.setattr(repo_ops.git_ops, "_git", lambda *args, **kwargs: _FakeCompletedProcess())
    monkeypatch.setattr(repo_ops, "retry_transient", lambda func, operation_name=None: func())
    monkeypatch.setattr(repo_ops.shutil, "rmtree", lambda path, ignore_errors=True: None)

    assert _run(runner.process_pending_uploads()) is True
    assert any(
        "Uploaded 0 task files to tasks/ and pushed to develop" in event
        for event in runner.events
    )


def test_process_pending_uploads_counts_unique_task_filenames_in_log_event(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    Path(runner.repo_path).mkdir(parents=True)
    staging = tmp_path / "uploads" / "demo"
    staging.mkdir(parents=True)
    (staging / "QUEUE.md").write_text("# Queue\n", encoding="utf-8")
    (staging / "PR-001.md").write_text("# PR-001\n", encoding="utf-8")
    (staging / "PR-002.md").write_text("# PR-002\n", encoding="utf-8")
    key = f"upload:{runner.name}:pending"
    manifest = json.dumps(
        {
            "files": ["QUEUE.md", "PR-001.md", "PR-001.md", "PR-002.md"],
            "staging_dir": str(staging),
        }
    )
    runner.redis.store[key] = manifest

    monkeypatch.setattr(repo_ops.git_ops, "_git", lambda *args, **kwargs: _FakeCompletedProcess())
    monkeypatch.setattr(repo_ops, "retry_transient", lambda func, operation_name=None: func())
    monkeypatch.setattr(repo_ops.shutil, "rmtree", lambda path, ignore_errors=True: None)

    assert _run(runner.process_pending_uploads()) is True
    assert any(
        "Uploaded 2 task files to tasks/ and pushed to main" in event
        for event in runner.events
    )


def test_process_pending_uploads_clears_crashed_pr_ids_on_reupload_with_sibling(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """PR-186: Re-uploading a previously-crashed task file is the user's
    signal to retry. The crashed-pr-ids set on the runner must be
    cleared for any uploaded PR-id so the next IDLE cycle picks the
    task again instead of treating it as still ERROR. Other
    crashed entries that were not re-uploaded must remain intact."""
    runner = _Runner(tmp_path)
    runner._crashed_task_pr_ids.update({"PR-001", "PR-999"})
    Path(runner.repo_path).mkdir(parents=True)
    staging = tmp_path / "uploads" / "demo"
    staging.mkdir(parents=True)
    (staging / "PR-001.md").write_text("# PR-001\n", encoding="utf-8")
    key = f"upload:{runner.name}:pending"
    manifest = json.dumps({"files": ["PR-001.md"], "staging_dir": str(staging)})
    runner.redis.store[key] = manifest

    monkeypatch.setattr(repo_ops.git_ops, "_git", lambda *args, **kwargs: _FakeCompletedProcess())
    monkeypatch.setattr(repo_ops, "retry_transient", lambda func, operation_name=None: func())
    monkeypatch.setattr(repo_ops.shutil, "rmtree", lambda path, ignore_errors=True: None)

    assert _run(runner.process_pending_uploads()) is True
    assert runner._crashed_task_pr_ids == {"PR-999"}


def test_process_pending_uploads_clears_status_write_fallback_on_reupload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Re-uploading a task clears the in-memory status-write fallback."""
    runner = _Runner(tmp_path)
    runner._status_write_failed_task_pr_ids.update({"PR-001", "PR-999"})
    runner.redis.store["recovered_tasks:demo"] = '["PR-001"]'
    Path(runner.repo_path).mkdir(parents=True)
    staging = tmp_path / "uploads" / "demo"
    staging.mkdir(parents=True)
    (staging / "PR-001.md").write_text("# PR-001\n", encoding="utf-8")
    key = f"upload:{runner.name}:pending"
    manifest = json.dumps({"files": ["PR-001.md"], "staging_dir": str(staging)})
    runner.redis.store[key] = manifest

    monkeypatch.setattr(repo_ops.git_ops, "_git", lambda *args, **kwargs: _FakeCompletedProcess())
    monkeypatch.setattr(repo_ops, "retry_transient", lambda func, operation_name=None: func())
    monkeypatch.setattr(repo_ops.shutil, "rmtree", lambda path, ignore_errors=True: None)

    assert _run(runner.process_pending_uploads()) is True
    assert runner._status_write_failed_task_pr_ids == {"PR-999"}
    assert runner.cleared_status_write_failed_calls == 1
    assert "recovered_tasks:demo" not in runner.redis.store


def test_process_pending_uploads_ignores_legacy_recovered_delete_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Legacy marker cleanup is best-effort during re-upload."""
    runner = _Runner(tmp_path)
    runner._status_write_failed_task_pr_ids.add("PR-001")
    runner.redis.store["recovered_tasks:demo"] = '["PR-001"]'
    runner.redis.delete_error = RuntimeError("redis down")
    Path(runner.repo_path).mkdir(parents=True)
    staging = tmp_path / "uploads" / "demo"
    staging.mkdir(parents=True)
    (staging / "PR-001.md").write_text("# PR-001\n", encoding="utf-8")
    key = f"upload:{runner.name}:pending"
    manifest = json.dumps({"files": ["PR-001.md"], "staging_dir": str(staging)})
    runner.redis.store[key] = manifest

    monkeypatch.setattr(repo_ops.git_ops, "_git", lambda *args, **kwargs: _FakeCompletedProcess())
    monkeypatch.setattr(repo_ops, "retry_transient", lambda func, operation_name=None: func())
    monkeypatch.setattr(repo_ops.shutil, "rmtree", lambda path, ignore_errors=True: None)

    assert _run(runner.process_pending_uploads()) is True
    assert runner._status_write_failed_task_pr_ids == set()
    assert runner.cleared_status_write_failed_calls == 1


def test_process_pending_uploads_clears_persisted_status_fallback_before_hydrate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Re-upload clears persisted fallback state even before recovery hydrates it."""
    runner = _Runner(tmp_path)
    runner.redis.store["status_write_failed_tasks:demo"] = '["PR-001", "PR-999"]'
    Path(runner.repo_path).mkdir(parents=True)
    staging = tmp_path / "uploads" / "demo"
    staging.mkdir(parents=True)
    (staging / "PR-001.md").write_text("# PR-001\n", encoding="utf-8")
    key = f"upload:{runner.name}:pending"
    manifest = json.dumps({"files": ["PR-001.md"], "staging_dir": str(staging)})
    runner.redis.store[key] = manifest

    monkeypatch.setattr(repo_ops.git_ops, "_git", lambda *args, **kwargs: _FakeCompletedProcess())
    monkeypatch.setattr(repo_ops, "retry_transient", lambda func, operation_name=None: func())
    monkeypatch.setattr(repo_ops.shutil, "rmtree", lambda path, ignore_errors=True: None)

    assert _run(runner.process_pending_uploads()) is True
    assert runner._status_write_failed_task_pr_ids == {"PR-999"}
    assert json.loads(runner.redis.store["status_write_failed_tasks:demo"]) == [
        "PR-999"
    ]


def test_process_pending_uploads_clears_cancellation_cause_on_reupload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Re-uploading a task clears stale cancellation cause attribution."""
    runner = _Runner(tmp_path)
    runner.redis.store["cancellation:demo:PR-001"] = "{}"
    Path(runner.repo_path).mkdir(parents=True)
    staging = tmp_path / "uploads" / "demo"
    staging.mkdir(parents=True)
    (staging / "PR-001.md").write_text("# PR-001\n", encoding="utf-8")
    key = f"upload:{runner.name}:pending"
    manifest = json.dumps({"files": ["PR-001.md"], "staging_dir": str(staging)})
    runner.redis.store[key] = manifest

    monkeypatch.setattr(repo_ops.git_ops, "_git", lambda *args, **kwargs: _FakeCompletedProcess())
    monkeypatch.setattr(repo_ops, "retry_transient", lambda func, operation_name=None: func())
    monkeypatch.setattr(repo_ops.shutil, "rmtree", lambda path, ignore_errors=True: None)

    assert _run(runner.process_pending_uploads()) is True
    assert "cancellation:demo:PR-001" not in runner.redis.store


def test_process_pending_uploads_clears_crashed_pr_ids_on_reupload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Re-uploading an errored task file is the user's retry signal."""
    runner = _Runner(tmp_path)
    runner._crashed_task_pr_ids.update({"PR-001", "PR-999"})
    Path(runner.repo_path).mkdir(parents=True)
    staging = tmp_path / "uploads" / "demo"
    staging.mkdir(parents=True)
    (staging / "PR-001.md").write_text("# PR-001\n", encoding="utf-8")
    key = f"upload:{runner.name}:pending"
    manifest = json.dumps({"files": ["PR-001.md"], "staging_dir": str(staging)})
    runner.redis.store[key] = manifest

    monkeypatch.setattr(repo_ops.git_ops, "_git", lambda *args, **kwargs: _FakeCompletedProcess())
    monkeypatch.setattr(repo_ops, "retry_transient", lambda func, operation_name=None: func())
    monkeypatch.setattr(repo_ops.shutil, "rmtree", lambda path, ignore_errors=True: None)

    assert _run(runner.process_pending_uploads()) is True
    assert runner._crashed_task_pr_ids == {"PR-999"}


def test_process_pending_uploads_flips_canceled_to_todo_in_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """PR-267: Uploading a ERROR task flips its snapshot status to TODO."""
    from src.models import QueueTask, TaskStatus

    runner = _Runner(tmp_path)
    runner.state.current_queue = [
        QueueTask(pr_id="PR-001", title="Crashed", status=TaskStatus.ERROR),
        QueueTask(pr_id="PR-002", title="Other", status=TaskStatus.TODO),
    ]
    Path(runner.repo_path).mkdir(parents=True)
    staging = tmp_path / "uploads" / "demo"
    staging.mkdir(parents=True)
    (staging / "PR-001.md").write_text("# PR-001\n", encoding="utf-8")
    key = f"upload:{runner.name}:pending"
    manifest = json.dumps({"files": ["PR-001.md"], "staging_dir": str(staging)})
    runner.redis.store[key] = manifest

    monkeypatch.setattr(repo_ops.git_ops, "_git", lambda *args, **kwargs: _FakeCompletedProcess())
    monkeypatch.setattr(repo_ops, "retry_transient", lambda func, operation_name=None: func())
    monkeypatch.setattr(repo_ops.shutil, "rmtree", lambda path, ignore_errors=True: None)

    assert _run(runner.process_pending_uploads()) is True
    statuses = {q.pr_id: q.status for q in runner.state.current_queue}
    assert statuses == {"PR-001": TaskStatus.TODO, "PR-002": TaskStatus.TODO}


def test_clear_canceled_in_snapshot_refreshes_snapshot_timestamp(
    tmp_path: Path,
) -> None:
    """Re-stamps current_queue_snapshot_at via the __setattr__ hook.

    Regression: in-place ``snapshot[index] = ...`` bypasses the hook,
    leaving ``current_queue_snapshot_at`` pinned to the pre-mutation
    time. Clients keying refreshes off ``snapshot_at`` would miss the
    ERROR→TODO transition until the next IDLE rebuild.
    """
    from datetime import datetime, timedelta, timezone

    from src.models import QueueTask, RepoState, TaskStatus

    runner = _Runner(tmp_path)
    runner.state = RepoState(url=runner.repo_config.url, name=runner.name)
    runner.state.current_queue = [
        QueueTask(pr_id="PR-001", title="Crashed", status=TaskStatus.ERROR),
    ]
    stale_stamp = datetime.now(timezone.utc) - timedelta(hours=1)
    runner.state.current_queue_snapshot_at = stale_stamp

    runner._clear_canceled_in_snapshot({"PR-001"})

    refreshed = runner.state.current_queue_snapshot_at
    assert refreshed is not None
    assert refreshed > stale_stamp


def test_clear_canceled_in_snapshot_skips_reassign_when_no_match(
    tmp_path: Path,
) -> None:
    """No ERROR match ⇒ leave snapshot_at alone (idempotent)."""
    from datetime import datetime, timedelta, timezone

    from src.models import QueueTask, RepoState, TaskStatus

    runner = _Runner(tmp_path)
    runner.state = RepoState(url=runner.repo_config.url, name=runner.name)
    runner.state.current_queue = [
        QueueTask(pr_id="PR-001", title="Done", status=TaskStatus.TODO),
    ]
    stale_stamp = datetime.now(timezone.utc) - timedelta(hours=1)
    runner.state.current_queue_snapshot_at = stale_stamp

    runner._clear_canceled_in_snapshot({"PR-001"})

    assert runner.state.current_queue_snapshot_at == stale_stamp





def test_process_pending_uploads_logs_overwrite_collision_hashes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    repo_dir = Path(runner.repo_path)
    tasks_dir = repo_dir / "tasks"
    tasks_dir.mkdir(parents=True)
    existing = tasks_dir / "PR-001.md"
    existing.write_text("# old\n", encoding="utf-8")

    staging = tmp_path / "uploads" / "demo"
    staging.mkdir(parents=True)
    incoming = staging / "PR-001.md"
    incoming.write_text("# new\n", encoding="utf-8")
    key = f"upload:{runner.name}:pending"
    runner.redis.store[key] = json.dumps(
        {"files": ["PR-001.md"], "staging_dir": str(staging)}
    )

    monkeypatch.setattr(
        repo_ops.git_ops, "_git", lambda *args, **kwargs: _FakeCompletedProcess()
    )
    monkeypatch.setattr(
        repo_ops, "retry_transient", lambda func, operation_name=None: func()
    )
    monkeypatch.setattr(repo_ops.shutil, "rmtree", lambda path, ignore_errors=True: None)

    assert _run(runner.process_pending_uploads()) is True

    old_hash = hashlib.sha256(b"# old\n").hexdigest()
    new_hash = hashlib.sha256(b"# new\n").hexdigest()
    assert any(
        "Upload overwrite warning: tasks/PR-001.md "
        f"existing_sha256={old_hash} new_sha256={new_hash}" in event
        for event in runner.events
    )


def test_process_pending_uploads_skips_queue_md_only_manifest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A manifest containing only QUEUE.md must be discarded without
    touching git: QUEUE.md is gitignored (PR-181) and ``git add`` would
    abort the whole upload, blocking dispatch retries indefinitely.
    """
    runner = _Runner(tmp_path)
    Path(runner.repo_path).mkdir(parents=True)
    staging = tmp_path / "uploads" / "demo"
    staging.mkdir(parents=True)
    (staging / "QUEUE.md").write_text("# Queue\n", encoding="utf-8")
    manifest = json.dumps({"files": ["QUEUE.md"], "staging_dir": str(staging)})
    key = f"upload:{runner.name}:pending"
    runner.redis.store[key] = manifest
    git_calls: list[tuple[Any, ...]] = []
    removed: list[Path] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        git_calls.append((repo_path, *args))
        return _FakeCompletedProcess()

    monkeypatch.setattr(repo_ops.git_ops, "_git", fake_git)
    monkeypatch.setattr(repo_ops, "retry_transient", lambda func, operation_name=None: func())
    monkeypatch.setattr(
        repo_ops.shutil,
        "rmtree",
        lambda path, ignore_errors=True: removed.append(Path(path)),
    )

    assert _run(runner.process_pending_uploads()) is False
    assert git_calls == []
    assert removed == [staging]
    assert key not in runner.redis.store
    assert any(
        "Skipping QUEUE.md from upload" in event for event in runner.events
    )


def test_process_pending_uploads_handles_failures_and_safe_mode(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    Path(runner.repo_path).mkdir(parents=True)
    staging = tmp_path / "uploads" / "demo"
    staging.mkdir(parents=True)
    (staging / "PR-001.md").write_text("# PR-001\n", encoding="utf-8")
    manifest = json.dumps({"files": ["PR-001.md"], "staging_dir": str(staging)})
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


@pytest.fixture
def upload_git(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _Runner:
    """Real local transport; no GitHub or live daemon state."""
    runner = _Runner(tmp_path)
    remote = tmp_path / "remote.git"
    subprocess.run(["git", "init", "--bare", str(remote)], check=True, capture_output=True)
    subprocess.run(["git", "clone", str(remote), runner.repo_path], check=True, capture_output=True)
    git = repo_ops.git_ops._git
    git(runner.repo_path, "config", "user.email", "test@example.com")
    git(runner.repo_path, "config", "user.name", "Test")
    git(runner.repo_path, "checkout", "-b", "main")
    tasks = Path(runner.repo_path) / "tasks"
    tasks.mkdir()
    (tasks / "PR-001.md").write_text(_upload_spec("ERROR"))
    git(runner.repo_path, "add", ".")
    git(runner.repo_path, "commit", "-m", "accepted task")
    git(runner.repo_path, "push", "origin", "main")
    _patch_retry_passthrough(monkeypatch)
    runner.redis.store[retry_count_key("demo", "PR-001")] = "3"
    runner.redis.store[task_spec_hash_key("demo", "PR-001")] = task_spec_content_hash(_upload_spec("ERROR"))
    runner.redis.store["cancellation:demo:PR-001"] = "failure evidence"
    runner._crashed_task_pr_ids = {"PR-001"}
    runner._status_write_failed_task_pr_ids = {"PR-001"}
    runner.state.current_queue = [SimpleNamespace(pr_id="PR-001", status=repo_ops.TaskStatus.ERROR)]
    return runner


def _upload_spec(status: str = "TODO", instruction: str = "original") -> str:
    reason = "blocked_reason: crash\n" if status == "ERROR" else ""
    return (
        f"---\nstatus: {status}\n{reason}---\n"
        "# PR-001: Task\nBranch: task-one\n- Type: bugfix\n"
        "- Complexity: medium\n- Depends on: none\n"
        f"\n{instruction}\n"
    )


def _stage_upload(runner: _Runner, content: str) -> tuple[Path, str]:
    staging = Path(runner.repo_path).parent / "staging"
    staging.mkdir(exist_ok=True)
    (staging / "PR-001.md").write_text(content)
    key = f"upload:{runner.name}:pending"
    runner.redis.store[key] = json.dumps({
        "files": ["PR-001.md"], "staging_dir": str(staging),
        # A stale archive's manifest is not authoritative about accepted bytes.
        "task_hashes": {"PR-001": "untrusted-hash"},
    })
    return staging, key


@pytest.mark.parametrize("status", ["ERROR", "DONE", "DOING"])
@pytest.mark.parametrize("bookkeeping_only", [False, True])
def test_upload_replay_preserves_exact_bytes_and_history(
    upload_git: _Runner, status: str, bookkeeping_only: bool,
) -> None:
    runner = upload_git
    task = Path(runner.repo_path) / "tasks/PR-001.md"
    task.write_text(_upload_spec(status))
    git = repo_ops.git_ops._git
    git(runner.repo_path, "add", ".")
    git(runner.repo_path, "commit", "--allow-empty", "-m", "daemon bookkeeping")
    git(runner.repo_path, "push", "origin", "main")
    accepted = task.read_bytes()
    head = git(runner.repo_path, "rev-parse", "HEAD").stdout
    history = dict(runner.redis.store)
    for _ in range(2):
        _stage_upload(runner, _upload_spec() if bookkeeping_only else accepted.decode())
        assert _run(runner.process_pending_uploads()) is True
        assert task.read_bytes() == accepted
        assert git(runner.repo_path, "rev-parse", "HEAD").stdout == head
        assert runner.redis.store == history
        assert runner._crashed_task_pr_ids == {"PR-001"}
        assert runner._status_write_failed_task_pr_ids == {"PR-001"}
        assert runner.cleared_status_write_failed_calls == 0
        assert runner.state.current_queue[0].status == repo_ops.TaskStatus.ERROR


def test_changed_upload_replaces_only_its_history_once(upload_git: _Runner) -> None:
    runner = upload_git
    runner.state.current_queue = None
    _stage_upload(runner, _upload_spec(instruction="new instruction"))
    assert _run(runner.process_pending_uploads()) is True
    assert runner.redis.store[retry_count_key("demo", "PR-001")] == "0"
    assert not runner._crashed_task_pr_ids
    assert not runner._status_write_failed_task_pr_ids
    assert "cancellation:demo:PR-001" not in runner.redis.store
    assert runner.cleared_status_write_failed_calls == 1
    runner.redis.store[retry_count_key("demo", "PR-001")] = "2"
    _stage_upload(runner, _upload_spec(instruction="new instruction"))
    assert _run(runner.process_pending_uploads()) is True
    assert runner.redis.store[retry_count_key("demo", "PR-001")] == "2"
    assert runner.cleared_status_write_failed_calls == 1


@pytest.mark.parametrize("conflict", [
    "done", "doing", "missing", "stale", "task", "branch", "pr", "invalid",
    "publication-id", "publication-branch",
])
def test_upload_conflict_keeps_entire_batch_staged(upload_git: _Runner, conflict: str) -> None:
    runner = upload_git
    task = Path(runner.repo_path) / "tasks/PR-001.md"
    incoming = _upload_spec(instruction="new instruction")
    if conflict in {"done", "doing"}:
        task.write_text(_upload_spec(conflict.upper()))
    elif conflict == "missing":
        task.unlink()
        incoming = _upload_spec()
    elif conflict == "stale":
        task.write_text(_upload_spec(instruction="another accepted spec"))
        incoming = _upload_spec()
    elif conflict.startswith("publication-"):
        runner.state.publication_attempt = SimpleNamespace(
            pending=True, task_id="PR-001" if conflict == "publication-id" else "PR-002",
            branch="task-one",
        )
    elif conflict in {"task", "branch", "invalid"}:
        runner.state.current_task = SimpleNamespace(
            pr_id="PR-001" if conflict == "task" else "PR-002",
            branch="task-one",
        )
        if conflict == "invalid":
            incoming = "invalid task"
    else:
        runner.state.current_pr = SimpleNamespace(branch="task-one")
    staging, key = _stage_upload(runner, incoming)
    (staging / "AGENTS.md").write_text("new instructions")
    manifest = json.loads(runner.redis.store[key])
    manifest["files"].insert(0, "AGENTS.md")
    runner.redis.store[key] = json.dumps(manifest)
    before = task.read_bytes() if task.exists() else None
    history = dict(runner.redis.store)
    assert _run(runner.process_pending_uploads()) is None
    if conflict == "done":
        verdict = json.loads(runner.redis.store[key])
        assert "completed task replacement blocked" in verdict.pop("admission_error")
        assert verdict.pop("admission_base_sha")
        assert verdict == json.loads(history[key])
        history[key] = runner.redis.store[key]
    assert runner.redis.store == history
    assert staging.is_dir()
    assert (task.read_bytes() if task.exists() else None) == before
    assert not (Path(runner.repo_path) / "AGENTS.md").exists()


def test_ambiguous_push_replay_cannot_reset_history(
    upload_git: _Runner, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = upload_git
    git = repo_ops.git_ops._git
    staging, key = _stage_upload(runner, _upload_spec(instruction="new instruction"))

    def ambiguous_push(repo: str, *args: str, **kwargs: Any) -> Any:
        result = git(repo, *args, **kwargs)
        if args[0] == "push":
            raise subprocess.TimeoutExpired("git push", 60)
        return result

    monkeypatch.setattr(repo_ops.git_ops, "_git", ambiguous_push)
    assert _run(runner.process_pending_uploads()) is None
    assert key in runner.redis.store and staging.is_dir()
    monkeypatch.setattr(repo_ops.git_ops, "_git", git)
    assert _run(runner.process_pending_uploads()) is True
    assert runner.redis.store[retry_count_key("demo", "PR-001")] == "3"
    assert runner._crashed_task_pr_ids == {"PR-001"}
    assert runner.redis.store["cancellation:demo:PR-001"] == "failure evidence"
    assert runner.cleared_status_write_failed_calls == 0


@pytest.mark.parametrize("owner", ["task", "pr", "publication", "settled-publication"])
def test_changed_upload_with_unrelated_owner_is_admitted(upload_git: _Runner, owner: str) -> None:
    runner = upload_git
    runner.state.current_queue = None
    if owner == "task":
        runner.state.current_task = SimpleNamespace(pr_id="PR-002", branch="other")
    elif owner == "pr":
        runner.state.current_pr = SimpleNamespace(branch="other")
    else:
        runner.state.publication_attempt = SimpleNamespace(
            pending=owner == "publication", task_id="PR-002", branch="other",
        )
    _stage_upload(runner, _upload_spec(instruction="changed"))
    assert _run(runner.process_pending_uploads()) is True
    assert runner.redis.store[retry_count_key("demo", "PR-001")] == "0"


def test_recorded_completion_bytes_survive_pending_replacement(upload_git: _Runner) -> None:
    runner = upload_git
    git = repo_ops.git_ops._git
    task = Path(runner.repo_path) / "tasks/PR-001.md"
    accepted = task.read_bytes()
    receipt = {
        "schema_version": 1, "repository": "repo", "base_branch": "main",
        "completions": {"PR-001": {
            "task_sha256": hashlib.sha256(accepted).hexdigest(),
            "merge_commit": git(runner.repo_path, "rev-parse", "HEAD").stdout.strip(),
            "pull_request": 1, "reason": "completed outside task branch",
        }},
    }
    (task.parent / "completions.json").write_text(json.dumps(receipt))
    _stage_upload(runner, _upload_spec(instruction="changed"))
    assert _run(runner.process_pending_uploads()) is None
    assert task.read_bytes() == accepted
    assert runner.redis.store[retry_count_key("demo", "PR-001")] == "3"


@pytest.mark.parametrize("inhibitor", ["pause", "stop", "memory_pause", "unreadable_pause"])
def test_pending_upload_obeys_durable_controls(upload_git: _Runner, inhibitor: str) -> None:
    from src.keyspace import control_stop, pipeline_state
    from src.models import RepoState

    runner = upload_git
    staging, key = _stage_upload(runner, _upload_spec(instruction="accepted bytes"))
    if inhibitor == "pause":
        runner.redis.store[pipeline_state(runner.name)] = RepoState(
            name=runner.name, url=runner.repo_config.url, user_paused=True,
        ).model_dump_json()
    elif inhibitor == "stop":
        runner.redis.store[control_stop(runner.name)] = "1"
    elif inhibitor == "memory_pause":
        runner.state.user_paused = True
    else:
        runner.redis.store[pipeline_state(runner.name)] = "unreadable"
    git = repo_ops.git_ops._git
    before = git(runner.repo_path, "rev-parse", "HEAD").stdout
    manifest = runner.redis.store[key]
    assert _run(runner.process_pending_uploads()) is None
    assert runner.redis.store[key] == manifest
    assert (staging / "PR-001.md").read_text() == _upload_spec(instruction="accepted bytes")
    assert git(runner.repo_path, "rev-parse", "HEAD").stdout == before


def test_pending_upload_prunes_stale_members_without_losing_sibling(upload_git: _Runner) -> None:
    runner = upload_git
    runner.state.current_queue = None
    content = _upload_spec(instruction="exact candidate\nwith trailing spaces  \n")
    _, key = _stage_upload(runner, content)
    manifest = json.loads(runner.redis.store[key])
    manifest["files"] += ["PR-999.md", "../outside.md", 7, "PR-001.md"]
    runner.redis.store[key] = json.dumps(manifest)
    assert _run(runner.process_pending_uploads()) is True
    assert (Path(runner.repo_path) / "tasks/PR-001.md").read_bytes() == content.encode()
    head = repo_ops.git_ops._git(runner.repo_path, "rev-parse", "HEAD").stdout
    assert _run(runner.process_pending_uploads()) is False
    assert repo_ops.git_ops._git(runner.repo_path, "rev-parse", "HEAD").stdout == head


@pytest.mark.parametrize("conflict", ["dependency", "branch", "cycle", "identity"])
def test_pending_upload_rechecks_current_effective_graph(upload_git: _Runner, conflict: str) -> None:
    runner = upload_git
    content = _upload_spec(instruction="new accepted candidate")
    if conflict == "dependency":
        content = content.replace("Depends on: none", "Depends on: PR-999")
    elif conflict == "identity":
        content = content.replace("# PR-001:", "# PR-002:")
    else:
        sibling = _upload_spec().replace("PR-001", "PR-002")
        if conflict == "cycle":
            sibling = sibling.replace("Branch: task-one", "Branch: task-two")
            sibling = sibling.replace("Depends on: none", "Depends on: PR-001")
            content = content.replace("Depends on: none", "Depends on: PR-002")
        (Path(runner.repo_path) / "tasks/PR-002.md").write_text(sibling)
    staging, key = _stage_upload(runner, content)
    before = (Path(runner.repo_path) / "tasks/PR-001.md").read_bytes()
    assert _run(runner.process_pending_uploads()) is None
    assert key in runner.redis.store
    assert (staging / "PR-001.md").read_bytes() == content.encode()
    assert (Path(runner.repo_path) / "tasks/PR-001.md").read_bytes() == before
    assert any("current task graph invalid" in event for event in runner.events)


def test_status_writer_reflow_replay_publishes_sibling_without_touching_history(upload_git):
    from src.queue_parser import write_frontmatter_status

    runner = upload_git
    task = Path(runner.repo_path) / "tasks/PR-001.md"
    incoming = _upload_spec().replace("status: TODO\n", "status: TODO\nnotes: " + "a long rationale " * 12 + "\n")
    task.write_text(incoming)
    write_frontmatter_status(task, "DONE")
    accepted = task.read_bytes()
    assert task_spec_content_hash(accepted.decode()) != task_spec_content_hash(incoming)
    git = repo_ops.git_ops._git
    git(runner.repo_path, "add", ".")
    git(runner.repo_path, "commit", "-m", "task completed")
    git(runner.repo_path, "push", "origin", "main")
    history = dict(runner.redis.store)
    staging, key = _stage_upload(runner, incoming)
    sibling = _upload_spec().replace("PR-001", "PR-002").replace("task-one", "task-two")
    (staging / "PR-002.md").write_text(sibling)
    manifest = json.loads(runner.redis.store[key])
    manifest["files"].append("PR-002.md")
    runner.redis.store[key] = json.dumps(manifest)
    assert _run(runner.process_pending_uploads()) is True
    assert task.read_bytes() == accepted
    assert (task.parent / "PR-002.md").read_text() == sibling
    for k, v in history.items():
        assert runner.redis.store[k] == v
    assert runner._crashed_task_pr_ids == {"PR-001"}
    assert key not in runner.redis.store
    assert git(runner.repo_path, "show", "origin/main:tasks/PR-001.md").stdout.encode() == accepted
    assert git(runner.repo_path, "show", "origin/main:tasks/PR-002.md").stdout == sibling


def test_blocked_upload_survives_restart_and_rechecks_changed_base(upload_git, monkeypatch):
    runner = upload_git
    task = Path(runner.repo_path) / "tasks/PR-001.md"
    task.write_text(_upload_spec("DONE"))
    incoming = _upload_spec(instruction="changed instructions")
    staging, key = _stage_upload(runner, incoming)
    assert _run(runner.process_pending_uploads()) is None
    verdict = runner.redis.store[key]
    assert len(runner.events) == 1
    restarted = _Runner(Path(runner.repo_path).parent)
    restarted.redis = runner.redis
    monkeypatch.setattr(repo_ops, "get_merged_pr_ids", lambda *a: pytest.fail("unchanged batch revalidated"))
    for _ in range(3):
        assert _run(restarted.process_pending_uploads()) is None
    assert restarted.events == []
    assert restarted.redis.store[key] == verdict
    assert (staging / "PR-001.md").read_text() == incoming
    monkeypatch.setattr(repo_ops, "get_merged_pr_ids", lambda *a: set())
    # A fetched base change can resolve a conflict; do not cache across it.
    task.write_text(incoming)
    git = repo_ops.git_ops._git
    git(runner.repo_path, "add", ".")
    git(runner.repo_path, "commit", "-m", "accepted corrected task")
    git(runner.repo_path, "push", "origin", "main")
    assert _run(restarted.process_pending_uploads()) is True
    assert key not in runner.redis.store
    assert not staging.exists()


@pytest.mark.parametrize("race", [False, True])
def test_blocked_upload_cas_never_overwrites_newer_batch(upload_git, race):
    runner = upload_git
    (Path(runner.repo_path) / "tasks/PR-001.md").write_text(_upload_spec("DONE"))
    staging, key = _stage_upload(runner, _upload_spec(instruction="changed"))
    original = runner.redis.store[key]

    async def fail_or_race(*args):
        if not race:
            raise RuntimeError("Redis unavailable")
        runner.redis.store[key] = "newer upload"
        return 0

    runner.redis.eval = fail_or_race
    assert _run(runner.process_pending_uploads()) is None
    assert runner.redis.store[key] == ("newer upload" if race else original)
    assert staging.is_dir()
    assert not any("Task upload blocked" in event for event in runner.events)


def test_blocked_upload_base_read_failure_retains_source(upload_git, monkeypatch):
    runner = upload_git
    (Path(runner.repo_path) / "tasks/PR-001.md").write_text(_upload_spec("DONE"))
    staging, key = _stage_upload(runner, _upload_spec(instruction="changed"))
    assert _run(runner.process_pending_uploads()) is None
    manifest = runner.redis.store[key]

    def unavailable(*args, **kwargs):
        raise subprocess.CalledProcessError(1, "git rev-parse")

    monkeypatch.setattr(repo_ops.git_ops, "_git", unavailable)
    assert _run(runner.process_pending_uploads()) is None
    assert runner.redis.store[key] == manifest
    assert staging.is_dir()


@pytest.mark.parametrize("pending", [False, True])
def test_reflow_replay_preserves_exact_completion_receipt(upload_git, pending):
    from src.queue_parser import write_frontmatter_status
    from src.web.services.upload_validation import effective_recorded_completions

    runner = upload_git
    task = Path(runner.repo_path) / "tasks/PR-001.md"
    incoming = _upload_spec().replace("status: TODO\n", "status: TODO\nnotes: " + "long rationale " * 14 + "\n")
    task.write_text(incoming)
    write_frontmatter_status(task, "DONE")
    accepted = task.read_bytes()
    git = repo_ops.git_ops._git
    git(runner.repo_path, "add", ".")
    git(runner.repo_path, "commit", "-m", "completed task")
    sha = git(runner.repo_path, "rev-parse", "HEAD").stdout.strip()
    (task.parent / "completions.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "repository": "repo",
                "base_branch": "main",
                "completions": {
                    "PR-001": {
                        "task_sha256": hashlib.sha256(accepted).hexdigest(),
                        "merge_commit": sha,
                        "pull_request": 1,
                        "reason": "completed outside task branch",
                    }
                },
            }
        )
    )
    git(runner.repo_path, "add", ".")
    git(runner.repo_path, "commit", "-m", "receipt")
    git(runner.repo_path, "push", "origin", "main")
    _, key = _stage_upload(runner, incoming)
    assert effective_recorded_completions(
        runner.repo_path,
        "main",
        {"PR-001"},
        {} if pending else {"PR-001.md": incoming},
        runner.redis.store[key] if pending else None,
    ) == {"PR-001"}
    assert _run(runner.process_pending_uploads()) is True
    assert task.read_bytes() == accepted


def test_upload_replay_receipt_reader_keeps_unrelated_unreadable_legacy_bytes(upload_git):
    runner = upload_git
    runner.state.current_queue = None
    legacy = Path(runner.repo_path) / "tasks/PR-002.md"
    legacy.write_bytes(b"\xff historical data")
    git = repo_ops.git_ops._git
    git(runner.repo_path, "add", ".")
    git(runner.repo_path, "commit", "-m", "legacy bytes")
    git(runner.repo_path, "push", "origin", "main")
    _stage_upload(runner, _upload_spec(instruction="corrected task"))
    assert _run(runner.process_pending_uploads()) is True
    assert legacy.read_bytes() == b"\xff historical data"
