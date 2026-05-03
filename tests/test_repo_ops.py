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
        self._crashed_task_pr_ids: set[str] = set()
        self._recovered_task_pr_ids: set[str] = set()
        self._recovered_persist_calls: list[frozenset[str]] = []

    def log_event(self, message: str) -> None:
        self.events.append(message)

    async def _persist_recovered_task_pr_ids(self) -> None:
        self._recovered_persist_calls.append(
            frozenset(self._recovered_task_pr_ids)
        )


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


def test_parse_base_queue_reads_local_working_tree_and_handles_missing_file(
    tmp_path: Path,
) -> None:
    """PR-181: ``_parse_base_queue`` reads ``tasks/QUEUE.md`` from the
    local working tree (gitignored) and returns ``None`` if the file is
    absent or unreadable, letting the caller treat the missing snapshot
    as a retryable ERROR until the next IDLE cycle regenerates it.
    """
    runner = _Runner(tmp_path)
    queue_text = (
        "# Task Queue\n\n"
        "## PR-112: Example\n"
        "- Status: TODO\n"
        "- Tasks file: tasks/PR-112.md\n"
        "- Branch: pr-112-coverage-repo-ops\n"
    )

    repo = Path(runner.repo_path)
    (repo / "tasks").mkdir(parents=True)
    queue_path = repo / "tasks" / "QUEUE.md"
    queue_path.write_text(queue_text, encoding="utf-8")

    parsed = runner._parse_base_queue(strict=True)
    assert parsed is not None
    assert parsed[0].pr_id == "PR-112"
    assert parsed[0].branch == "pr-112-coverage-repo-ops"

    queue_path.unlink()
    assert runner._parse_base_queue() is None


def test_parse_base_queue_reads_origin_when_queue_md_tracked(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Legacy repos (pre-PR-181) keep ``tasks/QUEUE.md`` tracked on the
    base ref. Recovery may run while the working tree is checked out on
    a feature branch left behind by an interrupted cycle, so reading
    from disk would surface a branch-local snapshot. ``_parse_base_queue``
    must read directly from ``origin/{branch}`` whenever the file is
    tracked there, to ensure recovery sees the authoritative base-branch
    queue state.
    """
    runner = _Runner(tmp_path)
    repo = Path(runner.repo_path)
    repo.mkdir(parents=True, exist_ok=True)
    # A stale, branch-local working-tree copy that recovery must NOT see.
    (repo / "tasks").mkdir(parents=True)
    (repo / "tasks" / "QUEUE.md").write_text(
        "## PR-999: stale feature-branch snapshot\n- Status: DOING\n",
        encoding="utf-8",
    )

    origin_text = (
        "## PR-112: From origin\n"
        "- Status: TODO\n"
        "- Branch: pr-112-coverage-repo-ops\n"
    )

    git_calls: list[tuple[Any, ...]] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        del kwargs
        git_calls.append((repo_path, *args))
        if args[:2] == ("cat-file", "-e"):
            return _FakeCompletedProcess(returncode=0)
        if args[0] == "show":
            return _FakeCompletedProcess(stdout=origin_text)
        return _FakeCompletedProcess()

    monkeypatch.setattr(repo_ops.git_ops, "_git", fake_git)

    parsed = runner._parse_base_queue()
    assert parsed is not None
    assert len(parsed) == 1
    assert parsed[0].pr_id == "PR-112"
    assert ("show",) in {tuple(call[1:2]) for call in git_calls}


def test_parse_base_queue_origin_show_failure_returns_none(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """When the file is tracked but ``git show`` fails (transient), the
    method returns ``None`` so the caller retries instead of falling
    back to a possibly stale working-tree snapshot.
    """
    runner = _Runner(tmp_path)
    Path(runner.repo_path).mkdir(parents=True, exist_ok=True)

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        del kwargs
        if args[:2] == ("cat-file", "-e"):
            return _FakeCompletedProcess(returncode=0)
        if args[0] == "show":
            return _FakeCompletedProcess(returncode=128, stderr="fatal")
        return _FakeCompletedProcess()

    monkeypatch.setattr(repo_ops.git_ops, "_git", fake_git)

    assert runner._parse_base_queue() is None


def test_parse_base_queue_origin_show_timeout_returns_none(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A stalled ``git show`` against ``origin/{branch}`` should bail
    cleanly with ``None`` rather than escape to the caller. Covers the
    transient outage path the recovery loop relies on for retries.
    """
    runner = _Runner(tmp_path)
    Path(runner.repo_path).mkdir(parents=True, exist_ok=True)

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        del kwargs
        if args[:2] == ("cat-file", "-e"):
            return _FakeCompletedProcess(returncode=0)
        if args[0] == "show":
            raise subprocess.TimeoutExpired(["git", *args], timeout=15)
        return _FakeCompletedProcess()

    monkeypatch.setattr(repo_ops.git_ops, "_git", fake_git)

    assert runner._parse_base_queue() is None


def test_origin_queue_md_tracked_returns_true_when_cat_file_succeeds(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A clean ``returncode == 0`` from ``cat-file -e`` means the file
    exists on ``origin/{branch}`` — the legacy tracked-QUEUE state."""
    runner = _Runner(tmp_path)

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        del kwargs
        assert args[:2] == ("cat-file", "-e")
        return _FakeCompletedProcess(returncode=0)

    monkeypatch.setattr(repo_ops.git_ops, "_git", fake_git)

    assert runner._origin_queue_md_tracked() is True


def test_origin_queue_md_tracked_returns_false_when_cat_file_reports_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A non-zero ``returncode`` is a definitive "file not on origin" —
    the post-PR-181 state — and must stay distinguishable from probe
    failures so the daemon can take the gitignored/regenerate path."""
    runner = _Runner(tmp_path)

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        del kwargs
        assert args[:2] == ("cat-file", "-e")
        return _FakeCompletedProcess(returncode=1)

    monkeypatch.setattr(repo_ops.git_ops, "_git", fake_git)

    assert runner._origin_queue_md_tracked() is False


@pytest.mark.parametrize(
    "exc",
    [
        subprocess.TimeoutExpired(
            ["git", "cat-file", "-e", "origin/main:tasks/QUEUE.md"], timeout=10,
        ),
        OSError("git binary missing"),
    ],
)
def test_origin_queue_md_tracked_returns_none_on_probe_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    exc: BaseException,
) -> None:
    """Probe failures are genuinely indeterminate. Returning ``False``
    here would make legacy repos look post-PR-181 and route recovery
    into the working-tree path — where a feature-branch checkout (or
    missing ``tasks/QUEUE.md``) would yield a stale/empty queue and
    detach the daemon from in-flight DOING work. Returning ``None``
    lets each caller pick its own conservative fallback."""
    runner = _Runner(tmp_path)

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        del kwargs
        raise exc

    monkeypatch.setattr(repo_ops.git_ops, "_git", fake_git)

    assert runner._origin_queue_md_tracked() is None


def test_parse_base_queue_origin_probe_timeout_returns_none(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A flaky ``cat-file`` probe is genuinely indeterminate, not
    "untracked". Falling back to the working tree on probe failure
    would route a legacy repo's recovery into a feature-branch
    snapshot and detach the daemon from in-flight DOING work. Instead
    the helper returns ``None`` so the caller (recovery) can ERROR
    and retry once git is responsive.
    """
    runner = _Runner(tmp_path)
    repo = Path(runner.repo_path)
    (repo / "tasks").mkdir(parents=True, exist_ok=True)
    (repo / "tasks" / "QUEUE.md").write_text(
        "## PR-200: Working tree\n- Status: TODO\n- Branch: pr-200\n",
        encoding="utf-8",
    )
    show_calls: list[tuple[str, ...]] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        del kwargs
        if args[:2] == ("cat-file", "-e"):
            raise subprocess.TimeoutExpired(["git", *args], timeout=10)
        if args[0] == "show":
            show_calls.append(args)
        return _FakeCompletedProcess()

    monkeypatch.setattr(repo_ops.git_ops, "_git", fake_git)

    assert runner._parse_base_queue() is None
    assert show_calls == [], (
        "must not silently fall back to origin or working-tree on "
        "indeterminate probe result"
    )


def test_parse_base_queue_honors_explicit_queue_from_origin_without_reprobing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """When the caller threads in an already-resolved probe result via
    ``queue_from_origin``, the helper must trust it instead of running a
    second ``_origin_queue_md_tracked`` probe. Otherwise a transient
    probe failure could let the parse source disagree with the caller's
    downstream ghost-filter decision (recovery's invariant)."""
    runner = _Runner(tmp_path)
    repo = Path(runner.repo_path)
    repo.mkdir(parents=True, exist_ok=True)
    # Working-tree snapshot exists and would be returned if the helper
    # silently re-probed and got ``False`` due to a flaky cat-file.
    (repo / "tasks").mkdir(parents=True)
    (repo / "tasks" / "QUEUE.md").write_text(
        "## PR-WT: working-tree snapshot\n- Status: TODO\n- Branch: pr-wt\n",
        encoding="utf-8",
    )

    origin_text = (
        "## PR-ORIGIN: from origin\n- Status: TODO\n- Branch: pr-origin\n"
    )
    git_calls: list[tuple[Any, ...]] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        del kwargs
        git_calls.append(args)
        if args[:2] == ("cat-file", "-e"):  # pragma: no cover - must not run
            return _FakeCompletedProcess(returncode=0)
        if args[0] == "show":
            return _FakeCompletedProcess(stdout=origin_text)
        return _FakeCompletedProcess()

    monkeypatch.setattr(repo_ops.git_ops, "_git", fake_git)

    parsed = runner._parse_base_queue(queue_from_origin=True)

    assert parsed is not None
    assert parsed[0].pr_id == "PR-ORIGIN"
    assert all(
        call[:2] != ("cat-file", "-e") for call in git_calls
    ), "must not re-probe when caller supplied queue_from_origin"


def test_parse_base_queue_with_explicit_false_skips_origin_show(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """When the caller declares the queue is *not* on origin, the helper
    reads the working tree even if a re-probe would have reported
    ``True``. Confirms the explicit ``queue_from_origin=False`` case
    short-circuits the probe entirely."""
    runner = _Runner(tmp_path)
    repo = Path(runner.repo_path)
    (repo / "tasks").mkdir(parents=True)
    (repo / "tasks" / "QUEUE.md").write_text(
        "## PR-WT: working-tree snapshot\n- Status: TODO\n- Branch: pr-wt\n",
        encoding="utf-8",
    )

    git_calls: list[tuple[Any, ...]] = []

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        del kwargs
        git_calls.append(args)
        return _FakeCompletedProcess(returncode=0)

    monkeypatch.setattr(repo_ops.git_ops, "_git", fake_git)

    parsed = runner._parse_base_queue(queue_from_origin=False)

    assert parsed is not None
    assert parsed[0].pr_id == "PR-WT"
    assert git_calls == [], "must skip both probe and origin show"


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
        lambda upload_key, expected: asyncio.sleep(0, result=False),
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


def test_process_pending_uploads_clears_crashed_pr_ids_on_reupload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """PR-186: Re-uploading a previously-crashed task file is the user's
    signal to retry. The crashed-pr-ids set on the runner must be
    cleared for any uploaded PR-id so the next IDLE cycle picks the
    task again instead of treating it as still CANCELED. Other
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


def test_process_pending_uploads_clears_recovered_pr_ids_on_reupload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """PR-247 follow-up: Re-uploading a task file the operator canceled
    via the HUNG recover button is the user's signal to retry. The
    recovered-pr-ids set must be cleared for any uploaded PR-id so the
    next IDLE cycle no longer forces it CANCELED, mirroring the PR-186
    contract for the crashed-pr-ids set. Other recovered entries that
    were not re-uploaded must remain intact."""
    runner = _Runner(tmp_path)
    runner._recovered_task_pr_ids.update({"PR-001", "PR-999"})
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
    assert runner._recovered_task_pr_ids == {"PR-999"}
    # PR-247 follow-up: the trimmed recovered-task set must be persisted
    # so a daemon restart immediately after upload does not rehydrate
    # the stale set from Redis and re-cancel the freshly retried task.
    assert runner._recovered_persist_calls == [frozenset({"PR-999"})]


def test_process_pending_uploads_clears_canceled_queue_rows_on_reupload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """PR-186 P2: Re-uploading a crashed task must also flip its
    ``Status: CANCELED`` row in the working-tree ``tasks/QUEUE.md`` to
    ``Status: TODO``. Otherwise a daemon restart between this upload and
    the next IDLE regeneration of QUEUE.md would re-read the stale
    CANCELED row, rehydrate ``_crashed_task_pr_ids`` from it, and
    re-cancel the just-retried task. Rows for other PR-ids must not be
    touched.
    """
    runner = _Runner(tmp_path)
    repo_dir = Path(runner.repo_path)
    tasks_dir = repo_dir / "tasks"
    tasks_dir.mkdir(parents=True)
    queue_md = tasks_dir / "QUEUE.md"
    queue_md.write_text(
        "# Task Queue\n"
        "\n"
        "## PR-001: Retried task\n"
        "- Status: CANCELED\n"
        "- Tasks file: tasks/PR-001.md\n"
        "- Branch: pr-001-retried\n"
        "\n"
        "## PR-999: Untouched crash\n"
        "- Status: CANCELED\n"
        "- Tasks file: tasks/PR-999.md\n"
        "- Branch: pr-999-untouched\n"
        "\n",
        encoding="utf-8",
    )
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

    rewritten = queue_md.read_text(encoding="utf-8")
    assert "## PR-001: Retried task\n- Status: TODO\n" in rewritten
    assert "## PR-999: Untouched crash\n- Status: CANCELED\n" in rewritten


def test_process_pending_uploads_canceled_clear_is_safe_without_queue_md(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The CANCELED-row clear must be a no-op when ``tasks/QUEUE.md``
    does not exist yet (the next IDLE cycle regenerates it from
    headers, so there is no stale row to clear)."""
    runner = _Runner(tmp_path)
    repo_dir = Path(runner.repo_path)
    repo_dir.mkdir(parents=True)
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
    assert not (repo_dir / "tasks" / "QUEUE.md").exists()


def test_process_pending_uploads_canceled_clear_logs_on_write_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A failure to rewrite ``tasks/QUEUE.md`` after a successful upload
    must be logged but must not propagate, so the upload itself stays
    successful (the next IDLE regeneration will overwrite the queue
    anyway)."""
    runner = _Runner(tmp_path)
    repo_dir = Path(runner.repo_path)
    tasks_dir = repo_dir / "tasks"
    tasks_dir.mkdir(parents=True)
    queue_md = tasks_dir / "QUEUE.md"
    queue_md.write_text(
        "# Task Queue\n"
        "\n"
        "## PR-001: Retried task\n"
        "- Status: CANCELED\n"
        "- Tasks file: tasks/PR-001.md\n"
        "- Branch: pr-001-retried\n",
        encoding="utf-8",
    )
    staging = tmp_path / "uploads" / "demo"
    staging.mkdir(parents=True)
    (staging / "PR-001.md").write_text("# PR-001\n", encoding="utf-8")
    key = f"upload:{runner.name}:pending"
    manifest = json.dumps({"files": ["PR-001.md"], "staging_dir": str(staging)})
    runner.redis.store[key] = manifest

    monkeypatch.setattr(repo_ops.git_ops, "_git", lambda *args, **kwargs: _FakeCompletedProcess())
    monkeypatch.setattr(repo_ops, "retry_transient", lambda func, operation_name=None: func())
    monkeypatch.setattr(repo_ops.shutil, "rmtree", lambda path, ignore_errors=True: None)

    original_write_text = Path.write_text

    def fake_write_text(self: Path, *args: Any, **kwargs: Any) -> int:
        if self == queue_md:
            raise OSError("disk full")
        return original_write_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", fake_write_text)

    assert _run(runner.process_pending_uploads()) is True
    assert any(
        "Failed to clear stale CANCELED rows in QUEUE.md" in event
        for event in runner.events
    )


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
