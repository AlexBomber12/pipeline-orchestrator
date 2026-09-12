"""Coverage for Retry-adjacent helpers shared with repository Reset."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import pytest
from src.models import PipelineState, PRInfo, QueueTask, RepoState, TaskStatus
from src.web.routes import repo_control


class _Redis:
    def __init__(self, store: dict[str, Any] | None = None) -> None:
        self.store = store or {}
        self.deleted: list[str] = []
        self.expiries: dict[str, int] = {}

    async def get(self, key: str) -> Any:
        return self.store.get(key)

    def set(
        self,
        key: str,
        value: str,
        ex: int | None = None,
        nx: bool = False,
    ) -> bool:
        if nx and key in self.store:
            return False
        self.store[key] = value
        if ex is not None:
            self.expiries[key] = ex
        return True

    async def delete(self, key: str) -> int:
        self.deleted.append(key)
        return int(self.store.pop(key, None) is not None)

    def multi(self) -> None:
        return None

    async def transaction(
        self,
        callback: Any,
        *keys: str,
        value_from_callable: bool = False,
    ) -> Any:
        result = await callback(self)
        return result if value_from_callable else None


def _state(
    state: PipelineState = PipelineState.IDLE,
    *,
    paused: bool = False,
    history: list[dict[str, Any]] | None = None,
) -> str:
    return RepoState(
        url="https://github.com/example/repo.git",
        name="repo",
        state=state,
        user_paused=paused,
        history=history or [],
    ).model_dump_json()


def _structured_task(task_id: str = "PR-1", branch: str = "fix/pr-1") -> str:
    return (
        "---\nstatus: ERROR\nblocked_reason: daemon\n---\n\n"
        f"# {task_id}: Retry helper\n\n"
        f"Branch: {branch}\n"
        "- Type: bugfix\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 3\n"
        "- Coder: codex\n\n"
        "Body\n"
    )


@pytest.mark.asyncio
async def test_retry_reservation_acquire_release_and_async_set() -> None:
    class _AsyncSet(_Redis):
        async def set(self, *args: Any, **kwargs: Any) -> bool:  # type: ignore[override]
            return super().set(*args, **kwargs)

    redis = _AsyncSet({"pipeline:repo": _state()})
    previous = await repo_control._reserve_repo_for_retry(
        redis, "repo", "https://github.com/example/repo.git"
    )
    assert previous is False
    assert RepoState.model_validate_json(redis.store["pipeline:repo"]).user_paused
    await repo_control._release_repo_retry_reservation(redis, "repo", previous)
    assert not RepoState.model_validate_json(redis.store["pipeline:repo"]).user_paused
    assert "control:retry_reservation:repo" not in redis.store

    missing = _Redis()
    assert (
        await repo_control._reserve_repo_for_retry(
            missing, "repo", "https://github.com/example/repo.git"
        )
        is False
    )


@pytest.mark.asyncio
async def test_retry_reservation_rejects_busy_invalid_and_concurrent_state() -> None:
    for state in (
        RepoState(
            url="https://github.com/example/repo.git",
            name="repo",
            state=PipelineState.IDLE,
            user_paused=True,
        ),
        RepoState(
            url="https://github.com/example/repo.git",
            name="repo",
            state=PipelineState.CODING,
        ),
        RepoState(
            url="https://github.com/example/repo.git",
            name="repo",
            state=PipelineState.PAUSED,
        ),
    ):
        redis = _Redis({"pipeline:repo": state.model_dump_json()})
        with pytest.raises(repo_control._RepoStateMutationError):
            await repo_control._reserve_repo_for_retry(
                redis, "repo", "https://github.com/example/repo.git"
            )
        assert "control:retry_reservation:repo" not in redis.store

    invalid = _Redis({"pipeline:repo": "bad"})
    with pytest.raises(repo_control._RepoStateMutationError):
        await repo_control._reserve_repo_for_retry(
            invalid, "repo", "https://github.com/example/repo.git"
        )

    concurrent = _Redis({"control:retry_reservation:repo": "already"})
    with pytest.raises(repo_control._RepoStateMutationError) as exc:
        await repo_control._reserve_repo_for_retry(
            concurrent, "repo", "https://github.com/example/repo.git"
        )
    assert exc.value.status_code == 409


@pytest.mark.asyncio
async def test_release_preserves_active_or_newer_operator_pause() -> None:
    active = _Redis({"pipeline:repo": _state(PipelineState.CODING, paused=True)})
    await repo_control._release_repo_retry_reservation(active, "repo", False)
    assert RepoState.model_validate_json(active.store["pipeline:repo"]).user_paused

    history = [
        {"event": "Other", "time": "2026-05-01T12:01:00+00:00"},
        {"event": "Pause requested.", "time": "2026-05-01T12:02:00+00:00"},
    ]
    paused = _Redis(
        {
            "pipeline:repo": _state(paused=True, history=history),
            "control:retry_reservation:repo": "2026-05-01T12:00:00+00:00",
        }
    )
    await repo_control._release_repo_retry_reservation(paused, "repo", False)
    assert RepoState.model_validate_json(paused.store["pipeline:repo"]).user_paused


def test_pause_control_detection_defensive_paths() -> None:
    state = RepoState(
        url="https://github.com/example/repo.git",
        name="repo",
        history=[
            {"event": "Resume requested.", "time": "bad"},
            {"event": "Pause requested."},
            {"event": "Other", "time": "2026-05-01T12:01:00+00:00"},
        ],
    )
    assert not repo_control._has_pause_control_after_reservation(state, None)
    assert not repo_control._has_pause_control_after_reservation(state, "bad")
    assert not repo_control._has_pause_control_after_reservation(
        state, "2026-05-01T12:00:00+00:00"
    )


@pytest.mark.asyncio
async def test_release_tolerates_read_transaction_and_delete_failures() -> None:
    class _ReadFails(_Redis):
        async def get(self, key: str) -> Any:
            if key == "control:retry_reservation:repo":
                raise RuntimeError("read")
            return await super().get(key)

    redis = _ReadFails({"pipeline:repo": _state(paused=True)})
    await repo_control._release_repo_retry_reservation(redis, "repo", False)
    assert not RepoState.model_validate_json(redis.store["pipeline:repo"]).user_paused

    class _TransactionFails(_Redis):
        async def transaction(self, *args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("transaction")

    await repo_control._release_repo_retry_reservation(
        _TransactionFails(), "repo", False
    )

    class _DeleteFails(_Redis):
        async def delete(self, key: str) -> int:
            raise RuntimeError("delete")

    await repo_control._release_repo_retry_reservation(_DeleteFails(), "repo", False)
    await repo_control._release_repo_retry_reservation(_Redis(), "repo", False)
    await repo_control._release_repo_retry_reservation(
        _Redis({"pipeline:repo": "invalid"}), "repo", False
    )


@pytest.mark.asyncio
async def test_decode_await_and_state_helpers() -> None:
    assert repo_control._retry_fingerprint_key("repo", "PR-1").endswith("repo:PR-1")
    assert repo_control._retry_reservation_key("repo") == "control:retry_reservation:repo"
    assert repo_control._decode_redis_text(None) is None
    assert repo_control._decode_redis_text(b"bytes") == "bytes"
    assert repo_control._decode_redis_text(3) == "3"
    assert await repo_control._await_if_needed("sync") == "sync"

    async def async_value() -> str:
        return "async"

    assert await repo_control._await_if_needed(async_value()) == "async"
    assert await repo_control._repo_state_for_retry(_Redis(), "repo") is None
    assert (
        await repo_control._repo_state_for_retry(
            _Redis({"pipeline:repo": "invalid"}), "repo"
        )
        is None
    )

    class _ReadFails(_Redis):
        async def get(self, key: str) -> Any:
            raise RuntimeError("down")

    assert await repo_control._repo_state_for_retry(_ReadFails(), "repo") is None


def test_git_output_and_retry_status_helpers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    error = subprocess.CalledProcessError(
        1, ["git"], output="nothing to commit", stderr="detail"
    )
    assert repo_control._is_nothing_to_commit(error)
    assert "detail" in repo_control._called_process_output(error)
    result = subprocess.CompletedProcess(["git"], 0, "out", "err")
    assert repo_control._git_output(result) == "out\nerr"
    monkeypatch.setattr(repo_control, "_run_retry_git", lambda *args: result)
    assert repo_control._head_commit_subject(tmp_path) == "out"

    task = QueueTask(pr_id="PR-1", title="one", status=TaskStatus.ERROR)
    assert repo_control._task_status_from_snapshot([task], "PR-1") == TaskStatus.ERROR
    assert repo_control._task_status_from_snapshot([], "PR-1") is None
    assert repo_control._task_status_from_snapshot(None, "PR-1") is None
    assert repo_control._is_retryable_task_status(TaskStatus.ERROR, None)
    assert repo_control._is_retryable_task_status(TaskStatus.TODO, None)
    assert repo_control._is_retryable_task_status(None, TaskStatus.ERROR)
    assert not repo_control._is_retryable_task_status(TaskStatus.DONE, TaskStatus.ERROR)


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        ("# no frontmatter\n", None),
        ("---\n---\nBody\n", None),
        ("---\ntitle: x\nstatus: ERROR # note\n---\n", TaskStatus.ERROR),
        ("---\ntitle: x\nstatus: unknown\n---\n", None),
        ("---\ntitle: x\n", None),
    ],
)
def test_read_task_status_variants(
    tmp_path: Path, content: str, expected: TaskStatus | None
) -> None:
    path = tmp_path / "task.md"
    path.write_text(content, encoding="utf-8")
    assert repo_control._read_task_frontmatter_status(path) == expected


def test_blocked_reason_restore_and_missing_pathspec(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = tmp_path / "PR-1.md"
    task.write_text(_structured_task(), encoding="utf-8")
    assert repo_control._read_task_blocked_reason(task) == "daemon"
    task.write_text("broken", encoding="utf-8")
    assert repo_control._read_task_blocked_reason(task) is None
    repo_control._restore_retry_error_status(task, None)
    assert "status: ERROR" in task.read_text(encoding="utf-8")
    monkeypatch.setattr(
        repo_control,
        "write_frontmatter_status",
        lambda *args: (_ for _ in ()).throw(OSError("write")),
    )
    repo_control._restore_retry_error_status(task, "daemon")
    missing = subprocess.CalledProcessError(
        1, ["git"], stderr="pathspec did not match any file"
    )
    assert repo_control._is_missing_task_pathspec(missing)


def test_checkout_commit_push_and_reset_helpers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []

    def git(repo: Path, *args: str):
        calls.append(args)
        return subprocess.CompletedProcess(["git"], 0, "", "")

    monkeypatch.setattr(repo_control, "_run_retry_git", git)
    relative = Path("tasks/PR-1.md")
    repo_control._checkout_retry_base_task(tmp_path, "main", relative)
    repo_control._commit_and_push_retry_reset(
        tmp_path, relative, "retry subject", "main"
    )
    repo_control._reset_retry_worktree(tmp_path, "main")
    assert ("checkout", "-f", "main") in calls
    assert ("push", "origin", "HEAD:main") in calls
    assert calls[-1] == ("reset", "--hard", "origin/main")


def test_commit_push_retry_replay_and_failure_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    relative = Path("tasks/PR-1.md")

    def failure(repo: Path, *args: str):
        if args[0] == "commit":
            raise subprocess.CalledProcessError(1, ["git"], stderr="real error")
        return subprocess.CompletedProcess(["git"], 0, "", "")

    monkeypatch.setattr(repo_control, "_run_retry_git", failure)
    with pytest.raises(subprocess.CalledProcessError):
        repo_control._commit_and_push_retry_reset(tmp_path, relative, "subject", "main")

    def nothing(repo: Path, *args: str):
        if args[0] == "commit":
            raise subprocess.CalledProcessError(1, ["git"], stderr="nothing to commit")
        if args[0] == "log":
            return subprocess.CompletedProcess(["git"], 0, "other", "")
        return subprocess.CompletedProcess(["git"], 0, "", "")

    monkeypatch.setattr(repo_control, "_run_retry_git", nothing)
    with pytest.raises(repo_control._TaskNotRetryable):
        repo_control._commit_and_push_retry_reset(tmp_path, relative, "subject", "main")

    def replayed(repo: Path, *args: str):
        if args[0] == "commit":
            raise subprocess.CalledProcessError(1, ["git"], stderr="nothing to commit")
        if args[0] == "log":
            return subprocess.CompletedProcess(["git"], 0, "subject", "")
        if args[0] == "push":
            return subprocess.CompletedProcess(["git"], 0, "Everything up-to-date", "")
        return subprocess.CompletedProcess(["git"], 0, "", "")

    monkeypatch.setattr(repo_control, "_run_retry_git", replayed)
    with pytest.raises(repo_control._TaskNotRetryable):
        repo_control._commit_and_push_retry_reset(tmp_path, relative, "subject", "main")


def test_relevant_pr_and_failure_identity_paths() -> None:
    task = QueueTask(
        pr_id="PR-1",
        title="one",
        status=TaskStatus.ERROR,
        branch="fix/pr-1",
    )
    pr = PRInfo(number=1, branch="fix/pr-1", pr_id="PR-1", head_sha="abc")
    state = RepoState(
        url="https://github.com/example/repo.git",
        name="repo",
        state=PipelineState.ERROR,
        current_task=task,
        current_pr=pr,
        error_message="failed",
    )
    assert repo_control._relevant_retry_pr(None, task) is None
    assert repo_control._relevant_retry_pr(state, task) == pr
    assert repo_control._retry_failure_identity(task, state, None)
    different_task = task.model_copy(update={"pr_id": "PR-2"})
    state.current_task = different_task
    assert repo_control._relevant_retry_pr(state, task) == pr
    state.current_pr = pr.model_copy(update={"pr_id": None})
    assert repo_control._relevant_retry_pr(state, task) == state.current_pr
    task.branch = "other"
    assert repo_control._relevant_retry_pr(state, task) is None


@pytest.mark.asyncio
async def test_binding_context_rejects_parse_identity_and_reads_state(
    tmp_path: Path,
) -> None:
    redis = _Redis({"pipeline:repo": _state(PipelineState.ERROR)})
    task = QueueTask(pr_id="PR-1", title="one", status=TaskStatus.ERROR)
    path = tmp_path / "task.md"
    path.write_text("broken", encoding="utf-8")
    assert (
        await repo_control._retry_binding_context(
            redis, "repo", task, path, "tasks/PR-1.md", retry_count=0
        )
        is None
    )
    path.write_text(_structured_task(task_id="PR-2"), encoding="utf-8")
    assert (
        await repo_control._retry_binding_context(
            redis, "repo", task, path, "tasks/PR-1.md", retry_count=0
        )
        is None
    )
    path.write_text(_structured_task(), encoding="utf-8")
    context = await repo_control._retry_binding_context(
        redis, "repo", task, path, "tasks/PR-1.md", retry_count=0
    )
    assert context is not None
    assert context["pipeline_state"] == "ERROR"
