"""PR-257: repo_state helpers that fold cancellation roots into the
queue task graph and project blocked-set / dependents-count maps.

PR-265 migrated these helpers to read ``RepoState.current_queue`` from
Redis first, with the QUEUE.md disk parse retained as a fallback for
the daemon-warmup window. The tests here cover both source paths.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.models import PipelineState, QueueTask, RepoState, TaskStatus
from src.web import app as web_app
from src.web.routes.repo_control import _resolve_repo_task_path
from src.web.services.repo_state import (
    build_repo_task_nodes,
    compute_repo_blocked_set,
    compute_repo_dependents_count,
)

_QUEUE_BODY = """# Task Queue

## PR-100: Root canceled task
- Status: CANCELED
- Tasks file: tasks/PR-100.md
- Branch: pr-100-root

## PR-101: Direct dependent of PR-100
- Status: TODO
- Tasks file: tasks/PR-101.md
- Branch: pr-101-direct
- Depends on: PR-100

## PR-102: Transitive dependent via PR-101
- Status: TODO
- Tasks file: tasks/PR-102.md
- Branch: pr-102-transitive
- Depends on: PR-101

## PR-103: Independent task
- Status: TODO
- Tasks file: tasks/PR-103.md
- Branch: pr-103-independent
"""


def _seed_repo(tmp_path: Path, slug: str, body: str = _QUEUE_BODY) -> Path:
    repo_root = tmp_path / slug
    (repo_root / "tasks").mkdir(parents=True)
    (repo_root / "tasks" / "QUEUE.md").write_text(body, encoding="utf-8")
    return tmp_path


@pytest.mark.asyncio
async def test_repo_state_includes_dependents_count(tmp_path: Path) -> None:
    repos_dir = _seed_repo(tmp_path, "owner__repo")
    counts = await compute_repo_dependents_count(
        str(repos_dir), "owner__repo", canceled_task_ids=set()
    )
    # Two tasks (PR-101, PR-102) transitively block on PR-100; PR-103 does not.
    assert counts == {"PR-100": 2}


@pytest.mark.asyncio
async def test_repo_state_includes_blocked_by(tmp_path: Path) -> None:
    repos_dir = _seed_repo(tmp_path, "owner__repo")
    blocked = await compute_repo_blocked_set(
        str(repos_dir), "owner__repo", canceled_task_ids=set()
    )
    assert blocked == {"PR-101": "PR-100", "PR-102": "PR-100"}


@pytest.mark.asyncio
async def test_extra_canceled_ids_treated_as_canceled_root(
    tmp_path: Path,
) -> None:
    """A cause id present in Redis but missing from QUEUE.md still counts."""
    body = """# Task Queue

## PR-200: Lives only in QUEUE.md as TODO, but Redis recorded a cause
- Status: TODO
- Tasks file: tasks/PR-200.md
- Branch: pr-200-todo

## PR-201: Depends on PR-200
- Status: TODO
- Tasks file: tasks/PR-201.md
- Branch: pr-201-dep
- Depends on: PR-200
"""
    repos_dir = _seed_repo(tmp_path, "owner__r2", body=body)
    counts = await compute_repo_dependents_count(
        str(repos_dir), "owner__r2", canceled_task_ids={"PR-200"}
    )
    assert counts == {"PR-200": 1}


@pytest.mark.asyncio
async def test_canceled_ids_unknown_to_queue_appended_as_roots(
    tmp_path: Path,
) -> None:
    """Cause ids absent from QUEUE.md are still folded in as canceled roots."""
    body = """# Task Queue

## PR-300: Lonely task with no dependents
- Status: TODO
- Tasks file: tasks/PR-300.md
- Branch: pr-300-only
"""
    repos_dir = _seed_repo(tmp_path, "owner__r3", body=body)
    nodes = await build_repo_task_nodes(
        str(repos_dir),
        "owner__r3",
        extra_canceled_ids={"PR-PHANTOM"},
    )
    ids = {n.task_id: n.is_canceled for n in nodes}
    assert ids == {"PR-300": False, "PR-PHANTOM": True}


@pytest.mark.asyncio
async def test_missing_queue_file_returns_empty(tmp_path: Path) -> None:
    """No queue source ⇒ empty task graph ⇒ empty maps, no exception."""
    nodes = await build_repo_task_nodes(str(tmp_path), "ghost__repo")
    assert nodes == []
    assert (
        await compute_repo_dependents_count(
            str(tmp_path), "ghost__repo", set()
        )
        == {}
    )
    assert (
        await compute_repo_blocked_set(str(tmp_path), "ghost__repo", set())
        == {}
    )


@pytest.mark.asyncio
async def test_empty_queue_returns_empty_maps(tmp_path: Path) -> None:
    """A QUEUE.md that contains zero task headers also yields empty maps."""
    repo_root = tmp_path / "empty__repo"
    (repo_root / "tasks").mkdir(parents=True)
    (repo_root / "tasks" / "QUEUE.md").write_text(
        "# Task Queue\n\nNothing here yet.\n", encoding="utf-8"
    )
    assert (
        await compute_repo_dependents_count(
            str(tmp_path), "empty__repo", set()
        )
        == {}
    )
    assert (
        await compute_repo_blocked_set(str(tmp_path), "empty__repo", set())
        == {}
    )


@pytest.mark.asyncio
async def test_unreadable_queue_returns_empty(
    tmp_path: Path, monkeypatch
) -> None:
    """An OSError while reading QUEUE.md degrades to an empty graph."""
    from src.web.services import repo_state as rs

    def boom(_path):
        raise OSError("permission denied")

    monkeypatch.setattr(rs, "parse_queue", boom)
    nodes = await build_repo_task_nodes(str(tmp_path), "broken__repo")
    assert nodes == []


class _SnapshotRedis:
    def __init__(self, state: RepoState) -> None:
        self._payload = state.model_dump_json()

    async def get(self, key: str) -> str | None:
        if key == "pipeline:owner__repo":
            return self._payload
        return None


def _snapshot_redis_for(name: str, state: RepoState) -> _SnapshotRedis:
    """Return a stub Redis client serving ``state`` under ``pipeline:{name}``."""
    expected_key = f"pipeline:{name}"

    class _Stub:
        def __init__(self) -> None:
            self._payload = state.model_dump_json()

        async def get(self, key: str) -> str | None:
            return self._payload if key == expected_key else None

    return _Stub()


@pytest.mark.asyncio
async def test_build_repo_task_nodes_from_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Snapshot path: TaskNodes mirror current_queue with CANCELED preserved."""
    state = RepoState(
        url="https://github.com/owner/repo.git",
        name="owner__repo",
        state=PipelineState.IDLE,
        current_queue=[
            QueueTask(
                pr_id="PR-001",
                title="Canceled root",
                status=TaskStatus.CANCELED,
            ),
            QueueTask(
                pr_id="PR-002",
                title="Direct dep of PR-001",
                status=TaskStatus.TODO,
                depends_on=["PR-001"],
            ),
            QueueTask(
                pr_id="PR-003",
                title="Transitive dep via PR-002",
                status=TaskStatus.TODO,
                depends_on=["PR-002"],
            ),
            QueueTask(
                pr_id="PR-004",
                title="Independent",
                status=TaskStatus.TODO,
            ),
        ],
    )
    monkeypatch.setattr(
        web_app.app.state, "redis", _snapshot_redis_for("owner__repo", state)
    )
    nodes = await build_repo_task_nodes(str(tmp_path), "owner__repo")
    by_id = {n.task_id: n for n in nodes}
    assert set(by_id) == {"PR-001", "PR-002", "PR-003", "PR-004"}
    assert by_id["PR-001"].is_canceled is True
    assert by_id["PR-002"].is_canceled is False
    assert by_id["PR-003"].is_canceled is False
    assert by_id["PR-004"].is_canceled is False
    assert by_id["PR-002"].depends_on == ["PR-001"]
    assert by_id["PR-003"].depends_on == ["PR-002"]


@pytest.mark.asyncio
async def test_build_repo_task_nodes_falls_back_to_disk_when_snapshot_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Disk fallback engaged when ``current_queue`` is None on the snapshot."""
    state = RepoState(
        url="https://github.com/owner/repo.git",
        name="owner__repo",
        state=PipelineState.IDLE,
        current_queue=None,
    )
    monkeypatch.setattr(
        web_app.app.state, "redis", _snapshot_redis_for("owner__repo", state)
    )
    repos_dir = _seed_repo(tmp_path, "owner__repo")
    nodes = await build_repo_task_nodes(str(repos_dir), "owner__repo")
    by_id = {n.task_id: n.is_canceled for n in nodes}
    assert by_id == {
        "PR-100": True,
        "PR-101": False,
        "PR-102": False,
        "PR-103": False,
    }


@pytest.mark.asyncio
async def test_build_repo_task_nodes_extra_canceled_overlay_works_in_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``extra_canceled_ids`` overlay applies even on the snapshot path."""
    state = RepoState(
        url="https://github.com/owner/repo.git",
        name="owner__repo",
        state=PipelineState.IDLE,
        current_queue=[
            QueueTask(
                pr_id="PR-099",
                title="TODO in snapshot, CANCELED in Redis cause record",
                status=TaskStatus.TODO,
            ),
            QueueTask(
                pr_id="PR-100",
                title="Depends on PR-099",
                status=TaskStatus.TODO,
                depends_on=["PR-099"],
            ),
        ],
    )
    monkeypatch.setattr(
        web_app.app.state, "redis", _snapshot_redis_for("owner__repo", state)
    )
    nodes = await build_repo_task_nodes(
        str(tmp_path),
        "owner__repo",
        extra_canceled_ids={"PR-099"},
    )
    by_id = {n.task_id: n.is_canceled for n in nodes}
    assert by_id == {"PR-099": True, "PR-100": False}


@pytest.mark.asyncio
async def test_compute_repo_dependents_count_uses_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """dependents_count derived from ``current_queue`` snapshot."""
    state = RepoState(
        url="https://github.com/owner/repo.git",
        name="owner__repo",
        state=PipelineState.IDLE,
        current_queue=[
            QueueTask(
                pr_id="PR-005",
                title="Canceled root",
                status=TaskStatus.CANCELED,
            ),
            QueueTask(
                pr_id="PR-006",
                title="Direct dep",
                status=TaskStatus.TODO,
                depends_on=["PR-005"],
            ),
            QueueTask(
                pr_id="PR-007",
                title="Transitive dep",
                status=TaskStatus.TODO,
                depends_on=["PR-006"],
            ),
        ],
    )
    monkeypatch.setattr(
        web_app.app.state, "redis", _snapshot_redis_for("owner__repo", state)
    )
    counts = await compute_repo_dependents_count(
        str(tmp_path), "owner__repo", canceled_task_ids=set()
    )
    assert counts == {"PR-005": 2}


@pytest.mark.asyncio
async def test_compute_repo_blocked_set_uses_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """blocked_set derived from ``current_queue`` snapshot."""
    state = RepoState(
        url="https://github.com/owner/repo.git",
        name="owner__repo",
        state=PipelineState.IDLE,
        current_queue=[
            QueueTask(
                pr_id="PR-005",
                title="Canceled root",
                status=TaskStatus.CANCELED,
            ),
            QueueTask(
                pr_id="PR-006",
                title="Direct dep",
                status=TaskStatus.TODO,
                depends_on=["PR-005"],
            ),
            QueueTask(
                pr_id="PR-007",
                title="Transitive dep",
                status=TaskStatus.TODO,
                depends_on=["PR-006"],
            ),
        ],
    )
    monkeypatch.setattr(
        web_app.app.state, "redis", _snapshot_redis_for("owner__repo", state)
    )
    blocked = await compute_repo_blocked_set(
        str(tmp_path), "owner__repo", canceled_task_ids=set()
    )
    assert blocked == {"PR-006": "PR-005", "PR-007": "PR-005"}


@pytest.mark.asyncio
async def test_snapshot_and_disk_yield_identical_dependents_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Snapshot and disk paths produce identical dependents_count for same data.

    Acts as a regression guard for the cancellation surface: PR-265's
    migration is purely substitutional, so output must not drift between
    the two source paths on the same underlying queue.
    """
    repos_dir = _seed_repo(tmp_path, "owner__repo")
    disk_counts = await compute_repo_dependents_count(
        str(repos_dir), "owner__repo", canceled_task_ids=set()
    )
    state = RepoState(
        url="https://github.com/owner/repo.git",
        name="owner__repo",
        state=PipelineState.IDLE,
        current_queue=[
            QueueTask(
                pr_id="PR-100",
                title="Root canceled task",
                status=TaskStatus.CANCELED,
            ),
            QueueTask(
                pr_id="PR-101",
                title="Direct dependent of PR-100",
                status=TaskStatus.TODO,
                depends_on=["PR-100"],
            ),
            QueueTask(
                pr_id="PR-102",
                title="Transitive dependent via PR-101",
                status=TaskStatus.TODO,
                depends_on=["PR-101"],
            ),
            QueueTask(
                pr_id="PR-103",
                title="Independent task",
                status=TaskStatus.TODO,
            ),
        ],
    )
    monkeypatch.setattr(
        web_app.app.state, "redis", _snapshot_redis_for("owner__repo", state)
    )
    snapshot_counts = await compute_repo_dependents_count(
        str(repos_dir), "owner__repo", canceled_task_ids=set()
    )
    assert snapshot_counts == disk_counts


@pytest.mark.asyncio
async def test_resolve_repo_task_path_uses_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "owner__repo"
    tasks_dir = repo_root / "tasks"
    tasks_dir.mkdir(parents=True)
    default_path = tasks_dir / "PR-099.md"
    custom_path = tasks_dir / "custom-name.md"
    default_path.write_text("# default\n", encoding="utf-8")
    custom_path.write_text("# custom\n", encoding="utf-8")
    (tasks_dir / "QUEUE.md").write_text(
        "# Task Queue\n\n"
        "## PR-099: Disk mapping\n"
        "- Status: TODO\n"
        "- Tasks file: tasks/PR-099.md\n",
        encoding="utf-8",
    )
    state = RepoState(
        url="https://github.com/owner/repo.git",
        name="owner__repo",
        state=PipelineState.IDLE,
        current_queue=[
            QueueTask(
                pr_id="PR-099",
                title="Snapshot mapping",
                status=TaskStatus.TODO,
                task_file="tasks/custom-name.md",
            )
        ],
    )
    monkeypatch.setattr(web_app, "REPOS_DIR", str(tmp_path))
    monkeypatch.setattr(web_app.app.state, "redis", _SnapshotRedis(state))

    resolved = await _resolve_repo_task_path("owner__repo", "PR-099")

    assert resolved == (custom_path.resolve(), "tasks/custom-name.md")
