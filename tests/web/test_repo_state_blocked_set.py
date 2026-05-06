"""PR-257 helpers: cancellation roots and dependents/blocked maps.

PR-267 dropped the on-disk ``QUEUE.md`` fallback; ``RepoState.current_queue``
is now the only source.
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


class _SnapshotRedis:
    def __init__(self, state: RepoState, *, name: str = "owner__repo") -> None:
        self._payload = state.model_dump_json()
        self._key = f"pipeline:{name}"

    async def get(self, key: str) -> str | None:
        return self._payload if key == self._key else None


def _set_redis(monkeypatch: pytest.MonkeyPatch, redis_client: object) -> None:
    monkeypatch.setattr(
        web_app.app.state, "redis", redis_client, raising=False
    )


def _state_with_queue(
    queue: list[QueueTask], *, name: str = "owner__repo"
) -> RepoState:
    return RepoState(
        url="https://github.com/owner/repo.git",
        name=name,
        state=PipelineState.IDLE,
        current_queue=queue,
    )


@pytest.mark.asyncio
async def test_repo_state_includes_dependents_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = _state_with_queue(
        [
            QueueTask(pr_id="PR-100", title="Root canceled", status=TaskStatus.CANCELED),
            QueueTask(
                pr_id="PR-101",
                title="Direct dep",
                status=TaskStatus.TODO,
                depends_on=["PR-100"],
            ),
            QueueTask(
                pr_id="PR-102",
                title="Transitive dep",
                status=TaskStatus.TODO,
                depends_on=["PR-101"],
            ),
            QueueTask(pr_id="PR-103", title="Independent", status=TaskStatus.TODO),
        ]
    )
    _set_redis(monkeypatch, _SnapshotRedis(state))
    counts = await compute_repo_dependents_count(
        str(tmp_path), "owner__repo", canceled_task_ids=set()
    )
    assert counts == {"PR-100": 2}


@pytest.mark.asyncio
async def test_repo_state_includes_blocked_by(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = _state_with_queue(
        [
            QueueTask(pr_id="PR-100", title="Root canceled", status=TaskStatus.CANCELED),
            QueueTask(
                pr_id="PR-101",
                title="Direct dep",
                status=TaskStatus.TODO,
                depends_on=["PR-100"],
            ),
            QueueTask(
                pr_id="PR-102",
                title="Transitive dep",
                status=TaskStatus.TODO,
                depends_on=["PR-101"],
            ),
        ]
    )
    _set_redis(monkeypatch, _SnapshotRedis(state))
    blocked = await compute_repo_blocked_set(
        str(tmp_path), "owner__repo", canceled_task_ids=set()
    )
    assert blocked == {"PR-101": "PR-100", "PR-102": "PR-100"}


@pytest.mark.asyncio
async def test_extra_canceled_ids_treated_as_canceled_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A cause id present in Redis but TODO in the snapshot still counts."""
    state = _state_with_queue(
        [
            QueueTask(pr_id="PR-200", title="TODO in snapshot", status=TaskStatus.TODO),
            QueueTask(
                pr_id="PR-201",
                title="Depends on PR-200",
                status=TaskStatus.TODO,
                depends_on=["PR-200"],
            ),
        ],
        name="owner__r2",
    )
    _set_redis(monkeypatch, _SnapshotRedis(state, name="owner__r2"))
    counts = await compute_repo_dependents_count(
        str(tmp_path), "owner__r2", canceled_task_ids={"PR-200"}
    )
    assert counts == {"PR-200": 1}


@pytest.mark.asyncio
async def test_canceled_ids_unknown_to_queue_appended_as_roots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cause ids absent from the snapshot are still folded in as canceled roots."""
    state = _state_with_queue(
        [
            QueueTask(pr_id="PR-300", title="Lonely task", status=TaskStatus.TODO),
        ],
        name="owner__r3",
    )
    _set_redis(monkeypatch, _SnapshotRedis(state, name="owner__r3"))
    nodes = await build_repo_task_nodes(
        str(tmp_path),
        "owner__r3",
        extra_canceled_ids={"PR-PHANTOM"},
    )
    ids = {n.task_id: n.is_canceled for n in nodes}
    assert ids == {"PR-300": False, "PR-PHANTOM": True}


@pytest.mark.asyncio
async def test_missing_snapshot_returns_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No snapshot ⇒ empty task graph ⇒ empty maps, no exception."""
    _set_redis(monkeypatch, None)
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
async def test_build_repo_task_nodes_from_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = _state_with_queue(
        [
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
        ]
    )
    _set_redis(monkeypatch, _SnapshotRedis(state))
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
async def test_build_repo_task_nodes_returns_empty_when_snapshot_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When ``current_queue`` is None on the snapshot, return [] (no disk fallback)."""
    state = RepoState(
        url="https://github.com/owner/repo.git",
        name="owner__repo",
        state=PipelineState.IDLE,
        current_queue=None,
    )
    _set_redis(monkeypatch, _SnapshotRedis(state))
    nodes = await build_repo_task_nodes(str(tmp_path), "owner__repo")
    assert nodes == []


@pytest.mark.asyncio
async def test_build_repo_task_nodes_extra_canceled_overlay_works_in_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``extra_canceled_ids`` overlay applies to the snapshot path."""
    state = _state_with_queue(
        [
            QueueTask(
                pr_id="PR-099",
                title="TODO in snapshot, CANCELED in cause record",
                status=TaskStatus.TODO,
            ),
            QueueTask(
                pr_id="PR-100",
                title="Depends on PR-099",
                status=TaskStatus.TODO,
                depends_on=["PR-099"],
            ),
        ]
    )
    _set_redis(monkeypatch, _SnapshotRedis(state))
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
    state = _state_with_queue(
        [
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
        ]
    )
    _set_redis(monkeypatch, _SnapshotRedis(state))
    counts = await compute_repo_dependents_count(
        str(tmp_path), "owner__repo", canceled_task_ids=set()
    )
    assert counts == {"PR-005": 2}


@pytest.mark.asyncio
async def test_compute_repo_blocked_set_uses_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = _state_with_queue(
        [
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
        ]
    )
    _set_redis(monkeypatch, _SnapshotRedis(state))
    blocked = await compute_repo_blocked_set(
        str(tmp_path), "owner__repo", canceled_task_ids=set()
    )
    assert blocked == {"PR-006": "PR-005", "PR-007": "PR-005"}


@pytest.mark.asyncio
async def test_resolve_repo_task_path_uses_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "owner__repo"
    tasks_dir = repo_root / "tasks"
    tasks_dir.mkdir(parents=True)
    custom_path = tasks_dir / "custom-name.md"
    custom_path.write_text("# custom\n", encoding="utf-8")
    state = _state_with_queue(
        [
            QueueTask(
                pr_id="PR-099",
                title="Snapshot mapping",
                status=TaskStatus.TODO,
                task_file="tasks/custom-name.md",
            )
        ]
    )
    monkeypatch.setattr(web_app, "REPOS_DIR", str(tmp_path))
    _set_redis(monkeypatch, _SnapshotRedis(state))

    resolved = await _resolve_repo_task_path("owner__repo", "PR-099")

    assert resolved == (custom_path.resolve(), "tasks/custom-name.md")
