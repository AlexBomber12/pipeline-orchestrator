"""PR-257: repo_state helpers that fold cancellation roots into the
QUEUE.md task graph and project blocked-set / dependents-count maps."""

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


def test_repo_state_includes_dependents_count(tmp_path: Path) -> None:
    repos_dir = _seed_repo(tmp_path, "owner__repo")
    counts = compute_repo_dependents_count(
        str(repos_dir), "owner__repo", canceled_task_ids=set()
    )
    # Two tasks (PR-101, PR-102) transitively block on PR-100; PR-103 does not.
    assert counts == {"PR-100": 2}


def test_repo_state_includes_blocked_by(tmp_path: Path) -> None:
    repos_dir = _seed_repo(tmp_path, "owner__repo")
    blocked = compute_repo_blocked_set(
        str(repos_dir), "owner__repo", canceled_task_ids=set()
    )
    assert blocked == {"PR-101": "PR-100", "PR-102": "PR-100"}


def test_extra_canceled_ids_treated_as_canceled_root(tmp_path: Path) -> None:
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
    counts = compute_repo_dependents_count(
        str(repos_dir), "owner__r2", canceled_task_ids={"PR-200"}
    )
    assert counts == {"PR-200": 1}


def test_canceled_ids_unknown_to_queue_appended_as_roots(tmp_path: Path) -> None:
    """Cause ids absent from QUEUE.md are still folded in as canceled roots."""
    body = """# Task Queue

## PR-300: Lonely task with no dependents
- Status: TODO
- Tasks file: tasks/PR-300.md
- Branch: pr-300-only
"""
    repos_dir = _seed_repo(tmp_path, "owner__r3", body=body)
    nodes = build_repo_task_nodes(
        str(repos_dir),
        "owner__r3",
        extra_canceled_ids={"PR-PHANTOM"},
    )
    ids = {n.task_id: n.is_canceled for n in nodes}
    assert ids == {"PR-300": False, "PR-PHANTOM": True}


def test_missing_queue_file_returns_empty(tmp_path: Path) -> None:
    """No QUEUE.md ⇒ empty task graph ⇒ empty maps, no exception."""
    nodes = build_repo_task_nodes(str(tmp_path), "ghost__repo")
    assert nodes == []
    assert (
        compute_repo_dependents_count(str(tmp_path), "ghost__repo", set())
        == {}
    )
    assert (
        compute_repo_blocked_set(str(tmp_path), "ghost__repo", set())
        == {}
    )


def test_empty_queue_returns_empty_maps(tmp_path: Path) -> None:
    """A QUEUE.md that contains zero task headers also yields empty maps."""
    repo_root = tmp_path / "empty__repo"
    (repo_root / "tasks").mkdir(parents=True)
    (repo_root / "tasks" / "QUEUE.md").write_text(
        "# Task Queue\n\nNothing here yet.\n", encoding="utf-8"
    )
    assert (
        compute_repo_dependents_count(str(tmp_path), "empty__repo", set())
        == {}
    )
    assert (
        compute_repo_blocked_set(str(tmp_path), "empty__repo", set())
        == {}
    )


def test_unreadable_queue_returns_empty(tmp_path: Path, monkeypatch) -> None:
    """An OSError while reading QUEUE.md degrades to an empty graph."""
    from src.web.services import repo_state as rs

    def boom(_path):
        raise OSError("permission denied")

    monkeypatch.setattr(rs, "parse_queue", boom)
    nodes = build_repo_task_nodes(str(tmp_path), "broken__repo")
    assert nodes == []


class _SnapshotRedis:
    def __init__(self, state: RepoState) -> None:
        self._payload = state.model_dump_json()

    async def get(self, key: str) -> str | None:
        if key == "pipeline:owner__repo":
            return self._payload
        return None


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
