from __future__ import annotations

import json
from typing import Any

from src.metrics import MetricsStore, RunRecord


class _FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.ttls: dict[str, int] = {}
        self.lists: dict[str, list[str]] = {}

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.store[key] = value
        if ex is not None:
            self.ttls[key] = ex

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def lpush(self, key: str, value: str) -> int:
        bucket = self.lists.setdefault(key, [])
        bucket.insert(0, value)
        return len(bucket)

    async def lrem(self, key: str, count: int, value: str) -> int:
        values = self.lists.setdefault(key, [])
        if count != 0:
            raise NotImplementedError("test fake only supports removing all matches")
        kept = [item for item in values if item != value]
        removed = len(values) - len(kept)
        self.lists[key] = kept
        return removed

    async def lrange(self, key: str, start: int, stop: int) -> list[str]:
        values = self.lists.get(key, [])
        if stop < 0:
            stop = len(values) + stop
        return values[start:stop + 1]

    async def ltrim(self, key: str, start: int, stop: int) -> None:
        values = self.lists.get(key, [])
        if stop < 0:
            stop = len(values) + stop
        self.lists[key] = values[start:stop + 1]


def _record(run_id: str, **overrides: Any) -> RunRecord:
    base: dict[str, Any] = {
        "run_id": run_id,
        "task_id": "PR-080",
        "repo_name": "AlexBomber12__pipeline-orchestrator",
        "profile_id": "claude:opus:container",
        "task_type": "feature",
        "complexity": "medium",
        "started_at": "2026-04-18T10:00:00+00:00",
        "ended_at": "2026-04-18T10:05:00+00:00",
        "duration_ms": 300000,
        "fix_iterations": 0,
        "tokens_in": 1200,
        "tokens_out": 800,
        "exit_reason": "success_merged",
        "operator_intervention": False,
    }
    base.update(overrides)
    return RunRecord(**base)


async def test_save_and_get_record() -> None:
    redis = _FakeRedis()
    store = MetricsStore(redis)
    record = _record("run-1")

    await store.save(record)

    saved = await store.get("run-1")

    assert saved == record
    assert redis.ttls["metrics:run:run-1"] == 90 * 86400
    assert redis.lists["metrics:repo:AlexBomber12__pipeline-orchestrator:PR"] == ["run-1"]


async def test_recent_returns_latest() -> None:
    redis = _FakeRedis()
    store = MetricsStore(redis)

    await store.save(_record("run-1", started_at="2026-04-18T10:00:00+00:00"))
    await store.save(_record("run-2", started_at="2026-04-18T11:00:00+00:00"))
    await store.save(_record("run-3", started_at="2026-04-18T12:00:00+00:00"))

    recent = await store.recent(
        limit=2,
        repo_name="AlexBomber12__pipeline-orchestrator",
    )

    assert [record.run_id for record in recent] == ["run-3", "run-2"]


async def test_recent_reads_from_requested_task_namespace() -> None:
    redis = _FakeRedis()
    store = MetricsStore(redis)

    await store.save(_record("pr-run", task_id="PR-080"))
    await store.save(_record("ops-run", task_id="OPS-001", repo_name="ops__repo"))

    recent = await store.recent(task_id="OPS-001", limit=5, repo_name="ops__repo")

    assert [record.run_id for record in recent] == ["ops-run"]


async def test_recent_isolated_by_repository_scope() -> None:
    redis = _FakeRedis()
    store = MetricsStore(redis)

    await store.save(_record("repo-a-run", task_id="PR-080", repo_name="repo-a"))
    await store.save(_record("repo-b-run", task_id="PR-081", repo_name="repo-b"))

    recent = await store.recent(task_id="PR-999", limit=5, repo_name="repo-a")

    assert [record.run_id for record in recent] == ["repo-a-run"]


async def test_save_trims_recent_index() -> None:
    redis = _FakeRedis()
    store = MetricsStore(redis)

    for index in range(205):
        await store.save(_record(f"run-{index}"))

    recent_ids = redis.lists["metrics:repo:AlexBomber12__pipeline-orchestrator:PR"]

    assert len(recent_ids) == 200
    assert recent_ids[0] == "run-204"
    assert recent_ids[-1] == "run-5"


async def test_save_deduplicates_recent_index() -> None:
    redis = _FakeRedis()
    store = MetricsStore(redis)

    await store.save(_record("run-1", task_id="PR-080", fix_iterations=0))
    await store.save(_record("run-2", task_id="PR-080"))
    await store.save(_record("run-1", task_id="PR-080", fix_iterations=1))

    recent = await store.recent(
        task_id="PR-080",
        limit=5,
        repo_name="AlexBomber12__pipeline-orchestrator",
    )

    assert [record.run_id for record in recent] == ["run-1", "run-2"]
    assert recent[0].fix_iterations == 1


async def test_record_serialization() -> None:
    redis = _FakeRedis()
    store = MetricsStore(redis)
    record = _record("run-serialized", ended_at=None, duration_ms=None)

    await store.save(record)

    payload = json.loads(redis.store["metrics:run:run-serialized"])

    assert payload == {
        "base_branch": "",
        "complexity": "medium",
        "diff_lines_added": 0,
        "diff_lines_deleted": 0,
        "duration_ms": None,
        "ended_at": None,
        "exit_reason": "success_merged",
        "files_touched_count": 0,
        "fix_iterations": 0,
        "had_merge_conflict": False,
        "languages_touched": [],
        "operator_intervention": False,
        "stage": "coder",
        "profile_id": "claude:opus:container",
        "repo_name": "AlexBomber12__pipeline-orchestrator",
        "run_id": "run-serialized",
        "started_at": "2026-04-18T10:00:00+00:00",
        "task_id": "PR-080",
        "task_type": "feature",
        "test_file_ratio": 0.0,
        "tokens_in": 1200,
        "tokens_out": 800,
    }


async def test_runrecord_roundtrip_with_new_fields() -> None:
    redis = _FakeRedis()
    store = MetricsStore(redis)
    record = _record(
        "run-new-fields",
        stage="reviewer",
        files_touched_count=3,
        languages_touched=["python", "yaml"],
        diff_lines_added=12,
        diff_lines_deleted=4,
        test_file_ratio=0.333,
        had_merge_conflict=True,
        base_branch="main",
    )

    await store.save(record)

    saved = await store.get("run-new-fields")

    assert saved == record


async def test_runrecord_roundtrip_from_old_payload_sets_defaults() -> None:
    redis = _FakeRedis()
    store = MetricsStore(redis)
    old_payload = {
        "run_id": "legacy-run",
        "task_id": "PR-080",
        "repo_name": "AlexBomber12__pipeline-orchestrator",
        "profile_id": "claude:opus:container",
        "task_type": "feature",
        "complexity": "medium",
        "started_at": "2026-04-18T10:00:00+00:00",
        "ended_at": "2026-04-18T10:05:00+00:00",
        "duration_ms": 300000,
        "fix_iterations": 0,
        "tokens_in": 1200,
        "tokens_out": 800,
        "exit_reason": "success_merged",
        "operator_intervention": False,
    }
    redis.store["metrics:run:legacy-run"] = json.dumps(old_payload)

    saved = await store.get("legacy-run")

    assert saved is not None
    assert saved.stage == "coder"
    assert saved.files_touched_count == 0
    assert saved.languages_touched == []
    assert saved.diff_lines_added == 0
    assert saved.diff_lines_deleted == 0
    assert saved.test_file_ratio == 0.0
    assert saved.had_merge_conflict is False
    assert saved.base_branch == ""


def test_runrecord_stage_default_is_coder() -> None:
    record = _record("stage-default")

    assert record.stage == "coder"


def test_runrecord_stage_accepts_planner_reviewer() -> None:
    planner = _record("stage-planner", stage="planner")
    reviewer = _record("stage-reviewer", stage="reviewer")

    assert planner.stage == "planner"
    assert reviewer.stage == "reviewer"
