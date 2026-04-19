"""Run metrics schema and Redis-backed storage helpers."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any

_TTL_SECONDS = 90 * 86400
_RECENT_INDEX_LIMIT = 200


@dataclass
class RunRecord:
    run_id: str
    task_id: str
    profile_id: str
    task_type: str
    complexity: str
    started_at: str
    ended_at: str | None
    duration_ms: int | None
    fix_iterations: int
    tokens_in: int
    tokens_out: int
    exit_reason: str
    operator_intervention: bool
    repo_name: str = ""
    # Pipeline stage that produced this record. Current values: 'coder'.
    # Reserved for future: 'planner', 'reviewer', 'qa'. PR-level cost
    # aggregation sums across all stages for one (task_id, repo_name) bundle.
    stage: str = "coder"
    files_touched_count: int = 0
    languages_touched: list[str] = field(default_factory=list)
    diff_lines_added: int = 0
    diff_lines_deleted: int = 0
    test_file_ratio: float = 0.0
    had_merge_conflict: bool = False
    base_branch: str = ""


class MetricsStore:
    """Persist run records in Redis with a small recency index."""

    def __init__(self, redis_client: Any) -> None:
        self._redis = redis_client

    async def save(self, record: RunRecord) -> None:
        key = self._record_key(record.run_id)
        payload = json.dumps(asdict(record), sort_keys=True)
        recent_key = self._recent_key(record.task_id, record.repo_name)
        await self._redis.set(key, payload, ex=_TTL_SECONDS)
        await self._redis.lrem(recent_key, 0, record.run_id)
        await self._redis.lpush(recent_key, record.run_id)
        await self._redis.ltrim(recent_key, 0, _RECENT_INDEX_LIMIT - 1)

    async def get(self, run_id: str) -> RunRecord | None:
        raw = await self._redis.get(self._record_key(run_id))
        if raw is None:
            return None
        return RunRecord(**json.loads(raw))

    async def recent(
        self,
        task_id: str = "PR",
        limit: int = 20,
        repo_name: str = "",
    ) -> list[RunRecord]:
        if limit <= 0:
            return []
        run_ids = await self._redis.lrange(
            self._recent_key(task_id, repo_name),
            0,
            limit - 1,
        )
        records: list[RunRecord] = []
        for run_id in run_ids:
            record = await self.get(run_id)
            if record is not None:
                records.append(record)
        return records

    @staticmethod
    def _record_key(run_id: str) -> str:
        return f"metrics:run:{run_id}"

    @staticmethod
    def _recent_key(task_id: str, repo_name: str = "") -> str:
        task_prefix = task_id.split("-", 1)[0]
        repo_scope = repo_name or "global"
        return f"metrics:repo:{repo_scope}:{task_prefix}"
