"""Run metrics schema and Redis-backed storage helpers."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

_TTL_SECONDS = 365 * 86400
RUN_RECORD_TTL_SECONDS = _TTL_SECONDS
_RECENT_INDEX_LIMIT = 200

RunOutcome = Literal["merged", "failed", "paused", "superseded"]
RunCause = Literal["CRASH", "ESCALATE", "TIMEOUT", "INFRA", "NO_PUSH_DEADLOCK"]
RunPhase = Literal["coding", "fix", "merge", "recovery"]

_LEGACY_EXIT_REASON_MAP: dict[str, tuple[RunOutcome, RunCause | None]] = {
    "success_merged": ("merged", None),
    "coding_complete": ("superseded", None),
    "closed_unmerged": ("superseded", None),
    "rate_limit": ("paused", None),
    "paused": ("paused", None),
    "stopped": ("paused", None),
    "cancelled": ("paused", None),
    "timeout": ("failed", "TIMEOUT"),
    "escalated": ("failed", "ESCALATE"),
    "error": ("failed", "CRASH"),
    "crash": ("failed", "CRASH"),
}


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
    # Deprecated: retained only so legacy Redis payloads and UI readers can
    # still deserialize until PR-287 backfills outcome/cause.
    exit_reason: str
    operator_intervention: bool
    outcome: RunOutcome = ""
    cause: RunCause | None = None
    run_phase: RunPhase = "coding"
    attempt_index: int = 1
    coder_session_id: str = ""
    base_sha: str = ""
    head_sha: str = ""
    task_spec_hash: str = ""
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

    def __post_init__(self) -> None:
        if not self.outcome:
            self.outcome, self.cause = _LEGACY_EXIT_REASON_MAP.get(
                self.exit_reason,
                ("failed", "CRASH"),
            )
        if self.outcome == "failed":
            if self.cause is None:
                raise ValueError("failed run records require cause")
            if self.cause not in RunCause.__args__:
                raise ValueError(f"invalid run record cause: {self.cause}")
        else:
            self.cause = None
        if self.outcome not in RunOutcome.__args__:
            raise ValueError(f"invalid run record outcome: {self.outcome}")
        if self.run_phase not in RunPhase.__args__:
            raise ValueError(f"invalid run record phase: {self.run_phase}")
        if self.attempt_index < 1:
            raise ValueError("attempt_index must be >= 1")


class MetricsStore:
    """Persist run records in Redis with a small recency index."""

    def __init__(self, redis_client: Any) -> None:
        self._redis = redis_client

    async def save(self, record: RunRecord) -> None:
        key = self._record_key(record.run_id)
        payload = json.dumps(asdict(record), sort_keys=True)
        recent_key = self._recent_key(record.task_id, record.repo_name)
        await self._redis.set(key, payload, ex=_TTL_SECONDS)
        task_runs_key = self._task_runs_key(record.repo_name, record.task_id)
        await self._redis.sadd(task_runs_key, record.run_id)
        expire = getattr(self._redis, "expire", None)
        if expire is not None:
            await expire(task_runs_key, _TTL_SECONDS)
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

    async def list_task_runs(self, repo_name: str, task_id: str) -> list[RunRecord]:
        run_ids = await self._redis.smembers(
            self._task_runs_key(repo_name, task_id)
        )
        if not run_ids:
            return []
        normalized = [
            run_id.decode("utf-8") if isinstance(run_id, bytes) else str(run_id)
            for run_id in run_ids
        ]
        raw_records = await self._redis.mget(
            [self._record_key(run_id) for run_id in normalized]
        )
        records: list[RunRecord] = []
        for raw in raw_records:
            if raw is None:
                continue
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            records.append(RunRecord(**json.loads(raw)))
        return records

    @staticmethod
    def _record_key(run_id: str) -> str:
        return f"metrics:run:{run_id}"

    @staticmethod
    def _recent_key(task_id: str, repo_name: str = "") -> str:
        task_prefix = task_id.split("-", 1)[0]
        repo_scope = repo_name or "global"
        return f"metrics:repo:{repo_scope}:{task_prefix}"

    @staticmethod
    def _task_runs_key(repo_name: str, task_id: str) -> str:
        repo_scope = repo_name or "global"
        return f"metrics:task_runs:{repo_scope}:{task_id}"
