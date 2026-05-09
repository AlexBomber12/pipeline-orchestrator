"""Tests for the legacy run-record outcome/cause backfill."""

from __future__ import annotations

import logging
from typing import Any

import pytest
from src.daemon.migrations.run_record_backfill import (
    NULL_CAUSE_VALUE,
    RUN_RECORD_TTL_SECONDS,
    _extract_repo_and_record_id,
    migrate_run_records_to_outcome_cause,
)


class _FakeRedis:
    def __init__(self) -> None:
        self.hashes: dict[str | bytes, dict[Any, Any]] = {}
        self.sets: dict[str, set[str]] = {}
        self.ttls: dict[str | bytes, int] = {}

    async def scan_iter(self, match: str):
        prefix = match.removesuffix("*")
        for key in list(self.hashes):
            normalized = key.decode("utf-8") if isinstance(key, bytes) else key
            if normalized.startswith(prefix):
                yield key

    async def hgetall(self, key: str | bytes) -> dict[Any, Any]:
        return dict(self.hashes.get(key, {}))

    async def hset(self, key: str | bytes, mapping: dict[str, str]) -> int:
        self.hashes.setdefault(key, {}).update(mapping)
        return len(mapping)

    async def sadd(self, key: str, member: str) -> int:
        before = len(self.sets.setdefault(key, set()))
        self.sets[key].add(member)
        return int(len(self.sets[key]) > before)

    async def expire(self, key: str | bytes, seconds: int) -> bool:
        self.ttls[key] = seconds
        return True


class _SyncRedis:
    def __init__(self, hashes: dict[str | bytes, dict[Any, Any]]) -> None:
        self.hashes = hashes
        self.sets: dict[str, set[str]] = {}
        self.ttls: dict[str | bytes, int] = {}

    def scan_iter(self, match: str) -> list[str | bytes]:
        prefix = match.removesuffix("*")
        return [
            key
            for key in self.hashes
            if (key.decode("utf-8") if isinstance(key, bytes) else key).startswith(
                prefix
            )
        ]

    def hgetall(self, key: str | bytes) -> dict[Any, Any]:
        return dict(self.hashes[key])

    def hset(self, key: str | bytes, mapping: dict[str, str]) -> int:
        self.hashes[key].update(mapping)
        return len(mapping)

    def sadd(self, key: str, member: str) -> int:
        self.sets.setdefault(key, set()).add(member)
        return 1

    def expire(self, key: str | bytes, seconds: int) -> bool:
        self.ttls[key] = seconds
        return True


def _record(
    redis: _FakeRedis,
    repo: str,
    record_id: str,
    exit_reason: str,
    *,
    task_id: str | None = "PR-287",
    extra: dict[str, str] | None = None,
) -> str:
    key = f"metrics:run:{repo}:{record_id}"
    payload = {"exit_reason": exit_reason}
    if task_id is not None:
        payload["task_id"] = task_id
    if extra:
        payload.update(extra)
    redis.hashes[key] = payload
    return key


async def test_backfill_maps_exit_reason_to_outcome_cause() -> None:
    redis = _FakeRedis()
    expected = {
        "success_merged": ("merged", NULL_CAUSE_VALUE),
        "closed_unmerged": ("superseded", NULL_CAUSE_VALUE),
        "crash": ("failed", "CRASH"),
        "timeout": ("failed", "TIMEOUT"),
        "error": ("failed", "INFRA"),
        "escalated": ("failed", "ESCALATE"),
        "paused": ("paused", NULL_CAUSE_VALUE),
        "stopped": ("paused", NULL_CAUSE_VALUE),
        "cancelled": ("failed", "ESCALATE"),
    }
    keys = {
        exit_reason: _record(redis, "repo", f"run-{index}", exit_reason)
        for index, exit_reason in enumerate(expected)
    }

    counts = await migrate_run_records_to_outcome_cause(
        redis,
        logging.getLogger(__name__),
    )

    assert counts == {
        "records_scanned": 9,
        "records_migrated": 9,
        "records_skipped_already_migrated": 0,
        "records_skipped_malformed": 0,
    }
    for exit_reason, (outcome, cause) in expected.items():
        assert redis.hashes[keys[exit_reason]]["outcome"] == outcome
        assert redis.hashes[keys[exit_reason]]["cause"] == cause


async def test_backfill_populates_task_runs_index() -> None:
    redis = _FakeRedis()
    _record(redis, "repo", "run-a", "success_merged", task_id="PR-1")
    _record(redis, "repo", "run-b", "timeout", task_id="PR-1")

    await migrate_run_records_to_outcome_cause(redis, logging.getLogger(__name__))

    assert redis.sets["metrics:task_runs:repo:PR-1"] == {"run-a", "run-b"}


async def test_backfill_extends_ttl_to_365d() -> None:
    redis = _FakeRedis()
    key = _record(redis, "repo", "run-ttl", "success_merged")

    await migrate_run_records_to_outcome_cause(redis, logging.getLogger(__name__))

    assert redis.ttls[key] == RUN_RECORD_TTL_SECONDS


async def test_backfill_idempotent_skip_already_migrated() -> None:
    redis = _FakeRedis()
    _record(redis, "repo", "run-idempotent", "success_merged")

    first = await migrate_run_records_to_outcome_cause(
        redis,
        logging.getLogger(__name__),
    )
    second = await migrate_run_records_to_outcome_cause(
        redis,
        logging.getLogger(__name__),
    )

    assert first["records_migrated"] == 1
    assert second == {
        "records_scanned": 1,
        "records_migrated": 0,
        "records_skipped_already_migrated": 1,
        "records_skipped_malformed": 0,
    }


async def test_backfill_skips_malformed_records(
    caplog: pytest.LogCaptureFixture,
) -> None:
    redis = _FakeRedis()
    key = _record(redis, "repo", "run-missing-task", "success_merged", task_id=None)
    log = logging.getLogger("test_run_record_backfill")

    with caplog.at_level(logging.WARNING, logger=log.name):
        counts = await migrate_run_records_to_outcome_cause(redis, log)

    assert counts["records_skipped_malformed"] == 1
    assert "outcome" not in redis.hashes[key]
    assert any("missing task_id" in rec.getMessage() for rec in caplog.records)


async def test_backfill_unknown_exit_reason_defaults_to_failed_unknown(
    caplog: pytest.LogCaptureFixture,
) -> None:
    redis = _FakeRedis()
    key = _record(redis, "repo", "run-weird", "weird")
    log = logging.getLogger("test_run_record_backfill")

    with caplog.at_level(logging.WARNING, logger=log.name):
        counts = await migrate_run_records_to_outcome_cause(redis, log)

    assert counts["records_migrated"] == 1
    assert redis.hashes[key]["outcome"] == "failed"
    assert redis.hashes[key]["cause"] == NULL_CAUSE_VALUE
    assert any("Unknown run-record exit_reason" in rec.getMessage() for rec in caplog.records)


async def test_backfill_supports_sync_redis_bytes_and_callable_logger() -> None:
    key = b"metrics:run:repo:run-bytes"
    redis = _SyncRedis({key: {b"task_id": b"PR-287", b"exit_reason": b"timeout"}})
    messages: list[str] = []

    counts = await migrate_run_records_to_outcome_cause(redis, messages.append)

    assert counts["records_migrated"] == 1
    assert redis.hashes[key]["outcome"] == "failed"
    assert redis.hashes[key]["cause"] == "TIMEOUT"
    assert redis.sets["metrics:task_runs:repo:PR-287"] == {"run-bytes"}
    assert redis.ttls[key] == RUN_RECORD_TTL_SECONDS
    assert messages == []


async def test_backfill_callable_logger_warns_for_malformed_keys() -> None:
    redis = _SyncRedis(
        {
            "metrics:run:missing-record-id": {"task_id": "PR-1"},
            "metrics:run::run-id": {"task_id": "PR-1"},
            "metrics:run:repo:": {"task_id": "PR-1"},
        }
    )
    messages: list[str] = []

    counts = await migrate_run_records_to_outcome_cause(redis, messages.append)

    assert counts["records_scanned"] == 3
    assert counts["records_skipped_malformed"] == 3
    assert len(messages) == 3
    assert all("Skipping malformed run-record key" in message for message in messages)


async def test_backfill_skips_non_hash_records(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _BadHashRedis(_FakeRedis):
        async def hgetall(self, key: str | bytes) -> list[str]:  # type: ignore[override]
            return ["not-a-hash"]

    redis = _BadHashRedis()
    redis.hashes["metrics:run:repo:run-bad"] = {}
    log = logging.getLogger("test_run_record_backfill")

    with caplog.at_level(logging.WARNING, logger=log.name):
        counts = await migrate_run_records_to_outcome_cause(redis, log)

    assert counts["records_skipped_malformed"] == 1
    assert any("Skipping malformed run-record hash" in rec.getMessage() for rec in caplog.records)


def test_extract_repo_and_record_id_rejects_non_string_key() -> None:
    assert _extract_repo_and_record_id(123) is None  # type: ignore[arg-type]
