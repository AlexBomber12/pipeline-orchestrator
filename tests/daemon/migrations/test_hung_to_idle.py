"""Tests for the legacy HUNG Redis startup migration."""

from __future__ import annotations

import json
import logging
from typing import Any

import pytest
from src.cancellation.storage import CancellationCause, cause_key
from src.daemon.migrations import hung_to_idle
from src.daemon.migrations.hung_to_idle import migrate_hung_to_idle_on_startup


class _FakePipeline:
    def __init__(self, redis: "_FakeRedis") -> None:
        self._redis = redis
        self._ops: list[tuple[Any, ...]] = []

    def set(self, key: str, value: str, ex: int | None = None) -> "_FakePipeline":
        self._ops.append(("set", key, value, ex))
        return self

    def zadd(self, key: str, mapping: dict[str, float]) -> "_FakePipeline":
        self._ops.append(("zadd", key, dict(mapping)))
        return self

    def zremrangebyscore(self, key: str, min_score: Any, max_score: Any) -> "_FakePipeline":
        self._ops.append(("zremrangebyscore", key, min_score, max_score))
        return self

    def expire(self, key: str, seconds: int) -> "_FakePipeline":
        self._ops.append(("expire", key, seconds))
        return self

    async def execute(self) -> list[Any]:
        results: list[Any] = []
        for op in self._ops:
            if op[0] == "set":
                _, key, value, _ex = op
                self._redis.values[key] = value
                results.append(True)
            elif op[0] == "zadd":
                _, key, mapping = op
                self._redis.zsets.setdefault(key, {}).update(mapping)
                results.append(len(mapping))
            elif op[0] in {"zremrangebyscore", "expire"}:
                results.append(True)
        return results


class _FakeRedis:
    def __init__(self, values: dict[str, str]) -> None:
        self.values = dict(values)
        self.zsets: dict[str, dict[str, float]] = {}
        self.set_calls: list[tuple[str, str]] = []

    async def scan_iter(self, match: str):
        prefix = match.removesuffix("*")
        for key in list(self.values):
            if key.startswith(prefix):
                yield key

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def set(self, key: str, value: str) -> bool:
        self.set_calls.append((key, value))
        self.values[key] = value
        return True

    def pipeline(self) -> _FakePipeline:
        return _FakePipeline(self)


class _SyncRedis:
    def __init__(self, values: dict[str | bytes, str]) -> None:
        self.values = dict(values)
        self.set_calls: list[tuple[str | bytes, str]] = []

    def scan_iter(self, match: str) -> list[str | bytes]:
        return list(self.values)

    def get(self, key: str | bytes) -> str | None:
        return self.values.get(key)

    def set(self, key: str | bytes, value: str) -> bool:
        self.set_calls.append((key, value))
        self.values[key] = value
        return True


def _state(state: str, current_task: Any = None) -> str:
    return json.dumps({"state": state, "current_task": current_task})


async def test_migrate_no_hung_repos_is_noop() -> None:
    redis = _FakeRedis(
        {
            "pipeline:repo_a": _state("IDLE"),
            "pipeline:repo_b": _state("CODING", {"pr_id": "PR-1"}),
            "pipeline:repo_c": _state("ERROR", {"pr_id": "PR-2"}),
        }
    )

    count = await migrate_hung_to_idle_on_startup(redis, logging.getLogger(__name__))

    assert count == 0
    assert redis.set_calls == []


async def test_migrate_one_hung_repo_rewrites_state() -> None:
    redis = _FakeRedis(
        {
            "pipeline:test_repo": _state(
                "HUNG",
                {"pr_id": "PR-100", "title": "legacy hung task"},
            )
        }
    )

    count = await migrate_hung_to_idle_on_startup(redis, logging.getLogger(__name__))

    assert count == 1
    assert json.loads(redis.values["pipeline:test_repo"])["state"] == "IDLE"
    cause = CancellationCause.from_redis(
        redis.values[cause_key("test_repo", "PR-100")]
    )
    assert cause.category == "ERROR"
    assert cause.payload["subsource"] == "daemon"
    assert cause.payload["legacy_category"] == "ESCALATE"
    assert cause.payload["migration_note"]


async def test_migrate_rewrites_legacy_canceled_task_statuses() -> None:
    redis = _FakeRedis(
        {
            "pipeline:test_repo": json.dumps(
                {
                    "state": "IDLE",
                    "current_task": {
                        "pr_id": "PR-100",
                        "title": "legacy canceled task",
                        "status": "CANCELED",
                    },
                    "current_queue": [
                        {
                            "pr_id": "PR-100",
                            "title": "legacy canceled task",
                            "status": "CANCELED",
                        },
                        {
                            "pr_id": "PR-101",
                            "title": "active task",
                            "status": "TODO",
                        },
                    ],
                }
            )
        }
    )

    count = await migrate_hung_to_idle_on_startup(redis, logging.getLogger(__name__))
    payload = json.loads(redis.values["pipeline:test_repo"])

    assert count == 1
    assert payload["current_task"]["status"] == "ERROR"
    assert payload["current_queue"][0]["status"] == "ERROR"
    assert payload["current_queue"][1]["status"] == "TODO"


async def test_migrate_skips_non_object_pipeline_payload() -> None:
    redis = _FakeRedis({"pipeline:test_repo": json.dumps(["not", "repo", "state"])})

    count = await migrate_hung_to_idle_on_startup(redis, logging.getLogger(__name__))

    assert count == 0
    assert redis.set_calls == []


async def test_migrate_hung_repo_no_current_task() -> None:
    redis = _FakeRedis({"pipeline:test_repo": _state("HUNG", None)})

    count = await migrate_hung_to_idle_on_startup(redis, logging.getLogger(__name__))

    assert count == 1
    assert json.loads(redis.values["pipeline:test_repo"])["state"] == "IDLE"
    assert not any(key.startswith("cancellation:") for key in redis.values)


async def test_migrate_idempotent() -> None:
    redis = _FakeRedis(
        {"pipeline:test_repo": _state("HUNG", {"pr_id": "PR-100"})}
    )

    first = await migrate_hung_to_idle_on_startup(redis, logging.getLogger(__name__))
    redis.set_calls.clear()
    second = await migrate_hung_to_idle_on_startup(redis, logging.getLogger(__name__))

    assert first == 1
    assert second == 0
    assert redis.set_calls == []


async def test_migrate_skips_malformed_keys(
    caplog: pytest.LogCaptureFixture,
) -> None:
    redis = _FakeRedis(
        {
            "pipeline:test_repo": "not valid json",
            "pipeline:other_repo": _state("HUNG", None),
        }
    )
    log = logging.getLogger("test_migration")

    with caplog.at_level(logging.WARNING, logger=log.name):
        count = await migrate_hung_to_idle_on_startup(redis, log)

    assert count == 1
    assert redis.values["pipeline:test_repo"] == "not valid json"
    assert any(
        "Skipping malformed pipeline:* key pipeline:test_repo" in rec.getMessage()
        for rec in caplog.records
    )


async def test_migrate_proceeds_if_cause_writer_fails(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def boom(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("redis transient")

    monkeypatch.setattr(hung_to_idle, "record_cancellation_cause", boom)
    redis = _FakeRedis({"pipeline:test_repo": _state("HUNG", {"pr_id": "PR-100"})})
    log = logging.getLogger("test_migration")

    with caplog.at_level(logging.WARNING, logger=log.name):
        count = await migrate_hung_to_idle_on_startup(redis, log)

    assert count == 1
    assert json.loads(redis.values["pipeline:test_repo"])["state"] == "IDLE"
    assert cause_key("test_repo", "PR-100") not in redis.values
    assert any(
        "Failed to record cancellation cause" in rec.getMessage()
        for rec in caplog.records
    )


async def test_migrate_supports_sync_redis_scan_and_byte_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded: list[tuple[str, str, str]] = []

    async def fake_record(
        _redis_client: Any,
        repo_slug: str,
        task_id: str,
        _cause: CancellationCause,
    ) -> None:
        recorded.append(("cause", repo_slug, task_id))

    messages: list[str] = []
    monkeypatch.setattr(hung_to_idle, "record_cancellation_cause", fake_record)
    redis = _SyncRedis({b"pipeline:test_repo": _state("HUNG", "PR-STRING")})

    count = await migrate_hung_to_idle_on_startup(redis, messages.append)

    assert count == 1
    assert json.loads(redis.values[b"pipeline:test_repo"])["state"] == "IDLE"
    assert recorded == [("cause", "test_repo", "PR-STRING")]
    assert any("HUNG\u2192IDLE for test_repo" in message for message in messages)


async def test_migrate_callable_logger_warns_for_sync_malformed_key() -> None:
    messages: list[str] = []
    redis = _SyncRedis({"pipeline:test_repo": "not-json"})

    count = await migrate_hung_to_idle_on_startup(redis, messages.append)

    assert count == 0
    assert messages == ["[MIGRATION] Skipping malformed pipeline:* key pipeline:test_repo"]
