"""Tests for ``list_pending_guardrail_decisions`` (PR-305a).

The operator override backend reads guardrail-flagged pending
cancellations from Redis. PR-305a adds the storage-layer helper; the
HTTP endpoints (PR-305b GET, PR-305c POST) consume what this returns.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from src.cancellation.storage import (
    CancellationCause,
    GuardrailPending,
    cause_key,
    index_key,
    list_pending_guardrail_decisions,
)


class _FakeRedis:
    """Minimal Redis double exposing only the surface the helper uses."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.zrange_calls: list[tuple[int, int]] = []

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def zrange(
        self,
        key: str,
        start: int,
        stop: int,
        withscores: bool = False,
    ) -> list[Any]:
        self.zrange_calls.append((start, stop))
        ordered = sorted(self.zsets.get(key, {}).items(), key=lambda kv: kv[1])
        members = [tid for tid, _ in ordered]
        sliced = members[start:] if stop == -1 else members[start : stop + 1]
        return [(m, dict(ordered)[m]) for m in sliced] if withscores else sliced

    async def zrem(self, key: str, *members: str) -> int:
        zset = self.zsets.get(key)
        if not zset:
            return 0
        removed = 0
        for member in members:
            if member in zset:
                del zset[member]
                removed += 1
        return removed


def _put(
    redis: _FakeRedis,
    repo_slug: str,
    task_id: str,
    payload: dict[str, Any],
    created_at: str,
) -> None:
    cause = CancellationCause(
        category="ERROR",
        payload=payload,
        created_at=created_at,
        task_id=task_id,
        repo_slug=repo_slug,
    )
    redis.values[cause_key(repo_slug, task_id)] = cause.to_redis()
    score = datetime.fromisoformat(created_at).timestamp()
    redis.zsets.setdefault(index_key(repo_slug), {})[task_id] = score


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


_BASE = datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc).timestamp()


async def test_list_pending_guardrail_decisions_filters_by_subsource() -> None:
    redis = _FakeRedis()
    _put(redis, "alpha", "PR-1", {"subsource": "guardrail"}, _iso(_BASE))
    _put(redis, "alpha", "PR-2", {"subsource": "guardrail"}, _iso(_BASE + 1))
    _put(redis, "alpha", "PR-3", {"subsource": "coder"}, _iso(_BASE + 2))

    result = await list_pending_guardrail_decisions(redis, "alpha")

    assert [p.task_id for p in result] == ["PR-1", "PR-2"]
    assert all(isinstance(p, GuardrailPending) for p in result)


async def test_list_pending_guardrail_decisions_sorted_by_recorded_at_ascending() -> None:
    redis = _FakeRedis()
    for tid, offset in [("PR-MID", 5), ("PR-EARLY", 0), ("PR-LATE", 10)]:
        _put(redis, "alpha", tid, {"subsource": "guardrail"}, _iso(_BASE + offset))

    result = await list_pending_guardrail_decisions(redis, "alpha")

    assert [p.task_id for p in result] == ["PR-EARLY", "PR-MID", "PR-LATE"]
    assert [p.recorded_at for p in result] == [
        int(_BASE),
        int(_BASE + 5),
        int(_BASE + 10),
    ]


async def test_list_pending_guardrail_decisions_extracts_rule_and_excerpt() -> None:
    redis = _FakeRedis()
    _put(
        redis,
        "alpha",
        "PR-7",
        {
            "subsource": "guardrail",
            "rule": "large_diff_threshold",
            "excerpt": "+1800 LOC across 35 files",
        },
        _iso(_BASE),
    )

    [pending] = await list_pending_guardrail_decisions(redis, "alpha")

    assert pending.rule == "large_diff_threshold"
    assert pending.excerpt == "+1800 LOC across 35 files"
    assert pending.task_id == "PR-7"
    assert pending.repo_slug == "alpha"


async def test_list_pending_guardrail_decisions_handles_missing_payload_keys() -> None:
    redis = _FakeRedis()
    _put(redis, "alpha", "PR-9", {"subsource": "guardrail"}, _iso(_BASE))

    [pending] = await list_pending_guardrail_decisions(redis, "alpha")

    assert pending.rule == ""
    assert pending.excerpt == ""


async def test_list_pending_guardrail_decisions_repo_isolation() -> None:
    redis = _FakeRedis()
    _put(redis, "repo_a", "PR-A1", {"subsource": "guardrail"}, _iso(_BASE))
    _put(redis, "repo_b", "PR-B1", {"subsource": "guardrail"}, _iso(_BASE + 1))

    result = await list_pending_guardrail_decisions(redis, "repo_a")

    assert [p.task_id for p in result] == ["PR-A1"]
    assert all(p.repo_slug == "repo_a" for p in result)


async def test_list_pending_guardrail_decisions_bounded_by_limit() -> None:
    redis = _FakeRedis()
    for idx in range(150):
        _put(
            redis,
            "alpha",
            f"PR-{idx:03d}",
            {"subsource": "guardrail"},
            _iso(_BASE + idx),
        )

    result = await list_pending_guardrail_decisions(redis, "alpha", limit=100)

    assert len(result) == 100
    assert result[0].task_id == "PR-000"
    assert result[-1].task_id == "PR-099"


async def test_list_pending_guardrail_decisions_empty_returns_empty_list() -> None:
    redis = _FakeRedis()
    assert await list_pending_guardrail_decisions(redis, "alpha") == []


async def test_list_pending_guardrail_decisions_prunes_stale_index_entries() -> None:
    """Stale members (payload expired) must be skipped AND removed from the index."""
    redis = _FakeRedis()
    _put(redis, "alpha", "PR-STALE", {"subsource": "guardrail"}, _iso(_BASE))
    _put(redis, "alpha", "PR-LIVE", {"subsource": "guardrail"}, _iso(_BASE + 1))
    # Simulate TTL expiry: payload gone, but index still references the task.
    del redis.values[cause_key("alpha", "PR-STALE")]

    result = await list_pending_guardrail_decisions(redis, "alpha")

    assert [p.task_id for p in result] == ["PR-LIVE"]
    # Stale member is dropped so future calls do not re-scan it.
    assert "PR-STALE" not in redis.zsets[index_key("alpha")]
    assert "PR-LIVE" in redis.zsets[index_key("alpha")]


async def test_list_pending_guardrail_decisions_paginates_below_full_index() -> None:
    """When ``limit`` is small relative to the index, do not load the whole ZSET."""
    redis = _FakeRedis()
    # 50 guardrail entries; helper called with limit=10 should fetch one batch
    # of size 10, not the entire ZSET.
    for idx in range(50):
        _put(
            redis,
            "alpha",
            f"PR-{idx:03d}",
            {"subsource": "guardrail"},
            _iso(_BASE + idx),
        )

    result = await list_pending_guardrail_decisions(redis, "alpha", limit=10)

    assert len(result) == 10
    # Each zrange call requested at most ``limit`` rows — never the full set.
    assert redis.zrange_calls, "helper must call zrange at least once"
    for start, stop in redis.zrange_calls:
        assert stop - start + 1 <= 10
        assert stop != -1


async def test_list_pending_guardrail_decisions_decodes_bytes_task_ids() -> None:
    """Redis returns members as bytes by default; helper must decode them."""
    redis = _FakeRedis()
    _put(redis, "alpha", "PR-BYTES", {"subsource": "guardrail"}, _iso(_BASE))

    async def _bytes_zrange(
        key: str, start: int, stop: int, withscores: bool = False
    ) -> list[bytes]:
        return [b"PR-BYTES"]

    redis.zrange = _bytes_zrange  # type: ignore[assignment]

    result = await list_pending_guardrail_decisions(redis, "alpha")

    assert [p.task_id for p in result] == ["PR-BYTES"]


async def test_list_pending_guardrail_decisions_skips_non_dict_payload() -> None:
    """Malformed records where ``payload`` is not a dict must be skipped, not crash."""
    redis = _FakeRedis()
    # Live guardrail entry that must still be returned.
    _put(redis, "alpha", "PR-OK", {"subsource": "guardrail"}, _iso(_BASE + 1))
    # Corrupted legacy records: payload deserializes to a string / list / None
    # instead of dict. Calling ``.get`` on them would raise AttributeError and
    # poison the whole pending-decisions read for the operator override view.
    for tid, bad_payload, offset in [
        ("PR-BAD-STR", "guardrail", 2),
        ("PR-BAD-LIST", ["guardrail"], 3),
        ("PR-BAD-NONE", None, 4),
    ]:
        created_at = _iso(_BASE + offset)
        raw = (
            '{"category":"ERROR","payload":'
            + (
                "null"
                if bad_payload is None
                else (
                    f'"{bad_payload}"'
                    if isinstance(bad_payload, str)
                    else '["guardrail"]'
                )
            )
            + f',"created_at":"{created_at}","task_id":"{tid}","repo_slug":"alpha"}}'
        )
        redis.values[cause_key("alpha", tid)] = raw
        score = datetime.fromisoformat(created_at).timestamp()
        redis.zsets.setdefault(index_key("alpha"), {})[tid] = score

    result = await list_pending_guardrail_decisions(redis, "alpha")

    assert [p.task_id for p in result] == ["PR-OK"]
