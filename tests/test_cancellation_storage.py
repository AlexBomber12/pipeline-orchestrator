"""Tests for src.cancellation.storage (PR-252 substrate)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from src.cancellation import storage
from src.cancellation.storage import (
    CATEGORIES,
    READ_REFRESH_TTL_SECONDS,
    TTL_SECONDS,
    CancellationCause,
    cause_key,
    get_cancellation_cause,
    index_key,
    list_recent_cancellations,
    record_cancellation_cause,
)


class _FakePipeline:
    def __init__(self, store: "_FakeRedis") -> None:
        self._store = store
        self._ops: list[tuple] = []

    def set(self, key: str, value: str, ex: int | None = None) -> "_FakePipeline":
        self._ops.append(("set", key, value, ex))
        return self

    def zadd(self, key: str, mapping: dict[str, float]) -> "_FakePipeline":
        self._ops.append(("zadd", key, dict(mapping)))
        return self

    def zremrangebyscore(self, key: str, min_score, max_score) -> "_FakePipeline":
        self._ops.append(("zremrangebyscore", key, min_score, max_score))
        return self

    def expire(self, key: str, seconds: int) -> "_FakePipeline":
        self._ops.append(("expire", key, seconds))
        return self

    async def execute(self) -> list:
        results = []
        for op in self._ops:
            if op[0] == "set":
                _, key, value, ex = op
                self._store.values[key] = value
                if ex is not None:
                    self._store.ttls[key] = ex
                results.append(True)
            elif op[0] == "zadd":
                _, key, mapping = op
                bucket = self._store.zsets.setdefault(key, {})
                bucket.update(mapping)
                results.append(len(mapping))
            elif op[0] == "zremrangebyscore":
                _, key, min_score, max_score = op
                results.append(self._store._zremrangebyscore(key, min_score, max_score))
            elif op[0] == "expire":
                _, key, seconds = op
                if key in self._store.zsets or key in self._store.values:
                    self._store.ttls[key] = seconds
                    results.append(True)
                else:
                    results.append(False)
        self._ops.clear()
        return results


class _FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.ttls: dict[str, int] = {}
        self.zsets: dict[str, dict[str, float]] = {}

    def pipeline(self) -> _FakePipeline:
        return _FakePipeline(self)

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.values[key] = value
        if ex is not None:
            self.ttls[key] = ex

    async def delete(self, key: str) -> int:
        existed = key in self.values
        self.values.pop(key, None)
        self.ttls.pop(key, None)
        return int(existed)

    async def zrangebyscore(self, key: str, min_score, max_score) -> list[str]:
        bucket = self.zsets.get(key, {})
        if max_score == "+inf":
            upper = float("inf")
        else:
            upper = float(max_score)
        lower = float(min_score)
        items = [tid for tid, score in bucket.items() if lower <= score <= upper]
        items.sort(key=lambda tid: bucket[tid])
        return items

    async def zrem(self, key: str, *members: str) -> int:
        bucket = self.zsets.get(key, {})
        removed = 0
        for member in members:
            if member in bucket:
                del bucket[member]
                removed += 1
        return removed

    async def zadd(self, key: str, mapping: dict[str, float]) -> int:
        bucket = self.zsets.setdefault(key, {})
        added = 0
        for member, score in mapping.items():
            if member not in bucket:
                added += 1
            bucket[member] = float(score)
        return added

    async def zscore(self, key: str, member: str) -> float | None:
        bucket = self.zsets.get(key, {})
        score = bucket.get(member)
        return None if score is None else float(score)

    async def expire(self, key: str, seconds: int) -> bool:
        if key in self.zsets or key in self.values:
            self.ttls[key] = seconds
            return True
        return False

    def _zremrangebyscore(self, key: str, min_score, max_score) -> int:
        bucket = self.zsets.get(key)
        if not bucket:
            return 0
        lower, lower_excl = self._parse_bound(min_score, default=float("-inf"))
        upper, upper_excl = self._parse_bound(max_score, default=float("inf"))
        to_remove = [
            tid
            for tid, score in bucket.items()
            if (score > lower if lower_excl else score >= lower)
            and (score < upper if upper_excl else score <= upper)
        ]
        for tid in to_remove:
            del bucket[tid]
        return len(to_remove)

    @staticmethod
    def _parse_bound(value, default: float) -> tuple[float, bool]:
        if value in ("-inf", "+inf"):
            return float(value), False
        if isinstance(value, str) and value.startswith("("):
            return float(value[1:]), True
        return float(value), False


def test_categories_tuple_does_not_include_operator_recovery() -> None:
    assert "OPERATOR_RECOVERY" not in CATEGORIES


def test_categories_tuple_collapsed_to_unified_error() -> None:
    assert CATEGORIES == ("ERROR",)


def test_cause_and_index_key_format() -> None:
    assert cause_key("alpha", "PR-101") == "cancellation:alpha:PR-101"
    assert index_key("alpha") == "cancellation_index:alpha"


def test_cancellation_cause_round_trip_serialization() -> None:
    cause = CancellationCause(
        category="CRASH",
        payload={"exit_code": 137, "stderr": "killed"},
        created_at="2026-05-04T12:00:00+00:00",
        task_id="PR-200",
        repo_slug="example__repo",
    )
    raw = cause.to_redis()
    restored = CancellationCause.from_redis(raw)
    assert restored == cause
    restored_bytes = CancellationCause.from_redis(raw.encode("utf-8"))
    assert restored_bytes == cause


async def test_round_trip_write_read() -> None:
    redis = _FakeRedis()
    cause = CancellationCause(
        category="ESCALATE",
        payload={"reason": "manual ESCALATE"},
        created_at="2026-05-04T10:00:00+00:00",
    )
    await record_cancellation_cause(redis, "alpha", "PR-300", cause)

    fetched = await get_cancellation_cause(redis, "alpha", "PR-300")
    assert fetched is not None
    assert fetched.category == "ESCALATE"
    assert fetched.payload == {"reason": "manual ESCALATE"}
    assert fetched.task_id == "PR-300"
    assert fetched.repo_slug == "alpha"
    assert fetched.created_at == "2026-05-04T10:00:00+00:00"


async def test_record_fills_missing_metadata() -> None:
    redis = _FakeRedis()
    cause = CancellationCause(category="INFRA")
    await record_cancellation_cause(redis, "beta", "PR-301", cause)

    stored = await get_cancellation_cause(redis, "beta", "PR-301")
    assert stored is not None
    assert stored.task_id == "PR-301"
    assert stored.repo_slug == "beta"
    assert stored.created_at
    datetime.fromisoformat(stored.created_at)


async def test_record_overrides_stale_identifiers_in_payload() -> None:
    redis = _FakeRedis()
    cause = CancellationCause(
        category="CRASH",
        created_at="2026-05-04T08:00:00+00:00",
        task_id="PR-OLD",
        repo_slug="old-repo",
    )
    await record_cancellation_cause(redis, "new-repo", "PR-NEW", cause)

    stored = await get_cancellation_cause(redis, "new-repo", "PR-NEW")
    assert stored is not None
    assert stored.task_id == "PR-NEW"
    assert stored.repo_slug == "new-repo"
    # Caller's object is also coerced to match the key arguments.
    assert cause.task_id == "PR-NEW"
    assert cause.repo_slug == "new-repo"


async def test_missing_key_returns_none() -> None:
    redis = _FakeRedis()
    assert await get_cancellation_cause(redis, "alpha", "PR-404") is None


async def test_task_spec_hash_helpers_decode_bytes_and_delete() -> None:
    class _BytesRedis(_FakeRedis):
        async def get(self, key: str) -> bytes | None:
            raw = self.values.get(key)
            return raw.encode("utf-8") if raw is not None else None

    redis = _BytesRedis()
    await storage.record_task_spec_hash(redis, "alpha", "PR-300", "abc123")

    assert storage.task_spec_hash_key("alpha", "PR-300") in redis.values
    assert await storage.get_task_spec_hash(redis, "alpha", "PR-300") == "abc123"

    await storage.delete_task_spec_hash(redis, "alpha", "PR-300")
    assert storage.task_spec_hash_key("alpha", "PR-300") not in redis.values


def test_task_spec_content_hash_ignores_frontmatter_status() -> None:
    error_text = "---\nstatus: ERROR\n---\n\nBody\n"
    todo_text = "---\nstatus: TODO\n---\n\nBody\n"
    changed_text = "---\nstatus: TODO\n---\n\nChanged\n"
    plain_text = "status: ERROR\n\nBody\n"
    no_frontmatter_text = "Body\n"

    assert storage.task_spec_content_hash(error_text) == storage.task_spec_content_hash(
        todo_text
    )
    assert storage.task_spec_content_hash(error_text) == storage.task_spec_content_hash(
        no_frontmatter_text
    )
    assert storage.task_spec_content_hash(changed_text) != storage.task_spec_content_hash(
        error_text
    )
    assert storage.task_spec_content_hash(plain_text) != storage.task_spec_content_hash(
        error_text
    )


def test_task_spec_content_hash_preserves_non_status_frontmatter() -> None:
    metadata_text = "---\nstatus: ERROR\nowner: ops\n---\n\nBody\n"
    changed_status_text = "---\nstatus: TODO\nowner: ops\n---\n\nBody\n"
    no_metadata_text = "Body\n"

    assert storage.task_spec_content_hash(metadata_text) == storage.task_spec_content_hash(
        changed_status_text
    )
    assert storage.task_spec_content_hash(metadata_text) != storage.task_spec_content_hash(
        no_metadata_text
    )


def test_task_spec_content_hash_preserves_unclosed_frontmatter() -> None:
    malformed_text = "---\nstatus: ERROR\nBody\n"
    plain_text = "Body\n"

    assert storage.task_spec_content_hash(malformed_text) != storage.task_spec_content_hash(
        plain_text
    )


async def test_retry_count_helpers_reset_and_delete() -> None:
    redis = _FakeRedis()
    await storage.reset_retry_count(redis, "alpha", "PR-300")

    key = storage.retry_count_key("alpha", "PR-300")
    assert redis.values[key] == "0"
    assert redis.ttls[key] == TTL_SECONDS

    await storage.delete_retry_count(redis, "alpha", "PR-300")
    assert key not in redis.values


def test_current_run_started_at_key_format() -> None:
    assert (
        storage.current_run_started_at_key("alpha", "PR-318")
        == "current_run_started_at:alpha:PR-318"
    )


async def test_current_run_started_at_record_uses_now_by_default() -> None:
    redis = _FakeRedis()
    before = datetime.now(timezone.utc)
    await storage.record_current_run_started_at(redis, "alpha", "PR-318")
    after = datetime.now(timezone.utc)

    key = storage.current_run_started_at_key("alpha", "PR-318")
    assert key in redis.values
    parsed = datetime.fromisoformat(redis.values[key])
    assert before <= parsed <= after
    assert redis.ttls[key] == TTL_SECONDS


async def test_current_run_started_at_record_honors_explicit_timestamp() -> None:
    redis = _FakeRedis()
    ts = datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc)
    await storage.record_current_run_started_at(redis, "alpha", "PR-318", ts)

    key = storage.current_run_started_at_key("alpha", "PR-318")
    assert redis.values[key] == ts.isoformat()


async def test_current_run_started_at_get_round_trip() -> None:
    redis = _FakeRedis()
    ts = datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc)
    await storage.record_current_run_started_at(redis, "alpha", "PR-318", ts)

    fetched = await storage.get_current_run_started_at(redis, "alpha", "PR-318")
    assert fetched == ts


async def test_current_run_started_at_get_missing_returns_none() -> None:
    redis = _FakeRedis()
    assert (
        await storage.get_current_run_started_at(redis, "alpha", "PR-MISSING")
        is None
    )


async def test_current_run_started_at_get_malformed_returns_none() -> None:
    redis = _FakeRedis()
    key = storage.current_run_started_at_key("alpha", "PR-318")
    redis.values[key] = "not-a-timestamp"
    assert (
        await storage.get_current_run_started_at(redis, "alpha", "PR-318")
        is None
    )


async def test_current_run_started_at_get_naive_iso_normalized_to_utc() -> None:
    redis = _FakeRedis()
    key = storage.current_run_started_at_key("alpha", "PR-318")
    redis.values[key] = "2026-05-04T12:00:00"

    fetched = await storage.get_current_run_started_at(redis, "alpha", "PR-318")
    assert fetched == datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc)


async def test_current_run_started_at_get_decodes_bytes() -> None:
    class _BytesRedis(_FakeRedis):
        async def get(self, key: str) -> bytes | None:
            raw = self.values.get(key)
            return raw.encode("utf-8") if raw is not None else None

    redis = _BytesRedis()
    ts = datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc)
    await storage.record_current_run_started_at(redis, "alpha", "PR-318", ts)

    fetched = await storage.get_current_run_started_at(redis, "alpha", "PR-318")
    assert fetched == ts


async def test_current_run_started_at_delete_drops_key() -> None:
    redis = _FakeRedis()
    await storage.record_current_run_started_at(redis, "alpha", "PR-318")
    key = storage.current_run_started_at_key("alpha", "PR-318")
    assert key in redis.values

    await storage.delete_current_run_started_at(redis, "alpha", "PR-318")
    assert key not in redis.values


async def test_multi_repo_isolation() -> None:
    redis = _FakeRedis()
    await record_cancellation_cause(
        redis,
        "alpha",
        "PR-500",
        CancellationCause(
            category="TIMEOUT",
            payload={"limit": "wallclock"},
            created_at="2026-05-04T09:00:00+00:00",
        ),
    )
    await record_cancellation_cause(
        redis,
        "beta",
        "PR-500",
        CancellationCause(
            category="ESCALATE",
            payload={"actor": "alice"},
            created_at="2026-05-04T09:05:00+00:00",
        ),
    )

    alpha = await get_cancellation_cause(redis, "alpha", "PR-500")
    beta = await get_cancellation_cause(redis, "beta", "PR-500")
    assert alpha is not None and alpha.category == "TIMEOUT"
    assert beta is not None and beta.category == "ESCALATE"
    assert alpha.repo_slug == "alpha"
    assert beta.repo_slug == "beta"


async def test_list_recent_filters_by_timestamp_and_orders_newest_first() -> None:
    redis = _FakeRedis()
    base = datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc)
    timestamps = [base - timedelta(hours=2), base - timedelta(minutes=15), base]
    for idx, ts in enumerate(timestamps):
        await record_cancellation_cause(
            redis,
            "alpha",
            f"PR-{600 + idx}",
            CancellationCause(category="CRASH", created_at=ts.isoformat()),
        )

    since = base - timedelta(minutes=30)
    recent = await list_recent_cancellations(redis, "alpha", since)
    assert [c.task_id for c in recent] == ["PR-602", "PR-601"]


async def test_list_recent_returns_empty_when_no_index() -> None:
    redis = _FakeRedis()
    since = datetime(2026, 5, 4, tzinfo=timezone.utc)
    assert await list_recent_cancellations(redis, "ghost", since) == []


async def test_list_recent_skips_expired_payloads_and_prunes_index() -> None:
    redis = _FakeRedis()
    created = datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc)
    await record_cancellation_cause(
        redis,
        "alpha",
        "PR-700",
        CancellationCause(category="CRASH", created_at=created.isoformat()),
    )
    redis.values.pop(cause_key("alpha", "PR-700"))

    recent = await list_recent_cancellations(
        redis, "alpha", created - timedelta(hours=1)
    )
    assert recent == []
    # Stale member must be removed from the index so list work stays bounded.
    assert "PR-700" not in redis.zsets.get(index_key("alpha"), {})


async def test_record_prunes_index_entries_older_than_ttl() -> None:
    redis = _FakeRedis()
    now = datetime.now(timezone.utc)
    ancient_ts = (now - timedelta(seconds=TTL_SECONDS + 3600)).timestamp()
    redis.zsets[index_key("alpha")] = {"PR-EXPIRED": ancient_ts}

    await record_cancellation_cause(
        redis,
        "alpha",
        "PR-FRESH",
        CancellationCause(category="INFRA", created_at=now.isoformat()),
    )

    bucket = redis.zsets[index_key("alpha")]
    assert "PR-EXPIRED" not in bucket
    assert "PR-FRESH" in bucket


async def test_list_recent_orders_by_parsed_timestamp_across_offsets() -> None:
    redis = _FakeRedis()
    # Same UTC instant, expressed with different offsets — string sort would
    # misorder these even though Redis scores rank them correctly.
    earlier_utc = "2026-05-04T12:00:00+00:00"  # 12:00 UTC
    later_minus5 = "2026-05-04T08:30:00-05:00"  # 13:30 UTC
    await record_cancellation_cause(
        redis,
        "alpha",
        "PR-EARLY",
        CancellationCause(category="CRASH", created_at=earlier_utc),
    )
    await record_cancellation_cause(
        redis,
        "alpha",
        "PR-LATER",
        CancellationCause(category="CRASH", created_at=later_minus5),
    )

    since = datetime(2026, 5, 4, tzinfo=timezone.utc)
    recent = await list_recent_cancellations(redis, "alpha", since)
    assert [c.task_id for c in recent] == ["PR-LATER", "PR-EARLY"]


async def test_record_sets_ttl_on_index_key() -> None:
    redis = _FakeRedis()
    await record_cancellation_cause(
        redis,
        "alpha",
        "PR-TTL",
        CancellationCause(category="INFRA"),
    )
    assert redis.ttls[index_key("alpha")] == TTL_SECONDS


async def test_list_recent_decodes_bytes_task_ids() -> None:
    redis = _FakeRedis()
    created = datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc)
    await record_cancellation_cause(
        redis,
        "alpha",
        "PR-800",
        CancellationCause(category="ESCALATE", created_at=created.isoformat()),
    )

    async def _bytes_zrange(key: str, min_score, max_score) -> list[bytes]:
        return [b"PR-800"]

    redis.zrangebyscore = _bytes_zrange  # type: ignore[assignment]

    recent = await list_recent_cancellations(
        redis, "alpha", created - timedelta(hours=1)
    )
    assert [c.task_id for c in recent] == ["PR-800"]


async def test_ttl_set_to_thirty_days() -> None:
    redis = _FakeRedis()
    await record_cancellation_cause(
        redis,
        "alpha",
        "PR-900",
        CancellationCause(category="INFRA"),
    )
    assert TTL_SECONDS == 30 * 24 * 3600
    assert redis.ttls[cause_key("alpha", "PR-900")] == TTL_SECONDS


def test_module_reexports_match_storage() -> None:
    from src import cancellation

    assert cancellation.CancellationCause is storage.CancellationCause
    assert cancellation.record_cancellation_cause is storage.record_cancellation_cause
    assert cancellation.get_cancellation_cause is storage.get_cancellation_cause
    assert cancellation.list_recent_cancellations is storage.list_recent_cancellations


@pytest.mark.parametrize(
    "raw",
    [
        b'{"category":"CRASH","payload":{},"created_at":"","task_id":"","repo_slug":""}',
        '{"category":"CRASH","payload":{},"created_at":"","task_id":"","repo_slug":""}',
    ],
)
def test_from_redis_accepts_bytes_or_str(raw) -> None:
    cause = CancellationCause.from_redis(raw)
    assert cause.category == "CRASH"


# PR-345: TTL-refresh-on-read for the 90-day forensic window.


def _seed_cause_with_ttl(
    redis: _FakeRedis,
    repo: str,
    pr_id: str,
    *,
    created_at: str,
    ttl: int = TTL_SECONDS,
) -> CancellationCause:
    cause = CancellationCause(
        category="ERROR",
        payload={"subsource": "fix_iteration_cap"},
        created_at=created_at,
        task_id=pr_id,
        repo_slug=repo,
    )
    redis.values[cause_key(repo, pr_id)] = cause.to_redis()
    redis.ttls[cause_key(repo, pr_id)] = ttl
    score = datetime.fromisoformat(created_at).timestamp()
    redis.zsets.setdefault(index_key(repo), {})[pr_id] = score
    redis.ttls[index_key(repo)] = ttl
    return cause


async def test_read_refreshes_ttl_when_default_true() -> None:
    redis = _FakeRedis()
    _seed_cause_with_ttl(
        redis,
        "alpha",
        "PR-300",
        created_at="2026-05-04T10:00:00+00:00",
        ttl=TTL_SECONDS,
    )

    fetched = await get_cancellation_cause(redis, "alpha", "PR-300")

    assert fetched is not None
    assert redis.ttls[cause_key("alpha", "PR-300")] == READ_REFRESH_TTL_SECONDS
    assert READ_REFRESH_TTL_SECONDS == 90 * 24 * 3600


async def test_read_does_not_refresh_when_refresh_ttl_false() -> None:
    redis = _FakeRedis()
    _seed_cause_with_ttl(
        redis,
        "alpha",
        "PR-301",
        created_at="2026-05-04T10:00:00+00:00",
        ttl=TTL_SECONDS,
    )

    fetched = await get_cancellation_cause(
        redis, "alpha", "PR-301", refresh_ttl=False
    )

    assert fetched is not None
    assert redis.ttls[cause_key("alpha", "PR-301")] == TTL_SECONDS
    assert redis.ttls[index_key("alpha")] == TTL_SECONDS


async def test_read_refresh_extends_index_zset_ttl() -> None:
    redis = _FakeRedis()
    _seed_cause_with_ttl(
        redis,
        "alpha",
        "PR-302",
        created_at="2026-05-04T10:00:00+00:00",
        ttl=TTL_SECONDS,
    )

    await get_cancellation_cause(redis, "alpha", "PR-302")

    assert redis.ttls[index_key("alpha")] == READ_REFRESH_TTL_SECONDS


async def test_read_refresh_does_not_change_canceled_at_score() -> None:
    redis = _FakeRedis()
    cause = _seed_cause_with_ttl(
        redis,
        "alpha",
        "PR-303",
        created_at="2026-05-04T10:00:00+00:00",
        ttl=TTL_SECONDS,
    )
    original_score = datetime.fromisoformat(cause.created_at).timestamp()

    await get_cancellation_cause(redis, "alpha", "PR-303")

    assert await redis.zscore(index_key("alpha"), "PR-303") == original_score


async def test_read_returns_none_for_missing_key_without_issuing_expire() -> None:
    redis = _FakeRedis()
    expire_calls: list[tuple[str, int]] = []
    original_expire = redis.expire

    async def _tracking_expire(key: str, seconds: int) -> bool:
        expire_calls.append((key, seconds))
        return await original_expire(key, seconds)

    redis.expire = _tracking_expire  # type: ignore[assignment]

    assert await get_cancellation_cause(redis, "alpha", "PR-404") is None
    assert expire_calls == []


async def test_read_handles_concurrent_refresh_safely() -> None:
    import asyncio

    redis = _FakeRedis()
    cause = _seed_cause_with_ttl(
        redis,
        "alpha",
        "PR-305",
        created_at="2026-05-04T10:00:00+00:00",
        ttl=TTL_SECONDS,
    )
    original_score = datetime.fromisoformat(cause.created_at).timestamp()

    results = await asyncio.gather(
        *(get_cancellation_cause(redis, "alpha", "PR-305") for _ in range(10))
    )

    assert all(r is not None and r.task_id == "PR-305" for r in results)
    assert redis.ttls[cause_key("alpha", "PR-305")] == READ_REFRESH_TTL_SECONDS
    assert redis.ttls[index_key("alpha")] == READ_REFRESH_TTL_SECONDS
    assert redis.zsets[index_key("alpha")] == {"PR-305": original_score}
    assert CancellationCause.from_redis(
        redis.values[cause_key("alpha", "PR-305")]
    ) == cause


async def test_read_refresh_skips_index_when_created_at_unparseable() -> None:
    redis = _FakeRedis()
    bad_cause = CancellationCause(
        category="ERROR",
        payload={"subsource": "crash"},
        created_at="not-a-timestamp",
        task_id="PR-306",
        repo_slug="alpha",
    )
    redis.values[cause_key("alpha", "PR-306")] = bad_cause.to_redis()
    redis.ttls[cause_key("alpha", "PR-306")] = TTL_SECONDS

    fetched = await get_cancellation_cause(redis, "alpha", "PR-306")

    assert fetched is not None
    # The cause-key TTL was still bumped (best-effort) but the index is left
    # alone because the score cannot be reconstructed from a malformed
    # created_at; a stale score would mis-order the record.
    assert redis.ttls[cause_key("alpha", "PR-306")] == READ_REFRESH_TTL_SECONDS
    assert index_key("alpha") not in redis.zsets


async def test_list_recent_does_not_refresh_underlying_ttls() -> None:
    """Internal scan helper passes refresh_ttl=False to avoid amplification."""
    redis = _FakeRedis()
    base = datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc)
    await record_cancellation_cause(
        redis,
        "alpha",
        "PR-307",
        CancellationCause(
            category="ERROR",
            payload={"subsource": "crash"},
            created_at=base.isoformat(),
        ),
    )
    assert redis.ttls[cause_key("alpha", "PR-307")] == TTL_SECONDS

    await list_recent_cancellations(
        redis, "alpha", base - timedelta(hours=1)
    )

    assert redis.ttls[cause_key("alpha", "PR-307")] == TTL_SECONDS
    assert redis.ttls[index_key("alpha")] == TTL_SECONDS
