"""PR-250: classify_ci_status_with_age + stuck-PENDING tracker tests.

Covers the wrapper that augments ``_map_rest_ci_status_to_enum`` with a
per-(repo, pr, sha) Redis-backed first-seen-PENDING anchor and rewrites
PENDING to FAILURE once ``daemon.ci_pending_max_min`` has elapsed.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from src.github import checks
from src.github.checks import (
    _clear_pending_tracker,
    _get_or_set_pending_first_seen,
    _pending_tracker_key,
    classify_ci_status_with_age,
)
from src.models import CIStatus


class _FakeRedis:
    """Minimal async Redis double supporting GET/SET NX EX/DELETE."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.set_calls: list[tuple[str, str, dict[str, Any]]] = []
        self.deleted: list[str] = []

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def set(
        self,
        key: str,
        value: str,
        ex: int | None = None,
        nx: bool = False,
    ) -> bool | None:
        self.set_calls.append((key, value, {"ex": ex, "nx": nx}))
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True

    async def delete(self, key: str) -> int:
        self.deleted.append(key)
        return int(self.store.pop(key, None) is not None)


class _FakeRedisWithServerTime(_FakeRedis):
    """``_FakeRedis`` extended with an awaitable ``time()`` method."""

    def __init__(self, server_time: float) -> None:
        super().__init__()
        self._server_time = server_time

    async def time(self) -> tuple[int, int]:
        seconds = int(self._server_time)
        microseconds = int((self._server_time - seconds) * 1_000_000)
        return seconds, microseconds


def _runs_pending() -> tuple[list[dict], dict]:
    """REST payloads that ``_map_rest_ci_status_to_enum`` reads as PENDING."""
    return [{"status": "in_progress"}], {}


def _runs_success() -> tuple[list[dict], dict]:
    return [{"conclusion": "success"}], {}


def test_pending_within_threshold_returns_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _FakeRedis()
    runs, statuses = _runs_pending()
    base = 1_700_000_000.0
    monkeypatch.setattr(checks.time, "time", lambda: base)
    asyncio.run(
        _get_or_set_pending_first_seen(
            redis, "octo/repo", 7, "sha-aaa", pending_max_seconds=1800
        )
    )
    monkeypatch.setattr(checks.time, "time", lambda: base + 5 * 60)

    status, reason = asyncio.run(
        classify_ci_status_with_age(
            "octo/repo",
            7,
            "sha-aaa",
            redis,
            pending_max_seconds=1800,
            runs_payload=runs,
            statuses_payload=statuses,
        )
    )

    assert status == CIStatus.PENDING
    assert reason is None


def test_pending_beyond_threshold_returns_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _FakeRedis()
    runs, statuses = _runs_pending()
    base = 1_700_000_000.0
    monkeypatch.setattr(checks.time, "time", lambda: base)
    asyncio.run(
        _get_or_set_pending_first_seen(
            redis, "octo/repo", 7, "sha-aaa", pending_max_seconds=1800
        )
    )
    monkeypatch.setattr(checks.time, "time", lambda: base + 35 * 60)

    status, reason = asyncio.run(
        classify_ci_status_with_age(
            "octo/repo",
            7,
            "sha-aaa",
            redis,
            pending_max_seconds=1800,
            runs_payload=runs,
            statuses_payload=statuses,
        )
    )

    assert status == CIStatus.FAILURE
    assert reason == "stuck_pending"


def test_status_transition_clears_tracker() -> None:
    """Raw status leaving PENDING drops the Redis anchor."""
    redis = _FakeRedis()
    key = _pending_tracker_key("octo/repo", 7, "sha-aaa")
    redis.store[key] = "1700000000.0"

    runs, statuses = _runs_success()
    status, reason = asyncio.run(
        classify_ci_status_with_age(
            "octo/repo",
            7,
            "sha-aaa",
            redis,
            pending_max_seconds=1800,
            runs_payload=runs,
            statuses_payload=statuses,
        )
    )

    assert status == CIStatus.SUCCESS
    assert reason is None
    assert key not in redis.store
    assert key in redis.deleted


def test_head_sha_rotation_resets_tracker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A new head_sha gets its own first-seen anchor untouched by the old SHA."""
    redis = _FakeRedis()
    runs, statuses = _runs_pending()
    base = 1_700_000_000.0
    # Old SHA: written 35 min ago — would be stuck if checked.
    old_key = _pending_tracker_key("octo/repo", 7, "sha-old")
    redis.store[old_key] = str(base - 35 * 60)

    monkeypatch.setattr(checks.time, "time", lambda: base)
    status, reason = asyncio.run(
        classify_ci_status_with_age(
            "octo/repo",
            7,
            "sha-new",
            redis,
            pending_max_seconds=1800,
            runs_payload=runs,
            statuses_payload=statuses,
        )
    )

    assert status == CIStatus.PENDING
    assert reason is None
    new_key = _pending_tracker_key("octo/repo", 7, "sha-new")
    assert new_key in redis.store
    # Old SHA tracker remains independent — TTL handles cleanup, not us.
    assert old_key in redis.store


def test_config_threshold_honored(monkeypatch: pytest.MonkeyPatch) -> None:
    """Same elapsed PENDING returns PENDING under a high threshold and FAILURE under a low one."""
    runs, statuses = _runs_pending()
    base = 1_700_000_000.0

    redis_high = _FakeRedis()
    monkeypatch.setattr(checks.time, "time", lambda: base)
    asyncio.run(
        _get_or_set_pending_first_seen(
            redis_high, "octo/repo", 7, "sha-aaa", pending_max_seconds=3600
        )
    )
    monkeypatch.setattr(checks.time, "time", lambda: base + 20 * 60)
    status_high, reason_high = asyncio.run(
        classify_ci_status_with_age(
            "octo/repo",
            7,
            "sha-aaa",
            redis_high,
            pending_max_seconds=3600,
            runs_payload=runs,
            statuses_payload=statuses,
        )
    )
    assert status_high == CIStatus.PENDING
    assert reason_high is None

    redis_low = _FakeRedis()
    monkeypatch.setattr(checks.time, "time", lambda: base)
    asyncio.run(
        _get_or_set_pending_first_seen(
            redis_low, "octo/repo", 7, "sha-aaa", pending_max_seconds=600
        )
    )
    monkeypatch.setattr(checks.time, "time", lambda: base + 20 * 60)
    status_low, reason_low = asyncio.run(
        classify_ci_status_with_age(
            "octo/repo",
            7,
            "sha-aaa",
            redis_low,
            pending_max_seconds=600,
            runs_payload=runs,
            statuses_payload=statuses,
        )
    )
    assert status_low == CIStatus.FAILURE
    assert reason_low == "stuck_pending"


def test_first_seen_uses_redis_server_time_when_available() -> None:
    """``redis.time()`` is preferred over the local clock for the first-seen anchor."""
    redis = _FakeRedisWithServerTime(server_time=1_700_000_500.5)
    first_seen = asyncio.run(
        _get_or_set_pending_first_seen(
            redis, "octo/repo", 7, "sha-aaa", pending_max_seconds=1800
        )
    )
    assert first_seen == pytest.approx(1_700_000_500.5)
    key = _pending_tracker_key("octo/repo", 7, "sha-aaa")
    assert redis.store[key] == "1700000500.5"
    # SET was issued with NX and a TTL of 2 * pending_max_seconds.
    assert redis.set_calls[-1][2] == {"ex": 3600, "nx": True}


def test_pending_with_unparseable_existing_value_overwrites(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A corrupted Redis value falls through to the SET path and recovers."""
    redis = _FakeRedis()
    key = _pending_tracker_key("octo/repo", 7, "sha-aaa")
    redis.store[key] = "not-a-float"
    monkeypatch.setattr(checks.time, "time", lambda: 1_700_000_000.0)

    first_seen = asyncio.run(
        _get_or_set_pending_first_seen(
            redis, "octo/repo", 7, "sha-aaa", pending_max_seconds=1800
        )
    )
    # NX prevents overwrite, but the corrupted value also fails the
    # post-SET re-read parse — fallback returns the now timestamp.
    assert first_seen == pytest.approx(1_700_000_000.0)


def test_post_set_reread_falls_back_when_value_disappears(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If something deletes the key between SET and re-GET, we still return now."""
    redis = _FakeRedis()
    monkeypatch.setattr(checks.time, "time", lambda: 1_700_000_000.0)

    real_set = redis.set

    async def evicting_set(*args: Any, **kwargs: Any) -> bool | None:
        result = await real_set(*args, **kwargs)
        # Simulate a TTL race that nukes the key right after the write.
        redis.store.pop(args[0], None)
        return result

    redis.set = evicting_set  # type: ignore[assignment]

    first_seen = asyncio.run(
        _get_or_set_pending_first_seen(
            redis, "octo/repo", 7, "sha-aaa", pending_max_seconds=1800
        )
    )
    assert first_seen == pytest.approx(1_700_000_000.0)


def test_classify_skips_when_redis_or_sha_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without a Redis client or head SHA the wrapper is a passthrough."""
    runs, statuses = _runs_pending()
    monkeypatch.setattr(checks.time, "time", lambda: 1_700_000_000.0)

    status_no_redis, reason_no_redis = asyncio.run(
        classify_ci_status_with_age(
            "octo/repo",
            7,
            "sha-aaa",
            None,
            pending_max_seconds=1800,
            runs_payload=runs,
            statuses_payload=statuses,
        )
    )
    assert status_no_redis == CIStatus.PENDING
    assert reason_no_redis is None

    status_no_sha, reason_no_sha = asyncio.run(
        classify_ci_status_with_age(
            "octo/repo",
            7,
            "",
            _FakeRedis(),
            pending_max_seconds=1800,
            runs_payload=runs,
            statuses_payload=statuses,
        )
    )
    assert status_no_sha == CIStatus.PENDING
    assert reason_no_sha is None

    status_zero_threshold, reason_zero_threshold = asyncio.run(
        classify_ci_status_with_age(
            "octo/repo",
            7,
            "sha-aaa",
            _FakeRedis(),
            pending_max_seconds=0,
            runs_payload=runs,
            statuses_payload=statuses,
        )
    )
    assert status_zero_threshold == CIStatus.PENDING
    assert reason_zero_threshold is None


def test_clear_pending_tracker_noops_without_client_or_sha() -> None:
    """Defensive: a no-op when called with no client or no SHA — no exceptions."""
    asyncio.run(_clear_pending_tracker(None, "octo/repo", 7, "sha"))
    asyncio.run(_clear_pending_tracker(_FakeRedis(), "octo/repo", 7, ""))


def test_redis_server_time_returns_none_for_missing_method() -> None:
    """Clients without ``time()`` (the in-test stub) fall back to the local clock."""

    class _NoTimeRedis:
        pass

    result = asyncio.run(checks._redis_server_time(_NoTimeRedis()))
    assert result is None


def test_redis_server_time_returns_none_for_malformed_response() -> None:
    """Malformed ``time()`` payloads fall back rather than crash."""

    class _BadRedis:
        async def time(self) -> Any:
            return None

    assert asyncio.run(checks._redis_server_time(_BadRedis())) is None

    class _ShortRedis:
        async def time(self) -> Any:
            return (123,)

    assert asyncio.run(checks._redis_server_time(_ShortRedis())) is None


def test_redis_server_time_accepts_sync_time_method() -> None:
    """A sync ``time()`` method (as some redis-py clients expose) is honored."""

    class _SyncTimeRedis:
        def time(self) -> tuple[int, int]:
            return 1_700_000_500, 750_000

    result = asyncio.run(checks._redis_server_time(_SyncTimeRedis()))
    assert result == pytest.approx(1_700_000_500.75)
