"""Tests for the PR-315 cancellation-cause category collapse migration."""

from __future__ import annotations

import json
import logging
from typing import Any

import pytest
from src.cancellation.storage import cause_key
from src.daemon.migrations.escalate_to_error import (
    migrate_escalate_to_error_on_startup,
)


class _FakeRedis:
    def __init__(
        self,
        values: dict[str | bytes, str],
        ttls: dict[Any, int] | None = None,
    ) -> None:
        self.values: dict[Any, str] = dict(values)
        self.ttls: dict[Any, int] = dict(ttls or {})
        self.set_calls: list[tuple[Any, str]] = []

    async def scan_iter(self, match: str):
        prefix = match.removesuffix("*")
        for key in list(self.values):
            decoded = key.decode("utf-8") if isinstance(key, bytes) else key
            if decoded.startswith(prefix):
                yield key

    async def get(self, key: Any) -> str | None:
        return self.values.get(key)

    async def set(
        self,
        key: Any,
        value: str,
        *,
        keepttl: bool = False,
        ex: int | None = None,
    ) -> bool:
        self.set_calls.append((key, value))
        self.values[key] = value
        if keepttl:
            pass  # preserve existing self.ttls[key] if any
        elif ex is not None:
            self.ttls[key] = ex
        else:
            self.ttls.pop(key, None)
        return True

    async def ttl(self, key: Any) -> int:
        if key not in self.values:
            return -2
        return self.ttls.get(key, -1)


class _SyncRedis:
    def __init__(self, values: dict[Any, str]) -> None:
        self.values: dict[Any, str] = dict(values)
        self.ttls: dict[Any, int] = {}
        self.set_calls: list[tuple[Any, str]] = []

    def scan_iter(self, match: str):
        prefix = match.removesuffix("*")
        for key in list(self.values):
            decoded = key.decode("utf-8") if isinstance(key, bytes) else key
            if decoded.startswith(prefix):
                yield key

    def get(self, key: Any) -> str | None:
        return self.values.get(key)

    def set(
        self,
        key: Any,
        value: str,
        *,
        keepttl: bool = False,
        ex: int | None = None,
    ) -> bool:
        self.set_calls.append((key, value))
        self.values[key] = value
        if keepttl:
            pass
        elif ex is not None:
            self.ttls[key] = ex
        else:
            self.ttls.pop(key, None)
        return True

    def ttl(self, key: Any) -> int:
        if key not in self.values:
            return -2
        return self.ttls.get(key, -1)


def _legacy_payload(category: str, payload: dict[str, Any]) -> str:
    return json.dumps(
        {
            "category": category,
            "payload": payload,
            "created_at": "2026-05-01T00:00:00+00:00",
            "task_id": f"PR-{category}",
            "repo_slug": "alpha",
        }
    )


async def test_migration_rewrites_legacy_categories() -> None:
    """All five legacy categories collapse to ERROR with subsource preserved."""
    seeded = {
        cause_key("alpha", "PR-CRASH"): _legacy_payload(
            "CRASH", {"error_message": "boom"}
        ),
        cause_key("alpha", "PR-ESCALATE"): _legacy_payload(
            "ESCALATE",
            {"subsource": "guardrail", "reason_text": "tier-1 violation"},
        ),
        cause_key("alpha", "PR-TIMEOUT"): _legacy_payload(
            "TIMEOUT",
            {"limit_type": "fix_idle", "duration_elapsed_sec": 600},
        ),
        cause_key("alpha", "PR-INFRA"): _legacy_payload(
            "INFRA",
            {"subsystem": "gh_api", "retry_count": 3},
        ),
        cause_key("alpha", "PR-NPD"): _legacy_payload(
            "NO_PUSH_DEADLOCK", {"attempts": 3, "pr_number": 9}
        ),
    }
    redis = _FakeRedis(seeded)

    migrated = await migrate_escalate_to_error_on_startup(
        redis, logging.getLogger(__name__)
    )

    assert migrated == 5
    for task_id, expected_legacy in (
        ("PR-CRASH", "CRASH"),
        ("PR-ESCALATE", "ESCALATE"),
        ("PR-TIMEOUT", "TIMEOUT"),
        ("PR-INFRA", "INFRA"),
        ("PR-NPD", "NO_PUSH_DEADLOCK"),
    ):
        record = json.loads(redis.values[cause_key("alpha", task_id)])
        assert record["category"] == "ERROR"
        assert record["payload"]["legacy_category"] == expected_legacy

    # Subsource on ESCALATE record was preserved alongside legacy_category.
    esc = json.loads(redis.values[cause_key("alpha", "PR-ESCALATE")])
    assert esc["payload"]["subsource"] == "guardrail"
    assert esc["payload"]["reason_text"] == "tier-1 violation"


async def test_migration_idempotent_on_already_migrated() -> None:
    """A second run on already-ERROR records performs no writes."""
    seeded = {
        cause_key("alpha", "PR-ALREADY"): json.dumps(
            {
                "category": "ERROR",
                "payload": {
                    "subsource": "crash",
                    "error_message": "boom",
                    "legacy_category": "CRASH",
                },
                "created_at": "2026-05-01T00:00:00+00:00",
                "task_id": "PR-ALREADY",
                "repo_slug": "alpha",
            }
        ),
    }
    redis = _FakeRedis(seeded)

    first = await migrate_escalate_to_error_on_startup(
        redis, logging.getLogger(__name__)
    )
    second = await migrate_escalate_to_error_on_startup(
        redis, logging.getLogger(__name__)
    )

    assert first == 0
    assert second == 0
    assert redis.set_calls == []


async def test_migration_idempotent_after_full_rewrite() -> None:
    """Running the migration twice in succession is a no-op on the second run."""
    seeded = {
        cause_key("alpha", "PR-CRASH"): _legacy_payload(
            "CRASH", {"error_message": "boom"}
        ),
    }
    redis = _FakeRedis(seeded)

    first = await migrate_escalate_to_error_on_startup(
        redis, logging.getLogger(__name__)
    )
    redis.set_calls.clear()
    second = await migrate_escalate_to_error_on_startup(
        redis, logging.getLogger(__name__)
    )

    assert first == 1
    assert second == 0
    assert redis.set_calls == []


async def test_migration_handles_missing_payload_dict() -> None:
    """Records where ``payload`` is missing or not a dict still migrate."""
    seeded = {
        cause_key("alpha", "PR-NO-PAYLOAD"): json.dumps(
            {
                "category": "CRASH",
                "payload": None,
                "created_at": "2026-05-01T00:00:00+00:00",
                "task_id": "PR-NO-PAYLOAD",
                "repo_slug": "alpha",
            }
        ),
    }
    redis = _FakeRedis(seeded)

    migrated = await migrate_escalate_to_error_on_startup(
        redis, logging.getLogger(__name__)
    )

    assert migrated == 1
    record = json.loads(redis.values[cause_key("alpha", "PR-NO-PAYLOAD")])
    assert record["category"] == "ERROR"
    assert record["payload"] == {"legacy_category": "CRASH"}


async def test_migration_skips_malformed_records(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Malformed JSON is logged and skipped without aborting the migration."""
    seeded = {
        cause_key("alpha", "PR-BAD"): "not valid json",
        cause_key("alpha", "PR-GOOD"): _legacy_payload("CRASH", {}),
    }
    redis = _FakeRedis(seeded)
    log = logging.getLogger("test_migration")

    with caplog.at_level(logging.WARNING, logger=log.name):
        migrated = await migrate_escalate_to_error_on_startup(redis, log)

    assert migrated == 1
    assert redis.values[cause_key("alpha", "PR-BAD")] == "not valid json"
    assert any(
        "Skipping malformed cancellation:* key" in rec.getMessage()
        for rec in caplog.records
    )


async def test_migration_logs_when_write_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A Redis write failure must be logged but not abort the migration."""

    class _BoomRedis(_FakeRedis):
        async def set(self, key: Any, value: str, **_: Any) -> bool:
            raise RuntimeError("redis down")

    seeded = {
        cause_key("alpha", "PR-CRASH"): _legacy_payload("CRASH", {}),
    }
    redis = _BoomRedis(seeded)
    log = logging.getLogger("test_migration_writes")

    with caplog.at_level(logging.WARNING, logger=log.name):
        migrated = await migrate_escalate_to_error_on_startup(redis, log)

    assert migrated == 0
    assert any(
        "Failed to rewrite" in rec.getMessage() for rec in caplog.records
    )


async def test_migration_supports_sync_redis_and_callable_logger() -> None:
    """Synchronous-style fake clients and a callable logger both work."""
    seeded = {
        cause_key("alpha", "PR-CRASH").encode("utf-8"): _legacy_payload(
            "CRASH", {"error_message": "boom"}
        ),
    }
    redis = _SyncRedis(seeded)
    messages: list[str] = []

    migrated = await migrate_escalate_to_error_on_startup(redis, messages.append)

    assert migrated == 1
    assert any("→ERROR" in message for message in messages)


async def test_migration_decodes_bytes_get_results() -> None:
    """Redis clients that return bytes from ``get`` are decoded inline."""

    class _BytesRedis(_FakeRedis):
        async def get(self, key: Any) -> bytes | None:
            value = self.values.get(key)
            return value.encode("utf-8") if value is not None else None

    seeded = {
        cause_key("alpha", "PR-CRASH"): _legacy_payload(
            "CRASH", {"error_message": "boom"}
        ),
    }
    redis = _BytesRedis(seeded)

    migrated = await migrate_escalate_to_error_on_startup(
        redis, logging.getLogger(__name__)
    )

    assert migrated == 1
    record = json.loads(redis.values[cause_key("alpha", "PR-CRASH")])
    assert record["category"] == "ERROR"


async def test_migration_skips_keys_with_none_value() -> None:
    """A race where the key vanished between scan and get must not abort."""

    class _NoneGetRedis(_FakeRedis):
        async def get(self, key: Any) -> str | None:
            return None

    seeded = {
        cause_key("alpha", "PR-GONE"): _legacy_payload("CRASH", {}),
    }
    redis = _NoneGetRedis(seeded)

    migrated = await migrate_escalate_to_error_on_startup(
        redis, logging.getLogger(__name__)
    )

    assert migrated == 0


async def test_migration_skips_non_object_payload() -> None:
    """A non-dict JSON top-level value (e.g. a list) must not be rewritten."""
    seeded = {
        cause_key("alpha", "PR-WEIRD"): json.dumps(["not", "an", "object"]),
    }
    redis = _FakeRedis(seeded)

    migrated = await migrate_escalate_to_error_on_startup(
        redis, logging.getLogger(__name__)
    )

    assert migrated == 0
    assert redis.set_calls == []


async def test_migration_preserves_ttl_via_keepttl() -> None:
    """Migration must not reset the TTL on rewritten cancellation:* records."""
    key = cause_key("alpha", "PR-CRASH")
    redis = _FakeRedis(
        {key: _legacy_payload("CRASH", {"error_message": "boom"})},
        ttls={key: 12345},
    )

    migrated = await migrate_escalate_to_error_on_startup(
        redis, logging.getLogger(__name__)
    )

    assert migrated == 1
    # The prior TTL is preserved verbatim — neither cleared nor reset to
    # the 30-day default that record_cancellation_cause would apply.
    assert redis.ttls[key] == 12345


async def test_migration_falls_back_to_explicit_ex_when_keepttl_unsupported() -> None:
    """Clients that reject ``keepttl=`` get the remaining TTL reapplied via ex=."""

    class _NoKeepTTLRedis(_FakeRedis):
        async def set(
            self,
            key: Any,
            value: str,
            *,
            ex: int | None = None,
        ) -> bool:
            return await super().set(key, value, ex=ex)

    key = cause_key("alpha", "PR-CRASH")
    redis = _NoKeepTTLRedis(
        {key: _legacy_payload("CRASH", {"error_message": "boom"})},
        ttls={key: 999},
    )

    migrated = await migrate_escalate_to_error_on_startup(
        redis, logging.getLogger(__name__)
    )

    assert migrated == 1
    # Fallback reapplied the remaining TTL exactly.
    assert redis.ttls[key] == 999


async def test_migration_fallback_logs_when_ttl_read_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """If keepttl is rejected and ttl() also fails, the record is skipped."""

    class _BrokenTTLRedis(_FakeRedis):
        async def set(self, key: Any, value: str) -> bool:
            return await super().set(key, value)

        async def ttl(self, key: Any) -> int:
            raise RuntimeError("ttl broken")

    key = cause_key("alpha", "PR-CRASH")
    redis = _BrokenTTLRedis(
        {key: _legacy_payload("CRASH", {})},
        ttls={key: 100},
    )
    log = logging.getLogger("test_migration_ttl_read")

    with caplog.at_level(logging.WARNING, logger=log.name):
        migrated = await migrate_escalate_to_error_on_startup(redis, log)

    assert migrated == 0
    assert any("Failed to read TTL" in rec.getMessage() for rec in caplog.records)


async def test_migration_fallback_writes_without_ex_when_no_ttl() -> None:
    """Records without a TTL stay without a TTL through the fallback path."""

    class _NoKeepTTLRedis(_FakeRedis):
        async def set(
            self,
            key: Any,
            value: str,
            *,
            ex: int | None = None,
        ) -> bool:
            return await super().set(key, value, ex=ex)

    key = cause_key("alpha", "PR-CRASH")
    redis = _NoKeepTTLRedis(
        {key: _legacy_payload("CRASH", {})},
        ttls={},  # no TTL on the key
    )

    migrated = await migrate_escalate_to_error_on_startup(
        redis, logging.getLogger(__name__)
    )

    assert migrated == 1
    assert key not in redis.ttls


async def test_migration_fallback_logs_when_set_with_ex_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A TTL-aware set failure in the fallback path must be logged."""

    class _NoKeepTTLBoom(_FakeRedis):
        async def set(
            self,
            key: Any,
            value: str,
            *,
            ex: int | None = None,
        ) -> bool:
            raise RuntimeError("redis down")

    key = cause_key("alpha", "PR-CRASH")
    redis = _NoKeepTTLBoom(
        {key: _legacy_payload("CRASH", {})},
        ttls={key: 50},
    )
    log = logging.getLogger("test_migration_fallback_write")

    with caplog.at_level(logging.WARNING, logger=log.name):
        migrated = await migrate_escalate_to_error_on_startup(redis, log)

    assert migrated == 0
    assert any("Failed to rewrite" in rec.getMessage() for rec in caplog.records)


async def test_migration_fallback_preserves_expiry_when_ttl_reports_zero() -> None:
    """A near-expiry TTL of 0 must not become a persistent (no-ex) record.

    Redis TTL is second-granularity, so a key within its final second of
    life reports ``0``. The pre-fix branch took the ``set(...)`` path
    without ``ex``, converting the soon-to-expire cancellation record
    into a permanent entry. The fallback now reapplies ``ex=1`` so the
    expiry is preserved.
    """

    class _NoKeepTTLRedis(_FakeRedis):
        async def set(
            self,
            key: Any,
            value: str,
            *,
            ex: int | None = None,
        ) -> bool:
            return await super().set(key, value, ex=ex)

    key = cause_key("alpha", "PR-NEAR-EXPIRY")
    redis = _NoKeepTTLRedis(
        {key: _legacy_payload("CRASH", {"error_message": "boom"})},
        ttls={key: 0},
    )

    migrated = await migrate_escalate_to_error_on_startup(
        redis, logging.getLogger(__name__)
    )

    assert migrated == 1
    # ex floor is 1 so the record retains an expiry rather than becoming
    # persistent.
    assert redis.ttls[key] == 1


async def test_migration_callable_logger_warns_for_malformed_key() -> None:
    """Callable loggers receive ``_warn`` messages alongside info messages."""
    seeded = {
        cause_key("alpha", "PR-BAD"): "not valid json",
    }
    redis = _FakeRedis(seeded)
    messages: list[str] = []

    migrated = await migrate_escalate_to_error_on_startup(redis, messages.append)

    assert migrated == 0
    assert any("Skipping malformed" in message for message in messages)
