"""Unit tests for durable Retry command storage and idempotency."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from src.cancellation import retry_count_key
from src.keyspace import (
    retry_command,
    retry_command_dedupe,
    retry_command_pending,
)
from src.retry_commands import (
    COMMAND_TTL_SECONDS,
    PROCESSING_LEASE_SECONDS,
    RETRY_COUNT_TTL_SECONDS,
    TRANSITION_HISTORY_LIMIT,
    RetryCapReached,
    RetryCommandStatus,
    RetryEffectStage,
    _append_transition,
    _decode_count,
    _decode_text,
    _parse,
    claim_retry_command,
    enqueue_retry_command,
    list_pending_retry_commands,
    load_latest_retry_command,
    load_retry_command,
    load_retry_command_for_binding,
    new_retry_command,
    reserve_retry_attempt,
    retry_request_binding,
    update_retry_command,
)

from tests.runner._helpers import _FakeRedis

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _new(*, binding: str = "b" * 64):
    return new_retry_command(
        repo_slug="octo__demo",
        task_id="PR-42",
        task_file="tasks/PR-42.md",
        task_branch="fix/pr-42",
        task_fingerprint="f" * 64,
        request_binding=binding,
        failure_id="e" * 64,
        retry_cap=3,
        failure_subsource="crash",
        failure_created_at=NOW.isoformat(),
        bound_pr_number=42,
        bound_pr_branch="fix/pr-42",
        bound_pr_head_sha="abc123",
        now=NOW,
    )


def test_binding_is_stable_and_sensitive_to_every_identity_field() -> None:
    values = {
        "repo_slug": "octo__demo",
        "task_id": "PR-42",
        "task_file": "tasks/PR-42.md",
        "task_branch": "fix/pr-42",
        "task_fingerprint": "f" * 64,
        "failure_id": "e" * 64,
        "retry_ordinal": 1,
        "pr_number": 42,
        "pr_branch": "fix/pr-42",
        "pr_head_sha": "abc123",
    }
    original = retry_request_binding(**values)
    assert original == retry_request_binding(**values)
    assert len(original) == 64
    for field, replacement in {
        "repo_slug": "other",
        "task_id": "PR-43",
        "task_file": "tasks/other.md",
        "task_branch": "fix/other",
        "task_fingerprint": "0" * 64,
        "failure_id": "1" * 64,
        "retry_ordinal": 2,
        "pr_number": 43,
        "pr_branch": "fix/other-pr",
        "pr_head_sha": "def456",
    }.items():
        changed = {**values, field: replacement}
        assert retry_request_binding(**changed) != original


def test_new_command_records_full_binding_and_initial_history() -> None:
    command = _new()
    assert command.command_id
    assert command.status == RetryCommandStatus.QUEUED
    assert command.requested_at == NOW
    assert command.updated_at == NOW
    assert command.bound_pr_number == 42
    assert command.history[0].status == RetryCommandStatus.QUEUED


def test_decode_and_parse_helpers_are_defensive() -> None:
    assert _decode_text(None) is None
    assert _decode_text(b"hello") == "hello"
    assert _decode_text(3) == "3"
    assert _decode_count(None) == 0
    assert _decode_count(b"2") == 2
    assert _decode_count("-3") == 0
    assert _decode_count("bad") == 0
    assert _parse(None) is None
    assert _parse("not-json") is None


def test_transition_deduplication_and_history_bound() -> None:
    command = _new()
    _append_transition(
        command,
        status=RetryCommandStatus.PROCESSING,
        reason="claimed",
        continuation=None,
        now=NOW,
    )
    size = len(command.history)
    _append_transition(
        command,
        status=RetryCommandStatus.PROCESSING,
        reason="claimed",
        continuation=None,
        now=NOW,
    )
    assert len(command.history) == size
    for offset in range(TRANSITION_HISTORY_LIMIT + 2):
        _append_transition(
            command,
            status=RetryCommandStatus.DEFERRED,
            reason=str(offset),
            continuation=None,
            now=NOW + timedelta(seconds=offset + 1),
        )
    assert len(command.history) == TRANSITION_HISTORY_LIMIT
    assert command.history[-1].reason == str(TRANSITION_HISTORY_LIMIT + 1)


@pytest.mark.asyncio
async def test_enqueue_load_and_duplicate_are_atomic() -> None:
    redis = _FakeRedis()
    command = _new()
    stored, created = await enqueue_retry_command(redis, command)
    duplicate, duplicate_created = await enqueue_retry_command(redis, _new())

    assert stored.command_id == command.command_id
    assert created is True
    assert duplicate.command_id == command.command_id
    assert duplicate_created is False
    assert await load_retry_command(redis, "octo__demo", command.command_id) == command
    assert await load_retry_command_for_binding(redis, "octo__demo", "b" * 64) == command
    assert await load_latest_retry_command(redis, "octo__demo", "PR-42") == command
    assert redis.ttls[retry_command("octo__demo", command.command_id)] == COMMAND_TTL_SECONDS
    assert redis.ttls[retry_command_pending("octo__demo")] == COMMAND_TTL_SECONDS


@pytest.mark.asyncio
async def test_enqueue_recovers_stale_dedupe_pointer_and_missing_loads() -> None:
    redis = _FakeRedis()
    redis.store[retry_command_dedupe("octo__demo", "b" * 64)] = "missing"
    command = _new()
    stored, created = await enqueue_retry_command(redis, command)
    assert created is True
    assert stored.command_id == command.command_id
    assert await load_retry_command_for_binding(redis, "octo__demo", "x" * 64) is None
    assert await load_latest_retry_command(redis, "octo__demo", "missing") is None


@pytest.mark.asyncio
async def test_pending_list_prunes_stale_payloads_and_decodes_bytes() -> None:
    redis = _FakeRedis()
    command = _new()
    await enqueue_retry_command(redis, command)
    pending_key = retry_command_pending("octo__demo")
    redis.zsets[pending_key]["missing"] = NOW.timestamp() - 1
    original = redis.zrangebyscore

    async def byte_ids(key: str, low: object, high: object):
        return [item.encode() for item in await original(key, low, high)]

    redis.zrangebyscore = byte_ids  # type: ignore[method-assign]
    commands = await list_pending_retry_commands(redis, "octo__demo")
    assert [item.command_id for item in commands] == [command.command_id]
    assert "missing" not in redis.zsets[pending_key]

    async def includes_none(key: str, low: object, high: object):
        return [None]

    redis.zrangebyscore = includes_none  # type: ignore[method-assign]
    assert await list_pending_retry_commands(redis, "octo__demo") == []


@pytest.mark.asyncio
async def test_claim_lifecycle_and_processing_lease() -> None:
    redis = _FakeRedis()
    command = _new()
    await enqueue_retry_command(redis, command)
    claimed = await claim_retry_command(
        redis, "octo__demo", command.command_id, "worker-a", now=NOW
    )
    assert claimed is not None
    assert claimed.status == RetryCommandStatus.PROCESSING
    assert claimed.processing_attempts == 1
    assert (
        await claim_retry_command(
            redis,
            "octo__demo",
            command.command_id,
            "worker-b",
            now=NOW + timedelta(seconds=PROCESSING_LEASE_SECONDS - 1),
        )
        is None
    )
    reclaimed = await claim_retry_command(
        redis,
        "octo__demo",
        command.command_id,
        "worker-b",
        now=NOW + timedelta(seconds=PROCESSING_LEASE_SECONDS),
    )
    assert reclaimed is not None
    assert reclaimed.processing_owner == "worker-b"
    assert reclaimed.processing_attempts == 2


@pytest.mark.asyncio
async def test_claim_handles_missing_failed_and_applied_records() -> None:
    redis = _FakeRedis()
    pending_key = retry_command_pending("octo__demo")
    redis.zsets[pending_key] = {"missing": NOW.timestamp()}
    assert (
        await claim_retry_command(redis, "octo__demo", "missing", "worker", now=NOW)
        is None
    )
    assert not redis.zsets[pending_key]

    command = _new()
    await enqueue_retry_command(redis, command)
    key = retry_command("octo__demo", command.command_id)
    command.status = RetryCommandStatus.FAILED
    redis.store[key] = command.model_dump_json()
    assert (
        await claim_retry_command(
            redis, "octo__demo", command.command_id, "worker", now=NOW
        )
        is None
    )
    command.status = RetryCommandStatus.APPLIED
    redis.store[key] = command.model_dump_json()
    assert (
        await claim_retry_command(
            redis, "octo__demo", command.command_id, "worker", now=NOW
        )
        == command
    )


@pytest.mark.asyncio
async def test_update_records_transition_and_pending_membership() -> None:
    redis = _FakeRedis()
    command = _new()
    await enqueue_retry_command(redis, command)

    def defer(current):
        current.status = RetryCommandStatus.DEFERRED

    updated = await update_retry_command(
        redis,
        "octo__demo",
        command.command_id,
        defer,
        keep_pending=True,
        transition_reason="waiting",
        now=NOW + timedelta(seconds=1),
    )
    assert updated is not None
    assert updated.outcome_reason == "waiting"
    assert updated.history[-1].reason == "waiting"

    removed = await update_retry_command(
        redis,
        "octo__demo",
        command.command_id,
        lambda current: setattr(current, "status", RetryCommandStatus.FAILED),
        keep_pending=False,
        now=NOW + timedelta(seconds=2),
    )
    assert removed is not None
    assert command.command_id not in redis.zsets[retry_command_pending("octo__demo")]
    assert (
        await update_retry_command(
            redis,
            "octo__demo",
            "missing",
            lambda current: None,
            keep_pending=False,
        )
        is None
    )


@pytest.mark.asyncio
async def test_retry_allowance_is_reserved_exactly_once() -> None:
    redis = _FakeRedis()
    command = _new()
    await enqueue_retry_command(redis, command)
    first = await reserve_retry_attempt(
        redis, "octo__demo", command.command_id, 3, now=NOW
    )
    second = await reserve_retry_attempt(
        redis,
        "octo__demo",
        command.command_id,
        3,
        now=NOW + timedelta(seconds=1),
    )
    count_key = retry_count_key("octo__demo", "PR-42")
    assert first.retry_count == second.retry_count == 1
    assert first.effect_stage == RetryEffectStage.RETRY_RESERVED
    assert redis.store[count_key] == "1"
    assert redis.ttls[count_key] == RETRY_COUNT_TTL_SECONDS


@pytest.mark.asyncio
async def test_retry_allowance_handles_bad_count_cap_and_missing_command() -> None:
    redis = _FakeRedis()
    command = _new()
    await enqueue_retry_command(redis, command)
    count_key = retry_count_key("octo__demo", "PR-42")
    redis.store[count_key] = "bad"
    reserved = await reserve_retry_attempt(redis, "octo__demo", command.command_id, 3)
    assert reserved.retry_count == 1

    other = _new(binding="c" * 64)
    other.command_id = "at-cap"
    await enqueue_retry_command(redis, other)
    redis.store[count_key] = "3"
    with pytest.raises(RetryCapReached) as cap:
        await reserve_retry_attempt(redis, "octo__demo", other.command_id, 3)
    assert (cap.value.current, cap.value.cap) == (3, 3)

    with pytest.raises(RuntimeError, match="disappeared"):
        await reserve_retry_attempt(redis, "octo__demo", "missing", 3)

    class _VanishingRedis(_FakeRedis):
        async def transaction(self, func, *watches, **kwargs):
            self.store.pop(retry_command("octo__demo", other.command_id), None)
            return await super().transaction(func, *watches, **kwargs)

    vanishing = _VanishingRedis()
    vanishing.store = dict(redis.store)
    with pytest.raises(RuntimeError, match="disappeared"):
        await reserve_retry_attempt(vanishing, "octo__demo", other.command_id, 3)
