"""Retry store replay uses real transactions in the existing isolated stack."""

import asyncio
import subprocess
import uuid

import redis.asyncio as aioredis
from src.cancellation import retry_count_key
from src.keyspace import retry_command_pending
from src.retry_commands import (
    RetryCommandStatus,
    claim_retry_command,
    enqueue_retry_command,
    list_pending_retry_commands,
    load_retry_command_for_binding,
    new_retry_command,
    reserve_retry_attempt,
    update_retry_command,
)


def test_retry_replay_is_atomic_before_and_after_apply() -> None:
    address = subprocess.run(
        ["docker", "inspect", "--format",
         "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
         "pipeline-orchestrator-redis-test"],
        check=True, capture_output=True, text=True, timeout=10,
    ).stdout.strip()
    assert address

    async def exercise() -> None:
        redis = aioredis.Redis(host=address, port=6379, db=15, decode_responses=True)
        slug = f"retry-replay__{uuid.uuid4().hex}"
        binding = "b" * 64

        def candidate():
            return new_retry_command(
                repo_slug=slug, task_id="PR-412", task_file="tasks/PR-412.md",
                task_branch="pr-412", task_fingerprint="f" * 64,
                request_binding=binding, failure_id="e" * 64, retry_cap=3,
            )

        try:
            await redis.ping()
            # Race two deliveries of a request whose HTTP response may be lost.
            deliveries = await asyncio.gather(*(
                enqueue_retry_command(redis, candidate()) for _ in range(8)
            ))
            ids = {command.command_id for command, _ in deliveries}
            assert len(ids) == 1
            assert sum(created for _, created in deliveries) == 1
            command_id = ids.pop()
            assert [c.command_id for c in await list_pending_retry_commands(redis, slug)] == [command_id]
            claims = await asyncio.gather(*(
                claim_retry_command(redis, slug, command_id, f"runner-{index}")
                for index in range(8)
            ))
            assert sum(claim is not None for claim in claims) == 1
            reservations = await asyncio.gather(*(
                reserve_retry_attempt(redis, slug, command_id, 3) for _ in range(8)
            ))
            assert {command.retry_count for command in reservations} == {1}
            applied = await update_retry_command(
                redis, slug, command_id,
                lambda command: setattr(command, "status", RetryCommandStatus.APPLIED),
                keep_pending=False,
            )
            # Exact re-delivery after apply returns the original durable receipt.
            replays = await asyncio.gather(*(
                enqueue_retry_command(redis, candidate()) for _ in range(8)
            ))
            assert all(command == applied and not created for command, created in replays)
            assert await load_retry_command_for_binding(redis, slug, binding) == applied
            assert await load_retry_command_for_binding(redis, slug, "c" * 64) is None
            assert await reserve_retry_attempt(redis, slug, command_id, 3) == applied
            assert await claim_retry_command(redis, slug, command_id, "late-runner") == applied
            assert applied.processing_attempts == 1
            assert await redis.get(retry_count_key(slug, "PR-412")) == "1"
            assert await redis.zcard(retry_command_pending(slug)) == 0
            keys = [key async for key in redis.scan_iter(match=f"control:{slug}:retry:command:*")]
            assert len(keys) == 1
        finally:
            # Delete only this fixture's random namespace; never flush the shared DB.
            keys = [key async for key in redis.scan_iter(match=f"control:{slug}:retry:*")]
            keys.append(retry_count_key(slug, "PR-412"))
            await redis.delete(*keys)
            await redis.aclose()

    asyncio.run(exercise())
