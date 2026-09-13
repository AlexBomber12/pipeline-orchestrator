"""Run the connected Reject scenario against an isolated local Redis container.

Usage: .venv/bin/python tests-manual/rejection/verify_redis.py
No existing Redis address is accepted. The container has no network, no TCP
listener, no persistence, and only a temporary directory mounted at /test.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest
from redis.asyncio import Redis
from src.approval_commands import build_approval, enqueue_approval
from src.rejection_commands import enqueue_rejection, load_rejection
from src.task_attempts import AttemptChanged
from tests.test_approval_commands import isolated_daemon_process_view
from tests.test_rejection_commands import rejected, test_connected_http_reject_rewrite_clean_base_and_new_pr


async def exercise(socket: Path, root: Path):
    async with Redis(unix_socket_path=str(socket), decode_responses=True) as redis:
        for single in (False, True):
            await redis.flushall()
            directory = root / str(single)
            directory.mkdir()
            with pytest.MonkeyPatch.context() as patch:
                isolated_daemon_process_view.__wrapped__(patch)
                fixture = await rejected.__wrapped__(directory, patch)
                runner, command, repo, _, _, app = fixture
                for key, value in runner.redis.store.items():
                    await redis.set(key, value)
                runner.redis = app.state.redis = redis
                # Concurrent duplicate deliveries return the same operation.
                first, replay = await asyncio.gather(
                    enqueue_rejection(redis, command),
                    enqueue_rejection(redis, command),
                )
                assert first.binding == replay.binding
                await test_connected_http_reject_rewrite_clean_base_and_new_pr(fixture, patch, single)
        # Race actual approval application against Reject acceptance. A lost
        # decision either fails its CAS, or is superseded before execution.
        await redis.flushall()
        directory = root / "race"
        directory.mkdir()
        with pytest.MonkeyPatch.context() as patch:
            isolated_daemon_process_view.__wrapped__(patch)
            fixture = await rejected.__wrapped__(directory, patch)
            runner, command, repo, _, _, app = fixture
            for key, value in runner.redis.store.items():
                await redis.set(key, value)
            runner.redis = app.state.redis = redis
            approval = build_approval(runner.name, runner.state, command.failure, repo)
            await enqueue_approval(redis, approval)
            outcomes = await asyncio.gather(
                runner._commit_approval_state(approval),
                enqueue_rejection(redis, command),
                return_exceptions=True,
            )
            stored = await load_rejection(redis, runner.name, command.binding)
            if stored:
                assert isinstance(outcomes[0], Exception)
                assert await runner._attempt_execution_blocked()
            else:
                assert isinstance(outcomes[1], AttemptChanged)
                assert outcomes[0] is None
        print(
            "Isolated Redis: connected scenario passed with both feature settings; "
            "duplicate and Approve/Reject CAS races passed."
        )


def main():
    with tempfile.TemporaryDirectory(prefix="reject-redis-") as directory:
        root = Path(directory)
        name = f"po-reject-test-{uuid.uuid4().hex[:12]}"
        subprocess.run(
            [
                "docker",
                "run",
                "--detach",
                "--rm",
                "--name",
                name,
                "--network",
                "none",
                "--user",
                f"{os.getuid()}:{os.getgid()}",
                "--volume",
                f"{root}:/test",
                "redis:7-alpine",
                "redis-server",
                "--port",
                "0",
                "--save",
                "",
                "--appendonly",
                "no",
                "--unixsocket",
                "/test/redis.sock",
                "--unixsocketperm",
                "700",
            ],
            check=True,
            capture_output=True,
        )
        try:
            import time

            for _ in range(100):
                if (root / "redis.sock").exists():
                    break
                time.sleep(0.05)
            else:
                raise RuntimeError("Isolated Redis socket did not become ready")
            asyncio.run(exercise(root / "redis.sock", root))
        finally:
            subprocess.run(["docker", "stop", name], check=True, capture_output=True)


if __name__ == "__main__":
    main()
