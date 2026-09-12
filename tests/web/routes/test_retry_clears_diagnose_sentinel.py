"""Retry evidence ownership tests.

The web layer now preserves failure sentinels. The daemon clears them only
after it has accepted the durable command; daemon coverage lives in
``tests/runner/test_retry_commands.py``.
"""

from __future__ import annotations

import asyncio

from src.web.routes import repo_control

from tests.web.test_retry_endpoint import _WebRedis


def test_status_write_failed_marker_helper_defensive_branches() -> None:
    marker_key = "status_write_failed_tasks:example__alpha"

    redis_client = _WebRedis()
    redis_client.store[marker_key] = b'["PR-001"]'  # type: ignore[assignment]
    asyncio.run(
        repo_control._clear_status_write_failed_marker(
            redis_client,
            "example__alpha",
            "PR-001",
        )
    )
    assert marker_key not in redis_client.store

    for value in ("not-json", '{"task":"PR-001"}', '["PR-999"]'):
        redis_client = _WebRedis()
        redis_client.store[marker_key] = value
        asyncio.run(
            repo_control._clear_status_write_failed_marker(
                redis_client,
                "example__alpha",
                "PR-001",
            )
        )
        assert redis_client.store[marker_key] == value

    redis_client = _WebRedis()
    asyncio.run(
        repo_control._clear_status_write_failed_marker(
            redis_client,
            "example__alpha",
            "PR-001",
        )
    )
    assert marker_key not in redis_client.store
