"""Tests for the Redis bridge that keeps model discovery in the daemon."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from src import model_catalog_bridge as bridge
from src.coder_registry import (
    CoderRegistry,
    ModelCatalogUnavailable,
)
from src.coders.codex import CodexPlugin
from src.coders.codex_models import CodexModel, CodexReasoningEffort
from src.config import AppConfig


class _BridgeRedis:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[tuple[str, bytes]] = asyncio.Queue()
        self.values: dict[str, str] = {}
        self.deleted: list[str] = []

    async def rpush(self, key: str, value: str) -> None:
        await self.queue.put((key, value.encode()))

    async def blpop(
        self, key: str, timeout: int
    ) -> tuple[str, bytes] | None:
        assert key == bridge.MODEL_CATALOG_REQUEST_QUEUE
        try:
            return await asyncio.wait_for(self.queue.get(), timeout=timeout)
        except TimeoutError:
            return None

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def set(
        self, key: str, value: str, *, ex: int | None = None
    ) -> None:
        assert ex == 30
        self.values[key] = value

    async def delete(self, key: str) -> None:
        self.deleted.append(key)
        self.values.pop(key, None)


@pytest.mark.asyncio
async def test_loader_round_trips_catalog_through_daemon(
    tmp_path: Path,
) -> None:
    redis = _BridgeRedis()

    async def discover(**_kwargs: object) -> tuple[CodexModel, ...]:
        return (
            CodexModel(
                "invoke-me",
                "Provider Name",
                True,
                "medium",
                (CodexReasoningEffort("medium", "Balanced"),),
            ),
        )

    plugin = CodexPlugin(discover=discover)
    registry = CoderRegistry()
    registry.register(plugin)
    config_path = str(tmp_path / "config.yml")
    server = asyncio.create_task(
        bridge.serve_model_catalog_requests(
            redis,
            registry,
            config_path=config_path,
        )
    )
    loader = bridge.DaemonModelCatalogLoader(
        redis,
        timeout_seconds=1,
        poll_interval_seconds=0,
    )

    catalog = await loader(
        plugin,
        config=AppConfig(),
        config_path=config_path,
    )

    assert catalog.source == "discovered"
    assert catalog.description == "1 model advertised by Codex CLI."
    assert catalog.models[0].invocation_id == "invoke-me"
    assert catalog.models[0].reasoning_efforts[0].description == "Balanced"
    assert redis.deleted
    server.cancel()
    with pytest.raises(asyncio.CancelledError):
        await server


@pytest.mark.asyncio
async def test_loader_reports_queue_read_payload_timeout_and_cleanup_failures(
) -> None:
    plugin = CodexPlugin(discover=lambda **_kwargs: None)

    class BrokenQueue:
        async def rpush(self, *_args: object) -> None:
            raise ConnectionError

    with pytest.raises(ModelCatalogUnavailable, match="unavailable"):
        await bridge.DaemonModelCatalogLoader(BrokenQueue())(
            plugin,
            config=AppConfig(),
            config_path="config.yml",
        )

    class BrokenRead(_BridgeRedis):
        async def get(self, key: str) -> str | None:
            raise ConnectionError(key)

    with pytest.raises(ModelCatalogUnavailable, match="unavailable"):
        await bridge.DaemonModelCatalogLoader(BrokenRead())(
            plugin,
            config=AppConfig(),
            config_path="config.yml",
        )

    class InvalidResponse(_BridgeRedis):
        async def get(self, key: str) -> str | None:
            return "not-json"

        async def delete(self, key: str) -> None:
            raise ConnectionError(key)

    with pytest.raises(ModelCatalogUnavailable, match="invalid"):
        await bridge.DaemonModelCatalogLoader(InvalidResponse())(
            plugin,
            config=AppConfig(),
            config_path="config.yml",
        )

    with pytest.raises(ModelCatalogUnavailable, match="timed out"):
        await bridge.DaemonModelCatalogLoader(
            _BridgeRedis(), timeout_seconds=0
        )(
            plugin,
            config=AppConfig(),
            config_path="config.yml",
        )


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {"ok": False},
        {"ok": True},
        {"ok": True, "catalog": {"models": {}, "source": "x", "description": "x"}},
        {
            "ok": True,
            "catalog": {
                "models": [{}],
                "source": "x",
                "description": "x",
            },
        },
    ],
)
def test_parse_catalog_rejects_invalid_payloads(payload: object) -> None:
    with pytest.raises(ModelCatalogUnavailable):
        bridge._parse_catalog(payload)


@pytest.mark.asyncio
async def test_daemon_handler_ignores_invalid_and_expires_stale_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _BridgeRedis()
    registry = CoderRegistry()
    request_id = "a" * 32
    monkeypatch.setattr(bridge.time, "time", lambda: 100.0)

    for invalid in (
        "not-json",
        json.dumps([]),
        json.dumps({"request_id": "bad", "plugin": "codex", "expires_at": 200}),
    ):
        await bridge.handle_model_catalog_request(
            redis,
            registry,
            invalid,
            config_path="config.yml",
        )
    assert redis.values == {}

    await bridge.handle_model_catalog_request(
        redis,
        registry,
        json.dumps(
            {
                "request_id": request_id,
                "plugin": "codex",
                "expires_at": 99,
            }
        ).encode(),
        config_path="config.yml",
    )
    assert json.loads(redis.values[bridge._response_key(request_id)]) == {
        "ok": False,
        "error": "request expired",
    }

    await bridge.handle_model_catalog_request(
        redis,
        registry,
        json.dumps(
            {
                "request_id": "b" * 32,
                "plugin": "missing",
                "expires_at": 200,
            }
        ),
        config_path="config.yml",
    )
    assert json.loads(redis.values[bridge._response_key("b" * 32)]) == {
        "ok": False,
        "error": "catalog unavailable",
    }


@pytest.mark.asyncio
async def test_daemon_server_handles_idle_and_redis_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Redis:
        def __init__(self) -> None:
            self.calls = 0

        async def blpop(self, *_args: object, **_kwargs: object) -> None:
            self.calls += 1
            if self.calls == 1:
                return None
            raise ConnectionError

    redis = Redis()

    async def stop_after_retry(_seconds: float) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(bridge.asyncio, "sleep", stop_after_retry)
    with pytest.raises(asyncio.CancelledError):
        await bridge.serve_model_catalog_requests(
            redis,
            CoderRegistry(),
            config_path="config.yml",
        )
    assert redis.calls == 2


@pytest.mark.asyncio
async def test_daemon_server_disables_bridge_without_redis_list_support() -> None:
    await bridge.serve_model_catalog_requests(
        object(),
        CoderRegistry(),
        config_path="config.yml",
    )
