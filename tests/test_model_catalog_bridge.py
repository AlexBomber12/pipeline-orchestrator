"""Tests for the Redis bridge that keeps model discovery in the daemon."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import pytest
from src import model_catalog_bridge as bridge
from src.coder_registry import (
    CoderRegistry,
    ModelCatalog,
    ModelCatalogUnavailable,
    ModelMetadata,
)
from src.coders.codex import CodexPlugin
from src.coders.codex_models import CodexModel, CodexReasoningEffort
from src.config import AppConfig


class _BridgeRedis:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[tuple[str, bytes]] = asyncio.Queue()
        self.values: dict[str, str] = {}
        self.deleted: list[str] = []
        self.trimmed: list[tuple[str, int, int]] = []
        self.removed: list[tuple[str, int, str]] = []

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

    async def ltrim(self, key: str, start: int, end: int) -> None:
        self.trimmed.append((key, start, end))

    async def lrem(self, key: str, count: int, value: str) -> None:
        self.removed.append((key, count, value))

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


class _CatalogWorkerProcess:
    def __init__(self, stdout: bytes, returncode: int = 0) -> None:
        self.pid = 12345
        self.returncode = returncode
        self._stdout = stdout
        self.reaped = False

    async def communicate(self) -> tuple[bytes, bytes]:
        return self._stdout, b""

    async def wait(self) -> int:
        self.reaped = True
        return self.returncode


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
    metadata = await loader.load_plugin_metadata("codex")

    assert catalog.source == "discovered"
    assert catalog.description == "1 model advertised by Codex CLI."
    assert catalog.models[0].invocation_id == "invoke-me"
    assert catalog.models[0].reasoning_efforts[0].description == "Balanced"
    assert metadata.name == "codex"
    assert metadata.display_name == "Codex CLI"
    assert metadata.model_setting.setting_key == "model"
    assert metadata.model_catalog_refreshable is True
    assert redis.trimmed == [
        (bridge.MODEL_CATALOG_REQUEST_QUEUE, -64, -1),
        (bridge.MODEL_CATALOG_REQUEST_QUEUE, -64, -1),
    ]
    assert redis.removed[0][:2] == (
        bridge.MODEL_CATALOG_REQUEST_QUEUE,
        1,
    )
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

        async def lrem(self, key: str, count: int, value: str) -> None:
            raise ConnectionError(key, count, value)

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


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {"ok": True},
        {
            "ok": True,
            "metadata": {
                "name": "wrong",
                "display_name": "Third",
                "models": [],
                "model_setting": {},
                "model_catalog_refreshable": False,
            },
        },
        {
            "ok": True,
            "metadata": {
                "name": "third",
                "display_name": "Third",
                "models": [],
                "model_setting": {
                    "config_field": None,
                    "default_value": 3,
                    "default_label": "Default",
                    "setting_key": "model",
                },
                "model_catalog_refreshable": False,
            },
        },
    ],
)
def test_parse_plugin_metadata_rejects_invalid_payloads(payload: object) -> None:
    with pytest.raises(ModelCatalogUnavailable):
        bridge._parse_plugin_metadata(payload, expected_name="third")


@pytest.mark.asyncio
async def test_configured_catalog_worker_response_redacts_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Plugin:
        async def get_model_catalog(self, **_kwargs: object) -> ModelCatalog:
            return ModelCatalog(
                (ModelMetadata("third", "Third"),),
                "configured",
                "Configured catalog.",
            )

    monkeypatch.setattr("src.coders._load_plugin", lambda *_args: Plugin())
    monkeypatch.setattr(bridge, "load_config", lambda _path: AppConfig())
    response = await bridge._configured_catalog_worker_response(
        "third", "module:factory", "/cfg"
    )
    assert bridge._parse_catalog(response).models[0].invocation_id == "third"

    monkeypatch.setattr(
        "src.coders._load_plugin",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("must-not-leak")),
    )
    assert await bridge._configured_catalog_worker_response(
        "third", "module:factory", "/cfg"
    ) == {"ok": False, "error": "catalog unavailable"}


def test_configured_catalog_worker_main_validates_and_prints(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(bridge.sys, "argv", ["catalog-worker"])
    with pytest.raises(SystemExit, match="2"):
        bridge._configured_catalog_worker_main()

    async def response(*_args: object) -> dict[str, object]:
        return {"ok": False, "error": "unavailable"}

    monkeypatch.setattr(bridge, "_configured_catalog_worker_response", response)
    monkeypatch.setattr(
        bridge.sys,
        "argv",
        [
            "catalog-worker",
            "--configured-worker",
            "third",
            "module:factory",
            "/cfg",
        ],
    )
    bridge._configured_catalog_worker_main()
    assert capsys.readouterr().out.strip() == (
        bridge._WORKER_RESULT_PREFIX
        + '{"ok":false,"error":"unavailable"}'
    )


@pytest.mark.asyncio
async def test_isolated_configured_catalog_parses_worker_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog = ModelCatalog(
        (ModelMetadata("isolated", "Isolated"),),
        "configured",
        "Configured catalog.",
    )
    stdout = (
        bridge._WORKER_RESULT_PREFIX
        + json.dumps(bridge._catalog_payload(catalog), separators=(",", ":"))
        + "\nnoise\n"
    ).encode()
    captured: dict[str, object] = {}

    async def create(*args: object, **kwargs: object) -> _CatalogWorkerProcess:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return _CatalogWorkerProcess(stdout)

    monkeypatch.setattr(bridge.asyncio, "create_subprocess_exec", create)
    result = await bridge._isolated_configured_catalog(
        "third", "module:factory", config_path="/cfg"
    )

    assert result == catalog
    assert captured["args"] == (
        bridge.sys.executable,
        "-m",
        "src.model_catalog_bridge",
        "--configured-worker",
        "third",
        "module:factory",
        "/cfg",
    )
    assert captured["kwargs"] == {
        "stdout": bridge.asyncio.subprocess.PIPE,
        "stderr": bridge.asyncio.subprocess.DEVNULL,
        "start_new_session": True,
    }


@pytest.mark.parametrize(
    ("stdout", "returncode", "message"),
    [
        (b"", 1, "worker failed"),
        (
            b"PIPELINE_CATALOG_RESULT:{bad json}\n",
            0,
            "invalid result",
        ),
    ],
)
@pytest.mark.asyncio
async def test_isolated_configured_catalog_rejects_worker_failures(
    monkeypatch: pytest.MonkeyPatch,
    stdout: bytes,
    returncode: int,
    message: str,
) -> None:
    async def create(*_args: object, **_kwargs: object) -> _CatalogWorkerProcess:
        return _CatalogWorkerProcess(stdout, returncode)

    monkeypatch.setattr(bridge.asyncio, "create_subprocess_exec", create)
    with pytest.raises(ModelCatalogUnavailable, match=message):
        await bridge._isolated_configured_catalog(
            "third", "module:factory", config_path="/cfg"
        )


@pytest.mark.asyncio
async def test_isolated_configured_catalog_handles_start_timeout_and_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def failed_start(*_args: object, **_kwargs: object) -> object:
        raise OSError("must-not-leak")

    monkeypatch.setattr(
        bridge.asyncio,
        "create_subprocess_exec",
        failed_start,
    )
    with pytest.raises(ModelCatalogUnavailable, match="failed to start"):
        await bridge._isolated_configured_catalog(
            "third", "module:factory", config_path="/cfg"
        )

    class BlockedProcess(_CatalogWorkerProcess):
        async def communicate(self) -> tuple[bytes, bytes]:
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

    process = BlockedProcess(b"")
    terminated: list[_CatalogWorkerProcess] = []

    async def create(*_args: object, **_kwargs: object) -> _CatalogWorkerProcess:
        return process

    async def terminate(worker: _CatalogWorkerProcess) -> None:
        terminated.append(worker)

    monkeypatch.setattr(bridge.asyncio, "create_subprocess_exec", create)
    monkeypatch.setattr(bridge, "terminate_plugin_worker", terminate)
    with pytest.raises(ModelCatalogUnavailable, match="timed out"):
        await bridge._isolated_configured_catalog(
            "third",
            "module:factory",
            config_path="/cfg",
            timeout_seconds=0.001,
        )

    async def cancel_scenario() -> None:
        task = asyncio.create_task(
            bridge._isolated_configured_catalog(
                "third",
                "module:factory",
                config_path="/cfg",
                timeout_seconds=60,
            )
        )
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    await cancel_scenario()
    assert terminated == [process, process]


@pytest.mark.asyncio
async def test_daemon_handler_isolates_configured_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _BridgeRedis()
    registry = CoderRegistry()
    plugin = CodexPlugin(discover=lambda **_kwargs: None)
    registry.register(plugin, reference="operator.plugin:factory")
    calls: list[tuple[str, str, str]] = []

    async def isolated(
        plugin_id: str,
        reference: str,
        *,
        config_path: str,
    ) -> ModelCatalog:
        calls.append((plugin_id, reference, config_path))
        return ModelCatalog((), "configured", "Isolated catalog.")

    monkeypatch.setattr(bridge, "_isolated_configured_catalog", isolated)
    await bridge.handle_model_catalog_request(
        redis,
        registry,
        json.dumps(
            {
                "request_id": "c" * 32,
                "plugin": "codex",
                "expires_at": time.time() + 10,
            }
        ),
        config_path="/cfg",
    )

    assert calls == [("codex", "operator.plugin:factory", "/cfg")]
    response = json.loads(redis.values[bridge._response_key("c" * 32)])
    assert response["ok"] is True
    assert response["catalog"]["source"] == "configured"


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
