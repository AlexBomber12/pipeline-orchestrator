"""Tests for the Redis bridge that keeps model discovery in the daemon."""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from src import model_catalog_bridge as bridge
from src.coder_login import CoderCredentialReservations
from src.coder_registry import (
    CoderAuthCapabilities,
    CoderAuthStatus,
    CoderBrowserLoginFailure,
    CoderRegistry,
    ModelCatalog,
    ModelCatalogUnavailable,
    ModelMetadata,
    ModelReasoningEffort,
    coder_auth_payload,
)
from src.coders.claude import ClaudePlugin
from src.coders.codex import CodexPlugin
from src.coders.codex_models import CodexModel, CodexReasoningEffort
from src.config import AppConfig
from src.process_supervisor import (
    CleanupResult,
    CleanupStatus,
    ProcessLaunchCleanupError,
)

from tests import test_coder_login as login_tests


class _BridgeRedis:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[tuple[str, bytes]] = asyncio.Queue()
        self.values: dict[str, str] = {}
        self.deleted: list[str] = []
        self.trimmed: list[tuple[str, int, int]] = []
        self.removed: list[tuple[str, int, str]] = []
        self.handoffs: list[tuple[str, int, bool]] = []
        self.requests: list[str] = []

    async def rpush(self, key: str, value: str) -> None:
        self.requests.append(value)
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
        self, key: str, value: str, *, ex: int | None = None, nx: bool = False
    ) -> bool:
        assert ex == 30
        if nx:
            self.handoffs.append((key, ex, nx))
            if key in self.values:
                return False
        self.values[key] = value
        return True

    async def getdel(self, key: str) -> str | None:
        return self.values.pop(key, None)

    async def delete(self, key: str) -> None:
        self.deleted.append(key)
        self.values.pop(key, None)


class _CatalogWorkerProcess:
    def __init__(self, stdout: bytes, returncode: int = 0) -> None:
        self.pid = 12345
        self.returncode = returncode
        self._stdout = stdout
        self.stdin = _CatalogWorkerStdin()
        self.stdout = _CatalogWorkerStdout(stdout)
        self.reaped = False

    async def communicate(self) -> tuple[bytes, bytes]:
        return self._stdout, b""

    async def wait(self) -> int:
        self.reaped = True
        return self.returncode


class _CatalogWorkerStdout:
    def __init__(self, output: bytes, *, blocked: bool = False) -> None:
        self.lines = output.splitlines(keepends=True)
        self.blocked = blocked

    async def readline(self) -> bytes:
        if self.blocked:
            await asyncio.Event().wait()
        return self.lines.pop(0) if self.lines else b""


class _CatalogWorkerStdin:
    def __init__(self, *, broken: bool = False) -> None:
        self.broken = broken
        self.writes: list[bytes] = []
        self.closed = False

    def write(self, data: bytes) -> None:
        if self.broken:
            raise BrokenPipeError
        self.writes.append(data)

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True


class _ManagedCatalogWorker:
    def __init__(
        self,
        process: _CatalogWorkerProcess,
        *,
        reconciliation: CleanupResult | None = None,
        cleanup: CleanupResult | None = None,
    ) -> None:
        self.process = process
        self.reconciliation = reconciliation or _cleanup_result(quiescent=False)
        self.cleanup_result = cleanup or _cleanup_result(quiescent=True)
        self.reconcile_calls = 0
        self.cleanup_calls = 0

    async def reconcile_cleanup(self, *, observation_grace: float) -> CleanupResult:
        assert observation_grace == 0.0
        self.reconcile_calls += 1
        return self.reconciliation

    async def cleanup(
        self, *, term_grace: float, kill_grace: float
    ) -> CleanupResult:
        assert term_grace == bridge._CATALOG_RECONCILE_GRACE_SECONDS
        assert kill_grace == bridge._CATALOG_RECONCILE_GRACE_SECONDS
        self.cleanup_calls += 1
        return self.cleanup_result


def _cleanup_result(*, quiescent: bool) -> CleanupResult:
    return CleanupResult(
        status=(
            CleanupStatus.QUIESCENT
            if quiescent
            else CleanupStatus.FAILED
        ),
        leader_returncode=None,
        term_sent=True,
        kill_sent=True,
        detail=None if quiescent else "ownership still unresolved",
    )


class _RetainedProcess:
    def __init__(self, *reconciliations: CleanupResult | Exception) -> None:
        self.reconciliations = list(reconciliations)
        self.reconcile_calls = 0

    async def reconcile_cleanup(
        self, *, observation_grace: float
    ) -> CleanupResult:
        assert observation_grace == bridge._CATALOG_RECONCILE_GRACE_SECONDS
        self.reconcile_calls += 1
        result = self.reconciliations.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


@pytest.mark.asyncio
async def test_loader_round_trips_catalog_through_daemon(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _BridgeRedis()
    codex_home = tmp_path / "codex-home"
    replacement_codex_home = tmp_path / "replacement-codex-home"
    reservations = CoderCredentialReservations()
    monkeypatch.delenv("CODEX_HOME", raising=False)

    async def discover(**kwargs: object) -> tuple[CodexModel, ...]:
        assert reservations.reserve_login(str(codex_home / ".codex")) is False
        environment = kwargs["env"]
        assert isinstance(environment, dict)
        assert environment["CODEX_HOME"] == str(codex_home / ".codex")
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
    reference = "src.coders.codex:CodexPlugin"
    registry.register(plugin, reference=reference)
    auth_calls: list[tuple[str, str, str, str, float, str]] = []

    async def auth_probe(
        plugin_id: str,
        factory_reference: str,
        display_name: str,
        *,
        config_path: str,
        timeout: float,
        env: dict[str, str] | None,
    ) -> dict[str, object]:
        assert env is not None
        config_file.write_text(
            f"auth:\n  codex_home_dir: {replacement_codex_home}\n",
            encoding="utf-8",
        )
        replacement_location = str(replacement_codex_home / ".codex")
        assert reservations.reserve_login(replacement_location) is True
        reservations.release_login(replacement_location)
        auth_calls.append(
            (
                plugin_id,
                factory_reference,
                display_name,
                config_path,
                timeout,
                env["CODEX_HOME"],
            )
        )
        return coder_auth_payload(
            CoderAuthStatus(
                status="ok",
                detail="daemon-owned",
                cli_available=True,
                cli_version="1.2.3",
                saved_credentials_present=True,
                authentication_mode="browser_oauth",
            ),
            capabilities=CoderAuthCapabilities(
                can_check_cli=True,
                can_check_saved_credentials=True,
                can_report_authentication_mode=True,
                can_verify_service_access=False,
                interactive_login_methods=("browser_oauth",),
            ),
        )

    monkeypatch.setattr(bridge, "isolated_auth_probe", auth_probe)
    config_file = tmp_path / "config.yml"
    config_file.write_text(
        f"auth:\n  codex_home_dir: {codex_home}\n",
        encoding="utf-8",
    )
    config_path = str(config_file)
    server = asyncio.create_task(
        bridge.serve_model_catalog_requests(
            redis,
            registry,
            config_path=config_path,
            credential_reservations=reservations,
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
    metadata = await loader.load_plugin_metadata(
        "codex",
        expected_reference=reference,
    )
    auth = await loader.load_auth_status(
        "codex",
        expected_reference=reference,
    )

    assert catalog.source == "discovered"
    assert catalog.description == "1 model advertised by Codex CLI."
    assert catalog.models[0].invocation_id == "invoke-me"
    assert catalog.models[0].reasoning_efforts[0].description == "Balanced"
    assert metadata.name == "codex"
    assert metadata.display_name == "Codex CLI"
    assert metadata.model_setting.setting_key == "model"
    assert metadata.model_catalog_refreshable is True
    assert auth["status"] == "ok"
    assert auth["detail"] == "daemon-owned"
    assert auth["cli_available"] is True
    assert auth["saved_credentials_present"] is True
    assert auth["authentication_mode"] == "browser_oauth"
    assert auth["service_access_verified"] is None
    assert auth["capabilities"]["can_verify_service_access"] is False
    assert reservations.reserve_login(str(codex_home / ".codex")) is True
    reservations.release_login(str(codex_home / ".codex"))
    assert auth_calls == [
        (
            "codex",
            reference,
            "Codex CLI",
            config_path,
            bridge._CONFIGURED_CATALOG_TIMEOUT_SECONDS,
            str(codex_home / ".codex"),
        )
    ]
    assert redis.trimmed == [
        (bridge.MODEL_CATALOG_REQUEST_QUEUE, -64, -1),
        (bridge.MODEL_CATALOG_REQUEST_QUEUE, -64, -1),
        (bridge.MODEL_CATALOG_REQUEST_QUEUE, -64, -1),
    ]
    assert redis.removed[0][:2] == (
        bridge.MODEL_CATALOG_REQUEST_QUEUE,
        1,
    )
    assert redis.deleted
    with pytest.raises(ModelCatalogUnavailable, match="unavailable"):
        await loader.load_plugin_metadata(
            "codex",
            expected_reference="old.module:factory",
        )
    server.cancel()
    with pytest.raises(asyncio.CancelledError):
        await server


@pytest.mark.asyncio
async def test_loader_round_trips_discovered_claude_catalog_through_daemon(
    tmp_path: Path,
) -> None:
    redis = _BridgeRedis()
    advertised = (
        ModelMetadata(
            "claude-invoke",
            "Claude Provider Name",
            reasoning_efforts=(ModelReasoningEffort("high"),),
        ),
        ModelMetadata("claude-compatible", "Claude Compatible"),
    )
    discovery_calls: list[dict[str, object]] = []

    reservations = CoderCredentialReservations()

    async def discover(**kwargs: object) -> tuple[ModelMetadata, ...]:
        assert reservations.reserve_login(str(claude_config_dir)) is False
        discovery_calls.append(kwargs)
        return advertised

    plugin = ClaudePlugin(discover=discover)
    registry = CoderRegistry()
    reference = "src.coders.claude:ClaudePlugin"
    registry.register(plugin, reference=reference)
    config_path = tmp_path / "config.yml"
    claude_config_dir = tmp_path / "claude-auth"
    config_path.write_text(
        f"auth:\n  claude_config_dir: {claude_config_dir}\n",
        encoding="utf-8",
    )
    server = asyncio.create_task(
        bridge.serve_model_catalog_requests(
            redis,
            registry,
            config_path=str(config_path),
            credential_reservations=reservations,
        )
    )
    loader = bridge.DaemonModelCatalogLoader(
        redis,
        timeout_seconds=1,
        poll_interval_seconds=0,
    )

    try:
        catalog = await loader(
            plugin,
            config=AppConfig(),
            config_path=str(config_path),
        )
    finally:
        server.cancel()
        with pytest.raises(asyncio.CancelledError):
            await server

    assert catalog.models == advertised
    assert catalog.source == "discovered"
    assert "service access and account entitlement are not verified" in (
        catalog.description
    )
    assert len(discovery_calls) == 1
    assert discovery_calls[0]["cwd"] == str(tmp_path)
    env = discovery_calls[0]["env"]
    assert isinstance(env, dict)
    assert env["CLAUDE_CONFIG_DIR"] == str(claude_config_dir)
    assert reservations.reserve_login(str(claude_config_dir)) is True
    reservations.release_login(str(claude_config_dir))


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["auth", "catalog"])
async def test_claude_readers_are_blocked_by_matching_login_reservation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    redis = _BridgeRedis()
    registry = CoderRegistry()

    async def must_not_discover(**_kwargs: object) -> object:
        raise AssertionError("catalog must not run during matching login")

    plugin = ClaudePlugin(discover=must_not_discover)
    reference = "src.coders.claude:ClaudePlugin"
    registry.register(plugin, reference=reference)
    claude_config_dir = tmp_path / "claude-auth"
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        f"auth:\n  claude_config_dir: {claude_config_dir}\n",
        encoding="utf-8",
    )
    reservations = CoderCredentialReservations()
    assert reservations.reserve_login(str(claude_config_dir)) is True

    async def must_not_probe(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("auth must not run during matching login")

    monkeypatch.setattr(bridge, "isolated_auth_probe", must_not_probe)
    request_id = "9" * 32
    await bridge.handle_model_catalog_request(
        redis,
        registry,
        json.dumps(
            {
                "request_id": request_id,
                "plugin": "claude",
                "operation": operation,
                "reference": reference,
                "expires_at": time.time() + 10,
            }
        ),
        config_path=str(config_path),
        credential_reservations=reservations,
    )

    response = json.loads(redis.values[bridge._response_key(request_id)])
    if operation == "auth":
        assert response["auth"]["failure_reason"] == "probe_unavailable"
    else:
        assert response == {"ok": False, "error": "catalog unavailable"}


@pytest.mark.asyncio
async def test_daemon_auth_probe_is_suppressed_during_device_login(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _BridgeRedis()
    registry = CoderRegistry()
    async def must_not_discover(**_kwargs: object) -> object:
        raise AssertionError("model discovery must wait for device login")

    plugin = CodexPlugin(discover=must_not_discover)
    reference = "src.coders.codex:CodexPlugin"
    registry.register(plugin, reference=reference)
    config_path = tmp_path / "config.yml"
    codex_home = tmp_path / "codex-home"
    config_path.write_text(
        f"auth:\n  codex_home_dir: {codex_home}\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("CODEX_HOME", raising=False)
    reservations = CoderCredentialReservations()
    assert reservations.reserve_login(str(codex_home / ".codex")) is True

    async def must_not_probe(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("auth probe must wait for device login")

    monkeypatch.setattr(bridge, "isolated_auth_probe", must_not_probe)
    request_id = "f" * 32
    await bridge.handle_model_catalog_request(
        redis,
        registry,
        json.dumps(
            {
                "request_id": request_id,
                "plugin": "codex",
                "operation": "auth",
                "reference": reference,
                "expires_at": time.time() + 10,
            }
        ),
        config_path=str(config_path),
        credential_reservations=reservations,
    )

    response = json.loads(redis.values[bridge._response_key(request_id)])
    assert response["ok"] is True
    assert response["auth"]["status"] == "error"
    assert response["auth"]["failure_reason"] == "probe_unavailable"

    catalog_request_id = "d" * 32
    await bridge.handle_model_catalog_request(
        redis,
        registry,
        json.dumps(
            {
                "request_id": catalog_request_id,
                "plugin": "codex",
                "operation": "catalog",
                "reference": reference,
                "expires_at": time.time() + 10,
            }
        ),
        config_path=str(config_path),
        credential_reservations=reservations,
    )
    assert json.loads(
        redis.values[bridge._response_key(catalog_request_id)]
    ) == {"ok": False, "error": "catalog unavailable"}
    reservations.release_login(str(codex_home / ".codex"))


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["auth", "catalog"])
async def test_daemon_reader_rejects_invalid_credential_location(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    redis = _BridgeRedis()
    registry = CoderRegistry()
    plugin = CodexPlugin(discover=lambda **_kwargs: None)
    reference = "src.coders.codex:CodexPlugin"
    registry.register(plugin, reference=reference)
    monkeypatch.setattr(
        plugin,
        "device_login_credential_location",
        lambda **_kwargs: "",
    )
    request_id = "e" * 32

    await bridge.handle_model_catalog_request(
        redis,
        registry,
        json.dumps(
            {
                "request_id": request_id,
                "plugin": "codex",
                "operation": operation,
                "reference": reference,
                "expires_at": time.time() + 10,
            }
        ),
        config_path=str(tmp_path / "config.yml"),
        credential_reservations=CoderCredentialReservations(),
    )

    response = json.loads(redis.values[bridge._response_key(request_id)])
    assert response == {"ok": False, "error": "catalog unavailable"}


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["auth", "catalog"])
async def test_daemon_reader_rejects_invalid_browser_credential_location(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    redis = _BridgeRedis()
    registry = CoderRegistry()
    plugin = ClaudePlugin(discover=lambda **_kwargs: None)
    reference = "src.coders.claude:ClaudePlugin"
    registry.register(plugin, reference=reference)
    monkeypatch.setattr(
        plugin,
        "browser_login_credential_location",
        lambda **_kwargs: "",
    )

    async def must_not_probe(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("provider must not run with an invalid context")

    monkeypatch.setattr(bridge, "isolated_auth_probe", must_not_probe)
    request_id = "8" * 32
    await bridge.handle_model_catalog_request(
        redis,
        registry,
        json.dumps(
            {
                "request_id": request_id,
                "plugin": "claude",
                "operation": operation,
                "reference": reference,
                "expires_at": time.time() + 10,
            }
        ),
        config_path=str(tmp_path / "config.yml"),
        credential_reservations=CoderCredentialReservations(),
    )

    assert json.loads(redis.values[bridge._response_key(request_id)]) == {
        "ok": False,
        "error": "catalog unavailable",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["auth", "catalog"])
@pytest.mark.parametrize("builder_failure", ["raises", "invalid"])
async def test_daemon_reader_releases_reservation_after_context_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    builder_failure: str,
) -> None:
    redis = _BridgeRedis()
    registry = CoderRegistry()
    plugin = CodexPlugin(discover=lambda **_kwargs: None)
    reference = "src.coders.codex:CodexPlugin"
    registry.register(plugin, reference=reference)
    monkeypatch.delenv("CODEX_HOME", raising=False)
    codex_home = tmp_path / "codex-home"
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        f"auth:\n  codex_home_dir: {codex_home}\n",
        encoding="utf-8",
    )
    reservations = CoderCredentialReservations()
    location = str(codex_home / ".codex")

    def fail(**_kwargs: object) -> object:
        if builder_failure == "raises":
            raise RuntimeError("must-not-leak")
        return 1

    monkeypatch.setattr(plugin, "build_credential_environment", fail)
    request_id = "b" * 32

    await bridge.handle_model_catalog_request(
        redis,
        registry,
        json.dumps(
            {
                "request_id": request_id,
                "plugin": "codex",
                "operation": operation,
                "reference": reference,
                "expires_at": time.time() + 10,
            }
        ),
        config_path=str(config_path),
        credential_reservations=reservations,
    )

    assert json.loads(redis.values[bridge._response_key(request_id)]) == {
        "ok": False,
        "error": "catalog unavailable",
    }
    assert reservations.reserve_login(location) is True
    reservations.release_login(location)


@pytest.mark.asyncio
async def test_daemon_auth_reader_requires_credential_environment_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _BridgeRedis()
    registry = CoderRegistry()
    plugin = CodexPlugin(discover=lambda **_kwargs: None)
    reference = "src.coders.codex:CodexPlugin"
    registry.register(plugin, reference=reference)
    monkeypatch.setattr(plugin, "build_credential_environment", None)
    config_path = tmp_path / "config.yml"
    config_path.write_text("auth:\n  codex_home_dir: /reserved\n", encoding="utf-8")

    async def must_not_probe(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("probe must fail closed before worker startup")

    monkeypatch.setattr(bridge, "isolated_auth_probe", must_not_probe)
    request_id = "a" * 32
    await bridge.handle_model_catalog_request(
        redis,
        registry,
        json.dumps(
            {
                "request_id": request_id,
                "plugin": "codex",
                "operation": "auth",
                "reference": reference,
                "expires_at": time.time() + 10,
            }
        ),
        config_path=str(config_path),
        credential_reservations=CoderCredentialReservations(),
    )

    assert json.loads(redis.values[bridge._response_key(request_id)]) == {
        "ok": False,
        "error": "catalog unavailable",
    }


def test_credential_environment_handles_unreserved_and_missing_contract() -> None:
    plugin = object()
    config = AppConfig()

    assert (
        bridge._credential_environment(
            plugin,
            config=config,
            credential_location=None,
        )
        is None
    )
    with pytest.raises(ValueError, match="credential environment builder unavailable"):
        bridge._credential_environment(
            plugin,
            config=config,
            credential_location="/reserved",
        )


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

    with pytest.raises(ModelCatalogUnavailable, match="unavailable"):
        await bridge.DaemonModelCatalogLoader(_BridgeRedis())(
            plugin,
            config=AppConfig.model_construct(coder_plugins={}),
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


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {"ok": True},
        {"ok": True, "auth": {"status": "unknown", "detail": "bad"}},
    ],
)
def test_parse_auth_status_rejects_invalid_payloads(payload: object) -> None:
    with pytest.raises(ModelCatalogUnavailable):
        bridge._parse_auth_status(payload)


def _login_payload(
    *, state: str = "starting", failure_reason: str | None = None
) -> dict[str, object]:
    return {
        "plugin": "codex",
        "session_id": "A" * 43,
        "state": state,
        "detail": "Device login status",
        "failure_reason": failure_reason,
        "verification_url": None,
        "user_code": None,
        "expires_at": None,
        "cleanup_confirmed": None,
        "replacement_requested": False,
        "reused_session": False,
        "replacement_warning": "Credentials can become unavailable.",
        "auth_status": None,
    }


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {"ok": False},
        {"ok": True, "login": {"state": "secret"}},
    ],
)
def test_parse_device_login_rejects_invalid_payloads(payload: object) -> None:
    with pytest.raises(ModelCatalogUnavailable):
        bridge._parse_device_login(payload, expected_plugin="codex")


@pytest.mark.asyncio
async def test_loader_round_trips_device_login_operations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _BridgeRedis()
    registry = CoderRegistry()
    reference = "src.coders.codex:CodexPlugin"
    registry.register(CodexPlugin(discover=lambda **_kwargs: None), reference=reference)
    calls: list[tuple[object, ...]] = []

    class LoginManager:
        async def start(
            self,
            plugin: str,
            *,
            expected_reference: str,
            replace_existing: bool,
        ) -> dict[str, object]:
            calls.append(("start", plugin, expected_reference, replace_existing))
            return _login_payload()

        async def inspect(
            self,
            plugin: str,
            session_id: str,
            *,
            expected_reference: str,
        ) -> dict[str, object]:
            calls.append(("inspect", plugin, session_id, expected_reference))
            return _login_payload(state="waiting_for_user") | {
                "verification_url": "https://auth.openai.com/codex/device",
                "user_code": "ABCD-EFGH",
                "expires_at": 1234.0,
            }

        async def cancel(
            self,
            plugin: str,
            session_id: str,
            *,
            expected_reference: str,
        ) -> dict[str, object]:
            calls.append(("cancel", plugin, session_id, expected_reference))
            return _login_payload(state="cancelled") | {"cleanup_confirmed": True}

        async def shutdown(self) -> None:
            calls.append(("shutdown",))

    manager = LoginManager()
    monkeypatch.setattr(
        bridge,
        "CoderLoginSessionManager",
        lambda *_args, **_kwargs: manager,
    )
    server = asyncio.create_task(
        bridge.serve_model_catalog_requests(
            redis,
            registry,
            config_path="/cfg",
            credential_location_in_use=lambda _location: False,
        )
    )
    loader = bridge.DaemonModelCatalogLoader(
        redis, timeout_seconds=1, poll_interval_seconds=0
    )

    started = await loader.start_device_login(
        "codex",
        expected_reference=reference,
        replace_existing=False,
    )
    inspected = await loader.inspect_device_login(
        "codex", "A" * 43, expected_reference=reference
    )
    cancelled = await loader.cancel_device_login(
        "codex", "A" * 43, expected_reference=reference
    )

    assert started["state"] == "starting"
    assert inspected["user_code"] == "ABCD-EFGH"
    assert cancelled["cleanup_confirmed"] is True
    assert calls[:3] == [
        ("start", "codex", reference, False),
        ("inspect", "codex", "A" * 43, reference),
        ("cancel", "codex", "A" * 43, reference),
    ]
    server.cancel()
    with pytest.raises(asyncio.CancelledError):
        await server
    assert calls[-1] == ("shutdown",)


@pytest.mark.asyncio
async def test_daemon_login_handler_rejects_unavailable_or_invalid_requests() -> None:
    redis = _BridgeRedis()
    registry = CoderRegistry()
    reference = "src.coders.codex:CodexPlugin"
    registry.register(CodexPlugin(discover=lambda **_kwargs: None), reference=reference)

    await bridge.handle_model_catalog_request(
        redis,
        registry,
        json.dumps(
            {
                "request_id": "a" * 32,
                "plugin": "codex",
                "operation": "device_login_start",
                "reference": reference,
                "replace_existing": False,
                "expires_at": time.time() + 10,
            }
        ),
        config_path="/cfg",
    )
    assert json.loads(redis.values[bridge._response_key("a" * 32)]) == {
        "ok": False,
        "error": "coder login unavailable",
    }

    for request_id, fields in (
        ("b" * 32, {"operation": "device_login_start"}),
        (
            "c" * 32,
            {
                "operation": "device_login_inspect",
                "session_id": "x" * 129,
            },
        ),
    ):
        await bridge.handle_model_catalog_request(
            redis,
            registry,
            json.dumps(
                {
                    "request_id": request_id,
                    "plugin": "codex",
                    "reference": reference,
                    "expires_at": time.time() + 10,
                    **fields,
                }
            ),
            config_path="/cfg",
            login_manager=object(),  # type: ignore[arg-type]
        )
        assert bridge._response_key(request_id) not in redis.values


@pytest.mark.asyncio
async def test_configured_worker_redacts_failures_and_rejects_location_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Plugin:
        calls = 0

        def create_device_login(self, *, config_path: str) -> object:
            del config_path
            return object()

        def device_login_credential_location(self, *, config: AppConfig) -> str:
            return config.auth.codex_home_dir

        def build_credential_environment(
            self,
            *,
            config: AppConfig,
            credential_location: str,
        ) -> dict[str, str]:
            del config
            return {"CODEX_HOME": credential_location}

        async def get_model_catalog(self, **_kwargs: object) -> ModelCatalog:
            self.calls += 1
            return ModelCatalog(
                (ModelMetadata("third", "Third"),),
                "configured",
                "Configured catalog.",
            )

    plugin = Plugin()
    monkeypatch.setattr("src.coders._load_plugin", lambda *_args: plugin)
    config = AppConfig.model_validate(
        {"auth": {"codex_home_dir": "/configured/location-b"}}
    )
    monkeypatch.setattr(bridge, "load_config", lambda _path: config)
    response = await bridge._configured_catalog_worker_response(
        "third", "module:factory", "/cfg"
    )
    assert bridge._parse_catalog(response).models[0].invocation_id == "third"
    assert plugin.calls == 1

    assert await bridge._configured_catalog_worker_response(
        "third",
        "module:factory",
        "/cfg",
        "/reserved/credential/location",
        {"CODEX_HOME": "/reserved/credential/location"},
    ) == {"ok": False, "error": "catalog unavailable"}
    assert plugin.calls == 1

    monkeypatch.setattr(
        "src.coders._load_plugin",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("must-not-leak")),
    )
    assert await bridge._configured_catalog_worker_response(
        "third", "module:factory", "/cfg"
    ) == {"ok": False, "error": "catalog unavailable"}


@pytest.mark.asyncio
async def test_bound_configured_catalog_rejects_incompatible_plugin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    class Plugin:
        def create_device_login(self, *, config_path: str) -> object:
            del config_path
            return object()

        def device_login_credential_location(self, *, config: AppConfig) -> str:
            return config.auth.codex_home_dir

        def build_credential_environment(
            self,
            *,
            config: AppConfig,
            credential_location: str,
        ) -> dict[str, str]:
            del config
            return {"CODEX_HOME": credential_location}

        async def get_model_catalog(
            self, *, config: AppConfig, config_path: str
        ) -> ModelCatalog:
            nonlocal calls
            del config, config_path
            calls += 1
            return ModelCatalog((), "configured", "must not run")

    config = AppConfig.model_validate(
        {"auth": {"codex_home_dir": "/reserved"}}
    )
    monkeypatch.setattr("src.coders._load_plugin", lambda *_args: Plugin())
    monkeypatch.setattr(bridge, "load_config", lambda _path: config)
    sensitive = "sensitive-environment-value"

    response = await bridge._configured_catalog_worker_response(
        "third",
        "module:factory",
        "/cfg",
        "/reserved",
        {"CODEX_HOME": "/reserved", "SECRET": sensitive},
    )

    assert response == {"ok": False, "error": "catalog unavailable"}
    assert calls == 0
    assert sensitive not in str(response)


def test_configured_catalog_worker_main_validates_and_prints(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(bridge.sys, "argv", ["catalog-worker"])
    with pytest.raises(SystemExit, match="2"):
        bridge._configured_catalog_worker_main()
    monkeypatch.setattr(
        bridge.sys,
        "argv",
        [
            "catalog-worker",
            "--configured-worker",
            "third",
            "module:factory",
            "/cfg",
            bridge.EXPLICIT_ENVIRONMENT_ARG,
            "/misordered/location",
        ],
    )
    with pytest.raises(SystemExit, match="2"):
        bridge._configured_catalog_worker_main()

    calls: list[dict[str, object]] = []

    async def response(*_args: object, **kwargs: object) -> None:
        calls.append(kwargs)
        print(
            bridge._WORKER_RESULT_PREFIX
            + '{"ok":false,"error":"unavailable"}'
        )

    monkeypatch.setattr(bridge, "_run_configured_catalog_worker", response)
    monkeypatch.setenv("BOUND_HOME", "/credentials/a")
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
    assert calls == [{"environment": None, "monitor_parent": True}]

    monkeypatch.setattr(
        bridge.sys,
        "argv",
        [
            "catalog-worker",
            "--configured-worker",
            "third",
            "module:factory",
            "/cfg",
            bridge.EXPLICIT_ENVIRONMENT_ARG,
        ],
    )
    bridge._configured_catalog_worker_main()
    bound_environment = calls[1]["environment"]
    assert isinstance(bound_environment, dict)
    assert bound_environment["BOUND_HOME"] == "/credentials/a"


@pytest.mark.asyncio
async def test_configured_worker_retains_child_until_reconciled(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    failed = _cleanup_result(quiescent=False)
    managed = _RetainedProcess(failed, _cleanup_result(quiescent=True))

    class Plugin:
        async def get_model_catalog(self, **_kwargs: object) -> ModelCatalog:
            raise ModelCatalogUnavailable(
                "private configured-plugin detail",
                managed=managed,  # type: ignore[arg-type]
                cleanup_result=failed,
            )

    async def no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("src.coders._load_plugin", lambda *_args: Plugin())
    monkeypatch.setattr(bridge, "load_config", lambda _path: AppConfig())
    monkeypatch.setattr(bridge.asyncio, "sleep", no_sleep)

    await bridge._run_configured_catalog_worker(
        "third", "module:factory", "/cfg"
    )

    output = capsys.readouterr().out.strip()
    assert output.startswith(bridge._WORKER_RESULT_PREFIX)
    payload = json.loads(output.removeprefix(bridge._WORKER_RESULT_PREFIX))
    assert payload == {
        "ok": False,
        "error": "catalog unavailable",
        bridge._WORKER_CLEANUP_PENDING: True,
    }
    assert "private configured-plugin detail" not in output
    assert managed.reconcile_calls == 2


@pytest.mark.asyncio
async def test_configured_worker_stays_alive_for_unowned_cleanup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failed = _cleanup_result(quiescent=False)
    published = asyncio.Event()
    output: list[str] = []

    class Plugin:
        async def get_model_catalog(self, **_kwargs: object) -> ModelCatalog:
            raise ModelCatalogUnavailable(
                "private startup detail",
                cleanup_result=failed,
            )

    def capture_print(*values: object, **_kwargs: object) -> None:
        output.append(" ".join(str(value) for value in values))
        published.set()

    monkeypatch.setattr("src.coders._load_plugin", lambda *_args: Plugin())
    monkeypatch.setattr(bridge, "load_config", lambda _path: AppConfig())
    monkeypatch.setattr("builtins.print", capture_print)
    task = asyncio.create_task(
        bridge._run_configured_catalog_worker(
            "third", "module:factory", "/cfg"
        )
    )
    await published.wait()
    assert task.done() is False
    payload = json.loads(
        output[0].removeprefix(bridge._WORKER_RESULT_PREFIX)
    )
    assert payload[bridge._WORKER_CLEANUP_PENDING] is True
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_configured_worker_serializes_success_and_sanitized_failures(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    catalog = ModelCatalog((), "configured", "Configured catalog")

    class Plugin:
        def __init__(self, result: ModelCatalog | Exception) -> None:
            self.result = result

        def device_login_credential_location(self, *, config: AppConfig) -> str:
            del config
            return "/expected"

        async def get_model_catalog(self, **_kwargs: object) -> ModelCatalog:
            if isinstance(self.result, Exception):
                raise self.result
            return self.result

    async def run(plugin: Plugin, expected: str | None = None) -> dict[str, object]:
        monkeypatch.setattr("src.coders._load_plugin", lambda *_args: plugin)
        monkeypatch.setattr(bridge, "load_config", lambda _path: AppConfig())
        await bridge._run_configured_catalog_worker(
            "third", "module:factory", "/cfg", expected
        )
        output = capsys.readouterr().out.strip()
        return json.loads(output.removeprefix(bridge._WORKER_RESULT_PREFIX))

    assert (await run(Plugin(catalog)))["catalog"]["source"] == "configured"
    assert await run(Plugin(catalog), "/wrong") == {
        "ok": False,
        "error": "catalog unavailable",
    }
    assert await run(Plugin(RuntimeError("private"))) == {
        "ok": False,
        "error": "catalog unavailable",
    }


@pytest.mark.asyncio
async def test_configured_worker_result_cannot_be_spoofed_by_plugin_stdout(
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    catalog = ModelCatalog(
        (ModelMetadata("real", "Real"),),
        "configured",
        "Configured catalog",
    )

    class Plugin:
        async def get_model_catalog(self, **_kwargs: object) -> ModelCatalog:
            print(
                bridge._WORKER_RESULT_PREFIX
                + '{"ok":true,"catalog":{"models":[],"source":"spoofed",'
                '"description":"Spoofed"}}',
                flush=True,
            )
            return catalog

    monkeypatch.setattr("src.coders._load_plugin", lambda *_args: Plugin())
    monkeypatch.setattr(bridge, "load_config", lambda _path: AppConfig())

    await bridge._run_configured_catalog_worker(
        "third", "module:factory", "/cfg"
    )

    output = capfd.readouterr().out.strip().splitlines()
    assert len(output) == 1
    payload = json.loads(output[0].removeprefix(bridge._WORKER_RESULT_PREFIX))
    assert payload["catalog"]["source"] == "configured"
    assert payload["catalog"]["models"][0]["invocation_id"] == "real"


@pytest.mark.asyncio
async def test_configured_worker_retries_reconcile_observation_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    failed = _cleanup_result(quiescent=False)
    managed = _RetainedProcess(
        RuntimeError("observation unavailable"),
        _cleanup_result(quiescent=True),
    )

    class Plugin:
        async def get_model_catalog(self, **_kwargs: object) -> ModelCatalog:
            raise ModelCatalogUnavailable(
                "private",
                managed=managed,  # type: ignore[arg-type]
                cleanup_result=failed,
            )

    async def no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr("src.coders._load_plugin", lambda *_args: Plugin())
    monkeypatch.setattr(bridge, "load_config", lambda _path: AppConfig())
    monkeypatch.setattr(bridge.asyncio, "sleep", no_sleep)
    await bridge._run_configured_catalog_worker("third", "module:factory", "/cfg")
    assert managed.reconcile_calls == 2
    assert bridge._WORKER_CLEANUP_PENDING in capsys.readouterr().out


@pytest.mark.asyncio
@pytest.mark.parametrize("parent_cancels", [False, True])
async def test_configured_worker_monitors_parent_control_pipe(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    parent_cancels: bool,
) -> None:
    callbacks: list[object] = []
    removed: list[int] = []

    class Loop:
        def add_reader(self, fd: int, callback: object) -> None:
            assert fd == 7
            callbacks.append(callback)
            if parent_cancels:
                callback()  # type: ignore[operator]

        def remove_reader(self, fd: int) -> bool:
            removed.append(fd)
            return True

    class Plugin:
        async def get_model_catalog(self, **_kwargs: object) -> ModelCatalog:
            if parent_cancels:
                await asyncio.Event().wait()
            return ModelCatalog((), "configured", "Configured catalog")

    monkeypatch.setattr("src.coders._load_plugin", lambda *_args: Plugin())
    monkeypatch.setattr(bridge, "load_config", lambda _path: AppConfig())
    monkeypatch.setattr(bridge.sys, "stdin", SimpleNamespace(fileno=lambda: 7))
    monkeypatch.setattr(bridge.asyncio, "get_running_loop", lambda: Loop())

    if parent_cancels:
        with pytest.raises(asyncio.CancelledError):
            await bridge._run_configured_catalog_worker(
                "third",
                "module:factory",
                "/cfg",
                monitor_parent=True,
            )
    else:
        await bridge._run_configured_catalog_worker(
            "third",
            "module:factory",
            "/cfg",
            monitor_parent=True,
        )
        assert bridge._WORKER_RESULT_PREFIX in capsys.readouterr().out
    assert len(callbacks) == 1
    assert removed == [7]


@pytest.mark.asyncio
async def test_configured_worker_payload_bounds_and_skips_noise() -> None:
    class RaisingStdout:
        async def readline(self) -> bytes:
            raise ValueError

    for stdout in (
        None,
        RaisingStdout(),
        _CatalogWorkerStdout(b"x" * (bridge._CONFIGURED_WORKER_OUTPUT_BYTES + 1)),
    ):
        managed = SimpleNamespace(process=SimpleNamespace(stdout=stdout))
        assert await bridge._configured_worker_payload(managed) is None

    stdout = _CatalogWorkerStdout(
        b"noise\n" + bridge._WORKER_RESULT_PREFIX.encode() + b'{"ok":false}\n'
    )
    managed = SimpleNamespace(process=SimpleNamespace(stdout=stdout))
    assert await bridge._configured_worker_payload(managed) == {"ok": False}

    class FailedObservation:
        async def reconcile_cleanup(self, **_kwargs: object) -> CleanupResult:
            raise RuntimeError("unavailable")

    assert await bridge._observe_configured_worker(FailedObservation()) is None


@pytest.mark.asyncio
async def test_stop_configured_worker_preserves_pending_hold_after_broken_pipe() -> None:
    stdout = (
        bridge._WORKER_RESULT_PREFIX
        + json.dumps(
            {
                "ok": False,
                "error": "catalog unavailable",
                bridge._WORKER_CLEANUP_PENDING: True,
            },
            separators=(",", ":"),
        )
        + "\n"
    ).encode()
    process = _CatalogWorkerProcess(stdout)
    process.stdin = _CatalogWorkerStdin(broken=True)
    managed = _ManagedCatalogWorker(process)

    payload, cleanup_result = await bridge._stop_configured_worker(managed)

    assert payload == {"ok": False, "error": "catalog unavailable"}
    assert cleanup_result is managed.reconciliation
    assert managed.cleanup_calls == 0

    completed_process = _CatalogWorkerProcess(
        (
            bridge._WORKER_RESULT_PREFIX
            + '{"ok":false,"error":"catalog unavailable"}\n'
        ).encode()
    )
    completed = _ManagedCatalogWorker(completed_process)
    payload, cleanup_result = await bridge._stop_configured_worker(completed)
    assert payload == {"ok": False, "error": "catalog unavailable"}
    assert cleanup_result is completed.cleanup_result
    assert completed_process.reaped is True
    assert completed.cleanup_calls == 1


@pytest.mark.asyncio
async def test_worker_stop_handoff_survives_repeated_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    managed = SimpleNamespace()

    async def stop(_managed: object) -> tuple[None, None]:
        started.set()
        await release.wait()
        return None, None

    monkeypatch.setattr(bridge, "_stop_configured_worker", stop)
    task = asyncio.create_task(
        bridge._finish_configured_worker_stop(managed)  # type: ignore[arg-type]
    )
    await started.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    release.set()
    payload, cleanup_result, cancellation = await task
    assert payload is None
    assert cleanup_result is None
    assert isinstance(cancellation, asyncio.CancelledError)

    async def fail(_managed: object) -> tuple[None, None]:
        raise RuntimeError("stop failed")

    monkeypatch.setattr(bridge, "_stop_configured_worker", fail)
    assert await bridge._finish_configured_worker_stop(  # type: ignore[arg-type]
        managed
    ) == (None, None, None)


@pytest.mark.asyncio
async def test_catalog_timeout_propagates_cancellation_after_confirmed_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _CatalogWorkerProcess(b"")
    process.stdout = _CatalogWorkerStdout(b"", blocked=True)
    managed = _ManagedCatalogWorker(process)
    stop_started = asyncio.Event()
    release_stop = asyncio.Event()

    async def create(*_args: object, **_kwargs: object) -> _ManagedCatalogWorker:
        return managed

    async def stop(
        _managed: object,
    ) -> tuple[None, CleanupResult]:
        stop_started.set()
        await release_stop.wait()
        return None, _cleanup_result(quiescent=True)

    monkeypatch.setattr(bridge, "launch_process", create)
    monkeypatch.setattr(bridge, "_stop_configured_worker", stop)
    task = asyncio.create_task(
        bridge._isolated_configured_catalog(
            "third",
            "module:factory",
            config_path="/cfg",
            timeout_seconds=0.001,
        )
    )
    await stop_started.wait()
    task.cancel()
    release_stop.set()
    with pytest.raises(asyncio.CancelledError):
        await task


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
    process = _CatalogWorkerProcess(stdout)
    managed = _ManagedCatalogWorker(process)

    async def create(*args: object, **kwargs: object) -> _ManagedCatalogWorker:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return managed

    monkeypatch.setattr(bridge, "launch_process", create)
    result = await bridge._isolated_configured_catalog(
        "third",
        "module:factory",
        config_path="/cfg",
        credential_location="/reserved/credentials",
        env={
            "CODEX_HOME": "/reserved/credentials",
            "SECRET": "sensitive-environment-value",
        },
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
        "/reserved/credentials",
        bridge.EXPLICIT_ENVIRONMENT_ARG,
    )
    assert captured["kwargs"] == {
        "stdin": bridge.asyncio.subprocess.PIPE,
        "stdout": bridge.asyncio.subprocess.PIPE,
        "stderr": bridge.asyncio.subprocess.DEVNULL,
        "limit": bridge._CONFIGURED_WORKER_OUTPUT_BYTES + 1,
        "env": {
            "CODEX_HOME": "/reserved/credentials",
            "SECRET": "sensitive-environment-value",
        },
    }
    assert "sensitive-environment-value" not in " ".join(captured["args"])
    assert process.reaped is True
    assert managed.cleanup_calls == 1


@pytest.mark.asyncio
async def test_explicit_environment_crosses_real_configured_worker_boundary(
    tmp_path: Path,
) -> None:
    credential_location = tmp_path / "claude-auth"
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        f"auth:\n  claude_config_dir: {credential_location}\n",
        encoding="utf-8",
    )
    environment = {"CLAUDE_CONFIG_DIR": str(credential_location)}
    original = dict(environment)

    catalog = await bridge._isolated_configured_catalog(
        "third",
        (
            "tests.configured_coder_plugin:"
            "build_bound_environment_catalog_plugin"
        ),
        config_path=str(config_path),
        credential_location=str(credential_location),
        env=environment,
    )

    assert catalog.models[0].invocation_id == "bound-environment"
    assert environment == original


@pytest.mark.asyncio
async def test_isolated_configured_catalog_propagates_worker_hold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stdout = (
        bridge._WORKER_RESULT_PREFIX
        + json.dumps(
            {
                "ok": False,
                "error": "catalog unavailable",
                bridge._WORKER_CLEANUP_PENDING: True,
            },
            separators=(",", ":"),
        )
        + "\n"
    ).encode()
    managed = _ManagedCatalogWorker(_CatalogWorkerProcess(stdout))

    async def create(*_args: object, **_kwargs: object) -> _ManagedCatalogWorker:
        return managed

    monkeypatch.setattr(bridge, "launch_process", create)
    with pytest.raises(ModelCatalogUnavailable, match="cleanup is unresolved") as exc_info:
        await bridge._isolated_configured_catalog(
            "third", "module:factory", config_path="/cfg"
        )

    assert exc_info.value.managed is managed
    assert exc_info.value.cleanup_result is managed.reconciliation
    assert managed.cleanup_calls == 0


@pytest.mark.asyncio
async def test_isolated_configured_catalog_preserves_startup_cleanup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cleanup = _cleanup_result(quiescent=False)
    managed = SimpleNamespace()

    async def create(*_args: object, **_kwargs: object) -> object:
        raise ProcessLaunchCleanupError(
            "private startup detail",
            managed=managed,  # type: ignore[arg-type]
            cleanup_result=cleanup,
        )

    monkeypatch.setattr(bridge, "launch_process", create)
    with pytest.raises(ModelCatalogUnavailable, match="startup cleanup") as exc_info:
        await bridge._isolated_configured_catalog(
            "third", "module:factory", config_path="/cfg"
        )
    assert exc_info.value.managed is managed
    assert exc_info.value.cleanup_result is cleanup


@pytest.mark.asyncio
async def test_isolated_configured_catalog_reports_unconfirmed_final_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog = ModelCatalog((), "configured", "Configured catalog")
    stdout = (
        bridge._WORKER_RESULT_PREFIX
        + json.dumps(bridge._catalog_payload(catalog), separators=(",", ":"))
        + "\n"
    ).encode()
    failed = _cleanup_result(quiescent=False)
    managed = _ManagedCatalogWorker(
        _CatalogWorkerProcess(stdout), cleanup=failed
    )

    async def create(*_args: object, **_kwargs: object) -> _ManagedCatalogWorker:
        return managed

    monkeypatch.setattr(bridge, "launch_process", create)
    with pytest.raises(ModelCatalogUnavailable, match="cleanup is unresolved") as exc_info:
        await bridge._isolated_configured_catalog(
            "third", "module:factory", config_path="/cfg"
        )
    assert exc_info.value.managed is managed
    assert exc_info.value.cleanup_result is failed


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
    async def create(*_args: object, **_kwargs: object) -> _ManagedCatalogWorker:
        return _ManagedCatalogWorker(_CatalogWorkerProcess(stdout, returncode))

    monkeypatch.setattr(bridge, "launch_process", create)
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

    monkeypatch.setattr(bridge, "launch_process", failed_start)
    with pytest.raises(ModelCatalogUnavailable, match="failed to start"):
        await bridge._isolated_configured_catalog(
            "third", "module:factory", config_path="/cfg"
        )

    process = _CatalogWorkerProcess(b"")
    process.stdout = _CatalogWorkerStdout(b"", blocked=True)
    managed = _ManagedCatalogWorker(
        process,
        cleanup=_cleanup_result(quiescent=False),
    )
    monkeypatch.setattr(
        bridge, "_CONFIGURED_WORKER_CANCEL_GRACE_SECONDS", 0.001
    )

    async def create(*_args: object, **_kwargs: object) -> _ManagedCatalogWorker:
        return managed

    monkeypatch.setattr(bridge, "launch_process", create)
    with pytest.raises(ModelCatalogUnavailable, match="timed out") as exc_info:
        await bridge._isolated_configured_catalog(
            "third",
            "module:factory",
            config_path="/cfg",
            timeout_seconds=0.001,
        )
    assert exc_info.value.managed is managed

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
        with pytest.raises(ModelCatalogUnavailable, match="cancellation") as exc_info:
            await task
        assert exc_info.value.managed is managed

    await cancel_scenario()
    assert managed.cleanup_calls == 0
    assert process.stdin.writes == [b"\n", b"\n"]


@pytest.mark.asyncio
async def test_isolated_configured_catalog_propagates_cancel_after_quiescence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        bridge, "_CONFIGURED_WORKER_CANCEL_GRACE_SECONDS", 0.001
    )
    class CancelThenEof:
        def __init__(self) -> None:
            self.calls = 0

        async def readline(self) -> bytes:
            self.calls += 1
            if self.calls == 1:
                await asyncio.Event().wait()
            return b""

    process = _CatalogWorkerProcess(b"")
    process.stdout = CancelThenEof()
    managed = _ManagedCatalogWorker(
        process,
        reconciliation=_cleanup_result(quiescent=True),
    )

    async def create(*_args: object, **_kwargs: object) -> _ManagedCatalogWorker:
        return managed

    monkeypatch.setattr(bridge, "launch_process", create)
    task = asyncio.create_task(
        bridge._isolated_configured_catalog(
            "third", "module:factory", config_path="/cfg"
        )
    )
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert managed.cleanup_calls == 1


@pytest.mark.asyncio
async def test_daemon_handler_isolates_configured_catalog(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    redis = _BridgeRedis()
    registry = CoderRegistry()
    plugin = CodexPlugin(discover=lambda **_kwargs: None)
    registry.register(plugin, reference="operator.plugin:factory")
    monkeypatch.delenv("CODEX_HOME", raising=False)
    config_path = tmp_path / "config.yml"
    codex_home = tmp_path / "codex-home"
    config_path.write_text(
        f"auth:\n  codex_home_dir: {codex_home}\n",
        encoding="utf-8",
    )
    reservations = CoderCredentialReservations()
    calls: list[tuple[str, str, str, str | None, str | None]] = []

    async def isolated(
        plugin_id: str,
        reference: str,
        *,
        config_path: str,
        credential_location: str | None = None,
        env: dict[str, str] | None = None,
    ) -> ModelCatalog:
        calls.append(
            (
                plugin_id,
                reference,
                config_path,
                credential_location,
                None if env is None else env.get("CODEX_HOME"),
            )
        )
        return ModelCatalog((), "configured", "Isolated catalog.")

    monkeypatch.setattr(bridge, "_isolated_configured_catalog", isolated)
    await bridge.handle_model_catalog_request(
        redis,
        registry,
        json.dumps(
            {
                "request_id": "c" * 32,
                "plugin": "codex",
                "reference": "operator.plugin:factory",
                "expires_at": time.time() + 10,
            }
        ),
        config_path=str(config_path),
        credential_reservations=reservations,
    )

    expected_location = str(codex_home / ".codex")
    assert calls == [
        (
            "codex",
            "operator.plugin:factory",
            str(config_path),
            expected_location,
            expected_location,
        )
    ]
    response = json.loads(redis.values[bridge._response_key("c" * 32)])
    assert response["ok"] is True
    assert response["catalog"]["source"] == "configured"

    await bridge.handle_model_catalog_request(
        redis,
        registry,
        json.dumps(
            {
                "request_id": "d" * 32,
                "plugin": "codex",
                "reference": "old.plugin:factory",
                "expires_at": time.time() + 10,
            }
        ),
        config_path=str(config_path),
        credential_reservations=reservations,
    )
    assert len(calls) == 1
    assert json.loads(redis.values[bridge._response_key("d" * 32)]) == {
        "ok": False,
        "error": "plugin reference mismatch",
    }


@pytest.mark.asyncio
async def test_catalog_owner_blocks_replacement_until_quiescence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    redis = _BridgeRedis()
    registry = CoderRegistry()
    plugin = CodexPlugin(discover=lambda **_kwargs: None)
    reference = "src.coders.codex:CodexPlugin"
    registry.register(plugin, reference=reference)
    monkeypatch.delenv("CODEX_HOME", raising=False)
    codex_home = tmp_path / "codex-home"
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        f"auth:\n  codex_home_dir: {codex_home}\n",
        encoding="utf-8",
    )
    failed = _cleanup_result(quiescent=False)
    managed = _RetainedProcess(failed, _cleanup_result(quiescent=True))
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    calls = 0

    async def discover(**_kwargs: object) -> ModelCatalog:
        nonlocal calls
        calls += 1
        if calls == 1:
            first_started.set()
            await release_first.wait()
            raise ModelCatalogUnavailable(
                "provider detail must not cross Redis",
                managed=managed,  # type: ignore[arg-type]
                cleanup_result=failed,
            )
        return ModelCatalog((), "live", "Live catalog")

    monkeypatch.setattr(plugin, "get_model_catalog", discover)
    reservations = CoderCredentialReservations()
    owner = bridge.ModelCatalogProcessOwner()

    def request(request_id: str) -> str:
        return json.dumps(
            {
                "request_id": request_id,
                "plugin": "codex",
                "reference": reference,
                "expires_at": time.time() + 10,
            }
        )

    first = asyncio.create_task(
        bridge.handle_model_catalog_request(
            redis,
            registry,
            request("1" * 32),
            config_path=str(config_path),
            credential_reservations=reservations,
            process_owner=owner,
        )
    )
    await first_started.wait()
    second = asyncio.create_task(
        bridge.handle_model_catalog_request(
            redis,
            registry,
            request("2" * 32),
            config_path=str(config_path),
            credential_reservations=reservations,
            process_owner=owner,
        )
    )
    await asyncio.sleep(0)
    assert calls == 1
    release_first.set()
    await asyncio.gather(first, second)

    location = str(codex_home / ".codex")
    assert calls == 1
    assert managed.reconcile_calls == 1
    assert owner._holds["codex"].managed is managed
    assert reservations.reserve_login(location) is False
    for request_id in ("1" * 32, "2" * 32):
        assert json.loads(redis.values[bridge._response_key(request_id)]) == {
            "ok": False,
            "error": "catalog unavailable",
        }

    await bridge.handle_model_catalog_request(
        redis,
        registry,
        request("3" * 32),
        config_path=str(config_path),
        credential_reservations=reservations,
        process_owner=owner,
    )

    assert calls == 2
    assert managed.reconcile_calls == 2
    assert owner._holds == {}
    assert reservations.reserve_login(location) is True
    reservations.release_login(location)
    assert json.loads(redis.values[bridge._response_key("3" * 32)])["ok"] is True


@pytest.mark.asyncio
async def test_catalog_request_rechecks_expiry_after_plugin_lock(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    redis = _BridgeRedis()
    registry = CoderRegistry()
    plugin = CodexPlugin(discover=lambda **_kwargs: None)
    reference = "src.coders.codex:CodexPlugin"
    registry.register(plugin, reference=reference)
    calls = 0

    async def discover(**_kwargs: object) -> ModelCatalog:
        nonlocal calls
        calls += 1
        return ModelCatalog((), "live", "Live catalog")

    monkeypatch.setattr(plugin, "get_model_catalog", discover)
    owner = bridge.ModelCatalogProcessOwner()
    lock = owner.lock_for("codex")
    await lock.acquire()
    request_id = "7" * 32
    task = asyncio.create_task(
        bridge.handle_model_catalog_request(
            redis,
            registry,
            json.dumps(
                {
                    "request_id": request_id,
                    "plugin": "codex",
                    "reference": reference,
                    "expires_at": time.time() + 0.001,
                }
            ),
            config_path=str(tmp_path / "config.yml"),
            process_owner=owner,
        )
    )
    await asyncio.sleep(0.01)
    lock.release()
    await task

    assert calls == 0
    assert json.loads(redis.values[bridge._response_key(request_id)]) == {
        "ok": False,
        "error": "request expired",
    }


@pytest.mark.asyncio
async def test_catalog_owner_preserves_hold_through_cancellation_and_redis_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class FailingRedis(_BridgeRedis):
        async def set(
            self, key: str, value: str, *, ex: int | None = None
        ) -> None:
            raise ConnectionError("response store unavailable")

    redis = FailingRedis()
    registry = CoderRegistry()
    plugin = CodexPlugin(discover=lambda **_kwargs: None)
    reference = "src.coders.codex:CodexPlugin"
    registry.register(plugin, reference=reference)
    monkeypatch.delenv("CODEX_HOME", raising=False)
    codex_home = tmp_path / "codex-home"
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        f"auth:\n  codex_home_dir: {codex_home}\n",
        encoding="utf-8",
    )
    failed = _cleanup_result(quiescent=False)
    managed = _RetainedProcess(RuntimeError("observation unavailable"))
    started = asyncio.Event()

    async def discover(**_kwargs: object) -> ModelCatalog:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise ModelCatalogUnavailable(
                "cancelled cleanup detail",
                managed=managed,  # type: ignore[arg-type]
                cleanup_result=failed,
            ) from None
        raise AssertionError("unreachable")

    monkeypatch.setattr(plugin, "get_model_catalog", discover)
    reservations = CoderCredentialReservations()
    owner = bridge.ModelCatalogProcessOwner()
    task = asyncio.create_task(
        bridge.handle_model_catalog_request(
            redis,
            registry,
            json.dumps(
                {
                    "request_id": "4" * 32,
                    "plugin": "codex",
                    "reference": reference,
                    "expires_at": time.time() + 10,
                }
            ),
            config_path=str(config_path),
            credential_reservations=reservations,
            process_owner=owner,
        )
    )
    await started.wait()
    task.cancel()
    with pytest.raises(ConnectionError, match="response store unavailable"):
        await task

    location = str(codex_home / ".codex")
    assert owner._holds["codex"].managed is managed
    assert reservations.reserve_login(location) is False
    report = await owner.shutdown()
    assert report.unresolved_count == 1
    assert report.quiescent is False
    assert owner._holds["codex"].managed is managed


@pytest.mark.asyncio
async def test_catalog_timeout_preserves_unconfirmed_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    redis = _BridgeRedis()
    registry = CoderRegistry()
    plugin = CodexPlugin(discover=lambda **_kwargs: None)
    reference = "src.coders.codex:CodexPlugin"
    registry.register(plugin, reference=reference)
    monkeypatch.delenv("CODEX_HOME", raising=False)
    monkeypatch.setattr(bridge, "_CONFIGURED_CATALOG_TIMEOUT_SECONDS", 0.001)
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        f"auth:\n  codex_home_dir: {tmp_path / 'codex-home'}\n",
        encoding="utf-8",
    )
    failed = _cleanup_result(quiescent=False)
    managed = _RetainedProcess()

    async def discover(**_kwargs: object) -> ModelCatalog:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise ModelCatalogUnavailable(
                "timeout cleanup detail",
                managed=managed,  # type: ignore[arg-type]
                cleanup_result=failed,
            ) from None
        raise AssertionError("unreachable")

    monkeypatch.setattr(plugin, "get_model_catalog", discover)
    owner = bridge.ModelCatalogProcessOwner()
    await bridge.handle_model_catalog_request(
        redis,
        registry,
        json.dumps(
            {
                "request_id": "6" * 32,
                "plugin": "codex",
                "reference": reference,
                "expires_at": time.time() + 10,
            }
        ),
        config_path=str(config_path),
        process_owner=owner,
    )

    assert owner._holds["codex"].managed is managed
    assert json.loads(redis.values[bridge._response_key("6" * 32)]) == {
        "ok": False,
        "error": "catalog unavailable",
    }


@pytest.mark.asyncio
async def test_catalog_owner_releases_confirmed_clean_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    redis = _BridgeRedis()
    registry = CoderRegistry()
    plugin = CodexPlugin(discover=lambda **_kwargs: None)
    reference = "src.coders.codex:CodexPlugin"
    registry.register(plugin, reference=reference)
    monkeypatch.delenv("CODEX_HOME", raising=False)
    codex_home = tmp_path / "codex-home"
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        f"auth:\n  codex_home_dir: {codex_home}\n",
        encoding="utf-8",
    )
    managed = _RetainedProcess()

    async def discover(**_kwargs: object) -> ModelCatalog:
        raise ModelCatalogUnavailable(
            "ordinary provider failure",
            managed=managed,  # type: ignore[arg-type]
            cleanup_result=_cleanup_result(quiescent=True),
        )

    monkeypatch.setattr(plugin, "get_model_catalog", discover)
    reservations = CoderCredentialReservations()
    owner = bridge.ModelCatalogProcessOwner()
    await bridge.handle_model_catalog_request(
        redis,
        registry,
        json.dumps(
            {
                "request_id": "5" * 32,
                "plugin": "codex",
                "reference": reference,
                "expires_at": time.time() + 10,
            }
        ),
        config_path=str(config_path),
        credential_reservations=reservations,
        process_owner=owner,
    )

    location = str(codex_home / ".codex")
    assert owner._holds == {}
    assert managed.reconcile_calls == 0
    assert reservations.reserve_login(location) is True
    reservations.release_login(location)
    assert json.loads(redis.values[bridge._response_key("5" * 32)]) == {
        "ok": False,
        "error": "catalog unavailable",
    }


@pytest.mark.asyncio
async def test_catalog_owner_retains_unowned_failed_launch_reservation() -> None:
    failed = _cleanup_result(quiescent=False)
    reservations = CoderCredentialReservations()
    location = "/reserved/credentials"
    assert reservations.reserve_coder(location) is True
    owner = bridge.ModelCatalogProcessOwner()
    assert owner.retain_failure(
        "ordinary",
        ModelCatalogUnavailable("ordinary failure"),
        credential_location=None,
        credential_reservations=None,
    ) is False

    retained = owner.retain_failure(
        "third",
        ModelCatalogUnavailable("startup cleanup", cleanup_result=failed),
        credential_location=location,
        credential_reservations=reservations,
    )

    assert retained is True
    assert owner._holds["third"].managed is None
    async with owner.lock_for("third"):
        assert await owner.reconcile_before_launch("third") is False
    report = await owner.shutdown()
    assert report.unresolved_count == 1
    assert reservations.reserve_login(location) is False


@pytest.mark.asyncio
async def test_catalog_server_reports_unresolved_shutdown(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    redis = _BridgeRedis()
    registry = CoderRegistry()
    plugin = CodexPlugin(discover=lambda **_kwargs: None)
    reference = "src.coders.codex:CodexPlugin"
    registry.register(plugin, reference=reference)
    monkeypatch.delenv("CODEX_HOME", raising=False)
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        f"auth:\n  codex_home_dir: {tmp_path / 'codex-home'}\n",
        encoding="utf-8",
    )
    failed = _cleanup_result(quiescent=False)
    managed = _RetainedProcess(failed)

    async def discover(**_kwargs: object) -> ModelCatalog:
        raise ModelCatalogUnavailable(
            "must remain private",
            managed=managed,  # type: ignore[arg-type]
            cleanup_result=failed,
        )

    monkeypatch.setattr(plugin, "get_model_catalog", discover)
    owner = bridge.ModelCatalogProcessOwner()
    server = asyncio.create_task(
        bridge.serve_model_catalog_requests(
            redis,
            registry,
            config_path=str(config_path),
            credential_reservations=CoderCredentialReservations(),
            process_owner=owner,
        )
    )
    loader = bridge.DaemonModelCatalogLoader(
        redis, timeout_seconds=1, poll_interval_seconds=0
    )
    with pytest.raises(ModelCatalogUnavailable):
        await loader(plugin, config=AppConfig(), config_path=str(config_path))
    server.cancel()
    with caplog.at_level(logging.ERROR, logger=bridge.logger.name):
        with pytest.raises(asyncio.CancelledError):
            await server

    assert owner._holds["codex"].managed is managed
    assert "left 1 supervised cleanup hold(s) unresolved" in caplog.text
    assert "must remain private" not in caplog.text


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
                "reference": "src.coders.codex:CodexPlugin",
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
                "reference": "missing.module:factory",
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
async def test_daemon_server_dispatches_requests_concurrently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests = [
        (bridge.MODEL_CATALOG_REQUEST_QUEUE, b"first"),
        (bridge.MODEL_CATALOG_REQUEST_QUEUE, b"second"),
    ]
    both_started = asyncio.Event()
    release = asyncio.Event()
    active = 0
    maximum_active = 0

    class Redis:
        async def blpop(
            self, *_args: object, **_kwargs: object
        ) -> tuple[str, bytes] | None:
            if requests:
                return requests.pop(0)
            await release.wait()
            return None

    async def handle(
        *_args: object,
        **_kwargs: object,
    ) -> None:
        nonlocal active, maximum_active
        active += 1
        maximum_active = max(maximum_active, active)
        if active == 2:
            both_started.set()
        try:
            await release.wait()
        finally:
            active -= 1

    monkeypatch.setattr(bridge, "handle_model_catalog_request", handle)
    server = asyncio.create_task(
        bridge.serve_model_catalog_requests(
            Redis(),
            CoderRegistry(),
            config_path="config.yml",
        )
    )
    await asyncio.wait_for(both_started.wait(), timeout=1)
    assert maximum_active == 2

    server.cancel()
    with pytest.raises(asyncio.CancelledError):
        await server


@pytest.mark.asyncio
async def test_daemon_server_bounds_concurrent_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests = [
        (bridge.MODEL_CATALOG_REQUEST_QUEUE, str(index).encode())
        for index in range(bridge._MAX_CONCURRENT_REQUESTS + 1)
    ]
    at_capacity = asyncio.Event()
    final_started = asyncio.Event()
    release = asyncio.Event()
    started = 0

    class Redis:
        async def blpop(
            self, *_args: object, **_kwargs: object
        ) -> tuple[str, bytes] | None:
            if requests:
                return requests.pop(0)
            await asyncio.Future()

    async def handle(
        *_args: object,
        **_kwargs: object,
    ) -> None:
        nonlocal started
        started += 1
        if started == bridge._MAX_CONCURRENT_REQUESTS:
            at_capacity.set()
        if started == bridge._MAX_CONCURRENT_REQUESTS + 1:
            final_started.set()
            return
        await release.wait()

    monkeypatch.setattr(bridge, "handle_model_catalog_request", handle)
    server = asyncio.create_task(
        bridge.serve_model_catalog_requests(
            Redis(),
            CoderRegistry(),
            config_path="config.yml",
        )
    )
    await asyncio.wait_for(at_capacity.wait(), timeout=1)
    await asyncio.sleep(0)
    assert started == bridge._MAX_CONCURRENT_REQUESTS

    release.set()
    await asyncio.wait_for(final_started.wait(), timeout=1)
    server.cancel()
    with pytest.raises(asyncio.CancelledError):
        await server


@pytest.mark.asyncio
async def test_daemon_server_logs_request_task_failure(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    requests = [(bridge.MODEL_CATALOG_REQUEST_QUEUE, b"request")]
    failed = asyncio.Event()

    class Redis:
        async def blpop(
            self, *_args: object, **_kwargs: object
        ) -> tuple[str, bytes] | None:
            if requests:
                return requests.pop()
            await asyncio.Future()

    async def handle(*_args: object, **_kwargs: object) -> None:
        failed.set()
        raise RuntimeError("request failed")

    monkeypatch.setattr(bridge, "handle_model_catalog_request", handle)
    server = asyncio.create_task(
        bridge.serve_model_catalog_requests(
            Redis(),
            CoderRegistry(),
            config_path="config.yml",
        )
    )
    await asyncio.wait_for(failed.wait(), timeout=1)
    for _ in range(10):
        if "Model catalog bridge request failed" in caplog.text:
            break
        await asyncio.sleep(0)
    assert "Model catalog bridge request failed" in caplog.text

    server.cancel()
    with pytest.raises(asyncio.CancelledError):
        await server


@pytest.mark.asyncio
async def test_daemon_server_disables_bridge_without_redis_list_support() -> None:
    await bridge.serve_model_catalog_requests(
        object(),
        CoderRegistry(),
        config_path="config.yml",
    )


# Reuse the synthetic provider context and admission probe from manager tests.
browser_admission = login_tests.browser_admission
browser_preparation = login_tests.browser_preparation


def _browser_request(operation="start", **updates):
    return {
        "request_id": "f" * 32, "plugin": "claude",
        "operation": f"browser_login_{operation}",
        "reference": "src.coders.claude:ClaudePlugin", "expires_at": time.time() + 2,
        **({"replace_existing": False} if operation == "start" else {"session_id": "B" * 43}),
        **updates,
    }


async def _handle_browser(redis, registry, manager, request):
    await bridge.handle_model_catalog_request(
        redis, registry, json.dumps(request), config_path="/cfg", login_manager=manager,
    )


@pytest.fixture
async def browser_bridge(monkeypatch, browser_admission, tmp_path):
    manager, registry, _, context, _, _, _, _ = browser_admission
    factory = context["plugin"].create_browser_login

    def synthetic_factory(**kwargs):
        adapter = factory(**kwargs)
        object.__setattr__(adapter, "working_directory", str(tmp_path))
        return adapter

    monkeypatch.setattr(context["plugin"], "create_browser_login", synthetic_factory)
    redis = _BridgeRedis()
    monkeypatch.setattr(bridge, "CoderLoginSessionManager", lambda *_a, **_kw: manager)
    server = asyncio.create_task(bridge.serve_model_catalog_requests(redis, registry, config_path="/cfg"))
    loader = bridge.DaemonModelCatalogLoader(redis, timeout_seconds=1, poll_interval_seconds=.001)
    yield loader, redis
    server.cancel()
    with pytest.raises(asyncio.CancelledError):
        await server


@pytest.mark.parametrize("registry_change", ["replace", "remove"])
async def test_browser_bridge_owned_round_trip(browser_bridge, browser_admission, registry_change):
    loader, redis = browser_bridge
    manager, registry, reservations, context, _, _, settled, calls = browser_admission
    settled.set()
    reference = context["expected_reference"]
    started = await loader.start_browser_login("claude", expected_reference=reference, replace_existing=False)
    sid = started["session_id"]
    if registry_change == "replace":
        registry.register(ClaudePlugin(), reference="replacement")
    else:
        registry._plugins.clear()
    inspected = await loader.inspect_browser_login("claude", sid, expected_reference=reference)
    assert inspected["session_id"] == sid and inspected["login_method"] == "browser_code"
    cancelled = await loader.cancel_browser_login("claude", sid, expected_reference=reference)
    assert cancelled["state"] == "cancelled" and cancelled["cleanup_confirmed"] is True
    assert not reservations.login_active(context["credential_location"])
    assert len(calls) == 1 and len(manager._sessions) == 1
    assert len(redis.deleted) == 3 and not redis.values
    assert all(item[0] == bridge.MODEL_CATALOG_REQUEST_QUEUE for item in redis.removed)


@pytest.mark.parametrize("reason", list(bridge._BROWSER_REFUSALS))
async def test_browser_bridge_admission_refusals(browser_bridge, browser_admission, monkeypatch, reason):
    loader, _ = browser_bridge
    manager = browser_admission[0]

    async def refuse(*_a, **_kw):
        return CoderBrowserLoginFailure(reason, "synthetic-secret /data/secrets/private")

    monkeypatch.setattr(manager, "start_browser", refuse)
    result = await loader.start_browser_login(
        "claude", expected_reference="src.coders.claude:ClaudePlugin", replace_existing=True,
    )
    assert result == CoderBrowserLoginFailure(reason, bridge._BROWSER_REFUSALS[reason])
    assert "synthetic-secret" not in result.detail


@pytest.mark.parametrize("operation", ["inspect", "cancel"])
@pytest.mark.parametrize("lookup", ["missing", "plugin", "reference", "device"])
async def test_browser_lookup_refusals_do_not_cancel_device(
    browser_bridge, browser_admission, operation, lookup,
):
    loader, _ = browser_bridge
    manager, _, _, context, _, _, settled, _ = browser_admission
    settled.set()
    started = await loader.start_browser_login(
        "claude", expected_reference=context["expected_reference"], replace_existing=False,
    )
    sid = started["session_id"]
    session = manager._sessions[sid]
    original_deadline = session.application_deadline
    if lookup == "device":
        session.application_deadline = None
    method = getattr(loader, f"{operation}_browser_login")
    try:
        result = await method(
            "wrong" if lookup == "plugin" else "claude",
            "X" * 43 if lookup == "missing" else sid,
            expected_reference="wrong" if lookup == "reference" else context["expected_reference"],
        )
        assert isinstance(result, CoderBrowserLoginFailure)
        assert result.reason == ("session_not_found" if lookup == "missing" else "session_plugin_mismatch")
        assert not session.cancel_requested
    finally:
        session.application_deadline = original_deadline


@pytest.mark.parametrize("updates", [
    {"expires_at": True}, {"expires_at": float("nan")}, {"expires_at": float("inf")},
    {"expires_at": 10 ** 400}, {"expires_at": "1"}, {"operation": []},
    {"replace_existing": 1}, {"replace_existing": None}, {"reference": ""}, {"plugin": ""},
    {"authorization_code": "synthetic-secret"}, {"extra": "synthetic-secret"},
    {"operation": "browser_login_inspect", "session_id": []},
])
async def test_browser_invalid_request_has_no_effect(browser_admission, updates):
    manager, registry, _, _, _, _, _, calls = browser_admission
    redis = _BridgeRedis()
    await _handle_browser(redis, registry, manager, _browser_request(**updates))
    assert not redis.values and not calls and not manager._sessions


@pytest.mark.parametrize("sid", [None, "", "short", "B" * 129, "B" * 42 + "\n", []])
async def test_browser_request_bounds_session_id(browser_admission, sid):
    manager, registry, *_ = browser_admission
    redis = _BridgeRedis()
    await _handle_browser(redis, registry, manager, _browser_request("cancel", session_id=sid))
    assert not redis.values


@pytest.mark.parametrize("payload", [
    None, {}, {"ok": 1, "login": {}}, {"ok": True, "login": {}},
    {"ok": False, "reason": "unknown", "detail": "synthetic-secret"},
    {"ok": False, "reason": [], "detail": "synthetic-secret"},
    {"ok": False, "reason": "unsupported", "detail": "synthetic-secret"},
    {"ok": False, "reason": "unsupported", "detail": bridge._BROWSER_REFUSALS["unsupported"], "secret": "value"},
    {"ok": False, "error": "synthetic-secret"},
])
def test_browser_invalid_reply_is_sanitized(payload):
    with pytest.raises(ModelCatalogUnavailable, match="^Daemon returned invalid browser login status$"):
        bridge._parse_browser_login(payload, expected_plugin="claude")


@pytest.mark.parametrize("failure", ["absent", "exception", "invalid_snapshot", "unknown_refusal", "bad_reason"])
async def test_browser_daemon_failures_are_unavailable(browser_admission, monkeypatch, failure):
    manager, registry, *_ = browser_admission
    redis = _BridgeRedis()

    async def broken(*_a, **_kw):
        if failure == "exception":
            raise RuntimeError("synthetic-secret")
        if failure in {"unknown_refusal", "bad_reason"}:
            return CoderBrowserLoginFailure("unknown" if failure == "unknown_refusal" else [], "synthetic-secret")
        return {"state": "starting", "secret": "synthetic-secret"}

    monkeypatch.setattr(manager, "start_browser", broken)
    await _handle_browser(redis, registry, None if failure == "absent" else manager, _browser_request())
    reply = json.loads(redis.values[bridge._response_key("f" * 32)])
    assert reply == {"ok": False, "error": "coder login unavailable"}
    with pytest.raises(ModelCatalogUnavailable, match="^Daemon coder login is unavailable$"):
        bridge._parse_browser_login(reply, expected_plugin="claude")


async def test_browser_expired_request_does_not_admit(browser_admission):
    manager, registry, _, _, _, _, _, calls = browser_admission
    redis = _BridgeRedis()
    await _handle_browser(redis, registry, manager, _browser_request(expires_at=time.time() - 1))
    assert not calls and not manager._sessions
    with pytest.raises(ModelCatalogUnavailable, match="^Daemon coder login is unavailable$"):
        bridge._parse_browser_login(json.loads(redis.values[bridge._response_key("f" * 32)]), expected_plugin="claude")


@pytest.mark.parametrize("shared", [False, True])
async def test_browser_bridge_expiry_withdraws_only_waiter(browser_admission, shared):
    manager, registry, reservations, context, _, entered, settled, calls = browser_admission
    redis = _BridgeRedis()
    request = _browser_request(expires_at=time.time() + .03)
    expiring = asyncio.create_task(_handle_browser(redis, registry, manager, request))
    await entered.wait()
    other = None
    if shared:
        other = asyncio.create_task(manager.start_browser(
            "claude", expected_reference=context["expected_reference"], replace_existing=False,
        ))
        await asyncio.sleep(0)
    admission = manager._browser_admissions[context["credential_location"]]
    await expiring
    assert reservations.login_active(context["credential_location"])
    assert not admission.task.done() and len(calls) == 1
    assert admission.cancel_requested is (not shared)
    settled.set()
    result = await admission.task
    if other:
        snapshot = await other
        assert snapshot["session_id"] == result["session_id"]
        await manager.shutdown()
    else:
        assert result.reason == "cancelled" and not manager._sessions
    assert not reservations.login_active(context["credential_location"])


async def test_browser_response_write_failure_retry_reuses_ownership(browser_admission, monkeypatch):
    manager, registry, reservations, context, _, _, settled, calls = browser_admission
    redis = _BridgeRedis()
    settled.set()
    original = redis.set

    async def failed_write(*_a, **_kw):
        raise RuntimeError("synthetic-secret")

    monkeypatch.setattr(redis, "set", failed_write)
    with pytest.raises(RuntimeError):
        await _handle_browser(redis, registry, manager, _browser_request())
    session = next(iter(manager._sessions.values()))
    assert not session.cancel_requested and reservations.login_active(context["credential_location"])
    monkeypatch.setattr(redis, "set", original)
    await _handle_browser(redis, registry, manager, _browser_request())
    retry = bridge._parse_browser_login(
        json.loads(redis.values[bridge._response_key("f" * 32)]), expected_plugin="claude",
    )
    assert retry["session_id"] == session.session_id and retry["reused_session"]
    assert len(calls) == 1 and len(manager._sessions) == 1
    await manager.shutdown()


async def test_browser_bridge_cleanup_failed_retains_ownership(browser_bridge, browser_admission, monkeypatch):
    loader, _ = browser_bridge
    manager, _, reservations, context, _, _, settled, _ = browser_admission
    settled.set()
    reference = context["expected_reference"]
    started = await loader.start_browser_login("claude", expected_reference=reference, replace_existing=False)
    session = manager._sessions[started["session_id"]]
    session.state, session.cleanup_confirmed, session.failure_reason = "cleanup_failed", False, "cancellation_failed"
    original = manager._cleanup

    async def unresolved(_session):
        return False

    monkeypatch.setattr(manager, "_cleanup", unresolved)
    inspected = await loader.inspect_browser_login("claude", session.session_id, expected_reference=reference)
    cancelled = await loader.cancel_browser_login("claude", session.session_id, expected_reference=reference)
    for result in (inspected, cancelled):
        assert result["state"] == "cleanup_failed" and result["cleanup_confirmed"] is False
    assert reservations.login_active(context["credential_location"])
    monkeypatch.setattr(manager, "_cleanup", original)


@pytest.mark.parametrize("failure", ["push", "read", "timeout", "json"])
async def test_browser_loader_transport_failure_is_not_cleanup(failure):
    redis = _BridgeRedis()

    async def broken(*_a, **_kw):
        raise RuntimeError("synthetic-secret")

    if failure == "push":
        redis.rpush = broken
    elif failure == "read":
        redis.get = broken
    elif failure == "json":
        async def bad_json(_key):
            return "invalid synthetic-secret"
        redis.get = bad_json
    loader = bridge.DaemonModelCatalogLoader(redis, timeout_seconds=.01, poll_interval_seconds=0)
    with pytest.raises(ModelCatalogUnavailable) as caught:
        await loader.start_browser_login("claude", expected_reference="reference", replace_existing=False)
    assert "synthetic-secret" not in str(caught.value)


async def test_browser_start_does_not_reuse_device_owner(browser_admission):
    manager, registry, reservations, context, _, _, _, calls = browser_admission
    location = context["credential_location"]
    adapter = context["plugin"].create_browser_login(
        credential_location=location, environment=context["environment"],
        config_path=manager._config_path, observed_cli_version="2.1.126",
    )
    # The shared manager uses application_deadline to distinguish session methods.
    device = login_tests.coder_login._LoginSession(
        "D" * 43, "claude", context["expected_reference"], adapter, False, manager._monotonic(),
        plugin_identity=context["plugin"], reservation_held=True,
    )
    assert device.application_deadline is None and reservations.reserve_login(location)
    manager._sessions[device.session_id] = device
    redis = _BridgeRedis()
    await _handle_browser(redis, registry, manager, _browser_request())
    refusal = bridge._parse_browser_login(
        json.loads(redis.values[bridge._response_key("f" * 32)]), expected_plugin="claude",
    )
    assert refusal.reason == "credential_in_use" and not calls
    assert not device.cancel_requested and reservations.login_active(location)
    await manager.shutdown()


async def test_browser_snapshot_validation_and_field_selection(browser_admission):
    manager, registry, _, _, _, _, settled, _ = browser_admission
    settled.set()
    redis = _BridgeRedis()
    await _handle_browser(redis, registry, manager, _browser_request())
    payload = json.loads(redis.values[bridge._response_key("f" * 32)])
    try:
        extra = payload["login"] | {"authorization_code": "synthetic-secret", "environment": "private"}
        selected = bridge._browser_login_envelope(extra, expected_plugin="claude")
        assert selected == payload
        assert bridge._parse_browser_login(selected, expected_plugin="claude") == payload["login"]
        for field, value in [("plugin", "wrong"), ("state", []), ("cleanup_confirmed", True),
                             ("application_deadline", float("nan"))]:
            with pytest.raises(ModelCatalogUnavailable, match="^Daemon returned invalid browser login status$"):
                bridge._parse_browser_login(
                    {"ok": True, "login": payload["login"] | {field: value}}, expected_plugin="claude",
                )
    finally:
        await manager.shutdown()


@pytest.fixture
async def submission_bridge(monkeypatch, browser_preparation):
    manager, reservations, session, managed, channel, writes, completed = await login_tests._submittable_browser(
        monkeypatch, browser_preparation,
    )
    redis = _BridgeRedis()
    server = asyncio.create_task(bridge.serve_model_catalog_requests(redis, manager._registry, config_path="/cfg"))
    # Use the already owned synthetic session, retaining the real bridge dispatcher.
    monkeypatch.setattr(bridge, "CoderLoginSessionManager", lambda *_a, **_kw: manager)
    loader = bridge.DaemonModelCatalogLoader(redis, timeout_seconds=.3, poll_interval_seconds=.001)
    yield loader, redis, manager, session, managed, channel, writes, completed
    completed.set()
    await session.task
    server.cancel()
    with pytest.raises(asyncio.CancelledError):
        await server


async def _loader_submit(loader, session, code="synthetic-private-code", **kwargs):
    return await loader.submit_browser_login(
        kwargs.get("plugin", session.plugin), kwargs.get("sid", session.session_id),
        expected_reference=kwargs.get("reference", session.reference), authorization_code=code,
    )


def _private_request(redis, session, *, handle="a" * 64, **updates):
    request = _browser_request("submit", session_id=session.session_id, code_handle=handle, **updates)
    key = bridge._browser_handoff_key(handle)
    redis.values[key] = json.dumps({"plugin": session.plugin, "reference": session.reference,
                                   "session_id": session.session_id, "code": "synthetic-private-code"})
    return request, key


async def test_submit_loader_exact_roundtrip_and_one_shot(submission_bridge, caplog):
    loader, redis, manager, session, _, _, writes, _ = submission_bridge
    code = "  synthetic-private-code é + / =  "
    reply = await _loader_submit(loader, session, code)
    assert writes == [code.encode() + b"\n"]
    assert reply["state"] == "authorizing"
    assert bridge._parse_browser_login({"ok": True, "login": reply}, expected_plugin=session.plugin) == reply
    with pytest.raises(ModelCatalogUnavailable, match="^Daemon coder login is unavailable$"):
        await _loader_submit(loader, session)
    assert writes == [code.encode() + b"\n"]
    assert len(redis.handoffs) == 2 and all(ex == 30 and nx for _, ex, nx in redis.handoffs)
    assert all(key in redis.deleted for key, _, _ in redis.handoffs)
    assert not redis.values
    assert code not in json.dumps(redis.requests) + json.dumps(reply) + caplog.text
    assert all(set(json.loads(r)) == {"request_id", "plugin", "operation", "reference", "expires_at",
                                        "session_id", "code_handle"} for r in redis.requests)


@pytest.mark.parametrize("code", [None, 1, "", "  ", "x\n", "x\x00", "x\x7f", "x\u2028", "x\u2029",
                                  "\ud800", "é" * 2049])
async def test_submit_invalid_code_before_redis(submission_bridge, code):
    loader, redis, _, session, _, _, writes, _ = submission_bridge
    with pytest.raises(ValueError, match="^Invalid browser authorization code$"):
        await _loader_submit(loader, session, code)
    assert not redis.requests and not redis.handoffs and not writes


@pytest.mark.parametrize("context", [{"plugin": ""}, {"plugin": None}, {"reference": ""},
                                     {"reference": 1}, {"sid": "wrong"}, {"sid": None},
                                     {"reference": "x" * 17000}])
async def test_submit_invalid_context_before_redis(submission_bridge, context):
    loader, redis, _, session, _, _, writes, _ = submission_bridge
    with pytest.raises(ValueError, match="^Invalid browser login context$"):
        await _loader_submit(loader, session, **context)
    assert not redis.requests and not redis.handoffs and not writes


@pytest.mark.parametrize("bad", [None, b"\xff", "{", "[]", "x" * 16385, "é" * 9000,
                                '{"code":"\\ud800"}', 123, "wrong_plugin", "wrong_reference",
                                "wrong_session_id", "extra", "invalid_code", "oversized_code"])
async def test_submit_bad_payload_no_write(submission_bridge, bad, caplog):
    _, redis, manager, session, _, _, writes, _ = submission_bridge
    request, key = _private_request(redis, session)
    if isinstance(bad, str) and bad.startswith("wrong_"):
        payload = json.loads(redis.values[key])
        payload[bad.removeprefix("wrong_")] = "wrong"
        bad = json.dumps(payload)
    elif bad in ("extra", "invalid_code", "oversized_code"):
        payload = json.loads(redis.values[key])
        if bad == "extra":
            payload["extra"] = "synthetic-private-code"
        else:
            payload["code"] = "x\n" if bad == "invalid_code" else "x" * 4097
        bad = json.dumps(payload)
    redis.values[key] = bad
    await _handle_browser(redis, manager._registry, manager, request)
    reply = json.loads(redis.values[bridge._response_key(request["request_id"])])
    assert reply == {"ok": False, "error": "coder login unavailable"}
    assert not writes and not session.submission_claimed and key not in redis.values
    assert "synthetic-private-code" not in caplog.text + json.dumps(reply)


@pytest.mark.parametrize("change", ["plugin", "reference", "session", "method", "phase", "identity", "handle",
                                  "raw_key", "secret_field", "expired"])
async def test_submit_context_guard(submission_bridge, monkeypatch, change):
    _, redis, manager, session, _, _, writes, _ = submission_bridge
    request, key = _private_request(redis, session)
    if change in {"plugin", "reference"}:
        request[change] = "wrong"
    elif change == "session":
        request["session_id"] = "X" * 43
    elif change == "method":
        monkeypatch.setattr(session, "application_deadline", None)
    elif change == "phase":
        monkeypatch.setattr(session, "state", "authorizing")
    elif change == "identity":
        monkeypatch.setattr(manager._registry, "get_optional", lambda _name: ClaudePlugin())
    elif change == "handle":
        request["code_handle"] = "a" * 63
    elif change == "raw_key":
        request["code_handle"] = key
    elif change == "secret_field":
        request["code"] = "synthetic-private-code"
    else:
        request["expires_at"] = time.time() - 1
    await _handle_browser(redis, manager._registry, manager, request)
    assert not writes and not session.submission_claimed
    if change not in {"handle", "raw_key"}:
        assert key not in redis.values and key in redis.deleted


async def test_submit_concurrent_replay_consumes_once(submission_bridge):
    _, redis, manager, session, _, _, writes, _ = submission_bridge
    request, key = _private_request(redis, session)
    requests = [{**request, "request_id": str(i) * 32} for i in range(1, 4)]
    await asyncio.gather(*(_handle_browser(redis, manager._registry, manager, r) for r in requests))
    replies = [json.loads(redis.values[bridge._response_key(r["request_id"])]) for r in requests]
    assert sum(r["ok"] for r in replies) == 1
    assert all(r == {"ok": False, "error": "coder login unavailable"} for r in replies if not r["ok"])
    assert key not in redis.values and writes == [b"synthetic-private-code\n"]


@pytest.mark.parametrize("failure", ["set", "collision", "queue", "getdel", "getdel_after", "response", "timeout",
                                      "cancel", "delete"])
async def test_submit_transport_failures_never_resend(submission_bridge, monkeypatch, failure, caplog):
    loader, redis, manager, session, _, _, writes, _ = submission_bridge
    original_set, original_getdel = redis.set, redis.getdel
    marker = "synthetic-private-code"
    entered = asyncio.Event()

    async def broken(*_a, **_kw):
        raise RuntimeError(marker)

    async def set_value(key, value, **kwargs):
        if kwargs.get("nx"):
            redis.values[key] = value
            if failure == "set":
                raise RuntimeError(marker)
            if failure == "collision":
                return False
        elif failure == "response":
            raise RuntimeError(marker)
        return await original_set(key, value, **kwargs)

    if failure in {"set", "collision", "response"}:
        # For response failures, allow successful handoff admission.
        if failure == "response":
            async def set_value(key, value, **kwargs):
                if not kwargs.get("nx"):
                    raise RuntimeError(marker)
                return await original_set(key, value, **kwargs)
        monkeypatch.setattr(redis, "set", set_value)
    elif failure == "queue":
        monkeypatch.setattr(redis, "rpush", broken)
    elif failure in {"getdel", "getdel_after"}:
        async def getdel(key):
            if failure == "getdel_after":
                await original_getdel(key)
            raise RuntimeError(marker)
        monkeypatch.setattr(redis, "getdel", getdel)
    elif failure in {"timeout", "cancel"}:
        async def getdel(key):
            entered.set()
            await asyncio.Event().wait()
        monkeypatch.setattr(redis, "getdel", getdel)
    else:
        monkeypatch.setattr(redis, "delete", broken)
    if failure == "cancel":
        task = asyncio.create_task(_loader_submit(loader, session))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    elif failure == "delete":
        assert (await _loader_submit(loader, session))["state"] == "authorizing"
    else:
        with pytest.raises(ModelCatalogUnavailable, match="^Daemon coder login is unavailable$"):
            await _loader_submit(loader, session)
    assert len(redis.requests) <= 1 and len(writes) <= 1
    assert bool(writes) == (failure in {"response", "delete"})
    assert marker not in caplog.text
    if failure not in {"collision", "delete"}:
        assert not any(key.startswith(bridge._BROWSER_HANDOFF_PREFIX) for key in redis.values)


async def test_submit_rechecks_expiry_after_getdel(submission_bridge, monkeypatch):
    _, redis, manager, session, _, _, writes, _ = submission_bridge
    request, key = _private_request(redis, session)
    original = redis.getdel

    async def expired(key):
        result = await original(key)
        monkeypatch.setattr(bridge.time, "time", lambda: request["expires_at"] + 1)
        return result

    monkeypatch.setattr(redis, "getdel", expired)
    await _handle_browser(redis, manager._registry, manager, request)
    assert not writes and not session.submission_claimed and key not in redis.values


async def test_submit_real_pty_exact_input(browser_bridge, browser_admission, monkeypatch, tmp_path):
    loader, redis = browser_bridge
    manager, _, _, context, auth, _, settled, _ = browser_admission
    code = "  synthetic-real-pty é +/=  "
    factory = context["plugin"].create_browser_login
    ready = asyncio.Event()
    observe = manager._observe_browser_output

    def observed(session):
        observe(session)
        if session.state == "waiting_for_code":
            ready.set()

    def synthetic(**kwargs):
        adapter = factory(**kwargs)
        script = (
            "import os\n"
            f"os.write(1,({login_tests._BROWSER_URL!r}+'\\nPaste code here if prompted >').encode())\n"
            "received=b''\n"
            "while not received.endswith(b'\\n'): received+=os.read(0,1)\n"
            f"assert received=={(code.encode() + bytes([10]))!r}\n"
        )
        object.__setattr__(adapter, "command", (sys.executable, "-c", script))
        object.__setattr__(adapter, "working_directory", str(tmp_path))
        return adapter

    monkeypatch.setattr(context["plugin"], "create_browser_login", synthetic)
    monkeypatch.setattr(manager, "_observe_browser_output", observed)
    settled.set()
    started = await loader.start_browser_login(
        "claude", expected_reference=context["expected_reference"], replace_existing=False,
    )
    await asyncio.wait_for(ready.wait(), 3)
    session = manager._sessions[started["session_id"]]
    auth.update(status="ok", saved_credentials_present=True, failure_reason=None)
    await _loader_submit(loader, session, code)
    await asyncio.wait_for(session.task, 3)
    final = await loader.inspect_browser_login("claude", session.session_id, expected_reference=session.reference)
    assert final["state"] == "succeeded" and final["cleanup_confirmed"] is True
    assert code not in json.dumps(redis.requests) + json.dumps(final)


async def test_submit_public_wrapper_refuses_wrong_context(submission_bridge):
    _, _, manager, session, _, _, writes, _ = submission_bridge
    result = await manager.submit_browser_code(
        "wrong", session.session_id, expected_reference=session.reference, authorization_code="synthetic-private-code",
    )
    assert isinstance(result, CoderBrowserLoginFailure) and not writes


@pytest.mark.parametrize("action", ["partial_failure", "request_expiry", "daemon_cancel"])
@pytest.mark.parametrize("quiescent", [True, False])
async def test_submit_writer_failure_preserves_ownership(submission_bridge, action, quiescent, caplog):
    _, redis, manager, session, managed, channel, writes, completed = submission_bridge
    managed.quiescent = quiescent
    entered = asyncio.Event()

    async def write(data):
        writes.append(data[:3])
        entered.set()
        if action == "partial_failure":
            raise RuntimeError("synthetic-private-code")
        await asyncio.Event().wait()

    channel.write = write
    request, key = _private_request(redis, session, expires_at=time.time() + .03)
    task = asyncio.create_task(_handle_browser(redis, manager._registry, manager, request))
    await entered.wait()
    if action == "daemon_cancel":
        task.cancel()
    if action == "daemon_cancel":
        with pytest.raises(asyncio.CancelledError):
            await task
        assert bridge._response_key(request["request_id"]) not in redis.values
        reply = bridge._browser_login_envelope(await manager.inspect_browser(
            "claude", session.session_id, expected_reference=session.reference,
        ), expected_plugin="claude")
    else:
        await task
        reply = json.loads(redis.values[bridge._response_key(request["request_id"])])
    assert reply["ok"] is True
    assert reply["login"]["cleanup_confirmed"] is quiescent
    expected = ("failed" if action == "partial_failure" else "cancelled") if quiescent else "cleanup_failed"
    assert reply["login"]["state"] == expected
    assert session.submission_claimed and not session.code_delivered and len(writes) == 1
    assert key not in redis.values
    if not quiescent:
        assert session.managed is managed and session.pty_channel is channel and not channel.closed
        assert manager._credential_reservations.login_active(session.adapter.credential_location)
    assert "synthetic-private-code" not in caplog.text + json.dumps(reply)
    completed.set()
    await session.task
    managed.reconcile_quiescent = True
    await manager.cancel_browser("claude", session.session_id, expected_reference=session.reference)


@pytest.mark.parametrize("failure", ["absent_manager", "inspection_failure", "delete_failure"])
async def test_submit_daemon_abandoned_key_cleanup(submission_bridge, monkeypatch, failure, caplog):
    _, redis, manager, session, _, _, writes, _ = submission_bridge
    request, key = _private_request(redis, session)
    request["session_id"] = "X" * 43

    async def broken(*_a, **_kw):
        raise RuntimeError("synthetic-private-code")

    if failure == "inspection_failure":
        monkeypatch.setattr(manager, "inspect_browser", broken)
    elif failure == "delete_failure":
        monkeypatch.setattr(redis, "delete", broken)
    await _handle_browser(redis, manager._registry, None if failure == "absent_manager" else manager, request)
    assert not writes and not session.submission_claimed
    assert (key in redis.values) == (failure == "delete_failure")
    assert "synthetic-private-code" not in caplog.text


class _CleanupStallRedis(_BridgeRedis):
    def __init__(self, stalls, *, fail=False):
        super().__init__()
        self.stalls = stalls
        self.fail = fail
        self.read_started = asyncio.Event()
        self.release = asyncio.Event()
        self.attempted = []
        self.interrupted = []

    async def get(self, key):
        self.read_started.set()
        # These tests require cancellation before cleanup starts. An immediate
        # read can let the polling deadline expire before the timeout callback.
        await asyncio.Future()

    async def _cleanup(self, operation):
        self.attempted.append(operation)
        if operation in self.stalls:
            # These calls start after request/caller cancellation has reached finally.
            if operation != "handoff":
                assert asyncio.current_task().cancelling() >= 1
            if self.fail:
                raise RuntimeError("synthetic-private-code")
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.interrupted.append(operation)
                raise

    async def lrem(self, key, count, value):
        await self._cleanup("lrem")
        return await super().lrem(key, count, value)

    async def delete(self, key):
        operation = "handoff" if key.startswith(bridge._BROWSER_HANDOFF_PREFIX) else "response"
        await self._cleanup(operation)
        return await super().delete(key)


@pytest.mark.parametrize("stalls", [{"lrem"}, {"response"}, {"lrem", "response", "handoff"}])
@pytest.mark.parametrize("termination", ["timeout", "caller_cancel"])
async def test_submit_stalled_rpc_cleanup_reaches_handoff(stalls, termination, caplog):
    redis = _CleanupStallRedis(stalls)
    request_timeout = .02 if termination == "timeout" else 5
    loader = bridge.DaemonModelCatalogLoader(redis, timeout_seconds=request_timeout, poll_interval_seconds=.001)
    task = asyncio.create_task(loader.submit_browser_login(
        "claude", "A" * 43, expected_reference="src.coders.claude:ClaudePlugin",
        authorization_code="synthetic-private-code",
    ))
    try:
        await redis.read_started.wait()
        if termination == "caller_cancel":
            task.cancel()  # The sole external cancellation; observing completion must not cancel again.
        started = time.monotonic()
        # RPC cleanup: 2 x 0.1s. Private handoff cleanup: 1s. Allow scheduling slack.
        budget = (request_timeout if termination == "timeout" else 0) + 1.2
        done, _ = await asyncio.wait({task}, timeout=budget + .2)
        assert task in done, "Submission exceeded its request timeout and bounded cleanup budget"
        assert time.monotonic() - started <= budget + .2
        if termination == "caller_cancel":
            assert task.cancelled()
        else:
            with pytest.raises(ModelCatalogUnavailable, match="^Daemon coder login is unavailable$") as failure:
                task.result()
            assert "synthetic-private-code" not in str(failure.value)
        assert redis.attempted == ["lrem", "response", "handoff"]
        assert set(redis.interrupted) == stalls
        assert len(redis.requests) == len(redis.handoffs) == 1
        key, ttl, nx = redis.handoffs[0]
        assert ttl == 30 and nx
        assert (key in redis.values) == ("handoff" in stalls)
        assert "synthetic-private-code" not in caplog.text + json.dumps(redis.requests)
    finally:
        # Release doubles even on failure; never hide a hang with a second cancellation.
        redis.release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("termination", ["timeout", "caller_cancel"])
async def test_submit_rpc_cleanup_exceptions_are_private(termination, caplog):
    redis = _CleanupStallRedis({"lrem", "response"}, fail=True)
    loader = bridge.DaemonModelCatalogLoader(
        redis, timeout_seconds=.02 if termination == "timeout" else 5, poll_interval_seconds=.001,
    )
    task = asyncio.create_task(loader.submit_browser_login(
        "claude", "A" * 43, expected_reference="src.coders.claude:ClaudePlugin",
        authorization_code="synthetic-private-code",
    ))
    try:
        await redis.read_started.wait()
        if termination == "caller_cancel":
            task.cancel()
        done, _ = await asyncio.wait({task}, timeout=1.4)
        assert task in done
        if termination == "caller_cancel":
            assert task.cancelled()
        else:
            with pytest.raises(ModelCatalogUnavailable, match="^Daemon coder login is unavailable$"):
                task.result()
        assert redis.attempted == ["lrem", "response", "handoff"]
        assert redis.handoffs[0][0] in redis.deleted and not redis.values
        assert len(redis.requests) == len(redis.handoffs) == 1
        assert "synthetic-private-code" not in caplog.text + json.dumps(redis.requests)
    finally:
        redis.release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_submit_caller_cancel_during_private_cleanup_propagates(submission_bridge, monkeypatch, caplog):
    loader, redis, _, session, _, _, writes, _ = submission_bridge
    entered, release = asyncio.Event(), asyncio.Event()
    original_delete = redis.delete
    task = None

    async def delete(key):
        if key.startswith(bridge._BROWSER_HANDOFF_PREFIX) and asyncio.current_task() is task:
            entered.set()
            await release.wait()
        await original_delete(key)

    monkeypatch.setattr(redis, "delete", delete)
    task = asyncio.create_task(_loader_submit(loader, session))
    try:
        await entered.wait()
        task.cancel()  # Cancel exactly once, specifically during client private-key deletion.
        done, _ = await asyncio.wait({task}, timeout=.2)
        assert task in done and task.cancelled()
        assert session.submission_claimed and session.code_delivered
        assert writes == [b"synthetic-private-code\n"] and len(redis.requests) == 1
        assert "synthetic-private-code" not in caplog.text + json.dumps(redis.requests)
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("path", ["expired", "inspection_error"])
async def test_daemon_cancel_during_handoff_cleanup_skips_response(
    submission_bridge, monkeypatch, path, caplog,
):
    _, redis, manager, session, _, _, writes, _ = submission_bridge
    entered, release = asyncio.Event(), asyncio.Event()
    original_delete = redis.delete
    responses = []
    request, key = _private_request(
        redis, session, expires_at=time.time() + (-1 if path == "expired" else 5),
    )

    async def delete(key):
        if key.startswith(bridge._BROWSER_HANDOFF_PREFIX):
            entered.set()
            await release.wait()
        await original_delete(key)

    async def response(key, value, **kwargs):
        responses.append(key)
        await release.wait()

    async def inspect(*_a, **_kw):
        raise RuntimeError("synthetic-private-code")

    monkeypatch.setattr(redis, "delete", delete)
    monkeypatch.setattr(redis, "set", response)
    if path == "inspection_error":
        monkeypatch.setattr(manager, "inspect_browser", inspect)
    task = asyncio.create_task(_handle_browser(redis, manager._registry, manager, request))
    try:
        await entered.wait()
        task.cancel()  # Simulate service shutdown while daemon handoff deletion is pending.
        done, _ = await asyncio.wait({task}, timeout=.2)
        assert task in done and task.cancelled()
        assert not responses and not writes and not session.submission_claimed
        assert key in redis.values  # Interrupted deletion leaves the existing TTL as fallback.
        assert "synthetic-private-code" not in caplog.text
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)



@pytest.mark.parametrize("operation", ["browser_login_start", "browser_login_cancel", "device_login_start",
                                       "device_login_cancel", "catalog", "metadata", "auth"])
@pytest.mark.parametrize("stall", ["lrem", "response"])
async def test_non_submit_rpc_preserves_existing_cleanup(operation, stall):
    redis = _CleanupStallRedis({stall})
    original_lrem = redis.lrem

    async def withdraw(key, count, value):
        await original_lrem(key, count, value)
        queued = redis.queue.get_nowait()
        assert queued == (key, value.encode())

    redis.lrem = withdraw
    loader = bridge.DaemonModelCatalogLoader(redis, timeout_seconds=5, poll_interval_seconds=.001)
    task = asyncio.create_task(loader._request(
        "claude", operation=operation, expected_reference="src.coders.claude:ClaudePlugin",
    ))
    try:
        await redis.read_started.wait()
        task.cancel()  # Withdrawal must finish despite taking longer than submit cleanup permits.
        done, _ = await asyncio.wait({task}, timeout=.15)
        assert not done and not redis.interrupted
        if stall == "lrem":
            assert not redis.queue.empty()
        redis.release.set()
        done, _ = await asyncio.wait({task}, timeout=.2)
        assert task in done and task.cancelled() and redis.queue.empty()
        assert redis.attempted == ["lrem", "response"]
        assert not redis.interrupted and len(redis.requests) == 1
    finally:
        redis.release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("updates", [
    {"extra": True}, {"code": "synthetic-private-code"}, {"request_id": "bad"}, {"plugin": []},
    {"plugin": ""}, {"reference": None}, {"reference": ""}, {"expires_at": None},
    {"expires_at": float("inf")}, {"session_id": "bad"}, {"session_id": None},
])
async def test_submit_validation_rejection_discards_valid_handle(submission_bridge, updates, caplog):
    _, redis, manager, session, _, _, writes, _ = submission_bridge
    request, key = _private_request(redis, session)
    request.update(updates)
    await _handle_browser(redis, manager._registry, manager, request)
    assert key in redis.deleted and key not in redis.values
    assert not writes and not session.submission_claimed
    assert not any(k.startswith(bridge._RESPONSE_PREFIX) for k in redis.values)
    assert "synthetic-private-code" not in caplog.text


@pytest.mark.parametrize("handle", [None, "wrong", "arbitrary:redis:key"])
async def test_invalid_submit_metadata_cannot_delete_arbitrary_key(submission_bridge, handle):
    _, redis, manager, session, _, _, writes, _ = submission_bridge
    redis.values["arbitrary:redis:key"] = "synthetic-unrelated-value"
    request = _browser_request("submit", request_id="bad", code_handle=handle)
    await _handle_browser(redis, manager._registry, manager, request)
    assert not redis.deleted and redis.values["arbitrary:redis:key"] == "synthetic-unrelated-value"
    assert not writes and not session.submission_claimed


async def test_daemon_shutdown_during_submission_skips_stalled_response(monkeypatch, browser_preparation, caplog):
    manager, _, session, _, channel, writes, completed = await login_tests._submittable_browser(
        monkeypatch, browser_preparation,
    )
    redis = _BridgeRedis()
    entered, release = asyncio.Event(), asyncio.Event()
    responses = []

    async def write(data):
        writes.append(data[:3])
        entered.set()
        await release.wait()

    async def response(key, value, **kwargs):
        responses.append(key)
        await release.wait()

    channel.write = write
    monkeypatch.setattr(redis, "set", response)
    monkeypatch.setattr(bridge, "CoderLoginSessionManager", lambda *_a, **_kw: manager)
    request, key = _private_request(redis, session, expires_at=time.time() + 5)
    server = asyncio.create_task(bridge.serve_model_catalog_requests(redis, manager._registry, config_path="/cfg"))
    try:
        await redis.rpush(bridge.MODEL_CATALOG_REQUEST_QUEUE, json.dumps(request))
        await entered.wait()
        server.cancel()  # The sole external shutdown cancellation; it cancels the owned request once.
        done, _ = await asyncio.wait({server}, timeout=.4)
        assert server in done and server.cancelled(), "Shutdown entered a stalled response write"
        assert not responses and key not in redis.values and key in redis.deleted
        assert len(writes) == len(redis.requests) == 1 and session.submission_claimed
        assert "synthetic-private-code" not in caplog.text
    finally:
        release.set()
        completed.set()
        await asyncio.gather(server, return_exceptions=True)
        await manager.shutdown()
