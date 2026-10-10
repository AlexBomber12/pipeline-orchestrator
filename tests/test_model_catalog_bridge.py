"""Tests for the Redis bridge that keeps model discovery in the daemon."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from src import model_catalog_bridge as bridge
from src.coder_login import CoderCredentialReservations
from src.coder_registry import (
    CoderAuthCapabilities,
    CoderAuthStatus,
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

    async def discover(**_kwargs: object) -> tuple[CodexModel, ...]:
        assert reservations.reserve_login(str(codex_home / ".codex")) is False
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

    async def discover(**kwargs: object) -> tuple[ModelMetadata, ...]:
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
async def test_configured_catalog_worker_response_redacts_failures(
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
    monkeypatch.setattr(bridge, "load_config", lambda _path: AppConfig())
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
    ) == {"ok": False, "error": "catalog unavailable"}
    assert plugin.calls == 1

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

    async def response(*_args: object, **_kwargs: object) -> None:
        print(
            bridge._WORKER_RESULT_PREFIX
            + '{"ok":false,"error":"unavailable"}'
        )

    monkeypatch.setattr(bridge, "_run_configured_catalog_worker", response)
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
        env={"CODEX_HOME": "/reserved/credentials"},
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
    )
    assert captured["kwargs"] == {
        "stdin": bridge.asyncio.subprocess.PIPE,
        "stdout": bridge.asyncio.subprocess.PIPE,
        "stderr": bridge.asyncio.subprocess.DEVNULL,
        "limit": bridge._CONFIGURED_WORKER_OUTPUT_BYTES + 1,
        "env": {"CODEX_HOME": "/reserved/credentials"},
    }
    assert process.reaped is True
    assert managed.cleanup_calls == 1


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
