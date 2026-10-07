"""Redis request/response bridge for daemon-owned coder plugin operations."""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
import uuid
from dataclasses import asdict
from typing import Any, Callable

from src.coder_auth import isolated_auth_probe, terminate_plugin_worker
from src.coder_login import CoderCredentialReservations, CoderLoginSessionManager
from src.coder_registry import (
    CoderAuthStatus,
    CoderMetadataView,
    CoderPlugin,
    CoderRegistry,
    ModelCatalog,
    ModelCatalogUnavailable,
    ModelMetadata,
    ModelReasoningEffort,
    ModelSetting,
    coder_auth_payload,
    parse_coder_auth_payload,
    parse_coder_device_login_payload,
)
from src.config import DEFAULT_CODER_PLUGINS, AppConfig, load_config

logger = logging.getLogger(__name__)

MODEL_CATALOG_REQUEST_QUEUE = "orchestrator:model-catalog:requests"
_RESPONSE_PREFIX = "orchestrator:model-catalog:response"
_RESPONSE_TTL_SECONDS = 30
_REQUEST_TIMEOUT_SECONDS = 7.0
_POLL_INTERVAL_SECONDS = 0.05
_MAX_PENDING_REQUESTS = 64
_MAX_CONCURRENT_REQUESTS = 8
_CONFIGURED_CATALOG_TIMEOUT_SECONDS = 5.0
_WORKER_RESULT_PREFIX = "PIPELINE_CATALOG_RESULT:"


def _response_key(request_id: str) -> str:
    return f"{_RESPONSE_PREFIX}:{request_id}"


def _catalog_payload(catalog: ModelCatalog) -> dict[str, Any]:
    return {
        "ok": True,
        "catalog": {
            "models": [asdict(model) for model in catalog.models],
            "source": catalog.source,
            "description": catalog.description,
        },
    }


def _plugin_metadata_payload(plugin: CoderPlugin) -> dict[str, Any]:
    setting = plugin.model_setting
    return {
        "ok": True,
        "metadata": {
            "name": plugin.name,
            "display_name": plugin.display_name,
            "models": plugin.models,
            "model_setting": asdict(setting),
            "model_catalog_refreshable": plugin.model_catalog_refreshable,
        },
    }


def _parse_auth_status(payload: object) -> dict[str, Any]:
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        raise ModelCatalogUnavailable("Daemon coder auth status is unavailable")
    try:
        return parse_coder_auth_payload(payload.get("auth"))
    except TypeError:
        raise ModelCatalogUnavailable("Daemon returned invalid coder auth status")


def _parse_device_login(
    payload: object, *, expected_plugin: str
) -> dict[str, Any]:
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        raise ModelCatalogUnavailable("Daemon coder login is unavailable")
    try:
        return parse_coder_device_login_payload(
            payload.get("login"), expected_plugin=expected_plugin
        )
    except TypeError:
        raise ModelCatalogUnavailable(
            "Daemon returned invalid coder login status"
        ) from None


def _parse_plugin_metadata(
    payload: object,
    *,
    expected_name: str,
) -> CoderMetadataView:
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        raise ModelCatalogUnavailable("Daemon coder metadata is unavailable")
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        raise ModelCatalogUnavailable("Daemon returned invalid coder metadata")
    name = metadata.get("name")
    display_name = metadata.get("display_name")
    models = metadata.get("models")
    raw_setting = metadata.get("model_setting")
    refreshable = metadata.get("model_catalog_refreshable")
    if (
        name != expected_name
        or not isinstance(display_name, str)
        or not display_name
        or not isinstance(models, list)
        or not all(isinstance(model, str) for model in models)
        or not isinstance(raw_setting, dict)
        or not isinstance(refreshable, bool)
    ):
        raise ModelCatalogUnavailable("Daemon returned invalid coder metadata")
    config_field = raw_setting.get("config_field")
    default_value = raw_setting.get("default_value")
    default_label = raw_setting.get("default_label")
    setting_key = raw_setting.get("setting_key")
    if (
        (config_field is not None and not isinstance(config_field, str))
        or not isinstance(default_value, str)
        or not isinstance(default_label, str)
        or not isinstance(setting_key, str)
    ):
        raise ModelCatalogUnavailable("Daemon returned invalid coder metadata")
    return CoderMetadataView(
        name=name,
        display_name=display_name,
        models=list(models),
        model_setting=ModelSetting(
            config_field=config_field,
            default_value=default_value,
            default_label=default_label,
            setting_key=setting_key,
        ),
        model_catalog_refreshable=refreshable,
    )


def _parse_catalog(payload: object) -> ModelCatalog:
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        raise ModelCatalogUnavailable("Daemon model catalog is unavailable")
    raw_catalog = payload.get("catalog")
    if not isinstance(raw_catalog, dict):
        raise ModelCatalogUnavailable("Daemon returned invalid model metadata")
    raw_models = raw_catalog.get("models")
    source = raw_catalog.get("source")
    description = raw_catalog.get("description")
    if (
        not isinstance(raw_models, list)
        or not isinstance(source, str)
        or not isinstance(description, str)
    ):
        raise ModelCatalogUnavailable("Daemon returned invalid model metadata")
    try:
        models = tuple(
            ModelMetadata(
                invocation_id=item["invocation_id"],
                display_name=item["display_name"],
                is_default=item["is_default"],
                default_reasoning_effort=item["default_reasoning_effort"],
                reasoning_efforts=tuple(
                    ModelReasoningEffort(**effort)
                    for effort in item["reasoning_efforts"]
                ),
            )
            for item in raw_models
        )
    except (KeyError, TypeError):
        raise ModelCatalogUnavailable(
            "Daemon returned invalid model metadata"
        ) from None
    return ModelCatalog(models, source, description)


async def _configured_catalog_worker_response(
    plugin_id: str,
    reference: str,
    config_path: str,
) -> dict[str, Any]:
    """Load one configured plugin and serialize its catalog in a worker."""
    try:
        from src.coders import _load_plugin

        plugin = _load_plugin(plugin_id, reference)
        catalog = await plugin.get_model_catalog(
            config=load_config(config_path),
            config_path=config_path,
        )
    except Exception:
        return {"ok": False, "error": "catalog unavailable"}
    return _catalog_payload(catalog)


def _configured_catalog_worker_main() -> None:
    """Subprocess entry point for configured model catalog discovery."""
    if len(sys.argv) != 5 or sys.argv[1] != "--configured-worker":
        raise SystemExit(2)
    result = asyncio.run(
        _configured_catalog_worker_response(
            sys.argv[2],
            sys.argv[3],
            sys.argv[4],
        )
    )
    print(
        f"{_WORKER_RESULT_PREFIX}"
        f"{json.dumps(result, separators=(',', ':'))}"
    )


async def _isolated_configured_catalog(
    plugin_id: str,
    reference: str,
    *,
    config_path: str,
    timeout_seconds: float = _CONFIGURED_CATALOG_TIMEOUT_SECONDS,
) -> ModelCatalog:
    """Discover configured plugin metadata in a bounded process group."""
    try:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "src.model_catalog_bridge",
            "--configured-worker",
            plugin_id,
            reference,
            config_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            start_new_session=True,
        )
    except OSError:
        raise ModelCatalogUnavailable(
            "Configured model catalog worker failed to start"
        ) from None
    try:
        stdout, _ = await asyncio.wait_for(
            process.communicate(),
            timeout=timeout_seconds,
        )
    except asyncio.CancelledError:
        await terminate_plugin_worker(process)
        raise
    except asyncio.TimeoutError:
        await terminate_plugin_worker(process)
        raise ModelCatalogUnavailable(
            "Configured model catalog request timed out"
        ) from None
    if process.returncode != 0:
        raise ModelCatalogUnavailable(
            "Configured model catalog worker failed"
        )
    for raw_line in reversed(stdout.decode("utf-8", errors="replace").splitlines()):
        if not raw_line.startswith(_WORKER_RESULT_PREFIX):
            continue
        try:
            payload = json.loads(raw_line.removeprefix(_WORKER_RESULT_PREFIX))
        except (json.JSONDecodeError, TypeError):
            break
        return _parse_catalog(payload)
    raise ModelCatalogUnavailable(
        "Configured model catalog worker returned an invalid result"
    )


class DaemonModelCatalogLoader:
    """Call daemon-owned plugin operations without executing them in web."""

    def __init__(
        self,
        redis_client: Any,
        *,
        timeout_seconds: float = _REQUEST_TIMEOUT_SECONDS,
        poll_interval_seconds: float = _POLL_INTERVAL_SECONDS,
    ) -> None:
        self._redis = redis_client
        self._timeout_seconds = timeout_seconds
        self._poll_interval_seconds = poll_interval_seconds

    async def __call__(
        self,
        plugin: CoderPlugin,
        *,
        config: AppConfig,
        config_path: str,
    ) -> ModelCatalog:
        del config_path
        try:
            expected_reference = config.coder_plugins[plugin.name]
        except KeyError:
            raise ModelCatalogUnavailable(
                "Daemon model catalog is unavailable"
            ) from None
        return _parse_catalog(
            await self._request(
                plugin.name,
                operation="catalog",
                expected_reference=expected_reference,
            )
        )

    async def load_plugin_metadata(
        self,
        plugin_id: str,
        *,
        expected_reference: str,
    ) -> CoderMetadataView:
        """Load validated control-plane metadata from the daemon."""
        return _parse_plugin_metadata(
            await self._request(
                plugin_id,
                operation="metadata",
                expected_reference=expected_reference,
            ),
            expected_name=plugin_id,
        )

    async def load_auth_status(
        self,
        plugin_id: str,
        *,
        expected_reference: str,
    ) -> dict[str, Any]:
        """Run one configured auth probe inside the daemon boundary."""
        return _parse_auth_status(
            await self._request(
                plugin_id,
                operation="auth",
                expected_reference=expected_reference,
            )
        )

    async def start_device_login(
        self,
        plugin_id: str,
        *,
        expected_reference: str,
        replace_existing: bool,
    ) -> dict[str, Any]:
        """Ask the daemon to start one background device-login session."""
        return _parse_device_login(
            await self._request(
                plugin_id,
                operation="device_login_start",
                expected_reference=expected_reference,
                extra_fields={"replace_existing": replace_existing},
            ),
            expected_plugin=plugin_id,
        )

    async def inspect_device_login(
        self,
        plugin_id: str,
        session_id: str,
        *,
        expected_reference: str,
    ) -> dict[str, Any]:
        """Return one daemon-owned device-login session snapshot."""
        return _parse_device_login(
            await self._request(
                plugin_id,
                operation="device_login_inspect",
                expected_reference=expected_reference,
                extra_fields={"session_id": session_id},
            ),
            expected_plugin=plugin_id,
        )

    async def cancel_device_login(
        self,
        plugin_id: str,
        session_id: str,
        *,
        expected_reference: str,
    ) -> dict[str, Any]:
        """Ask the daemon to cancel one owned device-login session."""
        return _parse_device_login(
            await self._request(
                plugin_id,
                operation="device_login_cancel",
                expected_reference=expected_reference,
                extra_fields={"session_id": session_id},
            ),
            expected_plugin=plugin_id,
        )

    async def _request(
        self,
        plugin_id: str,
        *,
        operation: str,
        expected_reference: str | None = None,
        extra_fields: dict[str, object] | None = None,
    ) -> object:
        """Round-trip one plugin metadata request through Redis."""
        request_id = uuid.uuid4().hex
        response_key = _response_key(request_id)
        request_payload = {
            "request_id": request_id,
            "plugin": plugin_id,
            "operation": operation,
            "expires_at": time.time() + self._timeout_seconds,
        }
        if expected_reference is not None:
            request_payload["reference"] = expected_reference
        if extra_fields is not None:
            request_payload.update(extra_fields)
        request = json.dumps(request_payload, separators=(",", ":"))
        try:
            await self._redis.rpush(MODEL_CATALOG_REQUEST_QUEUE, request)
            await self._redis.ltrim(
                MODEL_CATALOG_REQUEST_QUEUE,
                -_MAX_PENDING_REQUESTS,
                -1,
            )
        except Exception:
            raise ModelCatalogUnavailable(
                "Daemon model catalog is unavailable"
            ) from None

        deadline = time.monotonic() + self._timeout_seconds
        try:
            while time.monotonic() < deadline:
                try:
                    raw_response = await self._redis.get(response_key)
                except Exception:
                    raise ModelCatalogUnavailable(
                        "Daemon model catalog is unavailable"
                    ) from None
                if raw_response is not None:
                    try:
                        return json.loads(raw_response)
                    except (json.JSONDecodeError, TypeError):
                        raise ModelCatalogUnavailable(
                            "Daemon returned invalid plugin metadata"
                        ) from None
                await asyncio.sleep(self._poll_interval_seconds)
        finally:
            try:
                await self._redis.lrem(
                    MODEL_CATALOG_REQUEST_QUEUE,
                    1,
                    request,
                )
            except Exception:
                pass
            try:
                await self._redis.delete(response_key)
            except Exception:
                pass
        raise ModelCatalogUnavailable("Daemon model catalog request timed out")


async def _store_response(
    redis_client: Any, request_id: str, payload: dict[str, Any]
) -> None:
    await redis_client.set(
        _response_key(request_id),
        json.dumps(payload, separators=(",", ":")),
        ex=_RESPONSE_TTL_SECONDS,
    )


async def handle_model_catalog_request(
    redis_client: Any,
    registry: CoderRegistry,
    raw_request: object,
    *,
    config_path: str,
    login_manager: CoderLoginSessionManager | None = None,
    credential_reservations: CoderCredentialReservations | None = None,
) -> None:
    """Execute one validated request inside the daemon process."""
    if isinstance(raw_request, bytes):
        raw_request = raw_request.decode("utf-8", errors="replace")
    try:
        request = json.loads(raw_request)
    except (json.JSONDecodeError, TypeError):
        return
    if not isinstance(request, dict):
        return
    request_id = request.get("request_id")
    plugin_name = request.get("plugin")
    operation = request.get("operation", "catalog")
    expected_reference = request.get("reference")
    expires_at = request.get("expires_at")
    if (
        not isinstance(request_id, str)
        or len(request_id) != 32
        or not all(character in "0123456789abcdef" for character in request_id)
        or not isinstance(plugin_name, str)
        or operation
        not in {
            "auth",
            "catalog",
            "metadata",
            "device_login_start",
            "device_login_inspect",
            "device_login_cancel",
        }
        or not isinstance(expected_reference, str)
        or not isinstance(expires_at, (int, float))
    ):
        return
    if expires_at <= time.time():
        await _store_response(
            redis_client,
            request_id,
            {"ok": False, "error": "request expired"},
        )
        return
    try:
        plugin = registry.get(plugin_name)
        reference = registry.reference_for(plugin_name)
        if expected_reference != reference:
            await _store_response(
                redis_client,
                request_id,
                {"ok": False, "error": "plugin reference mismatch"},
            )
            return
        if operation.startswith("device_login_"):
            if login_manager is None:
                await _store_response(
                    redis_client,
                    request_id,
                    {"ok": False, "error": "coder login unavailable"},
                )
                return
            if operation == "device_login_start":
                replace_existing = request.get("replace_existing")
                if not isinstance(replace_existing, bool):
                    return
                login = await login_manager.start(
                    plugin_name,
                    expected_reference=expected_reference,
                    replace_existing=replace_existing,
                )
            else:
                session_id = request.get("session_id")
                if not isinstance(session_id, str) or len(session_id) > 128:
                    return
                method = (
                    login_manager.inspect
                    if operation == "device_login_inspect"
                    else login_manager.cancel
                )
                login = await method(
                    plugin_name,
                    session_id,
                    expected_reference=expected_reference,
                )
            await _store_response(
                redis_client,
                request_id,
                {"ok": True, "login": login},
            )
            return
        if operation == "metadata":
            await _store_response(
                redis_client,
                request_id,
                _plugin_metadata_payload(plugin),
            )
            return
        if operation == "auth":
            assert reference is not None
            credential_location: str | None = None
            resolver = getattr(plugin, "device_login_credential_location", None)
            if callable(resolver) and credential_reservations is not None:
                credential_location = resolver(config=load_config(config_path))
                if (
                    not isinstance(credential_location, str)
                    or not credential_location
                ):
                    raise ValueError("invalid coder credential location")
                if not credential_reservations.reserve_coder(
                    credential_location
                ):
                    auth = coder_auth_payload(
                        CoderAuthStatus(
                            status="error",
                            detail="Device login is in progress",
                            failure_reason="probe_unavailable",
                        ),
                        capabilities=plugin.auth_capabilities,
                    )
                    await _store_response(
                        redis_client,
                        request_id,
                        {"ok": True, "auth": auth},
                    )
                    return
            try:
                auth = await isolated_auth_probe(
                    plugin_name,
                    reference,
                    plugin.display_name,
                    config_path=config_path,
                    timeout=_CONFIGURED_CATALOG_TIMEOUT_SECONDS,
                )
            finally:
                if (
                    credential_location is not None
                    and credential_reservations is not None
                ):
                    credential_reservations.release_coder(
                        credential_location
                    )
            await _store_response(
                redis_client,
                request_id,
                {"ok": True, "auth": auth},
            )
            return
        credential_location = None
        config = load_config(config_path)
        resolver = getattr(plugin, "device_login_credential_location", None)
        if callable(resolver) and credential_reservations is not None:
            credential_location = resolver(config=config)
            if (
                not isinstance(credential_location, str)
                or not credential_location
            ):
                raise ValueError("invalid coder credential location")
            if not credential_reservations.reserve_coder(credential_location):
                await _store_response(
                    redis_client,
                    request_id,
                    {"ok": False, "error": "catalog unavailable"},
                )
                return
        try:
            if (
                reference is not None
                and reference != DEFAULT_CODER_PLUGINS.get(plugin_name)
            ):
                catalog = await _isolated_configured_catalog(
                    plugin_name,
                    reference,
                    config_path=config_path,
                )
            else:
                catalog = await asyncio.wait_for(
                    plugin.get_model_catalog(
                        config=config,
                        config_path=config_path,
                    ),
                    timeout=_CONFIGURED_CATALOG_TIMEOUT_SECONDS,
                )
        finally:
            if (
                credential_location is not None
                and credential_reservations is not None
            ):
                credential_reservations.release_coder(credential_location)
    except Exception:
        logger.warning("%s model discovery failed in daemon", plugin_name)
        payload = {"ok": False, "error": "catalog unavailable"}
    else:
        payload = _catalog_payload(catalog)
    await _store_response(redis_client, request_id, payload)


async def serve_model_catalog_requests(
    redis_client: Any,
    registry: CoderRegistry,
    *,
    config_path: str,
    credential_location_in_use: Callable[[str], bool] | None = None,
    credential_reservations: CoderCredentialReservations | None = None,
) -> None:
    """Consume durable web requests for the lifetime of the daemon."""
    # Lightweight Redis doubles and alternate clients may not implement lists.
    # The ordinary daemon loop must remain usable when the optional discovery
    # bridge cannot be installed.
    if not callable(getattr(redis_client, "blpop", None)):
        return
    login_manager = CoderLoginSessionManager(
        registry,
        config_path=config_path,
        credential_location_in_use=credential_location_in_use,
        credential_reservations=credential_reservations,
    )
    pending: set[asyncio.Task[None]] = set()

    def request_done(task: asyncio.Task[None]) -> None:
        pending.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.warning("Model catalog bridge request failed", exc_info=True)

    try:
        while True:
            try:
                if len(pending) >= _MAX_CONCURRENT_REQUESTS:
                    await asyncio.wait(
                        tuple(pending),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    continue
                queued = await redis_client.blpop(
                    MODEL_CATALOG_REQUEST_QUEUE,
                    timeout=1,
                )
                if queued is not None:
                    task = asyncio.create_task(
                        handle_model_catalog_request(
                            redis_client,
                            registry,
                            queued[1],
                            config_path=config_path,
                            login_manager=login_manager,
                            credential_reservations=credential_reservations,
                        )
                    )
                    pending.add(task)
                    task.add_done_callback(request_done)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("Model catalog bridge unavailable", exc_info=True)
                await asyncio.sleep(1)
    finally:
        tasks = tuple(pending)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await login_manager.shutdown()


if __name__ == "__main__":  # pragma: no cover - exercised in the isolated worker
    _configured_catalog_worker_main()
