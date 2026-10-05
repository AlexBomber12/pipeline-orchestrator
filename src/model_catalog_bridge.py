"""Redis request/response bridge for daemon-owned model discovery."""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import asdict
from typing import Any

from src.coder_registry import (
    CoderPlugin,
    CoderRegistry,
    ModelCatalog,
    ModelCatalogUnavailable,
    ModelMetadata,
    ModelReasoningEffort,
)
from src.config import AppConfig, load_config

logger = logging.getLogger(__name__)

MODEL_CATALOG_REQUEST_QUEUE = "orchestrator:model-catalog:requests"
_RESPONSE_PREFIX = "orchestrator:model-catalog:response"
_RESPONSE_TTL_SECONDS = 30
_REQUEST_TIMEOUT_SECONDS = 7.0
_POLL_INTERVAL_SECONDS = 0.05


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


class DaemonModelCatalogLoader:
    """Load plugin metadata through Redis without executing it in the web app."""

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
        del config, config_path
        request_id = uuid.uuid4().hex
        response_key = _response_key(request_id)
        request = json.dumps(
            {
                "request_id": request_id,
                "plugin": plugin.name,
                "expires_at": time.time() + self._timeout_seconds,
            },
            separators=(",", ":"),
        )
        try:
            await self._redis.rpush(MODEL_CATALOG_REQUEST_QUEUE, request)
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
                        return _parse_catalog(json.loads(raw_response))
                    except (json.JSONDecodeError, TypeError):
                        raise ModelCatalogUnavailable(
                            "Daemon returned invalid model metadata"
                        ) from None
                await asyncio.sleep(self._poll_interval_seconds)
        finally:
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
    expires_at = request.get("expires_at")
    if (
        not isinstance(request_id, str)
        or len(request_id) != 32
        or not all(character in "0123456789abcdef" for character in request_id)
        or not isinstance(plugin_name, str)
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
        config = load_config(config_path)
        catalog = await plugin.get_model_catalog(
            config=config,
            config_path=config_path,
        )
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
) -> None:
    """Consume durable web requests for the lifetime of the daemon."""
    # Lightweight Redis doubles and alternate clients may not implement lists.
    # The ordinary daemon loop must remain usable when the optional discovery
    # bridge cannot be installed.
    if not callable(getattr(redis_client, "blpop", None)):
        return
    while True:
        try:
            queued = await redis_client.blpop(
                MODEL_CATALOG_REQUEST_QUEUE,
                timeout=1,
            )
            if queued is not None:
                await handle_model_catalog_request(
                    redis_client,
                    registry,
                    queued[1],
                    config_path=config_path,
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("Model catalog bridge unavailable", exc_info=True)
            await asyncio.sleep(1)
