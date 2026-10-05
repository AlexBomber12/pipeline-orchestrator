"""Bounded model discovery through the Codex app-server metadata protocol."""

from __future__ import annotations

import asyncio
import json
from contextlib import suppress
from typing import Any, NamedTuple


class CodexReasoningEffort(NamedTuple):
    name: str
    description: str | None


class CodexModel(NamedTuple):
    identifier: str
    display_name: str
    is_default: bool
    default_reasoning_effort: str | None
    reasoning_efforts: tuple[CodexReasoningEffort, ...]


class CodexModelDiscoveryUnavailable(RuntimeError): ...


class CodexModelDiscoveryInvalid(RuntimeError): ...


async def discover_codex_models(
    *, timeout_seconds: float = 5.0, page_size: int = 50, max_pages: int = 10, max_output_bytes: int = 1_000_000
) -> tuple[CodexModel, ...]:
    """Return advertised visible models, not proof of account entitlement."""
    if min(timeout_seconds, page_size, max_pages, max_output_bytes) <= 0:
        raise ValueError("discovery bounds must be positive")
    process: asyncio.subprocess.Process | None = None
    try:
        async with asyncio.timeout(timeout_seconds):
            process = await asyncio.create_subprocess_exec(
                "codex", "app-server",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                limit=max_output_bytes + 1,
            )
            return await _discover(process, page_size, max_pages, max_output_bytes)
    except TimeoutError:
        raise CodexModelDiscoveryUnavailable("Codex model discovery timed out") from None
    except OSError:
        raise CodexModelDiscoveryUnavailable("Codex model discovery is unavailable") from None
    finally:
        if process is not None:
            await _close_process(process)


async def _discover(process: asyncio.subprocess.Process, page_size: int, max_pages: int, output_limit: int) -> tuple[
    CodexModel, ...
]:
    assert process.stdin is not None and process.stdout is not None
    writer, reader, output = process.stdin, process.stdout, [0]
    async def send(message: dict[str, Any]) -> None:
        writer.write(json.dumps(message, separators=(",", ":")).encode() + b"\n")
        await writer.drain()

    async def request(request_id: int, method: str, params: dict[str, Any]) -> dict[str, Any]:
        await send({"method": method, "id": request_id, "params": params})
        return await _response(reader, request_id, output, output_limit)
    await request(1, "initialize", {"clientInfo": {"name": "pipeline_orchestrator", "version": "1"}})
    await send({"method": "initialized", "params": {}})
    models: list[CodexModel] = []
    identifiers: set[str] = set()
    cursors: set[str] = set()
    cursor: str | None = None
    for page in range(max_pages):
        params: dict[str, Any] = {"limit": page_size, "includeHidden": True}
        if cursor is not None:
            params["cursor"] = cursor
        result = await request(page + 2, "model/list", params)
        data = result.get("data")
        if not isinstance(data, list):
            raise CodexModelDiscoveryInvalid("invalid Codex model catalog")
        for item in data:
            model = _normalize_model(item)
            if model is None:
                continue
            if model.identifier in identifiers:
                raise CodexModelDiscoveryInvalid("duplicate Codex model identifier")
            identifiers.add(model.identifier)
            models.append(model)
        next_cursor = result.get("nextCursor")
        if next_cursor is None:
            return tuple(models)
        if not isinstance(next_cursor, str) or not next_cursor or next_cursor in cursors:
            raise CodexModelDiscoveryInvalid("invalid Codex pagination cursor")
        cursors.add(next_cursor)
        cursor = next_cursor
    raise CodexModelDiscoveryInvalid("Codex model catalog exceeded page limit")


def _normalize_model(item: object) -> CodexModel | None:
    if not isinstance(item, dict) or not isinstance(item.get("hidden", False), bool):
        raise CodexModelDiscoveryInvalid("invalid Codex model entry")
    if item.get("hidden", False):
        return None
    identifier = item.get("model")
    name = item.get("displayName", identifier)
    is_default = item.get("isDefault", False)
    default_effort = item.get("defaultReasoningEffort")
    raw_efforts = item.get("supportedReasoningEfforts", [])
    if (
        not isinstance(identifier, str)
        or not identifier.strip()
        or not isinstance(name, str)
        or not isinstance(is_default, bool)
        or (default_effort is not None and (not isinstance(default_effort, str) or not default_effort.strip()))
        or not isinstance(raw_efforts, list)
    ):
        raise CodexModelDiscoveryInvalid("invalid Codex model entry")
    for raw in raw_efforts:
        if (
            not isinstance(raw, dict)
            or not isinstance(raw.get("reasoningEffort"), str)
            or not raw["reasoningEffort"].strip()
            or (raw.get("description") is not None and not isinstance(raw["description"], str))
        ):
            raise CodexModelDiscoveryInvalid("invalid reasoning-effort metadata")
    identifier = identifier.strip()
    return CodexModel(
        identifier,
        name.strip() or identifier,
        is_default,
        default_effort.strip() if default_effort else None,
        tuple(
            CodexReasoningEffort(
                raw["reasoningEffort"].strip(), raw["description"].strip() or None if raw.get("description") else None
            )
            for raw in raw_efforts
        ),
    )


async def _response(
    reader: asyncio.StreamReader, request_id: int, output: list[int], output_limit: int) -> dict[str, Any]:
    while True:
        try:
            line = await reader.readline()
        except ValueError:
            raise CodexModelDiscoveryInvalid("Codex metadata output is too large") from None
        if not line:
            raise CodexModelDiscoveryUnavailable("Codex app-server closed unexpectedly")
        output[0] += len(line)
        if output[0] > output_limit:
            raise CodexModelDiscoveryInvalid("Codex metadata output is too large")
        try:
            message = json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise CodexModelDiscoveryInvalid("invalid Codex protocol response") from None
        if not isinstance(message, dict):
            raise CodexModelDiscoveryInvalid("invalid Codex protocol response")
        if "id" not in message:
            continue
        if type(message.get("id")) is not int or message["id"] != request_id:
            raise CodexModelDiscoveryInvalid("unexpected Codex response identifier")
        if "error" in message:
            raise CodexModelDiscoveryUnavailable("Codex model discovery was rejected")
        result = message.get("result")
        if not isinstance(result, dict):
            raise CodexModelDiscoveryInvalid("invalid Codex protocol response")
        return result


async def _close_process(process: asyncio.subprocess.Process) -> None:
    if process.stdin is not None:
        process.stdin.close()
    with suppress(ProcessLookupError):
        process.kill()
    await process.communicate()
