"""Bounded model discovery through the Claude Code SDK control protocol."""

from __future__ import annotations

import asyncio
import json
import math
import os
from pathlib import Path
from typing import Mapping

from src.coder_registry import ModelMetadata, ModelReasoningEffort
from src.process_supervisor import (
    CleanupResult,
    ProcessLaunchCleanupError,
    SupervisedProcess,
    launch_process,
)

_REQUEST_ID = "pipeline-orchestrator-model-discovery"
_COMMAND = (
    "claude", "--output-format", "stream-json", "--verbose",
    "--input-format", "stream-json",
)
_INVALID_RESPONSE = "invalid Claude model discovery response"


class ClaudeModelDiscoveryUnavailable(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        managed: SupervisedProcess | None = None,
        cleanup_result: CleanupResult | None = None,
    ) -> None:
        super().__init__(message)
        self.managed = managed
        self.cleanup_result = cleanup_result


class ClaudeModelDiscoveryInvalid(RuntimeError): ...


async def discover_claude_models(
    *,
    timeout_seconds: float = 5.0,
    max_output_bytes: int = 1_000_000,
    env: Mapping[str, str] | None = None,
    cwd: str | os.PathLike[str] | None = None,
) -> tuple[ModelMetadata, ...]:
    """Return models advertised by Claude Code without starting inference."""
    valid_timeout = (
        not isinstance(timeout_seconds, bool)
        and isinstance(timeout_seconds, (int, float))
        and math.isfinite(timeout_seconds)
        and timeout_seconds > 0
    )
    valid_output_limit = not isinstance(max_output_bytes, bool) and isinstance(
        max_output_bytes, int
    ) and max_output_bytes > 0
    if not valid_timeout or not valid_output_limit:
        raise ValueError("discovery bounds must be finite and positive")

    try:
        async with asyncio.timeout(timeout_seconds):
            managed = await launch_process(
                *_COMMAND,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                limit=max_output_bytes + 1,
                env=env,
                cwd=Path(cwd) if cwd is not None else None,
            )
            try:
                return await _exchange(managed.process, max_output_bytes)
            finally:
                if managed.process.stdin is not None:
                    managed.process.stdin.close()
                try:
                    cleanup = await managed.cleanup(
                        term_grace=1.0, kill_grace=1.0
                    )
                except Exception:
                    raise ClaudeModelDiscoveryUnavailable(
                        "Claude model discovery cleanup could not be confirmed",
                        managed=managed,
                    ) from None
                if not cleanup.quiescent:
                    raise ClaudeModelDiscoveryUnavailable(
                        "Claude model discovery cleanup could not be confirmed",
                        managed=managed,
                        cleanup_result=cleanup,
                    )
    except TimeoutError:
        raise ClaudeModelDiscoveryUnavailable("Claude model discovery timed out") from None
    except asyncio.CancelledError:
        raise
    except (ClaudeModelDiscoveryInvalid, ClaudeModelDiscoveryUnavailable):
        raise
    except ProcessLaunchCleanupError as exc:
        raise ClaudeModelDiscoveryUnavailable(
            "Claude model discovery startup cleanup could not be confirmed",
            managed=exc.managed,
            cleanup_result=exc.cleanup_result,
        ) from None
    except Exception:
        raise ClaudeModelDiscoveryUnavailable(
            "Claude model discovery is unavailable"
        ) from None


async def _exchange(
    process: asyncio.subprocess.Process, output_limit: int
) -> tuple[ModelMetadata, ...]:
    if process.stdin is None or process.stdout is None:
        raise ClaudeModelDiscoveryUnavailable(
            "Claude model discovery is unavailable"
        )
    request = {
        "type": "control_request",
        "request_id": _REQUEST_ID,
        "request": {"subtype": "initialize", "hooks": None},
    }
    process.stdin.write(
        json.dumps(request, separators=(",", ":")).encode() + b"\n"
    )
    try:
        await process.stdin.drain()
    except (BrokenPipeError, ConnectionError):
        raise ClaudeModelDiscoveryUnavailable(
            "Claude Code closed before model discovery completed"
        ) from None

    output_bytes = 0
    while True:
        try:
            line = await process.stdout.readline()
        except ValueError:
            raise ClaudeModelDiscoveryInvalid(
                "Claude model metadata output is too large"
            ) from None
        if not line:
            raise ClaudeModelDiscoveryUnavailable(
                "Claude Code closed before model discovery completed"
            )
        output_bytes += len(line)
        if output_bytes > output_limit:
            raise ClaudeModelDiscoveryInvalid(
                "Claude model metadata output is too large"
            )
        try:
            message = json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise ClaudeModelDiscoveryInvalid(_INVALID_RESPONSE) from None
        if not isinstance(message, dict):
            raise ClaudeModelDiscoveryInvalid(_INVALID_RESPONSE)
        if message.get("type") != "control_response":
            continue
        response = message.get("response")
        if not isinstance(response, dict):
            raise ClaudeModelDiscoveryInvalid(_INVALID_RESPONSE)
        if response.get("request_id") != _REQUEST_ID:
            raise ClaudeModelDiscoveryInvalid(
                "unexpected Claude model discovery response identifier"
            )
        if response.get("subtype") == "error":
            raise ClaudeModelDiscoveryUnavailable(
                "Claude Code does not support model discovery"
            )
        if response.get("subtype") != "success":
            raise ClaudeModelDiscoveryInvalid(_INVALID_RESPONSE)
        payload = response.get("response")
        if not isinstance(payload, dict):
            raise ClaudeModelDiscoveryInvalid(_INVALID_RESPONSE)
        return _normalize_models(payload.get("models"))


def _normalize_models(raw_models: object) -> tuple[ModelMetadata, ...]:
    if not isinstance(raw_models, list):
        raise ClaudeModelDiscoveryInvalid("invalid Claude model catalog")
    models: list[ModelMetadata] = []
    invocation_ids: set[str] = set()
    for raw_model in raw_models:
        if not isinstance(raw_model, dict):
            raise ClaudeModelDiscoveryInvalid("invalid Claude model entry")
        invocation_id = raw_model.get("value")
        display_name = raw_model.get("displayName")
        supports_effort = raw_model.get("supportsEffort")
        raw_efforts = raw_model.get("supportedEffortLevels", [])
        if (
            not isinstance(invocation_id, str)
            or not invocation_id.strip()
            or not isinstance(display_name, str)
            or not display_name.strip()
            or supports_effort is not None
            and not isinstance(supports_effort, bool)
            or not isinstance(raw_efforts, list)
        ):
            raise ClaudeModelDiscoveryInvalid("invalid Claude model entry")
        efforts: list[ModelReasoningEffort] = []
        effort_names: set[str] = set()
        for effort in raw_efforts:
            if (
                not isinstance(effort, str)
                or not effort.strip()
                or effort in effort_names
            ):
                raise ClaudeModelDiscoveryInvalid(
                    "invalid Claude effort metadata"
                )
            effort_names.add(effort)
            efforts.append(ModelReasoningEffort(effort))
        if invocation_id in invocation_ids:
            raise ClaudeModelDiscoveryInvalid(
                "duplicate Claude model invocation identifier"
            )
        invocation_ids.add(invocation_id)
        models.append(
            ModelMetadata(
                invocation_id,
                display_name,
                default_reasoning_effort=None,
                reasoning_efforts=tuple(efforts),
            )
        )
    return tuple(models)
