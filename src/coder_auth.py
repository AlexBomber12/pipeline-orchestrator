"""Killable isolation for configured coder authentication probes."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from typing import Any

from src.coder_auth_worker import RESULT_PREFIX
from src.coder_registry import (
    CoderAuthStatus,
    coder_auth_payload,
    parse_coder_auth_payload,
)


async def terminate_plugin_worker(process: asyncio.subprocess.Process) -> None:
    """Kill and reap one isolated plugin worker process group."""
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    await process.wait()


async def isolated_auth_probe(
    plugin_id: str,
    reference: str,
    display_name: str,
    *,
    config_path: str,
    timeout: float = 5,
) -> dict[str, Any]:
    """Probe trusted plugin code in a subprocess killed at ``timeout``."""
    try:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "src.coder_auth_worker",
            plugin_id,
            reference,
            config_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            start_new_session=True,
        )
    except OSError as exc:
        return coder_auth_payload(
            CoderAuthStatus(
                status="error",
                detail=(
                    f"{display_name} auth check failed ({type(exc).__name__})"
                ),
                failure_reason="probe_failed",
            )
        )
    try:
        stdout, _ = await asyncio.wait_for(
            process.communicate(),
            timeout=timeout,
        )
    except asyncio.CancelledError:
        await terminate_plugin_worker(process)
        raise
    except asyncio.TimeoutError:
        await terminate_plugin_worker(process)
        return coder_auth_payload(
            CoderAuthStatus(
                status="error",
                detail=f"{display_name} auth check timed out after {timeout:g}s",
                failure_reason="probe_timeout",
            )
        )
    if process.returncode != 0:
        return coder_auth_payload(
            CoderAuthStatus(
                status="error",
                detail=f"{display_name} auth check worker failed",
                failure_reason="probe_failed",
            )
        )
    for raw_line in reversed(stdout.decode("utf-8", errors="replace").splitlines()):
        if not raw_line.startswith(RESULT_PREFIX):
            continue
        try:
            result = json.loads(raw_line.removeprefix(RESULT_PREFIX))
        except (json.JSONDecodeError, TypeError):
            break
        try:
            return parse_coder_auth_payload(result)
        except TypeError:
            pass
        break
    return coder_auth_payload(
        CoderAuthStatus(
            status="error",
            detail=f"{display_name} auth check returned an invalid result",
            failure_reason="unrecognized_output",
        )
    )
