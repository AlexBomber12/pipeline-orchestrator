"""Killable isolation for configured coder authentication probes."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys

from src.coder_auth_worker import RESULT_PREFIX


async def _terminate_worker(process: asyncio.subprocess.Process) -> None:
    """Kill and reap one isolated auth worker process group."""
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
) -> dict[str, str]:
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
        return {
            "status": "error",
            "detail": (
                f"{display_name} auth check failed ({type(exc).__name__})"
            ),
        }
    try:
        stdout, _ = await asyncio.wait_for(
            process.communicate(),
            timeout=timeout,
        )
    except asyncio.CancelledError:
        await _terminate_worker(process)
        raise
    except asyncio.TimeoutError:
        await _terminate_worker(process)
        return {
            "status": "error",
            "detail": f"{display_name} auth check timed out after {timeout:g}s",
        }
    if process.returncode != 0:
        return {
            "status": "error",
            "detail": f"{display_name} auth check worker failed",
        }
    for raw_line in reversed(stdout.decode("utf-8", errors="replace").splitlines()):
        if not raw_line.startswith(RESULT_PREFIX):
            continue
        try:
            result = json.loads(raw_line.removeprefix(RESULT_PREFIX))
        except (json.JSONDecodeError, TypeError):
            break
        if (
            isinstance(result, dict)
            and result.get("status") in {"ok", "error"}
            and isinstance(result.get("detail"), str)
            and all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in result.items()
            )
        ):
            return result
        break
    return {
        "status": "error",
        "detail": f"{display_name} auth check returned an invalid result",
    }
