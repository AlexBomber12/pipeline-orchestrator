"""Wrapper around the ``codex`` CLI for the pipeline orchestrator daemon.

Mirrors ``claude_cli.py`` but targets OpenAI's Codex CLI (Rust binary).
Exposes ``run_planned_pr_async`` and ``fix_review_async`` for the runner
to dispatch PLANNED PR and FIX REVIEW workflows through Codex.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Callable

logger = logging.getLogger(__name__)


async def run_codex_async(
    prompt: str,
    cwd: str,
    timeout: int | None = 600,
    model: str | None = None,
    on_process_start: Callable[[asyncio.subprocess.Process], None] | None = None,
) -> tuple[int, str, str]:
    """Invoke ``codex exec`` with ``prompt`` inside ``cwd``.

    Returns ``(returncode, stdout, stderr)``.  On timeout, missing CLI, or
    missing ``cwd``, returns ``(-1, "", <error message>)`` instead of raising.
    """
    cmd = [
        "codex",
        "--ask-for-approval",
        "never",
        "exec",
        "--sandbox",
        "danger-full-access",
    ]
    if model:
        cmd.extend(["--model", model])
    cmd.append(prompt)
    logger.info("[codex] running codex exec with prompt: %s", prompt[:80])

    proc: asyncio.subprocess.Process | None = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=cwd,
            stdin=asyncio.subprocess.DEVNULL,
        )
        if on_process_start is not None:
            on_process_start(proc)

        stdout_bytes, stderr_bytes = await asyncio.wait_for(
            proc.communicate(), timeout=timeout
        )
        stdout = stdout_bytes.decode("utf-8", errors="replace")
        stderr = stderr_bytes.decode("utf-8", errors="replace")
        code = proc.returncode or 0
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        await proc.wait()
        logger.error("[codex] codex exec timed out after %ss", timeout)
        return (-1, "", f"Timeout after {timeout}s")
    except asyncio.CancelledError:
        if proc is not None:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            await proc.wait()
        logger.error("[codex] codex exec task cancelled, subprocess killed")
        raise
    except FileNotFoundError as exc:
        missing = getattr(exc, "filename", "")
        if missing and missing != cmd[0]:
            return (-1, "", f"cwd not found: {missing}")
        return (-1, "", "codex CLI not found")
    logger.info("[codex] codex exec exited with code %s", code)
    return (code, stdout, stderr)


async def run_planned_pr_async(
    repo_path: str,
    model: str | None = None,
    timeout: int = 900,
    on_process_start: Callable[[asyncio.subprocess.Process], None] | None = None,
    **_kwargs: object,
) -> tuple[int, str, str]:
    """Trigger a ``PLANNED PR`` run in ``repo_path`` via Codex CLI."""
    kwargs: dict[str, object] = {"timeout": timeout, "model": model}
    if on_process_start is not None:
        kwargs["on_process_start"] = on_process_start
    return await run_codex_async("PLANNED PR", repo_path, **kwargs)


async def fix_review_async(
    repo_path: str,
    model: str | None = None,
    timeout: int | None = None,
    on_process_start: Callable[[asyncio.subprocess.Process], None] | None = None,
    **_kwargs: object,
) -> tuple[int, str, str]:
    """Trigger a ``FIX REVIEW`` run in ``repo_path`` via Codex CLI."""
    kwargs: dict[str, object] = {"timeout": timeout, "model": model}
    if on_process_start is not None:
        kwargs["on_process_start"] = on_process_start
    return await run_codex_async("FIX REVIEW", repo_path, **kwargs)


async def diagnose_error_async(
    repo_path: str, context: str, model: str | None = None
) -> tuple[int, str, str]:
    prompt = (
        "You are the pipeline orchestrator. An infrastructure error occurred. "
        f"Error context: {context} "
        "Respond with exactly one word on the first line: FIX, SKIP, or ESCALATE. "
        "If FIX, include a brief action plan on subsequent lines."
    )
    return await run_codex_async(prompt, repo_path, timeout=120, model=model)
