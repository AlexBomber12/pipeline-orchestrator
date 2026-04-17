"""Wrapper around the ``codex`` CLI for the pipeline orchestrator daemon.

Mirrors ``claude_cli.py`` but targets OpenAI's Codex CLI (Rust binary).
Exposes ``run_planned_pr_async`` and ``fix_review_async`` for the runner
to dispatch PLANNED PR and FIX REVIEW workflows through Codex.
"""

from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger(__name__)


async def run_codex_async(
    prompt: str,
    cwd: str,
    timeout: int | None = 600,
    model: str | None = None,
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
    **_kwargs: object,
) -> tuple[int, str, str]:
    """Trigger a ``PLANNED PR`` run in ``repo_path`` via Codex CLI."""
    return await run_codex_async(
        "PLANNED PR", repo_path, timeout=timeout, model=model
    )


async def fix_review_async(
    repo_path: str,
    model: str | None = None,
    timeout: int | None = None,
    **_kwargs: object,
) -> tuple[int, str, str]:
    """Trigger a ``FIX REVIEW`` run in ``repo_path`` via Codex CLI."""
    return await run_codex_async(
        "FIX REVIEW", repo_path, timeout=timeout, model=model
    )
