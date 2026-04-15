"""Wrapper around the `claude` CLI for the pipeline orchestrator daemon.

Exposes the trigger-phrase entry points defined in AGENTS.md (``PLANNED PR``
and ``FIX REVIEW``) plus an infrastructure-error diagnosis helper.
"""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess

logger = logging.getLogger(__name__)


def run_claude(
    prompt: str,
    cwd: str,
    timeout: int = 600,
    model: str | None = None,
) -> tuple[int, str, str]:
    """Invoke the ``claude`` CLI with ``prompt`` inside ``cwd``.

    Returns ``(returncode, stdout, stderr)``. On timeout, missing CLI, or
    missing ``cwd``, returns ``(-1, "", <error message>)`` instead of raising.
    """
    cmd = [
        "claude",
        "--print",
        "--dangerously-skip-permissions",
    ]
    if model:
        cmd.extend(["--model", model])
    cmd.append(prompt)
    logger.info("running claude CLI with prompt: %s", prompt[:80])

    # Preserve any pre-existing NODE_OPTIONS (e.g. CA bundle, proxy flags set
    # by the daemon environment) and append the memory cap rather than
    # clobbering them.
    memory_flag = "--max-old-space-size=4096"
    existing_node_options = os.environ.get("NODE_OPTIONS", "").strip()
    node_options = (
        f"{existing_node_options} {memory_flag}".strip()
        if existing_node_options
        else memory_flag
    )

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=cwd,
            stdin=subprocess.DEVNULL,
            env={**os.environ, "NODE_OPTIONS": node_options},
        )
    except subprocess.TimeoutExpired:
        logger.error("claude CLI timed out after %ss", timeout)
        return (-1, "", f"Timeout after {timeout}s")
    except FileNotFoundError as exc:
        # subprocess.run raises FileNotFoundError for two distinct cases:
        # 1. the executable (cmd[0]) is not on PATH
        # 2. the cwd directory does not exist
        # exc.filename distinguishes them so the daemon can pick the right
        # recovery path (reinstall CLI vs. reclone repo).
        missing = exc.filename
        if missing and missing != cmd[0]:
            logger.error("claude CLI cwd not found: %s", missing)
            return (-1, "", f"cwd not found: {missing}")
        logger.error("claude CLI not found on PATH")
        return (-1, "", "claude CLI not found")

    logger.info("claude CLI exited with code %s", result.returncode)
    return (result.returncode, result.stdout, result.stderr)


def run_planned_pr(
    repo_path: str, model: str | None = None, timeout: int = 900
) -> tuple[int, str, str]:
    """Trigger a ``PLANNED PR`` run in ``repo_path``."""
    return run_claude("PLANNED PR", repo_path, timeout=timeout, model=model)


def fix_review(
    repo_path: str, model: str | None = None, timeout: int = 3600
) -> tuple[int, str, str]:
    """Trigger a ``FIX REVIEW`` run in ``repo_path``."""
    return run_claude("FIX REVIEW", repo_path, timeout=timeout, model=model)


def diagnose_error(
    repo_path: str, context: str, model: str | None = None
) -> tuple[int, str, str]:
    """Ask the ``claude`` CLI to classify an infrastructure error.

    The first line of stdout is expected to be exactly one of ``FIX``,
    ``SKIP``, or ``ESCALATE``; subsequent lines may contain a short plan.
    """
    prompt = (
        "You are the pipeline orchestrator. An infrastructure error occurred. "
        f"Error context: {context} "
        "Respond with exactly one word on the first line: FIX, SKIP, or ESCALATE. "
        "If FIX, include a brief action plan on subsequent lines."
    )
    return run_claude(prompt, repo_path, timeout=120, model=model)


def parse_diagnosis(stdout: str) -> str:
    """Return ``FIX``, ``SKIP``, or ``ESCALATE`` from a ``diagnose_error`` stdout.

    Anything that does not clearly start with one of those tokens is treated
    as ``ESCALATE`` so ambiguous responses never silently trigger a fix.
    """
    tokens = stdout.split()
    if not tokens:
        return "ESCALATE"

    first = tokens[0].upper()
    for verdict in ("FIX", "SKIP", "ESCALATE"):
        if first.startswith(verdict):
            return verdict
    return "ESCALATE"


def _build_node_options() -> str:
    memory_flag = "--max-old-space-size=4096"
    existing = os.environ.get("NODE_OPTIONS", "").strip()
    return f"{existing} {memory_flag}".strip() if existing else memory_flag


async def run_claude_async(
    prompt: str,
    cwd: str,
    timeout: int = 600,
    model: str | None = None,
    system_prompt_file: str | None = "CLAUDE.md",
) -> tuple[int, str, str]:
    cmd = [
        "claude",
        "--print",
        "--dangerously-skip-permissions",
    ]
    if model:
        cmd.extend(["--model", model])
    if system_prompt_file:
        cmd.extend(["--append-system-prompt-file", system_prompt_file])
    cmd.append(prompt)
    logger.info("running claude CLI with prompt: %s", prompt[:80])
    env = {**os.environ, "NODE_OPTIONS": _build_node_options()}
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=cwd,
            stdin=asyncio.subprocess.DEVNULL,
            env=env,
        )
        stdout_bytes, stderr_bytes = await asyncio.wait_for(
            proc.communicate(), timeout=timeout
        )
        stdout = stdout_bytes.decode("utf-8", errors="replace")
        stderr = stderr_bytes.decode("utf-8", errors="replace")
        code = proc.returncode or 0
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        logger.error("claude CLI timed out after %ss", timeout)
        return (-1, "", f"Timeout after {timeout}s")
    except FileNotFoundError as exc:
        missing = getattr(exc, "filename", "")
        if missing and missing != cmd[0]:
            return (-1, "", f"cwd not found: {missing}")
        return (-1, "", "claude CLI not found")
    logger.info("claude CLI exited with code %s", code)
    return (code, stdout, stderr)


async def run_planned_pr_async(
    repo_path: str, model: str | None = None, timeout: int = 900
) -> tuple[int, str, str]:
    return await run_claude_async(
        "PLANNED PR", repo_path, timeout=timeout, model=model
    )


async def fix_review_async(
    repo_path: str, model: str | None = None, timeout: int = 3600
) -> tuple[int, str, str]:
    return await run_claude_async(
        "FIX REVIEW", repo_path, timeout=timeout, model=model
    )


async def diagnose_error_async(
    repo_path: str, context: str, model: str | None = None
) -> tuple[int, str, str]:
    prompt = (
        "You are the pipeline orchestrator. An infrastructure error occurred. "
        f"Error context: {context} "
        "Respond with exactly one word on the first line: FIX, SKIP, or ESCALATE. "
        "If FIX, include a brief action plan on subsequent lines."
    )
    return await run_claude_async(
        prompt, repo_path, timeout=120, model=model, system_prompt_file=None
    )
