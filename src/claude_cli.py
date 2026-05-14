"""Wrapper around the `claude` CLI for the pipeline orchestrator daemon.

Exposes the trigger-phrase entry points defined in AGENTS.md (``PLANNED PR``
and ``FIX FEEDBACK``) plus an infrastructure-error diagnosis helper.
"""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import uuid
from typing import Callable

from src.config import load_config
from src.daemon.sandbox import build_bwrap_command, is_bubblewrap_available
from src.diagnosis import build_diagnosis_prompt

logger = logging.getLogger(__name__)


def _maybe_wrap_sandbox(cmd: list[str], cwd: str) -> list[str]:
    """Wrap ``cmd`` with bwrap when ``coder_filesystem_isolation`` is on."""
    cfg = load_config()
    if not cfg.daemon.coder_filesystem_isolation:
        return cmd
    if not is_bubblewrap_available():
        logger.warning(
            "[SANDBOX] coder_filesystem_isolation enabled but bwrap not "
            "available; spawning coder unsandboxed"
        )
        return cmd
    # Bind the daemon HOME so files written outside of claude_config_dir
    # (notably ~/.gitconfig from ``gh auth setup-git``) remain visible to
    # the sandboxed coder; without it non-interactive git push fails.
    home = os.environ.get("HOME")
    additional_rw_dirs = [home] if home else None
    return build_bwrap_command(
        command=cmd,
        repo_path=cwd,
        coder_config_dir=cfg.auth.claude_config_dir,
        gh_config_dir=cfg.auth.gh_config_dir,
        additional_rw_dirs=additional_rw_dirs,
    )


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

    cmd = _maybe_wrap_sandbox(cmd, cwd)
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


def _build_auto_pr_prompt(pr_id: str, task_file: str, task_body: str) -> str:
    return f"AUTO PR\nTask: {pr_id}\nFile: {task_file}\n\n{task_body}"


def run_auto_pr(
    repo_path: str,
    pr_id: str,
    task_file: str,
    task_body: str,
    *,
    model: str | None = None,
    timeout: int = 900,
) -> tuple[int, str, str]:
    """Trigger an ``AUTO PR`` run in ``repo_path`` with task injected inline."""
    return run_claude(
        _build_auto_pr_prompt(pr_id, task_file, task_body),
        repo_path,
        timeout=timeout,
        model=model,
    )


def _build_fix_feedback_prompt(
    extra_context: str | None,
    *,
    pr_id: str | None = None,
    task_file: str | None = None,
) -> str:
    """Compose the FIX FEEDBACK prompt with optional Task anchor + context."""
    parts: list[str] = []
    if pr_id and task_file:
        parts.append(f"Task: {pr_id}")
        parts.append(f"File: {task_file}")
        parts.append(
            "Stay in the scope of this task. Do not address any "
            "other PR or task in this run."
        )
    parts.append("FIX FEEDBACK")
    if extra_context:
        parts.append(extra_context)
    return "\n\n".join(parts)


def fix_review(
    repo_path: str,
    model: str | None = None,
    timeout: int = 3600,
    extra_context: str | None = None,
) -> tuple[int, str, str]:
    """Trigger a ``FIX FEEDBACK`` run in ``repo_path``."""
    return run_claude(
        _build_fix_feedback_prompt(extra_context),
        repo_path,
        timeout=timeout,
        model=model,
    )


def diagnose_error(
    repo_path: str, context: str, model: str | None = None
) -> tuple[int, str, str]:
    """Ask the ``claude`` CLI to classify an infrastructure error.

    The first line of stdout is expected to be exactly one of ``FIX``,
    ``SKIP``, or ``ESCALATE``; subsequent lines may contain a short plan.
    """
    return run_claude(
        build_diagnosis_prompt(repo_path, context),
        repo_path,
        timeout=120,
        model=model,
    )


def _build_node_options() -> str:
    memory_flag = "--max-old-space-size=4096"
    existing = os.environ.get("NODE_OPTIONS", "").strip()
    return f"{existing} {memory_flag}".strip() if existing else memory_flag


async def run_claude_async(
    prompt: str,
    cwd: str,
    timeout: int | None = 600,
    model: str | None = None,
    system_prompt_file: str | None = "CLAUDE.md",
    on_process_start: Callable[[asyncio.subprocess.Process], None] | None = None,
    breach_dir: str | None = None,
    breach_run_id: str | None = None,
    session_threshold: int | None = None,
    weekly_threshold: int | None = None,
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

    # In-flight rate-limit monitoring: the caller provides a breach_dir
    # and run_id so the statusline hook can write a breach marker that the
    # daemon monitors concurrently.
    if breach_dir is not None:
        env["PIPELINE_BREACH_DIR"] = breach_dir
        env["PIPELINE_RUN_ID"] = breach_run_id or uuid.uuid4().hex[:12]
        if session_threshold is not None:
            env["PIPELINE_SESSION_THRESHOLD"] = str(session_threshold)
        if weekly_threshold is not None:
            env["PIPELINE_WEEKLY_THRESHOLD"] = str(weekly_threshold)

    cmd = _maybe_wrap_sandbox(cmd, cwd)
    proc: asyncio.subprocess.Process | None = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=cwd,
            stdin=asyncio.subprocess.DEVNULL,
            env=env,
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
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            logger.warning("claude CLI subprocess did not exit within 5s after kill")
        logger.error("claude CLI timed out after %ss", timeout)
        return (-1, "", f"Timeout after {timeout}s")
    except asyncio.CancelledError:
        if proc is not None:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                logger.warning("claude CLI subprocess did not exit within 5s after kill")
        logger.error("claude CLI task cancelled, subprocess killed")
        raise
    except FileNotFoundError as exc:
        missing = getattr(exc, "filename", "")
        if missing and missing != cmd[0]:
            return (-1, "", f"cwd not found: {missing}")
        return (-1, "", "claude CLI not found")
    logger.info("claude CLI exited with code %s", code)
    return (code, stdout, stderr)


async def run_planned_pr_async(
    repo_path: str,
    model: str | None = None,
    timeout: int = 900,
    on_process_start: Callable[[asyncio.subprocess.Process], None] | None = None,
    breach_dir: str | None = None,
    breach_run_id: str | None = None,
    session_threshold: int | None = None,
    weekly_threshold: int | None = None,
) -> tuple[int, str, str]:
    kwargs: dict[str, object] = {
        "timeout": timeout,
        "model": model,
        "breach_dir": breach_dir,
        "breach_run_id": breach_run_id,
        "session_threshold": session_threshold,
        "weekly_threshold": weekly_threshold,
    }
    if on_process_start is not None:
        kwargs["on_process_start"] = on_process_start
    return await run_claude_async("PLANNED PR", repo_path, **kwargs)


async def run_auto_pr_async(
    repo_path: str,
    pr_id: str,
    task_file: str,
    task_body: str,
    *,
    model: str | None = None,
    timeout: int = 900,
    on_process_start: Callable[[asyncio.subprocess.Process], None] | None = None,
    breach_dir: str | None = None,
    breach_run_id: str | None = None,
    session_threshold: int | None = None,
    weekly_threshold: int | None = None,
) -> tuple[int, str, str]:
    """Trigger an ``AUTO PR`` run in ``repo_path`` with task injected inline."""
    kwargs: dict[str, object] = {
        "timeout": timeout,
        "model": model,
        "breach_dir": breach_dir,
        "breach_run_id": breach_run_id,
        "session_threshold": session_threshold,
        "weekly_threshold": weekly_threshold,
    }
    if on_process_start is not None:
        kwargs["on_process_start"] = on_process_start
    return await run_claude_async(
        _build_auto_pr_prompt(pr_id, task_file, task_body), repo_path, **kwargs
    )


async def fix_review_async(
    repo_path: str,
    model: str | None = None,
    timeout: int | None = None,
    on_process_start: Callable[[asyncio.subprocess.Process], None] | None = None,
    breach_dir: str | None = None,
    breach_run_id: str | None = None,
    session_threshold: int | None = None,
    weekly_threshold: int | None = None,
    extra_context: str | None = None,
    pr_id: str | None = None,
    task_file: str | None = None,
) -> tuple[int, str, str]:
    kwargs: dict[str, object] = {
        "timeout": timeout,
        "model": model,
        "breach_dir": breach_dir,
        "breach_run_id": breach_run_id,
        "session_threshold": session_threshold,
        "weekly_threshold": weekly_threshold,
    }
    if on_process_start is not None:
        kwargs["on_process_start"] = on_process_start
    return await run_claude_async(
        _build_fix_feedback_prompt(
            extra_context,
            pr_id=pr_id,
            task_file=task_file,
        ),
        repo_path,
        **kwargs,
    )


async def diagnose_error_async(
    repo_path: str, context: str, model: str | None = None
) -> tuple[int, str, str]:
    return await run_claude_async(
        build_diagnosis_prompt(repo_path, context),
        repo_path,
        timeout=120,
        model=model,
        system_prompt_file=None,
    )
