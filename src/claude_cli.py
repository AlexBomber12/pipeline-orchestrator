"""Wrapper around the `claude` CLI for the pipeline orchestrator daemon.

Exposes the trigger-phrase entry points defined in AGENTS.md (``PLANNED PR``
and ``FIX REVIEW``) plus an infrastructure-error diagnosis helper.
"""

from __future__ import annotations

import logging
import subprocess

logger = logging.getLogger(__name__)


def run_claude(
    prompt: str,
    cwd: str,
    timeout: int = 600,
) -> tuple[int, str, str]:
    """Invoke the ``claude`` CLI with ``prompt`` inside ``cwd``.

    Returns ``(returncode, stdout, stderr)``. On timeout or when the CLI is
    missing, returns ``(-1, "", <error message>)`` instead of raising.
    """
    cmd = ["claude", "--print", "--dangerously-skip-permissions", prompt]
    logger.info("running claude CLI with prompt: %s", prompt[:80])

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=cwd,
        )
    except subprocess.TimeoutExpired:
        logger.error("claude CLI timed out after %ss", timeout)
        return (-1, "", f"Timeout after {timeout}s")
    except FileNotFoundError:
        logger.error("claude CLI not found on PATH")
        return (-1, "", "claude CLI not found")

    logger.info("claude CLI exited with code %s", result.returncode)
    return (result.returncode, result.stdout, result.stderr)


def run_planned_pr(repo_path: str) -> tuple[int, str, str]:
    """Trigger a ``PLANNED PR`` run in ``repo_path``."""
    return run_claude("PLANNED PR", repo_path, timeout=900)


def fix_review(repo_path: str) -> tuple[int, str, str]:
    """Trigger a ``FIX REVIEW`` run in ``repo_path``."""
    return run_claude("FIX REVIEW", repo_path, timeout=600)


def diagnose_error(repo_path: str, context: str) -> tuple[int, str, str]:
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
    return run_claude(prompt, repo_path, timeout=120)


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
