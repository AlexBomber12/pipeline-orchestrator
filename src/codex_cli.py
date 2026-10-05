"""Wrapper around the ``codex`` CLI for the pipeline orchestrator daemon.

Mirrors ``claude_cli.py`` but targets OpenAI's Codex CLI (Rust binary).
Exposes ``run_planned_pr_async`` and ``fix_review_async`` for the runner
to dispatch PLANNED PR and FIX FEEDBACK workflows through Codex.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Callable

from src.config import load_config
from src.daemon.sandbox import build_bwrap_command, is_bubblewrap_available
from src.diagnosis import build_diagnosis_prompt
from src.process_supervisor import (
    ProcessSupervisionError,
    SupervisedProcess,
    launch_process,
    run_supervised_process,
)

logger = logging.getLogger(__name__)


_CODER_EXECUTION_POLICY = """OPERATOR-AUTHORIZED EXECUTION POLICY
- Deliver one coherent, independently verifiable outcome. Estimates and expected
  paths guide planning; they do not cap necessary implementation, compatibility
  work, fixtures, regression fixes, tests, or review fixes.
- You may repair a small, self-contained defect that blocks completion or
  verification, including a pre-existing test defect. Explain its cause, the
  incidental change, and validation in the PR description. Use a separate repair
  task only when the repair becomes substantial or independently useful.
- This policy supersedes blanket stop or escalation wording in older tasks when
  the sole reason is an estimate, an expected path, or a small incidental gate
  repair. It does not override genuine feature exclusions, repository or
  authorization boundaries, security or production restrictions, credential
  rules, runtime guardrails, or process-ownership requirements. Do not add
  discretionary features, broad refactors, dependency upgrades, production
  operations, credential changes, or unrelated repository work.
- Run focused checks and attempt scripts/make-review-artifacts.sh. Fix failures
  introduced by the change. A confirmed pre-existing baseline failure permits a
  ready PR only when focused checks pass, artifacts are complete, and the PR
  records the exact base SHA, a matching reproduction in an isolated checkout of
  that SHA under comparable conditions, and every remaining failure. Never call
  a failed gate green; an unexplained failure does not qualify.
- Publication is not merge approval. Unit and integration CI, coverage
  requirements, and current-change Codex approval must still pass. Do not weaken
  checks, skip tests, change coverage settings, or alter workflow enforcement.
  A baseline exception never authorizes cleanup of an unconfirmed coder process;
  process ownership must be confirmed."""


_DAEMON_AUTO_PR_HANDOFF = f"""DAEMON INVOCATION -- PUBLICATION HANDOFF
This AUTO PR run was dispatched by the pipeline-orchestrator daemon.
{_CODER_EXECUTION_POLICY}
Completion boundary for this invocation:
- Complete the supplied coherent outcome and run its required focused checks.
- Create the intended commit, then attempt scripts/make-review-artifacts.sh as
  the single final local full-gate and artifact entrypoint. Fix introduced
  failures. Normally it must exit 0; a nonzero result permits publication only
  under the confirmed-baseline policy above. The review artifacts must be
  complete and artifacts/pr.patch must be nonempty. Do not repeat an unchanged
  full gate.
- Push the task branch, publish a ready (not draft) PR, and verify the PR's
  repository, base branch, head branch, and HEAD SHA against the intended
  repository and local HEAD. Report that evidence, then exit.
- Green GitHub CI and current-change Codex approval remain merge requirements,
  but the daemon owns review triggering, CI/review waiting, later FIX dispatch,
  and merge decisions. Do not trigger or poll review, start a FIX round, or merge.
- If implementation or publication genuinely cannot complete under this policy,
  report the blocker and use the repository's ESCALATE protocol. Estimate or path
  deviation and a small incidental repair are not by themselves escalation
  reasons. Never fabricate a PR, push, gate, or approval."""

_DAEMON_FIX_HANDOFF = f"""DAEMON INVOCATION -- ONE-ITERATION PUBLICATION HANDOFF
This FIX FEEDBACK run was dispatched by the pipeline-orchestrator daemon.
{_CODER_EXECUTION_POLICY}
Completion boundary for this invocation:
- Address the supplied current feedback and any small, self-contained blocker
  needed to complete or verify it, in one iteration, and run the required
  focused checks. Do not address another PR or add unrelated work.
- Create the changed commit, then attempt scripts/make-review-artifacts.sh as the
  single final local full-gate and artifact entrypoint. Fix introduced failures.
  Normally it must exit 0; a nonzero result permits publication only under the
  confirmed-baseline policy above. The review artifacts must be complete and
  artifacts/pr.patch must be nonempty. Do not repeat an unchanged full gate.
- Push to the same PR branch, verify the remote PR HEAD is the pushed local HEAD,
  report the PR and HEAD evidence, then exit.
- Green GitHub CI and current-change Codex approval remain merge requirements,
  but the daemon owns review triggering, CI/review waiting, later FIX dispatch,
  and merge decisions. Do not wait for a new review or act on newly arriving
  findings in this invocation, and do not merge.
- If implementation or publication genuinely cannot complete under this policy,
  report the blocker and use the repository's ESCALATE protocol. Estimate or path
  deviation and a small incidental repair are not by themselves escalation
  reasons. Never fabricate a push, gate, or approval."""


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
    # Bind the daemon HOME so files written outside of codex_home_dir
    # (notably ~/.gitconfig from ``gh auth setup-git``) remain visible to
    # the sandboxed coder; without it non-interactive git push fails.
    home = os.environ.get("HOME")
    additional_rw_dirs = [home] if home else None
    return build_bwrap_command(
        command=cmd,
        repo_path=cwd,
        coder_config_dir=cfg.auth.codex_home_dir,
        gh_config_dir=cfg.auth.gh_config_dir,
        additional_rw_dirs=additional_rw_dirs,
    )


async def run_codex_async(
    prompt: str,
    cwd: str,
    timeout: int | None = 600,
    model: str | None = None,
    on_process_start: Callable[[asyncio.subprocess.Process], None] | None = None,
    on_supervised_process_start: Callable[[SupervisedProcess], None] | None = None,
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

    cmd = _maybe_wrap_sandbox(cmd, cwd)
    try:
        managed = await launch_process(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=cwd,
            stdin=asyncio.subprocess.DEVNULL,
        )
    except FileNotFoundError as exc:
        missing = getattr(exc, "filename", "")
        if missing and missing != cmd[0]:
            return (-1, "", f"cwd not found: {missing}")
        return (-1, "", "codex CLI not found")
    except Exception as exc:
        logger.error("[codex] supervised launch failed: %s", exc)
        return (
            -1,
            "",
            f"Process supervision launch failed: {type(exc).__name__}: {exc}",
        )

    try:
        result = await run_supervised_process(
            managed,
            timeout=timeout,
            on_process_start=on_process_start,
            on_supervised_process_start=on_supervised_process_start,
        )
    except ProcessSupervisionError as exc:
        logger.error("[codex] process supervision failed: %s", exc)
        stdout = exc.stdout.decode("utf-8", errors="replace")
        captured_stderr = exc.stderr.decode("utf-8", errors="replace")
        if captured_stderr:
            separator = "" if not stdout or stdout.endswith("\n") else "\n"
            stdout = f"{stdout}{separator}[captured provider stderr]\n{captured_stderr}"
        return (-1, stdout, f"Process supervision failed: {exc}")

    if result.timed_out:
        logger.error("[codex] codex exec timed out after %ss", timeout)
        return (-1, "", f"Timeout after {timeout}s")
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")
    code = result.returncode
    logger.info("[codex] codex exec exited with code %s", code)
    return (code, stdout, stderr)


async def run_planned_pr_async(
    repo_path: str,
    model: str | None = None,
    timeout: int = 900,
    on_process_start: Callable[[asyncio.subprocess.Process], None] | None = None,
    on_supervised_process_start: Callable[[SupervisedProcess], None] | None = None,
    **_kwargs: object,
) -> tuple[int, str, str]:
    """Trigger a ``PLANNED PR`` run in ``repo_path`` via Codex CLI."""
    kwargs: dict[str, object] = {"timeout": timeout, "model": model}
    if on_process_start is not None:
        kwargs["on_process_start"] = on_process_start
    if on_supervised_process_start is not None:
        kwargs["on_supervised_process_start"] = on_supervised_process_start
    return await run_codex_async("PLANNED PR", repo_path, **kwargs)


def _build_auto_pr_prompt(pr_id: str, task_file: str, task_body: str) -> str:
    return (
        f"AUTO PR\nTask: {pr_id}\nFile: {task_file}\n\n{task_body}"
        f"\n\n{_DAEMON_AUTO_PR_HANDOFF}"
    )


async def run_auto_pr_async(
    repo_path: str,
    pr_id: str,
    task_file: str,
    task_body: str,
    *,
    model: str | None = None,
    timeout: int = 900,
    on_process_start: Callable[[asyncio.subprocess.Process], None] | None = None,
    on_supervised_process_start: Callable[[SupervisedProcess], None] | None = None,
    **_kwargs: object,
) -> tuple[int, str, str]:
    """Trigger an ``AUTO PR`` run in ``repo_path`` via Codex CLI."""
    kwargs: dict[str, object] = {"timeout": timeout, "model": model}
    if on_process_start is not None:
        kwargs["on_process_start"] = on_process_start
    if on_supervised_process_start is not None:
        kwargs["on_supervised_process_start"] = on_supervised_process_start
    return await run_codex_async(
        _build_auto_pr_prompt(pr_id, task_file, task_body), repo_path, **kwargs
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
            "other PR or task in this run. Necessary small repairs that "
            "complete or verify this task are in scope under the injected "
            "execution policy."
        )
    parts.append("FIX FEEDBACK")
    parts.append(_DAEMON_FIX_HANDOFF)
    if extra_context:
        parts.append(extra_context)
    return "\n\n".join(parts)


async def fix_review_async(
    repo_path: str,
    model: str | None = None,
    timeout: int | None = None,
    on_process_start: Callable[[asyncio.subprocess.Process], None] | None = None,
    on_supervised_process_start: Callable[[SupervisedProcess], None] | None = None,
    extra_context: str | None = None,
    pr_id: str | None = None,
    task_file: str | None = None,
    **_kwargs: object,
) -> tuple[int, str, str]:
    """Trigger a ``FIX FEEDBACK`` run in ``repo_path`` via Codex CLI."""
    kwargs: dict[str, object] = {"timeout": timeout, "model": model}
    if on_process_start is not None:
        kwargs["on_process_start"] = on_process_start
    if on_supervised_process_start is not None:
        kwargs["on_supervised_process_start"] = on_supervised_process_start
    return await run_codex_async(
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
    return await run_codex_async(
        build_diagnosis_prompt(repo_path, context),
        repo_path,
        timeout=120,
        model=model,
    )
