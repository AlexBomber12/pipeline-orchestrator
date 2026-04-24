"""
Helpers to simulate CLI and network failures for external-failure tests.

CLI paths inside daemon container (verified 2026-04-23):
- claude: /usr/bin/claude
- codex:  /home/runner/.npm-global/bin/codex
"""

import subprocess
import time
import pytest
from contextlib import contextmanager

REPO_DIR = "/home/alexey/pipeline-orchestrator"
CLAUDE_PATH = "/usr/bin/claude"
CODEX_PATH = "/home/runner/.npm-global/bin/codex"


def _exec_in_container(container: str, *cmd, check=True):
    """Run shell command inside docker compose container."""
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", container, "bash", "-c", " ".join(cmd)],
        cwd=REPO_DIR,
        capture_output=True,
        text=True,
        timeout=30,
        check=check,
    )
    return result


@contextmanager
def claude_shim(mode: str):
    """
    Replace claude CLI with a shim that simulates different failures.

    Modes:
    - "not_found": command not found (CLI uninstalled simulation)
    - "exit_0_no_pr": exits 0 without creating PR (livelock trigger)
    - "exit_1": exits 1 with auth error message
    - "hang": sleeps indefinitely (hang simulation)

    Real claude CLI path inside daemon: /usr/bin/claude.
    """
    shim_content = {
        "not_found": "#!/bin/bash\necho 'claude: command not found' >&2\nexit 127\n",
        "exit_0_no_pr": (
            "#!/bin/bash\n"
            "echo 'Reading the task file and current code shows...' >&1\n"
            "echo '[truncated output, no PR URL]' >&1\n"
            "exit 0\n"
        ),
        "exit_1": (
            "#!/bin/bash\n"
            "echo 'Not logged in Please run /login' >&2\n"
            "exit 1\n"
        ),
        "hang": "#!/bin/bash\nsleep 9999\n",
    }
    script = shim_content.get(mode)
    if not script:
        raise ValueError(f"Unknown claude_shim mode: {mode}")

    # Backup real claude, install shim
    _exec_in_container("daemon", f"mv {CLAUDE_PATH} /tmp/claude-real.bak", check=False)
    shim_b64 = __import__("base64").b64encode(script.encode()).decode()
    _exec_in_container(
        "daemon",
        f"echo {shim_b64} | base64 -d > {CLAUDE_PATH} && chmod +x {CLAUDE_PATH}",
    )

    try:
        yield
    finally:
        # Restore real claude
        _exec_in_container(
            "daemon",
            f"mv /tmp/claude-real.bak {CLAUDE_PATH}",
            check=False,
        )


@contextmanager
def codex_shim(mode: str):
    """Same as claude_shim but for codex CLI at /home/runner/.npm-global/bin/codex."""
    shim_content = {
        "not_found": "#!/bin/bash\necho 'codex: command not found' >&2\nexit 127\n",
        "exit_0_no_pr": (
            "#!/bin/bash\n"
            "echo 'Task analyzed but PR creation skipped.' >&1\n"
            "exit 0\n"
        ),
    }
    script = shim_content.get(mode)
    if not script:
        raise ValueError(f"Unknown codex_shim mode: {mode}")

    _exec_in_container("daemon", f"mv {CODEX_PATH} /tmp/codex-real.bak", check=False)
    shim_b64 = __import__("base64").b64encode(script.encode()).decode()
    _exec_in_container(
        "daemon",
        f"echo {shim_b64} | base64 -d > {CODEX_PATH} && chmod +x {CODEX_PATH}",
    )

    try:
        yield
    finally:
        _exec_in_container(
            "daemon",
            f"mv /tmp/codex-real.bak {CODEX_PATH}",
            check=False,
        )


@contextmanager
def block_host(hostname: str):
    """Block DNS resolution of hostname inside daemon container via /etc/hosts."""
    _exec_in_container(
        "daemon",
        f"echo '0.0.0.0 {hostname}' >> /etc/hosts",
    )
    try:
        yield
    finally:
        _exec_in_container(
            "daemon",
            f"sed -i '/0.0.0.0 {hostname}/d' /etc/hosts",
            check=False,
        )


@contextmanager
def redis_down():
    """Stop redis container, yield, then start again."""
    subprocess.run(
        ["docker", "compose", "stop", "redis"],
        cwd=REPO_DIR,
        check=True,
        timeout=10,
    )
    try:
        yield
    finally:
        subprocess.run(
            ["docker", "compose", "start", "redis"],
            cwd=REPO_DIR,
            check=True,
            timeout=10,
        )
        # Wait for redis readiness
        for _ in range(10):
            try:
                result = subprocess.run(
                    ["docker", "compose", "exec", "-T", "redis", "redis-cli", "PING"],
                    cwd=REPO_DIR,
                    capture_output=True,
                    text=True,
                    timeout=3,
                )
                if "PONG" in result.stdout:
                    return
            except Exception:
                pass
            time.sleep(1)
