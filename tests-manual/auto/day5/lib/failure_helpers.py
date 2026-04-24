"""
Helpers to simulate CLI and network failures for external-failure tests.

Day 5 fixes incorporated vs Day 4:
- OBS-9: Privileged ops (write /usr/local/bin/*, write /etc/hosts) use `-u root`
  to bypass the runner:1000:1000 user restriction. Verification still runs as runner.
- Shim install/uninstall is idempotent and safe if previous test crashed.
"""

import base64
import subprocess
import time
from contextlib import contextmanager

REPO_DIR = "/home/alexey/pipeline-orchestrator"


def _exec_in_container(container: str, *cmd, check=True, as_root: bool = False):
    """Run shell command inside docker compose container.

    as_root: adds `-u root` to docker compose exec for privileged file writes.
    Needed for shim installation and /etc/hosts edits (OBS-9).
    """
    exec_args = ["docker", "compose", "exec", "-T"]
    if as_root:
        exec_args.extend(["-u", "root"])
    exec_args.extend([container, "bash", "-c", " ".join(cmd)])

    result = subprocess.run(
        exec_args,
        cwd=REPO_DIR,
        capture_output=True,
        text=True,
        timeout=30,
        check=check,
    )
    return result


@contextmanager
def claude_shim(mode: str):
    """Replace claude CLI inside daemon container with a failure shim.

    Modes:
    - "not_found": command not found (exit 127)
    - "exit_0_no_pr": exits 0 without creating PR (livelock trigger for PR-190a)
    - "exit_1": exits 1 with auth error message
    - "hang": sleeps indefinitely (timeout trigger)

    Uses `-u root` for install/restore so the shim can be written to
    /usr/local/bin/ (see OBS-9 Day 4).
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

    _exec_in_container(
        "daemon",
        "mv /usr/local/bin/claude /tmp/claude-real.bak 2>/dev/null || true",
        check=False, as_root=True,
    )

    shim_b64 = base64.b64encode(script.encode()).decode()
    _exec_in_container(
        "daemon",
        f"echo {shim_b64} | base64 -d > /usr/local/bin/claude && chmod +x /usr/local/bin/claude",
        as_root=True,
    )

    try:
        yield
    finally:
        _exec_in_container(
            "daemon",
            "mv /tmp/claude-real.bak /usr/local/bin/claude 2>/dev/null || true",
            check=False, as_root=True,
        )


@contextmanager
def codex_shim(mode: str):
    """Replace codex CLI inside daemon container. See claude_shim docs."""
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

    _exec_in_container(
        "daemon",
        "mv /usr/local/bin/codex /tmp/codex-real.bak 2>/dev/null || true",
        check=False, as_root=True,
    )

    shim_b64 = base64.b64encode(script.encode()).decode()
    _exec_in_container(
        "daemon",
        f"echo {shim_b64} | base64 -d > /usr/local/bin/codex && chmod +x /usr/local/bin/codex",
        as_root=True,
    )

    try:
        yield
    finally:
        _exec_in_container(
            "daemon",
            "mv /tmp/codex-real.bak /usr/local/bin/codex 2>/dev/null || true",
            check=False, as_root=True,
        )


@contextmanager
def block_host(hostname: str):
    """Block DNS resolution of hostname inside daemon container via /etc/hosts.

    Uses `-u root` (OBS-9 fix) since /etc/hosts is not writable as runner user.
    """
    _exec_in_container(
        "daemon",
        f"echo '0.0.0.0 {hostname}' >> /etc/hosts",
        as_root=True,
    )
    try:
        yield
    finally:
        _exec_in_container(
            "daemon",
            f"sed -i '/0.0.0.0 {hostname}/d' /etc/hosts",
            check=False, as_root=True,
        )


@contextmanager
def redis_down(duration_sec: int = 10):
    """Stop redis container, yield for duration, then start again."""
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
