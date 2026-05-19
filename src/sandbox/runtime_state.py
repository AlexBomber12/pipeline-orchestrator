"""Runtime detection of bubblewrap sandbox availability.

Three-state outcome:

- ``disabled``: ``coder_filesystem_isolation`` is off in config.
- ``active``: config flag is on, ``bwrap`` is on PATH, and a minimal
  smoke test launches a sandboxed child successfully.
- ``unavailable``: config flag is on but the binary is missing, the
  smoke test fails (non-zero exit), times out, or the kernel/seccomp
  profile refuses the namespace creation.

The detector is intentionally separate from
:mod:`src.daemon.sandbox`. That module owns the synchronous PATH +
smoke-test cache used on the hot coder-dispatch path; this module owns
the async detector that the daemon runs at startup and on every config
reload to populate the Redis-backed dashboard badge. Keeping the two
apart means the UI cannot accidentally promote a stale cached result
from the dispatch path into the badge, and the dispatch path cannot
accidentally pay the async-subprocess cost on the per-coder hot loop.

The Redis key consumed by the web template is
:data:`REDIS_SANDBOX_STATE_KEY`. The default value when the daemon has
not yet run a probe (test harnesses, dashboards rendered before the
first detection completes) is :attr:`SandboxState.DISABLED`, matching
the documented "absence-of-evidence" treatment in the spec.
"""

from __future__ import annotations

import asyncio
import logging
import shutil
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)

#: Redis key the daemon writes after each :func:`detect_sandbox_state`
#: probe and the web layer reads when building the dashboard context.
REDIS_SANDBOX_STATE_KEY = "daemon:sandbox_state"

#: Maximum number of seconds the bwrap smoke test is allowed to run
#: before the detector treats the sandbox as :attr:`SandboxState.UNAVAILABLE`.
#: Five seconds matches the synchronous probe in
#: :mod:`src.daemon.sandbox` so both detectors trip on the same hosts.
SMOKE_TEST_TIMEOUT_SEC = 5.0


class SandboxState(str, Enum):
    """Three-valued sandbox-availability outcome for the dashboard."""

    DISABLED = "disabled"
    ACTIVE = "active"
    UNAVAILABLE = "unavailable"


async def detect_sandbox_state(coder_filesystem_isolation: bool) -> SandboxState:
    """Return the runtime sandbox state matching the current host.

    ``coder_filesystem_isolation`` mirrors the
    ``daemon.coder_filesystem_isolation`` config flag. When the flag is
    off, the function returns :attr:`SandboxState.DISABLED` without
    invoking any subprocess so dashboards on operator workstations do
    not need ``bwrap`` installed to render correctly.

    When the flag is on, the function performs two checks in order:

    1. ``shutil.which("bwrap")`` confirms the binary is on PATH.
    2. A minimal ``bwrap --bind / / --dev-bind /dev /dev /bin/true``
       smoke test verifies that the kernel and seccomp profile actually
       allow sandbox creation. Many hardened hosts ship ``bwrap`` but
       refuse the namespace operations at runtime; without this probe
       the badge would lie to operators.

    Any failure mode (missing binary, non-zero exit, timeout, OSError
    while spawning) collapses to :attr:`SandboxState.UNAVAILABLE` so
    the dashboard never reports ``active`` when coders would silently
    fall back to running without a sandbox.
    """
    if not coder_filesystem_isolation:
        return SandboxState.DISABLED
    if shutil.which("bwrap") is None:
        return SandboxState.UNAVAILABLE
    try:
        proc = await asyncio.create_subprocess_exec(
            "bwrap",
            "--bind", "/", "/",
            "--dev-bind", "/dev", "/dev",
            "/bin/true",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except (OSError, FileNotFoundError):
        return SandboxState.UNAVAILABLE
    try:
        await asyncio.wait_for(proc.wait(), timeout=SMOKE_TEST_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        proc.kill()
        try:
            await proc.wait()
        except Exception:
            pass
        return SandboxState.UNAVAILABLE
    if proc.returncode != 0:
        return SandboxState.UNAVAILABLE
    return SandboxState.ACTIVE


async def refresh_sandbox_state(
    redis_client: Any,
    coder_filesystem_isolation: bool,
) -> SandboxState:
    """Probe sandbox availability and persist the result in Redis.

    Writes :data:`REDIS_SANDBOX_STATE_KEY` so the web dashboard renders
    the badge from the same probe the daemon just ran. When the config
    flag is on but the runtime is unavailable, logs a single ``WARNING``
    with installation guidance so operators see the mismatch in the
    daemon log even if they never open the dashboard.

    Redis write failures are logged and swallowed: a transient Redis
    outage must not prevent daemon startup, and the next refresh
    (config reload or next restart) gets another chance.
    """
    state = await detect_sandbox_state(coder_filesystem_isolation)
    try:
        await redis_client.set(REDIS_SANDBOX_STATE_KEY, state.value)
    except Exception:
        logger.warning(
            "Failed to persist %s; dashboard badge may be stale",
            REDIS_SANDBOX_STATE_KEY,
            exc_info=True,
        )
    if (
        coder_filesystem_isolation
        and state == SandboxState.UNAVAILABLE
    ):
        logger.warning(
            "Sandbox enabled in config but bubblewrap unavailable. "
            "Coders will run WITHOUT sandbox. Install bwrap or set "
            "coder_filesystem_isolation=false to suppress this warning."
        )
    return state
