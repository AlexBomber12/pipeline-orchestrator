"""Identify inherited attempt ownership even when a child changes directory.

Only the attempt marker is compared; environment contents are never logged.
Linux pidfds bind signals to the observed process instead of a reusable PID.
"""

from __future__ import annotations

import os
import signal
from pathlib import Path

ATTEMPT_ENV = "PIPELINE_ATTEMPT_ID"


def stop_attempt_children(attempt_id: str, *, force: bool = False) -> str | None:
    marker = f"{ATTEMPT_ENV}={attempt_id}".encode()
    found = False
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit() or int(entry.name) == os.getpid():
            continue
        descriptor = None
        try:
            if entry.stat().st_uid != os.getuid():
                continue
            descriptor = os.pidfd_open(int(entry.name))
            environment = (entry / "environ").read_bytes().split(b"\0")
            if marker not in environment:
                continue
            signal.pidfd_send_signal(descriptor, signal.SIGKILL if force else signal.SIGTERM)
            found = True
        except (FileNotFoundError, ProcessLookupError):
            continue
        except (PermissionError, OSError):
            return "Process ownership could not be verified; rejection remains pending."
        finally:
            if descriptor is not None:
                os.close(descriptor)
    return "Attempt-owned child processes are stopping; waiting for confirmed exit." if found else None
