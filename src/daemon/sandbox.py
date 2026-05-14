"""Bubblewrap sandbox helper.

Pure functions for detecting `bwrap` availability and constructing a
`bwrap` command line that wraps an inner command with kernel-enforced
filesystem restrictions.

PR-312a introduces only the helper module; PR-312b wires it into the
coder dispatch path.
"""

from __future__ import annotations

import functools
import shutil
import subprocess
import sys

# Minimal set of host paths bound read-only into the sandbox so that
# common coder binaries (claude, codex, gh, git, python) and their
# shared libraries / TLS trust store remain resolvable. The sandbox
# otherwise starts from an empty root, so sensitive trees like
# ``/home``, ``/root``, ``/data/secrets`` and ``/data/auth`` are not
# visible unless the caller explicitly opts in via the bind parameters.
ESSENTIAL_RO_PATHS: tuple[str, ...] = (
    "/usr",
    "/bin",
    "/sbin",
    "/lib",
    "/lib32",
    "/lib64",
    "/libx32",
    "/etc",
)


@functools.cache
def is_bubblewrap_available() -> bool:
    """Return True if ``bwrap`` is on PATH **and** can launch a sandbox.

    Many hardened or containerized hosts ship ``bwrap`` on PATH but
    refuse to create user namespaces at runtime (kernel
    ``user.max_user_namespaces=0``, restrictive seccomp profile,
    missing ``CAP_SYS_ADMIN`` inside an unprivileged container, etc.).
    A pure PATH check would let the helper wrap every command and
    then fail unconditionally at exec time, defeating the intended
    graceful fallback to the inner command. The smoke test below
    actually invokes ``bwrap`` to launch a trivial child, so the
    helper only returns True when sandboxing will really work.

    The result is cached for the process lifetime: both PATH lookup
    and kernel-level sandbox capability are stable once the daemon
    is running, and re-running the subprocess on every call would be
    wasteful on the hot dispatch path.
    """
    if shutil.which("bwrap") is None:
        return False
    try:
        result = subprocess.run(
            [
                "bwrap",
                "--ro-bind", "/", "/",
                "--",
                sys.executable, "-c", "",
            ],
            capture_output=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return result.returncode == 0


def build_bwrap_command(
    *,
    command: list[str],
    repo_path: str,
    coder_config_dir: str | None = None,
    gh_config_dir: str | None = None,
    additional_rw_dirs: list[str] | None = None,
    additional_ro_dirs: list[str] | None = None,
) -> list[str]:
    """Construct a ``bwrap`` command-line wrapping the given inner command.

    The sandbox starts from an empty root and binds only an allowlist of
    paths required for the wrapped process to run. The host filesystem
    root is **not** exposed wholesale, so sensitive trees such as
    ``/home``, ``/root``, ``/data/secrets`` and ``/data/auth`` are
    inaccessible unless the caller opts in via the bind parameters
    below.

    Filesystem layout inside the sandbox:

    - :data:`ESSENTIAL_RO_PATHS` (``/usr``, ``/bin``, ``/sbin``,
      ``/lib``, ``/lib32``, ``/lib64``, ``/libx32``, ``/etc``) bound
      read-only via ``--ro-bind-try`` so the binary, its shared
      libraries, and the TLS trust store remain resolvable; missing
      paths on the host are silently skipped.
    - ``repo_path``: read-write bind mount (the coder's working tree)
    - ``coder_config_dir``: read-write bind mount via ``--bind-try``
      if provided; missing on the host is silently skipped so that a
      stale optional path does not abort sandbox startup.
    - ``gh_config_dir``: read-write bind mount via ``--bind-try`` if
      provided; same fail-soft semantics as ``coder_config_dir``.
    - ``/proc``: bwrap-managed procfs
    - ``/dev``: bwrap-managed minimal devfs
    - ``/tmp``: bwrap-managed tmpfs
    - ``additional_rw_dirs``: each bound read-write
    - ``additional_ro_dirs``: each bound read-only

    Returns the full ``bwrap`` argv. If ``bwrap`` is unavailable, returns
    the inner command unchanged (caller should check
    :func:`is_bubblewrap_available` first if availability matters).
    """
    if not is_bubblewrap_available():
        return list(command)
    args: list[str] = ["bwrap"]
    for path in ESSENTIAL_RO_PATHS:
        args.extend(["--ro-bind-try", path, path])
    args.extend(["--proc", "/proc"])
    args.extend(["--dev", "/dev"])
    args.extend(["--tmpfs", "/tmp"])
    args.extend(["--bind", repo_path, repo_path])
    if coder_config_dir:
        args.extend(["--bind-try", coder_config_dir, coder_config_dir])
    if gh_config_dir:
        args.extend(["--bind-try", gh_config_dir, gh_config_dir])
    for path in additional_rw_dirs or []:
        args.extend(["--bind", path, path])
    for path in additional_ro_dirs or []:
        args.extend(["--ro-bind", path, path])
    args.extend(["--unshare-pid", "--unshare-uts", "--unshare-ipc"])
    args.append("--die-with-parent")
    args.append("--")
    args.extend(command)
    return args
