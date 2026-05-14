"""Bubblewrap sandbox helper.

Pure functions for detecting `bwrap` availability and constructing a
`bwrap` command line that wraps an inner command with kernel-enforced
filesystem restrictions.

PR-312a introduces only the helper module; PR-312b wires it into the
coder dispatch path.
"""

from __future__ import annotations

import shutil


def is_bubblewrap_available() -> bool:
    """Return True if the ``bwrap`` executable is on PATH."""
    return shutil.which("bwrap") is not None


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

    Filesystem layout inside the sandbox:

    - ``/`` read-only bind mount of host ``/``
    - ``repo_path``: read-write bind mount (the coder's working tree)
    - ``coder_config_dir``: read-write bind mount if provided
    - ``gh_config_dir``: read-write bind mount if provided
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
    args.extend(["--ro-bind", "/", "/"])
    args.extend(["--proc", "/proc"])
    args.extend(["--dev", "/dev"])
    args.extend(["--tmpfs", "/tmp"])
    args.extend(["--bind", repo_path, repo_path])
    if coder_config_dir:
        args.extend(["--bind", coder_config_dir, coder_config_dir])
    if gh_config_dir:
        args.extend(["--bind", gh_config_dir, gh_config_dir])
    for path in additional_rw_dirs or []:
        args.extend(["--bind", path, path])
    for path in additional_ro_dirs or []:
        args.extend(["--ro-bind", path, path])
    args.extend(["--unshare-pid", "--unshare-uts", "--unshare-ipc"])
    args.append("--die-with-parent")
    args.append("--")
    args.extend(command)
    return args
