"""Bubblewrap sandbox helper.

Pure functions for detecting `bwrap` availability and constructing a
`bwrap` command line that wraps an inner command with kernel-enforced
filesystem restrictions.

PR-312a introduces only the helper module; PR-312b wires it into the
coder dispatch path.
"""

from __future__ import annotations

import shutil

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
    - ``coder_config_dir``: read-write bind mount if provided
    - ``gh_config_dir``: read-write bind mount if provided
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
