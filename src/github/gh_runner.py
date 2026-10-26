"""GitHub CLI subprocess wrapper and shared low-level helpers.

Foundation submodule of the ``src.github`` package: no inbound dependencies
on any other ``src.github`` submodule. Owns the ``gh`` CLI runner, the
``owner/repo`` URL parser, and the ISO-8601 / HTTP-404 detection helpers
that every other submodule reuses.
"""

from __future__ import annotations

import json
import re
import subprocess
from datetime import datetime

_REPO_URL_RE = re.compile(
    r"github\.com[:/]+(?P<owner>[^/]+)/(?P<repo>[^/]+?)(?:\.git)?/?$"
)


class GhNotStartedError(RuntimeError):
    """The CLI process was not created; no remote request could be submitted."""


def run_gh(
    args: list[str],
    repo: str | None = None,
    timeout: int = 30,
) -> dict | list | str:
    """Run `gh` with the given arguments and return parsed output.

    If ``repo`` is provided, ``-R <repo>`` is appended. The command's stdout is
    parsed as JSON when possible; otherwise the raw stripped string is returned.
    A non-zero exit raises ``RuntimeError`` with stderr. Known process-creation
    failures raise ``GhNotStartedError``; errors after creation keep their type.
    """
    cmd: list[str] = ["gh", *args]
    if repo:
        cmd.extend(["-R", repo])

    try:
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
    except (BlockingIOError, FileNotFoundError, PermissionError) as exc:
        # Classify only known spawn failures, at construction. Errors after
        # creation (including timeouts) retain their uncertain remote outcome.
        raise GhNotStartedError(f"gh process could not start: {exc}") from exc
    with process:
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except BaseException:
            # Match subprocess.run cleanup: terminate on communication failure;
            # the context manager reaps the child while the original error exits.
            process.kill()
            raise
    if process.returncode != 0:
        raise RuntimeError(
            f"gh {' '.join(args)} failed (exit {process.returncode}): "
            f"{stderr.strip()}"
        )

    stdout = stdout.strip()
    if not stdout:
        return ""

    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        return stdout


def get_repo_full_name(url: str) -> str:
    """Extract ``owner/repo`` from a GitHub URL.

    Accepts ``https://github.com/owner/repo``, ``...repo.git``, ``...repo/``,
    and ``git@github.com:owner/repo.git``.
    """
    match = _REPO_URL_RE.search(url.strip())
    if not match:
        raise ValueError(f"Not a recognizable GitHub URL: {url!r}")
    return f"{match.group('owner')}/{match.group('repo')}"


def _is_http_404_error(exc: RuntimeError) -> bool:
    return ("HTTP" + " 404") in str(exc)


def _parse_iso(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _extract_head_sha(payload: object) -> str:
    """Pull ``.head.sha`` out of a PR detail payload (or a bare-string mock)."""
    if isinstance(payload, dict):
        head = payload.get("head")
        if isinstance(head, dict):
            sha = head.get("sha")
            if isinstance(sha, str):
                return sha.strip()
    elif isinstance(payload, str):
        return payload.strip()
    return ""


def _extract_commit_date(payload: object) -> str:
    """Pull ``.commit.committer.date`` out of a commit detail payload."""
    if isinstance(payload, dict):
        commit = payload.get("commit")
        if isinstance(commit, dict):
            committer = commit.get("committer")
            if isinstance(committer, dict):
                date = committer.get("date")
                if isinstance(date, str):
                    return date.strip()
    elif isinstance(payload, str):
        return payload.strip()
    return ""
