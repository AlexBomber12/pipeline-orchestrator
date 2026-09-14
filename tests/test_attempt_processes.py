"""Attempt-owned children remain stoppable after leaving the checkout."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from src import claude_cli, codex_cli
from src.daemon import attempt_processes as processes


@pytest.mark.parametrize("force", [False, True])
def test_child_outside_checkout_stops_without_touching_other_attempt(tmp_path, monkeypatch, force):
    identity = str(uuid.uuid4())
    code = "import time; time.sleep(300)"
    owned = subprocess.Popen(
        [sys.executable, "-c", code], cwd=tmp_path, env={**os.environ, processes.ATTEMPT_ENV: identity}
    )
    unrelated = subprocess.Popen(
        [sys.executable, "-c", code], cwd=tmp_path, env={**os.environ, processes.ATTEMPT_ENV: "another-attempt"}
    )
    original = Path.iterdir

    def isolated(path):
        return (
            iter([Path("/proc") / str(owned.pid), Path("/proc") / str(unrelated.pid), Path("/proc/self")])
            if path == Path("/proc")
            else original(path)
        )

    monkeypatch.setattr(Path, "iterdir", isolated)
    try:
        assert "stopping" in processes.stop_attempt_children(identity, force=force)
        assert owned.wait(timeout=5) == -(signal.SIGKILL if force else signal.SIGTERM)
        assert unrelated.poll() is None
        assert processes.stop_attempt_children(identity) is None
    finally:
        for child in (owned, unrelated):
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)


@pytest.mark.parametrize("error", [PermissionError, OSError])
def test_uncertain_process_ownership_holds_checkout(monkeypatch, error):
    monkeypatch.setattr(Path, "iterdir", lambda path: iter([Path("/proc/1234567")]))
    monkeypatch.setattr(Path, "stat", lambda path: type("Stat", (), {"st_uid": os.getuid()})())
    monkeypatch.setattr(os, "pidfd_open", lambda pid: (_ for _ in ()).throw(error()))
    assert "could not be verified" in processes.stop_attempt_children("owned")


@pytest.mark.parametrize("provider", [claude_cli, codex_cli])
@pytest.mark.parametrize("operation", ["run_planned_pr_async", "run_auto_pr_async", "fix_review_async"])
async def test_coder_entrypoints_inherit_attempt_marker(provider, operation, monkeypatch):
    captured = []
    proc = MagicMock(returncode=0)
    proc.communicate = AsyncMock(return_value=(b"ok", b""))

    async def create(*args, **kwargs):
        captured.append(kwargs["env"])
        return proc

    monkeypatch.setattr(provider.asyncio, "create_subprocess_exec", create)
    fn = getattr(provider, operation)
    args = ("/tmp", "PR-42", "tasks/PR-42.md", "accepted body") if operation == "run_auto_pr_async" else ("/tmp",)
    await fn(*args, attempt_id="attempt-owned")
    assert captured[0][processes.ATTEMPT_ENV] == "attempt-owned"


def test_process_owned_by_another_user_is_outside_attempt_scope(monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setattr(Path, "iterdir", lambda path: iter([Path("/proc/123")]))
    monkeypatch.setattr(Path, "stat", lambda path: SimpleNamespace(st_uid=os.getuid() + 1))
    monkeypatch.setattr(os, "pidfd_open", lambda pid: pytest.fail("foreign user must not be signalled"))
    assert processes.stop_attempt_children("owned") is None
