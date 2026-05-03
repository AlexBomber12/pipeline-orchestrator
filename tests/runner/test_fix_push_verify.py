"""Direct unit tests for ``src/daemon/fix_push_verify.py`` (PR-230)."""

from __future__ import annotations

import subprocess
from typing import Any

import pytest

from src.daemon import fix_push_verify
from src.daemon import git_ops as git_ops_module
from tests.runner import _helpers as h


def _fake_git_factory(
    *,
    remote_head: str = "head-after-def",
    is_ancestor_rc: int = 0,
    fetch_exc: Exception | None = None,
    rev_parse_exc: Exception | None = None,
    merge_base_exc: Exception | None = None,
):
    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> h._FakeCompletedProcess:
        if args and args[0] == "fetch":
            if fetch_exc is not None:
                raise fetch_exc
            return h._FakeCompletedProcess(args=["git", *args], returncode=0)
        if args[:2] == ("rev-parse",) + (args[1],):
            if rev_parse_exc is not None:
                raise rev_parse_exc
            return h._FakeCompletedProcess(
                args=["git", *args],
                stdout=f"{remote_head}\n",
                returncode=0,
            )
        if args[:2] == ("merge-base", "--is-ancestor"):
            if merge_base_exc is not None:
                raise merge_base_exc
            return h._FakeCompletedProcess(args=["git", *args], returncode=is_ancestor_rc)
        return h._FakeCompletedProcess(args=["git", *args], returncode=0)

    return fake_git


def test_verify_pushes_since_true_when_remote_equals_head_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Remote SHA matches the new HEAD: clean push observed."""
    monkeypatch.setattr(
        git_ops_module,
        "_git",
        _fake_git_factory(remote_head="bbb222"),
    )
    runner = h._make_runner()
    result = fix_push_verify.verify_pushes_since(
        runner, "pr-feature", "aaa111", "bbb222", context="after FIX exit"
    )
    assert result is True


def test_verify_pushes_since_false_when_remote_still_at_last_known(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Remote unchanged from ``last_known_sha`` despite local move: no push."""
    monkeypatch.setattr(
        git_ops_module,
        "_git",
        _fake_git_factory(remote_head="aaa111"),
    )
    runner = h._make_runner()
    result = fix_push_verify.verify_pushes_since(
        runner, "pr-feature", "aaa111", "bbb222", context="after FIX exit"
    )
    assert result is False


def test_verify_pushes_since_true_via_ancestry_when_remote_advanced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Remote moved past ``head_after`` (merge or amend); ancestry confirms push."""
    monkeypatch.setattr(
        git_ops_module,
        "_git",
        _fake_git_factory(remote_head="ccc333", is_ancestor_rc=0),
    )
    runner = h._make_runner()
    result = fix_push_verify.verify_pushes_since(
        runner, "pr-feature", "aaa111", "bbb222", context="after FIX exit"
    )
    assert result is True


def test_verify_pushes_since_false_when_force_push_compresses_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Force-push: remote diverged from ``head_after`` and ancestry fails."""
    monkeypatch.setattr(
        git_ops_module,
        "_git",
        _fake_git_factory(remote_head="ddd444", is_ancestor_rc=1),
    )
    runner = h._make_runner()
    result = fix_push_verify.verify_pushes_since(
        runner, "pr-feature", "aaa111", "bbb222", context="after FIX exit"
    )
    assert result is False


def test_verify_pushes_since_none_on_fetch_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``git fetch`` timeout returns ``None`` so callers can fail-open."""
    monkeypatch.setattr(
        git_ops_module,
        "_git",
        _fake_git_factory(fetch_exc=subprocess.TimeoutExpired(cmd="git", timeout=60)),
    )
    runner = h._make_runner()
    result = fix_push_verify.verify_pushes_since(
        runner, "pr-feature", "aaa111", "bbb222", context="after FIX stop"
    )
    assert result is None
    assert any(
        "fetch pr-feature failed after FIX stop" in event["event"]
        for event in runner.state.history
    )


def test_verify_pushes_since_none_on_rev_parse_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``rev-parse`` OSError returns ``None`` and logs the failure."""
    monkeypatch.setattr(
        git_ops_module,
        "_git",
        _fake_git_factory(rev_parse_exc=OSError("disk full")),
    )
    runner = h._make_runner()
    result = fix_push_verify.verify_pushes_since(
        runner, "pr-feature", "aaa111", "bbb222", context="after FIX exit"
    )
    assert result is None
    assert any(
        "rev-parse origin/pr-feature failed after FIX exit" in event["event"]
        for event in runner.state.history
    )


def test_verify_pushes_since_none_on_merge_base_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``merge-base`` failure on the ancestry leg returns ``None``."""
    monkeypatch.setattr(
        git_ops_module,
        "_git",
        _fake_git_factory(
            remote_head="ccc333",
            merge_base_exc=subprocess.TimeoutExpired(cmd="git", timeout=60),
        ),
    )
    runner = h._make_runner()
    result = fix_push_verify.verify_pushes_since(
        runner, "pr-feature", "aaa111", "bbb222", context="after FIX exit"
    )
    assert result is None
