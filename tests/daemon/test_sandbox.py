"""Unit tests for src/daemon/sandbox.py (PR-312a)."""

from __future__ import annotations

import subprocess

import pytest

from src.daemon import sandbox


@pytest.fixture(autouse=True)
def _clear_bwrap_cache() -> None:
    """Reset the cached availability result between tests."""
    sandbox.is_bubblewrap_available.cache_clear()


def test_is_bubblewrap_available_returns_bool() -> None:
    assert isinstance(sandbox.is_bubblewrap_available(), bool)


def test_is_bubblewrap_available_false_when_not_on_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sandbox.shutil, "which", lambda _: None)
    assert sandbox.is_bubblewrap_available() is False


def test_is_bubblewrap_available_false_when_runtime_check_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sandbox.shutil, "which", lambda _: "/usr/bin/bwrap")
    monkeypatch.setattr(
        sandbox.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=args, returncode=1, stdout=b"", stderr=b"bwrap: ..."
        ),
    )
    assert sandbox.is_bubblewrap_available() is False


def test_is_bubblewrap_available_false_when_runtime_check_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sandbox.shutil, "which", lambda _: "/usr/bin/bwrap")

    def _raise(*args, **kwargs):
        raise OSError("could not exec bwrap")

    monkeypatch.setattr(sandbox.subprocess, "run", _raise)
    assert sandbox.is_bubblewrap_available() is False


def test_is_bubblewrap_available_true_when_runtime_check_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sandbox.shutil, "which", lambda _: "/usr/bin/bwrap")
    monkeypatch.setattr(
        sandbox.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=args, returncode=0, stdout=b"", stderr=b""
        ),
    )
    assert sandbox.is_bubblewrap_available() is True


def test_build_bwrap_command_no_bwrap_returns_inner_command_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sandbox, "is_bubblewrap_available", lambda: False)
    inner = ["claude", "--help"]
    result = sandbox.build_bwrap_command(command=inner, repo_path="/data/repos/x")
    assert result == inner
    assert result is not inner


def test_build_bwrap_command_includes_bwrap_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sandbox, "is_bubblewrap_available", lambda: True)
    result = sandbox.build_bwrap_command(
        command=["echo", "hi"], repo_path="/repo"
    )
    assert result[0] == "bwrap"
    # Host root must not be bound wholesale into the sandbox; only the
    # allowlisted essential paths are exposed read-only.
    for i in range(len(result) - 2):
        if result[i] in ("--ro-bind", "--ro-bind-try", "--bind"):
            assert not (
                result[i + 1] == "/" and result[i + 2] == "/"
            ), "host root must not be bound into the sandbox"
    for path in sandbox.ESSENTIAL_RO_PATHS:
        assert any(
            result[i : i + 3] == ["--ro-bind-try", path, path]
            for i in range(len(result) - 2)
        ), f"essential ro path {path} should be bound read-only"


def test_build_bwrap_command_does_not_expose_sensitive_host_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sandbox, "is_bubblewrap_available", lambda: True)
    result = sandbox.build_bwrap_command(
        command=["echo", "hi"], repo_path="/repo"
    )
    joined = " ".join(result)
    for sensitive in ("/home", "/root", "/data/auth", "/data/secrets"):
        assert sensitive not in joined, (
            f"sensitive host path {sensitive} must not appear in the "
            "default sandbox mount setup"
        )


def test_build_bwrap_command_repo_path_rw_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sandbox, "is_bubblewrap_available", lambda: True)
    result = sandbox.build_bwrap_command(
        command=["echo", "hi"], repo_path="/data/repos/foo__bar"
    )
    assert "--bind" in result
    idx = result.index("--bind")
    while idx != -1:
        if result[idx + 1] == "/data/repos/foo__bar" and result[idx + 2] == "/data/repos/foo__bar":
            break
        try:
            idx = result.index("--bind", idx + 1)
        except ValueError:
            idx = -1
    assert idx != -1, "repo_path should be --bind mounted to itself"


def test_build_bwrap_command_optional_dirs_excluded_when_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sandbox, "is_bubblewrap_available", lambda: True)
    result = sandbox.build_bwrap_command(
        command=["echo", "hi"],
        repo_path="/repo",
        coder_config_dir=None,
        gh_config_dir=None,
    )
    joined = " ".join(result)
    assert "/data/auth/claude" not in joined
    assert "/data/auth/gh" not in joined


def test_build_bwrap_command_optional_dirs_included_when_provided(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sandbox, "is_bubblewrap_available", lambda: True)
    result = sandbox.build_bwrap_command(
        command=["echo", "hi"],
        repo_path="/repo",
        coder_config_dir="/data/auth/claude",
        gh_config_dir="/data/auth/gh",
        additional_rw_dirs=["/data/secrets"],
        additional_ro_dirs=["/etc/ssl"],
    )

    def _has_bind(flag: str, src: str, dst: str) -> bool:
        for i in range(len(result) - 2):
            if result[i] == flag and result[i + 1] == src and result[i + 2] == dst:
                return True
        return False

    assert _has_bind("--bind-try", "/data/auth/claude", "/data/auth/claude")
    assert _has_bind("--bind-try", "/data/auth/gh", "/data/auth/gh")
    assert _has_bind("--bind", "/data/secrets", "/data/secrets")
    assert _has_bind("--ro-bind", "/etc/ssl", "/etc/ssl")


def test_build_bwrap_command_namespace_isolation_flags_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sandbox, "is_bubblewrap_available", lambda: True)
    result = sandbox.build_bwrap_command(
        command=["echo", "hi"], repo_path="/repo"
    )
    for flag in ("--unshare-pid", "--unshare-uts", "--unshare-ipc", "--die-with-parent"):
        assert flag in result, f"missing namespace-isolation flag {flag}"


def test_build_bwrap_command_inner_command_after_dashdash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sandbox, "is_bubblewrap_available", lambda: True)
    inner = ["claude", "--config", "/tmp/c.json"]
    result = sandbox.build_bwrap_command(command=inner, repo_path="/repo")
    assert "--" in result
    dashdash_idx = result.index("--")
    assert result[dashdash_idx + 1 :] == inner
