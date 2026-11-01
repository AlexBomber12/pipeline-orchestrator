"""Host-side config readers tolerate Settings' container-owned replacements."""

import subprocess
from pathlib import Path

import pytest

from tests.e2e import conftest as stack


def test_shared_config_snapshot_uses_owner_transport(monkeypatch, tmp_path):
    shared = tmp_path / "config.test.yml"
    content = b"repositories:\n  - url: https://github.com/octo/demo.git\n    branch: fixture\n"
    shared.write_bytes(content)
    shared.chmod(0o600)
    original_open = Path.open

    def host_open(path, *args, **kwargs):
        if path == shared:
            raise PermissionError("container-owned config")
        return original_open(path, *args, **kwargs)

    def container_read(args, **kwargs):
        assert args == [
            "docker", "compose", "-f", "docker-compose.test.yml", "exec", "-T",
            "web-test", "cat", "/data/config.test.yml",
        ]
        assert kwargs["check"] and kwargs["timeout"] == 15
        return subprocess.CompletedProcess(args, 0, content)

    monkeypatch.setattr(Path, "open", host_open)
    monkeypatch.setattr(subprocess, "run", container_read)
    monkeypatch.setattr(stack, "SHARED_CONFIG_PATH", shared)
    with pytest.raises(PermissionError):
        stack.load_config(str(shared))
    config = stack._load_shared_config()
    assert config.repositories[0].branch == "fixture"
    assert config.repositories[0].url == "https://github.com/octo/demo.git"
    with original_open(shared, "rb") as source:
        assert source.read() == content
    assert shared.stat().st_mode & 0o777 == 0o600


def test_shared_config_snapshot_propagates_transport_failure(monkeypatch):
    def unavailable(*args, **kwargs):
        raise subprocess.CalledProcessError(1, "docker")

    monkeypatch.setattr(subprocess, "run", unavailable)
    with pytest.raises(subprocess.CalledProcessError):
        stack._load_shared_config()


@pytest.mark.parametrize("producer", ["trusted-ci", "untrusted-ci"])
def test_escalation_status_uses_owner_config_and_checks_producer(monkeypatch, producer):
    from tests.e2e import test_fix_coder_escalate as escalation

    content = (
        "repositories:\n"
        "  - url: https://github.com/octo/demo.git\n"
        "    ci_fix_trusted_statuses:\n"
        "      e2e/watch-merge-gate: [user:trusted-ci]\n"
    ).encode()
    payload = {
        "id": 42, "state": "failure", "context": escalation.WATCH_GATE_CONTEXT,
        "creator": {"login": producer},
    }
    calls = []
    original_open = Path.open

    def host_open(path, *args, **kwargs):
        if path.name == "config.test.yml":
            raise PermissionError("container-owned config")
        return original_open(path, *args, **kwargs)

    def run(args, **kwargs):
        import json

        calls.append(args)
        if args[0] == "docker":
            assert args == [
                "docker", "compose", "-f", "docker-compose.test.yml", "exec", "-T",
                "web-test", "cat", "/data/config.test.yml",
            ]
            return subprocess.CompletedProcess(args, 0, content)
        assert args[:2] == ["gh", "api"]
        response = payload
        if args[-1].endswith("/status"):
            response = {"statuses": [payload]}
        elif args[-1].endswith("/statuses?per_page=100"):
            response = [payload]
        return subprocess.CompletedProcess(args, 0, json.dumps(response))

    monkeypatch.setattr(Path, "open", host_open)
    monkeypatch.setattr(subprocess, "run", run)
    if producer == "trusted-ci":
        escalation._post_failed_status("test-head")
        assert len(calls) == 4  # POST, container config, combined status, history
    else:
        with pytest.raises(AssertionError, match="does not match configured"):
            escalation._post_failed_status("test-head")
