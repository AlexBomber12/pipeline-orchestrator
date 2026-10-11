"""Focused tests for isolated coder authentication context forwarding."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from src import coder_auth, coder_auth_worker
from src.coder_registry import CoderAuthCapabilities, coder_auth_payload
from src.coders import claude as claude_module
from src.coders import codex as codex_module
from src.coders.claude import ClaudePlugin
from src.coders.codex import CodexPlugin


def _assert_probe_error(result: dict[str, Any], display_name: str) -> None:
    assert result["status"] == "error"
    assert result["detail"] == (
        f"{display_name} auth check failed (TypeError)"
    )
    assert result["failure_reason"] == "probe_failed"


def test_explicit_environment_crosses_real_worker_boundary(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "location.txt"
    config_path.write_text("location-b")
    sensitive = "sensitive-auth-sentinel"
    environment = {
        "PIPELINE_TEST_CREDENTIAL_LOCATION": "location-a",
        "PIPELINE_TEST_SENSITIVE": sensitive,
    }
    original = dict(environment)

    result = asyncio.run(
        coder_auth.isolated_auth_probe(
            "third",
            (
                "tests.configured_coder_plugin:"
                "build_bound_environment_auth_plugin"
            ),
            "Bound Environment Test Coder",
            config_path=str(config_path),
            env=environment,
        )
    )

    assert result["status"] == "ok"
    assert result["detail"] == "bound environment preserved"
    assert environment == original
    assert sensitive not in str(result)


def test_parent_snapshots_environment_and_uses_nonsecret_control_arg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_args: tuple[object, ...] = ()
    captured_kwargs: dict[str, object] = {}

    class _Process:
        returncode = 1

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"", b""

    async def create_subprocess(
        *args: object, **kwargs: object
    ) -> _Process:
        nonlocal captured_args
        captured_args = args
        captured_kwargs.update(kwargs)
        return _Process()

    monkeypatch.setattr(
        coder_auth.asyncio,
        "create_subprocess_exec",
        create_subprocess,
    )
    sensitive = "sensitive-auth-sentinel"
    environment = {"CREDENTIAL_SENTINEL": sensitive}

    result = asyncio.run(
        coder_auth.isolated_auth_probe(
            "third",
            "module:factory",
            "Worker",
            config_path="/cfg",
            env=environment,
        )
    )

    worker_environment = captured_kwargs["env"]
    assert worker_environment == environment
    assert worker_environment is not environment
    assert coder_auth_worker.EXPLICIT_ENVIRONMENT_ARG in captured_args
    assert sensitive not in " ".join(str(arg) for arg in captured_args)
    assert sensitive not in str(result)


@pytest.mark.parametrize(
    ("plugin", "module"),
    [
        (ClaudePlugin(), claude_module),
        (CodexPlugin(), codex_module),
    ],
)
def test_worker_forwards_environment_to_builtin_auth_methods(
    monkeypatch: pytest.MonkeyPatch,
    plugin: ClaudePlugin | CodexPlugin,
    module: object,
) -> None:
    calls: list[dict[str, str] | None] = []

    def run_auth_command(
        _command: list[str], *, env: dict[str, str] | None = None
    ) -> tuple[int, str, str]:
        calls.append(env)
        return 127, "", "not found"

    monkeypatch.setattr(module, "_run_auth_command", run_auth_command)
    monkeypatch.setattr(
        coder_auth_worker,
        "_load_plugin",
        lambda _plugin_id, _reference: plugin,
    )
    environment = {"BOUND_HOME": "/credentials/a"}

    result = coder_auth_worker.run_probe(
        plugin.name,
        "module:factory",
        "/configuration/b",
        environment=environment,
    )

    assert result["failure_reason"] == "cli_missing"
    assert calls == [environment]


def test_worker_supports_kwargs_and_passes_fresh_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    received: dict[str, object] = {}

    class _KwargsPlugin:
        display_name = "Kwargs Coder"

        def check_auth(self, **kwargs: object) -> dict[str, str]:
            received.update(kwargs)
            environment = kwargs["environment"]
            assert isinstance(environment, dict)
            environment["MUTATED"] = "yes"
            return {"status": "ok", "detail": "ready"}

    monkeypatch.setattr(
        coder_auth_worker,
        "_load_plugin",
        lambda _plugin_id, _reference: _KwargsPlugin(),
    )
    environment = {"BOUND_HOME": "/credentials/a"}

    result = coder_auth_worker.run_probe(
        "third",
        "module:factory",
        "/configuration/b",
        environment=environment,
    )

    assert result["status"] == "ok"
    assert received["environment"] == {
        "BOUND_HOME": "/credentials/a",
        "MUTATED": "yes",
    }
    assert received["environment"] is not environment
    assert environment == {"BOUND_HOME": "/credentials/a"}


def test_legacy_probe_is_unbound_but_explicit_incompatibility_is_sanitized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    class _LegacyPlugin:
        display_name = "Legacy Coder"

        def check_auth(self) -> dict[str, str]:
            nonlocal calls
            calls += 1
            return {"status": "ok", "detail": "ready"}

    monkeypatch.setattr(
        coder_auth_worker,
        "_load_plugin",
        lambda _plugin_id, _reference: _LegacyPlugin(),
    )

    unbound = coder_auth_worker.run_probe(
        "third", "module:factory", "/configuration/b"
    )
    sensitive = "sensitive-auth-sentinel"
    incompatible = coder_auth_worker.run_probe(
        "third",
        "module:factory",
        "/configuration/b",
        environment={"SECRET": sensitive},
    )

    assert unbound["status"] == "ok"
    assert calls == 1
    _assert_probe_error(incompatible, "Legacy Coder")
    assert sensitive not in str(incompatible)


def test_worker_main_validates_control_argument_and_binds_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    def run_probe(
        _plugin_id: str,
        _reference: str,
        _config_path: str,
        **kwargs: object,
    ) -> dict[str, str]:
        calls.append(kwargs)
        return {"status": "ok", "detail": "ready"}

    monkeypatch.setattr(coder_auth_worker, "run_probe", run_probe)
    monkeypatch.setenv("BOUND_HOME", "/credentials/a")

    monkeypatch.setattr(
        coder_auth_worker.sys,
        "argv",
        ["worker", "third", "module:factory", "/cfg", "--unknown"],
    )
    with pytest.raises(SystemExit, match="2"):
        coder_auth_worker.main()

    monkeypatch.setattr(
        coder_auth_worker.sys,
        "argv",
        [
            "worker",
            "third",
            "module:factory",
            "/cfg",
            coder_auth_worker.EXPLICIT_ENVIRONMENT_ARG,
        ],
    )
    coder_auth_worker.main()

    environment = calls[0]["environment"]
    assert isinstance(environment, dict)
    assert environment["BOUND_HOME"] == "/credentials/a"


@pytest.mark.parametrize(
    ("version_result", "methods", "reason"),
    [
        ((0, "2.1.126 (Claude Code)", ""), ["browser_code"], "credentials_missing"),
        ((0, "2.2.0", ""), ["browser_code"], "credentials_missing"),
        ((0, "2.1.125", ""), [], "credentials_missing"),
        ((127, "", "claude not found"), [], "cli_missing"),
        ((124, "", "timeout"), [], "probe_timeout"),
        ((0, "unrecognized", ""), [], "unrecognized_output"),
    ],
)
def test_worker_preserves_real_claude_probe_capabilities(
    monkeypatch: pytest.MonkeyPatch,
    version_result: tuple[int, str, str], methods: list[str], reason: str,
) -> None:
    environment = {"CLAUDE_CONFIG_DIR": "/bound/claude", "HOME": "/bound/home"}
    calls: list[list[str]] = []

    def run_auth_command(
        command: list[str], *, env: dict[str, str] | None = None,
    ) -> tuple[int, str, str]:
        assert env == environment
        assert env is not environment
        calls.append(command)
        if command == ["claude", "--version"]:
            return version_result
        assert command == ["claude", "auth", "status"]
        return 1, '{"loggedIn":false,"authMethod":"none"}', ""

    monkeypatch.setattr(claude_module, "_run_auth_command", run_auth_command)
    result = coder_auth_worker.run_probe(
        "claude", "src.coders.claude:ClaudePlugin", "/unused/config.yml",
        environment=environment,
    )
    assert result["capabilities"]["interactive_login_methods"] == methods
    assert result["failure_reason"] == reason
    assert result["saved_credentials_present"] is None
    assert result["service_access_verified"] is None
    assert result["status"] == "error"
    assert calls[0] == ["claude", "--version"]
    assert ClaudePlugin.auth_capabilities.interactive_login_methods == ()


@pytest.mark.parametrize(
    ("extra", "methods", "failed"),
    [
        ({}, ["device_code"], False),
        ({"capabilities": {}}, None, False),
        ({"capabilities": None}, None, False),
        ({"capabilities": {"interactive_login_methods": []}}, [], False),
        ({"capabilities": {"interactive_login_methods": None}}, None, False),
        ({"capabilities": {"interactive_login_methods": ["browser_code"], "secret": "synthetic-secret"}},
         ["browser_code"], False),
        ({"capabilities": "synthetic-secret"}, None, True),
        ({"capabilities": []}, None, True),
        ({"capabilities": {"can_check_cli": "synthetic-secret"}}, None, True),
        ({"capabilities": {"interactive_login_methods": "synthetic-secret"}}, None, True),
        ({"capabilities": {"interactive_login_methods": ["synthetic-secret"]}}, None, True),
    ],
)
def test_worker_selects_explicit_capabilities_by_presence_and_validates(
    monkeypatch: pytest.MonkeyPatch, extra: dict[str, Any],
    methods: list[str] | None, failed: bool,
) -> None:
    raw = {"status": "ok", "detail": "ready", "secret": "synthetic-secret", **extra}

    class _Plugin:
        display_name = "Test Coder"
        auth_capabilities = CoderAuthCapabilities(interactive_login_methods=("device_code",))

        def check_auth(self) -> dict[str, Any]:
            return raw

    monkeypatch.setattr(coder_auth_worker, "_load_plugin", lambda *_args: _Plugin())
    result = coder_auth_worker.run_probe("test", "module:factory", "/unused")
    if failed:
        _assert_probe_error(result, "Test Coder")
    else:
        assert result["status"] == "ok"
        assert result["detail"] == "ready"
    assert result["capabilities"]["interactive_login_methods"] == methods
    assert "synthetic-secret" not in str(result)
    assert "secret" not in result
    assert "secret" not in result["capabilities"]
    assert result["saved_credentials_present"] is None
    assert result["service_access_verified"] is None
    assert _Plugin.auth_capabilities.interactive_login_methods == ("device_code",)
    assert raw == {"status": "ok", "detail": "ready", "secret": "synthetic-secret", **extra}
    if "capabilities" not in extra:
        assert result == coder_auth_payload(raw, capabilities=_Plugin.auth_capabilities)
