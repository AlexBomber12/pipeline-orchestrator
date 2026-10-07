from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError
from src.coder_registry import (
    ModelCatalogUnavailable,
    ModelMetadata,
    ModelReasoningEffort,
)
from src.coders import codex as codex_module
from src.coders.codex import CodexPlugin
from src.coders.codex_models import (
    CodexModel,
    CodexModelDiscoveryInvalid,
    CodexModelDiscoveryUnavailable,
    CodexReasoningEffort,
)
from src.config import AppConfig, DaemonConfig
from src.usage import OpenAIUsageProvider

_CODEX_AUTH_CAPABILITIES = {
    "can_check_cli": True,
    "can_check_saved_credentials": True,
    "can_report_authentication_mode": True,
    "can_verify_service_access": False,
    "interactive_login_methods": ["browser_oauth", "device_code"],
}


def _assert_codex_auth_status(
    result: dict[str, Any],
    *,
    status: str,
    detail: str,
    cli_available: bool | None = None,
    cli_version: str | None = None,
    saved_credentials_present: bool | None = None,
    authentication_mode: str | None = None,
    failure_reason: str | None = None,
) -> None:
    assert result == {
        "status": status,
        "detail": detail,
        "cli_available": cli_available,
        "cli_version": cli_version,
        "saved_credentials_present": saved_credentials_present,
        "authentication_mode": authentication_mode,
        "service_access_verified": None,
        "failure_reason": failure_reason,
        "capabilities": _CODEX_AUTH_CAPABILITIES,
    }


def test_codex_plugin_name() -> None:
    plugin = CodexPlugin()

    assert plugin.name == "codex"
    assert plugin.display_name == "Codex CLI"


def test_codex_plugin_models_remains_legacy_compatibility_metadata() -> None:
    plugin = CodexPlugin()

    assert plugin.models[0] == ""
    assert "gpt-5.4" in plugin.models


@pytest.mark.asyncio
async def test_codex_plugin_adapts_discovery_and_auth_context(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    captured: dict[str, object] = {}

    async def discover(**kwargs: object) -> tuple[CodexModel, ...]:
        captured.update(kwargs)
        return (
            CodexModel(
                "invoke-new",
                "Provider Name",
                True,
                "medium",
                (CodexReasoningEffort("low", "Fast"),),
            ),
        )

    monkeypatch.setenv("OPENAI_API_KEY", "must-not-be-used")
    monkeypatch.setenv("HOME", str(tmp_path / "daemon-home"))
    monkeypatch.delenv("CODEX_HOME", raising=False)
    config = AppConfig.model_validate(
        {"auth": {"codex_home_dir": str(tmp_path / "auth")}}
    )
    catalog = await CodexPlugin(discover=discover).get_model_catalog(
        config=config,
        config_path=str(tmp_path / "config.yml"),
    )

    assert catalog.source == "discovered"
    assert catalog.models == (
        ModelMetadata(
            "invoke-new",
            "Provider Name",
            True,
            "medium",
            (ModelReasoningEffort("low", "Fast"),),
        ),
    )
    assert captured["cwd"] == str(tmp_path)
    env = captured["env"]
    assert isinstance(env, dict)
    assert env["HOME"] == str(tmp_path / "daemon-home")
    assert env["CODEX_HOME"] == str(tmp_path / "auth" / ".codex")
    assert "OPENAI_API_KEY" not in env


@pytest.mark.parametrize(
    "error",
    [
        CodexModelDiscoveryUnavailable("offline"),
        CodexModelDiscoveryInvalid("malformed"),
    ],
)
@pytest.mark.asyncio
async def test_codex_plugin_normalizes_discovery_failures(
    error: Exception,
) -> None:
    async def discover(**_kwargs: object) -> tuple[CodexModel, ...]:
        raise error

    with pytest.raises(ModelCatalogUnavailable, match="unavailable"):
        await CodexPlugin(discover=discover).get_model_catalog(
            config=AppConfig(),
            config_path="config.yml",
        )


@pytest.mark.asyncio
async def test_codex_plugin_run_planned_pr_delegates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    async def fake_run_planned_pr_async(
        repo_path: str,
        model: str | None = None,
        timeout: int = 900,
        **kwargs: object,
    ) -> tuple[int, str, str]:
        captured["repo_path"] = repo_path
        captured["model"] = model
        captured["timeout"] = timeout
        captured["kwargs"] = kwargs
        return (0, "ok", "")

    monkeypatch.setattr(
        "src.coders.codex.codex_cli.run_planned_pr_async",
        fake_run_planned_pr_async,
    )

    result = await CodexPlugin().run_planned_pr(
        "/data/repos/demo",
        model="",
        timeout=321,
        reasoning_effort="high",
    )

    assert result == (0, "ok", "")
    assert captured == {
        "repo_path": "/data/repos/demo",
        "model": None,
        "timeout": 321,
        "kwargs": {"reasoning_effort": "high"},
    }


def test_codex_plugin_check_auth(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "auth:\n"
        f"  codex_home_dir: {tmp_path / 'codex-home'}\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("HOME", str(tmp_path / "daemon-home"))
    monkeypatch.delenv("CODEX_HOME", raising=False)

    calls: list[tuple[list[str], dict[str, str] | None]] = []

    def fake_run_auth_command(
        cmd: list[str], *, env: dict[str, str] | None = None
    ) -> tuple[int, str, str]:
        calls.append((cmd, env))
        if cmd == ["codex", "--version"]:
            return (0, "codex 0.99.0\n", "")
        if cmd == ["codex", "login", "status"]:
            return (0, "Logged in using ChatGPT\n", "")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "src.coders.codex._run_auth_command",
        fake_run_auth_command,
    )

    result = CodexPlugin().check_auth()

    _assert_codex_auth_status(
        result,
        status="ok",
        detail=(
            "Codex CLI 0.99.0; saved ChatGPT credentials found; "
            "service access not verified"
        ),
        cli_available=True,
        cli_version="0.99.0",
        saved_credentials_present=True,
        authentication_mode="chatgpt",
    )
    assert [cmd for cmd, _env in calls] == [
        ["codex", "--version"],
        ["codex", "login", "status"],
    ]
    assert all(env is not None for _cmd, env in calls)
    assert calls[0][1]["HOME"] == str(tmp_path / "daemon-home")
    assert calls[1][1]["HOME"] == str(tmp_path / "daemon-home")
    assert calls[0][1]["CODEX_HOME"] == str(
        tmp_path / "codex-home" / ".codex"
    )
    assert calls[1][1]["CODEX_HOME"] == str(
        tmp_path / "codex-home" / ".codex"
    )


def test_codex_plugin_check_auth_uses_pinned_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reserved_environment = {
        "HOME": "/reserved/home",
        "CODEX_HOME": "/reserved/codex-home",
    }
    calls: list[dict[str, str] | None] = []

    def fake_run_auth_command(
        cmd: list[str], *, env: dict[str, str] | None = None
    ) -> tuple[int, str, str]:
        calls.append(env)
        if cmd == ["codex", "--version"]:
            return (0, "codex 0.160.0", "")
        return (0, "Logged in using ChatGPT", "")

    monkeypatch.setattr(codex_module, "_run_auth_command", fake_run_auth_command)
    monkeypatch.setattr(
        codex_module,
        "load_config",
        lambda *_args: (_ for _ in ()).throw(AssertionError("must not reload")),
    )

    result = CodexPlugin().check_auth(environment=reserved_environment)

    assert result["status"] == "ok"
    assert calls == [reserved_environment, reserved_environment]


@pytest.mark.parametrize(
    ("output", "expected_mode", "mode_label"),
    [
        ("Logged in using an API key - sk-secret", "api_key", "API-key"),
        ("Logged in using access token", "access_token", "access-token"),
        (
            "Logged in using personal access token",
            "access_token",
            "access-token",
        ),
        (
            "Logged in using Amazon Bedrock API key",
            "bedrock_api_key",
            "Amazon Bedrock API-key",
        ),
        (
            "Logged in using Amazon Bedrock AWS access keys",
            "bedrock_access_keys",
            "Amazon Bedrock AWS access-key",
        ),
    ],
)
def test_codex_plugin_reports_known_saved_credential_modes_without_secrets(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    output: str,
    expected_mode: str,
    mode_label: str,
) -> None:
    def fake_run_auth_command(
        cmd: list[str], *, env: dict[str, str] | None = None
    ) -> tuple[int, str, str]:
        assert env is not None
        if cmd == ["codex", "--version"]:
            return (0, "codex-cli 0.160.0\n", "")
        return (0, output, "")

    monkeypatch.setattr(codex_module, "_run_auth_command", fake_run_auth_command)

    result = CodexPlugin().check_auth(
        config_path=str(tmp_path / "missing-config.yml")
    )

    _assert_codex_auth_status(
        result,
        status="ok",
        detail=(
            f"Codex CLI 0.160.0; saved {mode_label} credentials found; "
            "service access not verified"
        ),
        cli_available=True,
        cli_version="0.160.0",
        saved_credentials_present=True,
        authentication_mode=expected_mode,
    )
    assert "sk-secret" not in str(result)


def test_codex_plugin_reports_workload_identity_without_saved_credentials(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run_auth_command(
        cmd: list[str], *, env: dict[str, str] | None = None
    ) -> tuple[int, str, str]:
        assert env is not None
        if cmd == ["codex", "--version"]:
            return (0, "codex 0.160.0", "")
        return (0, "Logged in using workload identity", "")

    monkeypatch.setattr(codex_module, "_run_auth_command", fake_run_auth_command)

    result = CodexPlugin().check_auth(
        config_path=str(tmp_path / "missing-config.yml")
    )

    _assert_codex_auth_status(
        result,
        status="ok",
        detail=(
            "Codex CLI 0.160.0; workload identity selected; "
            "service access not verified"
        ),
        cli_available=True,
        cli_version="0.160.0",
        authentication_mode="workload_identity",
    )


def test_codex_plugin_rejects_unrecognized_saved_credential_output(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run_auth_command(
        cmd: list[str], *, env: dict[str, str] | None = None
    ) -> tuple[int, str, str]:
        assert env is not None
        if cmd == ["codex", "--version"]:
            return (0, "codex 0.160.0", "")
        return (0, "Logged in with token=must-not-leak", "")

    monkeypatch.setattr(codex_module, "_run_auth_command", fake_run_auth_command)

    result = CodexPlugin().check_auth(
        config_path=str(tmp_path / "missing-config.yml")
    )

    _assert_codex_auth_status(
        result,
        status="error",
        detail=(
            "Codex CLI 0.160.0; saved credential status could not be recognized"
        ),
        cli_available=True,
        cli_version="0.160.0",
        failure_reason="unrecognized_output",
    )
    assert "must-not-leak" not in str(result)


def test_codex_plugin_bounds_saved_credential_timeout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run_auth_command(
        cmd: list[str], *, env: dict[str, str] | None = None
    ) -> tuple[int, str, str]:
        assert env is not None
        if cmd == ["codex", "--version"]:
            return (0, "codex 0.160.0", "")
        return (124, "", "secret timeout details")

    monkeypatch.setattr(codex_module, "_run_auth_command", fake_run_auth_command)

    result = CodexPlugin().check_auth(
        config_path=str(tmp_path / "missing-config.yml")
    )

    _assert_codex_auth_status(
        result,
        status="error",
        detail="Codex CLI 0.160.0; saved credential check timed out",
        cli_available=True,
        cli_version="0.160.0",
        failure_reason="probe_timeout",
    )
    assert "secret timeout details" not in str(result)


@pytest.mark.parametrize(
    ("version_result", "detail", "cli_available", "failure_reason"),
    [
        (
            (124, "", "secret timeout details"),
            "Codex CLI version check timed out",
            None,
            "probe_timeout",
        ),
        (
            (0, "unexpected version token=secret", ""),
            "Codex CLI returned an unrecognized version",
            True,
            "unrecognized_output",
        ),
    ],
)
def test_codex_plugin_bounds_version_probe_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    version_result: tuple[int, str, str],
    detail: str,
    cli_available: bool | None,
    failure_reason: str,
) -> None:
    monkeypatch.setattr(
        codex_module,
        "_run_auth_command",
        lambda _cmd, *, env=None: version_result,
    )

    result = CodexPlugin().check_auth(
        config_path=str(tmp_path / "missing-config.yml")
    )

    _assert_codex_auth_status(
        result,
        status="error",
        detail=detail,
        cli_available=cli_available,
        failure_reason=failure_reason,
    )
    assert "secret" not in str(result)


def test_codex_plugin_bounds_generic_saved_credential_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run_auth_command(
        cmd: list[str], *, env: dict[str, str] | None = None
    ) -> tuple[int, str, str]:
        assert env is not None
        if cmd == ["codex", "--version"]:
            return (0, "codex 0.160.0", "")
        return (2, "", "provider response token=secret")

    monkeypatch.setattr(codex_module, "_run_auth_command", fake_run_auth_command)

    result = CodexPlugin().check_auth(
        config_path=str(tmp_path / "missing-config.yml")
    )

    _assert_codex_auth_status(
        result,
        status="error",
        detail="Codex CLI 0.160.0; saved credential check failed",
        cli_available=True,
        cli_version="0.160.0",
        failure_reason="probe_failed",
    )
    assert "secret" not in str(result)


def test_codex_plugin_create_usage_provider(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "auth:\n"
        f"  codex_home_dir: {tmp_path / 'codex-home'}\n"
        "daemon:\n"
        "  usage_api_cache_ttl_sec: 123\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("CODEX_HOME", raising=False)

    provider = CodexPlugin().create_usage_provider()

    assert isinstance(provider, OpenAIUsageProvider)
    assert provider._credentials_path == tmp_path / "codex-home" / ".codex" / "auth.json"
    assert provider._cache_ttl == 123


def test_codex_plugin_usage_provider_uses_effective_codex_home(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    configured_home = tmp_path / "configured-home"
    explicit_codex_home = tmp_path / "explicit-codex-home"
    config = AppConfig.model_validate(
        {"auth": {"codex_home_dir": str(configured_home)}}
    )
    monkeypatch.setenv("CODEX_HOME", str(explicit_codex_home))

    provider = CodexPlugin().create_usage_provider(config=config)

    assert provider._credentials_path == explicit_codex_home / "auth.json"


def test_auth_command_returns_126_on_permission_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*args: object, **kwargs: object) -> object:
        raise PermissionError("permission denied")

    monkeypatch.setattr(codex_module.subprocess, "run", fake_run)

    assert codex_module._run_auth_command(["codex", "--version"]) == (
        126,
        "",
        "permission denied",
    )


def test_auth_command_returns_127_on_file_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*args: object, **kwargs: object) -> object:
        raise FileNotFoundError()

    monkeypatch.setattr(codex_module.subprocess, "run", fake_run)

    assert codex_module._run_auth_command(["codex", "--version"]) == (
        127,
        "",
        "codex not found",
    )


def test_auth_command_returns_124_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*args: object, **kwargs: object) -> object:
        raise subprocess.TimeoutExpired(cmd=["codex", "--version"], timeout=5)

    monkeypatch.setattr(codex_module.subprocess, "run", fake_run)

    assert codex_module._run_auth_command(["codex", "--version"]) == (
        124,
        "",
        "codex timed out after 5s",
    )


def test_auth_command_returns_completed_process_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    completed = subprocess.CompletedProcess(
        args=["codex", "--version"],
        returncode=3,
        stdout="",
        stderr="warn",
    )

    def fake_run(*args: object, **kwargs: object) -> object:
        return completed

    monkeypatch.setattr(codex_module.subprocess, "run", fake_run)

    assert codex_module._run_auth_command(["codex", "--version"]) == (
        3,
        "",
        "warn",
    )


def test_first_probe_line_returns_empty_when_only_warnings_or_blank() -> None:
    assert (
        codex_module._first_probe_line("warning: heads up\n\n  \nwarning: again")
        == ""
    )


def test_check_auth_detail_fallback_for_unknown_version_stderr(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "auth:\n"
        f"  codex_home_dir: {tmp_path / 'codex-home'}\n",
        encoding="utf-8",
    )

    def fake_run_auth_command(
        cmd: list[str], *, env: dict[str, str] | None = None
    ) -> tuple[int, str, str]:
        assert env is not None
        if cmd == ["codex", "--version"]:
            return (1, "", "mysterious failure\n")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(codex_module, "_run_auth_command", fake_run_auth_command)

    result = CodexPlugin().check_auth()

    _assert_codex_auth_status(
        result,
        status="error",
        detail="Codex CLI version check failed",
        failure_reason="probe_failed",
    )
    assert "mysterious failure" not in str(result)


def test_check_auth_version_not_found_returns_cli_not_installed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "auth:\n"
        f"  codex_home_dir: {tmp_path / 'codex-home'}\n",
        encoding="utf-8",
    )

    def fake_run_auth_command(
        cmd: list[str], *, env: dict[str, str] | None = None
    ) -> tuple[int, str, str]:
        assert env is not None
        if cmd == ["codex", "--version"]:
            return (1, "", "codex: not found")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(codex_module, "_run_auth_command", fake_run_auth_command)

    _assert_codex_auth_status(
        CodexPlugin().check_auth(),
        status="error",
        detail="Codex CLI is not installed",
        cli_available=False,
        failure_reason="cli_missing",
    )


def test_check_auth_login_status_not_found_returns_cli_not_installed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "auth:\n"
        f"  codex_home_dir: {tmp_path / 'codex-home'}\n",
        encoding="utf-8",
    )

    def fake_run_auth_command(
        cmd: list[str], *, env: dict[str, str] | None = None
    ) -> tuple[int, str, str]:
        assert env is not None
        if cmd == ["codex", "--version"]:
            return (0, "codex 0.99.0\n", "")
        if cmd == ["codex", "login", "status"]:
            return (1, "", "codex: not found")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(codex_module, "_run_auth_command", fake_run_auth_command)

    _assert_codex_auth_status(
        CodexPlugin().check_auth(),
        status="error",
        detail="Codex CLI is not installed",
        cli_available=False,
        cli_version="0.99.0",
        failure_reason="cli_missing",
    )


@pytest.mark.asyncio
async def test_codex_plugin_run_auto_pr_delegates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    async def fake_run_auto_pr_async(
        repo_path: str,
        pr_id: str,
        task_file: str,
        task_body: str,
        *,
        model: str | None = None,
        timeout: int = 900,
        **kwargs: object,
    ) -> tuple[int, str, str]:
        captured["args"] = (repo_path, pr_id, task_file, task_body)
        captured["model"] = model
        captured["timeout"] = timeout
        captured["kwargs"] = kwargs
        return (0, "ok", "")

    monkeypatch.setattr(
        "src.coders.codex.codex_cli.run_auto_pr_async",
        fake_run_auto_pr_async,
    )

    result = await CodexPlugin().run_auto_pr(
        "/data/repos/demo",
        pr_id="PR-270",
        task_file="tasks/PR-270.md",
        task_body="<body>",
        model="",
        timeout=321,
        reasoning_effort="xhigh",
    )

    assert result == (0, "ok", "")
    assert captured["args"] == (
        "/data/repos/demo",
        "PR-270",
        "tasks/PR-270.md",
        "<body>",
    )
    assert captured["model"] is None
    assert captured["timeout"] == 321
    assert captured["kwargs"] == {"reasoning_effort": "xhigh"}


@pytest.mark.asyncio
async def test_codex_plugin_fix_review_delegates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    async def fake_fix_review_async(
        repo_path: str,
        model: str | None = None,
        timeout: int | None = None,
        **kwargs: object,
    ) -> tuple[int, str, str]:
        captured["repo_path"] = repo_path
        captured["model"] = model
        captured["timeout"] = timeout
        captured["kwargs"] = kwargs
        return (0, "fixed", "")

    monkeypatch.setattr(
        "src.coders.codex.codex_cli.fix_review_async",
        fake_fix_review_async,
    )

    result = await CodexPlugin().fix_review(
        "/data/repos/demo",
        model="",
        timeout=654,
        reasoning_effort="low",
    )

    assert result == (0, "fixed", "")
    assert captured == {
        "repo_path": "/data/repos/demo",
        "model": None,
        "timeout": 654,
        "kwargs": {
            "pr_id": None,
            "task_file": None,
            "reasoning_effort": "low",
        },
    }


@pytest.mark.parametrize(
    ("coder_settings", "expected"),
    [
        ({}, {"model": "legacy-codex"}),
        (
            {"codex": {"reasoning_effort": ""}},
            {"model": "legacy-codex"},
        ),
        (
            {"codex": {"reasoning_effort": "ultra"}},
            {"model": "legacy-codex", "reasoning_effort": "ultra"},
        ),
    ],
)
def test_codex_plugin_build_run_kwargs_resolves_reasoning_effort(
    coder_settings: dict[str, dict[str, object]],
    expected: dict[str, str],
) -> None:
    config = DaemonConfig(
        codex_model="legacy-codex",
        coder_settings=coder_settings,
    )

    assert CodexPlugin().build_run_kwargs(daemon_config=config) == expected


@pytest.mark.parametrize("malformed", [None, 7, False, ["high"]])
def test_daemon_config_rejects_malformed_reasoning_effort(
    malformed: object,
) -> None:
    with pytest.raises(
        ValidationError,
        match=r"coder_settings\.codex\.reasoning_effort must be a string",
    ):
        DaemonConfig(coder_settings={"codex": {"reasoning_effort": malformed}})


@pytest.mark.parametrize("malformed", [None, 7, False, ["high"]])
def test_codex_plugin_rejects_malformed_reasoning_effort(
    malformed: object,
) -> None:
    config = DaemonConfig.model_construct(
        coder_settings={"codex": {"reasoning_effort": malformed}}
    )

    with pytest.raises(
        ValueError,
        match=r"daemon\.coder_settings\.codex\.reasoning_effort must be a string",
    ):
        CodexPlugin().build_run_kwargs(daemon_config=config)


@pytest.mark.asyncio
async def test_codex_plugin_forwards_auxiliary_reasoning_effort(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def process_callback(_process: object) -> None:
        pass

    def supervised_callback(_managed: object) -> None:
        pass

    async def fake_run_codex_async(
        prompt: str, repo_path: str, **kwargs: object
    ) -> tuple[int, str, str]:
        calls.append((f"prompt:{prompt}:{repo_path}", kwargs))
        return (0, "ok", "")

    async def fake_diagnose_error_async(
        repo_path: str, context: str, **kwargs: object
    ) -> tuple[int, str, str]:
        calls.append((f"diagnose:{context}:{repo_path}", kwargs))
        return (0, "ok", "")

    monkeypatch.setattr(
        codex_module.codex_cli,
        "run_codex_async",
        fake_run_codex_async,
    )
    monkeypatch.setattr(
        codex_module.codex_cli,
        "diagnose_error_async",
        fake_diagnose_error_async,
    )
    plugin = CodexPlugin()

    await plugin.run_prompt(
        "merge",
        "/repo",
        model="",
        timeout=300,
        reasoning_effort="high",
        on_process_start=process_callback,
        on_supervised_process_start=supervised_callback,
    )
    await plugin.diagnose_error(
        "/repo",
        "boom",
        model="",
        reasoning_effort="high",
        on_process_start=process_callback,
        on_supervised_process_start=supervised_callback,
    )

    assert [call[0] for call in calls] == [
        "prompt:merge:/repo",
        "diagnose:boom:/repo",
    ]
    for _kind, kwargs in calls:
        assert kwargs["model"] is None
        assert kwargs["reasoning_effort"] == "high"
        assert kwargs["on_process_start"] is process_callback
        assert kwargs["on_supervised_process_start"] is supervised_callback


def test_api_key_environment_does_not_manufacture_saved_authentication(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "auth:\n"
        f"  codex_home_dir: {tmp_path / 'codex-home'}\n",
        encoding="utf-8",
    )

    def fake_run_auth_command(
        cmd: list[str], *, env: dict[str, str] | None = None
    ) -> tuple[int, str, str]:
        assert env is not None
        if cmd == ["codex", "--version"]:
            return (0, "codex 0.99.0\n", "")
        if cmd == ["codex", "login", "status"]:
            return (1, "", "Not logged in")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(codex_module, "_run_auth_command", fake_run_auth_command)

    _assert_codex_auth_status(
        CodexPlugin().check_auth(),
        status="error",
        detail=(
            "Codex CLI 0.99.0; no saved credentials found; "
            "service access not verified"
        ),
        cli_available=True,
        cli_version="0.99.0",
        saved_credentials_present=False,
        failure_reason="credentials_missing",
    )


def test_rate_limit_patterns_returns_both_codex_patterns() -> None:
    assert CodexPlugin().rate_limit_patterns() == [
        codex_module._CODEX_RETRY_PATTERN,
        codex_module._CODEX_USAGE_LIMIT_PATTERN,
    ]


def test_codex_device_login_context_uses_effective_auth_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path = tmp_path / "config.yml"
    configured_home = tmp_path / "configured-home"
    explicit_codex_home = tmp_path / "explicit-codex-home"
    config_path.write_text(
        f"auth:\n  codex_home_dir: {configured_home}\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("HOME", str(tmp_path / "daemon-home"))
    monkeypatch.setenv("CODEX_HOME", str(explicit_codex_home))

    plugin = CodexPlugin()
    adapter = plugin.create_device_login(config_path=str(config_path))

    assert adapter.command == ("codex", "login", "--device-auth")
    assert adapter.environment["HOME"] == str(tmp_path / "daemon-home")
    assert adapter.environment["CODEX_HOME"] == str(explicit_codex_home)
    assert adapter.working_directory == str(tmp_path)
    assert adapter.credential_location == str(explicit_codex_home)
    assert plugin.device_login_credential_location(
        config=AppConfig.model_validate(
            {"auth": {"codex_home_dir": str(configured_home)}}
        )
    ) == str(explicit_codex_home)
    assert "unsuccessful or cancelled replacement" in adapter.replacement_warning

    monkeypatch.delenv("CODEX_HOME")
    default_adapter = plugin.create_device_login(config_path=str(config_path))
    assert default_adapter.environment["HOME"] == str(tmp_path / "daemon-home")
    assert default_adapter.environment["CODEX_HOME"] == str(
        configured_home / ".codex"
    )
    assert default_adapter.credential_location == str(configured_home / ".codex")


def test_codex_credential_location_canonicalizes_symlink_aliases(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    credential_parent = tmp_path / "credential-parent"
    credential_parent.mkdir()
    first_alias = tmp_path / "first-alias"
    second_alias = tmp_path / "second-alias"
    first_alias.symlink_to(credential_parent, target_is_directory=True)
    second_alias.symlink_to(credential_parent, target_is_directory=True)
    monkeypatch.delenv("CODEX_HOME", raising=False)
    plugin = CodexPlugin()

    first = plugin.device_login_credential_location(
        config=AppConfig.model_validate(
            {"auth": {"codex_home_dir": str(first_alias)}}
        )
    )
    second = plugin.device_login_credential_location(
        config=AppConfig.model_validate(
            {"auth": {"codex_home_dir": str(second_alias)}}
        )
    )

    expected = str((credential_parent / ".codex").resolve(strict=False))
    assert first == expected
    assert second == expected


def test_codex_device_login_parser_handles_ansi_and_incremental_output() -> None:
    adapter = codex_module.CodexDeviceLoginAdapter(
        command=("codex",),
        environment={},
        working_directory="/tmp",
        credential_location="/tmp/.codex",
    )
    first = (
        "Welcome to Codex\n"
        "1. Open this link in your browser and sign in to your account\n"
        "   \x1b[94mhttps://auth.openai.com/codex/de"
    )
    assert adapter.parse_progress(first, "") is None
    complete = first + (
        "vice\x1b[0m\n\n"
        "2. Enter this one-time code \x1b[90m(expires in 15 minutes)\x1b[0m\n"
        "   \x1b[94mABCD-EFGH\x1b[0m\n"
    )

    prompt = adapter.parse_progress(complete, "ignored")

    assert prompt is not None
    assert prompt.verification_url == "https://auth.openai.com/codex/device"
    assert prompt.user_code == "ABCD-EFGH"
    assert prompt.expires_in_seconds == 900

    partial_code = complete.removesuffix("-EFGH\x1b[0m\n")
    assert adapter.parse_progress(partial_code, "") is None
    completed_code = partial_code + "-EFGH\x1b[0m\n"
    assert adapter.parse_progress(completed_code, "") == prompt

    assert (
        codex_module._line_after_marker(
            "marker with no following value\n", "marker with no following value"
        )
        is None
    )


@pytest.mark.parametrize(
    "output",
    [
        (
            "1. Open this link in your browser and sign in to your account\n"
            "   http://auth.openai.com/codex/device\n"
            "2. Enter this one-time code (expires in 15 minutes)\n"
            "   ABCD-EFGH\n"
        ),
        (
            "1. Open this link in your browser and sign in to your account\n"
            "   https://auth.openai.com/codex/device\n"
            "2. Enter this one-time code (expires in 15 minutes)\n"
            "   bad code\n"
        ),
    ],
)
def test_codex_device_login_parser_rejects_untrusted_instructions(
    output: str,
) -> None:
    adapter = codex_module.CodexDeviceLoginAdapter(
        command=("codex",),
        environment={},
        working_directory="/tmp",
        credential_location="/tmp/.codex",
    )

    with pytest.raises(ValueError, match="invalid Codex device"):
        adapter.parse_progress(output, "")


@pytest.mark.parametrize(
    ("output", "reason"),
    [
        (
            "Error logging in with device code: device auth timed out after 15 minutes",
            "provider_expired",
        ),
        (
            "device code login is not enabled for this Codex server",
            "device_login_disabled",
        ),
        (
            "ChatGPT login is disabled. Use API key login instead.",
            "device_login_disabled",
        ),
        ("sensitive provider failure", "process_failed"),
    ],
)
def test_codex_device_login_failure_classification_is_sanitized(
    output: str, reason: str
) -> None:
    adapter = codex_module.CodexDeviceLoginAdapter(
        command=("codex",),
        environment={},
        working_directory="/tmp",
        credential_location="/tmp/.codex",
    )

    failure = adapter.classify_failure("", output, 1)

    assert failure.reason == reason
    assert "sensitive provider failure" not in failure.detail
