from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError
from src.coder_registry import (
    ModelCatalogUnavailable,
    ModelMetadata,
    ModelReasoningEffort,
)
from src.coders import claude as claude_module
from src.coders.claude import ClaudePlugin
from src.coders.claude_login import ClaudeBrowserLoginAdapter
from src.coders.claude_models import (
    ClaudeModelDiscoveryInvalid,
    ClaudeModelDiscoveryUnavailable,
)
from src.config import AppConfig, DaemonConfig

_CLAUDE_AUTH_CAPABILITIES = {
    "can_check_cli": True,
    "can_check_saved_credentials": True,
    "can_report_authentication_mode": True,
    "can_verify_service_access": False,
    "interactive_login_methods": [],
}


def _assert_claude_auth_status(
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
        "capabilities": _CLAUDE_AUTH_CAPABILITIES,
    }


def _check_auth_with_results(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    version_result: tuple[int, str, str],
    status_result: tuple[int, str, str] | None,
) -> tuple[dict[str, Any], list[tuple[list[str], dict[str, str] | None]]]:
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        "auth:\n"
        f"  claude_config_dir: {tmp_path / 'claude-auth'}\n",
        encoding="utf-8",
    )
    calls: list[tuple[list[str], dict[str, str] | None]] = []

    def fake_run_auth_command(
        cmd: list[str], *, env: dict[str, str] | None = None
    ) -> tuple[int, str, str]:
        calls.append((cmd, env))
        if cmd == ["claude", "--version"]:
            return version_result
        if cmd == ["claude", "auth", "status"] and status_result is not None:
            return status_result
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(claude_module, "_run_auth_command", fake_run_auth_command)
    return ClaudePlugin().check_auth(config_path=str(config_path)), calls


def test_claude_plugin_name() -> None:
    plugin = ClaudePlugin()

    assert plugin.name == "claude"
    assert plugin.display_name == "Claude Code"


def test_claude_plugin_models() -> None:
    plugin = ClaudePlugin()

    assert plugin.models == ["opus", "sonnet"]


def test_claude_credential_hooks_normalize_without_side_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("UNRELATED", "preserved")
    configured = Path("auth") / "nested" / ".." / "claude"
    config = AppConfig.model_validate(
        {"auth": {"claude_config_dir": str(configured)}}
    )
    plugin = ClaudePlugin()

    location = plugin.browser_login_credential_location(config=config)
    environment = plugin.build_credential_environment(
        config=config,
        credential_location=location,
    )
    second = plugin.build_credential_environment(
        config=config,
        credential_location="exact/alternate-location",
    )

    assert location == str(tmp_path / "auth" / "claude")
    assert not (tmp_path / "auth").exists()
    assert environment["HOME"] == str(tmp_path / "home")
    assert environment["UNRELATED"] == "preserved"
    assert environment["CLAUDE_CONFIG_DIR"] == location
    assert second["CLAUDE_CONFIG_DIR"] == "exact/alternate-location"
    assert second is not environment
    descriptor = ClaudeBrowserLoginAdapter(
        environment=environment,
        working_directory=str(tmp_path),
        credential_location=location,
        observed_cli_version="2.1.126",
    )
    assert descriptor.credential_location == location


@pytest.mark.asyncio
async def test_claude_plugin_discovers_exact_catalog_in_configured_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claude_config_dir = tmp_path / "claude-auth"
    config_path = tmp_path / "config" / "config.yml"
    config = AppConfig.model_validate(
        {"auth": {"claude_config_dir": str(claude_config_dir)}}
    )
    advertised = (
        ModelMetadata(
            "provider-opus",
            "Claude Opus (Provider)",
            reasoning_efforts=(
                ModelReasoningEffort("low"),
                ModelReasoningEffort("high"),
            ),
        ),
        ModelMetadata("provider-sonnet", "Claude Sonnet (Provider)"),
    )
    captured: dict[str, object] = {}
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", "must-be-overridden")
    monkeypatch.setenv("CLAUDE_AUTH_MODE", "preserve-me")

    async def discover(**kwargs: object) -> tuple[ModelMetadata, ...]:
        captured.update(kwargs)
        return advertised

    plugin = ClaudePlugin(discover=discover)
    catalog = await plugin.get_model_catalog(
        config=config,
        config_path=str(config_path),
    )

    assert captured["cwd"] == str(config_path.parent)
    env = captured["env"]
    assert isinstance(env, dict)
    assert env["CLAUDE_CONFIG_DIR"] == str(claude_config_dir)
    assert env["CLAUDE_AUTH_MODE"] == "preserve-me"
    assert catalog.models is advertised
    assert catalog.source == "discovered"
    assert catalog.description == (
        "2 models advertised by Claude CLI; service access and account "
        "entitlement are not verified."
    )
    assert plugin.model_catalog_refreshable is True
    assert plugin.model_setting.default_label == "Application default (opus)"


@pytest.mark.asyncio
async def test_claude_plugin_discovery_uses_copied_bound_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = AppConfig.model_validate(
        {"auth": {"claude_config_dir": str(tmp_path / "configured-b")}}
    )
    supplied = {
        "HOME": str(tmp_path / "bound-home"),
        "CLAUDE_CONFIG_DIR": str(tmp_path / "bound-a"),
        "MARKER": "caller",
    }
    captured: dict[str, object] = {}
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "ambient-c"))

    async def discover(**kwargs: object) -> tuple[ModelMetadata, ...]:
        captured.update(kwargs)
        env = kwargs["env"]
        assert isinstance(env, dict)
        env["MARKER"] = "mutated"
        return ()

    await ClaudePlugin(discover=discover).get_model_catalog(
        config=config,
        config_path=str(tmp_path / "config.yml"),
        environment=supplied,
    )

    env = captured["env"]
    assert isinstance(env, dict)
    assert env["CLAUDE_CONFIG_DIR"] == str(tmp_path / "bound-a")
    assert supplied["MARKER"] == "caller"


def test_claude_plugin_catalog_cache_key_scopes_credentials_and_working_dir(
    tmp_path: Path,
) -> None:
    plugin = ClaudePlugin()
    first = AppConfig.model_validate(
        {"auth": {"claude_config_dir": str(tmp_path / "first-auth")}}
    )
    second = AppConfig.model_validate(
        {"auth": {"claude_config_dir": str(tmp_path / "second-auth")}}
    )

    first_key = plugin.model_catalog_cache_key(
        config=first,
        config_path=str(tmp_path / "first-workspace" / "config.yml"),
    )

    assert first_key != plugin.model_catalog_cache_key(
        config=second,
        config_path=str(tmp_path / "first-workspace" / "config.yml"),
    )
    assert first_key != plugin.model_catalog_cache_key(
        config=first,
        config_path=str(tmp_path / "second-workspace" / "config.yml"),
    )
    assert first_key == (
        str(tmp_path / "first-auth"),
        str(tmp_path / "first-workspace"),
    )


@pytest.mark.asyncio
async def test_claude_plugin_preserves_empty_discovery_result() -> None:
    async def discover(**_kwargs: object) -> tuple[ModelMetadata, ...]:
        return ()

    catalog = await ClaudePlugin(discover=discover).get_model_catalog(
        config=AppConfig(),
        config_path="config.yml",
    )

    assert catalog.models == ()
    assert catalog.source == "discovered"
    assert catalog.description == "Claude CLI advertised no usable models."


@pytest.mark.asyncio
async def test_claude_plugin_sanitizes_discovery_error_and_preserves_ownership(
) -> None:
    managed = object()
    cleanup_result = object()

    async def discover(**_kwargs: object) -> tuple[ModelMetadata, ...]:
        raise ClaudeModelDiscoveryUnavailable(
            "provider-secret-must-not-leak",
            managed=managed,  # type: ignore[arg-type]
            cleanup_result=cleanup_result,  # type: ignore[arg-type]
        )

    with pytest.raises(ModelCatalogUnavailable) as raised:
        await ClaudePlugin(discover=discover).get_model_catalog(
            config=AppConfig(),
            config_path="config.yml",
        )

    assert str(raised.value) == "Claude CLI model discovery is unavailable"
    assert "provider-secret" not in str(raised.value)
    assert raised.value.managed is managed
    assert raised.value.cleanup_result is cleanup_result


@pytest.mark.asyncio
async def test_claude_plugin_sanitizes_invalid_discovery_response() -> None:
    async def discover(**_kwargs: object) -> tuple[ModelMetadata, ...]:
        raise ClaudeModelDiscoveryInvalid("provider-output-must-not-leak")

    with pytest.raises(
        ModelCatalogUnavailable,
        match="^Claude CLI model discovery is unavailable$",
    ):
        await ClaudePlugin(discover=discover).get_model_catalog(
            config=AppConfig(),
            config_path="config.yml",
        )


@pytest.mark.asyncio
async def test_claude_plugin_run_planned_pr_delegates(
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
        "src.coders.claude.claude_cli.run_planned_pr_async",
        fake_run_planned_pr_async,
    )

    result = await ClaudePlugin().run_planned_pr(
        "/data/repos/demo",
        model="opus",
        timeout=321,
        reasoning_effort="high",
    )

    assert result == (0, "ok", "")
    assert captured == {
        "repo_path": "/data/repos/demo",
        "model": "opus",
        "timeout": 321,
        "kwargs": {"reasoning_effort": "high"},
    }


@pytest.mark.asyncio
async def test_claude_plugin_run_auto_pr_delegates(
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
        "src.coders.claude.claude_cli.run_auto_pr_async",
        fake_run_auto_pr_async,
    )

    result = await ClaudePlugin().run_auto_pr(
        "/data/repos/demo",
        pr_id="PR-270",
        task_file="tasks/PR-270.md",
        task_body="<body>",
        model="opus",
        timeout=321,
        breach_dir="/tmp/breach",
        reasoning_effort="medium",
    )

    assert result == (0, "ok", "")
    assert captured["args"] == (
        "/data/repos/demo",
        "PR-270",
        "tasks/PR-270.md",
        "<body>",
    )
    assert captured["model"] == "opus"
    assert captured["timeout"] == 321
    assert captured["kwargs"] == {
        "breach_dir": "/tmp/breach",
        "reasoning_effort": "medium",
    }


@pytest.mark.asyncio
async def test_claude_plugin_fix_review_delegates(
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
        "src.coders.claude.claude_cli.fix_review_async",
        fake_fix_review_async,
    )

    result = await ClaudePlugin().fix_review(
        "/data/repos/demo",
        model="sonnet",
        timeout=123,
        reasoning_effort="low",
    )

    assert result == (0, "fixed", "")
    assert captured == {
        "repo_path": "/data/repos/demo",
        "model": "sonnet",
        "timeout": 123,
        "kwargs": {
            "pr_id": None,
            "task_file": None,
            "reasoning_effort": "low",
        },
    }


@pytest.mark.parametrize(
    ("coder_settings", "expected"),
    [
        ({}, {"model": "opus"}),
        ({"claude": {"reasoning_effort": ""}}, {"model": "opus"}),
        (
            {"claude": {"reasoning_effort": "high"}},
            {"model": "opus", "reasoning_effort": "high"},
        ),
    ],
)
def test_claude_plugin_build_run_kwargs_resolves_reasoning_effort(
    coder_settings: dict[str, dict[str, object]],
    expected: dict[str, str],
) -> None:
    config = DaemonConfig(coder_settings=coder_settings)

    assert ClaudePlugin().build_run_kwargs(daemon_config=config) == expected


def test_claude_plugin_build_run_kwargs_preserves_breach_settings() -> None:
    config = DaemonConfig(
        coder_settings={"claude": {"reasoning_effort": "high"}},
        rate_limit_session_pause_percent=31,
        rate_limit_weekly_pause_percent=62,
    )

    assert ClaudePlugin().build_run_kwargs(
        daemon_config=config,
        breach_dir="/tmp/breach",
        breach_run_id="run-123",
    ) == {
        "model": "opus",
        "reasoning_effort": "high",
        "breach_dir": "/tmp/breach",
        "breach_run_id": "run-123",
        "session_threshold": 31,
        "weekly_threshold": 62,
    }


@pytest.mark.parametrize("malformed", [None, 7, False, ["high"]])
def test_daemon_config_rejects_malformed_claude_reasoning_effort(
    malformed: object,
) -> None:
    with pytest.raises(
        ValidationError,
        match=r"coder_settings\.claude\.reasoning_effort must be a string",
    ):
        DaemonConfig(coder_settings={"claude": {"reasoning_effort": malformed}})


@pytest.mark.parametrize("malformed", [None, 7, False, ["high"]])
def test_claude_plugin_rejects_malformed_reasoning_effort(
    malformed: object,
) -> None:
    config = DaemonConfig.model_construct(
        coder_settings={"claude": {"reasoning_effort": malformed}}
    )

    with pytest.raises(
        ValueError,
        match=r"daemon\.coder_settings\.claude\.reasoning_effort must be a string",
    ):
        ClaudePlugin().build_run_kwargs(daemon_config=config)


@pytest.mark.asyncio
async def test_claude_plugin_forwards_auxiliary_reasoning_effort_and_callbacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def process_callback(_process: object) -> None:
        pass

    def supervised_callback(_managed: object) -> None:
        pass

    async def fake_run_claude_async(
        prompt: str, repo_path: str, **kwargs: object
    ) -> tuple[int, str, str]:
        calls.append(("prompt", kwargs))
        return (0, prompt, repo_path)

    async def fake_diagnose_error_async(
        repo_path: str, context: str, **kwargs: object
    ) -> tuple[int, str, str]:
        calls.append(("diagnose", kwargs))
        return (0, context, repo_path)

    monkeypatch.setattr(
        claude_module.claude_cli,
        "run_claude_async",
        fake_run_claude_async,
    )
    monkeypatch.setattr(
        claude_module.claude_cli,
        "diagnose_error_async",
        fake_diagnose_error_async,
    )

    plugin = ClaudePlugin()
    environment = {
        "HOME": "/bound/home",
        "CLAUDE_CONFIG_DIR": "/bound/claude",
    }
    await plugin.run_prompt(
        "prompt",
        "/repo",
        "opus",
        12,
        on_process_start=process_callback,
        on_supervised_process_start=supervised_callback,
        reasoning_effort="high",
        environment=environment,
    )
    await plugin.diagnose_error(
        "/repo",
        "failure",
        "sonnet",
        on_process_start=process_callback,
        on_supervised_process_start=supervised_callback,
        reasoning_effort="high",
        environment=environment,
    )

    assert [kind for kind, _kwargs in calls] == ["prompt", "diagnose"]
    for _kind, kwargs in calls:
        assert kwargs["reasoning_effort"] == "high"
        assert kwargs["on_process_start"] is process_callback
        assert kwargs["on_supervised_process_start"] is supervised_callback
        assert kwargs["environment"] is environment


def test_claude_plugin_auth_uses_copied_bound_environment_without_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    supplied = {
        "HOME": "/bound/home",
        "CLAUDE_CONFIG_DIR": "/bound/a",
        "MARKER": "caller",
    }
    calls: list[dict[str, str]] = []

    def fail_load_config(*_args: object, **_kwargs: object) -> AppConfig:
        raise AssertionError("bound auth must not reload configuration")

    def fake_run_auth_command(
        cmd: list[str], *, env: dict[str, str] | None = None
    ) -> tuple[int, str, str]:
        assert env is not None
        calls.append(env)
        env["MARKER"] = "internal"
        if cmd == ["claude", "--version"]:
            return (0, "2.1.126", "")
        return (0, '{"loggedIn":true,"authMethod":"claude.ai"}', "")

    monkeypatch.setattr(claude_module, "load_config", fail_load_config)
    monkeypatch.setattr(claude_module, "_run_auth_command", fake_run_auth_command)

    result = ClaudePlugin().check_auth(environment=supplied)

    assert result["status"] == "ok"
    assert all(env["CLAUDE_CONFIG_DIR"] == "/bound/a" for env in calls)
    assert supplied == {
        "HOME": "/bound/home",
        "CLAUDE_CONFIG_DIR": "/bound/a",
        "MARKER": "caller",
    }


def test_claude_plugin_reports_saved_subscription_authentication(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    result, calls = _check_auth_with_results(
        monkeypatch,
        tmp_path,
        version_result=(0, "2.1.47 (Claude Code)\n", ""),
        status_result=(
            0,
            (
                '{"loggedIn":true,"authMethod":"claude.ai",'
                '"email":"synthetic-secret@example.test",'
                '"organization":"synthetic-secret-org"}'
            ),
            "provider stderr synthetic-secret",
        ),
    )

    _assert_claude_auth_status(
        result,
        status="ok",
        detail=(
            "Claude Code CLI 2.1.47; saved Claude account credentials found; "
            "service access not verified"
        ),
        cli_available=True,
        cli_version="2.1.47",
        saved_credentials_present=True,
        authentication_mode="claude_ai",
    )
    assert [cmd for cmd, _env in calls] == [
        ["claude", "--version"],
        ["claude", "auth", "status"],
    ]
    assert all(
        env is not None
        and env["CLAUDE_CONFIG_DIR"] == str(tmp_path / "claude-auth")
        for _cmd, env in calls
    )
    assert "synthetic-secret" not in str(result)


@pytest.mark.parametrize(
    ("provider_method", "authentication_mode", "evidence_detail"),
    [
        (
            "oauth_token",
            "oauth_token",
            "configured OAuth-token authentication found",
        ),
        ("api_key", "api_key", "configured API-key authentication found"),
        (
            "api_key_helper",
            "api_key_helper",
            "configured API-key helper authentication found",
        ),
        (
            "third_party",
            "third_party",
            "configured third-party authentication found",
        ),
    ],
)
def test_claude_plugin_reports_configured_auth_without_claiming_saved_credentials(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    provider_method: str,
    authentication_mode: str,
    evidence_detail: str,
) -> None:
    result, _calls = _check_auth_with_results(
        monkeypatch,
        tmp_path,
        version_result=(0, "claude 2.1.47", ""),
        status_result=(
            0,
            (
                f'{{"loggedIn":true,"authMethod":"{provider_method}",'
                '"token":"synthetic-secret"}'
            ),
            "",
        ),
    )

    _assert_claude_auth_status(
        result,
        status="ok",
        detail=(
            f"Claude Code CLI 2.1.47; {evidence_detail}; "
            "service access not verified"
        ),
        cli_available=True,
        cli_version="2.1.47",
        authentication_mode=authentication_mode,
    )
    assert "synthetic-secret" not in str(result)


def test_claude_plugin_reports_missing_authentication(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    result, _calls = _check_auth_with_results(
        monkeypatch,
        tmp_path,
        version_result=(0, "claude-code 2.1.47", ""),
        status_result=(
            1,
            '{"loggedIn":false,"authMethod":"none","apiProvider":"none"}',
            "",
        ),
    )

    _assert_claude_auth_status(
        result,
        status="error",
        detail=(
            "Claude Code CLI 2.1.47; no active authentication reported; "
            "service access not verified"
        ),
        cli_available=True,
        cli_version="2.1.47",
        failure_reason="credentials_missing",
    )


def test_claude_plugin_rejects_unknown_authentication_method_without_disclosure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    result, _calls = _check_auth_with_results(
        monkeypatch,
        tmp_path,
        version_result=(0, "2.1.47", ""),
        status_result=(
            0,
            (
                '{"loggedIn":true,"authMethod":"future_synthetic_secret",'
                '"message":"arbitrary synthetic secret"}'
            ),
            "",
        ),
    )

    _assert_claude_auth_status(
        result,
        status="error",
        detail=(
            "Claude Code CLI 2.1.47; authentication method was not recognized"
        ),
        cli_available=True,
        cli_version="2.1.47",
        failure_reason="unrecognized_output",
    )
    assert "synthetic" not in str(result)


@pytest.mark.parametrize(
    ("status_result", "detail", "failure_reason"),
    [
        (
            (2, "", "error: unknown command 'auth' token=synthetic-secret"),
            "Claude Code CLI 2.1.47; authentication status is unsupported",
            "probe_unavailable",
        ),
        (
            (124, "", "timeout token=synthetic-secret"),
            "Claude Code CLI 2.1.47; authentication status check timed out",
            "probe_timeout",
        ),
        (
            (2, "", "failed token=synthetic-secret"),
            "Claude Code CLI 2.1.47; authentication status check failed",
            "probe_failed",
        ),
        (
            (0, "not-json synthetic-secret", ""),
            "Claude Code CLI 2.1.47; authentication status was not recognized",
            "unrecognized_output",
        ),
    ],
)
def test_claude_plugin_sanitizes_auth_status_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    status_result: tuple[int, str, str],
    detail: str,
    failure_reason: str,
) -> None:
    result, _calls = _check_auth_with_results(
        monkeypatch,
        tmp_path,
        version_result=(0, "2.1.47 (Claude Code)", ""),
        status_result=status_result,
    )

    _assert_claude_auth_status(
        result,
        status="error",
        detail=detail,
        cli_available=True,
        cli_version="2.1.47",
        failure_reason=failure_reason,
    )
    assert "synthetic-secret" not in str(result)


@pytest.mark.parametrize(
    "status_result",
    [
        (0, '{"loggedIn":false,"authMethod":"none"}', ""),
        (1, '{"loggedIn":true,"authMethod":"api_key"}', ""),
        (0, '{"loggedIn":true,"authMethod":"none"}', ""),
        (1, '{"loggedIn":false,"authMethod":"api_key"}', ""),
    ],
)
def test_claude_plugin_rejects_inconsistent_auth_status(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    status_result: tuple[int, str, str],
) -> None:
    result, _calls = _check_auth_with_results(
        monkeypatch,
        tmp_path,
        version_result=(0, "2.1.47", ""),
        status_result=status_result,
    )

    _assert_claude_auth_status(
        result,
        status="error",
        detail="Claude Code CLI 2.1.47; authentication status was inconsistent",
        cli_available=True,
        cli_version="2.1.47",
        failure_reason="probe_failed",
    )


@pytest.mark.parametrize(
    ("version_result", "detail", "cli_available", "failure_reason"),
    [
        (
            (127, "", "claude not found"),
            "Claude Code CLI was not found",
            False,
            "cli_missing",
        ),
        (
            (124, "", "timeout synthetic-secret"),
            "Claude Code CLI version check timed out",
            None,
            "probe_timeout",
        ),
        (
            (2, "", "failure synthetic-secret"),
            "Claude Code CLI version check failed",
            None,
            "probe_failed",
        ),
        (
            (0, "unexpected-version synthetic-secret", ""),
            "Claude Code CLI returned an unrecognized version",
            True,
            "unrecognized_output",
        ),
    ],
)
def test_claude_plugin_sanitizes_version_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    version_result: tuple[int, str, str],
    detail: str,
    cli_available: bool | None,
    failure_reason: str,
) -> None:
    result, calls = _check_auth_with_results(
        monkeypatch,
        tmp_path,
        version_result=version_result,
        status_result=None,
    )

    _assert_claude_auth_status(
        result,
        status="error",
        detail=detail,
        cli_available=cli_available,
        failure_reason=failure_reason,
    )
    assert [cmd for cmd, _env in calls] == [["claude", "--version"]]
    assert "synthetic-secret" not in str(result)


def test_claude_plugin_reports_executable_disappearing_during_probe(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    result, _calls = _check_auth_with_results(
        monkeypatch,
        tmp_path,
        version_result=(0, "2.1.47", ""),
        status_result=(127, "", "claude not found"),
    )

    _assert_claude_auth_status(
        result,
        status="error",
        detail="Claude Code CLI was not found",
        cli_available=False,
        cli_version="2.1.47",
        failure_reason="cli_missing",
    )


@pytest.mark.parametrize(
    "output",
    [
        "not JSON",
        "[]",
        '{"loggedIn":"yes","authMethod":"claude.ai"}',
        '{"loggedIn":true,"authMethod":null}',
    ],
)
def test_parse_auth_status_rejects_malformed_shapes(output: str) -> None:
    assert claude_module._parse_auth_status(output) is None


def test_probe_helpers_ignore_warnings_and_recognize_known_errors() -> None:
    assert claude_module._claude_version(
        "warning: npm notice\n2.1.47-beta.1 (Claude Code)"
    ) == "2.1.47-beta.1"
    assert claude_module._first_probe_line("warning: one\n\nwarning: two") == ""
    assert claude_module._reports_missing_cli(
        "claude: /missing: no such file or directory"
    )
    assert claude_module._auth_status_unsupported(
        "status is not a supported subcommand"
    )


def test_run_auth_command_returns_completed_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class CompletedProcess:
        returncode = 3
        stdout = "stdout text"
        stderr = "stderr text"

    def fake_run(*args: object, **kwargs: object) -> object:
        return CompletedProcess()

    monkeypatch.setattr("src.coders.claude.subprocess.run", fake_run)

    result = claude_module._run_auth_command(["claude", "--version"])

    assert result == (3, "stdout text", "stderr text")


def test_run_auth_command_returns_127_on_missing_binary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*args: object, **kwargs: object) -> object:
        raise FileNotFoundError

    monkeypatch.setattr("src.coders.claude.subprocess.run", fake_run)

    result = claude_module._run_auth_command(["claude", "--version"])

    assert result == (127, "", "claude not found")


def test_auth_check_returns_126_on_permission_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*args: object, **kwargs: object) -> object:
        raise PermissionError("denied")

    monkeypatch.setattr("src.coders.claude.subprocess.run", fake_run)

    result = claude_module._run_auth_command(["claude", "--version"])

    assert result == (126, "", "denied")


def test_run_auth_command_returns_124_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*args: object, **kwargs: object) -> object:
        raise claude_module.subprocess.TimeoutExpired(
            cmd=["claude", "--version"],
            timeout=claude_module._AUTH_CHECK_TIMEOUT_SEC,
        )

    monkeypatch.setattr("src.coders.claude.subprocess.run", fake_run)

    result = claude_module._run_auth_command(["claude", "--version"])

    assert result == (
        124,
        "",
        "claude timed out after 5s",
    )


def test_create_usage_provider_loads_config_from_default_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_load_config(path: str) -> AppConfig:
        captured["config_path"] = path
        return AppConfig()

    def fake_oauth_usage_provider(**kwargs: object) -> object:
        captured["provider_kwargs"] = kwargs
        return {"provider_kwargs": kwargs}

    monkeypatch.setattr("src.coders.claude.load_config", fake_load_config)
    monkeypatch.setattr(
        "src.coders.claude.OAuthUsageProvider",
        fake_oauth_usage_provider,
    )

    result = ClaudePlugin().create_usage_provider()

    assert captured["config_path"] == claude_module.CONFIG_PATH
    assert result == {"provider_kwargs": captured["provider_kwargs"]}


def test_create_usage_provider_uses_normalized_default_and_explicit_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    config = AppConfig.model_validate(
        {"auth": {"claude_config_dir": "auth/../claude-auth"}}
    )

    default_provider = ClaudePlugin().create_usage_provider(config=config)
    explicit_provider = ClaudePlugin().create_usage_provider(
        config=config,
        credentials_path="relative/explicit.json",
    )

    assert default_provider._credentials_path == (
        tmp_path / "claude-auth" / ".credentials.json"
    )
    assert explicit_provider._credentials_path == Path("relative/explicit.json")


def test_rate_limit_patterns_returns_anthropic_pattern() -> None:
    patterns = ClaudePlugin().rate_limit_patterns()

    assert patterns == [claude_module._ANTHROPIC_RATE_LIMIT_PATTERN]
