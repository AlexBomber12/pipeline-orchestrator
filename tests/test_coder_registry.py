from __future__ import annotations

import asyncio
import builtins
import re
from pathlib import Path
from typing import Any

import pytest
from src import claude_cli, codex_cli
from src import coders as coders_module
from src.coder_registry import (
    CoderAuthCapabilities,
    CoderAuthStatus,
    CoderPlugin,
    CoderRegistry,
    ModelCatalog,
    ModelMetadata,
    ModelSetting,
    coder_auth_payload,
    parse_coder_auth_payload,
    parse_coder_browser_login_payload,
    parse_coder_device_login_payload,
    resolve_coder_credential_location,
    resolve_device_login_credential_location,
)
from src.coders import CoderPluginConfigurationError, build_coder_registry
from src.coders.claude import ClaudePlugin
from src.coders.codex import CodexPlugin
from src.config import AppConfig, DaemonConfig


class DummyCoderPlugin:
    def __init__(self, name: str, display_name: str) -> None:
        self.name = name
        self.display_name = display_name
        self.models = ["model-a", "model-b"]
        self.model_setting = ModelSetting(
            "claude_model", "model-a", "(default)"
        )
        self.model_catalog_refreshable = False

    def model_catalog_cache_key(
        self, *, config: AppConfig, config_path: str
    ) -> str:
        del config, config_path
        return "static"

    async def get_model_catalog(
        self, *, config: AppConfig, config_path: str
    ) -> ModelCatalog:
        del config, config_path
        return ModelCatalog(
            tuple(ModelMetadata(model, model) for model in self.models),
            "static_compatibility",
            "Static test catalog.",
        )

    async def run_planned_pr(
        self, repo_path: str, model: str | None, timeout: int
    ) -> tuple[int, str, str]:
        return (0, repo_path, model or str(timeout))

    async def run_auto_pr(
        self,
        repo_path: str,
        *,
        pr_id: str,
        task_file: str,
        task_body: str,
        model: str | None,
        timeout: int,
    ) -> tuple[int, str, str]:
        return (0, repo_path, f"{pr_id}|{task_file}|{task_body}|{model}|{timeout}")

    async def fix_review(
        self, repo_path: str, model: str | None, timeout: int | None
    ) -> tuple[int, str, str]:
        return (0, repo_path, model or str(timeout))

    async def run_prompt(
        self,
        prompt: str,
        repo_path: str,
        model: str | None,
        timeout: int | None,
        **kwargs: Any,
    ) -> tuple[int, str, str]:
        return (0, prompt, f"{repo_path}|{model}|{timeout}|{bool(kwargs)}")

    def check_auth(self) -> dict[str, str]:
        return {"status": "ok"}

    def create_usage_provider(self, **kwargs: object) -> None:
        return None

    def rate_limit_patterns(self) -> list[re.Pattern[str]]:
        return [re.compile("limit")]

    @property
    def supports_breach_lifecycle(self) -> bool:
        return True

    @property
    def default_session_pause_percent(self) -> int:
        return 95

    @property
    def default_weekly_pause_percent(self) -> int:
        return 80

    async def diagnose_error(
        self,
        repo_path: str,
        context: str,
        model: str | None,
        **kwargs: Any,
    ) -> tuple[int, str, str]:
        return (0, f"{repo_path}|{context}|{model}", "")

    def resolve_model(self, daemon_config: DaemonConfig) -> str:
        return self.model_setting.resolve(self.name, daemon_config)

    def build_run_kwargs(
        self,
        *,
        daemon_config: DaemonConfig,
        breach_dir: str | None = None,
        breach_run_id: str | None = None,
    ) -> dict[str, Any]:
        return {"model": self.resolve_model(daemon_config)}


class _BrowserCredentialPlugin:
    def __init__(self, location: object) -> None:
        self.location = location
        self.locator_calls = 0

    def browser_login_credential_location(self, *, config: AppConfig) -> object:
        del config
        self.locator_calls += 1
        return self.location

    def build_credential_environment(
        self,
        *,
        config: AppConfig,
        credential_location: str,
    ) -> dict[str, str]:
        del config, credential_location
        raise AssertionError("environment builder must not run during resolution")


class _DualCredentialPlugin(_BrowserCredentialPlugin):
    def __init__(self, browser_location: str, device_location: str) -> None:
        super().__init__(browser_location)
        self.device_location = device_location

    def create_device_login(self, *, config_path: str) -> object:
        raise AssertionError(f"login factory must not run: {config_path}")

    def device_login_credential_location(self, *, config: AppConfig) -> str:
        del config
        return self.device_location


def test_common_credential_location_resolves_builtin_contexts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("CODEX_HOME", raising=False)
    config = AppConfig.model_validate(
        {
            "auth": {
                "claude_config_dir": "relative/claude",
                "codex_home_dir": str(tmp_path / "codex-home"),
            }
        }
    )
    claude = ClaudePlugin()
    codex = CodexPlugin()

    assert claude.auth_capabilities.interactive_login_methods == ()
    assert resolve_coder_credential_location(
        claude,
        config=config,
    ) == str(tmp_path / "relative" / "claude")
    assert resolve_coder_credential_location(
        codex,
        config=config,
    ) == resolve_device_login_credential_location(codex, config=config)


def test_common_credential_location_accepts_browser_context_without_factory(
    tmp_path: Path,
) -> None:
    location = str(tmp_path / "browser-credentials")
    plugin = _BrowserCredentialPlugin(location)

    assert resolve_coder_credential_location(
        plugin,
        config=AppConfig(),
    ) == location
    assert plugin.locator_calls == 1


def test_common_credential_location_ignores_legacy_plugin() -> None:
    assert (
        resolve_coder_credential_location(
            DummyCoderPlugin("legacy", "Legacy"),
            config=AppConfig(),
        )
        is None
    )


@pytest.mark.parametrize("location", [None, "", 7])
def test_common_credential_location_rejects_invalid_browser_location(
    location: object,
) -> None:
    with pytest.raises(ValueError, match="^invalid coder credential location$"):
        resolve_coder_credential_location(
            _BrowserCredentialPlugin(location),
            config=AppConfig(),
        )


def test_common_credential_location_rejects_noncallable_browser_locator() -> None:
    plugin = _BrowserCredentialPlugin("unused")
    plugin.browser_login_credential_location = "sensitive/path"  # type: ignore[method-assign]

    with pytest.raises(ValueError, match="^invalid coder credential locator$"):
        resolve_coder_credential_location(plugin, config=AppConfig())


def test_common_credential_location_requires_browser_environment_builder() -> None:
    class MissingBuilder:
        def browser_login_credential_location(
            self, *, config: AppConfig
        ) -> str:
            del config
            return "/sensitive/browser/location"

    with pytest.raises(
        ValueError,
        match=(
            "^browser credential context is missing a credential environment "
            "builder$"
        ),
    ) as caught:
        resolve_coder_credential_location(MissingBuilder(), config=AppConfig())

    assert "/sensitive/browser/location" not in str(caught.value)


def test_common_credential_location_accepts_matching_dual_contexts() -> None:
    plugin = _DualCredentialPlugin("shared/location", "shared/location")

    assert resolve_coder_credential_location(
        plugin,
        config=AppConfig(),
    ) == "shared/location"
    assert plugin.locator_calls == 1


def test_common_credential_location_rejects_conflicting_dual_contexts() -> None:
    plugin = _DualCredentialPlugin(
        "/sensitive/browser/location",
        "/sensitive/device/location",
    )

    with pytest.raises(
        ValueError,
        match="^coder credential contexts use conflicting locations$",
    ) as caught:
        resolve_coder_credential_location(plugin, config=AppConfig())

    message = str(caught.value)
    assert "/sensitive/browser/location" not in message
    assert "/sensitive/device/location" not in message


def test_device_credential_location_keeps_device_factory_contract() -> None:
    plugin = _BrowserCredentialPlugin("browser/location")
    plugin.device_login_credential_location = (  # type: ignore[attr-defined]
        lambda *, config: "device/location"
    )

    assert (
        resolve_device_login_credential_location(plugin, config=AppConfig())
        is None
    )


def test_build_registry_without_config_keeps_builtin_compatibility() -> None:
    registry = build_coder_registry()

    assert registry.coder_names() == ["claude", "codex"]
    assert isinstance(registry.get("claude"), ClaudePlugin)
    assert isinstance(registry.get("codex"), CodexPlugin)


def test_build_registry_loads_configured_plugin_and_builtin_override() -> None:
    config = AppConfig(
        coder_plugins={
            "claude": (
                "tests.configured_coder_plugin:build_claude_override"
            ),
            "third": "tests.configured_coder_plugin:build_test_plugin",
        }
    )

    registry = build_coder_registry(config)

    assert registry.coder_names() == ["claude", "codex", "third"]
    assert registry.get("claude").display_name == "Configured Claude"
    assert registry.get("third").name == "third"
    assert isinstance(registry.get("codex"), CodexPlugin)


def test_build_registry_validates_configured_custom_model_setting_value() -> None:
    reference = "tests.configured_coder_plugin:build_variant_setting_plugin"
    daemon = DaemonConfig.model_construct(
        coder_settings={"third": {"variant": 123}}
    )
    config = AppConfig.model_construct(
        coder_plugins={"third": reference},
        daemon=daemon,
    )

    with pytest.raises(CoderPluginConfigurationError) as caught:
        build_coder_registry(config)

    message = str(caught.value)
    assert repr("third") in message
    assert repr(reference) in message
    assert "failed at model setting validation" in message
    assert "daemon.coder_settings.third.variant must be a string" in message


def test_build_registry_accepts_string_custom_model_setting_value() -> None:
    config = AppConfig(
        coder_plugins={
            "third": (
                "tests.configured_coder_plugin:build_variant_setting_plugin"
            )
        },
        daemon=DaemonConfig(
            coder_settings={"third": {"variant": "third-invoke"}}
        ),
    )

    registry = build_coder_registry(config)

    assert registry.get("third").resolve_model(config.daemon) == "third-invoke"


def test_build_registry_rejects_reasoning_effort_as_model_setting_key() -> None:
    reference = (
        "tests.configured_coder_plugin:build_reasoning_effort_setting_plugin"
    )
    config = AppConfig(coder_plugins={"third": reference})

    with pytest.raises(CoderPluginConfigurationError) as caught:
        build_coder_registry(config)

    message = str(caught.value)
    assert "failed at metadata validation" in message
    assert "setting_key 'reasoning_effort' is reserved for plugin options" in message


@pytest.mark.parametrize(
    ("plugin_id", "reference", "stage"),
    [
        ("third", "missing-separator", "reference"),
        ("third", ":factory", "reference"),
        ("third", "missing.module:factory", "module import"),
        (
            "third",
            "tests.configured_coder_plugin:missing_factory",
            "factory lookup",
        ),
        (
            "third",
            "tests.configured_coder_plugin:NOT_CALLABLE",
            "factory validation",
        ),
        (
            "third",
            "tests.configured_coder_plugin:build_incompatible_plugin",
            "contract validation",
        ),
        (
            "expected",
            "tests.configured_coder_plugin:build_mismatched_plugin",
            "identity validation",
        ),
        (
            "third",
            "tests.configured_coder_plugin:build_missing_metadata_plugin",
            "metadata validation",
        ),
        (
            "third",
            "tests.configured_coder_plugin:build_raising_metadata_plugin",
            "metadata validation",
        ),
        (
            "third",
            "tests.configured_coder_plugin:build_empty_name_plugin",
            "metadata validation",
        ),
        (
            "third",
            "tests.configured_coder_plugin:build_invalid_models_plugin",
            "metadata validation",
        ),
        (
            "third",
            "tests.configured_coder_plugin:build_invalid_model_setting_plugin",
            "metadata validation",
        ),
        (
            "third",
            "tests.configured_coder_plugin:build_unknown_legacy_field_plugin",
            "metadata validation",
        ),
        (
            "third",
            "tests.configured_coder_plugin:build_non_string_legacy_field_plugin",
            "metadata validation",
        ),
        (
            "third",
            "tests.configured_coder_plugin:build_non_model_legacy_field_plugin",
            "metadata validation",
        ),
        (
            "third",
            "tests.configured_coder_plugin:build_foreign_legacy_field_plugin",
            "metadata validation",
        ),
        (
            "claude",
            (
                "tests.configured_coder_plugin:"
                "build_claude_foreign_legacy_field_plugin"
            ),
            "metadata validation",
        ),
        (
            "third",
            "tests.configured_coder_plugin:build_non_string_default_value_plugin",
            "metadata validation",
        ),
        (
            "third",
            "tests.configured_coder_plugin:build_empty_default_label_plugin",
            "metadata validation",
        ),
        (
            "third",
            "tests.configured_coder_plugin:build_empty_setting_key_plugin",
            "metadata validation",
        ),
        (
            "third",
            "tests.configured_coder_plugin:build_non_string_setting_key_plugin",
            "metadata validation",
        ),
        (
            "third",
            "tests.configured_coder_plugin:build_dotted_setting_key_plugin",
            "metadata validation",
        ),
        (
            "third",
            "tests.configured_coder_plugin:build_invalid_refreshable_plugin",
            "metadata validation",
        ),
    ],
)
def test_build_registry_reports_configured_loading_stage(
    plugin_id: str,
    reference: str,
    stage: str,
) -> None:
    config = AppConfig(coder_plugins={plugin_id: reference})

    with pytest.raises(CoderPluginConfigurationError) as caught:
        build_coder_registry(config)

    message = str(caught.value)
    assert repr(plugin_id) in message
    assert repr(reference) in message
    assert f"failed at {stage}" in message


def test_build_registry_rejects_non_string_reference_from_constructed_config() -> None:
    config = AppConfig.model_construct(coder_plugins={"third": 3})

    with pytest.raises(
        CoderPluginConfigurationError,
        match="failed at reference: expected a module:factory string",
    ):
        build_coder_registry(config)


@pytest.mark.parametrize("plugin_id", ["third/plugin", "third#plugin"])
def test_build_registry_rejects_route_unsafe_plugin_id(plugin_id: str) -> None:
    reference = "tests.configured_coder_plugin:build_test_plugin"
    config = AppConfig(coder_plugins={plugin_id: reference})

    with pytest.raises(CoderPluginConfigurationError) as caught:
        build_coder_registry(config)

    message = str(caught.value)
    assert repr(plugin_id) in message
    assert repr(reference) in message
    assert "failed at plugin ID validation" in message


def test_build_registry_rejects_task_inheritance_plugin_id() -> None:
    reference = "tests.configured_coder_plugin:build_test_plugin"
    config = AppConfig(coder_plugins={"any": reference})

    with pytest.raises(CoderPluginConfigurationError) as caught:
        build_coder_registry(config)

    message = str(caught.value)
    assert repr("any") in message
    assert repr(reference) in message
    assert "failed at plugin ID validation" in message
    assert "reserved for task inheritance" in message


def test_build_registry_rejects_reserved_infrastructure_plugin_id() -> None:
    reference = "tests.configured_coder_plugin:build_test_plugin"
    config = AppConfig(coder_plugins={"gh": reference})

    with pytest.raises(CoderPluginConfigurationError) as caught:
        build_coder_registry(config)

    message = str(caught.value)
    assert repr("gh") in message
    assert repr(reference) in message
    assert "failed at plugin ID validation" in message
    assert "reserved for infrastructure status" in message


def test_build_registry_wraps_contract_inspection_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_isinstance = builtins.isinstance

    def raising_isinstance(value: object, class_or_tuple: object) -> bool:
        if class_or_tuple is CoderPlugin:
            raise RuntimeError("contract secret")
        return real_isinstance(value, class_or_tuple)

    monkeypatch.setattr(
        coders_module,
        "isinstance",
        raising_isinstance,
        raising=False,
    )
    config = AppConfig(
        coder_plugins={
            "third": "tests.configured_coder_plugin:build_test_plugin"
        }
    )

    with pytest.raises(CoderPluginConfigurationError) as caught:
        build_coder_registry(config)

    message = str(caught.value)
    assert "failed at contract validation" in message
    assert "RuntimeError" in message
    assert "contract secret" not in message


def test_build_registry_redacts_factory_exception_detail() -> None:
    reference = "tests.configured_coder_plugin:build_exploding_plugin"
    config = AppConfig(coder_plugins={"third": reference})

    with pytest.raises(CoderPluginConfigurationError) as caught:
        build_coder_registry(config)

    message = str(caught.value)
    assert "failed at factory invocation" in message
    assert "RuntimeError" in message
    assert "must-not-leak" not in message


def test_register_and_get() -> None:
    registry = CoderRegistry()
    plugin = DummyCoderPlugin(name="claude", display_name="Claude")

    registry.register(plugin)

    assert registry.get("claude") is plugin


def test_register_tracks_and_clears_startup_reference() -> None:
    registry = CoderRegistry()
    plugin = DummyCoderPlugin(name="claude", display_name="Claude")

    registry.register(plugin, reference="package.module:factory")
    assert registry.reference_for("claude") == "package.module:factory"

    registry.register(plugin)
    assert registry.reference_for("claude") is None


def test_get_unknown_raises() -> None:
    registry = CoderRegistry()

    with pytest.raises(KeyError, match="Unknown coder: missing"):
        registry.get("missing")


def test_list_coders() -> None:
    registry = CoderRegistry()
    claude = DummyCoderPlugin(name="claude", display_name="Claude")
    codex = DummyCoderPlugin(name="codex", display_name="Codex")

    registry.register(claude)
    registry.register(codex)

    assert registry.list_coders() == [claude, codex]


def test_coder_names() -> None:
    registry = CoderRegistry()
    registry.register(DummyCoderPlugin(name="claude", display_name="Claude"))
    registry.register(DummyCoderPlugin(name="codex", display_name="Codex"))

    assert registry.coder_names() == ["claude", "codex"]


def test_protocol_includes_diagnose_error() -> None:
    """``CoderPlugin`` declares ``diagnose_error`` and ``isinstance`` checks
    succeed for the bundled plugins."""
    plugin = DummyCoderPlugin(name="claude", display_name="Claude")
    assert isinstance(plugin, CoderPlugin)
    assert isinstance(ClaudePlugin(), CoderPlugin)
    assert isinstance(CodexPlugin(), CoderPlugin)


def test_protocol_includes_run_prompt() -> None:
    assert "run_prompt" in dir(CoderPlugin)
    assert isinstance(DummyCoderPlugin("dummy", "Dummy"), CoderPlugin)
    assert isinstance(ClaudePlugin(), CoderPlugin)
    assert isinstance(CodexPlugin(), CoderPlugin)


def test_auth_capabilities_are_optional_protocol_metadata() -> None:
    plugin = DummyCoderPlugin("dummy", "Dummy")

    assert "auth_capabilities" not in dir(CoderPlugin)
    assert not hasattr(plugin, "auth_capabilities")
    assert isinstance(plugin, CoderPlugin)


def _device_login_payload() -> dict[str, Any]:
    return {
        "plugin": "codex",
        "session_id": "A" * 43,
        "state": "waiting_for_user",
        "detail": "Authorize this login",
        "failure_reason": None,
        "verification_url": "https://auth.openai.com/codex/device",
        "user_code": "ABCD-EFGH",
        "expires_at": 1234,
        "cleanup_confirmed": None,
        "replacement_requested": True,
        "reused_session": False,
        "replacement_warning": "Existing credentials can be removed.",
        "auth_status": None,
        "raw_output": "must-not-cross",
    }


def test_device_login_contract_selects_allowlisted_fields() -> None:
    payload = parse_coder_device_login_payload(
        _device_login_payload(), expected_plugin="codex"
    )

    assert payload["expires_at"] == 1234.0
    assert payload["user_code"] == "ABCD-EFGH"
    assert "raw_output" not in payload

    completed = _device_login_payload()
    completed.update(
        {
            "state": "succeeded",
            "verification_url": None,
            "user_code": None,
            "expires_at": None,
            "cleanup_confirmed": True,
            "auth_status": coder_auth_payload(
                CoderAuthStatus(status="ok", detail="refreshed")
            ),
        }
    )
    assert (
        parse_coder_device_login_payload(completed)["auth_status"][
            "service_access_verified"
        ]
        is None
    )


@pytest.mark.parametrize(
    "mutate",
    [
        None,
        lambda payload: payload.update(plugin="Bad/Plugin"),
        lambda payload: payload.update(plugin="other"),
        lambda payload: payload.update(session_id="short"),
        lambda payload: payload.update(state="unknown"),
        lambda payload: payload.update(detail=""),
        lambda payload: payload.update(failure_reason="secret_error"),
        lambda payload: payload.update(verification_url="http://example.com"),
        lambda payload: payload.update(user_code="bad code"),
        lambda payload: payload.update(expires_at=True),
        lambda payload: payload.update(expires_at=float("nan")),
        lambda payload: payload.update(cleanup_confirmed="yes"),
        lambda payload: payload.update(replacement_requested="yes"),
        lambda payload: payload.update(replacement_warning=""),
        lambda payload: payload.update(
            state="failed", verification_url="https://example.com"
        ),
        lambda payload: payload.update(user_code=None),
        lambda payload: payload.update(
            auth_status={"status": "unknown", "detail": "bad"}
        ),
    ],
)
def test_device_login_contract_rejects_invalid_payloads(
    mutate: Any,
) -> None:
    payload: object = _device_login_payload()
    if mutate is None:
        payload = []
    else:
        mutate(payload)

    with pytest.raises(TypeError):
        parse_coder_device_login_payload(payload, expected_plugin="codex")


def _browser_login_payload(
    *,
    state: str = "waiting_for_user",
) -> dict[str, Any]:
    return {
        "login_method": "browser_code",
        "plugin": "claude",
        "session_id": "B" * 43,
        "state": state,
        "detail": "Continue browser-code login",
        "failure_reason": None,
        "authorization_url": (
            "https://login.example.test/oauth/authorize?"
            "state=opaque%2Bvalue%3D&redirect_uri=https%3A%2F%2Flocalhost"
        ),
        "application_deadline": 1234,
        "cleanup_confirmed": None,
        "replacement_requested": True,
        "reused_session": False,
        "replacement_warning": "Existing credentials may become unavailable.",
        "auth_status": None,
        "observed_cli_version": "2.1.126",
        "minimum_cli_version": "2.1.126",
    }


def _successful_browser_login_payload() -> dict[str, Any]:
    payload = _browser_login_payload(state="succeeded")
    payload.update(
        {
            "authorization_url": None,
            "application_deadline": None,
            "cleanup_confirmed": True,
            "auth_status": coder_auth_payload(
                CoderAuthStatus(
                    status="ok",
                    detail="Saved credentials found; service access unknown",
                    cli_available=True,
                    cli_version="2.1.126",
                    saved_credentials_present=True,
                    authentication_mode="browser_oauth",
                    service_access_verified=None,
                )
            ),
        }
    )
    return payload


@pytest.mark.parametrize("state", ["waiting_for_user", "waiting_for_code"])
def test_browser_login_contract_accepts_waiting_snapshots(state: str) -> None:
    source = _browser_login_payload(state=state)
    expected_url = source["authorization_url"]

    payload = parse_coder_browser_login_payload(
        source, expected_plugin="claude"
    )

    assert payload["login_method"] == "browser_code"
    assert payload["authorization_url"] == expected_url
    assert payload["application_deadline"] == 1234.0


def test_browser_login_contract_accepts_authorizing_and_cleanup_failure() -> None:
    authorizing = _browser_login_payload(state="authorizing")
    authorizing.update(
        authorization_url=None,
        observed_cli_version=None,
        minimum_cli_version=None,
    )
    parsed_authorizing = parse_coder_browser_login_payload(authorizing)
    assert parsed_authorizing["state"] == "authorizing"
    assert parsed_authorizing["observed_cli_version"] is None
    assert parsed_authorizing["minimum_cli_version"] is None

    cleanup_failed = _browser_login_payload(state="cleanup_failed")
    cleanup_failed.update(
        authorization_url=None,
        cleanup_confirmed=False,
        failure_reason="cancellation_failed",
    )
    assert (
        parse_coder_browser_login_payload(cleanup_failed)["cleanup_confirmed"]
        is False
    )


@pytest.mark.parametrize(
    "state",
    ["unsupported", "failed", "cancelled", "expired", "timed_out", "not_found"],
)
def test_browser_login_contract_accepts_terminal_snapshots(state: str) -> None:
    payload = _browser_login_payload(state=state)
    payload.update(
        authorization_url=None,
        application_deadline=None,
        cleanup_confirmed=True,
        failure_reason=("process_failed" if state == "failed" else None),
    )

    parsed = parse_coder_browser_login_payload(payload)

    assert parsed["state"] == state
    assert parsed["authorization_url"] is None


def test_browser_login_contract_requires_supported_success_evidence() -> None:
    parsed = parse_coder_browser_login_payload(
        _successful_browser_login_payload()
    )

    assert parsed["state"] == "succeeded"
    assert parsed["cleanup_confirmed"] is True
    assert parsed["auth_status"]["saved_credentials_present"] is True
    assert parsed["auth_status"]["service_access_verified"] is None


def test_browser_login_contract_preserves_bounded_version_evidence() -> None:
    payload = _browser_login_payload(state="unsupported")
    payload.update(
        session_id=None,
        authorization_url=None,
        application_deadline=None,
        cleanup_confirmed=True,
        failure_reason="unsupported_cli_version",
        observed_cli_version="2.1.119",
        minimum_cli_version="2.1.126",
    )

    parsed = parse_coder_browser_login_payload(payload)

    assert parsed["observed_cli_version"] == "2.1.119"
    assert parsed["minimum_cli_version"] == "2.1.126"


@pytest.mark.parametrize("missing_field", ["observed_cli_version", "minimum_cli_version"])
def test_browser_login_contract_requires_unsupported_version_evidence(
    missing_field: str,
) -> None:
    payload = _browser_login_payload(state="unsupported")
    payload.update(
        session_id=None,
        authorization_url=None,
        application_deadline=None,
        cleanup_confirmed=True,
        failure_reason="unsupported_cli_version",
    )
    payload[missing_field] = None

    with pytest.raises(
        TypeError,
        match="^invalid unsupported browser login version evidence$",
    ):
        parse_coder_browser_login_payload(payload)


def test_browser_login_contract_binds_unsupported_version_to_state() -> None:
    payload = _browser_login_payload()
    payload["failure_reason"] = "unsupported_cli_version"

    with pytest.raises(
        TypeError,
        match="^invalid unsupported browser login version evidence$",
    ):
        parse_coder_browser_login_payload(payload)


def test_browser_login_contract_selects_only_allowlisted_fields() -> None:
    payload = _successful_browser_login_payload()
    payload.update(
        submitted_code="submitted-code-fixture",
        access_token="access-token-fixture",
        refresh_token="refresh-token-fixture",
        terminal_output="terminal-output-fixture",
        environment={"SECRET": "environment-fixture"},
        oauth_state="oauth-state-fixture",
        code_challenge="pkce-fixture",
    )
    payload["auth_status"]["credential_contents"] = "credential-fixture"

    parsed = parse_coder_browser_login_payload(payload)

    assert set(parsed) == {
        "login_method",
        "plugin",
        "session_id",
        "state",
        "detail",
        "failure_reason",
        "authorization_url",
        "application_deadline",
        "cleanup_confirmed",
        "replacement_requested",
        "reused_session",
        "replacement_warning",
        "auth_status",
        "observed_cli_version",
        "minimum_cli_version",
    }
    assert "credential_contents" not in parsed["auth_status"]
    assert not any(
        marker in str(parsed)
        for marker in (
            "submitted-code-fixture",
            "access-token-fixture",
            "refresh-token-fixture",
            "terminal-output-fixture",
            "environment-fixture",
            "oauth-state-fixture",
            "pkce-fixture",
            "credential-fixture",
        )
    )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload.update(login_method="device_code"),
        lambda payload: payload.update(plugin="Bad/Plugin"),
        lambda payload: payload.update(plugin="other"),
        lambda payload: payload.update(session_id="short"),
        lambda payload: payload.update(state="unknown"),
        lambda payload: payload.update(detail=""),
        lambda payload: payload.update(detail="unsafe\ntext"),
        lambda payload: payload.update(failure_reason="secret_error"),
        lambda payload: payload.update(failure_reason="device_login_disabled"),
        lambda payload: payload.update(authorization_url="http://example.test"),
        lambda payload: payload.update(
            authorization_url="https://user:password@example.test/login"
        ),
        lambda payload: payload.update(
            authorization_url="https://example.test/login#fragment"
        ),
        lambda payload: payload.update(
            authorization_url="https://example.test/login\r\nheader:value"
        ),
        lambda payload: payload.update(
            authorization_url="https://example.test/contains space"
        ),
        lambda payload: payload.update(
            authorization_url="https://example.test:invalid/login"
        ),
        lambda payload: payload.update(
            authorization_url="https://" + "a" * 4096
        ),
        lambda payload: payload.update(application_deadline=True),
        lambda payload: payload.update(application_deadline=0),
        lambda payload: payload.update(application_deadline=float("nan")),
        lambda payload: payload.update(application_deadline=float("inf")),
        lambda payload: payload.update(application_deadline=10**309),
        lambda payload: payload.update(cleanup_confirmed="yes"),
        lambda payload: payload.update(replacement_requested="yes"),
        lambda payload: payload.update(reused_session=1),
        lambda payload: payload.update(replacement_warning=""),
        lambda payload: payload.update(replacement_warning="unsafe\x7fwarning"),
        lambda payload: payload.update(observed_cli_version=""),
        lambda payload: payload.update(observed_cli_version="1" * 65),
        lambda payload: payload.update(minimum_cli_version=True),
        lambda payload: payload.update(minimum_cli_version="2.1\n126"),
        lambda payload: payload.update(
            auth_status={"status": "unknown", "detail": "bad"}
        ),
    ],
)
def test_browser_login_contract_rejects_invalid_fields(mutate: Any) -> None:
    payload = _browser_login_payload()
    mutate(payload)

    with pytest.raises(TypeError):
        parse_coder_browser_login_payload(payload, expected_plugin="claude")


def test_browser_login_contract_rejects_non_mapping_payload() -> None:
    with pytest.raises(TypeError, match="^invalid browser login payload$"):
        parse_coder_browser_login_payload([])


def test_browser_login_contract_requires_failed_reason() -> None:
    payload = _browser_login_payload(state="failed")
    payload.update(
        authorization_url=None,
        application_deadline=None,
        cleanup_confirmed=None,
        failure_reason=None,
    )

    with pytest.raises(
        TypeError,
        match="^browser login failure reason is required$",
    ):
        parse_coder_browser_login_payload(payload)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload.update(session_id=None),
        lambda payload: payload.update(authorization_url=None),
        lambda payload: payload.update(
            state="authorizing",
            authorization_url="https://example.test/authorize",
        ),
        lambda payload: payload.update(
            state="cleanup_failed", authorization_url=None, cleanup_confirmed=None
        ),
        lambda payload: payload.update(
            state="cleanup_failed", authorization_url=None, cleanup_confirmed=True
        ),
        lambda payload: payload.update(cleanup_confirmed=False),
    ],
)
def test_browser_login_contract_rejects_cross_state_inconsistencies(
    mutate: Any,
) -> None:
    payload = _browser_login_payload()
    mutate(payload)

    with pytest.raises(TypeError):
        parse_coder_browser_login_payload(payload)


@pytest.mark.parametrize(
    "state",
    ["starting", "waiting_for_user", "waiting_for_code", "authorizing", "canceling"],
)
def test_browser_login_contract_requires_active_session_id(state: str) -> None:
    payload = _browser_login_payload(state=state)
    payload["session_id"] = None
    if state not in {"waiting_for_user", "waiting_for_code"}:
        payload["authorization_url"] = None

    with pytest.raises(TypeError, match="^browser login session ID is required$"):
        parse_coder_browser_login_payload(payload)


@pytest.mark.parametrize(
    "state",
    ["succeeded", "cancelled", "expired", "timed_out"],
)
def test_browser_login_contract_requires_terminal_session_id(state: str) -> None:
    payload = (
        _successful_browser_login_payload()
        if state == "succeeded"
        else _browser_login_payload(state=state)
    )
    payload.update(
        session_id=None,
        authorization_url=None,
        application_deadline=None,
        cleanup_confirmed=True,
    )

    with pytest.raises(TypeError, match="^browser login session ID is required$"):
        parse_coder_browser_login_payload(payload)


@pytest.mark.parametrize(
    "state",
    ["starting", "waiting_for_user", "waiting_for_code", "authorizing"],
)
def test_browser_login_contract_requires_progress_deadline(state: str) -> None:
    payload = _browser_login_payload(state=state)
    payload["application_deadline"] = None
    if state not in {"waiting_for_user", "waiting_for_code"}:
        payload["authorization_url"] = None

    with pytest.raises(
        TypeError,
        match="^browser login application deadline is required$",
    ):
        parse_coder_browser_login_payload(payload)


def test_browser_login_contract_requires_cleanup_failure_session_id() -> None:
    payload = _browser_login_payload(state="cleanup_failed")
    payload.update(
        session_id=None,
        authorization_url=None,
        cleanup_confirmed=False,
        failure_reason="cancellation_failed",
    )

    with pytest.raises(TypeError, match="^browser login session ID is required$"):
        parse_coder_browser_login_payload(payload)


@pytest.mark.parametrize("state", ["cancelled", "expired", "timed_out"])
def test_browser_login_contract_requires_terminal_cleanup(state: str) -> None:
    payload = _browser_login_payload(state=state)
    payload.update(
        authorization_url=None,
        application_deadline=None,
        cleanup_confirmed=None,
    )

    with pytest.raises(
        TypeError,
        match="^browser login terminal cleanup is unconfirmed$",
    ):
        parse_coder_browser_login_payload(payload)


def test_browser_login_contract_requires_process_failure_cleanup() -> None:
    payload = _browser_login_payload(state="failed")
    payload.update(
        authorization_url=None,
        cleanup_confirmed=None,
        failure_reason="process_failed",
    )

    with pytest.raises(
        TypeError,
        match="^invalid browser login process failure evidence$",
    ):
        parse_coder_browser_login_payload(payload)


@pytest.mark.parametrize(
    ("state", "failure_reason"),
    [("expired", "provider_expired"), ("timed_out", "application_timeout")],
)
def test_browser_login_contract_preserves_process_terminal_state(
    state: str,
    failure_reason: str,
) -> None:
    payload = _browser_login_payload(state=state)
    payload.update(
        authorization_url=None,
        application_deadline=None,
        cleanup_confirmed=True,
        failure_reason=failure_reason,
    )

    parsed = parse_coder_browser_login_payload(payload)

    assert parsed["state"] == state
    assert parsed["failure_reason"] == failure_reason


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload.update(state="cancelled", cleanup_confirmed=True),
        lambda payload: payload.update(session_id=None, cleanup_confirmed=True),
    ],
)
def test_browser_login_contract_binds_process_failure_to_session(
    mutate: Any,
) -> None:
    payload = _browser_login_payload(state="failed")
    payload.update(
        authorization_url=None,
        cleanup_confirmed=True,
        failure_reason="process_failed",
    )
    mutate(payload)

    with pytest.raises(
        TypeError,
        match="^invalid browser login process failure evidence$",
    ):
        parse_coder_browser_login_payload(payload)


def test_browser_login_contract_allows_foreign_session_mismatch() -> None:
    payload = _browser_login_payload(state="failed")
    payload.update(
        authorization_url=None,
        cleanup_confirmed=None,
        failure_reason="session_plugin_mismatch",
    )

    parsed = parse_coder_browser_login_payload(payload)

    assert parsed["session_id"] == payload["session_id"]
    assert parsed["cleanup_confirmed"] is None


@pytest.mark.parametrize(
    ("session_id", "cleanup_confirmed"),
    [(None, None), ("B" * 43, True)],
)
def test_browser_login_contract_accepts_startup_failure_evidence(
    session_id: str | None,
    cleanup_confirmed: bool | None,
) -> None:
    payload = _browser_login_payload(state="failed")
    payload.update(
        session_id=session_id,
        authorization_url=None,
        cleanup_confirmed=cleanup_confirmed,
        failure_reason="startup_failed",
    )

    parsed = parse_coder_browser_login_payload(payload)

    assert parsed["session_id"] == session_id
    assert parsed["cleanup_confirmed"] is cleanup_confirmed


def test_browser_login_contract_rejects_unconfirmed_owned_startup_failure() -> None:
    payload = _browser_login_payload(state="failed")
    payload.update(
        authorization_url=None,
        cleanup_confirmed=None,
        failure_reason="startup_failed",
    )

    with pytest.raises(
        TypeError,
        match="^invalid browser login startup failure evidence$",
    ):
        parse_coder_browser_login_payload(payload)


def test_browser_login_contract_binds_cancellation_failure_to_cleanup() -> None:
    payload = _browser_login_payload(state="failed")
    payload.update(
        authorization_url=None,
        cleanup_confirmed=None,
        failure_reason="cancellation_failed",
    )

    with pytest.raises(
        TypeError,
        match="^invalid browser login cancellation failure state$",
    ):
        parse_coder_browser_login_payload(payload)


@pytest.mark.parametrize(
    ("state", "failure_reason"),
    [
        ("unsupported", "unsupported"),
        ("not_found", "session_not_found"),
        ("failed", "session_plugin_mismatch"),
        ("failed", "session_capacity"),
        ("failed", "replacement_required"),
        ("failed", "credential_in_use"),
        ("failed", "cli_missing"),
    ],
)
def test_browser_login_contract_accepts_out_of_band_failure_state(
    state: str,
    failure_reason: str,
) -> None:
    payload = _browser_login_payload(state=state)
    payload.update(
        session_id=None,
        authorization_url=None,
        application_deadline=None,
        cleanup_confirmed=None,
        failure_reason=failure_reason,
    )
    if failure_reason == "session_plugin_mismatch":
        payload["session_id"] = "B" * 43

    assert parse_coder_browser_login_payload(payload)["state"] == state


@pytest.mark.parametrize("failure_reason", ["unsupported", "session_not_found"])
def test_browser_login_contract_rejects_out_of_band_failure_state(
    failure_reason: str,
) -> None:
    payload = _browser_login_payload(state="failed")
    payload.update(
        authorization_url=None,
        cleanup_confirmed=None,
        failure_reason=failure_reason,
    )

    with pytest.raises(
        TypeError,
        match="^invalid browser login out-of-band failure state$",
    ):
        parse_coder_browser_login_payload(payload)


@pytest.mark.parametrize(
    ("state", "failure_reason", "session_id", "cleanup_confirmed"),
    [
        ("failed", "daemon_unavailable", None, None),
        ("failed", "daemon_shutdown", None, None),
        ("cancelled", "daemon_shutdown", "B" * 43, True),
    ],
)
def test_browser_login_contract_accepts_daemon_lifecycle_failure(
    state: str,
    failure_reason: str,
    session_id: str | None,
    cleanup_confirmed: bool | None,
) -> None:
    payload = _browser_login_payload(state=state)
    payload.update(
        session_id=session_id,
        authorization_url=None,
        application_deadline=None,
        cleanup_confirmed=cleanup_confirmed,
        failure_reason=failure_reason,
    )

    assert parse_coder_browser_login_payload(payload)["state"] == state


@pytest.mark.parametrize(
    ("state", "failure_reason"),
    [
        ("unsupported", "daemon_unavailable"),
        ("expired", "daemon_shutdown"),
    ],
)
def test_browser_login_contract_rejects_daemon_lifecycle_failure(
    state: str,
    failure_reason: str,
) -> None:
    payload = _browser_login_payload(state=state)
    payload.update(
        authorization_url=None,
        application_deadline=None,
        cleanup_confirmed=True,
        failure_reason=failure_reason,
    )

    with pytest.raises(TypeError):
        parse_coder_browser_login_payload(payload)


@pytest.mark.parametrize("state", ["waiting_for_user", "succeeded"])
def test_browser_login_contract_rejects_failure_reason_for_nonfailure_state(
    state: str,
) -> None:
    payload = (
        _browser_login_payload()
        if state == "waiting_for_user"
        else _successful_browser_login_payload()
    )
    payload["failure_reason"] = "credential_in_use"

    with pytest.raises(
        TypeError,
        match="^browser login failure reason contradicts state$",
    ):
        parse_coder_browser_login_payload(payload)


@pytest.mark.parametrize(
    "state",
    ["starting", "waiting_for_user", "waiting_for_code", "authorizing", "canceling"],
)
def test_browser_login_contract_requires_unknown_active_cleanup(state: str) -> None:
    payload = _browser_login_payload(state=state)
    payload["cleanup_confirmed"] = True
    if state not in {"waiting_for_user", "waiting_for_code"}:
        payload["authorization_url"] = None

    with pytest.raises(
        TypeError,
        match="^active browser login cleanup status must be unknown$",
    ):
        parse_coder_browser_login_payload(payload)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda payload: payload.update(cleanup_confirmed=False),
        lambda payload: payload.update(auth_status=None),
        lambda payload: payload["auth_status"].update(saved_credentials_present=False),
        lambda payload: payload["auth_status"].update(saved_credentials_present=None),
        lambda payload: payload["auth_status"].update(status="error"),
        lambda payload: payload["auth_status"].update(failure_reason="probe_timeout"),
    ],
)
def test_browser_login_contract_rejects_unsupported_success_claims(
    mutate: Any,
) -> None:
    payload = _successful_browser_login_payload()
    mutate(payload)

    with pytest.raises(TypeError):
        parse_coder_browser_login_payload(payload)


def test_browser_login_contract_errors_never_echo_rejected_values() -> None:
    secrets = (
        "sensitive-plugin-value",
        "sensitive-session-value",
        "sensitive-url-value",
        "sensitive-version-value\n",
    )
    payloads = []
    for field, secret in zip(
        ("plugin", "session_id", "authorization_url", "observed_cli_version"),
        secrets,
        strict=True,
    ):
        payload = _browser_login_payload()
        payload[field] = secret
        payloads.append(payload)

    for payload in payloads:
        with pytest.raises(TypeError) as error:
            parse_coder_browser_login_payload(payload, expected_plugin="claude")
        assert not any(secret in str(error.value) for secret in secrets)


def test_auth_contract_adapts_legacy_result_and_drops_unknown_keys() -> None:
    payload = coder_auth_payload(
        {
            "status": "ok",
            "detail": "Legacy probe succeeded",
            "raw_secret": "must-not-cross-the-boundary",
        }
    )

    assert payload == {
        "status": "ok",
        "detail": "Legacy probe succeeded",
        "cli_available": None,
        "cli_version": None,
        "saved_credentials_present": None,
        "authentication_mode": None,
        "service_access_verified": None,
        "failure_reason": None,
        "capabilities": {
            "can_check_cli": None,
            "can_check_saved_credentials": None,
            "can_report_authentication_mode": None,
            "can_verify_service_access": None,
            "interactive_login_methods": None,
        },
    }
    assert "must-not-cross-the-boundary" not in str(payload)


def test_auth_contract_round_trips_explicit_status_and_capabilities() -> None:
    payload = coder_auth_payload(
        CoderAuthStatus(
            status="ok",
            detail="Credentials saved; service access not verified",
            cli_available=True,
            cli_version="1.2.3",
            saved_credentials_present=True,
            authentication_mode="browser_oauth",
            service_access_verified=None,
        ),
        capabilities=CoderAuthCapabilities(
            can_check_cli=True,
            can_check_saved_credentials=True,
            can_report_authentication_mode=True,
            can_verify_service_access=False,
            interactive_login_methods=("browser_oauth", "device_code"),
        ),
    )

    assert parse_coder_auth_payload(payload) == payload


@pytest.mark.parametrize(
    "result",
    [
        None,
        {"status": "unknown", "detail": "bad"},
        {"status": "ok", "detail": None},
        {"status": "ok", "detail": "bad", "cli_available": "yes"},
        {"status": "ok", "detail": "bad", "cli_version": 7},
        {"status": "ok", "detail": "bad", "cli_version": "x" * 65},
        {"status": "error", "detail": "bad", "failure_reason": "secret"},
        {"status": "ok", "detail": "bad", "authentication_mode": "BAD"},
        CoderAuthStatus(
            status="ok",
            detail="bad",
            service_access_verified="yes",  # type: ignore[arg-type]
        ),
    ],
)
def test_auth_contract_rejects_invalid_status_fields(result: object) -> None:
    with pytest.raises(TypeError):
        coder_auth_payload(result)


@pytest.mark.parametrize(
    "capabilities",
    [
        CoderAuthCapabilities(
            interactive_login_methods=["browser_oauth"]  # type: ignore[arg-type]
        ),
        CoderAuthCapabilities(
            interactive_login_methods=(None,)  # type: ignore[arg-type]
        ),
        CoderAuthCapabilities(
            can_check_cli="yes"  # type: ignore[arg-type]
        ),
    ],
)
def test_auth_contract_rejects_invalid_plugin_capabilities(
    capabilities: CoderAuthCapabilities,
) -> None:
    with pytest.raises(TypeError):
        coder_auth_payload(
            {"status": "ok", "detail": "ok"},
            capabilities=capabilities,
        )


@pytest.mark.parametrize(
    "capabilities",
    [
        "not-a-mapping",
        {"interactive_login_methods": "browser_oauth"},
        {"interactive_login_methods": [1]},
        {"interactive_login_methods": ["BAD"]},
    ],
)
def test_auth_contract_rejects_invalid_wire_capabilities(
    capabilities: object,
) -> None:
    with pytest.raises(TypeError):
        parse_coder_auth_payload(
            {
                "status": "ok",
                "detail": "ok",
                "capabilities": capabilities,
            }
        )


def test_protocol_includes_supports_breach_lifecycle() -> None:
    """``CoderPlugin`` declares ``supports_breach_lifecycle`` so handlers
    can gate breach monitoring without hardcoding coder names."""
    assert "supports_breach_lifecycle" in dir(CoderPlugin)
    assert isinstance(ClaudePlugin(), CoderPlugin)
    assert isinstance(CodexPlugin(), CoderPlugin)


def test_claude_plugin_supports_breach_lifecycle_true() -> None:
    assert ClaudePlugin().supports_breach_lifecycle is True


def test_codex_plugin_supports_breach_lifecycle_false() -> None:
    assert CodexPlugin().supports_breach_lifecycle is False


def test_protocol_includes_default_pause_percent_properties() -> None:
    """``CoderPlugin`` declares ``default_session_pause_percent`` and
    ``default_weekly_pause_percent`` so handlers can read per-plugin
    rate-limit thresholds without hardcoding coder names."""
    assert "default_session_pause_percent" in dir(CoderPlugin)
    assert "default_weekly_pause_percent" in dir(CoderPlugin)
    assert isinstance(ClaudePlugin(), CoderPlugin)
    assert isinstance(CodexPlugin(), CoderPlugin)


def test_claude_plugin_default_session_pause_percent() -> None:
    assert ClaudePlugin().default_session_pause_percent == 95


def test_claude_plugin_default_weekly_pause_percent() -> None:
    assert ClaudePlugin().default_weekly_pause_percent == 80


def test_codex_plugin_default_session_pause_percent() -> None:
    assert CodexPlugin().default_session_pause_percent == 100


def test_codex_plugin_default_weekly_pause_percent() -> None:
    assert CodexPlugin().default_weekly_pause_percent == 100


def test_claude_plugin_diagnose_error_delegates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    async def fake(
        repo_path: str,
        context: str,
        *,
        model: str | None = None,
        on_process_start: object = None,
        on_supervised_process_start: object = None,
    ) -> tuple[int, str, str]:
        captured["repo_path"] = repo_path
        captured["context"] = context
        captured["model"] = model
        captured["on_process_start"] = on_process_start
        captured["on_supervised_process_start"] = on_supervised_process_start
        return (0, "FIX", "")

    monkeypatch.setattr(claude_cli, "diagnose_error_async", fake)

    code, stdout, stderr = asyncio.run(
        ClaudePlugin().diagnose_error(
            "/tmp/repo", "ci red", model="opus"
        )
    )

    assert (code, stdout, stderr) == (0, "FIX", "")
    assert captured == {
        "repo_path": "/tmp/repo",
        "context": "ci red",
        "model": "opus",
        "on_process_start": None,
        "on_supervised_process_start": None,
    }


def test_codex_plugin_diagnose_error_delegates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    async def fake(
        repo_path: str,
        context: str,
        *,
        model: str | None = None,
        reasoning_effort: str | None = None,
        environment: dict[str, str] | None = None,
        on_process_start: object = None,
        on_supervised_process_start: object = None,
    ) -> tuple[int, str, str]:
        captured["repo_path"] = repo_path
        captured["context"] = context
        captured["model"] = model
        captured["reasoning_effort"] = reasoning_effort
        captured["environment"] = environment
        captured["on_process_start"] = on_process_start
        captured["on_supervised_process_start"] = on_supervised_process_start
        return (0, "SKIP", "")

    monkeypatch.setattr(codex_cli, "diagnose_error_async", fake)

    code, stdout, stderr = asyncio.run(
        CodexPlugin().diagnose_error(
            "/tmp/repo",
            "ci red",
            model="gpt-5.4",
            reasoning_effort="high",
        )
    )

    assert (code, stdout, stderr) == (0, "SKIP", "")
    assert captured == {
        "repo_path": "/tmp/repo",
        "context": "ci red",
        "model": "gpt-5.4",
        "reasoning_effort": "high",
        "environment": None,
        "on_process_start": None,
        "on_supervised_process_start": None,
    }


@pytest.mark.parametrize(
    ("plugin", "module", "runner_name", "model"),
    [
        (ClaudePlugin(), claude_cli, "run_claude_async", "opus"),
        (CodexPlugin(), codex_cli, "run_codex_async", "gpt-5.4"),
    ],
)
def test_plugin_run_prompt_forwards_auxiliary_invocation(
    monkeypatch: pytest.MonkeyPatch,
    plugin: CoderPlugin,
    module: object,
    runner_name: str,
    model: str,
) -> None:
    captured: dict[str, object] = {}

    def process_callback(process: object) -> None:
        del process

    def supervised_callback(managed: object) -> None:
        del managed

    async def fake(prompt: str, repo_path: str, **kwargs: object):
        captured.update(prompt=prompt, repo_path=repo_path, **kwargs)
        return (0, "done", "")

    monkeypatch.setattr(module, runner_name, fake)

    result = asyncio.run(
        plugin.run_prompt(
            "resolve this",
            "/tmp/repo",
            model=model,
            timeout=300,
            on_process_start=process_callback,
            on_supervised_process_start=supervised_callback,
        )
    )

    assert result == (0, "done", "")
    assert captured["prompt"] == "resolve this"
    assert captured["repo_path"] == "/tmp/repo"
    assert captured["model"] == model
    assert captured["timeout"] == 300
    assert captured["on_process_start"] is process_callback
    assert captured["on_supervised_process_start"] is supervised_callback
    if isinstance(plugin, ClaudePlugin):
        assert captured["system_prompt_file"] is None


def test_protocol_includes_run_auto_pr() -> None:
    """``CoderPlugin`` declares ``run_auto_pr`` so the daemon can dispatch
    AUTO PR runs without depending on QUEUE.md/AGENTS.md indirection."""
    assert "run_auto_pr" in dir(CoderPlugin)
    assert isinstance(ClaudePlugin(), CoderPlugin)
    assert isinstance(CodexPlugin(), CoderPlugin)


def test_protocol_includes_build_run_kwargs() -> None:
    """``CoderPlugin`` declares ``build_run_kwargs`` so handlers can
    delegate plugin-specific kwargs construction without hardcoding
    coder names."""
    assert "build_run_kwargs" in dir(CoderPlugin)
    assert isinstance(ClaudePlugin(), CoderPlugin)
    assert isinstance(CodexPlugin(), CoderPlugin)


def test_protocol_includes_effective_model_resolution() -> None:
    assert "resolve_model" in dir(CoderPlugin)
    assert isinstance(ClaudePlugin(), CoderPlugin)
    assert isinstance(CodexPlugin(), CoderPlugin)


def test_builtin_model_resolution_prefers_generic_then_legacy() -> None:
    daemon = DaemonConfig(
        claude_model="legacy-claude",
        codex_model="legacy-codex",
        coder_settings={
            "claude": {"model": "generic-claude"},
            "codex": {"model": ""},
        },
    )

    assert ClaudePlugin().resolve_model(daemon) == "generic-claude"
    assert CodexPlugin().resolve_model(daemon) == ""


def test_builtin_model_resolution_retains_legacy_defaults() -> None:
    daemon = DaemonConfig(claude_model="sonnet", codex_model="legacy-codex")

    assert ClaudePlugin().resolve_model(daemon) == "sonnet"
    assert CodexPlugin().resolve_model(daemon) == "legacy-codex"
    assert ClaudePlugin().resolve_model(DaemonConfig()) == "opus"
    assert CodexPlugin().resolve_model(DaemonConfig()) == ""
    assert (
        ClaudePlugin().resolve_model(
            DaemonConfig(coder_settings={"claude": {"model": ""}})
        )
        == "opus"
    )


def test_model_setting_rejects_non_string_constructed_value() -> None:
    daemon = DaemonConfig.model_construct(
        coder_settings={"custom": {"model": 123}}
    )
    setting = ModelSetting(None, "fallback", "Default")

    with pytest.raises(
        ValueError,
        match=r"daemon\.coder_settings\.custom\.model must be a string",
    ):
        setting.resolve("custom", daemon)


def test_claude_plugin_build_run_kwargs_with_breach() -> None:
    daemon = DaemonConfig(
        claude_model="opus",
        rate_limit_session_pause_percent=90,
        rate_limit_weekly_pause_percent=70,
    )
    kwargs = ClaudePlugin().build_run_kwargs(
        daemon_config=daemon,
        breach_dir="/tmp/breach",
        breach_run_id="abc123",
    )
    assert kwargs == {
        "model": "opus",
        "breach_dir": "/tmp/breach",
        "breach_run_id": "abc123",
        "session_threshold": 90,
        "weekly_threshold": 70,
    }


def test_claude_plugin_build_run_kwargs_without_breach() -> None:
    daemon = DaemonConfig(claude_model="sonnet")
    kwargs = ClaudePlugin().build_run_kwargs(daemon_config=daemon)
    assert kwargs == {"model": "sonnet"}


def test_claude_plugin_build_run_kwargs_partial_breach_input_omits_breach() -> None:
    """A single breach input without the other yields no breach kwargs.

    Both ``breach_dir`` and ``breach_run_id`` must be supplied together
    for the plugin to emit breach-monitoring kwargs.
    """
    daemon = DaemonConfig(claude_model="opus")
    only_dir = ClaudePlugin().build_run_kwargs(
        daemon_config=daemon, breach_dir="/tmp/breach"
    )
    only_id = ClaudePlugin().build_run_kwargs(
        daemon_config=daemon, breach_run_id="abc"
    )
    assert only_dir == {"model": "opus"}
    assert only_id == {"model": "opus"}


def test_codex_plugin_build_run_kwargs_no_breach_keys() -> None:
    """Codex returns only ``model`` even when breach inputs are passed.

    ``supports_breach_lifecycle`` is False so the plugin silently
    ignores breach inputs, letting callers pass them unconditionally.
    """
    daemon = DaemonConfig(codex_model="gpt-5.4")
    kwargs = CodexPlugin().build_run_kwargs(
        daemon_config=daemon,
        breach_dir="/tmp/breach",
        breach_run_id="abc123",
    )
    assert kwargs == {"model": "gpt-5.4"}


def test_build_run_kwargs_uses_generic_model_over_legacy() -> None:
    daemon = DaemonConfig(
        claude_model="legacy-claude",
        codex_model="legacy-codex",
        coder_settings={
            "claude": {"model": "generic-claude"},
            "codex": {"model": "generic-codex"},
        },
    )

    assert ClaudePlugin().build_run_kwargs(daemon_config=daemon)["model"] == (
        "generic-claude"
    )
    assert CodexPlugin().build_run_kwargs(daemon_config=daemon) == {
        "model": "generic-codex"
    }


def test_codex_plugin_build_run_kwargs_default_codex_model_empty() -> None:
    daemon = DaemonConfig()
    kwargs = CodexPlugin().build_run_kwargs(daemon_config=daemon)
    assert kwargs == {"model": ""}
