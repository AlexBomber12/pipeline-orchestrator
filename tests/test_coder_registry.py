from __future__ import annotations

import asyncio
import builtins
import re
from typing import Any

import pytest
from src import claude_cli, codex_cli
from src import coders as coders_module
from src.coder_registry import (
    CoderPlugin,
    CoderRegistry,
    ModelCatalog,
    ModelMetadata,
    ModelSetting,
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
    config = AppConfig(
        coder_plugins={"third": reference},
        daemon=DaemonConfig(coder_settings={"third": {"variant": 123}}),
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
        on_process_start: object = None,
        on_supervised_process_start: object = None,
    ) -> tuple[int, str, str]:
        captured["repo_path"] = repo_path
        captured["context"] = context
        captured["model"] = model
        captured["on_process_start"] = on_process_start
        captured["on_supervised_process_start"] = on_supervised_process_start
        return (0, "SKIP", "")

    monkeypatch.setattr(codex_cli, "diagnose_error_async", fake)

    code, stdout, stderr = asyncio.run(
        CodexPlugin().diagnose_error(
            "/tmp/repo", "ci red", model="gpt-5.4"
        )
    )

    assert (code, stdout, stderr) == (0, "SKIP", "")
    assert captured == {
        "repo_path": "/tmp/repo",
        "context": "ci red",
        "model": "gpt-5.4",
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
