"""Importable coder factories used by configuration-loading tests."""

from __future__ import annotations

import time
from typing import Any

from src.coder_registry import ModelSetting
from src.coders.claude import ClaudePlugin

FACTORY_CALLS = 0
NOT_CALLABLE = "not a factory"


class ConfiguredTestPlugin(ClaudePlugin):
    name = "third"
    display_name = "Configured Test Coder"
    auth_capabilities = None
    models = ["third-default", "third-invoke"]
    model_setting = ModelSetting(
        config_field=None,
        default_value="third-default",
        default_label="Test default",
    )

    def __init__(self) -> None:
        self.run_auto_pr_calls: list[dict[str, Any]] = []
        self.fix_review_calls: list[dict[str, Any]] = []

    def check_auth(self) -> dict[str, str]:
        return {"status": "ok", "detail": "configured test plugin auth"}

    async def run_planned_pr(
        self, *_args: object, **_kwargs: object
    ) -> tuple[int, str, str]:
        return (0, "configured planned", "")

    async def run_auto_pr(
        self, *_args: object, **kwargs: Any
    ) -> tuple[int, str, str]:
        self.run_auto_pr_calls.append(dict(kwargs))
        return (0, "configured coding", "")

    async def fix_review(
        self, *_args: object, **kwargs: Any
    ) -> tuple[int, str, str]:
        self.fix_review_calls.append(dict(kwargs))
        return (0, "configured fix", "")

    async def run_prompt(
        self, *_args: object, **_kwargs: object
    ) -> tuple[int, str, str]:
        return (0, "configured prompt", "")

    async def diagnose_error(
        self, *_args: object, **_kwargs: object
    ) -> tuple[int, str, str]:
        return (0, "FIX\nconfigured diagnosis", "")

    def create_usage_provider(self, **_kwargs: object) -> None:
        return None


class TestUsageProvider:
    def __init__(self) -> None:
        self.snapshot: object | None = None
        self.consecutive_failures = 0
        self.fetch_count = 0
        self.invalidated = False

    def fetch(self) -> object | None:
        self.fetch_count += 1
        return self.snapshot

    def invalidate_cache(self) -> None:
        self.invalidated = True


class TelemetryTestPlugin(ConfiguredTestPlugin):
    name = "telemetry"
    display_name = "Telemetry Test Coder"

    def __init__(self) -> None:
        super().__init__()
        self.usage_provider = TestUsageProvider()

    def create_usage_provider(self, **_kwargs: object) -> TestUsageProvider:
        return self.usage_provider


class ClaudeOverridePlugin(ClaudePlugin):
    name = "claude"
    display_name = "Configured Claude"
    auth_capabilities = None

    def check_auth(self, **_kwargs: object) -> dict[str, str]:
        return {"status": "ok", "detail": "configured plugin auth"}

    def create_usage_provider(self, **_kwargs: object) -> None:
        return None


class DigitLeadingTestPlugin(ConfiguredTestPlugin):
    name = "3rd"
    model_catalog_refreshable = True


class VariantSettingTestPlugin(ConfiguredTestPlugin):
    model_setting = ModelSetting(
        config_field=None,
        default_value="third-default",
        default_label="Test default",
        setting_key="variant",
    )


class ReasoningEffortSettingTestPlugin(ConfiguredTestPlugin):
    model_setting = ModelSetting(
        config_field=None,
        default_value="third-default",
        default_label="Test default",
        setting_key="reasoning_effort",
    )


class RaisingAuthTestPlugin(ConfiguredTestPlugin):
    def check_auth(self) -> dict[str, str]:
        raise RuntimeError("credential=must-not-leak")


class SlowAuthTestPlugin(ConfiguredTestPlugin):
    def check_auth(self) -> dict[str, str]:
        time.sleep(0.05)
        return {"status": "ok", "detail": "too late"}


class MissingMetadataPlugin(ConfiguredTestPlugin):
    display_name = ""


class RaisingMetadataPlugin(ConfiguredTestPlugin):
    @property
    def display_name(self) -> str:
        raise RuntimeError("metadata secret")


def build_test_plugin() -> ConfiguredTestPlugin:
    global FACTORY_CALLS
    FACTORY_CALLS += 1
    return ConfiguredTestPlugin()


def build_telemetry_plugin() -> TelemetryTestPlugin:
    return TelemetryTestPlugin()


def build_claude_override() -> ClaudeOverridePlugin:
    return ClaudeOverridePlugin()


def build_digit_leading_plugin() -> DigitLeadingTestPlugin:
    return DigitLeadingTestPlugin()


def build_variant_setting_plugin() -> VariantSettingTestPlugin:
    return VariantSettingTestPlugin()


def build_reasoning_effort_setting_plugin() -> ReasoningEffortSettingTestPlugin:
    return ReasoningEffortSettingTestPlugin()


def build_raising_auth_plugin() -> RaisingAuthTestPlugin:
    return RaisingAuthTestPlugin()


def build_slow_auth_plugin() -> SlowAuthTestPlugin:
    return SlowAuthTestPlugin()


def build_mismatched_plugin() -> ConfiguredTestPlugin:
    return ConfiguredTestPlugin()


def build_incompatible_plugin() -> object:
    return object()


def build_missing_metadata_plugin() -> MissingMetadataPlugin:
    return MissingMetadataPlugin()


def build_raising_metadata_plugin() -> RaisingMetadataPlugin:
    return RaisingMetadataPlugin()


def build_empty_name_plugin() -> ConfiguredTestPlugin:
    plugin = ConfiguredTestPlugin()
    plugin.name = ""
    return plugin


def build_invalid_models_plugin() -> ConfiguredTestPlugin:
    plugin = ConfiguredTestPlugin()
    plugin.models = ["valid", 3]  # type: ignore[list-item]
    return plugin


def build_invalid_model_setting_plugin() -> ConfiguredTestPlugin:
    plugin = ConfiguredTestPlugin()
    plugin.model_setting = object()  # type: ignore[assignment]
    return plugin


def build_unknown_legacy_field_plugin() -> ConfiguredTestPlugin:
    plugin = ConfiguredTestPlugin()
    plugin.model_setting = ModelSetting(
        config_field="typo",
        default_value="third-default",
        default_label="Test default",
    )
    return plugin


def build_non_string_legacy_field_plugin() -> ConfiguredTestPlugin:
    plugin = ConfiguredTestPlugin()
    plugin.model_setting = ModelSetting(
        config_field=3,  # type: ignore[arg-type]
        default_value="third-default",
        default_label="Test default",
    )
    return plugin


def build_non_model_legacy_field_plugin() -> ConfiguredTestPlugin:
    plugin = ConfiguredTestPlugin()
    plugin.model_setting = ModelSetting(
        config_field="poll_interval_sec",
        default_value="third-default",
        default_label="Test default",
    )
    return plugin


def build_foreign_legacy_field_plugin() -> ConfiguredTestPlugin:
    plugin = ConfiguredTestPlugin()
    plugin.model_setting = ModelSetting(
        config_field="claude_model",
        default_value="third-default",
        default_label="Test default",
    )
    return plugin


def build_claude_foreign_legacy_field_plugin() -> ClaudeOverridePlugin:
    plugin = ClaudeOverridePlugin()
    plugin.model_setting = ModelSetting(
        config_field="codex_model",
        default_value="",
        default_label="CLI default",
    )
    return plugin


def build_non_string_default_value_plugin() -> ConfiguredTestPlugin:
    plugin = ConfiguredTestPlugin()
    plugin.model_setting = ModelSetting(
        config_field=None,
        default_value=3,  # type: ignore[arg-type]
        default_label="Test default",
    )
    return plugin


def build_empty_default_label_plugin() -> ConfiguredTestPlugin:
    plugin = ConfiguredTestPlugin()
    plugin.model_setting = ModelSetting(
        config_field=None,
        default_value="third-default",
        default_label="",
    )
    return plugin


def build_empty_setting_key_plugin() -> ConfiguredTestPlugin:
    plugin = ConfiguredTestPlugin()
    plugin.model_setting = ModelSetting(
        config_field=None,
        default_value="third-default",
        default_label="Test default",
        setting_key="",
    )
    return plugin


def build_non_string_setting_key_plugin() -> ConfiguredTestPlugin:
    plugin = ConfiguredTestPlugin()
    plugin.model_setting = ModelSetting(
        config_field=None,
        default_value="third-default",
        default_label="Test default",
        setting_key=3,  # type: ignore[arg-type]
    )
    return plugin


def build_dotted_setting_key_plugin() -> ConfiguredTestPlugin:
    plugin = ConfiguredTestPlugin()
    plugin.model_setting = ModelSetting(
        config_field=None,
        default_value="third-default",
        default_label="Test default",
        setting_key="reasoning.effort",
    )
    return plugin


def build_invalid_refreshable_plugin() -> ConfiguredTestPlugin:
    plugin = ConfiguredTestPlugin()
    plugin.model_catalog_refreshable = "yes"  # type: ignore[assignment]
    return plugin


def build_exploding_plugin() -> object:
    raise RuntimeError("credential=must-not-leak")


def reset_factory_calls() -> None:
    global FACTORY_CALLS
    FACTORY_CALLS = 0
