"""Importable coder factories used by configuration-loading tests."""

from __future__ import annotations

from src.coder_registry import ModelSetting
from src.coders.claude import ClaudePlugin

FACTORY_CALLS = 0
NOT_CALLABLE = "not a factory"


class ConfiguredTestPlugin(ClaudePlugin):
    name = "third"
    display_name = "Configured Test Coder"
    models = ["third-default", "third-invoke"]
    model_setting = ModelSetting(
        config_field=None,
        default_value="third-default",
        default_label="Test default",
    )

    async def run_planned_pr(self, *_args: object, **_kwargs: object) -> object:
        raise AssertionError("configured plugin inference must not start")

    async def run_auto_pr(self, *_args: object, **_kwargs: object) -> object:
        raise AssertionError("configured plugin inference must not start")

    async def fix_review(self, *_args: object, **_kwargs: object) -> object:
        raise AssertionError("configured plugin inference must not start")

    async def run_prompt(self, *_args: object, **_kwargs: object) -> object:
        raise AssertionError("configured plugin inference must not start")

    async def diagnose_error(self, *_args: object, **_kwargs: object) -> object:
        raise AssertionError("configured plugin inference must not start")


class ClaudeOverridePlugin(ClaudePlugin):
    name = "claude"
    display_name = "Configured Claude"


class MissingMetadataPlugin(ConfiguredTestPlugin):
    display_name = ""


def build_test_plugin() -> ConfiguredTestPlugin:
    global FACTORY_CALLS
    FACTORY_CALLS += 1
    return ConfiguredTestPlugin()


def build_claude_override() -> ClaudeOverridePlugin:
    return ClaudeOverridePlugin()


def build_mismatched_plugin() -> ConfiguredTestPlugin:
    return ConfiguredTestPlugin()


def build_incompatible_plugin() -> object:
    return object()


def build_missing_metadata_plugin() -> MissingMetadataPlugin:
    return MissingMetadataPlugin()


def build_exploding_plugin() -> object:
    raise RuntimeError("credential=must-not-leak")


def reset_factory_calls() -> None:
    global FACTORY_CALLS
    FACTORY_CALLS = 0
