"""Shared unit-test fixtures.

The autouse ``isolate_analytics_dir`` redirects
``src.analytics.outcome_logger`` writes into a per-test tmp directory so
``handle_merge`` exercises (which now appends a structured outcome row)
do not touch the real ``/data/analytics/`` partition during the suite.

The autouse ``isolate_events_dir`` does the same for
``src.events.disk_log`` so any test that exercises
``publish_repo_event`` does not touch the real ``/data/events/``
partition.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from src.coder_registry import ModelMetadata
from src.coders.codex_models import CodexModel


@pytest.fixture(autouse=True)
def _mock_settings_model_discovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep web tests deterministic and prevent real coder subprocesses."""

    async def discover(**_kwargs: object) -> tuple[CodexModel, ...]:
        return (CodexModel("gpt-5.4", "GPT-5.4", True, None, ()),)

    monkeypatch.setattr(
        "src.coders.codex.discover_codex_models",
        discover,
    )

    async def discover_claude(
        **_kwargs: object,
    ) -> tuple[ModelMetadata, ...]:
        return (
            ModelMetadata("opus", "opus"),
            ModelMetadata("sonnet", "sonnet"),
        )

    monkeypatch.setattr(
        "src.coders.claude.discover_claude_models",
        discover_claude,
    )

    class DirectCatalogLoader:
        async def __call__(
            self, plugin: object, **kwargs: object
        ) -> object:
            target = plugin
            if not hasattr(target, "get_model_catalog"):
                from src.coders import _load_plugin
                from src.config import load_config
                from src.web import app as web_app

                config = load_config(web_app.CONFIG_PATH)
                target = _load_plugin(
                    plugin.name,
                    config.coder_plugins[plugin.name],
                )
            return await target.get_model_catalog(**kwargs)

        async def load_plugin_metadata(
            self,
            plugin_id: str,
            *,
            expected_reference: str,
        ) -> object:
            from src.coders import _load_plugin
            from src.config import load_config
            from src.model_catalog_bridge import (
                _parse_plugin_metadata,
                _plugin_metadata_payload,
            )
            from src.web import app as web_app

            config = load_config(web_app.CONFIG_PATH)
            assert config.coder_plugins[plugin_id] == expected_reference
            plugin = _load_plugin(plugin_id, expected_reference)
            return _parse_plugin_metadata(
                _plugin_metadata_payload(plugin),
                expected_name=plugin_id,
            )

        async def load_auth_status(
            self,
            plugin_id: str,
            *,
            expected_reference: str,
        ) -> dict[str, str]:
            from src.coder_auth import isolated_auth_probe
            from src.coders import _load_plugin
            from src.web import app as web_app
            from src.web.services import auth_probe

            plugin = _load_plugin(plugin_id, expected_reference)
            return await isolated_auth_probe(
                plugin_id,
                expected_reference,
                plugin.display_name,
                config_path=web_app.CONFIG_PATH,
                timeout=auth_probe._AUTH_CHECK_TIMEOUT_SEC,
            )

    monkeypatch.setattr(
        "src.web.app.DaemonModelCatalogLoader",
        lambda _redis: DirectCatalogLoader(),
    )


@pytest.fixture(autouse=True)
def isolate_analytics_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    target = tmp_path / "analytics"
    monkeypatch.setenv("PO_ANALYTICS_DIR", str(target))
    return target


@pytest.fixture(autouse=True)
def isolate_events_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    target = tmp_path / "events"
    monkeypatch.setenv("PO_EVENTS_DIR", str(target))
    return target
