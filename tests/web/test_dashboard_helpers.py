from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_repo_template_context_invokes_load_config_via_to_thread(monkeypatch):
    from src.config import AppConfig, RepoConfig
    from src.web.routes import dashboard as dashboard_routes

    repo_url = "https://github.com/AlexBomber12/example.git"
    config = AppConfig(repositories=[RepoConfig(url=repo_url)])
    to_thread = AsyncMock(return_value=config)
    registry = MagicMock()
    registry.list_coders.return_value = []

    monkeypatch.setattr(dashboard_routes.asyncio, "to_thread", to_thread)
    monkeypatch.setattr(
        dashboard_routes,
        "_build_recent_graphql_burns_view",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(dashboard_routes, "build_coder_registry", lambda: registry)

    await dashboard_routes._repo_template_context(
        "AlexBomber12__example",
        redis_client=None,
    )

    to_thread.assert_any_await(
        dashboard_routes.load_config,
        dashboard_routes._app.CONFIG_PATH,
    )


@pytest.mark.asyncio
async def test_repo_template_context_uses_configured_runtime_coders(monkeypatch):
    from src.coders import build_coder_registry
    from src.config import AppConfig, RepoConfig
    from src.web.routes import dashboard as dashboard_routes

    repo_url = "https://github.com/AlexBomber12/example.git"
    config = AppConfig(
        repositories=[RepoConfig(url=repo_url)],
        coder_plugins={
            "claude": (
                "tests.configured_coder_plugin:build_claude_override"
            ),
            "third": "tests.configured_coder_plugin:build_test_plugin",
        },
    )
    monkeypatch.setattr(
        dashboard_routes.asyncio,
        "to_thread",
        AsyncMock(return_value=config),
    )
    monkeypatch.setattr(
        dashboard_routes,
        "_build_recent_graphql_burns_view",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        dashboard_routes._app.app.state,
        "coder_registry",
        build_coder_registry(config),
        raising=False,
    )

    context = await dashboard_routes._repo_template_context(
        "AlexBomber12__example",
        redis_client=None,
    )

    assert [plugin.name for plugin in context["coders"]] == [
        "claude",
        "codex",
    ]
    assert context["coders"][0].display_name == "Configured Claude"
