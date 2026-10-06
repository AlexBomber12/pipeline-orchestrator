"""PR-227c: third-coder dispatch through CoderPlugin Protocol.

These tests are the acceptance criterion for the PR-227 series: a
hypothetical third coder plugin (``FakeCoderPlugin``) drives
``handle_coding`` and ``handle_fix`` end-to-end without any edit to the
handlers. If a future regression sneaks a ``coder_name == "claude"``
or ``coder_name == "codex"`` branch back into the kwargs path, those
plugins' kwargs would leak through and this test would observe them.
"""

from __future__ import annotations

import asyncio
import random
import re
import time
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from src import codex_cli
from src.coder_registry import (
    CoderPlugin,
    CoderRegistry,
    ModelCatalog,
    ModelMetadata,
    ModelSetting,
)
from src.coders import build_coder_registry
from src.config import AppConfig, DaemonConfig, load_config
from src.daemon import main as main_module
from src.daemon import runner as runner_module
from src.daemon.runner import PipelineRunner
from src.daemon.selector import CoderResolution, CoderSelectionUnavailable
from src.models import (
    CIStatus,
    PipelineState,
    PRInfo,
    QueueTask,
    ReviewStatus,
    TaskStatus,
)
from src.usage import UsageSnapshot

from tests.runner import _helpers as h


class FakeCoderPlugin:
    """Third coder implementing the full ``CoderPlugin`` Protocol.

    No production code knows the name ``fake``; if a handler still
    branches on coder name, this plugin would surface as a missing
    case.
    """

    name = "fake"
    display_name = "Fake Coder"
    models = ["fake-1", "fake-2"]
    model_setting = ModelSetting(None, "fake-1", "(default)")
    model_catalog_refreshable = False

    def __init__(self) -> None:
        self.run_planned_pr_calls: list[dict[str, Any]] = []
        self.run_auto_pr_calls: list[dict[str, Any]] = []
        self.fix_review_calls: list[dict[str, Any]] = []
        self.run_prompt_calls: list[dict[str, Any]] = []
        self.diagnose_calls: list[dict[str, Any]] = []

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

    async def run_planned_pr(self, repo_path: str, **kwargs: Any) -> tuple[int, str, str]:
        self.run_planned_pr_calls.append({"repo_path": repo_path, **kwargs})
        return (0, "fake stdout", "")

    async def run_auto_pr(self, repo_path: str, **kwargs: Any) -> tuple[int, str, str]:
        self.run_auto_pr_calls.append({"repo_path": repo_path, **kwargs})
        return (0, "fake stdout", "")

    async def fix_review(self, repo_path: str, **kwargs: Any) -> tuple[int, str, str]:
        self.fix_review_calls.append({"repo_path": repo_path, **kwargs})
        return (0, "fake stdout", "")

    async def run_prompt(
        self, prompt: str, repo_path: str, **kwargs: Any
    ) -> tuple[int, str, str]:
        self.run_prompt_calls.append(
            {"prompt": prompt, "repo_path": repo_path, **kwargs}
        )
        return (0, "fake stdout", "")

    def check_auth(self) -> dict[str, str]:
        return {"status": "ok", "detail": "fake auth ok"}

    def create_usage_provider(self, **kwargs: Any) -> None:
        return None

    def rate_limit_patterns(self) -> list[re.Pattern[str]]:
        return [re.compile("fake rate limit")]

    @property
    def supports_breach_lifecycle(self) -> bool:
        return False

    @property
    def default_session_pause_percent(self) -> int:
        return 100

    @property
    def default_weekly_pause_percent(self) -> int:
        return 100

    async def diagnose_error(
        self, repo_path: str, context: str, model: str, **kwargs: Any
    ) -> tuple[int, str, str]:
        self.diagnose_calls.append(
            {
                "repo_path": repo_path,
                "context": context,
                "model": model,
                **kwargs,
            }
        )
        return (0, "FIX\nfake diagnose", "")

    def resolve_model(self, daemon_config: DaemonConfig) -> str:
        return self.model_setting.resolve(self.name, daemon_config)

    def build_run_kwargs(
        self,
        *,
        daemon_config: DaemonConfig,
        breach_dir: str | None = None,
        breach_run_id: str | None = None,
    ) -> dict[str, Any]:
        # No ``fake_model`` field exists on DaemonConfig; the plugin resolves
        # its stable-ID namespace through the shared setting contract.
        return {"model": self.resolve_model(daemon_config)}


def test_fake_plugin_satisfies_protocol() -> None:
    """``FakeCoderPlugin`` is recognized as a ``CoderPlugin`` at runtime."""
    assert isinstance(FakeCoderPlugin(), CoderPlugin)


def test_handle_coding_dispatches_to_fake_plugin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``handle_coding`` runs end-to-end against a third coder.

    The handler must take whatever kwargs ``plugin.build_run_kwargs``
    returns and pass them via ``**kwargs`` to ``plugin.run_auto_pr``.
    No claude/codex special-case branch may leak Claude or Codex
    kwargs into the call.
    """
    h._patch_subprocess(monkeypatch)
    fake = FakeCoderPlugin()
    opened_pr = PRInfo(
        number=42,
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [opened_pr],
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )

    runner = h._make_runner()
    runner._registry.register(fake)  # type: ignore[arg-type]
    runner._get_coder = (  # type: ignore[method-assign]
        lambda allow_exploration=False: ("fake", fake)
    )
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
    )

    asyncio.run(runner.handle_coding())

    assert len(fake.run_auto_pr_calls) == 1
    call = fake.run_auto_pr_calls[0]
    # The plugin's build_run_kwargs returned only {"model": "fake-1"};
    # the handler added timeout + on_process_start. Crucially, no
    # breach_dir / breach_run_id / session_threshold / weekly_threshold
    # kwargs leaked from the Claude path because plugin.build_run_kwargs
    # owns the per-plugin shape.
    assert call["model"] == "fake-1"
    assert "timeout" in call
    assert "on_process_start" in call
    assert "pr_id" in call
    assert "task_file" in call
    assert "task_body" in call
    assert "breach_dir" not in call
    assert "breach_run_id" not in call
    assert "session_threshold" not in call
    assert "weekly_threshold" not in call
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 42


def test_handle_fix_dispatches_to_fake_plugin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``handle_fix`` runs end-to-end against a third coder.

    The handler must dispatch to ``plugin.fix_review`` with the kwargs
    returned by ``plugin.build_run_kwargs`` plus the handler's own
    composition (``on_process_start`` and ``extra_context`` when
    applicable). No breach kwargs from the Claude path may leak.
    """
    h._patch_subprocess(monkeypatch)
    fake = FakeCoderPlugin()
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )
    monkeypatch.setattr(
        "src.github.prs.get_branch_last_push_time",
        lambda repo, number: None,
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: None,
    )

    runner = h._make_runner()
    runner._registry.register(fake)  # type: ignore[arg-type]
    runner._get_coder = (  # type: ignore[method-assign]
        lambda allow_exploration=False: ("fake", fake)
    )
    runner.state.current_pr = PRInfo(
        number=99,
        branch="pr-001",
        ci_status=CIStatus.FAILURE,
        review_status=ReviewStatus.PENDING,
    )

    asyncio.run(runner.handle_fix())

    assert len(fake.fix_review_calls) == 1
    call = fake.fix_review_calls[0]
    assert call["model"] == "fake-1"
    assert "on_process_start" in call
    assert "breach_dir" not in call
    assert "breach_run_id" not in call
    assert "session_threshold" not in call
    assert "weekly_threshold" not in call
    # FIX FEEDBACK exits 0 with a productive push (head_before !=
    # head_after via the _patch_subprocess defaults), so the runner
    # transitions to WATCH after recording the push.
    assert runner.state.state == PipelineState.WATCH


def test_configured_plugin_reaches_normal_coding_and_fix_dispatch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A module:factory plugin is selected without patching runner selection."""
    h._patch_subprocess(monkeypatch)
    opened_pr = PRInfo(
        number=42,
        branch="pr-901",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kwargs: [opened_pr],
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )
    monkeypatch.setattr(
        "src.github.prs.get_branch_last_push_time",
        lambda repo, number: None,
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: None,
    )

    config_path = tmp_path / "config.yml"
    config_path.write_text(
        "coder_plugins:\n"
        "  third: tests.configured_coder_plugin:build_test_plugin\n"
        "daemon:\n"
        "  coder: third\n"
        "  exploration_epsilon: 0\n"
        "repositories:\n"
        "  - url: https://github.com/example/repo.git\n"
        "    branch: main\n",
        encoding="utf-8",
    )
    config = load_config(str(config_path))
    registry = build_coder_registry(config)
    claude_provider, codex_provider = h._usage_providers()
    runner = PipelineRunner(
        config.repositories[0],
        config,
        h._FakeRedis(),
        claude_provider,
        codex_provider,
        registry=registry,
    )
    breach_attributions: list[str] = []

    def capture_late_breach(
        breach_dir: str,
        run_id: str,
        coder_name: str,
        breach_flag: dict[str, bool],
    ) -> None:
        del breach_dir, run_id, breach_flag
        breach_attributions.append(coder_name)

    monkeypatch.setattr(runner, "_check_late_breach", capture_late_breach)
    runner.repo_path = str(tmp_path)
    (tmp_path / ".git" / "info").mkdir(parents=True)
    (tmp_path / "tasks").mkdir()
    task_body = (
        "---\n---\n"
        "# PR-901: Configured dispatch\n\n"
        "Branch: pr-901\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: third\n"
    )
    (tmp_path / "tasks" / "PR-901.md").write_text(task_body, encoding="utf-8")
    runner.state.current_task = QueueTask(
        pr_id="PR-901",
        title="Configured dispatch",
        status=TaskStatus.DOING,
        branch="pr-901",
        task_file="tasks/PR-901.md",
    )
    runner._auth_status_cache = {
        name: {"status": "ok"} for name in registry.coder_names()
    }
    runner._auth_status_cache_expires_at = datetime.now(timezone.utc) + timedelta(
        minutes=5
    )

    asyncio.run(runner.handle_coding())

    plugin = registry.get("third")
    assert len(plugin.run_auto_pr_calls) == 1
    assert plugin.run_auto_pr_calls[0]["model"] == "third-default"
    assert breach_attributions == ["third"]
    assert runner.state.coder == "third"
    assert runner.state.state == PipelineState.WATCH

    runner.state.current_pr = PRInfo(
        number=42,
        branch="pr-901",
        ci_status=CIStatus.FAILURE,
        review_status=ReviewStatus.PENDING,
    )
    asyncio.run(runner.handle_fix())

    assert len(plugin.fix_review_calls) == 1
    assert plugin.fix_review_calls[0]["model"] == "third-default"
    assert breach_attributions == ["third", "third"]
    assert runner.state.state == PipelineState.WATCH


@pytest.mark.parametrize(
    ("handler_name", "log_prefix"),
    [("handle_coding", "[CODING]"), ("handle_fix", "[FIX]")],
)
def test_dispatch_handler_reports_unavailable_selection(
    monkeypatch: pytest.MonkeyPatch,
    handler_name: str,
    log_prefix: str,
) -> None:
    runner = h._make_runner()
    transitions: list[tuple[str, dict[str, Any]]] = []

    async def refresh_auth() -> None:
        return None

    def unavailable(*_args: object, **_kwargs: object) -> None:
        raise CoderSelectionUnavailable("configured coder unavailable")

    async def transition(message: str, **kwargs: Any) -> None:
        transitions.append((message, kwargs))

    monkeypatch.setattr(runner, "_refresh_auth_status_cache", refresh_auth)
    monkeypatch.setattr(runner, "_get_coder", unavailable)
    monkeypatch.setattr(runner, "_transition_to_error", transition)

    asyncio.run(getattr(runner, handler_name)())

    assert transitions == [
        (
            "configured coder unavailable",
            {"publish": False, "log_prefix": log_prefix},
        )
    ]


def test_usage_provider_identity_follows_selected_plugin() -> None:
    config = AppConfig(
        repositories=[h._repo_cfg()],
        daemon=DaemonConfig(coder="telemetry"),
        coder_plugins={
            "telemetry": (
                "tests.configured_coder_plugin:build_telemetry_plugin"
            )
        },
    )
    registry = build_coder_registry(config)
    claude_provider, codex_provider = main_module._create_usage_providers(
        config, registry
    )
    plugin = registry.get("telemetry")
    plugin.usage_provider.snapshot = UsageSnapshot(
        session_percent=17,
        session_resets_at=18,
        weekly_percent=19,
        weekly_resets_at=20,
        fetched_at=time.time(),
    )
    runner = PipelineRunner(
        config.repositories[0],
        config,
        h._FakeRedis(),
        claude_provider,
        codex_provider,
        registry=registry,
        usage_providers=registry.usage_providers(),
    )
    runner.state.coder = "telemetry"

    asyncio.run(runner.publish_state())

    assert plugin.usage_provider.fetch_count == 1
    assert runner.state.usage_session_percent == 17
    assert runner.state.usage_weekly_percent == 19


def test_plugin_without_usage_provider_does_not_inherit_builtin_quota() -> None:
    config = AppConfig(
        repositories=[h._repo_cfg()],
        daemon=DaemonConfig(coder="third"),
        coder_plugins={
            "third": "tests.configured_coder_plugin:build_test_plugin"
        },
    )
    registry = build_coder_registry(config)
    claude_provider, codex_provider = main_module._create_usage_providers(
        config, registry
    )
    claude_provider._cached = UsageSnapshot(
        session_percent=100,
        session_resets_at=18,
        weekly_percent=100,
        weekly_resets_at=20,
        fetched_at=time.time(),
    )
    codex_provider._cached = claude_provider._cached
    runner = PipelineRunner(
        config.repositories[0],
        config,
        h._FakeRedis(),
        claude_provider,
        codex_provider,
        registry=registry,
        usage_providers=registry.usage_providers(),
    )
    runner.state.coder = "third"
    runner.state.usage_session_percent = 55
    runner.state.usage_weekly_percent = 66

    assert asyncio.run(runner._fetch_usage_snapshot("third")) is None
    assert asyncio.run(runner.usage_gate(proactive_coder="third")) is True
    asyncio.run(runner.publish_state())

    assert runner.state.state != PipelineState.PAUSED
    assert runner.state.usage_session_percent is None
    assert runner.state.usage_weekly_percent is None
    assert runner.state.usage_api_degraded is False


def test_overridden_builtin_uses_registered_rate_limit_patterns() -> None:
    class _ClaudeOverride(FakeCoderPlugin):
        name = "claude"

        def rate_limit_patterns(self) -> list[re.Pattern[str]]:
            return [re.compile("override throttle sentinel")]

    runner = h._make_runner()
    runner._registry.register(
        _ClaudeOverride(),
        reference="tests.example:build_claude_override",
    )

    runner._detect_rate_limit(
        "override throttle sentinel",
        coder_name="claude",
    )

    assert runner.state.rate_limit_reactive_coder == "claude"
    assert runner.state.rate_limited_until is not None


def test_handle_merge_dispatches_auxiliary_prompt_to_fake_plugin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Merge-conflict execution is plugin-owned for an arbitrary name."""

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:2] == ["git", "merge"] and "origin/main" in cmd:
            return h._FakeCompletedProcess(
                args=cmd,
                returncode=1,
                stdout="CONFLICT (content): merge conflict in foo",
            )
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    fake = _OverridingPlugin()
    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )

    runner = h._make_runner()
    runner._registry.register(fake)  # type: ignore[arg-type]
    runner._get_auxiliary_coder = lambda: ("fake", fake)  # type: ignore[method-assign]
    runner.state.current_pr = PRInfo(number=42, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )

    asyncio.run(runner.handle_merge())

    assert len(fake.run_prompt_calls) == 1
    call = fake.run_prompt_calls[0]
    assert call["prompt"] == (
        "Resolve all merge conflicts in the working tree. Keep both sides "
        "where possible. Run scripts/ci.sh to verify."
    )
    assert call["repo_path"] == runner.repo_path
    assert call["model"] == "fake-1"
    assert call["timeout"] == 300
    assert call["on_process_start"] == runner._track_current_coder_process
    assert call["on_supervised_process_start"] == (
        runner._track_current_coder_supervised_process
    )
    assert call["provider_option"] == "configured"
    assert call["timeout"] != _OverridingPlugin.SENTINEL_TIMEOUT
    assert call["on_process_start"] is not _OverridingPlugin.SENTINEL_HOOK
    assert runner.state.state == PipelineState.WATCH


def test_handle_merge_stop_cleans_auxiliary_process_and_aborts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    git_calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        git_calls.append(cmd)
        if cmd[:2] == ["git", "merge"] and "origin/main" in cmd:
            return h._FakeCompletedProcess(
                args=cmd,
                returncode=1,
                stdout="CONFLICT (content): merge conflict in foo",
            )
        return h._FakeCompletedProcess(args=cmd, returncode=0)

    class _Managed:
        process = types.SimpleNamespace(returncode=0)

        async def cleanup(self, **kwargs: object) -> object:
            del kwargs
            return types.SimpleNamespace(quiescent=True, detail=None)

    managed = _Managed()
    fake = FakeCoderPlugin()

    async def wait_for_stop(
        prompt: str, repo_path: str, **kwargs: Any
    ) -> tuple[int, str, str]:
        del prompt, repo_path
        kwargs["on_process_start"](managed.process)
        kwargs["on_supervised_process_start"](managed)
        await asyncio.Future()
        raise AssertionError("unreachable")

    fake.run_prompt = wait_for_stop  # type: ignore[method-assign]
    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    runner = h._make_runner()
    runner._get_auxiliary_coder = lambda: ("fake", fake)  # type: ignore[method-assign]
    runner.redis.store[f"control:{runner.name}:stop"] = "1"
    runner.state.current_pr = PRInfo(number=42, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.DOING
    )

    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.user_paused is True
    assert runner._current_coder_process is None
    assert runner._current_coder_supervised_process is None
    assert any(cmd[:3] == ["git", "merge", "--abort"] for cmd in git_calls)
    assert not any(cmd[:2] == ["git", "push"] for cmd in git_calls)


class _OverridingPlugin(FakeCoderPlugin):
    """Plugin whose ``build_run_kwargs`` returns handler-owned keys.

    Used to verify that handler keys remain authoritative when a plugin
    accidentally (or maliciously) emits ``timeout`` / ``on_process_start``.
    """

    SENTINEL_TIMEOUT = 1
    SENTINEL_HOOK = staticmethod(lambda proc: None)

    def build_run_kwargs(
        self,
        *,
        daemon_config: DaemonConfig,
        breach_dir: str | None = None,
        breach_run_id: str | None = None,
    ) -> dict[str, Any]:
        return {
            "model": "fake-1",
            "provider_option": "configured",
            "timeout": self.SENTINEL_TIMEOUT,
            "on_process_start": self.SENTINEL_HOOK,
        }


def test_handle_coding_handler_keys_override_plugin_kwargs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Plugin-supplied ``timeout`` / ``on_process_start`` must not win.

    Daemon-owned safety knobs (CODING timeout, process tracking hook)
    have to remain authoritative even when ``build_run_kwargs`` returns
    them — otherwise stop/kill behavior breaks.
    """
    h._patch_subprocess(monkeypatch)
    fake = _OverridingPlugin()
    opened_pr = PRInfo(
        number=42,
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [opened_pr],
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )

    runner = h._make_runner()
    runner._registry.register(fake)  # type: ignore[arg-type]
    runner._get_coder = (  # type: ignore[method-assign]
        lambda allow_exploration=False: ("fake", fake)
    )
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
    )

    asyncio.run(runner.handle_coding())

    call = fake.run_auto_pr_calls[0]
    assert call["timeout"] == runner.app_config.daemon.planned_pr_timeout_sec
    assert call["timeout"] != _OverridingPlugin.SENTINEL_TIMEOUT
    assert call["on_process_start"] == runner._track_current_coder_process
    assert call["on_process_start"] is not _OverridingPlugin.SENTINEL_HOOK


def test_handle_fix_handler_keys_override_plugin_kwargs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Plugin-supplied ``on_process_start`` must not win in FIX.

    FIX's process-tracking hook drives stop/idle/external-state control;
    a plugin that emits the same key cannot be allowed to overwrite it.
    """
    h._patch_subprocess(monkeypatch)
    fake = _OverridingPlugin()
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )
    monkeypatch.setattr(
        "src.github.prs.get_branch_last_push_time",
        lambda repo, number: None,
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: None,
    )

    runner = h._make_runner()
    runner._registry.register(fake)  # type: ignore[arg-type]
    runner._get_coder = (  # type: ignore[method-assign]
        lambda allow_exploration=False: ("fake", fake)
    )
    runner.state.current_pr = PRInfo(
        number=99,
        branch="pr-001",
        ci_status=CIStatus.FAILURE,
        review_status=ReviewStatus.PENDING,
    )

    asyncio.run(runner.handle_fix())

    call = fake.fix_review_calls[0]
    assert call["on_process_start"] == runner._track_current_coder_process
    assert call["on_process_start"] is not _OverridingPlugin.SENTINEL_HOOK


# ---------------------------------------------------------------------------
# PR-224b moved from tests/test_runner.py — third_coder group
# ---------------------------------------------------------------------------


def test_get_coder_returns_claude_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._allow_all_coder_auth(monkeypatch)
    runner = h._make_runner()
    name, plugin = runner._get_coder()
    assert name == "claude"
    assert plugin.name == "claude"


def test_get_coder_returns_codex_when_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.config import CoderType

    h._allow_all_coder_auth(monkeypatch)
    runner = h._make_runner()
    runner._app_config = h._app_cfg(coder=CoderType.CODEX)
    name, plugin = runner._get_coder()
    assert name == "codex"
    assert plugin.name == "codex"


def test_get_coder_repo_override_takes_precedence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.config import CoderType

    h._allow_all_coder_auth(monkeypatch)
    runner = h._make_runner(coder=CoderType.CODEX)
    # Daemon default is claude, repo override is codex
    name, plugin = runner._get_coder()
    assert name == "codex"
    assert plugin.name == "codex"


def test_get_coder_uses_selector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    codex = runner._registry.get("codex")
    seen = []

    def fake_resolve(ctx: object, *, purpose: object) -> CoderResolution:
        seen.append(ctx)
        return CoderResolution("codex", codex, "ranked")

    monkeypatch.setattr(runner_module, "resolve_active_coder", fake_resolve)

    name, plugin = runner._get_coder()

    assert seen
    assert name == "codex"
    assert plugin is codex


def test_get_coder_uses_cached_auth_statuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._allow_all_coder_auth(monkeypatch)
    runner = h._make_runner()
    runner._auth_status_cache = {
        "claude": {"status": "ok"},
        "codex": {"status": "error"},
    }
    seen: list[object] = []

    def fake_resolve(ctx: object, *, purpose: object) -> CoderResolution:
        seen.append(ctx)
        return CoderResolution("claude", runner._registry.get("claude"), "ranked")

    monkeypatch.setattr(runner_module, "resolve_active_coder", fake_resolve)

    runner._get_coder()

    assert seen
    assert getattr(seen[0], "auth_statuses") == runner._auth_status_cache


def test_select_auxiliary_coder_returns_none_when_resolver_has_no_plugin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    monkeypatch.setattr(
        runner_module,
        "resolve_active_coder",
        lambda ctx, *, purpose: CoderResolution("ghost", None, "fallback"),
    )

    assert runner._select_auxiliary_coder() is None


def test_get_coder_does_not_bypass_selector_when_no_coder_is_eligible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._allow_all_coder_auth(monkeypatch)
    runner = h._make_runner()
    monkeypatch.setattr(
        runner_module,
        "resolve_active_coder",
        lambda ctx, *, purpose: None,
    )

    with pytest.raises(CoderSelectionUnavailable, match="claude is unavailable"):
        runner._get_coder()


def test_get_coder_does_not_use_stale_reactive_coder_as_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._allow_all_coder_auth(monkeypatch)
    runner = h._make_runner()
    runner.state.rate_limit_reactive_coder = "ghost"
    monkeypatch.setattr(
        runner_module,
        "resolve_active_coder",
        lambda ctx, *, purpose: None,
    )

    with pytest.raises(CoderSelectionUnavailable, match="claude is unavailable"):
        runner._get_coder()


def test_get_coder_hard_pin_never_falls_back_when_selector_returns_none(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """When the active task pins a specific coder, ``_get_coder`` must not
    silently fall back to the repo/global default if the selector rejects
    the pin. Otherwise FIX iterations can run on the wrong coder."""
    from src.config import CoderType

    h._allow_all_coder_auth(monkeypatch)
    runner = h._make_runner(coder=CoderType.CLAUDE)
    runner.repo_path = str(tmp_path)
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-201.md").write_text(
        "---\n---\n"
        "# PR-201: Pinned to codex\n\n"
        "Branch: pr-201-pinned\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: codex\n",
        encoding="utf-8",
    )
    runner.state.current_task = QueueTask(
        pr_id="PR-201",
        title="Pinned to codex",
        status=TaskStatus.TODO,
        task_file="tasks/PR-201.md",
        branch="pr-201-pinned",
    )
    monkeypatch.setattr(
        runner_module,
        "resolve_active_coder",
        lambda ctx, *, purpose: None,
    )

    with pytest.raises(CoderSelectionUnavailable, match="pinned to codex"):
        runner._get_coder()


def test_get_coder_repo_override_uses_selector_for_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.config import CoderType

    h._allow_all_coder_auth(monkeypatch)
    runner = h._make_runner(coder=CoderType.CODEX)
    runner.state.rate_limited_coders.add("codex")

    name, plugin = runner._get_coder()

    assert name == "claude"
    assert plugin.name == "claude"


def test_get_coder_exploration_occasionally_picks_non_greedy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._allow_all_coder_auth(monkeypatch)
    registry = CoderRegistry()
    registry.register(runner_module.build_coder_registry().get("claude"))
    registry.register(runner_module.build_coder_registry().get("codex"))
    runner = PipelineRunner(
        h._repo_cfg(),
        h._app_cfg(
            auto_fallback=True,
            coder_priority={"claude": 10, "codex": 20},
            exploration_epsilon=0.15,
        ),
        h._FakeRedis(),
        h._FakeUsageProvider(),
        h._FakeUsageProvider(),
        registry=registry,
    )
    runner._selector_rng.seed(9)

    picks = [runner._get_coder()[0] for _ in range(200)]
    non_greedy = sum(1 for pick in picks if pick != "claude")

    assert 15 <= non_greedy <= 45


def test_event_log_includes_coder_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.config import CoderType

    h._patch_subprocess(monkeypatch)

    async def fake_run_planned_pr(path: str, *_args: object, **kwargs: object) -> tuple:
        return (0, "ok", "")

    monkeypatch.setattr(codex_cli, "run_auto_pr_async", fake_run_planned_pr)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda *a, **kw: [
            PRInfo(
                number=42,
                url="https://github.com/octo/demo/pull/42",
                branch="pr-001",
                ci_status=CIStatus.PENDING,
                review_status=ReviewStatus.PENDING,
            )
        ],
    )
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda *a, **kw: True,
    )

    runner = h._make_runner(coder=CoderType.CODEX)
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
    )
    asyncio.run(runner.handle_coding())

    events = [h["event"] for h in runner.state.history]
    assert any("[codex]" in e for e in events)


def test_runner_initializes_selector_rng_without_fixed_seed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_args: list[tuple[object, ...]] = []
    real_random = runner_module.random.Random

    def fake_random(*args: object, **kwargs: object) -> random.Random:
        assert not kwargs
        captured_args.append(args)
        return real_random(*args)

    monkeypatch.setattr(runner_module.random, "Random", fake_random)

    PipelineRunner(
        h._repo_cfg(),
        h._app_cfg(),
        h._FakeRedis(),
        h._FakeUsageProvider(),
        h._FakeUsageProvider(),
    )

    assert captured_args == [()]


def test_proactive_check_uses_codex_provider_when_coder_is_codex(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_proactive_usage_check should use codex provider for codex coder."""
    from src.config import CoderType

    h._patch_subprocess(monkeypatch)
    runner = h._make_runner(coder=CoderType.CODEX)
    runner.app_config.daemon.rate_limit_session_pause_percent = 80
    snap = UsageSnapshot(
        session_percent=90,
        session_resets_at=int(time.time()) + 3600,
        weekly_percent=10,
        weekly_resets_at=int(time.time()) + 86400,
        fetched_at=time.time(),
    )
    runner._codex_usage_provider = h._FakeUsageProvider(snapshot=snap)
    runner._claude_usage_provider = h._FakeUsageProvider(snapshot=None)

    result = asyncio.run(runner._proactive_usage_check())
    assert result is False
    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.rate_limit_reactive_coder == "codex"


def test_proactive_check_uses_claude_provider_when_coder_is_claude(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_proactive_usage_check should use claude provider for claude coder."""

    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    runner.app_config.daemon.rate_limit_session_pause_percent = 80
    snap = UsageSnapshot(
        session_percent=90,
        session_resets_at=int(time.time()) + 3600,
        weekly_percent=10,
        weekly_resets_at=int(time.time()) + 86400,
        fetched_at=time.time(),
    )
    runner._claude_usage_provider = h._FakeUsageProvider(snapshot=snap)
    runner._codex_usage_provider = h._FakeUsageProvider(snapshot=None)

    result = asyncio.run(runner._proactive_usage_check())
    assert result is False
    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.rate_limit_reactive_coder == "claude"
