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
from pathlib import Path
from typing import Any

import pytest
from src import codex_cli
from src.coder_registry import CoderPlugin, CoderRegistry
from src.config import DaemonConfig
from src.daemon import runner as runner_module
from src.daemon.runner import PipelineRunner
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

    def __init__(self) -> None:
        self.run_planned_pr_calls: list[dict[str, Any]] = []
        self.fix_review_calls: list[dict[str, Any]] = []
        self.diagnose_calls: list[tuple[str, str, str]] = []

    async def run_planned_pr(self, repo_path: str, **kwargs: Any) -> tuple[int, str, str]:
        self.run_planned_pr_calls.append({"repo_path": repo_path, **kwargs})
        return (0, "fake stdout", "")

    async def fix_review(self, repo_path: str, **kwargs: Any) -> tuple[int, str, str]:
        self.fix_review_calls.append({"repo_path": repo_path, **kwargs})
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

    async def diagnose_error(self, repo_path: str, context: str, model: str) -> tuple[int, str, str]:
        self.diagnose_calls.append((repo_path, context, model))
        return (0, "FIX\nfake diagnose", "")

    def build_run_kwargs(
        self,
        *,
        daemon_config: DaemonConfig,
        breach_dir: str | None = None,
        breach_run_id: str | None = None,
    ) -> dict[str, Any]:
        # Third coder uses ``claude_model`` config slot for its model;
        # the test doesn't add a fake_model attribute to DaemonConfig.
        # The point is the handler doesn't care which slot — the plugin
        # decides. supports_breach_lifecycle is False so breach inputs
        # are ignored.
        return {"model": "fake-1"}


def test_fake_plugin_satisfies_protocol() -> None:
    """``FakeCoderPlugin`` is recognized as a ``CoderPlugin`` at runtime."""
    assert isinstance(FakeCoderPlugin(), CoderPlugin)


def test_handle_coding_dispatches_to_fake_plugin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``handle_coding`` runs end-to-end against a third coder.

    The handler must take whatever kwargs ``plugin.build_run_kwargs``
    returns and pass them via ``**kwargs`` to ``plugin.run_planned_pr``.
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

    assert len(fake.run_planned_pr_calls) == 1
    call = fake.run_planned_pr_calls[0]
    # The plugin's build_run_kwargs returned only {"model": "fake-1"};
    # the handler added timeout + on_process_start. Crucially, no
    # breach_dir / breach_run_id / session_threshold / weekly_threshold
    # kwargs leaked from the Claude path because plugin.build_run_kwargs
    # owns the per-plugin shape.
    assert call["model"] == "fake-1"
    assert "timeout" in call
    assert "on_process_start" in call
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

    call = fake.run_planned_pr_calls[0]
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

    def fake_select(ctx: object) -> tuple[str, object]:
        seen.append(ctx)
        return ("codex", codex)

    monkeypatch.setattr(runner_module, "select_coder", fake_select)

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

    def fake_select(ctx: object) -> tuple[str, object]:
        seen.append(ctx)
        return ("claude", runner._registry.get("claude"))

    monkeypatch.setattr(runner_module, "select_coder", fake_select)

    runner._get_coder()

    assert seen
    assert getattr(seen[0], "auth_statuses") == runner._auth_status_cache


def test_get_coder_falls_through_to_default_when_selector_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._allow_all_coder_auth(monkeypatch)
    runner = h._make_runner()
    monkeypatch.setattr(runner_module, "select_coder", lambda ctx: None)

    name, plugin = runner._get_coder()

    assert name == "claude"
    assert plugin.name == "claude"


def test_get_coder_hard_pin_overrides_default_when_selector_returns_none(
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
    monkeypatch.setattr(runner_module, "select_coder", lambda ctx: None)

    name, plugin = runner._get_coder()

    assert name == "codex"
    assert plugin.name == "codex"


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

    async def fake_run_planned_pr(path: str, **kwargs: object) -> tuple:
        return (0, "ok", "")

    monkeypatch.setattr(codex_cli, "run_planned_pr_async", fake_run_planned_pr)
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
