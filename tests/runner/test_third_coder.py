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
import re
from typing import Any

import pytest
from src.coder_registry import CoderPlugin
from src.config import DaemonConfig
from src.daemon import runner as runner_module
from src.models import (
    CIStatus,
    PipelineState,
    PRInfo,
    QueueTask,
    ReviewStatus,
    TaskStatus,
)

from tests import test_runner as h


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

    async def run_planned_pr(
        self, repo_path: str, **kwargs: Any
    ) -> tuple[int, str, str]:
        self.run_planned_pr_calls.append({"repo_path": repo_path, **kwargs})
        return (0, "fake stdout", "")

    async def fix_review(
        self, repo_path: str, **kwargs: Any
    ) -> tuple[int, str, str]:
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

    async def diagnose_error(
        self, repo_path: str, context: str, model: str
    ) -> tuple[int, str, str]:
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
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [opened_pr],
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
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
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: None,
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_branch_last_push_time",
        lambda repo, number: None,
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_last_push_age_seconds",
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
