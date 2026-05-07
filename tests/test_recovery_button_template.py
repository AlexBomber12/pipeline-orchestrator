"""Recover button removal on the repo card."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

import pytest
from src.models import PipelineState, RepoState
from src.web import app as web_app


class _FakeRedis:
    def __init__(self, store: dict[str, str] | None = None) -> None:
        self.store = store or {}

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return self.store.get(key)


@pytest.fixture
def alpha_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n  - url: https://github.com/example/alpha.git\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    return cfg


def _render_summary(state: PipelineState) -> str:
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=state,
        last_updated=datetime(2026, 5, 3, 12, 0, 0, tzinfo=timezone.utc),
    )
    fake = _FakeRedis(
        {"pipeline:example__alpha": stored.model_dump_json()}
    )
    context = asyncio.run(
        web_app._repo_template_context("example__alpha", fake)
    )
    return web_app.templates.get_template(
        "components/repo_summary.html"
    ).render(context)


@pytest.mark.parametrize(
    "state",
    [
        PipelineState.IDLE,
        PipelineState.CODING,
        PipelineState.WATCH,
        PipelineState.FIX,
        PipelineState.MERGE,
        PipelineState.ERROR,
        PipelineState.PAUSED,
        PipelineState.PREFLIGHT,
    ],
)
def test_recover_button_never_renders(
    alpha_config: Path, state: PipelineState
) -> None:
    rendered = _render_summary(state)

    assert "/repos/example__alpha/recover" not in rendered
    assert "controls-recover-spinner-example__alpha" not in rendered
    assert "Recover from HUNG" not in rendered
