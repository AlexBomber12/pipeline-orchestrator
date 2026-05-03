"""PR-247: Recover button visibility on the repo card.

The Recover control is HUNG-specific. The dashboard repo cards must
render the button only when the persisted RepoState is HUNG and never
on any other state — surfacing it on IDLE/CODING/WATCH/FIX/MERGE/ERROR
or PAUSED would imply the operator can use it as a generic abort, but
the daemon-side handler refuses any state but HUNG.
"""

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


def test_recover_button_renders_when_state_is_hung(
    alpha_config: Path,
) -> None:
    rendered = _render_summary(PipelineState.HUNG)

    assert 'hx-post="/repos/example__alpha/recover"' in rendered
    assert "aria-label=\"Recover from HUNG\"" in rendered
    assert "cancel current task and return to IDLE" in rendered
    assert "id=\"controls-recover-spinner-example__alpha\"" in rendered


@pytest.mark.parametrize(
    "non_hung_state",
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
def test_recover_button_hidden_on_non_hung_state(
    alpha_config: Path, non_hung_state: PipelineState
) -> None:
    rendered = _render_summary(non_hung_state)

    assert "/repos/example__alpha/recover" not in rendered
    assert "controls-recover-spinner-example__alpha" not in rendered
