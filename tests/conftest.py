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
from src.coders.codex_models import CodexModel


@pytest.fixture(autouse=True)
def _mock_settings_codex_model_discovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep web tests deterministic and prevent real Codex subprocesses."""

    async def discover(**_kwargs: object) -> tuple[CodexModel, ...]:
        return (CodexModel("gpt-5.4", "GPT-5.4", True, None, ()),)

    monkeypatch.setattr(
        "src.coders.codex.discover_codex_models",
        discover,
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
