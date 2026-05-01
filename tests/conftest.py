"""Shared unit-test fixtures.

The autouse ``isolate_analytics_dir`` redirects
``src.analytics.outcome_logger`` writes into a per-test tmp directory so
``handle_merge`` exercises (which now appends a structured outcome row)
do not touch the real ``/data/analytics/`` partition during the suite.
"""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def isolate_analytics_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    target = tmp_path / "analytics"
    monkeypatch.setenv("PO_ANALYTICS_DIR", str(target))
    return target
