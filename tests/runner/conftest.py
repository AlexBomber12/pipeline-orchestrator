"""PR-224a: Shared autouse fixtures for the runner test package.

Mirrors the autouse fixtures defined in ``tests/test_runner.py`` so that
tests moved into ``tests/runner/`` retain the same default monkeypatching
behavior. Without these, moved tests would invoke the real DAG selector
and the real GitHub rate-limit fetch path.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from src.daemon.handlers import idle as idle_module


@pytest.fixture(autouse=True)
def _disable_dag_selection_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _no_dag(self) -> None:
        self._idle_dag_tasks = None
        return None

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        _no_dag,
    )


@pytest.fixture(autouse=True)
def _disable_github_rate_limit_fetch_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tests that don't pin a budget shouldn't actually call the gh CLI."""
    monkeypatch.setattr(
        "src.github.rate_limit.fetch_rate_limit_buckets",
        lambda: (None, None),
    )


@pytest.fixture
def recovery_golden_cases() -> list[tuple[str, dict, dict]]:
    fixture_root = (
        Path(__file__).resolve().parents[1] / "fixtures" / "recovery"
    )
    cases: list[tuple[str, dict, dict]] = []
    for scenario_dir in sorted(path for path in fixture_root.iterdir() if path.is_dir()):
        before = json.loads((scenario_dir / "before.json").read_text())
        expected = json.loads((scenario_dir / "expected.json").read_text())
        cases.append((scenario_dir.name, before, expected))
    return cases
