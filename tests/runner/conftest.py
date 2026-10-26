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
def _isolate_merge_generation_proof(monkeypatch: pytest.MonkeyPatch) -> None:
    """Runner unit tests use synthetic Git; recovery/test_merge_accounting.py
    exercises the production proof together with the real isolated writer.
    """
    from src.daemon.runner import PipelineRunner

    monkeypatch.setattr(
        PipelineRunner, "_proven_merge_generation",
        lambda self: self._current_run_record.task_spec_hash if self._current_run_record is not None else "",
    )


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


@pytest.fixture
def ci_policy_transport(monkeypatch: pytest.MonkeyPatch):
    """Exercise production PR/CI readers with bounded GitHub transport data."""
    from datetime import datetime, timezone

    from src.github import cache, checks, gh_runner, prs, reviews
    from src.models import ReviewStatus

    current = {"sha": "a" * 40, "mode": "missing", "reads": 0}
    checks.clear_ci_status_cache()
    cache.clear_etag_cache()

    def transport(args, **_kwargs):
        sha = current["sha"]
        if args[:2] == ["pr", "list"]:
            return [{"number": 404, "title": "PR-404: CI policy", "headRefName": "pr-404",
                     "headRefOid": sha, "commits": [{}], "author": {"login": "author"}}]
        path = args[1]
        if "/check-runs?" in path:
            current["reads"] += 1
            if "page=2" in path:
                return {"total_count": 3, "check_runs": []}
            names = ["unit", "integration"] if current["mode"] != "missing" else ["unit"]
            runs = [{"id": index, "name": name, "head_sha": sha, "status": "completed",
                     "conclusion": "success", "app": {"id": 1}}
                    for index, name in enumerate(names, 1)]
            total = len(runs) + 1 if current["mode"] == "incomplete" else len(runs)
            return {"total_count": total, "check_runs": runs}
        if "/status?" in path:
            total = 1 if current["mode"] == "status-incomplete" else 0
            return {"sha": sha, "state": "success", "total_count": total, "statuses": []}
        raise AssertionError(args)

    def publication(repo, number, *, expected_head_sha):
        return prs.HeadPublicationEvidence(
            repo, number, repo, "refs/heads/pr-404", expected_head_sha,
            datetime(2026, 10, 20, tzinfo=timezone.utc), "github-activity:branch_creation", None,
        )

    monkeypatch.setattr(gh_runner, "run_gh", transport)
    monkeypatch.setattr(prs, "get_pr_head_publication", publication)
    monkeypatch.setattr(prs, "get_pr_state", lambda *_: "OPEN")
    monkeypatch.setattr(reviews, "get_pr_review_status", lambda *a, **kw: ReviewStatus.APPROVED)
    yield current
    checks.clear_ci_status_cache()
    cache.clear_etag_cache()
