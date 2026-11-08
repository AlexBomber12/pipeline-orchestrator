"""Runs tab markup and the existing metrics endpoint's row/empty contracts."""
import re

import pytest
from fastapi.testclient import TestClient
from src.web.app import app

from tests.test_web_app import _FakeRedis, _metrics_record
from tests.test_web_app import two_repo_config as two_repo_config


@pytest.mark.usefixtures("two_repo_config")
@pytest.mark.parametrize("populated", [False, True])
def test_runs_tab_and_metrics_rows(populated):
    record = _metrics_record(
        "runs-fixture", task_id="PR-505", started_at="2026-10-01T10:00:00Z",
        ended_at="2026-10-01T10:01:00Z", duration_ms=60000,
        fix_iterations=1, exit_reason="success_merged",
        invocations=[{
            "plugin_id": "codex", "model_override": "<model>",
            "reasoning_effort_override": "high", "run_id": "runs-fixture",
            "attempt_index": 1, "phase": "fix", "fix_iteration": 1,
        }],
    )
    redis = _FakeRedis(
        store={"metrics:run:runs-fixture": record} if populated else {},
        lists={"metrics:repo:example__alpha:PR": ["runs-fixture"]} if populated else {},
    )
    with TestClient(app) as client:
        client.app.state.redis = redis
        page = client.get("/repo/example__alpha")
        response = client.get("/partials/repo/example__alpha/metrics")
    assert page.status_code == response.status_code == 200
    assert 'data-repo-tab="runs"' in page.text
    panel = re.search(r'id="repo-panel-runs".*?</template>', page.text, re.S).group()
    assert 'hidden' in panel and 'data-panel-content' in panel
    assert 'hx-get="/partials/repo/example__alpha/metrics"' in panel
    assert 'hx-trigger="load"' in panel
    body = response.text
    assert 'hx-trigger="repo:pr_metrics_update from:body"' in body
    if populated:
        assert 'table-fixed' in body and '<colgroup>' in body
        assert 'PR-505' in body and '1m' in body and 'merged' in body
        details = re.search(r'<details\b[^>]*>.*?</details>', body, re.S).group()
        assert ' open' not in details.split('>')[0]
        assert 'Plugin ID:' in details and 'FIX 1' in details
        assert 'Model override: &lt;model&gt;' in details
        assert 'Reasoning effort override: high' in details
    else:
        assert 'No completed PR runs recorded yet.' in body
        assert '<table' not in body
