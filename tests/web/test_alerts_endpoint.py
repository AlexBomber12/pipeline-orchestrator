"""Tests for the lightweight ``/api/alerts`` endpoint introduced in PR-241."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from src.models import PipelineState, RepoState
from src.web import app as web_app


def _make_state(name: str, state: PipelineState) -> RepoState:
    return RepoState(
        url=f"https://github.com/example/{name}.git",
        name=f"example__{name}",
        state=state,
        last_updated=datetime.now(timezone.utc),
    )


@pytest.fixture
def alerts_client(monkeypatch: pytest.MonkeyPatch):
    """Return a TestClient and a mutable list backing ``get_all_repo_states``.

    The endpoint reads through ``_app.get_all_repo_states`` (re-exported
    from the repo-state service); patching that single seam is enough to
    drive the route without standing up a config file or Redis.
    """
    fake_states: list[RepoState] = []

    async def fake_get_all_repo_states(_redis=None, _config_path=None):
        return list(fake_states), None

    monkeypatch.setattr(
        web_app, "get_all_repo_states", fake_get_all_repo_states
    )
    with TestClient(web_app.app) as client:
        yield client, fake_states


def test_alerts_endpoint_returns_zero_when_no_alerts(alerts_client) -> None:
    client, states = alerts_client
    states[:] = [
        _make_state("alpha", PipelineState.IDLE),
        _make_state("beta", PipelineState.WATCH),
    ]

    resp = client.get("/api/alerts")

    assert resp.status_code == 200
    assert resp.json() == {"has_alerts": False, "count": 0}


def test_alerts_endpoint_counts_hung_and_error_states(alerts_client) -> None:
    client, states = alerts_client
    states[:] = [
        _make_state("alpha", PipelineState.IDLE),
        _make_state("beta", PipelineState.HUNG),
        _make_state("gamma", PipelineState.ERROR),
        _make_state("delta", PipelineState.WATCH),
    ]

    resp = client.get("/api/alerts")

    assert resp.status_code == 200
    assert resp.json() == {"has_alerts": True, "count": 2}


def test_alerts_endpoint_payload_size_independent_of_repo_count(
    alerts_client,
) -> None:
    """The whole point of PR-241: payload size doesn't grow with repo count."""
    client, states = alerts_client
    sizes = []
    for n_repos in (1, 10, 50):
        states[:] = [
            _make_state(f"repo{i}", PipelineState.IDLE) for i in range(n_repos)
        ]
        resp = client.get("/api/alerts")
        sizes.append(len(json.dumps(resp.json())))

    assert all(size < 100 for size in sizes), sizes
    # Constant payload: every response is exactly the same length regardless
    # of how many repos were considered.
    assert len(set(sizes)) == 1


def test_alerts_endpoint_ignores_non_alert_states(alerts_client) -> None:
    """States other than HUNG/ERROR (CODING/FIX/MERGE/etc.) are not alerts."""
    client, states = alerts_client
    states[:] = [
        _make_state("alpha", PipelineState.CODING),
        _make_state("beta", PipelineState.FIX),
        _make_state("gamma", PipelineState.MERGE),
    ]

    resp = client.get("/api/alerts")

    assert resp.json() == {"has_alerts": False, "count": 0}
