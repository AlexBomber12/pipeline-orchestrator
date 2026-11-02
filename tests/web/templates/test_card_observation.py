"""Report age is published evidence, never process liveness."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from src.models import PipelineState, RepoState
from src.web import app as web_app
from src.web.routes import dashboard
from starlette.requests import Request

NOW = datetime(2026, 11, 2, 12, tzinfo=timezone.utc)


def _repo(**changes):
    return RepoState.model_construct(
        name="example__repo", url="https://github.com/example/repo",
        state=PipelineState.WATCH, last_updated=NOW - timedelta(minutes=14),
    ).model_copy(update=changes)


def _render(repo):
    return web_app.templates.get_template("components/repo_cards.html").render(
        repos=[repo], observation_now=NOW,
        observation_stale_after=dashboard.OBSERVATION_STALE_AFTER.total_seconds(),
        cancellation_subsources={}, drain_progress={},
    )


@pytest.mark.parametrize("state", list(PipelineState))
def test_stale_report_labels(state):
    html = _render(_repo(state=state))
    assert "No report for 14 min" in html
    assert 'text-warn" data-observation-age' in html
    assert ("Last known:" in html) == (state not in (PipelineState.IDLE, PipelineState.PAUSED))


@pytest.mark.parametrize("seconds,label", [
    (37, "Reported 37s ago"), (600, "Reported 600s ago"),
    (601, "No report for 10 min"), (-60, "Reported 0s ago"),
])
def test_age_boundaries_and_future_skew(seconds, label):
    html = _render(_repo(last_updated=NOW - timedelta(seconds=seconds)))
    assert label in html
    assert ("Last known:" in html) == (seconds > 600)


@pytest.mark.parametrize("timestamp", [None, "invalid", "", 0, NOW.replace(tzinfo=None)])
def test_invalid_report_time_is_unavailable(timestamp):
    html = _render(_repo(last_updated=timestamp))
    assert "Report time unavailable" in html
    assert "Reported 0s ago" not in html


def test_missing_and_synthetic_timestamps_are_unavailable():
    repo = RepoState(name="example__repo", url="https://github.com/example/repo")
    assert "Report time unavailable" in _render(repo)
    for message in ("Redis unavailable — state unknown", "State decode failed",
                    "Waiting for daemon to initialize"):
        assert "Report time unavailable" in _render(_repo(error_message=message))


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", [dashboard.index, dashboard.partial_repo_list])
@pytest.mark.parametrize("mode", ["fresh", "no_client", "warning", "synthetic"])
async def test_grid_availability_evidence(monkeypatch, tmp_path, endpoint, mode):
    config = tmp_path / "config.yml"
    config.write_text("repositories: []\n")
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(config))
    repo = _repo()
    if mode == "synthetic":
        repo.error_message = "Redis unavailable — state unknown"
    warning = "Redis connection lost" if mode == "warning" else None
    monkeypatch.setattr(web_app, "get_all_repo_states", AsyncMock(return_value=([repo], warning)))
    redis = None if mode == "no_client" else SimpleNamespace(get=AsyncMock(return_value=None))
    request = Request({"type": "http", "app": SimpleNamespace(state=SimpleNamespace(redis=redis)),
                       "headers": [], "method": "GET", "path": "/"})
    response = await endpoint(request)
    assert response.context["observation_available"] == (mode == "fresh")
    assert response.context["observation_now"].tzinfo is not None
    if endpoint == dashboard.partial_repo_list:
        assert response.headers["X-Observation-Available"] == str(mode == "fresh").lower()
        assert datetime.fromisoformat(response.headers["X-Observation-Time"]).tzinfo is not None
