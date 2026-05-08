"""Tests for PR-018 dashboard alerts panel.

Covers the pure alert-building helpers, the ``GET /partials/alerts``
HTTP handler, and the dashboard index mount so the HTMX polling
container stays wired up correctly.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from src.models import (
    PipelineState,
    RepoState,
)
from src.web import app as web_app
from src.web.app import (
    _build_alerts,
    _format_alert_duration,
    _most_recent_transition_into,
    app,
)


def _iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat()


class _FakeRedis:
    def __init__(self, store: dict[str, str] | None = None) -> None:
        self.store = store or {}

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return self.store.get(key)


class _StubAioredisClient:
    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return None

    async def aclose(self) -> None:
        return None


class _StubAioredis:
    @staticmethod
    def from_url(
        url: str, decode_responses: bool = True
    ) -> _StubAioredisClient:
        return _StubAioredisClient()


def _write_config(tmp_path: Path, urls: list[str]) -> Path:
    cfg = tmp_path / "config.yml"
    lines = ["repositories:"]
    for url in urls:
        lines.append(f"  - url: {url}")
    cfg.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return cfg



def _error_state(
    name: str,
    url: str,
    *,
    last_updated: datetime,
    error_message: str,
) -> RepoState:
    return RepoState(
        url=url,
        name=name,
        state=PipelineState.ERROR,
        error_message=error_message,
        last_updated=last_updated,
    )


# ---- pure helpers ---------------------------------------------------------


def test_format_alert_duration_spans_seconds_minutes_and_hours() -> None:
    assert _format_alert_duration(0) == "0 sec"
    assert _format_alert_duration(45) == "45 sec"
    assert _format_alert_duration(59) == "59 sec"
    assert _format_alert_duration(60) == "1 min"
    assert _format_alert_duration(15 * 60) == "15 min"
    assert _format_alert_duration(59 * 60) == "59 min"
    assert _format_alert_duration(60 * 60) == "1h"
    assert _format_alert_duration(2 * 60 * 60 + 30 * 60) == "2h 30min"
    # negative clock-skew input is clamped instead of rendering "-3 sec"
    assert _format_alert_duration(-5) == "0 sec"


def test_build_alerts_skips_healthy_repos() -> None:
    now = datetime.now(timezone.utc)
    healthy = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.CODING,
        last_updated=now,
    )
    alerts = _build_alerts([healthy])
    assert alerts == []


def test_build_alerts_sorts_errors_by_duration() -> None:
    now = datetime.now(timezone.utc)
    recent_error = _error_state(
        "example__gamma",
        "https://github.com/example/gamma.git",
        last_updated=now - timedelta(seconds=30),
        error_message="boom",
    )
    older_error = _error_state(
        "example__delta",
        "https://github.com/example/delta.git",
        last_updated=now - timedelta(minutes=5),
        error_message="earlier boom",
    )

    alerts = _build_alerts([recent_error, older_error])
    kinds = [a["kind"] for a in alerts]
    assert kinds == ["ERROR", "ERROR"]
    assert alerts[0]["repo_name"] == "example__delta"
    assert alerts[1]["repo_name"] == "example__gamma"



def test_most_recent_transition_into_picks_latest_run_start() -> None:
    """Helper returns the start of the MOST RECENT run of ``target_state``.

    Repeated same-state log entries inside a single run are repeat polls
    (the daemon logs from FIX, WATCH, etc. on every cycle even when the
    state hasn't actually flipped) — the transition is only the first
    entry of each run, and we want the start of the latest run so the
    alert duration reflects "time since the most recent error", not the
    first error ever seen.
    """
    t1 = datetime(2026, 4, 11, 9, 0, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 4, 11, 9, 5, 0, tzinfo=timezone.utc)
    t3 = datetime(2026, 4, 11, 9, 10, 0, tzinfo=timezone.utc)
    t4 = datetime(2026, 4, 11, 9, 15, 0, tzinfo=timezone.utc)
    t5 = datetime(2026, 4, 11, 9, 20, 0, tzinfo=timezone.utc)
    t6 = datetime(2026, 4, 11, 9, 25, 0, tzinfo=timezone.utc)
    history = [
        {"time": _iso(t1), "state": "CODING", "event": "start"},
        {"time": _iso(t2), "state": "ERROR", "event": "boom 1"},
        {"time": _iso(t3), "state": "ERROR", "event": "still boom 1"},
        {"time": _iso(t4), "state": "IDLE", "event": "recovered"},
        {"time": _iso(t5), "state": "ERROR", "event": "boom 2"},
        {"time": _iso(t6), "state": "ERROR", "event": "still boom 2"},
    ]
    # latest ERROR run starts at t5, not t2
    assert _most_recent_transition_into(history, "ERROR") == t5
    # state never present -> None
    assert _most_recent_transition_into(history, "HUNG") is None
    # empty history -> None
    assert _most_recent_transition_into([], "ERROR") is None
    # entries with unparseable time are skipped but run detection still
    # advances via ``prev_state``, so a later run with a valid time wins
    history_with_legacy = [
        {"time": "09:00:00", "state": "ERROR", "event": "legacy"},
        {"time": _iso(t5), "state": "IDLE", "event": "reset"},
        {"time": _iso(t6), "state": "ERROR", "event": "parseable boom"},
    ]
    assert _most_recent_transition_into(history_with_legacy, "ERROR") == t6


def test_build_alerts_error_duration_survives_publish_state_rewrite() -> None:
    """Regression for P2: ERROR duration must not reset every daemon cycle.

    ``Runner.publish_state`` rewrites ``state.last_updated`` to ``now``
    on every cycle (see ``src/daemon/runner.py``), so basing the alert
    "since" timestamp on ``last_updated`` makes hours-old errors always
    render as "a few sec" and breaks the duration-desc sort inside the
    ERROR bucket. The fix derives ``since`` from the most recent ERROR
    transition in ``state.history`` instead.
    """
    now = datetime.now(timezone.utc)
    error_transition = now - timedelta(minutes=47)
    stale = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.ERROR,
        error_message="claude CLI exited 1",
        # last_updated just got rewritten by a publish_state tick
        last_updated=now - timedelta(seconds=2),
        history=[
            {
                "time": _iso(now - timedelta(minutes=50)),
                "state": "CODING",
                "event": "started",
            },
            {
                "time": _iso(error_transition),
                "state": "ERROR",
                "event": "boom",
            },
            # a later ERROR entry inside the same run — this is a
            # repeat poll, not a new transition
            {
                "time": _iso(now - timedelta(minutes=3)),
                "state": "ERROR",
                "event": "still broken",
            },
        ],
    )
    [alert] = _build_alerts([stale])
    # Duration is measured from the transition INTO ERROR, not from the
    # most recent "still broken" poll and not from last_updated.
    assert alert["duration_text"] == "47 min"



def test_build_alerts_error_card_carries_message_and_repo_link() -> None:
    now = datetime.now(timezone.utc)
    err = _error_state(
        "example__alpha",
        "https://github.com/example/alpha.git",
        last_updated=now - timedelta(minutes=3),
        error_message="claude CLI exited 1",
    )
    [alert] = _build_alerts([err])
    assert alert["kind"] == "ERROR"
    assert alert["error_message"] == "claude CLI exited 1"
    assert alert["repo_url"] == "/repo/example__alpha"
    assert alert["duration_text"] == "3 min"


def test_build_alerts_normalizes_naive_since_timestamp() -> None:
    naive_since = datetime.now()
    err = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.ERROR,
        error_message="boom",
        last_updated=naive_since,
    )

    [alert] = _build_alerts([err])

    assert alert["since_iso"].endswith("+00:00")


# ---- HTTP handlers --------------------------------------------------------


@pytest.fixture
def alerts_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    cfg = _write_config(
        tmp_path,
        [
            "https://github.com/example/alpha.git",
            "https://github.com/example/beta.git",
        ],
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())
    return cfg


def test_partial_alerts_empty_when_all_healthy(alerts_config: Path) -> None:
    """No alerts -> fragment renders nothing substantive.

    The outer polling container stays mounted so it can keep polling,
    but the fragment must not leave behind any alert markup (no "Attention
    Required" header, no count badge, no cards).
    """
    with TestClient(app) as client:
        response = client.get("/partials/alerts")

    assert response.status_code == 200
    body = response.text
    assert "Attention Required" not in body
    assert "border-l-4" not in body
    # fragment body is effectively empty (only whitespace / comments)
    assert body.strip() == "" or "<section" not in body


def test_partial_alerts_renders_error_card(alerts_config: Path) -> None:
    now = datetime.now(timezone.utc)
    err = _error_state(
        "example__alpha",
        "https://github.com/example/alpha.git",
        last_updated=now - timedelta(minutes=2),
        error_message="claude CLI exited 1",
    )
    fake = _FakeRedis({"pipeline:example__alpha": err.model_dump_json()})

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.get("/partials/alerts")

    assert response.status_code == 200
    body = response.text
    assert "Attention Required" in body
    assert "claude CLI exited 1" in body
    # link to repo detail page is present on the card
    assert 'href="/repo/example__alpha"' in body
    # error cards use the fail border utility
    assert "border-fail" in body
    # count badge shows "1" (rendered inside the red rounded-full span)
    assert "bg-fail text-white" in body
    assert "1" in body



def test_alert_repo_link_points_to_repo_detail(alerts_config: Path) -> None:
    now = datetime.now(timezone.utc)
    err = _error_state(
        "example__alpha",
        "https://github.com/example/alpha.git",
        last_updated=now,
        error_message="boom",
    )
    fake = _FakeRedis({"pipeline:example__alpha": err.model_dump_json()})

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.get("/partials/alerts")

    body = response.text
    assert 'href="/repo/example__alpha"' in body


def test_index_mounts_alerts_partial(alerts_config: Path) -> None:
    """Dashboard index wires the status bar (with inline alerts) to the SSE state_change trigger."""
    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    body = response.text
    assert 'hx-get="/partials/stats"' in body
    assert 'hx-trigger="repo:state_change from:body, every 30s"' in body
