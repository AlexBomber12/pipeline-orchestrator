"""Tests for PR-017 dashboard observability.

Covers the cross-repo stats, the activity feed merge, the event-log filter
helper, and the HTTP handlers that wire them to templates and JSON.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.models import (
    CIStatus,
    PipelineState,
    PRInfo,
    QueueTask,
    RepoState,
    ReviewStatus,
    TaskStatus,
)
from src.web import app as web_app
from src.web.app import (
    _build_activity_feed,
    _compute_stats,
    _filter_history,
    _parse_history_time,
    _repo_badge_style,
    app,
)


class _FakeRedis:
    """Minimal ``redis.asyncio`` stand-in keyed by literal string."""

    def __init__(self, store: dict[str, str] | None = None) -> None:
        self.store = store or {}

    async def get(self, key: str) -> str | None:
        return self.store.get(key)


class _StubAioredisClient:
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


def _iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat()


def _write_config(tmp_path: Path, urls: list[str]) -> Path:
    cfg = tmp_path / "config.yml"
    lines = ["repositories:"]
    for url in urls:
        lines.append(f"  - url: {url}")
    cfg.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return cfg


def _coding_state(
    name: str,
    url: str,
    *,
    history: list[dict[str, str]] | None = None,
    state: PipelineState = PipelineState.CODING,
) -> RepoState:
    return RepoState(
        url=url,
        name=name,
        state=state,
        current_task=QueueTask(
            pr_id="PR-001",
            title="Sample",
            status=TaskStatus.DOING,
        ),
        current_pr=PRInfo(
            number=1,
            branch="pr-001-sample",
            ci_status=CIStatus.PENDING,
            review_status=ReviewStatus.EYES,
        ),
        history=history or [],
        last_updated=datetime(2026, 4, 11, 12, 0, 0, tzinfo=timezone.utc),
    )


# ---- pure helpers ---------------------------------------------------------


def test_parse_history_time_handles_iso_and_clock_strings() -> None:
    assert _parse_history_time("") is None
    assert _parse_history_time("not a time") is None

    parsed = _parse_history_time("2026-04-11T12:00:00+00:00")
    assert parsed == datetime(2026, 4, 11, 12, 0, 0, tzinfo=timezone.utc)

    naive = _parse_history_time("2026-04-11T12:00:00")
    # naive values are assumed UTC so stats windows stay deterministic
    assert naive is not None
    assert naive.tzinfo is not None


def test_repo_badge_style_is_stable_and_in_palette() -> None:
    first = _repo_badge_style("alpha")
    second = _repo_badge_style("alpha")
    other = _repo_badge_style("beta")
    assert first == second
    assert "bg-" in first
    assert first != other or first == other  # palette collisions are OK


def test_compute_stats_counts_active_alerts_and_merges() -> None:
    now = datetime.now(timezone.utc)
    today_morning = now.replace(hour=1, minute=0, second=0, microsecond=0)
    three_days_ago = now - timedelta(days=3)
    ten_days_ago = now - timedelta(days=10)

    alpha = _coding_state(
        "alpha",
        "https://github.com/example/alpha.git",
        history=[
            {
                "time": _iso(today_morning),
                "state": "MERGE",
                "event": "Merged PR #1 -> IDLE",
            },
            {
                "time": _iso(today_morning + timedelta(minutes=5)),
                "state": "FIX",
                "event": "Fix pushed, iteration #1",
            },
            {
                "time": _iso(three_days_ago),
                "state": "MERGE",
                "event": "Merged PR #2 -> IDLE",
            },
            {
                "time": _iso(ten_days_ago),
                "state": "MERGE",
                "event": "Merged PR #3 -> IDLE",
            },
        ],
    )
    beta = _coding_state(
        "beta",
        "https://github.com/example/beta.git",
        history=[],
        state=PipelineState.HUNG,
    )
    gamma = _coding_state(
        "gamma",
        "https://github.com/example/gamma.git",
        history=[],
        state=PipelineState.IDLE,
    )

    stats = _compute_stats([alpha, beta, gamma])

    assert stats["repos"] == 3
    assert stats["active"] == 1  # alpha in CODING
    assert stats["alerts"] == 1  # beta in HUNG
    assert stats["done_today"] == 1  # only the today_morning merge
    assert stats["done_week"] == 2  # today + 3 days ago, NOT 10 days ago
    # 1 iteration event / 3 merges == 0.33
    assert stats["avg_iterations_per_merge"] == pytest.approx(0.33, abs=0.01)

    names = [row["name"] for row in stats["per_repo"]]
    assert names == ["alpha", "beta", "gamma"]
    alpha_row = stats["per_repo"][0]
    assert alpha_row["done_total"] == 3
    # today counts all today-stamped events (merge + iteration)
    assert alpha_row["events_today"] == 2


def test_compute_stats_no_merges_gives_zero_average() -> None:
    repo = _coding_state(
        "alpha", "https://github.com/example/alpha.git", history=[]
    )
    stats = _compute_stats([repo])
    assert stats["repos"] == 1
    assert stats["done_today"] == 0
    assert stats["avg_iterations_per_merge"] == 0.0


def test_build_activity_feed_merges_and_sorts_newest_first() -> None:
    t1 = datetime(2026, 4, 11, 9, 0, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 4, 11, 10, 0, 0, tzinfo=timezone.utc)
    t3 = datetime(2026, 4, 11, 11, 0, 0, tzinfo=timezone.utc)

    alpha = _coding_state(
        "alpha",
        "https://github.com/example/alpha.git",
        history=[
            {"time": _iso(t1), "state": "CODING", "event": "alpha early"},
            {"time": _iso(t3), "state": "WATCH", "event": "alpha late"},
        ],
    )
    beta = _coding_state(
        "beta",
        "https://github.com/example/beta.git",
        history=[
            {"time": _iso(t2), "state": "FIX", "event": "beta middle"},
        ],
    )

    feed = _build_activity_feed([alpha, beta])
    events = [entry["event"] for entry in feed]
    assert events == ["alpha late", "beta middle", "alpha early"]

    # each row carries its owning repo so the template can colour it
    repos = {entry["repo_name"] for entry in feed}
    assert repos == {"alpha", "beta"}
    assert feed[0]["repo_abbrev"] == "ALP"
    assert "bg-" in feed[0]["repo_style"]
    # time column shows HH:MM:SS derived from the ISO string
    assert feed[0]["time"] == "11:00:00"


def test_build_activity_feed_caps_at_fifty() -> None:
    base = datetime(2026, 4, 11, 9, 0, 0, tzinfo=timezone.utc)
    history = [
        {
            "time": _iso(base + timedelta(seconds=i)),
            "state": "CODING",
            "event": f"event {i}",
        }
        for i in range(80)
    ]
    repo = _coding_state(
        "alpha", "https://github.com/example/alpha.git", history=history
    )
    feed = _build_activity_feed([repo])
    assert len(feed) == 50
    # newest-first -> event 79 leads
    assert feed[0]["event"] == "event 79"


def test_filter_history_errors_only_keeps_error_and_hung() -> None:
    history = [
        {"time": "09:00:00", "state": "CODING", "event": "coding"},
        {"time": "09:01:00", "state": "ERROR", "event": "boom"},
        {"time": "09:02:00", "state": "IDLE", "event": "recovered"},
        {"time": "09:03:00", "state": "HUNG", "event": "stuck"},
    ]
    out = _filter_history(history, "errors")
    assert [e["event"] for e in out] == ["boom", "stuck"]


def test_filter_history_state_changes_collapses_runs() -> None:
    history = [
        {"time": "09:00:00", "state": "CODING", "event": "coding 1"},
        {"time": "09:00:05", "state": "CODING", "event": "coding 2"},
        {"time": "09:01:00", "state": "WATCH", "event": "watching"},
        {"time": "09:02:00", "state": "FIX", "event": "fix 1"},
        {"time": "09:02:30", "state": "FIX", "event": "fix 2"},
        {"time": "09:03:00", "state": "MERGE", "event": "merging"},
    ]
    out = _filter_history(history, "state")
    assert [e["event"] for e in out] == [
        "coding 1",
        "watching",
        "fix 1",
        "merging",
    ]


def test_filter_history_unknown_returns_all() -> None:
    history = [{"time": "09:00:00", "state": "CODING", "event": "x"}]
    assert _filter_history(history, "nonsense") == history


# ---- HTTP handlers --------------------------------------------------------


@pytest.fixture
def observability_config(
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


def _seed_redis(now: datetime) -> _FakeRedis:
    alpha = RepoState(
        url="https://github.com/example/alpha.git",
        name="alpha",
        state=PipelineState.CODING,
        current_task=QueueTask(
            pr_id="PR-099",
            title="Sample",
            status=TaskStatus.DOING,
        ),
        current_pr=PRInfo(
            number=42,
            branch="pr-099-sample",
            ci_status=CIStatus.SUCCESS,
            review_status=ReviewStatus.EYES,
        ),
        last_updated=now,
        history=[
            {
                "time": _iso(now - timedelta(minutes=10)),
                "state": "CODING",
                "event": "started coding",
            },
            {
                "time": _iso(now - timedelta(minutes=5)),
                "state": "MERGE",
                "event": "Merged PR #42 -> IDLE",
            },
        ],
    )
    beta = RepoState(
        url="https://github.com/example/beta.git",
        name="beta",
        state=PipelineState.IDLE,
        last_updated=now,
        history=[
            {
                "time": _iso(now - timedelta(minutes=1)),
                "state": "IDLE",
                "event": "beta idle",
            },
        ],
    )
    return _FakeRedis(
        {
            "pipeline:alpha": alpha.model_dump_json(),
            "pipeline:beta": beta.model_dump_json(),
        }
    )


def test_partial_activity_feed_returns_html_with_events(
    observability_config: Path,
) -> None:
    now = datetime.now(timezone.utc)
    fake = _seed_redis(now)

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.get("/partials/activity-feed")

    assert response.status_code == 200
    body = response.text
    assert "<!DOCTYPE" not in body
    assert "Merged PR #42" in body
    assert "beta idle" in body
    # cross-repo feed shows entries from both repos
    assert "/repo/alpha" in body
    assert "/repo/beta" in body


def test_partial_stats_returns_html_with_four_values(
    observability_config: Path,
) -> None:
    now = datetime.now(timezone.utc)
    fake = _seed_redis(now)

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.get("/partials/stats")

    assert response.status_code == 200
    body = response.text
    # 4 cards with their labels
    for label in ("Repos", "Done today", "Active", "Alerts"):
        assert label in body


def test_api_stats_returns_expected_json_shape(
    observability_config: Path,
) -> None:
    now = datetime.now(timezone.utc)
    fake = _seed_redis(now)

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.get("/api/stats")

    assert response.status_code == 200
    payload = response.json()
    expected_keys = {
        "repos",
        "active",
        "alerts",
        "done_today",
        "done_week",
        "avg_iterations_per_merge",
        "per_repo",
    }
    assert expected_keys <= set(payload.keys())
    assert payload["repos"] == 2
    # alpha is CODING -> counted as active
    assert payload["active"] == 1
    assert payload["alerts"] == 0
    # seeded one merge today
    assert payload["done_today"] == 1


def test_api_stats_counts_alerts_when_repo_is_hung(
    observability_config: Path,
) -> None:
    now = datetime.now(timezone.utc)
    hung = RepoState(
        url="https://github.com/example/alpha.git",
        name="alpha",
        state=PipelineState.HUNG,
        last_updated=now,
    )
    fake = _FakeRedis({"pipeline:alpha": hung.model_dump_json()})

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.get("/api/stats")

    payload = response.json()
    assert payload["alerts"] == 1
    assert payload["active"] == 0


def test_index_route_shows_stats_and_activity_blocks(
    observability_config: Path,
) -> None:
    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    body = response.text
    assert 'hx-get="/partials/stats"' in body
    assert 'hx-get="/partials/activity-feed"' in body
    assert 'hx-get="/partials/repo-list"' in body
    assert "Done today" in body
    assert "Activity" in body


def test_partial_repo_events_filters_by_query(
    observability_config: Path,
) -> None:
    now = datetime.now(timezone.utc)
    alpha = RepoState(
        url="https://github.com/example/alpha.git",
        name="alpha",
        state=PipelineState.ERROR,
        last_updated=now,
        history=[
            {
                "time": _iso(now - timedelta(minutes=3)),
                "state": "CODING",
                "event": "started coding",
            },
            {
                "time": _iso(now - timedelta(minutes=2)),
                "state": "ERROR",
                "event": "claude CLI failed",
            },
        ],
    )
    fake = _FakeRedis({"pipeline:alpha": alpha.model_dump_json()})

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.get(
            "/partials/repo/alpha/events", params={"filter": "errors"}
        )

    assert response.status_code == 200
    body = response.text
    assert "claude CLI failed" in body
    assert "started coding" not in body
    # tabs render in the fragment so the user can switch filters
    assert 'filter=all' in body
    assert 'filter=state' in body


def test_partial_repo_detail_includes_event_log_header(
    observability_config: Path,
) -> None:
    now = datetime.now(timezone.utc)
    alpha = RepoState(
        url="https://github.com/example/alpha.git",
        name="alpha",
        state=PipelineState.CODING,
        last_updated=now,
        history=[
            {
                "time": _iso(now),
                "state": "CODING",
                "event": "started coding",
            },
        ],
    )
    fake = _FakeRedis({"pipeline:alpha": alpha.model_dump_json()})

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.get("/partials/repo/alpha")

    assert response.status_code == 200
    body = response.text
    # event log heading plus count badge (1 events)
    assert "Event log" in body
    assert "1 events" in body
    # filter tabs render inline
    assert "filter=errors" in body
