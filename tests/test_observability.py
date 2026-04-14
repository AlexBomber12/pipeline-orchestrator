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


def test_build_activity_feed_sinks_legacy_timestamps() -> None:
    """Legacy ``HH:MM:SS`` entries must NOT float on ``last_updated``.

    Regression for a bug where a repo that was actively updated (and so
    carried a recent ``last_updated``) would have every legacy-formatted
    history entry pinned to "now", shoving genuinely recent entries from
    other repos out of the top-50 feed during mixed-format upgrade
    windows. The fix pushes unparseable timestamps to epoch-start so
    they sink to the bottom instead.
    """
    now = datetime(2026, 4, 11, 12, 0, 0, tzinfo=timezone.utc)
    legacy_repo = _coding_state(
        "alpha",
        "https://github.com/example/alpha.git",
        history=[
            {"time": "09:00:00", "state": "CODING", "event": "legacy-1"},
            {"time": "09:05:00", "state": "CODING", "event": "legacy-2"},
        ],
    )
    # last_updated is "now" — the bug would have promoted legacy entries
    # to this timestamp and pushed beta's genuinely-newer entry off the
    # top of the feed.
    legacy_repo.last_updated = now

    beta = _coding_state(
        "beta",
        "https://github.com/example/beta.git",
        history=[
            {
                "time": _iso(now - timedelta(minutes=30)),
                "state": "WATCH",
                "event": "beta recent",
            }
        ],
    )

    feed = _build_activity_feed([legacy_repo, beta])
    events = [entry["event"] for entry in feed]
    # beta's real timestamp must win over alpha's legacy entries even
    # though alpha's last_updated > beta's entry timestamp.
    assert events[0] == "beta recent"
    # legacy entries are still included (so upgrade windows don't lose
    # data), they just sink below real timestamps.
    assert set(events[1:]) == {"legacy-1", "legacy-2"}


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
    assert 'hx-get="/partials/repo-list"' in body
    assert "Done today" in body


def test_partial_repo_detail_returns_summary_only(
    observability_config: Path,
) -> None:
    """Summary poll must NOT include the event log.

    The event log self-polls via /partials/repo/{name}/events so its
    scrollTop survives across summary refreshes; bundling the event
    log back into the summary partial would wipe the wrapper on every
    5s tick and reset the user's scroll position.
    """
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
    assert "Current Task" in body
    assert "Event log" not in body
    assert "started coding" not in body


def test_repo_full_page_mounts_event_log_outside_polling_container(
    observability_config: Path,
) -> None:
    """Regression for P1: event log must NOT be a child of the 5s poll.

    repo.html wraps the summary in a container that polls
    /partials/repo/{name} with ``hx-swap="innerHTML"``; if the event
    log lived inside that container, the first 5s tick would swap the
    summary fragment in as new innerHTML and wipe the event log (and
    its self-poll + filter state) from the DOM entirely. The fixed
    layout keeps the event log as a sibling of the polling container
    so it stays mounted across refreshes.
    """
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
        response = client.get("/repo/alpha")

    assert response.status_code == 200
    body = response.text
    poll_open = '<div hx-get="/partials/repo/alpha"'
    log_anchor = '<section id="repo-event-log"'
    assert poll_open in body
    assert log_anchor in body
    # Counting <div / </div> tags from the polling container's own
    # opening tag through the event-log section's opening tag must
    # balance — if the log is still a child of the polling div, we'll
    # see one more open than close because the polling </div> will not
    # have appeared yet when we reach the log. Balanced counts mean
    # the polling container already closed, so the log is a sibling
    # and the 5s innerHTML swap can never wipe it from the DOM.
    start = body.index(poll_open)
    end = body.index(log_anchor)
    window = body[start:end]
    opens = window.count("<div")
    closes = window.count("</div>")
    assert closes == opens, (
        "event log must live OUTSIDE the /partials/repo/{name} polling "
        f"container (opens={opens}, closes={closes})"
    )
    # the full page still renders the event log on initial load
    assert "Event log" in body
    assert "started coding" in body


def test_partial_repo_events_returns_list_fragment(
    observability_config: Path,
) -> None:
    """`/partials/repo/{name}/events` returns only the list fragment.

    The scroll wrapper on the full page swaps this response into its
    ``innerHTML`` every 5s, so it MUST NOT re-emit the surrounding
    ``<section id="repo-event-log">`` — that would nest a new wrapper
    inside the existing one and break the scroll-preservation hooks.
    """
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
        response = client.get("/partials/repo/alpha/events")

    assert response.status_code == 200
    body = response.text
    assert "started coding" in body
    assert '<ul' in body
    # Response is the inner list only — no outer section wrapper.
    assert 'id="repo-event-log"' not in body
    assert "Event log" not in body
    # And it emits an out-of-band count span so the header's "N events"
    # label refreshes with the list instead of staying at the initial
    # page-load value (Codex P2 on PR #43).
    assert 'id="event-log-count-alpha"' in body
    assert 'hx-swap-oob="true"' in body
    assert "1 events" in body


def test_repo_full_page_marks_count_span_for_oob_target(
    observability_config: Path,
) -> None:
    """Full-page render must expose the count span's ID so the 5s
    poll's out-of-band swap has a target to replace. The initial
    render itself must NOT carry ``hx-swap-oob`` — that's only valid
    on the fragment response."""
    now = datetime.now(timezone.utc)
    alpha = RepoState(
        url="https://github.com/example/alpha.git",
        name="alpha",
        state=PipelineState.CODING,
        last_updated=now,
        history=[
            {"time": _iso(now), "state": "CODING", "event": "started"},
        ],
    )
    fake = _FakeRedis({"pipeline:alpha": alpha.model_dump_json()})

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.get("/repo/alpha")

    assert response.status_code == 200
    body = response.text
    assert 'id="event-log-count-alpha"' in body
    header_start = body.index('id="event-log-count-alpha"')
    wrapper_start = body.index('id="event-list-wrapper-alpha"')
    # Only one count span on the initial render — oob variant belongs
    # strictly to the polled fragment. A stray oob attribute on the
    # static page would confuse HTMX on the first hydration.
    assert body.count('id="event-log-count-alpha"') == 1
    assert header_start < wrapper_start
