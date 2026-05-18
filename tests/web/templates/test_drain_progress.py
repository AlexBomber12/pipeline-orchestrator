"""Tests for the per-card drain-progress indicator (PR-341).

The dashboard surfaces a one-line "Draining: <phase> for Ns, est ~Mm
remaining" badge on each PAUSED repo card whose runner was caught
mid-CODING or mid-FIX by an operator Pause All. The view data is
computed in ``_build_drain_progress`` / ``_build_drain_progress_map``
and rendered by ``components/repo_cards.html``; tests cover both
layers so a regression in either fails the gate.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from src.config import AppConfig, DaemonConfig
from src.models import (
    PipelineState,
    PRInfo,
    QueueTask,
    RepoState,
    TaskStatus,
)
from src.web.app import templates
from src.web.routes import dashboard as dashboard_routes


def _config(
    *,
    planned_pr_timeout_sec: int = 3600,
    fix_idle_timeout_sec: int = 1800,
) -> AppConfig:
    daemon = DaemonConfig(
        planned_pr_timeout_sec=planned_pr_timeout_sec,
        fix_idle_timeout_sec=fix_idle_timeout_sec,
    )
    return AppConfig(repositories=[], daemon=daemon)


def _state(
    *,
    repo_name: str = "octo__demo",
    state: PipelineState = PipelineState.PAUSED,
    user_paused: bool = True,
    current_task: QueueTask | None = None,
    current_pr: PRInfo | None = None,
    history: list[dict[str, Any]] | None = None,
) -> RepoState:
    state_obj = RepoState(
        url=f"https://github.com/octo/{repo_name.split('__')[-1]}.git",
        name=repo_name,
        state=state,
        user_paused=user_paused,
        current_task=current_task,
        history=history or [],
    )
    if current_pr is not None:
        state_obj.current_pr = current_pr
    return state_obj


def _task(pr_id: str = "PR-001") -> QueueTask:
    return QueueTask(
        pr_id=pr_id,
        title=f"Title for {pr_id}",
        status=TaskStatus.DOING,
        branch=f"branch-{pr_id.lower()}",
    )


def _history_entry(
    *, state: str, time: datetime, event: str = ""
) -> dict[str, Any]:
    return {
        "time": time.isoformat(),
        "state": state,
        "event": event,
        "count": 1,
        "last_seen_at": time.isoformat(),
    }


def _render_cards(
    *,
    repos: list[RepoState],
    drain_progress: dict[str, dict[str, Any]] | None = None,
) -> str:
    """Render ``components/repo_cards.html`` for the given repos."""
    return templates.get_template("components/repo_cards.html").render(
        repos=repos,
        resources=None,
        cancellation_subsources={},
        subsource_lookup=lambda _name: None,
        drain_progress=drain_progress or {},
        css_escape=lambda v: v,
        upload_feedback_target=lambda _name: "",
        utcnow=lambda: datetime.now(timezone.utc),
    )


# ---------------------------------------------------------------------------
# Template rendering
# ---------------------------------------------------------------------------


def test_drain_progress_shown_for_paused_with_coding_phase() -> None:
    state = _state(
        current_task=_task(),
        history=[
            _history_entry(
                state="CODING", time=datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
            ),
            _history_entry(
                state="PAUSED", time=datetime(2026, 5, 18, 12, 1, 0, tzinfo=timezone.utc)
            ),
        ],
    )
    drain = {state.name: {"phase": "CODING", "elapsed_sec": 120.0, "est_remaining_sec": 540.0}}
    html = _render_cards(repos=[state], drain_progress=drain)
    assert "data-drain-progress" in html
    assert "Draining: CODING for 120s" in html
    assert "est ~9 min remaining" in html


def test_drain_progress_shown_for_paused_with_fix_phase() -> None:
    state = _state(
        current_task=_task(),
        history=[
            _history_entry(
                state="FIX", time=datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
            ),
        ],
    )
    drain = {state.name: {"phase": "FIX", "elapsed_sec": 45.0, "est_remaining_sec": 60.0}}
    html = _render_cards(repos=[state], drain_progress=drain)
    assert "Draining: FIX for 45s" in html
    assert "est ~1 min remaining" in html


def test_drain_progress_hidden_for_paused_without_current_task() -> None:
    state = _state(current_task=None, history=[])
    # Even if a stale entry exists in the map, the gate should still hide
    # the badge for repos with no current_task — view-layer omitting the
    # entry is the canonical guard, but the template must also degrade
    # gracefully when the map is empty.
    html = _render_cards(repos=[state], drain_progress={})
    assert "data-drain-progress" not in html
    assert "Draining" not in html


@pytest.mark.parametrize(
    "state_value",
    [PipelineState.IDLE, PipelineState.CODING, PipelineState.WATCH, PipelineState.FIX],
)
def test_drain_progress_hidden_for_non_paused_states(state_value: PipelineState) -> None:
    state = _state(
        state=state_value,
        user_paused=False,
        current_task=_task(),
        history=[],
    )
    html = _render_cards(repos=[state], drain_progress={})
    assert "data-drain-progress" not in html
    assert "Draining" not in html


def test_drain_progress_omits_remaining_when_phase_unknown() -> None:
    # When the view-layer cannot estimate a remaining time (e.g. timeout
    # config produces ``None`` for ``est_remaining_sec``), the indicator
    # still renders the elapsed counter but suppresses the "est ~N min
    # remaining" suffix so the operator does not see a misleading "0 min".
    state = _state(current_task=_task(), history=[])
    drain = {state.name: {"phase": "CODING", "elapsed_sec": 30.0, "est_remaining_sec": None}}
    html = _render_cards(repos=[state], drain_progress=drain)
    assert "Draining: CODING for 30s" in html
    assert "est" not in html.split("data-drain-progress", 1)[1].split("</div>", 1)[0]


# ---------------------------------------------------------------------------
# View-layer helper
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_build_drain_progress_estimated_remaining_for_coding() -> None:
    started = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    now = started + timedelta(seconds=600)
    state = _state(
        current_task=_task(),
        history=[_history_entry(state="CODING", time=started)],
    )
    view = await dashboard_routes._build_drain_progress(
        redis_client=None,
        state=state,
        config=_config(planned_pr_timeout_sec=3600),
        now=now,
    )
    assert view is not None
    assert view["phase"] == "CODING"
    assert view["elapsed_sec"] == pytest.approx(600.0)
    # 3600 - 600 = 3000 sec = 50 minutes
    assert view["est_remaining_sec"] == pytest.approx(3000.0)


@pytest.mark.asyncio
async def test_build_drain_progress_estimated_remaining_for_fix() -> None:
    started = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    last_push = started + timedelta(seconds=120)
    now = last_push + timedelta(seconds=600)
    pr = PRInfo(number=1, branch="b", last_activity=last_push)
    state = _state(
        current_task=_task(),
        current_pr=pr,
        history=[_history_entry(state="FIX", time=started)],
    )
    view = await dashboard_routes._build_drain_progress(
        redis_client=None,
        state=state,
        config=_config(fix_idle_timeout_sec=1800),
        now=now,
    )
    assert view is not None
    assert view["phase"] == "FIX"
    assert view["elapsed_sec"] == pytest.approx(600.0)
    # 1800 - 600 = 1200 sec = 20 minutes
    assert view["est_remaining_sec"] == pytest.approx(1200.0)


@pytest.mark.asyncio
async def test_build_drain_progress_clamps_remaining_at_zero() -> None:
    started = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    # Elapsed (4000s) exceeds the planned PR timeout (3600s) by 400s; the
    # remaining clamps at zero rather than rendering a negative value.
    now = started + timedelta(seconds=4000)
    state = _state(
        current_task=_task(),
        history=[_history_entry(state="CODING", time=started)],
    )
    view = await dashboard_routes._build_drain_progress(
        redis_client=None,
        state=state,
        config=_config(planned_pr_timeout_sec=3600),
        now=now,
    )
    assert view is not None
    assert view["est_remaining_sec"] == 0.0


@pytest.mark.asyncio
async def test_build_drain_progress_returns_none_without_user_pause() -> None:
    # Rate-limited PAUSED (user_paused=False) keeps the existing "Paused,
    # Nm remaining" indicator; this helper must not produce a competing
    # drain badge for that case.
    started = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    state = _state(
        user_paused=False,
        current_task=_task(),
        history=[_history_entry(state="CODING", time=started)],
    )
    view = await dashboard_routes._build_drain_progress(
        redis_client=None,
        state=state,
        config=_config(),
        now=started + timedelta(seconds=60),
    )
    assert view is None


@pytest.mark.asyncio
async def test_build_drain_progress_returns_none_when_history_lacks_coding_or_fix() -> None:
    started = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    state = _state(
        current_task=_task(),
        history=[_history_entry(state="WATCH", time=started)],
    )
    view = await dashboard_routes._build_drain_progress(
        redis_client=None,
        state=state,
        config=_config(),
        now=started + timedelta(seconds=60),
    )
    assert view is None


@pytest.mark.asyncio
async def test_build_drain_progress_returns_none_when_not_paused() -> None:
    started = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    state = _state(
        state=PipelineState.CODING,
        user_paused=False,
        current_task=_task(),
        history=[_history_entry(state="CODING", time=started)],
    )
    view = await dashboard_routes._build_drain_progress(
        redis_client=None,
        state=state,
        config=_config(),
        now=started + timedelta(seconds=60),
    )
    assert view is None


@pytest.mark.asyncio
async def test_build_drain_progress_returns_none_when_no_current_task() -> None:
    started = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    state = _state(
        current_task=None,
        history=[_history_entry(state="CODING", time=started)],
    )
    view = await dashboard_routes._build_drain_progress(
        redis_client=None,
        state=state,
        config=_config(),
        now=started + timedelta(seconds=60),
    )
    assert view is None


@pytest.mark.asyncio
async def test_build_drain_progress_uses_redis_marker_when_present() -> None:
    started = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    now = started + timedelta(seconds=300)

    class _StubRedis:
        async def get(self, key: str) -> str:
            # Mirrors the format ``record_current_run_started_at`` writes.
            assert key == "current_run_started_at:octo__demo:PR-001"
            return started.isoformat()

    state = _state(
        current_task=_task(),
        # History recorded the CODING entry well before ``started`` — the
        # Redis marker must win so the elapsed reading matches the actual
        # dispatch timestamp instead of the older history entry.
        history=[
            _history_entry(
                state="CODING",
                time=started - timedelta(seconds=900),
            ),
        ],
    )
    view = await dashboard_routes._build_drain_progress(
        redis_client=_StubRedis(),
        state=state,
        config=_config(planned_pr_timeout_sec=3600),
        now=now,
    )
    assert view is not None
    assert view["elapsed_sec"] == pytest.approx(300.0)


@pytest.mark.asyncio
async def test_build_drain_progress_falls_back_to_history_on_redis_error() -> None:
    # Redis transient outage must not break the indicator; the helper
    # falls back to the most recent CODING entry in history so the
    # elapsed reading still works without the dispatch-time marker.
    started = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)

    class _RaisingRedis:
        async def get(self, key: str) -> str:
            raise RuntimeError("redis down")

    state = _state(
        current_task=_task(),
        history=[_history_entry(state="CODING", time=started)],
    )
    view = await dashboard_routes._build_drain_progress(
        redis_client=_RaisingRedis(),
        state=state,
        config=_config(),
        now=started + timedelta(seconds=300),
    )
    assert view is not None
    assert view["phase"] == "CODING"
    assert view["elapsed_sec"] == pytest.approx(300.0)


@pytest.mark.asyncio
async def test_build_drain_progress_returns_none_when_history_time_unparseable() -> None:
    # When the Redis marker is missing AND every CODING history entry has
    # a non-ISO ``time`` value (legacy ``HH:MM:SS`` payloads), the helper
    # cannot derive a start anchor and must return None rather than
    # rendering a misleading 0s indicator.
    state = _state(
        current_task=_task(),
        history=[
            {
                "state": "CODING",
                "time": "12:00:00",  # legacy clock-only format
                "event": "",
                "count": 1,
                "last_seen_at": "12:00:00",
            }
        ],
    )
    view = await dashboard_routes._build_drain_progress(
        redis_client=None,
        state=state,
        config=_config(),
        now=datetime(2026, 5, 18, 12, 1, 0, tzinfo=timezone.utc),
    )
    assert view is None


@pytest.mark.asyncio
async def test_build_drain_progress_normalizes_naive_last_activity_to_utc() -> None:
    # ``current_pr.last_activity`` is typically aware but a legacy payload
    # may carry a naive datetime. The FIX branch must coerce it to UTC
    # before subtraction so the elapsed-since-last-push reading works.
    naive_last_push = datetime(2026, 5, 18, 12, 0, 0)
    now = datetime(2026, 5, 18, 12, 10, 0, tzinfo=timezone.utc)
    pr = PRInfo(number=1, branch="b", last_activity=naive_last_push)
    state = _state(
        current_task=_task(),
        current_pr=pr,
        history=[_history_entry(state="FIX", time=now - timedelta(seconds=1200))],
    )
    view = await dashboard_routes._build_drain_progress(
        redis_client=None,
        state=state,
        config=_config(fix_idle_timeout_sec=1800),
        now=now,
    )
    assert view is not None
    assert view["phase"] == "FIX"
    assert view["elapsed_sec"] == pytest.approx(600.0)


@pytest.mark.asyncio
async def test_build_drain_progress_map_skips_repos_without_drain() -> None:
    # Two PAUSED+user_paused repos: only the one with a CODING/FIX history
    # entry shows up in the result map. The other is filtered out so the
    # template can safely call ``drain_progress.get(repo.name)`` without
    # having to redo the gate logic.
    started = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    drained = _state(
        repo_name="octo__alpha",
        current_task=_task(pr_id="PR-001"),
        history=[_history_entry(state="CODING", time=started)],
    )
    not_drained = _state(
        repo_name="octo__beta",
        current_task=_task(pr_id="PR-002"),
        history=[_history_entry(state="WATCH", time=started)],
    )
    result = await dashboard_routes._build_drain_progress_map(
        redis_client=None,
        states=[drained, not_drained],
        config=_config(),
        now=started + timedelta(seconds=60),
    )
    assert set(result) == {"octo__alpha"}
    assert result["octo__alpha"]["phase"] == "CODING"


@pytest.mark.asyncio
async def test_build_drain_progress_map_empty_when_no_states() -> None:
    result = await dashboard_routes._build_drain_progress_map(
        redis_client=None,
        states=[],
        config=_config(),
    )
    assert result == {}
