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


@pytest.mark.parametrize(
    "state_value", [PipelineState.CODING, PipelineState.FIX]
)
def test_drain_progress_shown_for_active_coding_or_fix_drain(
    state_value: PipelineState,
) -> None:
    # Pause All flips ``user_paused=True`` but the runner keeps
    # publishing CODING/FIX until the cycle hands off. The card must
    # render the indicator during that active drain window.
    state = _state(state=state_value, current_task=_task())
    drain = {
        state.name: {
            "phase": state_value.value,
            "elapsed_sec": 30.0,
            "est_remaining_sec": 90.0,
        }
    }
    html = _render_cards(repos=[state], drain_progress=drain)
    assert "data-drain-progress" in html
    assert f"Draining: {state_value.value} for 30s" in html


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
async def test_build_drain_progress_estimated_remaining_for_fix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    last_push = started + timedelta(seconds=120)
    now = last_push + timedelta(seconds=600)
    pr = PRInfo(number=1, branch="b", last_activity=started)
    state = _state(
        current_task=_task(),
        current_pr=pr,
        history=[_history_entry(state="FIX", time=started)],
    )
    monkeypatch.setattr(
        "src.web.routes.dashboard.gh_prs.get_pr_last_push_time",
        lambda owner_repo, pr_number: last_push,
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
async def test_build_drain_progress_fix_ignores_pr_updated_activity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Codex commented shortly before Pause All (advancing PR ``updatedAt`` /
    # ``last_activity``) while the head branch had not been pushed for most
    # of the idle window. The card must reflect the supervisor's branch-push
    # anchor (~full window elapsed), not the comment-driven PR activity time
    # which would report a near-fresh window.
    started = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    last_push = started + timedelta(seconds=60)
    recent_comment = started + timedelta(seconds=1770)
    now = started + timedelta(seconds=1780)
    pr = PRInfo(number=1, branch="b", last_activity=recent_comment)
    state = _state(
        current_task=_task(),
        current_pr=pr,
        history=[_history_entry(state="FIX", time=started)],
    )
    monkeypatch.setattr(
        "src.web.routes.dashboard.gh_prs.get_pr_last_push_time",
        lambda owner_repo, pr_number: last_push,
    )
    view = await dashboard_routes._build_drain_progress(
        redis_client=None,
        state=state,
        config=_config(fix_idle_timeout_sec=1800),
        now=now,
    )
    assert view is not None
    assert view["elapsed_sec"] == pytest.approx(1720.0)
    assert view["est_remaining_sec"] == pytest.approx(80.0)


@pytest.mark.asyncio
async def test_build_drain_progress_fix_falls_back_to_history_when_push_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # GitHub activity API returns ``None`` (transient outage, unauthenticated
    # gh, etc.). The helper falls back to the FIX history start so the
    # elapsed reading still works rather than suppressing the indicator.
    started = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    now = started + timedelta(seconds=300)
    pr = PRInfo(number=1, branch="b")
    state = _state(
        current_task=_task(),
        current_pr=pr,
        history=[_history_entry(state="FIX", time=started)],
    )
    monkeypatch.setattr(
        "src.web.routes.dashboard.gh_prs.get_pr_last_push_time",
        lambda owner_repo, pr_number: None,
    )
    view = await dashboard_routes._build_drain_progress(
        redis_client=None,
        state=state,
        config=_config(fix_idle_timeout_sec=1800),
        now=now,
    )
    assert view is not None
    assert view["phase"] == "FIX"
    assert view["elapsed_sec"] == pytest.approx(300.0)


@pytest.mark.asyncio
async def test_build_drain_progress_fix_falls_back_when_push_older_than_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Push is older than the configured idle window — supervisor would
    # reset its baseline to FIX entry under this shape (OBS-DK behavior),
    # so the card matches that by anchoring at FIX history start instead
    # of reporting an "elapsed > timeout" reading driven by a stale push.
    started = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    stale_push = started - timedelta(seconds=3000)
    now = started + timedelta(seconds=120)
    pr = PRInfo(number=1, branch="b")
    state = _state(
        current_task=_task(),
        current_pr=pr,
        history=[_history_entry(state="FIX", time=started)],
    )
    monkeypatch.setattr(
        "src.web.routes.dashboard.gh_prs.get_pr_last_push_time",
        lambda owner_repo, pr_number: stale_push,
    )
    view = await dashboard_routes._build_drain_progress(
        redis_client=None,
        state=state,
        config=_config(fix_idle_timeout_sec=1800),
        now=now,
    )
    assert view is not None
    assert view["elapsed_sec"] == pytest.approx(120.0)
    assert view["est_remaining_sec"] == pytest.approx(1680.0)


@pytest.mark.asyncio
async def test_build_drain_progress_fix_falls_back_when_repo_url_unparseable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A repo registered with a non-GitHub URL (legacy seed, manual import)
    # must not crash the dashboard; the helper falls back to the FIX
    # history anchor and the indicator still renders.
    started = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    now = started + timedelta(seconds=180)
    pr = PRInfo(number=1, branch="b")
    state = RepoState(
        url="not-a-github-url",
        name="octo__demo",
        state=PipelineState.PAUSED,
        user_paused=True,
        current_task=_task(),
        history=[_history_entry(state="FIX", time=started)],
    )
    state.current_pr = pr

    def _should_not_be_called(owner_repo: str, pr_number: int) -> datetime | None:
        raise AssertionError("get_pr_last_push_time must be skipped on bad URL")

    monkeypatch.setattr(
        "src.web.routes.dashboard.gh_prs.get_pr_last_push_time",
        _should_not_be_called,
    )
    view = await dashboard_routes._build_drain_progress(
        redis_client=None,
        state=state,
        config=_config(fix_idle_timeout_sec=1800),
        now=now,
    )
    assert view is not None
    assert view["elapsed_sec"] == pytest.approx(180.0)


@pytest.mark.asyncio
async def test_build_drain_progress_fix_falls_back_when_gh_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # ``gh_prs.get_pr_last_push_time`` runs as a real subprocess in
    # production; any subprocess failure must fail open to the FIX history
    # anchor rather than suppress the indicator.
    started = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    now = started + timedelta(seconds=420)
    pr = PRInfo(number=1, branch="b")
    state = _state(
        current_task=_task(),
        current_pr=pr,
        history=[_history_entry(state="FIX", time=started)],
    )

    def _boom(owner_repo: str, pr_number: int) -> datetime | None:
        raise RuntimeError("gh down")

    monkeypatch.setattr(
        "src.web.routes.dashboard.gh_prs.get_pr_last_push_time", _boom
    )
    view = await dashboard_routes._build_drain_progress(
        redis_client=None,
        state=state,
        config=_config(fix_idle_timeout_sec=1800),
        now=now,
    )
    assert view is not None
    assert view["elapsed_sec"] == pytest.approx(420.0)


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
async def test_build_drain_progress_returns_none_when_rate_limited_until_in_future() -> None:
    # Pause All on a repo that is already PAUSED for a coder rate limit
    # flips ``user_paused=True`` but leaves ``current_task`` and the
    # earlier CODING/FIX history intact. The "Paused, Nm remaining" badge
    # is already covering that wait, so the drain badge must stay silent
    # rather than stacking a second competing indicator on a repo with no
    # in-flight coder process to drain.
    started = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    now = started + timedelta(seconds=60)
    state = _state(
        current_task=_task(),
        history=[_history_entry(state="CODING", time=started)],
    )
    state.rate_limited_until = now + timedelta(minutes=15)
    view = await dashboard_routes._build_drain_progress(
        redis_client=None,
        state=state,
        config=_config(),
        now=now,
    )
    assert view is None


@pytest.mark.asyncio
async def test_build_drain_progress_returns_none_when_rate_limit_marker_expired() -> None:
    # ``handle_paused`` short-circuits on ``user_paused`` and never
    # clears ``rate_limited_until``, so the timestamp lingers in the
    # past on a Pause-All-on-rate-limit-pause repo. The runner is
    # already quiesced behind the manual pause and no coder process is
    # draining; surfacing "Draining: CODING/FIX..." from stale history
    # would mislead the operator.
    started = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    now = started + timedelta(seconds=60)
    state = _state(
        current_task=_task(),
        history=[_history_entry(state="CODING", time=started)],
    )
    state.rate_limited_until = now - timedelta(seconds=1)
    view = await dashboard_routes._build_drain_progress(
        redis_client=None,
        state=state,
        config=_config(),
        now=now,
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
async def test_build_drain_progress_returns_none_when_coding_without_user_pause() -> None:
    # Active CODING without ``user_paused`` is a normal in-flight run,
    # not a drain. The ``user_paused`` gate keeps the helper silent so
    # nothing renders for steady-state coders.
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
async def test_build_drain_progress_active_coding_drain_with_user_paused() -> None:
    # Pause All flips ``user_paused`` but the runner keeps publishing
    # ``state == CODING`` until the cycle naturally hands off. The
    # helper must surface the drain immediately for that window so
    # operators see elapsed/remaining time without waiting for the
    # runner to finally land in PAUSED.
    started = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    now = started + timedelta(seconds=600)
    state = _state(
        state=PipelineState.CODING,
        user_paused=True,
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
    assert view["est_remaining_sec"] == pytest.approx(3000.0)


@pytest.mark.asyncio
async def test_build_drain_progress_active_fix_drain_with_user_paused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Mirror of the CODING active-drain case for FIX: the runner stays
    # in ``state == FIX`` until the fix cycle hands off, so the helper
    # must accept that shape too. Elapsed is anchored to the branch's
    # last-push time — the same signal ``monitor_fix_idle`` uses.
    started = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    last_push = started + timedelta(seconds=120)
    now = last_push + timedelta(seconds=600)
    pr = PRInfo(number=1, branch="b")
    state = _state(
        state=PipelineState.FIX,
        user_paused=True,
        current_task=_task(),
        current_pr=pr,
        history=[_history_entry(state="FIX", time=started)],
    )
    monkeypatch.setattr(
        "src.web.routes.dashboard.gh_prs.get_pr_last_push_time",
        lambda owner_repo, pr_number: last_push,
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
    assert view["est_remaining_sec"] == pytest.approx(1200.0)


@pytest.mark.asyncio
async def test_build_drain_progress_returns_none_for_idle_with_user_paused() -> None:
    # IDLE+user_paused has no in-flight coder cycle to drain — the gate
    # must reject states outside {PAUSED, CODING, FIX}. Otherwise an
    # already-quiesced repo would advertise a phantom drain immediately
    # after Pause All.
    started = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    state = _state(
        state=PipelineState.IDLE,
        user_paused=True,
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
async def test_build_drain_progress_returns_none_for_watch_with_user_paused() -> None:
    # WATCH means the coder already handed off; there is no coder run
    # left to drain, so the gate must reject this shape even with
    # ``user_paused=True``.
    started = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    state = _state(
        state=PipelineState.WATCH,
        user_paused=True,
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
async def test_build_drain_progress_normalizes_naive_push_to_utc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # ``get_pr_last_push_time`` normally returns an aware datetime but a
    # parser edge case could emit a naive one. The FIX branch must coerce
    # it to UTC before subtraction so the elapsed-since-last-push reading
    # works rather than raising on naive/aware arithmetic.
    naive_last_push = datetime(2026, 5, 18, 12, 0, 0)
    now = datetime(2026, 5, 18, 12, 10, 0, tzinfo=timezone.utc)
    pr = PRInfo(number=1, branch="b")
    state = _state(
        current_task=_task(),
        current_pr=pr,
        history=[_history_entry(state="FIX", time=now - timedelta(seconds=1200))],
    )
    monkeypatch.setattr(
        "src.web.routes.dashboard.gh_prs.get_pr_last_push_time",
        lambda owner_repo, pr_number: naive_last_push,
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
async def test_build_drain_progress_map_skips_rate_limited_paused_repos() -> None:
    # Pause All on a repo already PAUSED for a coder rate limit must not
    # produce a drain entry in the map. The "Paused, Nm remaining" badge
    # is already covering the wait; a second indicator would render
    # alongside it and confuse the operator about what is being drained.
    started = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    now = started + timedelta(seconds=60)
    rate_limited = _state(
        repo_name="octo__alpha",
        current_task=_task(pr_id="PR-001"),
        history=[_history_entry(state="CODING", time=started)],
    )
    rate_limited.rate_limited_until = now + timedelta(minutes=15)
    drained = _state(
        repo_name="octo__beta",
        current_task=_task(pr_id="PR-002"),
        history=[_history_entry(state="CODING", time=started)],
    )
    result = await dashboard_routes._build_drain_progress_map(
        redis_client=None,
        states=[rate_limited, drained],
        config=_config(),
        now=now,
    )
    assert set(result) == {"octo__beta"}


@pytest.mark.asyncio
async def test_build_drain_progress_map_empty_when_no_states() -> None:
    result = await dashboard_routes._build_drain_progress_map(
        redis_client=None,
        states=[],
        config=_config(),
    )
    assert result == {}


@pytest.mark.asyncio
async def test_build_drain_progress_map_includes_active_coding_drain() -> None:
    # Pause All flips ``user_paused=True`` but the runner stays in
    # CODING until the cycle hands off. The map must include that repo
    # so the per-card indicator appears during the actual drain window,
    # not only once the runner finally publishes PAUSED.
    started = datetime(2026, 5, 18, 12, 0, 0, tzinfo=timezone.utc)
    active = _state(
        repo_name="octo__alpha",
        state=PipelineState.CODING,
        user_paused=True,
        current_task=_task(pr_id="PR-001"),
        history=[_history_entry(state="CODING", time=started)],
    )
    parked = _state(
        repo_name="octo__beta",
        state=PipelineState.PAUSED,
        user_paused=True,
        current_task=_task(pr_id="PR-002"),
        history=[_history_entry(state="FIX", time=started)],
    )
    result = await dashboard_routes._build_drain_progress_map(
        redis_client=None,
        states=[active, parked],
        config=_config(),
        now=started + timedelta(seconds=60),
    )
    assert set(result) == {"octo__alpha", "octo__beta"}
    assert result["octo__alpha"]["phase"] == "CODING"
    assert result["octo__beta"]["phase"] == "FIX"
