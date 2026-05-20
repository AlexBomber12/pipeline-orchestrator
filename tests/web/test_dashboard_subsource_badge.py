"""Tests for the inline subsource badge on the main dashboard repo cards.

The badge surfaces the ERROR cancellation subsource next to the bare
ERROR state label so operators can triage many ERROR repos at a glance
instead of clicking through to each detail page.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.cancellation import CancellationCause
from src.models import PipelineState, QueueTask, RepoState, TaskStatus
from src.web import app as web_app
from src.web.routes import dashboard as dashboard_routes


class _StubAioredisClient:
    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return None

    async def aclose(self) -> None:
        return None


class _StubAioredis:
    @staticmethod
    def from_url(url: str, decode_responses: bool = True) -> _StubAioredisClient:
        return _StubAioredisClient()


@pytest.fixture
def empty_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cfg = tmp_path / "config.yml"
    cfg.write_text("repositories: []\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    return cfg


def _make_error_state(
    name: str = "example__alpha",
    *,
    state: PipelineState = PipelineState.ERROR,
    pr_id: str | None = "PR-1",
) -> RepoState:
    current_task = (
        QueueTask(pr_id=pr_id, title="Sample", status=TaskStatus.DOING)
        if pr_id is not None
        else None
    )
    return RepoState(
        url=f"https://github.com/example/{name}.git",
        name=name,
        state=state,
        current_task=current_task,
        last_updated=datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
    )


def _patch_states(
    monkeypatch: pytest.MonkeyPatch, states: list[RepoState]
) -> None:
    async def fake_get_all_repo_states(_redis=None, _config_path=None):
        return list(states), None

    monkeypatch.setattr(
        web_app, "get_all_repo_states", fake_get_all_repo_states
    )


def _patch_cause(
    monkeypatch: pytest.MonkeyPatch,
    *,
    payload: dict | None = None,
    raise_exc: bool = False,
    cause: CancellationCause | None = None,
) -> None:
    async def fake_get_cause(
        redis_client, repo_slug, task_id, *, refresh_ttl: bool = True
    ):
        if raise_exc:
            raise RuntimeError("redis down")
        if cause is not None:
            return cause
        if payload is None:
            return None
        return CancellationCause(
            category="ERROR",
            payload=payload,
            created_at=datetime.now(timezone.utc).isoformat(),
            task_id=task_id,
            repo_slug=repo_slug,
        )

    monkeypatch.setattr(
        dashboard_routes, "get_cancellation_cause", fake_get_cause
    )


@pytest.fixture(autouse=True)
def _stub_redis(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())


def test_repo_card_shows_subsource_badge_for_known_subsource(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_states(monkeypatch, [_make_error_state()])
    _patch_cause(monkeypatch, payload={"subsource": "fix_iteration_cap"})

    with TestClient(web_app.app) as client:
        response = client.get("/")

    assert response.status_code == 200
    body = response.text
    assert "data-subsource-badge" in body
    assert 'data-subsource="fix_iteration_cap"' in body
    assert "FIX iteration cap" in body


def test_repo_card_shows_severity_high_class_for_crash(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_states(monkeypatch, [_make_error_state()])
    _patch_cause(monkeypatch, payload={"subsource": "crash"})

    with TestClient(web_app.app) as client:
        response = client.get("/")

    body = response.text
    assert 'data-subsource="crash"' in body
    assert "bg-fail/10 text-fail border-fail/30" in body


def test_repo_card_shows_severity_medium_class_for_review_timeout(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_states(monkeypatch, [_make_error_state()])
    _patch_cause(monkeypatch, payload={"subsource": "review_timeout"})

    with TestClient(web_app.app) as client:
        response = client.get("/")

    body = response.text
    assert 'data-subsource="review_timeout"' in body
    assert "bg-warn/10 text-warn border-warn/30" in body


def test_repo_card_shows_severity_low_class_for_operator_reject(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_states(monkeypatch, [_make_error_state()])
    _patch_cause(monkeypatch, payload={"subsource": "operator_reject"})

    with TestClient(web_app.app) as client:
        response = client.get("/")

    body = response.text
    assert 'data-subsource="operator_reject"' in body
    assert "bg-surface-3 text-gray-300 border-white/10" in body


def test_repo_card_shows_raw_subsource_for_unknown_value(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_states(monkeypatch, [_make_error_state()])
    _patch_cause(
        monkeypatch, payload={"subsource": "some_unknown_subsource_value"}
    )

    with TestClient(web_app.app) as client:
        response = client.get("/")

    body = response.text
    assert 'data-subsource="some_unknown_subsource_value"' in body
    assert "some_unknown_subsource_value" in body
    # Unknown subsource falls back to medium severity styling.
    assert "bg-warn/10 text-warn border-warn/30" in body


def test_repo_card_omits_badge_when_not_in_error_state(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_states(
        monkeypatch, [_make_error_state(state=PipelineState.CODING)]
    )
    _patch_cause(monkeypatch, payload={"subsource": "fix_iteration_cap"})

    with TestClient(web_app.app) as client:
        response = client.get("/")

    assert "data-subsource-badge" not in response.text


def test_repo_card_omits_badge_when_subsource_missing(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_states(monkeypatch, [_make_error_state()])
    _patch_cause(monkeypatch, payload={"reason_text": "no subsource here"})

    with TestClient(web_app.app) as client:
        response = client.get("/")

    assert "data-subsource-badge" not in response.text


def test_repo_card_omits_badge_when_cause_missing(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_states(monkeypatch, [_make_error_state()])
    _patch_cause(monkeypatch, payload=None)

    with TestClient(web_app.app) as client:
        response = client.get("/")

    assert "data-subsource-badge" not in response.text


def test_badge_tooltip_renders_recovery_hint(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_states(monkeypatch, [_make_error_state()])
    _patch_cause(monkeypatch, payload={"subsource": "fix_iteration_cap"})

    with TestClient(web_app.app) as client:
        response = client.get("/")

    body = response.text
    assert (
        'title="FIX cycle exceeded fix_iteration_cap iterations.'
        " Revise spec or split.\""
    ) in body


def test_dashboard_context_fetch_handles_redis_error_gracefully(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_states(monkeypatch, [_make_error_state()])
    _patch_cause(monkeypatch, raise_exc=True)

    with TestClient(web_app.app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert "data-subsource-badge" not in response.text


def test_partial_repo_list_renders_badge(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The HTMX poll partial must surface the badge identically to ``/``."""
    _patch_states(monkeypatch, [_make_error_state()])
    _patch_cause(monkeypatch, payload={"subsource": "fix_iteration_cap"})

    with TestClient(web_app.app) as client:
        response = client.get("/partials/repo-list")

    assert response.status_code == 200
    assert 'data-subsource="fix_iteration_cap"' in response.text


@pytest.mark.asyncio
async def test_build_cancellation_subsources_short_circuits_without_redis() -> (
    None
):
    """No Redis means no cause lookup possible — return empty without IO."""
    result = await dashboard_routes._build_cancellation_subsources(
        None, [_make_error_state()]
    )
    assert result == {}


@pytest.mark.asyncio
async def test_build_cancellation_subsources_runs_lookups_concurrently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ERROR repos must be fetched in parallel, not serially — otherwise the
    dashboard latency scales linearly with the ERROR count under Redis RTT."""
    import asyncio

    inflight = 0
    peak = 0

    async def fake_get_cause(
        redis_client, repo_slug, task_id, *, refresh_ttl: bool = True
    ):
        nonlocal inflight, peak
        inflight += 1
        peak = max(peak, inflight)
        try:
            await asyncio.sleep(0.05)
            return CancellationCause(
                category="ERROR",
                payload={"subsource": "fix_iteration_cap"},
                created_at=datetime.now(timezone.utc).isoformat(),
                task_id=task_id,
                repo_slug=repo_slug,
            )
        finally:
            inflight -= 1

    monkeypatch.setattr(
        dashboard_routes, "get_cancellation_cause", fake_get_cause
    )

    states = [
        _make_error_state(name=f"example__r{i}", pr_id=f"PR-{i}")
        for i in range(5)
    ]
    result = await dashboard_routes._build_cancellation_subsources(
        _StubAioredisClient(), states
    )

    assert len(result) == 5
    assert peak == 5


@pytest.mark.asyncio
async def test_build_cancellation_subsources_does_not_refresh_ttl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-345 follow-up: the dashboard cards render on a 30s passive poll
    of ``/partials/repo-list``. Leaving the dashboard open must not
    perpetually pin every ERROR record's TTL to the 90-day forensic
    ceiling, so this helper passes ``refresh_ttl=False`` and reserves the
    default refresh for explicit diagnostic reads.
    """
    captured: list[bool] = []

    async def fake_get_cause(
        redis_client, repo_slug, task_id, *, refresh_ttl: bool = True
    ):
        captured.append(refresh_ttl)
        return CancellationCause(
            category="ERROR",
            payload={"subsource": "fix_iteration_cap"},
            created_at=datetime.now(timezone.utc).isoformat(),
            task_id=task_id,
            repo_slug=repo_slug,
        )

    monkeypatch.setattr(
        dashboard_routes, "get_cancellation_cause", fake_get_cause
    )

    states = [
        _make_error_state(name=f"example__r{i}", pr_id=f"PR-{i}")
        for i in range(3)
    ]
    await dashboard_routes._build_cancellation_subsources(
        _StubAioredisClient(), states
    )

    assert captured == [False, False, False]
