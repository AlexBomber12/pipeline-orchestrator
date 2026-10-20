"""Account-level quota observations belong in settings, with no provider calls."""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from src.daemon.github_rate_limit import BUDGET_GRAPHQL_REDIS_KEY, BUDGET_REST_REDIS_KEY
from src.keyspace import coder_usage
from src.usage import UsageObservation, UsageSnapshot
from src.web import app as web_app
from src.web.services.usage import usage_settings_context

from tests.test_web_app import _FakeRedis, _make_bucket_redis


def observation(*, status="available", age=0, session=0, weekly=47, reset=None):
    now = time.time()
    return UsageObservation(
        status=status, checked_at=now, last_successful_at=now-age,
        snapshot=UsageSnapshot(session, reset or int(now+600), weekly, int(now+900), now-age)
        if status == "available" else None,
    ).model_dump_json()


def context(redis, names=None):
    return asyncio.run(usage_settings_context(redis, names or ["codex"], 60))


def test_account_observation_uses_fetch_time_and_preserves_zero():
    redis = _FakeRedis(store={coder_usage("codex"): observation()})
    view = context(redis)["coder_usage"]["codex"]
    assert view["status"] == "available"
    assert view["windows"][0]["percent"] == 0
    assert view["windows"][1]["percent"] == 47
    assert view["updated_at"].endswith("UTC")


@pytest.mark.parametrize("age", [181, -61])
def test_old_or_future_observation_is_never_current(age):
    redis = _FakeRedis(store={coder_usage("codex"): observation(age=age)})
    assert context(redis)["coder_usage"]["codex"]["status"] == "stale"


def test_expired_window_has_no_current_counter():
    redis = _FakeRedis(store={coder_usage("codex"): observation(weekly=None, reset=int(time.time()-1))})
    window = context(redis)["coder_usage"]["codex"]["windows"][0]
    assert window["percent"] is None
    assert window["reset_at"] is None


def test_missing_window_is_not_a_zero_counter():
    redis = _FakeRedis(store={coder_usage("codex"): observation(session=None)})
    assert [w["label"] for w in context(redis)["coder_usage"]["codex"]["windows"]] == ["Weekly"]


@pytest.mark.parametrize("raw", [None, "{broken", "x"*16385])
def test_missing_or_invalid_observation_is_unknown(raw):
    redis = _FakeRedis(store={coder_usage("codex"): raw})
    view = context(redis)["coder_usage"]["codex"]
    assert view["supported"] is None
    assert view["windows"] == []


def test_redis_outage_and_absence_are_read_only_degradation():
    class Broken:
        async def get(self, key):
            raise RuntimeError("offline")
    for redis in [None, Broken()]:
        view = context(redis)
        assert view["coder_usage"]["codex"]["status"] == "unavailable"
        assert all(x["status"] == "unavailable" for x in view["github_usage"])


def test_plugin_without_quota_has_no_usage_controls():
    redis = _FakeRedis(store={coder_usage("local"): observation(status="unsupported")})
    data = context(redis, ["local"])
    html = web_app.templates.get_template("components/settings_coder_usage.html").render(
        coder=SimpleNamespace(name="local", display_name="Local"), **data,
    )
    assert 'data-coder-usage="local"' in html
    assert "Current provider data" not in html
    assert "% used" not in html
    assert "Usage data unavailable" not in html


def test_unavailable_provider_preserves_last_update_without_values():
    redis = _FakeRedis(store={coder_usage("codex"): observation(status="unavailable")})
    data = context(redis)
    html = web_app.templates.get_template("components/settings_coder_usage.html").render(
        coder=SimpleNamespace(name="codex", display_name="Codex"), **data,
    )
    assert "Usage data unavailable" in html
    assert "Last successful update:" in html
    assert "% used" not in html


def test_github_budgets_are_independent_and_expired_data_is_unknown():
    redis = _make_bucket_redis(rest_remaining=4500, graphql_remaining=1000)
    budgets = context(redis)["github_usage"]
    assert budgets[0]["remaining"] == 4500
    assert budgets[1]["remaining"] == 1000
    from datetime import datetime, timedelta, timezone
    redis = _make_bucket_redis(reset_at=datetime.now(timezone.utc)-timedelta(seconds=1))
    assert all(x["remaining"] is None for x in context(redis)["github_usage"])


def test_initial_settings_and_read_only_refresh_render_same_observations(tmp_path, monkeypatch):
    (tmp_path / "config.yml").write_text("repositories: []\n")
    monkeypatch.chdir(tmp_path)
    redis = _make_bucket_redis(rest_remaining=4500, graphql_remaining=1000)
    redis.store[coder_usage("codex")] = observation()
    redis.store[coder_usage("claude")] = observation(status="unsupported")
    monkeypatch.setattr(web_app, "aioredis", SimpleNamespace(from_url=lambda *a, **kw: redis))
    with TestClient(web_app.app) as client:
        initial = client.get("/settings")
        refresh = client.get("/partials/settings/usage")
    assert initial.status_code == refresh.status_code == 200
    assert "4500 / 5000 remaining" in initial.text
    assert 'id="settings-github"' in initial.text
    assert 'hx-get="/partials/settings/usage"' in initial.text
    for html in [initial.text, refresh.text]:
        assert "0% used" in html
        assert "47% used" in html
        assert 'data-coder-usage="codex"' in html
    assert 'hx-swap-oob="outerHTML"' in refresh.text
    assert BUDGET_REST_REDIS_KEY in redis.store
    assert BUDGET_GRAPHQL_REDIS_KEY in redis.store


def test_actual_github_slowdown_remains_visible_without_dashboard_counters():
    from src.inhibitor import InhibitorType, WorkInhibitor
    from src.models import PipelineState

    from tests.web.routes.test_inhibitor_ui import _render_card, _state

    repo = _state(state=PipelineState.WATCH, active_inhibitors=[WorkInhibitor(
        inhibitor_type=InhibitorType.GITHUB_BUDGET_SLOWDOWN,
        reason_text="GitHub polling is slowed until reset", source_key="test:budget",
    )])
    html = _render_card(repo)
    assert "GitHub polling is slowed until reset" in html
    assert '/settings#settings-github' in html
    assert 'data-resource-chips' not in html
