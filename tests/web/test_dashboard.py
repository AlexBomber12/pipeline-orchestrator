"""Tests for the dashboard cascade-panic banner (PR-308b)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.keyspace import daemon_panic_state
from src.web import app as web_app
from src.web.app import app
from src.web.routes import dashboard as dashboard_routes


class _DashboardRedis:
    """In-memory Redis stub serving the dashboard's panic-state read."""

    def __init__(self, store: dict[str, str] | None = None) -> None:
        self.store: dict[str, str] = dict(store or {})

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def aclose(self) -> None:
        return None


def _aioredis(redis_client: _DashboardRedis) -> object:
    return type(
        "_Aioredis",
        (),
        {"from_url": staticmethod(lambda url, decode_responses=True: redis_client)},
    )()


@pytest.fixture
def empty_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cfg = tmp_path / "config.yml"
    cfg.write_text("repositories: []\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    return cfg


def test_panic_banner_renders_when_state_enabled(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    triggered_at = datetime(2026, 5, 15, 11, 30, tzinfo=timezone.utc).isoformat()
    payload = {
        "enabled": True,
        "reason": "cascade_escalate_threshold_exceeded",
        "triggered_at": triggered_at,
        "affected_repos": ["example__alpha", "example__beta", "example__gamma"],
        "threshold_at_trigger": 3,
    }
    redis_client = _DashboardRedis({daemon_panic_state(): json.dumps(payload)})
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    body = response.text
    assert 'id="cascade-panic-banner"' in body
    assert "CASCADE PANIC" in body
    assert "cascade_escalate_threshold_exceeded" in body
    assert "example__alpha" in body
    assert 'hx-post="/daemon/panic/resume"' in body


def test_panic_banner_omits_when_state_absent(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    redis_client = _DashboardRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert 'id="cascade-panic-banner"' not in response.text


def test_panic_banner_omits_when_state_disabled(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An ``enabled: false`` record must render no banner."""
    payload = {"enabled": False, "reason": "manual_resume"}
    redis_client = _DashboardRedis({daemon_panic_state(): json.dumps(payload)})
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.get("/")

    assert 'id="cascade-panic-banner"' not in response.text


@pytest.mark.asyncio
async def test_read_panic_state_for_banner_handles_bytes_payload() -> None:
    """Bytes-mode Redis returns must decode cleanly into the banner dict."""

    class _BytesRedis:
        async def get(self, key: str) -> bytes:
            return b'{"enabled": true, "reason": "x"}'

    parsed = await dashboard_routes._read_panic_state_for_banner(_BytesRedis())
    assert parsed == {"enabled": True, "reason": "x"}


@pytest.mark.asyncio
async def test_read_panic_state_for_banner_returns_none_for_non_utf8_bytes() -> None:
    """Undecodable bytes must be treated as absent, not raise out of ``index``."""

    class _NonUtf8Redis:
        async def get(self, key: str) -> bytes:
            return b"\xff\xfe\x00\x00"

    assert (
        await dashboard_routes._read_panic_state_for_banner(_NonUtf8Redis())
        is None
    )


@pytest.mark.asyncio
async def test_read_panic_state_for_banner_returns_none_for_invalid_json() -> None:
    class _BrokenJsonRedis:
        async def get(self, key: str) -> str:
            return "{not json"

    assert (
        await dashboard_routes._read_panic_state_for_banner(_BrokenJsonRedis())
        is None
    )


@pytest.mark.asyncio
async def test_read_panic_state_for_banner_returns_none_when_not_dict() -> None:
    class _ListRedis:
        async def get(self, key: str) -> str:
            return "[1, 2, 3]"

    assert (
        await dashboard_routes._read_panic_state_for_banner(_ListRedis())
        is None
    )


@pytest.mark.asyncio
async def test_read_panic_state_for_banner_swallows_redis_errors() -> None:
    class _BoomRedis:
        async def get(self, key: str) -> str:
            raise RuntimeError("redis down")

    assert (
        await dashboard_routes._read_panic_state_for_banner(_BoomRedis())
        is None
    )


@pytest.mark.asyncio
async def test_read_panic_state_for_banner_returns_none_without_redis() -> None:
    assert await dashboard_routes._read_panic_state_for_banner(None) is None


class _SummaryRedis(_DashboardRedis):
    """Controlled Redis transport for the real MetricsStore reader/writer."""

    def __init__(self, store=None):
        super().__init__(store)
        self.lists = {}

    async def set(self, key, value, **kwargs):
        self.store[key] = value

    async def sadd(self, *args):
        pass

    async def lrem(self, key, count, value):
        self.lists[key] = [v for v in self.lists.get(key, []) if v != value]

    async def lpush(self, key, value):
        self.lists.setdefault(key, []).insert(0, value)

    async def ltrim(self, key, start, end):
        self.lists[key] = self.lists[key][start:end + 1]

    async def lrange(self, key, start, end):
        return self.lists.get(key, [])[start:end + 1]


def _summary_state(**changes):
    from src.models import RepoState
    return RepoState(name="example__alpha", url="https://github.com/example/alpha", **changes)


async def _summary(state, redis=None):
    if redis is None:
        redis = _SummaryRedis({f"pipeline:{state.name}": state.model_dump_json()})
    return await dashboard_routes._work_summary(state, redis, stale_after=600)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage, expected", [
    ("CODING", "observe publication"), ("FIX", "observe the fix"),
    ("WATCH", "current-change review"), ("MERGE", "verify the merge"),
    ("IDLE", "next eligible task"), ("PREFLIGHT", "recheck prerequisites"),
    ("PAUSED", "resource availability"), ("ERROR", "choosing Retry"),
])
async def test_work_summary_stages(stage, expected):
    state = _summary_state(state=stage)
    view = await _summary(state)
    assert expected in view["next_step"]
    assert view["freshness"] == "current"
    assert view["source_time"] == state.last_updated
    assert "Unknown" in view["activity"]
    if stage == "WATCH":
        assert "Waiting for CI" in view["reason"]


@pytest.mark.asyncio
async def test_work_summary_disabled_repository_requires_enabling():
    # Use the published flag, even if the caller's local state is still active.
    state = _summary_state(state="IDLE")
    disabled = state.model_copy(update={"active": False})
    redis = _SummaryRedis({f"pipeline:{state.name}": disabled.model_dump_json()})
    view = await _summary(state, redis)
    assert view["freshness"] == "current"
    assert view["reason"] == "This repository is disabled."
    assert view["next_step"] == "Enable this repository in Settings before work can continue."
    assert "automatically" not in view["next_step"]
    # Enabling restores the normal automatic next step.
    assert "next eligible task automatically" in (await _summary(state))["next_step"]


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["IDLE", "WATCH"])
@pytest.mark.parametrize("reason_text", ["GitHub API budget low", ""])
async def test_work_summary_github_slowdown_is_informational(stage, reason_text):
    from datetime import timedelta
    from src.inhibitor import WorkInhibitor

    state = _summary_state(state=stage)
    normal = await _summary(state)
    slowdown = WorkInhibitor(
        inhibitor_type="github_budget_slowdown", source_key="budget",
        reason_text=reason_text,
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    state.active_inhibitors = [slowdown]
    view = await _summary(state)
    assert view["next_step"] == normal["next_step"]
    assert view["reason"] == "; ".join(filter(None, (
        normal["reason"], reason_text or "GitHub slowdown",
    )))

    # An unknown slowdown reset must not change a real blocker's reset guidance.
    state.active_inhibitors = [
        slowdown.model_copy(update={"expires_at": None}),
        WorkInhibitor(
            inhibitor_type="github_budget_pause", source_key="pause",
            reason_text="GitHub budget exhausted", expires_at=slowdown.expires_at,
        ),
    ]
    blocked = await _summary(state)
    assert blocked["next_step"] == "The daemon will recheck resources after the reported reset."
    assert "GitHub budget exhausted" in blocked["reason"]
    assert (reason_text or "GitHub slowdown") in blocked["reason"]

    state.active_inhibitors = [slowdown.model_copy(update={
        "expires_at": datetime.now(timezone.utc) - timedelta(seconds=1),
    })]
    assert (await _summary(state))["reason"] == normal["reason"]


@pytest.mark.asyncio
@pytest.mark.parametrize("inhibitor_type", ["rate_limit", "spend_ceiling"])
@pytest.mark.parametrize("has_expiry", [True, False])
async def test_work_summary_watch_coder_limits_preserve_progress(inhibitor_type, has_expiry):
    from datetime import timedelta

    from src.inhibitor import WorkInhibitor
    from src.models import PipelineState

    state = _summary_state(state="WATCH")
    normal = await _summary(state)
    expiry = datetime.now(timezone.utc) + timedelta(hours=1) if has_expiry else None
    state.active_inhibitors = [WorkInhibitor(
        inhibitor_type=inhibitor_type, source_key="coder-limit",
        reason_text="Coder quota exhausted", expires_at=expiry,
    )]
    view = await _summary(state)
    assert view["freshness"] == "current"
    assert view["next_step"] == normal["next_step"]
    assert "before merging" in view["next_step"]
    assert view["reason"] == normal["reason"] + "; Coder quota exhausted"

    # Real WATCH blockers still override progress, with their own reset guidance.
    state.active_inhibitors.append(WorkInhibitor(
        inhibitor_type="github_budget_pause", source_key="budget",
        reason_text="GitHub budget exhausted",
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    ))
    blocked = await _summary(state)
    assert blocked["next_step"] == "The daemon will recheck resources after the reported reset."
    assert "GitHub budget exhausted" in blocked["reason"]
    assert "Coder quota exhausted" in blocked["reason"]

    # Coder dispatch in IDLE remains subject to the same resource limits.
    state.active_inhibitors.pop()
    state.state = PipelineState.IDLE
    dispatch = await _summary(state)
    assert "recheck resources" in dispatch["next_step"]
    assert dispatch["reason"] == "Coder quota exhausted"


@pytest.mark.asyncio
async def test_work_summary_resource_and_operator_waits():
    from datetime import timedelta
    from src.inhibitor import WorkInhibitor

    state = _summary_state(state="PAUSED", active_inhibitors=[WorkInhibitor(
        inhibitor_type="rate_limit", source_key="private-key", reason_text="Quota exhausted",
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )])
    view = await _summary(state)
    assert view["reason"] == "Quota exhausted"
    assert "recheck resource" in view["next_step"]
    state.active_inhibitors[0] = state.active_inhibitors[0].model_copy(update={"expires_at": None})
    assert "reset time is unknown" in (await _summary(state))["next_step"]
    state.user_paused = True
    assert "Choose Resume" in (await _summary(state))["next_step"]
    state.user_paused = False
    state.active_inhibitors = [WorkInhibitor(
        inhibitor_type="user_pause", source_key="pause",
    )]
    assert "Operator paused" in (await _summary(state))["reason"]
    state.active_inhibitors = []
    state.rate_limited_until = datetime.now(timezone.utc) + timedelta(hours=1)
    assert "rate limit" in (await _summary(state))["reason"]


@pytest.mark.asyncio
@pytest.mark.parametrize("raw, freshness", [
    (None, "unavailable"), ("broken", "unavailable"),
    ('{}', "unavailable"),
    ('{"name":"other","url":"other","last_updated":"2020-01-01"}', "unavailable"),
    ("stale", "stale"), ("future", "unknown (clock skew)"),
])
async def test_work_summary_source_is_not_liveness(raw, freshness):
    from datetime import timedelta
    state = _summary_state(state="CODING")
    if raw in ("stale", "future"):
        state.last_updated += timedelta(days=-1 if raw == "stale" else 1)
        raw = state.model_dump_json()
    view = await _summary(state, _SummaryRedis({f"pipeline:{state.name}": raw}))
    assert view["freshness"] == freshness
    assert "unconfirmed" in view["next_step"]
    assert "Unknown" in view["activity"]


@pytest.mark.asyncio
async def test_work_summary_down_and_missing_results():
    assert (await dashboard_routes._work_summary(
        _summary_state(), None, stale_after=600,
    ))["results_available"] is False
    assert (await _summary(_summary_state()))["result"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome, cause", [("merged", None), ("failed", "CRASH")])
async def test_durable_result_survives_rollover_and_reader_restart(outcome, cause):
    from src.metrics import MetricsStore, RunRecord
    from src.models import EventEntry

    state = _summary_state(state="IDLE")
    redis = _SummaryRedis()
    await MetricsStore(redis).save(RunRecord(
        run_id="durable", task_id="PR-422", repo_name=state.name, profile_id="coder",
        task_type="bugfix", complexity="medium", started_at="2026-10-01T00:00:00Z",
        ended_at="2026-10-01T01:00:00Z", duration_ms=3600000, fix_iterations=0,
        tokens_in=0, tokens_out=0, exit_reason="new-terminal-label",
        operator_intervention=False, outcome=outcome, cause=cause,
    ))
    state.history = [EventEntry(event=f"unrelated event {n}", state="IDLE") for n in range(150)]
    await redis.set(f"pipeline:{state.name}", state.model_dump_json())
    # A newly created reader retains the durable record, with no merge/failure event.
    view = await _summary(type(state).model_validate_json(state.model_dump_json()), redis)
    assert view["result"] == {
        "task": "PR-422", "outcome": outcome, "cause": cause,
        "ended_at": "2026-10-01T01:00:00Z",
    }
    html = web_app.templates.get_template("components/repo_summary.html").render(
        repo=state, work_summary=view, coders=[], effective_coder=None,
    )
    assert f"PR-422 — {outcome}" in html
    assert "resuming soon" not in html


@pytest.mark.asyncio
async def test_work_summary_failed_store_and_retained_safety_fence():
    from src.models import PublicationMigration

    state = _summary_state(state="ERROR", skip_ai_error_diagnose=True,
                           error_message="Coder crashed")
    class Broken(_SummaryRedis):
        async def lrange(self, *args):
            raise ConnectionError("down")
    view = await _summary(state, Broken({f"pipeline:{state.name}": state.model_dump_json()}))
    assert view["results_available"] is False
    assert "choose Retry" in view["next_step"]
    state.publication_migration = PublicationMigration(
        repo="example/alpha", head_repo="example/alpha", pr_number=620, ref="refs/heads/work", head_sha="a" * 40,
        reason="Publication evidence is incomplete",
    )
    view = await _summary(state)
    assert view["reason"] == "Publication evidence is incomplete"
    assert "safety fence remains active" in view["next_step"]
    html = web_app.templates.get_template("components/repo_summary.html").render(
        repo=state, work_summary=view, coders=[], effective_coder=None,
    )
    assert '<details class="mb-6" data-work-diagnostics>' in html
    assert "Publication diagnostics" in html
    assert "Result unavailable" not in html


def test_work_summary_in_full_and_poll_render(tmp_path, monkeypatch):
    cfg = tmp_path / "config.yml"
    cfg.write_text("repositories:\n  - url: https://github.com/example/alpha\n")
    monkeypatch.chdir(tmp_path)
    state = _summary_state(state="PREFLIGHT", error_message="Pinned coder unavailable")
    redis = _SummaryRedis({f"pipeline:{state.name}": state.model_dump_json()})
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis))
    with TestClient(app) as client:
        for path in ("/repo/example__alpha", "/partials/repo/example__alpha"):
            response = client.get(path)
            assert response.status_code == 200
            assert "data-work-summary" in response.text
            assert "Pinned coder unavailable" in response.text
            assert "Source: current" in response.text
            assert "recheck prerequisites" in response.text
            assert "No recorded result" in response.text


@pytest.mark.asyncio
async def test_work_summary_identifies_approval_decision_and_stop():
    from src.inhibitor import WorkInhibitor
    state = _summary_state(state="ERROR")
    redis = _SummaryRedis({f"pipeline:{state.name}": state.model_dump_json()})
    view = await dashboard_routes._work_summary(state, redis, stale_after=600, guardrail_decision=True)
    assert "choose Approve or Reject" in view["next_step"]
    state.active_inhibitors = [WorkInhibitor(inhibitor_type="user_stop", source_key="stop")]
    assert "stop or safety condition" in (await _summary(state))["next_step"]
