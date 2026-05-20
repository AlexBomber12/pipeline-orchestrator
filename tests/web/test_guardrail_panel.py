"""Tests for the operator override guardrail panel UI (PR-306).

Covers three layers: the pure helpers (``_truncate_guardrail_excerpt``,
``_format_guardrail_relative_time``, ``_serialize_guardrail_pending``),
the ``_build_guardrail_pending_view`` redis-facing builder, the
``_repo_template_context`` integration that exposes ``guardrail_pending``
to the template, and the partial template rendering itself (empty
state, populated state, button HTMX attributes, excerpt truncation,
relative time, PR-URL fallback).
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from redis.exceptions import RedisError
from src.cancellation.storage import GuardrailPending
from src.models import PipelineState, PRInfo, QueueTask, RepoState, TaskStatus
from src.web import app as web_app
from src.web.app import app
from src.web.routes import dashboard as dashboard_routes

_BASE_TS = int(
    datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc).timestamp()
)


def _now_at(seconds_after_base: int) -> datetime:
    return datetime.fromtimestamp(_BASE_TS + seconds_after_base, tz=timezone.utc)


def test_truncate_excerpt_preserves_short_excerpt() -> None:
    assert dashboard_routes._truncate_guardrail_excerpt("short") == "short"


def test_truncate_excerpt_caps_at_200_chars_with_ellipsis() -> None:
    long_text = "x" * 500
    out = dashboard_routes._truncate_guardrail_excerpt(long_text)
    assert len(out) <= 200
    assert out.endswith("…")


def test_truncate_excerpt_boundary_exact_200_chars_kept() -> None:
    exact = "y" * 200
    assert dashboard_routes._truncate_guardrail_excerpt(exact) == exact


def test_format_relative_time_just_now_under_one_minute() -> None:
    assert (
        dashboard_routes._format_guardrail_relative_time(
            _BASE_TS, now=_now_at(30)
        )
        == "just now"
    )


def test_format_relative_time_future_collapses_to_just_now() -> None:
    assert (
        dashboard_routes._format_guardrail_relative_time(
            _BASE_TS + 120, now=_now_at(0)
        )
        == "just now"
    )


def test_format_relative_time_minutes_plural_and_singular() -> None:
    assert (
        dashboard_routes._format_guardrail_relative_time(
            _BASE_TS, now=_now_at(60)
        )
        == "1 minute ago"
    )
    assert (
        dashboard_routes._format_guardrail_relative_time(
            _BASE_TS, now=_now_at(5 * 60)
        )
        == "5 minutes ago"
    )


def test_format_relative_time_hours() -> None:
    assert (
        dashboard_routes._format_guardrail_relative_time(
            _BASE_TS, now=_now_at(3600)
        )
        == "1 hour ago"
    )
    assert (
        dashboard_routes._format_guardrail_relative_time(
            _BASE_TS, now=_now_at(3 * 3600 + 5)
        )
        == "3 hours ago"
    )


def test_format_relative_time_days() -> None:
    assert (
        dashboard_routes._format_guardrail_relative_time(
            _BASE_TS, now=_now_at(86_400)
        )
        == "1 day ago"
    )
    assert (
        dashboard_routes._format_guardrail_relative_time(
            _BASE_TS, now=_now_at(2 * 86_400 + 5)
        )
        == "2 days ago"
    )


def test_format_relative_time_uses_real_now_when_omitted() -> None:
    out = dashboard_routes._format_guardrail_relative_time(_BASE_TS)
    assert isinstance(out, str) and out  # Just ensure it does not raise.


def _entry(
    pr_id: str,
    *,
    rule: str = "large_diff_threshold",
    excerpt: str = "+1800 LOC across 35 files",
    recorded_at: int = _BASE_TS,
) -> GuardrailPending:
    return GuardrailPending(
        repo_slug="example__alpha",
        task_id=pr_id,
        rule=rule,
        excerpt=excerpt,
        recorded_at=recorded_at,
    )


def test_serialize_includes_pr_url_when_provided() -> None:
    view = dashboard_routes._serialize_guardrail_pending(
        _entry("PR-296"),
        current_pr_url="https://github.com/example/alpha/pull/42",
        is_active=True,
        now=_now_at(5 * 60),
    )
    assert view["pr_id"] == "PR-296"
    assert view["rule"] == "large_diff_threshold"
    assert view["excerpt"] == "+1800 LOC across 35 files"
    assert view["recorded_at"] == _BASE_TS
    assert view["recorded_at_text"] == "5 minutes ago"
    assert view["pr_url"] == "https://github.com/example/alpha/pull/42"
    assert view["is_active"] is True


def test_serialize_truncates_long_excerpts() -> None:
    view = dashboard_routes._serialize_guardrail_pending(
        _entry("PR-296", excerpt="z" * 500),
        current_pr_url=None,
    )
    assert len(view["excerpt"]) <= 200
    assert view["excerpt"].endswith("…")
    assert view["pr_url"] is None


def test_serialize_defaults_is_active_false() -> None:
    """Historical entries serialize with ``is_active=False`` so the panel
    hides the Approve button, which would otherwise 409 against the
    approve endpoint's ``state.current_task.pr_id == pr_id`` gate."""
    view = dashboard_routes._serialize_guardrail_pending(
        _entry("PR-OTHER"),
        current_pr_url=None,
    )
    assert view["is_active"] is False


class _FakeRedis:
    """In-memory subset of the aioredis client used by the panel helpers."""

    def __init__(self) -> None:
        self.zsets: dict[str, dict[str, float]] = {}
        self.values: dict[str, str] = {}
        self.ttls: dict[str, int] = {}
        self._raise_on_zrange = False

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def zrange(
        self, key: str, start: int, stop: int, withscores: bool = False
    ) -> list[Any]:
        if self._raise_on_zrange:
            raise RedisError("simulated redis outage")
        ordered = sorted(self.zsets.get(key, {}).items(), key=lambda kv: kv[1])
        members = [tid for tid, _ in ordered]
        return members[start : (stop + 1) if stop != -1 else None]

    async def zrem(self, key: str, *members: str) -> int:
        zset = self.zsets.get(key, {})
        return sum(1 for m in members if zset.pop(m, None) is not None)

    async def zadd(self, key: str, mapping: dict[str, float]) -> int:
        bucket = self.zsets.setdefault(key, {})
        added = 0
        for member, score in mapping.items():
            if member not in bucket:
                added += 1
            bucket[member] = float(score)
        return added

    async def expire(self, key: str, seconds: int) -> bool:
        if key in self.values or key in self.zsets:
            self.ttls[key] = seconds
            return True
        return False

    async def aclose(self) -> None:
        return None


def _put_guardrail(
    redis: _FakeRedis,
    repo: str,
    pr_id: str,
    *,
    rule: str = "large_diff_threshold",
    excerpt: str = "+1800 LOC across 35 files",
    ts: float = float(_BASE_TS),
) -> None:
    from src.cancellation.storage import CancellationCause, cause_key, index_key

    cause = CancellationCause(
        category="ERROR",
        payload={
            "subsource": "guardrail",
            "rule": rule,
            "excerpt": excerpt,
        },
        created_at=datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
        task_id=pr_id,
        repo_slug=repo,
    )
    redis.values[cause_key(repo, pr_id)] = cause.to_redis()
    redis.zsets.setdefault(index_key(repo), {})[pr_id] = ts


def _bare_state(pr_id: str | None = None, *, url: str = "") -> RepoState:
    current_task = (
        QueueTask(pr_id=pr_id, title=pr_id, status=TaskStatus.ERROR)
        if pr_id is not None
        else None
    )
    current_pr = (
        PRInfo(number=99, branch="main", url=url) if pr_id is not None else None
    )
    return RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.ERROR,
        current_task=current_task,
        current_pr=current_pr,
    )


async def test_build_view_returns_empty_when_redis_is_none() -> None:
    out = await dashboard_routes._build_guardrail_pending_view(
        None, "example__alpha", _bare_state()
    )
    assert out == []


async def test_build_view_swallows_redis_error() -> None:
    redis = _FakeRedis()
    redis._raise_on_zrange = True
    out = await dashboard_routes._build_guardrail_pending_view(
        redis, "example__alpha", _bare_state()
    )
    assert out == []


async def test_build_view_attaches_pr_url_only_to_current_pr_entry() -> None:
    redis = _FakeRedis()
    _put_guardrail(redis, "example__alpha", "PR-296", ts=float(_BASE_TS))
    _put_guardrail(redis, "example__alpha", "PR-OTHER", ts=float(_BASE_TS) + 1)
    state = _bare_state(
        "PR-296", url="https://github.com/example/alpha/pull/42"
    )
    out = await dashboard_routes._build_guardrail_pending_view(
        redis, "example__alpha", state, now=_now_at(60)
    )
    by_id = {e["pr_id"]: e for e in out}
    assert (
        by_id["PR-296"]["pr_url"]
        == "https://github.com/example/alpha/pull/42"
    )
    assert by_id["PR-OTHER"]["pr_url"] is None
    assert by_id["PR-296"]["is_active"] is True
    assert by_id["PR-OTHER"]["is_active"] is False


async def test_build_view_no_pr_url_when_repo_has_no_current_pr() -> None:
    redis = _FakeRedis()
    _put_guardrail(redis, "example__alpha", "PR-296", ts=float(_BASE_TS))
    out = await dashboard_routes._build_guardrail_pending_view(
        redis, "example__alpha", _bare_state()
    )
    assert out and out[0]["pr_url"] is None
    assert out[0]["is_active"] is False


async def test_build_view_is_active_when_current_pr_lacks_url() -> None:
    """``is_active`` mirrors the approve gate (``state.current_pr`` set),
    which does not require ``current_pr.url``. An entry matching the
    current task with an empty PR URL is still approve-eligible."""
    redis = _FakeRedis()
    _put_guardrail(redis, "example__alpha", "PR-296", ts=float(_BASE_TS))
    state = _bare_state("PR-296", url="")
    out = await dashboard_routes._build_guardrail_pending_view(
        redis, "example__alpha", state
    )
    assert out and out[0]["pr_id"] == "PR-296"
    assert out[0]["pr_url"] is None
    assert out[0]["is_active"] is True


def _put_guardrail_reason_text_only(
    redis: _FakeRedis,
    repo: str,
    pr_id: str,
    *,
    reason_text: str,
    ts: float = float(_BASE_TS),
) -> None:
    """Mirror the cause shape emitted by CODING/FIX handlers.

    ``src.daemon.handlers.coding`` / ``fix`` record guardrail
    cancellations as ``payload={"subsource": "guardrail",
    "reason_text": "GUARDRAIL: <category>: <excerpt>"}`` with no
    structured ``rule``/``excerpt`` keys; this helper reproduces that
    shape so panel-rendering tests can exercise the fallback parser.
    """
    from src.cancellation.storage import CancellationCause, cause_key, index_key

    cause = CancellationCause(
        category="ERROR",
        payload={"subsource": "guardrail", "reason_text": reason_text},
        created_at=datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
        task_id=pr_id,
        repo_slug=repo,
    )
    redis.values[cause_key(repo, pr_id)] = cause.to_redis()
    redis.zsets.setdefault(index_key(repo), {})[pr_id] = ts


def test_resolve_guardrail_metadata_prefers_structured_fields() -> None:
    rule, excerpt = dashboard_routes._resolve_guardrail_metadata(
        {"rule": "large_diff_threshold", "excerpt": "+1800 LOC"}
    )
    assert rule == "large_diff_threshold"
    assert excerpt == "+1800 LOC"


def test_resolve_guardrail_metadata_parses_reason_text() -> None:
    rule, excerpt = dashboard_routes._resolve_guardrail_metadata(
        {
            "subsource": "guardrail",
            "reason_text": "GUARDRAIL: large_diff: +1800 LOC across 35 files",
        }
    )
    assert rule == "large_diff"
    assert excerpt == "+1800 LOC across 35 files"


def test_resolve_guardrail_metadata_falls_back_to_category_for_rule() -> None:
    rule, excerpt = dashboard_routes._resolve_guardrail_metadata(
        {"category": "large_diff", "excerpt": "+1800 LOC"}
    )
    assert rule == "large_diff"
    assert excerpt == "+1800 LOC"


def test_resolve_guardrail_metadata_empty_when_no_signal() -> None:
    rule, excerpt = dashboard_routes._resolve_guardrail_metadata(
        {"subsource": "guardrail"}
    )
    assert rule == ""
    assert excerpt == ""


async def test_build_view_parses_reason_text_when_rule_excerpt_missing() -> None:
    """CODING/FIX-emitted causes carry only ``reason_text``; the panel
    must surface the parsed rule + excerpt or operators lose context."""
    redis = _FakeRedis()
    _put_guardrail_reason_text_only(
        redis,
        "example__alpha",
        "PR-296",
        reason_text="GUARDRAIL: large_diff_threshold: +1800 LOC across 35 files",
        ts=float(_BASE_TS),
    )
    out = await dashboard_routes._build_guardrail_pending_view(
        redis, "example__alpha", _bare_state(), now=_now_at(60)
    )
    assert out and out[0]["pr_id"] == "PR-296"
    assert out[0]["rule"] == "large_diff_threshold"
    assert out[0]["excerpt"] == "+1800 LOC across 35 files"


async def test_build_view_leaves_blank_when_reason_text_unparseable() -> None:
    """If the cause carries neither structured fields nor a GUARDRAIL-prefixed
    reason_text, the view degrades to blank rather than crashing."""
    redis = _FakeRedis()
    _put_guardrail_reason_text_only(
        redis,
        "example__alpha",
        "PR-296",
        reason_text="cancelled by operator",
        ts=float(_BASE_TS),
    )
    out = await dashboard_routes._build_guardrail_pending_view(
        redis, "example__alpha", _bare_state()
    )
    assert out and out[0]["pr_id"] == "PR-296"
    assert out[0]["rule"] == ""
    assert out[0]["excerpt"] == ""


async def test_build_view_keeps_entry_when_recovery_lookup_raises(
    monkeypatch,
) -> None:
    """A transient Redis failure during the fallback re-fetch must not
    drop the row; the entry survives with whatever fields the initial
    list returned."""
    redis = _FakeRedis()
    _put_guardrail_reason_text_only(
        redis,
        "example__alpha",
        "PR-296",
        reason_text="GUARDRAIL: cat: text",
        ts=float(_BASE_TS),
    )

    async def _boom(*args: Any, **kwargs: Any) -> None:
        raise RedisError("simulated outage")

    monkeypatch.setattr(dashboard_routes, "get_cancellation_cause", _boom)
    out = await dashboard_routes._build_guardrail_pending_view(
        redis, "example__alpha", _bare_state()
    )
    assert out and out[0]["pr_id"] == "PR-296"
    assert out[0]["rule"] == ""
    assert out[0]["excerpt"] == ""


async def test_build_view_keeps_entry_when_cause_vanished_between_reads(
    monkeypatch,
) -> None:
    """A TOCTOU race where the cause expires between the helper's read
    and our recovery fetch must leave the row intact rather than crash."""
    redis = _FakeRedis()
    _put_guardrail_reason_text_only(
        redis,
        "example__alpha",
        "PR-296",
        reason_text="GUARDRAIL: cat: text",
        ts=float(_BASE_TS),
    )

    async def _race(*args: Any, **kwargs: Any) -> Any:
        return None

    monkeypatch.setattr(dashboard_routes, "get_cancellation_cause", _race)
    out = await dashboard_routes._build_guardrail_pending_view(
        redis, "example__alpha", _bare_state()
    )
    assert out and out[0]["pr_id"] == "PR-296"
    assert out[0]["rule"] == ""
    assert out[0]["excerpt"] == ""


async def test_recover_guardrail_metadata_does_not_refresh_ttl(
    monkeypatch,
) -> None:
    """PR-345 follow-up: ``_recover_guardrail_metadata`` runs inside the
    repo-detail panel render path, which is polled every 30s by
    ``/partials/repo/{name}``. Passive polling must not extend the
    forensic TTL on every poll cycle, so the recovery fetch passes
    ``refresh_ttl=False`` and the default refresh stays reserved for
    explicit diagnostic reads.
    """
    redis = _FakeRedis()
    _put_guardrail_reason_text_only(
        redis,
        "example__alpha",
        "PR-296",
        reason_text="GUARDRAIL: cat: text",
        ts=float(_BASE_TS),
    )

    captured: list[bool] = []

    async def spy(
        redis_client, repo_slug, task_id, *, refresh_ttl: bool = True
    ):
        captured.append(refresh_ttl)
        return None

    monkeypatch.setattr(dashboard_routes, "get_cancellation_cause", spy)
    await dashboard_routes._build_guardrail_pending_view(
        redis, "example__alpha", _bare_state()
    )

    assert captured, "expected _recover_guardrail_metadata to fetch the cause"
    assert all(value is False for value in captured)


# ----- partial template rendering -----


def _render_panel(
    *, guardrail_pending: list[dict[str, Any]], repo_name: str = "example__alpha"
) -> str:
    template = web_app.templates.env.get_template(
        "components/_guardrail_panel.html"
    )
    state = _bare_state()
    state.name = repo_name
    return template.render(
        guardrail_pending=guardrail_pending,
        repo=state,
    )


def _view(
    pr_id: str = "PR-296",
    *,
    rule: str = "large_diff_threshold",
    excerpt: str = "+1800 LOC across 35 files",
    recorded_at: int | None = None,
    recorded_at_text: str = "5 minutes ago",
    pr_url: str | None = None,
    is_active: bool = True,
) -> dict[str, Any]:
    return {
        "pr_id": pr_id,
        "rule": rule,
        "excerpt": excerpt,
        "recorded_at": _BASE_TS if recorded_at is None else recorded_at,
        "recorded_at_text": recorded_at_text,
        "pr_url": pr_url,
        "is_active": is_active,
    }


def test_panel_renders_nothing_when_pending_empty() -> None:
    html = _render_panel(guardrail_pending=[])
    assert "guardrail-panel" not in html
    assert "Pending guardrail decisions" not in html


def test_panel_renders_section_and_count_for_two_entries() -> None:
    html = _render_panel(
        guardrail_pending=[_view("PR-A"), _view("PR-B", rule="big_diff")]
    )
    assert "guardrail-panel" in html
    assert "Pending guardrail decisions" in html
    assert html.count('data-pr-id="PR-A"') == 1
    assert html.count('data-pr-id="PR-B"') == 1
    assert html.count("hx-post=\"/repos/example__alpha/guardrail/PR-A/decision\"") == 2
    assert html.count("hx-post=\"/repos/example__alpha/guardrail/PR-B/decision\"") == 2


def test_panel_buttons_carry_confirm_target_and_swap_attributes() -> None:
    html = _render_panel(guardrail_pending=[_view("PR-296")])
    assert "Approve" in html and "Reject" in html
    assert "approve-btn" in html and "reject-btn" in html
    assert 'hx-vals=\'{"decision": "approve"}\'' in html
    assert 'hx-vals=\'{"decision": "reject"}\'' in html
    assert "hx-confirm=\"Approve guardrail violation for PR-296" in html
    assert "hx-confirm=\"Reject guardrail violation for PR-296" in html
    assert 'hx-target="closest .guardrail-row"' in html
    assert 'hx-swap="outerHTML"' in html


def test_panel_buttons_opt_in_to_204_swap() -> None:
    """htmx 2.x skips 204 swaps by default; both buttons must opt in.

    The decision endpoint returns 204 No Content on success and the
    base.html global ``htmx:beforeSwap`` hook only flips ``shouldSwap``
    for 400/404/409/422/503. Without a per-button override the row would
    stay rendered after a successful click, inviting duplicate clicks
    while the operator waits for the next poll to reconcile.
    """
    html = _render_panel(guardrail_pending=[_view("PR-296")])
    handler = (
        'hx-on::before-swap="if (event.detail.xhr.status === 204) { '
        'event.detail.shouldSwap = true; }"'
    )
    assert html.count(handler) == 2


def test_panel_renders_pr_url_when_present_and_plain_text_when_missing() -> None:
    linked = _render_panel(
        guardrail_pending=[
            _view(
                "PR-LINK", pr_url="https://github.com/example/alpha/pull/42"
            )
        ]
    )
    assert 'href="https://github.com/example/alpha/pull/42"' in linked

    bare = _render_panel(guardrail_pending=[_view("PR-NOLINK", pr_url=None)])
    assert "https://github.com" not in bare
    assert "PR-NOLINK" in bare


def test_panel_renders_relative_time_label_and_unix_attr() -> None:
    html = _render_panel(
        guardrail_pending=[
            _view("PR-296", recorded_at_text="5 minutes ago")
        ]
    )
    assert "5 minutes ago" in html
    assert f'data-ts-unix="{_BASE_TS}"' in html


def test_panel_renders_truncated_excerpt() -> None:
    truncated = "z" * 199 + "…"
    html = _render_panel(
        guardrail_pending=[_view("PR-296", excerpt=truncated)]
    )
    assert truncated in html
    assert "z" * 500 not in html


# ----- route handler integration test -----


def _write_config(tmp_path: Path) -> None:
    (tmp_path / "config.yml").write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n"
        "daemon:\n"
        "  retry_button_cap: 3\n",
        encoding="utf-8",
    )


def _aioredis_factory(redis_client: _FakeRedis) -> Any:
    return type(
        "_Aioredis",
        (),
        {"from_url": staticmethod(lambda url, decode_responses=True: redis_client)},
    )()


def test_repo_detail_route_passes_guardrail_pending_to_template(
    tmp_path, monkeypatch
) -> None:
    """The /repo/{name} route must surface pending guardrails to the template."""
    _write_config(tmp_path)
    monkeypatch.chdir(tmp_path)
    redis_client = _FakeRedis()
    _put_guardrail(redis_client, "example__alpha", "PR-296", ts=float(_BASE_TS))
    _put_guardrail(
        redis_client, "example__alpha", "PR-LATER", ts=float(_BASE_TS) + 30
    )
    monkeypatch.setattr(web_app, "aioredis", _aioredis_factory(redis_client))

    client = TestClient(app)
    with client:
        resp = client.get("/repo/example__alpha")

    assert resp.status_code == 200
    assert "guardrail-panel" in resp.text
    assert "PR-296" in resp.text
    assert "PR-LATER" in resp.text
    assert (
        "/repos/example__alpha/guardrail/PR-296/decision" in resp.text
    )


def test_repo_detail_route_omits_panel_when_no_pending(
    tmp_path, monkeypatch
) -> None:
    _write_config(tmp_path)
    monkeypatch.chdir(tmp_path)
    redis_client = _FakeRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis_factory(redis_client))

    client = TestClient(app)
    with client:
        resp = client.get("/repo/example__alpha")

    assert resp.status_code == 200
    assert "guardrail-panel" not in resp.text
    assert "Pending guardrail decisions" not in resp.text


def test_repo_detail_route_swallows_redis_error_from_pending(
    tmp_path, monkeypatch
) -> None:
    """RedisError from the guardrail helper must not break repo detail rendering."""
    _write_config(tmp_path)
    monkeypatch.chdir(tmp_path)
    redis_client = _FakeRedis()
    redis_client._raise_on_zrange = True
    monkeypatch.setattr(web_app, "aioredis", _aioredis_factory(redis_client))

    client = TestClient(app)
    with client:
        resp = client.get("/repo/example__alpha")

    assert resp.status_code == 200
    assert "guardrail-panel" not in resp.text


def test_repo_detail_partial_route_includes_guardrail_panel(
    tmp_path, monkeypatch
) -> None:
    """The HTMX polling partial must include the guardrail panel too."""
    _write_config(tmp_path)
    monkeypatch.chdir(tmp_path)
    redis_client = _FakeRedis()
    _put_guardrail(redis_client, "example__alpha", "PR-296", ts=float(_BASE_TS))
    monkeypatch.setattr(web_app, "aioredis", _aioredis_factory(redis_client))

    client = TestClient(app)
    with client:
        resp = client.get("/partials/repo/example__alpha")

    assert resp.status_code == 200
    assert "guardrail-panel" in resp.text
    assert "PR-296" in resp.text


def test_repo_detail_route_route_response_template_pr_url_uses_current_pr(
    tmp_path, monkeypatch
) -> None:
    """If a pending entry matches ``current_task``, the partial links to current_pr.url."""
    _write_config(tmp_path)
    monkeypatch.chdir(tmp_path)
    redis_client = _FakeRedis()
    _put_guardrail(
        redis_client, "example__alpha", "PR-CURRENT", ts=float(_BASE_TS)
    )
    state = _bare_state(
        "PR-CURRENT", url="https://github.com/example/alpha/pull/42"
    )
    redis_client.values["pipeline:example__alpha"] = state.model_dump_json()
    monkeypatch.setattr(web_app, "aioredis", _aioredis_factory(redis_client))

    client = TestClient(app)
    with client:
        resp = client.get("/repo/example__alpha")

    assert resp.status_code == 200
    assert 'href="https://github.com/example/alpha/pull/42"' in resp.text


@pytest.mark.parametrize(
    "decision_text",
    ["Approve guardrail violation", "Reject guardrail violation"],
)
def test_panel_confirm_dialogs_mention_consequence(decision_text: str) -> None:
    html = _render_panel(guardrail_pending=[_view("PR-296")])
    assert decision_text in html


def test_panel_hides_approve_button_for_inactive_entry() -> None:
    """Approve only renders for entries matching the daemon's active PR.

    Without this gate the operator can click Approve on historical rows
    and the endpoint will 409 (``_approve_guardrail_decision`` requires
    ``state.current_task.pr_id == pr_id``), leaving a dead-end action
    that cannot clear the row.
    """
    html = _render_panel(guardrail_pending=[_view("PR-OTHER", is_active=False)])
    assert "approve-btn" not in html
    assert "Approve guardrail violation" not in html
    # Reject remains because the reject endpoint accepts non-current PRs.
    assert "reject-btn" in html
    assert "Reject guardrail violation for PR-OTHER" in html


def test_panel_shows_approve_only_on_active_row_in_mixed_list() -> None:
    """When the panel lists both active and historical entries, only the
    active row gets an Approve button — Reject still renders for both."""
    html = _render_panel(
        guardrail_pending=[
            _view("PR-CURRENT", is_active=True),
            _view("PR-OTHER", is_active=False),
        ]
    )
    assert html.count("approve-btn") == 1
    assert "Approve guardrail violation for PR-CURRENT" in html
    assert "Approve guardrail violation for PR-OTHER" not in html
    assert html.count("reject-btn") == 2
    assert "Reject guardrail violation for PR-CURRENT" in html
    assert "Reject guardrail violation for PR-OTHER" in html
