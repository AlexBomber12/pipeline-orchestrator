"""Tests for the cancellation surfacing endpoints introduced in PR-254.

Both endpoints — JSON (``/api/cancellations/{repo}``) and HTML
(``/partials/repo/{name}/cancellations``) — read through
``src.cancellation.list_recent_cancellations``. The tests patch that
single seam so the routes can be exercised without standing up a real
Redis or running the storage substrate end-to-end (which is already
covered by ``tests/test_cancellation_storage.py``).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from src.cancellation import CancellationCause
from src.web import app as web_app
from src.web.routes import dashboard as dashboard_routes


def _make_cause(
    task_id: str,
    *,
    category: str = "ESCALATE",
    payload: dict | None = None,
    created_at: str | None = None,
) -> CancellationCause:
    return CancellationCause(
        category=category,
        payload=payload if payload is not None else {"reason_text": "manual"},
        created_at=created_at or "2026-05-04T12:00:00+00:00",
        task_id=task_id,
        repo_slug="example__repo",
    )


@pytest.fixture
def cancellations_client(monkeypatch: pytest.MonkeyPatch):
    """Return a TestClient and a mutable list backing list_recent_cancellations.

    The fake records the ``since`` argument the route passed in so tests
    can assert the 7-day window trims older entries at the source rather
    than only at serialization.

    Also stubs ``_find_repo_config_by_name`` to treat ``example__repo`` as
    configured so the route's config-gate (introduced after PR-254 review
    feedback) does not short-circuit before the storage seam is hit.
    """
    fake_causes: list[CancellationCause] = []
    captured: dict = {}

    async def fake_list(redis_client, repo_slug, since):
        captured["redis_client"] = redis_client
        captured["repo_slug"] = repo_slug
        captured["since"] = since
        return [
            cause
            for cause in fake_causes
            if datetime.fromisoformat(cause.created_at) >= since
        ]

    monkeypatch.setattr(
        dashboard_routes, "list_recent_cancellations", fake_list
    )
    monkeypatch.setattr(
        dashboard_routes,
        "_find_repo_config_by_name",
        lambda config, name: object() if name == "example__repo" else None,
    )

    with TestClient(web_app.app) as client:
        yield client, fake_causes, captured


def test_endpoint_returns_recent_causes(cancellations_client) -> None:
    client, causes, _captured = cancellations_client
    now = datetime.now(timezone.utc)
    causes[:] = [
        _make_cause(
            "PR-1",
            category="CRASH",
            payload={"exit_code": 137},
            created_at=(now - timedelta(hours=1)).isoformat(),
        ),
        _make_cause(
            "PR-2",
            category="ESCALATE",
            payload={"reason_text": "stuck"},
            created_at=(now - timedelta(hours=2)).isoformat(),
        ),
        _make_cause(
            "PR-3",
            category="INFRA",
            payload={"subsystem": "gh_api", "retry_count": 3},
            created_at=(now - timedelta(hours=3)).isoformat(),
        ),
    ]

    resp = client.get("/api/cancellations/example__repo")

    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 3
    task_ids = {c["task_id"] for c in body}
    assert task_ids == {"PR-1", "PR-2", "PR-3"}
    # Each entry mirrors the CancellationCause dataclass fields.
    crash = next(c for c in body if c["task_id"] == "PR-1")
    assert crash["category"] == "CRASH"
    assert crash["payload"] == {"exit_code": 137}
    assert crash["repo_slug"] == "example__repo"


def test_endpoint_filters_older_than_7_days(cancellations_client) -> None:
    """The route's ``since`` window excludes records older than 7 days."""
    client, causes, captured = cancellations_client
    now = datetime.now(timezone.utc)
    causes[:] = [
        _make_cause(
            "PR-FRESH",
            created_at=(now - timedelta(days=1)).isoformat(),
        ),
        _make_cause(
            "PR-STALE",
            created_at=(now - timedelta(days=8)).isoformat(),
        ),
    ]

    resp = client.get("/api/cancellations/example__repo")

    assert resp.status_code == 200
    body = resp.json()
    assert [c["task_id"] for c in body] == ["PR-FRESH"]
    # The route asked storage for entries no older than ~7 days.
    assert captured["since"] <= now - timedelta(days=7) + timedelta(seconds=5)
    assert captured["since"] >= now - timedelta(days=7) - timedelta(seconds=5)


def test_endpoint_caps_at_50(cancellations_client) -> None:
    client, causes, _captured = cancellations_client
    now = datetime.now(timezone.utc)
    causes[:] = [
        _make_cause(
            f"PR-{idx:03d}",
            created_at=(now - timedelta(minutes=idx)).isoformat(),
        )
        for idx in range(60)
    ]

    resp = client.get("/api/cancellations/example__repo")

    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 50


def test_endpoint_empty_when_no_redis(monkeypatch: pytest.MonkeyPatch) -> None:
    """No Redis attached to app.state ⇒ endpoint returns ``[]`` short-circuit.

    Skipping the ``with`` block on TestClient deliberately bypasses the
    lifespan that would attach a Redis client to ``app.state.redis``;
    without it ``getattr(..., "redis", None)`` falls through to ``None``
    and the route must serve an empty list rather than reaching storage.
    """
    sentinel = {"called": False}

    async def fake_list(redis_client, repo_slug, since):  # pragma: no cover
        sentinel["called"] = True
        return []

    monkeypatch.setattr(
        dashboard_routes, "list_recent_cancellations", fake_list
    )
    monkeypatch.setattr(
        dashboard_routes,
        "_find_repo_config_by_name",
        lambda config, name: object() if name == "example__repo" else None,
    )

    if hasattr(web_app.app.state, "redis"):
        monkeypatch.delattr(web_app.app.state, "redis", raising=False)

    client = TestClient(web_app.app)
    resp = client.get("/api/cancellations/example__repo")

    assert resp.status_code == 200
    assert resp.json() == []
    assert sentinel["called"] is False


def test_partial_endpoint_renders_each_category(cancellations_client) -> None:
    """The HTML partial renders one card per category with the expected
    payload fields, exercising every branch of the macro."""
    client, causes, _captured = cancellations_client
    now = datetime.now(timezone.utc)
    causes[:] = [
        _make_cause(
            "PR-CRASH",
            category="CRASH",
            payload={"exit_code": 137, "error_message": "boom on stderr"},
            created_at=(now - timedelta(minutes=10)).isoformat(),
        ),
        _make_cause(
            "PR-ESC",
            category="ESCALATE",
            payload={"reason_text": "manual ESCALATE marker"},
            created_at=(now - timedelta(minutes=20)).isoformat(),
        ),
        _make_cause(
            "PR-TO",
            category="TIMEOUT",
            payload={
                "limit_type": "wallclock",
                "duration_elapsed_sec": 1800,
                "active_phase": "CODING",
            },
            created_at=(now - timedelta(minutes=30)).isoformat(),
        ),
        _make_cause(
            "PR-INF",
            category="INFRA",
            payload={
                "subsystem": "gh_api",
                "retry_count": 5,
                "last_attempt_iso": (now - timedelta(minutes=40)).isoformat(),
            },
            created_at=(now - timedelta(minutes=40)).isoformat(),
        ),
        _make_cause(
            "PR-OPR",
            category="OPERATOR_RECOVERY",
            payload={},
            created_at=(now - timedelta(minutes=50)).isoformat(),
        ),
        _make_cause(
            "PR-NPD",
            category="NO_PUSH_DEADLOCK",
            payload={"attempts": 3},
            created_at=(now - timedelta(minutes=60)).isoformat(),
        ),
    ]

    resp = client.get("/partials/repo/example__repo/cancellations")

    assert resp.status_code == 200
    body = resp.text
    # Per-category badge label + class hook present.
    assert "category-crash" in body and ">CRASH<" in body
    assert "category-escalate" in body and ">ESCALATE<" in body
    assert "category-timeout" in body and ">TIMEOUT<" in body
    assert "category-infra" in body and ">INFRA<" in body
    assert "category-operator_recovery" in body and ">OPERATOR_RECOVERY<" in body
    assert "category-no_push_deadlock" in body and ">NO_PUSH_DEADLOCK<" in body
    # Payload fields render per branch.
    assert "boom on stderr" in body
    assert "manual ESCALATE marker" in body
    assert "wallclock" in body and "1800s" in body and "CODING" in body
    assert "gh_api" in body
    assert "Manual recovery via dashboard" in body
    assert "no push for" in body and ">3<" in body


def test_partial_endpoint_renders_empty_state_when_no_causes(
    cancellations_client,
) -> None:
    client, causes, _captured = cancellations_client
    causes.clear()

    resp = client.get("/partials/repo/example__repo/cancellations")

    assert resp.status_code == 200
    assert "No cancellations recorded in the last 7 days." in resp.text


def test_partial_endpoint_no_redis_renders_empty_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without Redis the partial still renders the empty-state placeholder
    rather than 5xx — keeps the HTMX swap target stable."""
    sentinel = {"called": False}

    async def fake_list(redis_client, repo_slug, since):  # pragma: no cover
        sentinel["called"] = True
        return []

    monkeypatch.setattr(
        dashboard_routes, "list_recent_cancellations", fake_list
    )
    monkeypatch.setattr(
        dashboard_routes,
        "_find_repo_config_by_name",
        lambda config, name: object() if name == "example__repo" else None,
    )

    if hasattr(web_app.app.state, "redis"):
        monkeypatch.delattr(web_app.app.state, "redis", raising=False)

    client = TestClient(web_app.app)
    resp = client.get("/partials/repo/example__repo/cancellations")

    assert resp.status_code == 200
    assert "No cancellations recorded in the last 7 days." in resp.text
    assert sentinel["called"] is False


def test_endpoint_returns_empty_when_redis_read_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Redis attached at startup but raising at request time degrades to ``[]``
    rather than bubbling up as a 500."""

    async def boom(redis_client, repo_slug, since):
        raise ConnectionError("redis unreachable")

    monkeypatch.setattr(dashboard_routes, "list_recent_cancellations", boom)
    monkeypatch.setattr(
        dashboard_routes,
        "_find_repo_config_by_name",
        lambda config, name: object() if name == "example__repo" else None,
    )

    with TestClient(web_app.app) as client:
        # Replace the lifespan-attached client with a sentinel so the
        # ``redis_client is None`` short-circuit doesn't hide the raise.
        monkeypatch.setattr(web_app.app.state, "redis", object())
        resp = client.get("/api/cancellations/example__repo")

    assert resp.status_code == 200
    assert resp.json() == []


def test_partial_endpoint_renders_empty_state_when_redis_read_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The HTML partial falls back to the empty state on Redis read errors,
    keeping the HTMX swap target stable."""

    async def boom(redis_client, repo_slug, since):
        raise ConnectionError("redis unreachable")

    monkeypatch.setattr(dashboard_routes, "list_recent_cancellations", boom)
    monkeypatch.setattr(
        dashboard_routes,
        "_find_repo_config_by_name",
        lambda config, name: object() if name == "example__repo" else None,
    )

    with TestClient(web_app.app) as client:
        monkeypatch.setattr(web_app.app.state, "redis", object())
        resp = client.get("/partials/repo/example__repo/cancellations")

    assert resp.status_code == 200
    assert "No cancellations recorded in the last 7 days." in resp.text


def test_partial_endpoint_renders_legacy_records_without_payload_fields(
    cancellations_client,
) -> None:
    """Older records (recorded before PR-253 added payload fields)
    must render without 5xx — each subfield guard short-circuits cleanly."""
    client, causes, _captured = cancellations_client
    causes[:] = [
        _make_cause("PR-LEGACY-CRASH", category="CRASH", payload={}),
        _make_cause("PR-LEGACY-ESC", category="ESCALATE", payload={}),
        _make_cause("PR-LEGACY-TO", category="TIMEOUT", payload={}),
        _make_cause("PR-LEGACY-INF", category="INFRA", payload={}),
        _make_cause("PR-LEGACY-NPD", category="NO_PUSH_DEADLOCK", payload={}),
    ]

    resp = client.get("/partials/repo/example__repo/cancellations")

    assert resp.status_code == 200
    body = resp.text
    assert "PR-LEGACY-CRASH" in body
    assert "PR-LEGACY-ESC" in body
    # NO_PUSH_DEADLOCK falls back to the no-attempts message.
    assert "no push across consecutive cycles" in body


def test_endpoint_short_circuits_when_repo_not_in_config(
    cancellations_client,
) -> None:
    """A slug missing from ``config.yml`` must not reach storage.

    Guards against stale ``cancellation_index:*`` keys (TTL up to 30 days)
    resurfacing for repos that were removed or mistyped, matching the
    config-gate other repo endpoints already enforce.
    """
    client, causes, captured = cancellations_client
    now = datetime.now(timezone.utc)
    causes[:] = [
        _make_cause(
            "PR-1",
            created_at=(now - timedelta(hours=1)).isoformat(),
        ),
    ]

    resp = client.get("/api/cancellations/removed__repo")

    assert resp.status_code == 200
    assert resp.json() == []
    assert "repo_slug" not in captured


def test_partial_endpoint_short_circuits_when_repo_not_in_config(
    cancellations_client,
) -> None:
    """The HTML partial renders the empty placeholder for unconfigured
    repo slugs rather than reading Redis directly."""
    client, causes, captured = cancellations_client
    now = datetime.now(timezone.utc)
    causes[:] = [
        _make_cause(
            "PR-STALE",
            created_at=(now - timedelta(hours=1)).isoformat(),
        ),
    ]

    resp = client.get("/partials/repo/removed__repo/cancellations")

    assert resp.status_code == 200
    assert "No cancellations recorded in the last 7 days." in resp.text
    assert "PR-STALE" not in resp.text
    assert "repo_slug" not in captured
