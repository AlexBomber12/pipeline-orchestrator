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
    if created_at is None:
        created_at = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    return CancellationCause(
        category=category,
        payload=payload if payload is not None else {"reason_text": "manual"},
        created_at=created_at,
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


def test_partial_endpoint_renders_each_subsource(cancellations_client) -> None:
    """The HTML partial renders one card per PR-315 subsource with the
    expected payload fields, exercising every branch of the macro after
    the PR-319 rewrite. ``cause.category`` is always ``ERROR`` post-
    migration; detector identity lives in ``payload.subsource``."""
    client, causes, _captured = cancellations_client
    now = datetime.now(timezone.utc)
    causes[:] = [
        _make_cause(
            "PR-CRASH",
            category="ERROR",
            payload={
                "subsource": "crash",
                "exit_code": 137,
                "error_message": "boom on stderr",
            },
            created_at=(now - timedelta(minutes=10)).isoformat(),
        ),
        _make_cause(
            "PR-ESC-CODER",
            category="ERROR",
            payload={
                "subsource": "coder_escalate",
                "reason_text": "manual ESCALATE marker",
            },
            created_at=(now - timedelta(minutes=20)).isoformat(),
        ),
        _make_cause(
            "PR-GR",
            category="ERROR",
            payload={
                "subsource": "guardrail",
                "reason_text": "GUARDRAIL: deletion: rm -rf /",
            },
            created_at=(now - timedelta(minutes=25)).isoformat(),
        ),
        _make_cause(
            "PR-RT",
            category="ERROR",
            payload={
                "subsource": "review_timeout",
                "elapsed_min": 90,
                "reason_text": "review timeout",
            },
            created_at=(now - timedelta(minutes=30)).isoformat(),
        ),
        _make_cause(
            "PR-FIT",
            category="ERROR",
            payload={
                "subsource": "fix_idle_timeout",
                "duration_elapsed_sec": 1800,
                "active_phase": "FIX",
            },
            created_at=(now - timedelta(minutes=35)).isoformat(),
        ),
        _make_cause(
            "PR-FIC",
            category="ERROR",
            payload={
                "subsource": "fix_iteration_cap",
                "iteration_count": 8,
                "pr_number": 42,
            },
            created_at=(now - timedelta(minutes=38)).isoformat(),
        ),
        _make_cause(
            "PR-INF",
            category="ERROR",
            payload={
                "subsource": "infra_failure",
                "subsystem": "gh_api",
                "retry_count": 5,
                "last_attempt_iso": (now - timedelta(minutes=40)).isoformat(),
            },
            created_at=(now - timedelta(minutes=40)).isoformat(),
        ),
        _make_cause(
            "PR-NPD",
            category="ERROR",
            payload={"subsource": "no_push_deadlock", "attempts": 3},
            created_at=(now - timedelta(minutes=60)).isoformat(),
        ),
    ]

    resp = client.get("/partials/repo/example__repo/cancellations")

    assert resp.status_code == 200
    body = resp.text
    # Per-subsource wrapper class hook + badge label present.
    assert "subsource-crash" in body and "Daemon crash" in body
    assert "subsource-coder_escalate" in body and "Coder escalate" in body
    assert "subsource-guardrail" in body and "Guardrail violation" in body
    assert "subsource-review_timeout" in body and "Stale review" in body
    assert "subsource-fix_idle_timeout" in body and "FIX idle timeout" in body
    assert "subsource-fix_iteration_cap" in body and "FIX iteration cap" in body
    assert "subsource-infra_failure" in body and "Infrastructure failure" in body
    assert "subsource-no_push_deadlock" in body and "No-push deadlock" in body
    # Payload fields render per branch.
    assert "boom on stderr" in body
    assert "manual ESCALATE marker" in body
    assert "GUARDRAIL: deletion: rm -rf /" in body
    assert "90m" in body
    assert "1800s" in body
    assert "8 iterations" in body
    assert "gh_api" in body
    assert "3 consecutive cycles" in body


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
    """Older records must render without 5xx — each subfield guard short-
    circuits cleanly. After PR-319 the template dispatches on
    ``payload.subsource``; records missing the field (whether pre-PR-253
    bare payloads or pre-PR-315 raw legacy categories the migration
    missed) fall through to the legacy-category / generic-error fallback
    branch. ``OPERATOR_RECOVERY`` is the one legacy category the
    ``escalate_to_error`` migration intentionally leaves untouched, so
    the card still renders the dedicated manual-recovery label."""
    client, causes, _captured = cancellations_client
    causes[:] = [
        _make_cause("PR-LEGACY-MIG", category="ERROR", payload={
            "legacy_category": "ESCALATE",
            "reason_text": "manual",
        }),
        _make_cause("PR-LEGACY-RAW", category="CRASH", payload={}),
        _make_cause("PR-LEGACY-NPD-EMPTY", category="ERROR", payload={
            "subsource": "no_push_deadlock",
        }),
        _make_cause("PR-LEGACY-OP", category="OPERATOR_RECOVERY", payload={}),
    ]

    resp = client.get("/partials/repo/example__repo/cancellations")

    assert resp.status_code == 200
    body = resp.text
    assert "PR-LEGACY-MIG" in body
    assert "PR-LEGACY-RAW" in body
    # Migrated record surfaces the preserved legacy category badge.
    assert "Legacy: ESCALATE" in body
    # OPERATOR_RECOVERY keeps its dedicated label per the migration design.
    assert "Manual recovery via dashboard" in body
    # no_push_deadlock without ``attempts`` falls back to the no-count message.
    assert "no push across consecutive cycles" in body
    # Raw un-migrated category falls through to the generic error fallback.
    assert "Cancellation reason not recorded" in body


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


def test_partial_endpoint_renders_filter_dropdown(cancellations_client) -> None:
    """PR-310: the partial renders a subsource filter dropdown with the
    five canonical options (All + 4 groups) regardless of whether causes
    exist for the current window — operators can pre-select a filter on
    an empty repo."""
    client, causes, _captured = cancellations_client
    causes.clear()

    resp = client.get("/partials/repo/example__repo/cancellations")

    assert resp.status_code == 200
    body = resp.text
    assert 'name="subsource_filter"' in body
    assert 'value=""' in body and ">All<" in body
    assert 'value="guardrail"' in body and "Guardrail only" in body
    assert 'value="coder"' in body and "Coder ESCALATE only" in body
    assert 'value="daemon"' in body and "Daemon-detected only" in body
    assert 'value="operator_reject"' in body and "Operator rejections only" in body


def test_partial_endpoint_subsource_filter_guardrail(cancellations_client) -> None:
    """PR-310: ``?subsource_filter=guardrail`` returns only guardrail
    entries; coder_escalate, review_timeout, etc are filtered out."""
    client, causes, _captured = cancellations_client
    now = datetime.now(timezone.utc)
    causes[:] = [
        _make_cause(
            "PR-GR",
            category="ERROR",
            payload={"subsource": "guardrail", "rule": "large_diff"},
            created_at=(now - timedelta(minutes=10)).isoformat(),
        ),
        _make_cause(
            "PR-CODER",
            category="ERROR",
            payload={"subsource": "coder_escalate"},
            created_at=(now - timedelta(minutes=20)).isoformat(),
        ),
        _make_cause(
            "PR-RT",
            category="ERROR",
            payload={"subsource": "review_timeout"},
            created_at=(now - timedelta(minutes=30)).isoformat(),
        ),
    ]

    resp = client.get(
        "/partials/repo/example__repo/cancellations?subsource_filter=guardrail"
    )

    assert resp.status_code == 200
    body = resp.text
    assert "PR-GR" in body
    assert "PR-CODER" not in body
    assert "PR-RT" not in body
    # The selected option round-trips into the dropdown markup so the
    # operator's choice survives the htmx swap.
    assert 'value="guardrail" selected' in body


def test_partial_endpoint_subsource_filter_daemon_grouping(
    cancellations_client,
) -> None:
    """PR-310: ``?subsource_filter=daemon`` returns every daemon-detected
    subsource (review_timeout, FIX timers, no-push deadlock, infra streak,
    watch retrigger cap, raw crash) plus the literal ``"daemon"`` value
    emitted by ``_escalate_and_skip`` and the HUNG→IDLE migration, so
    operators can audit automatic failures in one view."""
    client, causes, _captured = cancellations_client
    now = datetime.now(timezone.utc)
    causes[:] = [
        _make_cause(
            "PR-CRASH",
            category="ERROR",
            payload={"subsource": "crash"},
            created_at=(now - timedelta(minutes=10)).isoformat(),
        ),
        _make_cause(
            "PR-RT",
            category="ERROR",
            payload={"subsource": "review_timeout"},
            created_at=(now - timedelta(minutes=20)).isoformat(),
        ),
        _make_cause(
            "PR-NPD",
            category="ERROR",
            payload={"subsource": "no_push_deadlock"},
            created_at=(now - timedelta(minutes=30)).isoformat(),
        ),
        _make_cause(
            "PR-DAEMON",
            category="ERROR",
            payload={"subsource": "daemon"},
            created_at=(now - timedelta(minutes=35)).isoformat(),
        ),
        _make_cause(
            "PR-WRC",
            category="ERROR",
            payload={"subsource": "watch_retrigger_cap"},
            created_at=(now - timedelta(minutes=38)).isoformat(),
        ),
        _make_cause(
            "PR-GR",
            category="ERROR",
            payload={"subsource": "guardrail"},
            created_at=(now - timedelta(minutes=40)).isoformat(),
        ),
    ]

    resp = client.get(
        "/partials/repo/example__repo/cancellations?subsource_filter=daemon"
    )

    assert resp.status_code == 200
    body = resp.text
    assert "PR-CRASH" in body
    assert "PR-RT" in body
    assert "PR-NPD" in body
    assert "PR-DAEMON" in body
    assert "PR-WRC" in body
    assert "PR-GR" not in body


def test_partial_endpoint_subsource_filter_operator_reject(
    cancellations_client,
) -> None:
    """PR-310: the operator_reject group surfaces only operator-driven
    rejections (PR-305c subsource), distinct from the still-pending
    guardrail subgroup."""
    client, causes, _captured = cancellations_client
    now = datetime.now(timezone.utc)
    causes[:] = [
        _make_cause(
            "PR-REJECT",
            category="ERROR",
            payload={"subsource": "operator_reject"},
            created_at=(now - timedelta(minutes=10)).isoformat(),
        ),
        _make_cause(
            "PR-GR",
            category="ERROR",
            payload={"subsource": "guardrail"},
            created_at=(now - timedelta(minutes=20)).isoformat(),
        ),
    ]

    resp = client.get(
        "/partials/repo/example__repo/cancellations?subsource_filter=operator_reject"
    )

    assert resp.status_code == 200
    body = resp.text
    assert "PR-REJECT" in body
    assert "PR-GR" not in body


def test_partial_endpoint_no_filter_returns_all_subsources(
    cancellations_client,
) -> None:
    """PR-310: omitting ``subsource_filter`` (or passing empty / unknown)
    leaves the candidate set untouched so the default view shows
    everything the 7-day window holds."""
    client, causes, _captured = cancellations_client
    now = datetime.now(timezone.utc)
    causes[:] = [
        _make_cause(
            "PR-GR",
            category="ERROR",
            payload={"subsource": "guardrail"},
            created_at=(now - timedelta(minutes=10)).isoformat(),
        ),
        _make_cause(
            "PR-CODER",
            category="ERROR",
            payload={"subsource": "coder_escalate"},
            created_at=(now - timedelta(minutes=20)).isoformat(),
        ),
    ]

    resp = client.get("/partials/repo/example__repo/cancellations")
    assert resp.status_code == 200
    assert "PR-GR" in resp.text
    assert "PR-CODER" in resp.text

    # Unknown filter values fall through to "no filter" rather than 4xx-ing.
    resp_unknown = client.get(
        "/partials/repo/example__repo/cancellations?subsource_filter=bogus"
    )
    assert resp_unknown.status_code == 200
    assert "PR-GR" in resp_unknown.text
    assert "PR-CODER" in resp_unknown.text


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
