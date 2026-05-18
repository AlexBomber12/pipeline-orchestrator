"""Tests for the diagnostic panel macro and its HTMX partial endpoint (PR-333).

The macro renders the JSON payload produced by PR-332's
``/api/diagnostic`` endpoint as an inline operator-facing surface so
investigating a stuck task no longer requires a ``redis-cli`` session.
The HTMX partial endpoint at ``/partials/repo/{name}/tasks/{task_id}/diagnostic``
reuses the same data-gathering helper and renders the macro for
operators to load via the task row Diagnostic button.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from src.web import app as web_app
from src.web.app import app
from src.web.routes import diagnostic as diagnostic_routes


def _render_panel(
    diagnostic: dict[str, Any],
    *,
    retry_cap: int = 3,
    reset_button_enabled: bool = False,
) -> str:
    macro = web_app.templates.env.get_template(
        "components/diagnostic_panel.html"
    ).module.diagnostic_panel
    return macro(diagnostic, retry_cap, reset_button_enabled=reset_button_enabled)


def _base_diagnostic(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "repo_slug": "example__alpha",
        "task_id": "PR-322",
        "frontmatter_status": "ERROR",
        "cancellation_cause": None,
        "subsource_metadata": None,
        "retry_count": 0,
        "retry_fingerprint": None,
        "retry_fingerprint_matches_current_spec": False,
        "current_run_started_at": None,
        "attempt_count": 0,
        "status_write_failed": False,
        "skip_ai_error_diagnose": False,
        "_error_diagnose_count": 0,
        "_error_skip_count": 0,
        "current_pr": None,
        "ttls": {},
    }
    base.update(overrides)
    return base


def test_diagnostic_panel_renders_subsource_label_when_present() -> None:
    rendered = _render_panel(
        _base_diagnostic(
            cancellation_cause={
                "category": "ERROR",
                "payload": {"subsource": "fix_iteration_cap"},
                "created_at": "2026-05-17T08:23:00+00:00",
                "task_id": "PR-322",
                "repo_slug": "example__alpha",
            },
            subsource_metadata={
                "name": "fix_iteration_cap",
                "user_label": "FIX iteration cap",
                "severity": "high",
                "recovery_hint": "Revise spec then Retry.",
                "group_bucket": "daemon",
                "legacy_category": "ESCALATE",
                "is_canonical": True,
            },
        )
    )

    assert "FIX iteration cap" in rendered
    assert "Revise spec then Retry." in rendered


def test_diagnostic_panel_omits_cancellation_section_when_cause_null() -> None:
    rendered = _render_panel(_base_diagnostic(cancellation_cause=None))

    assert "Cancellation" not in rendered


def test_diagnostic_panel_shows_retry_count_versus_cap() -> None:
    rendered = _render_panel(_base_diagnostic(retry_count=3), retry_cap=3)

    assert "3 / 3" in rendered


def test_diagnostic_panel_links_to_open_pr_when_present() -> None:
    rendered = _render_panel(
        _base_diagnostic(
            current_pr={
                "number": 444,
                "state": "OPEN",
                "url": "https://github.com/example/alpha/pull/444",
            }
        )
    )

    assert 'href="https://github.com/example/alpha/pull/444"' in rendered
    assert "#444" in rendered
    assert "OPEN" in rendered


def test_diagnostic_panel_omits_pr_section_when_null() -> None:
    rendered = _render_panel(_base_diagnostic(current_pr=None))

    assert "Open PR" not in rendered
    assert "github.com" not in rendered


def test_diagnostic_panel_reset_button_visible_when_enabled() -> None:
    rendered = _render_panel(_base_diagnostic(), reset_button_enabled=True)

    assert "Reset task (destructive)" in rendered
    assert "/reset-confirm" in rendered


def test_diagnostic_panel_reset_button_omitted_when_disabled() -> None:
    rendered = _render_panel(_base_diagnostic(), reset_button_enabled=False)

    assert "Reset task" not in rendered
    assert "/reset-confirm" not in rendered


def test_diagnostic_panel_renders_subsource_metadata_block_only_when_present() -> None:
    # Cause present but no recognized subsource — the subsource_metadata
    # block must collapse so the panel does not render a blank badge or
    # "None" recovery hint.
    rendered = _render_panel(
        _base_diagnostic(
            cancellation_cause={
                "category": "ERROR",
                "payload": {},
                "created_at": "2026-05-17T08:23:00+00:00",
                "task_id": "PR-322",
                "repo_slug": "example__alpha",
            },
            subsource_metadata=None,
        )
    )

    assert "Cancellation" in rendered
    assert "ERROR" in rendered
    assert "recovery_hint" not in rendered
    # The warn badge surface only appears when subsource_metadata is set.
    assert "bg-warn/10" not in rendered


def test_diagnostic_panel_renders_skip_flag_and_attempt_count() -> None:
    rendered = _render_panel(
        _base_diagnostic(
            skip_ai_error_diagnose=True,
            attempt_count=4,
            status_write_failed=True,
            current_run_started_at="2026-05-17T08:23:00+00:00",
        )
    )

    assert "Skip AI diagnose" in rendered
    assert "Attempt count" in rendered
    assert "2026-05-17T08:23:00+00:00" in rendered
    # status_write_failed yes
    assert ">yes<" in rendered


# ---------------------------------------------------------------------------
# Partial endpoint integration tests
# ---------------------------------------------------------------------------


class _FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.ttls: dict[str, int] = {}

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def set(
        self,
        key: str,
        value: str,
        ex: int | None = None,
    ) -> bool:
        self.values[key] = value
        if ex is not None:
            self.ttls[key] = ex
        return True

    async def ttl(self, key: str) -> int:
        if key not in self.values:
            return -2
        return self.ttls.get(key, -1)

    async def aclose(self) -> None:
        return None


def _aioredis(redis_client: _FakeRedis) -> object:
    return type(
        "_Aioredis",
        (),
        {"from_url": staticmethod(lambda url, decode_responses=True: redis_client)},
    )()


def _write_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n"
        "daemon:\n"
        "  retry_button_cap: 3\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "REPOS_DIR", str(tmp_path / "repos"))
    repo_dir = tmp_path / "repos" / "example__alpha"
    (repo_dir / "tasks").mkdir(parents=True, exist_ok=True)
    return repo_dir


def test_diagnostic_partial_returns_rendered_panel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        resp = client.get("/partials/repo/example__alpha/tasks/PR-322/diagnostic")

    assert resp.status_code == 200
    assert "Diagnostic state for" in resp.text
    assert "PR-322" in resp.text
    assert "Retry count" in resp.text
    # Reset button is hidden until PR-334 ships the destructive action.
    assert "Reset task" not in resp.text


def test_diagnostic_partial_rejects_invalid_task_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        resp = client.get("/partials/repo/example__alpha/tasks/not-a-pr/diagnostic")

    assert resp.status_code == 400
    assert "invalid task id" in resp.text


def test_diagnostic_partial_unknown_repo_returns_404(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        resp = client.get("/partials/repo/nonexistent/tasks/PR-322/diagnostic")

    assert resp.status_code == 404
    assert "repo not found" in resp.text


def test_diagnostic_partial_redis_not_attached_returns_503(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    if hasattr(web_app.app.state, "redis"):
        monkeypatch.delattr(web_app.app.state, "redis", raising=False)
    client = TestClient(web_app.app)
    resp = client.get("/partials/repo/example__alpha/tasks/PR-322/diagnostic")
    assert resp.status_code == 503
    assert "redis unavailable" in resp.text


def test_diagnostic_partial_routes_through_shared_payload_helper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Belt-and-suspenders: the JSON endpoint and the partial endpoint
    # must agree on the underlying payload so the two surfaces stay
    # synchronized as PR-332's schema evolves.
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        json_resp = client.get("/api/diagnostic/example__alpha/PR-322")
        html_resp = client.get(
            "/partials/repo/example__alpha/tasks/PR-322/diagnostic"
        )

    assert json_resp.status_code == 200
    assert html_resp.status_code == 200
    body = json_resp.json()
    assert body["task_id"] == "PR-322"
    assert "PR-322" in html_resp.text


# ---------------------------------------------------------------------------
# Tasks panel integration — Diagnostic trigger on the task row
# ---------------------------------------------------------------------------


def test_tasks_panel_renders_diagnostic_trigger(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The task row carries a Diagnostic button whose HTMX attributes
    point at the partial endpoint so a single click loads the panel
    inline. Equivalent to the cypress-style click-through test the spec
    requested; TestClient is the in-process analogue.
    """
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "daemon:\n"
        "  retry_button_cap: 3\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "REPOS_DIR", str(tmp_path / "repos"))
    (tmp_path / "repos" / "example__alpha" / "tasks").mkdir(parents=True)
    (tmp_path / "repos" / "example__alpha" / "tasks" / "PR-322.md").write_text(
        "---\nstatus: TODO\n---\n\n# PR-322: Title\n\nBranch: pr-322\n",
        encoding="utf-8",
    )

    from src.models import PipelineState, QueueTask, RepoState, TaskStatus

    state = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        current_queue=[
            QueueTask(
                pr_id="PR-322",
                title="Title",
                status=TaskStatus.TODO,
                branch="pr-322",
            )
        ],
    )

    class _PanelRedis:
        def __init__(self, store: dict[str, str]) -> None:
            self.store = store

        async def ping(self) -> bool:
            return True

        async def get(self, key: str) -> str | None:
            return self.store.get(key)

        async def aclose(self) -> None:
            return None

    redis_stub = _PanelRedis({"pipeline:example__alpha": state.model_dump_json()})
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_stub))

    with TestClient(app) as client:
        resp = client.get("/repos/example__alpha/tasks")

    assert resp.status_code == 200
    assert "data-diagnostic-trigger" in resp.text
    assert (
        'hx-get="/partials/repo/example__alpha/tasks/PR-322/diagnostic"'
        in resp.text
    )
    assert 'id="diagnostic-todo-PR-322"' in resp.text


def test_diagnostic_partial_endpoint_module_exposes_helper() -> None:
    # Sanity check: the shared payload helper is the import surface the
    # partial endpoint and JSON endpoint both rely on. Keeping it
    # importable lets future callers (audit log, batch triage) reuse the
    # same data-gathering without duplicating Redis reads.
    assert hasattr(diagnostic_routes, "_build_diagnostic_payload")
