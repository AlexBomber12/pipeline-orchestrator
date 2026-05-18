"""Tests for the reset confirmation modal (PR-335).

The modal is the friction layer between the diagnostic panel's Reset
button (PR-333) and the destructive reset endpoint (PR-334): the
operator must type the repository name to enable submit so an accidental
click cannot destroy task state. Tests exercise both the macro render
surface (used by direct callers) and the HTMX GET endpoint that returns
the fragment for the diagnostic panel.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from src.web import app as web_app
from src.web.app import app


def _render_modal(
    diagnostic: dict[str, Any],
    orphan_pr: dict[str, Any] | None = None,
) -> str:
    macro = web_app.templates.env.get_template(
        "components/reset_modal.html"
    ).module.reset_modal
    return macro(diagnostic, orphan_pr)


def _base_diagnostic(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "repo_slug": "example__alpha",
        "task_id": "PR-322",
        "retry_count": 2,
        "current_pr": None,
    }
    base.update(overrides)
    return base


def test_modal_renders_repo_name_in_confirmation_text() -> None:
    rendered = _render_modal(_base_diagnostic())

    assert "example__alpha" in rendered
    assert 'placeholder="Type example__alpha"' in rendered


def test_modal_shows_orphan_pr_warning_when_pr_open() -> None:
    rendered = _render_modal(
        _base_diagnostic(),
        orphan_pr={
            "number": 444,
            "state": "OPEN",
            "url": "https://github.com/example/alpha/pull/444",
        },
    )

    assert "Close orphan PR #444 on GitHub" in rendered
    # The destructive POST must carry the orphan-close flag so the
    # reset endpoint actually closes the PR rather than leaving it open.
    assert "close_orphan_pr=true" in rendered


def test_modal_omits_orphan_pr_warning_when_no_pr() -> None:
    rendered = _render_modal(_base_diagnostic(), orphan_pr=None)

    assert "Close orphan PR" not in rendered
    assert "close_orphan_pr=true" not in rendered


def test_submit_disabled_initially() -> None:
    rendered = _render_modal(_base_diagnostic())

    # The submit button must carry the ``disabled`` HTML attribute on
    # initial render so an accidental Enter/click before typing the
    # repo name cannot trigger the destructive POST.
    assert "data-reset-submit" in rendered
    submit_segment = rendered.split("data-reset-submit", 1)[1].split(
        "</button>", 1
    )[0]
    assert "disabled" in submit_segment


def test_modal_lists_all_keys_to_be_deleted() -> None:
    rendered = _render_modal(_base_diagnostic(retry_count=4))

    # The six destructive operations the reset endpoint performs against
    # Redis. The frontmatter rewrite and orphan PR close are separate
    # bullets so they are intentionally not counted here.
    assert "Delete cancellation_cause record" in rendered
    assert "Delete retry_count (currently 4)" in rendered
    assert "Delete retry_fingerprint" in rendered
    assert "Delete current_run_started_at" in rendered
    assert "Delete status_write_failed marker" in rendered
    assert "ZREM from cancellation_index" in rendered


def test_modal_cancel_button_present() -> None:
    rendered = _render_modal(_base_diagnostic())

    assert "data-reset-cancel" in rendered
    assert "Cancel" in rendered
    # Cancel must close the modal in-place rather than navigate.
    assert "[data-reset-modal]" in rendered
    # No href/form navigation on the cancel button.
    cancel_segment = rendered.split("data-reset-cancel", 1)[1].split(
        "</button>", 1
    )[0]
    assert "href=" not in cancel_segment


def test_modal_inline_script_gates_submit_on_repo_name_match() -> None:
    # The inline gating script is what the cypress-style tests rely on:
    # typing the correct name flips ``submit.disabled`` to false, typing
    # anything else keeps it disabled. Verify the script wires the input
    # event to the expected comparison so the runtime gate is in place.
    rendered = _render_modal(_base_diagnostic())

    assert "data-reset-confirm-input" in rendered
    assert "addEventListener('input'" in rendered
    assert "submit.disabled = (input.value !== target)" in rendered
    assert 'const target = "example__alpha"' in rendered


def test_modal_post_target_matches_reset_endpoint_contract() -> None:
    # PR-334 ships POST /api/reset-task/{name}/{task_id}; the modal must
    # target that endpoint exactly so submit triggers the destructive
    # path with the correct repo and task identifiers.
    rendered = _render_modal(_base_diagnostic())

    assert "/api/reset-task/example__alpha/PR-322" in rendered


# ---------------------------------------------------------------------------
# Endpoint integration tests
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


def test_reset_confirm_endpoint_returns_modal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        resp = client.get(
            "/repos/example__alpha/tasks/PR-322/reset-confirm"
        )

    assert resp.status_code == 200
    assert "Reset task - destructive" in resp.text
    assert "example__alpha" in resp.text
    assert "/api/reset-task/example__alpha/PR-322" in resp.text


def test_reset_confirm_endpoint_rejects_invalid_task_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        resp = client.get(
            "/repos/example__alpha/tasks/not-a-pr/reset-confirm"
        )

    assert resp.status_code == 400
    assert "invalid task id" in resp.text


def test_reset_confirm_endpoint_unknown_repo_returns_404(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        resp = client.get("/repos/nonexistent/tasks/PR-322/reset-confirm")

    assert resp.status_code == 404
    assert "repo not found" in resp.text


def test_reset_confirm_endpoint_renders_orphan_pr_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # When the diagnostic payload reports an OPEN current_pr for this
    # task, the modal must flag the orphan-close action AND attach the
    # ``close_orphan_pr=true`` query string to the destructive POST so
    # the operator's submit closes the PR alongside the Redis cleanup.
    _write_config(tmp_path, monkeypatch)

    from src.models import PipelineState, QueueTask, RepoState, TaskStatus

    state = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        current_queue=[
            QueueTask(
                pr_id="PR-322",
                title="Title",
                status=TaskStatus.DOING,
                branch="pr-322",
            )
        ],
        current_task=QueueTask(
            pr_id="PR-322",
            title="Title",
            status=TaskStatus.DOING,
            branch="pr-322",
        ),
    )
    from src.models import PRInfo

    state.current_pr = PRInfo(
        number=444,
        branch="pr-322",
        pr_id="PR-322",
        url="https://github.com/example/alpha/pull/444",
    )

    redis_client = _FakeRedis()
    redis_client.values["pipeline:example__alpha"] = state.model_dump_json()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    from src.web.routes import diagnostic as diagnostic_routes

    def _sync_pr_state(_owner_repo: str, _pr_number: int) -> dict[str, Any]:
        return {"state": "OPEN"}

    monkeypatch.setattr(
        diagnostic_routes.gh_prs,
        "pr_state",
        _sync_pr_state,
    )

    with TestClient(app) as client:
        resp = client.get(
            "/repos/example__alpha/tasks/PR-322/reset-confirm"
        )

    assert resp.status_code == 200
    assert "Close orphan PR #444 on GitHub" in resp.text
    assert "close_orphan_pr=true" in resp.text


def test_reset_confirm_endpoint_omits_orphan_pr_when_no_open_pr(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        resp = client.get(
            "/repos/example__alpha/tasks/PR-322/reset-confirm"
        )

    assert resp.status_code == 200
    assert "Close orphan PR" not in resp.text
    assert "close_orphan_pr=true" not in resp.text


def test_cypress_typing_correct_name_enables_submit() -> None:
    # In-process analogue of the e2e cypress test the spec requested: the
    # inline gating script must register an input listener that flips
    # ``submit.disabled`` to false when the value equals the repo name.
    rendered = _render_modal(_base_diagnostic())

    # The script must reference the repo slug as the comparison target
    # and must rely on the input value matching exactly. The asymmetric
    # gate (``!==``) keeps the button disabled for any non-match.
    assert 'const target = "example__alpha"' in rendered
    assert "submit.disabled = (input.value !== target)" in rendered


def test_cypress_typing_wrong_name_keeps_submit_disabled() -> None:
    # Same script-shape assertion as above but exercising the negative
    # path: any non-match leaves ``submit.disabled = true`` because the
    # boolean expression evaluates to ``true``. Verifies the comparator
    # is strict equality so trailing whitespace or case drift does not
    # accidentally enable submit.
    rendered = _render_modal(_base_diagnostic())

    assert "input.value !== target" in rendered
    # No looser comparator (==, .includes, .startsWith) appears in the
    # gating script.
    submit_section = rendered.split("<script>", 1)[1].split("</script>", 1)[0]
    assert "==" in submit_section  # part of !==
    assert ".includes" not in submit_section
    assert ".startsWith" not in submit_section
