"""Upload-pending badge coverage for dashboard repo cards."""

from __future__ import annotations

import asyncio
import json
import subprocess
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from src.daemon import repo_ops
from src.keyspace import pipeline_state, upload_pending, upload_pending_count
from src.models import PipelineState, RepoState
from src.web import app as web_app
from src.web.app import app, templates
from src.web.routes import dashboard as dashboard_routes

from tests.test_repo_ops import _FakeCompletedProcess, _Runner
from tests.test_upload import _StubAioredis, _task_file

pytestmark = pytest.mark.usefixtures("one_repo_config", "repo_dir", "uploads_dir")


@pytest.fixture
def one_repo_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())
    return cfg


@pytest.fixture
def repo_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    repos = tmp_path / "repos"
    repos.mkdir()
    alpha = repos / "example__alpha"
    alpha.mkdir()
    monkeypatch.setattr(web_app, "REPOS_DIR", str(repos))
    return alpha


@pytest.fixture
def uploads_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    uploads = tmp_path / "uploads"
    uploads.mkdir()
    monkeypatch.setattr(web_app, "UPLOADS_DIR", str(uploads))
    return uploads


def _upload_three(client: TestClient) -> Any:
    return client.post(
        "/repos/example__alpha/upload-tasks",
        files=[
            _task_file(name="PR-001.md", pr_id="PR-001"),
            _task_file(name="PR-002.md", pr_id="PR-002"),
            _task_file(name="PR-003.md", pr_id="PR-003"),
        ],
    )


def _pipeline_payload(state: PipelineState) -> str:
    return RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=state,
    ).model_dump_json()


def _render_cards(*, pending_count: int) -> str:
    repo = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        upload_pending_count=pending_count,
    )
    return templates.get_template("components/repo_cards.html").render(
        repos=[repo],
        resources=None,
        cancellation_subsources={},
        subsource_lookup=lambda _name: None,
        drain_progress={},
        inhibitor_labels={},
        css_escape=lambda v: v,
        upload_feedback_target=lambda _name: "",
    )


def test_upload_during_idle_no_pending_badge() -> None:
    with TestClient(app) as client:
        response = _upload_three(client)
        assert response.status_code == 200
        assert (
            upload_pending_count("example__alpha")
            not in client.app.state.redis._store
        )

    html = _render_cards(pending_count=0)
    assert "Upload pending" not in html


def test_upload_during_coding_sets_pending_count() -> None:
    with TestClient(app) as client:
        client.app.state.redis._store[pipeline_state("example__alpha")] = (
            _pipeline_payload(PipelineState.CODING)
        )

        response = _upload_three(client)

        assert response.status_code == 200
        assert (
            client.app.state.redis._store[
                upload_pending_count("example__alpha")
            ]
            == "3"
        )


def test_pending_count_cleared_after_deferred_commit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    repo_dir = Path(runner.repo_path)
    repo_dir.mkdir(parents=True)
    staging = tmp_path / "uploads" / "demo"
    staging.mkdir(parents=True)
    (staging / "PR-001.md").write_text("# PR-001\n", encoding="utf-8")
    manifest = json.dumps(
        {"files": ["PR-001.md"], "staging_dir": str(staging)}
    )
    runner.redis.store[upload_pending(runner.name)] = manifest
    runner.redis.store[upload_pending_count(runner.name)] = "1"

    monkeypatch.setattr(
        repo_ops,
        "retry_transient",
        lambda func, operation_name=None: func(),
    )
    monkeypatch.setattr(
        repo_ops.git_ops,
        "_git",
        lambda *args, **kwargs: _FakeCompletedProcess(),
    )

    assert asyncio.run(runner.process_pending_uploads()) is True
    assert upload_pending_count(runner.name) not in runner.redis.store


def test_pending_count_cleared_on_commit_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _Runner(tmp_path)
    repo_dir = Path(runner.repo_path)
    repo_dir.mkdir(parents=True)
    staging = tmp_path / "uploads" / "demo"
    staging.mkdir(parents=True)
    (staging / "PR-001.md").write_text("# PR-001\n", encoding="utf-8")
    manifest = json.dumps(
        {"files": ["PR-001.md"], "staging_dir": str(staging)}
    )
    runner.redis.store[upload_pending(runner.name)] = manifest
    runner.redis.store[upload_pending_count(runner.name)] = "1"

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> _FakeCompletedProcess:
        if args[:1] == ("add",):
            raise subprocess.CalledProcessError(1, args, stderr="git error")
        return _FakeCompletedProcess()

    monkeypatch.setattr(repo_ops.git_ops, "_git", fake_git)

    assert asyncio.run(runner.process_pending_uploads()) is None
    assert upload_pending(runner.name) in runner.redis.store
    assert upload_pending_count(runner.name) not in runner.redis.store


def test_pending_count_clear_ignores_redis_error(tmp_path: Path) -> None:
    runner = _Runner(tmp_path)
    runner.redis.get_error = RuntimeError("redis down")

    asyncio.run(
        runner._clear_upload_pending_count_if_manifest_matches(
            upload_pending(runner.name),
            "{}",
        )
    )


def test_dashboard_payload_includes_pending_count() -> None:
    with TestClient(app) as client:
        client.app.state.redis._store[upload_pending_count("example__alpha")] = "3"
        response = client.get("/api/states")

    assert response.status_code == 200
    assert response.json()[0]["upload_pending_count"] == 3


def test_dashboard_payload_zero_when_no_pending() -> None:
    with TestClient(app) as client:
        response = client.get("/api/states")

    assert response.status_code == 200
    assert response.json()[0]["upload_pending_count"] == 0


def test_dashboard_pending_count_helper_handles_missing_redis() -> None:
    assert (
        asyncio.run(
            dashboard_routes.get_upload_pending_count("example__alpha", None)
        )
        == 0
    )


def test_dashboard_pending_count_helper_handles_redis_error() -> None:
    class _FailingRedis:
        async def get(self, key: str) -> str:
            raise RuntimeError("redis down")

    assert (
        asyncio.run(
            dashboard_routes.get_upload_pending_count(
                "example__alpha",
                _FailingRedis(),  # type: ignore[arg-type]
            )
        )
        == 0
    )


def test_dashboard_pending_count_helper_handles_invalid_value() -> None:
    class _BadRedis:
        async def get(self, key: str) -> str:
            return "not-an-int"

    assert (
        asyncio.run(
            dashboard_routes.get_upload_pending_count(
                "example__alpha",
                _BadRedis(),  # type: ignore[arg-type]
            )
        )
        == 0
    )


def test_repo_card_template_renders_pending_badge() -> None:
    html = _render_cards(pending_count=5)

    assert "data-upload-pending-badge" in html
    assert "Upload pending (5)" in html


def test_repo_card_template_omits_badge_when_zero() -> None:
    html = _render_cards(pending_count=0)

    assert "data-upload-pending-badge" not in html
    assert "Upload pending" not in html
