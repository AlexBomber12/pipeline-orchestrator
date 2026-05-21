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
from src.web.routes import uploads as upload_routes

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


def test_idle_upload_extending_pending_manifest_keeps_pending_count(
    uploads_dir: Path,
) -> None:
    old_staging = uploads_dir / "example__alpha" / "existing"
    old_staging.mkdir(parents=True)
    (old_staging / "PR-001.md").write_text("# PR-001\n", encoding="utf-8")

    with TestClient(app) as client:
        client.app.state.redis._store[upload_pending("example__alpha")] = (
            json.dumps(
                {
                    "repo": "example__alpha",
                    "files": ["PR-001.md"],
                    "staging_dir": str(old_staging),
                }
            )
        )
        client.app.state.redis._store[
            upload_pending_count("example__alpha")
        ] = "1"

        response = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[_task_file(name="PR-002.md", pr_id="PR-002")],
        )

        assert response.status_code == 200
        assert (
            client.app.state.redis._store[
                upload_pending_count("example__alpha")
            ]
            == "2"
        )


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


def test_pending_count_failure_rolls_back_manifest() -> None:
    class _CountFailRedis:
        def __init__(self) -> None:
            self._store: dict[str, str] = {
                pipeline_state("example__alpha"): _pipeline_payload(
                    PipelineState.CODING
                )
            }

        async def get(self, key: str) -> str | None:
            return self._store.get(key)

        async def set(self, key: str, value: str, **kwargs: object) -> None:
            del kwargs
            if key == upload_pending_count("example__alpha"):
                raise RuntimeError("count write failed")
            self._store[key] = value

        async def delete(self, key: str) -> int:
            existed = key in self._store
            self._store.pop(key, None)
            return int(existed)

        async def scan_iter(self, match: str | None = None):
            del match
            if False:
                yield ""

    with TestClient(app) as client:
        redis_client = _CountFailRedis()
        client.app.state.redis = redis_client

        response = _upload_three(client)

        assert response.status_code == 503
        assert upload_pending("example__alpha") not in redis_client._store
        assert upload_pending_count("example__alpha") not in redis_client._store


def test_enqueue_upload_manifest_uses_lua_when_available() -> None:
    class _EvalRedis:
        def __init__(self) -> None:
            self.calls: list[tuple[str, int, tuple[str, ...]]] = []

        async def eval(self, script: str, numkeys: int, *args: str) -> int:
            self.calls.append((script, numkeys, args))
            return 1

    redis_client = _EvalRedis()

    asyncio.run(
        upload_routes._enqueue_upload_manifest(
            redis_client,
            pending_key=upload_pending("example__alpha"),
            manifest_json='{"files":["PR-001.md"]}',
            count_key=upload_pending_count("example__alpha"),
            pending_count=1,
        )
    )

    assert redis_client.calls == [
        (
            upload_routes._ENQUEUE_UPLOAD_PENDING_LUA,
            2,
            (
                upload_pending("example__alpha"),
                upload_pending_count("example__alpha"),
                '{"files":["PR-001.md"]}',
                "1",
            ),
        )
    ]


def test_enqueue_upload_manifest_falls_back_when_lua_fails() -> None:
    pending_key = upload_pending("example__alpha")
    count_key = upload_pending_count("example__alpha")

    class _EvalFailRedis:
        def __init__(self) -> None:
            self.store: dict[str, object] = {}

        async def eval(self, script: str, numkeys: int, *args: str) -> int:
            del script, numkeys, args
            raise RuntimeError("eval disabled")

        async def get(self, key: str) -> object:
            return self.store.get(key)

        async def set(self, key: str, value: object, **kwargs: object) -> None:
            del kwargs
            self.store[key] = value

        async def delete(self, key: str) -> int:
            existed = key in self.store
            self.store.pop(key, None)
            return int(existed)

    redis_client = _EvalFailRedis()

    asyncio.run(
        upload_routes._enqueue_upload_manifest(
            redis_client,
            pending_key=pending_key,
            manifest_json='{"files":["PR-001.md"]}',
            count_key=count_key,
            pending_count=1,
        )
    )

    assert redis_client.store[pending_key] == '{"files":["PR-001.md"]}'
    assert redis_client.store[count_key] == "1"


def test_rollback_upload_manifest_deletes_matching_bytes_manifest() -> None:
    class _RollbackRedis:
        def __init__(self) -> None:
            self.store: dict[str, object] = {
                upload_pending("example__alpha"): b'{"files":["PR-001.md"]}'
            }

        async def get(self, key: str) -> object:
            return self.store.get(key)

        async def delete(self, key: str) -> int:
            existed = key in self.store
            self.store.pop(key, None)
            return int(existed)

    redis_client = _RollbackRedis()

    asyncio.run(
        upload_routes._rollback_upload_manifest(
            redis_client,
            upload_pending("example__alpha"),
            '{"files":["PR-001.md"]}',
            None,
        )
    )

    assert upload_pending("example__alpha") not in redis_client.store


def test_enqueue_upload_manifest_restores_previous_manifest_on_count_failure() -> None:
    pending_key = upload_pending("example__alpha")
    count_key = upload_pending_count("example__alpha")
    previous_manifest = b'{"files":["PR-000.md"]}'

    class _CountFailRedis:
        def __init__(self) -> None:
            self.store: dict[str, object] = {pending_key: previous_manifest}

        async def get(self, key: str) -> object:
            return self.store.get(key)

        async def set(self, key: str, value: object, **kwargs: object) -> None:
            del kwargs
            if key == count_key:
                raise RuntimeError("count write failed")
            self.store[key] = value

        async def delete(self, key: str) -> int:
            existed = key in self.store
            self.store.pop(key, None)
            return int(existed)

    redis_client = _CountFailRedis()

    with pytest.raises(RuntimeError, match="count write failed"):
        asyncio.run(
            upload_routes._enqueue_upload_manifest(
                redis_client,
                pending_key=pending_key,
                manifest_json='{"files":["PR-001.md"]}',
                count_key=count_key,
                pending_count=1,
            )
        )

    assert redis_client.store[pending_key] == previous_manifest


def test_rollback_upload_manifest_leaves_newer_manifest() -> None:
    pending_key = upload_pending("example__alpha")

    class _RollbackRedis:
        def __init__(self) -> None:
            self.store: dict[str, object] = {
                pending_key: '{"files":["PR-999.md"]}'
            }

        async def get(self, key: str) -> object:
            return self.store.get(key)

        async def delete(self, key: str) -> int:
            existed = key in self.store
            self.store.pop(key, None)
            return int(existed)

    redis_client = _RollbackRedis()

    asyncio.run(
        upload_routes._rollback_upload_manifest(
            redis_client,
            pending_key,
            '{"files":["PR-001.md"]}',
            None,
        )
    )

    assert redis_client.store[pending_key] == '{"files":["PR-999.md"]}'


def test_rollback_upload_manifest_ignores_fallback_lookup_error() -> None:
    class _FallbackRedis:
        async def get(self, key: str) -> str | None:
            del key
            raise RuntimeError("redis down")

    asyncio.run(
        upload_routes._rollback_upload_manifest(
            _FallbackRedis(),
            upload_pending("example__alpha"),
            '{"files":["PR-001.md"]}',
            None,
        )
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
