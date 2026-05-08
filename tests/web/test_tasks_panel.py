"""Tests for task queue retry controls."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from src.models import PipelineState, QueueTask, RepoState, TaskStatus
from src.web import app as web_app
from src.web.app import app
from src.web.routes import repo_control


class _PanelRedis:
    def __init__(self, store: dict[str, str]) -> None:
        self.store = store

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def aclose(self) -> None:
        return None


def _aioredis(redis_client: _PanelRedis) -> object:
    return type(
        "_Aioredis",
        (),
        {"from_url": staticmethod(lambda url, decode_responses=True: redis_client)},
    )()


def _setup_panel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    retry_count: int,
    stored_fingerprint: str | None = None,
    task_body: str = "Body",
) -> None:
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
    (tmp_path / "repos" / "example__alpha" / "tasks" / "PR-283.md").write_text(
        f"---\nstatus: ERROR\n---\n\n# PR-283: Retry me\n\n{task_body}\n",
        encoding="utf-8",
    )
    state = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        current_queue=[
            QueueTask(
                pr_id="PR-283",
                title="Retry me",
                status=TaskStatus.ERROR,
                branch="pr-283",
            )
        ],
    )
    store = {
        "pipeline:example__alpha": state.model_dump_json(),
        "metrics:retry_count:example__alpha:PR-283": str(retry_count),
    }
    if stored_fingerprint is not None:
        store["metrics:retry_fingerprint:example__alpha:PR-283"] = stored_fingerprint
    monkeypatch.setattr(web_app, "aioredis", _aioredis(_PanelRedis(store)))


def test_renders_retry_button_when_below_cap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _setup_panel(tmp_path, monkeypatch, retry_count=2)

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks")

    assert response.status_code == 200
    body = response.text
    assert 'hx-post="/repos/example__alpha/tasks/PR-283/retry"' in body
    assert 'hx-confirm="Retry PR-283? Counter will increment to 3/3."' in body
    assert "Retry count 2/3" in body
    assert "disabled" not in body


def test_renders_disabled_button_at_cap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _setup_panel(tmp_path, monkeypatch, retry_count=3)

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks")

    assert response.status_code == 200
    body = response.text
    assert 'hx-post="/repos/example__alpha/tasks/PR-283/retry"' not in body
    assert "Retry count 3/3. Edit task spec or delete to proceed." in body
    assert "disabled" in body


def test_renders_retry_button_when_stale_fingerprint_count_is_at_cap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _setup_panel(
        tmp_path,
        monkeypatch,
        retry_count=3,
        stored_fingerprint="old",
        task_body="Changed spec",
    )

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks")

    assert response.status_code == 200
    body = response.text
    assert 'hx-post="/repos/example__alpha/tasks/PR-283/retry"' in body
    assert 'hx-confirm="Retry PR-283? Counter will increment to 1/3."' in body
    assert "Retry count 0/3" in body


def test_renders_raw_retry_count_when_fingerprint_read_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _setup_panel(tmp_path, monkeypatch, retry_count=2)

    def fake_fingerprint(task_path: Path) -> str:
        raise OSError("cannot read")

    monkeypatch.setattr(repo_control, "_task_retry_fingerprint", fake_fingerprint)

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks")

    assert response.status_code == 200
    assert "Retry count 2/3" in response.text
