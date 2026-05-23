"""Guardrail park card and repo-level reset controls."""

from __future__ import annotations

import inspect
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from redis.exceptions import RedisError
from src.cancellation.storage import CancellationCause, cause_key, index_key
from src.models import PipelineState, QueueTask, RepoState, TaskStatus
from src.web import app as web_app
from src.web.app import app
from src.web.routes import repo_control


class _GuardrailRedis:
    def __init__(self, store: dict[str, str] | None = None) -> None:
        self.store = store or {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.deleted: list[str] = []
        self.wakes: list[tuple[str, str]] = []

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    def set(
        self,
        key: str,
        value: str,
        ex: int | None = None,
        nx: bool = False,
    ) -> bool:
        if nx and key in self.store:
            return False
        self.store[key] = value
        return True

    async def delete(self, key: str) -> int:
        self.deleted.append(key)
        return int(self.store.pop(key, None) is not None)

    async def zadd(self, key: str, mapping: dict[str, float]) -> int:
        bucket = self.zsets.setdefault(key, {})
        for member, score in mapping.items():
            bucket[member] = float(score)
        return len(mapping)

    async def zrem(self, key: str, *members: str) -> int:
        bucket = self.zsets.setdefault(key, {})
        return sum(1 for member in members if bucket.pop(member, None) is not None)

    async def zrangebyscore(self, key: str, start: str, stop: str) -> list[str]:
        return []

    async def exists(self, key: str) -> bool:
        return key in self.store

    async def expire(self, key: str, seconds: int) -> bool:
        return key in self.store or key in self.zsets

    def pipeline(self, transaction: bool = True) -> "_GuardrailPipeline":
        return _GuardrailPipeline(self)

    async def transaction(
        self,
        callback: Any,
        *keys: str,
        value_from_callable: bool = False,
    ) -> Any:
        result = await callback(self)
        return result if value_from_callable else None

    def multi(self) -> None:
        return None

    async def aclose(self) -> None:
        return None


class _NoRedis:
    async def aclose(self) -> None:
        return None


class _GetFailsRedis(_GuardrailRedis):
    async def get(self, key: str) -> str | None:
        raise RedisError("down")


class _StateGetFailsRedis(_GuardrailRedis):
    async def get(self, key: str) -> str | None:
        if key == "pipeline:example__alpha":
            raise RedisError("state down")
        return await super().get(key)


class _TransactionFailsRedis(_GuardrailRedis):
    async def transaction(
        self,
        callback: Any,
        *keys: str,
        value_from_callable: bool = False,
    ) -> Any:
        raise RedisError("down")


class _StateChangesBeforeTransactionRedis(_GuardrailRedis):
    async def transaction(
        self,
        callback: Any,
        *keys: str,
        value_from_callable: bool = False,
    ) -> Any:
        raw = self.store["pipeline:example__alpha"]
        state = RepoState.model_validate_json(raw)
        state.state = PipelineState.CODING
        state.current_task = QueueTask(
            pr_id="PR-999",
            title="New active task",
            status=TaskStatus.DOING,
            task_file="tasks/PR-999.md",
        )
        self.store["pipeline:example__alpha"] = state.model_dump_json()
        result = await callback(self)
        return result if value_from_callable else None


class _SecondTransactionFailsRedis(_GuardrailRedis):
    def __init__(self, store: dict[str, str] | None = None) -> None:
        super().__init__(store)
        self.transactions = 0

    async def transaction(
        self,
        callback: Any,
        *keys: str,
        value_from_callable: bool = False,
    ) -> Any:
        self.transactions += 1
        if self.transactions == 2:
            raise RedisError("down")
        result = await callback(self)
        return result if value_from_callable else None


class _StateChangesBeforeFinalTransactionRedis(_GuardrailRedis):
    def __init__(self, store: dict[str, str] | None = None) -> None:
        super().__init__(store)
        self.transactions = 0

    async def transaction(
        self,
        callback: Any,
        *keys: str,
        value_from_callable: bool = False,
    ) -> Any:
        self.transactions += 1
        if self.transactions == 2:
            raw = self.store["pipeline:example__alpha"]
            state = RepoState.model_validate_json(raw)
            state.state = PipelineState.CODING
            state.current_task = QueueTask(
                pr_id="PR-999",
                title="New active task",
                status=TaskStatus.DOING,
                task_file="tasks/PR-999.md",
            )
            self.store["pipeline:example__alpha"] = state.model_dump_json()
        result = await callback(self)
        return result if value_from_callable else None


class _DeleteFailsRedis(_GuardrailRedis):
    async def delete(self, key: str) -> int:
        raise RuntimeError("delete failed")


class _GuardrailPipeline:
    def __init__(self, redis_client: _GuardrailRedis) -> None:
        self.redis = redis_client

    def set(
        self,
        key: str,
        value: str,
        ex: int | None = None,
        nx: bool = False,
    ) -> bool:
        return self.redis.set(key, value, ex=ex, nx=nx)

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        bucket = self.redis.zsets.setdefault(key, {})
        for member, score in mapping.items():
            bucket[member] = float(score)
        return len(mapping)

    def expire(self, key: str, seconds: int) -> bool:
        return key in self.redis.store or key in self.redis.zsets

    async def execute(self) -> list[object]:
        return []


def _aioredis(redis_client: _GuardrailRedis) -> object:
    return type(
        "_Aioredis",
        (),
        {"from_url": staticmethod(lambda url, decode_responses=True: redis_client)},
    )()


def _write_config(tmp_path: Path, monkeypatch: Any, body: str) -> None:
    (tmp_path / "config.yml").write_text(body, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "REPOS_DIR", str(tmp_path / "repos"))


def _setup_repo(
    tmp_path: Path,
    monkeypatch: Any,
    *,
    repo_state: PipelineState = PipelineState.ERROR,
    task_status: str = "ERROR",
) -> tuple[Path, _GuardrailRedis]:
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
    (repo_dir / "tasks").mkdir(parents=True)
    (repo_dir / "tasks" / "PR-384.md").write_text(
        f"---\nstatus: {task_status}\nblocked_reason: guardrail\n---\n\n"
        "# PR-384: Guardrail task\n\nBody\n",
        encoding="utf-8",
    )
    state = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=repo_state,
        current_task=QueueTask(
            pr_id="PR-384",
            title="Guardrail task",
            status=TaskStatus.ERROR,
            task_file="tasks/PR-384.md",
        ),
        current_queue=[
            QueueTask(
                pr_id="PR-384",
                title="Guardrail task",
                status=TaskStatus.ERROR,
                task_file="tasks/PR-384.md",
            )
        ],
        error_message="Guardrail park",
    )
    redis_client = _GuardrailRedis({"pipeline:example__alpha": state.model_dump_json()})
    cause = CancellationCause(
        category="ERROR",
        payload={
            "subsource": "guardrail",
            "rule": "dangerous_command",
            "excerpt": "GUARDRAIL: dangerous_command: rm -rf /",
        },
        created_at=datetime(2026, 5, 23, tzinfo=timezone.utc).isoformat(),
        task_id="PR-384",
        repo_slug="example__alpha",
    )
    redis_client.store[cause_key("example__alpha", "PR-384")] = cause.to_redis()
    redis_client.zsets[index_key("example__alpha")] = {"PR-384": 1.0}
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))
    return repo_dir, redis_client


def _render_guardrail_card() -> str:
    macro = web_app.templates.env.get_template(
        "components/cancellation_card.html"
    ).module.cancellation_card
    return macro(
        CancellationCause(
            category="ERROR",
            payload={
                "subsource": "guardrail",
                "excerpt": "GUARDRAIL: dangerous_command: rm -rf /",
                "reason_text": "Guardrail fired",
            },
            created_at="2026-05-23T00:00:00+00:00",
            task_id="PR-384",
            repo_slug="example__alpha",
        )
    )


def test_guardrail_card_shows_excerpt() -> None:
    assert "GUARDRAIL: dangerous_command: rm -rf /" in _render_guardrail_card()


def test_reject_text_present() -> None:
    assert "edit the spec before retrying" in _render_guardrail_card().lower()


def test_accept_once_sets_flag_and_clears(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    repo_dir, redis_client = _setup_repo(tmp_path, monkeypatch)
    marker_key = "status_write_failed_tasks:example__alpha"
    legacy_key = "recovered_tasks:example__alpha"
    redis_client.store[marker_key] = '["PR-384","PR-999"]'
    redis_client.store[legacy_key] = '["PR-384","PR-888"]'

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/tasks/PR-384/guardrail/accept-once"
        )

    assert response.status_code == 204
    raw = redis_client.store[cause_key("example__alpha", "PR-384")]
    assert json.loads(raw)["payload"]["approved_once"] is True
    task_text = (repo_dir / "tasks" / "PR-384.md").read_text(encoding="utf-8")
    assert "status: TODO" in task_text
    assert "blocked_reason" not in task_text
    assert redis_client.store[marker_key] == '["PR-999"]'
    assert redis_client.store[legacy_key] == '["PR-888"]'


def test_accept_once_requires_active_error_task(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    repo_dir, redis_client = _setup_repo(tmp_path, monkeypatch)
    state = RepoState.model_validate_json(redis_client.store["pipeline:example__alpha"])
    state.current_task = QueueTask(
        pr_id="PR-999",
        title="Different task",
        status=TaskStatus.ERROR,
        task_file="tasks/PR-999.md",
    )
    redis_client.store["pipeline:example__alpha"] = state.model_dump_json()

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/tasks/PR-384/guardrail/accept-once"
        )

    assert response.status_code == 409
    raw = redis_client.store[cause_key("example__alpha", "PR-384")]
    assert "approved_once" not in json.loads(raw)["payload"]
    task_text = (repo_dir / "tasks" / "PR-384.md").read_text(encoding="utf-8")
    assert "status: ERROR" in task_text


def test_accept_once_state_get_failure_returns_503(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _setup_repo(tmp_path, monkeypatch)
    source = web_app.aioredis.from_url("", decode_responses=True)
    redis_client = _StateGetFailsRedis(dict(source.store))
    redis_client.zsets = dict(source.zsets)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/tasks/PR-384/guardrail/accept-once"
        )

    assert response.status_code == 503


def test_accept_once_missing_state_returns_409(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _setup_repo(tmp_path, monkeypatch)
    source = web_app.aioredis.from_url("", decode_responses=True)
    redis_client = _GuardrailRedis(dict(source.store))
    redis_client.store.pop("pipeline:example__alpha")
    redis_client.zsets = dict(source.zsets)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/tasks/PR-384/guardrail/accept-once"
        )

    assert response.status_code == 409


def test_accept_once_bad_state_returns_503(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _setup_repo(tmp_path, monkeypatch)
    source = web_app.aioredis.from_url("", decode_responses=True)
    redis_client = _GuardrailRedis(dict(source.store))
    redis_client.store["pipeline:example__alpha"] = "not-json"
    redis_client.zsets = dict(source.zsets)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/tasks/PR-384/guardrail/accept-once"
        )

    assert response.status_code == 503


def test_accept_once_requires_repo_error_state(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    repo_dir, redis_client = _setup_repo(tmp_path, monkeypatch)
    state = RepoState.model_validate_json(redis_client.store["pipeline:example__alpha"])
    state.state = PipelineState.IDLE
    redis_client.store["pipeline:example__alpha"] = state.model_dump_json()

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/tasks/PR-384/guardrail/accept-once"
        )

    assert response.status_code == 409
    raw = redis_client.store[cause_key("example__alpha", "PR-384")]
    assert "approved_once" not in json.loads(raw)["payload"]
    task_text = (repo_dir / "tasks" / "PR-384.md").read_text(encoding="utf-8")
    assert "status: ERROR" in task_text


def test_accept_once_warns_will_retrigger() -> None:
    assert "the guardrail will trigger again on the next run" in _render_guardrail_card()


def test_reset_endpoint_forces_idle(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    repo_dir, redis_client = _setup_repo(tmp_path, monkeypatch)
    redis_client.store["diagnose_exhausted:example__alpha:PR-384"] = "1"
    wake_calls: list[tuple[str, str]] = []

    async def fake_publish_wake(redis: Any, repo_name: str, event_type: str) -> None:
        wake_calls.append((repo_name, event_type))

    monkeypatch.setattr(web_app, "publish_wake", fake_publish_wake)
    monkeypatch.setattr(
        repo_control,
        "_publish_history_entry_event",
        lambda *args, **kwargs: None,
    )

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/reset-to-idle")

    assert response.status_code == 200
    assert cause_key("example__alpha", "PR-384") not in redis_client.store
    assert "diagnose_exhausted:example__alpha:PR-384" not in redis_client.store
    task_text = (repo_dir / "tasks" / "PR-384.md").read_text(encoding="utf-8")
    assert "status: TODO" in task_text
    assert "blocked_reason" not in task_text
    state = RepoState.model_validate_json(redis_client.store["pipeline:example__alpha"])
    assert state.state == PipelineState.IDLE
    assert state.current_task is None
    assert wake_calls == [("example__alpha", "reset")]


def test_reset_button_only_in_error() -> None:
    template = web_app.templates.env.get_template("components/_controls.html")
    idle_repo = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
    )
    error_repo = idle_repo.model_copy(update={"state": PipelineState.ERROR})

    assert "reset-to-idle" not in template.module.repo_controls(idle_repo)
    assert "reset-to-idle" in template.module.repo_controls(error_repo)


def test_reset_confirm_present() -> None:
    rendered = web_app.templates.env.get_template(
        "components/_controls.html"
    ).module.repo_controls(
        RepoState(
            url="https://github.com/example/alpha.git",
            name="example__alpha",
            state=PipelineState.ERROR,
        )
    )

    assert 'hx-confirm="Reset this repo to IDLE? The current task returns to TODO."' in rendered


def test_reset_reuses_retry_clearing() -> None:
    assert "_clear_operator_park_for_task" in inspect.getsource(
        repo_control.retry_repo_task
    )
    assert "_clear_operator_park_for_task" in inspect.getsource(
        repo_control.reset_repo_to_idle
    )


def test_task_retry_button_unaffected(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _setup_repo(tmp_path, monkeypatch)

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks")

    assert response.status_code == 200
    assert 'hx-post="/repos/example__alpha/tasks/PR-384/retry"' in response.text


@pytest.mark.asyncio
async def test_clear_operator_park_swallows_cleanup_failures(
    monkeypatch: Any,
) -> None:
    class _Store:
        def __init__(self, redis_client: Any) -> None:
            pass

        async def clear(self, repo_slug: str, task_id: str) -> None:
            raise RuntimeError("clear failed")

    monkeypatch.setattr(repo_control, "RedisSuppressionStore", _Store)
    monkeypatch.setattr(
        repo_control,
        "_clear_status_write_failed_marker",
        lambda *args: (_ for _ in ()).throw(RuntimeError("marker failed")),
    )

    await repo_control._clear_operator_park_for_task(
        _DeleteFailsRedis(),
        "example__alpha",
        "PR-384",
    )


def test_guardrail_reject_invalid_id_returns_400() -> None:
    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/tasks/not-a-pr/guardrail/reject")

    assert response.status_code == 400


def test_guardrail_reject_confirms_spec_edit_text() -> None:
    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/tasks/PR-384/guardrail/reject")

    assert response.status_code == 200
    assert "Edit the spec before retrying" in response.text


def test_guardrail_accept_invalid_id_returns_400() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/tasks/not-a-pr/guardrail/accept-once"
        )

    assert response.status_code == 400


def test_guardrail_accept_unknown_repo_returns_404(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _write_config(tmp_path, monkeypatch, "repositories: []\n")
    monkeypatch.setattr(web_app, "aioredis", _aioredis(_GuardrailRedis()))

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/tasks/PR-384/guardrail/accept-once"
        )

    assert response.status_code == 404


def test_guardrail_accept_redis_unavailable_returns_503(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _write_config(
        tmp_path,
        monkeypatch,
        "repositories:\n  - url: https://github.com/example/alpha.git\n",
    )
    monkeypatch.setattr(
        web_app,
        "aioredis",
        type(
            "_Aioredis",
            (),
            {"from_url": staticmethod(lambda url, decode_responses=True: None)},
        )(),
    )

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/tasks/PR-384/guardrail/accept-once"
        )

    assert response.status_code == 503


def test_guardrail_accept_redis_get_failure_returns_503(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _setup_repo(tmp_path, monkeypatch)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(_GetFailsRedis()))

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/tasks/PR-384/guardrail/accept-once"
        )

    assert response.status_code == 503


def test_guardrail_accept_requires_pending_guardrail(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _setup_repo(tmp_path, monkeypatch)
    redis_client = _GuardrailRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/tasks/PR-384/guardrail/accept-once"
        )

    assert response.status_code == 404


def test_guardrail_accept_missing_task_returns_404(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _repo_dir, redis_client = _setup_repo(tmp_path, monkeypatch)
    state = RepoState.model_validate_json(redis_client.store["pipeline:example__alpha"])
    state.current_task = QueueTask(
        pr_id="PR-999",
        title="Missing task",
        status=TaskStatus.ERROR,
        task_file="tasks/PR-999.md",
    )
    redis_client.store["pipeline:example__alpha"] = state.model_dump_json()
    cause = CancellationCause(
        category="ERROR",
        payload={"subsource": "guardrail", "excerpt": "missing task"},
        created_at=datetime(2026, 5, 23, tzinfo=timezone.utc).isoformat(),
        task_id="PR-999",
        repo_slug="example__alpha",
    )
    redis_client.store[cause_key("example__alpha", "PR-999")] = cause.to_redis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/tasks/PR-999/guardrail/accept-once"
        )

    assert response.status_code == 404


def test_guardrail_accept_write_failure_returns_503(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _repo_dir, redis_client = _setup_repo(tmp_path, monkeypatch)
    monkeypatch.setattr(
        repo_control,
        "write_frontmatter_status",
        lambda *args: (_ for _ in ()).throw(ValueError("bad frontmatter")),
    )

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/tasks/PR-384/guardrail/accept-once"
        )

    assert response.status_code == 503
    raw = redis_client.store[cause_key("example__alpha", "PR-384")]
    assert "approved_once" not in json.loads(raw)["payload"]


def test_guardrail_accept_swallows_delete_and_publish_failure(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _setup_repo(tmp_path, monkeypatch)
    source = web_app.aioredis.from_url("", decode_responses=True)
    failing_delete = _DeleteFailsRedis(dict(source.store))
    failing_delete.zsets = dict(source.zsets)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(failing_delete))

    async def fail_publish(redis: Any, repo_name: str, event_type: str) -> None:
        raise RuntimeError("publish failed")

    async def fail_marker_cleanup(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("marker failed")

    monkeypatch.setattr(web_app, "publish_wake", fail_publish)
    monkeypatch.setattr(
        repo_control,
        "_clear_status_write_failed_marker",
        fail_marker_cleanup,
    )

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/tasks/PR-384/guardrail/accept-once"
        )

    assert response.status_code == 204


def test_reset_unknown_repo_returns_404(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _write_config(tmp_path, monkeypatch, "repositories: []\n")
    monkeypatch.setattr(web_app, "aioredis", _aioredis(_GuardrailRedis()))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/reset-to-idle")

    assert response.status_code == 404


def test_reset_redis_unavailable_returns_503(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _write_config(
        tmp_path,
        monkeypatch,
        "repositories:\n  - url: https://github.com/example/alpha.git\n",
    )
    monkeypatch.setattr(
        web_app,
        "aioredis",
        type(
            "_Aioredis",
            (),
            {"from_url": staticmethod(lambda url, decode_responses=True: None)},
        )(),
    )

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/reset-to-idle")

    assert response.status_code == 503


def test_reset_get_failure_returns_503(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _setup_repo(tmp_path, monkeypatch)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(_GetFailsRedis()))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/reset-to-idle")

    assert response.status_code == 503


def test_reset_missing_state_defaults_to_idle_and_rejects(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _setup_repo(tmp_path, monkeypatch)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(_GuardrailRedis()))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/reset-to-idle")

    assert response.status_code == 409


def test_reset_bad_state_returns_503(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _setup_repo(tmp_path, monkeypatch)
    redis_client = _GuardrailRedis({"pipeline:example__alpha": "not-json"})
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/reset-to-idle")

    assert response.status_code == 503


def test_reset_non_error_repo_returns_409(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _setup_repo(tmp_path, monkeypatch, repo_state=PipelineState.IDLE)

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/reset-to-idle")

    assert response.status_code == 409


def test_reset_error_without_current_task_returns_409(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _setup_repo(tmp_path, monkeypatch)
    source = web_app.aioredis.from_url("", decode_responses=True)
    state = RepoState.model_validate_json(source.store["pipeline:example__alpha"])
    state.current_task = None
    source.store["pipeline:example__alpha"] = state.model_dump_json()

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/reset-to-idle")

    assert response.status_code == 409


def test_reset_missing_task_returns_404(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    repo_dir, redis_client = _setup_repo(tmp_path, monkeypatch)
    (repo_dir / "tasks" / "PR-384.md").unlink()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/reset-to-idle")

    assert response.status_code == 404


def test_reset_write_failure_returns_503(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _setup_repo(tmp_path, monkeypatch)
    monkeypatch.setattr(
        repo_control,
        "write_frontmatter_status",
        lambda *args: (_ for _ in ()).throw(ValueError("bad frontmatter")),
    )

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/reset-to-idle")

    assert response.status_code == 503


def test_reset_transaction_failure_returns_503(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    repo_dir, _redis_client = _setup_repo(tmp_path, monkeypatch)
    source = web_app.aioredis.from_url("", decode_responses=True)
    redis_client = _TransactionFailsRedis(dict(source.store))
    redis_client.zsets = dict(source.zsets)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/reset-to-idle")

    assert response.status_code == 503
    task_text = (repo_dir / "tasks" / "PR-384.md").read_text(encoding="utf-8")
    assert "status: ERROR" in task_text


def test_reset_revalidates_error_task_inside_transaction(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _setup_repo(tmp_path, monkeypatch)
    source = web_app.aioredis.from_url("", decode_responses=True)
    redis_client = _StateChangesBeforeTransactionRedis(dict(source.store))
    redis_client.zsets = dict(source.zsets)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/reset-to-idle")

    assert response.status_code == 409
    state = RepoState.model_validate_json(redis_client.store["pipeline:example__alpha"])
    assert state.state == PipelineState.CODING
    assert state.current_task is not None
    assert state.current_task.pr_id == "PR-999"
    task_text = (
        tmp_path / "repos" / "example__alpha" / "tasks" / "PR-384.md"
    ).read_text(encoding="utf-8")
    assert "status: ERROR" in task_text


def test_reset_handles_final_transaction_failure(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    repo_dir, _redis_client = _setup_repo(tmp_path, monkeypatch)
    source = web_app.aioredis.from_url("", decode_responses=True)
    redis_client = _SecondTransactionFailsRedis(dict(source.store))
    redis_client.zsets = dict(source.zsets)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/reset-to-idle")

    assert response.status_code == 503
    state = RepoState.model_validate_json(redis_client.store["pipeline:example__alpha"])
    assert state.state == PipelineState.ERROR
    task_text = (repo_dir / "tasks" / "PR-384.md").read_text(encoding="utf-8")
    assert "status: ERROR" in task_text


def test_reset_revalidates_reserved_task_before_idle_transition(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _setup_repo(tmp_path, monkeypatch)
    source = web_app.aioredis.from_url("", decode_responses=True)
    redis_client = _StateChangesBeforeFinalTransactionRedis(dict(source.store))
    redis_client.zsets = dict(source.zsets)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/reset-to-idle")

    assert response.status_code == 409
    state = RepoState.model_validate_json(redis_client.store["pipeline:example__alpha"])
    assert state.state == PipelineState.CODING
    assert state.current_task is not None
    assert state.current_task.pr_id == "PR-999"
    task_text = (
        tmp_path / "repos" / "example__alpha" / "tasks" / "PR-384.md"
    ).read_text(encoding="utf-8")
    assert "status: ERROR" in task_text


def test_reset_swallows_history_and_publish_failure(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    _setup_repo(tmp_path, monkeypatch)

    async def fail_history(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("history failed")

    async def fail_publish(redis: Any, repo_name: str, event_type: str) -> None:
        raise RuntimeError("publish failed")

    monkeypatch.setattr(repo_control, "_publish_history_entry_event", fail_history)
    monkeypatch.setattr(web_app, "publish_wake", fail_publish)

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/reset-to-idle")

    assert response.status_code == 200
