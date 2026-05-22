from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from redis.exceptions import RedisError
from src.audit import operator_actions
from src.cancellation.storage import cause_key, index_key
from src.keyspace import pipeline_state
from src.models import PipelineState, PRInfo, RepoState
from src.subsource_registry import SuppressionReason
from src.web import app as web_app
from src.web.app import app
from src.web.routes import repo_control


class _Pipe:
    def __init__(self, redis: "_Redis") -> None:
        self.redis = redis

    async def get(self, key: str) -> str | None:
        return self.redis.store.get(key)

    def multi(self) -> None:
        return None

    def set(self, key: str, value: str) -> None:
        self.redis.store[key] = value


class _Redis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.raise_on_get = False
        self.raise_on_transaction = False
        self.clear_before_transaction = False
        self.remove_quarantine_before_transaction = False

    async def get(self, key: str) -> str | None:
        if self.raise_on_get:
            raise RedisError("redis down")
        return self.store.get(key)

    async def delete(self, key: str) -> int:
        if key in self.store:
            del self.store[key]
            return 1
        return 0

    async def exists(self, key: str) -> int:
        return int(key in self.store)

    async def zrangebyscore(
        self, key: str, min_score: object, max_score: object
    ) -> list[str]:
        lower = float("-inf") if min_score == "-inf" else float(min_score)
        upper = float("inf") if max_score == "+inf" else float(max_score)
        bucket = self.zsets.get(key, {})
        return [
            member
            for member, score in sorted(bucket.items(), key=lambda item: item[1])
            if lower <= score <= upper
        ]

    async def zrem(self, key: str, *members: str) -> int:
        bucket = self.zsets.setdefault(key, {})
        removed = 0
        for member in members:
            if member in bucket:
                del bucket[member]
                removed += 1
        return removed

    async def transaction(self, func: Any, *keys: str, value_from_callable: bool = False) -> Any:
        if self.raise_on_transaction:
            raise RuntimeError("transaction failed")
        if self.clear_before_transaction:
            self.store.clear()
        if self.remove_quarantine_before_transaction:
            raw = self.store.get(pipeline_state("example__alpha"))
            if raw is not None:
                state = RepoState.model_validate_json(raw)
                state.quarantined_prs.clear()
                self.store[pipeline_state("example__alpha")] = state.model_dump_json()
        return await func(_Pipe(self))


def _write_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(cfg))


@pytest.fixture(autouse=True)
def _patch_publish(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []

    async def fake_publish_repo_event(*args: Any, **kwargs: Any) -> None:
        events.append({"args": args, "kwargs": kwargs})

    monkeypatch.setattr(web_app, "publish_repo_event", fake_publish_repo_event)
    return events


def _seed(redis: _Redis, *prs: int) -> None:
    state = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.WATCH,
        quarantined_prs=set(prs),
    )
    redis.store[pipeline_state("example__alpha")] = state.model_dump_json()


def _seed_with_current_pr(redis: _Redis, *prs: int, current_pr: PRInfo) -> None:
    state = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.WATCH,
        current_pr=current_pr,
        quarantined_prs=set(prs),
    )
    redis.store[pipeline_state("example__alpha")] = state.model_dump_json()


def _seed_suppression(redis: _Redis, task_id: str, payload: dict[str, Any]) -> None:
    redis.store[cause_key("example__alpha", task_id)] = json.dumps(
        {
            "category": "ERROR",
            "payload": payload,
            "created_at": "2026-05-22T00:00:00+00:00",
            "task_id": task_id,
            "repo_slug": "example__alpha",
        }
    )
    redis.zsets.setdefault(index_key("example__alpha"), {})[task_id] = 1779408000.0


def test_release_endpoint_removes_from_set(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis = _Redis()
    _seed(redis, 442)
    monkeypatch.setattr(repo_control.gh_runner, "run_gh", lambda *a, **kw: "")
    with TestClient(app) as client:
        client.app.state.redis = redis
        response = client.post("/repos/example__alpha/quarantine/442/release")

    assert response.status_code == 200
    assert response.json()["status"] == "released"
    state = RepoState.model_validate_json(redis.store[pipeline_state("example__alpha")])
    assert state.quarantined_prs == set()


def test_suppressed_task_ids_for_pr_tolerates_store_failure() -> None:
    class BrokenRedis(_Redis):
        async def zrangebyscore(
            self, key: str, min_score: object, max_score: object
        ) -> list[str]:
            raise RuntimeError("redis unavailable")

    found = asyncio.run(
        repo_control._suppressed_task_ids_for_pr(BrokenRedis(), "example__alpha", 442)
    )

    assert found == set()


def test_suppressed_task_ids_for_pr_ignores_invalid_pr_detail() -> None:
    redis = _Redis()
    _seed_suppression(
        redis,
        "PR-bad",
        {
            "subsource": SuppressionReason.GUARDRAIL.value,
            "pr_number": "not-a-number",
        },
    )

    found = asyncio.run(
        repo_control._suppressed_task_ids_for_pr(redis, "example__alpha", 442)
    )

    assert found == set()


def test_suppressed_task_ids_for_pr_requires_guardrail_reason() -> None:
    redis = _Redis()
    _seed_suppression(
        redis,
        "PR-rate-limit",
        {
            "subsource": SuppressionReason.RATE_LIMIT.value,
            "pr_number": 442,
        },
    )
    _seed_suppression(
        redis,
        "PR-guardrail",
        {
            "subsource": SuppressionReason.GUARDRAIL.value,
            "pr_number": 442,
        },
    )

    found = asyncio.run(
        repo_control._suppressed_task_ids_for_pr(redis, "example__alpha", 442)
    )

    assert found == {"PR-guardrail"}


def test_release_endpoint_clears_non_current_pr_suppression(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis = _Redis()
    _seed_with_current_pr(
        redis,
        442,
        current_pr=PRInfo(number=441, branch="pr-441", pr_id="PR-441"),
    )
    _seed_suppression(
        redis,
        "PR-442",
        {
            "subsource": SuppressionReason.GUARDRAIL.value,
            "pr_number": 442,
        },
    )
    monkeypatch.setattr(repo_control.gh_runner, "run_gh", lambda *a, **kw: "")

    with TestClient(app) as client:
        client.app.state.redis = redis
        response = client.post("/repos/example__alpha/quarantine/442/release")

    assert response.status_code == 200
    assert response.json()["status"] == "released"
    assert cause_key("example__alpha", "PR-442") not in redis.store


def test_release_endpoint_tolerates_suppression_delete_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis = _Redis()
    _seed(redis, 442)
    _seed_suppression(
        redis,
        "PR-442",
        {
            "subsource": SuppressionReason.GUARDRAIL.value,
            "pr_number": 442,
        },
    )
    monkeypatch.setattr(repo_control.gh_runner, "run_gh", lambda *a, **kw: "")

    async def fail_delete(*args: object, **kwargs: object) -> None:
        raise RuntimeError("delete failed")

    monkeypatch.setattr(repo_control, "delete_cancellation_cause", fail_delete)
    with TestClient(app) as client:
        client.app.state.redis = redis
        response = client.post("/repos/example__alpha/quarantine/442/release")

    assert response.status_code == 200
    assert response.json()["status"] == "released"


def test_release_endpoint_idempotent_for_non_quarantined(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis = _Redis()
    _seed(redis)
    with TestClient(app) as client:
        client.app.state.redis = redis
        response = client.post("/repos/example__alpha/quarantine/999/release")

    assert response.status_code == 200
    assert response.json() == {"status": "not_quarantined", "pr": 999}


def test_release_endpoint_idempotent_when_state_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis = _Redis()
    with TestClient(app) as client:
        client.app.state.redis = redis
        response = client.post("/repos/example__alpha/quarantine/999/release")

    assert response.status_code == 200
    assert response.json() == {"status": "not_quarantined", "pr": 999}


def test_release_endpoint_repo_not_found(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(tmp_path, monkeypatch)
    with TestClient(app) as client:
        client.app.state.redis = _Redis()
        response = client.post("/repos/missing/quarantine/999/release")

    assert response.status_code == 404


def test_release_endpoint_redis_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(tmp_path, monkeypatch)
    with TestClient(app) as client:
        client.app.state.redis = None
        response = client.post("/repos/example__alpha/quarantine/999/release")

    assert response.status_code == 503


def test_release_endpoint_redis_get_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis = _Redis()
    redis.raise_on_get = True
    with TestClient(app) as client:
        client.app.state.redis = redis
        response = client.post("/repos/example__alpha/quarantine/999/release")

    assert response.status_code == 503


def test_release_endpoint_invalid_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis = _Redis()
    redis.store[pipeline_state("example__alpha")] = "{"
    with TestClient(app) as client:
        client.app.state.redis = redis
        response = client.post("/repos/example__alpha/quarantine/999/release")

    assert response.status_code == 503


def test_release_endpoint_removes_labels_via_gh(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis = _Redis()
    _seed(redis, 442)
    calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str) -> str:
        calls.append(args)
        if args[:2] == ["pr", "view"]:
            return "quarantine:large_diff\nneeds-review\n"
        return ""

    monkeypatch.setattr(repo_control.gh_runner, "run_gh", fake_run_gh)
    with TestClient(app) as client:
        client.app.state.redis = redis
        client.post("/repos/example__alpha/quarantine/442/release")

    assert ["pr", "edit", "442", "--remove-label", "quarantine:large_diff"] in calls
    assert ["pr", "edit", "442", "--remove-label", "needs-review"] not in calls


def test_release_endpoint_continues_when_repo_full_name_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis = _Redis()
    _seed(redis, 442)
    monkeypatch.setattr(
        repo_control.gh_runner,
        "get_repo_full_name",
        lambda url: (_ for _ in ()).throw(ValueError("bad url")),
    )
    with TestClient(app) as client:
        client.app.state.redis = redis
        response = client.post("/repos/example__alpha/quarantine/442/release")

    assert response.status_code == 200


def test_release_endpoint_continues_when_label_removal_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis = _Redis()
    _seed(redis, 442)
    monkeypatch.setattr(
        repo_control.gh_runner,
        "run_gh",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("gh failed")),
    )
    with TestClient(app) as client:
        client.app.state.redis = redis
        response = client.post("/repos/example__alpha/quarantine/442/release")

    assert response.status_code == 200


def test_release_endpoint_handles_missing_state_during_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis = _Redis()
    _seed(redis, 442)
    redis.clear_before_transaction = True
    monkeypatch.setattr(repo_control.gh_runner, "run_gh", lambda *a, **kw: "")
    with TestClient(app) as client:
        client.app.state.redis = redis
        response = client.post("/repos/example__alpha/quarantine/442/release")

    assert response.status_code == 200


def test_release_endpoint_handles_already_released_during_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis = _Redis()
    _seed(redis, 442)
    redis.remove_quarantine_before_transaction = True
    monkeypatch.setattr(repo_control.gh_runner, "run_gh", lambda *a, **kw: "")
    with TestClient(app) as client:
        client.app.state.redis = redis
        response = client.post("/repos/example__alpha/quarantine/442/release")

    assert response.status_code == 200


def test_release_endpoint_transaction_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis = _Redis()
    _seed(redis, 442)
    redis.raise_on_transaction = True
    monkeypatch.setattr(repo_control.gh_runner, "run_gh", lambda *a, **kw: "")
    with TestClient(app) as client:
        client.app.state.redis = redis
        response = client.post("/repos/example__alpha/quarantine/442/release")

    assert response.status_code == 503


def test_split_gh_label_output_handles_list_and_unknown() -> None:
    assert repo_control._split_gh_label_output([" a ", "", "b"]) == ["a", "b"]
    assert repo_control._split_gh_label_output(object()) == []


def test_release_endpoint_writes_audit_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config(tmp_path, monkeypatch)
    audit_dir = tmp_path / "audit"
    monkeypatch.setattr(operator_actions, "AUDIT_DIR", audit_dir)
    redis = _Redis()
    _seed(redis, 442)
    monkeypatch.setattr(repo_control.gh_runner, "run_gh", lambda *a, **kw: "")
    with TestClient(app) as client:
        client.app.state.redis = redis
        client.post(
            "/repos/example__alpha/quarantine/442/release",
            headers={"X-Session-Id": "session-1"},
        )

    record = json.loads(next(audit_dir.glob("*.jsonl")).read_text().strip())
    assert record["event"] == "quarantine_release"
    assert record["operator_session_id"] == "session-1"


def test_release_endpoint_publishes_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _patch_publish: list[dict[str, Any]],
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis = _Redis()
    _seed(redis, 442)
    monkeypatch.setattr(repo_control.gh_runner, "run_gh", lambda *a, **kw: "")
    with TestClient(app) as client:
        client.app.state.redis = redis
        client.post("/repos/example__alpha/quarantine/442/release")

    assert len(_patch_publish) == 1
