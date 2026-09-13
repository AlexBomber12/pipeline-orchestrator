"""Tests for the operator override POST decision endpoint (PR-305c)."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from redis.exceptions import WatchError
from src.cancellation.storage import (
    CancellationCause,
    cause_key,
)
from src.models import PipelineState, PRInfo, QueueTask, RepoState, TaskStatus
from src.web import app as web_app
from src.web.app import app
from src.web.routes import repo_control


class _FakePipeline:
    def __init__(self, redis: "_GuardrailRedis", *, transaction: bool) -> None:
        self.redis = redis
        self.transaction = transaction
        self.pending: list[tuple[str, tuple[Any, ...]]] = []
        self.watching = False
        self.watch_keys: tuple[str, ...] = ()

    async def __aenter__(self) -> "_FakePipeline":
        return self

    async def __aexit__(self, *exc: Any) -> bool:
        return False

    async def watch(self, *keys: str) -> None:
        self.watching = True
        self.watch_keys = keys

    async def unwatch(self) -> None:
        self.watching = False

    async def get(self, key: str) -> str | None:
        return self.redis.store.get(key)

    def multi(self) -> None:
        return None

    def set(self, key: str, value: str, ex: int | None = None) -> "_FakePipeline":
        self.pending.append(("set", (key, value)))
        return self

    def delete(self, key: str) -> "_FakePipeline":
        self.pending.append(("delete", (key,)))
        return self

    def zadd(self, key: str, mapping: dict[str, float]) -> "_FakePipeline":
        self.pending.append(("zadd", (key, mapping)))
        return self

    def zrem(self, key: str, *members: str) -> "_FakePipeline":
        self.pending.append(("zrem", (key, members)))
        return self

    def zremrangebyscore(
        self, key: str, mn: Any, mx: Any
    ) -> "_FakePipeline":
        self.pending.append(("zremrangebyscore", (key,)))
        return self

    def expire(self, key: str, seconds: int) -> "_FakePipeline":
        return self

    async def execute(self) -> list[Any]:
        if self.redis.pending_watch_error and self.watching:
            self.redis.pending_watch_error = False
            self.pending.clear()
            raise WatchError("simulated concurrent change")
        for op, args in self.pending:
            if op == "set":
                self.redis.store[args[0]] = args[1]
            elif op == "delete":
                self.redis.store.pop(args[0], None)
                self.redis.deleted.append(args[0])
            elif op == "zadd":
                self.redis.zsets.setdefault(args[0], {}).update(args[1])
            elif op == "zrem":
                zset = self.redis.zsets.get(args[0], {})
                for m in args[1]:
                    zset.pop(m, None)
                self.redis.zremmed.append((args[0], args[1]))
        results = [None] * len(self.pending)
        self.pending.clear()
        return results


class _GuardrailRedis:
    def __init__(self, store: dict[str, str] | None = None) -> None:
        self.store: dict[str, str] = dict(store or {})
        self.zsets: dict[str, dict[str, float]] = {}
        self.deleted: list[str] = []
        self.zremmed: list[tuple[str, tuple[str, ...]]] = []
        self.pending_watch_error = False

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def set(
        self, key: str, value: str, ex: int | None = None, nx: bool = False
    ) -> bool:
        if nx and key in self.store:
            return False
        self.store[key] = value
        return True

    async def delete(self, key: str) -> int:
        self.deleted.append(key)
        return 1 if self.store.pop(key, None) is not None else 0

    async def zrem(self, key: str, *members: str) -> int:
        zset = self.zsets.get(key, {})
        removed = sum(1 for m in members if zset.pop(m, None) is not None)
        self.zremmed.append((key, members))
        return removed

    async def zrangebyscore(
        self, key: str, min_score: Any, max_score: Any
    ) -> list[str]:
        bucket = self.zsets.get(key, {})

        def _bound(value: Any, default: float) -> tuple[float, bool]:
            if value in ("-inf", "+inf"):
                return float(value), False
            if isinstance(value, str) and value.startswith("("):
                return float(value[1:]), True
            return float(value), False

        lower, lower_excl = _bound(min_score, float("-inf"))
        upper, upper_excl = _bound(max_score, float("inf"))
        items = [
            tid
            for tid, score in bucket.items()
            if (score > lower if lower_excl else score >= lower)
            and (score < upper if upper_excl else score <= upper)
        ]
        items.sort(key=lambda tid: bucket[tid])
        return items

    async def exists(self, key: str) -> int:
        return int(key in self.store)

    async def transaction(
        self, callback: Any, *keys: str, value_from_callable: bool = False
    ) -> Any:
        pipe = _FakePipeline(self, transaction=True)
        result = await callback(pipe)
        await pipe.execute()
        return result if value_from_callable else None

    def pipeline(self, transaction: bool = False) -> _FakePipeline:
        return _FakePipeline(self, transaction=transaction)

    async def aclose(self) -> None:
        return None


def _aioredis_factory(redis_client: _GuardrailRedis) -> Any:
    return type(
        "_Aioredis",
        (),
        {"from_url": staticmethod(lambda url, decode_responses=True: redis_client)},
    )()


def _seed_state(
    *,
    pr_id: str = "PR-305c",
    pr_number: int = 99,
    active: bool = True,
    current_queue: list[QueueTask] | None = None,
) -> str:
    current_task = (
        QueueTask(pr_id=pr_id, title=pr_id, status=TaskStatus.ERROR) if active else None
    )
    current_pr = (
        PRInfo(number=pr_number, branch="pr-305c-feature", head_sha="head") if active else None
    )
    state = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.ERROR,
        current_task=current_task,
        current_pr=current_pr,
        current_queue=current_queue,
    )
    return state.model_dump_json()


def _seed_cause(payload: dict[str, Any]) -> str:
    return CancellationCause(
        category="ERROR",
        payload=payload,
        created_at="2026-05-14T12:00:00+00:00",
        task_id="PR-305c",
        repo_slug="example__alpha",
    ).to_redis()


def _setup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, store: dict[str, str] | None = None
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
    (repo_dir / "tasks" / "PR-305c.md").write_text(
        "---\nstatus: ERROR\n---\n\n# PR-305c: Guardrail task\n\nBranch: pr-305c-feature\n- Type: feature\n- Complexity: low\n- Depends on: none\n\n## Body\n",
        encoding="utf-8",
    )
    redis_client = _GuardrailRedis(store or {})
    monkeypatch.setattr(web_app, "aioredis", _aioredis_factory(redis_client))
    return repo_dir, redis_client


def _post(decision: str, *, repo: str = "example__alpha", pr_id: str = "PR-305c") -> Any:
    with TestClient(app) as client:
        return client.post(
            f"/repos/{repo}/guardrail/{pr_id}/decision",
            data={"decision": decision},
        )


def test_guardrail_decision_invalid_pr_id_returns_400(tmp_path, monkeypatch) -> None:
    _setup(tmp_path, monkeypatch)
    resp = _post("approve", pr_id="not-a-valid-id")
    assert resp.status_code == 400
    assert "Invalid task identifier" in resp.text


def test_guardrail_decision_invalid_decision_value_returns_400(
    tmp_path, monkeypatch
) -> None:
    _setup(tmp_path, monkeypatch)
    resp = _post("approven")
    assert resp.status_code == 400
    assert "Invalid decision" in resp.text


def test_guardrail_decision_unknown_repo_returns_404(tmp_path, monkeypatch) -> None:
    _setup(tmp_path, monkeypatch)
    resp = _post("approve", repo="nonexistent")
    assert resp.status_code == 404
    assert "Repository not found" in resp.text


def test_guardrail_decision_approve_missing_task_file_returns_404(
    tmp_path, monkeypatch
) -> None:
    repo_dir, _ = _setup(
        tmp_path,
        monkeypatch,
        store={
            "pipeline:example__alpha": _seed_state(),
            cause_key("example__alpha", "PR-305c"): _seed_cause(
                {"subsource": "guardrail"}
            ),
        },
    )
    (repo_dir / "tasks" / "PR-305c.md").unlink()
    resp = _post("approve")
    assert resp.status_code == 404
    assert "Task file not found" in resp.text


def test_guardrail_decision_approve_no_pending_cause_returns_404(
    tmp_path, monkeypatch
) -> None:
    _setup(tmp_path, monkeypatch, store={"pipeline:example__alpha": _seed_state()})
    resp = _post("approve")
    assert resp.status_code == 404
    assert "no pending guardrail decision" in resp.text


def test_guardrail_decision_approve_wrong_subsource_returns_404(
    tmp_path, monkeypatch
) -> None:
    _setup(
        tmp_path,
        monkeypatch,
        store={
            "pipeline:example__alpha": _seed_state(),
            cause_key("example__alpha", "PR-305c"): _seed_cause(
                {"subsource": "coder_escalate"}
            ),
        },
    )
    resp = _post("approve")
    assert resp.status_code == 404


def test_guardrail_decision_approve_inactive_task_returns_409(
    tmp_path, monkeypatch
) -> None:
    _setup(
        tmp_path,
        monkeypatch,
        store={
            "pipeline:example__alpha": _seed_state(active=False),
            cause_key("example__alpha", "PR-305c"): _seed_cause(
                {"subsource": "guardrail"}
            ),
        },
    )
    resp = _post("approve")
    assert resp.status_code == 409
    assert "not active in daemon state" in resp.text


def test_guardrail_decision_redis_unavailable_returns_503(
    tmp_path, monkeypatch
) -> None:
    _setup(tmp_path, monkeypatch)
    if hasattr(web_app.app.state, "redis"):
        monkeypatch.delattr(web_app.app.state, "redis", raising=False)
    client = TestClient(web_app.app)
    resp = client.post(
        "/repos/example__alpha/guardrail/PR-305c/decision",
        data={"decision": "approve"},
    )
    assert resp.status_code == 503


@pytest.mark.asyncio
async def test_validated_guardrail_cause_edges() -> None:
    assert repo_control._validated_guardrail_cause(None) is None
    assert repo_control._validated_guardrail_cause("not json") is None
    bad_payload = CancellationCause(category="ERROR", payload={}).to_redis()
    assert repo_control._validated_guardrail_cause(bad_payload) is None
    list_payload = CancellationCause(
        category="ERROR", payload={"subsource": "coder_escalate"}
    ).to_redis()
    assert repo_control._validated_guardrail_cause(list_payload) is None


def test_guardrail_decision_approve_initial_get_redis_error_returns_503(
    tmp_path, monkeypatch
) -> None:
    """Transient Redis outage on the first read surfaces as 503, not 500."""
    _, redis_client = _setup(
        tmp_path,
        monkeypatch,
        store={
            "pipeline:example__alpha": _seed_state(),
            cause_key("example__alpha", "PR-305c"): _seed_cause(
                {"subsource": "guardrail"}
            ),
        },
    )
    from redis.exceptions import RedisError as _RedisError

    async def boom_get(key: str) -> str | None:
        raise _RedisError("conn refused")

    monkeypatch.setattr(redis_client, "get", boom_get)
    resp = _post("approve")
    assert resp.status_code == 503
    assert "Redis unavailable" in resp.text


def test_guardrail_decision_approve_state_get_redis_error_returns_503(
    tmp_path, monkeypatch
) -> None:
    """RedisError on the state read after the cause read surfaces as 503."""
    _, redis_client = _setup(
        tmp_path,
        monkeypatch,
        store={
            "pipeline:example__alpha": _seed_state(),
            cause_key("example__alpha", "PR-305c"): _seed_cause(
                {"subsource": "guardrail"}
            ),
        },
    )
    from redis.exceptions import RedisError as _RedisError

    original_get = redis_client.get
    call_count = {"n": 0}

    async def flaky_get(key: str) -> str | None:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return await original_get(key)
        raise _RedisError("conn dropped")

    monkeypatch.setattr(redis_client, "get", flaky_get)
    resp = _post("approve")
    assert resp.status_code == 503
    assert "Redis unavailable" in resp.text
