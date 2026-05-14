from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from redis.exceptions import WatchError
from src.cancellation.storage import CancellationCause, cause_key, index_key
from src.keyspace import pipeline_state
from src.models import PipelineState, PRInfo, QueueTask, RepoState, TaskStatus
from src.web import app as web_app
from src.web.app import app
from src.web.routes import repo_control


class _GuardrailRedis:
    def __init__(
        self,
        store: dict[str, str] | None = None,
        zsets: dict[str, dict[str, float]] | None = None,
        *,
        conflict: bool = False,
    ) -> None:
        self.store = store or {}
        self.zsets = zsets or {}
        self.conflict = conflict

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    def set(self, key: str, value: str, ex: int | None = None, nx: bool = False) -> bool:
        if nx and key in self.store:
            return False
        self.store[key] = value
        return True

    async def zrange(self, key: str, start: int, end: int) -> list[str]:
        items = sorted(self.zsets.get(key, {}).items(), key=lambda item: item[1])
        return [member for member, _score in items[start : end + 1]]

    def pipeline(self) -> "_GuardrailPipe":
        return _GuardrailPipe(self)

    async def aclose(self) -> None:
        return None


class _GuardrailPipe:
    def __init__(self, redis: _GuardrailRedis) -> None:
        self.redis = redis
        self.commands: list[tuple[str, tuple[Any, ...]]] = []

    async def watch(self, key: str) -> None:
        return None

    async def get(self, key: str) -> str | None:
        return self.redis.store.get(key)

    async def unwatch(self) -> None:
        return None

    def multi(self) -> None:
        return None

    def delete(self, key: str) -> None:
        self.commands.append(("delete", (key,)))

    def zrem(self, key: str, *members: str) -> None:
        self.commands.append(("zrem", (key, *members)))

    def set(self, key: str, value: str) -> None:
        self.commands.append(("set", (key, value)))

    def zadd(self, key: str, mapping: dict[str, float]) -> None:
        self.commands.append(("zadd", (key, mapping)))

    async def execute(self) -> None:
        if self.redis.conflict:
            raise WatchError("changed")
        for command, args in self.commands:
            if command == "delete":
                self.redis.store.pop(args[0], None)
            elif command == "zrem":
                zset = self.redis.zsets.setdefault(args[0], {})
                for member in args[1:]:
                    zset.pop(member, None)
            elif command == "set":
                self.redis.store[args[0]] = args[1]
            elif command == "zadd":
                self.redis.zsets.setdefault(args[0], {}).update(args[1])

    async def reset(self) -> None:
        return None


class _ResetFailRedis(_GuardrailRedis):
    def pipeline(self) -> "_ResetFailPipe":
        return _ResetFailPipe(self)


class _ResetFailPipe(_GuardrailPipe):
    async def reset(self) -> None:
        raise RuntimeError("reset failed")


def _aioredis(redis_client: _GuardrailRedis) -> object:
    return type(
        "_Aioredis",
        (),
        {"from_url": staticmethod(lambda url, decode_responses=True: redis_client)},
    )()


def _write_repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "REPOS_DIR", str(tmp_path / "repos"))
    repo_dir = tmp_path / "repos" / "example__alpha"
    (repo_dir / "tasks").mkdir(parents=True)
    (repo_dir / "tasks" / "PR-305.md").write_text(
        "---\nstatus: ERROR\n---\n\n# PR-305\n",
        encoding="utf-8",
    )
    return repo_dir


def _guardrail_cause(rule: str = "large_diff") -> CancellationCause:
    return CancellationCause(
        category="ERROR",
        payload={"subsource": "guardrail", "rule": rule, "excerpt": "+1800 LOC"},
        created_at="2026-05-14T00:00:00+00:00",
        task_id="PR-305",
        repo_slug="example__alpha",
    )


def _state() -> RepoState:
    task = QueueTask(
        pr_id="PR-305",
        title="Operator override",
        status=TaskStatus.ERROR,
        task_file="tasks/PR-305.md",
        branch="pr-305-operator-override-backend",
    )
    return RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.ERROR,
        current_task=task,
        current_pr=PRInfo(number=42, branch=task.branch or "", pr_id="PR-305"),
    )


def _redis(cause: CancellationCause | None = None, *, conflict: bool = False) -> _GuardrailRedis:
    store = {pipeline_state("example__alpha"): _state().model_dump_json()}
    zsets: dict[str, dict[str, float]] = {}
    if cause is not None:
        store[cause_key("example__alpha", "PR-305")] = cause.to_redis()
        zsets[index_key("example__alpha")] = {"PR-305": 1.0}
    return _GuardrailRedis(store, zsets, conflict=conflict)


def test_guardrail_decision_approve_clears_cause_and_writes_todo(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_dir = _write_repo(tmp_path, monkeypatch)
    redis = _redis(_guardrail_cause())
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis))
    monkeypatch.setattr(web_app, "publish_wake", lambda *args: None)
    calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/guardrail/PR-305/decision",
            json={"decision": "approve"},
        )

    assert response.status_code == 204
    assert cause_key("example__alpha", "PR-305") not in redis.store
    assert "PR-305" not in redis.zsets[index_key("example__alpha")]
    assert "status: TODO" in (repo_dir / "tasks" / "PR-305.md").read_text()
    state = RepoState.model_validate_json(redis.store[pipeline_state("example__alpha")])
    assert state.state == PipelineState.WATCH
    assert [
        "gh",
        "api",
        "-X",
        "DELETE",
        "/repos/example/alpha/issues/42/labels/escalated",
    ] in calls
    assert any("[skip ci]" in call for call in calls)


def test_guardrail_decision_reject_records_operator_reject_cause(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_dir = _write_repo(tmp_path, monkeypatch)
    redis = _redis(_guardrail_cause("secret_in_diff"))
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis))
    calls: list[list[str]] = []
    monkeypatch.setattr(
        repo_control.subprocess,
        "run",
        lambda args, **kwargs: calls.append(args)
        or subprocess.CompletedProcess(args, 0, "", ""),
    )

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/guardrail/PR-305/decision",
            data={"decision": "reject"},
        )

    assert response.status_code == 204
    assert "status: ERROR" in (repo_dir / "tasks" / "PR-305.md").read_text()
    cause = CancellationCause.from_redis(redis.store[cause_key("example__alpha", "PR-305")])
    assert cause.payload["subsource"] == "operator_reject"
    assert cause.payload["original_rule"] == "secret_in_diff"
    assert ["gh", "pr", "close", "42", "--comment", "Guardrail violation rejected by operator"] in calls


def test_guardrail_decision_no_pending_cause_returns_404(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_repo(tmp_path, monkeypatch)
    redis = _redis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis))

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/guardrail/PR-305/decision",
            json={"decision": "approve"},
        )

    assert response.status_code == 404
    assert response.json()["error"] == "PR has no pending guardrail decision"


def test_guardrail_decision_wrong_subsource_returns_404(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_repo(tmp_path, monkeypatch)
    redis = _redis(CancellationCause(category="ERROR", payload={"subsource": "coder"}))
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis))

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/guardrail/PR-305/decision",
            json={"decision": "approve"},
        )

    assert response.status_code == 404


def test_guardrail_decision_invalid_decision_returns_400(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_repo(tmp_path, monkeypatch)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(_redis(_guardrail_cause())))

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/guardrail/PR-305/decision",
            json={"decision": "approven"},
        )

    assert response.status_code == 400


def test_guardrail_decision_invalid_pr_id_format_returns_400(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_repo(tmp_path, monkeypatch)

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/guardrail/PR-12345/decision",
            json={"decision": "approve"},
        )

    assert response.status_code == 400


def test_guardrail_decision_concurrent_change_returns_409(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_repo(tmp_path, monkeypatch)
    redis = _redis(_guardrail_cause(), conflict=True)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis))
    async def fake_remove(*args: Any) -> None:
        return None

    monkeypatch.setattr(repo_control, "_remove_escalated_label", fake_remove)
    monkeypatch.setattr(repo_control, "_commit_task_status_change", lambda *args: None)

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/guardrail/PR-305/decision",
            json={"decision": "approve"},
        )

    assert response.status_code == 409
    assert response.json()["error"] == "Concurrent state change; retry"


def test_guardrail_pending_returns_sorted_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_repo(tmp_path, monkeypatch)
    first = _guardrail_cause("large")
    first.created_at = "2026-05-14T00:00:02+00:00"
    second = _guardrail_cause("secret")
    second.task_id = "PR-304"
    second.created_at = "2026-05-14T00:00:01+00:00"
    redis = _GuardrailRedis(
        {
            cause_key("example__alpha", "PR-305"): first.to_redis(),
            cause_key("example__alpha", "PR-304"): second.to_redis(),
        },
        {index_key("example__alpha"): {"PR-305": 2.0, "PR-304": 1.0}},
    )
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis))

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/guardrail/pending")

    assert response.status_code == 200
    assert [item["pr_id"] for item in response.json()["pending"]] == ["PR-304", "PR-305"]


def test_guardrail_pending_empty_returns_empty_list(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_repo(tmp_path, monkeypatch)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(_GuardrailRedis()))

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/guardrail/pending")

    assert response.status_code == 200
    assert response.json() == {"pending": []}


def test_guardrail_approve_uses_phase2_frontmatter_writer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_dir = _write_repo(tmp_path, monkeypatch)
    redis = _redis(_guardrail_cause())
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis))
    async def fake_remove(*args: Any) -> None:
        return None

    monkeypatch.setattr(repo_control, "_remove_escalated_label", fake_remove)
    monkeypatch.setattr(repo_control, "_commit_task_status_change", lambda *args: None)
    calls: list[tuple[Path, str]] = []
    monkeypatch.setattr(
        repo_control,
        "write_frontmatter_status",
        lambda path, status: calls.append((path, status)),
    )

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/guardrail/PR-305/decision",
            json={"decision": "approve"},
        )

    assert response.status_code == 204
    assert calls == [(repo_dir / "tasks" / "PR-305.md", "TODO")]


def test_guardrail_approve_handles_writer_failure_gracefully(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_repo(tmp_path, monkeypatch)
    redis = _redis(_guardrail_cause())
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis))
    label_calls: list[bool] = []
    async def fake_remove(*args: Any) -> None:
        label_calls.append(True)

    monkeypatch.setattr(repo_control, "_remove_escalated_label", fake_remove)
    monkeypatch.setattr(
        repo_control,
        "write_frontmatter_status",
        lambda *args: (_ for _ in ()).throw(OSError("boom")),
    )

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/guardrail/PR-305/decision",
            json={"decision": "approve"},
        )

    assert response.status_code == 204
    assert label_calls == [True]
    assert cause_key("example__alpha", "PR-305") not in redis.store


def test_guardrail_owner_repo_parses_ssh_url() -> None:
    assert repo_control._owner_repo_from_url("git@github.com:example/alpha.git") == "example/alpha"


def test_guardrail_commit_task_status_truncates_long_reason(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []
    monkeypatch.setattr(
        repo_control,
        "_run_guardrail_git",
        lambda repo_root, *args: calls.append(list(args))
        or subprocess.CompletedProcess(list(args), 0, "", ""),
    )

    repo_control._commit_task_status_change(
        tmp_path,
        Path("tasks/PR-305.md"),
        "PR-305",
        "TODO",
        "x" * 100,
        "main",
    )

    commit = next(call for call in calls if call[0] == "commit")
    assert commit[2].endswith("...")
    assert len(commit[2].split(": ", 1)[1]) == 80


@pytest.mark.asyncio
async def test_guardrail_find_open_pr_scans_gh_list(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        current_task=QueueTask(
            pr_id="PR-305",
            title="Task",
            status=TaskStatus.ERROR,
            branch="feature-branch",
        ),
    )
    monkeypatch.setattr(
        repo_control,
        "_run_guardrail_gh",
        lambda *args: subprocess.CompletedProcess(
            list(args),
            0,
            '[{"number": 77, "headRefName": "feature-branch", "title": "x"}]',
            "",
        ),
    )

    assert await repo_control._find_open_guardrail_pr(tmp_path, "PR-305", state) == 77


@pytest.mark.asyncio
async def test_guardrail_find_open_pr_handles_gh_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*args: Any) -> subprocess.CompletedProcess[str]:
        raise subprocess.CalledProcessError(1, "gh")

    monkeypatch.setattr(repo_control, "_run_guardrail_gh", fail)
    assert await repo_control._find_open_guardrail_pr(tmp_path, "PR-305", None) is None

    monkeypatch.setattr(
        repo_control,
        "_run_guardrail_gh",
        lambda *args: subprocess.CompletedProcess(list(args), 0, "{", ""),
    )
    assert await repo_control._find_open_guardrail_pr(tmp_path, "PR-305", None) is None

    monkeypatch.setattr(
        repo_control,
        "_run_guardrail_gh",
        lambda *args: subprocess.CompletedProcess(
            list(args),
            0,
            '[{"number": "bad", "headRefName": "PR-305", "title": ""}]',
            "",
        ),
    )
    assert await repo_control._find_open_guardrail_pr(tmp_path, "PR-305", None) is None

    monkeypatch.setattr(
        repo_control,
        "_run_guardrail_gh",
        lambda *args: subprocess.CompletedProcess(
            list(args),
            0,
            '[{"number": 1, "headRefName": "other", "title": ""}]',
            "",
        ),
    )
    assert await repo_control._find_open_guardrail_pr(tmp_path, "PR-305", None) is None


@pytest.mark.asyncio
async def test_guardrail_label_and_close_failures_are_best_effort(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*args: Any) -> subprocess.CompletedProcess[str]:
        raise OSError("boom")

    monkeypatch.setattr(repo_control, "_run_guardrail_gh", fail)

    await repo_control._remove_escalated_label(
        tmp_path,
        "https://github.com/example/alpha.git",
        42,
    )
    await repo_control._close_guardrail_pr(tmp_path, 42)


def test_guardrail_pending_repo_not_found_returns_404(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "config.yml").write_text("repositories: []\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(_GuardrailRedis()))

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/guardrail/pending")

    assert response.status_code == 404


def test_guardrail_pending_without_redis_returns_503(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_repo(tmp_path, monkeypatch)
    monkeypatch.setattr(
        web_app,
        "aioredis",
        type("_Aioredis", (), {"from_url": staticmethod(lambda *args, **kwargs: None)})(),
    )

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/guardrail/pending")

    assert response.status_code == 503


def test_guardrail_decision_bad_json_returns_400(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_repo(tmp_path, monkeypatch)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(_redis(_guardrail_cause())))

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/guardrail/PR-305/decision",
            content="{",
            headers={"content-type": "application/json"},
        )

    assert response.status_code == 400


def test_guardrail_decision_repo_not_found_returns_404(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "config.yml").write_text("repositories: []\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/guardrail/PR-305/decision",
            json={"decision": "approve"},
        )

    assert response.status_code == 404


def test_guardrail_decision_without_redis_returns_503(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_repo(tmp_path, monkeypatch)
    monkeypatch.setattr(
        web_app,
        "aioredis",
        type("_Aioredis", (), {"from_url": staticmethod(lambda *args, **kwargs: None)})(),
    )

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/guardrail/PR-305/decision",
            json={"decision": "approve"},
        )

    assert response.status_code == 503


def test_guardrail_decision_task_missing_returns_404(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_repo(tmp_path, monkeypatch)
    (tmp_path / "repos" / "example__alpha" / "tasks" / "PR-305.md").unlink()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(_redis(_guardrail_cause())))

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/guardrail/PR-305/decision",
            json={"decision": "approve"},
        )

    assert response.status_code == 404


def test_guardrail_decision_state_read_failure_returns_503(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_repo(tmp_path, monkeypatch)
    redis = _redis(_guardrail_cause())
    redis.store[pipeline_state("example__alpha")] = "{"
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis))

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/guardrail/PR-305/decision",
            json={"decision": "approve"},
        )

    assert response.status_code == 503


def test_guardrail_decision_malformed_cause_returns_404(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_repo(tmp_path, monkeypatch)
    redis = _redis()
    redis.store[cause_key("example__alpha", "PR-305")] = "{"
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis))

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/guardrail/PR-305/decision",
            json={"decision": "approve"},
        )

    assert response.status_code == 404


def test_guardrail_decision_open_pr_missing_returns_409(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_repo(tmp_path, monkeypatch)
    state = _state()
    state.current_pr = None
    redis = _redis(_guardrail_cause())
    redis.store[pipeline_state("example__alpha")] = state.model_dump_json()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis))
    monkeypatch.setattr(
        repo_control,
        "_run_guardrail_gh",
        lambda *args: subprocess.CompletedProcess(list(args), 0, "[]", ""),
    )

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/guardrail/PR-305/decision",
            json={"decision": "approve"},
        )

    assert response.status_code == 409


def test_guardrail_approve_commit_failure_is_best_effort(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_repo(tmp_path, monkeypatch)
    redis = _redis(_guardrail_cause())
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis))

    async def fake_remove(*args: Any) -> None:
        return None

    monkeypatch.setattr(repo_control, "_remove_escalated_label", fake_remove)
    monkeypatch.setattr(
        repo_control,
        "_commit_task_status_change",
        lambda *args: (_ for _ in ()).throw(subprocess.CalledProcessError(1, "git")),
    )

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/guardrail/PR-305/decision",
            json={"decision": "approve"},
        )

    assert response.status_code == 204
    assert cause_key("example__alpha", "PR-305") not in redis.store


@pytest.mark.asyncio
async def test_guardrail_transaction_missing_and_wrong_subsource() -> None:
    redis = _GuardrailRedis()
    assert (
        await repo_control._apply_guardrail_decision_transaction(
            redis, "example__alpha", "PR-305", "approve"
        )
        == "missing"
    )
    redis.store[cause_key("example__alpha", "PR-305")] = CancellationCause(
        category="ERROR",
        payload={"subsource": "coder"},
    ).to_redis()
    assert (
        await repo_control._apply_guardrail_decision_transaction(
            redis, "example__alpha", "PR-305", "approve"
        )
        == "missing"
    )


@pytest.mark.asyncio
async def test_guardrail_read_repo_state_returns_none_for_missing_state() -> None:
    assert await repo_control._read_repo_state(_GuardrailRedis(), "example__alpha") is None


@pytest.mark.asyncio
async def test_guardrail_transaction_ignores_pipe_reset_failure() -> None:
    redis = _ResetFailRedis(
        {cause_key("example__alpha", "PR-305"): _guardrail_cause().to_redis()},
        {index_key("example__alpha"): {"PR-305": 1.0}},
    )

    result = await repo_control._apply_guardrail_decision_transaction(
        redis,
        "example__alpha",
        "PR-305",
        "approve",
    )

    assert isinstance(result, CancellationCause)


def test_guardrail_decision_reject_concurrent_change_returns_409(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_repo(tmp_path, monkeypatch)
    redis = _redis(_guardrail_cause(), conflict=True)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis))

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/guardrail/PR-305/decision",
            json={"decision": "reject"},
        )

    assert response.status_code == 409


def test_guardrail_decision_task_path_outside_repo_returns_404(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_repo(tmp_path, monkeypatch)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(_redis(_guardrail_cause())))

    async def fake_resolve(name: str, pr_id: str) -> tuple[Path, str]:
        return tmp_path / "outside.md", "outside.md"

    monkeypatch.setattr(repo_control, "_resolve_repo_task_path", fake_resolve)

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/guardrail/PR-305/decision",
            json={"decision": "approve"},
        )

    assert response.status_code == 404


def test_guardrail_decision_approve_missing_after_precheck_returns_404(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_repo(tmp_path, monkeypatch)
    redis = _redis(_guardrail_cause())
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis))

    async def fake_apply(*args: Any) -> str:
        return "missing"

    async def fake_remove(*args: Any) -> None:
        return None

    monkeypatch.setattr(repo_control, "_apply_guardrail_decision_transaction", fake_apply)
    monkeypatch.setattr(repo_control, "_remove_escalated_label", fake_remove)
    monkeypatch.setattr(repo_control, "_commit_task_status_change", lambda *args: None)

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/guardrail/PR-305/decision",
            json={"decision": "approve"},
        )

    assert response.status_code == 404


def test_guardrail_decision_reject_missing_after_precheck_returns_404(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_repo(tmp_path, monkeypatch)
    redis = _redis(_guardrail_cause())
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis))

    async def fake_apply(*args: Any) -> str:
        return "missing"

    monkeypatch.setattr(repo_control, "_apply_guardrail_decision_transaction", fake_apply)

    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/guardrail/PR-305/decision",
            json={"decision": "reject"},
        )

    assert response.status_code == 404
