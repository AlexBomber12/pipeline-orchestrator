"""Tests for the per-task diagnostic API endpoint (PR-332)."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from redis.exceptions import RedisError

from src.cancellation.storage import (
    CancellationCause,
    cause_key,
    current_run_started_at_key,
    retry_count_key,
)
from src.keyspace import pipeline_state, status_write_failed_tasks
from src.models import (
    CIStatus,
    PipelineState,
    PRInfo,
    QueueTask,
    RepoState,
    ReviewStatus,
    TaskStatus,
)
from src.web import app as web_app
from src.web.app import app
from src.web.routes import diagnostic as diagnostic_routes


class _FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.ttls: dict[str, int] = {}
        self.get_error: Exception | None = None
        self.ttl_error: Exception | None = None

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        if self.get_error is not None:
            raise self.get_error
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
        if self.ttl_error is not None:
            raise self.ttl_error
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


def _write_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    repo_slug: str = "example__alpha",
) -> Path:
    owner, name = repo_slug.split("__", 1)
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        f"  - url: https://github.com/{owner}/{name}.git\n"
        "    branch: main\n"
        "daemon:\n"
        "  retry_button_cap: 3\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "REPOS_DIR", str(tmp_path / "repos"))
    repo_dir = tmp_path / "repos" / repo_slug
    (repo_dir / "tasks").mkdir(parents=True, exist_ok=True)
    return repo_dir


def _write_task(
    repo_dir: Path,
    *,
    pr_id: str = "PR-322",
    status: str | None = "TODO",
    body: str = "Body content\n",
    branch: str = "pr-322-feature",
) -> Path:
    if status is None:
        text = f"# {pr_id}: Title\n\nBranch: {branch}\n\n{body}"
    else:
        text = (
            f"---\nstatus: {status}\n---\n\n# {pr_id}: Title\n\n"
            f"Branch: {branch}\n\n{body}"
        )
    path = repo_dir / "tasks" / f"{pr_id}.md"
    path.write_text(text, encoding="utf-8")
    return path


def _stub_aioredis(monkeypatch: pytest.MonkeyPatch, redis_client: _FakeRedis) -> None:
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))


def _seed_state(
    redis_client: _FakeRedis,
    slug: str,
    *,
    state: PipelineState = PipelineState.ERROR,
    pr_id: str = "PR-322",
    branch: str = "pr-322-feature",
    skip_ai_error_diagnose: bool = False,
    current_pr: PRInfo | None = None,
) -> None:
    repo_state = RepoState(
        url="https://github.com/example/alpha.git",
        name=slug,
        state=state,
        current_task=QueueTask(
            pr_id=pr_id,
            title=pr_id,
            status=TaskStatus.ERROR,
            task_file=f"tasks/{pr_id}.md",
            branch=branch,
        ),
        current_pr=current_pr,
        skip_ai_error_diagnose=skip_ai_error_diagnose,
    )
    redis_client.values[pipeline_state(slug)] = repo_state.model_dump_json()


def _record_cause(
    redis_client: _FakeRedis,
    slug: str,
    pr_id: str,
    payload: dict[str, Any],
    *,
    created_at: str | None = None,
    ttl: int | None = 2592000,
) -> None:
    created = created_at or datetime(2026, 5, 17, 8, 23, tzinfo=timezone.utc).isoformat()
    cause = CancellationCause(
        category="ERROR",
        payload=payload,
        created_at=created,
        task_id=pr_id,
        repo_slug=slug,
    )
    redis_client.values[cause_key(slug, pr_id)] = cause.to_redis()
    if ttl is not None:
        redis_client.ttls[cause_key(slug, pr_id)] = ttl


def _get(name: str, task_id: str) -> Any:
    with TestClient(app) as client:
        return client.get(f"/api/diagnostic/{name}/{task_id}")


def test_diagnostic_returns_all_fields_for_stuck_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_dir = _write_config(tmp_path, monkeypatch)
    _write_task(repo_dir, pr_id="PR-322", status="ERROR")
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)
    _seed_state(redis_client, "example__alpha")
    _record_cause(
        redis_client,
        "example__alpha",
        "PR-322",
        {"subsource": "fix_iteration_cap", "iteration_count": 25},
        ttl=2_592_000,
    )
    redis_client.values[retry_count_key("example__alpha", "PR-322")] = "3"
    redis_client.values[
        diagnostic_routes._retry_fingerprint_key("example__alpha", "PR-322")
    ] = "stale-fingerprint"
    redis_client.values[
        current_run_started_at_key("example__alpha", "PR-322")
    ] = "2026-05-17T08:23:00+00:00"
    redis_client.values[
        diagnostic_routes._attempt_count_key("example__alpha", "PR-322")
    ] = "4"
    redis_client.values[status_write_failed_tasks("example__alpha")] = (
        '["PR-322", "PR-999"]'
    )

    body = _get("example__alpha", "PR-322").json()
    expected_keys = {
        "repo_slug",
        "task_id",
        "frontmatter_status",
        "cancellation_cause",
        "subsource_metadata",
        "retry_count",
        "retry_fingerprint",
        "retry_fingerprint_matches_current_spec",
        "current_run_started_at",
        "attempt_count",
        "status_write_failed",
        "skip_ai_error_diagnose",
        "_error_diagnose_count",
        "_error_skip_count",
        "current_pr",
        "ttls",
    }
    assert expected_keys <= set(body)
    assert body["repo_slug"] == "example__alpha"
    assert body["task_id"] == "PR-322"
    assert body["frontmatter_status"] == "ERROR"
    assert body["cancellation_cause"]["payload"]["subsource"] == "fix_iteration_cap"
    assert body["subsource_metadata"]["user_label"] == "FIX iteration cap"
    assert body["retry_count"] == 3
    assert body["retry_fingerprint"] == "stale-fingerprint"
    assert body["retry_fingerprint_matches_current_spec"] is False
    assert body["current_run_started_at"] == "2026-05-17T08:23:00+00:00"
    assert body["attempt_count"] == 4
    assert body["status_write_failed"] is True
    assert body["skip_ai_error_diagnose"] is False
    assert body["_error_diagnose_count"] == 0
    assert body["_error_skip_count"] == 0
    assert body["ttls"]["cancellation_cause"] == 2_592_000


def test_diagnostic_returns_nulls_for_missing_keys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)

    body = _get("example__alpha", "PR-322").json()
    assert body["frontmatter_status"] is None
    assert body["cancellation_cause"] is None
    assert body["subsource_metadata"] is None
    assert body["retry_count"] == 0
    assert body["retry_fingerprint"] is None
    assert body["retry_fingerprint_matches_current_spec"] is False
    assert body["current_run_started_at"] is None
    assert body["attempt_count"] == 0
    assert body["status_write_failed"] is False
    assert body["skip_ai_error_diagnose"] is False
    assert body["current_pr"] is None
    assert body["ttls"] == {
        "cancellation_cause": None,
        "retry_count": None,
        "retry_fingerprint": None,
        "current_run_started_at": None,
        "attempt_count": None,
    }


def test_diagnostic_subsource_metadata_resolved_via_registry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)
    _record_cause(redis_client, "example__alpha", "PR-322", {"subsource": "crash"})

    body = _get("example__alpha", "PR-322").json()
    assert body["subsource_metadata"]["user_label"] == "Daemon crash"
    assert body["subsource_metadata"]["severity"] == "high"
    assert body["subsource_metadata"]["group_bucket"] == "daemon"


def test_diagnostic_subsource_unknown_returns_null_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)
    _record_cause(
        redis_client,
        "example__alpha",
        "PR-322",
        {"subsource": "custom_value"},
    )

    body = _get("example__alpha", "PR-322").json()
    assert body["subsource_metadata"] is None


def test_diagnostic_handles_redis_unavailable_gracefully(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    redis_client.get_error = RedisError("connection refused")
    _stub_aioredis(monkeypatch, redis_client)

    resp = _get("example__alpha", "PR-322")
    assert resp.status_code == 503
    assert resp.json() == {"error": "redis unavailable"}


def test_diagnostic_pr_info_includes_state_when_pr_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)
    current_pr = PRInfo(
        number=444,
        branch="pr-322-feature",
        url="https://github.com/example/alpha/pull/444",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )
    _seed_state(redis_client, "example__alpha", current_pr=current_pr)

    calls: list[tuple[str, int]] = []

    def fake_pr_state(repo: str, pr_number: int) -> dict[str, str | None]:
        calls.append((repo, pr_number))
        return {"state": "OPEN", "mergedAt": None, "closedAt": None}

    monkeypatch.setattr(diagnostic_routes.gh_prs, "pr_state", fake_pr_state)
    body = _get("example__alpha", "PR-322").json()
    assert calls == [("example/alpha", 444)]
    assert body["current_pr"] == {
        "number": 444,
        "state": "OPEN",
        "url": "https://github.com/example/alpha/pull/444",
    }


def test_diagnostic_pr_info_null_when_no_pr_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)
    current_pr = PRInfo(
        number=444,
        branch="pr-322-feature",
        url="https://github.com/example/alpha/pull/444",
    )
    _seed_state(redis_client, "example__alpha", current_pr=current_pr)

    def fake_pr_state(repo: str, pr_number: int) -> dict[str, str | None] | None:
        return None

    monkeypatch.setattr(diagnostic_routes.gh_prs, "pr_state", fake_pr_state)
    body = _get("example__alpha", "PR-322").json()
    assert body["current_pr"] is None


def test_diagnostic_ttls_section_includes_keys_with_remaining_ttl(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)
    _record_cause(
        redis_client,
        "example__alpha",
        "PR-322",
        {"subsource": "crash"},
        ttl=1234,
    )

    body = _get("example__alpha", "PR-322").json()
    assert isinstance(body["ttls"]["cancellation_cause"], int)
    assert body["ttls"]["cancellation_cause"] > 0
    assert body["ttls"]["cancellation_cause"] == 1234


def test_diagnostic_rejects_invalid_task_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)
    resp = _get("example__alpha", "not-a-pr")
    assert resp.status_code == 400
    assert resp.json() == {"error": "invalid task id"}


def test_diagnostic_unknown_repo_returns_404(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)
    resp = _get("nonexistent", "PR-322")
    assert resp.status_code == 404
    assert resp.json() == {"error": "repo not found"}


def test_diagnostic_redis_not_attached_returns_503(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    if hasattr(web_app.app.state, "redis"):
        monkeypatch.delattr(web_app.app.state, "redis", raising=False)
    client = TestClient(web_app.app)
    resp = client.get("/api/diagnostic/example__alpha/PR-322")
    assert resp.status_code == 503
    assert resp.json() == {"error": "redis unavailable"}


def test_diagnostic_invalid_repo_state_treated_as_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)
    redis_client.values[pipeline_state("example__alpha")] = "{not json"

    body = _get("example__alpha", "PR-322").json()
    assert body["skip_ai_error_diagnose"] is False
    assert body["current_pr"] is None


def test_diagnostic_skip_ai_error_diagnose_propagated_from_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)
    _seed_state(
        redis_client,
        "example__alpha",
        skip_ai_error_diagnose=True,
    )

    body = _get("example__alpha", "PR-322").json()
    assert body["skip_ai_error_diagnose"] is True


def test_diagnostic_status_write_failed_malformed_payload_is_false(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)
    redis_client.values[status_write_failed_tasks("example__alpha")] = "not json"

    body = _get("example__alpha", "PR-322").json()
    assert body["status_write_failed"] is False


def test_diagnostic_status_write_failed_non_list_is_false(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)
    redis_client.values[status_write_failed_tasks("example__alpha")] = '{"x": 1}'

    body = _get("example__alpha", "PR-322").json()
    assert body["status_write_failed"] is False


def test_diagnostic_fingerprint_matches_current_spec(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_dir = _write_config(tmp_path, monkeypatch)
    task_path = _write_task(repo_dir, pr_id="PR-322", status="ERROR")
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)
    current_fp = diagnostic_routes._current_spec_fingerprint(task_path)
    assert current_fp is not None
    redis_client.values[
        diagnostic_routes._retry_fingerprint_key("example__alpha", "PR-322")
    ] = current_fp

    body = _get("example__alpha", "PR-322").json()
    assert body["retry_fingerprint"] == current_fp
    assert body["retry_fingerprint_matches_current_spec"] is True


def test_diagnostic_retry_count_zero_when_value_unparseable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)
    redis_client.values[retry_count_key("example__alpha", "PR-322")] = "not-int"

    body = _get("example__alpha", "PR-322").json()
    assert body["retry_count"] == 0


def test_diagnostic_decodes_bytes_redis_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)
    # Simulate decode_responses=False client returning bytes.
    redis_client.values[retry_count_key("example__alpha", "PR-322")] = b"7"  # type: ignore[assignment]
    redis_client.values[
        diagnostic_routes._retry_fingerprint_key("example__alpha", "PR-322")
    ] = b"deadbeef"  # type: ignore[assignment]

    body = _get("example__alpha", "PR-322").json()
    assert body["retry_count"] == 7
    assert body["retry_fingerprint"] == "deadbeef"


def test_diagnostic_no_frontmatter_returns_null_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_dir = _write_config(tmp_path, monkeypatch)
    _write_task(repo_dir, pr_id="PR-322", status=None)
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)

    body = _get("example__alpha", "PR-322").json()
    assert body["frontmatter_status"] is None


def test_diagnostic_frontmatter_without_status_returns_null(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_dir = _write_config(tmp_path, monkeypatch)
    path = repo_dir / "tasks" / "PR-322.md"
    path.write_text(
        "---\npriority: 3\n---\n\n# PR-322: Title\n\nBody\n",
        encoding="utf-8",
    )
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)

    body = _get("example__alpha", "PR-322").json()
    assert body["frontmatter_status"] is None


def test_diagnostic_empty_task_file_returns_null_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_dir = _write_config(tmp_path, monkeypatch)
    path = repo_dir / "tasks" / "PR-322.md"
    path.write_text("", encoding="utf-8")
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)

    body = _get("example__alpha", "PR-322").json()
    assert body["frontmatter_status"] is None


def test_diagnostic_invalid_repo_url_yields_null_current_pr(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        "  - url: not-a-url\n"
        "    branch: main\n"
        "daemon:\n"
        "  retry_button_cap: 3\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "REPOS_DIR", str(tmp_path / "repos"))
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)
    current_pr = PRInfo(
        number=42,
        branch="pr-322-feature",
        url="https://github.com/example/alpha/pull/42",
    )
    _seed_state(redis_client, "not-a-url", current_pr=current_pr)

    body = _get("not-a-url", "PR-322").json()
    assert body["current_pr"] is None


def test_diagnostic_pr_info_null_when_task_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)
    current_pr = PRInfo(
        number=99,
        branch="pr-other-feature",
        url="https://github.com/example/alpha/pull/99",
    )
    _seed_state(
        redis_client,
        "example__alpha",
        pr_id="PR-999",
        current_pr=current_pr,
    )

    body = _get("example__alpha", "PR-322").json()
    assert body["current_pr"] is None


def test_diagnostic_frontmatter_without_status_or_close_returns_null(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_dir = _write_config(tmp_path, monkeypatch)
    path = repo_dir / "tasks" / "PR-322.md"
    path.write_text("---\npriority: 3\n", encoding="utf-8")
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)

    body = _get("example__alpha", "PR-322").json()
    assert body["frontmatter_status"] is None


def test_diagnostic_unreadable_task_file_returns_null_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_dir = _write_config(tmp_path, monkeypatch)
    path = repo_dir / "tasks" / "PR-322.md"
    path.write_bytes(b"---\nstatus: TODO\n---\n\xff\xfe non-utf8")
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)

    body = _get("example__alpha", "PR-322").json()
    assert body["frontmatter_status"] is None
    assert body["retry_fingerprint_matches_current_spec"] is False


def test_diagnostic_cause_without_subsource_returns_null_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)
    _record_cause(redis_client, "example__alpha", "PR-322", {"reason_text": "boom"})

    body = _get("example__alpha", "PR-322").json()
    assert body["subsource_metadata"] is None


def test_diagnostic_malformed_cancellation_cause_degrades_to_null(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)
    redis_client.values[cause_key("example__alpha", "PR-322")] = "{not json"
    redis_client.values[retry_count_key("example__alpha", "PR-322")] = "2"

    resp = _get("example__alpha", "PR-322")
    assert resp.status_code == 200
    body = resp.json()
    assert body["cancellation_cause"] is None
    assert body["subsource_metadata"] is None
    assert body["retry_count"] == 2


def test_diagnostic_legacy_cancellation_cause_shape_degrades_to_null(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)
    # Valid JSON, but not a dict — CancellationCause.from_redis unpacks
    # with ** and raises TypeError on a list payload.
    redis_client.values[cause_key("example__alpha", "PR-322")] = "[1, 2, 3]"

    resp = _get("example__alpha", "PR-322")
    assert resp.status_code == 200
    assert resp.json()["cancellation_cause"] is None


def test_diagnostic_ttl_failure_yields_none_ttl(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_aioredis(monkeypatch, redis_client)
    _record_cause(
        redis_client,
        "example__alpha",
        "PR-322",
        {"subsource": "crash"},
        ttl=42,
    )
    redis_client.ttl_error = RuntimeError("ttl failure")

    body = _get("example__alpha", "PR-322").json()
    assert body["ttls"]["cancellation_cause"] is None
