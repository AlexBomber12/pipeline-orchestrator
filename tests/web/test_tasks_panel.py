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
        self.zsets: dict[str, dict[str, float]] = {}
        self.ttls: dict[str, int] = {}

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def aclose(self) -> None:
        return None

    async def delete(self, key: str) -> int:
        return int(self.store.pop(key, None) is not None)

    async def zrem(self, key: str, *members: str) -> int:
        return 0

    async def zadd(self, key: str, mapping: dict[str, float]) -> int:
        bucket = self.zsets.setdefault(key, {})
        added = 0
        for member, score in mapping.items():
            if member not in bucket:
                added += 1
            bucket[member] = float(score)
        return added

    async def expire(self, key: str, seconds: int) -> bool:
        if key in self.store or key in self.zsets:
            self.ttls[key] = seconds
            return True
        return False


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
    task_status: TaskStatus = TaskStatus.ERROR,
    unresolved_deps: list[str] | None = None,
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
                status=task_status,
                branch="pr-283",
                unresolved_deps=list(unresolved_deps or []),
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


def test_renders_unresolved_deps_red_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _setup_panel(
        tmp_path,
        monkeypatch,
        retry_count=0,
        task_status=TaskStatus.TODO,
        unresolved_deps=["PR-275a", "PR-275b"],
    )

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks")

    assert response.status_code == 200
    body = response.text
    assert "Blocked by: PR-275a, PR-275b" in body
    assert "border-fail/40 bg-fail/10" in body


def test_normal_todo_no_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _setup_panel(
        tmp_path,
        monkeypatch,
        retry_count=0,
        task_status=TaskStatus.TODO,
    )

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks")

    assert response.status_code == 200
    assert "Blocked by:" not in response.text


def _setup_multi_error_panel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    error_tasks: list[tuple[str, str | None]],
    retry_cap: int = 3,
) -> None:
    """Install a panel where each entry in ``error_tasks`` becomes an ERROR
    task plus, when subsource is non-None, an associated cancellation_cause
    row in Redis. PR-310 sub-grouping renders depend on both signals so
    the helper bundles them.
    """
    import json as _json

    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        f"daemon:\n  retry_button_cap: {retry_cap}\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "REPOS_DIR", str(tmp_path / "repos"))
    tasks_dir = tmp_path / "repos" / "example__alpha" / "tasks"
    tasks_dir.mkdir(parents=True)
    queue_entries = []
    for pr_id, _subsource in error_tasks:
        (tasks_dir / f"{pr_id}.md").write_text(
            f"---\nstatus: ERROR\n---\n\n# {pr_id}: Sub-grouping case\n\nBody\n",
            encoding="utf-8",
        )
        queue_entries.append(
            QueueTask(
                pr_id=pr_id,
                title=f"{pr_id} title",
                status=TaskStatus.ERROR,
                branch=f"branch-{pr_id.lower()}",
            )
        )
    state = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        current_queue=queue_entries,
    )
    store: dict[str, str] = {
        "pipeline:example__alpha": state.model_dump_json(),
    }
    for pr_id, subsource in error_tasks:
        store[f"metrics:retry_count:example__alpha:{pr_id}"] = "0"
        if subsource is not None:
            store[f"cancellation:example__alpha:{pr_id}"] = _json.dumps(
                {
                    "category": "ERROR",
                    "payload": {"subsource": subsource},
                    "created_at": "2026-05-15T10:00:00+00:00",
                    "task_id": pr_id,
                    "repo_slug": "example__alpha",
                }
            )
    monkeypatch.setattr(web_app, "aioredis", _aioredis(_PanelRedis(store)))


def test_error_group_renders_guardrail_subgroup_when_present(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-310: with at least one guardrail-subsource entry the ERROR group
    splits into a guardrail subgroup (operator decision needed) and an
    other subgroup (automatic failure). Tasks land in the bucket that
    matches their cancellation cause payload, not their order in the
    queue.
    """
    _setup_multi_error_panel(
        tmp_path,
        monkeypatch,
        error_tasks=[
            ("PR-401", "guardrail"),
            ("PR-402", "guardrail"),
            ("PR-403", "coder_escalate"),
        ],
    )

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks")

    assert response.status_code == 200
    body = response.text
    assert "error-subgroup-guardrail" in body
    assert "error-subgroup-other" in body
    # Guardrail subgroup carries both guardrail tasks; the other subgroup
    # carries the coder_escalate entry. Slicing the body around the
    # subgroup wrappers keeps the assertion robust against unrelated
    # markup churn.
    guardrail_section = body.split("error-subgroup-guardrail", 1)[1].split(
        "error-subgroup-other", 1
    )[0]
    other_section = body.split("error-subgroup-other", 1)[1]
    assert "PR-401" in guardrail_section
    assert "PR-402" in guardrail_section
    assert "PR-403" not in guardrail_section
    assert "PR-403" in other_section
    assert "Operator decision needed" in guardrail_section


def test_error_group_renders_flat_when_no_guardrail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-310: without any guardrail subsource the original flat ERROR list
    renders unchanged — sub-grouping is opt-in on guardrail presence so
    existing operator muscle memory survives."""
    _setup_multi_error_panel(
        tmp_path,
        monkeypatch,
        error_tasks=[
            ("PR-411", "coder_escalate"),
            ("PR-412", "review_timeout"),
        ],
    )

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks")

    assert response.status_code == 200
    body = response.text
    assert "error-subgroup-guardrail" not in body
    assert "error-subgroup-other" not in body
    assert "PR-411" in body
    assert "PR-412" in body


def test_error_group_redis_read_error_falls_into_other(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-310: a Redis ``get`` failure while reading the cancellation
    cause for an ERROR task must not 5xx the panel; the task falls into
    the "other" bucket the same way a missing record does."""
    from src.web.routes import repo_control as repo_control_module

    async def boom(
        redis_client, repo_slug, task_id, *, refresh_ttl: bool = True
    ):  # pragma: no cover
        raise ConnectionError("redis unreachable")

    _setup_panel(tmp_path, monkeypatch, retry_count=0)
    monkeypatch.setattr(
        repo_control_module, "get_cancellation_cause", boom
    )

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks")

    assert response.status_code == 200
    body = response.text
    # No guardrail entries means the flat list still renders.
    assert "error-subgroup-guardrail" not in body
    assert "PR-283" in body


def test_error_group_legacy_no_cause_record_falls_into_other(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-310: ERROR tasks without a cancellation_cause record (legacy /
    pre-PR-253 / Redis miss) must land in the "other" bucket whenever a
    sub-grouping is rendered, never in the guardrail bucket — operators
    should not be misled into approving/rejecting a phantom guardrail
    decision."""
    _setup_multi_error_panel(
        tmp_path,
        monkeypatch,
        error_tasks=[
            ("PR-421", "guardrail"),
            ("PR-422", None),
        ],
    )

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks")

    assert response.status_code == 200
    body = response.text
    assert "error-subgroup-guardrail" in body
    other_section = body.split("error-subgroup-other", 1)[1]
    assert "PR-422" in other_section
    guardrail_section = body.split("error-subgroup-guardrail", 1)[1].split(
        "error-subgroup-other", 1
    )[0]
    assert "PR-422" not in guardrail_section


def test_tasks_panel_subsource_read_does_not_refresh_ttl(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-345 follow-up: the tasks panel renders an aggregate ERROR view
    that reads each task's cancellation cause to pick the subsource
    bucket. This is a display read, not an explicit per-record
    investigation, so it must pass ``refresh_ttl=False`` — otherwise
    opening the queue would pin every ERROR record's TTL to the 90-day
    forensic ceiling.
    """
    from src.web.routes import repo_control as repo_control_module

    captured: list[bool] = []

    async def spy(
        redis_client, repo_slug, task_id, *, refresh_ttl: bool = True
    ):
        captured.append(refresh_ttl)
        return None

    _setup_multi_error_panel(
        tmp_path,
        monkeypatch,
        error_tasks=[
            ("PR-431", "guardrail"),
            ("PR-432", "coder_escalate"),
        ],
    )
    monkeypatch.setattr(
        repo_control_module, "get_cancellation_cause", spy
    )

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks")

    assert response.status_code == 200
    assert captured, "expected get_cancellation_cause to be invoked"
    assert all(value is False for value in captured)
