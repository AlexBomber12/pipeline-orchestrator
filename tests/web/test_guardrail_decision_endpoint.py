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
        PRInfo(number=pr_number, branch="main") if active else None
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
        "---\nstatus: ERROR\n---\n\n# PR-305c\n\nBranch: pr-305c-feature\n\nBody\n",
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


def test_guardrail_decision_approve_with_pending_guardrail_cause(
    tmp_path, monkeypatch
) -> None:
    repo_dir, redis_client = _setup(
        tmp_path,
        monkeypatch,
        store={
            "pipeline:example__alpha": _seed_state(),
            cause_key("example__alpha", "PR-305c"): _seed_cause(
                {"subsource": "guardrail", "rule": "large_diff", "excerpt": "+1800 LOC"}
            ),
        },
    )
    git_calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        git_calls.append(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)

    resp = _post("approve")

    assert resp.status_code == 204
    assert cause_key("example__alpha", "PR-305c") not in redis_client.store
    assert cause_key("example__alpha", "PR-305c") in redis_client.deleted
    assert "status: TODO" in (repo_dir / "tasks" / "PR-305c.md").read_text(
        encoding="utf-8"
    )
    commit_calls = [c for c in git_calls if "commit" in c]
    assert commit_calls and "[skip ci]" in commit_calls[0]
    assert "chore(tasks): guardrail decision approve for PR-305c [skip ci]" in (
        " ".join(commit_calls[0])
    )
    # Base branch must be checked out and hard-reset BEFORE the push so the
    # commit lands on `main` regardless of which branch the worktree was on.
    git_subcmds = [c[3] for c in git_calls if c[:3] == ["git", "-C", str(repo_dir)]]
    push_idx = git_subcmds.index("push")
    assert "fetch" in git_subcmds[:push_idx]
    assert "checkout" in git_subcmds[:push_idx]
    assert "reset" in git_subcmds[:push_idx]
    stored = RepoState.model_validate_json(redis_client.store["pipeline:example__alpha"])
    assert stored.state == PipelineState.WATCH


def test_guardrail_decision_approve_uses_queue_mapped_task_file(
    tmp_path, monkeypatch
) -> None:
    """Approve must honor queue ``task_file`` mappings, not hardcode tasks/{pr_id}.md."""
    repo_dir, redis_client = _setup(
        tmp_path,
        monkeypatch,
        store={
            "pipeline:example__alpha": _seed_state(
                current_queue=[
                    QueueTask(
                        pr_id="PR-305c",
                        title="PR-305c",
                        status=TaskStatus.ERROR,
                        task_file="tasks/custom-name.md",
                    )
                ]
            ),
            cause_key("example__alpha", "PR-305c"): _seed_cause(
                {"subsource": "guardrail"}
            ),
        },
    )
    # Default file is absent; mapped file exists at a non-default name.
    (repo_dir / "tasks" / "PR-305c.md").unlink()
    (repo_dir / "tasks" / "custom-name.md").write_text(
        "---\nstatus: ERROR\n---\n\n# PR-305c\n\nBranch: pr-305c-feature\n\nBody\n",
        encoding="utf-8",
    )
    git_calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        git_calls.append(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    resp = _post("approve")
    assert resp.status_code == 204
    assert "status: TODO" in (repo_dir / "tasks" / "custom-name.md").read_text(
        encoding="utf-8"
    )
    add_calls = [c for c in git_calls if "add" in c]
    assert add_calls and add_calls[0][-1] == "tasks/custom-name.md"


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


def test_guardrail_decision_approve_attempts_label_removal(
    tmp_path, monkeypatch
) -> None:
    _setup(
        tmp_path,
        monkeypatch,
        store={
            "pipeline:example__alpha": _seed_state(),
            cause_key("example__alpha", "PR-305c"): _seed_cause(
                {"subsource": "guardrail"}
            ),
        },
    )
    gh_calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        gh_calls.append(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    resp = _post("approve")
    assert resp.status_code == 204
    label_calls = [c for c in gh_calls if c[:2] == ["gh", "api"]]
    assert label_calls and label_calls[0] == [
        "gh",
        "api",
        "-X",
        "DELETE",
        "repos/example/alpha/issues/99/labels/escalated",
    ]


def test_guardrail_decision_approve_label_removal_failure_continues(
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

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if args[:2] == ["gh", "api"]:
            return subprocess.CompletedProcess(args, 1, "", "not found")
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    resp = _post("approve")
    assert resp.status_code == 204
    assert "status: TODO" in (repo_dir / "tasks" / "PR-305c.md").read_text(
        encoding="utf-8"
    )


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
    assert "reject and re-upload" in resp.text


def test_guardrail_decision_approve_concurrent_change_returns_409(
    tmp_path, monkeypatch
) -> None:
    repo_dir, redis_client = _setup(
        tmp_path,
        monkeypatch,
        store={
            "pipeline:example__alpha": _seed_state(),
            cause_key("example__alpha", "PR-305c"): _seed_cause(
                {"subsource": "guardrail"}
            ),
        },
    )
    redis_client.pending_watch_error = True
    git_calls: list[list[str]] = []
    gh_calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if args and args[0] == "git":
            git_calls.append(args)
        elif args and args[0] == "gh":
            gh_calls.append(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    resp = _post("approve")
    assert resp.status_code == 409
    assert "Concurrent state change" in resp.text
    # CAS happens BEFORE the side effects so a concurrent reject cannot
    # interleave with frontmatter push. WatchError must therefore short
    # circuit with no git/gh subprocesses fired and the task file
    # untouched on disk. The cause stays in Redis because the EXEC failed.
    assert not git_calls
    assert not gh_calls
    assert cause_key("example__alpha", "PR-305c") in redis_client.store
    assert "status: TODO" not in (repo_dir / "tasks" / "PR-305c.md").read_text(
        encoding="utf-8"
    )


def test_guardrail_decision_reject_falls_back_to_category_and_reason_text(
    tmp_path, monkeypatch
) -> None:
    """Operator-reject preserves identifying signal regardless of cause shape.

    Daemon emissions vary: ``watch.py`` writes structured ``category`` /
    ``excerpt`` fields, while ``coding.py`` and ``fix.py`` write only
    ``reason_text="GUARDRAIL: {category}: {excerpt}"``. The reject record
    must derive ``original_rule`` / ``original_excerpt`` from whichever
    shape is present.
    """
    _, redis_client = _setup(
        tmp_path,
        monkeypatch,
        store={
            "pipeline:example__alpha": _seed_state(),
            cause_key("example__alpha", "PR-305c"): _seed_cause(
                {
                    "subsource": "guardrail",
                    "tier": "2",
                    "category": "large_diff_threshold",
                    "excerpt": "+1800 LOC across 35 files",
                }
            ),
        },
    )

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    resp = _post("reject")
    assert resp.status_code == 204
    raw = redis_client.store[cause_key("example__alpha", "PR-305c")]
    cause = CancellationCause.from_redis(raw)
    assert cause.payload["original_rule"] == "large_diff_threshold"
    assert cause.payload["original_excerpt"] == "+1800 LOC across 35 files"


def test_guardrail_decision_reject_parses_reason_text_when_fields_absent(
    tmp_path, monkeypatch
) -> None:
    """``coding.py`` / ``fix.py`` emit only ``reason_text`` — reject must parse it."""
    _, redis_client = _setup(
        tmp_path,
        monkeypatch,
        store={
            "pipeline:example__alpha": _seed_state(),
            cause_key("example__alpha", "PR-305c"): _seed_cause(
                {
                    "subsource": "guardrail",
                    "reason_text": "GUARDRAIL: secrets_leak: AWS_SECRET=...",
                }
            ),
        },
    )

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    resp = _post("reject")
    assert resp.status_code == 204
    raw = redis_client.store[cause_key("example__alpha", "PR-305c")]
    cause = CancellationCause.from_redis(raw)
    assert cause.payload["original_rule"] == "secrets_leak"
    assert cause.payload["original_excerpt"] == "AWS_SECRET=..."


def test_extract_guardrail_metadata_unparseable_reason_text() -> None:
    """When reason_text is non-conforming, fall back to empty strings."""
    rule, excerpt = repo_control._extract_guardrail_metadata(
        {"subsource": "guardrail", "reason_text": "no colon delimiters here"}
    )
    assert rule == ""
    assert excerpt == ""
    rule, excerpt = repo_control._extract_guardrail_metadata(
        {"subsource": "guardrail", "reason_text": 12345}
    )
    assert rule == ""
    assert excerpt == ""


def test_guardrail_decision_reject_records_operator_reject_cause(
    tmp_path, monkeypatch
) -> None:
    repo_dir, redis_client = _setup(
        tmp_path,
        monkeypatch,
        store={
            "pipeline:example__alpha": _seed_state(),
            cause_key("example__alpha", "PR-305c"): _seed_cause(
                {
                    "subsource": "guardrail",
                    "rule": "large_diff",
                    "excerpt": "+1800 LOC",
                }
            ),
        },
    )

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    resp = _post("reject")
    assert resp.status_code == 204
    raw = redis_client.store[cause_key("example__alpha", "PR-305c")]
    cause = CancellationCause.from_redis(raw)
    assert cause.payload["subsource"] == "operator_reject"
    assert cause.payload["original_rule"] == "large_diff"
    assert cause.payload["original_excerpt"] == "+1800 LOC"
    assert "PR #99" in cause.payload["reason_text"]
    assert (repo_dir / "tasks" / "PR-305c.md").read_text(encoding="utf-8").startswith(
        "---\nstatus: ERROR"
    )


def test_guardrail_decision_reject_attempts_pr_close(tmp_path, monkeypatch) -> None:
    _setup(
        tmp_path,
        monkeypatch,
        store={
            "pipeline:example__alpha": _seed_state(),
            cause_key("example__alpha", "PR-305c"): _seed_cause(
                {"subsource": "guardrail"}
            ),
        },
    )
    gh_calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        gh_calls.append(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    resp = _post("reject")
    assert resp.status_code == 204
    close_calls = [c for c in gh_calls if c[:3] == ["gh", "pr", "close"]]
    assert close_calls and close_calls[0] == [
        "gh",
        "pr",
        "close",
        "99",
        "--repo",
        "example/alpha",
        "--comment",
        "Guardrail violation rejected by operator",
    ]


def test_guardrail_decision_reject_falls_back_to_gh_list_when_inactive(
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
    gh_calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        gh_calls.append(args)
        if args[:3] == ["gh", "pr", "list"]:
            return subprocess.CompletedProcess(
                args,
                0,
                json.dumps(
                    [
                        {
                            "number": 503,
                            "headRefName": "pr-305c-feature",
                            "headRepositoryOwner": {"login": "example"},
                        }
                    ]
                ),
                "",
            )
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    resp = _post("reject")
    assert resp.status_code == 204
    list_calls = [c for c in gh_calls if c[:3] == ["gh", "pr", "list"]]
    close_calls = [c for c in gh_calls if c[:3] == ["gh", "pr", "close"]]
    assert list_calls
    # `gh pr list --head` must filter by the PR's head branch declared in
    # the task file (``Branch:`` header), not the repo's base branch.
    head_idx = list_calls[0].index("--head")
    assert list_calls[0][head_idx + 1] == "pr-305c-feature"
    repo_idx = list_calls[0].index("--repo")
    assert list_calls[0][repo_idx + 1] == "example/alpha"
    assert close_calls and close_calls[0][3] == "503"
    close_repo_idx = close_calls[0].index("--repo")
    assert close_calls[0][close_repo_idx + 1] == "example/alpha"


def test_guardrail_decision_reject_uses_current_task_branch_when_pr_missing(
    tmp_path, monkeypatch
) -> None:
    """Active current_task with no current_pr: head branch comes from current_task."""
    state = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.ERROR,
        current_task=QueueTask(
            pr_id="PR-305c",
            title="PR-305c",
            status=TaskStatus.ERROR,
            branch="branch-from-state",
        ),
        current_pr=None,
    )
    _setup(
        tmp_path,
        monkeypatch,
        store={
            "pipeline:example__alpha": state.model_dump_json(),
            cause_key("example__alpha", "PR-305c"): _seed_cause(
                {"subsource": "guardrail"}
            ),
        },
    )
    gh_calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        gh_calls.append(args)
        if args[:3] == ["gh", "pr", "list"]:
            return subprocess.CompletedProcess(
                args,
                0,
                json.dumps(
                    [
                        {
                            "number": 88,
                            "headRefName": "branch-from-state",
                            "headRepositoryOwner": {"login": "example"},
                        }
                    ]
                ),
                "",
            )
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    resp = _post("reject")
    assert resp.status_code == 204
    list_calls = [c for c in gh_calls if c[:3] == ["gh", "pr", "list"]]
    assert list_calls
    head_idx = list_calls[0].index("--head")
    assert list_calls[0][head_idx + 1] == "branch-from-state"


def test_read_task_branch_handles_unreadable_file(tmp_path) -> None:
    missing = tmp_path / "does-not-exist.md"
    assert repo_control._read_task_branch(missing) is None


def test_read_task_branch_ignores_body_section(tmp_path) -> None:
    """``Branch:`` lines after the first H2 must not drive ``gh pr close``.

    The canonical header lives in the preamble between frontmatter and the
    first ``## `` heading; matches from the body could otherwise target
    an unrelated PR.
    """
    task_path = tmp_path / "PR-body-branch.md"
    task_path.write_text(
        "---\nstatus: TODO\n---\n\n"
        "# PR-body-branch\n\n"
        "## Scope\n\n"
        "Example: Branch: rogue-branch\n",
        encoding="utf-8",
    )
    assert repo_control._read_task_branch(task_path) is None


def test_read_task_branch_matches_preamble_only(tmp_path) -> None:
    """Preamble ``Branch:`` is honored; body ``Branch:`` is ignored."""
    task_path = tmp_path / "PR-preamble.md"
    task_path.write_text(
        "---\nstatus: TODO\n---\n\n"
        "# PR-preamble\n\n"
        "Branch: real-branch\n"
        "- Type: feature\n\n"
        "## Scope\n\n"
        "Branch: rogue-branch\n",
        encoding="utf-8",
    )
    assert repo_control._read_task_branch(task_path) == "real-branch"


def test_guardrail_decision_reject_inactive_no_task_file_skips_close(
    tmp_path, monkeypatch
) -> None:
    repo_dir, _ = _setup(
        tmp_path,
        monkeypatch,
        store={
            "pipeline:example__alpha": _seed_state(active=False),
            cause_key("example__alpha", "PR-305c"): _seed_cause(
                {"subsource": "guardrail"}
            ),
        },
    )
    # Wipe the Branch: header so the fallback finds no head branch
    # and skips the gh lookup entirely.
    (repo_dir / "tasks" / "PR-305c.md").write_text(
        "---\nstatus: ERROR\n---\n\n# PR-305c\n\nBody\n", encoding="utf-8"
    )
    gh_calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        gh_calls.append(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    resp = _post("reject")
    assert resp.status_code == 204
    assert not any(c[:3] == ["gh", "pr", "list"] for c in gh_calls)
    assert not any(c[:3] == ["gh", "pr", "close"] for c in gh_calls)


def test_guardrail_decision_reject_no_pending_cause_returns_404(
    tmp_path, monkeypatch
) -> None:
    _setup(tmp_path, monkeypatch, store={"pipeline:example__alpha": _seed_state()})
    resp = _post("reject")
    assert resp.status_code == 404
    assert "no pending guardrail decision" in resp.text


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


@pytest.mark.asyncio
async def test_gh_lookup_pr_number_by_branch_handles_errors(monkeypatch) -> None:
    def raises(args: list[str], **kwargs: Any) -> Any:
        raise OSError("gh missing")

    monkeypatch.setattr(repo_control.subprocess, "run", raises)
    assert (
        repo_control._gh_lookup_pr_number_by_branch("main", "example/alpha")
        is None
    )

    monkeypatch.setattr(
        repo_control.subprocess,
        "run",
        lambda args, **kw: subprocess.CompletedProcess(args, 1, "", "boom"),
    )
    assert (
        repo_control._gh_lookup_pr_number_by_branch("main", "example/alpha")
        is None
    )

    monkeypatch.setattr(
        repo_control.subprocess,
        "run",
        lambda args, **kw: subprocess.CompletedProcess(args, 0, "not json", ""),
    )
    assert (
        repo_control._gh_lookup_pr_number_by_branch("main", "example/alpha")
        is None
    )

    monkeypatch.setattr(
        repo_control.subprocess,
        "run",
        lambda args, **kw: subprocess.CompletedProcess(args, 0, "[]", ""),
    )
    assert (
        repo_control._gh_lookup_pr_number_by_branch("main", "example/alpha")
        is None
    )


@pytest.mark.asyncio
async def test_gh_lookup_pr_number_by_branch_requires_owner_repo(
    monkeypatch,
) -> None:
    """Without ``owner_repo`` the helper cannot disambiguate forks; skip the
    gh call entirely so a stray subprocess cannot return a fork-owned PR."""
    called: list[list[str]] = []

    def fake_run(args: list[str], **kw: Any) -> subprocess.CompletedProcess[str]:
        called.append(args)
        return subprocess.CompletedProcess(args, 0, "[]", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    assert repo_control._gh_lookup_pr_number_by_branch("main") is None
    assert repo_control._gh_lookup_pr_number_by_branch("main", "") is None
    # Malformed ``owner_repo`` (missing slash) is rejected: without an
    # explicit owner segment the headRepositoryOwner filter cannot apply.
    assert repo_control._gh_lookup_pr_number_by_branch("main", "noslash") is None
    assert called == []


@pytest.mark.asyncio
async def test_gh_lookup_pr_number_by_branch_filters_fork_prs(monkeypatch) -> None:
    """A fork PR sharing the branch name must not match the base-repo PR."""
    payload = [
        {
            "number": 777,
            "headRefName": "shared-branch",
            "headRepositoryOwner": {"login": "fork-user"},
        },
        {
            "number": 42,
            "headRefName": "shared-branch",
            "headRepositoryOwner": {"login": "example"},
        },
    ]
    monkeypatch.setattr(
        repo_control.subprocess,
        "run",
        lambda args, **kw: subprocess.CompletedProcess(
            args, 0, json.dumps(payload), ""
        ),
    )
    assert (
        repo_control._gh_lookup_pr_number_by_branch(
            "shared-branch", "example/alpha"
        )
        == 42
    )


@pytest.mark.asyncio
async def test_gh_lookup_pr_number_by_branch_returns_none_on_zero_or_many(
    monkeypatch,
) -> None:
    """Zero or multiple base-owner matches must yield ``None`` so the
    caller cannot accidentally close an unrelated PR."""
    fork_only = [
        {
            "number": 777,
            "headRefName": "shared-branch",
            "headRepositoryOwner": {"login": "fork-user"},
        }
    ]
    monkeypatch.setattr(
        repo_control.subprocess,
        "run",
        lambda args, **kw: subprocess.CompletedProcess(
            args, 0, json.dumps(fork_only), ""
        ),
    )
    assert (
        repo_control._gh_lookup_pr_number_by_branch(
            "shared-branch", "example/alpha"
        )
        is None
    )

    two_base_owner = [
        {
            "number": 1,
            "headRefName": "shared-branch",
            "headRepositoryOwner": {"login": "example"},
        },
        {
            "number": 2,
            "headRefName": "shared-branch",
            "headRepositoryOwner": {"login": "example"},
        },
    ]
    monkeypatch.setattr(
        repo_control.subprocess,
        "run",
        lambda args, **kw: subprocess.CompletedProcess(
            args, 0, json.dumps(two_base_owner), ""
        ),
    )
    assert (
        repo_control._gh_lookup_pr_number_by_branch(
            "shared-branch", "example/alpha"
        )
        is None
    )

    # Defensive: ``gh pr list`` returning a non-list payload yields None.
    monkeypatch.setattr(
        repo_control.subprocess,
        "run",
        lambda args, **kw: subprocess.CompletedProcess(args, 0, "{}", ""),
    )
    assert (
        repo_control._gh_lookup_pr_number_by_branch(
            "shared-branch", "example/alpha"
        )
        is None
    )

    # Defensive: missing/null headRepositoryOwner (older gh schemas) yields None.
    no_owner = [{"number": 1, "headRefName": "shared-branch"}]
    monkeypatch.setattr(
        repo_control.subprocess,
        "run",
        lambda args, **kw: subprocess.CompletedProcess(
            args, 0, json.dumps(no_owner), ""
        ),
    )
    assert (
        repo_control._gh_lookup_pr_number_by_branch(
            "shared-branch", "example/alpha"
        )
        is None
    )

    # Defensive: head ref name mismatch (gh returning a superset) is dropped.
    mismatch = [
        {
            "number": 9,
            "headRefName": "different-branch",
            "headRepositoryOwner": {"login": "example"},
        }
    ]
    monkeypatch.setattr(
        repo_control.subprocess,
        "run",
        lambda args, **kw: subprocess.CompletedProcess(
            args, 0, json.dumps(mismatch), ""
        ),
    )
    assert (
        repo_control._gh_lookup_pr_number_by_branch(
            "shared-branch", "example/alpha"
        )
        is None
    )

    # Defensive: malformed entries (non-dict, non-int number) are skipped
    # so an unexpected gh schema cannot crash the helper.
    malformed = [
        "not-a-dict",
        {"number": "not-an-int", "headRepositoryOwner": {"login": "example"}},
        {
            "number": 13,
            "headRefName": "shared-branch",
            "headRepositoryOwner": {"login": "example"},
        },
    ]
    monkeypatch.setattr(
        repo_control.subprocess,
        "run",
        lambda args, **kw: subprocess.CompletedProcess(
            args, 0, json.dumps(malformed), ""
        ),
    )
    assert (
        repo_control._gh_lookup_pr_number_by_branch(
            "shared-branch", "example/alpha"
        )
        == 13
    )


@pytest.mark.asyncio
async def test_commit_guardrail_approve_tolerates_nothing_to_commit(
    tmp_path, monkeypatch
) -> None:
    calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        if "commit" in args:
            raise subprocess.CalledProcessError(
                returncode=1, cmd=args, output="", stderr="nothing to commit"
            )
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    repo_control._commit_guardrail_approve(tmp_path, Path("tasks/PR-1.md"), "msg", "main")
    assert any("push" in c for c in calls)


@pytest.mark.asyncio
async def test_commit_guardrail_approve_reraises_other_commit_errors(
    tmp_path, monkeypatch
) -> None:
    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if "commit" in args:
            raise subprocess.CalledProcessError(
                returncode=1, cmd=args, output="", stderr="merge conflict"
            )
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    with pytest.raises(subprocess.CalledProcessError):
        repo_control._commit_guardrail_approve(
            tmp_path, Path("tasks/PR-1.md"), "msg", "main"
        )


@pytest.mark.asyncio
async def test_gh_best_effort_swallows_exceptions(monkeypatch) -> None:
    def raises(args: list[str], **kwargs: Any) -> Any:
        raise OSError("nope")

    monkeypatch.setattr(repo_control.subprocess, "run", raises)
    await repo_control._gh_best_effort("repo", 1, "label", ["gh", "api"])


def test_guardrail_decision_approve_garbage_state_treated_as_inactive(
    tmp_path, monkeypatch
) -> None:
    _setup(
        tmp_path,
        monkeypatch,
        store={
            "pipeline:example__alpha": "not-json",
            cause_key("example__alpha", "PR-305c"): _seed_cause(
                {"subsource": "guardrail"}
            ),
        },
    )
    resp = _post("approve")
    assert resp.status_code == 409


def test_guardrail_decision_approve_invalid_url_skips_label_call(
    tmp_path, monkeypatch
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        "  - url: not-a-real-url\n"
        "    branch: main\n"
        "daemon:\n"
        "  retry_button_cap: 3\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "REPOS_DIR", str(tmp_path / "repos"))
    slug = "not-a-real-url"
    repo_dir = tmp_path / "repos" / slug
    (repo_dir / "tasks").mkdir(parents=True)
    (repo_dir / "tasks" / "PR-305c.md").write_text(
        "---\nstatus: ERROR\n---\n\n# x\n", encoding="utf-8"
    )
    state = RepoState(
        url="not-a-real-url",
        name=slug,
        state=PipelineState.ERROR,
        current_task=QueueTask(pr_id="PR-305c", title="x", status=TaskStatus.ERROR),
        current_pr=PRInfo(number=42, branch="main"),
    )
    redis_client = _GuardrailRedis(
        {
            f"pipeline:{slug}": state.model_dump_json(),
            cause_key(slug, "PR-305c"): _seed_cause({"subsource": "guardrail"}),
        }
    )
    monkeypatch.setattr(web_app, "aioredis", _aioredis_factory(redis_client))

    calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    with TestClient(app) as client:
        resp = client.post(
            f"/repos/{slug}/guardrail/PR-305c/decision",
            data={"decision": "approve"},
        )
    assert resp.status_code == 204
    assert not any(c[:2] == ["gh", "api"] for c in calls)


def test_guardrail_decision_approve_frontmatter_write_failure(
    tmp_path, monkeypatch
) -> None:
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

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)

    def raise_oserror(*args: Any, **kwargs: Any) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(repo_control, "write_frontmatter_status", raise_oserror)
    resp = _post("approve")
    assert resp.status_code == 503
    assert "Failed to update task status" in resp.text
    # Cause was CAS-deleted before side effects, then restored when the
    # frontmatter write failed — the operator must still see the decision
    # handle so they can retry approve or fall back to reject.
    assert cause_key("example__alpha", "PR-305c") in redis_client.store


def test_guardrail_decision_approve_checkout_failure_returns_503(
    tmp_path, monkeypatch
) -> None:
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

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if "fetch" in args or "reset" in args or "checkout" in args:
            raise subprocess.CalledProcessError(
                returncode=128, cmd=args, output="", stderr="branch protected"
            )
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    resp = _post("approve")
    assert resp.status_code == 503
    assert "Failed to commit guardrail decision" in resp.text
    # CAS-delete happened first; the rollback path restores the cause
    # so the operator does not lose the decision handle to a 404 after a
    # transient checkout failure.
    assert cause_key("example__alpha", "PR-305c") in redis_client.store


def test_guardrail_decision_reject_skips_gh_when_url_unparseable(
    tmp_path, monkeypatch
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        "  - url: not-a-real-url\n"
        "    branch: main\n"
        "daemon:\n"
        "  retry_button_cap: 3\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "REPOS_DIR", str(tmp_path / "repos"))
    slug = "not-a-real-url"
    repo_dir = tmp_path / "repos" / slug
    (repo_dir / "tasks").mkdir(parents=True)
    (repo_dir / "tasks" / "PR-305c.md").write_text(
        "---\nstatus: ERROR\n---\n\n# x\n", encoding="utf-8"
    )
    state = RepoState(
        url="not-a-real-url",
        name=slug,
        state=PipelineState.ERROR,
        current_task=QueueTask(pr_id="PR-305c", title="x", status=TaskStatus.ERROR),
        current_pr=PRInfo(number=42, branch="main"),
    )
    redis_client = _GuardrailRedis(
        {
            f"pipeline:{slug}": state.model_dump_json(),
            cause_key(slug, "PR-305c"): _seed_cause({"subsource": "guardrail"}),
        }
    )
    monkeypatch.setattr(web_app, "aioredis", _aioredis_factory(redis_client))

    gh_calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        gh_calls.append(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    with TestClient(app) as client:
        resp = client.post(
            f"/repos/{slug}/guardrail/PR-305c/decision",
            data={"decision": "reject"},
        )
    assert resp.status_code == 204
    # `--repo` cannot be derived, so `gh pr close` must be skipped entirely
    # to avoid acting on whichever repo gh infers from the process context.
    assert not any(c[:3] == ["gh", "pr", "close"] for c in gh_calls)


def test_guardrail_decision_approve_commit_failure_returns_503(
    tmp_path, monkeypatch
) -> None:
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

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if "commit" in args:
            raise subprocess.CalledProcessError(
                returncode=128, cmd=args, output="", stderr="merge conflict"
            )
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    resp = _post("approve")
    assert resp.status_code == 503
    assert "Failed to commit guardrail decision" in resp.text
    # CAS-delete happened first; the rollback path restores the cause
    # so the operator does not lose the decision handle to a 404 after a
    # transient commit failure.
    assert cause_key("example__alpha", "PR-305c") in redis_client.store


def test_guardrail_decision_approve_rollback_failure_still_surfaces_503(
    tmp_path, monkeypatch
) -> None:
    """If the side-effect failure rollback itself fails, log and surface 503."""
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

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if "commit" in args:
            raise subprocess.CalledProcessError(
                returncode=128, cmd=args, output="", stderr="merge conflict"
            )
        return subprocess.CompletedProcess(args, 0, "", "")

    async def boom_record(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("redis flaked during rollback")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    monkeypatch.setattr(repo_control, "record_cancellation_cause", boom_record)
    resp = _post("approve")
    assert resp.status_code == 503
    assert "Failed to commit guardrail decision" in resp.text
    # Rollback failed silently; CAS-deleted cause stays gone but the
    # operator gets a clear error to investigate.
    assert cause_key("example__alpha", "PR-305c") not in redis_client.store


def test_guardrail_decision_approve_watch_reread_missing_returns_404(
    tmp_path, monkeypatch
) -> None:
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

    original_pipeline = redis_client.pipeline

    def make_pipeline(transaction: bool = False) -> _FakePipeline:
        pipe = original_pipeline(transaction=transaction)
        original_watch = pipe.watch

        async def steal(*keys: str) -> None:
            await original_watch(*keys)
            for key in keys:
                redis_client.store.pop(key, None)

        pipe.watch = steal  # type: ignore[assignment]
        return pipe

    monkeypatch.setattr(redis_client, "pipeline", make_pipeline)

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    resp = _post("approve")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_guardrail_approve_transition_handles_failures(monkeypatch) -> None:
    redis_client = _GuardrailRedis(
        {
            "pipeline:example__alpha": _seed_state(),
            cause_key("example__alpha", "PR-305c"): _seed_cause(
                {"subsource": "guardrail"}
            ),
        }
    )

    async def boom_transaction(callback: Any, *keys: str, value_from_callable: bool = False) -> Any:
        raise RuntimeError("boom")

    async def boom_wake(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("wake down")

    monkeypatch.setattr(repo_control.subprocess, "run", lambda args, **kw: subprocess.CompletedProcess(args, 0, "", ""))
    monkeypatch.setattr(redis_client, "transaction", boom_transaction)
    monkeypatch.setattr(web_app, "publish_wake", boom_wake)

    config = type("R", (), {"url": "https://github.com/example/alpha.git", "branch": "main"})
    monkeypatch.setattr(web_app, "REPOS_DIR", "/tmp/_guardrail_test_repos")
    repo_dir = Path("/tmp/_guardrail_test_repos/example__alpha/tasks")
    repo_dir.mkdir(parents=True, exist_ok=True)
    (repo_dir / "PR-305c.md").write_text("---\nstatus: ERROR\n---\n", encoding="utf-8")
    resp = await repo_control._approve_guardrail_decision(
        "example__alpha", "PR-305c", config, redis_client
    )
    assert resp.status_code == 204


def test_guardrail_decision_approve_transition_no_state(tmp_path, monkeypatch) -> None:
    """The post-commit _transition tolerates missing or stale state."""
    repo_dir, redis_client = _setup(
        tmp_path,
        monkeypatch,
        store={
            "pipeline:example__alpha": _seed_state(),
            cause_key("example__alpha", "PR-305c"): _seed_cause(
                {"subsource": "guardrail"}
            ),
        },
    )

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if args[:3] == ["git", "-C", str(repo_dir)] and "push" in args:
            del redis_client.store["pipeline:example__alpha"]
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    resp = _post("approve")
    assert resp.status_code == 204


def test_guardrail_decision_approve_transition_bad_state_json(
    tmp_path, monkeypatch
) -> None:
    repo_dir, redis_client = _setup(
        tmp_path,
        monkeypatch,
        store={
            "pipeline:example__alpha": _seed_state(),
            cause_key("example__alpha", "PR-305c"): _seed_cause(
                {"subsource": "guardrail"}
            ),
        },
    )

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if args[:3] == ["git", "-C", str(repo_dir)] and "push" in args:
            redis_client.store["pipeline:example__alpha"] = "not-json"
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    resp = _post("approve")
    assert resp.status_code == 204


def test_guardrail_decision_approve_transition_other_task_active(
    tmp_path, monkeypatch
) -> None:
    repo_dir, redis_client = _setup(
        tmp_path,
        monkeypatch,
        store={
            "pipeline:example__alpha": _seed_state(),
            cause_key("example__alpha", "PR-305c"): _seed_cause(
                {"subsource": "guardrail"}
            ),
        },
    )

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if args[:3] == ["git", "-C", str(repo_dir)] and "push" in args:
            redis_client.store["pipeline:example__alpha"] = _seed_state(pr_id="PR-999")
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    resp = _post("approve")
    assert resp.status_code == 204


def test_guardrail_decision_reject_bad_state_falls_back_to_gh_list(
    tmp_path, monkeypatch
) -> None:
    _setup(
        tmp_path,
        monkeypatch,
        store={
            "pipeline:example__alpha": "not-json",
            cause_key("example__alpha", "PR-305c"): _seed_cause(
                {"subsource": "guardrail"}
            ),
        },
    )

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if args[:3] == ["gh", "pr", "list"]:
            return subprocess.CompletedProcess(args, 0, json.dumps([{"number": 7}]), "")
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    resp = _post("reject")
    assert resp.status_code == 204


def test_guardrail_decision_reject_record_failure_returns_503(
    tmp_path, monkeypatch
) -> None:
    """RedisError on the CAS-guarded write surfaces as 503."""
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

    original_pipeline = redis_client.pipeline

    def make_pipeline(transaction: bool = False) -> _FakePipeline:
        pipe = original_pipeline(transaction=transaction)

        async def boom_watch(*keys: str) -> None:
            raise _RedisError("conn refused")

        pipe.watch = boom_watch  # type: ignore[assignment]
        return pipe

    monkeypatch.setattr(redis_client, "pipeline", make_pipeline)
    resp = _post("reject")
    assert resp.status_code == 503
    assert "Redis unavailable" in resp.text


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


def test_guardrail_decision_approve_pipeline_redis_error_returns_503(
    tmp_path, monkeypatch
) -> None:
    """RedisError from the WATCH/MULTI pipeline surfaces as 503."""
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

    original_pipeline = redis_client.pipeline

    def make_pipeline(transaction: bool = False) -> _FakePipeline:
        pipe = original_pipeline(transaction=transaction)

        async def boom_watch(*keys: str) -> None:
            raise _RedisError("watch failed")

        pipe.watch = boom_watch  # type: ignore[assignment]
        return pipe

    monkeypatch.setattr(redis_client, "pipeline", make_pipeline)
    resp = _post("approve")
    assert resp.status_code == 503
    assert "Redis unavailable" in resp.text


def test_guardrail_decision_reject_initial_get_redis_error_returns_503(
    tmp_path, monkeypatch
) -> None:
    """Reject's first read must degrade to 503 on Redis outage."""
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
    resp = _post("reject")
    assert resp.status_code == 503
    assert "Redis unavailable" in resp.text


def test_guardrail_decision_reject_state_get_redis_error_returns_503(
    tmp_path, monkeypatch
) -> None:
    """Reject's state read must degrade to 503 on Redis outage."""
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
    resp = _post("reject")
    assert resp.status_code == 503
    assert "Redis unavailable" in resp.text


def test_guardrail_decision_reject_concurrent_change_returns_409(
    tmp_path, monkeypatch
) -> None:
    """A concurrent approve between initial read and CAS write must surface 409.

    Without the CAS guard the unconditional ``operator_reject`` write
    would resurrect a cancellation key that a concurrent approve just
    deleted, putting the task in split-brain: frontmatter pushed to TODO
    by approve but cause flipped to operator_reject by reject. The
    WATCH/MULTI on cause_key forces EXEC to fail when the key has been
    touched, and the reject path returns 409 so the operator retries.
    """
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
    redis_client.pending_watch_error = True
    gh_calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        gh_calls.append(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    resp = _post("reject")
    assert resp.status_code == 409
    assert "Concurrent state change" in resp.text
    # gh pr close must not run when the CAS aborts — the open PR may have
    # been approved by the concurrent decision and closing it would undo
    # that approval.
    assert not any(c[:3] == ["gh", "pr", "close"] for c in gh_calls)
    # Original guardrail cause must remain intact for the operator to
    # re-evaluate; the failed EXEC did not write operator_reject.
    raw = redis_client.store[cause_key("example__alpha", "PR-305c")]
    assert CancellationCause.from_redis(raw).payload["subsource"] == "guardrail"


def test_guardrail_decision_reject_cause_cleared_during_watch_returns_409(
    tmp_path, monkeypatch
) -> None:
    """If the cause is deleted between initial read and watch re-read, 409.

    Models the race where a concurrent approve CAS-deletes the cause
    after this handler's initial GET returned ``guardrail``. The
    re-read inside WATCH returns ``None`` and the reject must short
    circuit without resurrecting the cancellation key.
    """
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

    original_pipeline = redis_client.pipeline

    def make_pipeline(transaction: bool = False) -> _FakePipeline:
        pipe = original_pipeline(transaction=transaction)
        original_watch = pipe.watch

        async def steal(*keys: str) -> None:
            await original_watch(*keys)
            for key in keys:
                redis_client.store.pop(key, None)

        pipe.watch = steal  # type: ignore[assignment]
        return pipe

    monkeypatch.setattr(redis_client, "pipeline", make_pipeline)
    gh_calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        gh_calls.append(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    resp = _post("reject")
    assert resp.status_code == 409
    assert "Concurrent state change" in resp.text
    assert cause_key("example__alpha", "PR-305c") not in redis_client.store
    assert not any(c[:3] == ["gh", "pr", "close"] for c in gh_calls)


def test_guardrail_decision_approve_commit_failure_resets_worktree(
    tmp_path, monkeypatch
) -> None:
    """A commit/push failure after frontmatter write hard-resets the worktree.

    Without the reset, a failed push leaves the checkout with a local
    commit ahead of origin (or with staged frontmatter changes on a
    failed commit), and those uncommitted bytes leak into later daemon
    git operations from the same checkout.
    """
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
    git_calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        git_calls.append(args)
        if "push" in args:
            raise subprocess.CalledProcessError(
                returncode=1, cmd=args, output="", stderr="rejected"
            )
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    resp = _post("approve")
    assert resp.status_code == 503
    assert "Failed to commit guardrail decision" in resp.text
    # The cleanup reset must run after the failed push, with the same
    # base branch the checkout used.
    push_idx = next(i for i, c in enumerate(git_calls) if "push" in c)
    post_push = git_calls[push_idx + 1 :]
    assert any(
        c[3:] == ["reset", "--hard", "origin/main"] for c in post_push
    ), f"expected reset --hard origin/main after push failure, got: {post_push}"
    assert cause_key("example__alpha", "PR-305c") in redis_client.store


def test_guardrail_decision_approve_commit_failure_reset_failure_still_returns_503(
    tmp_path, monkeypatch
) -> None:
    """Even when the cleanup reset itself fails, the handler still returns 503.

    The reset is best-effort: if both the commit/push and the cleanup
    reset fail, the operator must still see 503 so they can investigate.
    """
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

    seen_push = {"flag": False}

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if "push" in args:
            seen_push["flag"] = True
            raise subprocess.CalledProcessError(
                returncode=1, cmd=args, output="", stderr="rejected"
            )
        if seen_push["flag"] and "reset" in args:
            raise subprocess.CalledProcessError(
                returncode=128, cmd=args, output="", stderr="lock held"
            )
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    resp = _post("approve")
    assert resp.status_code == 503
    assert "Failed to commit guardrail decision" in resp.text
    assert cause_key("example__alpha", "PR-305c") in redis_client.store


def test_guardrail_decision_reject_prune_failure_does_not_abort_reject(
    tmp_path, monkeypatch
) -> None:
    """A RedisError from the post-write prune must not block PR close.

    The CAS-guarded write has already flipped the cause to
    ``operator_reject``. Surfacing 503 from a best-effort prune would
    skip ``gh pr close``, and a retry would see no pending guardrail
    decision (cause is no longer ``guardrail``), leaving the rejected
    PR open while the operator's decision is already persisted.
    """
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

    async def boom_zrangebyscore(
        key: str, min_score: Any, max_score: Any
    ) -> list[str]:
        raise _RedisError("conn dropped")

    monkeypatch.setattr(redis_client, "zrangebyscore", boom_zrangebyscore)
    gh_calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        gh_calls.append(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)
    resp = _post("reject")
    assert resp.status_code == 204
    close_calls = [c for c in gh_calls if c[:3] == ["gh", "pr", "close"]]
    assert close_calls, "gh pr close must still run when prune fails"
    raw = redis_client.store[cause_key("example__alpha", "PR-305c")]
    assert CancellationCause.from_redis(raw).payload["subsource"] == "operator_reject"
