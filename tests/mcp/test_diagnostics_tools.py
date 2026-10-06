"""Regression coverage for structured read-only MCP runtime status."""

from __future__ import annotations

import asyncio
import copy
import json
import uuid
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

import pytest
from src.cancellation.storage import cause_key
from src.config import AppConfig, CoderType, RepoConfig
from src.inhibitor import InhibitorType, WorkInhibitor
from src.keyspace import pipeline_state, retry_command, retry_command_pending
from src.metrics import MetricsStore, RunRecord
from src.models import CIStatus, PRInfo, QueueTask, RepoState, ReviewStatus, TaskStatus
from src.retry_commands import new_retry_command

SLUG = "octo__demo"
OTHER_SLUG = "octo__other"
NOW = datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc)
HEAD_SHA = "a" * 40
BASE_SHA = "b" * 40


class FakeRedis:
    """Small Redis test double with only the read operations diagnostics use."""

    def __init__(self) -> None:
        self.store: dict[str, object] = {}
        self.lists: dict[str, list[object]] = {}
        self.zsets: dict[str, list[tuple[object, float]]] = {}
        self.ttls: dict[str, int] = {}
        self.fail: set[tuple[str, object]] = set()
        self.calls: list[tuple[str, object]] = []
        self.closed = False
        self.eval_override: object | None = None

    def _check(self, operation: str, key: object) -> None:
        self.calls.append((operation, key))
        if (operation, key) in self.fail or (operation, "*") in self.fail:
            raise ConnectionError("Authorization: Bearer should-never-be-returned")

    async def strlen(self, key: str) -> int:
        self._check("strlen", key)
        value = self.store.get(key)
        if value is None:
            return 0
        return len(value if isinstance(value, bytes) else str(value).encode())

    async def getrange(self, key: str, start: int, end: int) -> object:
        self._check("getrange", key)
        value = self.store.get(key, b"")
        raw = value if isinstance(value, bytes) else str(value).encode()
        selected = raw[start : end + 1]
        return selected if isinstance(value, bytes) else selected.decode()

    async def exists(self, key: str) -> int:
        self._check("exists", key)
        return int(key in self.store)

    async def ttl(self, key: str) -> int:
        self._check("ttl", key)
        return self.ttls.get(key, -1 if key in self.store else -2)

    async def eval_ro(
        self,
        script: str,
        _numkeys: int,
        key: str,
        *args: object,
    ) -> object:
        self._check("eval_ro", key)
        if self.eval_override is not None:
            return self.eval_override
        if "ZCARD" in script:
            limit, member_limit = (int(item) for item in args)
            values = sorted(self.zsets.get(key, []), key=lambda item: (item[1], str(item[0])))
            rows: list[object] = []
            for member, score in values[:limit]:
                raw = member if isinstance(member, bytes) else str(member).encode()
                rows.extend((len(raw), member if len(raw) <= member_limit else "", score))
            return [len(values), rows]
        scan_limit, member_limit = (int(item) for item in args)
        values = self.lists.get(key, [])
        rows = []
        for member in values[:scan_limit]:
            raw = member if isinstance(member, bytes) else str(member).encode()
            rows.extend((len(raw), member if len(raw) <= member_limit else ""))
        return [len(values), rows]

    async def aclose(self) -> None:
        self.calls.append(("aclose", ""))
        self.closed = True


class GrowingRedis(FakeRedis):
    async def strlen(self, key: str) -> int:
        self._check("strlen", key)
        return 1

    async def getrange(self, key: str, start: int, end: int) -> bytes:
        self._check("getrange", key)
        return b"x" * (end - start + 1)


def _repo(url: str = "https://github.com/octo/demo.git") -> RepoConfig:
    return RepoConfig(url=url, coder=CoderType.CODEX, poll_interval_sec=60)


def _config(*repositories: RepoConfig) -> AppConfig:
    return AppConfig(repositories=list(repositories))


def _patch_runtime(
    monkeypatch: pytest.MonkeyPatch,
    redis: FakeRedis,
    config: AppConfig | None = None,
) -> None:
    from src.mcp.tools import diagnostics

    selected = config or _config(_repo())
    mapping = {
        repo.url.split("github.com/")[-1].removesuffix(".git").replace("/", "__"): repo
        for repo in selected.repositories
    }
    monkeypatch.setattr(diagnostics, "_configured_repositories", lambda: (selected, mapping))
    monkeypatch.setattr(diagnostics, "_new_redis_client", lambda: redis)
    monkeypatch.setattr(diagnostics, "_utc_now", lambda: NOW)


def _state(*, updated: datetime | None = None) -> RepoState:
    task = QueueTask(
        pr_id="PR-9",
        title="secret task title",
        status=TaskStatus.DOING,
        task_file="tasks/secret.md",
        branch="secret-branch",
    )
    return RepoState(
        url="https://github.com/octo/demo.git",
        name=SLUG,
        state="WATCH",
        current_task=task,
        current_pr=PRInfo(
            number=565,
            branch="secret-branch",
            title="secret PR title",
            head_sha=HEAD_SHA,
            ci_status=CIStatus.FAILURE,
            review_status=ReviewStatus.CHANGES_REQUESTED,
        ),
        error_message="Authorization: Bearer state-secret",
        last_updated=updated or NOW - timedelta(minutes=20),
        coder="codex",
        history=[{"time": NOW.isoformat(), "state": "WATCH", "event": "secret event"}],
        active_inhibitors=[
            WorkInhibitor(
                inhibitor_type=InhibitorType.USER_PAUSE,
                coder_affected="codex",
                expires_at=NOW + timedelta(minutes=5),
                reason_text="secret inhibitor reason",
                source_key="secret:redis:key",
            )
        ],
    )


def _command(*, command_id: str | None = None, task_id: str = "PR-9"):
    command = new_retry_command(
        repo_slug=SLUG,
        task_id=task_id,
        task_file="tasks/secret.md",
        task_branch="secret-branch",
        task_fingerprint="secret-fingerprint",
        request_binding="secret-binding",
        failure_id="secret-failure",
        retry_cap=3,
        failure_subsource="guardrail",
        bound_pr_number=565,
        bound_pr_head_sha=HEAD_SHA,
        now=NOW - timedelta(minutes=2),
    )
    if command_id is not None:
        command.command_id = command_id
    command.outcome_reason = "secret Retry explanation"
    command.history[0].reason = "secret Retry history"
    return command


def _run(*, run_id: str | None = None, task_id: str = "PR-9", ended: bool = False) -> RunRecord:
    return RunRecord(
        run_id=run_id or str(uuid.uuid4()),
        task_id=task_id,
        profile_id="secret-profile",
        task_type="secret-type",
        complexity="secret-complexity",
        started_at=(NOW - timedelta(minutes=10)).isoformat(),
        ended_at=(NOW - timedelta(minutes=1)).isoformat() if ended else None,
        duration_ms=540_000 if ended else None,
        fix_iterations=2,
        tokens_in=123,
        tokens_out=456,
        exit_reason="success_merged" if ended else "",
        operator_intervention=False,
        outcome="merged" if ended else "",
        cause_subsource=None,
        repo_name=SLUG,
        base_sha=BASE_SHA,
        head_sha=HEAD_SHA,
        task_spec_hash="secret-task-hash",
    )


async def test_status_returns_only_allowlisted_structured_metadata_and_is_read_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis)
    state = _state()
    redis.store[pipeline_state(SLUG)] = state.model_dump_json()

    command = _command()
    retry_key = retry_command(SLUG, command.command_id)
    redis.zsets[retry_command_pending(SLUG)] = [(command.command_id, command.requested_at.timestamp())]
    redis.store[retry_key] = command.model_dump_json()
    redis.ttls[retry_key] = 120

    cancellation_key = cause_key(SLUG, "PR-9")
    redis.store[cancellation_key] = json.dumps(
        {
            "category": "ERROR",
            "payload": {
                "subsource": "guardrail",
                "reason_text": "secret cancellation explanation",
                "authorization": "secret-cancellation-token",
            },
            "created_at": (NOW - timedelta(minutes=3)).isoformat(),
            "task_id": "PR-9",
            "repo_slug": SLUG,
        }
    )
    redis.ttls[cancellation_key] = 360

    run = _run()
    redis.lists[MetricsStore._recent_key("PR-9", SLUG)] = [run.run_id]
    redis.store[MetricsStore._record_key(run.run_id)] = json.dumps(asdict(run))
    before = copy.deepcopy((redis.store, redis.lists, redis.zsets, redis.ttls))

    result = await diagnostics.get_orchestrator_status(SLUG)

    assert result["schema_version"] == 1
    assert result["redis"] == {"status": "available", "code": None}
    overview = result["repositories"][0]
    assert overview["snapshot"]["status"] == "stale"
    assert overview["snapshot"]["freshness_is_coder_activity"] is False
    assert overview["pipeline"] == {
        "state": "WATCH",
        "active": True,
        "paused": False,
        "coder": "codex",
        "current_task": {"id": "PR-9", "status": "DOING"},
        "current_pr": {
            "number": 565,
            "head_sha": HEAD_SHA,
            "ci_status": "FAILURE",
            "review_status": "CHANGES_REQUESTED",
        },
        "integrity_codes": [],
        "coder_activity": "unknown",
    }
    detail = result["detail"]
    assert detail["inhibitors"]["records"] == [
        {
            "type": "user_pause",
            "coder_scope": "codex",
            "expires_at": "2026-10-05T12:05:00Z",
            "expired_at_observation": False,
        }
    ]
    assert detail["current_cancellation"]["classification"] == {
        "category": "ERROR",
        "subsource": "guardrail",
        "task_id": "PR-9",
        "created_at": "2026-10-05T11:57:00Z",
    }
    retry = detail["pending_retries"]["records"][0]["metadata"]
    assert retry["command_id"] == command.command_id
    assert retry["failure_subsource"] == "guardrail"
    run_result = detail["run_records"]["records"][0]["metadata"]
    assert run_result["outcome"] == "in_progress"
    assert run_result["head_sha"] == HEAD_SHA
    serialized = json.dumps(result)
    for secret in (
        "state-secret",
        "secret task title",
        "secret PR title",
        "secret event",
        "secret inhibitor reason",
        "secret:redis:key",
        "secret Retry explanation",
        "secret Retry history",
        "secret-fingerprint",
        "secret-binding",
        "secret-failure",
        "secret cancellation explanation",
        "secret-cancellation-token",
        "secret-profile",
        "secret-task-hash",
    ):
        assert secret not in serialized
    assert before == (redis.store, redis.lists, redis.zsets, redis.ttls)
    assert not {"set", "expire", "delete", "zrem", "ltrim"}.intersection(operation for operation, _key in redis.calls)
    assert redis.closed is True


async def test_status_overview_reports_missing_malformed_oversized_and_partial_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    config = _config(_repo(), _repo("https://github.com/octo/other.git"))
    _patch_runtime(monkeypatch, redis, config)
    redis.store[pipeline_state(OTHER_SLUG)] = "{malformed"

    result = await diagnostics.get_orchestrator_status()
    assert result["detail"] is None
    assert [item["snapshot"]["status"] for item in result["repositories"]] == [
        "missing",
        "malformed",
    ]

    redis.store[pipeline_state(SLUG)] = "x" * (diagnostics._MAX_STATE_BYTES + 1)
    redis.fail.add(("strlen", pipeline_state(OTHER_SLUG)))
    result = await diagnostics.get_orchestrator_status(SLUG)
    assert result["repositories"][0]["snapshot"]["status"] == "oversized"
    assert result["repositories"][1]["snapshot"]["status"] == "unavailable"
    assert result["redis"]["status"] == "partially_available"
    assert result["detail"]["pending_retries"]["status"] == "available"


async def test_sparse_snapshot_cannot_default_to_fresh_idle_and_other_reads_continue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis)
    redis.store[pipeline_state(SLUG)] = json.dumps(
        {
            "name": SLUG,
            "url": "https://github.com/octo/demo.git",
        }
    )
    command = _command()
    redis.zsets[retry_command_pending(SLUG)] = [(command.command_id, command.requested_at.timestamp())]
    redis.store[retry_command(SLUG, command.command_id)] = command.model_dump_json()
    run = _run()
    redis.lists[MetricsStore._recent_key("PR", SLUG)] = [run.run_id]
    redis.store[MetricsStore._record_key(run.run_id)] = json.dumps(asdict(run))

    result = await diagnostics.get_orchestrator_status(SLUG)

    overview = result["repositories"][0]
    assert overview["snapshot"]["status"] == "malformed"
    assert overview["snapshot"]["code"] == "snapshot_invalid"
    assert overview["snapshot"]["source_timestamp"] is None
    assert overview["snapshot"]["age_seconds"] is None
    assert overview["pipeline"] is None
    assert result["detail"]["pipeline"] is None
    assert result["detail"]["pending_retries"]["records"][0]["status"] == "available"
    assert result["detail"]["run_records"]["records"][0]["status"] == "available"
    assert result["detail"]["run_records"]["task_filter"] is None


async def test_zero_padded_task_id_preserves_associated_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis)
    task_id = "PR-001"
    state = _state(updated=NOW)
    state.current_task.pr_id = task_id
    redis.store[pipeline_state(SLUG)] = state.model_dump_json()

    command = _command(task_id=task_id)
    redis.zsets[retry_command_pending(SLUG)] = [(command.command_id, command.requested_at.timestamp())]
    redis.store[retry_command(SLUG, command.command_id)] = command.model_dump_json()
    run = _run(task_id=task_id)
    redis.lists[MetricsStore._recent_key(task_id, SLUG)] = [run.run_id]
    redis.store[MetricsStore._record_key(run.run_id)] = json.dumps(asdict(run))
    redis.store[cause_key(SLUG, task_id)] = json.dumps(
        {
            "category": "ERROR",
            "payload": {"subsource": "guardrail"},
            "created_at": NOW.isoformat(),
            "task_id": task_id,
            "repo_slug": SLUG,
        }
    )

    result = await diagnostics.get_orchestrator_status(SLUG)

    detail = result["detail"]
    assert detail["pipeline"]["current_task"] == {"id": task_id, "status": "DOING"}
    assert detail["current_cancellation"]["classification"]["task_id"] == task_id
    assert detail["pending_retries"]["records"][0]["metadata"]["task_id"] == task_id
    assert detail["run_records"]["task_filter"] == task_id
    assert detail["run_records"]["records"][0]["metadata"]["task_id"] == task_id


async def test_status_connection_config_validation_and_cancellation_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    def bad_config():
        raise ValueError("password=configuration-secret")

    monkeypatch.setattr(diagnostics, "_configured_repositories", bad_config)
    monkeypatch.setattr(diagnostics, "_utc_now", lambda: NOW)
    result = await diagnostics.get_orchestrator_status()
    assert result["configuration"] == {"status": "unavailable", "code": "configuration_invalid"}
    assert "configuration-secret" not in json.dumps(result)

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis)
    with pytest.raises(ValueError, match="canonical"):
        await diagnostics.get_orchestrator_status("../secret")
    with pytest.raises(ValueError, match="not configured"):
        await diagnostics.get_orchestrator_status(OTHER_SLUG)
    with pytest.raises(ValueError, match="retry_limit"):
        await diagnostics.get_orchestrator_status(retry_limit=0)
    with pytest.raises(ValueError, match="run_limit"):
        await diagnostics.get_orchestrator_status(run_limit=True)

    def unavailable_client():
        raise ConnectionError("Authorization: Bearer connection-secret")

    monkeypatch.setattr(diagnostics, "_new_redis_client", unavailable_client)
    unavailable = await diagnostics.get_orchestrator_status(SLUG)
    assert unavailable["redis"] == {"status": "unavailable", "code": "redis_connection_failed"}
    assert unavailable["detail"]["current_cancellation"]["status"] == "unavailable"
    assert "connection-secret" not in json.dumps(unavailable)

    started = asyncio.Event()

    class BlockingRedis(FakeRedis):
        async def strlen(self, key: str) -> int:
            started.set()
            await asyncio.Event().wait()
            return 0

    blocking = BlockingRedis()
    _patch_runtime(monkeypatch, blocking)
    request = asyncio.create_task(diagnostics.get_orchestrator_status(SLUG))
    await started.wait()
    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request
    assert blocking.closed is True


def test_scalar_validation_and_configured_repo_guards(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.mcp.tools import diagnostics

    assert diagnostics._utc_now().tzinfo is timezone.utc
    sentinel = object()
    redis_options = {}

    def capture_redis_options(url: str, **options: object) -> object:
        redis_options["url"] = url
        redis_options.update(options)
        return sentinel

    monkeypatch.setenv("REDIS_URL", "redis://example.invalid/1")
    monkeypatch.setattr(diagnostics.aioredis, "from_url", capture_redis_options)
    assert diagnostics._new_redis_client() is sentinel
    assert redis_options == {
        "url": "redis://example.invalid/1",
        "decode_responses": False,
        "socket_connect_timeout": diagnostics._REDIS_TIMEOUT_SECONDS,
        "socket_timeout": diagnostics._REDIS_TIMEOUT_SECONDS,
    }
    assert diagnostics._timestamp(NOW) == NOW
    assert diagnostics._timestamp(datetime(2026, 1, 1)) == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert diagnostics._timestamp("2026-01-01T00:00:00Z") is not None
    assert diagnostics._timestamp("bad") is None
    assert diagnostics._timestamp("0001-01-01T00:00:00+23:59") is None
    assert diagnostics._timestamp(123) is None
    assert diagnostics._timestamp_text("bad") is None
    assert diagnostics._positive_int(0) == 0
    assert diagnostics._positive_int(True) is None
    assert diagnostics._positive_int(-1) is None
    assert diagnostics._positive_number(0) is None
    assert diagnostics._task_id("PR-1") == "PR-1"
    assert diagnostics._task_id("PR-001") == "PR-001"
    assert diagnostics._task_id("secret") is None
    assert diagnostics._sha(HEAD_SHA.upper()) == HEAD_SHA
    assert diagnostics._sha("abc") is None
    assert diagnostics._uuid(12) is None
    assert diagnostics._uuid("bad") is None
    identifier = str(uuid.uuid4())
    assert diagnostics._uuid(identifier.upper()) == identifier
    assert diagnostics._coder("codex") == "codex"
    assert diagnostics._coder("secret") is None
    assert diagnostics._subsource("guardrail") == "guardrail"
    assert diagnostics._subsource("secret") is None
    assert diagnostics._decode(b"ok") == "ok"
    assert diagnostics._decode("ok") == "ok"
    with pytest.raises(TypeError, match="not text"):
        diagnostics._decode(3)
    assert diagnostics._source_summary(["available", "missing"]) == "available"
    assert diagnostics._source_summary(["unavailable"]) == "unavailable"
    assert diagnostics._source_summary(["available", "unavailable"]) == "partially_available"

    monkeypatch.setattr(diagnostics, "load_config", lambda: _config(_repo()))
    config, repositories = diagnostics._configured_repositories()
    assert config.repositories and list(repositories) == [SLUG]
    monkeypatch.setattr(
        diagnostics,
        "load_config",
        lambda: _config(_repo(), _repo()),
    )
    with pytest.raises(ValueError, match="identity"):
        diagnostics._configured_repositories()
    monkeypatch.setattr(
        diagnostics,
        "load_config",
        lambda: _config(_repo("not-a-repository")),
    )
    with pytest.raises(ValueError, match="identity"):
        diagnostics._configured_repositories()


async def test_bounded_string_and_snapshot_contracts() -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    redis.store["oversized"] = "abcd"
    assert await diagnostics._read_bounded_string(redis, "oversized", 3) == (None, 4, True)
    redis.store["empty"] = ""
    assert await diagnostics._read_bounded_string(redis, "empty", 3) == ("", 0, False)
    assert await diagnostics._read_bounded_string(redis, "missing", 3) == (None, None, False)
    assert await diagnostics._read_bounded_string(GrowingRedis(), "growing", 3) == (None, 4, True)

    config = _config(_repo())
    repo = config.repositories[0]
    oversized, state = diagnostics._snapshot_result(
        None,
        size_bytes=diagnostics._MAX_STATE_BYTES + 1,
        oversized=True,
        slug=SLUG,
        repo=repo,
        config=config,
        observed_at=NOW,
    )
    assert oversized["status"] == "oversized" and state is None
    missing, state = diagnostics._snapshot_result(
        None,
        size_bytes=None,
        oversized=False,
        slug=SLUG,
        repo=repo,
        config=config,
        observed_at=NOW,
    )
    assert missing["status"] == "missing" and state is None
    malformed, state = diagnostics._snapshot_result(
        123,
        size_bytes=3,
        oversized=False,
        slug=SLUG,
        repo=repo,
        config=config,
        observed_at=NOW,
    )
    assert malformed["status"] == "malformed" and state is None
    non_object, state = diagnostics._snapshot_result(
        "[]",
        size_bytes=2,
        oversized=False,
        slug=SLUG,
        repo=repo,
        config=config,
        observed_at=NOW,
    )
    assert non_object["status"] == "malformed" and state is None
    mismatch_state = _state().model_copy(update={"name": OTHER_SLUG})
    mismatch, state = diagnostics._snapshot_result(
        mismatch_state.model_dump_json(),
        size_bytes=10,
        oversized=False,
        slug=SLUG,
        repo=repo,
        config=config,
        observed_at=NOW,
    )
    assert mismatch["code"] == "snapshot_repository_mismatch" and state is None
    future = _state(updated=NOW + timedelta(minutes=1))
    future_result, state = diagnostics._snapshot_result(
        future.model_dump_json(),
        size_bytes=10,
        oversized=False,
        slug=SLUG,
        repo=repo,
        config=config,
        observed_at=NOW,
    )
    assert future_result["status"] == "clock_skew" and state is not None
    fresh = _state(updated=NOW - timedelta(seconds=1))
    fresh_result, _state_result = diagnostics._snapshot_result(
        fresh.model_dump_json(),
        size_bytes=10,
        oversized=False,
        slug=SLUG,
        repo=repo,
        config=config,
        observed_at=NOW,
    )
    assert fresh_result["status"] == "fresh"
    naive = _state(updated=datetime(2026, 10, 5, 11, 59, 59))
    naive_result, _state_result = diagnostics._snapshot_result(
        naive.model_dump_json(),
        size_bytes=10,
        oversized=False,
        slug=SLUG,
        repo=repo,
        config=config,
        observed_at=NOW,
    )
    assert naive_result["status"] == "fresh"


@pytest.mark.parametrize(
    ("timestamp_present", "timestamp_value"),
    [
        (False, None),
        (True, "not-a-timestamp"),
        (True, 1_759_664_400),
    ],
)
def test_snapshot_requires_a_valid_producer_timestamp(
    timestamp_present: bool,
    timestamp_value: object,
) -> None:
    from src.mcp.tools import diagnostics

    payload = json.loads(_state(updated=NOW).model_dump_json())
    if timestamp_present:
        payload["last_updated"] = timestamp_value
    else:
        payload.pop("last_updated")

    result, state = diagnostics._snapshot_result(
        json.dumps(payload),
        size_bytes=100,
        oversized=False,
        slug=SLUG,
        repo=_repo(),
        config=_config(_repo()),
        observed_at=NOW,
    )

    assert result["status"] == "malformed"
    assert result["code"] == "snapshot_invalid"
    assert result["source_timestamp"] is None
    assert result["age_seconds"] is None
    assert state is None


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("state", None),
        ("active", None),
        ("user_paused", None),
        ("state", 1),
        ("state", "NOT_A_STATE"),
        ("active", "true"),
        ("user_paused", 0),
    ],
)
def test_snapshot_requires_producer_state_and_control_flags(
    field: str,
    replacement: object,
) -> None:
    from src.mcp.tools import diagnostics

    payload = json.loads(_state(updated=NOW).model_dump_json())
    if replacement is None:
        payload.pop(field)
    else:
        payload[field] = replacement

    result, state = diagnostics._snapshot_result(
        json.dumps(payload),
        size_bytes=100,
        oversized=False,
        slug=SLUG,
        repo=_repo(),
        config=_config(_repo()),
        observed_at=NOW,
    )

    assert result["status"] == "malformed"
    assert result["code"] == "snapshot_invalid"
    assert state is None


def test_pipeline_and_inhibitor_allowlists() -> None:
    from src.mcp.tools import diagnostics

    state = _state()
    state.coder = "secret-coder"
    state.current_task.pr_id = "secret-task"
    state.current_pr.number = 0
    state.current_pr.head_sha = "short"
    view = diagnostics._pipeline_view(state)
    assert view is not None
    assert view["coder"] is None
    assert view["current_task"] is None
    assert view["current_pr"] is None
    assert view["integrity_codes"] == [
        "invalid_coder",
        "invalid_current_task_id",
        "invalid_current_pr_number",
        "invalid_current_pr_sha",
    ]
    assert diagnostics._pipeline_view(None) is None

    state.active_inhibitors = [
        WorkInhibitor(
            inhibitor_type=InhibitorType.RATE_LIMIT,
            coder_affected="secret-coder",
            expires_at=NOW - timedelta(seconds=1),
            reason_text="secret",
            source_key="secret",
        )
        for _ in range(diagnostics._MAX_INHIBITORS + 1)
    ]
    result = diagnostics._inhibitors(state, NOW)
    assert result["truncated"] is True
    assert result["records"][0]["coder_scope"] == "unknown"
    assert result["records"][0]["expired_at_observation"] is True
    state.active_inhibitors[0] = WorkInhibitor(
        inhibitor_type=InhibitorType.USER_PAUSE,
        reason_text="secret",
        source_key="secret",
    )
    assert diagnostics._inhibitors(state, NOW)["records"][0]["coder_scope"] == "all"
    assert diagnostics._inhibitors(None, NOW)["status"] == "unavailable"
    assert diagnostics._ttl_fields(10)["expiry_status"] == "expires"
    assert diagnostics._ttl_fields(-1)["expiry_status"] == "persistent"
    assert diagnostics._ttl_fields(-2)["expiry_status"] == "missing"


async def test_cancellation_source_statuses() -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    assert (await diagnostics._current_cancellation(redis, SLUG, None))["status"] == "not_applicable"
    key = cause_key(SLUG, "PR-9")
    redis.fail.add(("strlen", key))
    assert (await diagnostics._current_cancellation(redis, SLUG, "PR-9"))["status"] == "unavailable"
    redis.fail.clear()
    redis.store[key] = "x" * (diagnostics._MAX_CANCELLATION_BYTES + 1)
    assert (await diagnostics._current_cancellation(redis, SLUG, "PR-9"))["status"] == "oversized"
    redis.store.pop(key)
    assert (await diagnostics._current_cancellation(redis, SLUG, "PR-9"))["status"] == "missing"

    malformed_values = [
        "[]",
        json.dumps(
            {
                "category": "ERROR",
                "payload": {},
                "created_at": "bad",
                "task_id": "PR-9",
                "repo_slug": SLUG,
            }
        ),
    ]
    for value in malformed_values:
        redis.store[key] = value
        result = await diagnostics._current_cancellation(redis, SLUG, "PR-9")
        assert result["status"] == "malformed"

    redis.store[key] = json.dumps(
        {
            "category": "future-category-secret",
            "payload": "secret-invalid-payload",
            "created_at": NOW.isoformat(),
            "task_id": "PR-9",
            "repo_slug": SLUG,
        }
    )
    result = await diagnostics._current_cancellation(redis, SLUG, "PR-9")
    assert result["classification"]["category"] == "unclassified"
    assert result["classification"]["subsource"] == "unclassified"
    assert "future-category-secret" not in json.dumps(result)
    assert "secret-invalid-payload" not in json.dumps(result)


async def test_retry_source_bounds_and_malformed_records() -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    index = retry_command_pending(SLUG)
    redis.fail.add(("eval_ro", index))
    assert (await diagnostics._pending_retries(redis, SLUG, 5))["status"] == "unavailable"
    redis.fail.clear()
    for override in ([1], [-1, []], [1, [1, "x"]]):
        redis.eval_override = override
        assert (await diagnostics._pending_retries(redis, SLUG, 5))["status"] == "unavailable"
    redis.eval_override = [2, [-1, "x", 1.0, 1, "x", float("inf")]]
    invalid = await diagnostics._pending_retries(redis, SLUG, 5)
    assert invalid["invalid_records_omitted"] == 2
    redis.eval_override = None

    available = _command()
    missing_id = str(uuid.uuid4())
    malformed_id = str(uuid.uuid4())
    mismatch_id = str(uuid.uuid4())
    oversized_id = str(uuid.uuid4())
    failed_id = str(uuid.uuid4())
    redis.zsets[index] = [
        ("x" * (diagnostics._MAX_INDEX_MEMBER_BYTES + 1), 0),
        (b"\xff", 1),
        ("not-a-uuid", 2),
        (failed_id, 3),
        (oversized_id, 4),
        (missing_id, 5),
        (malformed_id, 6),
        (mismatch_id, 7),
        (available.command_id, 8),
    ]
    redis.fail.add(("strlen", retry_command(SLUG, failed_id)))
    redis.store[retry_command(SLUG, oversized_id)] = "x" * (diagnostics._MAX_RETRY_BYTES + 1)
    redis.store[retry_command(SLUG, malformed_id)] = "{bad"
    mismatch = _command(command_id=str(uuid.uuid4()))
    redis.store[retry_command(SLUG, mismatch_id)] = mismatch.model_dump_json()
    redis.store[retry_command(SLUG, available.command_id)] = available.model_dump_json()
    result = await diagnostics._pending_retries(redis, SLUG, 20)
    assert result["status"] == "available"
    assert result["invalid_records_omitted"] == 2
    assert [record["status"] for record in result["records"]] == [
        "oversized_index_member",
        "unavailable",
        "oversized",
        "missing",
        "malformed",
        "malformed",
        "available",
    ]

    command = _command()
    command.failure_subsource = "future-secret"
    command.bound_pr_number = 0
    command.bound_pr_head_sha = "short"
    command.retry_count = -1
    metadata = diagnostics._retry_metadata(command, -2)
    assert metadata is not None
    assert metadata["failure_subsource"] == "unclassified"
    assert metadata["bound_pr_number"] is None
    assert metadata["bound_pr_head_sha"] is None
    assert metadata["retry_count"] is None
    command.failure_subsource = None
    assert diagnostics._retry_metadata(command, -1)["failure_subsource"] is None
    command.command_id = "bad"
    assert diagnostics._retry_metadata(command, 1) is None
    command.command_id = str(uuid.uuid4())
    command.task_id = "secret"
    assert diagnostics._retry_metadata(command, 1) is None


def test_run_metadata_allowlist_and_validation() -> None:
    from src.mcp.tools import diagnostics

    identifier = str(uuid.uuid4())
    assert diagnostics._run_metadata("bad", identifier, SLUG) is None
    assert diagnostics._run_metadata("[]", identifier, SLUG) is None
    run = _run(run_id=identifier, ended=True)
    metadata = diagnostics._run_metadata(json.dumps(asdict(run)), identifier, SLUG)
    assert metadata is not None
    assert metadata["outcome"] == "merged"
    assert metadata["duration_ms"] == 540_000
    assert diagnostics._run_metadata(json.dumps(asdict(run)), str(uuid.uuid4()), SLUG) is None
    run.task_id = "secret"
    assert diagnostics._run_metadata(json.dumps(asdict(run)), identifier, SLUG) is None
    run.task_id = "PR-9"
    run.repo_name = OTHER_SLUG
    assert diagnostics._run_metadata(json.dumps(asdict(run)), identifier, SLUG) is None
    run.repo_name = SLUG
    run.started_at = "bad"
    assert diagnostics._run_metadata(json.dumps(asdict(run)), identifier, SLUG) is None
    run.started_at = "0001-01-01T00:00:00+23:59"
    assert diagnostics._run_metadata(json.dumps(asdict(run)), identifier, SLUG) is None
    run.started_at = NOW.isoformat()
    run.ended_at = "bad"
    assert diagnostics._run_metadata(json.dumps(asdict(run)), identifier, SLUG) is None

    run.ended_at = NOW.isoformat()
    run.cause_subsource = "future-secret"
    run.outcome = "failed"
    run.cause = "ESCALATE"
    metadata = diagnostics._run_metadata(json.dumps(asdict(run)), identifier, SLUG)
    assert metadata is not None
    assert metadata["cause_subsource"] == "unclassified"
    run.cause_subsource = None
    assert diagnostics._run_metadata(json.dumps(asdict(run)), identifier, SLUG)["cause_subsource"] is None


@pytest.mark.parametrize("attempt_index", [0, True, 1.5, float("nan"), float("inf")])
def test_run_metadata_rejects_non_positive_integer_attempt_indices(
    attempt_index: object,
) -> None:
    from src.mcp.tools import diagnostics

    run = _run(ended=True)
    payload = asdict(run)
    payload["attempt_index"] = attempt_index

    assert diagnostics._run_metadata(json.dumps(payload), run.run_id, SLUG) is None


async def test_run_source_bounds_filtering_and_malformed_records() -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    index = MetricsStore._recent_key("PR-9", SLUG)
    redis.fail.add(("eval_ro", index))
    assert (await diagnostics._run_records(redis, SLUG, "PR-9", 5))["status"] == "unavailable"
    redis.fail.clear()
    for override in ([1], [-1, []], [1, [1]]):
        redis.eval_override = override
        assert (await diagnostics._run_records(redis, SLUG, "PR-9", 5))["status"] == "unavailable"
    redis.eval_override = [1, [-1, "x"]]
    invalid = await diagnostics._run_records(redis, SLUG, "PR-9", 5)
    assert invalid["invalid_records_omitted"] == 1
    redis.eval_override = None

    failed_id = str(uuid.uuid4())
    oversized_id = str(uuid.uuid4())
    missing_id = str(uuid.uuid4())
    malformed_id = str(uuid.uuid4())
    other_task = _run(task_id="PR-10")
    available = _run()
    redis.lists[index] = [
        "x" * (diagnostics._MAX_INDEX_MEMBER_BYTES + 1),
        b"\xff",
        "not-a-uuid",
        failed_id,
        oversized_id,
        missing_id,
        malformed_id,
        other_task.run_id,
        available.run_id,
    ]
    redis.fail.add(("strlen", MetricsStore._record_key(failed_id)))
    redis.store[MetricsStore._record_key(oversized_id)] = "x" * (diagnostics._MAX_RUN_BYTES + 1)
    redis.store[MetricsStore._record_key(malformed_id)] = "[]"
    redis.store[MetricsStore._record_key(other_task.run_id)] = json.dumps(asdict(other_task))
    redis.store[MetricsStore._record_key(available.run_id)] = json.dumps(asdict(available))
    result = await diagnostics._run_records(redis, SLUG, "PR-9", 20)
    assert result["invalid_records_omitted"] == 2
    assert [record["status"] for record in result["records"]] == [
        "oversized_index_member",
        "unavailable",
        "oversized",
        "missing",
        "malformed",
        "available",
    ]
    assert result["scanned_index_entries"] == 9

    limited = await diagnostics._run_records(redis, SLUG, "PR-9", 1)
    assert limited["truncated"] is True
