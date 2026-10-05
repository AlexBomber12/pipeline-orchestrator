"""Regression coverage for the read-only Orchestrator MCP diagnostics."""

from __future__ import annotations

import copy
import fnmatch
import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from src.config import AppConfig, CoderType, RepoConfig
from src.inhibitor import InhibitorType, WorkInhibitor
from src.keyspace import (
    cli_log_history,
    cli_log_latest,
    pipeline_state,
    repo_events_history,
    retry_command,
    retry_command_pending,
)
from src.metrics import MetricsStore, RunRecord
from src.models import CIStatus, PRInfo, QueueTask, RepoState, ReviewStatus, TaskStatus
from src.retry_commands import new_retry_command

SLUG = "octo__demo"
NOW = datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc)


class FakeRedis:
    """Small read-only Redis surface that records every operation."""

    def __init__(self) -> None:
        self.store: dict[str, object] = {}
        self.lists: dict[str, list[object]] = {}
        self.zsets: dict[str, list[tuple[object, float]]] = {}
        self.ttls: dict[str, int] = {}
        self.fail: set[str] = set()
        self.calls: list[tuple[str, object]] = []
        self.closed = False

    def _check(self, name: str, key: object = "") -> None:
        self.calls.append((name, key))
        if name in self.fail:
            raise ConnectionError(f"{name} failed with Authorization: Bearer redis-secret")

    async def mget(self, keys: list[str]) -> list[object | None]:
        self._check("mget", tuple(keys))
        return [self.store.get(key) for key in keys]

    async def get(self, key: str) -> object | None:
        self._check("get", key)
        return self.store.get(key)

    async def ttl(self, key: str) -> int:
        self._check("ttl", key)
        if key not in self.store:
            return -2
        return self.ttls.get(key, -1)

    async def lrange(self, key: str, start: int, stop: int) -> list[object]:
        self._check("lrange", key)
        values = self.lists.get(key, [])
        if stop == -1:
            return list(values[start:])
        return list(values[start : stop + 1])

    async def llen(self, key: str) -> int:
        self._check("llen", key)
        return len(self.lists.get(key, []))

    async def zcard(self, key: str) -> int:
        self._check("zcard", key)
        return len(self.zsets.get(key, []))

    async def zrange(self, key: str, start: int, stop: int, *, withscores: bool = False) -> list[Any]:
        self._check("zrange", key)
        values = self.zsets.get(key, [])
        selected = values[start:] if stop == -1 else values[start : stop + 1]
        return list(selected if withscores else [item[0] for item in selected])

    async def scan_iter(self, match: str):
        self._check("scan_iter", match)
        for key in sorted(self.store):
            if fnmatch.fnmatch(key, match):
                yield key

    async def aclose(self) -> None:
        self.calls.append(("aclose", ""))
        self.closed = True


def _config(*repos: RepoConfig) -> AppConfig:
    return AppConfig(repositories=list(repos))


def _repo(url: str = "https://github.com/octo/demo.git") -> RepoConfig:
    return RepoConfig(url=url, coder=CoderType.CODEX, poll_interval_sec=60)


def _patch_runtime(monkeypatch: pytest.MonkeyPatch, redis: FakeRedis, config: AppConfig) -> None:
    from src.mcp.tools import diagnostics

    mapping = {
        repo.url.split("github.com/")[-1].removesuffix(".git").replace("/", "__"): repo for repo in config.repositories
    }
    monkeypatch.setattr(diagnostics, "_configured_repositories", lambda: (config, mapping))
    monkeypatch.setattr(diagnostics, "_new_redis_client", lambda: redis)
    monkeypatch.setattr(diagnostics, "_utc_now", lambda: NOW)


def _run(run_id: str, *, ended: bool = False) -> RunRecord:
    return RunRecord(
        run_id=run_id,
        task_id="PR-9",
        profile_id="codex:gpt-6:container",
        task_type="feature",
        complexity="medium",
        started_at=(NOW - timedelta(minutes=10)).isoformat(),
        ended_at=(NOW - timedelta(minutes=2)).isoformat() if ended else None,
        duration_ms=480_000 if ended else None,
        fix_iterations=1,
        tokens_in=0,
        tokens_out=0,
        exit_reason="success_merged" if ended else "",
        operator_intervention=False,
        repo_name=SLUG,
        head_sha="abc123",
    )


async def test_status_detail_is_truthful_redacted_and_read_only(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.mcp.tools.diagnostics import get_orchestrator_status

    redis = FakeRedis()
    config = _config(_repo())
    _patch_runtime(monkeypatch, redis, config)
    task = QueueTask(
        pr_id="PR-9",
        title="Investigate",
        status=TaskStatus.DOING,
        task_file="tasks/PR-9.md",
        branch="pr-9",
    )
    state = RepoState(
        url=config.repositories[0].url,
        name=SLUG,
        state="WATCH",
        current_task=task,
        current_queue=[task],
        current_queue_snapshot_at=NOW - timedelta(minutes=3),
        current_pr=PRInfo(
            number=9,
            branch="pr-9",
            head_sha="abc123",
            ci_status=CIStatus.FAILURE,
            review_status=ReviewStatus.CHANGES_REQUESTED,
        ),
        error_message="Authorization: Bearer state-secret",
        last_updated=NOW - timedelta(minutes=20),
        coder="codex",
        history=[
            {
                "time": (NOW - timedelta(minutes=4)).isoformat(),
                "state": "WATCH",
                "event": "CI failed password=hunter2",
            }
        ],
        active_inhibitors=[
            WorkInhibitor(
                inhibitor_type=InhibitorType.USER_PAUSE,
                reason_text="Operator paused",
                source_key="state:octo__demo.user_paused",
            )
        ],
    )
    redis.store[pipeline_state(SLUG)] = state.model_dump_json()
    redis.lists[repo_events_history(SLUG)] = [
        json.dumps(
            {
                "type": "event_log_append",
                "repo": SLUG,
                "timestamp": (NOW - timedelta(minutes=2)).isoformat(),
                "data": {"entry": {"event": "failure"}},
            }
        ),
        "not-json",
    ]
    command = new_retry_command(
        repo_slug=SLUG,
        task_id="PR-9",
        task_file="tasks/PR-9.md",
        task_branch="pr-9",
        task_fingerprint="fingerprint",
        request_binding="binding",
        failure_id="failure",
        retry_cap=3,
        now=NOW - timedelta(minutes=1),
    )
    redis.zsets[retry_command_pending(SLUG)] = [
        (command.command_id, command.requested_at.timestamp()),
        ("missing-command", command.requested_at.timestamp() + 1),
        ("malformed-command", command.requested_at.timestamp() + 2),
    ]
    redis.store[retry_command(SLUG, command.command_id)] = command.model_dump_json()
    redis.store[retry_command(SLUG, "malformed-command")] = "{bad"
    redis.ttls[retry_command(SLUG, command.command_id)] = 120
    run = _run("run-active")
    redis.lists[MetricsStore._recent_key("PR-9", SLUG)] = ["run-active", "missing-run", "bad-run"]
    redis.store[MetricsStore._record_key("run-active")] = json.dumps(asdict(run))
    redis.store[MetricsStore._record_key("bad-run")] = "[]"
    before = copy.deepcopy((redis.store, redis.lists, redis.zsets, redis.ttls))

    result = await get_orchestrator_status(SLUG, event_limit=5, run_limit=5)

    overview = result["repositories"][0]
    assert overview["snapshot"]["status"] == "stale"
    assert overview["observed"]["state"] == "WATCH"
    assert overview["observed"]["ci_status"] == "FAILURE"
    assert "state-secret" not in json.dumps(result)
    assert overview["observed"]["error"] == "Authorization: [REDACTED]"
    assert result["detail"]["queue"]["counts_by_status"] == {"DOING": 1}
    assert "history" not in result["detail"]["state"]
    assert "current_queue" not in result["detail"]["state"]
    assert result["detail"]["recent_events"]["malformed_records"] == 1
    retry_statuses = [item["status"] for item in result["detail"]["pending_retries"]["commands"]]
    assert retry_statuses == ["available", "missing_payload", "malformed"]
    assert result["detail"]["run_records"]["missing_indexed_records"] == 1
    assert result["detail"]["coder_progress"]["process_activity"] == "unknown"
    assert result["detail"]["coder_progress"]["unfinished_run_records"][0]["run_id"] == "run-active"
    assert "do not prove" in result["detail"]["coder_progress"]["interpretation"]
    assert before == (redis.store, redis.lists, redis.zsets, redis.ttls)
    assert not ({"set", "expire", "delete", "zrem", "ltrim"} & {name for name, _ in redis.calls})
    assert redis.closed is True


async def test_status_reports_missing_malformed_and_redis_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools.diagnostics import get_orchestrator_status

    redis = FakeRedis()
    config = _config(_repo(), _repo("https://github.com/octo/other.git"))
    _patch_runtime(monkeypatch, redis, config)
    redis.store[pipeline_state("octo__other")] = "{malformed"

    result = await get_orchestrator_status()

    assert [item["snapshot"]["status"] for item in result["repositories"]] == ["missing", "malformed"]
    assert all(item["observed"]["state"] is None for item in result["repositories"])

    redis.fail.add("mget")
    unavailable = await get_orchestrator_status(SLUG)
    assert unavailable["redis"]["status"] == "unavailable"
    assert unavailable["repositories"][0]["snapshot"]["status"] == "unavailable"
    assert unavailable["detail"]["pending_retries"]["status"] == "unavailable"
    assert "redis-secret" not in json.dumps(unavailable)


async def test_status_configuration_failure_and_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.mcp.tools import diagnostics

    def fail_config():
        raise ValueError("password=configuration-secret")

    monkeypatch.setattr(diagnostics, "_configured_repositories", fail_config)
    result = await diagnostics.get_orchestrator_status()
    assert result["configuration"]["status"] == "unavailable"
    assert "configuration-secret" not in json.dumps(result)

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    with pytest.raises(ValueError, match="Invalid repo_slug"):
        await diagnostics.get_orchestrator_status("../escape")
    with pytest.raises(ValueError, match="not configured"):
        await diagnostics.get_orchestrator_status("octo__missing")
    with pytest.raises(ValueError, match="event_limit"):
        await diagnostics.get_orchestrator_status(event_limit=0)
    with pytest.raises(ValueError, match="run_limit"):
        await diagnostics.get_orchestrator_status(run_limit=999)


async def test_log_discovery_and_reads_are_bounded_and_redacted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    repos_root = tmp_path / "repos"
    events_root = tmp_path / "events"
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)
    monkeypatch.setattr(diagnostics, "_EVENTS_ROOT", events_root)
    latest = "[truncated]\nstart\nAuthorization: Bearer top-secret\nlast failure line"
    redis.store[cli_log_latest(SLUG)] = latest
    redis.ttls[cli_log_latest(SLUG)] = 1800
    timestamp = "2026-10-05T11:45:00+00:00"
    history_key = cli_log_history(SLUG, timestamp)
    redis.store[history_key] = b"historic output"
    redis.ttls[history_key] = 80000
    redis.store[cli_log_history(SLUG, "not-a-time")] = "ignored"
    redis.lists[repo_events_history(SLUG)] = [
        json.dumps({"timestamp": timestamp, "type": "event", "data": {}}),
        "broken",
    ]
    event_dir = events_root / SLUG
    event_dir.mkdir(parents=True)
    (event_dir / "2026-10-05.jsonl").write_text(
        json.dumps({"timestamp": timestamp, "event_type": "event"}) + "\nmalformed\n",
        encoding="utf-8",
    )
    ci_path = repos_root / SLUG / "artifacts" / "ci.log"
    ci_path.parent.mkdir(parents=True)
    ci_path.write_text("header\nAPI_KEY=ci-secret\nEXACT FAILURE\n", encoding="utf-8")
    before = copy.deepcopy((redis.store, redis.lists, redis.zsets, redis.ttls))

    first = await diagnostics.list_orchestrator_logs(SLUG, limit=3)
    second = await diagnostics.list_orchestrator_logs(SLUG, cursor=first["pagination"]["next_cursor"], limit=10)
    sources = first["sources"] + second["sources"]
    ids = {source["source_id"] for source in sources}
    assert {
        "cli:latest",
        f"cli:history/{timestamp}",
        "events:redis",
        "events:disk/2026-10-05",
        "ci:artifact",
        "daemon:stdout",
        "cli:live",
    } <= ids
    latest_source = next(item for item in sources if item["source_id"] == "cli:latest")
    assert latest_source["retention"]["truncated"] is True
    ci_source = next(item for item in sources if item["source_id"] == "ci:artifact")
    assert ci_source["association"] == {
        "recorded": False,
        "task_id": None,
        "run_id": None,
        "sha": None,
        "note": "No task, run, or SHA association is recorded by this source.",
    }
    assert any("malformed CLI history" in warning for warning in second["warnings"] + first["warnings"])

    cli_page = await diagnostics.read_orchestrator_log(SLUG, "cli:latest", max_chars=30)
    assert cli_page["content"].startswith("[truncated]\n")
    assert cli_page["pagination"]["next_cursor"] == 30
    assert cli_page["source"]["source_truncated"] is True
    assert cli_page["redaction"]["applied"] is True
    assert "top-secret" not in cli_page["content"]

    ci_tail = await diagnostics.read_orchestrator_log(SLUG, "ci:artifact", max_chars=25, tail=True)
    assert "EXACT FAILURE" in ci_tail["content"]
    assert "ci-secret" not in ci_tail["content"]
    assert ci_tail["source"]["association"]["recorded"] is False

    disk = await diagnostics.read_orchestrator_log(SLUG, "events:disk/2026-10-05")
    assert disk["source"]["malformed_records"] == 1
    assert disk["warnings"]
    events = await diagnostics.read_orchestrator_log(SLUG, "events:redis")
    assert events["source"]["malformed_records"] == 1
    assert before == (redis.store, redis.lists, redis.zsets, redis.ttls)


async def test_missing_expired_unretained_and_unavailable_logs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", tmp_path / "repos")
    monkeypatch.setattr(diagnostics, "_EVENTS_ROOT", tmp_path / "events")

    expired = await diagnostics.read_orchestrator_log(SLUG, "cli:history/2026-10-04T11:00:00+00:00")
    assert expired["source"]["availability"] == "missing_or_expired"
    assert expired["pagination"] is None
    missing_ci = await diagnostics.read_orchestrator_log(SLUG, "ci:artifact")
    assert missing_ci["source"]["availability"] == "missing"
    for source_id in ("daemon:stdout", "cli:live"):
        unavailable = await diagnostics.read_orchestrator_log(SLUG, source_id)
        assert unavailable["source"]["availability"] == "unavailable"

    redis.fail.add("get")
    unavailable = await diagnostics.read_orchestrator_log(SLUG, "cli:latest")
    assert unavailable["source"]["availability"] == "unavailable"
    assert "redis-secret" not in json.dumps(unavailable)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"cursor": -1}, "cursor"),
        ({"max_chars": 0}, "max_chars"),
        ({"max_chars": 20_001}, "max_chars"),
        ({"cursor": 1, "tail": True}, "cursor must be 0"),
    ],
)
async def test_read_validation(kwargs: dict[str, Any], message: str, monkeypatch: pytest.MonkeyPatch) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    with pytest.raises(ValueError, match=message):
        await diagnostics.read_orchestrator_log(SLUG, "cli:latest", **kwargs)


async def test_source_and_repository_isolation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", tmp_path / "repos")
    monkeypatch.setattr(diagnostics, "_EVENTS_ROOT", tmp_path / "events")
    with pytest.raises(ValueError, match="Invalid repo_slug"):
        await diagnostics.list_orchestrator_logs("../escape")
    with pytest.raises(ValueError, match="not configured"):
        await diagnostics.list_orchestrator_logs("octo__other")
    with pytest.raises(ValueError, match="Invalid CLI history"):
        await diagnostics.read_orchestrator_log(SLUG, "cli:history/not-a-date")
    with pytest.raises(ValueError, match="Invalid disk event"):
        await diagnostics.read_orchestrator_log(SLUG, "events:disk/2026-99-99")
    with pytest.raises(ValueError, match="Unknown filesystem"):
        await diagnostics.read_orchestrator_log(SLUG, "file:/etc/passwd")

    outside = tmp_path / "outside"
    outside.mkdir()
    roots = tmp_path / "symlink-events"
    roots.mkdir()
    (roots / SLUG).symlink_to(outside, target_is_directory=True)
    monkeypatch.setattr(diagnostics, "_EVENTS_ROOT", roots)
    with pytest.raises(ValueError, match="escapes"):
        await diagnostics.read_orchestrator_log(SLUG, "events:disk/2026-10-05")


def test_small_contract_helpers_cover_clock_skew_and_bounded_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    assert diagnostics._parse_timestamp(None) is None
    assert diagnostics._parse_timestamp("not-a-time") is None
    assert diagnostics._parse_timestamp("2026-10-05T12:00:00").tzinfo is not None
    assert diagnostics._error_text(RuntimeError()) == "RuntimeError"

    state = RepoState(
        url="https://github.com/octo/demo",
        name=SLUG,
        last_updated=datetime(2026, 10, 5, 12, 1),
        current_queue=[],
        current_queue_snapshot_at=datetime(2026, 10, 5, 11, 59),
    )
    snapshot = diagnostics._snapshot_metadata(
        state,
        status="observed",
        observed_at=NOW,
        stale_after_seconds=600,
    )
    assert snapshot["status"] == "clock_skew"
    assert diagnostics._queue_summary(state, NOW)["snapshot_age_seconds"] == 60
    assert diagnostics._bounded_event("[]")["status"] == "malformed"
    huge = json.dumps({"timestamp": NOW.isoformat(), "data": "x" * 5_000})
    assert diagnostics._bounded_event(huge)["record_truncated"] is True
    sensitive = "\n".join(
        [
            "token='two word secret'",
            "AKIAABCDEFGHIJKLMNOP",
            "sk-ant-" + "a" * 30,
            "sk-" + "b" * 48,
            "https://hooks.slack.com/services/TAAA/BBBB/" + "c" * 24,
            "rk_live_" + "d" * 24,
            "AIza" + "e" * 35,
            "eyJ" + "a" * 12 + "." + "b" * 12 + "." + "c" * 12,
            "-----BEGIN PRIVATE KEY-----\nmaterial\n-----END PRIVATE KEY-----",
            "redis://:redis-password@redis:6379/0",
        ]
    )
    redacted, replacements = diagnostics._redact_text(sensitive)
    assert "two word secret" not in redacted
    assert "material" not in redacted
    assert "redis-password" not in redacted
    assert replacements == 10
    assert diagnostics._iso_z(datetime(2026, 10, 5, 12, 0)).endswith("Z")

    captured: dict[str, Any] = {}

    def fake_from_url(url: str, **kwargs: Any) -> str:
        captured.update(url=url, **kwargs)
        return "client"

    monkeypatch.setattr(diagnostics.aioredis, "from_url", fake_from_url)
    monkeypatch.setenv("REDIS_URL", "redis://example:6379/1")
    assert diagnostics._new_redis_client() == "client"
    assert captured == {"url": "redis://example:6379/1", "decode_responses": True}

    config = _config(_repo())
    monkeypatch.setattr(diagnostics, "load_config", lambda: config)
    loaded, repositories = diagnostics._configured_repositories()
    assert loaded is config
    assert repositories == {SLUG: config.repositories[0]}


async def test_status_helper_failures_remain_explicit() -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    redis.fail.add("lrange")
    events = await diagnostics._recent_events(redis, SLUG, 5)
    runs = await diagnostics._relevant_runs(redis, SLUG, "PR-9", 5)
    assert events["status"] == runs["status"] == "unavailable"

    redis = FakeRedis()
    redis.fail.add("zcard")
    retries = await diagnostics._pending_retries(redis, SLUG)
    assert retries["status"] == "unavailable"

    redis = FakeRedis()
    redis.zsets[retry_command_pending(SLUG)] = [("command", 1.0)]
    redis.fail.add("get")
    retries = await diagnostics._pending_retries(redis, SLUG)
    assert retries["commands"][0]["status"] == "unavailable"


async def test_run_filtering_unavailable_record_and_limit() -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    index = MetricsStore._recent_key("PR-9", SLUG)
    redis.lists[index] = ["unavailable", "other-task", "wanted", "extra"]
    other = asdict(_run("other"))
    other["task_id"] = "PR-8"
    redis.store[MetricsStore._record_key("other-task")] = json.dumps(other)
    redis.store[MetricsStore._record_key("wanted")] = json.dumps(asdict(_run("wanted")))
    redis.store[MetricsStore._record_key("extra")] = json.dumps(asdict(_run("extra")))
    original_get = redis.get

    async def selective_get(key: str) -> object | None:
        if key == MetricsStore._record_key("unavailable"):
            raise ConnectionError("record unavailable")
        return await original_get(key)

    redis.get = selective_get  # type: ignore[method-assign]
    result = await diagnostics._relevant_runs(redis, SLUG, "PR-9", 2)
    assert [item["status"] for item in result["records"]] == ["unavailable", "available"]
    assert result["records"][1]["record"]["run_id"] == "wanted"

    assert diagnostics._state_history(None, 2)["status"] == "unavailable"
    state = RepoState(
        url="https://github.com/octo/demo",
        name=SLUG,
        history=[{"time": NOW.isoformat(), "state": "ERROR", "event": "x" * 2_100}],
    )
    history = diagnostics._state_history(state, 1)
    assert history["events"][0]["record_truncated"] is True


async def test_log_discovery_defensive_failures(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    redis.fail.add("get")
    sources, warnings = await diagnostics._redis_log_sources(redis, SLUG, NOW)
    assert all(item["availability"] == "unavailable" for item in sources)
    assert warnings

    redis = FakeRedis()
    history_key = cli_log_history(SLUG, "2026-10-05T11:00:00+00:00")
    redis.store[history_key] = "value"
    original_get = redis.get

    async def expire_during_scan(key: str) -> object | None:
        if key == history_key:
            return None
        return await original_get(key)

    redis.get = expire_during_scan  # type: ignore[method-assign]
    sources, _ = await diagnostics._redis_log_sources(redis, SLUG, NOW)
    assert not any(item["source_id"].startswith("cli:history/") for item in sources)

    redis = FakeRedis()

    async def broken_scan(match: str):
        del match
        raise ConnectionError("scan failure")
        yield ""  # pragma: no cover - makes this an async generator

    redis.scan_iter = broken_scan  # type: ignore[method-assign]
    _, warnings = await diagnostics._redis_log_sources(redis, SLUG, NOW)
    assert any("discovery incomplete" in warning for warning in warnings)

    events_root = tmp_path / "events"
    repo_dir = events_root / SLUG
    repo_dir.mkdir(parents=True)
    (repo_dir / "ignored.jsonl").write_text("ignored", encoding="utf-8")
    outside = tmp_path / "outside.jsonl"
    outside.write_text("outside", encoding="utf-8")
    (repo_dir / "2026-10-04.jsonl").symlink_to(outside)
    monkeypatch.setattr(diagnostics, "_EVENTS_ROOT", events_root)
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", tmp_path / "repos")
    _, warnings = diagnostics._file_log_sources(SLUG)
    assert any("2026-10-04" in warning for warning in warnings)

    escaped_root = tmp_path / "escaped-events"
    escaped_root.mkdir()
    (escaped_root / SLUG).symlink_to(tmp_path, target_is_directory=True)
    monkeypatch.setattr(diagnostics, "_EVENTS_ROOT", escaped_root)
    sources, warnings = diagnostics._file_log_sources(SLUG)
    assert sources == []
    assert warnings

    repos_root = tmp_path / "escaped-repos"
    repos_root.mkdir()
    (repos_root / SLUG).symlink_to(tmp_path, target_is_directory=True)
    monkeypatch.setattr(diagnostics, "_EVENTS_ROOT", tmp_path / "empty-events")
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)
    sources, warnings = diagnostics._file_log_sources(SLUG)
    assert sources[0]["source_id"] == "ci:artifact"
    assert sources[0]["availability"] == "missing"
    assert any("ci.log" in warning for warning in warnings)


async def test_list_outer_failure_and_file_read_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    monkeypatch.setattr(diagnostics, "_EVENTS_ROOT", tmp_path / "events")
    repos_root = tmp_path / "repos"
    ci_path = repos_root / SLUG / "artifacts" / "ci.log"
    ci_path.parent.mkdir(parents=True)
    ci_path.write_text("content", encoding="utf-8")
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)

    async def unexpected(*args: Any, **kwargs: Any):
        del args, kwargs
        raise RuntimeError("unexpected redis source failure")

    monkeypatch.setattr(diagnostics, "_redis_log_sources", unexpected)
    listed = await diagnostics.list_orchestrator_logs(SLUG)
    assert any(item["source_id"] == "redis:diagnostics" for item in listed["sources"])

    with pytest.raises(ValueError, match="Unknown Redis"):
        await diagnostics._read_redis_source(redis, SLUG, "unknown", NOW)

    original_read_text = Path.read_text

    def fail_ci_read(self: Path, *args: Any, **kwargs: Any) -> str:
        if self == ci_path:
            raise OSError("read failed")
        return original_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", fail_ci_read)
    missing = await diagnostics.read_orchestrator_log(SLUG, "ci:artifact")
    assert missing["source"]["availability"] == "unavailable"


def test_compose_wires_read_only_runtime_sources() -> None:
    import yaml

    compose = yaml.safe_load(Path("docker-compose.yml").read_text(encoding="utf-8"))
    service = compose["services"]["mcp"]
    assert service["environment"]["REDIS_URL"] == "redis://redis:6379/0"
    assert "redis" in service["depends_on"]
    assert "./data/events:/data/events:ro" in service["volumes"]
    assert all("docker.sock" not in volume for volume in service["volumes"])
    assert all("/data/auth" not in volume for volume in service["volumes"])
