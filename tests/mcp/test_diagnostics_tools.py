"""Regression coverage for the read-only Orchestrator MCP diagnostics."""

from __future__ import annotations

import copy
import fnmatch
import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from io import BytesIO
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

    async def scan(self, *, cursor: int, match: str, count: int) -> tuple[int, list[str]]:
        self._check("scan", (cursor, match, count))
        keys = [key for key in sorted(self.store) if fnmatch.fnmatch(key, match)]
        page = keys[cursor : cursor + count]
        next_cursor = cursor + len(page)
        return (0 if next_cursor >= len(keys) else next_cursor), page

    async def eval_ro(
        self,
        script: str,
        numkeys: int,
        key: str,
        start: int,
        stop: int,
        byte_limit: int,
    ) -> list[object]:
        self._check("eval_ro", (script, numkeys, key, start, stop, byte_limit))
        values = self.lists.get(key, [])
        size_bytes = sum(len(value if isinstance(value, bytes) else str(value).encode()) for value in values)
        if size_bytes > byte_limit:
            return [size_bytes, 1, len(values), []]
        selected = values[start:] if stop == -1 else values[start : stop + 1]
        return [size_bytes, 0, len(values), list(selected)]

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
                "data": {
                    "entry": {"event": "failure"},
                    "DATABASE_PASSWORD": "structured-secret",
                },
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
    assert "structured-secret" not in json.dumps(result)
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
    third = await diagnostics.list_orchestrator_logs(SLUG, cursor=second["pagination"]["next_cursor"], limit=10)
    sources = first["sources"] + second["sources"] + third["sources"]
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
    assert third["pagination"]["phase"] == "redis_history"
    assert third["pagination"]["next_cursor"] is None
    assert any(
        "malformed CLI history" in warning
        for warning in third["warnings"] + second["warnings"] + first["warnings"]
    )

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
    assert disk["source"]["malformed_records_in_page"] == 1
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

    redis.lists[repo_events_history(SLUG)] = ["x" * (diagnostics._MAX_REDIS_EVENT_HISTORY_BYTES + 1)]
    oversized = await diagnostics.read_orchestrator_log(SLUG, "events:redis")
    assert oversized["source"]["availability"] == "oversized"
    assert oversized["content"] == ""
    assert oversized["source"]["read_bound_bytes"] == diagnostics._MAX_REDIS_EVENT_HISTORY_BYTES
    assert not any(operation == "lrange" for operation, _ in redis.calls)
    recent = await diagnostics._recent_events(redis, SLUG, 5)
    assert recent["status"] == "oversized"
    listed, warnings = await diagnostics._redis_log_sources(redis, SLUG, NOW)
    event_source = next(source for source in listed if source["source_id"] == "events:redis")
    assert event_source["availability"] == "oversized"
    assert warnings

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
    with pytest.raises(ValueError, match="beyond"):
        await diagnostics.list_orchestrator_logs(SLUG, cursor=999)

    outside = tmp_path / "outside"
    outside.mkdir()
    roots = tmp_path / "symlink-events"
    roots.mkdir()
    (roots / SLUG).symlink_to(outside, target_is_directory=True)
    monkeypatch.setattr(diagnostics, "_EVENTS_ROOT", roots)
    with pytest.raises(ValueError, match="escapes"):
        await diagnostics.read_orchestrator_log(SLUG, "events:disk/2026-10-05")

    sibling_repos = tmp_path / "sibling-repos"
    selected_repo = sibling_repos / SLUG
    sibling_artifacts = sibling_repos / "octo__sibling" / "artifacts"
    selected_repo.mkdir(parents=True)
    sibling_artifacts.mkdir(parents=True)
    (sibling_artifacts / "ci.log").write_text("sibling secret", encoding="utf-8")
    (selected_repo / "artifacts").symlink_to(sibling_artifacts, target_is_directory=True)
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", sibling_repos)
    monkeypatch.setattr(diagnostics, "_EVENTS_ROOT", tmp_path / "empty-events")
    with pytest.raises(ValueError, match="escapes"):
        await diagnostics.read_orchestrator_log(SLUG, "ci:artifact")
    sources, warnings = diagnostics._file_log_sources(SLUG)
    assert sources[-1]["source_id"] == "ci:artifact"
    assert sources[-1]["availability"] == "missing"
    assert any("ci.log" in warning for warning in warnings)

    sibling_events = tmp_path / "sibling-events"
    selected_events = sibling_events / SLUG
    other_events = sibling_events / "octo__sibling"
    selected_events.mkdir(parents=True)
    other_events.mkdir(parents=True)
    (other_events / "2026-10-05.jsonl").write_text("sibling event secret", encoding="utf-8")
    (selected_events / "2026-10-05.jsonl").symlink_to(other_events / "2026-10-05.jsonl")
    monkeypatch.setattr(diagnostics, "_EVENTS_ROOT", sibling_events)
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
    oversized_secret = diagnostics._bounded_event(
        json.dumps(
            {
                "timestamp": NOW.isoformat(),
                "password": "word1 leaksecret " + "x" * 5_000,
            }
        )
    )
    assert oversized_secret["record_truncated"] is True
    assert "leaksecret" not in oversized_secret["record_excerpt"]
    assert "[REDACTED]" in oversized_secret["record_excerpt"]
    progress = diagnostics._progress_evidence(
        None,
        {"records": []},
        {"events": [diagnostics._bounded_event(huge)]},
    )
    assert progress["latest_retained_event_at"] == NOW.isoformat()
    malformed_secret = diagnostics._bounded_event('{"password":"word1 leaksecret ' + "x" * 600)
    assert "leaksecret" not in malformed_secret["raw_excerpt"]
    sensitive = "\n".join(
        [
            "token='two word secret'",
            "DATABASE_PASSWORD=database-secret",
            "MY_API_KEY='plain api secret'",
            '{"SERVICE_REFRESH_TOKEN":"json-secret"}',
            '''{"password":"abc'def"}''',
            r'''{"client_secret":"abc\"def"}''',
            '''password='abc"def' ''',
            r'''password="abc\"def"''',
            "AKIAABCDEFGHIJKLMNOP",
            "sk-ant-" + "a" * 30,
            "sk-" + "b" * 48,
            "https://hooks.slack.com/services/TAAA/BBBB/" + "c" * 24,
            "rk_live_" + "d" * 24,
            "AIza" + "e" * 35,
            "eyJ" + "a" * 12 + "." + "b" * 12 + "." + "c" * 12,
            "-----BEGIN PRIVATE KEY-----\nmaterial\n-----END PRIVATE KEY-----",
            "redis://:redis-password@redis:6379/0",
            "Cookie: theme=dark; session_id=cookie-secret",
            "> Cookie: theme=dark; session_id=prefixed-cookie-secret",
            '< Authorization: Digest username="user", response="digest-secret"',
            "> Proxy-Authorization: AWS4-HMAC-SHA256 Credential=user, Signature=sig-secret",
            "* Set-Cookie: harmless=yes; auth=prefixed-set-cookie-secret",
            "tool --password=option-secret",
            "tool --MY_API_KEY 'quoted option secret'",
            "tool --token plain-option-secret",
            "curl --user alice:curl-secret",
            "curl -u bob:short-curl-secret",
            "curl -U proxy:proxy-curl-secret",
        ]
    )
    redacted, replacements = diagnostics._redact_text(sensitive)
    assert "two word secret" not in redacted
    assert "database-secret" not in redacted
    assert "plain api secret" not in redacted
    assert "json-secret" not in redacted
    assert "abc'def" not in redacted
    assert r'abc\"def' not in redacted
    assert 'abc"def' not in redacted
    assert "material" not in redacted
    assert "redis-password" not in redacted
    assert "cookie-secret" not in redacted
    assert "prefixed-cookie-secret" not in redacted
    assert "digest-secret" not in redacted
    assert "sig-secret" not in redacted
    assert "prefixed-set-cookie-secret" not in redacted
    assert "option-secret" not in redacted
    assert "quoted option secret" not in redacted
    assert "plain-option-secret" not in redacted
    assert "curl-secret" not in redacted
    assert "short-curl-secret" not in redacted
    assert "proxy-curl-secret" not in redacted
    assert replacements == 28
    assert diagnostics._redact_text("tokens_in=123 tokens_out=456") == (
        "tokens_in=123 tokens_out=456",
        0,
    )
    for partial_key in (
        "-----BEGIN PRIVATE KEY-----\npartial-secret",
        "[truncated]\npartial-secret\n-----END PRIVATE KEY-----",
    ):
        safe_key, key_replacements = diagnostics._redact_text(partial_key)
        assert safe_key == "[REDACTED PRIVATE KEY]"
        assert key_replacements == 1
    structured, structured_count = diagnostics._redact_structure(
        {
            "data": {"DATABASE_PASSWORD": "plainsecret"},
            "tokens_in": 123,
        }
    )
    assert structured == {
        "data": {"DATABASE_PASSWORD": "[REDACTED]"},
        "tokens_in": 123,
    }
    assert structured_count == 1
    for camel_key in (
        "accessToken",
        "refreshToken",
        "clientSecret",
        "apiKey",
        "authToken",
        "privateKey",
        "awsSecretAccessKey",
        "secretAccessKey",
        "sessionToken",
        "databasePassword",
        "githubToken",
        "spring.datasource.password",
    ):
        structured, structured_count = diagnostics._redact_structure({camel_key: "camel-secret"})
        assert structured == {camel_key: "[REDACTED]"}
        assert structured_count == 1
        redacted_camel, camel_count = diagnostics._redact_text(
            json.dumps({camel_key: "camel-text-secret"})
        )
        assert "camel-text-secret" not in redacted_camel
        assert camel_count == 1
    pgp_key, pgp_replacements = diagnostics._redact_text(
        "-----BEGIN PGP PRIVATE KEY BLOCK-----\npgp-secret\n-----END PGP PRIVATE KEY BLOCK-----"
    )
    assert pgp_key == "[REDACTED PRIVATE KEY]"
    assert pgp_replacements == 1
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

    async def malformed_event_response(*args: Any, **kwargs: Any) -> list[object]:
        del args, kwargs
        return [0]

    redis.eval_ro = malformed_event_response  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="malformed bounded"):
        await diagnostics._read_bounded_event_history(redis, SLUG, start=0, stop=-1)

    async def malformed_event_records(*args: Any, **kwargs: Any) -> list[object]:
        del args, kwargs
        return [0, 0, 0, "not-a-list"]

    redis.eval_ro = malformed_event_records  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="malformed bounded"):
        await diagnostics._read_bounded_event_history(redis, SLUG, start=0, stop=-1)

    redis = FakeRedis()
    redis.fail.add("eval_ro")
    events = await diagnostics._recent_events(redis, SLUG, 5)
    assert events["status"] == "unavailable"

    redis = FakeRedis()
    redis.fail.add("lrange")
    runs = await diagnostics._relevant_runs(redis, SLUG, "PR-9", 5)
    assert runs["status"] == "unavailable"

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
    sources, _, _ = await diagnostics._redis_history_page(
        redis,
        SLUG,
        NOW,
        cursor_state={"scan_cursor": 0, "pending": [], "started": False},
        limit=10,
    )
    assert sources[0]["availability"] == "missing_or_expired"

    redis = FakeRedis()

    async def broken_scan(*, cursor: int, match: str, count: int):
        del cursor, match, count
        raise ConnectionError("scan failure")

    redis.scan = broken_scan  # type: ignore[method-assign]
    _, warnings, _ = await diagnostics._redis_history_page(
        redis,
        SLUG,
        NOW,
        cursor_state={"scan_cursor": 0, "pending": [], "started": False},
        limit=10,
    )
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

    for index in range(diagnostics._MAX_DISK_PARTITION_CANDIDATES + 5):
        (repo_dir / f"retained-{index:04d}.jsonl").write_text("event", encoding="utf-8")
    _, warnings = diagnostics._file_log_sources(SLUG)
    assert any("bounded candidate limit" in warning for warning in warnings)

    real_scandir = diagnostics.os.scandir

    def fail_event_scandir(path: Path):
        if Path(path) == repo_dir:
            raise OSError("directory unavailable")
        return real_scandir(path)

    monkeypatch.setattr(diagnostics.os, "scandir", fail_event_scandir)
    _, warnings = diagnostics._file_log_sources(SLUG)
    assert any("Could not discover disk event partitions" in warning for warning in warnings)
    monkeypatch.setattr(diagnostics.os, "scandir", real_scandir)

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


async def test_redis_history_discovery_uses_bounded_continuations() -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    for second in range(25):
        timestamp = f"2026-10-05T11:00:{second:02d}+00:00"
        key = cli_log_history(SLUG, timestamp)
        redis.store[key] = f"history {second}"
        redis.ttls[key] = 60

    sources, warnings, next_cursor = await diagnostics._redis_history_page(
        redis,
        SLUG,
        NOW,
        cursor_state={"scan_cursor": 0, "pending": [], "started": False},
        limit=2,
    )
    assert len(sources) == 2
    assert warnings == []
    assert next_cursor is not None
    history_gets = [key for operation, key in redis.calls if operation == "get"]
    assert len(history_gets) == 2
    assert len([call for call in redis.calls if call[0] == "scan"]) == 1

    kind, state = diagnostics._decode_log_cursor(next_cursor)
    assert kind == "history"
    redis.calls.clear()
    second, _, continuation = await diagnostics._redis_history_page(
        redis,
        SLUG,
        NOW,
        cursor_state=state,
        limit=2,
    )
    assert len(second) == 2
    assert continuation is not None
    assert not any(operation == "scan" for operation, _ in redis.calls)
    assert len([key for operation, key in redis.calls if operation == "get"]) == 2


async def test_redis_history_discovery_reports_defensive_bounds() -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    timestamps = [(NOW + timedelta(seconds=index)).isoformat() for index in range(205)]

    async def oversized_scan(*, cursor: int, match: str, count: int) -> tuple[int, list[bytes]]:
        del cursor, match, count
        keys = [b"outside:key", cli_log_history(SLUG, "not-a-time").encode()]
        keys.extend(cli_log_history(SLUG, timestamp).encode() for timestamp in timestamps)
        return 0, keys

    redis.scan = oversized_scan  # type: ignore[method-assign]
    sources, warnings, continuation = await diagnostics._redis_history_page(
        redis,
        SLUG,
        NOW,
        cursor_state={"scan_cursor": 0, "pending": [], "started": False},
        limit=1,
    )
    assert sources[0]["availability"] == "missing_or_expired"
    assert continuation is not None
    assert any("malformed CLI history" in warning for warning in warnings)
    assert any("oversized scan batch" in warning for warning in warnings)

    unavailable = FakeRedis()
    unavailable.fail.add("get")
    sources, _, continuation = await diagnostics._redis_history_page(
        unavailable,
        SLUG,
        NOW,
        cursor_state={"scan_cursor": 0, "pending": [timestamps[0]], "started": True},
        limit=1,
    )
    assert sources[0]["availability"] == "unavailable"
    assert "redis-secret" in sources[0]["error"]
    assert continuation is None


def test_log_discovery_cursor_validation() -> None:
    from src.mcp.tools import diagnostics

    assert diagnostics._decode_log_cursor(3) == ("static", 3)
    for invalid in (True, "not-a-cursor", f"{diagnostics._HISTORY_CURSOR_PREFIX}not-base64"):
        with pytest.raises(ValueError, match="cursor"):
            diagnostics._decode_log_cursor(invalid)

    encoded_list = diagnostics.base64.urlsafe_b64encode(b"[]").decode().rstrip("=")
    with pytest.raises(ValueError, match="history cursor"):
        diagnostics._decode_log_cursor(f"{diagnostics._HISTORY_CURSOR_PREFIX}{encoded_list}")
    invalid_payload = diagnostics._encode_history_cursor(-1, [], started=False)
    with pytest.raises(ValueError, match="history cursor"):
        diagnostics._decode_log_cursor(invalid_payload)


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

    original_open = Path.open

    def fail_ci_read(self: Path, *args: Any, **kwargs: Any):
        if self == ci_path and args and args[0] == "rb":
            raise OSError("read failed")
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", fail_ci_read)
    missing = await diagnostics.read_orchestrator_log(SLUG, "ci:artifact")
    assert missing["source"]["availability"] == "unavailable"


async def test_filesystem_reads_use_bounded_byte_windows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    monkeypatch.setattr(diagnostics, "_EVENTS_ROOT", tmp_path / "events")
    repos_root = tmp_path / "repos"
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)
    ci_path = repos_root / SLUG / "artifacts" / "ci.log"
    ci_path.parent.mkdir(parents=True)
    line = "ordinary diagnostic line\n"
    ci_path.write_text(
        line * ((diagnostics._MAX_FILE_SCAN_BYTES * 3) // len(line))
        + "Cookie: safe=no; session=tail-secret\nEXACT TAIL FAILURE\n",
        encoding="utf-8",
    )

    first = await diagnostics.read_orchestrator_log(SLUG, "ci:artifact", max_chars=1_000)
    assert len(first["content"]) <= 1_000
    assert first["pagination"]["cursor_unit"] == "source_byte"
    assert first["pagination"]["scanned_bytes"] <= diagnostics._MAX_FILE_SCAN_BYTES
    assert first["pagination"]["next_cursor"] is not None
    second = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=first["pagination"]["next_cursor"],
        max_chars=1_000,
    )
    assert second["pagination"]["cursor"] == first["pagination"]["next_cursor"]

    tail = await diagnostics.read_orchestrator_log(SLUG, "ci:artifact", max_chars=1_000, tail=True)
    assert "EXACT TAIL FAILURE" in tail["content"]
    assert "tail-secret" not in tail["content"]
    assert tail["pagination"]["scanned_bytes"] <= diagnostics._MAX_FILE_SCAN_BYTES
    assert tail["pagination"]["has_older"] is True

    private_payload = "private-key-material\n" * (diagnostics._MAX_FILE_SCAN_BYTES // 10)
    ci_path.write_text(
        "before\n-----BEGIN PGP PRIVATE KEY BLOCK-----\n"
        + private_payload
        + "-----END PGP PRIVATE KEY BLOCK-----\nafter\n",
        encoding="utf-8",
    )
    cursor = 0
    pages: list[str] = []
    while True:
        page = await diagnostics.read_orchestrator_log(
            SLUG,
            "ci:artifact",
            cursor=cursor,
            max_chars=200,
        )
        assert "private-key-material" not in page["content"]
        pages.append(page["content"])
        next_cursor = page["pagination"]["next_cursor"]
        if next_cursor is None:
            break
        cursor = next_cursor
    assert "after" in "".join(pages)

    private_tail = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        max_chars=200,
        tail=True,
    )
    assert "private-key-material" not in private_tail["content"]
    assert "after" in private_tail["content"]
    assert private_tail["pagination"]["context_scanned_bytes"] > 0

    multiline = b'before\n"password":\n\n  "multiline-secret"\nafter\n'
    ci_path.write_bytes(multiline)
    multiline_page = await diagnostics.read_orchestrator_log(SLUG, "ci:artifact", max_chars=200)
    assert "multiline-secret" not in multiline_page["content"]
    assert "[REDACTED SENSITIVE ASSIGNMENT]" in multiline_page["content"]

    value_cursor = multiline.index(b'\n\n') + 1
    value_page = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=value_cursor,
        max_chars=200,
    )
    assert "multiline-secret" not in value_page["content"]
    assert "[REDACTED SENSITIVE VALUE]" in value_page["content"]

    plain_yaml = b"password: correct horse battery staple\nafter\n"
    ci_path.write_bytes(plain_yaml)
    plain_yaml_page = await diagnostics.read_orchestrator_log(SLUG, "ci:artifact", max_chars=200)
    assert "correct horse battery staple" not in plain_yaml_page["content"]
    assert plain_yaml_page["content"].startswith("password: [REDACTED]")

    block_yaml = b"before\npassword: |\n  first-secret\n  second-secret\nafter\n"
    ci_path.write_bytes(block_yaml)
    block_yaml_page = await diagnostics.read_orchestrator_log(SLUG, "ci:artifact", max_chars=200)
    assert "first-secret" not in block_yaml_page["content"]
    assert "second-secret" not in block_yaml_page["content"]
    assert "[REDACTED SENSITIVE BLOCK]" in block_yaml_page["content"]
    block_cursor = block_yaml.index(b"  second-secret")
    block_continuation = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=block_cursor,
        max_chars=200,
    )
    assert "second-secret" not in block_continuation["content"]
    assert "[REDACTED SENSITIVE BLOCK]" in block_continuation["content"]
    assert "after" in block_continuation["content"]

    ordinary_size = diagnostics._MAX_PRIVATE_KEY_CONTEXT_BYTES + diagnostics._MAX_FILE_SCAN_BYTES * 2
    ci_path.write_bytes((b"ordinary line\n" * (ordinary_size // len(b"ordinary line\n") + 1))[:ordinary_size])
    fail_closed_tail = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        max_chars=200,
        tail=True,
    )
    assert fail_closed_tail["content"] == "[CONTENT OMITTED: PRIVATE-KEY CONTEXT UNKNOWN]\n"
    assert (
        fail_closed_tail["pagination"]["private_key_context_scanned_bytes"]
        == diagnostics._MAX_PRIVATE_KEY_CONTEXT_BYTES
    )
    assert any("omitted fail-closed" in warning for warning in fail_closed_tail["warnings"])

    warnings: list[str] = []
    units = diagnostics._redacted_file_units(
        b'"password":\n',
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        has_more_after_raw=True,
        warnings=warnings,
    )
    assert units[0][1] == "[REDACTED SENSITIVE ASSIGNMENT]\n"
    assert warnings

    assert diagnostics._sensitive_state_before(BytesIO(b"\n\n"), 2, b"next\n") == (
        False,
        False,
        None,
        2,
    )
    completed_block = b"password: |\n  secret\nnext: value\n"
    assert diagnostics._sensitive_state_before(
        BytesIO(completed_block),
        len(completed_block),
        b"  current\n",
    ) == (False, False, None, len(completed_block))
    unknown_context = b"x" * (diagnostics._MAX_SENSITIVE_ASSIGNMENT_CONTEXT_BYTES + 1)
    assert diagnostics._sensitive_state_before(BytesIO(unknown_context), len(unknown_context), b"  value\n") == (
        None,
        None,
        None,
        diagnostics._MAX_SENSITIVE_ASSIGNMENT_CONTEXT_BYTES,
    )
    indeterminate_block = b"x\n" + b"  ordinary\n" * diagnostics._MAX_SENSITIVE_ASSIGNMENT_CONTEXT_BYTES
    assert diagnostics._sensitive_state_before(
        BytesIO(indeterminate_block),
        len(indeterminate_block),
        b"  value\n",
    )[1] is None
    warnings = []
    assert diagnostics._redacted_file_units(
        b"unknown\n",
        starts_inside_private_key=False,
        starts_with_sensitive_value=None,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        has_more_after_raw=False,
        warnings=warnings,
    )[0][1] == "[CONTENT OMITTED: SENSITIVE-ASSIGNMENT CONTEXT UNKNOWN]\n"
    warnings = []
    assert diagnostics._redacted_file_units(
        b"  unknown\n",
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=None,
        sensitive_block_indent=None,
        has_more_after_raw=False,
        warnings=warnings,
    )[0][1] == "[CONTENT OMITTED: SENSITIVE-BLOCK CONTEXT UNKNOWN]\n"


async def test_filesystem_reader_omits_oversized_segments_and_lines(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    monkeypatch.setattr(diagnostics, "_EVENTS_ROOT", tmp_path / "events")
    repos_root = tmp_path / "repos"
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)
    ci_path = repos_root / SLUG / "artifacts" / "ci.log"
    ci_path.parent.mkdir(parents=True)
    ci_path.write_bytes(b"x" * (diagnostics._MAX_FILE_SCAN_BYTES * 2))

    from_start = await diagnostics.read_orchestrator_log(SLUG, "ci:artifact", max_chars=20)
    assert len(from_start["content"]) <= 20
    assert any("exceeds" in warning for warning in from_start["warnings"])
    from_middle = await diagnostics.read_orchestrator_log(SLUG, "ci:artifact", cursor=1, max_chars=20)
    assert len(from_middle["content"]) <= 20
    assert any("intersects" in warning for warning in from_middle["warnings"])
    tail = await diagnostics.read_orchestrator_log(SLUG, "ci:artifact", max_chars=20, tail=True)
    assert tail["content"] == ""
    assert any("Tail window" in warning for warning in tail["warnings"])

    ci_path.write_text("ok\n" + "y" * 200 + "\n", encoding="utf-8")
    stops_before_line = await diagnostics.read_orchestrator_log(SLUG, "ci:artifact", max_chars=10)
    assert stops_before_line["content"] == "ok\n"
    assert stops_before_line["pagination"]["next_cursor"] == 3

    ci_path.write_text("z" * 200 + "\n", encoding="utf-8")
    truncates_line = await diagnostics.read_orchestrator_log(SLUG, "ci:artifact", max_chars=10)
    assert truncates_line["content"] == "z" * 10
    assert any("exceeded max_chars" in warning for warning in truncates_line["warnings"])

    ci_path.write_text(
        "before\n"
        "-----BEGIN OPENSSH PRIVATE KEY-----\n"
        "private-key-material\n"
        "-----END OPENSSH PRIVATE KEY-----\n"
        "after\n",
        encoding="utf-8",
    )
    private_key = await diagnostics.read_orchestrator_log(SLUG, "ci:artifact", max_chars=200)
    assert private_key["content"] == "before\n[REDACTED PRIVATE KEY]\nafter\n"
    assert "private-key-material" not in private_key["content"]

    ci_path.write_text(
        "-----BEGIN PRIVATE KEY-----\n" + "secret\n" * 100,
        encoding="utf-8",
    )
    incomplete_key = await diagnostics.read_orchestrator_log(SLUG, "ci:artifact", max_chars=200)
    assert incomplete_key["content"] == "[REDACTED PRIVATE KEY]\n"
    assert any("crossed the bounded scan window" in warning for warning in incomplete_key["warnings"])

    ci_path.write_text("first line\nsecond line\n", encoding="utf-8")
    aligned = await diagnostics.read_orchestrator_log(SLUG, "ci:artifact", cursor=2, max_chars=20)
    assert aligned["content"] == "second line\n"
    assert aligned["pagination"]["cursor"] == len("first line\n")
    eof = await diagnostics.read_orchestrator_log(SLUG, "ci:artifact", cursor=10_000, max_chars=20)
    assert eof["content"] == ""
    assert eof["pagination"]["next_cursor"] is None


def test_compose_wires_read_only_runtime_sources() -> None:
    import yaml

    compose = yaml.safe_load(Path("docker-compose.yml").read_text(encoding="utf-8"))
    service = compose["services"]["mcp"]
    assert service["environment"]["REDIS_URL"] == "redis://redis:6379/0"
    assert "redis" in service["depends_on"]
    assert "./data/events:/data/events:ro" in service["volumes"]
    assert all("docker.sock" not in volume for volume in service["volumes"])
    assert all("/data/auth" not in volume for volume in service["volumes"])
