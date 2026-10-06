"""Regression coverage for the read-only Orchestrator MCP diagnostics."""

from __future__ import annotations

import asyncio
import copy
import fnmatch
import hashlib
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

    async def strlen(self, key: str) -> int:
        self._check("strlen", key)
        value = self.store.get(key)
        if value is None:
            return 0
        return len(value if isinstance(value, bytes) else str(value).encode())

    async def getrange(self, key: str, start: int, end: int) -> object:
        self._check("getrange", (key, start, end))
        value = self.store.get(key, b"")
        raw = value if isinstance(value, bytes) else str(value).encode()
        selected = raw[start : end + 1]
        return selected if isinstance(value, bytes) else selected.decode(errors="replace")

    async def exists(self, key: str) -> int:
        self._check("exists", key)
        return int(key in self.store)

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
        *args: object,
    ) -> list[object]:
        self._check("eval_ro", (script, numkeys, key, *args))
        if "'SCAN'" in script:
            match, count, maximum_size, maximum_count = args
            keys = [item for item in sorted(self.store) if fnmatch.fnmatch(item, str(match))]
            cursor = int(key)
            page = keys[cursor : cursor + int(count)]
            next_cursor = cursor + len(page)
            bounded: list[str] = []
            oversized = 0
            dropped = 0
            for item in page:
                if len(item.encode()) > int(maximum_size):
                    oversized += 1
                elif len(bounded) >= int(maximum_count):
                    dropped += 1
                else:
                    bounded.append(item)
            return [0 if next_cursor >= len(keys) else next_cursor, bounded, oversized, dropped]
        if "ZCARD" in script:
            (
                cursor_score,
                cursor_member,
                cursor_digest,
                cursor_index,
                cursor_score_tail,
                page_limit,
                member_limit,
                lookup_limit,
            ) = args
            values = sorted(self.zsets.get(key, []), key=lambda item: (item[1], str(item[0])))
            first = 0
            if cursor_score != "":
                if cursor_member != "":
                    cursor = (float(cursor_score), str(cursor_member))
                    while first < len(values) and (values[first][1], str(values[first][0])) <= cursor:
                        first += 1
                else:
                    while first < len(values) and values[first][1] < float(cursor_score):
                        first += 1
                    located = False
                    group_end = first
                    while (
                        group_end < len(values)
                        and values[group_end][1] == float(cursor_score)
                    ):
                        group_end += 1
                    if cursor_score_tail != "":
                        center = group_end - int(cursor_score_tail) - 1
                    elif cursor_index != "":
                        center = int(cursor_index)
                    else:
                        center = first
                    search_start = max(first, center - int(lookup_limit) // 2)
                    search_end = min(group_end, search_start + int(lookup_limit))
                    search_start = max(first, search_end - int(lookup_limit))
                    for position in range(search_start, search_end):
                        if values[position][1] != float(cursor_score):
                            continue
                        member = values[position][0]
                        raw_member = member if isinstance(member, bytes) else str(member).encode()
                        if hashlib.sha1(raw_member).hexdigest() == cursor_digest:
                            first = position + 1
                            located = True
                            break
                    if (
                        not located
                        and cursor_score_tail != ""
                        and cursor_index != ""
                        and first <= int(cursor_index) < group_end
                    ):
                        position = int(cursor_index)
                        member = values[position][0]
                        raw_member = member if isinstance(member, bytes) else str(member).encode()
                        if hashlib.sha1(raw_member).hexdigest() == cursor_digest:
                            first = position + 1
                            located = True
                    if not located:
                        return [len(values), -1, []]
            selected = values[first : first + int(page_limit)]
            flattened: list[object] = []
            for offset, (member, score) in enumerate(selected):
                raw_member = member if isinstance(member, bytes) else str(member).encode()
                oversized = len(raw_member) > int(member_limit)
                score_tail = sum(
                    1
                    for _later_member, later_score in values[first + offset + 1 :]
                    if later_score == score
                )
                flattened.extend(
                    (
                        "" if oversized else member,
                        score,
                        first + offset,
                        len(raw_member),
                        hashlib.sha1(raw_member).hexdigest() if oversized else "",
                        score_tail if oversized else "",
                    )
                )
            return [len(values), first, flattened]
        if "LINDEX" in script:
            scan_limit, member_limit = (int(item) for item in args)
            values = self.lists.get(key, [])
            rows: list[object] = []
            for index, member in enumerate(values[:scan_limit]):
                size_bytes = len(member if isinstance(member, bytes) else str(member).encode())
                rows.extend((index, size_bytes, member if size_bytes <= member_limit else ""))
            return [len(values), rows]
        start, stop, byte_limit = (int(item) for item in args)
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
    from src.mcp.tools import diagnostics

    get_orchestrator_status = diagnostics.get_orchestrator_status

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
                    "auths": {
                        "registry.example": {"auth": "status-docker-auth-secret"},
                    },
                    "jwtSecretKey": "status-secret-key-value",
                    "tls.key": "status-tls-key-value",
                    "SharedAccessKey": "status-azure-access-key-value",
                    "env": [
                        {"name": "PASSWORD", "value": "status-name-value-secret"},
                    ],
                    "message": 'PASSWORD="status-structured-first\nstatus-structured-second"',
                    "netrc": (
                        "machine status.example login alice password status-netrc-secret"
                    ),
                    "resource": {
                        "kind": "Secret",
                        "metadata": {"name": "retained-name"},
                        "data": {".dockerconfigjson": "status-kube-data-secret"},
                        "stringData": {"config": "status-kube-string-secret"},
                    },
                    "yaml": (
                        "apiVersion: v1\ndata:\n"
                        "  opaque: status-kube-yaml-secret\n"
                        "kind: Secret\nmetadata:\n  annotations:\n    note: |\n      ---\n"
                        "stringData: {config: status-kube-yaml-inline-secret}\n"
                        "---\nkind: Pod\nspec:\n  env:\n"
                        "    - name: PASSWORD\n      value: status-kube-yaml-env-secret\n"
                    ),
                },
            }
        ),
        '{"password":status-malformed-password,}',
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
    assert "status-docker-auth-secret" not in json.dumps(result)
    assert "status-secret-key-value" not in json.dumps(result)
    assert "status-tls-key-value" not in json.dumps(result)
    assert "status-azure-access-key-value" not in json.dumps(result)
    assert "status-name-value-secret" not in json.dumps(result)
    assert "status-structured-first" not in json.dumps(result)
    assert "status-structured-second" not in json.dumps(result)
    assert "status-netrc-secret" not in json.dumps(result)
    assert "status-kube-data-secret" not in json.dumps(result)
    assert "status-kube-string-secret" not in json.dumps(result)
    assert "status-kube-yaml-secret" not in json.dumps(result)
    assert "status-kube-yaml-inline-secret" not in json.dumps(result)
    assert "status-kube-yaml-env-secret" not in json.dumps(result)
    assert "status-malformed-password" not in json.dumps(result)
    assert overview["observed"]["error"] == "Authorization: [REDACTED]"
    assert result["detail"]["queue"]["counts_by_status"] == {"DOING": 1}
    assert "history" not in result["detail"]["state"]
    assert "current_queue" not in result["detail"]["state"]
    assert result["detail"]["recent_events"]["malformed_records"] == 1
    retry_statuses = [item["status"] for item in result["detail"]["pending_retries"]["commands"]]
    assert retry_statuses == ["available", "missing_payload", "malformed"]
    retry_cursor = diagnostics._retry_cursor(
        command.requested_at.timestamp() + 1,
        "missing-command",
    )
    retry_page = await get_orchestrator_status(SLUG, retry_cursor=retry_cursor)
    assert retry_page["detail"]["pending_retries"]["cursor"] == retry_cursor
    assert [
        item["status"] for item in retry_page["detail"]["pending_retries"]["commands"]
    ] == ["malformed"]
    assert result["detail"]["run_records"]["missing_indexed_records"] == 1
    assert result["detail"]["coder_progress"]["process_activity"] == "unknown"
    assert result["detail"]["coder_progress"]["unfinished_run_records"][0]["run_id"] == "run-active"
    assert "do not prove" in result["detail"]["coder_progress"]["interpretation"]
    assert before == (redis.store, redis.lists, redis.zsets, redis.ttls)
    assert not ({"set", "expire", "delete", "zrem", "ltrim"} & {name for name, _ in redis.calls})
    assert not ({"get", "mget"} & {name for name, _ in redis.calls})
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

    redis.fail.add("strlen")
    unavailable = await get_orchestrator_status(SLUG)
    assert unavailable["redis"]["status"] == "unavailable"
    assert unavailable["repositories"][0]["snapshot"]["status"] == "unavailable"
    assert unavailable["detail"]["recent_events"]["status"] == "available"
    assert unavailable["detail"]["pending_retries"]["status"] == "available"
    assert unavailable["detail"]["run_records"]["status"] == "available"
    assert "redis-secret" not in json.dumps(unavailable)

    def fail_client():
        raise ConnectionError("Authorization: Bearer connection-secret")

    from src.mcp.tools import diagnostics

    monkeypatch.setattr(diagnostics, "_new_redis_client", fail_client)
    disconnected = await get_orchestrator_status(SLUG)
    assert disconnected["redis"]["status"] == "unavailable"
    assert "connection-secret" not in json.dumps(disconnected)


async def test_status_closes_redis_when_detail_read_is_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    started = asyncio.Event()

    class BlockingRedis(FakeRedis):
        async def eval_ro(self, *args: object) -> list[object]:
            del args
            started.set()
            await asyncio.Event().wait()
            return []

    redis = BlockingRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    request = asyncio.create_task(diagnostics.get_orchestrator_status(SLUG))
    await started.wait()
    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request
    assert redis.closed is True


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
    with pytest.raises(ValueError, match="retry_cursor"):
        await diagnostics.get_orchestrator_status(retry_cursor="malformed")


async def test_log_discovery_and_reads_are_bounded_and_redacted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    repos_root = tmp_path / "repos"
    events_root = tmp_path / "events"
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)
    monkeypatch.setenv("PO_EVENTS_DIR", str(events_root))
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


async def test_all_retained_log_kinds_share_structured_and_multiline_redaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    events_root = tmp_path / "custom-events"
    repos_root = tmp_path / "repos"
    monkeypatch.setenv("PO_EVENTS_DIR", str(events_root))
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)

    docker_auth = "ZmFrZTpzZWNyZXQ="
    redis_payload = "\n".join(
        [
            "ordinary prefix",
            'export PASSWORD="fake-first-secret',
            'fake-second-secret"',
            'password = """toml-first-secret',
            'toml-second-secret"""',
            "PASSWORD=bare-slash-first-secret\\",
            "bare-slash-second-secret",
            "export PASSWORD=export-slash-first-secret\\",
            "export-slash-second-secret",
            "[env] PASSWORD=env-slash-first-secret\\",
            "env-slash-second-secret",
            "password:",
            "  yaml-first-secret",
            "  yaml-second-secret",
            "credentials:",
            "- redis-sequence-user",
            "- redis-same-indent-sequence-secret",
            "password =",
            "  redis-ini-first-secret",
            "  redis-ini-second-secret",
            "- password:",
            "    sequence-first-secret",
            "    sequence-second-secret",
            'INFO {"password":987654321,"debug":true}',
            "DJANGO_SECRET_KEY=django-secret-key-value",
            '{"jwtSecretKey":"jwt-secret-key-value"}',
            '{"data":{"tls.key":"redis-tls-key-value"}}',
            "AccountKey=redis-azure-account-key-value",
            "//registry.example/:_auth=redis-npm-basic-secret",
            "MYSQL_PWD=redis-pwd-secret",
            "Driver=Postgres;UID=alice;PWD=redis-odbc-pwd-secret;Server=db",
            "https://acct.blob.core.windows.net/c?sv=1&sp=r&sig=redis-sas-secret&se=tomorrow",
            "SharedAccessSignature=sv=1&sig=redis-shared-sas-secret",
            "https://example.cloudfront.net/file?Expires=1&Signature=redis-aws-signature&Key-Pair-Id=K1",
            "https://s3.example/file?X-Amz-Signature=redis-amz-signature&X-Amz-Expires=60",
            '{"SharedAccessKey":"redis-azure-shared-key-value"}',
            '{"_auth":"redis-npm-json-secret"}',
            '{"MYSQL_PWD":"redis-pwd-json-secret"}',
            '{"url":"https://acct.blob.core.windows.net/c?sv=1&sig=redis-json-sas-secret"}',
            '{"password":987650000,}',
            '{"password":123456789}',
            '{"credentials":["user","fake-list-secret"]}',
            '{"env":[{"name":"PASSWORD","value":"redis-name-value-secret"}]}',
            json.dumps(
                {"message": 'PASSWORD="redis-structured-first\nredis-structured-second"'}
            ),
            '{\n"password":\n"redis-same-indent-secret"\n}',
            "machine redis.example login alice password redis-netrc-secret",
            "spring.datasource.password redis-java-property-secret",
            "spring.datasource.username retained-java-property-user",
            "docker login -u user -p redis-docker-short-secret",
            "docker login -p",
            "Set-Item env:PASSWORD redis-powershell-positional-secret",
            "Set-Item -Path Env:\\CLIENT_SECRET -Value 'redis powershell named secret'",
            "Set-Item env:SAFE retained-powershell-safe",
            "aws configure set aws_secret_access_key redis-aws-configure-secret",
            "aws configure set region retained-aws-region",
            "mysql -u root -predis-mysql-short-secret",
            "mysql -p",
            "<password>redis-xml-element-secret</password>",
            "<password><![CDATA[redis-xml-cdata-secret]]></password>",
            "<password><value>redis-xml-nested-secret</value></password>",
            "<password>\nredis-xml-multiline-secret\n</password>",
            '<password\n encoding="plain">redis-xml-multiline-start-secret</password>',
            (
                '<password\n encoding="plain"\n mode="test">'
                "redis-xml-long-start-secret</password>"
            ),
            (
                "<clientSecret>\n<![CDATA[\n"
                "redis-xml-multiline-cdata-secret\n]]>\n</clientSecret>"
            ),
            "<password/>",
            "<password\n/>\nretained-after-split-self-close",
            '<property name="password" value="redis-xml-attribute-secret"/>',
            "<property value='redis-xml-reversed-secret' name='clientSecret'/>",
            "<username>retained-xml-element-value</username>",
            "<username>\nretained-xml-multiline-value\n</username>",
            '<username\n encoding="plain">retained-xml-multiline-start-value</username>',
            '<property name="username" value="retained-xml-attribute-value"/>',
            json.dumps(
                {
                    "kind": "Secret",
                    "data": {".dockerconfigjson": "redis-kube-data-secret"},
                    "stringData": {"config": "redis-kube-string-secret"},
                }
            ),
            "{apiVersion: v1, kind: Secret, data: {opaque: redis-kube-single-flow-secret}}",
            "INFO {kind: Secret, stringData: {password: redis-kube-prefixed-flow-secret}}",
            "{kind: ConfigMap, data: {harmless: retained-single-flow-config}}",
            (
                "{apiVersion: v1,\n kind: Secret,\n"
                " data: {opaque: redis-kube-multiline-flow-secret}}"
            ),
            "env: [{name: PASSWORD, value: redis-flow-env-secret}]",
            "env: [{value: redis-reversed-flow-env-secret, name: API_KEY}]",
            "env: [{name: PASSWORD,\n value: redis-multiline-flow-env-secret}]",
            "---",
            (
                "{apiVersion: v1,\n kind: ConfigMap,\n"
                " data: {harmless: retained-multiline-flow-config}}"
            ),
            "---",
            "env: [{name: SAFE, value: retained-flow-env-value}]",
            "---",
            '"pass\\u0077ord": redis-escaped-yaml-secret',
            '"pass\\u0061ge": retained-escaped-yaml-value',
            (
                "apiVersion: v1\ndata:\n"
                "  opaque: redis-kube-yaml-secret\n"
                "kind: Secret\nmetadata:\n  annotations:\n    note: |\n      ---\n"
                "stringData: {config: redis-kube-yaml-inline-secret}\n"
                "---\nkind: Pod\ncredentialName: &credentialName PASSWORD\n"
                "safeName: &safeName SAFE\nspec:\n  env:\n"
                "    - name: PASSWORD\n      value: redis-kube-yaml-env-secret\n"
                "    - value: redis-reversed-yaml-env-secret\n      name: PASSWORD\n"
                "    -\n      value: redis-standalone-yaml-env-secret\n      name: API_KEY\n"
                "    - name: &credential PASSWORD\n      value: redis-decorated-yaml-env-secret\n"
                "    - name: *credentialName\n      value: redis-aliased-yaml-env-secret\n"
                "    - name: *safeName\n      value: retained-aliased-yaml-env-value\n"
                "---\nkind: List\nitems:\n"
                "  - kind: Secret\n    data:\n      opaque: redis-list-kube-secret\n"
                "  - data:\n      opaque: redis-reversed-list-kube-secret\n    kind: Secret\n"
                "  - kind: ConfigMap\n    data:\n      harmless: retained-list-config-value\n"
                "---\nkind: Secret\ndata: {\n  opaque: redis-kube-flow-secret\n}\n"
                "---\nkind: Secret\ndata: &payload\n  opaque: redis-kube-anchor-secret\n"
                "---\nkind: &resourceKind Secret\ndata:\n  opaque: redis-decorated-kind-secret\n"
                "---\nkind: >-\n  Secret\ndata:\n  opaque: redis-block-kind-secret\n"
                "---\nkindValue: &kindValue Secret\nkind: *kindValue\n"
                "data:\n  opaque: redis-aliased-kind-secret\n"
                "---\nkind: |-\n  ConfigMap\ndata:\n"
                "  harmless: retained-block-kind-config-value\n"
                "---\nkindValue: &configKind ConfigMap\nkind: *configKind\n"
                "data:\n  harmless: retained-aliased-config-value\n"
                "---\nkind: ConfigMap\ndata:\n  harmless: retained-config-value"
            ),
            json.dumps({"auths": {"registry": {"auth": docker_auth}}, "debug": True}),
            f"DOCKER_AUTH_CONFIG={{\"auths\":{{\"registry\":{{\"auth\":\"{docker_auth}\"}}}}}}",
            "ordinary suffix",
        ]
    )
    timestamp = "2026-10-05T11:45:00+00:00"
    redis.store[cli_log_latest(SLUG)] = redis_payload
    redis.store[cli_log_history(SLUG, timestamp)] = redis_payload

    event_dir = events_root / SLUG
    event_dir.mkdir(parents=True)
    event_path = event_dir / "2026-10-05.jsonl"
    event_path.write_text(
        json.dumps(
            {
                "event_type": "failure",
                "password": 123456789,
                "credentials": ["user", "disk-list-secret"],
                "env": [{"name": "API_KEY", "value": "disk-name-value-secret"}],
                "message": 'PASSWORD="disk-structured-first\ndisk-structured-second"',
                "netrc": "machine disk.example login alice password disk-netrc-secret",
                "resource": {
                    "kind": "Secret",
                    "data": {"opaque": "disk-kube-data-secret"},
                    "stringData": {"config": "disk-kube-string-secret"},
                },
                "yaml": (
                    "apiVersion: v1\ndata:\n"
                    "  opaque: disk-kube-yaml-secret\n"
                    "kind: Secret\nmetadata:\n  annotations:\n    note: |\n      ---\n"
                    "stringData: {config: disk-kube-yaml-inline-secret}\n"
                    "---\nkind: !resource Secret\ndata:\n  opaque: disk-decorated-kind-secret\n"
                    "---\nkind: Pod\nspec:\n  env:\n"
                    "    - name: PASSWORD\n      value: disk-kube-yaml-env-secret\n"
                    "    - name: !!str PASSWORD\n      value: disk-decorated-yaml-env-secret\n"
                ),
                "auths": {"registry": {"auth": docker_auth}},
                "debug": True,
            }
        )
        + '\n{"password":987650001,}\n',
        encoding="utf-8",
    )
    ci_path = repos_root / SLUG / "artifacts" / "ci.log"
    ci_path.parent.mkdir(parents=True)
    ci_path.write_text(
        json.dumps(
            {
                "password": 123456789,
                "credentials": ["user", "ci-list-secret"],
                "env": [{"name": "CLIENT_SECRET", "value": "ci-name-value-secret"}],
                "message": 'PASSWORD="ci-structured-first\nci-structured-second"',
                "netrc": "machine ci.example login alice password ci-netrc-secret",
                "resource": {
                    "kind": "Secret",
                    "data": {"opaque": "ci-kube-data-secret"},
                    "stringData": {"config": "ci-kube-string-secret"},
                },
                "yaml": (
                    "apiVersion: v1\ndata:\n"
                    "  opaque: ci-structured-kube-yaml-secret\n"
                    "kind: Secret\n---\nkind: Pod\nspec:\n  env:\n"
                    "    - name: PASSWORD\n      value: ci-structured-kube-yaml-env-secret\n"
                ),
                "auths": {"registry": {"auth": docker_auth}},
                "debug": True,
            }
        )
        + '\npassword = """ci-toml-first-secret\nci-toml-second-secret"""\n'
        + "PASSWORD=ci-bare-slash-first-secret\\\nci-bare-slash-second-secret\n"
        + "export PASSWORD=ci-export-slash-first-secret\\\nci-export-slash-second-secret\n"
        + "[env] PASSWORD=ci-env-slash-first-secret\\\nci-env-slash-second-secret\n"
        + "password:\n  ci-yaml-first-secret\n  ci-yaml-second-secret\n"
        + "credentials:\n- ci-sequence-user\n- ci-same-indent-sequence-secret\n"
        + "password =\n  ci-ini-first-secret\n  ci-ini-second-secret\n"
        + "- password:\n    ci-sequence-first-secret\n    ci-sequence-second-secret\n"
        + "DJANGO_SECRET_KEY=ci-django-secret-key-value\n"
        + '{"jwtSecretKey":"ci-jwt-secret-key-value"}\n'
        + '{"data":{"tls.key":"ci-tls-key-value"}}\n'
        + "AccountKey=ci-azure-account-key-value\n"
        + "//registry.example/:_auth=ci-npm-basic-secret\n"
        + "MYSQL_PWD=ci-pwd-secret\n"
        + "Driver=Postgres;UID=alice;PWD=ci-odbc-pwd-secret;Server=db\n"
        + "https://acct.blob.core.windows.net/c?sv=1&sp=r&sig=ci-sas-secret&se=tomorrow\n"
        + "SharedAccessSignature=sv=1&sig=ci-shared-sas-secret\n"
        + "https://example.cloudfront.net/file?Expires=1&Signature=ci-aws-signature&Key-Pair-Id=K1\n"
        + "https://s3.example/file?X-Amz-Signature=ci-amz-signature&X-Amz-Expires=60\n"
        + '{"SharedAccessKey":"ci-azure-shared-key-value"}\n'
        + '{"_auth":"ci-npm-json-secret"}\n'
        + '{"MYSQL_PWD":"ci-pwd-json-secret"}\n'
        + '{"url":"https://acct.blob.core.windows.net/c?sv=1&sig=ci-json-sas-secret"}\n'
        + '{"password":987650002,}\n'
        + '{\n"password":\n"ci-same-indent-secret"\n}\n'
        + "  password ci-multiline-netrc-secret\n"
        + "service.clientSecret ci-java-property-secret with-spaces\n"
        + "service.username retained-ci-java-property-user\n"
        + "docker login -p 'ci docker short secret'\n"
        + "docker login -p\n"
        + "+ Set-Item Env:PASSWORD 'ci powershell positional secret'\n"
        + "+ Set-Item -Value \"ci powershell reversed secret\" -Path 'Env:API_KEY'\n"
        + "+ Set-Item Env:SAFE retained-ci-powershell-safe\n"
        + "+ aws configure set profile.prod.client_secret 'ci aws configure secret'\n"
        + "+ aws configure set region retained-ci-aws-region\n"
        + "mariadb -u root -pci-mysql-short-secret\n"
        + "mariadb -p\n"
        + "<m:password>ci-xml-element-secret</m:password>\n"
        + "<m:password><![CDATA[ci-xml-cdata-secret]]></m:password>\n"
        + "<m:password><value>ci-xml-nested-secret</value></m:password>\n"
        + "<m:password>\nci-xml-multiline-secret\n</m:password>\n"
        + '<m:password\n encoding="plain">ci-xml-multiline-start-secret</m:password>\n'
        + "<m:clientSecret>\n<![CDATA[\nci-xml-multiline-cdata-secret\n]]>\n"
        + "</m:clientSecret>\n"
        + "<m:password/>\n"
        + "<m:password\n/>\nretained-ci-after-split-self-close\n"
        + '<property name="apiKey" value="ci-xml-attribute-secret"/>\n'
        + "<property value='ci-xml-reversed-secret' name='refreshToken'/>\n"
        + "<m:username>retained-ci-xml-element-value</m:username>\n"
        + "<m:username>\nretained-ci-xml-multiline-value\n</m:username>\n"
        + '<m:username\n encoding="plain">retained-ci-xml-multiline-start-value</m:username>\n'
        + '<property name="username" value="retained-ci-xml-attribute-value"/>\n'
        + "apiVersion: v1\ndata:\n"
        + "  opaque: ci-kube-yaml-secret\n"
        + "kind: Secret\nmetadata:\n  annotations:\n    note: |\n      ---\n"
        + "stringData: {config: ci-kube-yaml-inline-secret}\n"
        + "---\nkind: Pod\ncredentialName: &credentialName PASSWORD\n"
        + "safeName: &safeName SAFE\nspec:\n  env:\n"
        + "    - name: PASSWORD\n      value: ci-kube-yaml-env-secret\n"
        + "    - value: ci-reversed-yaml-env-secret\n      name: PASSWORD\n"
        + "    -\n      value: ci-standalone-yaml-env-secret\n      name: API_KEY\n"
        + "    - name: &credential PASSWORD\n      value: ci-decorated-yaml-env-secret\n"
        + "    - name: *credentialName\n      value: ci-aliased-yaml-env-secret\n"
        + "    - name: *safeName\n      value: retained-ci-aliased-yaml-env-value\n"
        + "---\nkind: List\nitems:\n"
        + "  - kind: Secret\n    data:\n      opaque: ci-list-kube-secret\n"
        + "  - data:\n      opaque: ci-reversed-list-kube-secret\n    kind: Secret\n"
        + "  - kind: ConfigMap\n    data:\n      harmless: retained-ci-list-config-value\n"
        + "---\nkind: Secret\ndata: {\n  opaque: ci-kube-flow-secret\n}\n"
        + "---\nkind: Secret\ndata: &payload\n  opaque: ci-kube-anchor-secret\n"
        + "---\nkind: &resourceKind Secret\ndata:\n  opaque: ci-decorated-kind-secret\n"
        + "---\nkind: >-\n  Secret\ndata:\n  opaque: ci-block-kind-secret\n"
        + "---\nkindValue: &kindValue Secret\nkind: *kindValue\n"
        + "data:\n  opaque: ci-aliased-kind-secret\n"
        + "---\nkind: |-\n  ConfigMap\ndata:\n"
        + "  harmless: retained-ci-block-kind-config-value\n"
        + "---\nkindValue: &configKind ConfigMap\nkind: *configKind\n"
        + "data:\n  harmless: retained-ci-aliased-config-value\n"
        + "---\n{apiVersion: v1, kind: Secret, data: {opaque: ci-kube-single-flow-secret}}\n"
        + "{kind: ConfigMap, data: {harmless: retained-ci-single-flow-config}}\n"
        + "{apiVersion: v1,\n kind: Secret,\n"
        + " data: {opaque: ci-kube-multiline-flow-secret}}\n"
        + "env: [{name: PASSWORD, value: ci-flow-env-secret}]\n"
        + "env: [{value: ci-reversed-flow-env-secret, name: API_KEY}]\n"
        + "env: [{name: PASSWORD,\n value: ci-multiline-flow-env-secret}]\n"
        + "---\n"
        + "{apiVersion: v1,\n kind: ConfigMap,\n"
        + " data: {harmless: retained-ci-multiline-flow-config}}\n"
        + "---\n"
        + "env: [{name: SAFE, value: retained-ci-flow-env-value}]\n"
        + '"pass\\u0077ord": ci-escaped-yaml-secret\n'
        + '"pass\\u0077ord": |-\n  ci-escaped-yaml-first\n  ci-escaped-yaml-second\n'
        + '"pass\\u0061ge": retained-ci-escaped-yaml-value\n'
        + "---\nkind: ConfigMap\ndata:\n  harmless: retained-ci-config-value\n"
        + '2026-10-05 INFO {"password":987654322,"debug":true}\n',
        encoding="utf-8",
    )

    async def read_all(source_id: str, *, max_chars: int) -> str:
        cursor: int | str = 0
        parts: list[str] = []
        while True:
            page = await diagnostics.read_orchestrator_log(
                SLUG,
                source_id,
                cursor=cursor,
                max_chars=max_chars,
            )
            parts.append(page["content"])
            cursor = page["pagination"]["next_cursor"]
            if cursor is None:
                return "".join(parts)

    for source_id in ("cli:latest", f"cli:history/{timestamp}"):
        content = await read_all(source_id, max_chars=17)
        assert "ordinary prefix" in content
        assert "ordinary suffix" in content
        assert "fake-first-secret" not in content
        assert "fake-second-secret" not in content
        assert "toml-first-secret" not in content
        assert "toml-second-secret" not in content
        assert "bare-slash-first-secret" not in content
        assert "bare-slash-second-secret" not in content
        assert "export-slash-first-secret" not in content
        assert "export-slash-second-secret" not in content
        assert "env-slash-first-secret" not in content
        assert "env-slash-second-secret" not in content
        assert "yaml-first-secret" not in content
        assert "yaml-second-secret" not in content
        assert "redis-same-indent-sequence-secret" not in content
        assert "redis-ini-first-secret" not in content
        assert "redis-ini-second-secret" not in content
        assert "sequence-first-secret" not in content
        assert "sequence-second-secret" not in content
        assert "987654321" not in content
        assert "django-secret-key-value" not in content
        assert "jwt-secret-key-value" not in content
        assert "redis-tls-key-value" not in content
        assert "redis-azure-account-key-value" not in content
        assert "redis-azure-shared-key-value" not in content
        assert "redis-npm-basic-secret" not in content
        assert "redis-npm-json-secret" not in content
        assert "redis-pwd-secret" not in content
        assert "redis-odbc-pwd-secret" not in content
        assert "redis-pwd-json-secret" not in content
        assert "redis-sas-secret" not in content
        assert "redis-shared-sas-secret" not in content
        assert "redis-json-sas-secret" not in content
        assert "redis-aws-signature" not in content
        assert "redis-amz-signature" not in content
        assert "987650000" not in content
        assert "123456789" not in content
        assert "fake-list-secret" not in content
        assert "redis-name-value-secret" not in content
        assert "redis-structured-first" not in content
        assert "redis-structured-second" not in content
        assert "redis-same-indent-secret" not in content
        assert "redis-netrc-secret" not in content
        assert "redis-java-property-secret" not in content
        assert "retained-java-property-user" in content
        assert "redis-docker-short-secret" not in content
        assert "docker login -p" in content
        assert "redis-powershell-positional-secret" not in content
        assert "redis powershell named secret" not in content
        assert "retained-powershell-safe" in content
        assert "redis-aws-configure-secret" not in content
        assert "retained-aws-region" in content
        assert "redis-mysql-short-secret" not in content
        assert "mysql -p" in content
        assert "redis-xml-element-secret" not in content
        assert "redis-xml-cdata-secret" not in content
        assert "redis-xml-nested-secret" not in content
        assert "redis-xml-multiline-secret" not in content
        assert "redis-xml-multiline-start-secret" not in content
        assert "redis-xml-long-start-secret" not in content
        assert "redis-xml-multiline-cdata-secret" not in content
        assert "<password/>" in content
        assert "retained-after-split-self-close" in content
        assert "redis-xml-attribute-secret" not in content
        assert "redis-xml-reversed-secret" not in content
        assert "retained-xml-element-value" in content
        assert "retained-xml-multiline-value" in content
        assert "retained-xml-multiline-start-value" in content
        assert "retained-xml-attribute-value" in content
        assert "redis-kube-data-secret" not in content
        assert "redis-kube-string-secret" not in content
        assert "redis-kube-yaml-secret" not in content
        assert "redis-kube-yaml-inline-secret" not in content
        assert "redis-kube-yaml-env-secret" not in content
        assert "redis-reversed-yaml-env-secret" not in content
        assert "redis-standalone-yaml-env-secret" not in content
        assert "redis-decorated-yaml-env-secret" not in content
        assert "redis-aliased-yaml-env-secret" not in content
        assert "retained-aliased-yaml-env-value" in content
        assert "redis-list-kube-secret" not in content
        assert "redis-reversed-list-kube-secret" not in content
        assert "redis-kube-flow-secret" not in content
        assert "redis-kube-anchor-secret" not in content
        assert "redis-decorated-kind-secret" not in content
        assert "redis-block-kind-secret" not in content
        assert "redis-aliased-kind-secret" not in content
        assert "retained-block-kind-config-value" in content
        assert "retained-aliased-config-value" in content
        assert "redis-kube-single-flow-secret" not in content
        assert "redis-kube-prefixed-flow-secret" not in content
        assert "redis-kube-multiline-flow-secret" not in content
        assert "redis-flow-env-secret" not in content
        assert "redis-reversed-flow-env-secret" not in content
        assert "redis-multiline-flow-env-secret" not in content
        assert "redis-escaped-yaml-secret" not in content
        assert "retained-list-config-value" in content
        assert "retained-config-value" in content
        assert "retained-single-flow-config" in content
        assert "retained-multiline-flow-config" in content
        assert "retained-flow-env-value" in content
        assert "retained-escaped-yaml-value" in content
        assert docker_auth not in content

    for source_id, list_secret in (
        ("ci:artifact", "ci-list-secret"),
        ("events:disk/2026-10-05", "disk-list-secret"),
    ):
        content = await read_all(source_id, max_chars=23)
        assert "123456789" not in content
        assert list_secret not in content
        assert "ci-name-value-secret" not in content
        assert "disk-name-value-secret" not in content
        assert "ci-structured-first" not in content
        assert "ci-structured-second" not in content
        assert "disk-structured-first" not in content
        assert "disk-structured-second" not in content
        assert "ci-same-indent-secret" not in content
        assert "ci-netrc-secret" not in content
        assert "ci-multiline-netrc-secret" not in content
        assert "ci-java-property-secret" not in content
        assert "with-spaces" not in content
        assert "ci docker short secret" not in content
        assert "ci powershell positional secret" not in content
        assert "ci powershell reversed secret" not in content
        assert "ci aws configure secret" not in content
        assert "ci-mysql-short-secret" not in content
        assert "ci-xml-element-secret" not in content
        assert "ci-xml-cdata-secret" not in content
        assert "ci-xml-nested-secret" not in content
        assert "ci-xml-multiline-secret" not in content
        assert "ci-xml-multiline-start-secret" not in content
        assert "ci-xml-multiline-cdata-secret" not in content
        assert "ci-xml-attribute-secret" not in content
        assert "ci-xml-reversed-secret" not in content
        assert "disk-netrc-secret" not in content
        assert "ci-kube-data-secret" not in content
        assert "ci-kube-string-secret" not in content
        assert "disk-kube-data-secret" not in content
        assert "disk-kube-string-secret" not in content
        assert "ci-structured-kube-yaml-secret" not in content
        assert "ci-kube-yaml-secret" not in content
        assert "ci-kube-yaml-inline-secret" not in content
        assert "disk-kube-yaml-secret" not in content
        assert "disk-kube-yaml-inline-secret" not in content
        assert "ci-structured-kube-yaml-env-secret" not in content
        assert "ci-kube-yaml-env-secret" not in content
        assert "disk-kube-yaml-env-secret" not in content
        assert "disk-decorated-yaml-env-secret" not in content
        assert "disk-decorated-kind-secret" not in content
        assert "ci-reversed-yaml-env-secret" not in content
        assert "ci-standalone-yaml-env-secret" not in content
        assert "ci-decorated-yaml-env-secret" not in content
        assert "ci-aliased-yaml-env-secret" not in content
        assert docker_auth not in content
        assert "ci-toml-first-secret" not in content
        assert "ci-toml-second-secret" not in content
        assert "ci-bare-slash-first-secret" not in content
        assert "ci-bare-slash-second-secret" not in content
        assert "ci-export-slash-first-secret" not in content
        assert "ci-export-slash-second-secret" not in content
        assert "ci-env-slash-first-secret" not in content
        assert "ci-env-slash-second-secret" not in content
        assert "ci-yaml-first-secret" not in content
        assert "ci-yaml-second-secret" not in content
        assert "ci-same-indent-sequence-secret" not in content
        assert "ci-ini-first-secret" not in content
        assert "ci-ini-second-secret" not in content
        assert "ci-sequence-first-secret" not in content
        assert "ci-sequence-second-secret" not in content
        assert "987654322" not in content
        assert "ci-django-secret-key-value" not in content
        assert "ci-jwt-secret-key-value" not in content
        assert "ci-tls-key-value" not in content
        assert "ci-azure-account-key-value" not in content
        assert "ci-azure-shared-key-value" not in content
        assert "ci-npm-basic-secret" not in content
        assert "ci-npm-json-secret" not in content
        assert "ci-pwd-secret" not in content
        assert "ci-odbc-pwd-secret" not in content
        assert "ci-pwd-json-secret" not in content
        assert "ci-sas-secret" not in content
        assert "ci-shared-sas-secret" not in content
        assert "ci-json-sas-secret" not in content
        assert "ci-aws-signature" not in content
        assert "ci-amz-signature" not in content
        assert "987650002" not in content
        assert "987650001" not in content
        assert "debug" in content
        assert "ci-list-kube-secret" not in content
        assert "ci-reversed-list-kube-secret" not in content
        assert "ci-kube-flow-secret" not in content
        assert "ci-kube-anchor-secret" not in content
        assert "ci-decorated-kind-secret" not in content
        assert "ci-block-kind-secret" not in content
        assert "ci-aliased-kind-secret" not in content
        if source_id == "ci:artifact":
            assert "retained-ci-java-property-user" in content
            assert "docker login -p" in content
            assert "retained-ci-powershell-safe" in content
            assert "retained-ci-aws-region" in content
            assert "mariadb -p" in content
            assert "retained-ci-xml-element-value" in content
            assert "retained-ci-xml-multiline-value" in content
            assert "retained-ci-xml-multiline-start-value" in content
            assert "retained-ci-after-split-self-close" in content
            assert "retained-ci-xml-attribute-value" in content
            assert "retained-ci-list-config-value" in content
            assert "ci-kube-single-flow-secret" not in content
            assert "ci-kube-multiline-flow-secret" not in content
            assert "ci-flow-env-secret" not in content
            assert "ci-reversed-flow-env-secret" not in content
            assert "ci-multiline-flow-env-secret" not in content
            assert "ci-escaped-yaml-secret" not in content
            assert "ci-escaped-yaml-first" not in content
            assert "ci-escaped-yaml-second" not in content
            assert "retained-ci-single-flow-config" in content
            assert "retained-ci-multiline-flow-config" in content
            assert "retained-ci-flow-env-value" in content
            assert "retained-ci-escaped-yaml-value" in content
            assert "retained-ci-block-kind-config-value" in content
            assert "retained-ci-aliased-yaml-env-value" in content
            assert "retained-ci-aliased-config-value" in content

    ci_raw = ci_path.read_bytes()
    xml_page = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=ci_raw.index(b"ci-xml-multiline-secret"),
        max_chars=200,
    )
    assert "ci-xml-multiline-secret" not in xml_page["content"]
    assert "SENSITIVE XML ELEMENT" in xml_page["content"]

    xml_start_page = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=ci_raw.index(b"ci-xml-multiline-start-secret"),
        max_chars=200,
    )
    assert "ci-xml-multiline-start-secret" not in xml_start_page["content"]
    assert "SENSITIVE XML ELEMENT" in xml_start_page["content"]

    java_property_page = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=ci_raw.index(b"ci-java-property-secret"),
        max_chars=200,
    )
    assert "ci-java-property-secret" not in java_property_page["content"]
    assert "retained-ci-java-property-user" in java_property_page["content"]

    sequence_page = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=ci_raw.index(b"- ci-same-indent-sequence-secret"),
        max_chars=200,
    )
    assert "ci-same-indent-sequence-secret" not in sequence_page["content"]
    assert "SENSITIVE" in sequence_page["content"]

    ini_page = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=ci_raw.index(b"  ci-ini-second-secret"),
        max_chars=200,
    )
    assert "ci-ini-second-secret" not in ini_page["content"]
    assert "SENSITIVE" in ini_page["content"]

    yaml_secret_page = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=ci_raw.index(b"  opaque: ci-kube-yaml-secret"),
        max_chars=200,
    )
    assert "ci-kube-yaml-secret" not in yaml_secret_page["content"]
    assert "SENSITIVE" in yaml_secret_page["content"]

    nested_yaml_secret_page = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=ci_raw.index(b"      opaque: ci-list-kube-secret"),
        max_chars=300,
    )
    assert "ci-list-kube-secret" not in nested_yaml_secret_page["content"]
    assert "SENSITIVE" in nested_yaml_secret_page["content"]

    reversed_nested_yaml_secret_page = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=ci_raw.index(b"      opaque: ci-reversed-list-kube-secret"),
        max_chars=300,
    )
    assert "ci-reversed-list-kube-secret" not in reversed_nested_yaml_secret_page["content"]
    assert "SENSITIVE" in reversed_nested_yaml_secret_page["content"]

    flow_yaml_secret_page = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=ci_raw.index(b"  opaque: ci-kube-flow-secret"),
        max_chars=300,
    )
    assert "ci-kube-flow-secret" not in flow_yaml_secret_page["content"]
    assert "SENSITIVE" in flow_yaml_secret_page["content"]

    anchored_yaml_secret_page = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=ci_raw.index(b"  opaque: ci-kube-anchor-secret"),
        max_chars=300,
    )
    assert "ci-kube-anchor-secret" not in anchored_yaml_secret_page["content"]
    assert "SENSITIVE" in anchored_yaml_secret_page["content"]

    decorated_kind_page = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=ci_raw.index(b"  opaque: ci-decorated-kind-secret"),
        max_chars=300,
    )
    assert "ci-decorated-kind-secret" not in decorated_kind_page["content"]
    assert "SENSITIVE" in decorated_kind_page["content"]

    block_kind_page = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=ci_raw.index(b"  opaque: ci-block-kind-secret"),
        max_chars=300,
    )
    assert "ci-block-kind-secret" not in block_kind_page["content"]
    assert "SENSITIVE" in block_kind_page["content"]

    escaped_yaml_page = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=ci_raw.index(b"  ci-escaped-yaml-second"),
        max_chars=300,
    )
    assert "ci-escaped-yaml-second" not in escaped_yaml_page["content"]
    assert "SENSITIVE" in escaped_yaml_page["content"]

    decorated_env_page = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=ci_raw.index(b"      value: ci-decorated-yaml-env-secret"),
        max_chars=300,
    )
    assert "ci-decorated-yaml-env-secret" not in decorated_env_page["content"]
    assert "SENSITIVE" in decorated_env_page["content"]

    aliased_kind_page = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=ci_raw.index(b"  opaque: ci-aliased-kind-secret"),
        max_chars=300,
    )
    assert "ci-aliased-kind-secret" not in aliased_kind_page["content"]
    assert "SENSITIVE" in aliased_kind_page["content"]

    aliased_env_page = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=ci_raw.index(b"      value: ci-aliased-yaml-env-secret"),
        max_chars=300,
    )
    assert "ci-aliased-yaml-env-secret" not in aliased_env_page["content"]
    assert "SENSITIVE" in aliased_env_page["content"]

    triple_quote_continuation = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=ci_raw.index(b"ci-toml-second-secret"),
        max_chars=200,
    )
    assert "ci-toml-second-secret" not in triple_quote_continuation["content"]
    assert "[REDACTED SENSITIVE QUOTED SCALAR]" in triple_quote_continuation["content"]

    slash_continuation = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=ci_raw.index(b"ci-bare-slash-second-secret"),
        max_chars=200,
    )
    assert "ci-bare-slash-second-secret" not in slash_continuation["content"]
    assert "[REDACTED SENSITIVE CONTINUATION]" in slash_continuation["content"]

    prefixed_slash_continuation = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=ci_raw.index(b"ci-env-slash-second-secret"),
        max_chars=200,
    )
    assert "ci-env-slash-second-secret" not in prefixed_slash_continuation["content"]
    assert "[REDACTED SENSITIVE CONTINUATION]" in prefixed_slash_continuation["content"]

    yaml_continuation = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=ci_raw.index(b"  ci-yaml-second-secret"),
        max_chars=200,
    )
    assert "ci-yaml-second-secret" not in yaml_continuation["content"]
    assert "[REDACTED SENSITIVE BLOCK]" in yaml_continuation["content"]


async def test_yaml_merge_kinds_are_classified_across_retained_readers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    repos_root = tmp_path / "repos"
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)
    payload = (
        "base: &secretResource\n  kind: Secret\n"
        "<<: *secretResource\ndata:\n  opaque: merged-kind-secret\n"
        "---\nbase: &configResource\n  kind: ConfigMap\n"
        "<<: *configResource\ndata:\n  harmless: retained-merged-kind-config\n"
    )
    redis.store[cli_log_latest(SLUG)] = payload
    ci_path = repos_root / SLUG / "artifacts" / "ci.log"
    ci_path.parent.mkdir(parents=True)
    ci_path.write_text(payload, encoding="utf-8")

    redis_result = await diagnostics.read_orchestrator_log(
        SLUG,
        "cli:latest",
        max_chars=2_000,
    )
    file_result = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        max_chars=2_000,
    )
    for result in (redis_result, file_result):
        assert "merged-kind-secret" not in result["content"]
        assert "retained-merged-kind-config" in result["content"]

    raw = ci_path.read_bytes()
    mid_payload = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=raw.index(b"  opaque: merged-kind-secret"),
        max_chars=300,
    )
    assert "merged-kind-secret" not in mid_payload["content"]
    assert "SENSITIVE" in mid_payload["content"]


async def test_yaml_explicit_keys_are_redacted_across_retained_readers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    repos_root = tmp_path / "repos"
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)
    payload = (
        "? password\n: plain-explicit-secret\n"
        '? "pass\\u0077ord"\n: |\n  escaped-explicit-secret\n'
        "safe: retained-explicit-neighbor\n"
    )
    redis.store[cli_log_latest(SLUG)] = payload
    ci_path = repos_root / SLUG / "artifacts" / "ci.log"
    ci_path.parent.mkdir(parents=True)
    ci_path.write_text(payload, encoding="utf-8")

    redis_result = await diagnostics.read_orchestrator_log(
        SLUG,
        "cli:latest",
        max_chars=2_000,
    )
    file_result = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        max_chars=2_000,
    )
    for result in (redis_result, file_result):
        assert "plain-explicit-secret" not in result["content"]
        assert "escaped-explicit-secret" not in result["content"]
        assert "retained-explicit-neighbor" in result["content"]

    raw = ci_path.read_bytes()
    at_value = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=raw.index(b": plain-explicit-secret"),
        max_chars=2_000,
    )
    inside_block = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=raw.index(b"  escaped-explicit-secret"),
        max_chars=2_000,
    )
    assert "plain-explicit-secret" not in at_value["content"]
    assert "escaped-explicit-secret" not in inside_block["content"]
    assert "retained-explicit-neighbor" in inside_block["content"]


async def test_yaml_alias_keys_are_redacted_across_retained_readers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    repos_root = tmp_path / "repos"
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)
    payload = (
        "unused: &field password\n"
        "*field: alias-key-secret\n"
        "blockName: &blockName api_token\n"
        "*blockName: |\n  alias-key-block-secret\n"
        "safeName: &safeName harmless\n"
        "*safeName: retained-safe-alias-value\n"
    )
    redis.store[cli_log_latest(SLUG)] = payload
    ci_path = repos_root / SLUG / "artifacts" / "ci.log"
    ci_path.parent.mkdir(parents=True)
    ci_path.write_text(payload, encoding="utf-8")

    for source_id in ("cli:latest", "ci:artifact"):
        result = await diagnostics.read_orchestrator_log(
            SLUG,
            source_id,
            max_chars=2_000,
        )
        assert "alias-key-secret" not in result["content"]
        assert "alias-key-block-secret" not in result["content"]
        assert "retained-safe-alias-value" in result["content"]

    raw = ci_path.read_bytes()
    inside_block = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=raw.index(b"  alias-key-block-secret"),
        max_chars=2_000,
    )
    assert "alias-key-block-secret" not in inside_block["content"]


async def test_resolved_secret_payload_keys_are_redacted_across_retained_readers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    repos_root = tmp_path / "repos"
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)
    payload = (
        "dataKey: &payload data\nkind: Secret\n"
        "*payload: {opaque: alias-payload-secret}\n"
        "---\nkind: Secret\n? data\n:\n  opaque: explicit-payload-secret\n"
        '---\nkind: Secret\n"da\\u0074a":\n  opaque: escaped-payload-secret\n'
        "  second: escaped-payload-secret-two\n"
        '---\nkind: ConfigMap\n"da\\u0074a":\n  safe: retained-config-payload\n'
    )
    redis.store[cli_log_latest(SLUG)] = payload
    ci_path = repos_root / SLUG / "artifacts" / "ci.log"
    ci_path.parent.mkdir(parents=True)
    ci_path.write_text(payload, encoding="utf-8")

    for source_id in ("cli:latest", "ci:artifact"):
        result = await diagnostics.read_orchestrator_log(
            SLUG,
            source_id,
            max_chars=4_000,
        )
        assert "alias-payload-secret" not in result["content"]
        assert "explicit-payload-secret" not in result["content"]
        assert "escaped-payload-secret" not in result["content"]
        assert "escaped-payload-secret-two" not in result["content"]
        assert "retained-config-payload" in result["content"]

    raw = ci_path.read_bytes()
    alias_page = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=raw.index(b"*payload: {opaque: alias-payload-secret}"),
        max_chars=4_000,
    )
    assert "alias-payload-secret" not in alias_page["content"]


async def test_explicit_secret_kind_ignores_kind_text_inside_block_scalars(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    repos_root = tmp_path / "repos"
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)
    payload = (
        "? kind\n: Secret\nmetadata:\n  annotations:\n    note: |\n"
        "      kind: ConfigMap\ndata:\n  opaque: explicit-kind-secret\n"
        "---\n? kind\n: ConfigMap\nmetadata:\n  annotations:\n    note: |\n"
        "      kind: Secret\ndata:\n  harmless: retained-explicit-kind-config\n"
    )
    redis.store[cli_log_latest(SLUG)] = payload
    ci_path = repos_root / SLUG / "artifacts" / "ci.log"
    ci_path.parent.mkdir(parents=True)
    ci_path.write_text(payload, encoding="utf-8")

    for source_id in ("cli:latest", "ci:artifact"):
        result = await diagnostics.read_orchestrator_log(
            SLUG,
            source_id,
            max_chars=2_000,
        )
        assert "explicit-kind-secret" not in result["content"]
        assert "retained-explicit-kind-config" in result["content"]

    raw = ci_path.read_bytes()
    inside_payload = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=raw.index(b"  opaque: explicit-kind-secret"),
        max_chars=2_000,
    )
    assert "explicit-kind-secret" not in inside_payload["content"]
    assert "retained-explicit-kind-config" in inside_payload["content"]


async def test_decorated_yaml_mapping_keys_are_redacted_across_retained_readers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    repos_root = tmp_path / "repos"
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)
    payload = (
        "kind: Secret\n!!str data:\n  opaque: tagged-payload-key-secret\n"
        "---\nkind: Secret\n&payloadKey stringData:\n"
        "  config: anchored-payload-key-secret\n"
        "---\n!!str kind: Secret\nmetadata:\n  annotations:\n    kind: Note\n"
        "data:\n  opaque: tagged-kind-key-secret\n"
        "---\n&kindKey kind: Secret\nmetadata:\n  annotations:\n    kind: Note\n"
        "stringData:\n  config: anchored-kind-key-secret\n"
        "---\nkind: ConfigMap\n!!str data:\n"
        "  harmless: retained-decorated-config\n"
        "---\n!!str kind: ConfigMap\nmetadata:\n  annotations:\n    kind: Note\n"
        "data:\n  harmless: retained-tagged-kind-config\n"
        "---\nkind: Pod\nenv:\n"
        "  - name: PASSWORD\n    !!str value: tagged-value-field-secret\n"
        "  - !!str name: API_KEY\n    &valueKey value: anchored-value-field-secret\n"
        "  - name: SAFE\n    !!str value: retained-decorated-env\n"
    )
    redis.store[cli_log_latest(SLUG)] = payload
    ci_path = repos_root / SLUG / "artifacts" / "ci.log"
    ci_path.parent.mkdir(parents=True)
    ci_path.write_text(payload, encoding="utf-8")

    for source_id in ("cli:latest", "ci:artifact"):
        result = await diagnostics.read_orchestrator_log(
            SLUG,
            source_id,
            max_chars=4_000,
        )
        assert "tagged-payload-key-secret" not in result["content"]
        assert "anchored-payload-key-secret" not in result["content"]
        assert "tagged-kind-key-secret" not in result["content"]
        assert "anchored-kind-key-secret" not in result["content"]
        assert "tagged-value-field-secret" not in result["content"]
        assert "anchored-value-field-secret" not in result["content"]
        assert "retained-decorated-config" in result["content"]
        assert "retained-tagged-kind-config" in result["content"]
        assert "retained-decorated-env" in result["content"]

    raw = ci_path.read_bytes()
    for excerpt in (
        b"  opaque: tagged-payload-key-secret",
        b"  opaque: tagged-kind-key-secret",
        b"    !!str value: tagged-value-field-secret",
    ):
        page = await diagnostics.read_orchestrator_log(
            SLUG,
            "ci:artifact",
            cursor=raw.index(excerpt),
            max_chars=4_000,
        )
        assert "tagged-payload-key-secret" not in page["content"]
        assert "tagged-kind-key-secret" not in page["content"]
        assert "tagged-value-field-secret" not in page["content"]


async def test_tagged_flow_yaml_is_redacted_across_retained_readers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    repos_root = tmp_path / "repos"
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)
    payload = (
        "env: [{name: PASSWORD, value: !plain tagged-flow-env-secret}]\n"
        "---\n{kind: Secret, data: {opaque: !plain tagged-flow-payload-secret}}\n"
        "---\n{password: !plain tagged-flow-assignment-secret}\n"
        "---\nenv: [{name: SAFE, value: !plain retained-safe-tagged-flow}]\n"
    )
    redis.store[cli_log_latest(SLUG)] = payload
    ci_path = repos_root / SLUG / "artifacts" / "ci.log"
    ci_path.parent.mkdir(parents=True)
    ci_path.write_text(payload, encoding="utf-8")

    for source_id in ("cli:latest", "ci:artifact"):
        result = await diagnostics.read_orchestrator_log(
            SLUG,
            source_id,
            max_chars=4_000,
        )
        assert "tagged-flow-env-secret" not in result["content"]
        assert "tagged-flow-payload-secret" not in result["content"]
        assert "tagged-flow-assignment-secret" not in result["content"]
        assert "retained-safe-tagged-flow" in result["content"]


async def test_dense_logs_with_aliased_yaml_keys_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    repos_root = tmp_path / "repos"
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)
    payload = (
        "\n" * (diagnostics._MAX_REDACTION_PHYSICAL_LINES + 1)
        + "- &kind_key kind\n"
        + "- &payload_key data\n"
        + "*kind_key: Secret\n"
        + "*payload_key:\n"
        + "  opaque: dense-alias-secret\n"
    )
    redis.store[cli_log_latest(SLUG)] = payload
    ci_path = repos_root / SLUG / "artifacts" / "ci.log"
    ci_path.parent.mkdir(parents=True)
    ci_path.write_text(payload, encoding="utf-8")

    for source_id in ("cli:latest", "ci:artifact"):
        result = await diagnostics.read_orchestrator_log(
            SLUG,
            source_id,
            max_chars=20_000,
        )
        assert "dense-alias-secret" not in result["content"]
        assert result["content"] == "[CONTENT OMITTED: PHYSICAL LINE BOUND EXCEEDED]\n"
        assert any("Physical line count" in warning for warning in result["warnings"])


async def test_multiline_explicit_yaml_keys_are_redacted_across_retained_readers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    repos_root = tmp_path / "repos"
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)
    payload = (
        "? >-\n  password\n: multiline-credential-secret\n"
        "safe: retained-after-multiline-credential\n"
        "---\n? !!str password\n: tagged-explicit-credential-secret\n"
        "safe: retained-after-tagged-explicit-credential\n"
        "---\n? &credentialKey password\n: anchored-explicit-credential-secret\n"
        "---\nkind: Secret\n? >-\n  data\n:\n"
        "  opaque: multiline-payload-key-secret\n"
        "---\nkind: Pod\nenv:\n"
        "  - name: PASSWORD\n    ? >-\n      value\n"
        "    : multiline-env-key-secret\n"
        "  - ? !!str name\n    : PASSWORD\n"
        "    ? &valueKey value\n    : decorated-explicit-env-secret\n"
        "  - name: SAFE\n    value: retained-after-multiline-env\n"
        "  - ? !!str name\n    : SAFE\n"
        "    ? !!str value\n    : retained-after-decorated-explicit-env\n"
    )
    redis.store[cli_log_latest(SLUG)] = payload
    ci_path = repos_root / SLUG / "artifacts" / "ci.log"
    ci_path.parent.mkdir(parents=True)
    ci_path.write_text(payload, encoding="utf-8")

    for source_id in ("cli:latest", "ci:artifact"):
        result = await diagnostics.read_orchestrator_log(
            SLUG,
            source_id,
            max_chars=4_000,
        )
        assert "multiline-credential-secret" not in result["content"]
        assert "tagged-explicit-credential-secret" not in result["content"]
        assert "anchored-explicit-credential-secret" not in result["content"]
        assert "multiline-payload-key-secret" not in result["content"]
        assert "multiline-env-key-secret" not in result["content"]
        assert "decorated-explicit-env-secret" not in result["content"]
        assert "retained-after-multiline-credential" in result["content"]
        assert "retained-after-tagged-explicit-credential" in result["content"]
        assert "retained-after-multiline-env" in result["content"]
        assert "retained-after-decorated-explicit-env" in result["content"]

    raw = ci_path.read_bytes()
    for excerpt, secret in (
        (b"  password", "multiline-credential-secret"),
        (b": multiline-credential-secret", "multiline-credential-secret"),
        (b"? !!str password", "tagged-explicit-credential-secret"),
        (b": anchored-explicit-credential-secret", "anchored-explicit-credential-secret"),
        (b"  opaque: multiline-payload-key-secret", "multiline-payload-key-secret"),
        (b"    : multiline-env-key-secret", "multiline-env-key-secret"),
        (b"    : decorated-explicit-env-secret", "decorated-explicit-env-secret"),
    ):
        page = await diagnostics.read_orchestrator_log(
            SLUG,
            "ci:artifact",
            cursor=raw.index(excerpt),
            max_chars=4_000,
        )
        assert secret not in page["content"]


async def test_indirect_yaml_env_fields_are_redacted_across_retained_readers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    repos_root = tmp_path / "repos"
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)
    payload = (
        "fieldName: &fieldName name\nvalueField: &valueField value\n"
        "sharedValue: &sharedValue anchored-env-secret\n"
        "safeSharedValue: &safeSharedValue retained-safe-anchored-env\n"
        "credentialEnv: &credentialEnv\n"
        "  name: CLIENT_SECRET\n  value: direct-composed-env-secret\n"
        "nameOnly: &nameOnly\n  name: PASSWORD\n"
        "valueOnly: &valueOnly\n  value: split-merged-env-secret\n"
        "safeEnv: &safeEnv\n  name: SAFE\n  value: retained-safe-merged-env\n"
        "env:\n"
        "  - *fieldName: PASSWORD\n    *valueField: aliased-field-env-secret\n"
        "  - name: PASSWORD\n    value: *sharedValue\n"
        "  - name: SAFE\n    value: *safeSharedValue\n"
        "  - <<: *credentialEnv\n"
        "  - <<: [*nameOnly, *valueOnly]\n"
        "  - <<: *safeEnv\n"
        "  - name: >-\n      PASSWORD\n    value: block-name-env-secret\n"
        "  - !!str name: >-\n      API_KEY\n    value: tagged-block-name-env-secret\n"
        '  - "na\\u006de": >-\n      PASSWORD\n    value: escaped-block-name-env-secret\n'
        "  - name: |-\n      SAFE\n    value: retained-safe-block-name-env\n"
    )
    redis.store[cli_log_latest(SLUG)] = payload
    ci_path = repos_root / SLUG / "artifacts" / "ci.log"
    ci_path.parent.mkdir(parents=True)
    ci_path.write_text(payload, encoding="utf-8")

    for source_id in ("cli:latest", "ci:artifact"):
        result = await diagnostics.read_orchestrator_log(
            SLUG,
            source_id,
            max_chars=4_000,
        )
        assert "direct-composed-env-secret" not in result["content"]
        assert "split-merged-env-secret" not in result["content"]
        assert "aliased-field-env-secret" not in result["content"]
        assert "anchored-env-secret" not in result["content"]
        assert "block-name-env-secret" not in result["content"]
        assert "tagged-block-name-env-secret" not in result["content"]
        assert "escaped-block-name-env-secret" not in result["content"]
        assert "retained-safe-merged-env" in result["content"]
        assert "retained-safe-anchored-env" in result["content"]
        assert "retained-safe-block-name-env" in result["content"]

    raw = ci_path.read_bytes()
    for secret in (
        b"direct-composed-env-secret",
        b"aliased-field-env-secret",
        b"anchored-env-secret",
        b"block-name-env-secret",
        b"tagged-block-name-env-secret",
        b"escaped-block-name-env-secret",
    ):
        page = await diagnostics.read_orchestrator_log(
            SLUG,
            "ci:artifact",
            cursor=raw.index(secret),
            max_chars=4_000,
        )
        assert secret.decode() not in page["content"]


async def test_aliased_sensitive_value_definitions_are_redacted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    repos_root = tmp_path / "repos"
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)
    payload = (
        "sharedEnv: &sharedEnv standalone-env-anchor-secret\n"
        "env:\n  - name: PASSWORD\n    value: *sharedEnv\n"
        "---\nsharedPayload: &sharedPayload standalone-payload-anchor-secret\n"
        "kind: Secret\ndata:\n  opaque: *sharedPayload\n"
        "---\nflowShared: &flowShared standalone-flow-env-anchor-secret\n"
        "env: [{name: PASSWORD, value: *flowShared}]\n"
        "---\nsafeShared: &safeShared retained-safe-anchor-value\n"
        "kind: ConfigMap\ndata:\n  harmless: *safeShared\n"
        "---\nsafeFlowShared: &safeFlowShared retained-safe-flow-anchor-value\n"
        "env: [{name: SAFE, value: *safeFlowShared}]\n"
    )
    redis.store[cli_log_latest(SLUG)] = payload
    ci_path = repos_root / SLUG / "artifacts" / "ci.log"
    ci_path.parent.mkdir(parents=True)
    ci_path.write_text(payload, encoding="utf-8")

    for source_id in ("cli:latest", "ci:artifact"):
        result = await diagnostics.read_orchestrator_log(
            SLUG,
            source_id,
            max_chars=4_000,
        )
        assert "standalone-env-anchor-secret" not in result["content"]
        assert "standalone-payload-anchor-secret" not in result["content"]
        assert "standalone-flow-env-anchor-secret" not in result["content"]
        assert "retained-safe-anchor-value" in result["content"]
        assert "retained-safe-flow-anchor-value" in result["content"]

    raw = ci_path.read_bytes()
    for secret in (
        b"standalone-env-anchor-secret",
        b"standalone-payload-anchor-secret",
        b"standalone-flow-env-anchor-secret",
    ):
        page = await diagnostics.read_orchestrator_log(
            SLUG,
            "ci:artifact",
            cursor=raw.index(secret),
            max_chars=4_000,
        )
        assert secret.decode() not in page["content"]


async def test_shell_append_assignments_are_redacted_across_retained_readers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    repos_root = tmp_path / "repos"
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)
    payload = (
        "PASSWORD+=plain-append-secret\n"
        "+ PASSWORD+=xtrace-append-secret\n"
        'PASSWORD+="quoted-append-secret"\n'
        "+ PASSWORD+=continued-append-secret\\\n"
        "continued-append-secret-two\n"
        "set -gx PASSWORD fish-assignment-secret\n"
        "+ set --global --export API_KEY traced-fish-assignment-secret\n"
        "+ set -gx CLIENT_SECRET continued-fish-assignment-secret\\\n"
        "continued-fish-assignment-secret-two\n"
        "SAFE+=retained-safe-append\n"
        "set -gx SAFE retained-safe-fish-assignment\n"
    )
    redis.store[cli_log_latest(SLUG)] = payload
    ci_path = repos_root / SLUG / "artifacts" / "ci.log"
    ci_path.parent.mkdir(parents=True)
    ci_path.write_text(payload, encoding="utf-8")

    for source_id in ("cli:latest", "ci:artifact"):
        result = await diagnostics.read_orchestrator_log(
            SLUG,
            source_id,
            max_chars=4_000,
        )
        assert "plain-append-secret" not in result["content"]
        assert "xtrace-append-secret" not in result["content"]
        assert "quoted-append-secret" not in result["content"]
        assert "continued-append-secret" not in result["content"]
        assert "continued-append-secret-two" not in result["content"]
        assert "fish-assignment-secret" not in result["content"]
        assert "traced-fish-assignment-secret" not in result["content"]
        assert "continued-fish-assignment-secret" not in result["content"]
        assert "continued-fish-assignment-secret-two" not in result["content"]
        assert "retained-safe-append" in result["content"]
        assert "retained-safe-fish-assignment" in result["content"]

    raw = ci_path.read_bytes()
    continuation = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=raw.index(b"continued-append-secret-two"),
        max_chars=4_000,
    )
    assert "continued-append-secret-two" not in continuation["content"]
    assert "retained-safe-append" in continuation["content"]

    fish_continuation = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=raw.index(b"continued-fish-assignment-secret-two"),
        max_chars=4_000,
    )
    assert "continued-fish-assignment-secret-two" not in fish_continuation["content"]
    assert "retained-safe-fish-assignment" in fish_continuation["content"]


async def test_dockerfile_env_word_assignments_are_redacted_across_retained_readers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    repos_root = tmp_path / "repos"
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)
    payload = (
        "ENV PASSWORD docker-env-secret\n"
        'env API_KEY "quoted-docker-env-secret"\n'
        "ENV CLIENT_SECRET continued-docker-env-secret\\\n"
        "continued-docker-env-secret-two\n"
        "ENV SAFE retained-safe-docker-env\n"
    )
    redis.store[cli_log_latest(SLUG)] = payload
    ci_path = repos_root / SLUG / "artifacts" / "ci.log"
    ci_path.parent.mkdir(parents=True)
    ci_path.write_text(payload, encoding="utf-8")

    for source_id in ("cli:latest", "ci:artifact"):
        result = await diagnostics.read_orchestrator_log(
            SLUG,
            source_id,
            max_chars=4_000,
        )
        assert "docker-env-secret" not in result["content"]
        assert "quoted-docker-env-secret" not in result["content"]
        assert "continued-docker-env-secret" not in result["content"]
        assert "continued-docker-env-secret-two" not in result["content"]
        assert "ENV SAFE retained-safe-docker-env" in result["content"]

    raw = ci_path.read_bytes()
    continuation = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=raw.index(b"continued-docker-env-secret-two"),
        max_chars=4_000,
    )
    assert "continued-docker-env-secret-two" not in continuation["content"]
    assert "ENV SAFE retained-safe-docker-env" in continuation["content"]


async def test_delimited_pass_credentials_are_redacted_without_broad_suffix_matching(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    repos_root = tmp_path / "repos"
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)
    payload = (
        "PASS=exact-pass-secret\n"
        "DB_PASS=plain-pass-secret\n"
        "export CACHE.PASS=exported-pass-secret\n"
        "ENV SERVICE-PASS docker-pass-secret\n"
        "env:\n"
        "  - name: API_PASS\n"
        "    value: yaml-pass-secret\n"
        '{"BUILD_PASS":"json-pass-secret","COMPASS":"retained-compass"}\n'
        "COMPASS=retained-compass-assignment\n"
        "BYPASS=retained-bypass-assignment\n"
        "TRESPASS=retained-trespass-assignment\n"
    )
    redis.store[cli_log_latest(SLUG)] = payload
    ci_path = repos_root / SLUG / "artifacts" / "ci.log"
    ci_path.parent.mkdir(parents=True)
    ci_path.write_text(payload, encoding="utf-8")

    for source_id in ("cli:latest", "ci:artifact"):
        result = await diagnostics.read_orchestrator_log(
            SLUG,
            source_id,
            max_chars=4_000,
        )
        assert "exact-pass-secret" not in result["content"]
        assert "plain-pass-secret" not in result["content"]
        assert "exported-pass-secret" not in result["content"]
        assert "docker-pass-secret" not in result["content"]
        assert "yaml-pass-secret" not in result["content"]
        assert "json-pass-secret" not in result["content"]
        assert "retained-compass" in result["content"]
        assert "retained-compass-assignment" in result["content"]
        assert "retained-bypass-assignment" in result["content"]
        assert "retained-trespass-assignment" in result["content"]


async def test_sensitive_heredoc_bodies_are_redacted_across_retained_readers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    repos_root = tmp_path / "repos"
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)
    payload = (
        "password = <<EOT\n"
        "hcl-heredoc-secret\n"
        "EOT\n"
        "token = <<-TOKEN\n"
        "  indented-heredoc-secret\n"
        "  TOKEN\n"
        "PASSWORD=$(cat <<'EOF'\n"
        "shell-heredoc-secret\n"
        "EOF\n"
        ")\n"
        "SAFE=retained-safe-after-heredoc\n"
    )
    redis.store[cli_log_latest(SLUG)] = payload
    ci_path = repos_root / SLUG / "artifacts" / "ci.log"
    ci_path.parent.mkdir(parents=True)
    ci_path.write_text(payload, encoding="utf-8")

    for source_id in ("cli:latest", "ci:artifact"):
        result = await diagnostics.read_orchestrator_log(
            SLUG,
            source_id,
            max_chars=4_000,
        )
        assert "hcl-heredoc-secret" not in result["content"]
        assert "indented-heredoc-secret" not in result["content"]
        assert "shell-heredoc-secret" not in result["content"]
        assert "SAFE=retained-safe-after-heredoc" in result["content"]

    raw = ci_path.read_bytes()
    for secret in (
        b"hcl-heredoc-secret",
        b"indented-heredoc-secret",
        b"shell-heredoc-secret",
    ):
        page = await diagnostics.read_orchestrator_log(
            SLUG,
            "ci:artifact",
            cursor=raw.index(secret),
            max_chars=4_000,
        )
        assert secret.decode() not in page["content"]
        assert "SAFE=retained-safe-after-heredoc" in page["content"]

    warnings: list[str] = []
    crossed = diagnostics._redacted_file_units(
        b"password = <<EOT\ncrossed-heredoc-secret\n",
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=True,
        warnings=warnings,
    )
    assert "crossed-heredoc-secret" not in crossed[0][1]
    assert any("heredoc crossed" in warning for warning in warnings)

    warnings = []
    inside_crossed = diagnostics._redacted_file_units(
        b"inside-crossed-heredoc-secret\n",
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=True,
        sensitive_quote=diagnostics._heredoc_state(("EOT", False)),
        has_more_after_raw=True,
        warnings=warnings,
    )
    assert "inside-crossed-heredoc-secret" not in inside_crossed[0][1]
    assert any("heredoc crossed" in warning for warning in warnings)

    warnings = []
    unrecognized = diagnostics._redacted_file_units(
        b"password = <<$BAD\nunrecognized-heredoc-secret\n",
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=False,
        warnings=warnings,
    )
    assert "unrecognized-heredoc-secret" not in unrecognized[0][1]
    assert any("no recognized terminator" in warning for warning in warnings)

    warnings = []
    dense = diagnostics._redacted_file_units(
        (
            b"\n" * (diagnostics._MAX_REDACTION_PHYSICAL_LINES + 1)
            + b"password = <<EOT\ndense-heredoc-secret\nEOT\n"
        ),
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=False,
        warnings=warnings,
    )
    assert dense[0][1] == "[CONTENT OMITTED: PHYSICAL LINE BOUND EXCEEDED]\n"


async def test_sensitive_hcl_blocks_are_redacted_across_retained_readers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    repos_root = tmp_path / "repos"
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)
    payload = (
        'variable "database_password" { default = "inline-hcl-secret" }\n'
        'variable "escaped_pass\\u0077ord" {\n'
        '  default = "escaped-label-hcl-secret"\n'
        '}\n'
        'variable "heredoc_password" {\n'
        '  default = <<EOF\n'
        '  } ignored-heredoc-brace\n'
        '  hcl-heredoc-after-brace-secret\n'
        'EOF\n'
        '}\n'
        'output "api_token" {\n'
        '  /* multiline-comment-start\n'
        '  } ignored-comment-brace\n'
        '  */\n'
        '  # } ignored-hash-comment-brace\n'
        '  // } ignored-slash-comment-brace\n'
        '  value = { nested = "nested-hcl-secret" }\n'
        '  description = "} quoted-brace-secondary-secret"\n'
        '}\n'
        'variable "pass\\qword" { default = "malformed-label-hcl-secret" }\n'
        'variable "region" {\n'
        '  default = "retained-safe-hcl-region"\n'
        '}\n'
    )
    redis.store[cli_log_latest(SLUG)] = payload
    ci_path = repos_root / SLUG / "artifacts" / "ci.log"
    ci_path.parent.mkdir(parents=True)
    ci_path.write_text(payload, encoding="utf-8")

    for source_id in ("cli:latest", "ci:artifact"):
        result = await diagnostics.read_orchestrator_log(
            SLUG,
            source_id,
            max_chars=4_000,
        )
        assert "inline-hcl-secret" not in result["content"]
        assert "escaped-label-hcl-secret" not in result["content"]
        assert "hcl-heredoc-after-brace-secret" not in result["content"]
        assert "nested-hcl-secret" not in result["content"]
        assert "quoted-brace-secondary-secret" not in result["content"]
        assert "malformed-label-hcl-secret" not in result["content"]
        assert "retained-safe-hcl-region" in result["content"]

    raw = ci_path.read_bytes()
    for secret in (
        b"escaped-label-hcl-secret",
        b"hcl-heredoc-after-brace-secret",
        b"nested-hcl-secret",
    ):
        page = await diagnostics.read_orchestrator_log(
            SLUG,
            "ci:artifact",
            cursor=raw.index(secret),
            max_chars=4_000,
        )
        assert secret.decode() not in page["content"]
        assert "retained-safe-hcl-region" in page["content"]

    warnings: list[str] = []
    crossed = diagnostics._redacted_file_units(
        b'variable "database_password" {\ndefault = "crossed-hcl-secret"\n',
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=True,
        warnings=warnings,
    )
    assert "crossed-hcl-secret" not in crossed[0][1]
    assert any("HCL block crossed" in warning for warning in warnings)

    warnings = []
    inside_crossed = diagnostics._redacted_file_units(
        b'default = "inside-crossed-hcl-secret"\n',
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=True,
        sensitive_block_indent=diagnostics._encode_hcl_block_state(1, False),
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=True,
        warnings=warnings,
    )
    assert "inside-crossed-hcl-secret" not in inside_crossed[0][1]
    assert any("HCL block crossed" in warning for warning in warnings)

    dense = diagnostics._redacted_file_units(
        (
            b"\n" * (diagnostics._MAX_REDACTION_PHYSICAL_LINES + 1)
            + b'variable "database_password" { default = "dense-hcl-secret" }\n'
        ),
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=False,
        warnings=[],
    )
    assert dense[0][1] == "[CONTENT OMITTED: PHYSICAL LINE BOUND EXCEEDED]\n"
    assert diagnostics._hcl_brace_delta("default = <<$BAD") == (
        0,
        False,
        ("", False),
    )


async def test_truncated_redis_logs_omit_unknown_leading_sensitive_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    timestamp = "2026-10-05T11:45:00+00:00"
    truncated = (
        "[truncated]\n"
        "unknown-private-material\n"
        "-----END PRIVATE KEY-----\n"
        "unknown-multiline-secret\n"
    )
    redis.store[cli_log_latest(SLUG)] = truncated
    redis.store[cli_log_history(SLUG, timestamp)] = truncated

    for source_id in ("cli:latest", f"cli:history/{timestamp}"):
        result = await diagnostics.read_orchestrator_log(SLUG, source_id, max_chars=1_000)
        assert result["content"].startswith("[truncated]\n")
        assert "unknown-private-material" not in result["content"]
        assert "unknown-multiline-secret" not in result["content"]
        assert "[CONTENT OMITTED: PRIVATE-KEY CONTEXT UNKNOWN]" in result["content"]
        assert any("omitted fail-closed" in warning for warning in result["warnings"])


async def test_custom_event_root_is_used_for_discovery_and_exact_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    custom_root = tmp_path / "selected-event-root"
    monkeypatch.setenv("PO_EVENTS_DIR", str(custom_root))
    event_path = custom_root / SLUG / "2026-10-05.jsonl"
    event_path.parent.mkdir(parents=True)
    event_path.write_text('{"event_type":"custom-root"}\n', encoding="utf-8")
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", tmp_path / "repos")

    listed = await diagnostics.list_orchestrator_logs(SLUG)
    assert any(source["source_id"] == "events:disk/2026-10-05" for source in listed["sources"])
    read = await diagnostics.read_orchestrator_log(SLUG, "events:disk/2026-10-05")
    assert "custom-root" in read["content"]


async def test_redis_client_construction_failure_preserves_filesystem_discovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.mcp.tools import diagnostics

    config = _config(_repo())
    mapping = {SLUG: config.repositories[0]}
    monkeypatch.setattr(diagnostics, "_configured_repositories", lambda: (config, mapping))
    monkeypatch.setattr(diagnostics, "_utc_now", lambda: NOW)
    monkeypatch.setattr(
        diagnostics,
        "_new_redis_client",
        lambda: (_ for _ in ()).throw(ValueError("invalid REDIS_URL password=redis-url-secret")),
    )
    events_root = tmp_path / "events"
    monkeypatch.setenv("PO_EVENTS_DIR", str(events_root))
    event_path = events_root / SLUG / "2026-10-05.jsonl"
    event_path.parent.mkdir(parents=True)
    event_path.write_text('{"event_type":"disk-available"}\n', encoding="utf-8")
    repos_root = tmp_path / "repos"
    ci_path = repos_root / SLUG / "artifacts" / "ci.log"
    ci_path.parent.mkdir(parents=True)
    ci_path.write_text("ci available\n", encoding="utf-8")
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)

    listed = await diagnostics.list_orchestrator_logs(SLUG)

    sources = {source["source_id"]: source for source in listed["sources"]}
    assert sources["redis:diagnostics"]["availability"] == "unavailable"
    assert sources["events:disk/2026-10-05"]["availability"] == "available"
    assert sources["ci:artifact"]["availability"] == "available"
    assert "redis-url-secret" not in json.dumps(listed)

    history_cursor = diagnostics._encode_history_cursor(0, [], started=False)
    history = await diagnostics.list_orchestrator_logs(SLUG, cursor=history_cursor)
    assert history["pagination"]["phase"] == "redis_history"
    assert history["pagination"]["next_cursor"] is None
    assert history["sources"][0]["source_id"] == "redis:diagnostics"
    assert "redis-url-secret" not in json.dumps(history)


async def test_missing_expired_unretained_and_unavailable_logs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", tmp_path / "repos")
    monkeypatch.setenv("PO_EVENTS_DIR", str(tmp_path / "events"))

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

    oversized_value = "x" * (diagnostics._MAX_REDIS_CLI_LOG_BYTES + 1)
    latest_key = cli_log_latest(SLUG)
    redis.store[latest_key] = oversized_value
    redis.ttls[latest_key] = 60
    oversized_cli = await diagnostics.read_orchestrator_log(SLUG, "cli:latest")
    assert oversized_cli["source"]["availability"] == "oversized"
    assert oversized_cli["source"]["size_bytes"] == len(oversized_value)
    assert oversized_cli["source"]["read_bound_bytes"] == diagnostics._MAX_REDIS_CLI_LOG_BYTES
    assert oversized_cli["content"] == ""
    listed, warnings = await diagnostics._redis_log_sources(redis, SLUG, NOW)
    latest_source = next(source for source in listed if source["source_id"] == "cli:latest")
    assert latest_source["availability"] == "oversized"
    assert warnings

    timestamp = "2026-10-05T10:00:00+00:00"
    history_key = cli_log_history(SLUG, timestamp)
    redis.store[history_key] = oversized_value
    redis.ttls[history_key] = 60
    oversized_history = await diagnostics.read_orchestrator_log(
        SLUG, f"cli:history/{timestamp}"
    )
    assert oversized_history["source"]["availability"] == "oversized"
    history_sources, warnings, _ = await diagnostics._redis_history_page(
        redis,
        SLUG,
        NOW,
        cursor_state={"scan_cursor": 0, "pending": [timestamp], "started": True},
        limit=1,
    )
    assert history_sources[0]["availability"] == "oversized"
    assert warnings
    assert not any(
        operation == "get" and key in {latest_key, history_key}
        for operation, key in redis.calls
    )
    assert all(
        end == diagnostics._MAX_REDIS_CLI_LOG_BYTES
        for operation, value in redis.calls
        if operation == "getrange"
        for key, _start, end in [value]
        if key in {latest_key, history_key}
    )

    redis.fail.add("strlen")
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

    with pytest.raises(ValueError, match="escapes"):
        diagnostics._safe_path(tmp_path / "allowed", "..", "outside")
    with pytest.raises(ValueError, match="path component"):
        diagnostics._open_diagnostic_file(tmp_path)
    directory_source = tmp_path / "directory-source"
    directory_source.mkdir()
    with pytest.raises(OSError, match="regular file"):
        diagnostics._open_diagnostic_file(tmp_path, directory_source.name)
    assert diagnostics._yaml_flow_delta('{"value": "escaped \\" } [ text"}') == 0
    assert diagnostics._yaml_flow_delta("{'value': ']'}") == 0
    assert diagnostics._yaml_flow_delta("{ # ignored }") == 1
    excessive_flow = "[" * (diagnostics._MAX_YAML_FLOW_DEPTH + 1)
    assert diagnostics._yaml_flow_complexity_exceeded(excessive_flow)
    assert diagnostics._yaml_flow_sensitivity(excessive_flow) == (
        "YAML FLOW COMPLEXITY BOUND EXCEEDED"
    )
    excessive_lines = [excessive_flow.encode()]
    assert diagnostics._yaml_anchor_definitions(excessive_lines) == []
    assert diagnostics._yaml_mapping_scalar_values(excessive_lines, "kind") == {}
    assert diagnostics._yaml_sensitive_assignment(excessive_flow) is None
    assert diagnostics._yaml_secret_payload_lines(excessive_lines) == set()
    complexity_warnings: list[str] = []
    with monkeypatch.context() as yaml_patch:
        yaml_patch.setattr(
            diagnostics.yaml,
            "scan",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("bounded flow must not reach yaml.scan")
            ),
        )
        yaml_patch.setattr(
            diagnostics.yaml,
            "safe_load",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("bounded flow must not reach yaml.safe_load")
            ),
        )
        yaml_patch.setattr(
            diagnostics.yaml,
            "compose",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("bounded flow must not reach yaml.compose")
            ),
        )
        complexity_units = diagnostics._redacted_file_units(
            excessive_flow.encode(),
            starts_inside_private_key=False,
            starts_with_sensitive_value=False,
            starts_inside_sensitive_block=False,
            sensitive_block_indent=None,
            starts_inside_sensitive_quote=False,
            sensitive_quote=None,
            has_more_after_raw=False,
            warnings=complexity_warnings,
        )
    assert complexity_units[0][1] == "[CONTENT OMITTED: YAML FLOW COMPLEXITY BOUND EXCEEDED]\n"
    assert any("parse complexity" in warning for warning in complexity_warnings)
    block_heavy = "a: b\n" * (diagnostics._MAX_YAML_BLOCK_MAPPING_LINES + 1)
    assert diagnostics._yaml_block_complexity_exceeded(block_heavy)
    assert diagnostics._yaml_parse_complexity_exceeded(block_heavy)
    sequence_heavy = (
        "- &item {name: USER, value: ok}\n"
        "- <<: *item\n"
        + "- ordinary\n" * diagnostics._MAX_YAML_BLOCK_TOKENS
    )
    assert diagnostics._yaml_block_complexity_exceeded(sequence_heavy)
    assert diagnostics._yaml_parse_complexity_exceeded(sequence_heavy)
    physical_line_heavy = "\n" * (diagnostics._MAX_REDACTION_PHYSICAL_LINES + 1)
    decoded_key_heavy = '? "pass\\u0061ge"\n' * (
        diagnostics._MAX_YAML_PER_LINE_SCAN_CANDIDATES + 1
    )
    assert diagnostics._yaml_block_complexity_exceeded(decoded_key_heavy)
    block_warnings: list[str] = []
    with monkeypatch.context() as yaml_patch:
        yaml_patch.setattr(
            diagnostics.yaml,
            "scan",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("bounded block YAML must not reach yaml.scan")
            ),
        )
        yaml_patch.setattr(
            diagnostics.yaml,
            "compose",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("bounded block YAML must not reach yaml.compose")
            ),
        )
        block_units = diagnostics._redacted_file_units(
            block_heavy.encode(),
            starts_inside_private_key=False,
            starts_with_sensitive_value=False,
            starts_inside_sensitive_block=False,
            sensitive_block_indent=None,
            starts_inside_sensitive_quote=False,
            sensitive_quote=None,
            has_more_after_raw=False,
            warnings=block_warnings,
        )
        sequence_units = diagnostics._redacted_file_units(
            sequence_heavy.encode(),
            starts_inside_private_key=False,
            starts_with_sensitive_value=False,
            starts_inside_sensitive_block=False,
            sensitive_block_indent=None,
            starts_inside_sensitive_quote=False,
            sensitive_quote=None,
            has_more_after_raw=False,
            warnings=block_warnings,
        )
        physical_line_units = diagnostics._redacted_file_units(
            physical_line_heavy.encode(),
            starts_inside_private_key=False,
            starts_with_sensitive_value=False,
            starts_inside_sensitive_block=False,
            sensitive_block_indent=None,
            starts_inside_sensitive_quote=False,
            sensitive_quote=None,
            has_more_after_raw=False,
            warnings=block_warnings,
        )
    assert block_units[0][1] == "[CONTENT OMITTED: YAML BLOCK COMPLEXITY BOUND EXCEEDED]\n"
    assert sequence_units[0][1] == (
        "[CONTENT OMITTED: YAML BLOCK COMPLEXITY BOUND EXCEEDED]\n"
    )
    assert physical_line_units == [(physical_line_heavy.encode(), physical_line_heavy, 0)]
    risky_physical_line_units = diagnostics._redacted_file_units(
        (physical_line_heavy + "password:\n  hidden\n").encode(),
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=False,
        warnings=block_warnings,
    )
    assert risky_physical_line_units[0][1] == (
        "[CONTENT OMITTED: PHYSICAL LINE BOUND EXCEEDED]\n"
    )
    assert any("Physical line count" in warning for warning in block_warnings)
    assert any("block syntax" in warning for warning in block_warnings)

    real_scan = diagnostics.yaml.scan
    scan_calls = 0

    def counted_scan(*args: object, **kwargs: object) -> object:
        nonlocal scan_calls
        scan_calls += 1
        return real_scan(*args, **kwargs)

    with monkeypatch.context() as yaml_patch:
        yaml_patch.setattr(diagnostics.yaml, "scan", counted_scan)
        diagnostics._redacted_file_units(
            b"a: b\n" * 200,
            starts_inside_private_key=False,
            starts_with_sensitive_value=False,
            starts_inside_sensitive_block=False,
            sensitive_block_indent=None,
            starts_inside_sensitive_quote=False,
            sensitive_quote=None,
            has_more_after_raw=False,
            warnings=[],
        )
    assert scan_calls == 3
    assert diagnostics._yaml_node_scalar("&resourceKind !!str 'Secret'") == "Secret"
    anchors = diagnostics._yaml_scalar_anchors(
        [
            b"kindValue: &kindValue Secret\n",
            b'note: "fake &kindValue ConfigMap"\n',
            b"literal: |\n  fake: &kindValue ConfigMap\n",
            b"aliasValue: &aliasValue *kindValue\n",
            b"taggedValue: &taggedValue !!str Secret\n",
        ]
    )
    assert diagnostics._resolve_yaml_scalar("*aliasValue", anchors) == "Secret"
    assert diagnostics._resolve_yaml_scalar("*taggedValue", anchors) == "Secret"
    assert diagnostics._resolve_yaml_scalar("*missing", anchors) is None
    assert diagnostics._resolve_yaml_scalar("*cycle", {"cycle": "*cycle"}) is None
    assert diagnostics._yaml_scalar_anchors(
        [b"kindValue: &kindValue Secret\n", b"---\n", b"kindValue: &kindValue ConfigMap\n"]
    ) == {"kindValue": "ConfigMap"}
    assert diagnostics._yaml_mapping_scalar_values(
        [
            b"kind: >-\n",
            b"  Secret\n",
            b"---\n",
            b"kind: *kindValue\n",
        ],
        "kind",
    ) == {0: "Secret", 3: "*kindValue"}
    assert diagnostics._yaml_mapping_scalar_values([b"kind: [\n"], "kind") == {}
    assert diagnostics._yaml_mapping_scalar_field("!!str data:") == (0, False, "data", "")
    assert diagnostics._yaml_mapping_scalar_field("  !!str value: hidden") == (
        2,
        False,
        "value",
        "hidden",
    )
    assert diagnostics._yaml_mapping_scalar_field("- &field name: PASSWORD") == (
        2,
        True,
        "name",
        "PASSWORD",
    )
    assert diagnostics._yaml_mapping_scalar_field("ordinary") is None
    assert diagnostics._yaml_mapping_scalar_field('*field: "unterminated') is None
    assert diagnostics._yaml_mapping_scalar_field("? data # :") is None
    assert diagnostics._yaml_mapping_scalar_field("name: *field") == (
        0,
        False,
        "name",
        "*field",
    )
    assert diagnostics._yaml_has_payload_mapping_key("? !!str data") is True
    assert diagnostics._yaml_composed_documents(
        [b"a: b\n" * (diagnostics._MAX_YAML_BLOCK_MAPPING_LINES + 1)]
    ) == []
    with monkeypatch.context() as depth_patch:
        depth_patch.setattr(diagnostics, "_MAX_STRUCTURED_DEPTH", 0)
        assert diagnostics._yaml_sensitive_mapping_lines(
            [b"? >-\n", b"  password\n", b": hidden\n"]
        ) == set()
        assert diagnostics._yaml_sensitive_env_lines(
            [b"- name: PASSWORD\n", b"  ? >-\n", b"    value\n", b"  : hidden\n"]
        ) == set()
    assert diagnostics._yaml_sensitive_env_lines(
        [
            b"- <<: scalar\n",
            b"  ? [complex, key]\n",
            b"  : ignored\n",
            b"  value: hidden\n",
        ]
    ) == {0, 1, 2, 3}
    assert diagnostics._yaml_sensitive_env_lines(
        [b"env: &env\n", b"  <<: *env\n", b"  value: hidden\n"]
    ) == {0, 1, 2}
    assert diagnostics._yaml_kind_entries(
        [
            b"? kind\n",
            b": Secret\n",
            b"note: |\n",
            b"  kind: ConfigMap\n",
        ]
    ) == [(0, "Secret", 0, False)]
    assert diagnostics._yaml_kind_entries([b"- ? kind\n", b"  : Secret\n"]) == [
        (0, "Secret", 0, True)
    ]
    assert diagnostics._yaml_kind_entries([b"kind: [\n"]) == [(0, "[", 0, False)]
    assert diagnostics._yaml_secret_payload_lines(
        [
            b"base: &resource\n",
            b"  kind: Secret\n",
            b"<<: *resource\n",
            b"data:\n",
            b"  opaque: hidden\n",
        ]
    ) == {4}
    assert diagnostics._yaml_secret_payload_lines(
        [
            b"base: &resource\n",
            b"  kind: Secret\n",
            b"<<: *resource\n",
            b"data:\n",
            b"  - &value hidden\n",
            b"  - *value\n",
        ]
    ) == {4, 5}
    with monkeypatch.context() as depth_patch:
        depth_patch.setattr(diagnostics, "_MAX_STRUCTURED_DEPTH", 1)
        assert diagnostics._yaml_secret_payload_lines(
            [
                b"!!str kind: Secret\n",
                b"data:\n",
                b"  nested:\n",
                b"    opaque: hidden\n",
            ]
        ) == {2, 3}
    assert diagnostics._yaml_secret_payload_lines(
        [
            b"base: &resource\n",
            b"  kind: ConfigMap\n",
            b"<<: *resource\n",
            b"data:\n",
            b"  harmless: visible\n",
        ]
    ) == set()
    assert diagnostics._yaml_secret_payload_lines(
        [
            b"secret: &secret\n",
            b"  kind: Secret\n",
            b"config: &config\n",
            b"  kind: ConfigMap\n",
            b"<<: [*config, *secret]\n",
            b"data:\n",
            b"  opaque: hidden\n",
            b"invalid:\n",
            b"  <<: scalar\n",
            b"? [complex, key]\n",
            b": visible\n",
        ]
    ) == {6}
    assert diagnostics._yaml_secret_payload_lines(
        [
            b"resource: &resource\n",
            b"  <<: *resource\n",
            b"  data:\n",
            b"    opaque: fail-closed\n",
        ]
    ) == {3}
    assert diagnostics._yaml_secret_payload_lines([b"<<: [\n", b"data:\n"]) == set()
    assert diagnostics._yaml_has_payload_mapping_key('? "da\\u0074a') is False
    assert diagnostics._yaml_secret_payload_lines(
        [b"<<: " + b"[" * (diagnostics._MAX_YAML_FLOW_DEPTH + 1)]
    ) == set()
    assert diagnostics._yaml_sensitive_assignment(
        '  - "pass\\u0077ord": plainsecret'
    ) == (2, "plainsecret")
    assert diagnostics._yaml_sensitive_assignment("ordinary: visible") is None
    assert diagnostics._yaml_sensitive_assignment('"pass\\u0077ord": "') is None
    assert diagnostics._yaml_alias_sensitive_assignment(
        "  - *field: plainsecret",
        {"field": "password"},
    ) == (2, "plainsecret")
    assert diagnostics._yaml_alias_sensitive_assignment(
        "*field: visible",
        {"field": "harmless"},
    ) is None
    assert diagnostics._yaml_alias_sensitive_assignment("*missing: fail-closed", {}) == (
        0,
        "fail-closed",
    )
    assert diagnostics._yaml_alias_sensitive_assignment("ordinary: visible", {}) is None
    assert diagnostics._yaml_alias_sensitive_assignment("*field visible", {}) is None
    assert diagnostics._yaml_alias_sensitive_assignment('*field: "unterminated', {}) is None
    assert diagnostics._yaml_alias_sensitive_assignment(
        "outer: {*field: nested}",
        {"field": "password"},
    ) is None
    assert diagnostics._yaml_explicit_sensitive_key("? password") == 0
    assert diagnostics._yaml_explicit_sensitive_key('? "pass\\u0077ord"') == 0
    assert diagnostics._yaml_explicit_sensitive_key("? harmless") is None
    assert diagnostics._yaml_explicit_sensitive_key("? !!seq [password]") is None
    assert diagnostics._yaml_explicit_sensitive_key("? !!map {password: visible}") is None
    assert diagnostics._yaml_explicit_sensitive_key('? "pass\\u0077ord') is None
    assert diagnostics._yaml_explicit_sensitive_key('"pass\\u0077ord": visible # ?') is None
    assert diagnostics._yaml_explicit_value_end(
        [b"\n", b": visible\n"],
        0,
        minimum_indent=0,
        has_more_after_raw=False,
    ) == (2, False)
    assert diagnostics._yaml_explicit_value_end(
        [b"\n"],
        0,
        minimum_indent=0,
        has_more_after_raw=True,
    ) == (1, True)
    retry_cursor = diagnostics._retry_cursor(1.5, "command")
    assert diagnostics._validate_retry_cursor(retry_cursor) == (
        1.5,
        "command",
        "",
        None,
        None,
    )
    digest_cursor = diagnostics._retry_cursor(1.5, "", member_sha1="a" * 40)
    assert diagnostics._validate_retry_cursor(digest_cursor) == (
        1.5,
        "",
        "a" * 40,
        None,
        None,
    )
    indexed_digest_cursor = diagnostics._retry_cursor(
        1.5,
        "",
        member_sha1="a" * 40,
        member_index=250,
        member_score_tail=25,
    )
    assert diagnostics._validate_retry_cursor(indexed_digest_cursor) == (
        1.5,
        "",
        "a" * 40,
        250,
        25,
    )
    with pytest.raises(ValueError, match="nonempty bounded"):
        diagnostics._validate_retry_cursor("")
    with pytest.raises(ValueError, match="malformed"):
        diagnostics._validate_retry_cursor(diagnostics._retry_cursor(float("nan"), "command"))
    with pytest.raises(ValueError, match="malformed"):
        diagnostics._validate_retry_cursor(diagnostics._retry_cursor(1.5, ""))
    with pytest.raises(ValueError, match="malformed"):
        diagnostics._validate_retry_cursor(
            diagnostics._retry_cursor(1.5, "", member_sha1="not-a-digest")
        )
    with pytest.raises(ValueError, match="malformed"):
        diagnostics._validate_retry_cursor(
            diagnostics._retry_cursor(1.5, "command", member_index=1)
        )
    with pytest.raises(ValueError, match="malformed"):
        diagnostics._validate_retry_cursor(
            diagnostics._retry_cursor(1.5, "", member_sha1="a" * 40, member_index=-1)
        )
    with pytest.raises(ValueError, match="malformed"):
        diagnostics._validate_retry_cursor(
            diagnostics._retry_cursor(1.5, "command", member_score_tail=1)
        )
    with pytest.raises(ValueError, match="malformed"):
        diagnostics._validate_retry_cursor(
            diagnostics._retry_cursor(
                1.5,
                "",
                member_sha1="a" * 40,
                member_score_tail=-1,
            )
        )
    assert (
        diagnostics._yaml_explicit_value_end(
            [b"ordinary\n"],
            0,
            minimum_indent=0,
            has_more_after_raw=False,
        )
        is None
    )
    assert diagnostics._yaml_explicit_value_end(
        [b": {nested:\n", b"  visible}\n"],
        0,
        minimum_indent=0,
        has_more_after_raw=False,
    ) == (2, False)
    assert diagnostics._yaml_explicit_value_end(
        [b": {nested:\n"],
        0,
        minimum_indent=0,
        has_more_after_raw=True,
    ) == (1, True)
    assert diagnostics._yaml_sensitive_assignment('"pass\\u0061ge": visible') is None
    assert diagnostics._yaml_sensitive_assignment('"pass\\u0077ord') is None
    assert diagnostics._yaml_flow_sensitivity(
        '{"pass\\u0077ord": hidden}'
    ) == "YAML ASSIGNMENT"
    assert diagnostics._kubernetes_yaml_payload_flags(
        [
            b"kind: *laterKind\n",
            b"data:\n",
            b"  opaque: fail-closed\n",
            b"kindValue: &laterKind ConfigMap\n",
        ],
        starts_inside_secret=False,
    ) == [True, True, True, True]
    assert diagnostics._is_single_line_flow_yaml_secret(
        "[{kind: ConfigMap, data: {safe: visible}}, {data: {opaque: hidden}, kind: Secret}]"
    )
    assert diagnostics._is_single_line_flow_yaml_secret(
        "INFO {kind: Secret, data: {opaque: hidden}}"
    )
    assert diagnostics._is_single_line_flow_yaml_secret(
        "INFO [{kind: Secret, data: {opaque: hidden}}"
    )
    assert not diagnostics._is_single_line_flow_yaml_secret(
        "{kind: ConfigMap, data: {harmless: visible}}"
    )
    assert diagnostics._yaml_flow_sensitivity(
        "{apiVersion: v1,\n kind: Secret,\n data: {opaque: hidden}}"
    ) == "KUBERNETES SECRET"
    assert diagnostics._yaml_flow_sensitivity(
        "env: [{name: PASSWORD, value: hidden}]"
    ) == "YAML ENVIRONMENT VALUE"
    assert diagnostics._yaml_flow_sensitivity(
        "env: [{value: hidden, name: API_KEY}]"
    ) == "YAML ENVIRONMENT VALUE"
    assert diagnostics._yaml_flow_sensitivity(
        "env: [{name: SAFE, value: visible}]"
    ) is None
    cyclic: list[Any] = []
    cyclic.append(cyclic)
    assert not diagnostics._contains_kubernetes_secret_payload(cyclic)
    assert not diagnostics._contains_sensitive_yaml_environment(cyclic)
    assert not diagnostics._contains_sensitive_yaml_key(cyclic)

    class CountingDict(dict[str, Any]):
        value_reads = 0

        def values(self):
            self.value_reads += 1
            return super().values()

    shared = CountingDict({"safe": "visible"})
    alias_fanout = [shared, shared]
    for classifier in (
        diagnostics._contains_kubernetes_secret_payload,
        diagnostics._contains_sensitive_yaml_environment,
        diagnostics._contains_sensitive_yaml_key,
    ):
        shared.value_reads = 0
        assert not classifier(alias_fanout)
        assert shared.value_reads == 1

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", tmp_path / "repos")
    monkeypatch.setenv("PO_EVENTS_DIR", str(tmp_path / "events"))
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
    monkeypatch.setenv("PO_EVENTS_DIR", str(roots))
    with pytest.raises(ValueError, match="symlink"):
        await diagnostics.read_orchestrator_log(SLUG, "events:disk/2026-10-05")

    in_repo_root = tmp_path / "in-repo-symlink"
    in_repo_artifacts = in_repo_root / SLUG / "artifacts"
    in_repo_tasks = in_repo_root / SLUG / "tasks"
    in_repo_artifacts.mkdir(parents=True)
    in_repo_tasks.mkdir(parents=True)
    (in_repo_tasks / "PR-400.md").write_text("in-repo secret", encoding="utf-8")
    (in_repo_artifacts / "ci.log").symlink_to(in_repo_tasks / "PR-400.md")
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", in_repo_root)
    with pytest.raises(ValueError, match="symlink"):
        await diagnostics.read_orchestrator_log(SLUG, "ci:artifact")
    monkeypatch.setenv("PO_EVENTS_DIR", str(tmp_path / "empty-events"))
    sources, warnings = diagnostics._file_log_sources(SLUG)
    assert sources[-1]["source_id"] == "ci:artifact"
    assert sources[-1]["availability"] == "missing"
    assert any("symlink" in warning for warning in warnings)

    sibling_repos = tmp_path / "sibling-repos"
    selected_repo = sibling_repos / SLUG
    sibling_artifacts = sibling_repos / "octo__sibling" / "artifacts"
    selected_repo.mkdir(parents=True)
    sibling_artifacts.mkdir(parents=True)
    (sibling_artifacts / "ci.log").write_text("sibling secret", encoding="utf-8")
    (selected_repo / "artifacts").symlink_to(sibling_artifacts, target_is_directory=True)
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", sibling_repos)
    monkeypatch.setenv("PO_EVENTS_DIR", str(tmp_path / "empty-events"))
    with pytest.raises(ValueError, match="symlink"):
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
    monkeypatch.setenv("PO_EVENTS_DIR", str(sibling_events))
    with pytest.raises(ValueError, match="symlink"):
        await diagnostics.read_orchestrator_log(SLUG, "events:disk/2026-10-05")

    sibling_directory_events = tmp_path / "sibling-directory-events"
    sibling_directory_events.mkdir()
    sibling_directory = sibling_directory_events / "octo__sibling"
    sibling_directory.mkdir()
    (sibling_directory / "2026-10-05.jsonl").write_text("sibling event secret", encoding="utf-8")
    (sibling_directory_events / SLUG).symlink_to(sibling_directory, target_is_directory=True)
    monkeypatch.setenv("PO_EVENTS_DIR", str(sibling_directory_events))
    with pytest.raises(ValueError, match="symlink"):
        await diagnostics.read_orchestrator_log(SLUG, "events:disk/2026-10-05")
    sources, warnings = diagnostics._file_log_sources(SLUG)
    assert sources == []
    assert any("symlink" in warning for warning in warnings)


def test_small_contract_helpers_cover_clock_skew_and_bounded_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    assert diagnostics._parse_timestamp(None) is None
    assert diagnostics._parse_timestamp("not-a-time") is None
    assert diagnostics._parse_timestamp("2026-10-05T12:00:00").tzinfo is not None
    assert diagnostics._error_text(RuntimeError()) == "RuntimeError"
    assert diagnostics._has_closing_quote(r'escaped\"still-open', '"') is False
    assert diagnostics._has_closing_quote(r'escaped\"then-close"', '"') is True
    assert diagnostics._has_closing_quote("escaped''still-open", "'") is False
    assert diagnostics._has_closing_quote("escaped''then-close'", "'") is True
    assert diagnostics._has_closing_quote('first-secret', '"""') is False
    assert diagnostics._has_closing_quote('first-secret"""', '"""') is True
    assert diagnostics._has_closing_quote("first-secret'''", "'''") is True
    assert diagnostics._json_like_value_end("", 0) == 0
    escaped_json_string = r'"escaped\"quote" trailing'
    assert escaped_json_string[: diagnostics._json_like_value_end(escaped_json_string, 0)] == (
        r'"escaped\"quote"'
    )
    unterminated_json_string = '"unterminated'
    assert diagnostics._json_like_value_end(unterminated_json_string, 0) == len(
        unterminated_json_string
    )
    nested_json_value = r'{"nested":["value",{"escaped":"a\"b"}]} trailing'
    assert nested_json_value[: diagnostics._json_like_value_end(nested_json_value, 0)] == (
        r'{"nested":["value",{"escaped":"a\"b"}]}'
    )
    mismatched_json_value = "[} trailing"
    assert diagnostics._json_like_value_end(mismatched_json_value, 0) == len(
        mismatched_json_value
    )
    unterminated_nested_value = '{"nested":[1,2]'
    assert diagnostics._json_like_value_end(unterminated_nested_value, 0) == len(
        unterminated_nested_value
    )
    assert diagnostics._json_like_value_end("plain,tail", 0) == len("plain")
    assert diagnostics._redact_malformed_keyed_values('{"password":') == ('{"password":', 0)
    assert diagnostics._redact_malformed_keyed_values('"password":plain,') == (
        '"password":"[REDACTED]",',
        1,
    )
    assert diagnostics._redact_all_values({"nested": ["one", 2]}) == (
        {"nested": ["[REDACTED]", "[REDACTED]"]},
        2,
    )
    assert diagnostics._redact_all_values([], depth=diagnostics._MAX_STRUCTURED_DEPTH) == (
        "[CONTENT OMITTED: STRUCTURED NESTING BOUND EXCEEDED]",
        1,
    )
    deeply_nested_json = "[" * 1_100 + "0" + "]" * 1_100
    nested_safe, nested_replacements = diagnostics._redact_logical_text(deeply_nested_json)
    assert nested_safe == "[CONTENT OMITTED: STRUCTURED NESTING BOUND EXCEEDED]"
    assert nested_replacements == 1
    embedded_safe, embedded_replacements = diagnostics._redact_embedded_structures(
        "INFO " + deeply_nested_json
    )
    assert embedded_safe == "[CONTENT OMITTED: STRUCTURED REDACTION BOUND EXCEEDED]"
    assert embedded_replacements == 1
    assert "STRUCTURED NESTING BOUND EXCEEDED" in diagnostics._bounded_event(
        deeply_nested_json
    )["raw_excerpt"]
    nested_value: object = 0
    for _ in range(diagnostics._MAX_STRUCTURED_DEPTH + 1):
        nested_value = [nested_value]
    bounded_nested, bounded_nested_count = diagnostics._redact_structure(nested_value)
    assert "STRUCTURED NESTING BOUND EXCEEDED" in json.dumps(bounded_nested)
    assert bounded_nested_count == 1

    original_loads = diagnostics.json.loads
    monkeypatch.setattr(
        diagnostics.json,
        "loads",
        lambda _value: (_ for _ in ()).throw(RecursionError("nested")),
    )
    assert diagnostics._redact_logical_text("[]") == (
        "[CONTENT OMITTED: STRUCTURED NESTING BOUND EXCEEDED]",
        1,
    )
    assert "STRUCTURED NESTING BOUND EXCEEDED" in diagnostics._bounded_event("[]")[
        "raw_excerpt"
    ]
    monkeypatch.setattr(diagnostics.json, "loads", original_loads)

    original_decoder = diagnostics.json.JSONDecoder

    class DeepDecoder:
        def raw_decode(self, text: str, _start: int) -> tuple[object, int]:
            return nested_value, len(text)

    monkeypatch.setattr(diagnostics.json, "JSONDecoder", DeepDecoder)
    assert diagnostics._redact_embedded_structures("INFO {}") == (
        "[CONTENT OMITTED: STRUCTURED NESTING BOUND EXCEEDED]",
        1,
    )

    class RecursiveDecoder:
        def raw_decode(self, _text: str, _start: int) -> tuple[object, int]:
            raise RecursionError("nested")

    monkeypatch.setattr(diagnostics.json, "JSONDecoder", RecursiveDecoder)
    assert diagnostics._redact_embedded_structures("INFO {}") == (
        "[CONTENT OMITTED: STRUCTURED NESTING BOUND EXCEEDED]",
        1,
    )
    monkeypatch.setattr(diagnostics.json, "JSONDecoder", original_decoder)

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
            "GPG_PASSPHRASE=dotenv-passphrase",
            '{"passphrase":"json-passphrase"}',
            "tool --passphrase option-passphrase",
            "curl --user alice:curl-secret",
            "curl -u bob:short-curl-secret",
            "curl -U proxy:proxy-curl-secret",
            "mysql -u root -pmysql-short-option-secret",
            "mariadb-dump -p'mariadb short option secret' database",
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
    assert "dotenv-passphrase" not in redacted
    assert "json-passphrase" not in redacted
    assert "option-passphrase" not in redacted
    assert "curl-secret" not in redacted
    assert "short-curl-secret" not in redacted
    assert "proxy-curl-secret" not in redacted
    assert "mysql-short-option-secret" not in redacted
    assert "mariadb short option secret" not in redacted
    assert replacements == 34
    assert diagnostics._redact_text("mysql -p") == (
        "mysql -p",
        0,
    )
    assert diagnostics._redact_text("psql -p5432") == ("psql -p5432", 0)
    unclosed_xml, unclosed_xml_count, unclosed_xml_warnings = (
        diagnostics._redact_log_content("<password>\nunclosed-xml-secret\n")
    )
    assert "unclosed-xml-secret" not in unclosed_xml
    assert unclosed_xml_count == 1
    assert "no closing tag" in unclosed_xml_warnings[0]
    inside_xml_warnings: list[str] = []
    inside_xml_units = diagnostics._redacted_file_units(
        b"inside-xml-secret\n",
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=True,
        sensitive_quote=f"{diagnostics._XML_SENSITIVE_STATE_PREFIX}password",
        has_more_after_raw=True,
        warnings=inside_xml_warnings,
    )
    assert inside_xml_units[0][1] == "[REDACTED SENSITIVE XML ELEMENT]\n"
    assert "crossed the bounded page window" in inside_xml_warnings[0]
    assert diagnostics._redact_text("tokens_in=123 tokens_out=456") == (
        "tokens_in=123 tokens_out=456",
        0,
    )
    assert diagnostics._redact_logical_text('{"debug": true}\n') == ('{"debug": true}\n', 0)
    assert diagnostics._redact_logical_text("123") == ("123", 0)
    assert diagnostics._redact_embedded_structures('INFO {"debug":true}') == (
        'INFO {"debug":true}',
        0,
    )
    embedded_docker, embedded_docker_count = diagnostics._redact_embedded_structures(
        'INFO {"auths":{"registry":{"auth":"embedded-docker-secret"}}}'
    )
    assert "embedded-docker-secret" not in embedded_docker
    assert embedded_docker_count == 1
    bounded_embedded, bounded_embedded_count = diagnostics._redact_embedded_structures(
        "prefix " + "[" * (diagnostics._MAX_EMBEDDED_JSON_CANDIDATES + 1) + "\r\n"
    )
    assert bounded_embedded == "[CONTENT OMITTED: STRUCTURED REDACTION BOUND EXCEEDED]\r\n"
    assert bounded_embedded_count == 1
    same_line_toml, same_line_toml_count = diagnostics._redact_logical_text(
        'password = """same-line-toml-secret"""'
    )
    assert "same-line-toml-secret" not in same_line_toml
    assert same_line_toml_count == 1
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
        "gpgPassphrase",
        "client-key-data",
        "clientKeyData",
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
    redis.fail.add("eval_ro")
    runs = await diagnostics._relevant_runs(redis, SLUG, "PR-9", 5)
    assert runs["status"] == "unavailable"

    async def malformed_run_page(*args: Any, **kwargs: Any) -> list[object]:
        del args, kwargs
        return [0]

    redis = FakeRedis()
    redis.eval_ro = malformed_run_page  # type: ignore[method-assign]
    runs = await diagnostics._relevant_runs(redis, SLUG, "PR-9", 5)
    assert runs["status"] == "unavailable"

    async def incomplete_run_rows(*args: Any, **kwargs: Any) -> list[object]:
        del args, kwargs
        return [1, [0]]

    redis = FakeRedis()
    redis.eval_ro = incomplete_run_rows  # type: ignore[method-assign]
    runs = await diagnostics._relevant_runs(redis, SLUG, "PR-9", 5)
    assert runs["status"] == "unavailable"

    redis = FakeRedis()
    redis.fail.add("eval_ro")
    retries = await diagnostics._pending_retries(redis, SLUG)
    assert retries["status"] == "unavailable"

    async def malformed_retry_page(*args: Any, **kwargs: Any) -> list[object]:
        del args, kwargs
        return [0]

    redis = FakeRedis()
    redis.eval_ro = malformed_retry_page  # type: ignore[method-assign]
    retries = await diagnostics._pending_retries(redis, SLUG)
    assert retries["status"] == "unavailable"

    async def incomplete_retry_rows(*args: Any, **kwargs: Any) -> list[object]:
        del args, kwargs
        return [1, 0, ["command"]]

    redis = FakeRedis()
    redis.eval_ro = incomplete_retry_rows  # type: ignore[method-assign]
    retries = await diagnostics._pending_retries(redis, SLUG)
    assert retries["status"] == "unavailable"

    redis = FakeRedis()
    redis.zsets[retry_command_pending(SLUG)] = [("command", 1.0)]
    redis.fail.add("strlen")
    retries = await diagnostics._pending_retries(redis, SLUG)
    assert retries["commands"][0]["status"] == "unavailable"

    redis = FakeRedis()
    redis.zsets[retry_command_pending(SLUG)] = [("oversized", 1.0)]
    command_key = retry_command(SLUG, "oversized")
    redis.store[command_key] = "x" * (diagnostics._MAX_REDIS_RETRY_COMMAND_BYTES + 1)
    redis.ttls[command_key] = 120
    retries = await diagnostics._pending_retries(redis, SLUG)
    assert retries["commands"] == [
        {
            "status": "oversized",
            "command_id": "oversized",
            "index_score": 1.0,
            "source_size_bytes": diagnostics._MAX_REDIS_RETRY_COMMAND_BYTES + 1,
            "read_bound_bytes": diagnostics._MAX_REDIS_RETRY_COMMAND_BYTES,
            "ttl_seconds_remaining": 120,
            "error": (
                f"Stored Retry command is {diagnostics._MAX_REDIS_RETRY_COMMAND_BYTES + 1} bytes; "
                f"the diagnostic read bound is {diagnostics._MAX_REDIS_RETRY_COMMAND_BYTES} bytes."
            ),
        }
    ]
    assert not any(operation in {"get", "getrange"} for operation, _ in redis.calls)

    redis = FakeRedis()
    redis.zsets[retry_command_pending(SLUG)] = [
        (f"command-{index}", float(index)) for index in range(22)
    ]
    first_retry_page = await diagnostics._pending_retries(redis, SLUG)
    assert len(first_retry_page["commands"]) == diagnostics._MAX_PENDING_RETRIES
    next_cursor = first_retry_page["continuation"]["next_cursor"]
    second_retry_page = await diagnostics._pending_retries(
        redis,
        SLUG,
        next_cursor,
    )
    assert second_retry_page["cursor"] == next_cursor
    assert second_retry_page["position_at_observation"] == diagnostics._MAX_PENDING_RETRIES
    assert len(second_retry_page["commands"]) == 2
    assert second_retry_page["continuation"] is None

    redis.zsets[retry_command_pending(SLUG)].pop(0)
    stable_page = await diagnostics._pending_retries(redis, SLUG, next_cursor)
    assert [item["command_id"] for item in stable_page["commands"]] == [
        "command-20",
        "command-21",
    ]


async def test_pending_retry_index_members_are_bounded_and_paginated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    monkeypatch.setattr(diagnostics, "_MAX_PENDING_RETRIES", 2)
    redis = FakeRedis()
    pending_key = retry_command_pending(SLUG)
    oversized_member = "x" * (diagnostics._MAX_RETRY_INDEX_MEMBER_BYTES + 1)
    redis.zsets[pending_key] = [
        ("safe-before", 1.0),
        (oversized_member, 2.0),
        ("safe-after", 3.0),
    ]

    first = await diagnostics._pending_retries(redis, SLUG)

    assert [item["status"] for item in first["commands"]] == [
        "missing_payload",
        "oversized_index_member",
    ]
    assert first["commands"][1] == {
        "status": "oversized_index_member",
        "index": 1,
        "index_score": 2.0,
        "source_size_bytes": len(oversized_member),
        "read_bound_bytes": diagnostics._MAX_RETRY_INDEX_MEMBER_BYTES,
        "error": (
            f"Stored pending Retry index member is {len(oversized_member)} bytes; "
            f"the diagnostic read bound is {diagnostics._MAX_RETRY_INDEX_MEMBER_BYTES} bytes."
        ),
    }
    assert oversized_member not in json.dumps(first)
    next_cursor = first["continuation"]["next_cursor"]

    second = await diagnostics._pending_retries(redis, SLUG, next_cursor)

    assert [item["command_id"] for item in second["commands"]] == ["safe-after"]
    assert second["continuation"] is None
    assert not any(
        operation in {"strlen", "getrange", "exists", "ttl"}
        and oversized_member in str(value)
        for operation, value in redis.calls
    )

    legacy_cursor = diagnostics._retry_cursor(
        2.0,
        "",
        member_sha1=hashlib.sha1(oversized_member.encode()).hexdigest(),
        member_index=1,
    )
    legacy_second = await diagnostics._pending_retries(redis, SLUG, legacy_cursor)
    assert [item["command_id"] for item in legacy_second["commands"]] == ["safe-after"]

    redis.zsets[pending_key].pop(1)
    missing_cursor_member = await diagnostics._pending_retries(redis, SLUG, next_cursor)
    assert missing_cursor_member["status"] == "unavailable"
    assert "no longer locatable" in missing_cursor_member["error"]

    async def missing_digest(*args: Any, **kwargs: Any) -> list[object]:
        del args, kwargs
        return [
            1,
            0,
            ["", 1.0, 0, diagnostics._MAX_RETRY_INDEX_MEMBER_BYTES + 1, "", 0],
        ]

    redis.eval_ro = missing_digest  # type: ignore[method-assign]
    malformed = await diagnostics._pending_retries(redis, SLUG)
    assert malformed["status"] == "unavailable"

    digest = hashlib.sha1(oversized_member.encode()).hexdigest()

    async def dishonest_member_size(*args: Any, **kwargs: Any) -> list[object]:
        del args, kwargs
        return [1, 0, [oversized_member, 1.0, 0, 1, digest, 0]]

    redis.eval_ro = dishonest_member_size  # type: ignore[method-assign]
    dishonest = await diagnostics._pending_retries(redis, SLUG)
    assert dishonest["commands"][0]["status"] == "oversized_index_member"
    assert dishonest["commands"][0]["source_size_bytes"] == len(oversized_member)


async def test_oversized_retry_cursor_pages_past_large_score_tie(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    monkeypatch.setattr(diagnostics, "_MAX_PENDING_RETRIES", 20)
    redis = FakeRedis()
    pending_key = retry_command_pending(SLUG)
    member_prefix = "x" * diagnostics._MAX_RETRY_INDEX_MEMBER_BYTES
    redis.zsets[pending_key] = [
        (f"{member_prefix}{index:04d}", 1.0)
        for index in range(diagnostics._MAX_RETRY_CURSOR_LOOKUP + 30)
    ]

    cursor = None
    observed_indexes: list[int] = []
    while True:
        page = await diagnostics._pending_retries(redis, SLUG, cursor)
        assert page["status"] == "available"
        observed_indexes.extend(item["index"] for item in page["commands"])
        continuation = page["continuation"]
        if continuation is None:
            break
        cursor = continuation["next_cursor"]

    assert observed_indexes == list(range(diagnostics._MAX_RETRY_CURSOR_LOOKUP + 30))


async def test_oversized_retry_cursor_survives_earlier_deletions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    monkeypatch.setattr(diagnostics, "_MAX_PENDING_RETRIES", 20)
    redis = FakeRedis()
    pending_key = retry_command_pending(SLUG)
    member_prefix = "x" * diagnostics._MAX_RETRY_INDEX_MEMBER_BYTES
    members = [
        f"{member_prefix}{index:04d}"
        for index in range(diagnostics._MAX_RETRY_CURSOR_LOOKUP + 50)
    ]
    redis.zsets[pending_key] = [(member, 1.0) for member in members]
    cursor_index = diagnostics._MAX_RETRY_CURSOR_LOOKUP + 19
    cursor_member = members[cursor_index]
    cursor = diagnostics._retry_cursor(
        1.0,
        "",
        member_sha1=hashlib.sha1(cursor_member.encode()).hexdigest(),
        member_index=cursor_index,
        member_score_tail=len(members) - cursor_index - 1,
    )

    del redis.zsets[pending_key][:120]
    page = await diagnostics._pending_retries(redis, SLUG, cursor)

    assert page["status"] == "available"
    assert page["position_at_observation"] == cursor_index - 119
    assert page["commands"][0]["index"] == cursor_index - 119


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
    original_strlen = redis.strlen

    async def selective_strlen(key: str) -> int:
        if key == MetricsStore._record_key("unavailable"):
            raise ConnectionError("record unavailable")
        return await original_strlen(key)

    redis.strlen = selective_strlen  # type: ignore[method-assign]
    result = await diagnostics._relevant_runs(redis, SLUG, "PR-9", 2)
    assert [item["status"] for item in result["records"]] == ["unavailable", "available"]
    assert result["records"][1]["record"]["run_id"] == "wanted"
    assert not any(
        operation == "get" and str(key).startswith("metrics:run:")
        for operation, key in redis.calls
    )
    assert all(
        end == diagnostics._MAX_REDIS_RUN_RECORD_BYTES
        for operation, value in redis.calls
        if operation == "getrange"
        for key, _start, end in [value]
        if str(key).startswith("metrics:run:")
    )
    capped_unavailable = await diagnostics._relevant_runs(redis, SLUG, "PR-9", 1)
    assert [item["status"] for item in capped_unavailable["records"]] == ["unavailable"]
    assert capped_unavailable["scanned_index_entries"] == 1

    oversized_redis = FakeRedis()
    oversized_redis.lists[index] = ["oversized", "wanted"]
    oversized_redis.store[MetricsStore._record_key("oversized")] = (
        "x" * (diagnostics._MAX_REDIS_RUN_RECORD_BYTES + 1)
    )
    oversized_redis.store[MetricsStore._record_key("wanted")] = json.dumps(asdict(_run("wanted")))
    oversized = await diagnostics._relevant_runs(oversized_redis, SLUG, "PR-9", 2)
    assert oversized["records"][0] == {
        "status": "oversized",
        "run_id": "oversized",
        "source_size_bytes": diagnostics._MAX_REDIS_RUN_RECORD_BYTES + 1,
        "read_bound_bytes": diagnostics._MAX_REDIS_RUN_RECORD_BYTES,
        "error": (
            f"Stored run record is {diagnostics._MAX_REDIS_RUN_RECORD_BYTES + 1} bytes; "
            f"the diagnostic read bound is {diagnostics._MAX_REDIS_RUN_RECORD_BYTES} bytes."
        ),
    }
    assert oversized["records"][1]["record"]["run_id"] == "wanted"
    assert not any(operation == "get" for operation, _ in oversized_redis.calls)
    capped = await diagnostics._relevant_runs(oversized_redis, SLUG, "PR-9", 1)
    assert [item["status"] for item in capped["records"]] == ["oversized"]

    oversized_member = "x" * (diagnostics._MAX_RUN_INDEX_MEMBER_BYTES + 1)
    bounded_index_redis = FakeRedis()
    bounded_index_redis.lists[index] = [oversized_member, "wanted"]
    bounded_index_redis.store[MetricsStore._record_key("wanted")] = json.dumps(
        asdict(_run("wanted"))
    )
    bounded_index = await diagnostics._relevant_runs(
        bounded_index_redis,
        SLUG,
        "PR-9",
        2,
    )
    assert bounded_index["records"][0] == {
        "status": "oversized_index_member",
        "index": 0,
        "source_size_bytes": len(oversized_member),
        "read_bound_bytes": diagnostics._MAX_RUN_INDEX_MEMBER_BYTES,
        "error": (
            f"Stored run-index member is {len(oversized_member)} bytes; "
            f"the diagnostic read bound is {diagnostics._MAX_RUN_INDEX_MEMBER_BYTES} bytes."
        ),
    }
    assert bounded_index["records"][1]["record"]["run_id"] == "wanted"
    assert bounded_index["oversized_index_members"] == 1
    assert oversized_member not in json.dumps(bounded_index)
    assert not any(operation == "lrange" for operation, _ in bounded_index_redis.calls)

    async def dishonest_run_page(*args: Any, **kwargs: Any) -> list[object]:
        del args, kwargs
        return [1, [0, 1, oversized_member]]

    bounded_index_redis.eval_ro = dishonest_run_page  # type: ignore[method-assign]
    dishonest = await diagnostics._relevant_runs(bounded_index_redis, SLUG, "PR-9", 1)
    assert dishonest["records"][0]["status"] == "oversized_index_member"
    assert dishonest["records"][0]["source_size_bytes"] == len(oversized_member)

    assert diagnostics._state_history(None, 2)["status"] == "unavailable"
    state = RepoState(
        url="https://github.com/octo/demo",
        name=SLUG,
        history=[{"time": NOW.isoformat(), "state": "ERROR", "event": "x" * 2_100}],
    )
    history = diagnostics._state_history(state, 1)
    assert history["events"][0]["record_truncated"] is True


async def test_task_filtered_runs_scan_the_full_bounded_recent_index() -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    index = MetricsStore._recent_key("PR-9", SLUG)
    other_ids = [f"other-{number}" for number in range(25)]
    redis.lists[index] = [*other_ids, "wanted"]
    for run_id in other_ids:
        record = asdict(_run(run_id))
        record["task_id"] = "PR-8"
        redis.store[MetricsStore._record_key(run_id)] = json.dumps(record)
    redis.store[MetricsStore._record_key("wanted")] = json.dumps(asdict(_run("wanted")))

    result = await diagnostics._relevant_runs(redis, SLUG, "PR-9", 1)

    assert result["records"][0]["record"]["run_id"] == "wanted"
    assert result["scanned_index_entries"] == 26
    assert result["scan_limit"] == diagnostics._MAX_RUN_INDEX_ENTRIES


async def test_status_bounds_pipeline_snapshots_before_fetching(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    redis.store[pipeline_state(SLUG)] = "x" * (diagnostics._MAX_REDIS_STATE_BYTES + 1)

    result = await diagnostics.get_orchestrator_status()

    snapshot = result["repositories"][0]["snapshot"]
    assert snapshot["status"] == "oversized"
    assert snapshot["source_size_bytes"] == diagnostics._MAX_REDIS_STATE_BYTES + 1
    assert snapshot["read_bound_bytes"] == diagnostics._MAX_REDIS_STATE_BYTES
    assert not any(operation in {"get", "mget", "getrange"} for operation, _ in redis.calls)

    raced = FakeRedis()
    raced.store["pipeline:raced"] = b"x" * (diagnostics._MAX_REDIS_STATE_BYTES + 1)

    async def stale_strlen(key: str) -> int:
        raced._check("strlen", key)
        return 1

    raced.strlen = stale_strlen  # type: ignore[method-assign]
    raw, size_bytes, oversized = await diagnostics._read_bounded_redis_value(
        raced,
        "pipeline:raced",
        diagnostics._MAX_REDIS_STATE_BYTES,
    )
    assert raw is None
    assert size_bytes == diagnostics._MAX_REDIS_STATE_BYTES + 1
    assert oversized is True


async def test_log_discovery_defensive_failures(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    redis.fail.add("strlen")
    sources, warnings = await diagnostics._redis_log_sources(redis, SLUG, NOW)
    by_id = {item["source_id"]: item for item in sources}
    assert by_id["cli:latest"]["availability"] == "unavailable"
    assert by_id["events:redis"]["availability"] == "empty"
    assert warnings

    redis = FakeRedis()
    redis.store[cli_log_latest(SLUG)] = "retained"
    original_eval_ro = redis.eval_ro

    async def corrupt_event_history(
        script: str,
        numkeys: int,
        key: str,
        *args: object,
    ) -> list[object]:
        if key == repo_events_history(SLUG):
            raise TypeError("event history has the wrong Redis type")
        return await original_eval_ro(script, numkeys, key, *args)

    redis.eval_ro = corrupt_event_history  # type: ignore[method-assign]
    sources, warnings = await diagnostics._redis_log_sources(redis, SLUG, NOW)
    by_id = {item["source_id"]: item for item in sources}
    assert by_id["cli:latest"]["availability"] == "available"
    assert by_id["events:redis"]["availability"] == "unavailable"
    assert warnings

    redis = FakeRedis()
    history_key = cli_log_history(SLUG, "2026-10-05T11:00:00+00:00")
    redis.store[history_key] = "value"
    original_getrange = redis.getrange

    async def expire_during_scan(key: str, start: int, end: int) -> object:
        if key == history_key:
            redis.store.pop(key, None)
            return ""
        return await original_getrange(key, start, end)

    redis.getrange = expire_during_scan  # type: ignore[method-assign]
    sources, _, _ = await diagnostics._redis_history_page(
        redis,
        SLUG,
        NOW,
        cursor_state={"scan_cursor": 0, "pending": [], "started": False},
        limit=10,
    )
    assert sources[0]["availability"] == "missing_or_expired"

    redis = FakeRedis()

    async def broken_scan(*args: object):
        del args
        raise ConnectionError("scan failure")

    redis.eval_ro = broken_scan  # type: ignore[method-assign]
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
    monkeypatch.setenv("PO_EVENTS_DIR", str(events_root))
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
    monkeypatch.setenv("PO_EVENTS_DIR", str(escaped_root))
    sources, warnings = diagnostics._file_log_sources(SLUG)
    assert sources == []
    assert warnings

    repos_root = tmp_path / "escaped-repos"
    repos_root.mkdir()
    (repos_root / SLUG).symlink_to(tmp_path, target_is_directory=True)
    monkeypatch.setenv("PO_EVENTS_DIR", str(tmp_path / "empty-events"))
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
    history_gets = [key for operation, key in redis.calls if operation == "getrange"]
    assert len(history_gets) == 2
    assert len([call for call in redis.calls if call[0] == "eval_ro"]) == 1

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
    assert not any(operation == "eval_ro" for operation, _ in redis.calls)
    assert len([key for operation, key in redis.calls if operation == "getrange"]) == 2


async def test_redis_history_discovery_reports_defensive_bounds() -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    timestamps = [(NOW + timedelta(seconds=index)).isoformat() for index in range(205)]

    async def oversized_scan(*args: object) -> list[object]:
        del args
        keys = [
            b"outside:key",
            cli_log_history(SLUG, "not-a-time").encode(),
            b"x" * (diagnostics._MAX_REDIS_HISTORY_KEY_BYTES + 1),
        ]
        keys.extend(cli_log_history(SLUG, timestamp).encode() for timestamp in timestamps)
        return [0, keys, 0, 0]

    redis.eval_ro = oversized_scan  # type: ignore[method-assign]
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
    assert any("oversized CLI history key" in warning for warning in warnings)
    assert any("oversized scan batch" in warning for warning in warnings)

    async def malformed_scan(*args: object) -> list[object]:
        del args
        return [0]

    redis.eval_ro = malformed_scan  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="malformed bounded history scan"):
        await diagnostics._bounded_history_scan(redis, 0, "history:*", 10)

    async def malformed_scan_keys(*args: object) -> list[object]:
        del args
        return [0, "not-a-list", 0, 0]

    redis.eval_ro = malformed_scan_keys  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="malformed bounded history keys"):
        await diagnostics._bounded_history_scan(redis, 0, "history:*", 10)

    unavailable = FakeRedis()
    unavailable.fail.add("strlen")
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


def test_log_discovery_cursor_validation(monkeypatch: pytest.MonkeyPatch) -> None:
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

    def unexpected_decode(*args: object, **kwargs: object) -> bytes:
        del args, kwargs
        raise AssertionError("oversized cursors must be rejected before base64 decoding")

    monkeypatch.setattr(diagnostics.base64, "b64decode", unexpected_decode)
    oversized_history = diagnostics._HISTORY_CURSOR_PREFIX + "A" * (
        diagnostics._MAX_HISTORY_CURSOR_CHARS + 1
    )
    with pytest.raises(ValueError, match="history cursor"):
        diagnostics._decode_log_cursor(oversized_history)
    oversized_file = diagnostics._FILE_CURSOR_PREFIX + "A" * (
        diagnostics._MAX_FILE_CURSOR_CHARS + 1
    )
    with pytest.raises(ValueError, match="filesystem continuation cursor"):
        diagnostics._decode_file_cursor(oversized_file, "ci:artifact")


async def test_list_outer_failure_and_file_read_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    monkeypatch.setenv("PO_EVENTS_DIR", str(tmp_path / "events"))
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

    def fail_ci_read(root: Path, *parts: str):
        del root, parts
        raise OSError("read failed")

    monkeypatch.setattr(diagnostics, "_open_diagnostic_file", fail_ci_read)
    missing = await diagnostics.read_orchestrator_log(SLUG, "ci:artifact")
    assert missing["source"]["availability"] == "unavailable"


async def test_filesystem_open_rejects_symlink_replacement_race(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    repos_root = tmp_path / "repos"
    ci_path = repos_root / SLUG / "artifacts" / "ci.log"
    ci_path.parent.mkdir(parents=True)
    ci_path.write_text("ordinary content\n", encoding="utf-8")
    outside = tmp_path / "outside-secret"
    outside.write_text("outside-race-secret\n", encoding="utf-8")
    monkeypatch.setattr(diagnostics, "_REPOS_ROOT", repos_root)
    monkeypatch.setenv("PO_EVENTS_DIR", str(tmp_path / "events"))

    real_open = diagnostics.os.open
    swapped = False
    observed_flags: list[int] = []

    def swap_before_open(
        path: str | bytes | Path,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal swapped
        observed_flags.append(flags)
        if path == "ci.log" and dir_fd is not None and not swapped:
            swapped = True
            ci_path.unlink()
            ci_path.symlink_to(outside)
        return real_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(diagnostics.os, "open", swap_before_open)
    result = await diagnostics.read_orchestrator_log(SLUG, "ci:artifact")
    assert swapped is True
    assert result["source"]["availability"] == "unavailable"
    assert "outside-race-secret" not in json.dumps(result)
    assert observed_flags and all(flags & diagnostics.os.O_NOFOLLOW for flags in observed_flags)


async def test_filesystem_reads_use_bounded_byte_windows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    monkeypatch.setenv("PO_EVENTS_DIR", str(tmp_path / "events"))
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

    plain_yaml = b"password: correct horse\n  battery staple\nafter\n"
    ci_path.write_bytes(plain_yaml)
    plain_yaml_page = await diagnostics.read_orchestrator_log(SLUG, "ci:artifact", max_chars=200)
    assert "correct horse" not in plain_yaml_page["content"]
    assert "battery staple" not in plain_yaml_page["content"]
    assert plain_yaml_page["content"].startswith("password: [REDACTED]")
    plain_cursor = plain_yaml.index(b"  battery staple")
    plain_continuation = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=plain_cursor,
        max_chars=200,
    )
    assert "battery staple" not in plain_continuation["content"]
    assert "[REDACTED SENSITIVE BLOCK]" in plain_continuation["content"]
    assert "after" in plain_continuation["content"]

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

    quoted_yaml = b'before\npassword: "first-secret\n  second-secret"\nafter\n'
    ci_path.write_bytes(quoted_yaml)
    quoted_page = await diagnostics.read_orchestrator_log(SLUG, "ci:artifact", max_chars=200)
    assert "first-secret" not in quoted_page["content"]
    assert "second-secret" not in quoted_page["content"]
    assert "[REDACTED SENSITIVE QUOTED SCALAR]" in quoted_page["content"]
    quote_cursor = quoted_yaml.index(b"  second-secret")
    quoted_continuation = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=quote_cursor,
        max_chars=200,
    )
    assert "second-secret" not in quoted_continuation["content"]
    assert "[REDACTED SENSITIVE QUOTED SCALAR]" in quoted_continuation["content"]
    assert "after" in quoted_continuation["content"]

    for prefix in ("export ", "[env] "):
        prefixed_quote = f'{prefix}PASSWORD="first-secret\nsecond-secret"\nafter\n'.encode()
        ci_path.write_bytes(prefixed_quote)
        prefixed_page = await diagnostics.read_orchestrator_log(SLUG, "ci:artifact", max_chars=200)
        assert "first-secret" not in prefixed_page["content"]
        assert "second-secret" not in prefixed_page["content"]
        assert "[REDACTED SENSITIVE QUOTED SCALAR]" in prefixed_page["content"]

    distant_quote = b'PASSWORD="first\n' + b"column-zero-secret\n" * 4_000 + b'last-secret"\nafter\n'
    ci_path.write_bytes(distant_quote)
    distant_cursor = distant_quote.index(b'last-secret"')
    distant_page = await diagnostics.read_orchestrator_log(
        SLUG,
        "ci:artifact",
        cursor=distant_cursor,
        max_chars=200,
    )
    assert "last-secret" not in distant_page["content"]
    assert distant_page["content"].startswith("[REDACTED SENSITIVE QUOTED SCALAR]")

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
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=True,
        warnings=warnings,
    )
    assert units[0][1] == "[REDACTED SENSITIVE ASSIGNMENT]\n"
    assert warnings

    warnings = []
    flow_units = diagnostics._redacted_file_units(
        b"{kind: Secret, data:\n {opaque: bounded-flow-secret}\n",
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=True,
        warnings=warnings,
    )
    assert flow_units[0][1] == "[REDACTED SENSITIVE KUBERNETES SECRET]\n"
    assert any("flow collection crossed" in warning for warning in warnings)

    incomplete_json_units = diagnostics._redacted_file_units(
        b'{\n"password":\n"bounded-json-secret"\n',
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=True,
        warnings=[],
    )
    assert "bounded-json-secret" not in "".join(unit[1] for unit in incomplete_json_units)

    warnings = []
    value_units = diagnostics._redacted_file_units(
        b"\n  continued-sensitive-value\n",
        starts_inside_private_key=False,
        starts_with_sensitive_value=True,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=False,
        warnings=warnings,
    )
    assert value_units[0][1] == "[REDACTED SENSITIVE VALUE]\n"
    assert "continued-sensitive-value" not in "".join(unit[1] for unit in value_units)

    warnings = []
    assignment_units = diagnostics._redacted_file_units(
        b"PASSWORD=\n\nassignment-value\n",
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=False,
        warnings=warnings,
    )
    assert assignment_units[0][1] == "[REDACTED SENSITIVE ASSIGNMENT]\n"
    assert "assignment-value" not in "".join(unit[1] for unit in assignment_units)

    warnings = []
    diagnostics._redacted_file_units(
        b"PASSWORD=\n",
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=True,
        warnings=warnings,
    )
    assert any("crossed the bounded page window" in warning for warning in warnings)

    warnings = []
    kubernetes_units = diagnostics._redacted_file_units(
        b"kind: Secret\ndata:\n  opaque: kube-secret\n",
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=True,
        warnings=warnings,
    )
    assert "kube-secret" not in "".join(unit[1] for unit in kubernetes_units)
    assert any("Kubernetes Secret YAML payload crossed" in warning for warning in warnings)

    env_units = diagnostics._redacted_file_units(
        (
            b"- name: PASSWORD\n\n  # retained comment\n  value: yaml-env-secret\n"
            b"- value: reversed-yaml-env-secret\n  name: PASSWORD\n"
            b"-\n  value: standalone-yaml-env-secret\n  name: API_KEY\n"
            b"  extra: retained\n- name: SAFE\n  value: visible\n"
        ),
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=False,
        warnings=[],
    )
    env_content = "".join(unit[1] for unit in env_units)
    assert "yaml-env-secret" not in env_content
    assert "reversed-yaml-env-secret" not in env_content
    assert "standalone-yaml-env-secret" not in env_content
    assert "visible" in env_content

    fragment_units = diagnostics._redacted_file_units(
        b"value: fragment-yaml-env-secret\nname: PASSWORD\n---\nafter\n",
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=False,
        warnings=[],
    )
    fragment_content = "".join(unit[1] for unit in fragment_units)
    assert "fragment-yaml-env-secret" not in fragment_content
    assert "after" in fragment_content

    fieldless_item_units = diagnostics._redacted_file_units(
        b"-\n  command: run\nnext\n",
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=False,
        warnings=[],
    )
    assert "command: run" in "".join(unit[1] for unit in fieldless_item_units)

    warnings = []
    crossed_env_units = diagnostics._redacted_file_units(
        b"- value: bounded-window-secret\n",
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=True,
        warnings=warnings,
    )
    assert "bounded-window-secret" not in "".join(unit[1] for unit in crossed_env_units)
    assert any("YAML environment item crossed" in warning for warning in warnings)

    boundary_units = diagnostics._redacted_file_units(
        b"- name: PASSWORD\n- name: SAFE\n  value: visible\n",
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=False,
        warnings=[],
    )
    assert "visible" in "".join(unit[1] for unit in boundary_units)

    warnings = []
    assert diagnostics._redacted_file_units(
        b"visible\n",
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=False,
        warnings=warnings,
        starts_inside_kubernetes_secret=None,
    ) == [(b"visible\n", "[CONTENT OMITTED: KUBERNETES SECRET CONTEXT UNKNOWN]\n", 1)]
    assert any("Kubernetes Secret YAML context exceeded" in warning for warning in warnings)

    warnings = []
    starts_with_explicit = diagnostics._redacted_file_units(
        b": |\n  crossed-explicit-secret\n",
        starts_inside_private_key=False,
        starts_with_sensitive_value=True,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=True,
        warnings=warnings,
    )
    assert "crossed-explicit-secret" not in "".join(unit[1] for unit in starts_with_explicit)
    assert any("explicit YAML sensitive value crossed" in warning for warning in warnings)

    unmatched_explicit = diagnostics._redacted_file_units(
        b"? password\nordinary\n",
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=False,
        warnings=[],
    )
    assert "ordinary" in "".join(unit[1] for unit in unmatched_explicit)
    warnings = []
    crossed_explicit = diagnostics._redacted_file_units(
        b"? password\n: |\n  crossed-explicit-secret\n",
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=True,
        warnings=warnings,
    )
    assert "crossed-explicit-secret" not in "".join(unit[1] for unit in crossed_explicit)
    assert any("explicit YAML sensitive value crossed" in warning for warning in warnings)

    assert diagnostics._sensitive_state_before(BytesIO(b"\n\n"), 2, b"next\n") == (
        False,
        False,
        None,
        False,
        None,
        2,
    )
    assert diagnostics._kubernetes_yaml_state_before(BytesIO(b"\n\n"), 2, b"next\n") == (
        False,
        None,
        False,
        False,
        None,
        None,
        {},
        2,
    )
    assert diagnostics._kubernetes_yaml_payload_flags(
        [b"\n", b"- kind: ConfigMap\n"],
        starts_inside_secret=True,
        inherited_secret_scope=(0, True),
    ) == [True, False]
    env_context = b"    - name: PASSWORD\n\n"
    assert diagnostics._sensitive_state_before(
        BytesIO(env_context),
        len(env_context),
        b"      value: page-secret\n",
    )[0] is True
    non_name_context = b"    - command: run\n"
    assert diagnostics._sensitive_state_before(
        BytesIO(non_name_context),
        len(non_name_context),
        b"      value: visible\n",
    )[0] is False
    unmatched_explicit_context = b"? password\nordinary\n"
    assert diagnostics._sensitive_state_before(
        BytesIO(unmatched_explicit_context),
        len(unmatched_explicit_context),
        b"next\n",
    )[0] is False
    block_heavy_context = b"a: b\n" * (diagnostics._MAX_YAML_BLOCK_MAPPING_LINES + 1)
    assert diagnostics._sensitive_state_before(
        BytesIO(block_heavy_context),
        len(block_heavy_context),
        b"next\n",
    ) == (None, None, None, None, None, len(block_heavy_context))
    assert diagnostics._kubernetes_yaml_state_before(
        BytesIO(block_heavy_context),
        len(block_heavy_context),
        b"next\n",
    ) == (None, None, False, None, None, None, {}, len(block_heavy_context))
    context_limit = diagnostics._MAX_SENSITIVE_ASSIGNMENT_CONTEXT_BYTES
    monkeypatch.setattr(diagnostics, "_MAX_SENSITIVE_ASSIGNMENT_CONTEXT_BYTES", 4)
    assert diagnostics._kubernetes_yaml_state_before(BytesIO(b"abcde"), 5, b"next\n") == (
        None,
        None,
        False,
        None,
        None,
        None,
        {},
        4,
    )
    monkeypatch.setattr(diagnostics, "_MAX_SENSITIVE_ASSIGNMENT_CONTEXT_BYTES", 5)
    assert diagnostics._kubernetes_yaml_state_before(
        BytesIO(b"x\nfoo:"),
        6,
        b"  child: value\n",
    ) == (None, None, False, None, None, None, {}, 5)
    monkeypatch.setattr(
        diagnostics,
        "_MAX_SENSITIVE_ASSIGNMENT_CONTEXT_BYTES",
        context_limit,
    )
    completed_block = b"password: |\n  secret\nnext: value\n"
    assert diagnostics._sensitive_state_before(
        BytesIO(completed_block),
        len(completed_block),
        b"  current\n",
    ) == (False, False, None, False, None, len(completed_block))
    completed_quote = b'password: "first\n  second"\n'
    assert diagnostics._sensitive_state_before(
        BytesIO(completed_quote),
        len(completed_quote),
        b"next\n",
    ) == (False, False, None, False, None, len(completed_quote))
    unknown_context = b"x" * (diagnostics._MAX_SENSITIVE_ASSIGNMENT_CONTEXT_BYTES + 1)
    assert diagnostics._sensitive_state_before(BytesIO(unknown_context), len(unknown_context), b"  value\n") == (
        None,
        None,
        None,
        None,
        None,
        diagnostics._MAX_SENSITIVE_ASSIGNMENT_CONTEXT_BYTES,
    )
    ordinary_line = b"  ordinary\n"
    indeterminate_block = b"x\n" + ordinary_line * (
        diagnostics._MAX_SENSITIVE_ASSIGNMENT_CONTEXT_BYTES // len(ordinary_line) + 2
    )
    assert diagnostics._sensitive_state_before(
        BytesIO(indeterminate_block),
        len(indeterminate_block),
        b"  value\n",
    )[1] is None
    secret_line = b"column-zero-secret\n"
    distant_quote = b'PASSWORD="first\n' + secret_line * (
        diagnostics._MAX_SENSITIVE_ASSIGNMENT_CONTEXT_BYTES // len(secret_line) + 2
    )
    assert diagnostics._sensitive_state_before(
        BytesIO(distant_quote),
        len(distant_quote),
        b'last-secret"\n',
    )[3] is None
    continued_assignment = b"PASSWORD=first\\\nsecond\\\n"
    continued_state = diagnostics._sensitive_state_before(
        BytesIO(continued_assignment),
        len(continued_assignment),
        b"third\n",
    )
    assert continued_state[1:3] == (True, -1)
    unknown_continuation = b"x\\\n" * (
        diagnostics._MAX_SENSITIVE_ASSIGNMENT_CONTEXT_BYTES // len(b"x\\\n") + 2
    )
    assert diagnostics._sensitive_state_before(
        BytesIO(unknown_continuation),
        len(unknown_continuation),
        b"next\n",
    )[1] is None
    warnings = []
    crossed_continuation = diagnostics._redacted_file_units(
        b"PASSWORD=first-secret\\\n",
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=True,
        warnings=warnings,
    )
    assert crossed_continuation[0][1] == "PASSWORD=[REDACTED]\n"
    assert any("backslash-continued" in warning for warning in warnings)
    warnings = []
    assert diagnostics._redacted_file_units(
        b"unknown\n",
        starts_inside_private_key=False,
        starts_with_sensitive_value=None,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
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
        starts_inside_sensitive_quote=False,
        sensitive_quote=None,
        has_more_after_raw=False,
        warnings=warnings,
    )[0][1] == "[CONTENT OMITTED: SENSITIVE-BLOCK CONTEXT UNKNOWN]\n"
    warnings = []
    assert diagnostics._redacted_file_units(
        b"  unknown\n",
        starts_inside_private_key=False,
        starts_with_sensitive_value=False,
        starts_inside_sensitive_block=False,
        sensitive_block_indent=None,
        starts_inside_sensitive_quote=None,
        sensitive_quote=None,
        has_more_after_raw=False,
        warnings=warnings,
    )[0][1] == "[CONTENT OMITTED: SENSITIVE-QUOTE CONTEXT UNKNOWN]\n"


async def test_filesystem_reader_omits_oversized_segments_and_lines(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis, _config(_repo()))
    monkeypatch.setenv("PO_EVENTS_DIR", str(tmp_path / "events"))
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
    assert isinstance(truncates_line["pagination"]["next_cursor"], str)
    continued_parts = [truncates_line["content"]]
    continuation_cursor = truncates_line["pagination"]["next_cursor"]
    while continuation_cursor is not None:
        continuation_page = await diagnostics.read_orchestrator_log(
            SLUG,
            "ci:artifact",
            cursor=continuation_cursor,
            max_chars=10,
        )
        continued_parts.append(continuation_page["content"])
        continuation_cursor = continuation_page["pagination"]["next_cursor"]
    assert "".join(continued_parts) == "z" * 200 + "\n"

    stale_cursor = diagnostics._encode_file_cursor("ci:artifact", 0, 999)
    with pytest.raises(ValueError, match="no longer matches"):
        await diagnostics.read_orchestrator_log(SLUG, "ci:artifact", cursor=stale_cursor)
    with pytest.raises(ValueError, match="continuation cursor"):
        diagnostics._decode_file_cursor("not-a-cursor", "ci:artifact")
    with pytest.raises(ValueError, match="continuation cursor"):
        diagnostics._decode_file_cursor(f"{diagnostics._FILE_CURSOR_PREFIX}not-base64", "ci:artifact")

    with pytest.raises(ValueError, match="continuation cursor"):
        await diagnostics.read_orchestrator_log(
            SLUG,
            "events:disk/2026-10-05",
            cursor=truncates_line["pagination"]["next_cursor"],
        )
    with pytest.raises(ValueError, match="Redis source cursor"):
        await diagnostics.read_orchestrator_log(
            SLUG,
            "cli:latest",
            cursor=truncates_line["pagination"]["next_cursor"],
        )

    event_path = tmp_path / "events" / SLUG / "2026-10-05.jsonl"
    event_path.parent.mkdir(parents=True)
    event_path.write_text(json.dumps({"event": "x" * 200}) + "\n", encoding="utf-8")
    long_event = await diagnostics.read_orchestrator_log(
        SLUG,
        "events:disk/2026-10-05",
        max_chars=10,
    )
    assert isinstance(long_event["pagination"]["next_cursor"], str)

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
    assert set(compose["services"]) == {"web", "daemon", "mcp", "cloudflared", "redis"}
    service = compose["services"]["mcp"]
    assert "events-init" not in compose["services"]
    assert "mcp-tunnel" not in compose["services"]
    assert service["environment"]["REDIS_URL"] == "redis://redis:6379/0"
    assert service["environment"]["PO_EVENTS_DIR"] == "${PO_EVENTS_DIR:-/data/events}"
    assert compose["services"]["web"]["environment"]["PO_EVENTS_DIR"] == "${PO_EVENTS_DIR:-/data/events}"
    assert compose["services"]["daemon"]["environment"]["PO_EVENTS_DIR"] == "${PO_EVENTS_DIR:-/data/events}"
    assert service["depends_on"] == ["redis"]
    assert service["entrypoint"] == ["bash", "scripts/mcp-entrypoint.sh"]
    assert service["ports"] == ["127.0.0.1:${MCP_PORT:-5173}:5174"]
    for producer in ("web", "daemon"):
        event_mount = next(
            volume
            for volume in compose["services"][producer]["volumes"]
            if isinstance(volume, dict) and volume.get("target") == "${PO_EVENTS_DIR:-/data/events}"
        )
        assert event_mount["source"] == "${PO_EVENTS_HOST_DIR:-./data/events}"
        assert event_mount["bind"]["create_host_path"] is False
        assert event_mount.get("read_only", False) is False
    event_mount = next(
        volume
        for volume in service["volumes"]
        if isinstance(volume, dict) and volume.get("target") == "${PO_EVENTS_DIR:-/data/events}"
    )
    assert event_mount["source"] == "${PO_EVENTS_HOST_DIR:-./data/events}"
    assert event_mount["read_only"] is True
    assert event_mount["bind"]["create_host_path"] is False
    serialized_volumes = json.dumps(service["volumes"])
    assert "docker.sock" not in serialized_volumes
    assert "/data/auth" not in serialized_volumes
    assert compose["services"]["cloudflared"]["depends_on"] == ["mcp"]

    entrypoint = Path("scripts/mcp-entrypoint.sh").read_text(encoding="utf-8")
    assert "MCP_RUNTIME_DIAGNOSTICS=0 MCP_SERVER_PORT=5173" in entrypoint
    assert "MCP_RUNTIME_DIAGNOSTICS=1 MCP_SERVER_PORT=5174" in entrypoint
    assert "env -u REDIS_URL -u PO_EVENTS_DIR" in entrypoint
