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
from src.keyspace import cli_log_latest, pipeline_state, retry_command, retry_command_pending
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


async def test_latest_cli_log_available_empty_isolated_and_read_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(
        monkeypatch,
        redis,
        _config(_repo(), _repo("https://github.com/octo/other.git")),
    )
    key = cli_log_latest(SLUG)
    other_key = cli_log_latest(OTHER_SLUG)
    redis.store[key] = b""
    redis.store[other_key] = "other-repository-secret"
    redis.ttls[key] = 1_800

    result = await diagnostics.get_latest_cli_log(SLUG)

    assert result["observed_at"] == "2026-10-05T12:00:00Z"
    assert result["availability"] == {
        "status": "available",
        "code": None,
        "missing_may_mean_expired": False,
    }
    assert result["text"] == ""
    assert result["source_size_bytes"] == 0
    assert result["returned_size_bytes"] == 0
    assert result["ttl_seconds_remaining"] == 1_800
    assert result["expiry_status"] == "expires"
    assert result["source"] == {
        "kind": "latest_cli_log",
        "repo_slug": SLUG,
        "task_id": None,
        "invocation_id": None,
        "head_sha": None,
        "producer_timestamp": None,
        "association_status": "unavailable_legacy_record",
    }
    assert result["truncation"] == {
        "tail_truncated": False,
        "source_oversized": False,
        "producer_truncated": False,
        "omitted_prefix_bytes": 0,
    }
    assert result["read_only"] is True
    assert other_key not in [call_key for _, call_key in redis.calls]
    assert {operation for operation, _ in redis.calls} == {"strlen", "getrange", "exists", "ttl", "aclose"}
    assert redis.closed is True


async def test_latest_cli_log_validates_repo_tail_and_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis)
    with pytest.raises(ValueError, match="canonical"):
        await diagnostics.get_latest_cli_log("../secret")
    with pytest.raises(ValueError, match="not configured"):
        await diagnostics.get_latest_cli_log(OTHER_SLUG)
    with pytest.raises(ValueError, match="tail_bytes"):
        await diagnostics.get_latest_cli_log(SLUG, 0)
    with pytest.raises(ValueError, match="tail_bytes"):
        await diagnostics.get_latest_cli_log(SLUG, diagnostics._MAX_CLI_LOG_TAIL_BYTES + 1)
    with pytest.raises(ValueError, match="tail_bytes"):
        await diagnostics.get_latest_cli_log(SLUG, True)
    assert redis.calls == []

    monkeypatch.setattr(
        diagnostics,
        "_configured_repositories",
        lambda: (_ for _ in ()).throw(ValueError("api_key=config-secret")),
    )
    invalid_config = await diagnostics.get_latest_cli_log(SLUG)
    assert invalid_config["availability"]["code"] == "configuration_invalid"
    assert "config-secret" not in json.dumps(invalid_config)

    _patch_runtime(monkeypatch, redis)
    monkeypatch.setattr(
        diagnostics,
        "_new_redis_client",
        lambda: (_ for _ in ()).throw(ConnectionError("Authorization: Bearer connection-secret")),
    )
    unavailable = await diagnostics.get_latest_cli_log(SLUG)
    assert unavailable["availability"]["code"] == "redis_connection_failed"
    assert unavailable["ttl_seconds_remaining"] is None
    assert unavailable["expiry_status"] == "unknown"
    assert "connection-secret" not in json.dumps(unavailable)


async def test_latest_cli_log_missing_and_redis_read_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    missing_redis = FakeRedis()
    _patch_runtime(monkeypatch, missing_redis)
    missing = await diagnostics.get_latest_cli_log(SLUG)
    assert missing["availability"] == {
        "status": "missing",
        "code": "cli_log_missing_or_expired",
        "missing_may_mean_expired": True,
    }
    assert missing["text"] is None
    assert missing["expiry_status"] == "missing"
    assert missing["ttl_seconds_remaining"] is None

    read_failure_redis = FakeRedis()
    read_failure_redis.fail.add(("strlen", cli_log_latest(SLUG)))
    _patch_runtime(monkeypatch, read_failure_redis)
    failed = await diagnostics.get_latest_cli_log(SLUG)
    assert failed["availability"] == {
        "status": "unavailable",
        "code": "cli_log_read_failed",
        "missing_may_mean_expired": False,
    }
    assert "should-never-be-returned" not in json.dumps(failed)

    ttl_failure_redis = FakeRedis()
    ttl_failure_redis.store[cli_log_latest(SLUG)] = "available content"
    ttl_failure_redis.fail.add(("ttl", cli_log_latest(SLUG)))
    _patch_runtime(monkeypatch, ttl_failure_redis)
    ttl_failed = await diagnostics.get_latest_cli_log(SLUG)
    assert ttl_failed["availability"]["code"] == "cli_log_read_failed"
    assert ttl_failed["text"] is None


async def test_latest_cli_log_bounds_source_output_and_utf8(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    key = cli_log_latest(SLUG)
    maximum_redis = FakeRedis()
    maximum_redis.store[key] = ("�" * 3) + ("x" * (diagnostics._MAX_CLI_LOG_SOURCE_BYTES - 9))
    _patch_runtime(monkeypatch, maximum_redis)
    maximum = await diagnostics.get_latest_cli_log(SLUG)
    assert maximum["availability"]["status"] == "available"
    assert maximum["source_size_bytes"] == (64 * 1024) + 6
    assert maximum["truncation"]["tail_truncated"] is True

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis)
    redis.store[key] = b"x" * (diagnostics._MAX_CLI_LOG_SOURCE_BYTES + 1)
    redis.ttls[key] = 300
    oversized = await diagnostics.get_latest_cli_log(SLUG)
    assert oversized["availability"]["status"] == "oversized"
    assert oversized["availability"]["code"] == "cli_log_source_size_limit"
    assert oversized["source_size_bytes"] == diagnostics._MAX_CLI_LOG_SOURCE_BYTES + 1
    assert oversized["truncation"]["source_oversized"] is True
    assert oversized["text"] is None
    assert ("getrange", key) not in redis.calls

    growing = GrowingRedis()
    _patch_runtime(monkeypatch, growing)
    concurrently_oversized = await diagnostics.get_latest_cli_log(SLUG)
    assert concurrently_oversized["availability"]["status"] == "oversized"
    assert concurrently_oversized["source_size_bytes"] == diagnostics._MAX_CLI_LOG_SOURCE_BYTES + 1

    producer_truncated_redis = FakeRedis()
    producer_truncated_redis.store[key] = (
        "[truncated]\nopaque-value-without-credential-context\nsafe-tail"
    )
    producer_truncated_redis.ttls[key] = 300
    _patch_runtime(monkeypatch, producer_truncated_redis)
    producer_truncated = await diagnostics.get_latest_cli_log(SLUG)
    assert producer_truncated["availability"] == {
        "status": "unavailable",
        "code": "cli_log_producer_truncated",
        "missing_may_mean_expired": False,
    }
    assert producer_truncated["text"] is None
    assert producer_truncated["source_size_bytes"] == 61
    assert producer_truncated["truncation"]["producer_truncated"] is True
    assert producer_truncated["ttl_seconds_remaining"] == 300

    unicode_redis = FakeRedis()
    unicode_redis.store[key] = "prefix-" + ("🙂" * 10) + "-end"
    unicode_redis.ttls[key] = -1
    _patch_runtime(monkeypatch, unicode_redis)
    unicode_tail = await diagnostics.get_latest_cli_log(SLUG, 13)
    assert unicode_tail["text"].endswith("-end")
    assert "�" not in unicode_tail["text"]
    assert unicode_tail["returned_size_bytes"] <= 13
    assert unicode_tail["truncation"]["tail_truncated"] is True
    assert unicode_tail["truncation"]["omitted_prefix_bytes"] > 0
    assert unicode_tail["expiry_status"] == "persistent"

    invalid_utf8_redis = FakeRedis()
    invalid_utf8_redis.store[key] = b"before\xffafter"
    _patch_runtime(monkeypatch, invalid_utf8_redis)
    invalid_utf8 = await diagnostics.get_latest_cli_log(SLUG, 32)
    assert invalid_utf8["text"] == "before�after"

    malformed_json_redis = FakeRedis()
    malformed_json_redis.store[key] = b"[" * diagnostics._MAX_CLI_LOG_SOURCE_BYTES
    _patch_runtime(monkeypatch, malformed_json_redis)
    malformed_json = await diagnostics.get_latest_cli_log(SLUG)
    assert malformed_json["availability"]["status"] == "available"
    assert malformed_json["text"] == "[credential document omitted]"
    assert malformed_json["redaction"]["credential_documents_omitted"] == 1

    parse_failure_flood = "{]" * diagnostics._MAX_JSON_PARSE_FAILURES
    assert diagnostics._omit_json_credential_documents(parse_failure_flood) == (
        "[credential document omitted]",
        1,
    )

    fallback_classifications: list[int] = []

    def record_fallback_classification(container: str) -> bool:
        fallback_classifications.append(len(container))
        return False

    repeated_malformed = ("{a" * diagnostics._MAX_CLI_LOG_SOURCE_BYTES)[
        : diagnostics._MAX_CLI_LOG_SOURCE_BYTES
    ]
    with monkeypatch.context() as fallback_context:
        fallback_context.setattr(
            diagnostics,
            "_line_has_sensitive_context",
            record_fallback_classification,
        )
        bounded_json, bounded_omissions = diagnostics._omit_json_credential_documents(
            repeated_malformed
        )
    assert (bounded_json, bounded_omissions) == ("[credential document omitted]", 1)
    assert fallback_classifications == [diagnostics._MAX_CLI_LOG_SOURCE_BYTES]
    assert diagnostics._json_container_end("{", 0, 0) is None

    oversized_integer_redis = FakeRedis()
    oversized_integer_redis.store[key] = "[" + ("9" * 5_000) + "]"
    _patch_runtime(monkeypatch, oversized_integer_redis)
    oversized_integer = await diagnostics.get_latest_cli_log(SLUG)
    assert oversized_integer["availability"]["status"] == "available"
    assert oversized_integer["text"].startswith("[")
    assert oversized_integer["text"].endswith("]")

    mismatched_json_redis = FakeRedis()
    mismatched_json_redis.store[key] = "{]"
    _patch_runtime(monkeypatch, mismatched_json_redis)
    mismatched_json = await diagnostics.get_latest_cli_log(SLUG)
    assert mismatched_json["availability"]["status"] == "available"
    assert mismatched_json["text"] == "{]"
    assert diagnostics._omit_json_credential_documents('"{bad}') == ('"{bad}', 0)

    linear_scan_redis = FakeRedis()
    linear_scan_redis.store[key] = "a-" * (diagnostics._MAX_CLI_LOG_SOURCE_BYTES // 2)
    _patch_runtime(monkeypatch, linear_scan_redis)
    linear_scan = await diagnostics.get_latest_cli_log(SLUG, 32)
    assert linear_scan["availability"]["status"] == "available"
    assert linear_scan["text"] == "a-" * 16
    assert linear_scan["returned_size_bytes"] == 32

    pem_flood_redis = FakeRedis()
    pem_marker = "-----BEGIN PRIVATE KEY-----"
    pem_flood_redis.store[key] = (
        pem_marker * (diagnostics._MAX_CLI_LOG_SOURCE_BYTES // len(pem_marker) + 1)
    )[: diagnostics._MAX_CLI_LOG_SOURCE_BYTES]
    _patch_runtime(monkeypatch, pem_flood_redis)
    pem_flood = await diagnostics.get_latest_cli_log(SLUG)
    assert pem_flood["availability"]["status"] == "available"
    assert pem_flood["text"] == "[credential document omitted]"


async def test_latest_cli_log_redacts_before_tail_and_omits_credential_documents(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    _patch_runtime(monkeypatch, redis)
    key = cli_log_latest(SLUG)
    long_secret = "tail-fragment-" * 20
    redis.store[key] = f"Authorization: Bearer {long_secret}\nsafe-tail"
    before_tail = await diagnostics.get_latest_cli_log(SLUG, 64)
    assert before_tail["text"] == "[credential line omitted]\nsafe-tail"
    assert "tail-fragment" not in before_tail["text"]
    assert before_tail["truncation"]["tail_truncated"] is False

    deeply_nested: object = {"private_key": "deep-document-secret"}
    for _ in range(1_100):
        deeply_nested = {"nested": deeply_nested}
    bare_jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzZWNyZXQifQ.signatureValue"
    encoded_auth = "dXNlcjpTVVBFUlNFQ1JFVA=="
    standalone_basic = "dXNlcjpTVEFOREFMT05FLUJBU0lDLVNFQ1JFVA=="
    slack_webhook = "https://hooks.slack.com/services/T123/B456/" + ("A" * 24)
    aws_access_key = "AKIAABCDEFGHIJKLMNOP"
    credential_log = "\n".join(
        (
            "curl -H 'Authorization: ApiKey inline-auth-secret' https://example.test",
            "Cookie: session=cookie-secret; other=value",
            "curl -H 'Set-Cookie: session=inline-cookie-secret' https://example.test",
            'API_KEY="api assignment secret"',
            "oauthToken=oauth-secret",
            "Author: Synthetic User",
            "tokenizer: ready",
            "passwordless: enabled",
            "PWD=/synthetic/workspace",
            "https://url-user:url-password@example.test/path?access_token=query-secret",
            "curl https://user:split-url-first-secret-" + "\\",
            "split-url-second-secret@example.test/path",
            "safe-after-reconstructed-url",
            "https://example.test/?access%5Ftoken=encoded-query-secret",
            "https://example.test/?password[]=bracket-query-secret",
            "https://blob.example.test/c?sv=2024-01-01&sig=azure-sas-secret&se=2027-01-01",
            "https://blob.example.test/c?sv=2024-01-01&%73ig=encoded-azure-sas-secret",
            "X-Amz-Signature=aws-presigned-signature-secret",
            "X-Goog-Signature=google-presigned-signature-secret",
            "https://single-url-credential@example.test/path",
            "//network-user:network-password@example.test/path",
            "ghp_" + ("A" * 36),
            '{"safe": "value"}',
            'array-prefix=[{"private_key":"array-document-secret","project_id":"hidden-array-project"}]',
            'output: {"client_secret":"prefix-document-secret","client_id":"hidden-client"}',
            r'serialized="{\"private_key\":\"serialized-document-secret\",'
            r'\"project_id\":\"serialized-metadata\"}"',
            r'"\u007b\u0022password\u0022\u003a\u0022unicode-escaped-json-secret'
            r'\u0022\u007d"',
            json.dumps(deeply_nested),
            'structured={"headers":{"Authorization":"Bearer structured-auth-secret"}}',
            'structured-cookie={"Cookie":"session=structured-cookie-secret"}',
            '{"headers":[["Authorization","Bearer pair-auth-secret"],'
            '["Cookie","session=pair-cookie-secret"],'
            '["X-Safe","hidden-pair-metadata"]]}',
            "headers=[('Authorization', 'Bearer tuple-auth-secret'), "
            "('Cookie', 'session=tuple-cookie-secret')]",
            '{"dbPassword":"camel-db-secret","safe":"hidden-camel-metadata"}',
            '{"githubToken":"camel-token-secret"}',
            "DBPASSWORD=uppercase-compound-secret",
            "dbpassword=lowercase-compound-secret",
            "API key: whitespace-label-secret",
            "secret key = whitespace-secret-key",
            '"Secret access key": quoted-multiword-secret-key',
            "'api key': single-quoted-multiword-secret",
            "auth[password]=nested-bracket-secret",
            "AZURE_STORAGE_KEY=azure-storage-secret",
            "DefaultEndpointsProtocol=https;AccountName=demo;AccountKey=azure-account-secret",
            "Server=db.example.test;User ID=synthetic;Pwd=connection-pwd-secret",
            "safe-after-connection-pwd",
            'config["password"] = subscript-password-secret',
            "env['API_TOKEN']=subscript-token-secret",
            "SSH_KEY_PASSPHRASE=passphrase-assignment-secret",
            "tool --passphrase passphrase-option-secret",
            "jwt=jwt-assignment-secret",
            bare_jwt,
            slack_webhook,
            f'{{"auths":{{"registry":{{"auth":"{encoded_auth}"}}}}}}',
            f"_auth={encoded_auth}",
            "Bearer standalone-bearer-secret",
            f"Basic {standalone_basic}",
            'Digest username="user", realm="realm", nonce="abc", uri="/", '
            'response="digest-response-secret"',
            'tool --password cli-option-secret --token "quoted cli token"',
            'tool --pass"word" quoted-fragment-option-secret',
            "safe-after-quoted-option-name",
            'tool "--pass"word leading-quoted-fragment-option-secret',
            "safe-after-leading-quoted-option-name",
            "tool --pass$'word' dollar-single-quoted-option-secret",
            "safe-after-dollar-single-quoted-option-name",
            'tool --pass$"word" dollar-double-quoted-option-secret',
            "safe-after-dollar-double-quoted-option-name",
            r"tool --pass$'\x77ord' ansi-c-hex-option-secret",
            "safe-after-ansi-c-hex-option-name",
            r"tool --pass$'\u0077ord' ansi-c-unicode-option-secret",
            "safe-after-ansi-c-unicode-option-name",
            r"tool $'--pass\x77ord' whole-ansi-c-option-secret",
            "safe-after-whole-ansi-c-option-name",
            r"tool --pass\word same-line-escaped-option-secret",
            "safe-after-same-line-escaped-option-name",
            "tool --pass${ANY:+}word empty-parameter-option-secret",
            "safe-after-empty-parameter-option-name",
            "tool --pass$()word empty-command-option-secret",
            "safe-after-empty-command-option-name",
            "tool --pass``word empty-backtick-option-secret",
            "safe-after-empty-backtick-option-name",
            "cmd /c tool --pass^word same-line-caret-option-secret",
            "safe-after-same-line-caret-option-name",
            "tool --pass`word same-line-backtick-option-secret",
            "safe-after-same-line-backtick-option-name",
            "tool --password${IFS}generic-ifs-option-secret",
            "safe-after-generic-ifs-option",
            "tool --client-secret$IFS client-ifs-option-secret",
            "safe-after-client-ifs-option",
            "tool --pass$(true)word output-empty-command-option-secret",
            "safe-after-command-substitution-option",
            "tool --pass${UNSET}word ambiguous-parameter-option-secret",
            "safe-after-parameter-substitution-option",
            'EMPTY=; docker login --pass$EMPTY"word" bare-parameter-option-secret registry.example',
            "safe-after-bare-parameter-option",
            "tool --pass$1word positional-parameter-option-secret",
            "safe-after-positional-parameter-option",
            "tool --pass$?word special-parameter-option-secret",
            "safe-after-special-parameter-option",
            "tool --pass{w..w}ord brace-sequence-option-secret",
            "safe-after-brace-sequence-option",
            "tool --pass{w,w}ord brace-list-option-secret",
            "safe-after-brace-list-option",
            "set EMPTY=",
            "tool --pass%EMPTY%word cmd-variable-option-secret",
            "safe-after-cmd-variable-option",
            "tool --pass!EMPTY!word cmd-delayed-variable-option-secret",
            "safe-after-cmd-delayed-variable-option",
            "MYSQL_PWD=mysql-pwd-assignment-secret",
            "safe-after-mysql-pwd",
            "mysql -pmysql-attached-option-secret synthetic_db",
            "safe-after-mysql-attached-option",
            'mysqldump --host example.test -p"mysql-quoted-option-secret" synthetic_db',
            "safe-after-mysql-quoted-option",
            "docker login -p docker-password-option-secret registry.example.test",
            "safe-after-docker-password-option",
            "/usr/bin/docker login -p=docker-equals-password-secret registry.example.test",
            "safe-after-docker-equals-password-option",
            "docker login -pdocker-attached-password-secret registry.example.test",
            "safe-after-docker-attached-password-option",
            "docker login -'p' docker-fragmented-password-secret registry.example.test",
            "safe-after-docker-fragmented-password-option",
            "docker login '-p' docker-quoted-password-secret registry.example.test",
            "safe-after-docker-quoted-password-option",
            "az login --service-principal --username synthetic-app "
            "-p azure-login-password-secret --tenant synthetic-tenant",
            "safe-after-azure-login-password-option",
            "az login -'p' azure-fragmented-password-secret",
            "safe-after-azure-fragmented-password-option",
            "sshpass -p sshpass-separated-password-secret ssh synthetic@example.test",
            "safe-after-sshpass-separated-password-option",
            "/usr/bin/sshpass -psshpass-attached-password-secret ssh synthetic@example.test",
            "safe-after-sshpass-attached-password-option",
            "sshpass -'p' sshpass-fragmented-password-secret ssh synthetic@example.test",
            "safe-after-sshpass-fragmented-password-option",
            "aws configure set aws_secret_access_key aws-config-secret",
            "safe-after-aws-config-credential",
            "aws configure set aws_session_token aws-session-token-secret",
            "safe-after-aws-session-token",
            "aws --profile production configure set aws_secret_access_key aws-profile-secret",
            "safe-after-aws-profile-credential",
            "/usr/bin/aws configure set profile.synthetic.aws_access_key_id aws-access-id-secret",
            "safe-after-aws-access-id",
            "redis-cli -a redis-short-password-secret",
            "safe-after-redis-short-password",
            "redis-cli -aredis-attached-password-secret",
            "safe-after-redis-attached-password",
            "redis-cli -'a' redis-fragmented-password-secret",
            "safe-after-redis-fragmented-password",
            "redis-cli '-a' redis-quoted-password-secret",
            "safe-after-redis-quoted-password",
            "redis-cli --pass redis-separated-password-secret ping",
            "safe-after-redis-separated-password",
            "/usr/bin/redis-cli --pass=redis-long-password-secret ping",
            "safe-after-redis-long-password",
            "tool -pvisible-unrelated-option",
            "tool -avisible-unrelated-option",
            "tool -'p' visible-fragmented-unrelated-option",
            "az storage -p visible-unrelated-azure-option",
            "tool {alpha,beta} visible-standalone-brace-word",
            "curl --user alice:curl-user-secret https://example.test",
            "curl -u alice:curl-short-user-secret https://example.test",
            "curl -ualice:curl-attached-user-secret https://example.test",
            "curl --proxy-user bob:curl-proxy-user-secret https://example.test",
            "curl -U bob:curl-short-proxy-user-secret https://example.test",
            "curl -Ubob:curl-attached-proxy-secret https://example.test",
            "machine example.test login alice password netrc-password-secret",
            "machine other.example.test login bob",
            "password",
            "",
            "# synthetic netrc comment",
            "netrc-newline-password-secret",
            "safe-after-netrc-newline",
            "safe-before-aws-csv",
            "Access key ID,Secret access key",
            "ASIAABCDEFGHIJKLMNOP,aws-csv-secret-key",
            "safe-after-aws-csv",
            "safe-before-quoted-aws-csv",
            '"Access key ID","Secret access key"',
            '"AKIAABCDEFGHIJKLMNOP","quoted-aws-csv-secret-key"',
            "safe-after-quoted-aws-csv",
            "safe-before-bom-aws-csv",
            "\ufeffAccess key ID,Secret access key",
            "AKIAQRSTUVWXYZABCDEF,bom-aws-csv-secret-key",
            "safe-after-bom-aws-csv",
            "safe-before-kubernetes-secret",
            "---",
            "apiVersion: v1",
            "data:",
            "  .dockerconfigjson: kubernetes-dockerconfig-secret",
            "stringData:",
            "  arbitrary-name: kubernetes-stringdata-secret",
            "kind: Secret",
            "metadata:",
            "  name: credentials",
            "---",
            "apiVersion: v1",
            "kind: |-",
            "",
            "  Secret",
            "data:",
            "  arbitrary-name: kubernetes-block-kind-secret",
            "---",
            "apiVersion: v1",
            "kind: &resourceKind Secret",
            "data:",
            "  arbitrary-name: kubernetes-anchored-kind-secret",
            "---",
            "apiVersion: v1",
            "kind: !!str Secret",
            "data:",
            "  arbitrary-name: kubernetes-tagged-kind-secret",
            "---",
            "apiVersion: v1",
            "kind: ! Secret",
            "data:",
            "  arbitrary-name: kubernetes-nonspecific-tag-secret",
            "---",
            "apiVersion: v1",
            "kind: !<tag:yaml.org,2002:str> Secret",
            "data:",
            "  arbitrary-name: kubernetes-verbatim-tag-secret",
            "---",
            "apiVersion: v1",
            "kind:",
            "  Secret",
            "data:",
            "  arbitrary-name: kubernetes-multiline-kind-secret",
            "---",
            "apiVersion: v1",
            "kind: !!str",
            "  Secret",
            "data:",
            "  arbitrary-name: kubernetes-property-multiline-kind-secret",
            "---",
            "apiVersion: v1",
            "kind: &resourceKindMulti",
            "  !!str",
            "  Secret",
            "data:",
            "  arbitrary-name: kubernetes-multiple-property-lines-secret",
            "---",
            "apiVersion: v1",
            r'kind: "Sec\u0072et"',
            "data:",
            "  arbitrary-name: kubernetes-escaped-kind-secret",
            "---",
            "apiVersion: v1",
            '"kind": Secret',
            "data:",
            "  arbitrary-name: kubernetes-quoted-key-secret",
            "---",
            "apiVersion: v1",
            r'"k\u0069nd": Secret',
            "data:",
            "  arbitrary-name: kubernetes-escaped-key-secret",
            "---",
            "apiVersion: v1",
            "metadata:",
            "  annotations:",
            "    selected-kind: &resourceKindAlias Secret",
            "kind: *resourceKindAlias",
            "data:",
            "  arbitrary-name: kubernetes-aliased-kind-secret",
            "---",
            "apiVersion: v1",
            "&field kind: Secret",
            "data:",
            "  arbitrary-name: kubernetes-anchored-key-secret",
            "---",
            "apiVersion: v1",
            "!!str kind: Secret",
            "data:",
            "  arbitrary-name: kubernetes-tagged-key-secret",
            "---",
            "apiVersion: v1",
            "? kind",
            ": Secret",
            "data:",
            "  arbitrary-name: kubernetes-explicit-kind-secret",
            "---",
            "apiVersion: v1",
            "kind: List",
            "items:",
            "  - kind: Secret",
            "    data:",
            "      arbitrary-name: kubernetes-sequence-kind-secret",
            "---",
            r'"pass\x77ord": yaml-x-escape-secret',
            "---",
            r'{safe: visible, "pass\U00000077ord": yaml-u-escape-secret}',
            "---",
            "'pass''word': yaml-doubled-quote-key-secret",
            "safe-after-yaml-doubled-quote-key",
            "'public''label': safe-yaml-doubled-quote-visible",
            "---",
            "? password",
            ": explicit-yaml-credential-secret",
            "---",
            "field: &credentialKey password",
            "*credentialKey: alias-key-credential-secret",
            "---",
            "{data: {arbitrary: kubernetes-flow-secret}, kind: Secret}",
            "---",
            "{data: {arbitrary: kubernetes-flow-explicit-secret}, ? kind : Secret}",
            "---",
            r'{"k\u0069nd": "Sec\u0072et", data: {arbitrary: kubernetes-flow-escaped-secret}}',
            "---",
            "apiVersion: v1",
            "kind: ConfigMap",
            "data:",
            "  public: safe-after-kubernetes-secret",
            json.dumps(
                {
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "data": {"arbitrary-name": "kubernetes-json-secret"},
                }
            ),
            r'tool --password "abc\"escaped-option-secret" token="abc\"escaped-assignment-secret"',
            "PASSWORD = spaced-assignment-secret",
            "Authorization : Bearer spaced-header-secret",
            "\x1b[31mpassword=ansi-secret\x1b[0m",
            "pass\x1b[34mword=embedded-ansi-secret",
            "\x1b]0;title\x07password=osc-secret",
            "pass\x1bPterminal-data\x1b\\word=esc-dcs-secret",
            "pass\x1b_hidden\x1b\\word=apc-secret",
            "pass\x1b^hidden\x1b\\word=pm-secret",
            "pass\x1bXhidden\x1b\\word=sos-secret",
            "safe-\x1b]8;;https://example.test\x1b\\hyperlink\x1b]8;;\x1b\\-output",
            "pass\x9d0;title\x9cword=c1-osc-secret",
            "pass\x90terminal-data\x9cword=c1-dcs-secret",
            "passX\bword=backspace-secret",
            "visible\x00-control",
            "visible\x81-control-c1",
            r"visible\q-invalid-json-escape",
            "matched-bad={private_key:matched-container-secret}",
            '{"private_key":"same-line-document-secret"} password=same-line-trailing-secret',
            "password: |",
            "  yaml-multiline-secret",
            "safe-after-yaml",
            "password:",
            "- indentationless-yaml-secret",
            "safe-after-yaml-sequence",
            "password:",
            "",
            "  yaml-leading-blank-secret",
            "safe-after-yaml-blank",
            "password: |2-",
            "",
            "  numeric-yaml-block-secret",
            "safe-after-numeric-yaml-block",
            "password: >-2",
            "",
            "  reversed-numeric-yaml-block-secret",
            "safe-after-reversed-numeric-yaml-block",
            "tokens = [",
            '"toml-array-secret"',
            "]",
            "safe-after-toml-array",
            'password = """',
            'prefix " visible',
            "toml-triple-quoted-secret",
            '"""',
            "safe-after-toml-triple-quote",
            "password:",
            "# explanation",
            "",
            "  yaml-comment-line-secret",
            "safe-after-yaml-comment",
            "password: &credentialValue",
            "",
            "  yaml-anchor-property-secret",
            "safe-after-yaml-anchor-property",
            "password: !!str",
            "",
            "  yaml-tag-property-secret",
            "safe-after-yaml-tag-property",
            "PASSWORD=$(cat <<EOF)",
            "heredoc-secret",
            "EOF",
            ")",
            "safe-after-heredoc",
            "PASSWORD=$(cat <<123)",
            "digit-heredoc-secret",
            "123",
            ")",
            "safe-after-digit-heredoc",
            "PASSWORD=" + "\\",
            "shell-multiline-secret",
            "safe-after-shell",
            "PASS" + "\\",
            "WORD=split-assignment-name-secret",
            "safe-after-split-assignment-name",
            "--pass" + "\\",
            "word split-option-name-secret",
            "safe-after-split-option-name",
            'PASSWORD="alpha',
            'quoted-multiline-secret"',
            "safe-after-quote",
            "PASSWORD=(",
            "shell-array-secret",
            ")",
            "safe-after-shell-array",
            "PASSWORD=$( # unmatched comment close )",
            "printf '%s)' shell-command-secret",
            r"printf \) escaped-shell-secret",
            "printf $(",
            "nested-shell-group-secret",
            ")",
            "outer-shell-group-secret",
            ")",
            "safe-after-shell-command",
            'PASSWORD="$(echo foo"',
            "quoted-command-group-secret",
            '")"',
            "safe-after-quoted-command-group",
            'PASSWORD="${UNSET:-"',
            "quoted-parameter-group-secret",
            '"}"',
            "safe-after-quoted-parameter-group",
            'PASSWORD="`echo foo"',
            "quoted-backtick-group-secret",
            '"`"',
            "safe-after-quoted-backtick-group",
            "PASSWORD=${UNSET:-",
            "${OTHER:-",
            "nested-shell-brace-secret",
            "}",
            "outer-shell-brace-secret",
            "}",
            "safe-after-shell-brace",
            "PASSWORD=`",
            "printf backtick-shell-secret",
            "`",
            "safe-after-shell-backtick",
            "private_key=malformed-private-secret",
            "AWS_SECRET_ACCESS_KEY=aws-secret",
            aws_access_key,
            "{",
            '  "items": [',
            "    {",
            '      "refresh_token": "document-secret"',
            "    }",
            "  ],",
            '  "client_email": "private@example.test"',
            "}",
            '{"kty":"RSA","n":"public-modulus","e":"AQAB",'
            '"d":"jwk-rsa-private-secret"}',
            '{"kty":"EC","crv":"P-256","x":"public-x","y":"public-y",'
            '"d":"jwk-ec-private-secret"}',
            '{"kty":"OKP","crv":"Ed25519","x":"public-okp",'
            '"d":"jwk-okp-private-secret"}',
            '{"kty":"oct","k":"jwk-symmetric-private-secret"}',
            '{"kty":"RSA","n":"safe-public-jwk-modulus","e":"AQAB"}',
            "-----BEGIN PRIVATE KEY-----",
            "pem-document-secret",
            "-----END PRIVATE KEY-----",
            "---- BEGIN SSH2 ENCRYPTED PRIVATE KEY ----",
            "Comment: synthetic fixture",
            "ssh2-private-secret",
            "---- END SSH2 ENCRYPTED PRIVATE KEY ----",
            "safe-after-ssh2",
            "PuTTY-User-Key-File-1: ssh-rsa",
            "Encryption: none",
            "Public-Lines: 1",
            "cHVibGljLWtleQ==",
            "Private-Lines: 1",
            "putty-v1-private-secret",
            "Private-Hash: putty-v1-private-hash-secret",
            "safe-after-putty-v1",
            "PuTTY-User-Key-File-3: ssh-rsa",
            "Encryption: none",
            "Public-Lines: 1",
            "cHVibGljLWtleQ==",
            "Private-Lines: 1",
            "putty-private-secret",
            "Private-MAC: putty-private-mac-secret",
            "safe-after-putty",
            "safe-output",
            r'payload={\"private_key\":\"unwrapped-escaped-secret\"}',
            "{not-json",
        )
    )
    redis = FakeRedis()
    redis.store[key] = credential_log
    _patch_runtime(monkeypatch, redis)
    redacted = await diagnostics.get_latest_cli_log(SLUG, diagnostics._MAX_CLI_LOG_TAIL_BYTES)
    exported = redacted["text"]
    assert redacted["availability"]["status"] == "available"
    assert redacted["redaction"]["applied"] is True
    assert redacted["redaction"]["credential_documents_omitted"] >= 10
    assert "[credential line omitted]" in exported
    assert "safe-after-yaml" in exported
    assert "safe-after-yaml-sequence" in exported
    assert "safe-after-yaml-blank" in exported
    assert "safe-after-numeric-yaml-block" in exported
    assert "safe-after-reversed-numeric-yaml-block" in exported
    assert "safe-after-yaml-comment" in exported
    assert "safe-after-yaml-anchor-property" in exported
    assert "safe-after-yaml-tag-property" in exported
    assert "safe-after-heredoc" in exported
    assert "safe-after-digit-heredoc" in exported
    assert "safe-before-kubernetes-secret" in exported
    assert "safe-after-kubernetes-secret" in exported
    assert "safe-after-reconstructed-url" in exported
    assert "safe-after-connection-pwd" in exported
    assert "Author: Synthetic User" in exported
    assert "tokenizer: ready" in exported
    assert "passwordless: enabled" in exported
    assert "PWD=/synthetic/workspace" in exported
    assert "safe-after-yaml-doubled-quote-key" in exported
    assert "safe-yaml-doubled-quote-visible" in exported
    assert "safe-before-aws-csv" in exported
    assert "safe-after-aws-csv" in exported
    assert "safe-after-netrc-newline" in exported
    assert "safe-before-quoted-aws-csv" in exported
    assert "safe-after-quoted-aws-csv" in exported
    assert "safe-before-bom-aws-csv" in exported
    assert "safe-after-bom-aws-csv" in exported
    assert "safe-after-shell" in exported
    assert "safe-after-quoted-option-name" in exported
    assert "safe-after-leading-quoted-option-name" in exported
    assert "safe-after-dollar-single-quoted-option-name" in exported
    assert "safe-after-dollar-double-quoted-option-name" in exported
    assert "safe-after-ansi-c-hex-option-name" in exported
    assert "safe-after-ansi-c-unicode-option-name" in exported
    assert "safe-after-whole-ansi-c-option-name" in exported
    assert "safe-after-same-line-escaped-option-name" in exported
    assert "safe-after-empty-parameter-option-name" in exported
    assert "safe-after-empty-command-option-name" in exported
    assert "safe-after-empty-backtick-option-name" in exported
    assert "safe-after-same-line-caret-option-name" in exported
    assert "safe-after-same-line-backtick-option-name" in exported
    assert "safe-after-generic-ifs-option" in exported
    assert "safe-after-client-ifs-option" in exported
    assert "safe-after-command-substitution-option" in exported
    assert "safe-after-parameter-substitution-option" in exported
    assert "safe-after-bare-parameter-option" in exported
    assert "safe-after-positional-parameter-option" in exported
    assert "safe-after-special-parameter-option" in exported
    assert "safe-after-brace-sequence-option" in exported
    assert "safe-after-brace-list-option" in exported
    assert "safe-after-cmd-variable-option" in exported
    assert "safe-after-cmd-delayed-variable-option" in exported
    assert "safe-after-mysql-pwd" in exported
    assert "safe-after-mysql-attached-option" in exported
    assert "safe-after-mysql-quoted-option" in exported
    assert "safe-after-docker-password-option" in exported
    assert "safe-after-docker-equals-password-option" in exported
    assert "safe-after-docker-attached-password-option" in exported
    assert "safe-after-docker-fragmented-password-option" in exported
    assert "safe-after-docker-quoted-password-option" in exported
    assert "safe-after-azure-login-password-option" in exported
    assert "safe-after-azure-fragmented-password-option" in exported
    assert "safe-after-sshpass-separated-password-option" in exported
    assert "safe-after-sshpass-attached-password-option" in exported
    assert "safe-after-sshpass-fragmented-password-option" in exported
    assert "safe-after-aws-config-credential" in exported
    assert "safe-after-aws-session-token" in exported
    assert "safe-after-aws-profile-credential" in exported
    assert "safe-after-aws-access-id" in exported
    assert "safe-after-redis-short-password" in exported
    assert "safe-after-redis-attached-password" in exported
    assert "safe-after-redis-fragmented-password" in exported
    assert "safe-after-redis-quoted-password" in exported
    assert "safe-after-redis-separated-password" in exported
    assert "safe-after-redis-long-password" in exported
    assert "tool -pvisible-unrelated-option" in exported
    assert "tool -avisible-unrelated-option" in exported
    assert "tool -'p' visible-fragmented-unrelated-option" in exported
    assert "az storage -p visible-unrelated-azure-option" in exported
    assert "tool {alpha,beta} visible-standalone-brace-word" in exported
    assert "safe-after-toml-array" in exported
    assert "safe-after-toml-triple-quote" in exported
    assert "safe-after-split-assignment-name" in exported
    assert "safe-after-split-option-name" in exported
    assert "safe-after-quote" in exported
    assert "safe-after-shell-array" in exported
    assert "safe-after-shell-command" in exported
    assert "safe-after-quoted-command-group" in exported
    assert "safe-after-quoted-parameter-group" in exported
    assert "safe-after-quoted-backtick-group" in exported
    assert "safe-after-shell-brace" in exported
    assert "safe-after-shell-backtick" in exported
    assert "visible-control" in exported
    assert "visible-control-c1" in exported
    assert r"visible\q-invalid-json-escape" in exported
    assert "\x1b" not in exported
    assert "\x00" not in exported
    assert not any("\x80" <= character <= "\x9f" for character in exported)
    assert "safe-output" in exported
    assert "private@example.test" not in exported
    assert "safe-public-jwk-modulus" in exported
    assert "hidden-array-project" not in exported
    assert "hidden-client" not in exported
    assert "hidden-pair-metadata" not in exported
    assert "hidden-camel-metadata" not in exported
    assert "serialized-metadata" not in exported
    assert bare_jwt not in exported
    assert slack_webhook not in exported
    assert aws_access_key not in exported
    assert encoded_auth not in exported
    assert standalone_basic not in exported
    assert "safe-hyperlink-output" in exported
    assert "safe-after-ssh2" in exported
    assert "safe-after-putty-v1" in exported
    assert "safe-after-putty" in exported
    assert diagnostics._omit_kubernetes_secret_documents(
        "apiVersion: v1\nkind: Secret\ndata:\n  tls.key: source-end-secret"
    ) == ("[credential document omitted]", 1)
    assert diagnostics._yaml_single_quoted_sensitive_value_start("'unterminated") is None
    for secret in (
        "inline-auth-secret",
        "cookie-secret",
        "inline-cookie-secret",
        "api assignment secret",
        "oauth-secret",
        "url-user",
        "url-password",
        "split-url-first-secret-",
        "split-url-second-secret",
        "single-url-credential",
        "network-user",
        "network-password",
        "query-secret",
        "encoded-query-secret",
        "bracket-query-secret",
        "azure-sas-secret",
        "encoded-azure-sas-secret",
        "aws-presigned-signature-secret",
        "google-presigned-signature-secret",
        "array-document-secret",
        "prefix-document-secret",
        "serialized-document-secret",
        "unicode-escaped-json-secret",
        "deep-document-secret",
        "structured-auth-secret",
        "structured-cookie-secret",
        "pair-auth-secret",
        "pair-cookie-secret",
        "tuple-auth-secret",
        "tuple-cookie-secret",
        "camel-db-secret",
        "camel-token-secret",
        "uppercase-compound-secret",
        "lowercase-compound-secret",
        "whitespace-label-secret",
        "whitespace-secret-key",
        "quoted-multiword-secret-key",
        "single-quoted-multiword-secret",
        "nested-bracket-secret",
        "azure-storage-secret",
        "azure-account-secret",
        "connection-pwd-secret",
        "subscript-password-secret",
        "subscript-token-secret",
        "passphrase-assignment-secret",
        "passphrase-option-secret",
        "jwt-assignment-secret",
        "standalone-bearer-secret",
        "digest-response-secret",
        "cli-option-secret",
        "quoted-fragment-option-secret",
        "leading-quoted-fragment-option-secret",
        "dollar-single-quoted-option-secret",
        "dollar-double-quoted-option-secret",
        "ansi-c-hex-option-secret",
        "ansi-c-unicode-option-secret",
        "whole-ansi-c-option-secret",
        "same-line-escaped-option-secret",
        "empty-parameter-option-secret",
        "empty-command-option-secret",
        "empty-backtick-option-secret",
        "same-line-caret-option-secret",
        "same-line-backtick-option-secret",
        "generic-ifs-option-secret",
        "client-ifs-option-secret",
        "output-empty-command-option-secret",
        "ambiguous-parameter-option-secret",
        "bare-parameter-option-secret",
        "positional-parameter-option-secret",
        "special-parameter-option-secret",
        "brace-sequence-option-secret",
        "brace-list-option-secret",
        "cmd-variable-option-secret",
        "cmd-delayed-variable-option-secret",
        "mysql-pwd-assignment-secret",
        "mysql-attached-option-secret",
        "mysql-quoted-option-secret",
        "docker-password-option-secret",
        "docker-equals-password-secret",
        "docker-attached-password-secret",
        "docker-fragmented-password-secret",
        "docker-quoted-password-secret",
        "azure-login-password-secret",
        "azure-fragmented-password-secret",
        "sshpass-separated-password-secret",
        "sshpass-attached-password-secret",
        "sshpass-fragmented-password-secret",
        "aws-config-secret",
        "aws-session-token-secret",
        "aws-profile-secret",
        "aws-access-id-secret",
        "redis-short-password-secret",
        "redis-attached-password-secret",
        "redis-fragmented-password-secret",
        "redis-quoted-password-secret",
        "redis-separated-password-secret",
        "redis-long-password-secret",
        "curl-user-secret",
        "curl-short-user-secret",
        "curl-attached-user-secret",
        "curl-proxy-user-secret",
        "curl-short-proxy-user-secret",
        "curl-attached-proxy-secret",
        "netrc-password-secret",
        "netrc-newline-password-secret",
        "aws-csv-secret-key",
        "quoted-aws-csv-secret-key",
        "bom-aws-csv-secret-key",
        "kubernetes-dockerconfig-secret",
        "kubernetes-stringdata-secret",
        "kubernetes-json-secret",
        "kubernetes-block-kind-secret",
        "kubernetes-anchored-kind-secret",
        "kubernetes-tagged-kind-secret",
        "kubernetes-nonspecific-tag-secret",
        "kubernetes-verbatim-tag-secret",
        "kubernetes-multiline-kind-secret",
        "kubernetes-property-multiline-kind-secret",
        "kubernetes-multiple-property-lines-secret",
        "kubernetes-escaped-kind-secret",
        "kubernetes-quoted-key-secret",
        "kubernetes-escaped-key-secret",
        "kubernetes-aliased-kind-secret",
        "kubernetes-anchored-key-secret",
        "kubernetes-tagged-key-secret",
        "kubernetes-explicit-kind-secret",
        "kubernetes-sequence-kind-secret",
        "yaml-x-escape-secret",
        "yaml-u-escape-secret",
        "yaml-doubled-quote-key-secret",
        "explicit-yaml-credential-secret",
        "alias-key-credential-secret",
        "kubernetes-flow-secret",
        "kubernetes-flow-explicit-secret",
        "kubernetes-flow-escaped-secret",
        "quoted cli token",
        "escaped-option-secret",
        "escaped-assignment-secret",
        "unwrapped-escaped-secret",
        "spaced-assignment-secret",
        "spaced-header-secret",
        "ansi-secret",
        "embedded-ansi-secret",
        "osc-secret",
        "esc-dcs-secret",
        "apc-secret",
        "pm-secret",
        "sos-secret",
        "c1-osc-secret",
        "c1-dcs-secret",
        "backspace-secret",
        "matched-container-secret",
        "same-line-document-secret",
        "same-line-trailing-secret",
        "yaml-multiline-secret",
        "indentationless-yaml-secret",
        "yaml-leading-blank-secret",
        "numeric-yaml-block-secret",
        "reversed-numeric-yaml-block-secret",
        "toml-array-secret",
        "toml-triple-quoted-secret",
        "yaml-comment-line-secret",
        "yaml-anchor-property-secret",
        "yaml-tag-property-secret",
        "heredoc-secret",
        "digit-heredoc-secret",
        "shell-multiline-secret",
        "split-assignment-name-secret",
        "split-option-name-secret",
        "quoted-multiline-secret",
        "shell-array-secret",
        "shell-command-secret",
        "escaped-shell-secret",
        "nested-shell-group-secret",
        "outer-shell-group-secret",
        "quoted-command-group-secret",
        "quoted-parameter-group-secret",
        "quoted-backtick-group-secret",
        "nested-shell-brace-secret",
        "outer-shell-brace-secret",
        "backtick-shell-secret",
        "malformed-private-secret",
        "aws-secret",
        "document-secret",
        "jwk-rsa-private-secret",
        "jwk-ec-private-secret",
        "jwk-okp-private-secret",
        "jwk-symmetric-private-secret",
        "pem-document-secret",
        "ssh2-private-secret",
        "putty-v1-private-secret",
        "putty-v1-private-hash-secret",
        "putty-private-secret",
        "putty-private-mac-secret",
        "ghp_" + ("A" * 36),
    ):
        assert secret not in exported

    unsafe_terminal_cases = (
        ("passX\x1b[1Dword=cursor-secret", "cursor-secret"),
        ("passX\x9b1Dword=c1-cursor-secret", "c1-cursor-secret"),
        ("safe-carriage\rpassword=carriage-secret", "carriage-secret"),
        ("password=\rcarriage-value-secret", "carriage-value-secret"),
        ("Authorization:\rBearer carriage-auth-secret", "carriage-auth-secret"),
        ("-----BEGIN PRIVATE KEY-----\x1b[2K\npem-control-secret", "pem-control-secret"),
        ('PASSWORD="\x1b[2K\nmultiline-control-secret"', "multiline-control-secret"),
    )
    for payload, secret in unsafe_terminal_cases:
        unsafe_terminal_redis = FakeRedis()
        unsafe_terminal_redis.store[key] = f"safe-before\n{payload}\nsafe-after"
        _patch_runtime(monkeypatch, unsafe_terminal_redis)
        unsafe_terminal = await diagnostics.get_latest_cli_log(SLUG)
        assert unsafe_terminal["text"] == "safe-before\n[terminal control line omitted]"
        assert secret not in unsafe_terminal["text"]
        assert "safe-after" not in unsafe_terminal["text"]

    for payload, secret in (
        ("PASSWORD=(\nunterminated-array-secret", "unterminated-array-secret"),
        ("TOKENS=[\nunterminated-bracket-secret", "unterminated-bracket-secret"),
        ("PASSWORD=$(\nunterminated-command-secret", "unterminated-command-secret"),
        ("PASSWORD=${UNSET:-\nunterminated-brace-secret", "unterminated-brace-secret"),
        ("PASSWORD=`\nunterminated-backtick-secret", "unterminated-backtick-secret"),
    ):
        incomplete_group_redis = FakeRedis()
        incomplete_group_redis.store[key] = f"safe-before\n{payload}"
        _patch_runtime(monkeypatch, incomplete_group_redis)
        incomplete_group = await diagnostics.get_latest_cli_log(SLUG)
        ending = "" if payload.startswith("PASSWORD=`") else "\n"
        assert incomplete_group["text"] == f"safe-before\n[credential line omitted]{ending}"
        assert secret not in incomplete_group["text"]

    multiple_heredoc_redis = FakeRedis()
    multiple_heredoc_redis.store[key] = (
        "safe-before\nPASSWORD=$(cat <<FIRST <<SECOND)\n"
        "ignored-first-body\nFIRST\nmultiple-heredoc-secret\nSECOND\n)\nsafe-after"
    )
    _patch_runtime(monkeypatch, multiple_heredoc_redis)
    multiple_heredoc = await diagnostics.get_latest_cli_log(SLUG)
    assert multiple_heredoc["text"] == "safe-before\n[credential line omitted]\n"
    assert "multiple-heredoc-secret" not in multiple_heredoc["text"]
    assert "safe-after" not in multiple_heredoc["text"]

    orphaned_begin_redis = FakeRedis()
    orphaned_begin_redis.store[key] = (
        "safe-before\n-----BEGIN PRIVATE KEY-----\norphaned-private-secret"
    )
    _patch_runtime(monkeypatch, orphaned_begin_redis)
    orphaned_begin = await diagnostics.get_latest_cli_log(SLUG)
    assert orphaned_begin["text"] == "safe-before\n[credential document omitted]"
    assert "orphaned-private-secret" not in orphaned_begin["text"]

    orphaned_end_redis = FakeRedis()
    orphaned_end_redis.store[key] = (
        "orphaned-private-secret\n-----END PRIVATE KEY-----\nsafe-after"
    )
    _patch_runtime(monkeypatch, orphaned_end_redis)
    orphaned_end = await diagnostics.get_latest_cli_log(SLUG)
    assert orphaned_end["text"] == "[credential document omitted]\nsafe-after"
    assert "orphaned-private-secret" not in orphaned_end["text"]

    mismatched_pem_redis = FakeRedis()
    mismatched_pem_redis.store[key] = (
        "safe-before\n-----BEGIN PRIVATE KEY-----\n"
        "mismatched-before-secret\n-----END PGP PRIVATE KEY BLOCK-----\n"
        "mismatched-after-secret\n-----END PRIVATE KEY-----\nsafe-after"
    )
    _patch_runtime(monkeypatch, mismatched_pem_redis)
    mismatched_pem = await diagnostics.get_latest_cli_log(SLUG)
    assert mismatched_pem["text"] == "safe-before\n[credential document omitted]\nsafe-after"
    assert "mismatched-before-secret" not in mismatched_pem["text"]
    assert "mismatched-after-secret" not in mismatched_pem["text"]

    unclosed_mismatched_pem_redis = FakeRedis()
    unclosed_mismatched_pem_redis.store[key] = (
        "safe-before\n-----BEGIN PRIVATE KEY-----\n"
        "unclosed-before-secret\n-----END PGP PRIVATE KEY BLOCK-----\n"
        "unclosed-after-secret\nsafe-after"
    )
    _patch_runtime(monkeypatch, unclosed_mismatched_pem_redis)
    unclosed_mismatched_pem = await diagnostics.get_latest_cli_log(SLUG)
    assert unclosed_mismatched_pem["text"] == "safe-before\n[credential document omitted]"
    assert "unclosed-before-secret" not in unclosed_mismatched_pem["text"]
    assert "unclosed-after-secret" not in unclosed_mismatched_pem["text"]
    assert "safe-after" not in unclosed_mismatched_pem["text"]

    nested_pem_redis = FakeRedis()
    nested_pem_redis.store[key] = (
        "safe-before\n-----BEGIN PRIVATE KEY-----\n"
        "outer-private-secret\n-----BEGIN RSA PRIVATE KEY-----\n"
        "-----END PRIVATE KEY-----\ninner-private-secret"
    )
    _patch_runtime(monkeypatch, nested_pem_redis)
    nested_pem = await diagnostics.get_latest_cli_log(SLUG)
    assert nested_pem["text"] == "safe-before\n[credential document omitted]"
    assert "outer-private-secret" not in nested_pem["text"]
    assert "inner-private-secret" not in nested_pem["text"]

    closed_nested_pem_redis = FakeRedis()
    closed_nested_pem_redis.store[key] = (
        "safe-before\n-----BEGIN PRIVATE KEY-----\n"
        "-----BEGIN RSA PRIVATE KEY-----\nnested-private-secret\n"
        "-----END RSA PRIVATE KEY-----\n-----END PRIVATE KEY-----\nsafe-after"
    )
    _patch_runtime(monkeypatch, closed_nested_pem_redis)
    closed_nested_pem = await diagnostics.get_latest_cli_log(SLUG)
    assert closed_nested_pem["text"] == (
        "safe-before\n[credential document omitted]\nsafe-after"
    )
    assert "nested-private-secret" not in closed_nested_pem["text"]

    orphaned_ssh2_redis = FakeRedis()
    orphaned_ssh2_redis.store[key] = (
        "safe-before\n---- BEGIN SSH2 PRIVATE KEY ----\norphaned-ssh2-secret"
    )
    _patch_runtime(monkeypatch, orphaned_ssh2_redis)
    orphaned_ssh2 = await diagnostics.get_latest_cli_log(SLUG)
    assert orphaned_ssh2["text"] == "safe-before\n[credential document omitted]"
    assert "orphaned-ssh2-secret" not in orphaned_ssh2["text"]

    incomplete_putty_redis = FakeRedis()
    incomplete_putty_redis.store[key] = (
        "safe-before\nPuTTY-User-Key-File-2: ssh-rsa\n"
        "Private-Lines: 1\nincomplete-putty-secret"
    )
    _patch_runtime(monkeypatch, incomplete_putty_redis)
    incomplete_putty = await diagnostics.get_latest_cli_log(SLUG)
    assert incomplete_putty["text"] == "safe-before\n[credential document omitted]"
    assert "incomplete-putty-secret" not in incomplete_putty["text"]

    nested_putty_redis = FakeRedis()
    nested_putty_redis.store[key] = (
        "safe-before\nPuTTY-User-Key-File-3: ssh-rsa\nouter-putty-secret\n"
        "PuTTY-User-Key-File-2: ssh-rsa\ninner-putty-secret\n"
        "Private-MAC: synthetic-inner-mac\nouter-putty-after-secret\nsafe-after"
    )
    _patch_runtime(monkeypatch, nested_putty_redis)
    nested_putty = await diagnostics.get_latest_cli_log(SLUG)
    assert nested_putty["text"] == "safe-before\n[credential document omitted]"
    assert "outer-putty-secret" not in nested_putty["text"]
    assert "inner-putty-secret" not in nested_putty["text"]
    assert "outer-putty-after-secret" not in nested_putty["text"]
    assert "safe-after" not in nested_putty["text"]

    incomplete_json_redis = FakeRedis()
    incomplete_json_redis.store[key] = '{"private_key":\n"incomplete-document-secret"\n'
    _patch_runtime(monkeypatch, incomplete_json_redis)
    incomplete_json = await diagnostics.get_latest_cli_log(SLUG)
    assert incomplete_json["text"] == "[credential document omitted]"
    assert "incomplete-document-secret" not in incomplete_json["text"]

    split_key_json_redis = FakeRedis()
    split_key_json_redis.store[key] = '{"private_key"\n:\n"split-key-document-secret"\n'
    _patch_runtime(monkeypatch, split_key_json_redis)
    split_key_json = await diagnostics.get_latest_cli_log(SLUG)
    assert split_key_json["text"] == "[credential document omitted]"
    assert "split-key-document-secret" not in split_key_json["text"]

    duplicate_key_json_redis = FakeRedis()
    duplicate_key_json_redis.store[key] = (
        'safe-before\n{\n  "password": "duplicate-key-document-secret",\n'
        '  "password": null\n}\nsafe-after'
    )
    _patch_runtime(monkeypatch, duplicate_key_json_redis)
    duplicate_key_json = await diagnostics.get_latest_cli_log(SLUG)
    assert duplicate_key_json["text"] == (
        "safe-before\n[credential document omitted]\nsafe-after"
    )
    assert "duplicate-key-document-secret" not in duplicate_key_json["text"]

    escaped_key_json_redis = FakeRedis()
    escaped_key_json_redis.store[key] = (
        '{"pass\\u0077ord":\n"escaped-key-document-secret"\n'
    )
    _patch_runtime(monkeypatch, escaped_key_json_redis)
    escaped_key_json = await diagnostics.get_latest_cli_log(SLUG)
    assert escaped_key_json["text"] == "[credential document omitted]"
    assert "escaped-key-document-secret" not in escaped_key_json["text"]

    simple_escape_cases = (
        ("private\\/key", "slash-escaped-key-secret"),
        ("pass\\tword", "tab-escaped-key-secret"),
        ("pass\\nword", "newline-escaped-key-secret"),
    )
    for escaped_key, secret in simple_escape_cases:
        simple_escaped_key_redis = FakeRedis()
        simple_escaped_key_redis.store[key] = f'{{"{escaped_key}":\n"{secret}"\n'
        _patch_runtime(monkeypatch, simple_escaped_key_redis)
        simple_escaped_key = await diagnostics.get_latest_cli_log(SLUG)
        assert simple_escaped_key["text"] == "[credential document omitted]"
        assert secret not in simple_escaped_key["text"]

    unterminated_quote_redis = FakeRedis()
    unterminated_quote_redis.store[key] = 'safe-before\nPASSWORD="alpha\nunterminated-quote-secret'
    _patch_runtime(monkeypatch, unterminated_quote_redis)
    unterminated_quote = await diagnostics.get_latest_cli_log(SLUG)
    assert unterminated_quote["text"] == "safe-before\n[credential line omitted]\n"
    assert "unterminated-quote-secret" not in unterminated_quote["text"]

    incomplete_inline_redis = FakeRedis()
    incomplete_inline_redis.store[key] = 'safe-before\npassword="alpha unterminated-credential-secret'
    _patch_runtime(monkeypatch, incomplete_inline_redis)
    incomplete_inline = await diagnostics.get_latest_cli_log(SLUG)
    assert incomplete_inline["text"] == "safe-before\n[credential line omitted]"
    assert "unterminated-credential-secret" not in incomplete_inline["text"]

    unterminated_heredoc_redis = FakeRedis()
    unterminated_heredoc_redis.store[key] = (
        "safe-before\nPASSWORD=$(cat <<'EOF')\nunterminated-heredoc-secret"
    )
    _patch_runtime(monkeypatch, unterminated_heredoc_redis)
    unterminated_heredoc = await diagnostics.get_latest_cli_log(SLUG)
    assert unterminated_heredoc["text"] == "safe-before\n[credential line omitted]\n"
    assert "unterminated-heredoc-secret" not in unterminated_heredoc["text"]

    unsupported_heredoc_redis = FakeRedis()
    unsupported_heredoc_redis.store[key] = (
        "safe-before\nPASSWORD=$(cat <<EOF$SUFFIX)\nunsupported-heredoc-secret\n"
        "EOF\nstill-sensitive-after-prefix"
    )
    _patch_runtime(monkeypatch, unsupported_heredoc_redis)
    unsupported_heredoc = await diagnostics.get_latest_cli_log(SLUG)
    assert unsupported_heredoc["text"] == "safe-before\n[credential line omitted]\n"
    assert "unsupported-heredoc-secret" not in unsupported_heredoc["text"]
    assert "still-sensitive-after-prefix" not in unsupported_heredoc["text"]


async def test_latest_cli_log_redacts_bundled_curl_urls_and_structured_pairs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    key = cli_log_latest(SLUG)
    redis = FakeRedis()
    redis.store[key] = "\n".join(
        (
            "curl -suuser:SYNTHETIC_BUNDLED_USER_SECRET https://example.test",
            "safe-after-bundled-user",
            "curl -vUproxy:SYNTHETIC_BUNDLED_PROXY_SECRET https://example.test",
            "safe-after-bundled-proxy",
            "curl -0uuser:SYNTHETIC_NUMERIC_BUNDLED_SECRET https://example.test",
            "safe-after-numeric-bundled-user",
            "curl -#Uproxy:SYNTHETIC_SYMBOL_BUNDLED_SECRET https://example.test",
            "safe-after-symbol-bundled-proxy",
            "curl '-uuser:SYNTHETIC_QUOTED_CURL_SECRET' https://example.test",
            "safe-after-quoted-curl",
            "curl $'-uuser:SYNTHETIC_ANSI_QUOTED_CURL_SECRET' https://example.test",
            "safe-after-ansi-quoted-curl",
            "curl '--user' user:SYNTHETIC_QUOTED_LONG_CURL_SECRET https://example.test",
            "safe-after-quoted-long-curl",
            "curl $'--proxy-user' proxy:SYNTHETIC_ANSI_LONG_CURL_SECRET https://example.test",
            "safe-after-ansi-long-curl",
            'curl --u"ser" user:SYNTHETIC_FRAGMENTED_LONG_CURL_SECRET https://example.test',
            "safe-after-fragmented-long-curl",
            "curl --proxy-'user' proxy:SYNTHETIC_FRAGMENTED_PROXY_CURL_SECRET https://example.test",
            "safe-after-fragmented-proxy-curl",
            "curl --user${IFS}user:SYNTHETIC_IFS_CURL_SECRET https://example.test",
            "safe-after-ifs-curl",
            "curl --user$IFS user:SYNTHETIC_BARE_IFS_CURL_SECRET https://example.test",
            "safe-after-bare-ifs-curl",
            "curl --proxy-user${IFS:0:1}proxy:SYNTHETIC_IFS_PROXY_CURL_SECRET https://example.test",
            "safe-after-ifs-proxy-curl",
            'echo ghp_ABCDEFGHIJ"KLMNOPQRSTUVWXYZ0123456789"',
            "safe-after-fragmented-token",
            r"echo ghp_ABCDEFGHIJ\KLMNOPQRSTUVWXYZ0123456789",
            "safe-after-escaped-token",
            "echo ghp_ABCDEFGHIJ^KLMNOPQRSTUVWXYZ0123456789",
            "safe-after-caret-token",
            "echo ghp_ABCDEFGHIJ${ANY:+}KLMNOPQRSTUVWXYZ0123456789",
            "safe-after-empty-expansion-token",
            "echo ghp_ABCDEFGHIJ`KLMNOPQRSTUVWXYZ0123456789",
            "safe-after-backtick-token",
            r"echo $'ghp_ABCDEFGHIJ\x4bLMNOPQRSTUVWXYZ0123456789'",
            "safe-after-ansi-c-token",
            "echo ghp_ABCDEFGHIJ$(true)KLMNOPQRSTUVWXYZ0123456789",
            "safe-after-command-substitution-token",
            "echo ghp_ABCDEFGHIJ%EMPTY%KLMNOPQRSTUVWXYZ0123456789",
            "safe-after-cmd-variable-token",
            'curl https://blob.test/?sv=1\'&\'si"g"=SYNTHETIC_QUOTED_QUERY_SECRET',
            "safe-after-quoted-query",
            "https://user:SYNTHETIC_URL_FIRST@SYNTHETIC_URL_SECOND@example.test/path",
            "safe-after-url",
            "('X-Api-Key',",
            "'SYNTHETIC_STRUCTURED_PAIR_SECRET')",
            "safe-after-pair",
        )
    )
    _patch_runtime(monkeypatch, redis)

    result = await diagnostics.get_latest_cli_log(SLUG)

    assert result["availability"]["status"] == "available"
    assert "https://[REDACTED]@example.test/path" in result["text"]
    assert "safe-after-bundled-user" in result["text"]
    assert "safe-after-bundled-proxy" in result["text"]
    assert "safe-after-numeric-bundled-user" in result["text"]
    assert "safe-after-symbol-bundled-proxy" in result["text"]
    assert "safe-after-quoted-curl" in result["text"]
    assert "safe-after-ansi-quoted-curl" in result["text"]
    assert "safe-after-quoted-long-curl" in result["text"]
    assert "safe-after-ansi-long-curl" in result["text"]
    assert "safe-after-fragmented-long-curl" in result["text"]
    assert "safe-after-fragmented-proxy-curl" in result["text"]
    assert "safe-after-ifs-curl" in result["text"]
    assert "safe-after-bare-ifs-curl" in result["text"]
    assert "safe-after-ifs-proxy-curl" in result["text"]
    assert "safe-after-fragmented-token" in result["text"]
    assert "safe-after-escaped-token" in result["text"]
    assert "safe-after-caret-token" in result["text"]
    assert "safe-after-empty-expansion-token" in result["text"]
    assert "safe-after-backtick-token" in result["text"]
    assert "safe-after-ansi-c-token" in result["text"]
    assert "safe-after-command-substitution-token" in result["text"]
    assert "safe-after-cmd-variable-token" in result["text"]
    assert r"\x4b" not in result["text"]
    assert "safe-after-quoted-query" in result["text"]
    assert "safe-after-url" in result["text"]
    assert "safe-after-pair" in result["text"]
    assert "ghp_ABCDEFGHIJ" not in result["text"]
    assert "KLMNOPQRSTUVWXYZ0123456789" not in result["text"]
    assert "SYNTHETIC_QUOTED_QUERY_SECRET" not in result["text"]
    for secret in (
        "SYNTHETIC_BUNDLED_USER_SECRET",
        "SYNTHETIC_BUNDLED_PROXY_SECRET",
        "SYNTHETIC_NUMERIC_BUNDLED_SECRET",
        "SYNTHETIC_SYMBOL_BUNDLED_SECRET",
        "SYNTHETIC_QUOTED_CURL_SECRET",
        "SYNTHETIC_ANSI_QUOTED_CURL_SECRET",
        "SYNTHETIC_QUOTED_LONG_CURL_SECRET",
        "SYNTHETIC_ANSI_LONG_CURL_SECRET",
        "SYNTHETIC_FRAGMENTED_LONG_CURL_SECRET",
        "SYNTHETIC_FRAGMENTED_PROXY_CURL_SECRET",
        "SYNTHETIC_IFS_CURL_SECRET",
        "SYNTHETIC_BARE_IFS_CURL_SECRET",
        "SYNTHETIC_IFS_PROXY_CURL_SECRET",
        "SYNTHETIC_URL_FIRST",
        "SYNTHETIC_URL_SECOND",
        "SYNTHETIC_STRUCTURED_PAIR_SECRET",
    ):
        assert secret not in result["text"]

    incomplete_ansi_c_redis = FakeRedis()
    incomplete_ansi_c_redis.store[key] = r"echo $'ghp_ABCDEFGHIJ\x4bLMNOPQRSTUVWXYZ0123456789"
    _patch_runtime(monkeypatch, incomplete_ansi_c_redis)
    incomplete_ansi_c = await diagnostics.get_latest_cli_log(SLUG)
    assert incomplete_ansi_c["text"] == "[credential line omitted]"


async def test_latest_cli_log_omits_embedded_json_and_powershell_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    key = cli_log_latest(SLUG)
    redis = FakeRedis()
    redis.store[key] = "\n".join(
        (
            "safe-before-json",
            r'{"message":"response: \u007b\u0022password\u0022\u003a'
            r'\u0022SYNTHETIC_EMBEDDED_UNICODE_SECRET\u0022\u007d"}',
            "safe-after-json",
            "Connect-Service -ClientSecret SYNTHETIC_POWERSHELL_SECRET",
            "safe-after-powershell",
            "Connect-Service -ClientSec`",
            "ret SYNTHETIC_SPLIT_POWERSHELL_SECRET",
            "safe-after-split-powershell",
            "Connect-Service -Pass SYNTHETIC_ABBREVIATED_PASS_SECRET",
            "safe-after-abbreviated-pass",
            "Connect-Service -ClientSec SYNTHETIC_ABBREVIATED_CLIENT_SECRET",
            "safe-after-abbreviated-client",
            "cmd /c tool --pass^",
            "word SYNTHETIC_CMD_CONTINUATION_SECRET",
            "safe-after-cmd-continuation",
        )
    )
    _patch_runtime(monkeypatch, redis)

    result = await diagnostics.get_latest_cli_log(SLUG)

    assert result["availability"]["status"] == "available"
    assert result["text"] == (
        'safe-before-json\n{"message":[credential document omitted]}\n'
        "safe-after-json\n"
        "[credential line omitted]\nsafe-after-powershell\n"
        "[credential line omitted]\nsafe-after-split-powershell\n"
        "[credential line omitted]\nsafe-after-abbreviated-pass\n"
        "[credential line omitted]\nsafe-after-abbreviated-client\n"
        "[credential line omitted]\nsafe-after-cmd-continuation"
    )
    assert "SYNTHETIC_EMBEDDED_UNICODE_SECRET" not in result["text"]
    assert "SYNTHETIC_POWERSHELL_SECRET" not in result["text"]
    assert "SYNTHETIC_SPLIT_POWERSHELL_SECRET" not in result["text"]
    assert "SYNTHETIC_ABBREVIATED_PASS_SECRET" not in result["text"]
    assert "SYNTHETIC_ABBREVIATED_CLIENT_SECRET" not in result["text"]
    assert "SYNTHETIC_CMD_CONTINUATION_SECRET" not in result["text"]
    assert diagnostics._contains_credential_document_key(
        "{" + ("x" * diagnostics._MAX_CLI_LOG_SOURCE_BYTES)
    )
    assert not diagnostics._contains_credential_document_key("message: {not-json}")


async def test_latest_cli_log_omits_ambiguous_yaml_credential_documents(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    key = cli_log_latest(SLUG)
    supplied_cases = (
        "password: # explanation\n\n  SYNTHETIC_SECRET\nsafe: visible",
        "defaults: &value SYNTHETIC_SECRET\npassword: *value\nsafe: visible",
        '? "pass\\\n  word"\n: SYNTHETIC_SECRET\nsafe: visible',
        '{? "pass\\\n  word": SYNTHETIC_SECRET}\nsafe: visible',
        "? 'pass\n  word'\n: SYNTHETIC_SECRET\nsafe: visible",
        "? pass\n  word\n: SYNTHETIC_SECRET\nsafe: visible",
        "{? pass\n  word: SYNTHETIC_SECRET}\nsafe: visible",
        "? |-\n  password\n: SYNTHETIC_SECRET\nsafe: visible",
    )
    for payload in supplied_cases:
        supplied_redis = FakeRedis()
        supplied_redis.store[key] = payload
        _patch_runtime(monkeypatch, supplied_redis)
        supplied = await diagnostics.get_latest_cli_log(SLUG)
        assert supplied["text"] == "[credential document omitted]"
        assert "SYNTHETIC_SECRET" not in supplied["text"]

    redis = FakeRedis()
    redis.store[key] = "\n".join(
        (
            "safe: before",
            "---",
            "password: # explanation",
            "",
            "  SYNTHETIC_COMMENT_SECRET",
            "---",
            "defaults: &before SYNTHETIC_BEFORE_ALIAS_SECRET",
            "password: *before",
            "---",
            "password: *after",
            "defaults: &after SYNTHETIC_AFTER_ALIAS_SECRET",
            "---",
            "defaults:",
            "  nested: &nested SYNTHETIC_NESTED_ALIAS_SECRET",
            "password:",
            "  nested: *nested",
            "---",
            "password: [",
            "SYNTHETIC_FLOW_COLLECTION_SECRET",
            "]",
            "---",
            '? "pass\\',
            '  word"',
            ": SYNTHETIC_MULTILINE_EXPLICIT_KEY_SECRET",
            "---",
            "? 'pa''ss",
            "  word'",
            ": SYNTHETIC_SINGLE_QUOTED_EXPLICIT_KEY_SECRET",
            "---",
            "? pass",
            "  word",
            ": SYNTHETIC_PLAIN_MULTILINE_EXPLICIT_KEY_SECRET",
            "---",
            '{? "pass\\',
            '  word": SYNTHETIC_FLOW_MULTILINE_EXPLICIT_KEY_SECRET}',
            "---",
            "{? pass",
            "  word: SYNTHETIC_FLOW_PLAIN_CONTINUATION_SECRET}",
            "---",
            "? >-2",
            "  pass",
            "  word",
            ": SYNTHETIC_BLOCK_EXPLICIT_KEY_SECRET",
            "---",
            "safe: visible",
            "? 'public label'",
            ": safe-explicit-visible",
            "safe-block: |-",
            "  safe-block-visible",
        )
    )
    _patch_runtime(monkeypatch, redis)

    result = await diagnostics.get_latest_cli_log(SLUG, diagnostics._MAX_CLI_LOG_TAIL_BYTES)

    assert result["availability"]["status"] == "available"
    assert result["text"].count("[credential document omitted]") == 11
    assert "safe: before" in result["text"]
    assert "safe: visible" in result["text"]
    assert "safe-explicit-visible" in result["text"]
    assert "safe-block-visible" in result["text"]
    for secret in (
        "SYNTHETIC_COMMENT_SECRET",
        "SYNTHETIC_BEFORE_ALIAS_SECRET",
        "SYNTHETIC_AFTER_ALIAS_SECRET",
        "SYNTHETIC_NESTED_ALIAS_SECRET",
        "SYNTHETIC_FLOW_COLLECTION_SECRET",
        "SYNTHETIC_MULTILINE_EXPLICIT_KEY_SECRET",
        "SYNTHETIC_SINGLE_QUOTED_EXPLICIT_KEY_SECRET",
        "SYNTHETIC_PLAIN_MULTILINE_EXPLICIT_KEY_SECRET",
        "SYNTHETIC_FLOW_MULTILINE_EXPLICIT_KEY_SECRET",
        "SYNTHETIC_FLOW_PLAIN_CONTINUATION_SECRET",
        "SYNTHETIC_BLOCK_EXPLICIT_KEY_SECRET",
    ):
        assert secret not in result["text"]

    ambiguous_redis = FakeRedis()
    ambiguous_redis.store[key] = "\n".join(
        (
            "defaults: &value SYNTHETIC_AMBIGUOUS_SECRET",
            "password: *value",
            "safe: not-exported-without-boundary",
        )
    )
    _patch_runtime(monkeypatch, ambiguous_redis)

    ambiguous = await diagnostics.get_latest_cli_log(SLUG)

    assert ambiguous["text"] == "[credential document omitted]"
    assert "SYNTHETIC_AMBIGUOUS_SECRET" not in ambiguous["text"]
    assert "safe: not-exported-without-boundary" not in ambiguous["text"]

    plain_flood = "? pass\n" + ("  word\n" * 4_000)
    assert not diagnostics._has_multiline_explicit_plain_yaml_key(
        plain_flood, 0, len(plain_flood)
    )
    interrupted_plain = "? public\n\n# comment\nsafe: visible"
    assert not diagnostics._has_multiline_explicit_plain_yaml_key(
        interrupted_plain, 0, len(interrupted_plain)
    )


async def test_latest_cli_log_omits_xml_credential_contexts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    key = cli_log_latest(SLUG)
    redis = FakeRedis()
    redis.store[key] = "\n".join(
        (
            "<password>SYNTHETIC_XML_ELEMENT_SECRET</password>",
            "safe-between: visible",
            '<add key="ClearTextPassword" value="SYNTHETIC_XML_PAIR_SECRET"/>',
            '<add key="Pass&#x77;ord" value="SYNTHETIC_XML_ENTITY_SECRET"/>',
            '<add key="&pw;" value="SYNTHETIC_XML_INTERNAL_ENTITY_SECRET"/>',
            "<key>Password</key><string>SYNTHETIC_XML_PLIST_SECRET</string>",
            "<key><![CDATA[Password]]></key>"
            "<string>SYNTHETIC_XML_CDATA_PLIST_SECRET</string>",
            "<key>Pass<!-- synthetic note -->word</key>"
            "<string>SYNTHETIC_XML_COMMENT_PLIST_SECRET</string>",
            '<name type="setting">Password</name>'
            '<value format="text">SYNTHETIC_XML_ATTRIBUTE_PLIST_SECRET</value>',
            "<key>Visible</key><string>safe-plist-visible</string>",
            '<name type="set>ting">Visible</name>'
            '<value format="text">safe-attributed-plist-visible</value>',
            "safe-after: visible",
            "<cfg:connection cfg:password='SYNTHETIC_XML_ATTRIBUTE_SECRET'/>",
            "<add value='SYNTHETIC_XML_ORDER_SECRET' name='apiToken'/>",
            "<add key = password value = SYNTHETIC_XML_UNQUOTED_SECRET/>",
            "<safe ignored attr='visible'>visible</safe>",
            "not xml <broken! visible",
        )
    )
    _patch_runtime(monkeypatch, redis)

    result = await diagnostics.get_latest_cli_log(SLUG)

    assert result["availability"]["status"] == "available"
    assert result["text"].count("[credential document omitted]") == 11
    assert "safe-between: visible" in result["text"]
    assert "safe-after: visible" in result["text"]
    assert "<safe ignored attr='visible'>visible</safe>" in result["text"]
    assert "safe-plist-visible" in result["text"]
    assert "safe-attributed-plist-visible" in result["text"]
    assert "not xml <broken! visible" in result["text"]
    for secret in (
        "SYNTHETIC_XML_ELEMENT_SECRET",
        "SYNTHETIC_XML_PAIR_SECRET",
        "SYNTHETIC_XML_ENTITY_SECRET",
        "SYNTHETIC_XML_INTERNAL_ENTITY_SECRET",
        "SYNTHETIC_XML_PLIST_SECRET",
        "SYNTHETIC_XML_CDATA_PLIST_SECRET",
        "SYNTHETIC_XML_COMMENT_PLIST_SECRET",
        "SYNTHETIC_XML_ATTRIBUTE_PLIST_SECRET",
        "SYNTHETIC_XML_ATTRIBUTE_SECRET",
        "SYNTHETIC_XML_ORDER_SECRET",
        "SYNTHETIC_XML_UNQUOTED_SECRET",
    ):
        assert secret not in result["text"]

    fail_closed_cases = (
        "safe-before\n<password>\nSYNTHETIC_XML_MULTILINE_SECRET\n</password>\nsafe-after",
        "safe-before\n<password><!-- </password> -->\n"
        "SYNTHETIC_XML_COMMENT_CLOSE_SECRET\n</password>\nsafe-after",
        "safe-before\n<password><![CDATA[</password>]]>\n"
        "SYNTHETIC_XML_CDATA_CLOSE_SECRET\n</password>\nsafe-after",
        "safe-before\n<password><!-- incomplete\n"
        "SYNTHETIC_XML_INCOMPLETE_COMMENT_SECRET\n</password>\nsafe-after",
        "safe-before\n<password><![CDATA[incomplete\n"
        "SYNTHETIC_XML_INCOMPLETE_CDATA_SECRET\n</password>\nsafe-after",
        "safe-before\n<password><!UNKNOWN></password>\n"
        "SYNTHETIC_XML_UNKNOWN_MARKUP_SECRET\nsafe-after",
        "safe-before\n<password><value\n format='text'>"
        "SYNTHETIC_XML_CROSS_LINE_TAG_SECRET</value></password>\nsafe-after",
        "safe-before\n<Password>decoy</password>\n"
        "SYNTHETIC_XML_CASE_MISMATCH_SECRET\n</Password>\nsafe-after",
        "safe-before\n<key>Password</KEY>"
        "<string>SYNTHETIC_XML_SELECTOR_CASE_SECRET</string>\nsafe-after",
        'safe-before\n<add key="password"\n value="SYNTHETIC_XML_INCOMPLETE_SECRET"',
        "safe-before\n<key>Password</key>\n<string>SYNTHETIC_XML_PLIST_INCOMPLETE_SECRET",
        "safe-before\n<key>Pass<em>word</em></key>"
        "<string>SYNTHETIC_XML_MARKUP_PLIST_SECRET</string>\nsafe-after",
        'safe-before\n<!DOCTYPE settings [<!ENTITY pw "Password">]>\n'
        '<settings><add key="&pw;" value="SYNTHETIC_XML_DTD_SECRET"/></settings>\n'
        "safe-after",
    )
    for payload in fail_closed_cases:
        fail_closed_redis = FakeRedis()
        fail_closed_redis.store[key] = payload
        _patch_runtime(monkeypatch, fail_closed_redis)
        fail_closed = await diagnostics.get_latest_cli_log(SLUG)
        assert fail_closed["text"] == "safe-before\n[credential document omitted]"
        assert "SYNTHETIC_XML_" not in fail_closed["text"]
        assert "safe-after" not in fail_closed["text"]

    nested_redis = FakeRedis()
    nested_redis.store[key] = (
        "safe-before\n<password><password>nested-xml-secret</password>"
        "</password>\nsafe-after"
    )
    _patch_runtime(monkeypatch, nested_redis)
    nested = await diagnostics.get_latest_cli_log(SLUG)
    assert nested["text"] == "safe-before\n[credential document omitted]\nsafe-after"
    assert "nested-xml-secret" not in nested["text"]

    for incomplete_markup in ("Pass<!-- incomplete", "<![CDATA[Password"):
        markup_redis = FakeRedis()
        markup_redis.store[key] = (
            f"safe-before\n<key>{incomplete_markup}</key>"
            "<string>SYNTHETIC_XML_INCOMPLETE_MARKUP_SECRET</string>\nsafe-after"
        )
        _patch_runtime(monkeypatch, markup_redis)
        incomplete = await diagnostics.get_latest_cli_log(SLUG)
        assert incomplete["text"] == "safe-before\n[credential document omitted]"
        assert "SYNTHETIC_XML_INCOMPLETE_MARKUP_SECRET" not in incomplete["text"]

    selector_flood = ("<key>" * diagnostics._MAX_CLI_LOG_SOURCE_BYTES)[
        : diagnostics._MAX_CLI_LOG_SOURCE_BYTES
    ]
    assert diagnostics._omit_xml_selector_credential_contexts(selector_flood) == (
        "[credential document omitted]",
        1,
    )
    malformed_scalar_prefix = "<key>Password</key><string "
    malformed_scalar = malformed_scalar_prefix + (
        " " * (diagnostics._MAX_CLI_LOG_SOURCE_BYTES - len(malformed_scalar_prefix))
    )
    assert diagnostics._omit_xml_selector_credential_contexts(malformed_scalar) == (
        "[credential document omitted]",
        1,
    )
    assert diagnostics._xml_scalar_value_end("", 0) is None
    assert diagnostics._xml_scalar_value_end("<!", 0) is None
    assert diagnostics._xml_scalar_value_end("<string>value<!", 0) is None
    assert diagnostics._xml_scalar_value_end("<string>value</value>", 0) is None


async def test_latest_cli_log_redacts_standard_gitlab_token_prefixes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    prefixes = (
        "glpat",
        "gloas",
        "gldt",
        "glrt",
        "glrtr",
        "glcbt",
        "glptt",
        "glft",
        "glimt",
        "glagent",
        "glwt",
        "glsoat",
        "glffct",
    )
    synthetic_tokens = tuple(f"{prefix}-{'A' * 20}" for prefix in prefixes)
    redis = FakeRedis()
    redis.store[cli_log_latest(SLUG)] = "\n".join(
        ("safe-before", *synthetic_tokens, "safe-after")
    )
    _patch_runtime(monkeypatch, redis)

    result = await diagnostics.get_latest_cli_log(SLUG)

    assert result["text"] == "\n".join(
        ("safe-before", *("[REDACTED]" for _token in synthetic_tokens), "safe-after")
    )
    assert all(token not in result["text"] for token in synthetic_tokens)


async def test_latest_cli_log_redacts_recognizable_package_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    synthetic_tokens = ("pypi-" + ("A" * 85), "npm_" + ("a" * 36))
    redis = FakeRedis()
    redis.store[cli_log_latest(SLUG)] = "\n".join(
        ("safe-before", *synthetic_tokens, "safe-after")
    )
    _patch_runtime(monkeypatch, redis)

    result = await diagnostics.get_latest_cli_log(SLUG)

    assert result["text"] == "safe-before\n[REDACTED]\n[REDACTED]\nsafe-after"
    assert all(token not in result["text"] for token in synthetic_tokens)


async def test_latest_cli_log_omits_curl_cookies_and_cmd_batch_fragments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    cases = (
        ("cmd /c tool --pass%1ord cmd-positional-option-secret", "safe-cmd-1"),
        (
            "cmd /c tool --pass%~1ord cmd-modified-positional-option-secret",
            "safe-cmd-modified",
        ),
        ("cmd /c tool --pass%*word cmd-all-arguments-option-secret", "safe-cmd-all"),
        (
            "curl -b session=curl-cookie-secret https://example.test",
            "safe-curl-cookie",
        ),
        (
            "curl -'b' session=curl-fragmented-cookie-secret https://example.test",
            "safe-curl-fragmented-cookie",
        ),
        (
            "curl --cookie=session=curl-long-cookie-secret https://example.test",
            "safe-curl-long-cookie",
        ),
    )
    redis = FakeRedis()
    redis.store[cli_log_latest(SLUG)] = "\n".join(
        (
            *(
                line
                for payload, marker in cases
                for line in (payload, "---", marker, "---")
            ),
            "tool -b visible-unrelated-cookie-option",
        )
    )
    _patch_runtime(monkeypatch, redis)

    result = await diagnostics.get_latest_cli_log(SLUG)

    assert all(marker in result["text"] for _payload, marker in cases)
    assert "tool -b visible-unrelated-cookie-option" in result["text"]
    for secret in (
        "cmd-positional-option-secret",
        "cmd-modified-positional-option-secret",
        "cmd-all-arguments-option-secret",
        "curl-cookie-secret",
        "curl-fragmented-cookie-secret",
        "curl-long-cookie-secret",
    ):
        assert secret not in result["text"]


async def test_latest_cli_log_omits_openssl_passin_with_bounded_command_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    cases = (
        (
            "openssl rsa -passin pass:openssl-passin-secret -in encrypted.pem",
            "safe-openssl-passin",
        ),
        (
            r"C:\OpenSSL.exe rsa -passin=pass:openssl-attached-passin-secret",
            "safe-openssl-attached-passin",
        ),
        (
            "openssl rsa -'passin' pass:openssl-fragmented-passin-secret",
            "safe-openssl-fragmented-passin",
        ),
        (
            "openssl genpkey -passout pass:openssl-passout-secret -out key.pem",
            "safe-openssl-passout",
        ),
        (
            "openssl genpkey -passout=pass:openssl-attached-passout-secret",
            "safe-openssl-attached-passout",
        ),
    )
    redis = FakeRedis()
    redis.store[cli_log_latest(SLUG)] = "\n".join(
        (
            *(line for payload, marker in cases for line in (payload, "---", marker, "---")),
            "openssl rsa -passin file:/safe/password-source",
            "curl x ; tool -b session=visible-unrelated-command",
        )
    )
    _patch_runtime(monkeypatch, redis)

    result = await diagnostics.get_latest_cli_log(SLUG)

    assert all(marker in result["text"] for _payload, marker in cases)
    assert "openssl rsa -passin file:/safe/password-source" in result["text"]
    assert "tool -b session=visible-unrelated-command" in result["text"]
    assert all(payload not in result["text"] for payload, _marker in cases)
    assert "openssl-passin-secret" not in result["text"]
    assert "openssl-attached-passin-secret" not in result["text"]
    assert "openssl-fragmented-passin-secret" not in result["text"]
    assert "openssl-passout-secret" not in result["text"]
    assert "openssl-attached-passout-secret" not in result["text"]

    repeated_curl = ("curl x " * diagnostics._MAX_CLI_LOG_SOURCE_BYTES)[
        : diagnostics._MAX_CLI_LOG_SOURCE_BYTES
    ]
    assert diagnostics._command_specific_credential_value_start(repeated_curl) is None


async def test_latest_cli_log_scopes_curl_short_user_and_leading_connection_pwd(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    redis.store[cli_log_latest(SLUG)] = "\n".join(
        (
            "Pwd=leading-connection-password;Server=db.example.test",
            "safe-after-leading-pwd",
            "curl -u user:curl-scoped-user-secret https://example.test",
            "safe-after-curl-scoped-user",
            "python -u worker.py",
            "git status -uno",
            "PWD=/synthetic/workspace;Server=ordinary-shell-command",
            "Pwd=visible-nonconnection-value;MODE=test",
        )
    )
    _patch_runtime(monkeypatch, redis)

    result = await diagnostics.get_latest_cli_log(SLUG)

    assert "leading-connection-password" not in result["text"]
    assert "curl-scoped-user-secret" not in result["text"]
    assert "safe-after-leading-pwd" in result["text"]
    assert "safe-after-curl-scoped-user" in result["text"]
    assert "python -u worker.py" in result["text"]
    assert "git status -uno" in result["text"]
    assert "PWD=/synthetic/workspace;Server=ordinary-shell-command" in result["text"]
    assert "Pwd=visible-nonconnection-value;MODE=test" in result["text"]


async def test_latest_cli_log_scopes_mongosh_short_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    redis.store[cli_log_latest(SLUG)] = "\n".join(
        (
            "mongosh --username alice -p mongosh-separated-password-secret",
            "safe-after-mongosh-separated-password",
            "/usr/bin/mongosh -p=mongosh-attached-password-secret",
            "safe-after-mongosh-attached-password",
            "tool -p visible-unrelated-short-option",
        )
    )
    _patch_runtime(monkeypatch, redis)

    result = await diagnostics.get_latest_cli_log(SLUG)

    assert "mongosh-separated-password-secret" not in result["text"]
    assert "mongosh-attached-password-secret" not in result["text"]
    assert "safe-after-mongosh-separated-password" in result["text"]
    assert "safe-after-mongosh-attached-password" in result["text"]
    assert "tool -p visible-unrelated-short-option" in result["text"]


async def test_latest_cli_log_omits_word_leading_expansion_credential_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    redis.store[cli_log_latest(SLUG)] = "\n".join(
        (
            "EMPTY=; tool $EMPTY--password word-leading-password-secret",
            "safe-after-word-leading-password",
            "EMPTY=; tool ${EMPTY}--client-secret word-leading-client-secret",
            "safe-after-leading-client-option",
            "echo $HOME/bin/tool --help",
            "echo $EMPTY--author visible-noncredential-expansion",
            "EMPTY=; docker login ${EMPTY}-p shell-docker-password-secret",
            "safe-after-shell-docker-option",
            "set EMPTY= & docker login %EMPTY%-p cmd-docker-password-secret",
            "safe-after-cmd-docker-option",
            "EMPTY=; az login $EMPTY-p shell-azure-password-secret",
            "safe-after-shell-azure-option",
            "EMPTY=; mongosh $EMPTY-p shell-mongosh-password-secret",
            "safe-after-shell-mongosh-option",
            "EMPTY=; sshpass $EMPTY-p shell-sshpass-password-secret ssh host",
            "safe-after-shell-sshpass-option",
            "EMPTY=; redis-cli $EMPTY-a shell-redis-password-secret ping",
            "safe-after-shell-redis-option",
            "echo %EMPTY%-x visible-cmd-noncredential-expansion",
        )
    )
    _patch_runtime(monkeypatch, redis)

    result = await diagnostics.get_latest_cli_log(SLUG)

    assert "word-leading-password-secret" not in result["text"]
    assert "word-leading-client-secret" not in result["text"]
    assert "safe-after-word-leading-password" in result["text"]
    assert "safe-after-leading-client-option" in result["text"]
    assert "echo $HOME/bin/tool --help" in result["text"]
    assert "echo $EMPTY--author visible-noncredential-expansion" in result["text"]
    for secret in (
        "shell-docker-password-secret",
        "cmd-docker-password-secret",
        "shell-azure-password-secret",
        "shell-mongosh-password-secret",
        "shell-sshpass-password-secret",
        "shell-redis-password-secret",
    ):
        assert secret not in result["text"]
    for marker in (
        "safe-after-shell-docker-option",
        "safe-after-cmd-docker-option",
        "safe-after-shell-azure-option",
        "safe-after-shell-mongosh-option",
        "safe-after-shell-sshpass-option",
        "safe-after-shell-redis-option",
    ):
        assert marker in result["text"]
    assert "echo %EMPTY%-x visible-cmd-noncredential-expansion" in result["text"]


def test_mysql_attached_password_scan_is_command_scoped_and_bounded() -> None:
    from src.mcp.tools import diagnostics

    assert diagnostics._sensitive_value_start("mysql -pattached-password") is not None
    assert diagnostics._sensitive_value_start("mysql -P3306") is None
    assert diagnostics._sensitive_value_start("tool -pvisible") is None

    repeated_mysql = ("mysql x " * diagnostics._MAX_CLI_LOG_SOURCE_BYTES)[
        : diagnostics._MAX_CLI_LOG_SOURCE_BYTES
    ]
    assert diagnostics._command_specific_credential_value_start(repeated_mysql) is None


async def test_latest_cli_log_normalizes_unicode_yaml_line_separators(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    redis.store[cli_log_latest(SLUG)] = (
        "---\npassword:\u2028- unicode-line-separator-secret\n"
        "---\nsafe-after-line-separator\n"
        "---\npassword:\u2029- unicode-paragraph-separator-secret\n"
        "---\nsafe-after-paragraph-separator"
    )
    _patch_runtime(monkeypatch, redis)

    result = await diagnostics.get_latest_cli_log(SLUG)

    assert "unicode-line-separator-secret" not in result["text"]
    assert "unicode-paragraph-separator-secret" not in result["text"]
    assert "safe-after-line-separator" in result["text"]
    assert "safe-after-paragraph-separator" in result["text"]


def test_command_password_scans_are_monotonic_at_source_bound() -> None:
    from src.mcp.tools import diagnostics

    for executable in ("az", "docker", "redis-cli", "sshpass"):
        repeated = (f"{executable} x " * diagnostics._MAX_CLI_LOG_SOURCE_BYTES)[
            : diagnostics._MAX_CLI_LOG_SOURCE_BYTES
        ]
        assert diagnostics._command_specific_credential_value_start(repeated) is None


async def test_latest_cli_log_omits_curl_certificate_passwords(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    cases = (
        (
            "curl --cert client.pem:curl-cert-password-secret https://example.test",
            "safe-after-curl-cert-password",
        ),
        (
            "curl -Eclient.pem:curl-short-cert-password-secret https://example.test",
            "safe-after-curl-short-cert-password",
        ),
        (
            "curl --proxy-cert=proxy.pem:curl-proxy-cert-password-secret "
            "https://example.test",
            "safe-after-curl-proxy-cert-password",
        ),
    )
    redis = FakeRedis()
    redis.store[cli_log_latest(SLUG)] = "\n".join(
        (
            *(line for payload, marker in cases for line in (payload, "---", marker, "---")),
            "curl --cert client.pem https://example.test",
            "tool -Eclient.pem:visible-unrelated-cert-argument",
        )
    )
    _patch_runtime(monkeypatch, redis)

    result = await diagnostics.get_latest_cli_log(SLUG)

    assert all(marker in result["text"] for _payload, marker in cases)
    assert "curl --cert client.pem https://example.test" in result["text"]
    assert "tool -Eclient.pem:visible-unrelated-cert-argument" in result["text"]
    assert all(payload not in result["text"] for payload, _marker in cases)
    for secret in (
        "curl-cert-password-secret",
        "curl-short-cert-password-secret",
        "curl-proxy-cert-password-secret",
    ):
        assert secret not in result["text"]


async def test_latest_cli_log_omits_cloud_secret_response_shapes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    redis.store[cli_log_latest(SLUG)] = "\n".join(
        (
            '{"SecretString":"aws-secret-string-value","Name":"synthetic"}',
            "safe-after-aws-secret-string",
            '{"SecretBinary":"aws-secret-binary-value","ARN":"synthetic"}',
            "safe-after-aws-secret-binary",
            '[{"keyName":"key1","permissions":"FULL",'
            '"value":"azure-storage-access-key-value"}]',
            "safe-after-azure-storage-keys",
            "SSHPASS=sshpass-environment-password sshpass -e ssh synthetic@host",
            "safe-after-sshpass-environment",
            "az keyvault secret show --name ApiEndpoint --vault-name synthetic "
            "--query value",
            "",
            "azure-key-vault-plain-value",
            "safe-after-key-vault-scalar",
            '{"id":"https://synthetic.vault.azure.net/secrets/name/version",'
            '"value":"azure-key-vault-json-value","name":"name"}',
            "safe-after-key-vault-object",
            "ya29." + ("A" * 40),
            "safe-after-google-oauth-token",
            "client-key-data: kubeconfig-client-key-data-value",
            "safe-after-kubeconfig-client-key-data",
            '{"clientKeyData":"exec-credential-client-key-data-value"}',
            "safe-after-exec-credential-client-key-data",
            "aws secretsmanager get-secret-value --secret-id synthetic "
            "--query SecretString --output text",
            "",
            "aws-secret-manager-plain-value",
            "safe-after-aws-secret-manager-scalar",
            '{"name":"ordinary","value":"visible-generic-value"}',
            '{"id":"https://example.test/items/name","value":"visible-id-value"}',
            '{"keyName":"key1","value":""}',
        )
    )
    _patch_runtime(monkeypatch, redis)

    result = await diagnostics.get_latest_cli_log(SLUG)

    for secret in (
        "aws-secret-string-value",
        "aws-secret-binary-value",
        "azure-storage-access-key-value",
        "sshpass-environment-password",
        "azure-key-vault-plain-value",
        "azure-key-vault-json-value",
        "ya29." + ("A" * 40),
        "kubeconfig-client-key-data-value",
        "exec-credential-client-key-data-value",
        "aws-secret-manager-plain-value",
    ):
        assert secret not in result["text"]
    for marker in (
        "safe-after-aws-secret-string",
        "safe-after-aws-secret-binary",
        "safe-after-azure-storage-keys",
        "safe-after-sshpass-environment",
        "safe-after-key-vault-scalar",
        "safe-after-key-vault-object",
        "safe-after-google-oauth-token",
        "safe-after-kubeconfig-client-key-data",
        "safe-after-exec-credential-client-key-data",
        "safe-after-aws-secret-manager-scalar",
    ):
        assert marker in result["text"]
    assert '"value":"visible-generic-value"' in result["text"]
    assert '"value":"visible-id-value"' in result["text"]
    assert '{"keyName":"key1","value":""}' in result["text"]
    assert diagnostics._is_sensitive_key("sshpass")
    assert not diagnostics._is_sensitive_key("notsshpass")
    assert diagnostics._is_azure_key_vault_secret_value_command(
        "noop; /usr/bin/az.exe keyvault secret show --query=value"
    )
    assert diagnostics._is_aws_secrets_manager_value_command(
        "noop; /usr/bin/aws.exe secretsmanager get-secret-value "
        "--output=text --query=SecretBinary"
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
    assert overview["configured"]["coder"] == "codex"
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


async def test_snapshot_reads_share_one_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.mcp.tools import diagnostics

    second_url = "https://github.com/octo/other.git"

    class BlockingSecondSnapshotRedis(FakeRedis):
        async def getrange(self, key: str, start: int, end: int) -> object:
            if key == pipeline_state(OTHER_SLUG):
                await asyncio.Event().wait()
            return await super().getrange(key, start, end)

    redis = BlockingSecondSnapshotRedis()
    first = _state(updated=NOW)
    second = _state(updated=NOW)
    second.name = OTHER_SLUG
    second.url = second_url
    redis.store[pipeline_state(SLUG)] = first.model_dump_json()
    redis.store[pipeline_state(OTHER_SLUG)] = second.model_dump_json()
    _patch_runtime(monkeypatch, redis, _config(_repo(), _repo(second_url)))
    monkeypatch.setattr(diagnostics, "_REDIS_TIMEOUT_SECONDS", 0.01)

    result = await diagnostics.get_orchestrator_status()

    assert result["repositories"][0]["snapshot"]["status"] == "fresh"
    assert result["repositories"][1]["snapshot"] == diagnostics._snapshot_unavailable(
        "unavailable", "snapshot_read_failed"
    )
    assert result["redis"] == {"status": "partially_available", "code": "redis_read_failed"}
    assert redis.closed is True


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


async def test_redis_cleanup_preserves_results_and_propagates_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    class FailingCloseRedis(FakeRedis):
        async def aclose(self) -> None:
            raise ConnectionError("Authorization: Bearer close-secret")

    redis = FailingCloseRedis()
    _patch_runtime(monkeypatch, redis)
    result = await diagnostics.get_orchestrator_status()
    assert result["configuration"]["status"] == "available"
    assert "close-secret" not in json.dumps(result)

    class CancelledCloseRedis(FakeRedis):
        async def aclose(self) -> None:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await diagnostics._close_redis(CancelledCloseRedis())


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
    assert diagnostics._coder("custom-coder") == "custom-coder"
    assert diagnostics._coder("../secret") is None
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


@pytest.mark.parametrize("pr_number", [True, "7", 7.0])
def test_snapshot_rejects_coerced_current_pr_number(pr_number: object) -> None:
    from src.mcp.tools import diagnostics

    payload = json.loads(_state(updated=NOW).model_dump_json())
    payload["current_pr"]["number"] = pr_number

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


def test_snapshot_rejects_non_object_current_pr() -> None:
    from src.mcp.tools import diagnostics

    payload = json.loads(_state(updated=NOW).model_dump_json())
    payload["current_pr"] = []

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
    state.coder = "../secret"
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
            coder_affected="../secret",
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


async def test_cancellation_read_has_one_deadline_and_propagates_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    class BlockingCancellationRedis(FakeRedis):
        def __init__(self) -> None:
            super().__init__()
            self.read_started = asyncio.Event()

        async def getrange(self, key: str, start: int, end: int) -> object:
            self._check("getrange", key)
            self.read_started.set()
            await asyncio.Event().wait()

    redis = BlockingCancellationRedis()
    redis.store[cause_key(SLUG, "PR-9")] = "{}"
    monkeypatch.setattr(diagnostics, "_REDIS_TIMEOUT_SECONDS", 0.01)

    result = await diagnostics._current_cancellation(redis, SLUG, "PR-9")
    assert result == {
        "status": "unavailable",
        "code": "cancellation_read_failed",
        "classification": None,
    }

    monkeypatch.setattr(diagnostics, "_REDIS_TIMEOUT_SECONDS", 30.0)
    redis.read_started.clear()
    task = asyncio.create_task(diagnostics._current_cancellation(redis, SLUG, "PR-9"))
    await redis.read_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


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


async def test_retry_source_has_whole_scan_deadline_and_propagates_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    class BlockingRetryRedis(FakeRedis):
        def __init__(self) -> None:
            super().__init__()
            self.read_started = asyncio.Event()

        async def getrange(self, key: str, start: int, end: int) -> object:
            self._check("getrange", key)
            self.read_started.set()
            await asyncio.Event().wait()

    redis = BlockingRetryRedis()
    index = retry_command_pending(SLUG)
    redis.zsets[index] = [(str(uuid.uuid4()), 1.0)]
    monkeypatch.setattr(diagnostics, "_REDIS_TIMEOUT_SECONDS", 0.01)

    result = await diagnostics._pending_retries(redis, SLUG, 5)
    assert result == {
        "status": "unavailable",
        "code": "retry_record_read_failed",
        "records": [],
        "record_count": None,
        "scanned_index_entries": 0,
        "truncated": False,
    }

    monkeypatch.setattr(diagnostics, "_REDIS_TIMEOUT_SECONDS", 30.0)
    redis.read_started.clear()
    task = asyncio.create_task(diagnostics._pending_retries(redis, SLUG, 5))
    await redis.read_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_retry_source_rejects_coerced_numbers_and_invalid_timestamps() -> None:
    from src.mcp.tools import diagnostics

    redis = FakeRedis()
    command = _command()
    index = retry_command_pending(SLUG)
    redis.zsets[index] = [(command.command_id, 1.0)]
    key = retry_command(SLUG, command.command_id)
    payload = json.loads(command.model_dump_json())

    redis.store[key] = "[]"
    result = await diagnostics._pending_retries(redis, SLUG, 5)
    assert result["records"][0]["status"] == "malformed"
    assert result["records"][0]["code"] == "retry_record_invalid"

    for field in ("bound_pr_number", "retry_count", "retry_cap", "processing_attempts"):
        for invalid in (True, "1", 1.0):
            candidate = dict(payload)
            candidate[field] = invalid
            redis.store[key] = json.dumps(candidate)
            result = await diagnostics._pending_retries(redis, SLUG, 5)
            assert result["records"][0]["status"] == "malformed"
            assert result["records"][0]["code"] == "retry_record_invalid"

    for field in ("requested_at", "updated_at"):
        candidate = dict(payload)
        candidate[field] = "0001-01-01T00:00:00+23:59"
        redis.store[key] = json.dumps(candidate)
        result = await diagnostics._pending_retries(redis, SLUG, 5)
        assert result["records"][0]["status"] == "malformed"
        assert result["records"][0]["code"] == "retry_record_invalid"

    command.requested_at = datetime(1, 1, 1, tzinfo=timezone(timedelta(hours=23, minutes=59)))
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


async def test_run_source_has_whole_scan_deadline_and_propagates_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.mcp.tools import diagnostics

    class BlockingRunRedis(FakeRedis):
        def __init__(self) -> None:
            super().__init__()
            self.read_started = asyncio.Event()

        async def getrange(self, key: str, start: int, end: int) -> object:
            self._check("getrange", key)
            self.read_started.set()
            await asyncio.Event().wait()

    redis = BlockingRunRedis()
    index = MetricsStore._recent_key("PR-9", SLUG)
    redis.lists[index] = [str(uuid.uuid4())]
    monkeypatch.setattr(diagnostics, "_REDIS_TIMEOUT_SECONDS", 0.01)

    result = await diagnostics._run_records(redis, SLUG, "PR-9", 5)
    assert result == {
        "status": "unavailable",
        "code": "run_record_read_failed",
        "task_filter": "PR-9",
        "records": [],
        "record_count": None,
        "scanned_index_entries": 0,
        "truncated": False,
    }

    monkeypatch.setattr(diagnostics, "_REDIS_TIMEOUT_SECONDS", 30.0)
    redis.read_started.clear()
    task = asyncio.create_task(diagnostics._run_records(redis, SLUG, "PR-9", 5))
    await redis.read_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
