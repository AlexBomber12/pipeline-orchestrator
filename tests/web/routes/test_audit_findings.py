"""Tests for the direct-commit audit findings page."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from src.daemon.main_commit_audit import _branch_key_part
from src.daemon.main_commit_audit import _findings_key as audit_findings_key
from src.models import PipelineState, RepoState
from src.web import app as web_app
from src.web.app import app, templates
from src.web.routes import audit as audit_routes

REPO_NAME = "AlexBomber12__pipeline-orchestrator"
REPO_URL = "https://github.com/AlexBomber12/pipeline-orchestrator.git"


class _AuditRedis:
    def __init__(
        self,
        *,
        lists: dict[str, list[str]] | None = None,
        values: dict[str, str] | None = None,
    ) -> None:
        self.lists = dict(lists or {})
        self.values = dict(values or {})

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def lrange(self, key: str, start: int, stop: int) -> list[str]:
        if key in self.values:
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
        values = self.lists.get(key, [])
        end = None if stop == -1 else stop + 1
        return values[start:end]

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.values[key] = value

    async def aclose(self) -> None:
        return None


class _AuditAioredis:
    def __init__(self, redis_client: _AuditRedis) -> None:
        self.redis_client = redis_client

    def from_url(
        self,
        url: str,
        decode_responses: bool = True,
    ) -> _AuditRedis:
        return self.redis_client


@pytest.fixture
def base_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        f"  - url: {REPO_URL}\n"
        "    branch: main\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    return cfg


@pytest.fixture
def redis_client(monkeypatch: pytest.MonkeyPatch) -> _AuditRedis:
    client = _AuditRedis()
    monkeypatch.setattr(web_app, "aioredis", _AuditAioredis(client))
    return client


def _finding(index: int) -> dict[str, Any]:
    sha = f"{index}" * 40
    return {
        "sha": sha,
        "short_sha": sha[:7],
        "message_first_line": f"direct commit {index}",
        "parent_count": 1,
        "pr_number": None,
        "violation_category": "direct_commit_no_pr",
        "rule": "Commit landed without a PR.",
    }


def _findings_key() -> str:
    return audit_findings_key(REPO_NAME, "main")


def _last_audit_key(repo_name: str = REPO_NAME, branch: str = "main") -> str:
    return f"audit:main_commits:{repo_name}:{_branch_key_part(branch)}:last_audit_at"


def test_audit_page_renders_empty_state_when_no_findings(
    base_config: Path,
    redis_client: _AuditRedis,
) -> None:
    with TestClient(app) as client:
        response = client.get(f"/repo/{REPO_NAME}/audit")

    assert response.status_code == 200
    assert "No direct-to-main commits" in response.text


def test_audit_page_lists_findings(
    base_config: Path,
    redis_client: _AuditRedis,
) -> None:
    redis_client.lists[_findings_key()] = [
        json.dumps(_finding(index), sort_keys=True) for index in range(1, 4)
    ]

    with TestClient(app) as client:
        response = client.get(f"/repo/{REPO_NAME}/audit")

    assert response.status_code == 200
    assert response.text.count("data-audit-finding-row") == 3
    assert "direct_commit_no_pr" in response.text


def test_audit_page_uses_persisted_finding_fields(
    base_config: Path,
    redis_client: _AuditRedis,
) -> None:
    finding = _finding(1)
    redis_client.lists[_findings_key()] = [json.dumps(finding, sort_keys=True)]

    with TestClient(app) as client:
        response = client.get(f"/repo/{REPO_NAME}/audit")

    assert response.status_code == 200
    assert "<th class=\"py-2 text-left\">Category</th>" in response.text
    assert "<th class=\"py-2 text-left\">Parents</th>" in response.text
    assert "<th class=\"py-2 text-left\">Timestamp</th>" not in response.text
    assert "<th class=\"py-2 text-left\">Author</th>" not in response.text
    assert f"<td class=\"py-2 text-xs\">{finding['parent_count']}</td>" in response.text
    assert finding["violation_category"] in response.text


def test_audit_page_links_to_github_commit(
    base_config: Path,
    redis_client: _AuditRedis,
) -> None:
    finding = _finding(1)
    redis_client.lists[_findings_key()] = [json.dumps(finding)]

    with TestClient(app) as client:
        response = client.get(f"/repo/{REPO_NAME}/audit")

    assert (
        f'href="https://github.com/AlexBomber12/pipeline-orchestrator/commit/{finding["sha"]}"'
        in response.text
    )


def test_audit_page_shows_last_audit_timestamp(
    base_config: Path,
    redis_client: _AuditRedis,
) -> None:
    redis_client.values[_last_audit_key()] = "2026-05-19T12:00:00Z"

    with TestClient(app) as client:
        response = client.get(f"/repo/{REPO_NAME}/audit")

    assert response.status_code == 200
    assert "Last audit: 2026-05-19T12:00:00Z" in response.text


def test_audit_page_404_for_unknown_repo(
    base_config: Path,
    redis_client: _AuditRedis,
) -> None:
    with TestClient(app) as client:
        response = client.get("/repo/nope__missing/audit")

    assert response.status_code == 404
    assert "Unknown repo: nope__missing" in response.text


def test_audit_page_handles_corrupt_redis_value(
    base_config: Path,
    redis_client: _AuditRedis,
) -> None:
    redis_client.values[_findings_key()] = "{not json"

    with TestClient(app) as client:
        response = client.get(f"/repo/{REPO_NAME}/audit")

    assert response.status_code == 200
    assert "No direct-to-main commits" in response.text


def test_audit_page_ignores_findings_without_valid_sha(
    base_config: Path,
    redis_client: _AuditRedis,
) -> None:
    redis_client.lists[_findings_key()] = [
        json.dumps({"message_first_line": "missing sha"}),
        json.dumps({"sha": None, "message_first_line": "null sha"}),
        json.dumps(_finding(1)),
    ]

    with TestClient(app) as client:
        response = client.get(f"/repo/{REPO_NAME}/audit")

    assert response.status_code == 200
    assert response.text.count("data-audit-finding-row") == 1
    assert "direct commit 1" in response.text
    assert "missing sha" not in response.text
    assert "null sha" not in response.text


def test_audit_page_link_visible_in_repo_card() -> None:
    repo = RepoState(
        url=REPO_URL,
        name=REPO_NAME,
        state=PipelineState.IDLE,
        history=[],
    )

    html = templates.get_template("components/repo_cards.html").render(
        repos=[repo],
        resources=None,
        cancellation_subsources={},
        subsource_lookup=lambda _name: None,
        drain_progress={},
        css_escape=lambda v: v,
        upload_feedback_target=lambda _name: "",
        utcnow=lambda: datetime.now(timezone.utc),
    )

    assert f'href="/repo/{REPO_NAME}/audit"' in html
    assert ">audit</a>" in html


def test_audit_page_renders_sha_as_short_hash(
    base_config: Path,
    redis_client: _AuditRedis,
) -> None:
    finding = _finding(2)
    redis_client.lists[_findings_key()] = [json.dumps(finding)]

    with TestClient(app) as client:
        response = client.get(f"/repo/{REPO_NAME}/audit")

    body = response.text
    assert f"/commit/{finding['sha']}" in body
    assert f">{finding['sha'][:8]}</a>" in body
    assert f">{finding['sha'][:9]}</a>" not in body


@pytest.mark.asyncio
async def test_read_audit_findings_filters_bad_list_entries() -> None:
    key = _findings_key()
    redis_client = _AuditRedis(
        lists={
            key: [
                b"\xff\xfe",
                "{not json",
                json.dumps(["not", "a", "dict"]),
                json.dumps({"message_first_line": "missing sha"}),
                json.dumps({"sha": "", "message_first_line": "empty sha"}),
                json.dumps({"sha": None, "message_first_line": "null sha"}),
                json.dumps(_finding(1)),
            ]
        }
    )

    findings = await audit_routes._read_audit_findings(redis_client, REPO_NAME, "main")

    assert findings == [_finding(1)]


@pytest.mark.asyncio
async def test_read_audit_findings_accepts_legacy_json_payloads() -> None:
    key = _findings_key()
    redis_client = _AuditRedis(
        values={
            key: json.dumps(
                [
                    _finding(1),
                    "skip",
                    {"message_first_line": "missing sha"},
                    {"sha": None, "message_first_line": "null sha"},
                ]
            )
        }
    )

    findings = await audit_routes._read_audit_findings(redis_client, REPO_NAME, "main")

    assert findings == [_finding(1)]

    redis_client.values[key] = json.dumps(_finding(2))
    assert await audit_routes._read_audit_findings(redis_client, REPO_NAME, "main") == [
        _finding(2)
    ]

    redis_client.values[key] = json.dumps("not a finding")
    assert await audit_routes._read_audit_findings(redis_client, REPO_NAME, "main") == []

    redis_client.values[key] = json.dumps({"message_first_line": "missing sha"})
    assert await audit_routes._read_audit_findings(redis_client, REPO_NAME, "main") == []


@pytest.mark.asyncio
async def test_read_audit_findings_returns_empty_when_get_fails() -> None:
    class _GetBoomRedis:
        async def lrange(self, key: str, start: int, stop: int) -> None:
            raise TypeError("wrong type")

        async def get(self, key: str) -> str:
            raise RuntimeError("redis down")

    assert await audit_routes._read_audit_findings(_GetBoomRedis(), REPO_NAME, "main") == []


@pytest.mark.asyncio
async def test_read_last_audit_timestamp_returns_none_when_get_fails() -> None:
    class _GetBoomRedis:
        async def get(self, key: str) -> str:
            raise RuntimeError("redis down")

    assert (
        await audit_routes._read_last_audit_timestamp(_GetBoomRedis(), REPO_NAME, "main")
        is None
    )


def test_parse_findings_payload_handles_empty_and_invalid_bytes() -> None:
    assert audit_routes._parse_findings_payload(None) == []
    assert audit_routes._parse_findings_payload(b"\xff\xfe") == []


def test_github_owner_repo_falls_back_to_repo_name_slug() -> None:
    assert audit_routes._github_owner_repo("pipeline-orchestrator", REPO_NAME) == (
        "AlexBomber12/pipeline-orchestrator"
    )
    assert audit_routes._github_owner_repo("pipeline-orchestrator", "pipeline-orchestrator") == (
        "pipeline-orchestrator"
    )
