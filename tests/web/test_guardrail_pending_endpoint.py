"""Tests for the operator override GET pending guardrail decisions endpoint (PR-305b)."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from src.cancellation.storage import CancellationCause, cause_key, index_key
from src.web import app as web_app
from src.web.app import app

_BASE = datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc).timestamp()


class _FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.zsets: dict[str, dict[str, float]] = {}

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def zrange(self, key: str, start: int, stop: int, withscores: bool = False) -> list[Any]:
        ordered = sorted(self.zsets.get(key, {}).items(), key=lambda kv: kv[1])
        return [tid for tid, _ in ordered][start : stop + 1 if stop != -1 else None]

    async def zrem(self, key: str, *members: str) -> int:
        zset = self.zsets.get(key, {})
        return sum(1 for m in members if zset.pop(m, None) is not None)

    async def aclose(self) -> None:
        return None


def _put(redis: _FakeRedis, slug: str, tid: str, payload: dict[str, Any], ts: float) -> None:
    created_at = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    cause = CancellationCause(
        category="ERROR",
        payload=payload,
        created_at=created_at,
        task_id=tid,
        repo_slug=slug,
    )
    redis.values[cause_key(slug, tid)] = cause.to_redis()
    redis.zsets.setdefault(index_key(slug), {})[tid] = ts


def _setup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    repos: tuple[str, ...] = ("example__alpha",),
) -> _FakeRedis:
    lines = ["repositories:"]
    for slug in repos:
        owner, name = slug.split("__", 1)
        lines += [f"  - url: https://github.com/{owner}/{name}.git", "    branch: main"]
    lines += ["daemon:", "  retry_button_cap: 3"]
    (tmp_path / "config.yml").write_text("\n".join(lines) + "\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    redis_client = _FakeRedis()
    fake_module = type(
        "_Aioredis",
        (),
        {"from_url": staticmethod(lambda url, decode_responses=True: redis_client)},
    )()
    monkeypatch.setattr(web_app, "aioredis", fake_module)
    return redis_client


def _get(name: str) -> Any:
    with TestClient(app) as client:
        return client.get(f"/api/repo/{name}/guardrail/pending")


def test_guardrail_pending_returns_empty_list_when_no_pending(tmp_path, monkeypatch) -> None:
    _setup(tmp_path, monkeypatch)
    resp = _get("example__alpha")
    assert resp.status_code == 200
    assert resp.json() == {"pending": []}


def test_guardrail_pending_returns_sorted_oldest_first(tmp_path, monkeypatch) -> None:
    redis_client = _setup(tmp_path, monkeypatch)
    _put(redis_client, "example__alpha", "PR-MID", {"subsource": "guardrail"}, _BASE + 5)
    _put(redis_client, "example__alpha", "PR-EARLY", {"subsource": "guardrail"}, _BASE)
    _put(redis_client, "example__alpha", "PR-LATE", {"subsource": "guardrail"}, _BASE + 10)
    body = _get("example__alpha").json()
    assert [e["pr_id"] for e in body["pending"]] == ["PR-EARLY", "PR-MID", "PR-LATE"]
    assert [e["recorded_at"] for e in body["pending"]] == [
        int(_BASE),
        int(_BASE + 5),
        int(_BASE + 10),
    ]


def test_guardrail_pending_filters_out_non_guardrail_subsources(tmp_path, monkeypatch) -> None:
    redis_client = _setup(tmp_path, monkeypatch)
    _put(redis_client, "example__alpha", "PR-G1", {"subsource": "guardrail"}, _BASE)
    _put(redis_client, "example__alpha", "PR-G2", {"subsource": "guardrail"}, _BASE + 1)
    _put(redis_client, "example__alpha", "PR-CODER", {"subsource": "coder_escalate"}, _BASE + 2)
    body = _get("example__alpha").json()
    assert {e["pr_id"] for e in body["pending"]} == {"PR-G1", "PR-G2"}


def test_guardrail_pending_extracts_rule_and_excerpt_from_payload(tmp_path, monkeypatch) -> None:
    redis_client = _setup(tmp_path, monkeypatch)
    _put(
        redis_client,
        "example__alpha",
        "PR-RULE",
        {"subsource": "guardrail", "rule": "large_diff_threshold", "excerpt": "+1800 LOC"},
        _BASE,
    )
    [entry] = _get("example__alpha").json()["pending"]
    assert entry["rule"] == "large_diff_threshold"
    assert entry["excerpt"] == "+1800 LOC"
    assert entry["pr_id"] == "PR-RULE"


def test_guardrail_pending_repo_isolation(tmp_path, monkeypatch) -> None:
    redis_client = _setup(tmp_path, monkeypatch, repos=("example__alpha", "example__beta"))
    _put(redis_client, "example__alpha", "PR-A1", {"subsource": "guardrail"}, _BASE)
    _put(redis_client, "example__beta", "PR-B1", {"subsource": "guardrail"}, _BASE + 1)
    assert [e["pr_id"] for e in _get("example__alpha").json()["pending"]] == ["PR-A1"]


def test_guardrail_pending_repo_not_found_returns_404(tmp_path, monkeypatch) -> None:
    _setup(tmp_path, monkeypatch)
    resp = _get("nonexistent")
    assert resp.status_code == 404
    assert resp.json() == {"error": "repo not found"}


def test_guardrail_pending_bounded_at_100(tmp_path, monkeypatch) -> None:
    redis_client = _setup(tmp_path, monkeypatch)
    for idx in range(150):
        _put(redis_client, "example__alpha", f"PR-{idx:03d}", {"subsource": "guardrail"}, _BASE + idx)
    body = _get("example__alpha").json()
    assert len(body["pending"]) == 100
    assert body["pending"][0]["pr_id"] == "PR-000"
    assert body["pending"][-1]["pr_id"] == "PR-099"
