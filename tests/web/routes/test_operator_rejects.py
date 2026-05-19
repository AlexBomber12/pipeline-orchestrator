"""Tests for the operator-reject cancellation history page."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from src.cancellation.storage import CancellationCause, cause_key, index_key
from src.models import PipelineState, RepoState
from src.web import app as web_app
from src.web.app import app, templates
from src.web.routes import operator_rejects

REPO_NAME = "AlexBomber12__pipeline-orchestrator"
REPO_URL = "https://github.com/AlexBomber12/pipeline-orchestrator.git"


class _RejectRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.zsets: dict[str, dict[object, float]] = {}
        self.zrevrange_calls: list[tuple[str, int, int]] = []

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def set(
        self,
        key: str,
        value: str,
        ex: int | None = None,
    ) -> bool:
        self.values[key] = value
        return True

    async def zrevrange(self, key: str, start: int, stop: int) -> list[object]:
        self.zrevrange_calls.append((key, start, stop))
        members = sorted(
            self.zsets.get(key, {}).items(),
            key=lambda item: item[1],
            reverse=True,
        )
        end = None if stop == -1 else stop + 1
        return [member for member, _score in members[start:end]]

    async def aclose(self) -> None:
        return None


class _RejectAioredis:
    def __init__(self, redis_client: _RejectRedis) -> None:
        self.redis_client = redis_client

    def from_url(
        self,
        url: str,
        decode_responses: bool = True,
    ) -> _RejectRedis:
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
def redis_client(monkeypatch: pytest.MonkeyPatch) -> _RejectRedis:
    client = _RejectRedis()
    monkeypatch.setattr(web_app, "aioredis", _RejectAioredis(client))
    return client


def _record_cancellation(
    redis_client: _RejectRedis,
    task_id: str,
    *,
    payload: dict[str, Any],
    created_at: datetime,
) -> None:
    cause = CancellationCause(
        category="ERROR",
        payload=payload,
        created_at=created_at.isoformat(),
        task_id=task_id,
        repo_slug=REPO_NAME,
    )
    redis_client.values[cause_key(REPO_NAME, task_id)] = cause.to_redis()
    redis_client.zsets.setdefault(index_key(REPO_NAME), {})[task_id] = (
        created_at.timestamp()
    )


def _operator_payload(
    *,
    rule: str | None = "approval_blocker_X",
    excerpt: str = "offending content",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "subsource": "operator_reject",
        "operator_reject_excerpt": excerpt,
    }
    if rule is not None:
        payload["operator_reject_rule"] = rule
    return payload


def _get_rejects_page(repo_name: str = REPO_NAME, *, limit: int | None = None) -> Any:
    path = f"/repo/{repo_name}/rejects"
    if limit is not None:
        path = f"{path}?limit={limit}"
    with TestClient(app) as client:
        return client.get(path)


def test_rejects_page_lists_operator_reject_records_only(
    base_config: Path,
    redis_client: _RejectRedis,
) -> None:
    now = datetime(2026, 5, 19, 12, tzinfo=timezone.utc)
    _record_cancellation(
        redis_client,
        "PR-1",
        payload=_operator_payload(excerpt="operator excerpt"),
        created_at=now,
    )
    _record_cancellation(
        redis_client,
        "PR-2",
        payload={"subsource": "crash", "operator_reject_excerpt": "crash excerpt"},
        created_at=now - timedelta(minutes=1),
    )

    response = _get_rejects_page()

    assert response.status_code == 200
    assert response.text.count("data-operator-reject-row") == 1
    assert "PR-1" in response.text
    assert "operator excerpt" in response.text
    assert "crash excerpt" not in response.text


def test_rejects_page_sorted_desc_by_timestamp(
    base_config: Path,
    redis_client: _RejectRedis,
) -> None:
    base = datetime(2026, 5, 19, 12, tzinfo=timezone.utc)
    for offset, task_id in ((0, "PR-old"), (2, "PR-new"), (1, "PR-mid")):
        _record_cancellation(
            redis_client,
            task_id,
            payload=_operator_payload(excerpt=task_id),
            created_at=base + timedelta(minutes=offset),
        )

    body = _get_rejects_page().text

    assert body.index("PR-new") < body.index("PR-mid") < body.index("PR-old")


def test_rejects_page_renders_rule_when_present(
    base_config: Path,
    redis_client: _RejectRedis,
) -> None:
    _record_cancellation(
        redis_client,
        "PR-1",
        payload=_operator_payload(rule="approval_blocker_X"),
        created_at=datetime(2026, 5, 19, 12, tzinfo=timezone.utc),
    )

    response = _get_rejects_page()

    assert response.status_code == 200
    assert "approval_blocker_X" in response.text


def test_rejects_page_renders_dash_when_rule_missing(
    base_config: Path,
    redis_client: _RejectRedis,
) -> None:
    _record_cancellation(
        redis_client,
        "PR-1",
        payload=_operator_payload(rule=None),
        created_at=datetime(2026, 5, 19, 12, tzinfo=timezone.utc),
    )

    response = _get_rejects_page()

    assert response.status_code == 200
    assert '<td class="py-2 text-xs">-</td>' in response.text


def test_rejects_page_truncates_excerpt_at_200_chars(
    base_config: Path,
    redis_client: _RejectRedis,
) -> None:
    excerpt = "x" * 500
    _record_cancellation(
        redis_client,
        "PR-1",
        payload=_operator_payload(excerpt=excerpt),
        created_at=datetime(2026, 5, 19, 12, tzinfo=timezone.utc),
    )

    response = _get_rejects_page()

    assert response.status_code == 200
    assert ("x" * 200) in response.text
    assert ("x" * 201) not in response.text


def test_rejects_page_empty_state(
    base_config: Path,
    redis_client: _RejectRedis,
) -> None:
    response = _get_rejects_page()

    assert response.status_code == 200
    assert "No operator rejects recorded for this repo." in response.text


def test_rejects_page_404_for_unknown_repo(
    base_config: Path,
    redis_client: _RejectRedis,
) -> None:
    response = _get_rejects_page("nope__missing")

    assert response.status_code == 404
    assert "Unknown repo: nope__missing" in response.text


def test_rejects_page_respects_limit_param(
    base_config: Path,
    redis_client: _RejectRedis,
) -> None:
    base = datetime(2026, 5, 19, 12, tzinfo=timezone.utc)
    for index in range(100):
        _record_cancellation(
            redis_client,
            f"PR-{index:03d}",
            payload=_operator_payload(excerpt=f"reject {index}"),
            created_at=base + timedelta(minutes=index),
        )

    response = _get_rejects_page(limit=10)

    assert response.status_code == 200
    assert response.text.count("data-operator-reject-row") == 10
    assert "PR-099" in response.text
    assert "PR-089" not in response.text
    assert redis_client.zrevrange_calls == [(index_key(REPO_NAME), 0, 9)]


def test_rejects_page_paginates_until_limit_is_filled(
    base_config: Path,
    redis_client: _RejectRedis,
) -> None:
    base = datetime(2026, 5, 19, 12, tzinfo=timezone.utc)
    _record_cancellation(
        redis_client,
        "PR-crash",
        payload={"subsource": "crash", "operator_reject_excerpt": "crash"},
        created_at=base + timedelta(minutes=3),
    )
    for index in range(3):
        _record_cancellation(
            redis_client,
            f"PR-reject-{index}",
            payload=_operator_payload(excerpt=f"reject {index}"),
            created_at=base + timedelta(minutes=2 - index),
        )

    response = _get_rejects_page(limit=2)

    assert response.status_code == 200
    assert response.text.count("data-operator-reject-row") == 2
    assert "PR-reject-0" in response.text
    assert "PR-reject-1" in response.text
    assert redis_client.zrevrange_calls == [
        (index_key(REPO_NAME), 0, 1),
        (index_key(REPO_NAME), 2, 3),
    ]


def test_rejects_page_decodes_bytes_task_ids_and_coerces_payload_fields(
    base_config: Path,
    redis_client: _RejectRedis,
) -> None:
    created_at = datetime(2026, 5, 19, 12, tzinfo=timezone.utc)
    task_id = "PR-bytes"
    cause = CancellationCause(
        category="ERROR",
        payload={
            "subsource": "operator_reject",
            "operator_reject_rule": 123,
            "operator_reject_excerpt": 456,
        },
        created_at=created_at.isoformat(),
        task_id=task_id,
        repo_slug=REPO_NAME,
    )
    redis_client.values[cause_key(REPO_NAME, task_id)] = cause.to_redis()
    redis_client.zsets.setdefault(index_key(REPO_NAME), {})[task_id.encode()] = (
        created_at.timestamp()
    )

    response = _get_rejects_page()

    assert response.status_code == 200
    assert "PR-bytes" in response.text
    assert '<td class="py-2 text-xs">123</td>' in response.text
    assert '<td class="py-2 text-xs text-gray-300">456</td>' in response.text


def test_rejects_page_skips_non_dict_payload(
    base_config: Path,
    redis_client: _RejectRedis,
) -> None:
    created_at = datetime(2026, 5, 19, 12, tzinfo=timezone.utc)
    cause = CancellationCause(
        category="ERROR",
        payload=["not", "a", "dict"],  # type: ignore[arg-type]
        created_at=created_at.isoformat(),
        task_id="PR-list",
        repo_slug=REPO_NAME,
    )
    redis_client.values[cause_key(REPO_NAME, "PR-list")] = cause.to_redis()
    redis_client.zsets.setdefault(index_key(REPO_NAME), {})["PR-list"] = (
        created_at.timestamp()
    )

    response = _get_rejects_page()

    assert response.status_code == 200
    assert "No operator rejects recorded for this repo." in response.text


def test_rejects_page_handles_corrupt_cancellation_record(
    base_config: Path,
    redis_client: _RejectRedis,
) -> None:
    now = datetime(2026, 5, 19, 12, tzinfo=timezone.utc)
    redis_client.values[cause_key(REPO_NAME, "PR-bad")] = "{not json"
    redis_client.zsets.setdefault(index_key(REPO_NAME), {})["PR-bad"] = now.timestamp()
    _record_cancellation(
        redis_client,
        "PR-good",
        payload=_operator_payload(excerpt="good reject"),
        created_at=now - timedelta(minutes=1),
    )

    response = _get_rejects_page()

    assert response.status_code == 200
    assert response.text.count("data-operator-reject-row") == 1
    assert "PR-good" in response.text
    assert "good reject" in response.text


def test_rejects_page_skips_non_utf8_index_members(
    base_config: Path,
    redis_client: _RejectRedis,
) -> None:
    now = datetime(2026, 5, 19, 12, tzinfo=timezone.utc)
    redis_client.zsets.setdefault(index_key(REPO_NAME), {})[b"\xff\xfe"] = (
        now.timestamp()
    )
    _record_cancellation(
        redis_client,
        "PR-good",
        payload=_operator_payload(excerpt="good reject"),
        created_at=now - timedelta(minutes=1),
    )

    response = _get_rejects_page()

    assert response.status_code == 200
    assert response.text.count("data-operator-reject-row") == 1
    assert "PR-good" in response.text
    assert "good reject" in response.text


def test_rejects_page_link_present_in_repo_card() -> None:
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

    assert f'href="/repo/{REPO_NAME}/rejects"' in html
    assert ">rejects</a>" in html


def test_decode_task_id_handles_bytes() -> None:
    assert operator_rejects._decode_task_id(b"PR-9") == "PR-9"
