"""Tests for webhook delivery audit logging."""

from __future__ import annotations

import asyncio
import builtins
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import pytest
from src.audit import webhook_log
from src.audit.webhook_log import webhook_url_hash, write_webhook_audit
from src.daemon import notifications

FIXED_NOW = datetime(2026, 5, 17, 8, 23, tzinfo=timezone.utc)


class _FixedDatetime(datetime):
    @classmethod
    def now(cls, tz: Any = None) -> datetime:
        if tz is None:
            return FIXED_NOW.replace(tzinfo=None)
        return FIXED_NOW.astimezone(tz)


@pytest.fixture
def audit_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    target = tmp_path / "audit" / "webhooks"
    monkeypatch.setattr(webhook_log, "WEBHOOK_AUDIT_DIR", target)
    monkeypatch.setattr(webhook_log, "datetime", _FixedDatetime)
    return target


def _target(audit_dir: Path, event_type: str) -> Path:
    return audit_dir / event_type / "2026-05-17.jsonl"


def _read_records(audit_dir: Path, event_type: str) -> list[dict[str, Any]]:
    lines = _target(audit_dir, event_type).read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines]


def test_audit_creates_dated_file_per_event_type(audit_dir: Path) -> None:
    write_webhook_audit(
        event_type="guardrail_violation",
        webhook_url="https://hooks.example.test/secret",
        payload_size_bytes=1024,
        attempt_number=1,
        http_status=200,
        response_excerpt="ok",
        elapsed_ms=42,
    )

    assert _target(audit_dir, "guardrail_violation").exists()


def test_audit_hashes_url_not_stores_plaintext(audit_dir: Path) -> None:
    url = "https://hooks.example.test/secret"
    write_webhook_audit(
        event_type="guardrail_violation",
        webhook_url=url,
        payload_size_bytes=1024,
        attempt_number=1,
        http_status=200,
        response_excerpt="ok",
        elapsed_ms=42,
    )

    raw = _target(audit_dir, "guardrail_violation").read_text(encoding="utf-8")
    record = json.loads(raw)
    assert record["url_hash"].startswith("sha256:")
    assert url not in raw


def test_audit_truncates_response_excerpt_at_200_chars(audit_dir: Path) -> None:
    write_webhook_audit(
        event_type="guardrail_violation",
        webhook_url="https://hooks.example.test/secret",
        payload_size_bytes=1024,
        attempt_number=1,
        http_status=500,
        response_excerpt="x" * 500,
        elapsed_ms=42,
    )

    record = _read_records(audit_dir, "guardrail_violation")[0]
    assert len(record["response_excerpt"]) <= 200


def test_audit_writes_on_http_500(
    audit_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _Response:
        status_code = 500
        text = "Internal Server Error"

        def raise_for_status(self) -> None:
            raise httpx.HTTPStatusError(
                "500 Server Error",
                request=httpx.Request("POST", "https://example.test/hook"),
                response=httpx.Response(500),
            )

    class _Client:
        def __init__(self, timeout: float) -> None: ...
        async def __aenter__(self) -> _Client: return self
        async def __aexit__(self, *args: object) -> None: return None
        async def post(self, url: str, json: dict[str, Any]) -> _Response:
            return _Response()

    monkeypatch.setattr(notifications.httpx, "AsyncClient", _Client)

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(
            notifications._post_json_with_audit(
                event_type="guardrail_violation",
                webhook_url="https://example.test/hook",
                payload={"event": "guardrail_escalation"},
                timeout_seconds=1,
            )
        )

    record = _read_records(audit_dir, "guardrail_violation")[0]
    assert record["http_status"] == 500
    assert "retry_scheduled_at" not in record


def test_audit_records_explicit_retry_timestamp(audit_dir: Path) -> None:
    write_webhook_audit(
        event_type="guardrail_violation",
        webhook_url="https://hooks.example.test/secret",
        payload_size_bytes=1024,
        attempt_number=1,
        http_status=500,
        response_excerpt="Internal Server Error",
        elapsed_ms=42,
        retry_scheduled_at=FIXED_NOW,
    )

    record = _read_records(audit_dir, "guardrail_violation")[0]
    assert record["retry_scheduled_at"] == FIXED_NOW.isoformat()


def test_audit_writes_on_network_error(
    audit_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _Client:
        def __init__(self, timeout: float) -> None: ...
        async def __aenter__(self) -> _Client: return self
        async def __aexit__(self, *args: object) -> None: return None
        async def post(self, url: str, json: dict[str, Any]) -> httpx.Response:
            raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(notifications.httpx, "AsyncClient", _Client)

    with pytest.raises(httpx.ConnectError):
        asyncio.run(
            notifications._post_json_with_audit(
                event_type="guardrail_violation",
                webhook_url="https://example.test/hook",
                payload={"event": "guardrail_escalation"},
                timeout_seconds=1,
            )
        )

    record = _read_records(audit_dir, "guardrail_violation")[0]
    assert record["http_status"] is None
    assert "request_error" in record["response_excerpt"]


def test_audit_writes_on_2xx_success(
    audit_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _Client:
        def __init__(self, timeout: float) -> None: ...
        async def __aenter__(self) -> _Client: return self
        async def __aexit__(self, *args: object) -> None: return None
        async def post(self, url: str, json: dict[str, Any]) -> httpx.Response:
            return httpx.Response(200, text="ok", request=httpx.Request("POST", url))

    monkeypatch.setattr(notifications.httpx, "AsyncClient", _Client)

    asyncio.run(
        notifications._post_json_with_audit(
            event_type="guardrail_violation",
            webhook_url="https://example.test/hook",
            payload={"event": "guardrail_escalation"},
            timeout_seconds=1,
        )
    )

    record = _read_records(audit_dir, "guardrail_violation")[0]
    assert record["http_status"] == 200
    assert "retry_scheduled_at" not in record


def test_audit_ignores_response_decode_error_on_2xx(
    audit_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request = httpx.Request("POST", "https://example.test/hook")

    class _Response:
        status_code = 200

        @property
        def text(self) -> str:
            raise httpx.DecodingError("bad encoding", request=request)

        def raise_for_status(self) -> None:
            return None

    class _Client:
        def __init__(self, timeout: float) -> None: ...
        async def __aenter__(self) -> _Client: return self
        async def __aexit__(self, *args: object) -> None: return None
        async def post(self, url: str, json: dict[str, Any]) -> _Response:
            return _Response()

    monkeypatch.setattr(notifications.httpx, "AsyncClient", _Client)

    asyncio.run(
        notifications._post_json_with_audit(
            event_type="guardrail_violation",
            webhook_url="https://example.test/hook",
            payload={"event": "guardrail_escalation"},
            timeout_seconds=1,
        )
    )

    record = _read_records(audit_dir, "guardrail_violation")[0]
    assert record["http_status"] == 200
    assert "decode_error: DecodingError" in record["response_excerpt"]


def test_audit_write_runs_off_event_loop(
    audit_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[Any, tuple[Any, ...], dict[str, Any]]] = []

    async def fake_to_thread(
        func: Any, /, *args: Any, **kwargs: Any
    ) -> Any:
        calls.append((func, args, kwargs))
        return func(*args, **kwargs)

    class _Client:
        def __init__(self, timeout: float) -> None: ...
        async def __aenter__(self) -> _Client: return self
        async def __aexit__(self, *args: object) -> None: return None
        async def post(self, url: str, json: dict[str, Any]) -> httpx.Response:
            return httpx.Response(200, text="ok", request=httpx.Request("POST", url))

    monkeypatch.setattr(notifications.httpx, "AsyncClient", _Client)
    monkeypatch.setattr(notifications.asyncio, "to_thread", fake_to_thread)

    asyncio.run(
        notifications._post_json_with_audit(
            event_type="guardrail_violation",
            webhook_url="https://example.test/hook",
            payload={"event": "guardrail_escalation"},
            timeout_seconds=1,
        )
    )

    assert calls[0][0] is notifications.write_webhook_audit
    assert _read_records(audit_dir, "guardrail_violation")[0]["http_status"] == 200


def test_audit_payload_size_matches_httpx_json_encoding(
    audit_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _Client:
        def __init__(self, timeout: float) -> None: ...
        async def __aenter__(self) -> _Client: return self
        async def __aexit__(self, *args: object) -> None: return None
        async def post(self, url: str, json: dict[str, Any]) -> httpx.Response:
            return httpx.Response(200, text="ok", request=httpx.Request("POST", url))

    monkeypatch.setattr(notifications.httpx, "AsyncClient", _Client)
    payload = {"message": "hello café", "items": [1, 2]}

    asyncio.run(
        notifications._post_json_with_audit(
            event_type="guardrail_violation",
            webhook_url="https://example.test/hook",
            payload=payload,
            timeout_seconds=1,
        )
    )

    expected_size = len(
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    )
    record = _read_records(audit_dir, "guardrail_violation")[0]
    assert record["payload_size_bytes"] == expected_size


def test_audit_concurrent_writes_intact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "audit" / "webhooks"
    monkeypatch.setattr(webhook_log, "WEBHOOK_AUDIT_DIR", target)

    def _writer(index: int) -> None:
        write_webhook_audit(
            event_type="guardrail_violation",
            webhook_url="https://hooks.example.test/secret",
            payload_size_bytes=100 + index,
            attempt_number=index + 1,
            http_status=200,
            response_excerpt="ok",
            elapsed_ms=42,
        )

    with ThreadPoolExecutor(max_workers=10) as pool:
        list(pool.map(_writer, range(50)))

    files = list((target / "guardrail_violation").glob("*.jsonl"))
    assert len(files) == 1
    lines = files[0].read_text(encoding="utf-8").splitlines()
    assert len(lines) == 50
    records = [json.loads(line) for line in lines]
    assert sorted(record["attempt_number"] for record in records) == list(range(1, 51))


def test_audit_swallows_oserror(
    audit_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_open = builtins.open

    def fake_open(path: Any, *args: Any, **kwargs: Any) -> Any:
        if str(path).startswith(str(audit_dir)):
            raise OSError("disk full")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", fake_open)

    write_webhook_audit(
        event_type="guardrail_violation",
        webhook_url="https://hooks.example.test/secret",
        payload_size_bytes=1024,
        attempt_number=1,
        http_status=200,
        response_excerpt="ok",
        elapsed_ms=42,
    )

    assert not _target(audit_dir, "guardrail_violation").exists()


def test_url_hash_deterministic(audit_dir: Path) -> None:
    url = "https://hooks.example.test/secret"

    write_webhook_audit(
        event_type="guardrail_violation",
        webhook_url=url,
        payload_size_bytes=1024,
        attempt_number=1,
        http_status=200,
        response_excerpt="ok",
        elapsed_ms=42,
    )
    write_webhook_audit(
        event_type="guardrail_violation",
        webhook_url=url,
        payload_size_bytes=1024,
        attempt_number=2,
        http_status=200,
        response_excerpt="ok",
        elapsed_ms=42,
    )

    records = _read_records(audit_dir, "guardrail_violation")
    assert records[0]["url_hash"] == records[1]["url_hash"] == webhook_url_hash(url)
