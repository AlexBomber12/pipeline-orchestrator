"""Tests for the Settings webhook test button endpoint (PR-356)."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient
from src.audit import webhook_log
from src.web import app as web_app
from src.web.app import app
from src.web.routes import settings as settings_routes


class _StubAioredisClient:
    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return None

    async def aclose(self) -> None:
        return None


class _StubAioredis:
    @staticmethod
    def from_url(url: str, decode_responses: bool = True) -> _StubAioredisClient:
        return _StubAioredisClient()


class _FakeCompleted:
    def __init__(self, returncode: int, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class _WebhookClient:
    status_code = 200
    text = "ok"
    posted: list[tuple[str, dict[str, Any], float | None]] = []

    def __init__(self) -> None:
        return None

    async def __aenter__(self) -> _WebhookClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def post(
        self,
        url: str,
        json: dict[str, Any],
        timeout: float | None = None,
    ) -> httpx.Response:
        type(self).posted.append((url, json, timeout))
        return httpx.Response(
            self.status_code,
            text=self.text,
            request=httpx.Request("POST", url),
        )


def _make_response_client(status_code: int, text: str = "body") -> type[_WebhookClient]:
    class _Client(_WebhookClient):
        pass

    _Client.status_code = status_code
    _Client.text = text
    _Client.posted = []
    return _Client


@pytest.fixture(autouse=True)
def _stub_auth_subprocess(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub auth probes so the settings page renders without real CLIs."""

    def fake_run(cmd: list[str], *args: object, **kwargs: object) -> _FakeCompleted:
        if cmd and cmd[0] == "claude":
            return _FakeCompleted(0, stdout="claude 1.2.3\n")
        if cmd and cmd[0] == "codex":
            if cmd[1:] == ["--version"]:
                return _FakeCompleted(0, stdout="codex-cli 0.121.0\n")
            return _FakeCompleted(0, stdout="Logged in with ChatGPT\n")
        if cmd and cmd[0] == "gh":
            return _FakeCompleted(
                0,
                stderr=(
                    "github.com\n"
                    "  ✓ Logged in to github.com as octocat (oauth_token)\n"
                ),
            )
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(web_app.subprocess, "run", fake_run)
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())


@pytest.fixture
def base_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n"
        "daemon:\n"
        "  poll_interval_sec: 60\n"
        "  guardrail_notification_webhook_url: https://hooks.example.test/x\n"
        "  guardrail_notification_timeout_seconds: 2\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    return cfg


@pytest.fixture
def audit_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    target = tmp_path / "audit" / "webhooks"
    monkeypatch.setattr(webhook_log, "WEBHOOK_AUDIT_DIR", target)
    return target


def _read_webhook_test_records(audit_dir: Path) -> list[dict[str, Any]]:
    files = sorted((audit_dir / "webhook_test").glob("*.jsonl"))
    assert files
    return [
        json.loads(line)
        for file in files
        for line in file.read_text(encoding="utf-8").splitlines()
    ]


def test_test_endpoint_returns_no_url_message_when_unconfigured(
    base_config: Path,
) -> None:
    base_config.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n"
        "daemon:\n"
        "  poll_interval_sec: 60\n",
        encoding="utf-8",
    )

    with TestClient(app) as client:
        response = client.post("/settings/webhook/test")

    assert response.status_code == 200
    assert "No URL configured" in response.text


def test_test_endpoint_returns_ok_on_2xx_response(
    base_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_client = _make_response_client(200, "ok")
    monkeypatch.setattr(settings_routes.httpx, "AsyncClient", fake_client)

    with TestClient(app) as client:
        response = client.post("/settings/webhook/test")

    assert response.status_code == 200
    assert "✓ HTTP 200" in response.text


def test_test_endpoint_returns_fail_on_4xx_response(
    base_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_client = _make_response_client(404, "not found")
    monkeypatch.setattr(settings_routes.httpx, "AsyncClient", fake_client)

    with TestClient(app) as client:
        response = client.post("/settings/webhook/test")

    assert response.status_code == 200
    assert "✗ HTTP 404" in response.text


def test_test_endpoint_returns_fail_on_5xx_response(
    base_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_client = _make_response_client(503, "unavailable")
    monkeypatch.setattr(settings_routes.httpx, "AsyncClient", fake_client)

    with TestClient(app) as client:
        response = client.post("/settings/webhook/test")

    assert response.status_code == 200
    assert "✗ HTTP 503" in response.text


def test_test_endpoint_returns_network_error_message(
    base_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _Client(_WebhookClient):
        async def post(
            self,
            url: str,
            json: dict[str, Any],
            timeout: float | None = None,
        ) -> httpx.Response:
            raise httpx.ConnectError(
                "connection refused", request=httpx.Request("POST", url)
            )

    monkeypatch.setattr(settings_routes.httpx, "AsyncClient", _Client)

    with TestClient(app) as client:
        response = client.post("/settings/webhook/test")

    assert response.status_code == 200
    assert "ConnectError" in response.text


def test_test_endpoint_writes_audit_record_on_success(
    base_config: Path, audit_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_client = _make_response_client(200, "ok")
    monkeypatch.setattr(settings_routes.httpx, "AsyncClient", fake_client)

    with TestClient(app) as client:
        response = client.post("/settings/webhook/test")

    assert response.status_code == 200
    record = _read_webhook_test_records(audit_dir)[0]
    assert record["event_type"] == "webhook_test"
    assert record["http_status"] == 200


def test_test_endpoint_writes_audit_record_on_failure(
    base_config: Path, audit_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_client = _make_response_client(500, "server error")
    monkeypatch.setattr(settings_routes.httpx, "AsyncClient", fake_client)

    with TestClient(app) as client:
        response = client.post("/settings/webhook/test")

    assert response.status_code == 200
    record = _read_webhook_test_records(audit_dir)[0]
    assert record["event_type"] == "webhook_test"
    assert record["http_status"] == 500


def test_test_payload_includes_event_type_field(
    base_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_client = _make_response_client(200, "ok")
    monkeypatch.setattr(settings_routes.httpx, "AsyncClient", fake_client)

    with TestClient(app) as client:
        response = client.post("/settings/webhook/test")

    assert response.status_code == 200
    assert fake_client.posted[0][1]["event_type"] == "webhook_test"


def test_test_endpoint_respects_configured_timeout(
    base_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class _Client(_WebhookClient):
        posted_timeout: float | None = None

        async def post(
            self,
            url: str,
            json: dict[str, Any],
            timeout: float | None = None,
        ) -> httpx.Response:
            type(self).posted_timeout = timeout
            raise httpx.TimeoutException(
                "slow", request=httpx.Request("POST", url)
            )

    monkeypatch.setattr(settings_routes.httpx, "AsyncClient", _Client)

    started = time.monotonic()
    with TestClient(app) as client:
        response = client.post("/settings/webhook/test")
    elapsed = time.monotonic() - started

    assert response.status_code == 200
    assert "TimeoutException" in response.text
    assert _Client.posted_timeout == 2.0
    assert elapsed < 3


def test_button_renders_in_settings_after_url_field(base_config: Path) -> None:
    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    body = response.text
    notification_start = body.index("Notifications")
    url_index = body.index('name="guardrail_notification_webhook_url"')
    button_index = body.index('hx-post="/settings/webhook/test"')
    assert notification_start < url_index < button_index
    assert "Test webhook" in body
    assert 'id="webhook-test-status"' in body
