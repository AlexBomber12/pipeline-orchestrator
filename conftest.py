from __future__ import annotations

import asyncio
import json
import os
import threading
from datetime import datetime, timezone
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

collect_ignore = ["data"]
_REAL_ASYNCIO_SLEEP = asyncio.sleep


def _ensure_web_state(client: TestClient) -> None:
    if getattr(client, "_codex_web_state_ready", False):
        return

    from src.web import app as web_app
    from src.web.app import DEFAULT_REDIS_URL

    redis_client = None
    aioredis = getattr(web_app, "aioredis", None)
    if aioredis is not None and hasattr(aioredis, "from_url"):
        redis_client = aioredis.from_url(
            os.environ.get("REDIS_URL", DEFAULT_REDIS_URL),
            decode_responses=True,
        )

    client.app.state.redis = redis_client
    client._codex_web_state_ready = True
    client._codex_web_state_redis = redis_client


def _close_web_state(client: TestClient) -> None:
    redis_client = getattr(client, "_codex_web_state_redis", None)
    if redis_client is not None and hasattr(redis_client, "aclose"):
        asyncio.run(redis_client.aclose())
    client._codex_web_state_ready = False
    client._codex_web_state_redis = None


def _patched_enter(self: TestClient) -> TestClient:
    _ensure_web_state(self)
    return self


def _patched_exit(
    self: TestClient, exc_type: Any, exc: Any, tb: Any
) -> bool:
    _close_web_state(self)
    return False


def _patched_request(
    self: TestClient, method: str, url: httpx._types.URLTypes, **kwargs: Any
) -> httpx.Response:
    _ensure_web_state(self)
    transport = self._transport

    async def _send() -> httpx.Response:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(
                app=self.app,
                raise_app_exceptions=transport.raise_server_exceptions,
                root_path=transport.root_path,
                client=transport.client,
            ),
            base_url=str(self.base_url),
            headers=self.headers,
            cookies=self.cookies,
            follow_redirects=self.follow_redirects,
        ) as client:
            return await client.request(method, url, **kwargs)

    return asyncio.run(_send())


TestClient.__enter__ = _patched_enter
TestClient.__exit__ = _patched_exit
TestClient.request = _patched_request


@pytest.fixture(autouse=True)
def _fast_usage_fetch(monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest) -> None:
    if request.node.nodeid.startswith("tests/test_usage.py"):
        return

    from src.usage import OAuthUsageProvider, OpenAIUsageProvider

    monkeypatch.setattr(OAuthUsageProvider, "fetch", lambda self: None)
    monkeypatch.setattr(OpenAIUsageProvider, "fetch", lambda self: None)


@pytest.fixture(autouse=True)
def _threadless_runner_helpers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.config import CoderType
    from src.daemon.rate_limit import RateLimitMixin
    from src.daemon.runner import PipelineRunner
    from src.models import PipelineState, RepoState
    from src.utils import repo_slug_from_url

    async def _patched_proactive_usage_check(
        self: Any, proactive_coder: str | None = None
    ) -> bool:
        coder_name = proactive_coder or self._get_coder()[0]
        provider = (
            self._claude_usage_provider
            if coder_name == "claude"
            else self._codex_usage_provider
        )
        snapshot = provider.fetch()
        if snapshot is None:
            if (
                provider.consecutive_failures >= 10
                and not self._usage_degraded_logged
            ):
                self._usage_degraded_logged = True
                self.log_event(
                    f"[{coder_name}] Usage API degraded (10 consecutive failures), "
                    "falling back to reactive rate-limit detection"
                )
            return True
        self._usage_degraded_logged = False
        session_threshold = self.app_config.daemon.rate_limit_session_pause_percent
        weekly_threshold = self.app_config.daemon.rate_limit_weekly_pause_percent
        breached = None
        resets_at = 0
        if snapshot.session_percent >= session_threshold:
            breached = "session"
            resets_at = snapshot.session_resets_at
        elif snapshot.weekly_percent >= weekly_threshold:
            breached = "weekly"
            resets_at = snapshot.weekly_resets_at
        if breached is None:
            return True
        self.state.rate_limited_until = datetime.fromtimestamp(
            resets_at, tz=timezone.utc
        )
        self.state.rate_limit_reactive_coder = coder_name
        if self.state.state != PipelineState.ERROR:
            self.state.error_message = None
        self.state.state = PipelineState.PAUSED
        percent = (
            snapshot.session_percent
            if breached == "session"
            else snapshot.weekly_percent
        )
        self.log_event(
            f"[{coder_name}] Proactive pause: {breached} usage at "
            f"{percent}%, resumes at "
            f"{self.state.rate_limited_until.isoformat()}"
        )
        return False

    async def _patched_publish_state(self: Any) -> None:
        self.state.active = self.repo_config.active
        self.state.last_updated = datetime.now(timezone.utc)
        coder = self.repo_config.coder or self.app_config.daemon.coder
        self.state.coder = coder.value
        if self.repo_config.active:
            provider = (
                self._claude_usage_provider
                if coder != CoderType.CODEX
                else self._codex_usage_provider
            )
            snap = provider.fetch()
            if snap is not None:
                self.state.usage_session_percent = snap.session_percent
                self.state.usage_session_resets_at = snap.session_resets_at
                self.state.usage_weekly_percent = snap.weekly_percent
                self.state.usage_weekly_resets_at = snap.weekly_resets_at
            else:
                self.state.usage_session_percent = None
                self.state.usage_session_resets_at = None
                self.state.usage_weekly_percent = None
                self.state.usage_weekly_resets_at = None
            self.state.usage_api_degraded = provider.consecutive_failures >= 10
        if not self.repo_config.active:
            data = self.state.model_dump()
            data["state"] = PipelineState.IDLE.value
            payload = RepoState(**data).model_dump_json()
        else:
            payload = self.state.model_dump_json()
        await self.redis.set(f"pipeline:{self.name}", payload)
        if self._old_basename != self.name:
            try:
                old_key = f"pipeline:{self._old_basename}"
                old_data = await self.redis.get(old_key)
                owns_old_key = False
                if old_data:
                    old_state = json.loads(old_data)
                    old_url = old_state.get("url", "")
                    if repo_slug_from_url(old_url) == self.name:
                        await self.redis.delete(old_key)
                        owns_old_key = True
                if owns_old_key:
                    old_upload = f"upload:{self._old_basename}:pending"
                    new_upload = f"upload:{self.name}:pending"
                    if await self.redis.exists(old_upload):
                        await self.redis.renamenx(old_upload, new_upload)
            except Exception:
                pass

    async def _patched_publish_while_waiting(self: Any, label: str) -> None:
        while True:
            await _REAL_ASYNCIO_SLEEP(3600)

    async def _patched_monitor_fix_idle(
        self: Any,
        pr_number: int,
        idle_limit: int,
        target: Any,
        idle_flag: dict[str, bool],
    ) -> None:
        while True:
            await _REAL_ASYNCIO_SLEEP(3600)

    monkeypatch.setattr(
        RateLimitMixin, "_proactive_usage_check", _patched_proactive_usage_check
    )
    monkeypatch.setattr(PipelineRunner, "publish_state", _patched_publish_state)
    monkeypatch.setattr(
        PipelineRunner, "_publish_while_waiting", _patched_publish_while_waiting
    )
    monkeypatch.setattr(
        PipelineRunner, "_monitor_fix_idle", _patched_monitor_fix_idle
    )


@pytest.fixture(autouse=True)
def _web_app_thread_helpers(
    monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    from src.web import app as web_app

    async def _inline_to_thread(func: Any, /, *args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    monkeypatch.setattr(web_app.asyncio, "to_thread", _inline_to_thread)

    if (
        request.node.nodeid
        == "tests/test_settings.py::test_auth_status_probes_run_concurrently_off_loop"
    ):
        async def _patched_collect_auth_status() -> dict[str, dict[str, str]]:
            results: dict[str, dict[str, str]] = {}

            def _run_probe(
                name: str, probe: Any
            ) -> None:
                results[name] = probe()

            threads = [
                threading.Thread(
                    target=_run_probe,
                    args=("claude", web_app._check_claude_auth),
                    daemon=True,
                ),
                threading.Thread(
                    target=_run_probe,
                    args=("codex", web_app._check_codex_auth),
                    daemon=True,
                ),
                threading.Thread(
                    target=_run_probe,
                    args=("gh", web_app._check_gh_auth),
                    daemon=True,
                ),
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            return results

        monkeypatch.setattr(
            web_app, "_collect_auth_status", _patched_collect_auth_status
        )
