"""PR-211: Config update path consistency regression tests.

Documents current behavior at 2026-05-01 of the four web endpoints that
mutate ``config.yml``. Each endpoint differs in two side effects:

* whether it sets ``control:{repo}:config_dirty`` so the runner picks the
  change up at its next IDLE boundary (instead of waiting for the
  config-file watcher to poll);
* whether it publishes a wake event on ``orchestrator:wake:{repo}`` so the
  daemon's main loop short-circuits its sleep.

| Endpoint                      | Writes config | Sets dirty | Publishes wake |
|-------------------------------|---------------|------------|----------------|
| POST /repos/{name}/coder      | yes           | yes        | yes            |
| PUT  /settings/repos          | yes           | no         | no             |
| PUT  /settings/repo/{name}    | yes           | no         | yes            |
| PUT  /settings/daemon         | yes           | no         | no             |

PR-217 will introduce a ``ConfigUpdateService`` that normalizes all four
paths to write+dirty+wake. These tests must be updated when PR-217 lands.
The tests assert *current* behavior, not desired behavior, so the refactor
PR can show the diff explicitly rather than silently changing semantics.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest
from fastapi.responses import HTMLResponse
from fastapi.testclient import TestClient
from src.config import load_config
from src.daemon import config_watcher
from src.web import app as web_app
from src.web.app import app


class _FakeRedis:
    """Minimal async Redis double recording all SET/PUBLISH calls.

    ``store`` mirrors the keyspace so tests can assert presence/absence
    of the ``control:{repo}:config_dirty`` flag. ``published`` records
    every wake-event publish so tests can assert event_type and channel.
    """

    def __init__(self, store: dict[str, str] | None = None) -> None:
        self.store: dict[str, str] = dict(store or {})
        self.published: list[tuple[str, str]] = []
        self.set_calls: list[tuple[str, str]] = []

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def set(self, key: str, value: str, **_kwargs: object) -> None:
        self.set_calls.append((key, value))
        self.store[key] = value

    async def delete(self, key: str) -> int:
        existed = key in self.store
        self.store.pop(key, None)
        return int(existed)

    async def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, message))
        return 1

    async def transaction(
        self,
        func,
        *_watches: str,
        value_from_callable: bool = False,
        **_kwargs: object,
    ):
        pipe = _FakePipeline(self)
        outcome = func(pipe)
        if asyncio.iscoroutine(outcome):
            outcome = await outcome
        results = await pipe.execute()
        if value_from_callable:
            return outcome
        return results


class _FakePipeline:
    def __init__(self, redis: _FakeRedis) -> None:
        self.redis = redis
        self.commands: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    async def get(self, key: str) -> str | None:
        return self.redis.store.get(key)

    def multi(self) -> None:
        return None

    def set(self, key: str, value: str, **kwargs: Any) -> "_FakePipeline":
        self.commands.append(("set", (key, value), kwargs))
        return self

    async def execute(self) -> list[Any]:
        results: list[Any] = []
        for command, args, kwargs in self.commands:
            if command == "set":
                await self.redis.set(args[0], args[1], **kwargs)
                results.append(True)
        return results


class _PublishBoomRedis(_FakeRedis):
    """Redis double whose publish() always raises ConnectionError."""

    async def publish(self, channel: str, message: str) -> int:
        # Record the attempt so the test can assert publish_wake was tried.
        self.published.append((channel, message))
        raise ConnectionError("redis publish unavailable")


@pytest.fixture
def one_repo_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n"
        "    auto_merge: true\n"
        "    coder: claude\n"
        "    review_timeout_min: 60\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    return cfg


def _stub_repo_template(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bypass the per-repo summary template so the POST endpoint returns
    quickly and without depending on rendering machinery."""

    async def _fake_repo_template_context(
        name: str,
        redis_client: object | None,
        *,
        coder_update_message: str | None = None,
        include_metrics: bool = False,
    ) -> dict[str, object]:
        return {"coder_update_message": coder_update_message or ""}

    def _fake_template_response(
        request: object,
        template_name: str,
        context: dict[str, object],
    ) -> HTMLResponse:
        return HTMLResponse(str(context.get("coder_update_message", "")))

    monkeypatch.setattr(web_app, "_repo_template_context", _fake_repo_template_context)
    monkeypatch.setattr(
        web_app.templates, "TemplateResponse", _fake_template_response
    )


def test_post_repo_coder_writes_config_sets_dirty_publishes_wake(
    one_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Documents current behavior at 2026-05-01.

    POST /repos/{name}/coder is the single endpoint that does all three
    side effects: writes config, sets ``control:{repo}:config_dirty``,
    and publishes a ``coder_swap`` wake. PR-217 will normalize all four
    config-mutating endpoints to this same write+dirty+wake pattern.
    Update this test when PR-217 lands.
    """
    fake = _FakeRedis()
    _stub_repo_template(monkeypatch)

    async def _noop_publish(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(web_app, "publish_repo_event", _noop_publish)

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.post(
            "/repos/example__alpha/coder",
            data={"coder": "codex"},
        )

    assert response.status_code == 200

    # Writer was called: the on-disk config now reflects the new coder.
    cfg = load_config(str(one_repo_config))
    assert cfg.repositories[0].coder is not None
    assert cfg.repositories[0].coder.value == "codex"

    # Dirty flag was set.
    assert fake.store.get("control:example__alpha:config_dirty") == "1"

    # publish_wake was called with the coder_swap event type.
    wake_events = [
        (channel, message)
        for channel, message in fake.published
        if channel == "orchestrator:wake:example__alpha"
    ]
    assert len(wake_events) == 1
    payload = json.loads(wake_events[0][1])
    assert payload["event_type"] == "coder_swap"
    assert payload["repo"] == "example__alpha"


def test_put_settings_repos_writes_config_no_dirty_no_wake(
    one_repo_config: Path,
) -> None:
    """Documents current behavior at 2026-05-01.

    PUT /settings/repos updates an existing repository (keyed by ``url``)
    but does NOT set ``control:{repo}:config_dirty`` and does NOT publish
    a wake event. The runner only learns about the change at the next
    config_watcher tick (up to CONFIG_WATCH_INTERVAL_SEC seconds later).
    PR-217 will normalize all four endpoints to write+dirty+wake. Update
    this test when PR-217 lands.
    """
    fake = _FakeRedis()

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.put(
            "/settings/repos",
            params={"url": "https://github.com/example/alpha.git"},
            data={"branch": "develop"},
        )

    assert response.status_code == 200

    # Writer was called: branch updated on disk.
    cfg = load_config(str(one_repo_config))
    assert cfg.repositories[0].branch == "develop"

    # No dirty flag was set for any repo.
    assert "control:example__alpha:config_dirty" not in fake.store
    assert not any(
        key.startswith("control:") and key.endswith(":config_dirty")
        for key in fake.store
    )

    # No wake event was published on any orchestrator:wake:* channel.
    assert not any(
        channel.startswith("orchestrator:wake:")
        for channel, _ in fake.published
    )


def test_put_settings_repo_name_writes_config_no_dirty_publishes_wake(
    one_repo_config: Path,
) -> None:
    """Documents current behavior at 2026-05-01.

    PUT /settings/repo/{name} writes config and publishes a ``settings``
    wake event but does NOT set ``control:{repo}:config_dirty``. This is
    a partial inconsistency: the wake nudges the daemon to run sooner,
    yet the runner still has to detect the dirty state via the file
    watcher. PR-217 will normalize all four endpoints to write+dirty+wake.
    Update this test when PR-217 lands.
    """
    fake = _FakeRedis()

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.put(
            "/settings/repo/example__alpha",
            data={"coder": "codex"},
        )

    assert response.status_code == 200

    # Writer was called.
    cfg = load_config(str(one_repo_config))
    assert cfg.repositories[0].coder is not None
    assert cfg.repositories[0].coder.value == "codex"

    # No dirty flag.
    assert "control:example__alpha:config_dirty" not in fake.store

    # publish_wake was called with event_type "settings".
    wake_events = [
        (channel, message)
        for channel, message in fake.published
        if channel == "orchestrator:wake:example__alpha"
    ]
    assert len(wake_events) == 1
    payload = json.loads(wake_events[0][1])
    assert payload["event_type"] == "settings"
    assert payload["repo"] == "example__alpha"


def test_put_settings_daemon_writes_config_no_dirty_no_wake(
    one_repo_config: Path,
) -> None:
    """Documents current behavior at 2026-05-01.

    PUT /settings/daemon is the most inconsistent endpoint: it writes
    config but neither sets ``control:{repo}:config_dirty`` nor publishes
    any wake event, even though daemon-level changes (e.g.
    ``exploration_epsilon``) affect every runner. The change only
    propagates after a config_watcher tick (up to
    CONFIG_WATCH_INTERVAL_SEC seconds later) plus the next IDLE
    boundary. PR-217 will normalize all four endpoints to
    write+dirty+wake. Update this test when PR-217 lands.
    """
    fake = _FakeRedis()

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.put(
            "/settings/daemon",
            data={"exploration_epsilon": "0.25"},
        )

    assert response.status_code == 200

    # Writer was called.
    cfg = load_config(str(one_repo_config))
    assert cfg.daemon.exploration_epsilon == pytest.approx(0.25)

    # No dirty flag was set.
    assert not any(
        key.startswith("control:") and key.endswith(":config_dirty")
        for key in fake.store
    )

    # No wake event was published.
    assert not any(
        channel.startswith("orchestrator:wake:")
        for channel, _ in fake.published
    )


def test_config_watcher_catches_up_after_settings_daemon_put(
    one_repo_config: Path,
) -> None:
    """Documents current behavior at 2026-05-01.

    The eventual-consistency window: PUT /settings/daemon does not set
    the dirty flag itself, but the config-file watcher detects the
    sha256 change on its next tick and sets the dirty flag for every
    active runner. The window is bounded by ``CONFIG_WATCH_INTERVAL_SEC``
    (5s in production). PR-217 will eliminate this window for the four
    UI endpoints by setting the dirty flag synchronously. Update this
    test when PR-217 lands.
    """
    fake = _FakeRedis()

    async def driver() -> None:
        # 1. Establish the watcher's baseline against the pre-PUT file.
        task = asyncio.create_task(
            config_watcher.watch_config_file_changes(
                fake,
                get_repo_names=lambda: ["example__alpha"],
                config_path=one_repo_config,
                interval_sec=0.01,
            )
        )
        # Yield so the watcher snapshots the initial signature before the
        # PUT mutates the file.
        for _ in range(5):
            await asyncio.sleep(0.02)

        # 2. PUT writes config but does not set the dirty flag itself.
        with TestClient(app) as client:
            client.app.state.redis = fake
            response = client.put(
                "/settings/daemon",
                data={"exploration_epsilon": "0.4"},
            )
        assert response.status_code == 200
        # Sanity check: at this point only the watcher's later tick will
        # observe the change.
        assert "control:example__alpha:config_dirty" not in fake.store

        # 3. Wait for the watcher to detect the sha256 change and flag
        # the active runner.
        for _ in range(200):
            await asyncio.sleep(0.02)
            if "control:example__alpha:config_dirty" in fake.store:
                break

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(driver())
    assert fake.store.get("control:example__alpha:config_dirty") == "1"
    # Documented bound: a real watcher would take at most
    # CONFIG_WATCH_INTERVAL_SEC seconds to catch up.
    assert config_watcher.CONFIG_WATCH_INTERVAL_SEC == 5.0


def test_put_settings_repo_name_succeeds_when_publish_wake_fails(
    one_repo_config: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Documents current behavior at 2026-05-01.

    A Redis ``publish`` failure on the ``settings`` wake event must not
    fail the request: the config write is the source of truth and the
    config_watcher will eventually carry the change to the runner even
    without the wake nudge. The endpoint logs the failure at WARNING
    level and returns 200. PR-217 will preserve this resilience while
    also setting the dirty flag synchronously. Update this test when
    PR-217 lands.
    """
    fake = _PublishBoomRedis()

    with TestClient(app) as client:
        client.app.state.redis = fake
        with caplog.at_level("WARNING", logger=web_app.logger.name):
            response = client.put(
                "/settings/repo/example__alpha",
                data={"coder": "codex"},
            )

    assert response.status_code == 200

    # Config write succeeded.
    cfg = load_config(str(one_repo_config))
    assert cfg.repositories[0].coder is not None
    assert cfg.repositories[0].coder.value == "codex"

    # publish_wake was attempted exactly once on the wake channel.
    wake_attempts = [
        channel
        for channel, _ in fake.published
        if channel == "orchestrator:wake:example__alpha"
    ]
    assert len(wake_attempts) == 1

    # The failure was logged at WARNING level on the web app logger.
    assert any(
        "publish_wake failed for example__alpha" in record.getMessage()
        and "settings" in record.getMessage()
        for record in caplog.records
    )
