"""PR-217: Config update path consistency regression tests.

Verifies the four web endpoints that mutate ``config.yml`` now share the
same write+dirty+wake side-effect contract via ``apply_config_mutation``.

| Endpoint                      | Writes config | Sets dirty | Publishes wake |
|-------------------------------|---------------|------------|----------------|
| POST /repos/{name}/coder      | yes           | yes        | yes            |
| PUT  /settings/repos          | yes           | yes        | yes            |
| PUT  /settings/repo/{name}    | yes           | yes        | yes            |
| PUT  /settings/daemon         | yes           | yes (all)  | yes (all)      |

These tests previously documented the pre-PR-217 inconsistency (PR-211
baseline). After PR-217 they verify the normalized behavior — every
config-mutating endpoint sets ``control:{repo}:config_dirty`` and
publishes a wake event for every affected repo. For ``PUT /settings/daemon``
the affected set is every active repo because daemon-level fields feed
into every runner.
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


@pytest.fixture
def two_repo_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n"
        "    auto_merge: true\n"
        "    coder: claude\n"
        "    review_timeout_min: 60\n"
        "  - url: https://github.com/example/beta.git\n"
        "    branch: main\n"
        "    auto_merge: true\n"
        "    coder: codex\n"
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
    """Verifies PR-217 normalization.

    POST /repos/{name}/coder writes config, sets
    ``control:{repo}:config_dirty``, and publishes a ``coder_swap`` wake.
    Behavior is unchanged by PR-217 (this endpoint was the canonical one).
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


def test_put_settings_repos_writes_config_sets_dirty_publishes_wake(
    one_repo_config: Path,
) -> None:
    """Verifies PR-217 normalization.

    PUT /settings/repos updates an existing repository (keyed by ``url``)
    and now sets ``control:{repo}:config_dirty`` and publishes a
    ``settings`` wake. Pre-PR-217 it did neither, leaving the runner to
    learn about the change via the next config_watcher tick.
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

    # Dirty flag set for the affected repo (and only that repo).
    assert fake.store.get("control:example__alpha:config_dirty") == "1"

    # Wake event published with event_type "settings".
    wake_events = [
        (channel, message)
        for channel, message in fake.published
        if channel == "orchestrator:wake:example__alpha"
    ]
    assert len(wake_events) == 1
    payload = json.loads(wake_events[0][1])
    assert payload["event_type"] == "settings"
    assert payload["repo"] == "example__alpha"


def test_put_settings_repo_name_writes_config_sets_dirty_publishes_wake(
    one_repo_config: Path,
) -> None:
    """Verifies PR-217 normalization.

    PUT /settings/repo/{name} writes config, sets
    ``control:{repo}:config_dirty``, and publishes a ``settings`` wake.
    Pre-PR-217 the dirty flag was missing — the wake nudged the daemon
    but the runner still depended on the file watcher to notice.
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

    # Dirty flag set.
    assert fake.store.get("control:example__alpha:config_dirty") == "1"

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


def test_put_settings_daemon_writes_config_sets_dirty_publishes_wake_for_all_repos(
    two_repo_config: Path,
) -> None:
    """Verifies PR-217 normalization.

    PUT /settings/daemon writes daemon-level config and now broadcasts
    ``control:{repo}:config_dirty`` plus a ``settings`` wake to every
    active repo. Pre-PR-217 this endpoint did neither, despite
    daemon-level fields (``exploration_epsilon``, ``claude_model``)
    affecting every runner.
    """
    fake = _FakeRedis()

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.put(
            "/settings/daemon",
            data={"exploration_epsilon": "25"},
        )

    assert response.status_code == 200

    # Writer was called.
    cfg = load_config(str(two_repo_config))
    assert cfg.daemon.exploration_epsilon == pytest.approx(0.25)

    # Dirty flags set for every active repo.
    assert fake.store.get("control:example__alpha:config_dirty") == "1"
    assert fake.store.get("control:example__beta:config_dirty") == "1"

    # Wake events published on every active repo's wake channel.
    wake_channels = {
        channel for channel, _ in fake.published
        if channel.startswith("orchestrator:wake:")
    }
    assert wake_channels == {
        "orchestrator:wake:example__alpha",
        "orchestrator:wake:example__beta",
    }
    for _, message in fake.published:
        payload = json.loads(message)
        assert payload["event_type"] == "settings"


def test_config_watcher_still_serves_as_fallback_after_settings_daemon_put(
    one_repo_config: Path,
) -> None:
    """The config-file watcher remains the safety net for missed nudges.

    PR-217 makes PUT /settings/daemon set the dirty flag synchronously
    so operators no longer have to wait for the watcher in the happy
    path. The watcher itself is unchanged: if Redis is briefly down at
    write time, the next file-signature tick still surfaces the change
    within ``CONFIG_WATCH_INTERVAL_SEC`` seconds.
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

        # 2. PUT writes config; PR-217 also sets the dirty flag synchronously.
        with TestClient(app) as client:
            client.app.state.redis = fake
            response = client.put(
                "/settings/daemon",
                data={"exploration_epsilon": "40"},
            )
        assert response.status_code == 200
        # The synchronous dirty flag is already in place; the watcher's
        # later tick is no longer load-bearing for the happy path.
        assert fake.store.get("control:example__alpha:config_dirty") == "1"

        # 3. Confirm the watcher would still set the flag if it had been
        # missing. Clear it and wait for the next tick to repopulate it
        # via the file-signature path.
        fake.store.pop("control:example__alpha:config_dirty", None)
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
    # Documented bound: the watcher catches up within
    # CONFIG_WATCH_INTERVAL_SEC seconds.
    assert config_watcher.CONFIG_WATCH_INTERVAL_SEC == 5.0


def test_post_repo_coder_logs_warning_when_state_refresh_raises(
    one_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Exception inside the post-write state refresh is caught and logged.

    PR-217 keeps the broad ``except Exception`` around the optional
    ``RepoState.coder`` refresh so that a transient Redis read failure or
    a corrupted state payload cannot turn into a 500 — the config write
    has already succeeded and the runner will reconcile its own state on
    the next IDLE boundary via the dirty flag.
    """

    class _GetBoomRedis(_FakeRedis):
        async def get(self, key: str) -> str | None:
            raise ConnectionError("redis get unavailable")

    fake = _GetBoomRedis()
    _stub_repo_template(monkeypatch)

    async def _noop_publish(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(web_app, "publish_repo_event", _noop_publish)

    with TestClient(app) as client:
        client.app.state.redis = fake
        with caplog.at_level("WARNING", logger=web_app.logger.name):
            response = client.post(
                "/repos/example__alpha/coder",
                data={"coder": "codex"},
            )

    assert response.status_code == 200
    # Dirty + wake nudge still happened via the helper before the refresh.
    assert fake.store.get("control:example__alpha:config_dirty") == "1"
    assert any(
        "Failed to refresh repo state after coder update" in record.getMessage()
        for record in caplog.records
    )


def test_put_settings_repo_name_succeeds_when_publish_wake_fails(
    one_repo_config: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verifies PR-217 normalization.

    A Redis ``publish`` failure on the ``settings`` wake event must not
    fail the request: the config write is the source of truth and the
    config_watcher will eventually carry the change to the runner even
    without the wake nudge. The endpoint logs the failure at WARNING
    and returns 200. PR-217 routes the warning through
    ``src.web.services.config_updates`` while preserving the same
    user-visible resilience contract.
    """
    fake = _PublishBoomRedis()

    with TestClient(app) as client:
        client.app.state.redis = fake
        with caplog.at_level(
            "WARNING", logger="src.web.services.config_updates"
        ):
            response = client.put(
                "/settings/repo/example__alpha",
                data={"coder": "codex"},
            )

    assert response.status_code == 200

    # Config write succeeded.
    cfg = load_config(str(one_repo_config))
    assert cfg.repositories[0].coder is not None
    assert cfg.repositories[0].coder.value == "codex"

    # The dirty flag was still set synchronously even though publish failed.
    assert fake.store.get("control:example__alpha:config_dirty") == "1"

    # publish_wake was attempted exactly once on the wake channel.
    wake_attempts = [
        channel
        for channel, _ in fake.published
        if channel == "orchestrator:wake:example__alpha"
    ]
    assert len(wake_attempts) == 1

    # The failure was logged at WARNING level on the helper logger.
    assert any(
        "publish_wake failed for example__alpha" in record.getMessage()
        and "settings" in record.getMessage()
        for record in caplog.records
    )
