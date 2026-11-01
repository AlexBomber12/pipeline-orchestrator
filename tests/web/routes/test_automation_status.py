"""Configured automation intent stays independent of pipeline liveness."""

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from src.inhibitor import InhibitorType, WorkInhibitor
from src.keyspace import pipeline_state
from src.models import PipelineState, RepoState
from src.web.app import app

from tests.web.routes.test_daemon_control import _FakeRedis, _stub_redis, _write_config


@pytest.mark.parametrize(('paused', 'panic', 'label'), [
    (0, False, 'Automation: on'),
    (1, False, 'Automation: on · 1 of 9 repos paused'),
    (9, False, 'Automation: paused (all repos)'),
    (9, True, 'Automation: panic, dispatch blocked'),
])
def test_configured_automation_status(tmp_path, monkeypatch, paused, panic, label):
    urls = [f'https://github.com/example/repo{i}.git' for i in range(9)]
    _write_config(tmp_path, monkeypatch, repo_urls=urls)
    redis = _FakeRedis()
    _stub_redis(monkeypatch, redis)
    for i, url in enumerate(urls):
        state = RepoState(name=f'example__repo{i}', url=url, user_paused=i < paused,
                          state=PipelineState.CODING)
        if panic and i == 0:
            state.active_inhibitors = [WorkInhibitor(
                inhibitor_type=InhibitorType.CASCADE_PANIC, source_key='panic',
            )]
        redis.values[pipeline_state(state.name)] = state.model_dump_json()
    with TestClient(app) as client:
        response = client.get('/partials/automation-status')
    assert response.status_code == 200
    assert f'>{label}</span>' in response.text
    assert ('text-fail' in response.text) == panic
    assert f'data-paused="{str(bool(paused)).lower()}"' in response.text
    assert 'Running' not in response.text


@pytest.mark.parametrize('failure', ['absent', 'ping', 'get', 'missing', 'decode'])
def test_automation_status_unavailable(tmp_path, monkeypatch, failure):
    _write_config(tmp_path, monkeypatch, repo_urls=['https://github.com/example/alpha.git'])
    redis = _FakeRedis()
    if failure == 'ping':
        async def ping():
            raise RuntimeError('offline')
        redis.ping = ping
    if failure == 'get':
        redis.get_error = RuntimeError('offline')
    if failure == 'decode':
        redis.values[pipeline_state('example__alpha')] = 'invalid'
    _stub_redis(monkeypatch, None if failure == 'absent' else redis)
    with TestClient(app) as client:
        response = client.get('/partials/automation-status')
    assert response.status_code == 200
    assert 'Automation status unavailable' in response.text


def test_empty_config_and_expired_panic(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch, repo_urls=[])
    redis = _FakeRedis()
    _stub_redis(monkeypatch, redis)
    with TestClient(app) as client:
        assert '>Automation: on</span>' in client.get('/partials/automation-status').text
        _write_config(tmp_path, monkeypatch, repo_urls=['https://github.com/example/alpha.git'])
        state = RepoState(name='example__alpha', url='https://github.com/example/alpha.git',
                          active_inhibitors=[WorkInhibitor(
                              inhibitor_type=InhibitorType.CASCADE_PANIC, source_key='panic',
                              expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
                          )])
        redis.values[pipeline_state(state.name)] = state.model_dump_json()
        assert '>Automation: on</span>' in client.get('/partials/automation-status').text
