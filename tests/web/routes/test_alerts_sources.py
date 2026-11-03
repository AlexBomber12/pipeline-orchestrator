"""Attention rows and every dashboard counter use the same actionable sources."""
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient
from src.models import PipelineState, RepoState
from src.web import app as web_app
from src.web.routes import dashboard

from tests.web.routes import test_guardrail_groups

context = test_guardrail_groups.context


@pytest.mark.parametrize('pipeline_state', [PipelineState.ERROR, PipelineState.WATCH])
@pytest.mark.parametrize(('status', 'count'), [
    (None, 1), ('failed', 1), ('pending', 0), ('deferred', 0), ('applied', 0),
])
async def test_sources_and_counts(context, monkeypatch, pipeline_state, status, count):
    state, command, redis = context
    state.state = pipeline_state
    if status:
        command.status = status
        command.reason = 'Bound approval explanation'
    receipt = command if status else None
    monkeypatch.setattr(dashboard, 'list_approvals', AsyncMock(return_value=[receipt] if receipt else []))
    monkeypatch.setattr(dashboard, 'load_approval', AsyncMock(return_value=receipt))
    unrelated = RepoState(name='example__error', url='https://github.com/example/error',
                          state=PipelineState.ERROR, error_message='Coder failed <unsafe>')
    states = [state, unrelated]
    alerts = await dashboard._dashboard_alerts(states, redis)
    assert len(alerts) == count + 1
    error = next(row for row in alerts if row['repo_name'] == unrelated.name)
    assert error['kind'] == 'ERROR' and error['action_text'] == 'Open'
    assert error['action_url'] == '/repo/example__error'
    if count:
        decision = next(row for row in alerts if row['repo_name'] == state.name)
        assert decision['kind'] == 'DECISION'
        assert decision['action_url'] == '/repo/example__alpha#guardrail-panel-example__alpha'
        assert decision['reason'] == ('Bound approval explanation' if status else 'excerpt')
        assert decision['action_text'] == ('View explanation' if status else 'Open decision')

    monkeypatch.setattr(web_app, 'get_all_repo_states', AsyncMock(return_value=(states, None)))
    monkeypatch.setattr(web_app.app.state, 'redis', redis, raising=False)
    monkeypatch.setattr(dashboard, 'get_upload_pending_count', AsyncMock(return_value=0))
    client = TestClient(web_app.app)
    html = client.get('/partials/alerts').text
    assert html.count('data-alert-row') == count + 1
    assert html.count('Decision needed') == count
    assert 'Coder failed &lt;unsafe&gt;' in html
    assert 'Retry' not in html and 'Approve' not in html and 'Hung' not in html
    assert 'bg-fail/10' in html
    if count:
        assert ('bg-warn/10' in html) == (status is None)
        assert decision['action_text'] in html
    assert client.get('/api/alerts').json() == {'has_alerts': True, 'count': count + 1}
    assert client.get('/api/stats').json()['alerts'] == count + 1
    assert f'>{count + 1}</span>' in client.get('/partials/stats').text


async def test_history_only_unavailable_and_duplicates(context, monkeypatch):
    state, _, redis = context
    state.state = PipelineState.WATCH
    view = dict(is_active=True, approval_binding='current', excerpt='', rule='Rule explanation')
    view['group'] = dashboard._guardrail_group(view)
    history = dict(view, is_active=False, group='history')
    unknown = dict(view, approval_unavailable=True)
    unknown['group'] = dashboard._guardrail_group(unknown)
    monkeypatch.setattr(dashboard, '_build_guardrail_pending_view', AsyncMock(
        return_value=[history, unknown, view, view]))
    rows = await dashboard._dashboard_alerts([state, state], redis)
    assert len(rows) == 1 and rows[0]['reason'] == 'Rule explanation'
    monkeypatch.setattr(dashboard, '_build_guardrail_pending_view', AsyncMock(return_value=[history, unknown]))
    assert await dashboard._dashboard_alerts([state], redis) == []
    state.state = PipelineState.ERROR
    state.current_task = state.current_pr = None
    assert len(await dashboard._dashboard_alerts([state, state], redis)) == 1


def test_legacy_hung_is_one_error_with_open_action():
    state = RepoState(name='example__legacy', url='https://github.com/example/legacy', state='HUNG')
    rows = dashboard._build_alerts([state])
    assert len(rows) == 1 and rows[0]['kind'] == 'ERROR'
    assert rows[0]['action_text'] == 'Open'
