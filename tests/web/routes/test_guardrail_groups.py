"""Binding-aware decisions share one classification with dashboard attention."""
from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient
from src.approval_commands import build_approval
from src.cancellation.storage import CancellationCause, GuardrailPending
from src.models import PipelineState, PRInfo, QueueTask, RepoState
from src.web import app as web_app
from src.web.routes import dashboard

from tests.web.test_guardrail_panel import _render_panel, _view


@pytest.mark.parametrize(('active', 'binding', 'status', 'same', 'unavailable', 'superseded', 'group'), [
    (True, 'now', None, True, False, False, 'action'),
    (True, 'now', 'pending', True, False, False, 'applying'),
    (True, 'now', 'deferred', True, False, False, 'applying'),
    (True, 'now', 'failed', True, False, False, 'failed'),
    (True, 'now', 'failed', False, False, False, 'action'),
    (True, 'now', 'applied', True, False, False, 'history'),
    (False, None, 'applied', False, False, False, 'history'),
    (False, None, None, False, False, False, 'history'),
    (True, 'now', None, True, True, False, 'unknown'),
    (True, None, 'failed', False, False, False, 'unknown'),
    (True, 'now', 'pending', True, False, True, 'history'),
])
def test_classification_and_controls(active, binding, status, same, unavailable, superseded, group):
    view = _view(is_active=active)
    view.update(approval_binding=binding, approval_unavailable=unavailable)
    if status:
        view['approval'] = dict(binding=binding if same else 'old', status=status,
                                reason='Recorded explanation', active=False if status == 'failed' else True,
                                superseded=superseded)
    view['group'] = dashboard._guardrail_group(view)
    assert view['group'] == group
    html = _render_panel(guardrail_pending=[view])
    assert ('approve-btn' in html) == (group == 'action')
    assert ('reject-btn' in html) == (group in {'action', 'applying', 'failed'})
    assert ('bg-warn/10' in html) == (group == 'action')
    assert ('bg-fail/10' in html) == (group == 'failed')
    assert ('Approved, being applied' in html) == (group == 'applying')
    assert ('data-guardrail-history' in html) == (group == 'history')
    if status:
        assert 'Recorded explanation' in html
    if group == 'failed':
        assert 'cannot be retried' in html
    if group == 'action':
        assert 'Approve current PR' in html


@pytest.fixture
def context(tmp_path, monkeypatch):
    repo = tmp_path / 'example__alpha'
    (repo / 'tasks').mkdir(parents=True)
    (repo / 'tasks/PR-42.md').write_text(
        '---\nstatus: ERROR\n---\n# PR-42: example\nBranch: pr-42\n'
        '- Type: ux\n- Complexity: low\n- Depends on: none\n'
    )
    state = RepoState(name=repo.name, url='https://github.com/example/alpha',
                      current_task=QueueTask(pr_id='PR-42', title='example', status='ERROR'),
                      current_pr=PRInfo(number=42, branch='pr-42', head_sha='a' * 40))
    raw = CancellationCause(category='ERROR', payload={'subsource': 'guardrail'}).to_redis()
    command = build_approval(repo.name, state, raw, repo)
    pending = GuardrailPending(repo_slug=repo.name, task_id='PR-42', rule='rule', excerpt='excerpt', recorded_at=1)
    monkeypatch.setattr(web_app, 'REPOS_DIR', str(tmp_path))
    monkeypatch.setattr(dashboard, 'list_pending_guardrail_decisions', AsyncMock(return_value=[pending]))
    redis = AsyncMock()
    redis.get.return_value = raw
    return state, command, redis


@pytest.mark.parametrize('same_binding', [True, False])
async def test_failed_receipt_and_multiple_results_for_one_task(context, monkeypatch, same_binding):
    state, current, redis = context
    failed = current.model_copy(update=dict(status='failed', reason='Failed HEAD explanation', active=False))
    older = current.model_copy(update=dict(binding='older', status='applied', reason='Earlier result',
                                           requested_at=current.requested_at - timedelta(seconds=1)))
    if not same_binding:
        state.current_pr.head_sha = 'b' * 40
    monkeypatch.setattr(dashboard, 'list_approvals', AsyncMock(return_value=[older, failed]))
    monkeypatch.setattr(dashboard, 'load_approval', AsyncMock(return_value=failed if same_binding else None))
    views = await dashboard._build_guardrail_pending_view(redis, state.name, state)
    assert views[0]['group'] == ('failed' if same_binding else 'action')
    assert {v['approval']['binding'] for v in views if v.get('approval')} == {failed.binding, older.binding}
    html = _render_panel(guardrail_pending=views)
    assert 'Failed HEAD explanation' in html and 'Earlier result' in html
    assert html.count('approve-btn') == (0 if same_binding else 1)
    assert all(v['group'] == 'history' for v in views[1:])


async def test_current_receipt_outside_recent_window_is_not_offered_again(context, monkeypatch):
    state, command, redis = context
    monkeypatch.setattr(dashboard, 'list_approvals', AsyncMock(return_value=[]))
    monkeypatch.setattr(dashboard, 'load_approval', AsyncMock(return_value=command))
    views = await dashboard._build_guardrail_pending_view(redis, state.name, state)
    assert views[0]['group'] == 'applying'


async def test_unknown_binding_and_unavailable_approval(context, monkeypatch):
    state, command, redis = context
    monkeypatch.setattr(dashboard, 'list_approvals', AsyncMock(return_value=[command]))
    state.current_pr.head_sha = ''
    views = await dashboard._build_guardrail_pending_view(redis, state.name, state)
    assert views[0]['group'] == 'unknown'
    monkeypatch.setattr(dashboard, 'list_approvals', AsyncMock(side_effect=RuntimeError('Redis unavailable')))
    views = await dashboard._build_guardrail_pending_view(redis, state.name, state)
    assert views[0]['group'] == 'unknown'
    assert 'reject-btn' not in _render_panel(guardrail_pending=views)


@pytest.mark.parametrize('group', ['action', 'applying', 'failed', 'unknown', 'history'])
async def test_dashboard_attention_uses_panel_classification(context, tmp_path, monkeypatch, group):
    state, _, redis = context
    cfg = tmp_path / 'config.yml'
    cfg.write_text('repositories:\n  - url: https://github.com/example/alpha\n')
    monkeypatch.setattr(web_app, 'get_repo_state', AsyncMock(return_value=state))
    monkeypatch.setattr(dashboard, 'get_upload_pending_count', AsyncMock(return_value=0))
    monkeypatch.setattr(dashboard, 'get_upload_admission_error', AsyncMock(return_value=None))
    monkeypatch.setattr(dashboard, '_build_guardrail_pending_view', AsyncMock(return_value=[dict(group=group)]))
    summary = AsyncMock(return_value={})
    monkeypatch.setattr(dashboard, '_work_summary', summary)
    await dashboard._repo_template_context(state.name, redis, str(cfg))
    assert summary.call_args.kwargs['guardrail_decision'] == (group == 'action')


async def test_inactive_receipts_remain_neutral_during_unrelated_coding(context, monkeypatch):
    state, command, redis = context
    command.status = 'applied'
    state.current_task = state.current_task.model_copy(update={'pr_id': 'PR-99'})
    state.state = PipelineState.CODING
    monkeypatch.setattr(dashboard, 'list_approvals', AsyncMock(return_value=[command]))
    views = await dashboard._build_guardrail_pending_view(redis, state.name, state)
    assert len(views) == 1 and views[0]['group'] == 'history'
    html = _render_panel(guardrail_pending=views)
    assert html.count('data-guardrail-history') == 1
    assert 'bg-warn/10' not in html and 'bg-fail/10' not in html
    assert 'approve-btn' not in html and 'reject-btn' not in html


@pytest.mark.parametrize(('group', 'expected'), [
    ('action', 1), ('failed', 1), ('applying', 0), ('unknown', 0), ('history', 0),
])
async def test_alerts_share_guardrail_classification(context, monkeypatch, group, expected):
    state, _, redis = context
    state.state = PipelineState.ERROR
    monkeypatch.setattr(dashboard, '_build_guardrail_pending_view', AsyncMock(return_value=[
        dict(is_active=True, group=group, approval_binding='binding', excerpt='reason',
             approval={'reason': 'failed reason'}), dict(is_active=False, group='history'),
    ]))
    assert len(await dashboard._dashboard_alerts([state], redis)) == expected
    # Unrelated errors still require attention even with retained history.
    monkeypatch.setattr(dashboard, '_build_guardrail_pending_view', AsyncMock(return_value=[
        dict(is_active=False, group='history'),
    ]))
    assert len(await dashboard._dashboard_alerts([state], redis)) == 1


@pytest.mark.parametrize(('status', 'expected'), [
    ('pending', 1), ('deferred', 1), ('failed', 2), (None, 2),
])
def test_stats_api_uses_binding_aware_alert_count(context, monkeypatch, status, expected):
    state, command, redis = context
    state.state = PipelineState.ERROR
    if status:
        command.status = status
    receipt = command if status else None
    monkeypatch.setattr(dashboard, 'list_approvals', AsyncMock(return_value=[receipt] if receipt else []))
    monkeypatch.setattr(dashboard, 'load_approval', AsyncMock(return_value=receipt))
    unrelated = RepoState(name='example__other', url='https://github.com/example/other',
                          state=PipelineState.ERROR)
    active = RepoState(name='example__active', url='https://github.com/example/active',
                       state=PipelineState.CODING)
    monkeypatch.setattr(web_app.app.state, 'redis', redis, raising=False)
    monkeypatch.setattr(web_app, 'get_all_repo_states', AsyncMock(return_value=([state, unrelated, active], None)))
    monkeypatch.setattr(dashboard, 'get_upload_pending_count', AsyncMock(return_value=0))
    client = TestClient(web_app.app)
    stats = client.get('/api/stats')
    alerts = client.get('/api/alerts')
    assert stats.status_code == alerts.status_code == 200
    assert stats.json()['alerts'] == alerts.json()['count'] == expected
    assert stats.json()['repos'] == 3
    assert stats.json()['active'] == 1
