"""Dashboard attention mounting, initial rendering and fragment counts."""
import re
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from src.models import PipelineState, RepoState
from src.web import app as web_app
from src.web.routes import dashboard


def test_attention_mount_and_counts(monkeypatch):
    states = [RepoState(name=f'example__{i}', url=f'https://github.com/example/{i}', state=state)
              for i, state in enumerate([
                  PipelineState.CODING, PipelineState.WATCH, PipelineState.FIX,
                  PipelineState.PAUSED, PipelineState.IDLE, PipelineState.IDLE,
              ])]
    states[-1].user_paused = True
    monkeypatch.setattr(web_app, 'get_all_repo_states', AsyncMock(return_value=(states, None)))
    alerts = AsyncMock(return_value=[])
    monkeypatch.setattr(dashboard, '_dashboard_alerts', alerts)
    client = TestClient(web_app.app)
    html = client.get('/').text
    assert html.index('id="status-bar"') < html.index('id="alerts-panel"') < html.index('id="repo-grid"')
    assert html.count('id="sandbox-badge-wrapper"') == 1
    for fragment in ['stats', 'alerts']:
        assert re.search(r'hx-get="/partials/' + fragment + r'"\s+hx-trigger="repo:state_change from:body, every 30s"', html)
    assert 'data-alert-row' not in html
    assert not client.get('/partials/alerts').text.strip()
    alerts.return_value = [dict(repo_name='example__decision', kind='DECISION',
                               reason='Choose an action', tone='warn', duration_seconds=0,
                               repo_url='/repo/example__decision', action_url='/decision',
                               action_text='Open decision')]
    for path in ['/', '/partials/alerts']:
        html = client.get(path).text
        assert 'Needs your action' in html and 'data-alert-row' in html
        assert 'Choose an action' in html
    html = client.get('/partials/stats').text
    pairs = re.findall(r'>(\d+)</span>\s*<span[^>]*>([^<]+)</span>', html)
    assert pairs == [('3', 'Working'), ('1', 'Needs your action'), ('2', 'Paused'), ('1', 'Idle')]
    # Attention counts decision rows even when no repository is in ERROR.
    assert dashboard._compute_stats(states)['alerts'] == 0
