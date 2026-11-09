"""Dashboard feed mount and shared v3 event rendering."""
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient
from src.models import RepoState
from src.web import app as web_app
from src.web.routes import dashboard


def test_activity_feed_mount_and_refresh(monkeypatch):
    entry = dict(time='2026-11-09T12:00:00+00:00', tier='FIX', kind='transition',
                 event='FIX -> ERROR: <failed>', severity='error', count=2)
    states = [RepoState(name='example__alpha', url='https://github.com/example/alpha',
                        history=[entry])]
    read = AsyncMock(return_value=(states, None))
    monkeypatch.setattr(web_app, 'get_all_repo_states', read)
    monkeypatch.setattr(dashboard, '_dashboard_alerts', AsyncMock(return_value=[]))
    client = TestClient(web_app.app)
    response = client.get('/')
    assert response.status_code == 200
    html = response.text
    assert 'xl:grid-cols-[minmax(0,2fr)_minmax(0,1fr)]' in html
    assert html.index('id="repo-grid"') < html.index('id="activity-feed"')
    assert 'hx-get="/partials/activity-feed"' in html
    assert 'hx-trigger="repo:event_log_append from:body, every 30s"' in html
    for body in (html, client.get('/partials/activity-feed').text):
        assert '/repo/example__alpha' in body
        assert 'data-event-severity="error"' in body
        assert 'data-event-category="FIX"' in body
        assert 'data-event-transition' in body
        assert 'repeated 2 times' in body
        assert 'data-ts="2026-11-09T12:00:00+00:00"' in body
        assert '&lt;failed&gt;' in body
        assert 'border bg-gray-500/10 text-gray-300 border-gray-500/30' in body
        assert body.count('Unknown: Severity not recorded.') == 1
    states[0].history = [dict(time='12:01:00', event='fresh entry')]
    body = client.get('/partials/activity-feed').text
    assert 'fresh entry' in body and '&lt;failed&gt;' not in body
    assert 'data-event-severity="unknown"' in body
    read.return_value = ([], None)
    assert 'No activity yet' in client.get('/').text
    assert 'No activity yet' in client.get('/partials/activity-feed').text
