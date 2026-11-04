"""Repo header notice destinations, countdowns and labelled actions."""
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from src.inhibitor import InhibitorType, WorkInhibitor
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus
from src.web.app import templates

from tests.web.templates.test_state_styles_canonical import _render_repo_summary, _repo


def header_html(state=PipelineState.PAUSED, kind=InhibitorType.RATE_LIMIT):
    repo = _repo(state)
    repo.user_paused = state == PipelineState.PAUSED
    repo.current_task = QueueTask(pr_id='PR-493', title='Header', status=TaskStatus.DOING)
    repo.current_pr = PRInfo(number=493, branch='pr-493', url='https://github.com/example/repo/pull/493')
    repo.active_inhibitors = [WorkInhibitor(
        inhibitor_type=kind, reason_text='Resource limit reported.', source_key='test:limit',
        expires_at=datetime.now(timezone.utc) + timedelta(minutes=12),
    )]
    return _render_repo_summary(repo)


@pytest.mark.parametrize('kind,anchor', [
    (InhibitorType.RATE_LIMIT, 'settings-usage'),
    (InhibitorType.SPEND_CEILING, 'settings-usage'),
    (InhibitorType.GITHUB_BUDGET_PAUSE, 'settings-github'),
    (InhibitorType.GITHUB_BUDGET_SLOWDOWN, 'settings-github'),
])
def test_notice_has_one_countdown_and_existing_destination(kind, anchor):
    html = header_html(kind=kind)
    assert html.count('data-usage-inhibitor=') == 1
    assert html.count('data-inhibitor-countdown') == 1
    assert '12m remaining' in html
    assert f'href="/settings#{anchor}"' in html
    assert f'id="{anchor}"' in Path('src/web/templates/settings.html').read_text()
    assert html.index('data-header-status') < html.index('data-usage-inhibitor') < html.index('</header>')


@pytest.mark.parametrize('state,labels', [
    (PipelineState.PAUSED, ['Resume']), (PipelineState.CODING, ['Pause', 'Stop']),
    (PipelineState.ERROR, ['Reset to IDLE']),
])
def test_header_actions_are_labelled_without_changing_requests(state, labels):
    html = header_html(state)
    header = html.split('</header>')[0]
    assert 'requested</span>' in header
    assert 'PR-493' in header and '>#493</a>' in header
    for label in labels:
        assert f'<span>{label}</span>' in header
    assert header.index('data-header-identity') < header.index('data-header-status') < header.index('data-report-age')
    assert 'h-11 sm:h-8' in header
    compact = templates.get_template('components/_controls.html').module.repo_controls(_repo(state))
    assert not re.search(r'<span>(Resume|Pause|Stop|Reset to IDLE)</span>', compact)
    if state == PipelineState.CODING:
        assert 'hx-post="/repos/example__repo/stop"' in header
        assert 'data-confirm-title="Stop"' in header


def test_expired_countdown_is_clamped():
    repo = _repo(PipelineState.PAUSED)
    now = datetime.now(timezone.utc)
    repo.active_inhibitors = [WorkInhibitor(
        inhibitor_type=InhibitorType.RATE_LIMIT, source_key='test:expired',
        expires_at=now - timedelta(seconds=1),
    )]
    html = templates.get_template('components/usage_inhibitors.html').render(
        repo=repo, usage_notice_detail=True, utcnow=lambda: now,
    )
    assert '0m remaining' in html
