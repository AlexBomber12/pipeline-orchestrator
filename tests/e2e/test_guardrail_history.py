"""Chromium verifies the real history disclosure using isolated retained receipts."""
import time

from playwright.sync_api import expect
from src.approval_commands import ApprovalCommand, approval_key, approval_recent_index
from src.models import PRInfo, QueueTask

from tests.e2e.test_dashboard_controls import _redis


def test_guardrail_history_keyboard(page, dashboard_url, testbed_slug):
    identity = time.time_ns()
    task = f'PR-{identity}'
    binding = f'e2e-history-{identity}'
    key = approval_key(testbed_slug, binding)
    index = approval_recent_index(testbed_slug)
    receipt = ApprovalCommand(
        binding=binding, repo_slug=testbed_slug, repo_url='https://github.com/example/repo',
        task=QueueTask(pr_id=task, title='Keyboard history', status='ERROR'),
        pr=PRInfo(number=1, branch=f'e2e-history-{identity}'),
        task_fingerprint='fixture', failure='fixture', status='applied', active=False,
        reason=f'Keyboard retained result {identity}',
    )
    assert not _redis('GET', key), 'receipt belongs to another owner'
    try:
        _redis('SET', key, receipt.model_dump_json())
        _redis('ZADD', index, str(-receipt.requested_at.timestamp()), binding)
        page.goto(f'{dashboard_url}/repo/{testbed_slug}', wait_until='domcontentloaded')
        history = page.locator('[data-guardrail-history]')
        row = history.locator(f'[data-pr-id="{task}"]')
        expect(history).to_be_visible()
        expect(row).not_to_be_visible()
        summary = history.locator('summary')
        summary.focus()
        summary.press('Enter')
        expect(row).to_be_visible()
        expect(row).to_contain_text(receipt.reason)
        expect(row.locator('button')).to_have_count(0)
        expect(history.locator(f'a[href="/repo/{testbed_slug}/rejects"]')).to_be_visible()
        summary.press('Space')
        expect(row).not_to_be_visible()
        assert _redis('GET', key) == receipt.model_dump_json(), 'disclosure must not mutate receipts'
    finally:
        _redis('ZREM', index, binding)
        _redis('DEL', key)
