"""A reproduction runnable unchanged against the pre-repair source tree."""
from __future__ import annotations

import asyncio
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from types import MethodType

from src.daemon.handlers.coding import CodingMixin
from src.models import PipelineState
from tests.runner import _helpers as h
from tests.test_coding import _publication_transport, _runner


def test_successful_fix_with_delayed_pr_head_resumes_without_operator_retry(monkeypatch):
    runner = _runner(monkeypatch)
    # Exercise the production builder (older helper fixtures model v1 records).
    runner._begin_publication_attempt = MethodType(CodingMixin._begin_publication_attempt, runner)
    runner._rehydrate_last_push_at = lambda pr: None
    remote = _publication_transport(monkeypatch, runner, code=0, stderr='')
    local = {'sha': 'a' * 40}
    from src.daemon import git_ops
    from src.daemon.handlers import coding
    original_git = git_ops._git
    def probe(repo, *args, **kwargs):
        if args[:1] == ('ls-remote',):
            return subprocess.CompletedProcess([], 0, stdout=f'{local["sha"]} refs/heads/pr-001\n')
        if args[:2] == ('rev-parse', 'HEAD'):
            return subprocess.CompletedProcess([], 0, stdout=local['sha'] + '\n')
        return original_git(repo, *args, **kwargs)
    monkeypatch.setattr(git_ops, '_git', probe)
    monkeypatch.setattr(coding, '_local_branch_head_sha', lambda *a: local['sha'])
    asyncio.run(runner.handle_coding())
    assert runner.state.state == PipelineState.WATCH
    requests = []
    runner._post_codex_review = lambda n: requests.append(n) or True
    runner._start_current_run_record('claude', 'opus')
    async def fix(*args, **kwargs):
        local['sha'] = 'b' * 40  # Independently observed on the remote branch too.
        return 0, 'Implementation and push complete; PR view still old.', ''
    monkeypatch.setattr(h.claude_cli, 'fix_review_async', fix)
    asyncio.run(runner.handle_fix())
    assert runner.state.publication_attempt.pending
    assert requests == []
    remote['head'] = 'b' * 40  # Provider finally adopts the expected commit.
    # Controlled time only affects the new persisted polling interval. The
    # original implementation polls on every call and needs no clock advance.
    module = sys.modules.get('src.daemon.publication')
    if module is not None:
        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime.now(timezone.utc) + timedelta(minutes=1)
        monkeypatch.setattr(module, 'datetime', Clock)
    asyncio.run(runner.handle_error())
    assert runner.state.state == PipelineState.WATCH
    assert not runner.state.publication_attempt.pending
    assert runner.state.current_pr.head_sha == 'b' * 40
    assert runner.state.current_pr.fix_iteration_count == 1
    assert requests == [402]
