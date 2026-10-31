"""Publication continuation against disposable real Git and controlled GitHub.

The older synthetic handler fixtures exercise version-1 compatibility. These
cases use the production version-2 builder and actual runner persistence/ticks.
"""
from __future__ import annotations

import asyncio
import subprocess
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from src.daemon import fix_supervision, publication
from src.daemon.handlers import coding
from src.github import checks, prs
from src.keyspace import control_stop, pipeline_state
from src.models import CIStatus, PipelineState, PRInfo, QueueTask, RepoState, ReviewStatus, TaskStatus

from tests.runner import _helpers as h


class Clock(datetime):
    value = datetime.now(timezone.utc)

    @classmethod
    def now(cls, tz=None):
        return cls.value


def git(path, *args):
    return subprocess.run(
        ['git', '-C', str(path), *args], check=True, capture_output=True, text=True,
    ).stdout.strip()


@pytest.fixture
async def lifecycle(tmp_path, monkeypatch):
    remote = tmp_path / 'remote.git'
    repo = tmp_path / 'repo'
    remote.mkdir()
    repo.mkdir()
    git(remote, 'init', '--bare', '--initial-branch=main')
    git(repo, 'init', '--initial-branch=main')
    git(repo, 'config', 'user.email', 'fixture@example.test')
    git(repo, 'config', 'user.name', 'Fixture')
    (repo / 'tasks').mkdir()
    (repo / 'tasks/PR-001.md').write_text(
        '# PR-001: Repair\n\nBranch: pr-001\n- Type: bugfix\n- Complexity: low\n- Depends on: none\n'
    )
    git(repo, 'add', '.')
    git(repo, 'commit', '-m', 'base')
    git(repo, 'remote', 'add', 'origin', str(remote))
    git(repo, 'push', 'origin', 'main')
    git(repo, 'checkout', '-b', 'pr-001')
    old = git(repo, 'rev-parse', 'HEAD')
    git(repo, 'push', 'origin', 'pr-001')
    (repo / 'repair').write_text('implemented repair\n')
    git(repo, 'add', 'repair')
    git(repo, 'commit', '-m', 'repair')
    expected = git(repo, 'rev-parse', 'HEAD')
    git(repo, 'push', 'origin', 'pr-001')
    Clock.value = datetime.now(timezone.utc)
    monkeypatch.setattr(publication, 'datetime', Clock)
    provider = {'head': old, 'state': prs.ExactPRState.OPEN, 'error': None,
                'complete': True, 'requests': [], 'calls': 0, 'ci': []}

    def exact(repo_name, base, number, sha, *, expected_branch=None):
        provider['calls'] += 1
        assert (repo_name, base, number, expected_branch) == ('octo/demo', 'main', 646, 'pr-001')
        if provider['error']:
            raise OSError(provider['error'])
        complete = provider['complete'] and sha == provider['head']
        return prs.ExactPROutcome(
            repo_name, base, number, sha,
            provider['state'] if complete else prs.ExactPRState.UNKNOWN, complete,
            reason=None if complete else 'head mismatch', observed_head_sha=provider['head'],
        )

    def retrieve(repo_name, sha, **kwargs):
        provider['ci'].append(sha)
        return None

    monkeypatch.setattr(prs, 'get_exact_pr_outcome', exact)
    monkeypatch.setattr(prs, 'get_branch_publications', lambda *a: [])
    monkeypatch.setattr(prs, 'get_open_prs', lambda *a, **kw: [])
    monkeypatch.setattr(checks, '_retrieve_ci_status_evidence', retrieve)
    monkeypatch.setattr(checks, '_classify_ci_retrieval', lambda *a, **kw: CIStatus.PENDING)
    monkeypatch.setattr(publication.gh_comments, 'latest_codex_review_request_time',
                        lambda *a, **kw: provider['requests'][-1] if provider['requests'] else None)

    def configure(runner):
        runner.repo_path = str(repo)
        runner.ensure_repo_cloned = AsyncMock()
        runner.preflight = AsyncMock(return_value=True)
        runner._refresh_user_paused_from_redis = AsyncMock()
        runner._consume_approval_command = AsyncMock(return_value=False)
        runner._consume_retry_command = AsyncMock(return_value=h.runner_module.RetryDispatch.NONE)
        runner._check_github_api_budget = AsyncMock(return_value=True)
        runner._should_skip_codex_review_post = lambda number: False
        runner._rehydrate_last_push_at = lambda pr: None
        def post(number):
            assert runner.state.current_pr.head_sha == expected
            provider['requests'].append(Clock.value)
            runner.state.review_requested_at = Clock.value
            return True
        runner._post_codex_review = post
        return runner

    runner = configure(h._make_runner(publication_contract=2))
    runner.state.current_task = QueueTask(pr_id='PR-001', title='repair', status=TaskStatus.DOING,
                                         branch='pr-001', task_file='tasks/PR-001.md')
    runner.state.current_pr = PRInfo(number=646, branch='pr-001', pr_id='PR-001', head_sha=old,
                                   ci_status=CIStatus.SUCCESS, review_status=ReviewStatus.APPROVED,
                                   push_count=1, observed_head_shas={old}, fix_iteration_count=3)
    runner._start_current_run_record('claude', 'opus')
    runner._capture_invocation_snapshot('claude', {}, phase='fix', fix_iteration=4)
    runner._begin_publication_attempt(started_at=Clock.value)
    runner.state.state = PipelineState.FIX
    await runner._checkpoint_current_run_record()
    return runner, provider, repo, old, expected, configure


async def finish(runner):
    await publication.record_exit(runner, 0, 'implemented; PR view still old', '')
    await runner._capture_published_attempt()
    await publication.record_cleanup(runner)


async def restart(runner, configure):
    fresh = configure(h._make_runner(publication_contract=2))
    fresh.redis = runner.redis
    fresh._metrics_store = type(runner._metrics_store)(fresh.redis)
    await fresh._run_cycle_body()  # actual recovery into a new runner
    return fresh


async def test_delayed_head_survives_fresh_runner_and_accounts_once(lifecycle):
    runner, provider, repo, old, expected, configure = lifecycle
    await finish(runner)
    await runner._hold_unresolved_publication()
    attempt = runner.state.publication_attempt
    assert attempt.expected_head_sha == expected
    assert attempt.observed_branch_sha == expected
    assert attempt.observed_pr_sha == old
    assert not runner._merge_gates_satisfied(runner.state.current_pr)
    assert provider['requests'] == []
    deadline, polls = attempt.reconcile_deadline, attempt.reconcile_polls
    fresh = await restart(runner, configure)
    assert fresh.state.publication_attempt.reconcile_deadline == deadline
    assert fresh.state.publication_attempt.reconcile_polls == polls
    assert fresh.state.active_invocation == runner.state.active_invocation
    # Advancing the checkout after exit cannot change the frozen operation.
    (repo / 'later-local').write_text('another local change')
    git(repo, 'add', 'later-local')
    git(repo, 'commit', '-m', 'unpublished later work')
    provider['head'] = expected
    Clock.value += timedelta(seconds=31)
    await fresh._run_cycle_body()
    pr = fresh.state.current_pr
    assert fresh.state.state == PipelineState.WATCH
    assert not fresh.state.publication_attempt.pending
    assert pr.head_sha == expected
    assert pr.fix_iteration_count == 4
    assert pr.push_count == 2
    assert pr.review_status == ReviewStatus.PENDING
    assert pr.ci_status == CIStatus.PENDING
    assert provider['ci'] == [expected]
    records = await fresh._metrics_store.recent(task_id='PR-001', repo_name=fresh.name)
    assert len(records) == 1
    assert records[0].head_sha == expected
    assert records[0].run_phase == 'fix'
    assert records[0].files_touched_count == 1
    assert len(provider['requests']) == 1
    assert git(repo, 'ls-remote', 'origin', 'refs/heads/pr-001').split()[0] == expected
    assert not fresh._merge_gates_satisfied(pr)
    await fresh._hold_unresolved_publication()
    assert pr.fix_iteration_count == 4
    assert len(provider['requests']) == 1


@pytest.mark.parametrize('boundary', ['exit', 'capture', 'cleanup', 'poll', 'account', 'metric', 'review'])
async def test_restart_at_publication_boundaries(lifecycle, monkeypatch, boundary):
    runner, provider, _, _, expected, configure = lifecycle
    await publication.record_exit(runner, 0, '', '')
    if boundary != 'exit':
        await runner._capture_published_attempt()
    if boundary not in {'exit', 'capture'}:
        await publication.record_cleanup(runner)
    if boundary in {'poll', 'account', 'metric', 'review'}:
        await runner._hold_unresolved_publication()
    if boundary in {'account', 'metric', 'review'}:
        provider['head'] = expected
        Clock.value += timedelta(seconds=31)
        publish = runner.publish_state
        async def crash_checkpoint():
            if boundary == 'review' and provider['requests']:
                raise RuntimeError('simulated daemon death')
            await publish()
            a = runner.state.publication_attempt
            reached = (a.push_accounted if boundary == 'account' else
                       a.continuation_saved if boundary == 'metric' else len(provider['requests']) == 1)
            if reached:
                raise RuntimeError('simulated daemon death')
        monkeypatch.setattr(runner, 'publish_state', crash_checkpoint)
        with pytest.raises(RuntimeError, match='simulated daemon death'):
            await runner._hold_unresolved_publication()
        monkeypatch.setattr(runner, 'publish_state', publish)
    fresh = await restart(runner, configure)
    assert fresh.state.publication_attempt.expected_head_sha == expected
    if boundary in {'exit', 'capture'}:
        assert fresh.state.state == PipelineState.ERROR
        assert 'cleanup proof' in fresh.state.error_message
        assert provider['requests'] == []
        return
    provider['head'] = expected
    Clock.value += timedelta(seconds=31)
    await fresh._run_cycle_body()
    assert fresh.state.state == PipelineState.WATCH
    assert fresh.state.current_pr.push_count == 2
    assert fresh.state.current_pr.fix_iteration_count == 4
    assert len(provider['requests']) == 1


@pytest.mark.parametrize('failure', ['implementation', 'cleanup', 'guardrail', 'escalation', 'unrelated'])
async def test_publication_cannot_erase_real_failure(lifecycle, failure):
    runner, provider, _, _, expected, configure = lifecycle
    provider['head'] = expected
    await publication.record_exit(
        runner, 1 if failure == 'implementation' else 0,
        ('gh repo create forbidden' if failure == 'guardrail'
         else 'ESCALATE: blocked' if failure == 'escalation' else ''), '',
    )
    await runner._capture_published_attempt()
    if failure != 'cleanup':
        await publication.record_cleanup(runner)
    if failure == 'unrelated':
        await runner._transition_to_error('unrelated failure')
    fresh = await restart(runner, configure)
    assert fresh.state.publication_attempt.pending
    assert fresh.state.state == PipelineState.ERROR
    assert provider['requests'] == []
    assert fresh.state.current_task is not None


async def test_stop_resume_keeps_budget_and_no_effects_while_paused(lifecycle):
    runner, provider, _, _, expected, configure = lifecycle
    await finish(runner)
    deadline = runner.state.publication_attempt.reconcile_deadline
    runner.state.user_paused = True
    runner.state.state = PipelineState.PAUSED
    provider['head'] = expected
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    fresh = await restart(runner, configure)
    assert fresh.state.user_paused
    assert fresh.state.state == PipelineState.PAUSED
    assert fresh.state.publication_attempt.reconcile_polls == 0
    fresh.state.user_paused = False
    await fresh.redis.set(pipeline_state(fresh.name), fresh.state.model_dump_json())
    await fresh.handle_paused()
    assert fresh.state.state == PipelineState.WATCH
    assert fresh.state.publication_attempt.reconcile_deadline == deadline
    assert len(provider['requests']) == 1


@pytest.mark.parametrize('problem', ['api', 'incomplete', 'advanced', 'missing', 'mismatch'])
async def test_bounded_uncertainty_preserves_work_and_actionable_evidence(lifecycle, monkeypatch, problem):
    runner, provider, repo, old, expected, configure = lifecycle
    await finish(runner)
    if problem == 'api':
        provider['error'] = 'provider unavailable'
    elif problem == 'incomplete':
        provider['complete'] = False
    elif problem == 'advanced':
        (repo / 'advance').write_text('another actor')
        git(repo, 'add', 'advance')
        git(repo, 'commit', '-m', 'another actor')
        git(repo, 'push', 'origin', 'pr-001')
        provider['head'] = expected
    elif problem == 'missing':
        git(repo, 'push', 'origin', '--delete', 'pr-001')
        provider['head'] = expected
    await runner._hold_unresolved_publication()
    assert runner.state.publication_attempt.pending
    fresh = await restart(runner, configure)
    Clock.value = fresh.state.publication_attempt.reconcile_deadline
    status = AsyncMock(return_value=True)
    monkeypatch.setattr(fresh, '_commit_task_status_change', status)
    await fresh._run_cycle_body()
    assert fresh.state.publication_attempt.exhausted
    assert not fresh.state.publication_attempt.pending
    assert fresh.state.current_task is not None
    assert fresh.state.state == PipelineState.ERROR
    assert status.call_args.args[1] == 'ERROR'
    assert 'expected=' + expected in status.call_args.args[2]
    assert 'branch=' in status.call_args.args[2] and 'PR=' in status.call_args.args[2]
    assert git(repo, 'rev-parse', 'refs/heads/pr-001')
    assert provider['requests'] == []


@pytest.mark.parametrize('completion', ['normal', 'merged', 'closed', 'stop'])
async def test_actual_fix_exit_enters_same_durable_continuation(lifecycle, monkeypatch, completion):
    runner, provider, repo, old, expected, configure = lifecycle
    # Model the actual invocation creating/pushing the repair, rather than
    # calling only its post-exit helpers. All ref mutations are fixture-owned.
    remote = git(repo, 'remote', 'get-url', 'origin')
    git(remote, 'update-ref', 'refs/heads/pr-001', old)
    git(repo, 'reset', '--hard', old)
    runner.state.publication_attempt = None
    await runner._save_current_run_record('coding_complete')
    runner.state.active_invocation = None
    runner._refresh_auth_status_cache = AsyncMock()
    runner.usage_gate = AsyncMock(return_value=True)
    external_flag = {}
    def monitor(_number, _task, flag):
        nonlocal external_flag
        external_flag = flag
        return None
    runner._run_coder_with_polling = monitor
    publish = runner.publish_state
    async def stop_checkpoint():
        if completion == 'stop' and getattr(runner, '_completed_fix_run_id', None):
            from src.keyspace import control_stop
            runner.state.user_paused = True
            await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
            await runner.redis.set(control_stop(runner.name), '1')
        await publish()
    monkeypatch.setattr(runner, 'publish_state', stop_checkpoint)
    runner.state.current_pr.review_status = ReviewStatus.PENDING
    _, plugin = runner._get_coder()
    calls = []
    async def coder(*args, **kwargs):
        calls.append(kwargs)
        git(repo, 'reset', '--hard', expected)
        git(repo, 'push', 'origin', 'pr-001')
        if completion in {'merged', 'closed'}:
            external_flag['state'] = completion.upper()
            provider['state'] = prs.ExactPRState(completion.upper())
        return 0, 'Implementation complete; branch pushed, PR observation stale.', ''
    monkeypatch.setattr(plugin, 'fix_review', coder)
    await runner.handle_fix()
    assert len(calls) == 1
    assert runner.state.publication_attempt.expected_head_sha == expected
    assert runner.state.publication_attempt.exit_classification == 'success'
    assert runner.state.publication_attempt.cleanup_confirmed
    assert runner.state.publication_attempt.pending
    if completion == 'stop':
        assert runner.state.state == PipelineState.PAUSED
        assert provider['requests'] == []
        return
    provider['head'] = expected
    fresh = await restart(runner, configure)
    monkeypatch.setattr(fresh, '_commit_task_status_change', AsyncMock(return_value=True))
    Clock.value += timedelta(seconds=31)
    await fresh._run_cycle_body()
    if completion == 'normal':
        assert fresh.state.state == PipelineState.WATCH
        assert fresh.state.current_pr.fix_iteration_count == 4
        assert len(provider['requests']) == 1
    else:
        assert fresh.state.state == PipelineState.IDLE
        assert fresh.state.current_task is None
        assert provider['requests'] == []
    assert len(calls) == 1


@pytest.mark.parametrize('terminal', ['MERGED', 'CLOSED'])
@pytest.mark.parametrize('exit_kind,boundary', [
    ('cancel', 'delayed'), ('cancel', 'cleanup'), ('cancel', 'reopened'),
    ('signal', 'delayed'), ('failure', 'delayed'),
])
async def test_supervisor_cancelled_fix_retains_terminal_only_continuation(
    lifecycle, monkeypatch, terminal, exit_kind, boundary,
):
    runner, provider, repo, old, expected, configure = lifecycle
    remote = git(repo, 'remote', 'get-url', 'origin')
    git(remote, 'update-ref', 'refs/heads/pr-001', old)
    git(repo, 'reset', '--hard', old)
    runner.state.publication_attempt = None
    await runner._save_current_run_record('coding_complete')
    runner.state.active_invocation = None
    runner._refresh_auth_status_cache = AsyncMock()
    runner.usage_gate = AsyncMock(return_value=True)
    runner.state.current_pr.review_status = ReviewStatus.PENDING
    ready = asyncio.Event()
    terminated, exited = asyncio.Event(), asyncio.Event()

    async def poll_tick(_seconds):
        await ready.wait()

    # Exercise the real external-state poller and actual target cancellation,
    # with only the polling clock/provider transport controlled by the fixture.
    monkeypatch.setattr(fix_supervision, 'asyncio', SimpleNamespace(
        sleep=poll_tick, to_thread=asyncio.to_thread, create_task=asyncio.create_task,
    ))
    monkeypatch.setattr(runner, '_monitor_fix_idle', AsyncMock())
    monkeypatch.setattr(prs, 'pr_state', lambda *a: {'state': terminal})
    if exit_kind != 'cancel':
        async def terminate():
            terminated.set()
            await exited.wait()
            return True
        monkeypatch.setattr(runner, '_terminate_current_coder', terminate)
    _, plugin = runner._get_coder()
    async def coder(*args, **kwargs):
        git(repo, 'reset', '--hard', expected)
        git(repo, 'push', 'origin', 'pr-001')
        ready.set()
        if exit_kind != 'cancel':
            await terminated.wait()
            exited.set()
            return (-15 if exit_kind == 'signal' else 1), 'coder exited during cleanup', ''
        await asyncio.Future()
    monkeypatch.setattr(plugin, 'fix_review', coder)
    status = AsyncMock(return_value=True)
    monkeypatch.setattr(runner, '_commit_task_status_change', status)
    if boundary == 'cleanup':
        monkeypatch.setattr(runner, '_confirm_current_coder_cleanup', AsyncMock(return_value=False))
    provider['state'] = prs.ExactPRState(terminal)
    await runner.handle_fix()
    attempt = runner.state.publication_attempt
    assert attempt.exit_classification == ('failure' if exit_kind == 'failure' else 'success')
    assert attempt.terminal_cancelled is (exit_kind != 'failure')
    assert attempt.expected_head_sha == expected
    assert attempt.pending
    assert status.await_count == 0
    assert provider['requests'] == []
    provider['head'] = expected
    if boundary == 'reopened':
        provider['state'] = prs.ExactPRState.OPEN
    def restore(fresh):
        configure(fresh)
        fresh._commit_task_status_change = status
        return fresh
    fresh = await restart(runner, restore)
    Clock.value += timedelta(seconds=31)
    await fresh._run_cycle_body()
    if boundary == 'delayed' and exit_kind != 'failure':
        assert fresh.state.state == PipelineState.IDLE
        assert fresh.state.current_task is None
        if terminal == 'MERGED':
            assert status.call_args.args[1] == 'DONE'
        else:
            assert status.await_count == 0  # existing closed-unmerged bookkeeping
        records = await fresh._metrics_store.recent(task_id='PR-001', repo_name=fresh.name)
        assert records[0].exit_reason == ('success_merged' if terminal == 'MERGED' else 'closed_unmerged')
    else:
        assert fresh.state.publication_attempt.pending
        assert fresh.state.current_task is not None
        assert fresh.state.state != PipelineState.WATCH
        assert status.await_count == 0
        if boundary == 'reopened':
            fresh._admit_observed_pr(fresh.state.current_pr)
            assert fresh.state.publication_attempt.pending
    assert provider['requests'] == []


@pytest.mark.parametrize('phase', ['coding', 'fix'])
@pytest.mark.parametrize('terminal', ['MERGED', 'CLOSED'])
async def test_external_terminal_races_require_head_and_cleanup(lifecycle, monkeypatch, phase, terminal):
    runner, provider, _, _, expected, configure = lifecycle
    if phase == 'coding':
        runner._capture_invocation_snapshot('claude', {}, phase='coding')
        runner._begin_publication_attempt(started_at=Clock.value, baseline_numbers=set())
        runner.state.publication_attempt.pr_number = 646
        runner.state.publication_attempt.candidate = runner.state.current_pr.model_copy(deep=True)
    await finish(runner)
    provider['head'] = expected
    provider['state'] = prs.ExactPRState(terminal)
    # Capture alone cannot release a terminal task while cleanup is unconfirmed.
    runner.state.publication_attempt.cleanup_confirmed = False
    assert not await runner._resolve_published_attempt(resolve_terminal=False)
    assert runner.state.current_task is not None
    runner.state.publication_attempt.cleanup_confirmed = True
    monkeypatch.setattr(runner, '_commit_task_status_change', AsyncMock(return_value=True))
    await runner._hold_unresolved_publication()
    assert runner.state.current_task is None
    assert runner.state.state == PipelineState.IDLE
    assert not runner.state.publication_attempt.pending
    assert provider['requests'] == []
    records = await runner._metrics_store.recent(task_id='PR-001', repo_name=runner.name)
    assert len(records) == 1
    assert records[0].exit_reason == ('success_merged' if terminal == 'MERGED' else 'closed_unmerged')
    assert not await runner._hold_unresolved_publication()


@pytest.mark.parametrize('problem', ['baseline', 'empty', 'ambiguous', 'wrong_base', 'wrong_branch', 'ready'])
async def test_auto_pr_discovery_uses_bound_dispatch_without_recreating(lifecycle, monkeypatch, problem):
    runner, provider, _, _, expected, _ = lifecycle
    runner.state.current_pr = None
    runner._capture_invocation_snapshot('claude', {}, phase='coding')
    runner._begin_publication_attempt(started_at=Clock.value, baseline_numbers=set())
    await finish(runner)
    attempt = runner.state.publication_attempt
    if problem == 'baseline':
        attempt.baseline_numbers = None
    item = prs.BranchPublication(
        number=646, title='PR-001: repair',
        base_branch='other' if problem == 'wrong_base' else 'main',
        head_branch='other' if problem == 'wrong_branch' else 'pr-001',
        head_sha=expected, state='OPEN', is_draft=False, is_cross_repository=False,
        created_at=Clock.value, url='https://github.com/octo/demo/pull/646',
    )
    monkeypatch.setattr(prs, 'get_branch_publications', lambda *a: [] if problem == 'empty' else
                        [item, item] if problem == 'ambiguous' else [item])
    provider['head'] = expected
    Clock.value += timedelta(seconds=31)
    await runner._hold_unresolved_publication()
    if problem == 'ready':
        assert runner.state.state == PipelineState.WATCH
        assert runner.state.current_pr.push_count == 1
        assert len(provider['requests']) == 1
    else:
        assert attempt.pending
        assert attempt.pr_number is None
        assert provider['requests'] == []


@pytest.mark.parametrize('problem', [
    'missing_candidate', 'probe_failure', 'ci_error', 'unknown', 'deadline', 'binding',
])
async def test_partial_observations_fail_closed(lifecycle, monkeypatch, problem):
    runner, provider, _, _, expected, _ = lifecycle
    await finish(runner)
    provider['head'] = expected
    a = runner.state.publication_attempt
    if problem == 'missing_candidate':
        a.candidate = None
    elif problem == 'probe_failure':
        provider['state'] = prs.ExactPRState.CLOSED
        monkeypatch.setattr(publication.git_ops, '_git', lambda *a, **kw:
                            subprocess.CompletedProcess([], 128, stdout='', stderr='unavailable'))
    elif problem == 'ci_error':
        monkeypatch.setattr(checks, '_retrieve_ci_status_evidence', lambda *a, **kw: 1 / 0)
    elif problem == 'unknown':
        provider['state'] = prs.ExactPRState.UNKNOWN
    elif problem == 'deadline':
        a.reconcile_deadline = None
    else:
        a.run_id = 'unrelated invocation'
    await runner._hold_unresolved_publication()
    if problem == 'ci_error':
        assert runner.state.state == PipelineState.WATCH
        assert runner.state.current_pr.ci_status == CIStatus.PENDING
    else:
        assert a.pending
        assert provider['requests'] == []


async def test_exhaustion_retries_only_failed_terminal_writes(lifecycle, monkeypatch):
    runner, provider, _, _, expected, configure = lifecycle
    await finish(runner)
    Clock.value = runner.state.publication_attempt.reconcile_deadline
    monkeypatch.setattr(runner, '_commit_task_status_change', AsyncMock(return_value=False))
    await runner._hold_unresolved_publication()
    assert runner.state.publication_attempt.exhausted
    assert runner.state.publication_attempt.pending
    fresh = await restart(runner, configure)
    assert fresh.state.publication_attempt.exhausted
    assert provider['calls'] == 1  # only capture, no extra provider poll
    monkeypatch.setattr(fresh, '_commit_task_status_change', AsyncMock(return_value=True))
    await fresh._run_cycle_body()
    assert fresh.state.current_task is not None
    assert not fresh.state.publication_attempt.pending
    assert provider['requests'] == []


async def test_lost_review_response_requires_available_provider_history(lifecycle, monkeypatch):
    runner, provider, _, _, expected, configure = lifecycle
    await finish(runner)
    provider['head'] = expected
    runner.state.publication_attempt.review_handoff_started = True
    def unavailable(*a, **kw):
        raise OSError('comments unavailable')
    monkeypatch.setattr(publication.gh_comments, 'latest_codex_review_request_time', unavailable)
    await runner._hold_unresolved_publication()
    assert runner.state.publication_attempt.pending
    assert provider['requests'] == []
    fresh = await restart(runner, configure)
    Clock.value += timedelta(seconds=31)
    monkeypatch.setattr(publication.gh_comments, 'latest_codex_review_request_time', lambda *a, **kw: None)
    await fresh._run_cycle_body()
    assert fresh.state.state == PipelineState.WATCH
    assert len(provider['requests']) == 1


async def test_exit_checkpoint_ignores_legacy_or_missing_attempt(lifecycle):
    runner, *_ = lifecycle
    runner.state.publication_attempt.contract_version = 1
    await publication.record_exit(runner, 0, '', '')
    assert runner.state.publication_attempt.exit_classification is None
    runner.state.publication_attempt = None
    await publication.record_exit(runner, 0, '', '')
    await publication.record_cleanup(runner)


@pytest.mark.parametrize('boundary', ['observation', 'accounting', 'review'])
async def test_stop_arriving_during_reconciliation_defers_effects(lifecycle, monkeypatch, boundary):
    runner, provider, _, _, expected, _ = lifecycle
    await finish(runner)
    provider['head'] = expected
    publish = runner.publish_state
    async def checkpoint():
        a = runner.state.publication_attempt
        stop = (a.reconcile_polls > 0 if boundary == 'observation' else
                a.push_accounted if boundary == 'accounting' else a.review_handoff_started)
        if stop:
            runner.state.user_paused = True
            await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
        await publish()
    monkeypatch.setattr(runner, 'publish_state', checkpoint)
    await runner._hold_unresolved_publication()
    assert runner.state.publication_attempt.pending
    assert provider['requests'] == []
    monkeypatch.setattr(runner, 'publish_state', publish)
    runner.state.user_paused = False
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    Clock.value += timedelta(seconds=31)
    await runner._hold_unresolved_publication()
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr.push_count == 2
    assert len(provider['requests']) == 1


@pytest.mark.parametrize('boundary', ['entry', 'observation', 'accounting', 'review'])
async def test_global_stop_latches_pause_until_play_across_restart(lifecycle, monkeypatch, boundary):
    runner, provider, _, _, expected, configure = lifecycle
    await finish(runner)
    provider['head'] = expected
    deadline = runner.state.publication_attempt.reconcile_deadline
    invocation = runner.state.active_invocation
    publish = runner.publish_state
    signalled = False

    async def checkpoint():
        nonlocal signalled
        a = runner.state.publication_attempt
        reached = (a.reconcile_polls > 0 if boundary == 'observation' else
                   a.push_accounted if boundary == 'accounting' else a.review_handoff_started)
        if reached and not signalled:
            # /daemon/stop writes only this key, without changing RepoState.
            await runner.redis.set(control_stop(runner.name), '1', ex=60)
            signalled = True
        await publish()

    if boundary == 'entry':
        await runner.redis.set(control_stop(runner.name), '1', ex=60)
    else:
        monkeypatch.setattr(runner, 'publish_state', checkpoint)
    await runner._hold_unresolved_publication()
    monkeypatch.setattr(runner, 'publish_state', publish)
    persisted = RepoState.model_validate_json(await runner.redis.get(pipeline_state(runner.name)))
    assert persisted.user_paused
    assert persisted.state == PipelineState.PAUSED
    assert persisted.publication_attempt.pending
    assert runner._stop_requested
    assert await runner.redis.get(control_stop(runner.name)) is None
    polls = persisted.publication_attempt.reconcile_polls
    Clock.value += timedelta(seconds=61)  # the original Stop TTL cannot resume work
    await runner._run_cycle_body()
    fresh = await restart(runner, configure)
    await fresh._run_cycle_body()
    assert fresh.state.user_paused
    assert fresh.state.state == PipelineState.PAUSED
    assert fresh.state.publication_attempt.reconcile_polls == polls
    assert fresh.state.publication_attempt.reconcile_deadline == deadline
    assert fresh.state.active_invocation == invocation
    assert provider['requests'] == []
    # Play clears the durable pause; continuation retains the existing budget.
    fresh.state.user_paused = False
    await fresh.redis.set(pipeline_state(fresh.name), fresh.state.model_dump_json())
    await fresh._run_cycle_body()
    assert fresh.state.state == PipelineState.WATCH
    assert fresh.state.publication_attempt.reconcile_deadline == deadline
    assert fresh.state.current_pr.push_count == 2
    assert len(provider['requests']) == 1


@pytest.mark.parametrize('snapshot_present', [False, True])
async def test_stop_pause_persistence_failure_retains_signal(lifecycle, monkeypatch, snapshot_present):
    runner, provider, _, _, expected, _ = lifecycle
    await finish(runner)
    provider['head'] = expected
    if not snapshot_present:
        await runner.redis.delete(pipeline_state(runner.name))
    await runner.redis.set(control_stop(runner.name), '1', ex=60)
    transaction = runner.redis.transaction

    async def unavailable(callback, *keys, **kwargs):
        # Read and prepare the transaction, then fail before EXEC commits it.
        await callback(runner.redis.pipeline())
        raise OSError('Redis transaction unavailable')

    monkeypatch.setattr(runner.redis, 'transaction', unavailable)
    with pytest.raises(OSError, match='Redis transaction unavailable'):
        await runner._hold_unresolved_publication()
    assert await runner.redis.get(control_stop(runner.name)) == '1'
    assert not runner.state.user_paused
    assert runner.state.publication_attempt.pending
    assert provider['requests'] == []
    monkeypatch.setattr(runner.redis, 'transaction', transaction)
    await runner._hold_unresolved_publication()
    persisted = RepoState.model_validate_json(await runner.redis.get(pipeline_state(runner.name)))
    assert persisted.user_paused
    assert persisted.state == PipelineState.PAUSED
    assert persisted.publication_attempt == runner.state.publication_attempt
    assert await runner.redis.get(control_stop(runner.name)) is None


async def test_exhaustion_status_exception_retains_terminal_incident(lifecycle, monkeypatch):
    runner, provider, _, _, _, _ = lifecycle
    await finish(runner)
    Clock.value = runner.state.publication_attempt.reconcile_deadline
    monkeypatch.setattr(runner, '_commit_task_status_change', AsyncMock(side_effect=OSError('base unavailable')))
    await runner._hold_unresolved_publication()
    assert runner.state.publication_attempt.pending
    assert runner.state.publication_attempt.exhausted
    assert 'base unavailable' in str(runner.state.history)
    assert provider['requests'] == []


async def test_exact_terminal_helper_does_not_bypass_cleanup_proof(lifecycle):
    runner, provider, _, _, expected, _ = lifecycle
    await publication.record_exit(runner, 0, '', '')
    provider['head'] = expected
    await runner._capture_published_attempt()
    assert runner.state.publication_attempt.verified
    assert not await runner._handle_external_pr_resolution(runner.state.current_pr, 'MERGED')
    assert runner.state.current_task is not None
    assert runner.state.state == PipelineState.ERROR


async def test_retry_cannot_bypass_missing_cleanup_proof(lifecycle, monkeypatch):
    from types import SimpleNamespace
    runner, *_ = lifecycle
    deferred = AsyncMock(return_value='deferred')
    monkeypatch.setattr(runner, '_defer_retry_command', deferred)
    assert await runner._select_retry_continuation(SimpleNamespace(selected_continuation=None)) == 'deferred'
    assert 'cleanup is unconfirmed' in deferred.call_args.args[1]


@pytest.mark.parametrize('process_failure', [False, True])
async def test_post_cleanup_observation_failure_is_distinct_from_process_failure(lifecycle, process_failure):
    runner, provider, _, _, expected, _ = lifecycle
    await finish(runner)
    provider['head'] = expected
    await runner._post_coder_resolution(
        'claude', 0, 'Implementation completed.', '', target_branch='pr-001', current_pr_id='PR-001',
        publication=coding._PublicationResolution(failure=OSError('recheck unavailable'),
                                                  process_failure=process_failure),
    )
    assert runner.state.state == (PipelineState.ERROR if process_failure else PipelineState.WATCH)
    assert runner.state.publication_attempt.pending is process_failure
    assert len(provider['requests']) == (0 if process_failure else 1)


async def test_daemon_publication_cancel_is_not_implementation_failure(lifecycle):
    runner, *_ = lifecycle
    await publication.record_exit(runner, -15, 'Published implementation.', '', publication_cancelled=True)
    assert runner.state.publication_attempt.exit_classification == 'success'
    assert not runner.state.publication_attempt.cleanup_confirmed


@pytest.mark.parametrize('at_cap', [False, True])
async def test_delayed_unchanged_fix_accounts_one_no_push_without_review(lifecycle, monkeypatch, at_cap):
    runner, provider, _, _, expected, configure = lifecycle
    a = runner.state.publication_attempt
    a.initial_pr_head_sha = expected
    a.initial_branch_head_sha = expected
    a.candidate.head_sha = expected
    a.candidate.observed_head_shas = {expected}
    a.candidate.no_push_fix_count = runner.app_config.daemon.fix_no_push_cap - 1 if at_cap else 0
    await finish(runner)
    await runner._hold_unresolved_publication()
    fresh = await restart(runner, configure)
    cap = AsyncMock()
    monkeypatch.setattr(fresh, '_escalate_fix_no_push_deadlock', cap)
    provider['head'] = expected
    Clock.value += timedelta(seconds=31)
    await fresh._run_cycle_body()
    assert not fresh.state.publication_attempt.pending
    assert fresh.state.current_pr.fix_iteration_count == 3
    assert fresh.state.current_pr.push_count == 1
    assert provider['requests'] == []
    assert cap.await_count == (1 if at_cap else 0)
    assert fresh.state.current_pr.no_push_fix_count == (runner.app_config.daemon.fix_no_push_cap if at_cap else 1)
    await fresh._hold_unresolved_publication()
    assert cap.await_count == (1 if at_cap else 0)


async def test_missing_run_accounting_defers_without_losing_attempt(lifecycle, monkeypatch):
    runner, provider, _, _, expected, configure = lifecycle
    await finish(runner)
    provider['head'] = expected
    runner._current_run_record = None
    get = runner._metrics_store.get
    monkeypatch.setattr(runner._metrics_store, 'get', AsyncMock(side_effect=OSError('metrics unavailable')))
    await runner._hold_unresolved_publication()
    assert runner.state.publication_attempt.pending
    assert not runner.state.publication_attempt.continuation_saved
    assert provider['requests'] == []
    monkeypatch.setattr(runner._metrics_store, 'get', get)
    Clock.value += timedelta(seconds=31)
    await runner._hold_unresolved_publication()
    assert runner.state.state == PipelineState.WATCH
    assert len(provider['requests']) == 1


async def test_pending_attempt_blocks_fix_merge_and_exit_zero_no_pr_diagnosis(lifecycle, monkeypatch):
    runner, provider, _, _, _, _ = lifecycle
    await finish(runner)
    await runner.handle_fix()
    assert runner.state.publication_attempt.pending
    await runner.handle_merge()
    assert runner.state.publication_attempt.pending
    diagnose = AsyncMock()
    monkeypatch.setattr(runner, '_diagnose_exit_zero_no_pr', diagnose)
    await runner._post_coder_resolution('claude', 0, '', '', target_branch='pr-001', current_pr_id='PR-001')
    assert runner.state.publication_attempt.pending
    diagnose.assert_not_called()
    assert provider['requests'] == []


async def test_header_recovery_defers_if_snapshot_read_is_unavailable(lifecycle, monkeypatch):
    runner, *_ = lifecycle
    monkeypatch.setattr(runner, '_hydrate_current_task_from_persisted_state', AsyncMock(return_value=False))
    assert not await runner._recover_state_headers()


@pytest.mark.parametrize('local_work,deferred', [(False, False), (False, True), (True, False)])
async def test_prepublication_crash_finalizes_only_after_safe_release(lifecycle, monkeypatch, local_work, deferred):
    runner, provider, repo, _, _, configure = lifecycle
    monkeypatch.setattr('src.task_status.gh_pr_get_merged_branches', lambda *a: set())
    monkeypatch.setattr(prs, 'get_merged_prs_discovery',
                        lambda *a, **kw: prs.PRDiscovery('octo/demo', 'main', [], not provider.get('merged_down')))
    provider['merged_down'] = deferred
    remote = git(repo, 'remote', 'get-url', 'origin')
    git(remote, 'update-ref', '-d', 'refs/heads/pr-001')
    git(repo, 'checkout', 'main')
    if not local_work:
        git(repo, 'branch', '-D', 'pr-001')
    (repo / 'tasks/PR-001.md').write_text(
        '# PR-001: repair\n\nBranch: pr-001\n- Type: feature\n'
        '- Complexity: low\n- Depends on: none\n- Priority: 2\n- Status: TODO\n',
    )
    git(repo, 'add', 'tasks/PR-001.md')
    git(repo, 'commit', '-m', 'complete task headers')
    git(repo, 'push', 'origin', 'main')
    runner.state.current_pr = None
    runner._capture_invocation_snapshot('claude', {}, phase='coding')
    runner._begin_publication_attempt(started_at=Clock.value, baseline_numbers=set())
    runner.state.publication_attempt.initial_branch_head_sha = ''
    runner.state.state = PipelineState.CODING
    await runner.publish_state()
    status = AsyncMock(return_value=True)
    def restore(fresh):
        configure(fresh)
        fresh._commit_task_status_change = status
        return fresh
    fresh = await restart(runner, restore)
    if deferred:
        assert not fresh.state.publication_attempt.pending
        assert fresh.state.current_task.pr_id == 'PR-001'
        assert status.await_count == 0
        fresh = await restart(fresh, restore)
        assert fresh.state.current_task.pr_id == 'PR-001'
        assert status.await_count == 0
        provider['merged_down'] = False
        fresh = await restart(fresh, restore)
    assert provider['requests'] == []
    if local_work:
        assert fresh.state.current_task is not None
        assert fresh.state.publication_attempt.pending
        assert fresh.state.state == PipelineState.ERROR
        assert git(repo, 'rev-parse', 'refs/heads/pr-001')
        assert status.await_count == 0
    else:
        assert fresh.state.current_task is None, [item['event'] for item in fresh.state.history]
        assert fresh.state.state == PipelineState.IDLE
        assert not fresh.state.publication_attempt.pending
        assert status.call_args.args[1] == 'ERROR'
        assert any('crashed, marking ERROR' in item['event'] for item in fresh.state.history)


@pytest.mark.parametrize('problem', ['stale_pr', 'stale_list', 'omitted_list', 'advanced', 'identity',
                                     'cleanup', 'unknown_pr', 'invalid', 'api', 'terminal_cancelled', 'converged'])
async def test_retry_retains_exhausted_expected_head_after_independent_work(lifecycle, monkeypatch, problem):
    from types import SimpleNamespace

    from src.cancellation import cause_key, get_cancellation_cause
    runner, provider, repo, old, expected, _ = lifecycle
    await finish(runner)
    Clock.value = runner.state.publication_attempt.reconcile_deadline
    monkeypatch.setattr(runner, '_commit_task_status_change', AsyncMock(return_value=True))
    await runner._hold_unresolved_publication()
    assert runner.state.current_task is not None  # uncertainty now retains execution
    assert runner.state.publication_attempt.exhausted
    # Model a later operator-resolved hold; the retry floor must still survive.
    runner.state.current_task = None
    # Another completed task may replace the active attempt. The durable cause
    # must still prevent operator Retry from admitting the older green PR head.
    runner.state.publication_attempt = None
    cause = await get_cancellation_cause(runner.redis, runner.name, 'PR-001')
    blob = cause.payload['publication_attempt']
    if problem == 'identity':
        blob['base'] = 'wrong-base'
    elif problem == 'cleanup':
        blob['cleanup_confirmed'] = False
    elif problem == 'terminal_cancelled':
        blob['terminal_cancelled'] = True
    elif problem == 'unknown_pr':
        blob['pr_number'] = None
    elif problem == 'invalid':
        cause.payload['publication_attempt'] = 'damaged'
    await runner.redis.set(cause_key(runner.name, 'PR-001'), cause.to_redis())
    if problem == 'advanced':
        (repo / 'external').write_text('preserve actor work')
        git(repo, 'add', 'external')
        git(repo, 'commit', '-m', 'external advancement')
        git(repo, 'push', 'origin', 'pr-001')
    provider['head'] = old if problem == 'stale_pr' else expected
    provider['error'] = 'provider unavailable' if problem == 'api' else None
    observed = PRInfo(number=646, branch='pr-001', pr_id='PR-001',
                      head_sha=old if problem == 'stale_list' else expected)
    monkeypatch.setattr(prs, 'get_open_prs', lambda *a, **kw: [] if problem == 'omitted_list' else [observed])
    deferred = AsyncMock(return_value='deferred')
    monkeypatch.setattr(runner, '_defer_retry_command', deferred)
    command = SimpleNamespace(selected_continuation=None, task_id='PR-001', task_branch='pr-001',
                              bound_pr_number=646, bound_pr_branch='pr-001', bound_pr_head_sha=expected)
    result = await runner._select_retry_continuation(command)
    if problem == 'converged':
        assert result == ('watch', observed)
        deferred.assert_not_called()
    else:
        assert result == 'deferred'
        assert deferred.await_count == 1
    assert runner.state.current_task is None
    assert provider['requests'] == []


@pytest.mark.parametrize('outcome', ['pending', 'open', 'terminal'])
async def test_operator_retry_uses_shared_pending_publication_continuation(lifecycle, monkeypatch, outcome):
    from types import SimpleNamespace
    runner, provider, _, _, expected, _ = lifecycle
    await finish(runner)
    if outcome != 'pending':
        provider['head'] = expected
    if outcome == 'terminal':
        provider['state'] = prs.ExactPRState.CLOSED
    deferred = AsyncMock(return_value='deferred')
    failed = AsyncMock(return_value='failed')
    monkeypatch.setattr(runner, '_defer_retry_command', deferred)
    monkeypatch.setattr(runner, '_fail_retry_command', failed)
    monkeypatch.setattr(runner, '_commit_task_status_change', AsyncMock(return_value=True))
    command = SimpleNamespace(selected_continuation=None, task_id='PR-001', task_branch='pr-001',
                              bound_pr_number=646, bound_pr_branch='pr-001', bound_pr_head_sha=expected)
    result = await runner._select_retry_continuation(command)
    if outcome == 'pending':
        assert result == 'deferred'
        assert runner.state.publication_attempt.pending
        assert provider['requests'] == []
    elif outcome == 'terminal':
        assert result == 'failed'
        assert runner.state.current_task is None
        assert provider['requests'] == []
    else:
        assert result == ('watch', runner.state.current_pr)
        assert runner.state.current_pr.push_count == 2
        assert runner.state.current_pr.fix_iteration_count == 4
        assert len(provider['requests']) == 1


@pytest.mark.parametrize('pending', [False, True])
async def test_failed_unpublished_dirty_attempt_survives_preflight_and_restart(lifecycle, pending):
    from src.daemon.preflight import PreflightMixin
    from src.subsource_registry import SuppressionReason
    from src.web.services.repo_state import compute_repo_dependents_count, load_current_queue

    runner, provider, repo, _, expected, configure = lifecycle
    remote = git(repo, 'remote', 'get-url', 'origin')
    git(remote, 'update-ref', '-d', 'refs/heads/pr-001')
    runner.state.current_pr = None
    runner._capture_invocation_snapshot('claude', {}, phase='coding')
    runner._begin_publication_attempt(started_at=Clock.value, baseline_numbers=set())
    attempt = runner.state.publication_attempt
    attempt.initial_branch_head_sha = ''
    attempt.expected_head_sha = expected
    attempt.head_sha = expected
    attempt.exit_classification = 'failure'
    attempt.cleanup_confirmed = True
    attempt.pending = pending
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = 'deterministic continuity failure'
    queue = [
        runner.state.current_task.model_copy(update={'status': TaskStatus.ERROR}),
        QueueTask(pr_id='PR-002', title='independent', status=TaskStatus.TODO, branch='pr-002'),
        QueueTask(pr_id='PR-003', title='dependent', status=TaskStatus.TODO,
                  branch='pr-003', depends_on=['PR-001']),
    ]
    # Real dispatch publishes DOING before the failed coder exits. A pending
    # dirty hold never reaches header reconstruction to replace that snapshot.
    runner.state.current_queue = [
        task.model_copy(update={'status': TaskStatus.DOING})
        if pending and task.pr_id == 'PR-001' else task
        for task in queue
    ]
    dirty = repo / 'continuity-dirty.txt'
    dirty.write_text('unpreserved failed attempt work\n')
    await runner._suppress_task('PR-001', SuppressionReason.CRASH, {})

    def restore(fresh):
        configure(fresh)
        # Exercise real preflight and ERROR dispatch, controlling only the
        # remote status observation (the status writer uses another checkout).
        fresh.preflight = PreflightMixin.preflight.__get__(fresh)
        fresh._remote_frontmatter_error_status_for_current_task = lambda: (
            'error', SuppressionReason.CRASH, expected,
        )
        return fresh

    restore(runner)
    await runner.publish_state()
    # More than the three-cycle auto-reset threshold, with a fresh runner
    # halfway through the unresolved publication case observed in GHA.
    for cycle in range(8):
        if pending and cycle == 4:
            runner = await restart(runner, restore)
        else:
            runner._recovered = True
            await runner._run_cycle_body()
        assert runner.state.state == PipelineState.ERROR
        assert runner.state.current_task.pr_id == 'PR-001'
        retained = runner.state.publication_attempt
        for field in ('task_id', 'branch', 'run_id', 'attempt_index', 'expected_head_sha',
                      'exit_classification', 'cleanup_confirmed', 'pending'):
            assert getattr(retained, field) == getattr(attempt, field)
        assert runner.state.error_message == 'deterministic continuity failure'
        assert dirty.read_text() == 'unpreserved failed attempt work\n'
        assert git(repo, 'branch', '--show-current') == 'pr-001'
        assert git(repo, 'rev-parse', 'HEAD') == expected
        assert runner._consecutive_dirty_cycles == 0
        # Pending-publication recovery returns before header reconstruction.
        # Its published queue must still expose the retained failure's edge.
        assert await load_current_queue(runner.name, runner.redis) == queue
        assert await compute_repo_dependents_count(
            str(repo.parent), runner.name, {'PR-001'}, runner.redis,
        ) == {'PR-001': 1}
    assert provider['requests'] == []


@pytest.mark.parametrize('case', ['failed', 'other-branch', 'done', 'success', 'unbound', 'paused', 'absent'])
async def test_held_failure_queue_projection_is_bound(lifecycle, case):
    from src.web.services.repo_state import load_current_queue

    runner, _, _, _, _, _ = lifecycle
    attempt = runner.state.publication_attempt
    attempt.exit_classification = 'success' if case == 'success' else 'failure'
    runner.state.state = PipelineState.PAUSED if case == 'paused' else PipelineState.ERROR
    task = runner.state.current_task.model_copy(update={
        'branch': 'another-generation' if case == 'other-branch' else attempt.branch,
        'status': TaskStatus.DONE if case == 'done' else TaskStatus.DOING,
    })
    if case == 'unbound':
        attempt.task_id = 'PR-999'
    runner.state.current_queue = None if case == 'absent' else [task]
    await runner.publish_state()
    expected = None if case == 'absent' else [task.model_copy(update={
        'status': TaskStatus.ERROR if case == 'failed' else task.status,
    })]
    assert await load_current_queue(runner.name, runner.redis) == expected


@pytest.mark.parametrize('published', [False, True])
@pytest.mark.parametrize('dirty', [False, True])
@pytest.mark.parametrize('diagnostic', ['capacity', 'rate_limit'])
@pytest.mark.parametrize('feedback_source', ['review', 'ci', 'both'])
@pytest.mark.parametrize('interrupted_before_work', [False, True])
async def test_provider_interrupted_fix_preserves_work_and_eventually_progresses(
    lifecycle, monkeypatch, published, dirty, diagnostic, feedback_source, interrupted_before_work,
):
    runner, provider, repo, old, expected, configure = lifecycle
    remote = git(repo, 'remote', 'get-url', 'origin')
    git(remote, 'update-ref', 'refs/heads/pr-001', old)
    git(repo, 'reset', '--hard', old)
    runner.state.publication_attempt = None
    await runner._save_current_run_record('coding_complete')
    runner.state.active_invocation = None
    runner.state.current_pr.review_status = ReviewStatus.PENDING
    finding = 'Repair the missing ownership check before accepting a publication.'
    ci_log = 'FAILED test_original_repair: ownership evidence was discarded'
    if feedback_source in {'review', 'both'}:
        runner.state.current_pr.review_status = ReviewStatus.CHANGES_REQUESTED
        runner._fix_review_feedback = finding
        runner._fix_review_head = old
        monkeypatch.setattr(prs, 'get_pr_metadata', lambda *a: {'head_sha': old})
    if feedback_source in {'ci', 'both'}:
        runner.state.current_pr.ci_status = CIStatus.FAILURE
        monkeypatch.setattr('src.daemon.handlers.fix._current_ci_failure', lambda *a: {
            'repo': 'octo/demo', 'pr_number': 646, 'head_sha': old,
            'contexts': [{'name': 'unit', 'source': 'check', 'check_id': 1, 'attempt': 1,
                          'state': 'failure', 'workflow_run_id': None, 'job_id': None}],
        })
        monkeypatch.setattr('src.daemon.handlers.fix._fetch_failed_ci_logs', lambda *a: ci_log)
    calls = []
    with (repo / ".git/info/exclude").open("a") as exclusions:
        exclusions.write("artifacts/\n")

    def setup(r):
        configure(r)
        r.repo_config.coder = "codex"
        r.app_config.daemon.coder_settings = {"codex": {"model": "gpt-5.4", "reasoning_effort": "high"}}
        r._refresh_auth_status_cache = AsyncMock()
        r.usage_gate = AsyncMock(return_value=True)
        r._run_coder_with_polling = lambda *args: None
        return r

    setup(runner)
    _, plugin = runner._get_coder()

    async def coder(path, *args, **kwargs):
        calls.append(kwargs)
        assert (finding in kwargs['extra_context']) is (feedback_source in {'review', 'both'})
        assert (ci_log in kwargs['extra_context']) is (feedback_source in {'ci', 'both'})
        if len(calls) == 1:
            if not interrupted_before_work:
                git(repo, 'reset', '--hard', expected)
            if published:
                git(repo, 'push', 'origin', 'pr-001')
                provider['head'] = old if interrupted_before_work else expected
            if dirty:
                (repo / 'unfinished').write_text('retain me')
            (repo / 'artifacts').mkdir(exist_ok=True)
            (repo / 'artifacts/ci.log').write_text('pytest 2% -- no result')
            assert bool(git(repo, 'status', '--porcelain')) is dirty
            message = ('ERROR: Selected model is at capacity. Please try a different model.\n'
                       'ERROR: Selected model is at capacity. Please try a different model.\n'
                       'ERROR: rollout recording failed: missing thread'
                       if diagnostic == 'capacity' else 'ERROR: HTTP 429 Too Many Requests. Try again in 2 hours.')
            return 1, '', 'Task: PR-429\n' + message
        assert kwargs['model'] == calls[0]['model'] == 'gpt-5.4'
        assert kwargs['reasoning_effort'] == calls[0]['reasoning_effort'] == 'high'
        assert git(repo, 'rev-parse', 'HEAD') == (old if interrupted_before_work else expected)
        assert not dirty or (repo / 'unfinished').read_text() == 'retain me'
        assert (repo / 'artifacts/ci.log').read_text() == 'pytest 2% -- no result'
        assert 'gate is unfinished' in kwargs['extra_context']
        assert kwargs['extra_context'].endswith(calls[0]['extra_context'])
        if interrupted_before_work:
            # Fresh invocation still knows which repair to implement.
            git(repo, 'reset', '--hard', expected)
        (repo / 'artifacts/ci.log').write_text('required gate passed')
        if not published or interrupted_before_work:
            git(repo, 'push', 'origin', 'pr-001')
        provider['head'] = expected
        return 0, 'Gate completed successfully; existing PR retained.', ''

    monkeypatch.setattr(type(plugin), 'fix_review', lambda self, *a, **kw: coder(*a, **kw))
    await runner.handle_fix()
    attempt = runner.state.publication_attempt
    assert attempt.provider_error == diagnostic
    assert attempt.exit_classification == 'failure'
    assert (runner.state.rate_limited_until is None) is (diagnostic == 'capacity')
    interrupted_id = runner.state.active_invocation.run_id
    interrupted = await runner._metrics_store.get(interrupted_id)
    assert interrupted.ended_at and interrupted.outcome == 'paused'
    fresh = await restart(runner, setup)
    fresh._recovered = True
    fresh.app_config.daemon.coder_settings = {"codex": {"model": "gpt-5.4-mini", "reasoning_effort": "low"}}
    assert fresh.state.publication_attempt.expected_head_sha == (old if interrupted_before_work else expected)
    assert fresh.state.publication_attempt.fix_feedback_context == calls[0]['extra_context']
    # Recovery must use durable admitted evidence, even if remote logs disappear.
    monkeypatch.setattr('src.daemon.handlers.fix._current_ci_failure',
                        lambda *a: pytest.fail('recovery fetched new CI evidence'))
    monkeypatch.setattr('src.daemon.handlers.fix._fetch_failed_ci_logs',
                        lambda *a: pytest.fail('recovery fetched new CI logs'))
    await fresh._run_cycle_body()
    assert len(calls) == 1
    fresh.state.user_paused = True
    Clock.value = attempt.provider_retry_at + timedelta(seconds=1)
    await fresh._run_cycle_body()
    assert len(calls) == 1
    fresh.state.user_paused = False
    await fresh.redis.set(pipeline_state(fresh.name), fresh.state.model_dump_json())
    await fresh.publish_state()
    await fresh._run_cycle_body()
    assert len(calls) == 2, "\n".join(e["event"] for e in fresh.state.history[-15:])
    assert fresh.state.state == PipelineState.WATCH
    assert len(provider['requests']) == 1
    assert fresh.state.review_requested_at is not None
    assert fresh.state.publication_attempt.initial_pr_head_sha == old
    assert fresh.state.publication_attempt.initial_branch_head_sha == old
    assert git(repo, 'rev-parse', 'HEAD') == expected
    assert (repo / 'artifacts/ci.log').read_text() == 'required gate passed'
    assert fresh.state.current_task.pr_id == 'PR-001'
    records = await fresh._metrics_store.list_task_runs(fresh.name, 'PR-001')
    assert any(record.run_id == interrupted_id and record.ended_at for record in records)
    assert all(record.ended_at for record in records)
    assert len({record.run_id for record in records}) == len(records)


@pytest.mark.parametrize('flag', [False, True])
async def test_legacy_interrupted_fix_expiry_is_stably_parked(lifecycle, monkeypatch, flag):
    runner, _, _, _, _, _ = lifecycle
    await publication.record_exit(runner, 1, '', 'unclassified failure')
    await publication.record_cleanup(runner)
    runner.repo_config.feature_flags.use_single_error_exit = flag
    runner.state.state = PipelineState.PAUSED
    runner.state.rate_limited_until = Clock.value + timedelta(minutes=30)
    await runner.handle_paused()
    assert runner.state.state == PipelineState.PAUSED
    assert runner._current_run_record.ended_at
    assert runner._current_run_record.outcome == "failed"
    Clock.value += timedelta(minutes=31)
    for _ in range(3):
        assert await publication.resume_provider_failure(runner)
        assert runner.state.state == PipelineState.ERROR
        assert runner.state.rate_limited_until is None
    events = [e for e in runner.state.history if 'lacks bound provider' in e['event']]
    assert len(events) == 1


async def test_persistent_capacity_is_bounded_without_spending_fix_allowance(lifecycle, monkeypatch):
    runner, provider, repo, old, expected, configure = lifecycle
    runner._current_run_record.fix_iterations = 3
    runner.app_config.daemon.fix_iteration_cap = 4
    remote = git(repo, 'remote', 'get-url', 'origin')
    git(remote, 'update-ref', 'refs/heads/pr-001', old)
    runner.state.publication_attempt = None
    await runner._save_current_run_record('coding_complete')
    runner.state.active_invocation = None
    runner.state.current_pr.review_status = ReviewStatus.PENDING
    runner._refresh_auth_status_cache = AsyncMock()
    runner.usage_gate = AsyncMock(return_value=True)
    runner._run_coder_with_polling = lambda *args: None
    calls = []
    _, plugin = runner._get_coder()

    async def unavailable(*args, **kwargs):
        calls.append(kwargs)
        git(repo, 'reset', '--hard', expected)
        return 1, '', 'ERROR: Selected model is at capacity. Please try a different model.'

    monkeypatch.setattr(plugin, 'fix_review', unavailable)
    await runner.handle_fix()
    for count in (1, 2):
        Clock.value = runner.state.publication_attempt.provider_retry_at + timedelta(seconds=1)
        await publication.resume_provider_failure(runner)
        assert len(calls) == count + 1
    Clock.value = runner.state.publication_attempt.provider_retry_at + timedelta(seconds=1)
    for _ in range(5):
        assert await publication.resume_provider_failure(runner)
        assert runner.state.state == PipelineState.ERROR
    assert len(calls) == 3
    assert 'exhausted' in runner.state.error_message
    assert git(repo, 'rev-parse', 'HEAD') == expected
    assert git(remote, 'rev-parse', 'refs/heads/pr-001') == old
    assert provider['requests'] == []
    records = await runner._metrics_store.list_task_runs(runner.name, 'PR-001')
    paused = [r for r in records if r.outcome == 'paused']
    assert len(paused) == 3
    assert all(r.ended_at for r in paused)
    assert len({r.run_id for r in paused}) == 3
    assert sorted(r.fix_iterations for r in paused) == [4, 5, 6]
    assert sorted(r.fix_recovery_baseline for r in paused) == [0, 1, 2]
    assert sorted(r.attempt_index for r in paused) == [2, 3, 4]
    assert all(len(r.invocations) == 1 and r.invocations[0].phase == "fix" for r in paused)


@pytest.mark.parametrize('problem', [
    'checkout', 'remote', 'cleanup', 'identity', 'accounting', 'unavailable', 'closed', 'exit', 'pr', 'invocation',
])
async def test_provider_recovery_retains_conflicting_or_missing_evidence(lifecycle, monkeypatch, problem):
    runner, provider, repo, old, expected, _ = lifecycle
    await publication.record_exit(runner, 1, '', 'ERROR: Selected model is at capacity')
    await publication.record_cleanup(runner)
    Clock.value = runner.state.publication_attempt.provider_retry_at + timedelta(seconds=1)
    attempt = runner.state.publication_attempt
    if problem == 'checkout':
        git(repo, 'reset', '--hard', old)
    elif problem == 'remote':
        (repo / 'other').write_text('independent')
        git(repo, 'add', 'other')
        git(repo, 'commit', '-m', 'independent')
        git(repo, 'push', 'origin', 'pr-001')
        git(repo, 'reset', '--hard', expected)
    elif problem == 'cleanup':
        attempt.cleanup_confirmed = False
    elif problem == 'identity':
        attempt.run_id = 'other-run'
    elif problem == 'exit':
        attempt.exit_classification = None
    elif problem == 'invocation':
        runner.state.active_invocation = None
    elif problem == 'pr':
        runner.state.current_pr.number = 647
    elif problem == 'accounting':
        runner._current_run_record = None
        runner._metrics_store.get = AsyncMock(return_value=None)
    elif problem == 'unavailable':
        provider['error'] = 'provider observation down'
        Clock.value = attempt.reconcile_deadline + timedelta(seconds=1)
    else:
        provider['head'] = expected
        provider['state'] = prs.ExactPRState.CLOSED
    dispatch = AsyncMock()
    monkeypatch.setattr(runner, 'handle_fix', dispatch)
    for _ in range(3):
        assert await publication.resume_provider_failure(runner)
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.current_task.pr_id == 'PR-001'
    assert runner.state.publication_attempt.pending
    assert dispatch.await_count == 0
    assert provider['requests'] == []


async def test_provider_pause_blocks_direct_handler_and_repairs_interrupted_accounting(lifecycle, monkeypatch):
    runner, provider, repo, old, expected, _ = lifecycle
    await publication.record_exit(runner, 1, '', 'ERROR: Selected model is at capacity')
    await publication.record_cleanup(runner)
    runner.state.user_paused = True
    invocation = runner.state.active_invocation
    runner.state.active_invocation = None
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    assert await publication.resume_provider_failure(runner)
    assert runner.state.state == PipelineState.PAUSED
    runner.state.active_invocation = invocation
    assert await publication.resume_provider_failure(runner)
    assert runner.state.state == PipelineState.PAUSED
    runner.state.user_paused = False
    runner._current_run_record.ended_at = None  # legacy exited record awaiting terminal checkpoint
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    Clock.value = runner.state.publication_attempt.provider_retry_at + timedelta(seconds=1)
    provider['head'] = expected
    resumed = AsyncMock()
    monkeypatch.setattr(runner, 'handle_fix', resumed)
    assert await publication.resume_provider_failure(runner)
    assert resumed.await_count == 1
    assert runner._current_run_record.ended_at


async def test_provider_retry_without_new_work_returns_to_watch_without_publication(lifecycle, monkeypatch):
    runner, provider, repo, old, _, _ = lifecycle
    remote = git(repo, 'remote', 'get-url', 'origin')
    git(remote, 'update-ref', 'refs/heads/pr-001', old)
    git(repo, 'reset', '--hard', old)
    provider['head'] = old
    runner.state.publication_attempt = None
    await runner._save_current_run_record('coding_complete')
    runner.state.active_invocation = None
    runner.state.current_pr.review_status = ReviewStatus.PENDING
    runner._refresh_auth_status_cache = AsyncMock()
    runner.usage_gate = AsyncMock(return_value=True)
    runner._run_coder_with_polling = lambda *args: None
    calls = []
    _, plugin = runner._get_coder()

    async def coder(*args, **kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            return 1, '', 'ERROR: HTTP 503 Service Unavailable'
        return 0, 'Required validation complete; no new changes.', ''

    monkeypatch.setattr(plugin, 'fix_review', coder)
    await runner.handle_fix()
    Clock.value = runner.state.publication_attempt.provider_retry_at + timedelta(seconds=1)
    await publication.resume_provider_failure(runner)
    assert len(calls) == 2
    assert runner.state.state == PipelineState.WATCH
    assert not runner.state.publication_attempt.pending
    assert git(repo, 'rev-parse', 'HEAD') == old
    assert git(remote, 'rev-parse', 'refs/heads/pr-001') == old
    assert provider['requests'] == []
    assert runner._current_run_record.ended_at


@pytest.mark.parametrize('observation', ['unavailable', 'lagging'])
async def test_provider_retry_waits_for_exact_publication_observation(lifecycle, monkeypatch, observation):
    runner, provider, _, _, expected, _ = lifecycle
    await publication.record_exit(runner, 1, '', 'ERROR: Selected model is at capacity')
    await publication.record_cleanup(runner)
    Clock.value = runner.state.publication_attempt.provider_retry_at + timedelta(seconds=1)
    if observation == 'unavailable':
        provider['error'] = 'temporary API failure'
    resumed = AsyncMock()
    monkeypatch.setattr(runner, 'handle_fix', resumed)
    assert await publication.resume_provider_failure(runner)
    assert runner.state.state == PipelineState.PAUSED
    assert resumed.await_count == 0
    assert runner.state.publication_attempt.provider_retries == 1
    provider['error'] = None
    provider['head'] = expected
    Clock.value = runner.state.publication_attempt.provider_retry_at + timedelta(seconds=1)
    assert await publication.resume_provider_failure(runner)
    assert resumed.await_count == 1
    assert runner.state.publication_attempt.provider_retries == 1


async def test_provider_retry_never_switches_provider(lifecycle, monkeypatch):
    runner, provider, _, _, expected, _ = lifecycle
    await publication.record_exit(runner, 1, '', 'ERROR: Selected model is at capacity')
    await publication.record_cleanup(runner)
    Clock.value = runner.state.publication_attempt.provider_retry_at + timedelta(seconds=1)
    provider['head'] = expected
    runner.repo_config.coder = 'codex'
    runner._refresh_auth_status_cache = AsyncMock()
    await publication.resume_provider_failure(runner)
    assert runner.state.state == PipelineState.ERROR
    assert 'provider changed' in runner.state.error_message
    assert runner.state.active_invocation.plugin_id == 'claude'
    assert runner._current_run_record.ended_at


async def test_operator_pause_at_provider_retry_checkpoint_prevents_launch(lifecycle, monkeypatch):
    runner, provider, _, _, expected, _ = lifecycle
    await publication.record_exit(runner, 1, '', 'ERROR: Selected model is at capacity')
    await publication.record_cleanup(runner)
    Clock.value = runner.state.publication_attempt.provider_retry_at + timedelta(seconds=1)
    provider['head'] = expected
    publish = runner.publish_state

    async def pause_at_checkpoint():
        if runner.state.publication_attempt.provider_retry_ready:
            runner.state.user_paused = True
            await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
        await publish()

    monkeypatch.setattr(runner, 'publish_state', pause_at_checkpoint)
    resumed = AsyncMock()
    monkeypatch.setattr(runner, 'handle_fix', resumed)
    assert await publication.resume_provider_failure(runner)
    assert runner.state.user_paused
    assert resumed.await_count == 0


async def test_expired_observed_quota_is_not_rescheduled_from_its_marker(lifecycle, monkeypatch):
    runner, provider, _, _, expected, _ = lifecycle
    await publication.record_exit(runner, 1, '', 'Error: HTTP 429 Too Many Requests. Try again in 2 hours.')
    await publication.record_cleanup(runner)
    attempt = runner.state.publication_attempt
    until = runner.state.rate_limited_until
    Clock.value = until + timedelta(seconds=1)
    assert not await publication.pause_provider_failure(runner, 'unrelated error', 'claude')
    assert attempt.provider_retries == 1
    assert runner.state.rate_limited_until == until
    provider['head'] = expected
    dispatch = AsyncMock()
    monkeypatch.setattr(runner, 'handle_fix', dispatch)
    assert await publication.resume_provider_failure(runner)
    assert runner.state.rate_limited_until is None
    assert dispatch.await_count == 1


@pytest.mark.parametrize('restart_at', ['none', 'grant', 'applied', 'scheduled'])
async def test_operator_retry_resumes_exhausted_unpublished_fix(lifecycle, monkeypatch, restart_at):
    from src.cancellation import get_cancellation_cause, task_spec_content_hash
    from src.daemon.retry_commands import RetryCommandMixin, RetryDispatch
    from src.retry_commands import (
        RetryCommandStatus,
        RetryExecutionState,
        enqueue_retry_command,
        load_retry_command,
        new_retry_command,
        retry_failure_identity,
    )

    monkeypatch.setattr("src.retry_commands.utc_now", lambda: Clock.value)
    runner, provider, repo, old, expected, configure = lifecycle
    remote = git(repo, 'remote', 'get-url', 'origin')
    git(remote, 'update-ref', 'refs/heads/pr-001', old)
    git(repo, 'reset', '--hard', old)
    runner.state.publication_attempt = None
    await runner._save_current_run_record('coding_complete')
    runner.state.active_invocation = None
    runner.state.current_pr.review_status = ReviewStatus.PENDING
    calls = []
    status_writes = []

    async def status(*args, **kwargs):
        status_writes.append(args[1])
        return True

    def setup(r):
        configure(r)
        r._consume_retry_command = RetryCommandMixin._consume_retry_command.__get__(r)
        r._refresh_auth_status_cache = AsyncMock()
        r.usage_gate = AsyncMock(return_value=True)
        r._run_coder_with_polling = lambda *args: None
        r._commit_task_status_change = status
        return r

    setup(runner)
    _, plugin = runner._get_coder()

    async def coder(*args, **kwargs):
        calls.append(kwargs)
        if len(calls) <= 3:
            git(repo, 'reset', '--hard', expected)
            return 1, '', 'ERROR: Selected model is at capacity'
        assert len(calls) == 4
        assert git(repo, 'rev-parse', 'HEAD') == expected
        assert git(remote, 'rev-parse', 'refs/heads/pr-001') == old
        assert 'gate is unfinished' in kwargs['extra_context']
        git(repo, 'push', 'origin', 'pr-001')
        provider['head'] = expected
        return 0, 'Required gate rerun passed; retained work published to the existing PR.', ''

    monkeypatch.setattr(type(plugin), 'fix_review', lambda self, *a, **kw: coder(*a, **kw))
    await runner.handle_fix()
    for _ in range(3):
        Clock.value = runner.state.publication_attempt.provider_retry_at + timedelta(seconds=1)
        await publication.resume_provider_failure(runner)
    assert len(calls) == 3
    assert runner.state.state == PipelineState.ERROR
    task = runner.state.current_task.model_copy(update={'status': TaskStatus.ERROR})
    runner.state.current_queue = [task]
    await runner.publish_state()
    cause = await get_cancellation_cause(runner.redis, runner.name, task.pr_id, refresh_ttl=False)
    pr = runner.state.current_pr
    command = new_retry_command(
        repo_slug=runner.name, task_id=task.pr_id, task_file=task.task_file, task_branch=task.branch,
        task_fingerprint=task_spec_content_hash((repo / task.task_file).read_text()),
        request_binding='b' * 64, failure_id=retry_failure_identity(task, runner.state, cause), retry_cap=3,
        bound_pr_number=pr.number, bound_pr_branch=pr.branch, bound_pr_head_sha=pr.head_sha,
    )
    await enqueue_retry_command(runner.redis, command)
    original_run = runner.state.active_invocation.run_id
    original_fix_count = runner._current_run_record.fix_iterations
    original_pr_count = pr.fix_iteration_count
    checkpoint = runner.publish_state

    class Interrupted(BaseException):
        pass

    async def crash_checkpoint():
        await checkpoint()
        a = runner.state.publication_attempt
        if a.provider_retry_command_id == command.command_id and (
            restart_at == 'grant' or restart_at == 'scheduled' and runner.state.state == PipelineState.PAUSED
        ):
            raise Interrupted()

    if restart_at in {'grant', 'scheduled'}:
        monkeypatch.setattr(runner, 'publish_state', crash_checkpoint)
    if restart_at == 'applied':
        mark = runner._mark_retry_applied
        async def crash_mark(*args, **kwargs):
            await mark(*args, **kwargs)
            raise Interrupted()
        monkeypatch.setattr(runner, '_mark_retry_applied', crash_mark)
    if restart_at != 'none':
        with pytest.raises(Interrupted):
            await runner._consume_retry_command()
        Clock.value += timedelta(minutes=6)  # expired claim lease after daemon death
        fresh = setup(h._make_runner(publication_contract=2))
        fresh.redis = runner.redis
        fresh._metrics_store = type(runner._metrics_store)(fresh.redis)
        # A new runner loads the actual retained snapshot before command replay.
        await fresh._hydrate_current_task_from_persisted_state()
        runner = fresh
    assert await runner._consume_retry_command() == RetryDispatch.HANDLED
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored.status == RetryCommandStatus.APPLIED, stored.outcome_reason
    assert stored.execution_state == RetryExecutionState.COMPLETED
    assert stored.selected_continuation == 'provider_fix'
    assert stored.retry_count == 1
    assert stored.reset_counters == ['provider_retries']
    assert runner.state.publication_attempt.provider_retries == 0
    assert runner.state.publication_attempt.pending
    assert runner.state.active_invocation.run_id == original_run
    assert runner._current_run_record.fix_iterations == original_fix_count
    assert runner.state.current_pr.fix_iteration_count == original_pr_count
    assert git(repo, 'rev-parse', 'HEAD') == expected
    assert git(remote, 'rev-parse', 'refs/heads/pr-001') == old
    assert status_writes == ['TODO']
    assert len(calls) == 3
    assert await runner._consume_retry_command() == RetryDispatch.NONE
    await publication.resume_provider_failure(runner)
    assert len(calls) == 4
    assert runner.state.state == PipelineState.WATCH
    assert git(remote, 'rev-parse', 'refs/heads/pr-001') == expected
    assert runner._current_run_record.ended_at
    assert runner.state.publication_attempt.provider_retry_command_id == command.command_id
    # Replay of an acknowledged grant cannot reset a later provider allowance.
    runner.state.publication_attempt.provider_retries = 2
    assert await runner._reconcile_applied_retry(stored) == RetryDispatch.HANDLED
    assert runner.state.publication_attempt.provider_retries == 2
    assert len(calls) == 4
    replayed = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert replayed.status == RetryCommandStatus.APPLIED


async def enqueue_bound_provider_retry(runner, repo):
    from src.cancellation import get_cancellation_cause, task_spec_content_hash
    from src.daemon.retry_commands import RetryCommandMixin
    from src.retry_commands import enqueue_retry_command, new_retry_command, retry_failure_identity

    task = runner.state.current_task.model_copy(update={'status': TaskStatus.ERROR})
    runner.state.current_queue = [task]
    await runner.publish_state()
    cause = await get_cancellation_cause(runner.redis, runner.name, task.pr_id, refresh_ttl=False)
    pr = runner.state.current_pr
    command = new_retry_command(
        repo_slug=runner.name, task_id=task.pr_id, task_file=task.task_file, task_branch=task.branch,
        task_fingerprint=task_spec_content_hash((repo / task.task_file).read_text()),
        request_binding='c' * 64, failure_id=retry_failure_identity(task, runner.state, cause), retry_cap=3,
        bound_pr_number=pr.number if pr else None, bound_pr_branch=pr.branch if pr else None,
        bound_pr_head_sha=pr.head_sha if pr else None,
    )
    await enqueue_retry_command(runner.redis, command)
    runner._consume_retry_command = RetryCommandMixin._consume_retry_command.__get__(runner)
    runner._commit_task_status_change = AsyncMock(return_value=True)
    return command


@pytest.mark.parametrize('problem', ['restored', 'changed', 'independent', 'remote'])
async def test_retry_after_restoring_interrupted_provider(lifecycle, monkeypatch, problem):
    from src.daemon.retry_commands import RetryCommandMixin, RetryDispatch
    from src.retry_commands import RetryCommandStatus, load_retry_command

    runner, provider, repo, old, expected, configure = lifecycle
    await publication.record_exit(runner, 1, '', 'ERROR: Selected model is at capacity')
    await publication.record_cleanup(runner)
    provider['head'] = expected
    runner.repo_config.coder = 'codex'
    runner._refresh_auth_status_cache = AsyncMock()
    Clock.value = runner.state.publication_attempt.provider_retry_at + timedelta(seconds=1)
    await publication.resume_provider_failure(runner)
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.publication_attempt.failure == publication.PROVIDER_RECOVERY_CHANGED
    assert runner.state.active_invocation.plugin_id == 'claude'
    command = await enqueue_bound_provider_retry(runner, repo)
    runner = await restart(runner, configure)
    runner.repo_config.coder = 'codex' if problem == 'changed' else 'claude'
    runner._consume_retry_command = RetryCommandMixin._consume_retry_command.__get__(runner)
    runner._commit_task_status_change = AsyncMock(return_value=True)
    if problem == 'independent':
        await runner._transition_to_error('independent failure after provider change', save_run_record_as=None)
    if problem == 'remote':
        git(repo, 'commit', '--allow-empty', '-m', 'external change')
        git(repo, 'push', 'origin', 'pr-001')
        git(repo, 'reset', '--hard', expected)
    assert await runner._consume_retry_command() == RetryDispatch.HANDLED
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    attempt = runner.state.publication_attempt
    assert git(repo, 'rev-parse', 'HEAD') == expected
    assert attempt.initial_pr_head_sha == old
    if problem != 'restored':
        assert stored.status != RetryCommandStatus.APPLIED
        assert runner.state.state == PipelineState.ERROR
        assert attempt.provider_retries == 1
        assert attempt.failure == publication.PROVIDER_RECOVERY_CHANGED
        runner._commit_task_status_change.assert_not_awaited()
        return
    assert stored.status == RetryCommandStatus.APPLIED
    assert stored.retry_count == 1
    assert attempt.failure is None and runner.state.error_message is None
    assert attempt.provider_retries == 0
    runner._refresh_auth_status_cache = AsyncMock()
    runner.usage_gate = AsyncMock(return_value=True)
    runner._run_coder_with_polling = lambda *args: None
    coder_name, plugin = runner._get_coder(allow_exploration=False)
    assert coder_name == 'claude'
    dispatch = AsyncMock(return_value=(0, 'Required gate passed on retained checkout.', ''))
    monkeypatch.setattr(type(plugin), 'fix_review', lambda *a, **kw: dispatch(*a, **kw))
    await publication.resume_provider_failure(runner)
    dispatch.assert_awaited_once()
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.active_invocation.plugin_id == 'claude'
    assert runner.state.publication_attempt.provider_retry_command_id == command.command_id
    assert len(provider['requests']) == 1


async def queued_exhausted_provider_retry(lifecycle, *, observation=False):
    runner, provider, repo, old, expected, _ = lifecycle
    remote = git(repo, 'remote', 'get-url', 'origin')
    git(remote, 'update-ref', 'refs/heads/pr-001', old)
    await publication.record_exit(runner, 1, '', 'ERROR: Selected model is at capacity')
    await publication.record_cleanup(runner)
    if observation:
        provider['error'] = 'GitHub unavailable'
        Clock.value = runner.state.publication_attempt.reconcile_deadline + timedelta(seconds=1)
    else:
        runner.state.publication_attempt.provider_retries = 3
        Clock.value = runner.state.publication_attempt.provider_retry_at + timedelta(seconds=1)
    await publication.resume_provider_failure(runner)
    command = await enqueue_bound_provider_retry(runner, repo)
    return runner, command, provider, repo, remote, old, expected


@pytest.mark.parametrize('operator_retry', [False, True])
async def test_provider_chain_retains_intermediate_published_head(lifecycle, monkeypatch, operator_retry):
    from src.daemon.retry_commands import RetryDispatch

    runner, provider, repo, old, first, configure = lifecycle
    remote = git(repo, 'remote', 'get-url', 'origin')
    git(remote, 'update-ref', 'refs/heads/pr-001', old)
    git(repo, 'reset', '--hard', old)
    runner.state.publication_attempt = None
    await runner._save_current_run_record('coding_complete')
    runner.state.active_invocation = None
    runner.state.current_pr.review_status = ReviewStatus.PENDING
    calls = []
    second = None

    def setup(r):
        configure(r)
        r._refresh_auth_status_cache = AsyncMock()
        r.usage_gate = AsyncMock(return_value=True)
        r._run_coder_with_polling = lambda *args: None
        def post(number):
            assert r.state.current_pr.head_sha == second
            provider['requests'].append(Clock.value)
            r.state.review_requested_at = Clock.value
            return True
        r._post_codex_review = post
        return r

    setup(runner)
    _, plugin = runner._get_coder()

    async def coder(*args, **kwargs):
        nonlocal second
        calls.append(kwargs)
        if len(calls) == 1:
            git(repo, 'reset', '--hard', first)
            git(repo, 'push', 'origin', 'pr-001')
            provider['head'] = first
        elif len(calls) == 2:
            (repo / 'second-repair').write_text('retained unpublished repair')
            git(repo, 'add', 'second-repair')
            git(repo, 'commit', '-m', 'second repair')
            second = git(repo, 'rev-parse', 'HEAD')
        else:
            assert len(calls) == 3
            assert git(repo, 'rev-parse', 'HEAD') == second
            assert git(remote, 'rev-parse', 'refs/heads/pr-001') == first
            git(repo, 'push', 'origin', 'pr-001')
            provider['head'] = second
            return 0, 'Required validation passed; retained second repair published.', ''
        return 1, '', 'ERROR: Selected model is at capacity'

    monkeypatch.setattr(type(plugin), 'fix_review', lambda self, *a, **kw: coder(*a, **kw))
    await runner.handle_fix()
    for _ in range(2):
        runner = await restart(runner, setup)
        attempt = runner.state.publication_attempt
        assert attempt.initial_pr_head_sha == old
        assert attempt.provider_published_head_sha == first
        assert runner.state.current_pr.head_sha == first
        assert git(remote, 'rev-parse', 'refs/heads/pr-001') == first
        if operator_retry and len(calls) == 2:
            attempt.provider_retries = 3
        Clock.value = attempt.provider_retry_at + timedelta(seconds=1)
        await publication.resume_provider_failure(runner)
    if operator_retry:
        assert runner.state.state == PipelineState.ERROR
        assert len(calls) == 2
        await enqueue_bound_provider_retry(runner, repo)
        assert await runner._consume_retry_command() == RetryDispatch.HANDLED
        await publication.resume_provider_failure(runner)
    assert len(calls) == 3
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr.head_sha == second
    assert git(remote, 'rev-parse', 'refs/heads/pr-001') == second
    assert runner.state.publication_attempt.initial_pr_head_sha == old
    assert len(provider['requests']) == 1


@pytest.mark.parametrize('problem', ['resolved', 'unavailable', 'independent', 'remote'])
async def test_retry_revalidates_exhausted_provider_observation(lifecycle, monkeypatch, problem):
    from src.daemon.retry_commands import RetryCommandMixin, RetryDispatch
    from src.retry_commands import RetryCommandStatus, load_retry_command

    monkeypatch.setattr('src.daemon.retry_commands.datetime', Clock)
    runner, command, provider, repo, remote, old, expected = await queued_exhausted_provider_retry(
        lifecycle, observation=True,
    )
    message = runner.state.error_message
    assert runner.state.publication_attempt.provider_observation_failure == message
    assert runner.state.publication_attempt.failure == message
    runner = await restart(runner, lifecycle[-1])
    runner._consume_retry_command = RetryCommandMixin._consume_retry_command.__get__(runner)
    runner._commit_task_status_change = AsyncMock(return_value=True)
    if problem != 'unavailable':
        provider['error'] = None
    if problem == 'independent':
        await runner._transition_to_error('independent implementation failure', save_run_record_as=None)
    if problem == 'remote':
        git(remote, 'update-ref', 'refs/heads/pr-001', expected)
        # An unrelated remote commit, rather than either retained owned head.
        git(repo, 'commit', '--allow-empty', '-m', 'external change')
        git(repo, 'push', 'origin', 'pr-001')
        git(repo, 'reset', '--hard', expected)
    assert await runner._consume_retry_command() == RetryDispatch.HANDLED
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    attempt = runner.state.publication_attempt
    assert git(repo, 'rev-parse', 'HEAD') == expected
    if problem != 'resolved':
        assert runner.state.state == PipelineState.ERROR
        assert attempt.provider_retries == 1
        assert attempt.failure == message
        assert runner.state.error_message == (
            'independent implementation failure' if problem == 'independent' else message
        )
        assert stored.status != RetryCommandStatus.APPLIED
        runner._commit_task_status_change.assert_not_awaited()
        return
    assert stored.status == RetryCommandStatus.APPLIED
    assert stored.retry_count == 1
    assert attempt.failure is None and attempt.provider_observation_failure is None
    assert runner.state.error_message is None
    assert attempt.provider_retries == 0
    assert attempt.reconcile_deadline > Clock.value
    assert git(remote, 'rev-parse', 'refs/heads/pr-001') == old
    dispatch = AsyncMock()
    monkeypatch.setattr(runner, 'handle_fix', dispatch)
    assert await publication.resume_provider_failure(runner)
    dispatch.assert_awaited_once()
    assert runner.state.state == PipelineState.FIX


@pytest.mark.parametrize('problem', ['pause', 'remote', 'cleanup', 'run', 'observation', 'cap', 'failure', 'binding'])
async def test_operator_provider_retry_requires_ownership_and_existing_admission(lifecycle, monkeypatch, problem):
    from src.cancellation import retry_count_key
    from src.daemon.retry_commands import RetryDispatch
    from src.retry_commands import RetryCommandStatus, load_retry_command

    runner, command, provider, repo, remote, old, expected = await queued_exhausted_provider_retry(lifecycle)
    if problem == 'pause':
        runner.state.user_paused = True
    elif problem == 'remote':
        (repo / 'independent').write_text('other work')
        git(repo, 'add', 'independent')
        git(repo, 'commit', '-m', 'independent')
        git(repo, 'push', 'origin', 'pr-001')
        git(repo, 'reset', '--hard', expected)
    elif problem == 'cleanup':
        runner.state.publication_attempt.cleanup_confirmed = False
    elif problem == 'run':
        runner._current_run_record = None
        monkeypatch.setattr(runner._metrics_store, 'get', AsyncMock(return_value=None))
    elif problem == 'observation':
        provider['error'] = 'API unavailable'
    elif problem == 'cap':
        await runner.redis.set(retry_count_key(runner.name, command.task_id), '3')
    elif problem == 'failure':
        runner.state.publication_attempt.failure = 'independent implementation failure'
    else:
        command.bound_pr_number = 647
        assert await runner._select_retry_continuation(command) == RetryDispatch.HANDLED
    if problem != 'binding':
        assert await runner._consume_retry_command() == RetryDispatch.HANDLED
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored.status in {RetryCommandStatus.DEFERRED, RetryCommandStatus.FAILED}
    assert stored.retry_count is None
    assert runner.state.publication_attempt.provider_retries == 3
    assert runner.state.publication_attempt.provider_retry_command_id is None
    assert runner.state.publication_attempt.pending
    assert git(repo, 'rev-parse', 'HEAD') == expected
    runner._commit_task_status_change.assert_not_awaited()


@pytest.mark.parametrize('boundary', ['checkpoint', 'applied', 'clear', 'completion'])
async def test_operator_provider_retry_recovers_failed_acknowledgements(lifecycle, monkeypatch, boundary):
    from src.daemon import retry_commands as retry_module
    from src.daemon.retry_commands import RetryDispatch
    from src.retry_commands import RetryCommandStatus, load_retry_command

    runner, command, _, repo, _, _, expected = await queued_exhausted_provider_retry(lifecycle)
    if boundary == 'checkpoint':
        original = runner.publish_state
        monkeypatch.setattr(runner, 'publish_state', AsyncMock(side_effect=OSError('state write failed')))
    elif boundary == 'applied':
        original = runner._mark_retry_applied
        monkeypatch.setattr(runner, '_mark_retry_applied', AsyncMock(side_effect=OSError('ack failed')))
    elif boundary == 'clear':
        original = runner._clear_retry_failure_evidence
        monkeypatch.setattr(runner, '_clear_retry_failure_evidence', AsyncMock(side_effect=OSError('clear failed')))
    else:
        original = retry_module.update_retry_command
        async def lost_ack(*args, **kwargs):
            if kwargs.get('keep_pending') is False:
                return None
            return await original(*args, **kwargs)
        monkeypatch.setattr(retry_module, 'update_retry_command', lost_ack)
    assert await runner._consume_retry_command() == RetryDispatch.HANDLED
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored.status == RetryCommandStatus.DEFERRED
    assert stored.retry_count == 1
    if boundary == 'checkpoint':
        assert runner.state.publication_attempt.provider_retries == 3
        assert runner.state.publication_attempt.provider_retry_command_id is None
        monkeypatch.setattr(runner, 'publish_state', original)
    elif boundary == 'applied':
        monkeypatch.setattr(runner, '_mark_retry_applied', original)
    elif boundary == 'clear':
        monkeypatch.setattr(runner, '_clear_retry_failure_evidence', original)
    else:
        monkeypatch.setattr(retry_module, 'update_retry_command', original)
    deadline = runner.state.publication_attempt.reconcile_deadline
    assert await runner._consume_retry_command() == RetryDispatch.HANDLED
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored.status == RetryCommandStatus.APPLIED
    assert stored.retry_count == 1
    assert runner.state.publication_attempt.provider_retries == 0
    assert runner.state.publication_attempt.pending
    assert git(repo, 'rev-parse', 'HEAD') == expected
    if boundary != 'checkpoint':
        assert runner.state.publication_attempt.reconcile_deadline == deadline
    runner._commit_task_status_change.assert_awaited_once()


@pytest.mark.parametrize('change', ['owner', 'receipt', 'independent'])
async def test_applied_provider_retry_replay_never_overrides_another_failure(lifecycle, change):
    from src.daemon.retry_commands import RetryDispatch
    from src.retry_commands import RetryCommandStatus, load_retry_command

    runner, command, _, _, _, _, _ = await queued_exhausted_provider_retry(lifecycle)
    assert await runner._consume_retry_command() == RetryDispatch.HANDLED
    stored = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert stored.status == RetryCommandStatus.APPLIED
    if change == 'owner':
        runner.state.publication_attempt.task_id = 'PR-999'
    elif change == 'receipt':
        runner.state.publication_attempt.provider_retry_command_id = None
    else:
        await runner._transition_to_error('independent implementation failure', save_run_record_as=None)
    assert await runner._resume_provider_retry(stored) == RetryDispatch.HANDLED
    replay = await load_retry_command(runner.redis, runner.name, command.command_id)
    assert replay.status == (RetryCommandStatus.APPLIED if change == 'independent' else RetryCommandStatus.FAILED)
    assert runner.state.publication_attempt.provider_retries == 0
    if change == 'independent':
        assert runner.state.state == PipelineState.ERROR
        assert runner.state.error_message == 'independent implementation failure'
        assert runner.state.publication_attempt.failure == 'independent implementation failure'
    elif change == 'receipt':
        assert await publication.resume_provider_failure(runner)
        assert runner.state.state == PipelineState.ERROR


@pytest.fixture
async def coding_provider(lifecycle, monkeypatch):
    """Real Git, normal CODING/exit/persistence paths, controlled provider transport."""
    runner, provider, repo, old, _, configure = lifecycle
    remote = git(repo, 'remote', 'get-url', 'origin')
    git(repo, 'checkout', 'main')
    git(repo, 'branch', '-D', 'pr-001')
    git(remote, 'update-ref', '-d', 'refs/heads/pr-001')
    with (repo / '.git/info/exclude').open('a') as stream:
        stream.write('artifacts/\n')
    provider.update(published=False, created_at=None, calls_to_coder=[], fail=True, work='dirty')
    runner.redis = h._FakeRedis()
    runner._metrics_store = type(runner._metrics_store)(runner.redis)
    runner._current_run_record = None
    runner.state.current_pr = None
    runner.state.active_invocation = None
    runner.state.publication_attempt = None
    runner.state.state = PipelineState.CODING
    monkeypatch.setattr(coding, 'datetime', Clock)

    def publications(*args):
        if not provider['published']:
            return []
        return [prs.BranchPublication(
            646, 'PR-001: repair', 'main', 'pr-001', provider['head'], 'OPEN', False, False,
            provider['created_at'], 'https://github.com/octo/demo/pull/646',
        )]

    monkeypatch.setattr(prs, 'get_branch_publications', publications)

    def setup(r):
        configure(r)
        r.repo_config.coder = 'codex'
        r.app_config.daemon.coder_settings = {'codex': {'model': 'gpt-5.4', 'reasoning_effort': 'high'}}
        r._refresh_auth_status_cache = AsyncMock()
        r.usage_gate = AsyncMock(return_value=True)
        def post(number):
            assert r.state.current_pr.head_sha == provider['head']
            provider['requests'].append(Clock.value)
            r.state.review_requested_at = Clock.value
            return True
        r._post_codex_review = post
        return r

    setup(runner)
    _, plugin = runner._get_coder()

    async def coder(path, **kwargs):
        calls = provider['calls_to_coder']
        calls.append(kwargs)
        first = len(calls) == 1
        assert kwargs['model'] == 'gpt-5.4'
        assert kwargs['reasoning_effort'] == 'high'
        if first:
            if provider['work'] != 'before_branch':
                git(repo, 'checkout', '-b', 'pr-001')
            if provider['work'] in {'commit', 'published', 'intermediate'}:
                (repo / 'repair').write_text('local implementation retained\n')
                git(repo, 'add', 'repair')
                git(repo, 'commit', '-m', 'retained implementation')
            if provider['work'] in {'dirty', 'commit', 'published', 'intermediate'}:
                (repo / 'retained-test.py').write_text('# interrupted test\n')
            if provider['work'] in {'published', 'intermediate'}:
                git(repo, 'push', 'origin', 'pr-001')
                provider.update(published=True, head=git(repo, 'rev-parse', 'HEAD'),
                                created_at=Clock.value + timedelta(seconds=1))
                if provider['work'] == 'intermediate':
                    provider['intermediate_head'] = provider['head']
                    if provider.get('proof_problem') == 'capture_unavailable':
                        provider['error'] = 'exact PR API unavailable'
                    elif provider.get('proof_problem') == 'capture_incomplete':
                        provider['complete'] = False
                    elif provider.get('proof_problem') == 'capture_remote':
                        git(remote, 'update-ref', 'refs/heads/pr-001', git(repo, 'rev-parse', 'main'))
                    # The actual monitor must observe A before this invocation
                    # adds unpublished B. No transition or ownership result is mocked.
                    await asyncio.wait_for(provider['publication_seen'].wait(), timeout=5)
                    (repo / 'local-followup').write_text('unpublished followup retained\n')
                    git(repo, 'add', 'local-followup')
                    git(repo, 'commit', '-m', 'unpublished followup after push')
            (repo / 'artifacts').mkdir(exist_ok=True)
            (repo / 'artifacts/ci.log').write_text('pytest 2% -- unfinished')
            provider['retained_head'] = git(repo, 'rev-parse', 'HEAD')
        else:
            assert git(repo, 'rev-parse', 'HEAD') == provider['retained_head']
            assert kwargs['task_body'].startswith(calls[0]['task_body'])
            assert 'gate is unfinished' in kwargs['task_body']
            assert 'never create a duplicate' in kwargs['task_body']
            assert (repo / 'artifacts/ci.log').read_text() == 'pytest 2% -- unfinished'
            if provider['work'] in {'dirty', 'commit', 'published', 'intermediate'}:
                assert (repo / 'retained-test.py').read_text() == '# interrupted test\n'
            if provider['work'] == 'before_branch':
                assert 'Create the original task branch as usual' in kwargs['task_body']
            else:
                assert 'do not create or recreate it' in kwargs['task_body']
        if provider['fail']:
            return 1, '', provider.get('failure_text', (
                'ERROR: Selected model is at capacity. Please try a different model.\n'
                'ERROR: Selected model is at capacity. Please try a different model.\n'
                'ERROR: rollout recording failed: missing thread'))
        if provider['work'] == 'before_branch':
            git(repo, 'checkout', '-b', 'pr-001')
        (repo / 'repair').write_text('completed implementation marker\n')
        git(repo, 'add', '.')
        git(repo, 'commit', '-m', 'complete retained implementation')
        git(repo, 'push', 'origin', 'pr-001')
        (repo / 'artifacts/ci.log').write_text('gate completed successfully')
        provider.update(published=True, head=git(repo, 'rev-parse', 'HEAD'),
                        created_at=provider['created_at'] or Clock.value + timedelta(seconds=1))
        return 0, 'Published the completed task to its one ready PR', ''

    monkeypatch.setattr(type(plugin), 'run_auto_pr', lambda self, *args, **kwargs: coder(*args, **kwargs))
    return runner, provider, repo, remote, setup


@pytest.mark.parametrize('work', ['before_branch', 'branch', 'dirty', 'commit', 'published'])
@pytest.mark.parametrize('single_error_exit', [False, True])
async def test_coding_capacity_recovers_same_work_after_restart(coding_provider, work, single_error_exit):
    runner, provider, repo, remote, setup = coding_provider
    provider['work'] = work
    runner.repo_config.feature_flags.use_single_error_exit = single_error_exit
    await runner.handle_coding()
    first = await runner._metrics_store.get(runner.state.active_invocation.run_id)
    assert first.ended_at and first.outcome == 'paused' and first.run_phase == 'coding'
    assert first.attempt_index == 1 and len(first.invocations) == 1
    attempt = runner.state.publication_attempt
    assert attempt.pending and attempt.provider_error == 'capacity' and attempt.cleanup_confirmed
    assert attempt.coding_task_body == (repo / 'tasks/PR-001.md').read_text()
    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.rate_limited_until is None
    fresh = await restart(runner, setup)
    fresh._recovered = True
    fresh.repo_config.feature_flags.use_single_error_exit = single_error_exit
    assert len(provider['calls_to_coder']) == 1
    # A current model-setting change must not change the interrupted invocation.
    fresh.app_config.daemon.coder_settings = {'codex': {'model': 'gpt-5.4-mini', 'reasoning_effort': 'low'}}
    Clock.value = attempt.provider_retry_at + timedelta(seconds=1)
    fresh.state.user_paused = True
    await fresh.redis.set(pipeline_state(fresh.name), fresh.state.model_dump_json())
    await fresh._run_cycle_body()
    assert len(provider['calls_to_coder']) == 1
    fresh.state.user_paused = False
    fresh.state.rate_limited_until = Clock.value + timedelta(minutes=30)
    await fresh.redis.set(pipeline_state(fresh.name), fresh.state.model_dump_json())
    await fresh._run_cycle_body()
    assert len(provider['calls_to_coder']) == 1
    Clock.value += timedelta(minutes=31)
    provider['fail'] = False
    await fresh._run_cycle_body()
    assert fresh.state.state == PipelineState.WATCH
    assert fresh.state.current_task.pr_id == 'PR-001' and fresh.state.current_pr.number == 646
    assert fresh.state.current_pr.head_sha == git(remote, 'rev-parse', 'refs/heads/pr-001')
    assert fresh.state.current_pr.ci_status == CIStatus.PENDING
    assert fresh.state.current_pr.review_status == ReviewStatus.PENDING
    assert len(provider['requests']) == 1 and len(provider['calls_to_coder']) == 2
    assert (repo / 'artifacts/ci.log').read_text() == 'gate completed successfully'
    records = await fresh._metrics_store.list_task_runs(fresh.name, 'PR-001')
    assert len(records) == 2 and all(r.ended_at for r in records)
    assert {r.attempt_index for r in records} == {1, 2}
    assert all(len(r.invocations) == 1 and r.fix_iterations == 0 for r in records)
    original = await fresh._metrics_store.get(first.run_id)
    assert original == first
    successor = next(r for r in records if r.run_id != first.run_id)
    assert successor.exit_reason == 'coding_complete' and successor.head_sha == provider['head']


@pytest.mark.parametrize('single_error_exit', [False, True])
async def test_persistent_coding_capacity_is_bounded_and_stable(coding_provider, single_error_exit):
    runner, provider, repo, _, setup = coding_provider
    runner.repo_config.feature_flags.use_single_error_exit = single_error_exit
    await runner.handle_coding()
    for _ in range(2):
        runner = await restart(runner, setup)
        runner.repo_config.feature_flags.use_single_error_exit = single_error_exit
        runner._recovered = True
        Clock.value = runner.state.publication_attempt.provider_retry_at + timedelta(seconds=1)
        await runner._run_cycle_body()
    Clock.value = runner.state.publication_attempt.provider_retry_at + timedelta(seconds=1)
    for _ in range(4):
        await runner._run_cycle_body()
        assert runner.state.state == PipelineState.ERROR
    assert len(provider['calls_to_coder']) == 3
    assert runner.state.error_message == publication.PROVIDER_RECOVERY_EXHAUSTED.replace('FIX', 'CODING')
    assert (repo / 'retained-test.py').read_text() == '# interrupted test\n'
    assert (repo / 'artifacts/ci.log').read_text() == 'pytest 2% -- unfinished'
    assert len([e for e in runner.state.history if e['event'].endswith(runner.state.error_message + '.')]) == 1
    records = await runner._metrics_store.list_task_runs(runner.name, 'PR-001')
    assert len(records) == 3 and all(r.ended_at and r.outcome == 'paused' for r in records)
    assert all(len(r.invocations) == 1 and r.fix_iterations == 0 for r in records)


@pytest.mark.parametrize('problem', [
    'context', 'spec', 'run', 'proof', 'counter', 'failure', 'error', 'branch',
    'local_head', 'remote_head', 'remote_unavailable', 'published_deleted',
    'bound_pr_missing', 'ambiguous', 'pr_identity', 'pr_lag', 'pr_unavailable', 'terminal',
    'dirty_base', 'provider_changed',
])
async def test_coding_recovery_preserves_independent_blockers(coding_provider, monkeypatch, problem):
    runner, provider, repo, remote, _ = coding_provider
    if problem == 'dirty_base':
        provider['work'] = 'before_branch'
    await runner.handle_coding()
    attempt = runner.state.publication_attempt
    if problem == 'context':
        attempt.coding_task_body = None
        runner._current_run_record.task_spec_hash = None
    elif problem == 'spec':
        with (repo / 'tasks/PR-001.md').open('a') as stream:
            stream.write('\nDifferent generation\n')
    elif problem == 'run':
        await runner.redis.delete('metrics:run:' + attempt.run_id)
        runner._current_run_record = None
    elif problem == 'proof':
        attempt.cleanup_confirmed = False
    elif problem == 'counter':
        attempt.provider_retries = 0
    elif problem == 'failure':
        attempt.failure = 'Independent guardrail failure'
    elif problem == 'error':
        runner.state.error_message = 'Independent operator decision'
    elif problem == 'branch':
        git(repo, 'checkout', 'main')
    elif problem == 'local_head':
        git(repo, 'commit', '--allow-empty', '-m', 'independent local work')
    elif problem == 'remote_head':
        git(repo, 'commit', '--allow-empty', '-m', 'independent remote work')
        git(repo, 'push', 'origin', 'pr-001')
        git(repo, 'reset', '--hard', attempt.expected_head_sha)
    elif problem == 'remote_unavailable':
        git(repo, 'remote', 'set-url', 'origin', str(repo / 'missing-remote'))
    elif problem == 'published_deleted':
        attempt.provider_published_head_sha = attempt.expected_head_sha
    elif problem == 'bound_pr_missing':
        attempt.pr_number = 646
    elif problem in {'ambiguous', 'pr_identity', 'pr_lag', 'pr_unavailable', 'terminal'}:
        from dataclasses import replace
        git(repo, 'push', 'origin', 'pr-001')
        provider.update(published=True, head=attempt.expected_head_sha,
                        created_at=attempt.started_at + timedelta(seconds=1))
        observe = prs.get_branch_publications
        if problem == 'ambiguous':
            monkeypatch.setattr(prs, 'get_branch_publications', lambda *a: observe(*a) * 2)
        elif problem == 'pr_identity':
            monkeypatch.setattr(prs, 'get_branch_publications',
                                lambda *a: [replace(observe(*a)[0], is_cross_repository=True)])
        elif problem == 'pr_lag':
            monkeypatch.setattr(prs, 'get_branch_publications',
                                lambda *a: [replace(observe(*a)[0], head_sha='a' * 40)])
        elif problem == 'pr_unavailable':
            provider['error'] = 'network outage'
        else:
            provider['state'] = prs.ExactPRState.MERGED
    elif problem == 'dirty_base':
        (repo / 'unknown-work').write_text('preserve unknown base work')
    elif problem == 'provider_changed':
        runner.repo_config.coder = 'claude'
    Clock.value = attempt.provider_retry_at + timedelta(seconds=1)
    assert await publication.resume_provider_failure(runner)
    assert len(provider['calls_to_coder']) == 1
    assert runner.state.state in {PipelineState.ERROR, PipelineState.PAUSED}
    if problem not in {'dirty_base'}:
        assert (repo / 'retained-test.py').read_text() == '# interrupted test\n'
    if runner.state.state == PipelineState.PAUSED:
        # Observation outages share the bounded publication observation budget.
        Clock.value = attempt.reconcile_deadline + timedelta(seconds=1)
        assert await publication.resume_provider_failure(runner)
        assert runner.state.state == PipelineState.ERROR
    before = len(runner.state.history)
    for _ in range(3):
        assert await publication.resume_provider_failure(runner)
        assert runner.state.state == PipelineState.ERROR
    assert len(provider['calls_to_coder']) == 1 and len(runner.state.history) == before


@pytest.mark.parametrize(('delayed_publication', 'restart_at'), [
    (False, 'none'), (True, 'none'), (True, 'observed'), (True, 'receipt'), (True, 'state'),
    (True, 'missing'), (True, 'changed'),
])
async def test_coding_exhaustion_retry_preserves_work_and_receipt(
    coding_provider, delayed_publication, restart_at, monkeypatch,
):
    from src.cancellation import get_cancellation_cause, task_spec_content_hash
    from src.daemon.retry_commands import RetryCommandMixin, RetryDispatch
    from src.retry_commands import (
        RetryCommandStatus,
        enqueue_retry_command,
        load_retry_command,
        new_retry_command,
        retry_failure_identity,
    )

    monkeypatch.setattr('src.retry_commands.utc_now', lambda: Clock.value)
    runner, provider, repo, _, configure = coding_provider
    if delayed_publication:
        provider['work'] = 'published'
        provider['hidden'] = restart_at != 'observed'
        observe = prs.get_branch_publications
        monkeypatch.setattr(prs, 'get_branch_publications',
                            lambda *a: [] if provider.get('hidden', True) else observe(*a))
    await runner.handle_coding()
    attempt = runner.state.publication_attempt
    if delayed_publication:
        assert attempt.pr_number == (646 if restart_at == 'observed' else None)
        assert (runner.state.current_pr.number if runner.state.current_pr else None) == (
            646 if restart_at == 'observed' else None
        )
    attempt.provider_retries = 3
    Clock.value = attempt.provider_retry_at + timedelta(seconds=1)
    assert await publication.resume_provider_failure(runner)
    assert runner.state.state == PipelineState.ERROR
    task = runner.state.current_task.model_copy(update={'status': TaskStatus.ERROR})
    runner.state.current_queue = [task]
    await runner.publish_state()
    cause = await get_cancellation_cause(runner.redis, runner.name, task.pr_id, refresh_ttl=False)
    pr = runner.state.current_pr
    command = new_retry_command(
        repo_slug=runner.name, task_id=task.pr_id, task_file=task.task_file, task_branch=task.branch,
        task_fingerprint=task_spec_content_hash((repo / task.task_file).read_text()),
        request_binding='d' * 64, failure_id=retry_failure_identity(task, runner.state, cause), retry_cap=3,
        bound_pr_number=pr.number if pr else None, bound_pr_branch=pr.branch if pr else None,
        bound_pr_head_sha=pr.head_sha if pr else None,
    )
    await enqueue_retry_command(runner.redis, command)
    provider['hidden'] = False
    runner._commit_task_status_change = AsyncMock(return_value=True)
    runner._consume_retry_command = RetryCommandMixin._consume_retry_command.__get__(runner)
    first = await runner._metrics_store.get(attempt.run_id)
    if restart_at not in {'none', 'observed'}:
        checkpoint = runner.publish_state

        class Interrupted(BaseException):
            pass

        async def crash_binding():
            if runner.state.current_pr is not None:
                if restart_at == 'state':
                    await checkpoint()
                raise Interrupted()
            await checkpoint()

        monkeypatch.setattr(runner, 'publish_state', crash_binding)
        with pytest.raises(Interrupted):
            await runner._consume_retry_command()
        stored = await load_retry_command(runner.redis, runner.name, command.command_id)
        assert stored.bound_pr_number == 646 and stored.retry_count is None
        Clock.value += timedelta(minutes=6)
        fresh = configure(h._make_runner(publication_contract=2))
        fresh.redis = runner.redis
        fresh._metrics_store = type(runner._metrics_store)(fresh.redis)
        fresh.state = RepoState.model_validate_json(await fresh.redis.get(pipeline_state(fresh.name)))
        await fresh._hydrate_current_task_from_persisted_state()
        runner = fresh
        attempt = runner.state.publication_attempt
        runner._consume_retry_command = RetryCommandMixin._consume_retry_command.__get__(runner)
        runner._commit_task_status_change = AsyncMock(return_value=True)
        if restart_at == 'missing':
            provider['hidden'] = True
        elif restart_at == 'changed':
            from dataclasses import replace
            observe_changed = prs.get_branch_publications
            monkeypatch.setattr(prs, 'get_branch_publications',
                                lambda *a: [replace(item, number=647) for item in observe_changed(*a)])
    assert await runner._consume_retry_command() == RetryDispatch.HANDLED
    receipt = await load_retry_command(runner.redis, runner.name, command.command_id)
    if restart_at in {'missing', 'changed'}:
        assert receipt.status in {RetryCommandStatus.DEFERRED, RetryCommandStatus.FAILED}
        assert receipt.retry_count is None and attempt.provider_retries == 3
        assert runner.state.state == PipelineState.ERROR
        assert len(provider['calls_to_coder']) == 1
        assert await runner._metrics_store.get(first.run_id) == first
        return
    assert receipt.status == RetryCommandStatus.APPLIED, receipt.outcome_reason
    if delayed_publication:
        assert receipt.bound_pr_number == attempt.pr_number == runner.state.current_pr.number == 646
        assert receipt.bound_pr_branch == runner.state.current_pr.branch == 'pr-001'
        assert receipt.bound_pr_head_sha == runner.state.current_pr.head_sha == provider['head']
    assert runner.state.publication_attempt.provider_retries == 0
    assert (repo / 'retained-test.py').read_text() == '# interrupted test\n'
    provider['fail'] = False
    assert await publication.resume_provider_failure(runner)
    assert runner.state.state == PipelineState.WATCH
    assert len(provider['calls_to_coder']) == 2
    assert await runner._consume_retry_command() == RetryDispatch.NONE
    assert len(provider['calls_to_coder']) == 2
    assert receipt.retry_count == 1
    assert await runner._metrics_store.get(first.run_id) == first
    records = await runner._metrics_store.list_task_runs(runner.name, 'PR-001')
    assert len(records) == 2 and all(record.ended_at for record in records)


async def test_coding_capacity_real_cli_exit_and_cleanup_survive_restart(coding_provider, monkeypatch, tmp_path):
    """Actual CLI subprocesses/supervisor, with a disposable executable provider shim."""
    import os
    import sys

    from src import codex_cli

    runner, provider, repo, remote, setup = coding_provider
    bindir = tmp_path / 'bin'
    bindir.mkdir()
    receipt = tmp_path / 'cli-calls.json'
    executable = bindir / 'codex'
    executable.write_text(f'''#!{sys.executable}
import json, pathlib, subprocess, sys
receipt = pathlib.Path({str(receipt)!r})
calls = json.loads(receipt.read_text()) if receipt.exists() else []
calls.append(sys.argv[1:]); receipt.write_text(json.dumps(calls))
def git(*args): subprocess.run(['git', *args], check=True, capture_output=True)
artifact = pathlib.Path('artifacts/ci.log'); artifact.parent.mkdir(exist_ok=True)
if len(calls) == 1:
    git('checkout', '-b', 'pr-001')
    pathlib.Path('retained-test.py').write_text('# real retained work\\n')
    artifact.write_text('pytest 2% -- no terminal result')
    print('ERROR: Selected model is at capacity. Please try a different model.', file=sys.stderr)
    print('ERROR: rollout recording failed: missing thread', file=sys.stderr)
    sys.exit(1)
assert pathlib.Path('retained-test.py').read_text() == '# real retained work\\n'
assert artifact.read_text() == 'pytest 2% -- no terminal result'
assert 'do not create or recreate it' in sys.argv[-1]
pathlib.Path('repair').write_text('completed real CLI marker\\n')
git('add', '.'); git('commit', '-m', 'complete real retained work'); git('push', 'origin', 'pr-001')
artifact.write_text('gate completed successfully')
print('Complete; one matching ready PR published')
''')
    executable.chmod(0o755)
    monkeypatch.setenv('PATH', str(bindir) + os.pathsep + os.environ['PATH'])
    monkeypatch.setattr(codex_cli, '_maybe_wrap_sandbox', lambda cmd, cwd, environment: cmd)
    _, plugin = runner._get_coder()

    async def actual_cli(self, path, **kwargs):
        result = await codex_cli.run_auto_pr_async(path, **kwargs)
        if result[0] == 0:
            provider.update(published=True, head=git(repo, 'rev-parse', 'HEAD'),
                            created_at=Clock.value + timedelta(seconds=1))
        return result

    monkeypatch.setattr(type(plugin), 'run_auto_pr', actual_cli)
    await runner.handle_coding()
    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.publication_attempt.cleanup_confirmed
    original_id = runner.state.active_invocation.run_id
    original = await runner._metrics_store.get(original_id)
    assert original.ended_at and original.outcome == 'paused'
    fresh = await restart(runner, setup)
    Clock.value = fresh.state.publication_attempt.provider_retry_at + timedelta(seconds=1)
    await publication.resume_provider_failure(fresh)
    assert fresh.state.state == PipelineState.WATCH
    assert fresh.state.current_pr.head_sha == git(remote, 'rev-parse', 'refs/heads/pr-001')
    assert git(repo, 'show', 'HEAD:retained-test.py') == '# real retained work'
    import json
    calls = json.loads(receipt.read_text())
    assert len(calls) == 2 and all('--model' in args and 'gpt-5.4' in args for args in calls)
    assert await fresh._metrics_store.get(original_id) == original
    records = await fresh._metrics_store.list_task_runs(fresh.name, 'PR-001')
    assert len(records) == 2 and all(r.ended_at and len(r.invocations) == 1 for r in records)


@pytest.mark.parametrize('change', [
    'missing', 'incomplete_pr', 'foreign_pr', 'wrong_snapshot', 'restorable_run', 'restored_context',
])
async def test_coding_context_and_record_identity(coding_provider, change):
    runner, provider, repo, _, _ = coding_provider
    if change == 'incomplete_pr':
        provider['work'] = 'published'
    await runner.handle_coding()
    attempt = runner.state.publication_attempt
    if change == 'missing':
        (repo / 'tasks/PR-001.md').unlink()
    elif change == 'incomplete_pr':
        provider['complete'] = False
    elif change == 'foreign_pr':
        runner.state.current_pr = PRInfo(number=999, branch='pr-other', head_sha='b' * 40)
    elif change == 'restored_context':
        attempt.coding_task_body = None
        provider['fail'] = False
    elif change == 'wrong_snapshot':
        runner._current_run_record.invocations[-1] = replace(runner.state.active_invocation, phase='fix')
    else:
        runner._current_run_record = None
        provider['fail'] = False
    Clock.value = attempt.provider_retry_at + timedelta(seconds=1)
    assert await publication.resume_provider_failure(runner)
    assert len(provider['calls_to_coder']) == (2 if change in {'restorable_run', 'restored_context'} else 1)
    assert runner.state.state == (PipelineState.WATCH if change in {'restorable_run', 'restored_context'}
                                  else PipelineState.ERROR if change in {'foreign_pr', 'wrong_snapshot'}
                                  else PipelineState.PAUSED)


@pytest.mark.parametrize('single_error_exit', [False, True])
async def test_coding_real_rate_limit_keeps_reset_deadline(coding_provider, single_error_exit):
    runner, provider, _, _, _ = coding_provider
    provider['failure_text'] = 'ERROR: HTTP 429 Too Many Requests. Try again in 2 hours.'
    runner.repo_config.feature_flags.use_single_error_exit = single_error_exit
    await runner.handle_coding()
    attempt = runner.state.publication_attempt
    until = runner.state.rate_limited_until
    assert attempt.provider_error == 'rate_limit'
    assert until > Clock.value + timedelta(minutes=119)
    Clock.value = until - timedelta(seconds=1)
    assert await publication.resume_provider_failure(runner)
    assert len(provider['calls_to_coder']) == 1 and runner.state.state == PipelineState.PAUSED
    Clock.value = until + timedelta(seconds=1)
    provider['fail'] = False
    await runner._run_cycle_body()
    assert runner.state.state == PipelineState.WATCH and runner.state.rate_limited_until is None
    assert len(provider['calls_to_coder']) == 2


@pytest.mark.parametrize('diagnostic', [
    'ERROR: invalid API key / authentication failed',
    'Task: PR-429\nFile: tasks/PR-429.md\nNetwork connection closed unexpectedly',
])
async def test_coding_non_provider_errors_do_not_get_capacity_retries(coding_provider, diagnostic):
    runner, provider, repo, _, _ = coding_provider
    provider['failure_text'] = diagnostic
    await runner.handle_coding()
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.publication_attempt.provider_error is None
    assert not await publication.resume_provider_failure(runner)
    assert len(provider['calls_to_coder']) == 1
    assert (repo / 'retained-test.py').read_text() == '# interrupted test\n'
    record = await runner._metrics_store.get(runner.state.active_invocation.run_id)
    assert record.ended_at and record.outcome == 'failed'


@pytest.mark.parametrize('missing', ['attempt', 'invocation'])
async def test_provider_failure_without_dispatch_evidence_is_not_scheduled(coding_provider, missing):
    runner, _, _, _, _ = coding_provider
    if missing == 'attempt':
        runner.state.publication_attempt = None
    else:
        runner._start_current_run_record('codex', 'gpt-5.4')
        runner._capture_invocation_snapshot('codex', {}, phase='coding')
        runner._begin_publication_attempt(started_at=Clock.value, baseline_numbers=set())
        runner.state.active_invocation = None
    assert not await publication.pause_provider_failure(runner, 'ERROR: Selected model is at capacity', 'codex')


async def test_coding_provider_retry_rechecks_selected_provider(coding_provider):
    runner, provider, _, _, _ = coding_provider
    await runner.handle_coding()
    runner.repo_config.coder = 'claude'
    problem = await publication.provider_retry_problem(runner, allow_provider_park=True)
    assert problem == publication.PROVIDER_RECOVERY_CHANGED.replace('FIX', 'CODING')
    assert len(provider['calls_to_coder']) == 1


async def test_coding_checkout_change_during_auth_cannot_launch(coding_provider):
    runner, provider, repo, _, _ = coding_provider
    await runner.handle_coding()
    original = await runner._metrics_store.get(runner.state.active_invocation.run_id)
    runner._refresh_auth_status_cache = AsyncMock(side_effect=lambda: git(repo, 'checkout', 'main'))
    Clock.value = runner.state.publication_attempt.provider_retry_at + timedelta(seconds=1)
    assert await publication.resume_provider_failure(runner)
    assert runner.state.state == PipelineState.ERROR and len(provider['calls_to_coder']) == 1
    assert await runner._metrics_store.get(original.run_id) == original


async def test_coding_pause_during_auth_prevents_new_invocation(coding_provider):
    runner, provider, _, _, _ = coding_provider
    await runner.handle_coding()
    async def pause():
        saved = runner.state.model_copy(deep=True)
        saved.user_paused = True
        await runner.redis.set(pipeline_state(runner.name), saved.model_dump_json())
    runner._refresh_auth_status_cache = pause
    runner._refresh_user_paused_from_redis = (
        h.runner_module.PipelineRunner._refresh_user_paused_from_redis.__get__(runner)
    )
    Clock.value = runner.state.publication_attempt.provider_retry_at + timedelta(seconds=1)
    assert await publication.resume_provider_failure(runner)
    assert runner.state.user_paused and len(provider['calls_to_coder']) == 1
    assert len(await runner._metrics_store.list_task_runs(runner.name, 'PR-001')) == 1


@pytest.mark.parametrize('expired', [False, True])
async def test_coding_observation_loss_after_auth_preserves_closed_history(coding_provider, expired):
    runner, provider, repo, _, _ = coding_provider
    await runner.handle_coding()
    original = await runner._metrics_store.get(runner.state.active_invocation.run_id)
    runner._refresh_auth_status_cache = AsyncMock(
        side_effect=lambda: git(repo, 'remote', 'set-url', 'origin', str(repo / 'unavailable')),
    )
    Clock.value = runner.state.publication_attempt.provider_retry_at + timedelta(seconds=1)
    if expired:
        runner.state.publication_attempt.reconcile_deadline = Clock.value - timedelta(seconds=1)
    assert await publication.resume_provider_failure(runner)
    assert len(provider['calls_to_coder']) == 1
    assert runner.state.state == (PipelineState.ERROR if expired else PipelineState.PAUSED)
    assert await runner._metrics_store.get(original.run_id) == original


async def test_coding_unavailable_selector_does_not_rewrite_predecessor(coding_provider):
    runner, provider, _, _, _ = coding_provider
    await runner.handle_coding()
    original = await runner._metrics_store.get(runner.state.active_invocation.run_id)
    runner.repo_config.disabled_coders = runner._registry.coder_names()
    Clock.value = runner.state.publication_attempt.provider_retry_at + timedelta(seconds=1)
    assert await publication.resume_provider_failure(runner)
    assert runner.state.state == PipelineState.PAUSED
    assert len(provider['calls_to_coder']) == 1
    assert await runner._metrics_store.get(original.run_id) == original
    Clock.value = runner.state.publication_attempt.reconcile_deadline + timedelta(seconds=1)
    assert await publication.resume_provider_failure(runner)
    assert runner.state.state == PipelineState.ERROR
    assert await runner._metrics_store.get(original.run_id) == original


async def test_coding_renewed_usage_pause_creates_no_phantom_attempt(coding_provider):
    runner, provider, _, _, _ = coding_provider
    await runner.handle_coding()
    original = await runner._metrics_store.get(runner.state.active_invocation.run_id)
    # Resource-policy tests own deriving the pause. This port failure tests that
    # provider admission does not create another run before that policy allows it.
    runner.usage_gate = AsyncMock(return_value=False)
    Clock.value = runner.state.publication_attempt.provider_retry_at + timedelta(seconds=1)
    assert await publication.resume_provider_failure(runner)
    runner.usage_gate.assert_awaited_once_with(proactive_coder='codex')
    assert len(provider['calls_to_coder']) == 1
    assert await runner._metrics_store.get(original.run_id) == original
    assert len(await runner._metrics_store.list_task_runs(runner.name, 'PR-001')) == 1


@pytest.mark.parametrize('boundary', ['baseline', 'credentials'])
async def test_coding_prelaunch_deferral_keeps_retained_dispatch(coding_provider, monkeypatch, boundary):
    runner, provider, _, _, _ = coding_provider
    await runner.handle_coding()
    original = await runner._metrics_store.get(runner.state.active_invocation.run_id)
    frozen = runner.state.publication_attempt.model_copy(deep=True)
    observe = prs.get_branch_publications
    reserve = runner._reserve_coder_credentials
    reads = 0

    def flaky_baseline(*args):
        nonlocal reads
        reads += 1
        if reads == 3:
            raise OSError('temporary prelaunch baseline outage')
        return observe(*args)

    if boundary == 'baseline':
        monkeypatch.setattr(prs, 'get_branch_publications', flaky_baseline)
    else:
        runner._reserve_coder_credentials = lambda *a, **kw: False
    Clock.value = frozen.provider_retry_at + timedelta(seconds=1)
    assert await publication.resume_provider_failure(runner)
    assert len(provider['calls_to_coder']) == 1
    assert runner.state.current_task is not None and runner.state.current_task.pr_id == 'PR-001'
    assert runner.state.publication_attempt.run_id == runner.state.active_invocation.run_id == original.run_id
    assert runner.state.publication_attempt.expected_head_sha == frozen.expected_head_sha
    assert runner.state.publication_attempt.provider_retries == frozen.provider_retries
    assert runner._current_run_record.run_id == original.run_id
    assert await runner._metrics_store.get(original.run_id) == original
    assert await runner.redis.get(runner._attempt_count_key(runner.name, 'PR-001')) == '1'
    assert len(await runner._metrics_store.list_task_runs(runner.name, 'PR-001')) == 1
    # Clearing a real transient boundary must progress, not merely avoid ERROR.
    runner._reserve_coder_credentials = reserve
    provider['fail'] = False
    Clock.value += timedelta(seconds=runner.app_config.daemon.fix_poll_interval_sec + 1)
    assert await publication.resume_provider_failure(runner)
    assert runner.state.state == PipelineState.WATCH
    assert len(provider['calls_to_coder']) == 2
    records = await runner._metrics_store.list_task_runs(runner.name, 'PR-001')
    assert sorted(r.attempt_index for r in records) == [1, 2]
    assert all(r.ended_at for r in records)


@pytest.mark.parametrize('restore_context', [False, True])
@pytest.mark.parametrize('single_error_exit', [False, True])
async def test_coding_crlf_task_recovery_preserves_original_fingerprint(
    coding_provider, restore_context, single_error_exit,
):
    runner, provider, repo, _, setup = coding_provider
    path = repo / 'tasks/PR-001.md'
    original = path.read_bytes().replace(b'\n', b'\r\n')
    git(repo, 'config', 'core.autocrlf', 'false')
    path.write_bytes(original)
    git(repo, 'add', 'tasks/PR-001.md')
    git(repo, 'commit', '-m', 'Windows-authored task')
    git(repo, 'push', 'origin', 'main')
    await runner.handle_coding()
    first = await runner._metrics_store.get(runner.state.active_invocation.run_id)
    assert runner.state.publication_attempt.coding_task_body == original.decode('utf-8')
    if restore_context:
        runner.state.publication_attempt.coding_task_body = None
        await runner.publish_state()
    fresh = await restart(runner, setup)
    fresh._recovered = True
    fresh.repo_config.feature_flags.use_single_error_exit = single_error_exit
    Clock.value = fresh.state.publication_attempt.provider_retry_at + timedelta(seconds=1)
    provider['fail'] = False
    await fresh._run_cycle_body()
    assert fresh.state.state == PipelineState.WATCH, fresh.state.error_message
    assert path.read_bytes() == original
    assert len(provider['calls_to_coder']) == 2
    assert provider['calls_to_coder'][1]['task_body'].startswith(original.decode('utf-8'))
    records = await fresh._metrics_store.list_task_runs(fresh.name, 'PR-001')
    assert len(records) == 2 and {r.task_spec_hash for r in records} == {first.task_spec_hash}
    assert await fresh._metrics_store.get(first.run_id) == first


@pytest.mark.parametrize('single_error_exit', [False, True])
@pytest.mark.parametrize('recovery', [
    'automatic', 'operator', 'missing_proof', 'foreign_remote',
    'capture_unavailable', 'capture_incomplete', 'capture_remote',
])
async def test_coding_monitor_retains_intermediate_publication(
    coding_provider, monkeypatch, single_error_exit, recovery,
):
    from src.daemon.retry_commands import RetryDispatch

    runner, provider, repo, remote, setup = coding_provider
    provider.update(work='intermediate', publication_seen=asyncio.Event(), proof_problem=recovery)
    runner.app_config.daemon.fix_poll_interval_sec = 1
    runner.repo_config.feature_flags.use_single_error_exit = single_error_exit
    checkpoint = runner.publish_state

    async def retain_monitor_checkpoint():
        await checkpoint()
        attempt = runner.state.publication_attempt
        if (attempt is not None and attempt.pr_number == 646
            and attempt.candidate is not None
            and attempt.candidate.head_sha == provider.get('intermediate_head')):
            provider['publication_seen'].set()

    monkeypatch.setattr(runner, 'publish_state', retain_monitor_checkpoint)
    await runner.handle_coding()
    attempt = runner.state.publication_attempt
    a, b = provider['intermediate_head'], git(repo, 'rev-parse', 'HEAD')
    assert a != b and git(repo, 'rev-parse', b + '^') == a
    capture_failed = recovery.startswith('capture_')
    if recovery != 'capture_remote':
        assert git(remote, 'rev-parse', 'refs/heads/pr-001') == a
    assert attempt.expected_head_sha == b
    assert attempt.provider_published_head_sha == (None if capture_failed else a)
    # A late/replayed monitor observation cannot replace frozen exit evidence.
    await runner._retain_observed_publication(prs.get_branch_publications()[0])
    assert attempt.expected_head_sha == b
    assert attempt.provider_published_head_sha == (None if capture_failed else a)
    assert attempt.pending and not attempt.verified
    assert runner.state.state == PipelineState.PAUSED
    first = await runner._metrics_store.get(attempt.run_id)
    assert first.ended_at and first.outcome == 'paused' and first.head_sha == b
    assert (repo / 'artifacts/ci.log').read_text() == 'pytest 2% -- unfinished'
    if capture_failed:
        provider.update(error=None, complete=True)
        git(remote, 'update-ref', 'refs/heads/pr-001', a)
    if recovery == 'missing_proof':
        # An ancestor alone cannot establish publication ownership after restart.
        attempt.provider_published_head_sha = None
        await runner.publish_state()
    elif recovery == 'foreign_remote':
        git(repo, 'commit', '--allow-empty', '-m', 'independent remote writer')
        git(repo, 'push', 'origin', 'pr-001')
        git(repo, 'reset', '--hard', b)
    fresh = await restart(runner, setup)
    fresh._recovered = True
    fresh.repo_config.feature_flags.use_single_error_exit = single_error_exit
    Clock.value = fresh.state.publication_attempt.provider_retry_at + timedelta(seconds=1)
    if capture_failed or recovery in {'missing_proof', 'foreign_remote'}:
        for _ in range(3):
            assert await publication.resume_provider_failure(fresh)
            assert fresh.state.state == PipelineState.ERROR
        assert len(provider['calls_to_coder']) == 1
        assert git(repo, 'rev-parse', 'HEAD') == b
        assert (repo / 'local-followup').read_text() == 'unpublished followup retained\n'
        assert await fresh._metrics_store.get(first.run_id) == first
        return
    if recovery == 'operator':
        fresh.state.publication_attempt.provider_retries = 3
        assert await publication.resume_provider_failure(fresh)
        assert fresh.state.state == PipelineState.ERROR
        await enqueue_bound_provider_retry(fresh, repo)
        assert await fresh._consume_retry_command() == RetryDispatch.HANDLED
    provider['fail'] = False
    assert await publication.resume_provider_failure(fresh)
    assert fresh.state.state == PipelineState.WATCH
    assert fresh.state.current_pr.number == 646
    assert len(provider['calls_to_coder']) == 2 and len(provider['requests']) == 1
    assert git(repo, 'show', 'HEAD:local-followup') == 'unpublished followup retained'
    assert git(repo, 'rev-parse', 'HEAD^') == b
    assert git(remote, 'rev-parse', 'refs/heads/pr-001') == fresh.state.current_pr.head_sha
    assert (repo / 'artifacts/ci.log').read_text() == 'gate completed successfully'
    records = await fresh._metrics_store.list_task_runs(fresh.name, 'PR-001')
    assert len(records) == 2 and all(record.ended_at for record in records)
    assert sorted(i.phase for record in records for i in record.invocations) == ['coding', 'coding']
    assert all(record.fix_iterations == 0 for record in records)
    assert await fresh._metrics_store.get(first.run_id) == first
