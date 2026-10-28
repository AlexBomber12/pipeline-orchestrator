"""Publication continuation against disposable real Git and controlled GitHub.

The older synthetic handler fixtures exercise version-1 compatibility. These
cases use the production version-2 builder and actual runner persistence/ticks.
"""
from __future__ import annotations

import asyncio
import subprocess
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
    (repo / 'tasks/PR-001.md').write_text('# PR-001\n\nBranch: pr-001\n')
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
