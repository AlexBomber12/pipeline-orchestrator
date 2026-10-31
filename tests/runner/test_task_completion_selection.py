"""Completion evidence across real-Git recovery, queue refresh and selection."""
from __future__ import annotations

import asyncio
import subprocess

import pytest
from src.github import GhPrMergedBranchesUnavailable
from src.github.prs import PRDiscovery
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus
from src.task_status import MergeStatusUnavailable, derive_queue_task_statuses

from tests.runner import _helpers as h


def git(repo, *args):
    return subprocess.run(
        ['git', '-C', str(repo), '-c', 'user.name=Fixture',
         '-c', 'user.email=fixture@example.test', *args],
        check=True, capture_output=True, text=True,
    ).stdout.strip()


@pytest.fixture
def repair_history(tmp_path, monkeypatch):
    tasks = tmp_path / 'tasks'
    tasks.mkdir()
    for number, dependency in [(404, 'none'), (405, 'PR-404')]:
        (tasks / f'PR-{number}.md').write_text(
            '---\nstatus: DONE\n---\n'
            f'# PR-{number}: Completion regression\n\n'
            f'Branch: pr-{number}-watch-ci-policy\n'
            '- Type: bugfix\n- Complexity: low\n'
            f'- Depends on: {dependency}\n- Priority: 1\n- Coder: any\n'
        )
    git(tmp_path, 'init', '-b', 'main')
    git(tmp_path, 'add', 'tasks')
    git(tmp_path, 'commit', '-m', 'PR-404: repair another implementation')
    git(tmp_path, 'update-ref', 'refs/remotes/origin/main', 'HEAD')
    provider = {'merged': set(), 'open': [], 'offline': False}

    def branches(repo, candidates, *, base_branch="main"):
        assert repo == 'octo/demo'
        assert base_branch == 'main'
        if provider['offline']:
            raise GhPrMergedBranchesUnavailable('fixture unavailable')
        return provider['merged'] & set(candidates)

    monkeypatch.setattr('src.task_status.gh_pr_get_merged_branches', branches)
    monkeypatch.setattr('src.github.prs.get_open_prs', lambda *a, **kw: provider['open'])
    monkeypatch.setattr('src.github.prs.get_merged_prs_discovery',
                        lambda *a: PRDiscovery('octo/demo', 'main', [], True))
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    return runner, provider


def open_pr():
    return PRInfo(number=636, title='PR-404: CI policy',
                  branch='pr-404-watch-ci-policy', url='https://example.test/pull/636')


@pytest.mark.parametrize('has_open', [False, True])
@pytest.mark.parametrize('older_merge', [False, True])
def test_repair_subject_never_completes_current_generation(repair_history, has_open, older_merge):
    runner, provider = repair_history
    provider['open'] = [open_pr()] if has_open else []
    # Only an older merge on ANOTHER branch is irrelevant without a current PR.
    provider['merged'] = {'pr-404-watch-ci-policy' if has_open else 'pr-404-old'} if older_merge else set()
    runner._idle_open_prs = provider['open']
    expected = TaskStatus.DOING if has_open else TaskStatus.TODO
    selected = asyncio.run(h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG(runner))
    assert selected.pr_id == 'PR-404'
    assert selected.status == expected
    assert runner._idle_dag_statuses == {'PR-404': expected, 'PR-405': TaskStatus.TODO}
    assert runner._idle_dag_headers[1].depends_on == ['PR-404']
    refreshed = derive_queue_task_statuses(
        runner._idle_dag_tasks, runner.repo_path, 'main', 'octo/demo', provider['open'],
        log_event=runner.log_event,
    )
    assert [task.status for task in refreshed] == [expected, TaskStatus.TODO]
    if not has_open:
        assert not runner._is_doing_already_merged(selected)
    if has_open:
        assert asyncio.run(runner._recover_state_headers()) is True
        assert runner.state.state == PipelineState.WATCH
        assert runner.state.current_pr.number == 636
        assert runner.state.current_task.pr_id == 'PR-404'
        assert [t.status for t in runner.state.current_queue] == [expected, TaskStatus.TODO]
    else:
        recovered = runner._parse_tasks_from_headers()
        assert [t.status for t in recovered] == [TaskStatus.TODO, TaskStatus.TODO]


def test_matching_merge_unblocks_dependency(repair_history):
    runner, provider = repair_history
    provider['merged'] = {'pr-404-watch-ci-policy'}
    selected = asyncio.run(h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG(runner))
    assert selected.pr_id == 'PR-405'
    assert runner._idle_dag_statuses['PR-404'] == TaskStatus.DONE
    assert runner._is_doing_already_merged(runner._idle_dag_tasks[0])


def test_unavailable_merge_evidence_preserves_recovery_snapshot(repair_history):
    runner, provider = repair_history
    provider['offline'] = True
    snapshot = [QueueTask(pr_id='PR-404', title='retained', status=TaskStatus.DOING)]
    runner.state.current_queue = snapshot
    with pytest.raises(MergeStatusUnavailable):
        asyncio.run(h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG(runner))
    assert asyncio.run(runner._recover_state_headers()) is False
    assert runner.state.current_queue == snapshot
    assert not runner._is_doing_already_merged(snapshot[0])


def test_doing_recovery_does_not_drop_task_from_repair_subject(repair_history):
    runner, _ = repair_history
    doing = QueueTask(pr_id='PR-404', title='unfinished', status=TaskStatus.DOING,
                      task_file='tasks/PR-404.md', branch='pr-404-watch-ci-policy')
    assert not runner._is_doing_already_merged(doing)
