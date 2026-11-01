"""Capacity during initial CODING preserves committed/dirty work through real merge."""
import base64
import json
import subprocess
import time
from pathlib import Path

from tests.e2e.conftest import (
    EVIDENCE_DIR,
    _load_shared_config,
    _stop_daemon_and_wait_paused,
    _tested_sha,
)
from tests.e2e.lib.coder_shim import SHIM_SCENARIO_PATH, coder_shim
from tests.e2e.lib.review_api import MergeOutcomeApi, ReviewApi, task_released_and_done
from tests.e2e.test_fix_feedback import _runs, _wait

CALLS = Path(str(SHIM_SCENARIO_PATH) + '.capacity-calls')
GATE = Path(str(SHIM_SCENARIO_PATH) + '.capacity-gate')
PROOF = Path(str(SHIM_SCENARIO_PATH) + '.capacity-proof')


def test_coding_capacity_retained_work_reaches_merge(
    testbed_slug, get_state, upload_zip, make_task_zip, reset_testbed,
):
    repo = testbed_slug.replace('__', '/', 1)
    cfg = _load_shared_config()
    configured = next(r for r in cfg.repositories if repo in r.url)
    outcome = MergeOutcomeApi(repo, configured.branch)
    reviewer = ReviewApi.from_environment()
    identity = time.time_ns()
    task, branch = f'PR-{identity}', f'pr-{identity}-e2e-coding-capacity'
    assert not any(path.exists() for path in (CALLS, GATE, PROOF)), 'capacity journal belongs to another owner'
    evidence = {'tested_sha': _tested_sha(), 'task': task, 'branch': branch}

    def ready(state):
        pr = state.get('current_pr') or {}
        return (state.get('state') == 'WATCH' and (state.get('current_task') or {}).get('pr_id') == task
                and pr.get('branch') == branch and pr.get('publication_at')
                and state.get('review_request_head') == pr.get('head_sha'))

    with coder_shim('coding_capacity_once'):
        try:
            assert upload_zip(make_task_zip(identity, 'e2e-coding-capacity')) in (200, 201)
            watched = _wait(get_state, ready, timeout=300)
            pr = watched['current_pr']
            number, head = pr['number'], pr['head_sha']
            pull = outcome.assert_pull(number, branch, head)
            marker = outcome.published_marker(number, branch, head, task)
            runs = _runs(testbed_slug, task)
            assert len(runs) == 2 and sorted(r['attempt_index'] for r in runs) == [1, 2], runs
            first, second = sorted(runs, key=lambda r: r['attempt_index'])
            assert first['outcome'] == 'paused' and first['exit_reason'] == 'paused' and first['ended_at'], first
            assert second['outcome'] == 'superseded' and second['exit_reason'] == 'coding_complete', second
            assert second['head_sha'] == head and second['ended_at']
            assert first['run_id'] != second['run_id'] and first['head_sha'] != head
            assert first['task_spec_hash'] == second['task_spec_hash']
            for record in runs:
                assert record['task_id'] == task and record['repo_name'] == testbed_slug, record
                assert record['fix_iterations'] == 0 and len(record['invocations']) == 1, record
                invocation = record['invocations'][0]
                assert invocation['phase'] == 'coding' and invocation['run_id'] == record['run_id'], invocation
            assert first['invocations'][0]['plugin_id'] == second['invocations'][0]['plugin_id']
            for key in ['model_override', 'reasoning_effort_override']:
                assert first['invocations'][0][key] == second['invocations'][0][key]
            assert CALLS.read_text().splitlines() == [f'{task}\t{branch}'] * 2
            assert GATE.read_text().strip() == 'completed-after-capacity'
            proof = json.loads(PROOF.read_text())
            assert proof['task'] == task and proof['branch'] == branch
            assert proof['run_id'] == first['run_id'] and proof['pr'] == number
            assert proof['exit_classification'] is None
            assert [p['sha'] for p in outcome._get(f"commits/{first['head_sha']}")['parents']] == [proof['head']]
            evidence['intermediate_publication_proof'] = proof
            for path, text in [('tests/e2e-capacity-commit.txt', f'retained commit for {task}\n'),
                               ('tests/e2e-capacity-followup.txt', f'retained unpublished followup for {task}\n'),
                               ('tests/e2e-capacity-retained.txt', f'retained dirty work for {task}\n')]:
                content = outcome._get(f'contents/{path}?ref={head}')
                assert base64.b64decode(content['content']).decode() == text
            parents = outcome._get(f'commits/{head}')['parents']
            assert [p['sha'] for p in parents] == [first['head_sha']]
            reviewer.approve_head(number, head, pr['publication_at'])
            subprocess.run(
                ['gh', 'api', '-X', 'POST', f'repos/{repo}/statuses/{head}',
                 '-f', 'state=success', '-f', 'context=e2e/watch-merge-gate'],
                check=True, capture_output=True, text=True, timeout=30,
            )
            final = _wait(get_state, lambda s: task_released_and_done(s, task, branch), timeout=240)
            merged = outcome.assert_merged(number, branch, head, task, marker)
            final_runs = _runs(testbed_slug, task)
            by_id = {r['run_id']: r for r in final_runs}
            assert len(final_runs) == 2 and by_id[first['run_id']] == first, final_runs
            successor = by_id[second['run_id']]
            assert successor['outcome'] == 'merged' and successor['exit_reason'] == 'success_merged'
            assert successor['head_sha'] == head and successor['ended_at']
            pulls = outcome._get(f"pulls?state=all&head={repo.split('/')[0]}:{branch}")
            assert [p['number'] for p in pulls] == [number]
            assert CALLS.read_text().splitlines() == [f'{task}\t{branch}'] * 2
            evidence.update(pr=number, head=head, before_merge=runs, final_runs=final_runs,
                            merged=merged, author=pull['user']['login'], final_state=final['state'])
        finally:
            evidence['calls'] = CALLS.read_text().splitlines() if CALLS.exists() else []
            EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
            (EVIDENCE_DIR / f'coding-capacity-{task}.json').write_text(json.dumps(evidence, indent=2) + '\n')
            _stop_daemon_and_wait_paused(testbed_slug)
            CALLS.unlink(missing_ok=True)
            GATE.unlink(missing_ok=True)
            PROOF.unlink(missing_ok=True)
