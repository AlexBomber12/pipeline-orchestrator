"""Durable workflow-attempt admission and incident identity regressions."""

import asyncio
from pathlib import Path

from src.daemon.ci_attempts import attempt_scope, canonical_ci_incident, read_actions_run
from src.keyspace import pipeline_state
from src.models import PRInfo, QueueTask, RepoState, TaskStatus

from tests.runner import _helpers as h


def test_lagging_metadata_cannot_lower_observed_attempt_after_json_reload(
    monkeypatch, tmp_path: Path,
) -> None:
    task_file = tmp_path / "tasks" / "PR-019.md"
    task_file.parent.mkdir()
    task_file.write_text("---\nPR_ID: PR-019\nBranch: pr-019\nstatus: DOING\n---\nTask\n")
    state = RepoState(name="demo", url="https://github.com/octo/demo")
    state.current_task = QueueTask(
        pr_id="PR-019", title="CI", status=TaskStatus.DOING,
        branch="pr-019", task_file="tasks/PR-019.md",
    )
    pr = PRInfo(number=19, branch="pr-019", head_sha="a" * 40)
    observations = attempt_scope(state, "octo/demo", str(tmp_path), pr)
    assert observations == {}
    replies = iter([2, 1, 1])

    def run_view(args, **kwargs):
        assert args[:3] == ["run", "view", "99"]
        return {"databaseId": 99, "headSha": pr.head_sha, "attempt": next(replies),
                "status": "completed", "conclusion": "failure", "jobs": []}

    monkeypatch.setattr("src.daemon.ci_attempts.gh_runner.run_gh", run_view)
    assert read_actions_run("octo/demo", pr.head_sha, 99, observations)["attempt"] == 2
    restored = RepoState.model_validate_json(state.model_dump_json())
    restored_observations = attempt_scope(restored, "octo/demo", str(tmp_path), pr)
    assert restored_observations == {"99": 2}
    assert read_actions_run("octo/demo", pr.head_sha, 99, restored_observations) is None
    assert restored_observations == {"99": 2}
    # A different task generation cannot inherit the previous attempt floor.
    task_file.write_text(task_file.read_text() + "Updated task body\n")
    assert attempt_scope(restored, "octo/demo", str(tmp_path), pr) == {}
    assert read_actions_run("octo/demo", pr.head_sha, 99, restored.ci_attempts)["attempt"] == 1


def test_incident_identity_preserves_every_immutable_field() -> None:
    incident = {"repo": "octo/demo", "pr_number": 19, "head_sha": "a" * 40,
                "contexts": [{"check_id": 55, "attempt": 1}],
                "_observed_attempts": {"99": 2}}
    stable = canonical_ci_incident(incident)
    assert stable == {key: value for key, value in incident.items()
                      if key != "_observed_attempts"}
    assert "_observed_attempts" in incident
    changed = dict(incident, head_sha="b" * 40)
    assert canonical_ci_incident(changed) != stable


def test_runner_rehydrates_attempt_floor_from_persisted_snapshot() -> None:
    original = h._make_runner()
    original.state.ci_attempt_scope = '["octo/demo","PR-019","hash","pr-019",19,"sha"]'
    original.state.ci_attempts = {"99": 2}
    asyncio.run(original.redis.set(
        pipeline_state(original.name), original.state.model_dump_json(),
    ))
    restarted = h._make_runner()
    restarted.redis = original.redis
    assert asyncio.run(restarted._hydrate_current_task_from_persisted_state())
    assert restarted.state.ci_attempt_scope == original.state.ci_attempt_scope
    assert restarted.state.ci_attempts == {"99": 2}
