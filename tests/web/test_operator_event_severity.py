"""Severity persistence for existing web control-plane history events."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient
from src.approval_commands import ApprovalCommand, approval_key, load_approval
from src.models import PRInfo, RepoState
from src.web import app as web_app
from src.web.routes import repo_control

from tests.test_web_app import _FakeRedis, _stub_aioredis_with_state
from tests.web.test_guardrail_card_and_reset import _GuardrailRedis, _setup_repo


def test_history_severity_validation_and_coalescing():
    state = RepoState(name="repo", url="https://github.com/example/repo")
    repo_control._append_history_entry(state, "same")
    legacy = dict(state.history[0])
    for severity in ("info", "ok", "warn", "error"):
        repo_control._append_history_entry(state, "same", severity=severity)
        repo_control._append_history_entry(state, "same", severity=severity)
    reloaded = RepoState.model_validate_json(state.model_dump_json())
    assert reloaded.history[0] == legacy
    assert "severity" not in legacy
    assert [entry["severity"] for entry in reloaded.history[1:]] == ["info", "ok", "warn", "error"]
    assert [entry["count"] for entry in reloaded.history] == [1, 2, 2, 2, 2]
    with pytest.raises(ValueError, match="Unknown event severity"):
        repo_control._append_history_entry(state, "same", severity="success")
    assert state.history == reloaded.history


@pytest.mark.parametrize(
    "endpoint,kind,status",
    [("reset-to-idle", "reset", 200), ("tasks/PR-384/guardrail/accept-once", "guardrail_accept_once", 204)],
)
def test_confirmed_operator_transition_persists_ok(tmp_path, monkeypatch, endpoint, kind, status):
    _, redis = _setup_repo(tmp_path, monkeypatch)
    publish = AsyncMock()
    monkeypatch.setattr(web_app, "publish_repo_event", publish)
    monkeypatch.setattr(web_app, "publish_wake", AsyncMock())
    with TestClient(web_app.app) as client:
        response = client.post(f"/repos/example__alpha/{endpoint}")
    assert response.status_code == status
    state = RepoState.model_validate_json(redis.store["pipeline:example__alpha"])
    assert state.history[-1]["kind"] == kind
    assert state.history[-1]["severity"] == "ok"
    assert publish.call_args.args[2]["severity"] == "ok"


@pytest.mark.parametrize("endpoint", ["pause", "resume", "stop", "inhibitors/clear/user_pause"])
def test_control_acknowledgements_persist_info(tmp_path, monkeypatch, endpoint):
    _, seeded = _setup_repo(tmp_path, monkeypatch)
    redis = _FakeRedis(seeded.store)
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(redis))
    publish = AsyncMock()
    monkeypatch.setattr(web_app, "publish_repo_event", publish)
    monkeypatch.setattr(web_app, "publish_wake", AsyncMock())
    with TestClient(web_app.app) as client:
        response = client.post(f"/repos/example__alpha/{endpoint}")
    assert response.status_code == 200
    state = RepoState.model_validate_json(redis.store["pipeline:example__alpha"])
    assert state.history[-1]["severity"] == "info"
    assert publish.call_args.args[2]["severity"] == "info"


@pytest.mark.parametrize("status", ["pending", "deferred", "applied", "failed"])
async def test_same_binding_ack_retains_stored_outcome(monkeypatch, status):
    state = RepoState(name="repo", url="https://github.com/example/repo")
    repo_control._append_history_entry(state, "Prior failure", severity="error")
    command = ApprovalCommand(
        binding="binding", repo_slug="repo", repo_url=state.url,
        task={"pr_id": "PR-479", "title": "task", "status": "ERROR"},
        pr=PRInfo(number=1, branch="pr-479"), task_fingerprint="hash", failure="failure",
        status=status, reason=f"Server outcome: {status}",
    )
    redis = _GuardrailRedis({
        "pipeline:repo": state.model_dump_json(),
        approval_key("repo", "binding"): command.model_dump_json(),
    })
    before = dict(redis.store)
    publish = AsyncMock()
    monkeypatch.setattr(web_app, "publish_repo_event", publish)
    response = await repo_control._approve_guardrail_decision(
        "repo", "PR-479", SimpleNamespace(url=state.url), redis, "binding",
    )
    assert response.status_code == 202
    assert response.body.decode() == command.reason
    assert (await load_approval(redis, "repo", "binding")).status == status
    assert redis.store == before
    publish.assert_not_awaited()


@pytest.mark.parametrize("label_failure", [False, True])
async def test_quarantine_best_effort_release_is_informational(tmp_path, monkeypatch, label_failure):
    _, redis = _setup_repo(tmp_path, monkeypatch)
    state = RepoState.model_validate_json(redis.store["pipeline:example__alpha"])
    state.quarantined_prs.add(42)
    redis.store["pipeline:example__alpha"] = state.model_dump_json()
    monkeypatch.setattr(repo_control, "_suppressed_task_ids_for_pr", AsyncMock(return_value=set()))
    def gh_result(*args, **kwargs):
        if label_failure:
            raise RuntimeError("GitHub unavailable")
        return ""

    monkeypatch.setattr(repo_control.gh_runner, "run_gh", gh_result)
    publish = AsyncMock()
    monkeypatch.setattr(web_app, "publish_repo_event", publish)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(redis=redis)), headers={})
    response = await repo_control.release_quarantine("example__alpha", 42, request)
    assert response.status_code == 200
    state = RepoState.model_validate_json(redis.store["pipeline:example__alpha"])
    assert state.quarantined_prs == set()
    assert state.history[-1]["severity"] == "info"
    assert publish.call_args.args[2]["severity"] == "info"
