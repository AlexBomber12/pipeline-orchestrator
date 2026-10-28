"""Settings writes must steer a later dispatch by the real test daemon."""

import time

import requests
import yaml

from tests.e2e.conftest import (
    TESTBED_SLUG,
    _resume_daemon,
    _runner_apply_count,
    _runner_reload_count,
    _stop_daemon_and_wait_paused,
    _wait_for_runner_config_ack,
)
from tests.e2e.lib.coder_shim import coder_shim


def test_settings_repo_coder_changes_daemon_dispatch(
    dashboard_url,
    make_task_zip,
    upload_zip,
    wait_for_state,
    shared_test_config,
):
    read_config, expect_settings_coder_write, note_settings_write, wait_for_coder_dispatch = shared_test_config
    wait_for_state(["IDLE"], timeout_sec=30)
    original = yaml.safe_load(read_config().decode("utf-8"))
    repository = original["repositories"][0]
    assert repository["coder"] == "claude", "test requires a Claude coder baseline"
    reviewer_identities = original["daemon"]["trusted_reviewer_identities"]

    applies_before = _runner_apply_count()
    reloads_before = _runner_reload_count()
    expect_settings_coder_write(repository["url"], "codex")
    response = requests.put(
        f"{dashboard_url}/settings/repos",
        params={"url": repository["url"]},
        data={"coder": "codex"},
        timeout=15,
    )
    assert response.status_code == 200, response.text
    note_settings_write()
    updated = yaml.safe_load(read_config().decode("utf-8"))
    assert updated["repositories"][0]["coder"] == "codex"
    assert updated["daemon"]["trusted_reviewer_identities"] == reviewer_identities
    # End any in-flight IDLE cycle, then require a runner-level config acknowledgment.
    _stop_daemon_and_wait_paused(TESTBED_SLUG)

    pr_id = int(time.time())
    with coder_shim("hang"):
        zip_path = make_task_zip(pr_id, "e2e-shared-config", coder="any")
        assert upload_zip(zip_path) in (200, 201)
        # Queue the upload before resume so the first IDLE cycle can consume it.
        _resume_daemon(TESTBED_SLUG)
        _wait_for_runner_config_ack(applies_before, reloads_before)
        observed = wait_for_coder_dispatch(f"PR-{pr_id}", "codex")
        assert observed["current_task"]["branch"] == f"pr-{pr_id}-e2e-shared-config"


def test_settings_model_effort_reach_next_launch(
    page, dashboard_url, make_task_zip, upload_zip, wait_for_state,
    get_state, shared_test_config,
):
    """One Chromium journey, two real daemon launches, no provider inference."""
    import json
    from concurrent.futures import ThreadPoolExecutor
    from datetime import datetime, timezone

    from playwright.sync_api import expect

    from tests.e2e.conftest import (
        EVIDENCE_DIR,
        _daemon_reload_count,
        _tested_sha,
        _wait_for_daemon_reload,
    )
    from tests.e2e.lib.coder_shim import SHIM_SCENARIO_PATH

    read_config, expect_write, note_write, wait_dispatch = shared_test_config
    wait_for_state(["IDLE"], timeout_sec=30)
    original = yaml.safe_load(read_config())
    repository = original["repositories"][0]
    assert repository["coder"] == "claude"
    launch_path = SHIM_SCENARIO_PATH.with_name(SHIM_SCENARIO_PATH.name + ".settings-started")
    assert not launch_path.exists(), "unowned launch evidence from an earlier scenario"
    pr_id = int(time.time())
    evidence = {"tested_sha": _tested_sha(), "test_id": f"settings-{pr_id}", "launches": []}

    def save(control, value, updates):
        expect_write(repository["url"], "claude", daemon_updates=updates)
        with page.expect_response(
            lambda response: response.url.endswith("/settings/daemon")
            and response.request.method == "PUT"
        ) as response:
            page.locator(control).select_option(value)
        assert response.value.status == 200
        expect(page.locator(control)).to_have_value(value)
        expect(page.locator(f'{control} + [role="status"] span')).to_have_text("Saved")
        note_write()

    def launch(task_id, slug):
        assert upload_zip(make_task_zip(task_id, slug, coder="any", priority=1)) in (200, 201)
        _resume_daemon(TESTBED_SLUG)
        state = wait_dispatch(f"PR-{task_id}", "claude")
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if launch_path.exists():
                record = json.loads(launch_path.read_text())
                if record["pr_id"] == f"PR-{task_id}":
                    assert record["branch"] == f"pr-{task_id}-{slug}"
                    snapshot = state["active_invocation"]
                    assert record["plugin_id"] == snapshot["plugin_id"] == "claude"
                    assert record["model"] == snapshot["model_override"] == "sonnet"
                    evidence["launches"].append({"cli": record, "snapshot": snapshot})
                    return state, record
            time.sleep(0.2)
        raise AssertionError(f"no CLI launch evidence for PR-{task_id}")

    try:
        _stop_daemon_and_wait_paused(TESTBED_SLUG)
        page.goto(f"{dashboard_url}/settings", wait_until="domcontentloaded")
        page.wait_for_function("Boolean(window.htmx && window.settingsSaveFeedbackInstalled)")
        save("#claude-model", "sonnet", {"coder_settings": {"claude": {"model": "sonnet"}}})
        save("#claude-reasoning-effort", "low", {"coder_settings": {"claude": {"reasoning_effort": "low"}}})
        with coder_shim("hang"):
            first, first_cli = launch(pr_id, "e2e-settings-first")
            assert first_cli["effort"] == first["active_invocation"]["reasoning_effort_override"] == "low"
            snapshot = first["active_invocation"]
            reloads_before = _daemon_reload_count()
            timeout = original["daemon"]["review_timeout_min"] + 1
            updates = {"coder_settings": {"claude": {"reasoning_effort": "high"}}, "review_timeout_min": timeout}
            expect_write(repository["url"], "claude", daemon_updates=updates)
            # Two real disjoint writes overlap; the browser still uses the
            # existing control and HTMX request, with no response mocks.
            with ThreadPoolExecutor(max_workers=1) as executor:
                concurrent = executor.submit(
                    requests.put, f"{dashboard_url}/settings/daemon",
                    data={"review_timeout_min": str(timeout)}, timeout=15,
                )
                with page.expect_response(
                    lambda response: response.url.endswith("/settings/daemon")
                    and response.request.method == "PUT"
                ) as response:
                    page.locator("#claude-reasoning-effort").select_option("high")
                assert response.value.status == 200
                assert concurrent.result().status_code == 200
            note_write()
            saved = yaml.safe_load(read_config())
            assert saved["daemon"]["coder_settings"]["claude"] == {"model": "sonnet", "reasoning_effort": "high"}
            assert saved["daemon"]["review_timeout_min"] == timeout
            assert saved["daemon"]["trusted_reviewer_identities"] == original["daemon"]["trusted_reviewer_identities"]
            for key in ("model", "reasoning_effort"):
                before = read_config()
                rejected = requests.put(
                    f"{dashboard_url}/settings/daemon",
                    data={f"coder_settings.claude.{key}": "invalid-e2e-value"}, timeout=15,
                )
                assert rejected.status_code == 422
                assert read_config() == before
            _wait_for_daemon_reload(reloads_before)
            # Require a publication after reload acknowledgment, so a stale
            # pre-save Redis payload cannot prove snapshot immutability.
            acknowledged = datetime.now(timezone.utc)
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                active = get_state() or {}
                updated = datetime.fromisoformat(active.get("last_updated") or "1970-01-01T00:00:00+00:00")
                if updated > acknowledged:
                    break
                time.sleep(0.2)
            else:
                raise AssertionError("no fresh daemon publication after settings reload")
            assert active["state"] == "CODING"
            assert active["current_task"]["pr_id"] == f"PR-{pr_id}"
            assert active["active_invocation"] == snapshot
            assert json.loads(launch_path.read_text()) == first_cli
            page.reload()
            expect(page.locator("#claude-model")).to_have_value("sonnet")
            expect(page.locator("#claude-reasoning-effort")).to_have_value("high")
            evidence["saved"] = {"model": "sonnet", "effort": "high"}
            evidence["active_after_reload"] = active["active_invocation"]
            applies_before, reloads_before = _runner_apply_count(), _runner_reload_count()
            _stop_daemon_and_wait_paused(TESTBED_SLUG)
            second, second_cli = launch(pr_id + 1, "e2e-settings-next")
            _wait_for_runner_config_ack(applies_before, reloads_before)
            assert second_cli["effort"] == second["active_invocation"]["reasoning_effort_override"] == "high"
            assert second["active_invocation"] != snapshot
            evidence["outcome"] = "next CLI received saved model/effort; active snapshot retained"
            _stop_daemon_and_wait_paused(TESTBED_SLUG)
    finally:
        EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
        (EVIDENCE_DIR / f"settings-{pr_id}.json").write_text(json.dumps(evidence, indent=2) + "\n")
        launch_path.unlink(missing_ok=True)


def test_settings_overlay_precedence_is_disposable(tmp_path):
    """The production writer saves base values; overlay wins in launch kwargs."""
    from src.coders.claude import ClaudePlugin
    from src.config import load_config, update_daemon_config

    base = tmp_path / "config.yml"
    base.write_text(yaml.safe_dump({"daemon": {"coder_settings": {
        "claude": {"model": "opus", "reasoning_effort": "low"},
    }}}))
    overlay = tmp_path / "config.production.yml"
    overlay.write_text(yaml.safe_dump({"daemon": {"coder_settings": {
        "claude": {"model": "opus", "reasoning_effort": "high"},
    }}}))
    overlay_bytes = overlay.read_bytes()
    update_daemon_config(path=str(base), coder_settings={"claude": {"model": "sonnet", "reasoning_effort": "low"}})
    saved = yaml.safe_load(base.read_text())["daemon"]["coder_settings"]["claude"]
    assert saved == {"model": "sonnet", "reasoning_effort": "low"}
    effective = load_config(str(base))
    kwargs = ClaudePlugin().build_run_kwargs(daemon_config=effective.daemon)
    assert kwargs["model"] == "opus"
    assert kwargs["reasoning_effort"] == "high"
    assert overlay.read_bytes() == overlay_bytes
    overlay.unlink()
    kwargs = ClaudePlugin().build_run_kwargs(daemon_config=load_config(str(base)).daemon)
    assert kwargs["model"] == saved["model"]
    assert kwargs["reasoning_effort"] == saved["reasoning_effort"]
