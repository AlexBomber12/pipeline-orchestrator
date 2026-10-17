"""Settings writes must steer a later dispatch by the real test daemon."""

import time

import requests
import yaml

from tests.e2e.conftest import _daemon_reload_count, _wait_for_daemon_reload
from tests.e2e.lib.coder_shim import coder_shim


def test_settings_repo_coder_changes_daemon_dispatch(
    dashboard_url,
    make_task_zip,
    upload_zip,
    wait_for_state,
    shared_test_config,
):
    read_config, note_settings_write, wait_for_coder_dispatch = shared_test_config
    wait_for_state(["IDLE"], timeout_sec=30)
    original = yaml.safe_load(read_config().decode("utf-8"))
    repository = original["repositories"][0]
    assert repository["coder"] == "claude", "test requires a Claude coder baseline"
    reviewer_identities = original["daemon"]["trusted_reviewer_identities"]

    reloads_before = _daemon_reload_count()
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
    _wait_for_daemon_reload(reloads_before)

    pr_id = int(time.time())
    with coder_shim("hang"):
        zip_path = make_task_zip(pr_id, "e2e-shared-config", coder="any")
        assert upload_zip(zip_path) in (200, 201)
        observed = wait_for_coder_dispatch(f"PR-{pr_id}", "codex")
        assert observed["current_task"]["branch"] == f"pr-{pr_id}-e2e-shared-config"
