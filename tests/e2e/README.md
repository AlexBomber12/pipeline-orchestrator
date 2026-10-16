# Observable test contract and coverage map

Choose the lowest layer that proves the changed behavior. The local unit job
(`scripts/ci.sh`) runs Ruff and pytest with the repository's existing coverage
threshold. Deterministic unit/component tests under `tests/` cover handlers,
routes, templates, and APIs with external transport controlled; examples include
`tests/runner/test_handle_watch.py`, `tests/web/routes/test_uploads_atomic.py`,
and `tests/web/test_settings_save_feedback.py`. Where Redis or Git semantics
are the contract, use an isolated real instance or temporary repository/process
instead of a fake. `tests/test_approval_commands.py` exercises temporary real
Git repositories; the current Redis suppression tests in
`tests/suppression/test_redis_store.py` use a fake, while the e2e stack uses
real Redis. Reuse the existing fixtures.

## Current integration coverage

The GitHub Actions integration job runs `pytest tests/e2e/ -v` against
`docker-compose.test.yml`, the isolated port 18800 stack, and the authorized
testbed repository. It runs after the unit job. Current paths include:

| Area | Existing tests and observable contract |
| --- | --- |
| Stack/API and access | `test_smoke.py` checks `/api/states`; `test_shim.py` checks the installed coder shim; `test_ci_evidence_access.py` probes testbed GitHub App access to check runs and statuses in CI. |
| Task lifecycle | `test_upload_and_merge.py` exercises upload through CODING/WATCH to IDLE with the success shim; `test_stop_and_resume.py` checks stop/resume and next-task selection; `test_watch_changes_requested.py`, `test_fix_coder_escalate.py`, and `test_fix_external_merge.py` exercise review, FIX, ERROR, and external-merge paths. |
| Recovery and cancellation | `test_sigkill_recovery.py`, `test_sigkill_recovery_multitask.py`, `test_cancellation_cause_crash.py`, and `test_redis_down_and_recover.py` cover process/Redis failure and recovery signals. |
| Chromium UI | `test_settings_navigation_ui.py` checks settings navigation, layout, and theme persistence; `test_settings_save_feedback_ui.py` checks real and mocked save responses, stale responses, retry behavior, and webhook feedback through browser interactions. |

These are the test paths currently present. Historical PR references in tests
and task plans are context, not evidence that a proposed test exists. Any
additional lifecycle, API, or browser case remains **planned** until its test
is merged and collected by the existing workflow.
An isolated real-Redis component test for Redis atomicity is likewise **planned**
where that guarantee is needed; a fake Redis test does not prove it.

## Writing and running tests

Assert the specific task/PR identity and intended terminal result with a finite
deadline and useful last-observed state. IDLE alone can also follow ERROR or an
unrelated task: e2e success must inspect the matching external PR, task status,
or other outcome the scenario promises. Use deterministic fault injection and
fixture-owned cleanup (`tests/e2e/conftest.py` and `tests/e2e/lib/testbed_reset.py`).
Retain failure evidence; the integration workflow uploads `e2e-evidence` with
screenshots and stack logs. For changed DOM/HTMX behavior, exercise Chromium;
HTML-string or fake-DOM assertions alone do not establish browser behavior.

The daemon coder container has no host Docker socket. GitHub Actions owns the
integration stack and testbed. For authorized host-level debugging, see
`docs/local-e2e.md` and `scripts/test-e2e.sh`; the coder's local gate is
`scripts/make-review-artifacts.sh`, which runs `scripts/ci.sh`.

The mock coder shim verifies orchestration around its scripted responses, not
real coder/provider login, account access, or deployed behavior. Record those
remaining boundaries in the PR. Real credential, provider, and deployment smoke
checks are operator-run upgrade/release actions; a source merge is not deployment.
