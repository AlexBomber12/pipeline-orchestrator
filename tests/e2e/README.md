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
| Task lifecycle | `test_upload_and_merge.py` exercises upload through CODING/WATCH to IDLE with the success shim, but does not verify that the matching GitHub PR merged; `test_stop_and_resume.py` checks stop/resume and next-task selection; `test_watch_changes_requested.py`, `test_fix_coder_escalate.py`, and `test_fix_external_merge.py` exercise review, FIX, ERROR, and external-merge paths. |
| Failure continuity | `test_failure_continuity.py` uploads fresh A/C/B tasks while paused, fails A with the shim, requires A ERROR/cause and blocked C, then verifies independent B through GitHub `merged=true`, exact head/merge commit and main marker, and DONE. Its invocation journal rejects repeated A or executed C. Clean completion survives a fixture-owned daemon restart; a dirty failure remains held across restart with B TODO and its work retained. |
| Recovery and cancellation | `test_sigkill_recovery.py`, `test_sigkill_recovery_multitask.py`, `test_cancellation_cause_crash.py`, and `test_redis_down_and_recover.py` cover process/Redis failure and recovery signals. |
| Chromium UI | `test_settings_navigation_ui.py` checks settings navigation, layout, and theme persistence; `test_settings_save_feedback_ui.py` checks real and mocked save responses, stale responses, retry behavior, and webhook feedback through browser interactions. |

These are the test paths currently present. Historical PR references in tests
and task plans are context, not evidence that a proposed test exists. Any
additional lifecycle, API, or browser case remains **planned** until its test
is merged and collected by the existing workflow.
The failure-continuity journey verifies a matching GitHub merge; the older
`test_upload_and_merge.py` still uses IDLE alone.
An isolated real-Redis component test for Redis atomicity is likewise **planned**
where that guarantee is needed; a fake Redis test does not prove it.

## Writing and running tests

Assert the specific task/PR identity and intended terminal result with a finite
deadline and useful last-observed state. IDLE alone can also follow ERROR or an
unrelated task: e2e success must inspect the matching external PR, task status,
or other outcome the scenario promises. Use deterministic fault injection and
fixture-owned cleanup (`tests/e2e/conftest.py` and `tests/e2e/lib/testbed_reset.py`).
Before each lifecycle test, reset confirms the operator pause (a terminal
ERROR may retain its state with `user_paused=true`), stops only the
isolated `daemon-test` service, clears testbed state, and starts a fresh daemon.
This discards retained in-memory attempts and prevents a paused heartbeat from
restoring a cleared Redis snapshot. Production error-retention rules stay active;
the preceding scenario must establish its terminal assertions before reset.
Reset then waits for an empty, unpaused IDLE snapshot published after resume.
The web control's initial IDLE snapshot is insufficient: daemon startup migrations
must finish before a test injects a runtime fault such as a Redis outage.
SIGKILL tests also require the `crash_before_publish` shim's matching task/branch
handshake. CODING admission alone precedes CLI launch and cannot establish a
mid-invocation crash. The shim waits before Git side effects and fails without
publication if the fault is not injected; crash/ERROR assertions stay unchanged.
Retain failure evidence; the integration workflow uploads `e2e-evidence` with
bounded failure records, screenshots for tests that requested a Playwright page,
and the last 200 test-stack log lines when available. Each failure record names
the test, pytest phase, tested checkout SHA, current testbed state and the last
20 history entries (state metadata only). Browser records count page errors and
console errors without saving messages; screenshots mask editable controls and
skip authentication pages. A masked screenshot taken before browser teardown is
retained only if teardown fails. Setup and collection failures also produce a record, and the
workflow writes a checkout marker before test setup so an earlier setup failure
has an artifact. Collection errors are recorded when possible and never change
the test result. These artifacts use synthetic testbed data; do not add raw
configurations, headers, authorization codes, provider transcripts or browser
traces. For changed DOM/HTMX behavior, exercise Chromium;
HTML-string or fake-DOM assertions alone do not establish browser behavior.

The daemon coder container has no host Docker socket. GitHub Actions owns the
integration stack and testbed. For authorized host-level debugging, see
`docs/local-e2e.md` and `scripts/test-e2e.sh`; the coder's local gate is
`scripts/make-review-artifacts.sh`, which runs `scripts/ci.sh`.

The mock coder shim verifies orchestration around its scripted responses, not
real coder/provider login, account access, or deployed behavior. Record those
remaining boundaries in the PR. Real credential, provider, and deployment smoke
checks are operator-run upgrade/release actions; a source merge is not deployment.

## Failure continuity evidence and limits

The existing serialized integration job collects both continuity cases. Focused
host command: `python -m pytest tests/e2e/test_failure_continuity.py -v`.
Each case writes `e2e-evidence/continuity-<unique-id>.json` with the exact tested
checkout SHA (which may be GitHub's PR merge SHA), fresh task identities, outcome,
and successful-case PR head/merge SHA. The workflow's `run.json` and failure
records identify the tested SHA if setup fails before that evidence is written.
An absent outcome is not a passing journey. Local collection and shim unit tests
do not establish integration success; inspect the existing GHA job and evidence.

`failure_continuity` routes only fixture branches ending in `-clean-root` or
`-dirty-root` to deterministic failure; other tasks use the existing success
shim and a marker unique to the task ID. It journals task attempts outside the
checkout, excluding auth and diagnosis probes. Manual pause is observed across
several test poll cycles before any task is dispatched. Clean restart follows
production-confirmed failure release and B completion. Dirty restart is a held
negative control: the exited shim has confirmed process cleanup before restart,
but work preservation is unproved. A dirty checkout can keep the failed
publication pending: the test requires the bound task/branch, failed exit,
confirmed process cleanup and the dirty-work cleanup refusal (or terminal wait),
rather than waiting for terminal reconciliation to finish. It records that
attempt and checks the dirty file before and after restart.
Recovery must retain A's identity and dirty
file, without dispatching B or C. This does not prove restart ownership takeover
for a live or unconfirmed coder; such takeover remains unsupported/held by the
production contract. Only the fixture-owned `daemon-test` service is restarted.

No provider login, real coder output, deployment or production restart is tested.
The existing testbed App/stack prerequisites in `docs/ci-setup.md` still apply;
real provider and deployment smoke remain operator release actions.

## Applied Settings evidence

`python -m pytest tests/e2e/test_shared_config.py -v` runs in the existing
serialized integration job. The shared disposable config is restored using
exact expected write digests, including successive and concurrent saves.
The Chromium journey selects generic Claude model/effort controls, launches a
unique task with the real test daemon, and compares saved YAML, immutable
`active_invocation`, and allowlisted CLI arguments recorded by the hang shim.
While that process is active, a concurrent timeout save and effort change must
both survive; invalid model/effort writes must leave the bytes unchanged.
After daemon reload the original snapshot stays fixed. Stop/resume launches a
second unique task with the new effort. Success requires both matching CLI
handshakes; IDLE alone cannot pass. The fixture pauses and resets its owned
stack and removes its launch handshake.

`settings-<unique-id>.json` in `e2e-evidence` records the exact tested checkout
SHA, task/branch identities, saved values, snapshots and launch arguments. A
missing outcome is not success. This SHA may be GitHub's PR merge SHA; local
collection and shim component checks do not prove the GHA integration passed.
The same module checks overlay precedence with the production config writer,
loader and plugin launch-kwargs resolver in a temporary directory, with an
explicit `config.production.yml`; it never writes an overlay into the stack.
The shim advertises synthetic models through the real SDK discovery protocol.
This proves orchestration and argument propagation, not provider model support,
real account access or deployment. Existing testbed App/stack setup remains a
prerequisite; real provider/deployment smoke is an operator release action.
