# Local e2e debug guide

## 1. Overview

This guide covers running the e2e suite under `tests/e2e/` against a local docker compose stack. Local e2e is intended for two cases only: debugging a failing GitHub Actions integration run interactively, and developing new tests before pushing them through CI. Routine merge gating is owned by GitHub Actions (see `docs/ci-setup.md` for the one-time GHA setup); the local wrapper is purely developer ergonomics and does not change the merge contract.

The wrapper script is `scripts/test-e2e.sh`. It brings up the test stack on port 18800, runs pytest against `tests/e2e/`, and tears the stack down on exit. Flags adjust this lifecycle for iteration.

## 2. Prerequisites

- Docker and Docker Compose v2. Verify with `docker compose version` and confirm a v2.x line.
- Python 3.12 in the active virtualenv where pytest will run.
- `gh` CLI authenticated with `repo` scope. Verify `gh auth status` exits 0 and the listed account has access to `AlexBomber12/pipeline-orchestrator-testbed`.
- Test dependencies and Playwright browsers: `pip install -r requirements-test.txt` then `playwright install chromium`.

On AI-Server (192.168.50.4) and DESKTOP-5NT9DG3 WSL these prerequisites are already configured.

## 3. Quick start

- `scripts/test-e2e.sh` runs the full e2e suite: brings the stack up, runs every test, tears down, exits with the pytest exit code.
- `scripts/test-e2e.sh --filter test_upload_and_merge` runs only that test.
- `scripts/test-e2e.sh --keep-up` leaves the stack up after tests finish so containers can be inspected.
- `scripts/test-e2e.sh --no-up --keep-up --filter test_smoke` is the fastest iteration loop while actively developing: stack stays up across runs, only the named test executes.
- `scripts/test-e2e.sh --scenario success --filter test_upload_and_merge` pre-writes the named scenario to `tests/e2e/data/shim-scenario` before pytest starts, useful when a test reads the scenario rather than setting it via fixture.
- `scripts/test-e2e.sh --help` prints usage and exits 0.

The script always echoes a one-line `PASS in Ns` or `FAIL in Ns` summary on the last line and exits with the pytest exit code; the wall-clock duration uses bash's `SECONDS` builtin and covers stack bring-up plus pytest plus teardown.

## 4. Architecture

Production and test stacks coexist on the same host without conflict:

```
Production stack         Test stack
----------------         ----------
port 8800       <-->     port 18800
./data/                  ./tests/e2e/data/
network default          network pipeline-orchestrator-test-net
prod containers          *-test containers
```

Ports, on-disk data roots, docker networks, and container names are all distinct. You can leave the production stack running while iterating on the test stack; the two never overlap.

The compose file lives at `docker-compose.test.yml` at the repo root. Test data persists under `tests/e2e/data/` between runs unless you tear down with `-v`, in which case the named volumes are removed but the bind-mounted host directories remain. The wrapper passes `-v` on teardown so each clean run starts from a known state.

## 5. The mock coder shim

The shim lives at `tests/e2e/lib/coder_shim.sh` (introduced in PR-154a). It replaces the real coder binary inside the test stack so tests run without LLM calls and without spending tokens.

Supported scenarios today:

- `success`: coder shim performs the real git operations (commit, push, `gh pr create`) and exits cleanly.
- `slow`: same as success but with an injected delay, used to exercise timeouts.

Future PRs may extend this set with `no_pr`, `exit_nonzero`, `hang`, or `malformed_pr` as new failure modes need direct test coverage. The shim performs real git and gh calls against the testbed; only the LLM step is stubbed out.

## 6. Reproducing a GHA failure locally

1. Open the failing GHA run in the browser.
2. Note the failing test name and the assertion or timeout that fired.
3. Download the `e2e-evidence` artifact zip; extract screenshots and `stack-logs.txt`.
4. On the local machine, run `scripts/test-e2e.sh --filter <test_name> --keep-up`.
5. If it reproduces locally, debug interactively against the still-running stack:
   - `docker compose -f docker-compose.test.yml logs daemon-test`
   - `docker compose -f docker-compose.test.yml logs web-test`
   - `docker compose -f docker-compose.test.yml exec daemon-test bash`
   - `curl -sS http://localhost:18800/state` for the dashboard's view of state.
6. If it does not reproduce locally, suspect an environmental difference: Python version mismatch, Playwright version drift, network conditions, or `gh` credential differences. Capture the divergence in a follow-up issue rather than papering over it.

When you finish debugging, tear the stack down explicitly so the next `--no-up` run does not pick up stale containers: `docker compose -f docker-compose.test.yml down -v`.

## 7. Troubleshooting common failures

- **Port 18800 already in use.** Another test stack is up. Run `docker ps | grep test`, then `docker compose -f docker-compose.test.yml down -v` to clear it.
- **Tests fail with auth errors at the `gh push` step.** Check `gh auth status` shows you logged in as `AlexBomber12` with `repo` scope. If not, `gh auth login`.
- **"Permission denied" on `/data/auth/.codex` inside `daemon-test`.** Re-run `bash scripts/init-test-volume.sh` to chown the volume to UID 1000.
- **Tests hang at `wait_for_state(IDLE)`.** The testbed has open PRs from a previous run. Close them: `gh pr list -R AlexBomber12/pipeline-orchestrator-testbed --state open --json number --jq '.[].number' | xargs -I {} gh pr close {} -R AlexBomber12/pipeline-orchestrator-testbed --delete-branch`.
- **Playwright "browser not found".** Run `playwright install chromium` inside the venv where pytest runs.
- **Tests pass locally but fail in GHA.** Compare Python version (must be 3.12) and Playwright version. Browser version drift is the most common cause; pin in `requirements-test.txt` if drift causes flakes.

## 8. Why this is separate from `scripts/ci.sh`

`scripts/ci.sh` is the coder's pre-PR gate. It runs inside the daemon container as part of the daemon's normal workflow. The daemon container has no access to the host Docker socket; mounting it would give LLM-driven coder code the ability to manipulate any container on the host, including the production orchestrator. That security boundary is intentional.

Without socket access, `ci.sh` cannot start `docker compose` for the test stack. The integration tier therefore runs from a host-level entry point: `scripts/test-e2e.sh` for local invocation and the GitHub Actions runner for automated gating. Both share the same `docker-compose.test.yml` and conftest fixtures; they differ only in entry point and credential mechanism. Local runs use the developer's `gh` CLI auth, GHA runs use the App token from `docs/ci-setup.md`. This separation is consistent with the AGENTS.md description of the CI gate.

## 9. Adding new tests

1. Copy `tests/e2e/test_smoke.py` as a starting template.
2. Request fixtures from `tests/e2e/conftest.py` rather than duplicating setup logic.
3. Name the file `test_xxx.py` so pytest discovers it.
4. Run via `scripts/test-e2e.sh --filter test_xxx` until green; the `--no-up --keep-up` combination keeps the stack warm between iterations.
5. Push the change and let GHA validate it in a clean environment.

If the new test needs a shim scenario that does not yet exist, add the scenario to `tests/e2e/lib/coder_shim.sh` first, then write the test that invokes `coder_shim("new_scenario")`. Keep the shim deterministic: scenarios should not depend on wall-clock timing beyond the explicit `slow` injection, otherwise tests will flake under GHA's noisier runner load. When you submit the PR, mention any new scenarios in the PR description so reviewers know the shim contract changed.
