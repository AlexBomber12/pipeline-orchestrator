# CI setup: GitHub App for integration tests

## Overview

This is a one-time setup procedure performed by the human operator (AlexBomber12). After completion, the GitHub Actions workflow defined in `.github/workflows/ci.yml` runs automatically on every pull request with no further human action required. The procedure provisions a GitHub App that grants the integration job scoped, ephemeral access to the `pipeline-orchestrator-testbed` repository.

## Why GitHub App and not a PAT

GitHub Apps issue ephemeral 1-hour installation tokens, expose fine-grained per-repository permissions, and require no manual rotation. A long-lived personal access token (PAT) would have to be rotated periodically and is harder to scope precisely (PATs grant access to every repo the user can reach). For a tool intended to run for years, App-based authentication is the future-proof choice.

## Step-by-step setup

### Step A: Create the GitHub App

Navigate to <https://github.com/settings/apps/new>. Fill in the form:

- GitHub App name: `pipeline-orchestrator-testbed-ci` (must be globally unique on GitHub; if taken, append a suffix like your initials).
- Homepage URL: <https://github.com/AlexBomber12/pipeline-orchestrator> (or any valid URL).
- Webhook: uncheck "Active". The integration job does not consume webhooks.
- Repository permissions:
  - Contents: Read and write
  - Checks: Read-only
  - Issues: Read and write
  - Pull requests: Read and write
  - Commit statuses: Read and write
  - Metadata: Read-only (default, cannot be unchecked)
- Subscribe to events: leave all unchecked.
- Where can this GitHub App be installed: "Only on this account".
- Click "Create GitHub App".

### Step B: Generate the private key

After creation, on the App's settings page scroll to the "Private keys" section. Click "Generate a private key". The browser downloads a `.pem` file (filename like `pipeline-orchestrator-testbed-ci.YYYY-MM-DD.private-key.pem`). Keep this file secure; it grants admin rights to anything the App can access. Store it temporarily where you can copy its contents in Step E. Once the secret is configured, you can delete the file.

### Step C: Note the App ID

At the top of the App's settings page, find the line "App ID: 123456" (the number is App-specific). Note this number.

### Step D: Install the App on the testbed repository

In the App's settings, click "Install App" in the left sidebar. Click "Install" next to your account (AlexBomber12). On the next page, choose "Only select repositories" and pick exactly `pipeline-orchestrator-testbed`. Do NOT install on `pipeline-orchestrator` itself or any other repo. Click Install.

### Step E: Add repo secrets to pipeline-orchestrator

Navigate to <https://github.com/AlexBomber12/pipeline-orchestrator/settings/secrets/actions>. Click "New repository secret".

- Name: `TESTBED_APP_ID`. Value: the number from Step C. Click "Add secret".
- Click "New repository secret" again.
- Name: `TESTBED_APP_PRIVATE_KEY`. Value: the entire contents of the `.pem` file from Step B, including the `-----BEGIN RSA PRIVATE KEY-----` and `-----END RSA PRIVATE KEY-----` lines. Click "Add secret".

After adding the secret, you can delete the `.pem` file from your local machine; the secret is stored encrypted in GitHub.

### Step E2: Create the independent review simulator App

GitHub rejects requested changes from the PR author, so create a second, test-only App named `po-testbed-codex-reviewer`. The `codex` segment is required by the current testbed review classifier; do not broaden the production trust policy for this test.

Give this reviewer App only these repository permissions:

- Pull requests: Read and write (submit and read the review and inline comment)
- Contents: Read-only (locate a real changed line and bind the review to HEAD)
- Metadata: Read-only (GitHub's required default)

Install it only on `AlexBomber12/pipeline-orchestrator-testbed`, generate its private key, and add these Actions secrets to `pipeline-orchestrator`:

- `TESTBED_REVIEWER_APP_ID`
- `TESTBED_REVIEWER_APP_PRIVATE_KEY`

Each integration batch exposes its fresh separate installation token as `TESTBED_REVIEWER_TOKEN`; the fixture overrides `GH_TOKEN` only in reviewer subprocesses and never falls back to other credentials. Check only the names with `gh secret list --repo AlexBomber12/pipeline-orchestrator`.

### Step F: Verify

Re-run the most recent failed integration job (Actions tab → click the workflow run → "Re-run failed jobs"). The integration job's "Verify gh auth" step should now succeed. If it fails with "App not installed on repository", repeat Step D and ensure the testbed repo is selected.

The integration test `tests/e2e/test_ci_evidence_access.py` is the readiness
probe for CI evidence rollout. It runs with the same GitHub App installation
token as the test daemon, reads the testbed default-branch HEAD, and requires
successful retrieval of both:

- `GET /repos/AlexBomber12/pipeline-orchestrator-testbed/commits/<sha>/check-runs?per_page=100`
- `GET /repos/AlexBomber12/pipeline-orchestrator-testbed/commits/<sha>/status`

Empty check-run or status lists are acceptable; HTTP 403 or "Resource not
accessible by integration" is an external App permission/install prerequisite,
not a WATCH timeout symptom. To run just this probe in the supported integration
environment, re-run the GitHub Actions `integration` job or run:

```bash
GH_TOKEN=<testbed GitHub App installation token> \
GITHUB_ACTIONS=true \
pytest tests/e2e/test_ci_evidence_access.py -v
```

## Required check enforcement

The workflow file alone does not block merges; GitHub branch protection settings do. After the workflow has run successfully at least once on `main`:

- Navigate to <https://github.com/AlexBomber12/pipeline-orchestrator/settings/branches>.
- Edit the `main` branch protection rule (or create one if none exists).
- Under "Require status checks to pass before merging", check the box, then add `unit` and `integration` to the required checks list (they appear as searchable options once the workflow has run on `main`).
- Save.

From this point, all PRs to `main` require both checks green plus valid current-HEAD Codex review approval (a trusted reaction or explicitly successful bound review) to merge.

## Token rotation

GitHub App installation tokens expire after one hour; a token copied into
`GH_TOKEN`, the reviewer environment or the test-container credential files is
not refreshed automatically. A former single full-suite invocation exceeded that
lifetime and failed with HTTP 401 during its final test despite earlier passing
checks. Existing App IDs/private-key secrets remain unchanged.

The single required `integration` job now holds the same shared-testbed lock for
three **sequential** module batches. Python/dependency/browser/image preparation
runs before token issuance. Each `.github/actions/testbed-batch` invocation
creates fresh author and independent reviewer tokens, reseeds the stopped
isolated test stack, runs its tests, captures evidence and tears that stack down.
Before starting each batch, ownership of the entire retained test-data tree is
restored to container UID 1000, including analytics/audit lock and journal files.
A container-side create/append probe fails setup if those directories or existing
files are unwritable; it does not alter their contents.
Its 45-minute step bound leaves a margin below the one-hour credential lifetime;
a timeout fails the gate, rather than silently dropping tests. Later batches
still run after an earlier failure. No private key is mounted in the stack.

`--e2e-batch=1`, `2`, `3` partitions the actual pytest module collection, preserving
every parameterized case and automatically including newly added modules. CI
retains each collection manifest and JUnit report in `e2e-evidence` and verifies
that their disjoint union equals the full collection. Missing or contradictory
manifests fail the gate. The default local `pytest tests/e2e/ -v` remains unbatched.
There is no change to required check names, trusted review/status identities,
repository permissions, shared concurrency policy or test assertions. The private key (the `TESTBED_APP_PRIVATE_KEY` secret) does not expire by default. If you suspect compromise: regenerate the private key in App settings (delete the old one, generate a new one), then update the repository secret. The old key is invalidated immediately by GitHub.

## Cost and quota

`pipeline-orchestrator` is a public repository; GitHub Actions runner minutes are unlimited on the free tier for public repos. The integration job consumes roughly an hour for the full suite (split across three credential batches); the global concurrency lock means at most one integration runs at a time, so there is no parallelism multiplier. Expect 2 to 4 integration runs per active development day, well within unlimited free-tier capacity.

## Troubleshooting

Common issues and fixes:

- **"App not installed on repository"**: Step D was missed or the wrong repo was selected. Re-do Step D.
- **"Bad credentials" from `gh` CLI in workflow**: distinguish setup failure from a token that has expired during a long run. Check the failing batch, issuance time and its collection/JUnit artifacts first. If token creation fails immediately, verify the existing App installation and private-key secret (Step E); a late HTTP 401 does not by itself mean the private key is malformed.
- **Integration job hangs at `docker compose up -d --wait`**: the Dockerfile build is failing or a service healthcheck is failing. Check the prior step's output for image build errors, then check the redis-test healthcheck (uses `redis-cli ping`, which is in the `redis:7-alpine` image by default).
- **"Permission denied" pushing to testbed**: the App is installed but the Contents permission is read-only. Re-do Step A, ensuring Contents is "Read and write".
- **`tests/e2e/test_fix_external_merge.py` fails while posting or reading commit statuses**: the App is missing required Commit statuses access. In the App's "Permissions & events" page, change "Commit statuses" from no access to Read and write, then re-grant access on the install (the App owner will get an "approve permission update" prompt at the install URL).
- **`tests/e2e/test_ci_evidence_access.py` fails on check-runs**: the App lacks Checks read access or the install has not been re-approved after adding that permission. In the App's "Permissions & events" page, set "Checks" to Read-only, then approve the updated installation for `pipeline-orchestrator-testbed`.
- **`tests/e2e/test_ci_evidence_access.py` fails on combined status**: the App lacks Commit statuses read access or cannot see the testbed repository. Confirm "Commit statuses" is Read and write and repeat Step D for `pipeline-orchestrator-testbed`.
- **Missing `TESTBED_REVIEWER_APP_ID` or `TESTBED_REVIEWER_APP_PRIVATE_KEY`**: complete Step E2. The workflow intentionally fails before e2e setup instead of letting the review test create a PR with the author identity.

The batch reset keeps UID 1000 ownership for container-written state and shim
handshakes, then restores host ownership only for `shim-scenario`, which pytest
writes and containers read. Before container startup, the real `coder_shim`
context verifies host read/write access and restores the previous scenario.
This catches retained control-file permission failures without discarding
analytics, audit, invocation evidence, or changing provider settings.

The dirty-work continuity scenario retains its synthetic marker throughout all
preservation and restart assertions. Its teardown then stops the owned test
daemon and disposes only that marker after validating its exact task content
and branch, retaining the disposed bytes in evidence. The existing GitHub/Redis
reset completes while that test daemon is stopped, before restart. Reset alone
does not clean local untracked files; later tests must not depend on an
intervening scenario to clear the marker. Other dirty files remain untouched.
