# CI setup: GitHub App for integration tests

## Overview

This is a one-time setup procedure performed by the human operator (AlexBomber12). After completion, the GitHub Actions workflow defined in `.github/workflows/ci.yml` runs automatically on every pull request with no further human action required. The procedure provisions a GitHub App that grants the integration job scoped, ephemeral access to the `pipeline-orchestrator-testbed` repository.

## Why GitHub App and not a PAT

GitHub Apps generate ephemeral 1-hour tokens per workflow run, expose fine-grained per-repository permissions, and require no manual rotation. A long-lived personal access token (PAT) would have to be rotated periodically and is harder to scope precisely (PATs grant access to every repo the user can reach). For a tool intended to run for years, App-based authentication is the future-proof choice.

## Step-by-step setup

### Step A: Create the GitHub App

Navigate to <https://github.com/settings/apps/new>. Fill in the form:

- GitHub App name: `pipeline-orchestrator-testbed-ci` (must be globally unique on GitHub; if taken, append a suffix like your initials).
- Homepage URL: <https://github.com/AlexBomber12/pipeline-orchestrator> (or any valid URL).
- Webhook: uncheck "Active". The integration job does not consume webhooks.
- Repository permissions:
  - Contents: Read and write
  - Issues: Read and write
  - Pull requests: Read and write
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

### Step F: Verify

Re-run the most recent failed integration job (Actions tab → click the workflow run → "Re-run failed jobs"). The integration job's "Verify gh auth" step should now succeed. If it fails with "App not installed on repository", repeat Step D and ensure the testbed repo is selected.

## Required check enforcement

The workflow file alone does not block merges; GitHub branch protection settings do. After the workflow has run successfully at least once on `main`:

- Navigate to <https://github.com/AlexBomber12/pipeline-orchestrator/settings/branches>.
- Edit the `main` branch protection rule (or create one if none exists).
- Under "Require status checks to pass before merging", check the box, then add `unit` and `integration` to the required checks list (they appear as searchable options once the workflow has run on `main`).
- Save.

From this point, all PRs to `main` require both checks green plus a Codex review +1 to merge.

## Token rotation

GitHub Apps automatically generate ephemeral tokens per workflow run; no manual rotation is needed. The private key (the `TESTBED_APP_PRIVATE_KEY` secret) does not expire by default. If you suspect compromise: regenerate the private key in App settings (delete the old one, generate a new one), then update the repository secret. The old key is invalidated immediately by GitHub.

## Cost and quota

`pipeline-orchestrator` is a public repository; GitHub Actions runner minutes are unlimited on the free tier for public repos. The integration job consumes about 12 to 18 minutes per run; the global concurrency lock means at most one integration runs at a time, so there is no parallelism multiplier. Expect 2 to 4 integration runs per active development day, well within unlimited free-tier capacity.

## Troubleshooting

Common issues and fixes:

- **"App not installed on repository"**: Step D was missed or the wrong repo was selected. Re-do Step D.
- **"Bad credentials" from `gh` CLI in workflow**: the App private key in the secret is malformed. Re-do Step E, ensuring you paste the entire `.pem` contents including the BEGIN/END lines.
- **Integration job hangs at `docker compose up -d --wait`**: the Dockerfile build is failing or a service healthcheck is failing. Check the prior step's output for image build errors, then check the redis-test healthcheck (uses `redis-cli ping`, which is in the `redis:7-alpine` image by default).
- **"Permission denied" pushing to testbed**: the App is installed but the Contents permission is read-only. Re-do Step A, ensuring Contents is "Read and write".
