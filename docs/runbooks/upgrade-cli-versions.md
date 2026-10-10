# Upgrade pinned CLI versions

The Dockerfile pins `@anthropic-ai/claude-code` and `@openai/codex` to specific
npm versions via `ARG CLAUDE_CODE_VERSION` and `ARG CODEX_VERSION`. Pinning is
intentional: every upgrade is a deliberate, human-driven action so we are not
exposed to silent breakage from upstream CLI changes (renamed flags, changed
output format, OAuth flow shifts, etc).

## When to upgrade

- Security patches or CVE fixes from upstream.
- A bug we have hit is fixed in a newer release.
- We want to adopt a new feature deliberately (e.g. a new model surface).

There is no automatic version-bumping. Stale pins are acceptable as long as
the daemon is healthy.

## How to upgrade

1. Check the latest version on npm:
   ```sh
   npm view @anthropic-ai/claude-code version
   npm view @openai/codex version
   ```
2. Test the candidate version locally with a build-arg override, no Dockerfile
   edit yet:
   ```sh
   docker build --build-arg CLAUDE_CODE_VERSION=X.Y.Z .
   docker build --build-arg CODEX_VERSION=A.B.C .
   ```
3. Run the daemon against a sample task end-to-end and confirm CODING / WATCH /
   FIX cycles still work. Confirm `claude --version` and `codex --version`
   inside the container match the candidate.
4. Update the `ARG` defaults in `Dockerfile` and open a PR titled
   `PR-NNN: bump claude-code to X.Y.Z` (or codex). Include the rationale.
5. Wait for e2e tests to pass and merge after Aleksei review.

## Claude browser code-entry prerequisite

Claude Code 2.1.126 is the minimum investigated release that exposes the
authorization-code prompt needed for a future browser login flow. The pin alone
does not implement that flow: PTY transport, code submission, and Settings
integration remain subsequent work documented in `docs/claude-login-api.md`.

The 2.1.126 default was verified on 2026-10-10 with an unmodified default build
tagged `pipeline-orchestrator:pr395-claude-2.1.126-a5c5f8e-01` (local image ID
`sha256:dcf9479dd43adb0d6e9c43e6f237ac69e717740821e9c55e56d15fe2c8f0c5b3`).
Disposable containers used fresh tmpfs credential directories, no mounts, no
provider credentials, and bounded commands. They reported Claude Code 2.1.126
and Codex CLI 0.160.0; `claude auth login --help` and every flag used by current
Claude execution and model discovery were accepted. The existing bounded
initialization-only discovery adapter returned `default`, `sonnet[1m]`,
`opus[1m]`, and `haiku`, without sending an inference request. That
unauthenticated result does not prove login or account access. All probe
containers were confirmed removed.

## How to roll back

Revert the Dockerfile change and rebuild. The `ARG` defaults are the only
source of truth; there is no separate lockfile to coordinate.

## Signals to monitor after upgrade

- OAuth / login flows on both CLIs still complete.
- Prompts still produce the expected stdout shape (the daemon parses CLI
  output in `src/coders/`).
- All e2e tests under `tests/e2e/` continue to pass.
- Rate-limit reporting still surfaces correctly on the dashboard.
