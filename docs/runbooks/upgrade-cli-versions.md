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
authorization-code prompt used by the implemented browser-code login flow.
PTY transport, code submission, and Settings integration are in place; see
`docs/claude-login-api.md` for that flow's contract.

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

## Claude Code 2.1.301 candidate (2026-10-16)

The candidate was built before changing the default pin:

```sh
docker build --build-arg CLAUDE_CODE_VERSION=2.1.301 \
  -t pipeline-orchestrator:claude-2.1.301-candidate .
```

The disposable image ID was
`sha256:d3e436c476aba2cd877d3d8509c4b8dacc17ab2e4a878246e93ef74ae3ad133b`.
Probes used `docker run --rm --network none --tmpfs /tmp:rw,exec,nosuid,size=64m`
with `HOME=/tmp/probe-home`, `CLAUDE_CONFIG_DIR=/tmp/probe-creds`, and
`--entrypoint python`. The probe process created both directories with mode
0700. No host mounts, credentials, production data, or Docker socket were
provided. A reproducible discovery probe is:

```sh
docker run --rm -i --network none --tmpfs /tmp:rw,exec,nosuid,size=64m \
  -e HOME=/tmp/probe-home -e CLAUDE_CONFIG_DIR=/tmp/probe-creds \
  --entrypoint python pipeline-orchestrator:claude-2.1.301-candidate - <<'PY'
import asyncio
import os
from pathlib import Path
from src.coders.claude_models import discover_claude_models

for key in ("HOME", "CLAUDE_CONFIG_DIR"):
    Path(os.environ[key]).mkdir(mode=0o700)

async def probe():
    models = await discover_claude_models(
        timeout_seconds=15, env=dict(os.environ), cwd=os.environ["HOME"]
    )
    for model in models:
        print(model.invocation_id, model.display_name,
              [effort.name for effort in model.reasoning_efforts])

asyncio.run(probe())
PY
```

`claude --version` returned `2.1.301 (Claude Code)`. `claude --help` exposed
the current execution and discovery flags (`--print`,
`--dangerously-skip-permissions`, `--model`, `--effort`, `--output-format`,
`--input-format`, `--verbose`). `claude auth login --help` exited 0. The
execution adapter's `--append-system-prompt-file` flag was accepted by a
bounded, credential-free CLI startup probe; the CLI rejected the missing prompt
before inference. The initialize-only discovery adapter completed within 15
seconds and returned:

| ID | Label | Advertised effort levels |
| --- | --- | --- |
| `default` | Default (recommended) | low, medium, high, xhigh, max |
| `opus` | Opus | low, medium, high, xhigh, max |
| `fable` | Fable | low, medium, high, xhigh, max |
| `sonnet` | Sonnet | low, medium, high, xhigh, max |
| `haiku` | Haiku | low, medium, high, xhigh, max |

No explicit Opus 5.5 ID or label was advertised. The `opus` entry is an
alias; [Anthropic's model documentation](https://code.claude.com/docs/en/model-config)
says its resolution depends on provider and requires Claude Code 2.1.280 or
later for Opus 5.5. These unauthenticated observations cannot establish
account entitlement or the model reached during inference.

An isolated `claude auth login` PTY startup probe, bounded to 12 seconds,
passed its in-memory output through `ClaudeBrowserLoginAdapter.parse_progress` without
printing or retaining the authorization URL or terminal output. It parsed both
the authorization link and paste prompt. The owned CLI process was reaped and
all disposable containers were removed. Existing browser-login parser fixtures
and execution tests cover the remaining local contracts; no code submission,
OAuth completion, authenticated inference, or production restart was attempted.

To roll back this upgrade, restore `ARG CLAUDE_CODE_VERSION=2.1.126`, rebuild
the image, and redeploy through the normal operator-controlled process.

## How to roll back

Revert the Dockerfile change and rebuild. The `ARG` defaults are the only
source of truth; there is no separate lockfile to coordinate.

## Signals to monitor after upgrade

- OAuth / login flows on both CLIs still complete.
- Prompts still produce the expected stdout shape (the daemon parses CLI
  output in `src/coders/`).
- All e2e tests under `tests/e2e/` continue to pass.
- Rate-limit reporting still surfaces correctly on the dashboard.
