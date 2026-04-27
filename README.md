# Pipeline Orchestrator

Autonomous AI development pipeline that drives a Claude Code agent through a queue
of pre-planned PRs against one or more GitHub repositories.

## Architecture

Three components share a single `/data` runtime root:

- **daemon** — stateless pipeline state machine. Reads structured headers from
  `tasks/PR-*.md` files, derives statuses from git state via `task_status.py`,
  and queries the GitHub API on every tick to decide what to do next.
  Recoverable from a cold restart because runtime state is rebuilt from
  `tasks/QUEUE.md` and GitHub rather than process memory.
- **web** — FastAPI dashboard (Jinja2 + HTMX). Renders the current state of
  repositories and PRs. Provides settings UI for managing repos and daemon
  configuration, and supports uploading task files and pushing them to repos.
- **redis** — bridge that lets the dashboard observe daemon state without
  reaching into its internals.

Sources of truth:

- `tasks/PR-*.md` files for what work to do (one file per PR, with structured
  headers). `tasks/QUEUE.md` is a derived artifact that the daemon
  auto-generates during eligible IDLE cycles for human readability, and it
  remains a daemon recovery input on startup today.
- GitHub (via `gh` CLI) for PR status, reviews, and Codex reactions.
- `config.yml` for which repositories the daemon manages.

## Quick Start

```sh
docker compose up --build
```

On first run, log in to the tools that the daemon shells out to:

```sh
docker compose exec daemon gh auth login
docker compose exec daemon claude login
```

The dashboard is then available at http://localhost:8000.

Each connected repository must have a `CLAUDE.md` file that includes
`Read and follow AGENTS.md in this repository.` so the Claude Code agent
picks up the pipeline's conventions.

## Configuration

`config.yml` lives at the project root and is mounted into both the
`web` and `daemon` containers. Minimal example with one repository:

```yaml
repositories:
  - url: https://github.com/my-org/my-repo.git
    branch: main
    auto_merge: true

daemon:
  poll_interval_sec: 60
  review_timeout_min: 60
  hung_fallback_codex_review: true
  error_handler_use_ai: true
  claude_model: opus
  fix_idle_timeout_sec: 1800
  fix_iteration_cap: 15
  planned_pr_timeout_sec: 900

web:
  host: 0.0.0.0
  port: 8000

auth:
  claude_config_dir: /data/auth/claude
  gh_config_dir: /data/auth/gh
```

`PO_FIX_ITERATION_CAP` overrides `daemon.fix_iteration_cap` for daemon runs.

## Local development

For one-time setup of GitHub Actions integration tests (GitHub App provisioning), see [docs/ci-setup.md](docs/ci-setup.md).

For local debugging of e2e tests (running tests against a local test stack outside of CI), see [docs/local-e2e.md](docs/local-e2e.md).

The `@anthropic-ai/claude-code` and `@openai/codex` CLI versions are pinned in `Dockerfile` via build args. See [docs/runbooks/upgrade-cli-versions.md](docs/runbooks/upgrade-cli-versions.md) to upgrade.
