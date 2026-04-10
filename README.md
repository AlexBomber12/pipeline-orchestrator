# Pipeline Orchestrator

Autonomous AI development pipeline that drives a Claude Code agent through a queue
of pre-planned PRs against one or more GitHub repositories.

## Architecture

Three components share a single `/data` runtime root:

- **daemon** — stateless pipeline state machine. Reads `tasks/QUEUE.md` and the
  GitHub API on every tick and decides what to do next. Recoverable from a cold
  restart because no state lives in process memory.
- **web** — read-only FastAPI dashboard (Jinja2 + HTMX). Renders the current
  state of repositories and PRs. Holds zero AI logic and zero tokens.
- **redis** — bridge that lets the dashboard observe daemon state without
  reaching into its internals.

Sources of truth:

- `tasks/QUEUE.md` and `tasks/PR-*.md` for what work to do.
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

## Configuration

`config.yml` lives at the project root and is mounted read-only into both the
`web` and `daemon` containers. Minimal example with one repository:

```yaml
repositories:
  - name: my-repo
    owner: my-org
    url: https://github.com/my-org/my-repo.git

daemon:
  poll_interval_sec: 60
  review_timeout_min: 60
  hung_fallback_codex_review: true
  error_handler_use_ai: true

web:
  host: 0.0.0.0
  port: 8000

auth:
  claude_config_dir: /data/auth/claude
  gh_config_dir: /data/auth/gh
```
