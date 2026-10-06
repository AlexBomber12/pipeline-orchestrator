# Pipeline Orchestrator

Pipeline orchestrator daemon that drives Claude Code or Codex CLI agents through PLANNED PR and FIX FEEDBACK workflows.

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
docker compose exec daemon codex login
```

Authenticate either Claude Code (`claude login`) or Codex CLI (`codex login`) depending on which coder a repo is configured to use; both can be authenticated to support fallback. The daemon's auth probe is implemented in `src/coders/codex.py::check_auth` and runs `codex login status` to verify the Codex session.

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

coder_plugins:
  claude: src.coders.claude:ClaudePlugin
  codex: src.coders.codex:CodexPlugin

daemon:
  poll_interval_sec: 60
  review_timeout_min: 20
  hung_fallback_codex_review: true
  error_handler_use_ai: true
  coder_settings:
    claude:
      model: opus
    codex:
      reasoning_effort: high
  fix_idle_timeout_sec: 1800
  fix_iteration_cap: 15
  planned_pr_timeout_sec: 3600

web:
  host: 0.0.0.0
  port: 8000

auth:
  claude_config_dir: /data/auth/claude
  gh_config_dir: /data/auth/gh
```

`PO_FIX_ITERATION_CAP` overrides `daemon.fix_iteration_cap` for daemon runs.

Coder model choices are stored by stable plugin ID under
`daemon.coder_settings`. Legacy `daemon.claude_model` and
`daemon.codex_model` values remain supported as fallbacks when a plugin does
not have an explicit generic `model` setting. An explicit generic value wins
over its legacy fallback. Codex preserves `model: ""` as the CLI-default
choice; Claude normalizes an empty choice to its `opus` default.

`coder_plugins` is a top-level mapping from a stable, route-safe plugin ID
(ASCII letters/digits, underscores, and hyphens) to a trusted `module:factory`
reference. The ID `gh` is reserved for the dashboard's GitHub CLI
infrastructure status. The no-argument factory must already be importable
in both the web and daemon Python environments and must return a complete
`CoderPlugin` whose `name` exactly matches the configured ID. Claude and Codex
use the references shown above as compatibility defaults. An explicit entry
with either ID replaces that default; any other ID adds a registry entry.
Plugin options remain separate under `daemon.coder_settings.<plugin-id>`.
Those persisted option values are strings, which keeps configured model keys
safe across both startup and config-file reload validation without importing
plugin modules during configuration parsing. A plugin model binding cannot use
`reasoning_effort` as its `setting_key`; that key is reserved for the plugin's
execution option.

Plugin modules are operator-managed code: loading a reference does not install
packages, download code, or sandbox the import. Definitions are loaded only at
service startup, so deploy the module and restart both `web` and `daemon` after
changing `coder_plugins`; config reload does not hot-swap implementations.
Registration exposes shared metadata and generic model controls, but runtime
repository/default selection is still limited to the existing `CoderType`
values (`claude` and `codex`). Arbitrary registered IDs are therefore not yet
complete support for executing additional providers. Legacy model-field
fallbacks are likewise owned by their built-ins (`claude_model` by `claude`
and `codex_model` by `codex`); additional plugins must set their model
metadata's `config_field` to `None` and use `coder_settings`. Configured model
catalog methods run through the daemon in bounded worker processes, while the
web cache key is derived from validated configuration; configured catalog code
does not execute inside the web control plane. The daemon also supplies
validated plugin metadata to web over Redis, so configured modules and
factories are never imported or instantiated by FastAPI. If daemon metadata is
temporarily unavailable, the dashboard still starts with that plugin marked
unavailable and its model-setting control disabled.

Codex reasoning effort can be overridden for each new invocation with
`daemon.coder_settings.codex.reasoning_effort`. Omit the key or set it to an
empty string to let the Codex CLI use its configured/default effort. A nonempty
value is forwarded unchanged; supported values depend on the selected model.

## Local development

For one-time setup of GitHub Actions integration tests (GitHub App provisioning), see [docs/ci-setup.md](docs/ci-setup.md).

For local debugging of e2e tests (running tests against a local test stack outside of CI), see [docs/local-e2e.md](docs/local-e2e.md).

The `@anthropic-ai/claude-code` and `@openai/codex` CLI versions are pinned in `Dockerfile` via build args. See [docs/runbooks/upgrade-cli-versions.md](docs/runbooks/upgrade-cli-versions.md) to upgrade.
