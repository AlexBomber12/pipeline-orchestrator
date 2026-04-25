# tests/e2e

## What this is

Integration test layer that exercises the running daemon end-to-end against a
docker compose test stack (introduced fully in PR-153b) on port 18800,
isolated from the production stack on 8800. These tests drive HTTP requests to
the dashboard, observe Redis-backed state transitions, and capture browser
evidence via Playwright.

## Status of this PR (PR-153a)

Python skeleton only. The directory, conftest fixtures, and a single import
smoke test exist, but the layer is **not yet runnable end-to-end**: there is
no docker compose test stack, no `requirements-test.txt`, and Playwright is
not installed. PR-153b adds the docker stack; PR-153c adds the test
dependencies and `config.test.yml`.

## Directory layout

- `__init__.py`: package marker so pytest can discover the suite.
- `lib/`: shared helpers reused across e2e tests (placeholder; populated by
  PR-154a).
- `evidence/`: Playwright screenshots (gitignored).
- `data/`: runtime artifacts such as uploaded zips (gitignored).
- `test_*.py`: behavioral tests added in the PR-154 series.

## How to run

Forward-looking — valid only after PR-153c lands:

```
pip install -r requirements-test.txt
playwright install chromium
pytest tests/e2e/
```

For local debugging once PR-155c lands, use `scripts/test-e2e.sh`.

## Why not in scripts/ci.sh

The coder runs `scripts/ci.sh` inside the daemon container, which has no host
docker socket. Integration tests require docker compose against a separate
stack and therefore must run from a host-level entry point. This is the
mainstream CI pattern: a fast unit tier in the container, a slower
integration tier on the host or in a CI runner.
