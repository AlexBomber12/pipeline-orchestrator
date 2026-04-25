# End-to-end tests (integration tier)

This directory holds integration tests that exercise the running docker
stack rather than in-process Python objects. Tests here communicate with
`web-test` exclusively over HTTP and drive GitHub through the `gh` CLI.

## Layout

- `conftest.py` — shared fixtures (stack preflight, state polling, zip
  uploads, screenshot capture, testbed cleanup).
- `lib/` — helpers shared across multiple tests.
- `test_*.py` — the tests themselves. `test_smoke.py` is the one added
  in PR-153; PR-154 introduces the behavioural suite.
- `data/` — runtime bind mount for `/data` inside the test containers
  (gitignored).
- `evidence/` — screenshots captured by failing tests (gitignored).

## Running

Until `scripts/test-e2e.sh` lands (PR-155), bring the stack up and run
pytest against this directory explicitly:

```bash
docker compose -f docker-compose.test.yml up -d --build
pytest tests/e2e/
docker compose -f docker-compose.test.yml down -v
```

## Why this tier lives outside `scripts/ci.sh`

`scripts/ci.sh` is invoked by the coder from inside the `daemon`
container, which has no access to the host docker socket. It therefore
cannot bring up a sibling compose stack. Integration tests run through
`scripts/test-e2e.sh` (PR-155) on the host or in GitHub Actions instead,
and `pyproject.toml` sets `norecursedirs = ["tests/e2e"]` so the default
`pytest` invocation keeps skipping this tier.

See `docs/local-e2e.md` (added in PR-155b) for a deeper local-debug
walkthrough.
