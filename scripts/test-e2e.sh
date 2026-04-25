#!/usr/bin/env bash
# Wrapper to run the e2e suite locally against docker-compose.test.yml.
# Flags: --keep-up, --no-up, --filter PATTERN, --scenario NAME, --help/-h.
# Local dev only; GHA owns merge gating (see docs/local-e2e.md).
set -euo pipefail

cd "$(dirname "$0")/.."

KEEP_UP=0
NO_UP=0
FILTER=""
SCENARIO=""

usage() {
  cat <<'EOF'
Usage: scripts/test-e2e.sh [--keep-up] [--no-up] [--filter PATTERN] [--scenario NAME] [--help]

Brings up the test stack on port 18800, runs tests/e2e/ via pytest, then tears down.

Flags:
  --keep-up         Skip teardown after tests so you can inspect the stack.
  --no-up           Skip startup; assume the test stack is already running.
  --filter PATTERN  Forward as -k PATTERN to pytest.
  --scenario NAME   Pre-write NAME to tests/e2e/data/shim-scenario before running tests.
  -h, --help        Print this usage and exit 0.

See docs/local-e2e.md for details and troubleshooting.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --keep-up)
      KEEP_UP=1
      shift
      ;;
    --no-up)
      NO_UP=1
      shift
      ;;
    --filter)
      if [[ $# -lt 2 ]]; then
        echo "error: --filter requires a PATTERN argument" >&2
        exit 2
      fi
      FILTER="$2"
      shift 2
      ;;
    --scenario)
      if [[ $# -lt 2 ]]; then
        echo "error: --scenario requires a NAME argument" >&2
        exit 2
      fi
      SCENARIO="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$NO_UP" -eq 0 ]]; then
  bash scripts/init-test-volume.sh
  docker compose -f docker-compose.test.yml up -d --wait
fi

if [[ -n "$SCENARIO" ]]; then
  mkdir -p tests/e2e/data
  printf '%s' "$SCENARIO" > tests/e2e/data/shim-scenario
fi

FILTER_ARGS=()
if [[ -n "$FILTER" ]]; then
  FILTER_ARGS=(-k "$FILTER")
fi

set +e
python -m pytest tests/e2e/ -v "${FILTER_ARGS[@]}"
exit_code=$?
set -e

if [[ "$KEEP_UP" -eq 0 ]]; then
  docker compose -f docker-compose.test.yml down -v
fi

echo "$([ $exit_code -eq 0 ] && echo PASS || echo FAIL) in ${SECONDS}s"
exit "$exit_code"
