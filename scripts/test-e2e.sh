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
  --scenario NAME   Pre-write NAME to tests/e2e/data/shim-scenario before running
                    tests; the prior file contents (or success if absent) are
                    restored on exit so the override does not leak across runs.
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

SHIM_SCENARIO_PATH="tests/e2e/data/shim-scenario"
SHIM_BACKUP=""
RESTORE_SHIM=0
exit_code=0

cleanup() {
  trap_rc=$?
  trap - EXIT
  set +e

  if [[ "$RESTORE_SHIM" -eq 1 ]]; then
    if [[ -n "$SHIM_BACKUP" && -f "$SHIM_BACKUP" ]]; then
      mv -f "$SHIM_BACKUP" "$SHIM_SCENARIO_PATH"
    else
      printf 'success\n' > "$SHIM_SCENARIO_PATH"
    fi
  fi
  if [[ -n "$SHIM_BACKUP" && -f "$SHIM_BACKUP" ]]; then
    rm -f "$SHIM_BACKUP"
  fi

  if [[ "$NO_UP" -eq 0 && "$KEEP_UP" -eq 0 ]]; then
    teardown_rc=0
    docker compose -f docker-compose.test.yml down -v || teardown_rc=$?
    if [[ "$teardown_rc" -ne 0 ]]; then
      echo "warning: teardown failed (rc=$teardown_rc); preserving exit code $exit_code" >&2
    fi
  fi

  final_rc="$exit_code"
  if [[ "$final_rc" -eq 0 && "$trap_rc" -ne 0 ]]; then
    final_rc="$trap_rc"
  fi

  if [[ "$final_rc" -eq 0 ]]; then
    echo "PASS in ${SECONDS}s"
  else
    echo "FAIL in ${SECONDS}s"
  fi
  exit "$final_rc"
}
trap cleanup EXIT

if [[ "$NO_UP" -eq 0 ]]; then
  bash scripts/init-test-volume.sh
  docker compose -f docker-compose.test.yml up -d --wait
fi

if [[ -n "$SCENARIO" ]]; then
  mkdir -p tests/e2e/data
  if [[ -f "$SHIM_SCENARIO_PATH" ]]; then
    SHIM_BACKUP="$(mktemp)"
    cp "$SHIM_SCENARIO_PATH" "$SHIM_BACKUP"
  fi
  RESTORE_SHIM=1
  printf '%s' "$SCENARIO" > "$SHIM_SCENARIO_PATH"
fi

FILTER_ARGS=()
if [[ -n "$FILTER" ]]; then
  FILTER_ARGS=(-k "$FILTER")
fi

python -m pytest tests/e2e/ -v "${FILTER_ARGS[@]}" || exit_code=$?
