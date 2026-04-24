#!/usr/bin/env bash
# Day 4 auto-test runner (fixed service detection).
# Usage:
#   ./run.sh [--headed]
#
# --headed  Show chromium browser window (for debug).

set -euo pipefail

REPO_DIR="/home/alexey/pipeline-orchestrator"
cd "$REPO_DIR"

HEADED_FLAG=""
if [[ "${1:-}" == "--headed" ]]; then
  HEADED_FLAG="--headed"
fi

echo "=== Day 4 auto-test run ==="
echo "Repo: $REPO_DIR"
echo "Time: $(date -Iseconds)"
echo

# Check docker compose services via --services --filter status=running.
# This avoids column-format regex which breaks on docker compose v2 output
# ("Up 11 minutes" vs lowercase "running").
echo "--- Checking docker compose state ---"
RUNNING_SERVICES="$(docker compose ps --services --filter status=running 2>/dev/null || true)"

for svc in daemon web redis; do
  if ! printf '%s\n' "$RUNNING_SERVICES" | grep -qx "$svc"; then
    echo "ERROR: '$svc' service not running."
    echo "Fix: cd $REPO_DIR && docker compose up -d"
    echo
    echo "Current running services:"
    printf '%s\n' "$RUNNING_SERVICES" | sed 's/^/  /'
    exit 1
  fi
done
echo "Services OK: daemon, web, redis all running"
echo

# Check PR-190a marker in daemon src/.
echo "--- Checking PR-190a is merged ---"
if ! docker compose exec -T daemon grep -q "_NO_PR_RETRY_COUNTS" /app/src/daemon/handlers/coding.py 2>/dev/null; then
  echo "WARNING: PR-190a marker not found in daemon src/. EXT-01 may fail."
  echo "If PR-190a is merged, the in-memory counter variable should be present."
  read -p "Continue anyway? [y/N] " ans
  if [[ "$ans" != "y" ]]; then
    exit 1
  fi
fi
echo "PR-190a marker OK"
echo

# Verify dashboard reachable.
echo "--- Checking dashboard reachable ---"
if ! curl -s -f http://localhost:8800/api/states -o /dev/null; then
  echo "ERROR: dashboard at http://localhost:8800 not reachable."
  exit 1
fi
echo "Dashboard reachable"
echo

# Clean previous evidence.
echo "--- Cleaning previous evidence ---"
rm -f /tmp/day4-report.html
rm -rf /tmp/day4-evidence
mkdir -p /tmp/day4-evidence
echo "Cleaned"
echo

# Run pytest.
echo "--- Running pytest suite (may take 20-30 minutes) ---"
export PYTHONPATH="$REPO_DIR:${PYTHONPATH:-}"

pytest \
  tests-manual/auto/day4/tests/ \
  --html=/tmp/day4-report.html \
  --self-contained-html \
  -v \
  --tb=short \
  --capture=no \
  $HEADED_FLAG \
  || EXIT_CODE=$?

EXIT_CODE="${EXIT_CODE:-0}"

echo
echo "=== Run complete ==="
echo "Report: /tmp/day4-report.html"
echo "Evidence: /tmp/day4-evidence/"
echo "Exit code: $EXIT_CODE"
echo

# Try to open report in Windows browser via WSL.
if command -v wslview &>/dev/null; then
  wslview /tmp/day4-report.html 2>/dev/null || true
elif command -v explorer.exe &>/dev/null; then
  WIN_PATH=$(wslpath -w /tmp/day4-report.html)
  explorer.exe "$WIN_PATH" 2>/dev/null || true
fi

echo "Done. Open /tmp/day4-report.html in your browser if not opened automatically."

exit "$EXIT_CODE"
