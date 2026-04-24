#!/usr/bin/env bash
# Day 5 auto-test runner (incorporates all Day 4 lessons learned).
#
# Fixes baked in from Day 4:
# - OBS-6: service detection via --services --filter status=running (no regex).
# - OBS-8: screenshots embedded base64 in HTML report (see conftest.py).
# - OBS-9: privileged ops use -u root (see lib/failure_helpers.py).
# - OBS-10: take_screenshot waits for hydration (see conftest.py).
#
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

echo "=== Day 5 auto-test run ==="
echo "Repo: $REPO_DIR"
echo "Time: $(date -Iseconds)"
echo

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

echo "--- Checking PR-190a marker is merged ---"
if ! docker compose exec -T daemon grep -q "_NO_PR_RETRY_COUNTS" /app/src/daemon/handlers/coding.py 2>/dev/null; then
  echo "WARNING: PR-190a marker not found in daemon src/."
  echo "If you expected PR-190a to be merged, this is a problem."
  read -p "Continue anyway? [y/N] " ans
  if [[ "$ans" != "y" ]]; then
    exit 1
  fi
fi
echo "PR-190a marker OK"
echo

echo "--- Checking GITHUB_TOKEN env fallback for ENV-TOKEN-01 test ---"
TOKEN_IN_ENV="$(docker compose exec -T daemon printenv GITHUB_TOKEN 2>/dev/null || echo '')"
if [[ -z "$TOKEN_IN_ENV" ]]; then
  echo "NOTE: GITHUB_TOKEN not set in daemon container."
  echo "ENV-TOKEN-01 test will SKIP. To enable OBS-5 fallback verification:"
  echo "  echo \"GITHUB_TOKEN=\$(gh auth token)\" >> $REPO_DIR/.env"
  echo "  docker compose up -d daemon"
  echo
else
  echo "GITHUB_TOKEN present in daemon (OBS-5 fallback active, length=${#TOKEN_IN_ENV})"
  echo
fi

echo "--- Checking dashboard reachable ---"
if ! curl -s -f http://localhost:8800/api/states -o /dev/null; then
  echo "ERROR: dashboard at http://localhost:8800 not reachable."
  exit 1
fi
echo "Dashboard reachable"
echo

echo "--- Cleaning previous evidence ---"
rm -f /tmp/day5-report.html
rm -rf /tmp/day5-evidence
mkdir -p /tmp/day5-evidence
echo "Cleaned"
echo

echo "--- Running pytest suite (may take 25-35 minutes) ---"
export PYTHONPATH="$REPO_DIR:${PYTHONPATH:-}"

pytest \
  tests-manual/auto/day5/tests/ \
  --html=/tmp/day5-report.html \
  --self-contained-html \
  -v \
  --tb=short \
  --capture=no \
  $HEADED_FLAG \
  || EXIT_CODE=$?

EXIT_CODE="${EXIT_CODE:-0}"

echo
echo "=== Run complete ==="
echo "Report: /tmp/day5-report.html"
echo "Evidence: /tmp/day5-evidence/"
echo "Exit code: $EXIT_CODE"
echo

if command -v wslview &>/dev/null; then
  wslview /tmp/day5-report.html 2>/dev/null || true
elif command -v explorer.exe &>/dev/null; then
  WIN_PATH=$(wslpath -w /tmp/day5-report.html)
  explorer.exe "$WIN_PATH" 2>/dev/null || true
fi

echo "Done. Open /tmp/day5-report.html in your browser if not opened automatically."

exit "$EXIT_CODE"
