#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

mkdir -p artifacts

echo "==> Running scripts/ci.sh -> artifacts/ci.log"
set +e
bash scripts/ci.sh >artifacts/ci.log 2>&1
ci_status=$?
set -e
echo "ci.sh exit code: ${ci_status}" | tee -a artifacts/ci.log

echo "==> Generating artifacts/pr.patch"
git diff origin/main...HEAD >artifacts/pr.patch || true

echo "==> Generating artifacts/structure.txt"
find . -type f \
    -not -path './.git/*' \
    -not -path '*/__pycache__/*' \
    -not -path './node_modules/*' \
    -not -path './artifacts/*' \
    | sort >artifacts/structure.txt

exit "${ci_status}"
