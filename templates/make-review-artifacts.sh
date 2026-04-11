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

# Resolve the review base branch. The orchestrator supports repos
# whose base branch is not ``main``, so hardcoding ``origin/main``
# here would silently produce an empty or incorrect pr.patch on
# those repos. Order of precedence:
#   1. Explicit BASE_BRANCH env var (operator override).
#   2. The remote's HEAD symref, which git-clone sets to the
#      remote's default branch.
#   3. Fall back to ``main`` if neither is available.
base_branch="${BASE_BRANCH:-}"
if [[ -z "${base_branch}" ]]; then
    base_branch="$(git symbolic-ref --short refs/remotes/origin/HEAD 2>/dev/null | sed 's@^origin/@@' || true)"
fi
: "${base_branch:=main}"

echo "==> Generating artifacts/pr.patch (base: origin/${base_branch})"
git diff "origin/${base_branch}...HEAD" >artifacts/pr.patch || true

echo "==> Generating artifacts/structure.txt"
find . -type f \
    -not -path './.git/*' \
    -not -path '*/__pycache__/*' \
    -not -path './node_modules/*' \
    -not -path './artifacts/*' \
    | sort >artifacts/structure.txt

exit "${ci_status}"
