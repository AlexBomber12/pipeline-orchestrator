#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> ruff check src/"
python -m ruff check src/

if [ -d tests ]; then
    echo "==> pytest --cov=src --cov-report=term-missing --cov-fail-under=86 -q"
    python -m pytest --cov=src --cov-report=term-missing --cov-fail-under=86 -q
else
    echo "==> tests/ directory not present, skipping pytest"
fi

echo "==> ci.sh OK"
