#!/usr/bin/env bash
set -e

git config --global user.email "alexbomber12@users.noreply.github.com"
git config --global user.name "Pipeline Orchestrator"
git config --global safe.directory '*'
gh auth setup-git 2>/dev/null || true

exec "$@"
