#!/usr/bin/env bash
set -euo pipefail

VOLUME_NAME="${VOLUME_NAME:-pipeline-orchestrator_codex-auth-test}"

if ! docker volume inspect "$VOLUME_NAME" >/dev/null 2>&1; then
  docker volume create "$VOLUME_NAME" >/dev/null
fi

docker run --rm -v "${VOLUME_NAME}:/target" alpine chown -R 1000:1000 /target

echo "OK: $VOLUME_NAME owned by 1000:1000"
