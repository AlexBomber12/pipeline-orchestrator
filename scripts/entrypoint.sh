#!/usr/bin/env bash
set -e

git_config_lock_dir="${HOME:-/tmp}/.gitconfig.setup.lock"
git_config_lock_stale_sec=30
git_config_lock_acquired=0
for _ in {1..100}; do
    if mkdir "${git_config_lock_dir}" 2>/dev/null; then
        git_config_lock_acquired=1
        trap 'rmdir "${git_config_lock_dir}" 2>/dev/null || true' EXIT
        git config --global user.email "alexbomber12@users.noreply.github.com"
        git config --global user.name "Pipeline Orchestrator"
        git config --global safe.directory '*'
        gh auth setup-git 2>/dev/null || true
        rmdir "${git_config_lock_dir}"
        trap - EXIT
        break
    fi
    lock_mtime="$(stat -c %Y "${git_config_lock_dir}" 2>/dev/null || printf '0')"
    now="$(date +%s)"
    if [ "${lock_mtime}" -gt 0 ] && [ $((now - lock_mtime)) -ge "${git_config_lock_stale_sec}" ]; then
        rm -rf "${git_config_lock_dir}"
        continue
    fi
    sleep 0.1
done
if [ "${git_config_lock_acquired}" -ne 1 ]; then
    echo "error: could not acquire git config setup lock: ${git_config_lock_dir}" >&2
    exit 1
fi

# Test Compose nests this entrypoint; only the inner web command builds CSS.
if [[ "${1:-}" == "uvicorn" || ( "${1:-}" == "python" && "${2:-}" == "-m" && "${3:-}" == "uvicorn" ) ]]; then
    python scripts/build_tailwind.py || echo "ERROR: Tailwind unavailable; continuing with existing CDN fallback" >&2
fi

exec "$@"
