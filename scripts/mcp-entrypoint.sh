#!/usr/bin/env bash
set -uo pipefail

child_pids=()

shutdown() {
    trap - EXIT TERM INT
    if ((${#child_pids[@]})); then
        kill "${child_pids[@]}" 2>/dev/null || true
        wait "${child_pids[@]}" 2>/dev/null || true
    fi
}

trap shutdown EXIT TERM INT

# Port 5173 remains tunnel-compatible. Runtime diagnostics and Redis access are
# added there only when the listener's Cloudflare Access protection is enabled.
remote_diagnostics="${MCP_REMOTE_DIAGNOSTICS:-0}"
# Match Python's strip/lower parsing so a quoted value with surrounding
# whitespace cannot enable diagnostics after this script has removed Redis.
remote_diagnostics="${remote_diagnostics#"${remote_diagnostics%%[![:space:]]*}"}"
remote_diagnostics="${remote_diagnostics%"${remote_diagnostics##*[![:space:]]}"}"
case "${remote_diagnostics,,}" in
    1|true|yes|on)
        MCP_RUNTIME_DIAGNOSTICS=1 MCP_SERVER_PORT=5173 python -m src.mcp &
        ;;
    *)
        env -u REDIS_URL \
            MCP_RUNTIME_DIAGNOSTICS=0 MCP_SERVER_PORT=5173 \
            python -m src.mcp &
        ;;
esac
child_pids+=("$!")

MCP_REMOTE_DIAGNOSTICS=0 MCP_RUNTIME_DIAGNOSTICS=1 MCP_SERVER_PORT=5174 python -m src.mcp &
child_pids+=("$!")

set +e
wait -n "${child_pids[@]}"
status=$?
set -e

# Either listener exiting makes the service unhealthy and lets Compose apply
# its existing restart policy. A clean child exit is still unexpected here.
if ((status == 0)); then
    status=1
fi
exit "${status}"
