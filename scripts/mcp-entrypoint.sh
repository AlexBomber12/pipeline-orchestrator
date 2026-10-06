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

# Port 5173 remains the tunnel-compatible listener and deliberately has no
# runtime diagnostics. The localhost-published port maps to the opted-in 5174
# listener in the same MCP service container.
env -u REDIS_URL -u PO_EVENTS_DIR \
    MCP_RUNTIME_DIAGNOSTICS=0 MCP_SERVER_PORT=5173 \
    python -m src.mcp &
child_pids+=("$!")

MCP_RUNTIME_DIAGNOSTICS=1 MCP_SERVER_PORT=5174 python -m src.mcp &
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
