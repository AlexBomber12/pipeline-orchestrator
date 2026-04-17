"""Claude CLI statusline hook for in-flight rate-limit monitoring.

Executed by Claude CLI on every statusline update. Reads JSON from stdin,
compares ``rate_limits.five_hour.used_percentage`` and
``rate_limits.seven_day.used_percentage`` against configured thresholds,
and writes a breach marker file when limits are exceeded.

Fast, no network calls, no dependencies beyond stdlib.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path


def main() -> int:
    raw = sys.stdin.read()
    if not raw:
        sys.stdout.write("pipeline-orchestrator")
        return 0
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        sys.stdout.write("pipeline-orchestrator")
        return 0

    breach_dir = Path(os.environ.get("PIPELINE_BREACH_DIR", "/tmp/pipeline-breach"))
    breach_dir.mkdir(parents=True, exist_ok=True)

    session_threshold = int(os.environ.get("PIPELINE_SESSION_THRESHOLD", "95"))
    weekly_threshold = int(os.environ.get("PIPELINE_WEEKLY_THRESHOLD", "100"))

    rate_limits = data.get("rate_limits") or {}
    five_hour = rate_limits.get("five_hour") or {}
    seven_day = rate_limits.get("seven_day") or {}
    session_pct = int(five_hour.get("used_percentage") or 0)
    weekly_pct = int(seven_day.get("used_percentage") or 0)

    breach_type = None
    resets_at = 0
    if session_pct >= session_threshold:
        breach_type = "session"
        resets_at = int(five_hour.get("resets_at") or 0)
    elif weekly_pct >= weekly_threshold:
        breach_type = "weekly"
        resets_at = int(seven_day.get("resets_at") or 0)

    run_id = os.environ.get("PIPELINE_RUN_ID", "")
    if breach_type and run_id:
        marker = breach_dir / f"{run_id}.breach"
        marker.write_text(json.dumps({
            "type": breach_type,
            "resets_at": resets_at,
            "session_pct": session_pct,
            "weekly_pct": weekly_pct,
            "detected_at": time.time(),
        }))

    sys.stdout.write(
        f"orchestrator s{session_pct}% w{weekly_pct}%"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
