# Day 5 Auto-Test Pack

## Context

Day 5 of testing-week for pipeline-orchestrator. Scope: fix-and-re-verify.

Day 4 produced 12 observations (OBS-1..12) + 2 real product findings. 4 tests
failed: 2 due to pack infrastructure bugs (OBS-9 root exec missing), 2 due to
test design gaps (FINDING-1 event window, FINDING-2 coder routing).

Day 5 pack incorporates all Day 4 fixes and splits the EVENT-LOG-01 test into
two time-window sub-tests to isolate OBS-13 (event history eviction).

## What changed vs Day 4 pack

Pack infrastructure fixes:
- OBS-6 (run.sh): service detection uses `docker compose ps --services --filter status=running` instead of regex on formatted column output.
- OBS-8 (conftest.py): screenshots embedded as base64 data URL so they render inline in the self-contained HTML report.
- OBS-9 (lib/failure_helpers.py): privileged setup/teardown ops pass `-u root` to `docker compose exec`; verification still runs as runner.
- OBS-10 (conftest.py): `take_screenshot` awaits hydration selector before capturing to avoid empty white frames.

Test design changes:
- FINDING-2 (test_ext_02): task is now pinned to `coder: codex` via make_task_zip parameter, forcing daemon to actually attempt Codex.
- FINDING-1/OBS-13 (test_event_recheck_01): split into two tests — 60s wait and 3min wait — to measure whether EVENT-LOG-01 is fully resolved or only partially.
- New test (test_env_token_fallback): verifies OBS-5 fallback strategy. Adds `GITHUB_TOKEN` to `.env` and monitors 5 minutes for fetch 128 errors.
- PR numbering: Day 5 tests use PR-3XX range (301, 302, 310, 311, 320) to avoid collision with roadmap PR-153..PR-179.

Cross-test contamination:
- New `reset_testbed_clean` fixture closes open PRs + calls `/recover` endpoint after each test. Used by tests that create PRs.

## Prerequisites before running

1. docker compose up -d (daemon, web, redis all running)
2. Testbed repo added in dashboard as `AlexBomber12__pipeline-orchestrator-testbed`
3. Python venv with pytest, pytest-playwright, pytest-html, requests, redis
4. Playwright chromium installed: `playwright install chromium`
5. For ENV-TOKEN-01 test (OBS-5 verification) — add token to .env:
   ```
   cd /home/alexey/pipeline-orchestrator
   echo "GITHUB_TOKEN=$(gh auth token)" >> .env
   docker compose up -d daemon
   docker compose exec -T daemon printenv GITHUB_TOKEN   # confirm visible
   ```
   If skipped, ENV-TOKEN-01 will SKIP automatically with a hint.

## Install the pack

```
cd /home/alexey/pipeline-orchestrator
# Extract the zip into tests-manual/auto/ (if arriving as zip)
# OR if arriving as a directory, just cp -r it
cp -r /path/to/day5/ tests-manual/auto/day5/
chmod +x tests-manual/auto/day5/run.sh
```

## Run

```
cd /home/alexey/pipeline-orchestrator
./tests-manual/auto/day5/run.sh
```

Or with browser visible (debugging):
```
./tests-manual/auto/day5/run.sh --headed
```

Expected duration: 25-35 minutes.

Report opens automatically at `/tmp/day5-report.html` via wslview.

## Tests included

Total: 9 tests (8 Day 4 re-runs + 1 new).

| Test                              | Day 4 result | Day 5 expectation | Duration est. |
|-----------------------------------|--------------|-------------------|----------------|
| DATA-01 sigkill regen             | PASS         | PASS              | ~25s           |
| DATA-02 upload during redis down  | PASS         | PASS              | ~75s           |
| EVENT-LOG-01 short 60s            | N/A (new)    | PASS (regress check) | ~90s        |
| EVENT-LOG-01 long 3min            | FAIL (OBS-13)| FAIL expected, confirms bug | ~210s |
| EXT-01 no_pr triggers HUNG        | FAIL (OBS-9) | PASS with root fix | ~5min         |
| EXT-01 counter resets             | PASS         | PASS              | ~90s           |
| EXT-02 codex pin fails clearly    | FAIL (F-2)   | PASS with codex pin | ~3min         |
| EXT-03 github unreachable         | FAIL (OBS-9) | PASS with root fix | ~2min         |
| EXT-04 redis down recover         | PASS         | PASS              | ~55s           |
| EXT-05 usage api degradation      | PASS         | PASS              | ~1s            |
| ENV-TOKEN-01 OBS-5 fallback       | N/A (new)    | PASS if .env set, SKIP otherwise | ~5min |

## Decision matrix for observed results

Scenario A: all 9 tests PASS.
- Testing-week closes clean. Proceed to sprint 10 (SoT refactor).
- OBS-5 token fallback is production-grade; promote to roadmap.

Scenario B: 8-9 PASS, EVENT-LOG-01 long FAIL.
- Expected outcome per Day 4 diagnosis. OBS-13 confirmed.
- Add PR candidate for "exempt lifecycle events from dedup / expand history window".
- Testing-week still closes clean.

Scenario C: ENV-TOKEN-01 FAIL (with token set).
- OBS-5 root cause is not purely token availability.
- Promote OBS-5 to sprint 10 as PR-192 (deeper instrumentation).
- Consider rollback of .env change if it has side effects.

Scenario D: EXT-01 no_pr or EXT-03 FAIL despite root fix.
- New pack bug or product regression; investigate before closing.
- Likely shim interaction or logic issue, not fundamental.

Scenario E: EXT-02 pin test FAIL.
- Product bug: daemon silently swaps pinned coder. This is a real issue for users
  who depend on coder pinning. Escalate to sprint 10 blockers.

## Files in this pack

```
tests-manual/auto/day5/
  README.md              (this file)
  run.sh                 (entry point)
  pytest.ini             (test discovery config)
  conftest.py            (fixtures; OBS-8/10 fixed)
  lib/
    __init__.py
    failure_helpers.py   (shim helpers; OBS-9 fixed)
  tests/
    __init__.py
    test_data_01_sigkill_regen.py
    test_data_02_upload_during_redis_down.py
    test_event_recheck_01.py          (split: short + long)
    test_ext_01_claude_no_pr.py
    test_ext_02_codex_unavailable.py  (coder pinned)
    test_ext_03_github_unreachable.py
    test_ext_04_redis_disconnect.py
    test_ext_05_usage_api.py
    test_env_token_fallback.py        (new, OBS-5)
  evidence/              (empty, runtime output goes here)
```

## Known gotchas

- STALLED indicator is still in `base.html` (PR-156 not merged yet). If a test
  screenshot shows "WATCH STALLED" in the UI, that is the JS client-side 30-second
  staleness indicator, not a test failure. Not to be interpreted as product bug.
- Anthropic usage API returning 403 "check anthropic-beta header" is unrelated to
  tests and expected per Day 4 observation.
- ENV-TOKEN-01 test is time-sensitive (300s wait). If OBS-5 is driven by
  concurrent load rather than token availability, it may PASS on idle but
  OBS-5 remains in the backlog.
