# Architecture: pipeline state machine

This document describes the canonical pipeline states the daemon transitions
through for each PR, the meaning of each state, and the historical
`WATCH STALLED` compound rendering that the dashboard used to surface.

## Canonical states

`PipelineState` is defined in `src/models.py`. The dashboard renders one of
these values per repo at any given time; there are no compound states.

| State       | Meaning                                                                                                |
| ----------- | ------------------------------------------------------------------------------------------------------ |
| `IDLE`      | No active task. Daemon polls `tasks/QUEUE.md` plus GitHub for new work at `poll_interval_sec` cadence. |
| `CODING`    | Coder subprocess is running for a freshly selected task; daemon watches its stdout.                    |
| `WATCH`     | A PR is open. Daemon polls CI status, review state, and review comments; when CI is green and review approves, `handle_watch` calls `handle_merge` inline (no `MERGE` transition) and then returns to `IDLE`. |
| `FIX`       | Coder subprocess is running in `FIX FEEDBACK` mode against an existing PR branch.                      |
| `HUNG`      | Daemon detected a stuck WATCH cycle and is running the hung-recovery routine.                          |
| `ERROR`     | An error occurred during task processing. May persist across ticks; see [ERROR state details](#error-state-details). |
| `PAUSED`    | Operator pressed Pause. No state advancement until Resume.                                             |

Operators see the canonical state in the repo card badge; semantic detail
(CI status, review status, last push age) appears in the PR panel below it.

### `PipelineState` values that are not live daemon-run states

The enum also defines `PREFLIGHT` and `MERGE`, but the daemon does not assign
either during a normal run cycle. They appear in dashboards or stale-state
cleanup only:

- **`PREFLIGHT`** is a synthetic dashboard fallback. `src/web/app.py`
  substitutes `PREFLIGHT` when Redis is unreachable or no state has been
  published yet for a repo (`Redis unavailable — state unknown` /
  `Waiting for daemon to initialize`). The daemon's pre-tick checks live in
  `Runner.preflight()` but never write `PipelineState.PREFLIGHT` to
  `state.state`. If you see `PREFLIGHT` on a card, look at Redis health, not
  daemon progress.
- **`MERGE`** is a legacy enum value with no live transition path. No handler
  under `src/daemon/handlers/` sets `state.state = PipelineState.MERGE`;
  merges happen inline inside `handle_watch` (see `src/daemon/handlers/watch.py`)
  by calling `handle_merge` directly. `MERGE` only appears in
  `_TRANSIENT_STATES` in `src/daemon/runner.py`, which resets any stale
  pre-existing `MERGE` value to `IDLE` on the next tick. Triage as if the
  repo were in `WATCH`.

### Transitional states (not dispatched by run_cycle)

`CODING`, `FIX`, and `MERGE` are not dispatched by the run_cycle state-machine entry point. They are invoked inline from inside other handlers:

- `CODING` is invoked from `handle_idle` when a new task is selected. The happy-path exit is `WATCH` (PR created); off-path exits are `HUNG` (subprocess timeout / no-push circuit breaker) and `PAUSED` (coder rate-limit). `CODING` does not assign `IDLE` directly.
- `FIX` is invoked from `handle_watch` when CHANGES_REQUESTED or CI failure is detected. The happy-path exit is `WATCH` (commits pushed); other exits are `IDLE` (nothing to fix or task cleared), `HUNG` (subprocess timeout / circuit breaker), and `PAUSED` (coder rate-limit). A nested cycle can also re-enter `FIX`.
- `MERGE` is documented as a legacy enum value with no live transition path. Merges happen inline inside `handle_watch::handle_merge`.

This means the run_cycle dispatch table in `runner.py:1361-1377` lists 5 states (IDLE, WATCH, HUNG, PAUSED, ERROR), but the operator-visible state machine has 8 substates because dashboards reflect CODING and FIX during their inline execution.

## WATCH state details

WATCH is entered after `CODING` produces a PR, after `FIX` pushes new
commits, or on daemon restart when `recover_state` finds an open PR for the
active task.

While in WATCH the daemon polls:

- CI status (GitHub Checks, via REST `check-runs`)
- Review status (`PENDING`, `EYES`, `APPROVED`, `CHANGES_REQUESTED`)
- New Codex feedback comments since the last push

Polling cadence is governed by the slow-start / fast-tail adaptive logic in
`WatchMixin.effective_watch_poll_interval`: the first `watch_slow_window_sec`
after WATCH entry uses `watch_slow_poll_interval_sec` (Codex and CI rarely
respond in the first minutes, so fast polling there wastes GitHub quota);
past the window, `watch_fast_poll_interval_sec` takes over because a verdict
may arrive at any second.

### Stale review re-trigger

WATCH can detect that a review request has gone unanswered too long. When
`review_status` is `CHANGES_REQUESTED` and no new Codex feedback has appeared
within `stale_review_threshold_min` of the last push, or `review_status` is
`EYES` and `stale_review_threshold_eyes_min` has elapsed,
`_maybe_retrigger_stale_review` re-posts `@codex review` on the PR. This is a
silent, dedup-aware operation; the daemon stays in `WATCH` and the dashboard
continues to show `WATCH`.

## ERROR state details

ERROR is entered when any handler sets `state.state = PipelineState.ERROR` and
writes a context string to `state.error_message`. Unlike the transient states
that `_run_cycle_body` resets on each tick (`CODING`, `FIX`, `MERGE`), ERROR
is *not* auto-cleared. Each tick dispatches `handle_error`
(in `src/daemon/handlers/error.py`) when `daemon.error_handler_use_ai` is
enabled, and the state changes only based on the diagnosis outcome:

- **ERROR persists** when the context is classified as an infra/network
  failure, when the category is rate-limit or timeout, when no eligible
  diagnosis coder is available, when the per-context diagnosis budget
  (3 attempts) is exhausted, when the auto-fix push cannot post
  `@codex review`, when the diagnosis CLI exits non-zero, or when the
  verdict is `ESCALATE`.
- **Transition to `IDLE`** when the verdict is `FIX` (with the auto-fix
  applied and pushed) or `SKIP` (the active task/PR is cleared).
- **Transition to `PAUSED`** when the diagnosis CLI itself trips a
  rate-limit pause. `error_message` is preserved so `handle_paused` returns
  the runner to ERROR after the pause window.

For incident triage: a repo that remains in ERROR across multiple cycles is
expected behaviour rather than a stuck dispatcher, and the most recent
`diagnose_error` event log line records why the state did not clear
(`ESCALATE`, max attempts, infra skip, rate-limited diagnosis coder, etc.).
The legacy `_TRANSIENT_STATES` reset path does not apply to ERROR.

## WATCH STALLED substate (historical)

Earlier dashboard builds rendered a compound `WATCH STALLED` (and also
`IDLE STALLED`) when `data-updated-at` on a repo card aged beyond
`STALLED_THRESHOLD_MS = 30000`. Implementation lived entirely in
`src/web/templates/base.html` as a `.stalled` CSS class plus a
`syncStalledBadges()` IIFE polling on a 5-second interval — it was not a
daemon state and not a value of `PipelineState`.

The indicator was removed in PR-170. The decision (recorded in
`docs/roadmap.md`) was that healthy idleness — daemon between cycles, or a
WATCH cycle waiting on a slow human reviewer — was being misrendered as
"something is broken". Operators reported the false positive consistently:
the badge appeared seconds after a successful merge, or during normal review
wait windows, and produced no actionable signal.

What replaced it:

- **Stale review handling** moved into the daemon itself. PR-147 added the
  `_maybe_retrigger_stale_review` path described above, so a genuinely stuck
  review is unblocked automatically rather than flagged in the UI.
- **Daemon liveness** is now communicated through the pulsing dot on the
  state badge (active states pulse, terminal states do not) and through the
  event log timestamps. There is no compound state badge.

If a future operator searches the repo or older screenshots for
`WATCH STALLED` and finds nothing in `src/`, this is the explanation: it was
a UI artifact that was deleted by design, not a state that was renamed.
