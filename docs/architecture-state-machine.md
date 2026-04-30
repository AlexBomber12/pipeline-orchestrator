# Architecture: pipeline state machine

This document describes the canonical pipeline states the daemon transitions
through for each PR, the meaning of each state, and the historical
`WATCH STALLED` compound rendering that the dashboard used to surface.

## Canonical states

`PipelineState` is defined in `src/models.py`. The dashboard renders one of
these values per repo at any given time; there are no compound states.

| State       | Meaning                                                                                                |
| ----------- | ------------------------------------------------------------------------------------------------------ |
| `PREFLIGHT` | Daemon is verifying repo prerequisites (clone, auth, queue parse) before accepting work.               |
| `IDLE`      | No active task. Daemon polls `tasks/QUEUE.md` plus GitHub for new work at `poll_interval_sec` cadence. |
| `CODING`    | Coder subprocess is running for a freshly selected task; daemon watches its stdout.                    |
| `WATCH`     | A PR is open. Daemon polls CI status, review state, and review comments until a verdict is reached.    |
| `FIX`       | Coder subprocess is running in `FIX FEEDBACK` mode against an existing PR branch.                      |
| `MERGE`     | A PR is approved and CI is green; daemon is performing the merge.                                      |
| `HUNG`      | Daemon detected a stuck WATCH cycle and is running the hung-recovery routine.                          |
| `ERROR`     | A bounded recoverable error occurred. Daemon transitions back to `IDLE` on the next tick.              |
| `PAUSED`    | Operator pressed Pause. No state advancement until Resume.                                             |

Operators see the canonical state in the repo card badge; semantic detail
(CI status, review status, last push age) appears in the PR panel below it.

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
