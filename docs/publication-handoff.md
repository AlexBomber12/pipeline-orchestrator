# Coder publication handoff

A completed coder invocation and GitHub's view of its publication are separate
facts. AUTO PR and daemon FIX perform the implementation and publication attempt,
make one bounded observation, report expected and observed evidence, and exit.
Delayed PR synchronization is a normal handoff. An implementation, authorization,
guardrail, or process ownership failure still follows the escalation policy.
Neither free-form publication prose nor a local commit proves a successful push.

## Shared contract

`PublicationAttempt` binds repository, base, task, branch, invocation run and
attempt index, PR number when known, and expected commit. Version 2 freezes the
expected local branch commit and invocation outcome before cleanup or terminal
bookkeeping. Successful exit requires exit code zero without guardrail output or
an ESCALATE marker. Supervisor-confirmed cleanup is checkpointed separately.

The existing runner ticks reconcile that same attempt. They independently read
`origin` with `git ls-remote` and the exact PR API identity, including source
repository, base, head branch and SHA. No reconciliation poll creates a PR,
re-runs the coder, pushes, changes a remote ref, or consumes a no-push FIX retry.

| Evidence | Retained state | Next action | Boundary / outcome |
| --- | --- | --- | --- |
| Clean exit, expected branch SHA, older or unavailable PR head | Frozen expected SHA, observed branch and PR SHAs, invocation, cleanup, deadline | Poll the same operation | Exact convergence enters WATCH |
| Local commit only, incomplete discovery or API failure | Same evidence and unresolved boundary | Retry observation; never create a duplicate PR | Durable budget exhaustion parks this task ERROR |
| Branch advances beyond the expected SHA | Expected and observed identities | Preserve both actors' work; require exact intended head | Never accept an unrelated head or force the branch back |
| Implementation, authorization or guardrail failure | Original failure and publication evidence | Existing operator recovery | Publication cannot turn the failure into success |
| Cleanup proof missing | Invocation and process uncertainty | Existing owned-process cleanup / operator recovery | Never infer ownership from a published PR or an absent local handle |
| Operator Stop | Pause flag, attempt, counters and deadline | Defer polls and continuation effects until Play | Pause does not replenish the wall-clock budget |
| Exact intended PR externally closed or merged | Exact terminal evidence and confirmed cleanup | Existing terminal bookkeeping | No inference from disappearance or commit subjects |

Publication reconciliation atomically persists the operator pause before
consuming the global Stop key. The pause survives that key's expiry and daemon
restart; only Play clears it. A failed Redis transaction retains the Stop key
and blocks continuation.

When the FIX supervisor cancels a coder because the PR was externally merged
or closed, the exit checkpoint retains that cancellation reason. Confirmed
cleanup and exact terminal PR/branch evidence can then complete the existing
terminal bookkeeping. This cancellation permits no open-PR WATCH handoff or
review request, including after restart or operator Retry. Implementation,
Stop, breach and idle-timeout failures retain their existing classification.

When the existing safety checks release an interrupted unpublished attempt,
recovery still completes task ERROR bookkeeping before selecting queue work.
The bound task and invocation survive further restarts while header/provider
reads defer that bookkeeping. Unknown local work continues to block release.

The deadline uses the existing review timeout policy; the next poll uses the
existing FIX polling interval. Both are persisted before observation. Restarts
and pauses do not grant fresh budgets. GitHub budget admission still gates ticks.
No new configuration, public state, scheduler, or operator control is required.

Before releasing the publication hold, the daemon checkpoints push accounting
and its receipt together. FIX accounting uses the captured initial iteration;
replaying a checkpoint cannot increment it again. Run records retain their run
identity and terminal history. A completed CODING run remains finalized as
`superseded` / `coding_complete`. A review-triggered FIX admits a separate run
whose deterministic identity binds that predecessor and whose `attempt_index`
increments by one. Each record retains its own invocation snapshots; cumulative
FIX counters do not imply duplicate invocations in the predecessor. Successful
merge finalizes the successor as `merged` / `success_merged` for the exact PR
head proved merged, preserving the original CODING history. Provider recovery
keeps this same record/attempt contract rather than reopening an exited run.
Unavailable bound run accounting defers handoff;
terminal metrics use the frozen commit even if the local checkout later moves.
Review handoff checkpoints its start before POST;
a restart consults current-head request history before another POST. Unavailable
history defers confirmation. Existing review request cooldown and WATCH handling
remain responsible for retries after handoff. Remote POST and Redis cannot form
one transaction; deduplication depends on complete provider request history.

All pending attempts block final merge admission, including when the old head
has green CI and approval. After convergence the PR starts with pending review;
CI is hydrated through the canonical reader for the expected head. WATCH then
uses the existing publication freshness, CI provenance, workflow-attempt and
approval gates. Missing checks stay unresolved.

On exhaustion, the incident records expected SHA, observed branch SHA, observed
PR SHA and unresolved boundary in the durable cancellation cause. The daemon
writes this task ERROR before releasing its active slot for independent tasks.
A failed status write retains the slot and retries only bookkeeping. The task is
never marked DONE and its dependants are not released. Branches and commits are
preserved. Unrelated failure evidence cannot be cleared by reconciliation.
Operator Retry retains the expected-commit floor from the saved incident even
after independent work replaces the active attempt. It rechecks remote branch,
exact PR and open-PR discovery agreement before admission; an older green PR
view cannot bypass the floor. Missing identity or cleanup evidence requires
operator inspection rather than a new coder invocation.

## Interrupted provider attempts

A positively classified FIX provider capacity/service-unavailable error is not
quota exhaustion. Its failed exit checkpoints the frozen head, provider kind,
and next retry on the existing publication attempt. The run ends as paused;
each retry has a new run identity while retaining dispatch and attempt history.
Three consecutive provider interruptions exhaust automatic recovery. The existing
FIX polling interval supplies exponential backoff; the model and reasoning effort
remain bound to the interrupted invocation. Actual rate limits retain their
provider pause deadline. Provider interruptions do not spend the code-fix allowance.
The attempt also checkpoints the exact admitted review feedback and CI context,
including retrieved failure logs, before launching the coder. Recovery restores
that prompt after restart without fetching replacement evidence. Both original
branch and PR heads survive subsequent attempts, so a commit pushed before an
interruption still receives the normal current-head review request even when
the resumed coder only completes validation.

Before resuming, the runner requires bound cleanup/run evidence, the retained
local branch and head, and exact open-PR evidence. The remote may still contain
the initial head, the last exact published head owned by the recovery chain,
or the captured local commit; an independently changed head
parks recovery without rewriting either side. The resumed FIX keeps commits and
dirty changes, reruns unfinished validation, and uses normal publication and
review/CI gates after successful exit. A clean tree or an already-published commit
cannot convert a failed invocation into successful completion. Operator pause
continues to block automatic work. Expired quota markers are cleared before
interrupted-publication recovery so they cannot cause ERROR/PAUSED oscillation.
An exhausted ownership-observation budget records its exact provider-owned error.
Bound operator Retry rechecks all ownership evidence before clearing that error
and granting a new provider allowance. Independent failures remain blocking,
including when they occur after the observation park.
Changing the selected provider parks recovery without changing the retained
invocation. After the operator restores that provider, bound Retry revalidates
the selected provider and retained ownership before clearing the provider-change
error and granting a new allowance. Retry cannot authorize a different provider.
After provider exhaustion, operator Retry can reserve the existing bounded Retry
allowance and resume the retained FIX without a manual push. Admission uses the
same local/remote/PR and run-ownership proofs as automatic continuation. The
existing TODO status checkpoint precedes the handoff; only the provider allowance
is reset, leaving dispatched attempts, FIX counters, review/CI evidence, and the
unpublished checkout intact. A durable command receipt makes the allowance grant
idempotent across lost acknowledgements and restarts. The receipt follows the
retained FIX's subsequent attempts so replay can acknowledge an already-started
handoff without resetting its budget or executing CODING. Operator pause and quota
deadlines still gate execution.

Legacy snapshots lacking bound provider, cleanup, or accounting evidence remain
stably parked with the missing evidence and the existing Retry action identified.

## Compatibility and verification boundaries

Version 1 snapshots remain readable and keep their conservative recovery rules.
Historical ESCALATE records, including publication-related prose, are not
converted to success. A crash before an invocation outcome or cleanup proof was
checkpointed can lack enough evidence for safe automatic continuation; operator
recovery remains necessary. A source merge alone cannot repair those records.

Regression tests use disposable real Git repositories, controlled provider
responses, controlled time, serialization into fresh runners, and actual coder
exit paths. They cover delayed adoption, stale green gates, restarts, Stop,
external terminal races, conflicting refs, failures and bounded exhaustion. A
portable regression reproduces the pre-repair ERROR hold on the original base.
These tests do not prove real GitHub synchronization or production deployment.

Release verification must confirm the deployed source revision, exercise AUTO
PR and FIX against a disposable real provider repository, observe exact-head
review/CI admission and restart/Stop behavior, and check retained incident
handling. Production configuration, running services, credentials, live state
and existing PR branches are not changed by this source repair. Task completion
and DAG evidence repair (PR-520) remains separate.
