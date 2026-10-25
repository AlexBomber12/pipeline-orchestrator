# CI retry and recovery contract

The unit of work is a repository, task generation, PR number, HEAD, workflow run,
and workflow attempt. A newer observed attempt must survive JSON persistence and
restart. It invalidates older failure evidence even when GitHub later returns an
older cached payload. An observation does not consume a submission allowance.

## Rerun decisions

| Durable state | Observation | Required action |
| --- | --- | --- |
| No submitted intent | Complete failing check and matching current failed job | Persist intent and attempt before submitting within the existing allowance |
| No submitted intent | Incomplete or missing current checks | Observe saved targets; do not authorize a new request |
| Intent pending | Same, stale, unavailable, or mismatched workflow metadata | Retain intent; do not repeat the remote mutation |
| Intent pending | Bound attempt greater than the request attempt | Confirm progress and remove intent regardless of current check visibility or outcome |
| Target saved before metadata was available | New attempt proves the original failed job is obsolete | Record progress without submitting against the obsolete check |
| Process creation failed | Typed failure from the process creation boundary | Persist removal of intent and request attempt; allow the existing bounded retry |
| Process started | Timeout, I/O failure, nonzero exit, or lost response | Treat the remote outcome as unknown, preserving intent |
| Some workflows confirmed | Another target disappears from failing checks | Reconcile every remaining saved target independently |
| All targets confirmed | Fresh, bound terminal failure on a later attempt | Admit FIX only through current failure evidence |
| All targets confirmed | Queued, running, success, incomplete, or stale evidence | Do not manufacture a code failure |

`issued_runs` records both acknowledged submissions and independently observed
new attempts. The aggregate `issued` field is derived from per-workflow progress.
The persisted target set, rather than the current failing-check list, determines
which outstanding operations must be reconciled.

Unknown outcome is an explicit information limit. If the process may have sent a
request and GitHub never returns a reliable new observation, automatic replay
cannot guarantee both progress and absence of duplicate mutations. Keep the
intent and report the uncertainty. A timeout alone is not proof of non-execution.
This contract does not promise exactly-once delivery from a non-idempotent API.

Legacy intent without a recorded request attempt remains uncertain. A running
workflow with attempt 2 alone cannot prove that a request made during attempt 2
advanced to attempt 3. Confirmed intent requires advancement beyond its recorded
baseline; disappearance of jobs from the same attempt is insufficient.

## Executable coverage

The tests use the real WATCH and attempt readers with recorded-shape transport
responses. External effects and clock passage are controlled; the tests do not
need GitHub credentials or wall-clock sleeps.

| Boundary | Executable checks |
| --- | --- |
| Saved-operation reconciliation | `test_saved_rerun_reconciliation_matrix`: 5 check views × 5 workflow outcomes × restart/no restart × submitted/unsubmitted = 100 cases |
| Partial multi-workflow progress | `test_partial_rerun_reconciles_workflow_after_its_check_succeeds`: fresh/stale check and restart/no restart |
| Accepted request, lost acknowledgement | `test_accepted_rerun_reconciles_before_false_error`, with and without optional check attempt and delayed observations |
| Known non-execution | `test_spawn_failure_before_request_does_not_leave_permanent_ambiguous_intent`: temporary and persistent process creation failure |
| Process boundary | `test_gh_process_boundary_*`: real subprocess output/exit behavior, constructor failures, and cleanup after post-start exceptions |
| Interrupted aggregate write | `test_interrupted_final_issuance_marker_is_reconciled` and existing restart recovery tests |
| Monotonic attempt and immutable FIX provenance | `tests/test_ci_attempts.py`, `tests/runner/test_handle_fix.py` |
| Cleanup acknowledgement loss and continuation | `tests/runner/test_fix_idle_recovery.py`, `tests/runner/test_third_coder.py`, `tests/runner/test_recovery.py` |
| Terminal success | `tests/test_e2e_happy_path_oracle.py`: exact merged PR and DONE task, not only an idle daemon |

For future transition changes, add both a safety assertion (no obsolete action
or duplicate effect) and a progress assertion (a sufficient fresh observation
releases the wait). Exercise persistence at the side-effect boundary, then run
the existing related suites together. Do not replace terminal assertions with
intermediate state checks, or repair uncertain outcomes by increasing limits.

The final local gate remains `bash scripts/make-review-artifacts.sh`, including
100% source coverage. GitHub Actions owns the isolated Docker integration suite.
