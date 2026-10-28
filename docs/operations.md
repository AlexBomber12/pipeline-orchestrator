# Operations: runtime environment variables

Pipeline-orchestrator reads runtime connection settings such as
`REDIS_URL` and auth-directory paths from the environment. Recovery no
longer has rollout flags: daemon startup always reconstructs queue state
from structured `tasks/PR-*.md` headers.

## Read-only MCP diagnostics

The localhost-scoped Orchestrator MCP exposes `get_orchestrator_status` for a
compact overview of configured repositories. Pass a validated `owner__repo`
slug for structured pipeline, inhibitor, cancellation, pending Retry-command,
and run-record metadata. Results use explicit field allowlists and fixed status
codes: task text, error messages, exception text, arbitrary payload fields, and
other free-form producer content are not returned.

Redis values and indexes are read with fixed size and count bounds. Missing,
malformed, oversized, and unavailable sources are reported independently.
These reads do not refresh TTLs, prune stale index members, or mutate daemon
state. Snapshot age describes only the persisted `RepoState` observation; even
a fresh snapshot is not evidence that a coder process is alive or progressing.

Runtime diagnostics are available on the primary MCP service, whose published
port remains bound to localhost. The same service container runs two
streamable-HTTP listeners: the host port maps to the opted-in diagnostics
listener on container port 5174, while the optional `cloudflared` profile keeps
its existing `mcp:5173` target. With `MCP_REMOTE_DIAGNOSTICS=0` (the default),
the tunnel listener has no Redis access and does not register runtime
diagnostics, preserving its previous behavior.

Runtime diagnostics are opt-in at server startup. The MCP service entrypoint
sets `MCP_RUNTIME_DIAGNOSTICS=1` for the localhost-published listener. Operators
starting `python -m src.mcp` outside Compose must explicitly set that variable
and retain an equivalent loopback-only or authenticated access boundary.

`get_repo_events` reads the same retained `RepoState.history` entries shown in
the dashboard Event log for one configured `owner__repo` slug. It returns the
newest entries first, with event category, recorded state, timestamp, message,
and repetition count. Category is historical event metadata; it does not
describe the current task or pipeline state. The tool reports source freshness,
missing/malformed/oversized/unavailable sources, and truncation. It reads at
most a 256 KiB state snapshot and returns at most 20 events, 500 characters per
message, and 16 KiB of event data. Before export, each message is limited to
4,096 source characters; JSON escapes and supported terminal formatting are
normalized. Credential-bearing messages and kind metadata are omitted in full,
while event timestamps, category, state, and repetition count remain available.
Ambiguous or oversized text is also omitted. A bounded recognized timeout marker
is retained when available. Ordinary event text remains readable. Retention
follows the dashboard's capped history; this is not a live process or complete
audit stream. Reads do not write, refresh TTLs, or prune.

Raw CLI, CI, artifact, daemon-stdout, and live-stream retrieval is not exposed.

### Protected remote diagnostics with Cloudflare Access

Port 5173 can expose the same structured diagnostics through the existing
Cloudflare Tunnel target, `mcp:5173`. This mode is disabled by default. When it
is enabled, the origin authenticates every MCP HTTP request before MCP session
or tool dispatch. The local port 5174 listener remains loopback-only and does
not require Cloudflare headers.

Set all four variables in the Compose environment before enabling the mode:

```dotenv
MCP_REMOTE_DIAGNOSTICS=1
MCP_PUBLIC_HOSTNAME=mcp.example.com
MCP_CLOUDFLARE_ACCESS_ISSUER=https://example-team.cloudflareaccess.com
MCP_CLOUDFLARE_ACCESS_AUDIENCE=<64-character-application-AUD-tag>
```

`MCP_CLOUDFLARE_ACCESS_ISSUER` is the Cloudflare One team domain shown under
Zero Trust **Settings > Custom Pages > Team domain**. Use the exact HTTPS
issuer, without a trailing slash, path, or port. Copy the immutable Application
Audience (AUD) tag from **Access controls > Applications > Configure >
Additional settings** into `MCP_CLOUDFLARE_ACCESS_AUDIENCE`.
`MCP_PUBLIC_HOSTNAME` is the public DNS hostname routed by the existing tunnel
to `http://mcp:5173`; it is also the explicit host/origin allowlist used by the
MCP transport safeguards. Startup fails before binding port 5173 if protected
mode is requested with missing or malformed values. Do not put these settings
in the `cloudflared`-only `.cloudflared.env` unless they are also provided to
the `mcp` service; Compose interpolation normally reads them from the shell or
the project `.env` file.

#### Cloudflare and ChatGPT setup

1. Create or edit the Cloudflare Access application for the MCP public
   hostname. Keep the tunnel service target as `http://mcp:5173`. Restrict its
   Allow policy to the intended operator identity; do not use a broad email
   domain, Everyone rule, service token, or bypass rule for this interactive
   connection.
2. Under the Access application's Advanced settings, enable **Managed OAuth**.
   Cloudflare then owns OAuth discovery, authorization, code exchange, token
   refresh, and policy enforcement. It publishes discovery below the public
   application hostname and returns the appropriate `401` challenge at the
   edge. This service does not publish a competing OAuth server or scopes.
3. In Managed OAuth dynamic-client settings, allow the exact production
   redirect URI displayed by ChatGPT when the MCP connection is created.
   Current ChatGPT clients prefer Client ID Metadata Documents when the
   authorization server supports them and otherwise use dynamic client
   registration. The stable current client metadata URL is
   `https://chatgpt.com/oauth/client.json`, and the stable redirect is
   `https://chatgpt.com/connector_platform_oauth_redirect` only when the
   authorization server advertises and returns RFC 9207 issuer identification.
   Otherwise ChatGPT displays callback-specific URLs under
   `https://chatgpt.com/oauth/{callback_id}/client.json` and
   `https://chatgpt.com/connector/oauth/{callback_id}`. Treat the values shown
   in the connection management page as authoritative and allowlist those exact
   values; do not guess a callback ID or advertise invented scopes.
4. Use a short Managed OAuth access-token lifetime (Cloudflare recommends
   5–15 minutes for agents) and a longer grant session as appropriate. Save the
   Access application, then deploy the MCP configuration and recreate the
   `mcp` container. Do not change credentials in the repository.
5. Add or refresh the MCP connection in ChatGPT using
   `https://mcp.example.com/mcp`. If the connection predates the authentication
   change, disconnect and reconnect it so discovery and client registration run
   again. Complete the browser sign-in as the intended operator.

The edge-to-origin flow has two distinct credentials. ChatGPT receives an
opaque Managed OAuth access token and sends it to Cloudflare; it is not a JWT
and this origin never decodes it. Cloudflare validates that token, reapplies the
Access policy, and forwards a signed `Cf-Access-Jwt-Assertion` header. The MCP
origin accepts only that header, verifies its RS256 signature against keys from
`<issuer>/cdn-cgi/access/certs`, and checks the configured issuer, audience,
expiration, and other supplied time claims on every request. Cookies, identity
headers, bearer-token presence, decoded-but-unverified claims, and MCP session
IDs never grant access.

#### Verification and rollback

After deployment, verify these separately:

- Direct requests to the origin or public `/mcp` endpoint without a valid
  Access login fail with a fixed authentication response and expose no tool
  data. A previously issued MCP session ID must fail in the same way without a
  fresh assertion.
- An allowed operator can initialize the streamable-HTTP connection, discover
  only the existing allowlisted tools, and call `get_orchestrator_status` and
  `get_repo_events` for a configured repository. Confirm that no raw log,
  artifact, credential, arbitrary-file, or mutation tool appears.
- A denied identity cannot complete the Access policy. Rotate the Access
  signing key in a test application, reconnect, and confirm the origin accepts
  the new key after its bounded refresh without accepting the old application
  audience.

Code deployment alone prepares the origin; it does not prove a live ChatGPT
connection. Record live verification only after the browser authorization,
connection initialization, tool discovery, and diagnostics call all succeed
through the production hostname.

To roll back, set `MCP_REMOTE_DIAGNOSTICS=0` and recreate the `mcp` container.
This immediately returns port 5173 to the restricted, Redis-free tool set while
leaving local diagnostics on port 5174 available. Then disable Managed OAuth or
the Access application only if the public non-diagnostic MCP endpoint should no
longer be reachable. Disconnect or refresh the ChatGPT connection after either
change.

Reference material: [Cloudflare Managed OAuth](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/http-apps/managed-oauth/),
[Cloudflare Access JWT validation](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/http-apps/authorization-cookie/validating-json/),
and [OpenAI plugin authentication](https://developers.openai.com/plugins/build/auth).

## Task format migration

Task files are migrating from legacy status headers to explicit YAML
frontmatter. The offline operator script is dry-run by default:

```bash
python3 scripts/migrate_task_format.py --repo /data/repos/AlexBomber12__pipeline-orchestrator
```

Review the per-file status output, then apply the migration per managed
repository:

```bash
python3 scripts/migrate_task_format.py --repo /data/repos/AlexBomber12__pipeline-orchestrator --apply
python3 scripts/migrate_task_format.py --repo /data/repos/AlexBomber12__pipeline-orchestrator --verify
```

Repeat the same dry-run, apply, and verify sequence for
`megaraid-dashboard` and `sms-gateway-v2`. Apply mode writes backups
under `artifacts/task-format-backups/<timestamp>/` and prints the exact
backup path. Do not ship the legacy parser removal until `--verify`
passes on every repository.

## Recovery from ERROR

Diff size is a review signal, not a guardrail decision. The daemon logs the
size once per scanned PR HEAD and continues through the normal CI, Codex
review, FIX, and MERGE gates. Existing `daemon.large_diff_addition_threshold`
and `daemon.large_diff_files_threshold` configuration values remain accepted;
they now control only when that informational event appears. Deployments do
not need to remove their existing settings.

A legacy `large_diff_threshold` cancellation on a bound, open PR is
reconciled through the existing validated approval transaction. The daemon
checks the task generation, live PR identity and HEAD, checkout work, process
ownership, and any newer failure before clearing only that obsolete decision.
A pending artificial `status: ERROR` header is restored to `TODO`; the matching
published PR then derives DOING on restart. A failed status write remains
retryable through the approval receipt. The historical run HEAD is telemetry:
it may differ from the live published PR HEAD after a later push.

New CODING and FIX attempts use the ordinary lifecycle regardless of actual
diff size or differences from planned estimates. They do not write size labels,
artificial ERROR/TODO pairs, or size completion receipts. The daemon recognizes
only the former exact `size-ok:<40-hex-SHA>`, `size-fix:<40-hex-SHA>`, and
`quarantine:large_diff` labels. It removes them from the bound PR when it can;
GitHub label cleanup failure cannot hold FIX, WATCH, or MERGE. Malformed and
unrelated labels are left alone. A label alone never proves coder completion
or authorizes clearing an ERROR.

An old persisted size FIX completion receipt can reconcile a pending artificial
ERROR header after restart. The receipt must match the task generation,
invocation and run accounting, live PR identity and HEAD, exact old header
revision, and a quiescent checkout, with no newer cancellation or suppression.
The label may already have been removed. Missing or corrupt evidence leaves the
published PR and real failure parked for operator recovery; no coder is
redispatched on the strength of a label. These compatibility readers can be
removed after all deployments have crossed a quiescent upgrade boundary and
no retained task or Redis state contains these legacy artifacts.

Before upgrading a deployment with an active legacy FIX, pause new dispatch
and let the old invocation finish and its pending completion reconcile. Confirm
that no coder or child process owns the checkout and that the task header and
published PR agree. Upgrade at that quiescent boundary, then resume. If the
invocation outcome cannot be established, preserve the checkout, task, and PR
for the existing uncertainty/Retry procedure; an arbitrary restart during an
active invocation is not a safe migration step.

An externally merged PR remains in WATCH until its DONE task-status write is
confirmed; a transient write failure is retried on the next WATCH cycle.
Startup recovery also checks the checkout for unpublished work and active
processes. It fetches a missing verified PR HEAD without moving local refs;
unavailable evidence leaves recovery pending for the next cycle.
Unavailable GitHub evidence or unpublished task work leaves the PR parked
with a specific recovery reason; no branch synchronization is performed.

When a task ships its terminal failure state to ERROR (frontmatter
status:ERROR), the operator has two recovery affordances:

1. **Retry button** — clears the cancellation_cause record + frontmatter
   status, daemon re-dispatches the spec from the top on the next IDLE
   cycle. Retry counter capped by `DaemonConfig.retry_button_cap`
   (default 3, configurable via `daemon.retry_button_cap` in `config.yml`)
   in Redis (resets on file content change). Deployments that override
   the cap will enforce that configured value, not the default.

2. **Re-upload spec with changed content** — file content hash differs
   from stored hash → daemon treats as fresh task, cancellation_cause
   cleared, retry counter reset.

Both affordances are mutually exclusive: Retry is for unchanged content
("try again, environment may have transient issue"); re-upload is for
changed content ("operator iterated on the spec itself").

## Interrupted merge accounting

Merge reconciliation proves the exact remote PR outcome before bookkeeping.
The task on authoritative base, the PR head, and its merge commit must have the
same normalized specification fingerprint; the merge commit must remain in base.
Saved invocation IDs and task/repository equality alone do not admit a run.
Compatible unfinished accounting can retain an earlier ancestor HEAD after fixes
or base synchronization. Terminal failed attempts retain their original history.

If no compatible accounting exists, recovery checkpoints a separate record whose
identity is deterministic for the repository, base, PR, head, and generation.
The isolated status writer rechecks the remote generation during publication and
confirms the pushed commit before accounting is finalized. Repeated recovery
preserves finalized metrics and the durable outcome logger deduplicates appends.

A `Merge accounting conflict:` error parks reconciliation for operator Retry;
it does not write DONE or ERROR into a conflicting task generation. Provider,
Git, and Redis outages retain MERGE ownership and retry on a later cycle.
Retry for a retained accounting conflict revalidates its exact PR and task
generation, then resumes post-merge bookkeeping without dispatching a coder or
rewriting TODO. Both ERROR dispatchers retain the park until operator action.
The existing HTTP Retry action admits a bound accounting conflict while retaining
its header and normal task validation. Recovery preserves the saved merged PR
identity even if an open PR reuses that task branch.
Operator pause and Stop controls still apply. Resolving a conflict requires
reviewing the exact PR and current task generation before using existing Retry
or task-upload controls; this source change does not deploy or resume a daemon.

## WorkInhibitor rollback

The WorkInhibitor cutover is complete: `use_unified_inhibitor_check`
defaults to `true`, so repositories use the unified
`src.inhibitor.is_work_inhibited` path unless they opt out.

If a regression affects one repository, keep the daemon-wide default on
and add a per-repo override to that repository's entry in `config.yml`:

```yaml
feature_flags:
  use_unified_inhibitor_check: false
```

Do not put this rollback override in `user_state.yml`; the current
runtime config loader does not read that file. Reload the daemon config
through the normal inotify path, or restart the daemon container. Verify
the rollback by checking the dashboard event log for legacy throttle
decisions on the affected repository.

## Required Check Adoption

`RepoConfig.required_checks` lets a repository declare the exact GitHub
check context names required before merge. Names are exact-match strings;
for example, the tracked pipeline-orchestrator example declares `unit`
and `integration`.

Repositories without `required_checks` keep the existing weaker fallback:
the daemon observes the contexts GitHub reports for the current PR instead
of inventing required contexts from unrelated repositories. Preserved
server configs and production overlays must opt in explicitly during the
final production update; this repository change only updates committed
examples and validation.

CI-driven FIX admits failed GitHub Actions jobs from the current PR HEAD.
For commit statuses posted by another trusted producer, configure
`ci_fix_trusted_statuses` as context names mapped to explicit producer lists,
for example `{"external/gate": ["app:12345", "user:ci-app[bot]"]}`.
This policy controls coder admission only; it does not change required
checks or merge eligibility. Unconfigured status producers remain visible
as CI failures but do not launch a coder.
When GitHub's combined-status response omits a configured status producer,
the daemon binds that status by ID, context, state, and timestamp to the
complete status-history response for the same commit before admission.

## Automatic review replacement investigation (PR-406)

The managed Codex Review gate in `AGENTS.md` (read by
`src/onboarding/agents_md_template.py`) and the starter `templates/AGENTS.md`
describe the delivered stable-actor/current-change rules. Trust uses configured
`daemon.trusted_reviewer_identities` numeric GitHub IDs; logins and substrings
are not trust evidence. Reaction freshness needs proven publication of the
current full HEAD, including same-SHA republication. Unknown provenance remains
pending. The reader also supports explicitly commit-bound successful verdicts;
this repository still requires a current-change trusted `+1` before merge.
A receipt, mutable summary or fresh request cannot supply missing publication or
invocation linkage. AUTO PR and daemon FIX coders publish once and exit; the
daemon owns later review requests and waiting. Manual coders retain their review
loop and post the exact body `@codex review`. These instructions do not change
runtime policy or authorize an automatic migration protocol.

Result: **insufficient linkage; no automatic replacement is authorized**. Reusing
an existing request and posting a fresh exact `@codex review` both leave missing
publication provenance unknown. No runtime policy, review gate, Redis state,
operator confirmation, or migration code changes in this investigation.

### Recorded evidence and replay boundary

`tests/fixtures/review_provenance_cases.json` records selected public #620 REST
responses read on 2026-10-26: issue comments, body reactions, PR head projection,
and repository activity. User objects retain only numeric ID and login. Comment
IDs, timestamps, full SHAs, hidden connector metadata, and reaction identity are
retained. This is read-only evidence collection, not a live review experiment.
Sources are the repository's `/issues/620/comments`, `/issues/620/reactions`,
`/pulls/620`, and `/activity?ref=refs%2Fheads%2Fpr-459-e2e-failure-evidence`
REST endpoints. Existing `review_receipt_620.json`,
`review_summary_structured.json`, and `test_activity_api_url_syntax.py` remain
unchanged and supply the prior sanitized receipt/summary and activity contracts.

The component replay calls production `get_pr_head_publication`,
`get_pr_review_status`, `latest_codex_review_request_time`, and `post_comment`
with controlled external transport. It exercises the publication metadata reader
from #622, reused by #623's Retry/WATCH admission. It does not simulate a daemon
migration confirmation or claim that a reader verdict initializes a safety floor.
The recorded deletion is excluded only for a labeled pre-merge control; the
fixture retains it. Replaying today's complete activity would report deletion,
not a current publication.

The positive control is actual #620 data: full HEAD
`bd7bd1ea89766790981bd3f8749d2c407132db38`, push at
`2026-10-19T18:56:08Z`, exact request comment `6274859306` at `18:56:12Z`,
summary `6241079517` updated at `18:58:30Z`, and trusted actor ID `199175422`
body reaction `580423677` at `18:58:33Z`. With known publication the existing
reader returns APPROVED. This proves the existing freshness contract only.
The actual branch-creation event at `2026-10-17T21:06:35Z` proves full-SHA/ref/
head-repository publication metadata; the recorded abbreviated receipt alone
returns PENDING. Neither control proves an automatic trust migration.

### Supported and unsupported variants

| Evidence or replay | Observed result / limit |
| --- | --- |
| Known current publication plus recorded trusted body +1 | Existing review reader APPROVED; no request/invocation identity in the reaction. |
| Recorded `branch_creation` with full SHA, ref, head repository, timestamp | Publication reader returns `github-activity:branch_creation`; historical safety-floor initialization still requires existing admission policy. |
| Reuse latest exact request with missing activity | PENDING; discovering a request timestamp supplies no publication boundary. |
| Fresh exact request with missing activity | PENDING; successful posting supplies no provider review-result linkage. |
| Same SHA repeated requests | Recorded comments `6274750173` and `6274764254` precede receipt `6274795896` for `b40534d8f4`; no receipt field names either request. Another same-SHA request cannot identify the invocation. |
| Same SHA republished later (synthetic negative) | Later activity resets freshness; the old +1 is PENDING even though SHA is unchanged. |
| Old body +1 with new mutable full-SHA summary (synthetic negative) | PENDING, with and without known publication. Updating the summary cannot renew the reaction timestamp. |
| HEAD changes during post (injected race) | Re-read with expected full SHA reports `observed PR HEAD changed`; PENDING. A successful comment write is not a HEAD compare-and-swap. |
| Accepted post with lost response (injected failure) | Posting raises; exact comment is discoverable, but publication stays unknown/PENDING. Readback cannot identify which competing identical write was accepted. |
| Unknown numeric actor ID (synthetic negative) | PENDING even with known publication; a familiar login cannot substitute for trusted identity. |
| Abbreviated receipt with two possible full-SHA candidates (synthetic negative) | PENDING; the receipt classifier is not a full-SHA or invocation resolver. |

The structured summary supplies a full `commit_sha`, completion status and finding
flags, but its stable comment ID spans updates (`created_at` on October 17,
`updated_at` on October 19). Its current bytes do not retain earlier versions or
a request ID. The receipt supplies an abbreviated reviewed commit. The body
reaction supplies user, content, ID and creation time, without SHA, request ID,
run ID or summary-version binding. Joining those records by actor, chronology
and SHA cannot distinguish repeated reviews of the same SHA. A reaction on a
specific anchor would identify its comment target, but no positive recorded
anchor +1 is supplied here; that variant remains unproved, including its binding
to the full HEAD and a recovery invocation. No synthetic positive is admitted.

### Missing delta, operator panel and bounded follow-up

No operator panel behavior can safely disappear on this evidence. Retain the
exact publication migration proposal/confirmation, unknown-evidence blocking,
Retry behavior, and normal Approve semantics. A fresh request does not replace
confirmation. This result is not approval to issue requests or confirm trust
without the operator under current policy.

Minimum additional provider evidence is a durable, verifiable association of a
trusted +1 with the exact repository/PR, full reviewed HEAD, and particular
request/invocation (including a version if a mutable summary participates).
The association must survive same-SHA retries, reordered responses, accepted
writes with lost responses, and concurrent HEAD changes. The observed endpoints
do not deliver that association. Timestamp comparison alone cannot fill it.
An anchor-target reaction could be investigated only with an actual supported
recording and explicit full-HEAD/invocation evidence; no undocumented connector
field or future provider behavior should be assumed.

Follow-up scope: one evidence-only task, estimated 150–250 test/fixture lines and
60–100 documentation lines, zero production changes, to replay an operator-
supplied supported recording carrying that missing association. No live provider
experiment is implied. If it proves linkage, separately specify and estimate a
protocol for durable request identity, write reconciliation, HEAD revalidation,
restart safety and atomic state admission before implementation. A reliable
runtime implementation estimate is premature until that evidence exists.
Potential replacement paths are recovery's `_initialize_publication_admission`,
`_offer_publication_migration` and `_consume_publication_migration_command`, the
existing `src/publication_migration.py` command/receipt path, and the dashboard's
confirmation route/panel. None is removed now; ordinary watermark validation,
CI gates, trusted review checks and process ownership rules remain required.

Verification: `python -m pytest tests/test_github_client.py -q`. Before generating
review artifacts, ensure the checkout has the review base (`origin/main` for
this repository); fetch it with `git fetch origin main` if needed. After the
intended commit, run `bash scripts/make-review-artifacts.sh` and confirm that
`artifacts/ci.log`, `artifacts/structure.txt`, and a nonempty `artifacts/pr.patch`
exist. A focused test pass alone does not establish a successful full gate.
Controlled
transport proves reader behavior only, not real account access, provider delivery
guarantees, deployment, or restart behavior of a future protocol. Any eventual
provider smoke requires a separate operator release action; this investigation
has no deployment prerequisite and performs no production operation.

## Work preservation decision and restore proof (PR-410)

Audit base: `e92487e28ad735ae510c244d7e1f6a4b97f3b421` (current main).
This is an investigation, with zero runtime changes. Existing destructive paths
below remain hazards until their owners migrate them. Nothing here authorizes
cleanup of unknown directories, origin mismatches, or an unconfirmed process.
Traceability: IMP-01, A14/A16; historical specifications at
`7167c2e916a2bbb843ecb6a6ce7fc15795e61802` are not implementation dependencies.

### Decision and observed restore

Reuse `src/daemon/backups.py:create_repo_bundle`, which creates and verifies a
self-contained `git bundle create --all`. Do not create another backup service,
scheduler, retention policy or generic snapshot platform. Existing IDLE periodic
bundles are useful history backups, but their interval and optional configuration
cannot authorize an imminent destructive operation. A fresh verified capture
would be mandatory at each actual loss boundary.

`tests/test_work_preservation_contract.py` uses the existing real-Git initializer
from `tests/test_backups.py`, the production bundle API, and two disposable Git
repositories per case. Both attached and detached local-only commits are tested.
The source clone is deleted only after capture, before restoring into a separate
new repository. No alternates, source object directory, credentials or remote
service is used. A remote-tracking ref models the last published commit; this
proves local Git semantics, not provider push or recovery policy.

| Captured state | Bundle alone | Bundle plus minimum missing capture |
| --- | --- | --- |
| Local branch, tag, remote-tracking refs | Exact `show-ref` restored | Same exact refs |
| Unpushed branch commit / detached HEAD commit | Full SHA and committed file bytes recovered; bundle advertises `HEAD` even detached | Original raw HEAD restored, including attached vs detached identity |
| `README.md` staged `staged version\x00\xfe\n`, worktree `unstaged version\x00\xff\n` | Committed `hello\n` only | Both versions recovered independently |
| Index-only `staged-only.bin`, absent in worktree | Blob absent (`cat-file -e` fails) | Exact `index only\x00\xff` blob restored from object pack |
| Staged deletion, staged rename with newline in filename, unstaged deletion | Original committed files return | Index stages/modes/OIDs/path bytes and worktree absences match |
| Untracked binary `bytes(range(256))`, ignored binary, executable | Absent | Exact file bytes and permission bits match |
| Raw index, cached/uncached binary diffs, porcelain `-z` status | Not recovered | Raw index bytes match before Git refresh; all semantic comparisons match afterward |

The minimum missing capture demonstrated here is raw ordinary `.git/index`, a
`git pack-objects --stdout` pack of its indexed blob OIDs, raw `.git/HEAD`, and a
regular-file worktree inventory containing bytes and modes. NUL-delimited index
records avoid filename splitting. Restoring an exact inventory also restores
deletions; copying files over an existing checkout would leave unwanted files.
The index pack is necessary: the raw index points at objects that may never have
been committed. Refs and detached HEAD objects come from the verified bundle.
Restore fetches `+refs/*:refs/*` into an unborn placeholder branch before checking
out the saved HEAD; fetching into an already checked-out target branch is refused
by Git. It then imports the index object pack and restores inventory/index/HEAD.
The helper is test-only and keeps extra capture bytes in memory; it is not a
crash-durable writer, consistency protocol, or production restore command.

Existing recovery `_preserve_crashed_run_commits` and its `crash-backup/` fallback
push keep named branch commits on origin. They do not capture dirty files or the
index, nor arbitrary detached work. Keep this publication mechanism for its
existing purpose. A stash would mutate the source index/worktree and ordinary
stash omits ignored files; it is not the chosen capture primitive. A Git patch
alone does not preserve untracked bytes, refs or all index objects.

### Caller inventory and migration checks

Locations refer to `src/`; owner means the proposed existing component boundary,
not a new service. Reset can discard tracked/index versions and move the current
branch tip; force checkout can discard conflicting work; `clean -fd` deletes
nonignored untracked paths; `rmtree` loses all bytes including ignored files and
local object storage. Plain checkout/path checkout can also overwrite selected
bytes and must be included in the enclosing operation's checks.

| Caller / proposed owner | Loss set and required migration check |
| --- | --- |
| `daemon/runner.py:_commit_status_to_git` (status owner, PR-398) | Already isolated temporary repo; force detached checkout retries affect only its own generated status edits. Reuse isolation and existing identity/hash/retry tests; no coder snapshot. |
| `daemon/retry_commands.py:_remote_frontmatter_error_status_for_current_task` (Retry reader) | Isolated temporary fetch/checkout; no coder bytes affected. Retain origin/task identity validation and fixture-owned teardown. |
| `daemon/repo_ops.py:sync_to_main`, called by `handlers/idle.py` before/after upload processing (repo sync owner) | Shared checkout/reset/clean loses index, tracked and untracked bytes and can move local base ref. Require identity, quiescence and complete verified capture before first mutation; restore dirty base and local-only branch cases. IDLE alone proves no safety. |
| `daemon/preflight.py:_auto_reset_dirty_tree` (preflight owner) | Forced branch switch/reset/clean can lose all dirty versions and base tip. Capture before checkout; test dirty feature branch and preserved base/feature refs. Existing bounded recovery policy does not prove no loss. |
| `daemon/repo_ops.py:process_pending_uploads` failure reset (upload consumer) | Reset may discard pre-existing edits and an unpushed upload commit. Keep `_safe=True` recovery behavior; prefer isolated upload commit/push using status pattern, test failed push and concurrent manifest. |
| `daemon/scaffolder.py:ensure_claude_md` initial/failure resets (scaffolder owner) | Clean-tree checks do not rule out local commits. Prefer isolated backfill; test local-ahead base and rejected push before removing both resets. |
| `daemon/handlers/merge.py:handle_merge` pre-merge reset (merge owner) | Dirty/index state and unpublished branch tip; capture before checkout/reset or isolate merge. Test dirty branch and unpublished tip, preserve confirmed-merge accounting. |
| `daemon/handlers/fix.py:handle_fix` approved reset (FIX owner) | Existing approval/process/bound-ref checks reduce loss; retain them. Test mutation between checks and reset, dirty state and local-only HEAD; block or capture before mutation. |
| `daemon/handlers/error.py:diagnose_error` three reset/clean paths (diagnostic owner) | Reverting diagnostic/coder edits can also discard pre-existing dirty/untracked work. Capture before diagnostic execution, distinguish attempt-owned output, test every failure/skip/branch path. |
| `daemon/recovery.py:recover_state` and `_preserve_crashed_run_commits` (recovery owner) | Named-branch push is existing preservation, not a reset. Keep failure blocking; test dirty/index/detached state before downstream preflight/scaffold/sync can mutate. No second status snapshot. |
| `web/routes/repo_control.py:_checkout_retry_base_task`, `_reset_retry_worktree`, called by `reset_task` (web reset owner) | Forced checkout/reset/path checkout and rollback touch shared checkout. Replace checkout mutation with daemon command/isolated status pattern; test failed push, dirty bytes and restart. Web must remain a control plane. |
| `daemon/runner.py:RepoRunner.__init__` non-Git and wrong-origin `rmtree` (constructor owner) | Entire directory, potentially unrelated user data. Remove deletion in follow-up; block with diagnostic on unknown directory/origin mismatch. Test sentinel bytes survive both cases; do not snapshot as permission to delete. Legacy verified-origin move is distinct. |
| `daemon/repo_ops.py:ensure_repo_cloned` retry `_do_clone` rmtree (clone owner) | A directory can appear after initial absence. Clone into a uniquely owned temporary sibling and publish only to an absent destination; test foreign directory appearing between retries. Never infer ownership from path/name. |
| `daemon/main.py:_clean_breach_dir` (marker owner) | Deletes marker tree, not Git work. Require reserved marker-root ownership and symlink handling; no Git capture. Test unexpected directory retained and owned stale markers removed. |
| `daemon/repo_ops.py:process_pending_uploads` two staging rmtrees; `web/routes/uploads.py:upload_tasks` finally cleanup (upload owners) | Uploaded task source bytes, not necessarily Git objects. Require exact staging ownership and manifest CAS/receipt before deletion; test failed publication and newer manifest remain recoverable. |
| `web/services/upload_validation.py:sweep_abandoned_staging` (staging sweeper owner) | Age plus absence from active manifest alone does not prove disposable bytes. Require owned staging identity and durable consumed/abandoned disposition; retain unknown directories and active uploads in race tests. |

The inventory covers executable force checkout/reset/clean/rmtree sites found by
searching source, plus status, recovery and sync callers. GitHub CLI `-f` form
fields are not force checkout. TemporaryDirectory teardown of isolated Git
repositories is fixture/operation-owned disposal, not shared-checkout cleanup.
Ordinary coding checkout and scaffolder checkout/add/commit are still mutations;
their enclosing admission must protect work, even without a forced reset.

### Unsupported states and required blocking behavior

The tested envelope is a quiescent, ordinary non-bare full repository with an
ordinary complete index and regular files on Linux. Submodules (gitlinks and
nested repositories), LFS payloads/filters, symlinks, special files, empty directory
identity, ACLs/xattrs, hardlink relationships, reflog-only/dangling objects,
linked worktrees/common Git dirs, alternate object stores, shallow/partial clones,
sparse checkout/index, split index, intent-to-add, unmerged stages, index flags
and extensions beyond the tested ordinary index are unproved. Block these states
in a first implementation; do not imply bundle verification includes LFS media,
submodule objects or dangling history. Config, hooks, credentials and reflogs
are not reconstructed. Do not copy credential-bearing Git config into captures.
Unknown/non-Git directories and origin mismatch must be retained and blocked,
not deleted; the test proves only that bundle failure leaves unknown bytes alone,
not that the current constructor has been repaired.

Disk full, read error, missing indexed object, verification failure or concurrent
writer must block destructive work. Existing bundle failure tests cover mkdir,
I/O, timeout and verify failures; the new test covers a real non-Git read failure.
Production still needs an exclusive ownership/admission boundary, process
quiescence checks, before/after HEAD/refs/index/file identity validation, durable
external storage and atomic completed-capture publication. Check free space and
bound size/time, hash and re-read stored payloads, verify bundle/pack and restore
manifest, and leave source untouched on any failure. A digest comparison alone
cannot make concurrent writers safe. No such runtime protocol is proved here.

### Bounded follow-up and removal plan

First: constructor/clone ownership blocking, estimated 50–90 production and
100–160 test lines across existing runner/repo_ops modules/tests. No deletion of
unknown or mismatched directories and no automatic process termination.
Second: ordinary-repo capture/restore within existing backups boundary, estimated
180–280 production and 240–360 test lines, with unsupported-state rejection,
durable publication, consistency/failure tests and separate-directory restore.
Third: migrate shared preflight/sync/error/FIX/merge boundaries, estimated
100–180 production and 180–300 test lines; replace direct destructive chains
with guarded admission using that capture, preserving all existing policy checks.
Fourth: isolate upload/scaffold/web status mutations and tighten staging/marker
ownership, estimated 140–240 production and 200–320 test lines. These are planning
ranges requiring caller review, not approval to implement all work together.

Obsolete logic to replace is the constructor's destructive stale-directory
repair, clone retry's unconditional removal, shared status/upload/scaffold resets,
and unguarded reset/clean chains listed above. Keep verified bundles, crash
commit publication, PR-398 status isolation and existing approval/process guards.
This PR removes no runtime logic and changes no backup retention. The prototype
proves recoverable bytes/index/refs only; failure admission, real provider access,
deployment and process races require follow-up verification. No deployment is
needed for this investigation; eventual rollout needs an operator-run storage,
restore and ownership smoke, without production failure drills here.

Verification: `python -m pytest tests/test_work_preservation_contract.py -q`;
`python -m pytest tests/test_backups.py -q`; final gate and review artifacts via
`bash scripts/make-review-artifacts.sh` after the intended commit.

## Cleanup lifetime evidence (PR-408)

This inventory describes the implementation at base
`21e93658df99edad759ca77a6c0b0ae21682de89`. It reuses the delivered supervisor;
it does not implement restart ownership or change process control. Historical
scope references PR-416/419/420/421/422/454 refer to the retired specifications
at `7167c2e916a2bbb843ecb6a6ce7fc15795e61802`, not current task dependencies.

### Producers and consumers

| Boundary | Ownership produced or consumed | Remaining delta |
| --- | --- | --- |
| `process_supervisor.launch_process` / `launch_pty_process` | Dedicated session/group, leader birth token, internal same-session witness with pidfd/control pipe, known member birth tokens; constructor requires launch proof | These are live daemon objects/descriptors, not a durable restart receipt. |
| Supervisor witness launcher | Forks witness before target exec; witness survives ordinary leader exit and exits on control-pipe EOF | Daemon death closes the pipe; witness exit alone does not terminate the workload. |
| `claude_cli.run_claude_async` / `codex_cli.run_codex_async` | Optional bwrap command from `daemon/sandbox.py`, then supervised async launch; raw and supervised callbacks go through `run_supervised_process` | Wrapper PID/exit and pipe EOF alone are insufficient; no proof for descendants escaping the owned session/group or crossing containment boundaries. |
| Legacy synchronous `claude_cli.run_claude` and prompt wrappers | `subprocess.run` with timeout, optionally bwrap; no supervised ownership callback | Not the built-in daemon async coder path; no group-quiescence result. A future migration would replace this invocation boundary, not add a second supervisor. |
| `coder_auth.isolated_auth_probe` / `terminate_plugin_worker` | Raw new-session worker; timeout/cancellation kills numeric process group and awaits leader | No supervisor witness or retained failure receipt; replace launch/termination with existing supervisor in a separately scoped migration. Ordinary worker exit does not prove descendant quiescence. |
| `coders/codex_models.discover_codex_models` / `_close_process` | Raw app-server child; closes stdin, kills leader, communicates in finally | No dedicated owned group or descendant cleanup proof; migrate this producer/consumer pair separately. |
| `run_supervised_process` | Leader returncode, group cleanup, then bounded reader settlement; structured supervision failure retained on handle | A tuple result is not independently a process ownership receipt. Launch errors before callback publication are flattened by CLI adapters; runner conservatively retains a failure detail. |
| Runner `_track_current_coder_process` / `_track_current_coder_supervised_process` | Retain matching handles; reject replacement while outstanding | No rehydration from serialized invocation metadata. |
| CODING and FIX handler `finally` paths | `_record_unconfirmed_launch_cleanup`, then `_confirm_current_coder_cleanup` before lifecycle/publication handling | Require actual cleanup success; leave handles and ERROR park on failure. |
| Runner Stop monitor; FIX idle and GitHub-terminal pollers in `fix_supervision.py` | `_terminate_current_coder` before cancelling the coder await; handler finally confirms cleanup | Cancellation is not the proof. Cached cleanup failure still blocks. |
| Runner `_await_auxiliary_coder` | Stop/cancellation/finally cleanup for MERGE conflict resolution and ERROR diagnosis; credentials released only after confirmation | Uses the same runner owner, not a separate helper lifecycle. |
| Publication recovery, Retry PR creation and IDLE retained-publication upload | `_confirm_current_coder_cleanup` before consuming retained publication evidence; CODING timeout also terminates the current coder | Publication context is evidence of dispatch/publication identity, not independent process identity. |
| Runner `_hold_for_coder_cleanup` | Blocks commands, checkout work and handlers before each cycle when tracked cleanup is unresolved | Missing raw+managed handles and failure detail take a fast path; that is only local bookkeeping, not restart quiescence evidence. |
| `coder_login` terminal/device sessions | Supervised PTY/pipe launch; retained session and credential reservation on failure; fresh observation via `reconcile_cleanup` on subsequent cleanup | In-memory session lifetime only; reader settlement also required before release. |
| `coders/claude_models.py`, `model_catalog_bridge.py` | Supervised model probes/configured worker wrapper; structured unavailable errors retain owner; `ModelCatalogProcessOwner` serializes retained holds and observes before relaunch/shutdown | Catalog reconciliation can release a later-quiet hold without signalling; no durable daemon-restart owner. |
| `daemon/main.py` removal/shutdown; catalog bridge finally | Cancels and awaits owned cycles; SIGTERM requests cancellation and finally gathers owned tasks, reaching handler/adapter cleanup; bridge drains requests then shuts down login and catalog owners | Graceful shutdown is supported; abrupt daemon death bypasses Python finally blocks. |
| `InvocationSnapshot`, `RunRecord`, `RepoState`, publication context | Persist run/attempt, plugin/settings, phase/FIX count, task/branch and GitHub publication identity for recovery | No kernel lifetime identity, boot token, witness descriptor or cleanup receipt is restored. |
| `approval_commands.checkout_process_blocker` and its approval/recovery/retry callers | Conservative `/proc` cwd blocker, including orphan processes still using this checkout | Not a signal authority or global descendant inventory; cannot exclude escaped/chdir'd children. Never kill by its reported PID. |

### Tested positive proof and limits

For PR-431, the positive proof available **while the owner survives** is a
`CleanupResult.quiescent` from the retained, launch-proven `SupervisedProcess`,
with its leader reaped and owned-group snapshots reconciled. Signal delivery
uses freshly revalidated member birth tokens and pidfds, never a numeric group
ID as the delivery target. The witness preserves continuity when the leader
exits before a child. The supervisor excludes its own witness from workload
liveness, then closes/reaps that witness. A successful execution also settles
owned output readers; leader returncode, EOF, IDLE, cancellation, or a missing
handle alone cannot substitute for the cleanup result.

`test_runner_consumes_live_cleanup_proof_across_leader_exit` uses the existing
fixture-owned process pool and real supervisor/runner consumer for ordinary
exit, an exited leader with a TERM-ignoring child, and a leader killed through
its pidfd with that child still alive. It asserts child liveness before cleanup,
the surviving witness, KILL escalation where needed, terminal returncode,
quiescence and handle release. Existing supervisor tests additionally cover
lost/recycled identity, witness failure, cancellation, retained pipes and PTYs.

`test_real_failed_cleanup_retains_runner_owner_after_external_exit` denies only
pidfd signal delivery while leaving process inspection real. The live process
causes a retained ERROR hold. A fixture release file then permits real exit;
observation-only reconciliation proves later quiescence, but `cleanup()` still
returns its original cached failure and the runner still retains both handles.
The runner currently never calls `reconcile_cleanup`. Repeated cycles alone
therefore cannot release this hold even after external exit; login/catalog
consumers already have an explicit observation path.

`test_restored_invocation_is_not_restart_process_ownership` checkpoints a run
and serializes state, then restores both into a fresh runner while a real
fixture-owned process remains alive in a temporary checkout. Restored run and
invocation identity match, but neither process handle survives. The fresh
runner's missing-handle cleanup fast path returns true while the cwd blocker
still detects that process. This characterizes the gap; it is not restart
cleanup approval. The original fixture owner performs and verifies cleanup.
The test models the serialization boundary, not an actual daemon crash or
container restart. No test signals an unowned process or another worktree.

### Separately estimated follow-up

A restart implementation needs a durable invocation-bound ownership record
(repository/task generation, run/attempt, host/boot and process birth identity),
an explicit unknown/owned/quiet recovery decision, and a containment/continuity
mechanism that survives the owner disappearing. Numeric PID/PGID, saved settings
or absence from a cwd scan cannot provide that mechanism. Specify how to handle
crash windows before handle publication, PID reuse, reboot, lost witness,
chdir/session escape, stale receipts and persisted cleanup failure before
authorizing any signal or new launch. Ambiguous ownership must remain parked
for an operator; unrelated checkouts/worktrees are outside the authority.

Planning estimate for that separate outcome: 350–550 production lines across
supervisor ownership serialization/reconciliation, models and runner/recovery
admission; 450–700 test lines for isolated crash/restart and identity races;
80–120 documentation lines (roughly 6–9 files). This is a provisional range,
contingent on choosing the durable containment mechanism, not work delivered
here. Reuse supervisor signalling and existing login/catalog observation paths.
Replace the restart assumption implicit in runner missing-handle fast paths
with explicit recovery admission, and add runner reconciliation of retained
failures without overwriting the original failure evidence. CLI launch-error
string handoff is a candidate for structured ownership transfer. These are
replacement targets, not permission to remove legacy paths in this PR.
The raw auth-worker, Codex app-server and synchronous Claude migrations above
are additional scope, excluded from this restart estimate; each needs its own
caller compatibility and real-descendant verification estimate before execution.

Verification: `python -m pytest tests/test_process_supervisor.py
tests/runner/test_misc.py -q`. This investigation has no deployment prerequisite.
It does not prove real provider, bwrap/container, credential, or production
shutdown behavior. Operator release verification is separate; no deployment,
restart drill, runtime migration, or process-control code is delivered here.
