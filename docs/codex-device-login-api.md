# Codex device-login backend contract

The web control plane delegates device-code login to the daemon through the
existing coder-plugin Redis bridge. The daemon owns the supervised Codex CLI
process and retains login sessions only in memory. This contract is intended
for a later Settings UI; this change does not add login controls.

The adapter matches the `codex-cli 0.160.0` version pinned by the repository.
OpenAI's [access-token documentation](https://learn.chatgpt.com/docs/enterprise/access-tokens)
describes device-code authentication as administrator-controlled and warns
operators not to share the device code.

## Operations

- `POST /api/coders/{plugin}/device-login` starts a session. Its optional JSON
  body is `{"replace_existing": false}`. The field accepts JSON booleans only.
- `GET /api/coders/{plugin}/device-login/{session_id}` inspects a session.
- `DELETE /api/coders/{plugin}/device-login/{session_id}` requests cancellation.

A successful asynchronous start returns HTTP 202. Inspection and completed
cancellation return HTTP 200. Unsupported plugins return 422, missing or
expired sessions return 404, ownership/replacement conflicts return 409, and
an unavailable daemon bridge returns 503.

Every response contains only these allowlisted fields:

```text
plugin, session_id, state, detail, failure_reason,
verification_url, user_code, expires_at, cleanup_confirmed,
replacement_requested, reused_session, replacement_warning, auth_status
```

`verification_url`, `user_code`, and `expires_at` are present only while the
state is `waiting_for_user`. They are short-lived operator instructions and
must not be logged or copied into event history or diagnostics. Raw CLI output,
tokens, and credential contents are never returned. Provider prompts are checked
against these wire bounds before the session enters `waiting_for_user`; malformed
URLs or codes abort the owned process and return `malformed_output`.

The possible states are `unsupported`, `starting`, `waiting_for_user`,
`succeeded`, `failed`, `canceling`, `cancelled`, `expired`, `timed_out`,
`cleanup_failed`, and `not_found`. Callers should render `detail` and branch on
`state` or `failure_reason`; they must not infer process termination from a
cancellation request. `cleanup_failed` with `cleanup_confirmed: false` means
the daemon still retains ownership because process termination was not proven.

## Lifecycle and identity

Session identifiers are opaque. A session is bound at creation to the plugin
identity, effective environment, working directory, and credential location.
Login-capable plugins must expose a side-effect-free credential locator, and its
result must match the adapter's credential location; incomplete or inconsistent
capabilities fail closed before a login or coder process can start.
Repeating start for the same active plugin and credential location returns the
same session with `reused_session: true`; it does not spawn another login.
Another plugin or active coder invocation using that credential location is a
conflict. The daemon reserves a credential location synchronously before it
schedules either kind of process: coder invocations may share a read-only
reservation, while device login requires exclusive ownership. A login
reservation is retained until process cleanup is confirmed; if cleanup cannot
prove quiescence, both login replacement and new coder invocations remain
blocked for that location.

Auth probes, model discovery, and all daemon usage reads take the same shared
reservation before reading credentials. While login owns the location,
discovery and usage reads are suppressed, selection treats that coder as
provisionally available, and dispatch defers at the reservation boundary instead
of recording an auth, usage, or diagnosis failure. Releasing login ownership
first advances the location's credential generation; runners reject auth cache
entries from older generations, clear usage cache and failure-backoff state, and
refresh against the new credentials before launching work.

Device login, auth probes, model discovery, usage reads, and normal Codex coder
invocations preserve the daemon's inherited `HOME` so its Git configuration
remains available. They receive the same effective `CODEX_HOME`: an explicitly
inherited value when present, otherwise `<auth.codex_home_dir>/.codex`. A
successful login therefore updates the credential store used by subsequent
work. Web auth-status requests reach coder probes through the daemon bridge, so
they use the same credential reservation as coder dispatch and cannot race an
active login.

Terminal sessions are retained for five minutes. Sessions are daemon-memory
state: after a daemon restart, every old identifier returns `not_found` and
must not be presented as a live login. The Codex provider code expires after
15 minutes; the daemon application deadline is 16 minutes so provider expiry
can be distinguished from an application timeout when the CLI reports it.

On a successful Codex exit, the daemon refreshes the existing read-only auth
status before returning `succeeded`. `service_access_verified` remains unknown;
the login flow does not run inference to test entitlement.

## Credential replacement warning

The pinned Codex CLI clears existing authentication before beginning device
login. The backend therefore rejects a start when saved authentication exists
unless `replace_existing` is explicitly `true`, and it rejects replacement
while an active coder invocation uses the same credential location.

Explicit replacement is destructive: an unsuccessful, expired, timed-out, or
cancelled login can leave Codex authentication unavailable. The UI must show
`replacement_warning` before requesting replacement and must not promise that
the previous credentials will be preserved.
